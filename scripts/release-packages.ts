#!/usr/bin/env bun
/**
 * Root-level release workflow for the npm-publishable packages
 * (@gajae-gateway/protocol, @gajae-gateway/images, @gajae-gateway/sdk,
 * @gajae-gateway/cli), in dependency order.
 *
 * Usage:
 *   bun scripts/release-packages.ts            # build + `bun pm pack` dry run (default, safe)
 *   bun scripts/release-packages.ts --publish   # build + pack + `npm publish` of the tarball
 *   bun scripts/release-packages.ts --tag next  # forward a dist-tag to npm publish
 *
 * Every mode runs each package's `build` script, then `bun pm pack` into
 * dist-packed/, which rewrites `workspace:*` dependencies to real versions.
 * Dry-run mode stops there and never touches the registry. Publish mode
 * uploads that exact tarball with the npm CLI, because npm (>= 11.5.1)
 * supports OIDC trusted publishing from GitHub Actions and `bun publish`
 * does not. For OIDC dist-tag updates, enable "Allow npm dist-tag" for each
 * package's trusted publisher and use npm 11.21.0+ (11.x) or 12.2.0+ (12.x).
 */

import { mkdir, readdir } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

export const RELEASE_ORDER = ["protocol", "images", "sdk", "cli"] as const;

export function isPrereleaseVersion(version: string): boolean {
	return version.split("+")[0]?.includes("-") ?? false;
}

// The highest published stable version is the only version allowed to move `latest`.
// Prereleases are ignored: publishing 1.3.0-rc.1 does not make 1.2.3 stale for latest.
// The workflow derives exactly two channels from the released versions: `latest` for a
// stable release and `next` for a prerelease. Each channel has a floor in the published
// version list, which is what keeps a concurrent release of the same channel from being
// left behind; an operator-chosen tag has no channel and is ordered by its pointer only.
// The floor spans release lines: a `next` pointer follows the highest published
// prerelease, so a prerelease line published under another tag still moves it. A channel
// tag moved by hand below the floor is converged back up at the next release of that
// channel; a hand move above the floor is kept, because no write goes backwards.
// A Map, not an object literal: `tag` is maintainer-supplied and an object would answer
// for `constructor`, `toString`, or `__proto__`.
const CHANNEL_FLOORS = new Map<string, (version: string) => boolean>([
	["latest", (version) => !isPrereleaseVersion(version)],
	["next", (version) => isPrereleaseVersion(version)],
]);

export function highestPublishedVersion(
	versions: readonly string[],
	include: (version: string) => boolean,
): string | undefined {
	let highest: string | undefined;
	for (const candidate of versions) {
		if (!include(candidate)) continue;
		if (highest === undefined || Bun.semver.order(candidate, highest) > 0) highest = candidate;
	}
	return highest;
}

function parseArgs(argv: string[]): { publish: boolean; tag?: string } {
	let publish = false;
	let tag: string | undefined;
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i];
		if (arg === "--publish") publish = true;
		else if (arg === "--tag") {
			const value = argv[++i];
			if (!value || value.startsWith("-")) {
				console.error("--tag requires a non-empty value, e.g. --tag next");
				process.exit(2);
			}
			tag = value;
		} else if (arg?.startsWith("--tag=")) {
			const value = arg.slice("--tag=".length);
			if (!value || value.startsWith("-")) {
				console.error("--tag requires a non-empty value, e.g. --tag=next");
				process.exit(2);
			}
			tag = value;
		} else {
			console.error(`unknown argument: ${arg}`);
			process.exit(2);
		}
	}
	return { publish, tag };
}

async function run(cmd: string[], cwd: string): Promise<void> {
	console.log(`$ (${cwd}) ${cmd.join(" ")}`);
	const proc = Bun.spawn(cmd, { cwd, stdout: "inherit", stderr: "inherit" });
	const code = await proc.exited;
	if (code !== 0) throw new Error(`command failed (exit ${code}): ${cmd.join(" ")} in ${cwd}`);
}

export function parseNpmViewResult(
	stdout: string,
	stderr: string,
	code: number,
	expectedVersion: string,
): { alreadyPublished: boolean; error?: string } {
	if (code === 0) {
		const result = stdout.trim();
		if (result === expectedVersion) {
			return { alreadyPublished: true };
		}
		return { alreadyPublished: false, error: `npm view returned unexpected version: ${result}` };
	}

	// Check if the error is a 404 (not found) - this is expected for new packages
	if (stderr.includes("404")) {
		return { alreadyPublished: false };
	}

	// Any other error should fail
	return { alreadyPublished: false, error: stderr.trim() || stdout.trim() };
}

export async function checkNpmRegistry(
	name: string,
	version: string,
): Promise<{ alreadyPublished: boolean; error?: string }> {
	const proc = Bun.spawn(["npm", "view", `${name}@${version}`, "version"], {
		stdout: "pipe",
		stderr: "pipe",
	});
	const stdout = await new Response(proc.stdout).text();
	const stderr = await new Response(proc.stderr).text();
	const code = await proc.exited;

	return parseNpmViewResult(stdout, stderr, code, version);
}

async function readPublishedVersions(name: string, cwd: string): Promise<string[]> {
	const result = await npmOutput(["view", name, "versions", "--json"], cwd);
	if (result.code !== 0) {
		// A package that has never been published is the normal first-release case, and
		// npm answers the versions lookup with the same 404 as the metadata check.
		if (result.stderr.includes("404")) return [];
		throw new Error(`npm published-version lookup failed for ${name}: ${result.stderr || result.stdout}`);
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(result.stdout);
	} catch {
		throw new Error(`npm published-version lookup returned invalid JSON for ${name}: ${result.stdout}`);
	}
	if (!Array.isArray(parsed) || !parsed.every((value): value is string => typeof value === "string")) {
		throw new Error(`npm published-version lookup returned unexpected data for ${name}: ${result.stdout}`);
	}
	return parsed;
}

// A channel tag is ordered content whose floor is the highest published version in that
// channel, not the mutable pointer: reading the version list is what stops an old release
// rerun from writing a stale pointer backwards. It is still a read before an unconditional
// `npm dist-tag add` (npm has no compare-and-set), so a version published between the
// read and the write can still land out of order; the post-write re-read detects that
// instead of hiding it. The version list and the dist-tag pointer come from the same
// registry document, so a cache stale for one can be stale for the other; what this
// removes is authorizing a write from the pointer alone.
async function channelTarget(name: string, version: string, tag: string, cwd: string): Promise<string> {
	const channel = CHANNEL_FLOORS.get(tag);
	if (channel === undefined) return version;
	const published = await readPublishedVersions(name, cwd);
	let highest: string | undefined;
	try {
		highest = highestPublishedVersion(published, channel);
	} catch {
		throw new Error(`npm published-version list for ${name} contains an unparseable version: ${published.join(", ")}`);
	}
	if (highest === undefined) return version;
	return forwardTarget(version, highest, `npm published-version list for ${name}`);
}

// The version this tag may end up on: a candidate that already points past this version
// wins, because a tag never moves backwards. A channel tag takes its candidate from the
// published version list; every tag also takes the live pointer as a candidate, so a
// pointer an operator or an earlier run placed ahead is never overwritten downward.
async function tagTarget(
	name: string,
	version: string,
	tag: string,
	pointer: string | undefined,
	cwd: string,
): Promise<string> {
	const candidate = await channelTarget(name, version, tag, cwd);
	return forwardTarget(candidate, pointer, `npm dist-tag ${tag} for ${name}`);
}

// A candidate that already points past this version wins.
function forwardTarget(version: string, candidate: string | undefined, context: string): string {
	if (candidate === undefined) return version;
	return semverOrderOrThrow(candidate, version, context) > 0 ? candidate : version;
}

// A tag nobody claimed is simply absent from the tag map, which is why the map is read
// instead of `npm view <name>@<tag>`: that form makes an unset tag an error whose shape
// has to be guessed, while a missing entry is unambiguous data.
async function readDistTags(name: string, cwd: string): Promise<Record<string, string>> {
	const result = await npmOutput(["view", name, "dist-tags", "--json"], cwd);
	if (result.code !== 0) {
		// A package that was never published has no tags to read.
		if (result.stderr.includes("404")) return Object.create(null);
		throw new Error(`npm dist-tag lookup failed for ${name}: ${result.stderr || result.stdout}`);
	}
	return parseDistTagsJson(name, result.stdout);
}

// npm 11 prints the tag map itself; npm 12 wraps a single-package object field in a
// one-element array (`[{"latest":"0.5.0"}]`). Both shapes name exactly one map, so the
// wrapper is unwrapped; anything else is still unexpected data.
export function parseDistTagsJson(name: string, stdout: string): Record<string, string> {
	let parsed: unknown;
	try {
		parsed = JSON.parse(stdout);
	} catch {
		throw new Error(`npm dist-tag lookup returned invalid JSON for ${name}: ${stdout}`);
	}
	if (Array.isArray(parsed) && parsed.length === 1) parsed = parsed[0];
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
		throw new Error(`npm dist-tag lookup returned unexpected data for ${name}: ${stdout}`);
	}
	// A null prototype keeps a `__proto__` entry from resolving to Object.prototype.
	const tags: Record<string, string> = Object.create(null);
	for (const [key, value] of Object.entries(parsed)) {
		if (typeof value !== "string" || value.length === 0) {
			throw new Error(`npm dist-tag lookup returned unexpected data for ${name}: ${stdout}`);
		}
		tags[key] = value;
	}
	return tags;
}

async function readDistTagVersion(name: string, tag: string, cwd: string): Promise<string | undefined> {
	return (await readDistTags(name, cwd))[tag];
}

function semverOrderOrThrow(left: string, right: string, context: string): number {
	try {
		return Bun.semver.order(left, right);
	} catch {
		throw new Error(`${context} points at an unparseable version: ${left}`);
	}
}

async function npmOutput(args: string[], cwd: string): Promise<{ stdout: string; stderr: string; code: number }> {
	const proc = Bun.spawn(["npm", ...args], { cwd, stdout: "pipe", stderr: "pipe" });
	const [stdout, stderr, code] = await Promise.all([
		new Response(proc.stdout).text(),
		new Response(proc.stderr).text(),
		proc.exited,
	]);
	return { stdout: stdout.trim(), stderr: stderr.trim(), code };
}

// npm's own tag precedence: --tag, then the packed publishConfig.tag, then
// `npm config get tag`, run from the same cwd npm publish uses. release.yml resolves
// --tag from the release content, so the fallbacks only run for local releases.
async function resolveEffectiveTag(
	name: string,
	tag: string | undefined,
	manifest: PackedPackageManifest,
	cwd: string,
): Promise<string> {
	let effectiveTag = tag ?? manifest.publishConfig?.tag;
	if (!effectiveTag) {
		const result = await npmOutput(["config", "get", "tag"], cwd);
		if (result.code !== 0) {
			throw new Error(`npm tag configuration failed for ${name}: ${result.stderr || result.stdout}`);
		}
		if (!result.stdout) {
			throw new Error(`npm config get tag returned no tag for ${name}; pass --tag explicitly`);
		}
		effectiveTag = result.stdout;
	}
	// parseArgs rejects an option-shaped --tag, but publishConfig.tag and npm's own
	// config are package-controlled data: a tag such as "--force" would be consumed by
	// npm as an option, so the command would mutate a different tag than the one checked
	// here. Reject the shape rather than pass it as a positional argument.
	if (effectiveTag.startsWith("-")) {
		throw new Error(
			`refusing to use dist-tag ${JSON.stringify(effectiveTag)} for ${name}: npm would parse it as an option`,
		);
	}
	if (/\s/.test(effectiveTag)) {
		throw new Error(`refusing to use dist-tag ${JSON.stringify(effectiveTag)} for ${name}: it contains whitespace`);
	}
	return effectiveTag;
}

// Publishing immutable content and moving a dist-tag are separate concerns, and only
// the pointer move is order-sensitive: an already-published version must not fail its
// release because a newer release holds the tag, or the tagged GitHub release would
// never be created. Every write is still a read followed by an unconditional
// `npm dist-tag add` (npm has no compare-and-set), so ordering rests on two rules:
//   - no write is decided below the tag's live pointer or, for a channel tag (`latest`,
//     `next`), below the highest published version in that channel; a tag that already
//     points past this version is left alone and this release continues, and an older
//     version of the channel is converged forward instead of being written back.
//   - this run decides ordering before every write, including the tag npm writes during
//     `npm publish`, so it never writes a version below what it last observed.
// The remaining window is a version published between the read and the write. A channel
// tag closes it in the same run: applyDistTag re-derives the tag from the version list
// after writing and converges forward, so a stale write repairs itself instead of
// waiting for the next release. A tag with no channel floor has only its pointer as an
// ordering source, so every write to it is authorized by a read taken immediately before
// the write: a release that already moved the tag wins and this run never moves it back,
// and that outcome is printed instead of silently skipped. A release still landing
// between that final read and the write is invisible without a compare-and-set, so the
// next release has to repair that pointer.
async function resumeTag(
	name: string,
	version: string,
	tag: string | undefined,
	manifest: PackedPackageManifest,
	cwd: string,
): Promise<void> {
	const effectiveTag = await resolveEffectiveTag(name, tag, manifest, cwd);
	const tags = await readDistTags(name, cwd);
	const currentVersion = tags[effectiveTag];
	// A previous run can have published this version under a scratch tag and died before
	// removing it, including the run-unique form taken when the derived name was occupied.
	// Any such tag is safe to delete only while it still points at this version.
	const orphanBase = scratchTagBase(version);
	for (const [orphan, orphanVersion] of Object.entries(tags)) {
		// Never reclaim the tag this run is releasing: an operator may keep a tag in the
		// reserved namespace, and the snapshot above is not re-read after a deletion.
		if (orphan === effectiveTag) continue;
		if (orphanVersion !== version) continue;
		if (orphan !== orphanBase && !orphan.startsWith(`${orphanBase}-`)) continue;
		await removeDistTag(name, orphan, version, cwd);
	}

	// Only refuse the implicit promotion when it would move the tag: a prerelease that
	// already converged is a no-op, not an error.
	if (!tag && effectiveTag === "latest" && isPrereleaseVersion(version) && currentVersion !== version) {
		throw new Error(`refusing to implicitly tag prerelease ${name}@${version} as latest; specify --tag explicitly`);
	}

	// Reading the target unconditionally is what makes the equal-pointer case converge:
	// a pointer already sitting on this version does not prove it is still the newest, so
	// a later stable release still pulls `latest` forward instead of being ignored.
	const target = await tagTarget(name, version, effectiveTag, currentVersion, cwd);
	if (target !== version) {
		// A newer release already moved this tag. Moving it back is never allowed, and
		// failing would strand this tag's already-published version, because the GitHub
		// release still has to be created. Leave the pointer alone and finish the release.
		if (currentVersion === target) {
			console.log(
				`\n${name}@${effectiveTag} stays with ${target}; leaving it instead of moving it back to ${version}.`,
			);
			return;
		}
		console.log(
			`\n${name}@${effectiveTag} stays with ${target}; moving it forward from ${currentVersion ?? "no pointer"} instead of back to ${version}.`,
		);
		await applyDistTag(name, target, effectiveTag, cwd);
		return;
	}
	if (currentVersion === version) {
		console.log(`\n${name}@${version} already has dist-tag ${effectiveTag}.`);
		return;
	}
	await applyDistTag(name, version, effectiveTag, cwd);
}

// The scratch tag that holds a version published without moving a channel. It is
// derived from the version so an interrupted run can find it again, but never at the
// cost of an existing tag: if that name is already taken, a run-unique suffix is used
// instead and the run only ever removes a scratch tag it can still see its own version
// on. Both the derived name and every generated candidate are checked against the tag
// map read before publishing.
function scratchTagBase(version: string): string {
	return `gajae-pending-${version.replace(/[^0-9A-Za-z-]/g, "-")}`;
}

// The suffix makes a contested name unique to this run, but it is not evidence that the
// generated name is free: the tag map read before publishing is. A candidate that is
// already taken would be moved by `npm publish --tag` and then removed by the cleanup,
// so every candidate is checked against the map and a repeated random draw is retried.
// Failing after a bounded number of draws is safer than publishing to a tag this run
// cannot prove it owns; a collision is already improbable, so the bound only guards the
// generator itself.
const SCRATCH_TAG_ATTEMPTS = 8;

export function scratchTag(
	version: string,
	tags: Readonly<Record<string, string>>,
	mintSuffix: () => string = () => crypto.randomUUID().slice(0, 8),
): string {
	const base = scratchTagBase(version);
	const isFree = (candidate: string) => tags[candidate] === undefined;
	if (isFree(base)) return base;
	for (let attempt = 0; attempt < SCRATCH_TAG_ATTEMPTS; attempt++) {
		const candidate = `${base}-${mintSuffix()}`;
		if (isFree(candidate)) return candidate;
	}
	throw new Error(`could not find an unused scratch dist-tag for ${version}`);
}

async function removeDistTag(name: string, tag: string, version: string, cwd: string): Promise<void> {
	// Only a tag this run still sees its own version on is ours to delete.
	const current = await readDistTagVersion(name, tag, cwd);
	if (current !== version) {
		console.log(
			current === undefined
				? `\nwarning: could not see ${name}@${tag} after publishing ${version}; leaving it in place.`
				: `\nwarning: leaving ${name}@${tag} at ${current}; it is not this release.`,
		);
		return;
	}
	const result = await npmOutput(["dist-tag", "rm", name, tag], cwd);
	if (result.code !== 0) {
		console.log(`\nwarning: could not remove scratch dist-tag ${tag} from ${name}: ${result.stderr || result.stdout}`);
	}
}

async function applyDistTagOnce(name: string, version: string, tag: string, cwd: string): Promise<void> {
	try {
		await run(["npm", "dist-tag", "add", `${name}@${version}`, tag], cwd);
	} catch (error) {
		throw new Error(
			`npm dist-tag update failed for ${name}@${version}. OIDC requires npm 11.21.0+ (11.x) or 12.2.0+ (12.x) and "Allow npm dist-tag" on this package's trusted publisher.`,
			{ cause: error },
		);
	}

	// The write is the last chance to notice an interleaved release: if a concurrent run
	// moved the tag after our read, npm accepts our write and ours accepts theirs in
	// whichever order they land. Re-read until the registry reflects the write. A tag
	// that ends up above this version is a concurrent release winning forward, which is
	// the ordering enforced here, so the run continues; a lower version means an older
	// writer clobbered this one and fails the run. Never re-write here: a second
	// unconditional write could win by repetition and clobber a genuinely newer tag. A
	// read that fails outright is retried the same way: a tag written a moment ago can
	// still 404 while metadata catches up.
	let observed: string | undefined;
	let readFailure: string | undefined;
	for (let attempt = 0; attempt < 3; attempt++) {
		if (attempt > 0) await Bun.sleep(1_000);
		const verified = await npmOutput(["view", `${name}@${tag}`, "version"], cwd);
		if (verified.code !== 0) {
			readFailure = verified.stderr || verified.stdout || "npm view exited non-zero with no output";
			continue;
		}
		// An exit-0 read with no value is not a confirmation of the write.
		if (!verified.stdout) {
			readFailure = "npm view exited 0 with no version";
			continue;
		}
		readFailure = undefined;
		if (semverOrderOrThrow(verified.stdout, version, `npm dist-tag ${tag} for ${name}`) >= 0) {
			if (verified.stdout !== version) {
				console.log(
					`\n${name}@${tag} moved forward to ${verified.stdout} while this run was writing ${version}; keeping it.`,
				);
			}
			return;
		}
		observed = verified.stdout;
	}
	if (readFailure !== undefined) {
		throw new Error(`npm dist-tag verification failed for ${name}@${tag}: ${readFailure}`);
	}
	throw new Error(
		`dist-tag ${tag} for ${name} resolves to ${observed ?? "nothing"} after writing ${version}; a concurrent release moved it backwards`,
	);
}

// `npm dist-tag add` is unconditional, so the value a run writes can be stale by the
// time it lands: a resumed release that read the published version list before a newer
// release published would otherwise leave the channel tag on its own older version, and
// its post-write check would pass because the pointer matches what it wrote. A channel
// tag is therefore re-derived from the version list after writing and converged forward
// in the same run; each pass re-reads, so a release that lands during the repair is
// picked up too. A tag with no channel floor has no equivalent source and is only
// ordered by its pointer.
const CONVERGENCE_ATTEMPTS = 3;

async function reconcileChannel(name: string, version: string, tag: string, cwd: string): Promise<void> {
	for (let attempt = 0; attempt < CONVERGENCE_ATTEMPTS; attempt++) {
		const target = await channelTarget(name, version, tag, cwd);
		const pointer = await readDistTagVersion(name, tag, cwd);
		if (pointer !== undefined && semverOrderOrThrow(pointer, target, `npm dist-tag ${tag} for ${name}`) >= 0) return;
		console.log(`\n${name}@${tag} stays with ${target}; converged forward while ${version} was being released.`);
		await applyDistTagOnce(name, target, tag, cwd);
	}
	// Bounded repair, but never a success claim: if the channel tag is still behind the
	// floor after the last write, fail. `publish-npm` gates the GitHub release, so staying
	// silent here would let a stale channel pass for a finished release. The version is
	// already published, so a rerun resumes and repairs instead of republishing.
	//
	// The terminal read is retried once and its failure is reported separately: a metadata
	// read that has not caught up is not evidence that a concurrent release moved the tag,
	// and blaming a concurrent writer for a lagging read would be a false diagnosis.
	let belowFloor: string | undefined;
	let readFailure: string | undefined;
	for (let attempt = 0; attempt < 2; attempt++) {
		if (attempt > 0) await Bun.sleep(1_000);
		let floor: string;
		let pointer: string | undefined;
		try {
			floor = await channelTarget(name, version, tag, cwd);
			pointer = await readDistTagVersion(name, tag, cwd);
		} catch (error) {
			readFailure = `dist-tag ${tag} for ${name} could not be read after ${CONVERGENCE_ATTEMPTS} convergence attempts: ${error instanceof Error ? error.message : String(error)}`;
			continue;
		}
		if (pointer === undefined) {
			readFailure = `dist-tag ${tag} for ${name} could not be read after ${CONVERGENCE_ATTEMPTS} convergence attempts`;
			continue;
		}
		if (semverOrderOrThrow(pointer, floor, `npm dist-tag ${tag} for ${name}`) >= 0) return;
		// Keep the concrete evidence: a later read failure must not replace it.
		belowFloor = `dist-tag ${tag} for ${name} is ${pointer} after ${CONVERGENCE_ATTEMPTS} convergence attempts, below ${floor}; a concurrent release kept moving it`;
		readFailure = undefined;
	}
	throw new Error(belowFloor ?? readFailure ?? `dist-tag ${tag} for ${name} did not converge`);
}

// A tag with no channel floor has no published-version list to repair it afterwards, so
// its write is authorized by a fresh observation taken immediately before it. npm has no
// compare-and-set, but this closes the window between the snapshot read that decided the
// target and the write: a release that moved the tag in between must win, and this run
// must never move it back. The release continues because failing would strand this tag's
// already-published version and skip the GitHub release still to be created; the outcome
// is printed instead of silently skipped.
async function applyCustomDistTag(name: string, version: string, tag: string, cwd: string): Promise<void> {
	const current = await readDistTagVersion(name, tag, cwd);
	if (current !== undefined && semverOrderOrThrow(current, version, `npm dist-tag ${tag} for ${name}`) > 0) {
		console.log(`\n${name}@${tag} stays with ${current}; leaving it instead of moving it back to ${version}.`);
		return;
	}
	await applyDistTagOnce(name, version, tag, cwd);
}

async function applyDistTag(name: string, version: string, tag: string, cwd: string): Promise<void> {
	if (!CHANNEL_FLOORS.has(tag)) {
		await applyCustomDistTag(name, version, tag, cwd);
		return;
	}
	await applyDistTagOnce(name, version, tag, cwd);
	await reconcileChannel(name, version, tag, cwd);
}

async function main(): Promise<void> {
	const { publish, tag } = parseArgs(process.argv.slice(2));
	// Decode the URL form so a checkout path with spaces or `#` still resolves.
	const root = fileURLToPath(new URL("..", import.meta.url));
	const packedDir = `${root}dist-packed`;
	await mkdir(packedDir, { recursive: true });

	const versions = new Map<string, string>();
	for (const pkg of RELEASE_ORDER) {
		const manifest = await Bun.file(`${root}packages/${pkg}/package.json`).json();
		versions.set(manifest.name, manifest.version);
	}
	const workspacePackages = await readWorkspacePackages(root);
	const releasePackageNames = new Set(versions.keys());

	for (const pkg of RELEASE_ORDER) {
		const dir = `${root}packages/${pkg}`;
		await run(["bun", "run", "build"], dir);
		const tarball = `${packedDir}/gajae-gateway-${pkg}.tgz`;
		await run(["bun", "pm", "pack", "--filename", tarball], dir);
		const packedManifest = await readPackedManifest(tarball);
		assertPinnedWorkspaceDeps(packedManifest, versions);
		assertNoPrivateWorkspaceDeps(packedManifest, workspacePackages, releasePackageNames);

		if (publish) {
			const packageName = packedManifest.name;
			const packageVersion = versions.get(packageName)!;
			const registryCheck = await checkNpmRegistry(packageName, packageVersion);

			if (registryCheck.alreadyPublished) {
				await resumeTag(packageName, packageVersion, tag, packedManifest, dir);
			} else if (registryCheck.error) {
				throw new Error(`registry check failed for ${packageName}@${packageVersion}: ${registryCheck.error}`);
			} else {
				const effectiveTag = await resolveEffectiveTag(packageName, tag, packedManifest, dir);
				// --tag is opt-in, but an omitted tag is not: npm would default it to `latest`,
				// so a local prerelease release would be promoted implicitly. Refuse before
				// publishing rather than after the content is already on the registry.
				if (!tag && effectiveTag === "latest" && isPrereleaseVersion(packageVersion)) {
					throw new Error(
						`refusing to implicitly tag prerelease ${packageName}@${packageVersion} as latest; specify --tag explicitly`,
					);
				}
				// `npm publish` always writes a tag, so the ordering has to be decided before it
				// runs: when a newer release already owns this tag, the version is published under
				// a scratch tag instead, so this run never writes the pointer below what it last
				// observed; the window between that read and the write is the documented residual.
				const tags = await readDistTags(packageName, dir);
				let target = await tagTarget(packageName, packageVersion, effectiveTag, tags[effectiveTag], dir);
				let availableTags = tags;
				// A custom tag has no published-version floor and no post-publish reconcile, so it is
				// re-observed immediately before `npm publish` writes it: a newer release that moved it
				// after the snapshot read above must win, and this version goes under a scratch tag
				// instead of moving the pointer back. The fresh map also selects the scratch name, so an
				// operator tag created since the snapshot is never taken for a free scratch name. A
				// channel tag is covered by the version-list read inside tagTarget and by the
				// post-publish reconciliation.
				if (!CHANNEL_FLOORS.has(effectiveTag)) {
					availableTags = await readDistTags(packageName, dir);
					target = forwardTarget(
						target,
						availableTags[effectiveTag],
						`npm dist-tag ${effectiveTag} for ${packageName}`,
					);
				}
				const publishTag = target === packageVersion ? effectiveTag : scratchTag(packageVersion, availableTags);
				await run(["npm", "publish", tarball, "--access", "public", "--tag", publishTag], dir);
				if (publishTag !== effectiveTag) {
					console.log(
						`\n${packageName}@${effectiveTag} stays with ${target}; ${packageVersion} was published under ${publishTag} instead of moving it back.`,
					);
					await removeDistTag(packageName, publishTag, packageVersion, dir);
				}
				// `npm publish` writes the tag itself, so the post-write reconciliation is what
				// catches a release of the same channel that landed during the upload; a scratch
				// publish leaves the pointer untouched and reconciles from the same version list.
				if (CHANNEL_FLOORS.has(effectiveTag)) {
					await reconcileChannel(packageName, packageVersion, effectiveTag, dir);
				}
			}
		}
	}

	if (publish) console.log(`\nPublished, in order: ${RELEASE_ORDER.map((p) => `@gajae-gateway/${p}`).join(", ")}`);
	else console.log(`\nDry-run packed tarballs written to dist-packed/. Inspect them, then re-run with --publish.`);
}

if (import.meta.main) await main();

// `bun pm pack` rewrites `workspace:*` from bun.lock, which can hold a stale
// workspace version after a package.json bump. Refuse a tarball that would pin
// a sibling package to anything but the version being released alongside it.
function assertPinnedWorkspaceDeps(manifest: PackedPackageManifest, versions: Map<string, string>): void {
	for (const [dep, pinned] of Object.entries<string>(manifest.dependencies ?? {})) {
		const expected = versions.get(dep);
		if (expected !== undefined && pinned !== expected) {
			throw new Error(
				`${manifest.name} pins ${dep}@${pinned} but the workspace is at ${expected}; update the workspace versions in bun.lock`,
			);
		}
	}
}

interface PackedPackageManifest {
	readonly name: string;
	readonly publishConfig?: { readonly tag?: string };
	readonly dependencies?: Record<string, string>;
	readonly optionalDependencies?: Record<string, string>;
	readonly peerDependencies?: Record<string, string>;
}

interface WorkspacePackage {
	readonly name: string;
	readonly private?: boolean;
}

async function readWorkspacePackages(root: string): Promise<Map<string, WorkspacePackage>> {
	const workspacePackages = new Map<string, WorkspacePackage>();
	for (const entry of await readdir(join(root, "packages"), { withFileTypes: true })) {
		if (!entry.isDirectory()) continue;
		const manifestPath = join(root, "packages", entry.name, "package.json");
		const file = Bun.file(manifestPath);
		if (!(await file.exists())) continue;
		const manifest = (await file.json()) as WorkspacePackage;
		workspacePackages.set(manifest.name, manifest);
	}
	return workspacePackages;
}

export function assertNoPrivateWorkspaceDeps(
	manifest: PackedPackageManifest,
	workspacePackages: ReadonlyMap<string, WorkspacePackage>,
	releasePackageNames: ReadonlySet<string>,
): void {
	for (const section of ["dependencies", "optionalDependencies", "peerDependencies"] as const) {
		for (const dependency of Object.keys(manifest[section] ?? {})) {
			const workspacePackage = workspacePackages.get(dependency);
			if (workspacePackage === undefined) continue;
			if (workspacePackage.private === true || !releasePackageNames.has(dependency)) {
				const reason = workspacePackage.private === true ? "private" : "not in the release map";
				throw new Error(`${manifest.name} lists ${dependency} in ${section}, but the workspace package is ${reason}`);
			}
		}
	}
}

async function readPackedManifest(tarball: string): Promise<PackedPackageManifest> {
	const proc = Bun.spawn(["tar", "-xOzf", tarball, "package/package.json"], { stdout: "pipe", stderr: "inherit" });
	const text = await new Response(proc.stdout).text();
	if ((await proc.exited) !== 0) throw new Error(`could not read package.json from ${tarball}`);
	return JSON.parse(text) as PackedPackageManifest;
}
