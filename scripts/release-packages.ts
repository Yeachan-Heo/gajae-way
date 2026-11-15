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

export const RELEASE_ORDER = ["protocol", "images", "sdk", "cli"] as const;

export function isPrereleaseVersion(version: string): boolean {
	return version.split("+")[0]?.includes("-") ?? false;
}

// The highest published stable version is the only version allowed to move `latest`.
// Prereleases are ignored: publishing 1.3.0-rc.1 does not make 1.2.3 stale for latest.
export function highestPublishedStableVersion(versions: readonly string[]): string | undefined {
	let highest: string | undefined;
	for (const candidate of versions) {
		if (isPrereleaseVersion(candidate)) continue;
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

// `latest` is ordered content, not a pointer: only the highest published stable
// version may hold it. Reading the version list instead of the tag pointer is what
// stops an old release rerun from writing a stale pointer backwards. It is still a
// read before an unconditional `npm dist-tag add` (npm has no compare-and-set), so a
// version published between the read and the write can still land out of order; the
// post-write re-read detects that instead of hiding it. The version list and the
// dist-tag pointer come from the same registry document, so a cache stale for one can
// be stale for the other; what this removes is trusting the mutable pointer.
async function latestTarget(name: string, version: string, cwd: string): Promise<string> {
	const published = await readPublishedVersions(name, cwd);
	let highest: string | undefined;
	try {
		highest = highestPublishedStableVersion(published);
	} catch {
		throw new Error(`npm published-version list for ${name} contains an unparseable version: ${published.join(", ")}`);
	}
	if (highest === undefined) return version;
	return forwardTarget(version, highest, `npm published-version list for ${name}`);
}

// The version this tag may end up on: a candidate that already points past this version
// wins, because a tag never moves backwards. `latest` takes its candidate from the
// published version list; any other tag has no ordered list behind it, so its pointer is
// the only ordering source.
function forwardTarget(version: string, candidate: string | undefined, context: string): string {
	if (candidate === undefined) return version;
	return semverOrderOrThrow(candidate, version, context) > 0 ? candidate : version;
}

// A tag nobody claimed is a 404, not a failure: npm answers a tag lookup for a version
// that never held that tag the same way it answers a package that was never published.
async function readDistTagVersion(name: string, tag: string, cwd: string): Promise<string | undefined> {
	const current = await npmOutput(["view", `${name}@${tag}`, "version"], cwd);
	if (current.code !== 0) {
		if (current.stderr.includes("404")) return undefined;
		throw new Error(`npm dist-tag lookup failed for ${name}@${tag}: ${current.stderr || current.stdout}`);
	}
	if (!current.stdout) throw new Error(`npm dist-tag lookup returned an empty version for ${name}@${tag}`);
	return current.stdout;
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
//   - a tag that already points past this version is left alone, never moved back;
//   - `latest` is derived from the published version list, not from the tag pointer,
//     so a stale `latest` read cannot authorize a backwards write. When a newer
//     stable version is published, the pointer is converged forward (or left alone
//     when it is already there) and this release continues.
// The remaining window is a version published between the read and the write; the
// post-write re-read detects that instead of hiding it.
async function resumeTag(
	name: string,
	version: string,
	tag: string | undefined,
	manifest: PackedPackageManifest,
	cwd: string,
): Promise<void> {
	const effectiveTag = await resolveEffectiveTag(name, tag, manifest, cwd);
	const currentVersion = await readDistTagVersion(name, effectiveTag, cwd);

	// Only refuse the implicit promotion when it would move the tag: a prerelease that
	// already converged is a no-op, not an error.
	if (!tag && effectiveTag === "latest" && isPrereleaseVersion(version) && currentVersion !== version) {
		throw new Error(`refusing to implicitly tag prerelease ${name}@${version} as latest; specify --tag explicitly`);
	}

	// Reading the target unconditionally is what makes the equal-pointer case converge:
	// a pointer already sitting on this version does not prove it is still the newest, so
	// a later stable release still pulls `latest` forward instead of being ignored.
	const target =
		effectiveTag === "latest"
			? await latestTarget(name, version, cwd)
			: forwardTarget(version, currentVersion, `npm dist-tag ${effectiveTag} for ${name}`);
	if (target !== version) {
		// A newer release already moved this tag. Moving it back is never allowed, and
		// failing would strand this tag's already-published version, because the GitHub
		// release still has to be created. Leave the pointer alone and finish the release.
		console.log(`\n${name}@${effectiveTag} stays with ${target}; leaving it instead of moving it back to ${version}.`);
		if (currentVersion !== target) await applyDistTag(name, target, effectiveTag, cwd);
		return;
	}
	if (currentVersion === version) {
		console.log(`\n${name}@${version} already has dist-tag ${effectiveTag}.`);
		return;
	}
	await applyDistTag(name, version, effectiveTag, cwd);
}

// A scratch tag cannot collide with a release tag: npm rejects a tag that parses as a
// version, and everything but alphanumerics becomes a dash so the name stays a plain
// label.
function scratchTag(version: string): string {
	return `gajae-pending-${version.replace(/[^0-9A-Za-z-]/g, "-")}`;
}

async function removeDistTag(name: string, tag: string, cwd: string): Promise<void> {
	const result = await npmOutput(["dist-tag", "rm", name, tag], cwd);
	if (result.code !== 0) {
		console.log(`\nwarning: could not remove scratch dist-tag ${tag} from ${name}: ${result.stderr || result.stdout}`);
	}
}

async function applyDistTag(name: string, version: string, tag: string, cwd: string): Promise<void> {
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
		readFailure = undefined;
		if (!verified.stdout) continue;
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

async function main(): Promise<void> {
	const { publish, tag } = parseArgs(process.argv.slice(2));
	const root = new URL("..", import.meta.url).pathname;
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
				// a scratch tag instead, and the pointer is never moved backwards, not even for the
				// moment between publish and repair.
				const target =
					effectiveTag === "latest"
						? await latestTarget(packageName, packageVersion, dir)
						: forwardTarget(
								packageVersion,
								await readDistTagVersion(packageName, effectiveTag, dir),
								`npm dist-tag ${effectiveTag} for ${packageName}`,
							);
				const publishTag = target === packageVersion ? effectiveTag : scratchTag(packageVersion);
				await run(["npm", "publish", tarball, "--access", "public", "--tag", publishTag], dir);
				if (publishTag !== effectiveTag) {
					console.log(
						`\n${packageName}@${effectiveTag} stays with ${target}; ${packageVersion} was published under ${publishTag} instead of moving it back.`,
					);
					await removeDistTag(packageName, publishTag, dir);
				}
				// `npm publish` may have left the pointer on this version while a newer release
				// landed during the upload, and a scratch publish leaves the pointer untouched.
				// Re-read after publishing and converge on the same forward-only rule the resume
				// path uses, so neither case can end behind the newest release.
				const observed = await readDistTagVersion(packageName, effectiveTag, dir);
				const converged =
					effectiveTag === "latest"
						? forwardTarget(
								await latestTarget(packageName, packageVersion, dir),
								observed,
								`npm dist-tag latest for ${packageName}`,
							)
						: forwardTarget(packageVersion, observed, `npm dist-tag ${effectiveTag} for ${packageName}`);
				if (observed !== converged) {
					console.log(`\n${packageName}@${effectiveTag} stays with ${converged} after publishing ${packageVersion}.`);
					await applyDistTag(packageName, converged, effectiveTag, dir);
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
