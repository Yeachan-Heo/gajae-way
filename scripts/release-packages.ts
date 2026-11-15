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

async function npmOutput(args: string[], cwd: string): Promise<{ stdout: string; stderr: string; code: number }> {
	const proc = Bun.spawn(["npm", ...args], { cwd, stdout: "pipe", stderr: "pipe" });
	const [stdout, stderr, code] = await Promise.all([
		new Response(proc.stdout).text(),
		new Response(proc.stderr).text(),
		proc.exited,
	]);
	return { stdout: stdout.trim(), stderr: stderr.trim(), code };
}

// A resumed release must converge on the same dist-tags a clean run would have
// written, because the skip in main() also skipped `npm publish --tag`. The tag is
// resolved with npm's own precedence (--tag, then the packed publishConfig.tag, then
// `npm config get tag`, run from the same cwd npm publish uses) and then applied
// explicitly. release.yml always passes --tag, so the fallbacks and the prerelease
// guard only run for interactive releases; `--tag` is also the escape hatch when a
// tag must deliberately move backwards.
async function resumeTag(
	name: string,
	version: string,
	tag: string | undefined,
	manifest: PackedPackageManifest,
	cwd: string,
): Promise<void> {
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

	const current = await npmOutput(["view", `${name}@${effectiveTag}`, "version"], cwd);
	if (current.code !== 0 && !current.stderr.includes("404")) {
		throw new Error(`npm dist-tag lookup failed for ${name}@${effectiveTag}: ${current.stderr || current.stdout}`);
	}
	if (current.code === 0) {
		if (!current.stdout) throw new Error(`npm dist-tag lookup returned an empty version for ${name}@${effectiveTag}`);
		if (current.stdout === version) {
			console.log(`\n${name}@${version} already has dist-tag ${effectiveTag}.`);
			return;
		}
	}

	// Only refuse the implicit promotion when it would move the tag: a prerelease that
	// already converged took the no-op return above and is not an error.
	if (!tag && effectiveTag === "latest" && version.split("+")[0]?.includes("-")) {
		throw new Error(`refusing to implicitly tag prerelease ${name}@${version} as latest; specify --tag explicitly`);
	}
	if (current.code === 0) {
		let order: number;
		try {
			order = Bun.semver.order(current.stdout, version);
		} catch {
			throw new Error(`npm dist-tag ${effectiveTag} for ${name} points at an unparseable version: ${current.stdout}`);
		}
		if (order > 0) {
			throw new Error(`refusing to move ${name}@${effectiveTag} backwards from ${current.stdout} to ${version}`);
		}
	}

	try {
		await run(["npm", "dist-tag", "add", `${name}@${version}`, effectiveTag], cwd);
	} catch (error) {
		throw new Error(
			`npm dist-tag update failed for ${name}@${version}. OIDC requires npm 11.21.0+ (11.x) or 12.2.0+ (12.x) and "Allow npm dist-tag" on this package's trusted publisher.`,
			{ cause: error },
		);
	}
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
				const publishCmd = ["npm", "publish", tarball, "--access", "public"];
				if (tag) publishCmd.push("--tag", tag);
				await run(publishCmd, dir);
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
