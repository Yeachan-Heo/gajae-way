#!/usr/bin/env bun
/**
 * Root-level release workflow for the npm-publishable packages
 * (@gajae-gateway/protocol, @gajae-gateway/sdk, @gajae-gateway/cli), in dependency order.
 *
 * Usage:
 *   bun scripts/release-packages.ts            # build + npm-pack-style dry run (default, safe)
 *   bun scripts/release-packages.ts --publish   # build + bun publish (talks to the registry)
 *   bun scripts/release-packages.ts --tag next  # forward a dist-tag to bun publish
 *
 * Dry-run mode never touches the registry: it runs each package's `build`
 * script, then `bun pm pack` into a scratch directory so the exact tarball
 * contents and rewritten `workspace:*` dependency versions can be inspected
 * before a real publish.
 */

const RELEASE_ORDER = ["protocol", "sdk", "cli"] as const;

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

async function main(): Promise<void> {
	const { publish, tag } = parseArgs(process.argv.slice(2));
	const root = new URL("..", import.meta.url).pathname;

	for (const pkg of RELEASE_ORDER) {
		const dir = `${root}packages/${pkg}`;
		await run(["bun", "run", "build"], dir);
		if (publish) {
			const publishCmd = ["bun", "publish"];
			if (tag) publishCmd.push("--tag", tag);
			await run(publishCmd, dir);
		} else {
			await run(["bun", "pm", "pack", "--destination", `${root}dist-packed`], dir);
		}
	}

	if (publish) console.log(`\nPublished, in order: ${RELEASE_ORDER.map((p) => `@gajae-gateway/${p}`).join(", ")}`);
	else console.log(`\nDry-run packed tarballs written to dist-packed/. Inspect them, then re-run with --publish.`);
}

await main();
