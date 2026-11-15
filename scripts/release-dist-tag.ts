#!/usr/bin/env bun
/**
 * Resolves the npm dist-tag that release.yml publishes with.
 *
 * release.yml used to pass `--tag latest` for every tag-triggered run, including
 * prerelease tags such as v1.2.3-rc.1: `v*.*.*` matches those, and
 * scripts/release-packages.ts treats any supplied --tag as deliberate opt-in, so a
 * rerun of a prerelease tag silently promoted the prerelease to latest. The workflow
 * now asks this script instead: an explicit dispatch input wins, and otherwise the
 * tag follows the checked-out package versions, so a prerelease release goes to
 * `next` and only a stable release goes to `latest`.
 *
 * Usage:
 *   bun scripts/release-dist-tag.ts                       # print the derived tag
 *   bun scripts/release-dist-tag.ts --requested latest    # honor an explicit input
 */

import { fileURLToPath } from "node:url";
import { isPrereleaseVersion, RELEASE_ORDER } from "./release-packages";

export function resolveReleaseDistTag(requested: string | undefined, versions: readonly string[]): string {
	const explicit = requested?.trim() ?? "";
	if (explicit) return explicit;
	if (versions.length === 0) {
		throw new Error("no package versions were read; refusing to guess an npm dist-tag");
	}
	return versions.every((version) => !isPrereleaseVersion(version)) ? "latest" : "next";
}

async function readReleaseVersions(root: string): Promise<string[]> {
	const versions: string[] = [];
	for (const packageDirectory of RELEASE_ORDER) {
		const manifest = await Bun.file(`${root}packages/${packageDirectory}/package.json`).json();
		if (typeof manifest.version !== "string" || manifest.version.length === 0) {
			throw new Error(`packages/${packageDirectory}/package.json has no version`);
		}
		versions.push(manifest.version);
	}
	return versions;
}

async function main(): Promise<void> {
	let requested: string | undefined;
	const argv = process.argv.slice(2);
	for (let index = 0; index < argv.length; index++) {
		const arg = argv[index];
		if (arg === "--requested") {
			const value = argv[++index];
			if (value === undefined) {
				console.error("--requested requires a value; pass an empty string to derive the tag");
				process.exit(2);
			}
			requested = value;
		} else if (arg?.startsWith("--requested=")) {
			requested = arg.slice("--requested=".length);
		} else {
			console.error(`unknown argument: ${arg}`);
			process.exit(2);
		}
	}
	// A checkout path may contain characters the URL form escapes, so decode it before
	// using it as a filesystem path.
	const root = fileURLToPath(new URL("..", import.meta.url));
	console.log(resolveReleaseDistTag(requested, await readReleaseVersions(root)));
}

if (import.meta.main) {
	try {
		await main();
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exit(1);
	}
}
