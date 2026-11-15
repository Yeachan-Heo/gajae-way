// Covers the release workflow's dist-tag resolution and concurrency wiring.
// `v*.*.*` matches prerelease tags too, so a tag-triggered run that defaults to
// `--tag latest` promotes a prerelease on a plain rerun. These tests pin the
// resolution rules and the workflow lines that must use them. They live under
// packages/ because CI runs `bun test packages/` and `biome ci packages/`.
import { describe, expect, test } from "bun:test";
import { copyFile, mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { resolveReleaseDistTag } from "../../../scripts/release-dist-tag";
import { RELEASE_ORDER } from "../../../scripts/release-packages";

const WORKFLOW_URL = new URL("../../../.github/workflows/release.yml", import.meta.url);

async function releaseWorkflow(): Promise<string> {
	return Bun.file(WORKFLOW_URL).text();
}

describe("release dist-tag resolution", () => {
	test("derives latest for a stable release", () => {
		expect(resolveReleaseDistTag("", ["1.2.3", "1.2.3", "1.2.3"])).toBe("latest");
		expect(resolveReleaseDistTag(undefined, ["1.2.3"])).toBe("latest");
	});

	test("derives next for a prerelease release", () => {
		expect(resolveReleaseDistTag("", ["1.2.3-rc.1", "1.2.3-rc.1", "1.2.3-rc.1"])).toBe("next");
	});

	test("derives next when any released package is a prerelease", () => {
		expect(resolveReleaseDistTag(undefined, ["1.2.3", "0.1.0", "1.2.3-rc.1"])).toBe("next");
	});

	test("honors an explicit dispatch input over the derived tag", () => {
		expect(resolveReleaseDistTag("canary", ["1.2.3"])).toBe("canary");
		expect(resolveReleaseDistTag("latest", ["1.2.3-rc.1"])).toBe("latest");
	});

	test("refuses to guess a tag without package versions", () => {
		expect(() => resolveReleaseDistTag("", [])).toThrow("refusing to guess");
	});

	test("the CLI resolves the checked-out release versions", async () => {
		const script = new URL("../../../scripts/release-dist-tag.ts", import.meta.url);
		const derived = Bun.spawn([process.execPath, script.pathname, "--requested", ""], {
			stdout: "pipe",
			stderr: "pipe",
		});
		const [stdout, stderr] = await Promise.all([
			new Response(derived.stdout).text(),
			new Response(derived.stderr).text(),
		]);
		expect(await derived.exited).toBe(0);
		expect(stderr).toBe("");
		// Derive the expectation from the checked-out versions: hardcoding "latest"
		// would fail on the first prerelease bump, which is the case this protects.
		const versions: string[] = [];
		for (const packageDirectory of RELEASE_ORDER) {
			const manifest = await Bun.file(
				new URL(`../../../packages/${packageDirectory}/package.json`, import.meta.url),
			).json();
			versions.push(manifest.version);
		}
		expect(stdout.trim()).toBe(resolveReleaseDistTag("", versions));
	});

	test("the CLI fails instead of deriving when --requested has no value", async () => {
		const script = new URL("../../../scripts/release-dist-tag.ts", import.meta.url);
		const result = Bun.spawn([process.execPath, script.pathname, "--requested"], { stdout: "pipe", stderr: "pipe" });
		expect(await result.exited).toBe(2);
	});

	test("reads manifests from a checkout path with spaces and a hash", async () => {
		const root = await realpath(await mkdtemp(join(tmpdir(), "release dist-tag stable #")));
		try {
			await mkdir(join(root, "scripts"));
			for (const file of ["release-dist-tag.ts", "release-packages.ts"]) {
				await copyFile(new URL(`../../../scripts/${file}`, import.meta.url), join(root, "scripts", file));
			}
			for (const directory of RELEASE_ORDER) {
				await mkdir(join(root, "packages", directory), { recursive: true });
				await writeFile(
					join(root, "packages", directory, "package.json"),
					JSON.stringify({ name: `@fixture/${directory}`, version: "1.2.3" }),
				);
			}
			const derived = Bun.spawn([process.execPath, join(root, "scripts", "release-dist-tag.ts"), "--requested", ""], {
				stdout: "pipe",
				stderr: "pipe",
			});
			const [stdout, stderr] = await Promise.all([
				new Response(derived.stdout).text(),
				new Response(derived.stderr).text(),
			]);
			expect(await derived.exited).toBe(0);
			expect(stderr).toBe("");
			expect(stdout.trim()).toBe("latest");
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	}, 30_000);

	// The test above compares the CLI against the same function that feeds it, so it would
	// still pass with a hardcoded `latest`. This one runs the real CLI against prerelease
	// manifests, which is the regression that used to promote a prerelease on a rerun.
	test("the CLI derives next from prerelease package manifests", async () => {
		// Space and `#` force URL percent-encoding, so this fails if the resolver uses the
		// URL pathname instead of `fileURLToPath`.
		const root = await realpath(await mkdtemp(join(tmpdir(), "release dist-tag #")));
		try {
			await mkdir(join(root, "scripts"));
			for (const file of ["release-dist-tag.ts", "release-packages.ts"]) {
				await copyFile(new URL(`../../../scripts/${file}`, import.meta.url), join(root, "scripts", file));
			}
			for (const directory of RELEASE_ORDER) {
				await mkdir(join(root, "packages", directory), { recursive: true });
				await writeFile(
					join(root, "packages", directory, "package.json"),
					JSON.stringify({ name: `@fixture/${directory}`, version: "1.2.3-rc.1" }),
				);
			}
			const derived = Bun.spawn([process.execPath, join(root, "scripts", "release-dist-tag.ts"), "--requested", ""], {
				stdout: "pipe",
				stderr: "pipe",
			});
			const [stdout, stderr] = await Promise.all([
				new Response(derived.stdout).text(),
				new Response(derived.stderr).text(),
			]);
			expect(await derived.exited).toBe(0);
			expect(stderr).toBe("");
			expect(stdout.trim()).toBe("next");
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});
});

describe("release workflow wiring", () => {
	test("serializes reruns per ref so distinct tagged releases are never dropped", async () => {
		const workflow = await releaseWorkflow();
		// Anchored and workflow-level: a job-level block would shadow the workflow group and
		// reintroduce the dropped pending run, so the key must sit above `jobs:`.
		const concurrency = workflow.search(/^concurrency:$/m);
		const jobs = workflow.search(/^jobs:$/m);
		expect(concurrency).toBeGreaterThan(-1);
		expect(jobs).toBeGreaterThan(concurrency);
		expect(workflow).toMatch(/^ {2}group: release-\$\{\{ github\.ref \}\}$/m);
		expect(workflow).toContain("cancel-in-progress: false");
		expect(workflow.match(/^\s*concurrency:/gm) ?? []).toHaveLength(1);
	});

	test("publishes with the resolved dist-tag instead of a latest fallback", async () => {
		const workflow = await releaseWorkflow();
		expect(workflow).toContain('bun scripts/release-dist-tag.ts --requested "$NPM_TAG_INPUT"');
		expect(workflow).toMatch(/NPM_DIST_TAG: \$\{\{ steps\.dist-tag\.outputs\.tag \}\}/);
		expect(workflow).toContain('run: bun run release:publish -- --tag "$NPM_DIST_TAG"');
		// No fallback of any spelling can hide in the resolve step: the tag it prints is
		// either the dispatch input or the derived value.
		expect(workflow).toContain("- name: Resolve npm dist-tag");
		expect(workflow).toContain("tag=$TAG");
		const resolveStep = workflow.slice(workflow.indexOf("- name: Resolve npm dist-tag"));
		const resolveBody = resolveStep.slice(0, resolveStep.indexOf("\n      - name:"));
		expect(resolveBody).not.toContain("latest");
	});

	test("does not default a manual dispatch input to latest", async () => {
		const workflow = await releaseWorkflow();
		expect(workflow).toMatch(/npm_tag:[\s\S]*?default: ""/);
	});

	test("rejects a case-variant tag before it can share the ref's concurrency group", async () => {
		const workflow = await releaseWorkflow();
		expect(workflow).toMatch(/^ {2}canonical-tag:$/m);
		expect(workflow).toContain("tr '[:upper:]' '[:lower:]'");
		expect(workflow).toContain("release tags must be lowercase");
		// Publishing must be gated by the casing check, not only by the build.
		expect(workflow).toMatch(/needs: \[verify, canonical-tag\]/);
	});

	test("mirrors the npm dist-tag in the GitHub release's prerelease flag", async () => {
		const workflow = await releaseWorkflow();
		expect(workflow).toMatch(/npm-tag: \$\{\{ steps\.dist-tag\.outputs\.tag \}\}/);
		expect(workflow).toMatch(/prerelease: \$\{\{ needs\.publish-npm\.outputs\.npm-tag != 'latest' \}\}/);
		// The tag name is not the source of truth: `v*.*.*` also matches prerelease tags
		// whose checked-out versions are stable, and the npm tag is what this run published.
		expect(workflow).not.toContain("contains(github.ref_name, '-')");
	});
});
