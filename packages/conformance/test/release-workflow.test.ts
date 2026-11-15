// Covers the release workflow's dist-tag resolution and concurrency wiring.
// `v*.*.*` matches prerelease tags too, so a tag-triggered run that defaults to
// `--tag latest` promotes a prerelease on a plain rerun. These tests pin the
// resolution rules and the workflow lines that must use them. They live under
// packages/ because CI runs `bun test packages/` and `biome ci packages/`.
import { describe, expect, test } from "bun:test";
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
});

describe("release workflow wiring", () => {
	test("serializes reruns per ref so distinct tagged releases are never dropped", async () => {
		const workflow = await releaseWorkflow();
		expect(workflow).toMatch(/group: release-\$\{\{ github\.ref \}\}/);
		expect(workflow).toContain("cancel-in-progress: false");
		// The old global group let a third tag push cancel a pending tagged release.
		expect(workflow).not.toMatch(/^ {2}group: release$/m);
	});

	test("publishes with the resolved dist-tag instead of a latest fallback", async () => {
		const workflow = await releaseWorkflow();
		expect(workflow).toContain('bun scripts/release-dist-tag.ts --requested "$NPM_TAG_INPUT"');
		expect(workflow).toMatch(/NPM_DIST_TAG: \$\{\{ steps\.dist-tag\.outputs\.tag \}\}/);
		expect(workflow).toContain('run: bun run release:publish -- --tag "$NPM_DIST_TAG"');
		expect(workflow).not.toContain('if [ -z "$TAG" ]; then TAG="latest"; fi');
	});

	test("does not default a manual dispatch input to latest", async () => {
		const workflow = await releaseWorkflow();
		expect(workflow).toMatch(/npm_tag:[\s\S]*?default: ""/);
	});

	test("marks a prerelease tag's GitHub release as a prerelease", async () => {
		const workflow = await releaseWorkflow();
		expect(workflow).toMatch(/prerelease: \$\{\{ github\.ref_type == 'tag' && contains\(github\.ref_name, '-'\) \}\}/);
	});
});
