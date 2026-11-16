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

function publishJob(workflow: string): string {
	const job = workflow.slice(workflow.indexOf("  publish-npm:"));
	return job.slice(0, job.indexOf("\n  build-binaries:"));
}

// Returns the shell body of a named step so a gate can be executed, not just pattern-matched.
function stepScript(job: string, name: string): string {
	const start = job.indexOf(`      - name: ${name}`);
	if (start < 0) return "";
	const step = job.slice(start);
	const run = /^ {8}run: \|\n/m.exec(step);
	if (!run) return "";
	const lines: string[] = [];
	for (const line of step.slice(run.index + run[0].length).split("\n")) {
		if (line.trim() === "") {
			lines.push("");
			continue;
		}
		if (!line.startsWith("          ")) break;
		lines.push(line.slice(10));
	}
	return lines.join("\n").trim();
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
	test("documents which dist-tag writers the workflow serializes", async () => {
		const readme = await Bun.file(new URL("../../../README.md", import.meta.url)).text();
		const publishing = readme.split("### Publishing packages")[1]?.split("### CI/CD")[0] ?? "";
		// The narrowed guarantee: runs requesting one explicit dist-tag share a group, so
		// writers of that tag serialize even across refs.
		expect(publishing).toContain(
			"What the workflow does serialize is every writer of one explicitly requested dist-tag",
		);
		expect(publishing).toContain("cannot interleave a read and a write on that tag");
		// Serializing costs GitHub's single pending slot, and the README has to say so
		// instead of implying every requested release is eventually published.
		expect(publishing).toContain("a third same-tag dispatch replaces the older pending one");
		// The residual has to stay documented: a derived channel tag is not in that group,
		// so the README must not promise cross-ref non-regression for every write.
		expect(publishing).toContain("Serialization therefore covers requested tags, not the channel tag");
		expect(publishing).toContain("can still interleave, so a concurrent release can move a channel tag backwards");
		expect(publishing).toContain(
			"an interrupted or failed reconciliation may leave a tag stale until a successful retry",
		);
		// The group key is the tag as written, so the padded spelling has to be rejected;
		// otherwise the README would promise serialization the workflow does not enforce.
		expect(publishing).toContain("A dispatched `npm_tag` must not contain whitespace or start with `-`");
		expect(publishing).not.toContain("so no tag moves backwards even for the moment");
	});
	test("serializes reruns of one immutable tag after the casing gate accepts it", async () => {
		const workflow = await releaseWorkflow();
		// A workflow-level group is entered before `canonical-tag` runs, so a case-variant
		// tag could replace a valid pending release and then fail the casing check.
		expect(workflow).not.toMatch(/^concurrency:/m);
		// One block only, and the scalar form counts too: a workflow-level `concurrency:`
		// declaration is the cheapest way to reintroduce the queue collision.
		expect(workflow.match(/^\s*concurrency:/gm) ?? []).toHaveLength(1);
		expect(publishJob(workflow)).toMatch(
			/concurrency:\n {6}group: release-publish-\$\{\{ github\.ref_type == 'tag' && github\.ref \|\| \(inputs\.npm_tag != '' && format\('tag-\{0\}', inputs\.npm_tag\) \|\| github\.run_id\) \}\}\n {6}cancel-in-progress: false/,
		);
	});

	// Two supported `workflow_dispatch` runs from different refs may request the same
	// custom dist-tag. Keyed by ref or run, their read-then-write interleaves: the older
	// run reads the pointer, the newer advances it, and the older write moves it back
	// until the post-write read accepts the version it just wrote. The requested tag is
	// therefore its own scope, while tag pushes keep the per-ref scope that stops distinct
	// tags from displacing each other.
	test("serializes concurrent writers of one explicitly requested dist-tag across refs", async () => {
		const workflow = await releaseWorkflow();
		const group = /^ {6}group: (.+)$/m.exec(publishJob(workflow))?.[1] ?? "";
		// Distinct tags never contend: a tag push stays on its own ref's group.
		expect(group).toContain("github.ref_type == 'tag' && github.ref");
		// Nothing ref- or run-specific may key the explicit-tag scope, or two refs
		// requesting the same tag would not share a group.
		const tagScope = /inputs\.npm_tag != '' && (.+?) \|\| github\.run_id/.exec(group)?.[1] ?? "";
		expect(tagScope).toBe("format('tag-{0}', inputs.npm_tag)");
		// An untagged branch dispatch still falls back to a per-run key, so it has no
		// second pending slot that a later dispatch could displace.
		expect(group.endsWith("|| github.run_id) }}")).toBe(true);
		expect(group).not.toBe("release-publish-${{ github.ref }}");
	});

	test("documents that branch dispatches are not queued behind each other", async () => {
		const readme = await Bun.file(new URL("../../../README.md", import.meta.url)).text();
		const publishing = readme.split("### Publishing packages")[1]?.split("### CI/CD")[0] ?? "";
		expect(publishing).toContain(
			"manual dispatches on a branch that request no `npm_tag` each run as their own release",
		);
		expect(publishing).toContain("so a group keyed by branch would silently skip a distinct requested release");
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

	test("rejects a case-variant tag without skipping a branch dispatch", async () => {
		const workflow = await releaseWorkflow();
		const job = workflow.slice(workflow.indexOf("  canonical-tag:"));
		const body = job.slice(0, job.indexOf("\n  publish-npm:"));
		// The casing check must be a step condition: a job-level `if` would make `needs`
		// skip publishing for a branch dispatch, which is a supported way to release.
		expect(body).not.toMatch(/^ {4}if:/m);
		expect(body).toContain("if: github.ref_type == 'tag'");
		expect(body).toContain("tr '[:upper:]' '[:lower:]'");
		expect(body).toContain("release tags must be lowercase");
		// The queue keys on `npm_tag` as written while the resolver and the step output
		// normalize it, so the same gate has to reject a divergent spelling before the
		// publish job enters a group.
		expect(body).toContain("require a canonical npm dist-tag");
		expect(body).toContain("NPM_TAG_INPUT: ${{ inputs.npm_tag }}");
		// The gate has to use the scripts' predicates: an ASCII whitespace class would let
		// NBSP or a BOM through, and a trim-only check would let an interior newline split
		// the `tag=$TAG` output line.
		const gateScript = stepScript(body, "require a canonical npm dist-tag");
		expect(gateScript).toContain('raw.startsWith("-")');
		expect(gateScript).toContain("/\\s/.test(raw)");
		// Publishing must be gated by the check, not only by the build.
		expect(workflow).toMatch(/needs: \[verify, canonical-tag\]/);
	});

	// The publish queue keys on `npm_tag` as written, while the rest of the pipeline reads
	// it differently: ` next ` and `next` publish one tag, and `canary\ntag=next` writes a
	// second output line so the run publishes `next` from the group named after the whole
	// raw string. Both are two writers of one tag in different groups, which is the race
	// this PR closes. The gate has to reject those spellings before the publish job enters
	// any group.
	test("rejects an npm_tag spelling the publish queue would not share", async () => {
		const workflow = await releaseWorkflow();
		const gate = workflow.slice(workflow.indexOf("  canonical-tag:"));
		const script = stepScript(gate, "require a canonical npm dist-tag");
		expect(script).toContain("NPM_TAG_INPUT");
		const root = await realpath(await mkdtemp(join(tmpdir(), "release npm tag gate ")));
		try {
			const file = join(root, "check.sh");
			await writeFile(file, `${script}\n`);
			const check = async (value: string) => {
				const result = Bun.spawn(["bash", file], {
					env: { ...process.env, NPM_TAG_INPUT: value },
					stdout: "pipe",
					stderr: "pipe",
				});
				const [stdout] = await Promise.all([new Response(result.stdout).text()]);
				return { exitCode: await result.exited, stdout };
			};
			const rejected = [
				" next ",
				"\tnext",
				"\u00a0next",
				"\ufeffnext",
				"next\u3000",
				"next\n",
				"canary\ntag=next",
				"canary tag",
				"canary\ttag",
				"-next",
				"--major",
			];
			for (const value of rejected) {
				const result = await check(value);
				expect(result.exitCode).toBe(1);
				// `::error::` annotations go to stdout, where the runner reads them.
				expect(result.stdout).toContain("::error::npm_tag");
			}
			// A padded tag resolves to the plain one: the two would key different groups
			// while `npm publish` wrote the same tag.
			expect(resolveReleaseDistTag(" next ", ["1.2.3"])).toBe("next");
			expect(resolveReleaseDistTag("\u00a0next", ["1.2.3"])).toBe("next");
			// Canonical spellings and an empty input (derive the channel tag) still pass.
			for (const value of ["next", "canary", "rc-1", "beta.1", ""]) {
				expect((await check(value)).exitCode).toBe(0);
				expect(resolveReleaseDistTag(value, ["1.2.3"])).toBe(value === "" ? "latest" : value);
			}
		} finally {
			await rm(root, { recursive: true, force: true });
		}
	});

	test("creates the GitHub release only for a tag ref", async () => {
		const workflow = await releaseWorkflow();
		const job = workflow.slice(workflow.indexOf("  github-release:"));
		// Nothing needs this job, so skipping it on a branch dispatch does not propagate and
		// the manual dispatch still publishes and finishes green.
		expect(job).toMatch(/^ {4}if: github\.ref_type == 'tag'$/m);
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
