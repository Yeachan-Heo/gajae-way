import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { autolinkCorpus, autolinkText, buildAliasIndex, frontmatterList } from "../src/memory/autolink";
import { initializeMemory, memoryGit } from "../src/memory/doctrine";

async function commitAll(root: string, message: string): Promise<void> {
	await memoryGit(root, ["add", "--all", "."]);
	await memoryGit(root, ["commit", "-m", message]);
}

test("alias index: filename stem, heading title, frontmatter aliases; ambiguous aliases dropped", async () => {
	const texts = new Map([
		["people/gaebal-gajae.md", "---\naliases: [개발가재, gaebal]\n---\n\n# 개발가재\n\n프로필.\n"],
		["projects/runtime.md", "# runtime\n\n내용.\n"],
		["projects/other-runtime.md", "# runtime\n\n다른 내용.\n"],
	]);
	const index = await buildAliasIndex("/x", [...texts.keys()], texts);
	const byAlias = new Map(index.map((entry) => [entry.alias, entry.path]));
	expect(byAlias.get("gaebal-gajae")).toBe("people/gaebal-gajae.md");
	expect(byAlias.get("개발가재")).toBe("people/gaebal-gajae.md");
	expect(byAlias.get("gaebal")).toBe("people/gaebal-gajae.md");
	// "runtime" is claimed by two files -> ambiguous -> dropped.
	expect(byAlias.has("runtime")).toBe(false);
});

test("autolinkText wraps first plain mention only and respects protected regions", () => {
	const index = [{ alias: "개발가재", path: "people/gaebal-gajae.md" }];
	const text = [
		"---",
		"tags: [ops]",
		"---",
		"# 개발가재 관련 규칙",
		"",
		"`개발가재` 코드 스팬은 보호된다.",
		"[개발가재](../people/gaebal-gajae.md) 기존 링크도 보호된다.",
		"본문에서 개발가재를 언급하면 여기가 링크된다. 두 번째 개발가재 언급은 그대로.",
	].join("\n");
	const { text: out, added } = autolinkText(text, "ops/rules/some.md", index);
	expect(added).toBe(1);
	expect(out).toContain("본문에서 [개발가재](../../people/gaebal-gajae.md)를 언급");
	expect(out.match(/\[개발가재\]/g)?.length).toBe(2); // existing + new, not more
	// Idempotent: a second pass adds nothing.
	expect(autolinkText(out, "ops/rules/some.md", index).added).toBe(0);
});

test("autolinkText falls back to a shorter alias when a longer alternative has an invalid suffix", () => {
	const index = [
		{ alias: "foo bar", path: "projects/long.md" },
		{ alias: "foo", path: "projects/short.md" },
	];
	const fallback = autolinkText("foo barx", "notes/source.md", index);
	expect(fallback.added).toBe(1);
	expect(fallback.text).toBe("[foo](../projects/short.md) barx");

	const longest = autolinkText("foo bar.", "notes/source.md", index);
	expect(longest.added).toBe(1);
	expect(longest.text).toBe("[foo bar](../projects/long.md).");
});

test("autolinkText case-folds aliases while preserving unicode prefix and CJK suffix boundaries", () => {
	const index = [{ alias: "foo", path: "projects/foo.md" }];
	const result = autolinkText("αFOO is embedded; FoO를 link.", "notes/source.md", index);
	expect(result.added).toBe(1);
	expect(result.text).toBe("αFOO is embedded; [FoO](../projects/foo.md)를 link.");
});

test("autolinkCorpus sweeps the corpus, skips raw daily, and reports counts", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-autolink-"));
	try {
		const root = await initializeMemory(home);
		await mkdir(join(root, "people"), { recursive: true });
		await mkdir(join(root, "ops/rules"), { recursive: true });
		await writeFile(
			join(root, "people/way-gajae.md"),
			"---\naliases: [웨이가재, way-gajae]\n---\n\n# 웨이가재\n\n프로필.\n",
		);
		await writeFile(join(root, "ops/rules/restart.md"), "# 재시작 규칙\n\n웨이가재 호스트에서 확립.\n");
		await writeFile(join(root, "daily/2026-09-01.md"), "## 캡처\n\n- user: 웨이가재 언급 raw 레이어.\n");
		await commitAll(root, "fixture");
		const report = await autolinkCorpus(root);
		expect(report.linksAdded).toBeGreaterThanOrEqual(1);
		expect(report.filesSkippedDirty).toBe(0);
		expect(report.runId).toBeString();
		const rule = await readFile(join(root, "ops/rules/restart.md"), "utf8");
		expect(rule).toContain("[웨이가재](../../people/way-gajae.md)");
		// Append-only raw capture axes are never rewritten.
		const daily = await readFile(join(root, "daily/2026-09-01.md"), "utf8");
		expect(daily).not.toContain("](");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("production-sized alias matching scans once instead of once per alias", () => {
	const index = Array.from({ length: 2_400 }, (_, value) => ({
		alias: `entity-${value.toString().padStart(4, "0")}`,
		path: `people/entity-${value.toString().padStart(4, "0")}.md`,
	}));
	const text = `${"ordinary corpus prose without entities. ".repeat(7_000)} entity-2399 appears here.`;
	const started = performance.now();
	const result = autolinkText(text, "ops/rules/large.md", index);
	expect(result.added).toBe(1);
	expect(result.text).toContain("[entity-2399](../../people/entity-2399.md)");
	expect(performance.now() - started).toBeLessThan(3_000);
}, 10_000);

test("concurrent retries share one sweep while dirty and staged user work stays untouched", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-autolink-concurrent-"));
	try {
		const root = await initializeMemory(home);
		await mkdir(join(root, "people"), { recursive: true });
		await mkdir(join(root, "ops/rules"), { recursive: true });
		await writeFile(join(root, "people/entity.md"), "---\naliases: [entity]\n---\n# Entity\n");
		for (let index = 0; index < 60; index++)
			await writeFile(join(root, `ops/rules/rule-${index}.md`), `# Rule ${index}\n\nentity mention\n`);
		await writeFile(join(root, "ops/rules/user-edit.md"), "# User edit\n\nentity original\n");
		await commitAll(root, "fixture");

		await writeFile(join(root, "ops/rules/user-edit.md"), "# User edit\n\nentity concurrent draft\n");
		await writeFile(join(root, "operator-note.txt"), "keep staged\n");
		await memoryGit(root, ["add", "operator-note.txt"]);

		let timerFired = false;
		setTimeout(() => {
			timerFired = true;
		}, 0);
		const first = autolinkCorpus(root);
		const retry = autolinkCorpus(root);
		expect(retry).toBe(first);
		const [receipt, retryReceipt] = await Promise.all([first, retry]);

		expect(retryReceipt.runId).toBe(receipt.runId);
		expect(receipt.filesSkippedDirty).toBe(1);
		expect(timerFired).toBe(true);
		expect(await readFile(join(root, "ops/rules/user-edit.md"), "utf8")).toBe(
			"# User edit\n\nentity concurrent draft\n",
		);
		expect(await memoryGit(root, ["diff", "--cached", "--name-only"])).toBe("operator-note.txt");
		expect(await memoryGit(root, ["log", "--format=%s", "-1"])).toStartWith("Memory autolink sweep:");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}, 20_000);

test("frontmatterList parses inline and block lists", () => {
	expect(frontmatterList("---\ntags: [a, b]\n---\nx", "tags")).toEqual(["a", "b"]);
	expect(frontmatterList("---\naliases:\n  - one\n  - two\n---\nx", "aliases")).toEqual(["one", "two"]);
	expect(frontmatterList("no frontmatter", "tags")).toEqual([]);
});
