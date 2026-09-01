import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { autolinkCorpus, autolinkText, buildAliasIndex, frontmatterList } from "../src/memory/autolink";
import { initializeMemory } from "../src/memory/doctrine";

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

test("autolinkCorpus sweeps the corpus, skips raw daily, and reports counts", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-autolink-"));
	const root = await initializeMemory(home);
	await mkdir(join(root, "people"), { recursive: true });
	await mkdir(join(root, "ops/rules"), { recursive: true });
	await writeFile(
		join(root, "people/way-gajae.md"),
		"---\naliases: [웨이가재, way-gajae]\n---\n\n# 웨이가재\n\n프로필.\n",
	);
	await writeFile(join(root, "ops/rules/restart.md"), "# 재시작 규칙\n\n웨이가재 호스트에서 확립.\n");
	await writeFile(join(root, "daily/2026-09-01.md"), "## 캡처\n\n- user: 웨이가재 언급 raw 레이어.\n");
	const report = await autolinkCorpus(root);
	expect(report.linksAdded).toBeGreaterThanOrEqual(1);
	const rule = await readFile(join(root, "ops/rules/restart.md"), "utf8");
	expect(rule).toContain("[웨이가재](../../people/way-gajae.md)");
	// Raw daily layer untouched.
	const daily = await readFile(join(root, "daily/2026-09-01.md"), "utf8");
	expect(daily).not.toContain("](");
});

test("frontmatterList parses inline and block lists", () => {
	expect(frontmatterList("---\ntags: [a, b]\n---\nx", "tags")).toEqual(["a", "b"]);
	expect(frontmatterList("---\naliases:\n  - one\n  - two\n---\nx", "aliases")).toEqual(["one", "two"]);
	expect(frontmatterList("no frontmatter", "tags")).toEqual([]);
});
