import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initializeMemory, memoryRoot, regenerateMap } from "../src/memory/doctrine";
import { searchMemory } from "../src/memory/retrieve";

test("retrieves mapped Markdown with BM25", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-retrieve-"));
	try {
		const root = await initializeMemory(home);
		await writeFile(
			join(root, "projects", "aurora.md"),
			"# Aurora\n\nThe aurora release uses a cobalt deployment key.\n",
		);
		await writeFile(join(root, "tasks", "chores.md"), "# Chores\n\nWash the mugs.\n");
		await regenerateMap(root);
		const hits = await searchMemory(memoryRoot(home), "cobalt deployment", 3);
		expect(hits[0]?.path).toBe("projects/aurora.md");
		expect(hits[0]?.excerpt).toContain("cobalt");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("korean queries match inflected forms via cjk character bigrams", async () => {
	const { mkdtemp } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { mkdir, writeFile } = await import("node:fs/promises");
	const { initializeMemory } = await import("../src/memory/doctrine");
	const { searchMemory } = await import("../src/memory/retrieve");
	const home = await mkdtemp(join(tmpdir(), "gajaeway-cjk-"));
	const root = await initializeMemory(home);
	await mkdir(join(root, "decisions"), { recursive: true });
	await writeFile(join(root, "decisions/canon.md"), "# 결정\n\n정본화를 매 6시간 주기로 돌린다.\n");
	await writeFile(join(root, "decisions/other.md"), "# 결정\n\n배포 절차는 재서명을 포함한다.\n");
	// Stem query hits the document that only contains the inflected form.
	const hits = await searchMemory(root, "정본화 주기");
	expect(hits.length).toBeGreaterThanOrEqual(1);
	expect(hits[0]?.path).toBe("decisions/canon.md");
	// Latin behavior unchanged: exact word match still required.
	const latin = await searchMemory(root, "resign");
	expect(latin.find((hit) => hit.path === "decisions/other.md")).toBeUndefined();
});

test("a strong hit pulls its crosslinked neighbour in as a discounted secondary hit", async () => {
	const { mkdtemp, mkdir, writeFile } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { initializeMemory } = await import("../src/memory/doctrine");
	const { searchMemory } = await import("../src/memory/retrieve");
	const home = await mkdtemp(join(tmpdir(), "gajaeway-linkhop-"));
	const root = await initializeMemory(home);
	await mkdir(join(root, "decisions"), { recursive: true });
	await mkdir(join(root, "people"), { recursive: true });
	// The neighbour deliberately contains NO query terms: only the link reaches it.
	await writeFile(join(root, "people/collab.md"), "# collab\n\n조용한 협력자 프로필.\n");
	await writeFile(
		join(root, "decisions/cutover.md"),
		"# cutover decision\n\ncutover 결정은 [collab](../people/collab.md) 협의로 확정.\n",
	);
	const hits = await searchMemory(root, "cutover 결정");
	expect(hits[0]?.path).toBe("decisions/cutover.md");
	const neighbour = hits.find((hit) => hit.path === "people/collab.md");
	expect(neighbour).toBeDefined();
	expect(neighbour!.score).toBeLessThan(hits[0]!.score);
	expect(neighbour!.excerpt).toContain("linked from decisions/cutover.md");
	// Secondary hits never displace direct hits under a tight limit.
	const tight = await searchMemory(root, "cutover 결정", 1);
	expect(tight).toHaveLength(1);
	expect(tight[0]?.path).toBe("decisions/cutover.md");
});
