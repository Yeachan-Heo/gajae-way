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
