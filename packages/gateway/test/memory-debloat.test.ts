import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { DEFAULT_DEBLOAT_BYTE_THRESHOLD, oversizedMemoryFiles } from "../src/memory/debloat";

const roots: string[] = [];

afterEach(async () => {
	for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true });
});

async function corpus(files: Record<string, string>): Promise<string> {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-debloat-"));
	roots.push(root);
	for (const [path, text] of Object.entries(files)) {
		await mkdir(dirname(join(root, path)), { recursive: true });
		await writeFile(join(root, path), text);
	}
	return root;
}

test("default threshold is 100 KB", () => {
	expect(DEFAULT_DEBLOAT_BYTE_THRESHOLD).toBe(102_400);
});

test("flags only files strictly larger than the threshold, largest first", async () => {
	const root = await corpus({
		"people/at-limit.md": "a".repeat(102_400),
		"people/over.md": "b".repeat(102_401),
		"projects/much-over.md": "c".repeat(300_000),
		"daily/2026-10/2026-10-21.md": "small",
	});
	expect(await oversizedMemoryFiles(root, DEFAULT_DEBLOAT_BYTE_THRESHOLD)).toEqual([
		{ path: "projects/much-over.md", bytes: 300_000 },
		{ path: "people/over.md", bytes: 102_401 },
	]);
});

test("respects a configured threshold and counts bytes, not characters", async () => {
	const root = await corpus({ "people/korean.md": "가".repeat(400) });
	// 400 Hangul syllables are 1200 UTF-8 bytes.
	expect(await oversizedMemoryFiles(root, 1_199)).toEqual([{ path: "people/korean.md", bytes: 1_200 }]);
	expect(await oversizedMemoryFiles(root, 1_200)).toEqual([]);
});

test("skips generated surfaces and non-corpus files but still checks ordinary index files", async () => {
	const big = "x".repeat(2_000);
	const root = await corpus({
		"MEMORY.md": big,
		"ops/rules/index.md": `---\ngenerated_by: rules-index\n---\n${big}`,
		"people/index.md": big,
		".git/objects/pack/pack-1.pack": big,
		"attachments/blob.bin": big,
	});
	expect(await oversizedMemoryFiles(root, 1_000)).toEqual([{ path: "people/index.md", bytes: 2_000 }]);
});

test("a missing corpus has nothing to de-bloat", async () => {
	expect(await oversizedMemoryFiles(join(tmpdir(), "gajaeway-debloat-missing", crypto.randomUUID()), 1)).toEqual([]);
});
