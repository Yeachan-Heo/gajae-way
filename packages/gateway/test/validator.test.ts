import { afterEach, expect, test } from "bun:test";
import { appendFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initializeMemory, memoryRoot, regenerateMap } from "../src/memory/doctrine";
import { validateMemory } from "../src/memory/validator";

let home = "";
afterEach(async () => {
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});
async function root() {
	home = await mkdtemp(join(tmpdir(), "gajaeway-validator-"));
	return initializeMemory(home);
}
// The corpus is audited exactly as it sits on disk. Re-entering initializeMemory
// here would repair the generated map first and hide the very issue under test:
// startup guarantees the map covers every registered axis, while the audit is
// what reports a map that does not.
async function issue(code: string): Promise<void> {
	expect((await validateMemory(memoryRoot(home))).some((item) => item.code === code)).toBe(true);
}

test("validator reports map_dangling", async () => {
	const memory = await root();
	await appendFile(join(memory, "MEMORY.md"), "\n- [missing](daily/missing.md)\n");
	await issue("map_dangling");
});
test("validator reports unmapped_axis_dir", async () => {
	const memory = await root();
	await writeFile(join(memory, "MEMORY.md"), "# Memory map\n");
	await issue("unmapped_axis_dir");
});
test("validator reports long_form_map", async () => {
	const memory = await root();
	await appendFile(join(memory, "MEMORY.md"), `${"x".repeat(201)}\n`);
	await issue("long_form_map");
});
test("a generated pointer to a deep path is navigation, not long-form prose", async () => {
	const memory = await root();
	const deep = `daily/${"nested-directory-with-a-long-name/".repeat(3)}entry.md`;
	await appendFile(join(memory, "MEMORY.md"), `- [${deep}](${deep})\n`);
	// The rendered line is 240 characters; what it says is 111.
	expect((await validateMemory(memory)).some((found) => found.code === "long_form_map")).toBe(false);
});
test("long-form prose still fails even when it carries a link", async () => {
	const memory = await root();
	await appendFile(join(memory, "MEMORY.md"), `${"x".repeat(190)} [see](daily/a.md) ${"y".repeat(20)}\n`);
	await issue("long_form_map");
});
test("validator reports duplicate_file_hash", async () => {
	const memory = await root();
	await writeFile(join(memory, "daily", "a.md"), "same\n");
	await writeFile(join(memory, "tasks", "b.md"), "same\n");
	await regenerateMap(memory);
	await issue("duplicate_file_hash");
});
test("validator reports orphan_file", async () => {
	const memory = await root();
	await writeFile(join(memory, "stray.md"), "orphan\n");
	await issue("orphan_file");
});
test("validator reports out_of_root_link", async () => {
	const memory = await root();
	await appendFile(join(memory, "MEMORY.md"), "\n- [escape](../outside.md)\n");
	await issue("out_of_root_link");
});
test("validator reports map_content_drift", async () => {
	const memory = await root();
	await writeFile(join(memory, "daily", "2099-01-01.md"), "newest\n");
	await issue("map_content_drift");
});
test("validator accepts clean generated tree", async () => {
	const memory = await root();
	expect(await validateMemory(memory)).toEqual([]);
});

// A tree-indexed custom axis whose only index sits behind 173 characters of
// nesting. The old plain-text notice (`6 entries omitted; see <path>`) measured
// 213 characters once the audit replaced links with their text, so a map this
// code generated failed its own audit; the link form measures the 188-character
// path once and the notice text alone stays short.
async function longIndexCorpus(leaf: string): Promise<{ readonly memory: string; readonly index: string }> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-validator-long-index-"));
	const memory = memoryRoot(home);
	const directory = [leaf.repeat(57), leaf.repeat(57), leaf.repeat(57)].join("/");
	const index = `notes/${directory}/index.md`;
	await mkdir(join(memory, "notes", directory), { recursive: true, mode: 0o700 });
	await writeFile(
		join(memory, "axes.json"),
		`${JSON.stringify({ version: 1, axes: [{ id: "notes", root: "notes", index: "tree" }] }, null, "\t")}\n`,
	);
	// 205 entries plus the index exceed the 200-entry tree cap, so the map states
	// an omission in both sort orders.
	await Promise.all(
		Array.from({ length: 205 }, (_, position) =>
			writeFile(join(memory, `notes/entry-${String(position).padStart(3, "0")}.md`), `# entry ${position}\n`),
		),
	);
	await writeFile(join(memory, index), "# Note index\n");
	await initializeMemory(home);
	await regenerateMap(memory);
	return { memory, index };
}

const STRIPPED_AS_AUDIT = /\[([^\]]*)\]\([^)]*\)/g;

async function longNotice(memory: string): Promise<{ readonly section: string; readonly notice: string }> {
	const map = await readFile(join(memory, "MEMORY.md"), "utf8");
	const section = map.slice(map.indexOf("## notes"));
	const notice = section.split("\n").find((line) => line.includes("omitted"));
	expect(notice).toBeDefined();
	return { section, notice: notice! };
}

test("a long index omission notice stays short when the index sorts after the rendered prefix", async () => {
	const { memory, index } = await longIndexCorpus("z");
	const { section, notice } = await longNotice(memory);
	expect(notice.replaceAll(STRIPPED_AS_AUDIT, "$1").length).toBeLessThanOrEqual(200);
	expect(notice).toMatch(/^_\d+ entries omitted; see \[the index\]\(notes\/z+\/z+\/z+\/index\.md\)_$/);
	// The notice link is the only pointer; the index bullet is not appended again.
	expect(section.split(`](${index})`)).toHaveLength(2);
	expect((await validateMemory(memory)).filter((found) => found.code === "long_form_map")).toEqual([]);
});

test("a long index omission notice links once when the index sorts inside the rendered prefix", async () => {
	const { memory, index } = await longIndexCorpus("a");
	const { section, notice } = await longNotice(memory);
	expect(notice).toMatch(/^_\d+ entries omitted; see the index above_$/);
	// The rendered bullet above is the only pointer, and the notice carries none.
	expect(notice).not.toContain("](");
	expect(section.split(`](${index})`)).toHaveLength(2);
	expect((await validateMemory(memory)).filter((found) => found.code === "long_form_map")).toEqual([]);
});
