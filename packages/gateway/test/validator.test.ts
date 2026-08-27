import { afterEach, expect, test } from "bun:test";
import { appendFile, mkdtemp, rm, writeFile } from "node:fs/promises";
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
