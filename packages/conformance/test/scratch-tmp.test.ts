import { afterEach, expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { createScratchTmp, removeScratchTmp, SCRATCH_PREFIX, sweepDeadScratch } from "../src/scratch-tmp";

// #188: test fixtures leaked hundreds of dirs per run into the shared /tmp, where
// the host cleanup cron then retried them forever. Every run is scoped to one
// scratch dir that is removed at exit, whatever the fixtures left behind.

const parents: string[] = [];
afterEach(async () => {
	for (const parent of parents.splice(0)) await rm(parent, { recursive: true, force: true });
});
async function parent() {
	const dir = await mkdtemp(join(tmpdir(), "scratch-parent-"));
	parents.push(dir);
	return dir;
}

test("the test run's tmpdir is a per-run scratch dir, not the shared tmp root", () => {
	expect(basename(tmpdir()).startsWith(`${SCRATCH_PREFIX}${process.pid}-`)).toBe(true);
});

test("removal succeeds on fixtures that left read-only directories behind", async () => {
	const root = await parent();
	const scratch = await createScratchTmp(root);
	const locked = join(scratch, "perm-x", "s");
	await mkdir(locked, { recursive: true });
	await writeFile(join(locked, "f"), "x");
	await chmod(locked, 0o500);
	await chmod(join(scratch, "perm-x"), 0o500);
	await removeScratchTmp(scratch);
	expect(await readdir(root)).toEqual([]);
});

test("a killed run's scratch dir is swept; a live run's is kept", async () => {
	const root = await parent();
	const live = await createScratchTmp(root);
	// PID 2^22+1 exceeds Linux pid_max, so no process can own it.
	const dead = join(root, `${SCRATCH_PREFIX}4194305-abc123`);
	await mkdir(join(dead, "slack-recovery-x"), { recursive: true });
	await chmod(join(dead, "slack-recovery-x"), 0o500);
	const unrelated = join(root, "someone-else");
	await mkdir(unrelated);
	await sweepDeadScratch(root);
	expect((await readdir(root)).sort()).toEqual([basename(live), "someone-else"].sort());
	expect((await stat(live)).isDirectory()).toBe(true);
});
