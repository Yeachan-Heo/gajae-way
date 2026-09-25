import { afterAll } from "bun:test";
import { tmpdir } from "node:os";
import { createScratchTmp, removeScratchTmp, sweepDeadScratch } from "./scratch-tmp";

// Loaded by the root bunfig.toml before every `bun test` run (#188).
const root = tmpdir();
await sweepDeadScratch(root);
const scratch = await createScratchTmp(root);
process.env.TMPDIR = scratch;

// A preload-level afterAll runs once, after the last test file of the run.
afterAll(async () => {
	process.env.TMPDIR = root;
	await removeScratchTmp(scratch);
});
