import { afterEach, expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryClosureQueue } from "../src/memory/closure";
import { memoryRoot } from "../src/memory/doctrine";
import { validateMemory } from "../src/memory/validator";
import { GatewayDatabase } from "../src/store/db";

let home = "";
let database: GatewayDatabase | undefined;
afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("serializes concurrent memory mutations into receipted linear commits", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const queue = new MemoryClosureQueue(database, home);
	await queue.initialize();
	await Promise.all(
		Array.from({ length: 20 }, (_, index) =>
			Promise.resolve(
				queue.enqueue({
					kind: "daily_capture",
					originRefJson: `{"n":${index}}`,
					userText: `user ${index}`,
					replyText: `reply ${index}`,
				}),
			),
		),
	);
	await queue.drain();
	expect(database.memoryIntentRows().every((intent) => intent.state === "receipted")).toBe(true);
	expect((await readFile(join(home, "memory-receipts.jsonl"), "utf8")).trim().split("\n")).toHaveLength(20);
	expect(await validateMemory(memoryRoot(home))).toEqual([]);
});
