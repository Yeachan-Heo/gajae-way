import { afterEach, expect, test } from "bun:test";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryClosureQueue } from "../src/memory/closure";
import { memoryGit, memoryRoot } from "../src/memory/doctrine";
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
test("a closure commit tracks every axis, not only the capture axis", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const queue = new MemoryClosureQueue(database, home);
	await queue.initialize();
	const root = memoryRoot(home);

	// Curated material a persona writes between two captures: an append-only
	// reflection and a routable ops rule. Neither lives under the capture axis.
	await mkdir(join(root, "ops/rules"), { recursive: true });
	await writeFile(join(root, "reflections/2026-08-27.md"), "# drift\n\nStage the whole corpus.\n");
	await writeFile(join(root, "ops/rules/staging.md"), "# staging\n\nCommit every axis.\n");

	queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "u", replyText: "r" });
	await queue.drain();

	const tracked = (await memoryGit(root, ["ls-files"])).split("\n").filter(Boolean);
	expect(tracked).toContain("reflections/2026-08-27.md");
	expect(tracked).toContain("ops/rules/staging.md");
	// And the corpus is clean: nothing was left behind as an uncommitted change.
	expect((await memoryGit(root, ["status", "--porcelain"])).trim()).toBe("");
});

test("a failing memory intent is contained at the worker and later intents still close (#227)", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const queue = new MemoryClosureQueue(database, home);
	await queue.initialize();

	const broken = crypto.randomUUID();
	database.memoryIntentCreate({ id: broken, kind: "daily_capture", payloadJson: "{}" });
	queue.enqueueExistingId(broken);
	const good = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "u", replyText: "r" });

	// Before the fix the rejected tail was an unhandled rejection (gateway exit)
	// and the good intent behind it never ran.
	await queue.drain();
	expect(queue.failures).toBe(1);
	const states = new Map(database.memoryIntentRows().map((row) => [row.id, row.state]));
	expect(states.get(good)).toBe("receipted");
	// The failed intent stays non-terminal so boot recovery retries it.
	expect(states.get(broken)).toBe("queued");
});

test("a rejected memory git commit is contained, keeps its reason, and later intents still close (#192)", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const queue = new MemoryClosureQueue(database, home);
	await queue.initialize();
	const root = memoryRoot(home);

	// A hook that rejects one commit and then gets out of the way, the way a
	// transient hook/index fault does live.
	const hooks = join(root, ".git", "hooks");
	await mkdir(hooks, { recursive: true });
	const hook = join(hooks, "pre-commit");
	await writeFile(
		hook,
		`#!/bin/sh\nif [ ! -e "${hook}.fired" ]; then : > "${hook}.fired"; echo 'hook says no' >&2; exit 1; fi\nexit 0\n`,
	);
	await chmod(hook, 0o755);

	const errors: string[] = [];
	const original = console.error;
	console.error = (...parts: unknown[]) => {
		errors.push(parts.map(String).join(" "));
	};
	let rejected: string;
	let good: string;
	try {
		rejected = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "u1", replyText: "r1" });
		good = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "u2", replyText: "r2" });
		await queue.drain();
	} finally {
		console.error = original;
	}

	// The process is alive (we are still running), the git failure was recorded
	// with its reason, and the queue moved on to the next intent.
	expect(queue.failures).toBe(1);
	const states = new Map(database.memoryIntentRows().map((row) => [row.id, row.state]));
	expect(states.get(rejected)).toBe("written");
	expect(states.get(good)).toBe("receipted");
	const report = errors.find((line) => line.includes(`memory intent ${rejected} failed`));
	expect(report).toContain("memory git commit failed (exit 1, argv: git commit -m");
	expect(report).toContain("hook says no");
});
