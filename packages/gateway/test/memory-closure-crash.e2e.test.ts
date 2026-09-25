import { afterEach, expect, spyOn, test } from "bun:test";
import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { originKey } from "@gajae-gateway/protocol";
import { MemoryClosureQueue } from "../src/memory/closure";
import { memoryRoot } from "../src/memory/doctrine";
import { GatewayDatabase } from "../src/store/db";

let home = "";
afterEach(async () => {
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function run(killPoint?: string, recover = false): Promise<number> {
	const child = Bun.spawn(["bun", "packages/gateway/test/helpers/closure-runner.ts"], {
		cwd: join(import.meta.dir, "../../.."),
		env: {
			...process.env,
			GAJAEWAY_HOME: home,
			...(killPoint ? { GAJAEWAY_MEMORY_KILL_POINT: killPoint } : {}),
			...(recover ? { GAJAEWAY_MEMORY_RUNNER_MODE: "recover" } : {}),
		},
		stdout: "ignore",
		stderr: "ignore",
	});
	return child.exited;
}

async function intent() {
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const row = database.memoryIntentRows()[0];
	database.close();
	if (!row) throw new Error("missing memory intent");
	return row;
}

async function trailerCommits(id: string): Promise<string[]> {
	const child = Bun.spawn(["git", "log", "--format=%H", "--grep", `Gajaeway-Mutation-Id: ${id}`], {
		cwd: memoryRoot(home),
		stdout: "pipe",
		stderr: "ignore",
	});
	const output = await new Response(child.stdout).text();
	await child.exited;
	return output.trim() ? output.trim().split("\n") : [];
}

async function receiptContains(id: string): Promise<boolean> {
	try {
		return (await readFile(join(home, "memory-receipts.jsonl"), "utf8"))
			.split("\n")
			.some((line) => line && JSON.parse(line).id === id);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
		throw error;
	}
}

test("quarantined memory intents persist a reason, log it, and emit a negative receipt", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-quarantine-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const id = "invalid-memory-intent";
	const origin = { platform: "discord", kind: "channel", conversationId: "C1" } as const;
	const reason = "Error: unsupported memory intent unsupported";
	database.memoryIntentCreate({
		id,
		kind: "unsupported",
		payloadJson: JSON.stringify({
			originRefJson: JSON.stringify(origin),
			userText: "captured",
			replyText: "reply",
		}),
	});
	const warning = spyOn(console, "warn").mockImplementation(() => {});
	try {
		await new MemoryClosureQueue(database, home).initialize();
		const row = database.memoryIntentRows()[0];
		expect(row).toMatchObject({ state: "quarantined", attempts: 1, quarantine_reason: reason });
		expect(warning).toHaveBeenCalledTimes(1);
		expect(warning.mock.calls[0]?.[0]).toContain(
			`id=${id} kind=unsupported origin=${originKey(origin)} reason=${reason}`,
		);
		const receipt = JSON.parse((await readFile(join(home, "memory-receipts.jsonl"), "utf8")).trim());
		expect(receipt).toMatchObject({ id, state: "quarantined", reason });
		expect(Number.isFinite(Date.parse(receipt.at))).toBe(true);
	} finally {
		warning.mockRestore();
		database.close();
	}
});

for (const [name, killPoint] of [
	["M1", "after-intent"],
	["M2", "after-write"],
	["M3", "after-commit"],
] as const) {
	test(`${name} real process kill recovers closure intent exactly once`, async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-memory-crash-"));
		expect(await run(killPoint)).not.toBe(0);
		const before = await intent();
		if (name === "M1") {
			expect(before.state).toBe("queued");
			await expect(
				access(join(memoryRoot(home), "daily", `${new Date().toISOString().slice(0, 10)}.md`)),
			).rejects.toThrow();
		}
		if (name === "M2") {
			expect(before.state).toBe("written");
			expect(await trailerCommits(before.id)).toEqual([]);
		}
		if (name === "M3") {
			expect(before.state).toBe("committed");
			expect(await trailerCommits(before.id)).toHaveLength(1);
			expect(await receiptContains(before.id)).toBe(false);
		}
		expect(await run(undefined, true)).toBe(0);
		const after = await intent();
		expect(after.state).toBe("receipted");
		expect(await trailerCommits(after.id)).toHaveLength(1);
		expect(await receiptContains(after.id)).toBe(true);
	});
}
