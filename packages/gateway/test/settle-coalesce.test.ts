import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "coalesce" } as const;
const ORIGIN_KEY = "loopback/loopback/coalesce";

let home = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function enqueue(messageId: string, authorName: string, body: string, receivedAt: string): void {
	expect(
		database?.inboundEnqueue({
			messageId,
			originKey: ORIGIN_KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			engagementJson: JSON.stringify({ authorName }),
			receivedAt,
		}),
	).toBe(true);
}

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("rapid idle fragments coalesce once at the first-row cutoff with every speaker label", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-settle-coalesce-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort();
	const base = Date.parse("2026-09-02T00:00:00.000Z");
	let now = base;
	const timers: Array<{ readonly work: () => void; readonly delayMs: number }> = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "coalesce-test",
		repo: join(home, "workspace"),
		settleWindowMs: 100,
		now: () => now,
		setTimeout: (work, delayMs) => {
			timers.push({ work, delayMs });
			return timers.length;
		},
		clearTimeout: () => {},
		onTurnStart: ({ rows }) => ({
			text: rows
				.map((row) => {
					const engagement = JSON.parse(row.engagement_json ?? "{}") as { authorName?: string };
					return `[speaker:${engagement.authorName ?? "unknown"} message:${row.message_id}] ${row.body}`;
				})
				.join("\n"),
		}),
	});

	enqueue("m-1", "Ari", "first fragment", new Date(base).toISOString());
	await manager.notifyInbound(ORIGIN_KEY);
	enqueue("m-2", "Bea", "second fragment", new Date(base + 25).toISOString());
	await manager.notifyInbound(ORIGIN_KEY);
	enqueue("m-3", "Cy", "third fragment", new Date(base + 100).toISOString());
	await manager.notifyInbound(ORIGIN_KEY);

	expect(manager.state(ORIGIN_KEY)).toBe("settling");
	expect(timers).toEqual([expect.objectContaining({ delayMs: 100 })]);
	now = base + 100;
	await manager.tick(ORIGIN_KEY);
	await eventually(() => port.sends.length === 1, "fixed-from-first cutoff did not dispatch one coalesced send");

	const send = port.sends[0]!;
	expect(send.text).toBe(
		"[speaker:Ari message:m-1] first fragment\n[speaker:Bea message:m-2] second fragment\n[speaker:Cy message:m-3] third fragment",
	);
	expect(port.sends).toHaveLength(1);
	expect(port.steers).toEqual([]);
	const batch = database.inboundNonterminalBatches(ORIGIN_KEY)[0]!;
	expect(database.inboundBatchRows(batch.batchKey).map((row) => row.batch_role)).toEqual([
		"trigger",
		"member",
		"member",
	]);

	port.complete(send.opRef, "coalesced done");
	await eventually(
		() => database?.inboundPendingCount(ORIGIN_KEY) === 0,
		"coalesced rows remained pending after tail terminal",
	);
});
