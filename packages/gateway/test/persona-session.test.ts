import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager, personaBatchOpRef } from "../src/orchestrator/persona-session";
import { GatewayDatabase, type InboundMessageRow } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;
let latestBatchKey = "";
let latestOpRef = "";

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
	latestBatchKey = "";
	latestOpRef = "";
});

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "persona" } as const;
const KEY = "loopback/loopback/persona";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function enqueue(messageId: string, body: string): void {
	const accepted = database?.inboundEnqueue({
		messageId,
		originKey: KEY,
		originRefJson: JSON.stringify(ORIGIN),
		body,
	});
	expect(accepted).toBe(true);
}

async function harness(port: ScriptedSessionPort, hooks: { terminal?: (text: string) => void; retired?: () => void } = {}) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		settleWindowMs: 0,
		onTurnStart: ({ rows, batch }) => {
			latestBatchKey = batch.batchKey;
			latestOpRef = batch.opRef;
			return {
				text: rows.map((row) => row.body).join("\n"),
				onTerminal: ({ text }) => hooks.terminal?.(text),
				onRetired: () => hooks.retired?.(),
			};
		},
	});
}

test("actor settles durable inbound, sends one deterministic caller op-ref, then completes on tail terminal", async () => {
	const port = new ScriptedSessionPort({ onSend: (input, scripted) => scripted.complete(input.opRef, "persona reply") });
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) });
	enqueue("m-1", "hello");
	await manager?.notifyInbound(KEY);
	await eventually(() => terminal.length === 1, "tail terminal did not reach lifecycle");

	expect(port.sends).toHaveLength(1);
	const send = port.sends[0]!;
	expect(send.text).toBe("hello");
	expect(send.opRef).toBe(latestOpRef);
	expect(send.opRef).toMatch(/^gw-p-[0-9a-f]{32}$/);
	expect(terminal).toEqual(["persona reply"]);
	expect(database?.inboundPendingCount(KEY)).toBe(0);
	expect(database?.inboundBatchRows(latestBatchKey)[0]).toMatchObject({ state: "done", batch_state: "done" });
});

test("a message admitted while a persistent turn is running becomes an operator-gated steer", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "first");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "initial persistent send did not start");
	expect(port.sends).toHaveLength(1);
	enqueue("m-2", "correction");
	await manager?.notifyInbound(KEY);

	expect(port.steers).toHaveLength(1);
	expect(port.steers[0]).toMatchObject({ sessionId: port.sends[0]!.sessionId, text: "correction" });
	port.complete(port.sends[0]!.opRef, "done");
	await eventually(
		() => database?.inboundBatchRows(latestBatchKey)[0]?.batch_state === "done",
		"accepted batch did not reconcile terminal",
	);
});

test("bounded shutdown reconciliation leaves a nonterminal accepted batch durable", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "still running");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted batch did not start before shutdown");
	await manager?.drain(0);
	expect(manager?.state(KEY)).toBe("turn-running");
	expect(database?.inboundBatchRows(latestBatchKey)[0]).toMatchObject({ state: "pending", batch_state: "accepted" });
});

test("/new retires an accepted turn, fences its late output, and preserves batch recovery until terminal", async () => {
	const port = new ScriptedSessionPort();
	let retired = 0;
	const terminal: string[] = [];
	await harness(port, { retired: () => retired++, terminal: (text) => terminal.push(text) });
	enqueue("m-1", "old turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted batch did not start before reset");
	const first = port.sends[0]!;
	await manager?.reset(KEY, JSON.stringify(ORIGIN));

	expect(retired).toBe(1);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database?.inboundBatchRows(latestBatchKey)[0]).toMatchObject({ state: "pending", batch_state: "accepted" });
	port.complete(first.opRef, "stale output");
	await eventually(
		() => database?.inboundBatchRows(latestBatchKey)[0]?.batch_state === "done",
		"retired batch did not reconcile",
	);
	expect(terminal).toEqual([]);
});

test("a retired stalled turn detaches into a durable hold and reconciles terminal without stale delivery", async () => {
	const port = new ScriptedSessionPort();
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) });
	enqueue("m-1", "old turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted batch did not start before retired stall");
	const send = port.sends[0]!;
	const batchKey = latestBatchKey;
	await manager?.reset(KEY, JSON.stringify(ORIGIN));
	port.emitStall(send.sessionId);
	await manager?.recover();
	port.complete(send.opRef, "must remain fenced");
	await eventually(() => database?.inboundBatchRows(batchKey)[0]?.batch_state === "done", "retired hold did not reconcile terminal");
	expect(terminal).toEqual([]);
});

test("startup recovery reconstructs an accepted durable batch and reconciles its terminal tail", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "recover me");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted batch did not start before restart");
	const oldManager = manager!;
	const opRef = port.sends[0]!.opRef;
	const batchKey = latestBatchKey;
	await oldManager.stop();
	const terminal: string[] = [];
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		settleWindowMs: 0,
		onTurnStart: ({ rows }) => ({
			text: rows[0]!.body,
			onTerminal: ({ text }) => {
				terminal.push(text);
			},
		}),
	});
	await manager.recover();
	port.complete(opRef, "recovered reply");
	await eventually(() => terminal.length === 1, "recovered actor did not deliver terminal output");
	expect(terminal).toEqual(["recovered reply"]);
	expect(database?.inboundBatchRows(batchKey)[0]).toMatchObject({ state: "done", batch_state: "done" });
});

test("the immutable first-row settle cutoff yields a safe op-ref even when platform ids contain SDK-unsafe bytes", async () => {
	const opRef = personaBatchOpRef("instance", "discord/channel/room", 4, "message id / with spaces", "2026-09-01T00:00:00.000Z");
	expect(opRef).toMatch(/^gw-p-[0-9a-f]{32}$/);
});

test("live per-channel settle resolution is evaluated from the durable first row", async () => {
	const port = new ScriptedSessionPort({ onSend: (input, scripted) => scripted.complete(input.opRef, "done") });
	await harness(port);
	let rowsSeen: readonly InboundMessageRow[] = [];
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		settleWindowMs: 2_000,
		settleWindowFor: () => 0,
		onTurnStart: ({ rows }) => {
			rowsSeen = rows;
			return { text: rows[0]!.body };
		},
	});
	enqueue("m-1", "live policy");
	await manager.notifyInbound(KEY);
	await eventually(() => rowsSeen.length === 1, "live settle resolver did not dispatch immediately");
	expect(port.sends).toHaveLength(1);
});

test("pending rows older than maxInboundAgeMs are expired instead of answered (stale floor)", async () => {
	const port = new ScriptedSessionPort({ onSend: (input, scripted) => scripted.complete(input.opRef, "reply") });
	const sent: string[] = [];
	const logs: string[] = [];
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		settleWindowMs: 0,
		maxInboundAgeMs: 60_000,
		log: (line) => logs.push(line),
		onTurnStart: ({ rows }) => {
			sent.push(rows.map((row) => row.body).join("|"));
			return { text: rows.map((row) => row.body).join("\n") };
		},
	});
	const old = new Date(Date.now() - 5 * 60_000).toISOString();
	expect(database.inboundEnqueue({ messageId: "old-1", originKey: KEY, originRefJson: JSON.stringify(ORIGIN), body: "stale question", receivedAt: old })).toBe(true);
	expect(database.inboundEnqueue({ messageId: "old-2", originKey: KEY, originRefJson: JSON.stringify(ORIGIN), body: "stale follow-up", receivedAt: old })).toBe(true);
	enqueue("fresh-1", "fresh question");
	await manager.notifyInbound(KEY);
	await eventually(() => sent.length === 1, "fresh row was not answered");
	expect(sent).toEqual(["fresh question"]);
	expect(logs.some((line) => line.startsWith(`inbound_expired origin=${KEY} count=2`))).toBe(true);
	expect(database.inboundPendingCount(KEY)).toBe(0);
});
