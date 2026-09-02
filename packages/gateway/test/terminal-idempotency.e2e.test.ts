import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

/**
 * The live double-reply (2026-09-02 DM 1468535438498336923): a turn's tail was
 * closed by a broker generation fence, status reconcile then completed the
 * batch from a transcript read, and the same answer went out twice under two
 * different delivery ids. The terminal reply is now keyed on the durable
 * inbound trigger, so the second onTerminal for one batch is a ledger no-op.
 */

let directory = "";
let server: GatewayServer | undefined;
let database: GatewayDatabase | undefined;

afterEach(async () => {
	await server?.stop();
	server = undefined;
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function connect(path: string) {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: path,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value: unknown) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function startGateway(port: ScriptedSessionPort) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-terminal-idem-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-1": { engagement: "open" } },
		settleWindowMs: 0,
	};
	database = await GatewayDatabase.open(config.dbPath);
	const runtime = await startUnixServer({ config, database, sessionPort: port, onStop: () => database?.close() });
	server = runtime;
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	return { client, runtime };
}

function sendChannelMessage(client: { send(value: unknown): void }, id: string, text: string): void {
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "channel", conversationId: "chan-1" },
			text,
			messageId: `m-${id}`,
			engagement: { mentioned: true, group: true, authorId: "human-1" },
		},
	});
}

function messages(client: { frames: any[] }): any[] {
	return client.frames.filter((frame: any) => frame.type === "event" && frame.event === "chat.message");
}

async function eventually(predicate: () => boolean, message: string, attempts = 400): Promise<void> {
	for (let attempt = 0; attempt < attempts; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

test("a batch whose tail was fenced and then reconciled from status invokes onTerminal for one batch at most once", async () => {
	// Drives PersonaSessionManager directly: the server wires onTerminal to the
	// ledger, and the ledger id is the trigger, so one onTerminal == one post.
	const port = new ScriptedSessionPort();
	directory = await mkdtemp(join(tmpdir(), "gajaeway-terminal-fence-"));
	database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const terminals: Array<{ trigger: string; text: string }> = [];
	const logs: string[] = [];
	const manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "fence",
		repo: join(directory, "workspace"),
		settleWindowMs: 0,
		onTurnStart: ({ rows, batch }) => ({
			text: rows.map((row) => row.body).join("\n"),
			onTerminal: ({ text }) => {
				terminals.push({ trigger: batch.triggerMessageId, text });
			},
		}),
		log: (line) => {
			logs.push(line);
		},
	});
	try {
		expect(
			database.inboundEnqueue({
				messageId: "m-fence",
				originKey: "discord/channel/chan-1",
				originRefJson: JSON.stringify({ platform: "discord", kind: "channel", conversationId: "chan-1" }),
				body: "왜 두 번 말해?",
				receivedAt: new Date().toISOString(),
			}),
		).toBe(true);
		await manager.notifyInbound("discord/channel/chan-1");
		await eventually(() => port.sends.length === 1, "turn was not sent");
		const send = port.sends[0]!;
		// The fence closes the tail BEFORE the terminal frame arrives, exactly as
		// broker_generation_fenced did live; status then reports terminal_ok and the
		// actor reconciles from the transcript instead of the tail.
		await manager.onBrokerGeneration(2);
		port.seedOperation(send.opRef, send.sessionId, "terminal_ok", "한 번만 말할게");
		await manager.reconcile("discord/channel/chan-1");
		await Bun.sleep(1_200);
		await manager.reconcile("discord/channel/chan-1");
		await manager.reconcile("discord/channel/chan-1");
		await eventually(() => terminals.length >= 1, "reconcile never completed the batch");
		await Bun.sleep(50);
		expect(terminals).toEqual([{ trigger: "m-fence", text: "한 번만 말할게" }]);
		expect(
			database.inboundBatchRows(database.inboundNonterminalBatches("discord/channel/chan-1")[0]?.batchKey ?? ""),
		).toEqual([]);
	} finally {
		await manager.stop();
	}
});

test("the terminal delivery id is derived from the inbound trigger, so two onTerminal calls collide in the ledger", async () => {
	const port = new ScriptedSessionPort();
	const { client } = await startGateway(port);
	sendChannelMessage(client, "t2", "한 번만");
	await eventually(() => port.sends.length === 1, "turn was not sent");
	const send = port.sends[0]!;
	port.complete(send.opRef, "네 한 번만");
	await eventually(() => messages(client).length === 1, "terminal reply was not delivered");
	const first = database!.deliveryRows().find((row) => row.origin_key === "discord/channel/chan-1")!;
	// Same trigger, same text, second attempt: INSERT OR IGNORE keeps one row.
	const again = runtime_delivery_prepare(first);
	expect(again).toBe(false);

	function runtime_delivery_prepare(row: { delivery_id: string; turn_id: string }): boolean {
		return database!.deliveryCreate({
			id: row.delivery_id,
			turnId: `${row.turn_id}-replay`,
			originKey: "discord/channel/chan-1",
			payloadJson: "{}",
		});
	}
});

test("a tail-less reconcile never reposts a previous turn's answer as the current turn's reply", async () => {
	const port = new ScriptedSessionPort();
	// A runtime that reports no startedAt used to fall back to an unbounded
	// last-assistant read, which is how the previous turn's text got reposted.
	port.omitStartedAt = true;
	// `session.last_assistant` is session-wide: it returns the newest assistant
	// row regardless of which turn produced it (the live 2026-09-02 shape).
	port.fetchLastAssistant = async ({ sessionId }) => {
		const last = [...port.transcript(sessionId)].reverse().find((text) => text.length > 0);
		if (last === undefined) throw new Error("no assistant row");
		return { text: last, pages: 1, complete: true };
	};
	directory = await mkdtemp(join(tmpdir(), "gajaeway-terminal-repost-"));
	database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const terminals: Array<{ trigger: string; text: string }> = [];
	const manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "repost",
		repo: join(directory, "workspace"),
		settleWindowMs: 0,
		onTurnStart: ({ rows, batch }) => ({
			text: rows.map((row) => row.body).join("\n"),
			onTerminal: ({ text }) => {
				terminals.push({ trigger: batch.triggerMessageId, text });
			},
		}),
		log: () => {},
	});
	const enqueue = (messageId: string, body: string) =>
		expect(
			database!.inboundEnqueue({
				messageId,
				originKey: "discord/channel/chan-1",
				originRefJson: JSON.stringify({ platform: "discord", kind: "channel", conversationId: "chan-1" }),
				body,
				receivedAt: new Date().toISOString(),
			}),
		).toBe(true);
	try {
		enqueue("m-a1", "첫 질문");
		await manager.notifyInbound("discord/channel/chan-1");
		await eventually(() => port.sends.length === 1, "first turn was not sent");
		port.complete(port.sends[0]!.opRef, "첫 답");
		await eventually(() => terminals.length === 1, "first reply missing");

		enqueue("m-a2", "둘째 질문");
		await manager.notifyInbound("discord/channel/chan-1");
		await eventually(() => port.sends.length === 2, "second turn was not sent");
		const second = port.sends[1]!;
		// The second op completes with NO assistant row of its own (tool-only
		// turn); the tail never shows terminal, so status reconcile completes it.
		port.seedOperation(second.opRef, second.sessionId, "terminal_ok", "");
		await manager.reconcile("discord/channel/chan-1");
		await Bun.sleep(400);
		await manager.reconcile("discord/channel/chan-1");
		await eventually(() => terminals.length === 2, "second batch never completed");
		expect(terminals).toEqual([
			{ trigger: "m-a1", text: "첫 답" },
			{ trigger: "m-a2", text: "" },
		]);
	} finally {
		await manager.stop();
	}
});
