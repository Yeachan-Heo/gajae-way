import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { GjcPort, TurnOptions } from "../src/orchestrator/gjc-client";
import { GjcTurnStream } from "../src/orchestrator/gjc-client";
import { KeyedQueue } from "../src/server/keyed-queue";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

test("GjcTurnStream reports each completed assistant message as it streams", () => {
	const reported: string[] = [];
	const stream = new GjcTurnStream((text) => reported.push(text));
	const message = (text: string) =>
		`${JSON.stringify({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text }] } })}\n`;
	stream.feed(message("first step done"));
	stream.feed(`${JSON.stringify({ type: "tool_execution_start" })}\n`);
	stream.feed(message("second step done"));
	expect(reported).toEqual(["first step done", "second step done"]);
	expect(stream.finalText).toBe("second step done");
	expect(stream.toolCalls).toBe(1);
});

test("KeyedQueue.settle resolves only after every enqueued task settles", async () => {
	const queue = new KeyedQueue();
	let done = 0;
	void queue.run("a", async () => {
		await Bun.sleep(20);
		done++;
	});
	void queue
		.run("b", async () => {
			await Bun.sleep(10);
			done++;
			throw new Error("failed tasks settle too");
		})
		.catch(() => {});
	await queue.settle();
	expect(done).toBe(2);
});

test("turn_count survives increments and resets on epoch bump", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-turncount-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	try {
		const key = "discord/channel/rot-1";
		const origin = JSON.stringify({ platform: "discord", kind: "channel", conversationId: "rot-1" });
		database.putSession(key, "session-1");
		expect(database.incrementTurnCount(key)).toBe(1);
		expect(database.incrementTurnCount(key)).toBe(2);
		database.bumpEpoch(key, origin);
		expect(database.incrementTurnCount(key)).toBe(1);
	} finally {
		database.close();
	}
});

async function connect(socketPath: string): Promise<{ send(value: unknown): void; frames: any[]; close(): void }> {
	const frames: any[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line));
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function startGateway(
	gjc: GjcPort,
): Promise<{ client: Awaited<ReturnType<typeof connect>>; config: GatewayConfig }> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-latency-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-1": { engagement: "open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	return { client, config };
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

test("intermediate assistant messages are delivered while the turn is still running", async () => {
	let releaseTurn: (() => void) | undefined;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, _text, _preamble, _progress, options?: TurnOptions) => {
			options?.onAssistantText?.("working on it — checking the logs now");
			await new Promise<void>((resolve) => {
				releaseTurn = resolve;
			});
			options?.onAssistantText?.("done: found the culprit");
			return "done: found the culprit";
		},
	};
	const { client } = await startGateway(gjc);
	sendChannelMessage(client, "c1", "hey, dig into this");
	// The first assistant message must arrive BEFORE the turn completes.
	for (let attempt = 0; attempt < 100; attempt++) {
		if (client.frames.some((frame: any) => frame.type === "event" && frame.event === "chat.message")) break;
		await Bun.sleep(5);
	}
	const midTurn = client.frames.filter((frame: any) => frame.type === "event" && frame.event === "chat.message");
	expect(midTurn.length).toBe(1);
	expect(midTurn[0].payload.text).toContain("checking the logs");
	releaseTurn?.();
	for (let attempt = 0; attempt < 100; attempt++) {
		const count = client.frames.filter((frame: any) => frame.type === "event" && frame.event === "chat.message").length;
		if (count >= 2) break;
		await Bun.sleep(5);
	}
	const messages = client.frames.filter((frame: any) => frame.type === "event" && frame.event === "chat.message");
	// The final assistant message arrives exactly once (streamed, never redelivered).
	expect(messages.length).toBe(2);
	expect(messages[1].payload.text).toContain("found the culprit");
});

test("a queued message does not pay the debounce window twice", async () => {
	const turnStarts: number[] = [];
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async () => {
			turnStarts.push(Date.now());
			await Bun.sleep(600);
			return "ack";
		},
	};
	directory = await mkdtemp(join(tmpdir(), "gajaeway-debounce-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-1": { engagement: "open", debounceMs: 500 } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	sendChannelMessage(client, "d1", "first");
	// Arrives while turn 1 (500ms debounce + 600ms turn) is in flight; by the time it
	// is claimed it has already outwaited its debounce window.
	await Bun.sleep(700);
	sendChannelMessage(client, "d2", "second");
	for (let attempt = 0; attempt < 400 && turnStarts.length < 2; attempt++) await Bun.sleep(5);
	expect(turnStarts.length).toBe(2);
	// Turn 1 ends ~1100ms in; the second message (waiting since ~700ms) must start
	// nearly immediately after, not after another full 500ms debounce.
	const gapAfterFirstTurn = (turnStarts[1] as number) - ((turnStarts[0] as number) + 600);
	expect(gapAfterFirstTurn).toBeLessThan(400);
});

test("a stale gjc session binding is rebound once and the turn retried", async () => {
	let calls = 0;
	const epochs: number[] = [];
	const gjc: GjcPort = {
		ensureSession: async (_key, epoch) => {
			epochs.push(epoch ?? 0);
			return { sessionId: `session-e${epoch}` };
		},
		sendTurn: async (sessionId) => {
			calls++;
			if (sessionId === "session-e0") throw new Error('gjc turn exited 1: Error: Session "dead-beef" not found.');
			return "recovered reply";
		},
	};
	const { client } = await startGateway(gjc);
	sendChannelMessage(client, "r1", "are you alive?");
	for (let attempt = 0; attempt < 200; attempt++) {
		if (client.frames.some((frame: any) => frame.type === "event" && frame.event === "chat.message")) break;
		await Bun.sleep(5);
	}
	const message = client.frames.find((frame: any) => frame.type === "event" && frame.event === "chat.message");
	expect(calls).toBe(2);
	expect(epochs).toEqual([0, 1]);
	expect(message.payload.text).toContain("recovered reply");
});
