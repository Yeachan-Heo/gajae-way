import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { SessionPort } from "../src/orchestrator/session-port";
import { ScriptedSessionPort, sessionPortFromScript } from "./session-port.fake";
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


test("SessionPort.runExclusive serializes same-origin work and releases after rejection", async () => {
	const port = new ScriptedSessionPort();
	const order: string[] = [];
	const first = port.runExclusive("a", async () => {
		await Bun.sleep(20);
		order.push("first");
	});
	const rejected = port.runExclusive("a", async () => {
		order.push("second");
		throw new Error("failed work releases the session lock");
	});
	const other = port.runExclusive("b", async () => {
		await Bun.sleep(10);
		order.push("other");
	});
	await Promise.all([first, rejected.catch(() => undefined), other]);
	expect(order).toEqual(["other", "first", "second"]);
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
	options: { readonly sessionPort: SessionPort },
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
		settleWindowMs: 0,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	server = await startUnixServer({ config, database, ...options, onStop: () => database.close() });
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

test("tail frames deliver an assistant finding before the persistent operation reaches terminal", async () => {
	const sessionPort = new ScriptedSessionPort();
	const { client } = await startGateway({ sessionPort });
	sendChannelMessage(client, "c1", "hey, dig into this");
	for (let attempt = 0; attempt < 100 && sessionPort.sends.length === 0; attempt++) await Bun.sleep(5);
	const send = sessionPort.sends[0]!;
	sessionPort.emitTool(send.sessionId);
	sessionPort.emitAssistant(send.sessionId, "the 500s hit every 3 minutes, not randomly");
	for (let attempt = 0; attempt < 100; attempt++) {
		if (client.frames.some((frame: any) => frame.type === "event" && frame.event === "chat.message")) break;
		await Bun.sleep(5);
	}
	const midTurn = client.frames.filter((frame: any) => frame.type === "event" && frame.event === "chat.message");
	expect(midTurn).toHaveLength(1);
	expect(midTurn[0].payload.text).toContain("every 3 minutes");
	sessionPort.complete(send.opRef, "done: found the culprit");
	for (let attempt = 0; attempt < 100; attempt++) {
		const count = client.frames.filter((frame: any) => frame.type === "event" && frame.event === "chat.message").length;
		if (count >= 2) break;
		await Bun.sleep(5);
	}
	const messages = client.frames.filter((frame: any) => frame.type === "event" && frame.event === "chat.message");
	expect(messages).toHaveLength(2);
	expect(messages[1].payload.text).toContain("found the culprit");
});

	test("a message arriving during an active persistent turn is steered without a second send", async () => {
	const turnStarts: number[] = [];
	const sessionPort = sessionPortFromScript({
		bind: async () => ({ sessionId: "mock-session" }),
		respond: async () => {
			turnStarts.push(Date.now());
			await Bun.sleep(600);
			return "ack";
		},
	});
	directory = await mkdtemp(join(tmpdir(), "gajaeway-settle-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "chan-1": { engagement: "open", settleWindowMs: 500 } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	sendChannelMessage(client, "d1", "first");
	// It arrives while the first turn is active, so persistent-session admission
	// steers it into the accepted operation instead of creating another turn.
	await Bun.sleep(700);
	sendChannelMessage(client, "d2", "second");
	for (let attempt = 0; attempt < 400 && sessionPort.steers.length < 1; attempt++) await Bun.sleep(5);
	expect(turnStarts.length).toBe(1);
	expect(sessionPort.steers).toHaveLength(1);
});

test("a prose-only stale session failure surfaces visibly without a prose-driven rebind", async () => {
	// #13 mandates exact-code-only classification. gjc emits no structured code
	// for "session not found", so the old main behavior — a regex-driven epoch
	// bump and retry that could loop forever against a dead key — is replaced by
	// a visible structured failure whose remedy (/new) the operator controls.
	let calls = 0;
	const sessionPort = sessionPortFromScript({
		bind: async () => ({ sessionId: "session-e0" }),
		respond: async () => {
			calls++;
			throw new Error('session send failed: Session "dead-beef" not found.');
		},
	});
	const { client } = await startGateway({ sessionPort });
	sendChannelMessage(client, "r1", "are you alive?");
	for (let attempt = 0; attempt < 200; attempt++) {
		if (client.frames.some((frame: any) => frame.type === "event" && frame.event === "chat.message")) break;
		await Bun.sleep(5);
	}
	const message = client.frames.find((frame: any) => frame.type === "event" && frame.event === "chat.message");
	expect(calls).toBe(1);
	expect(message.payload.text).toContain("[turn failed]");
	expect(message.payload.text).toContain('Session "dead-beef" not found');
});
