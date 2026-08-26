import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { GjcPort } from "../src/orchestrator/gjc-client";
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

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "loopback" } as const;

async function makeConfig(): Promise<GatewayConfig> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-inbound-"));
	return {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
}

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

async function waitFor(frames: any[], count: number): Promise<void> {
	for (let attempt = 0; attempt < 200 && frames.length < count; attempt++) await Bun.sleep(5);
	expect(frames.length).toBeGreaterThanOrEqual(count);
}

function chatSend(id: string, messageId: string, text: string): unknown {
	return { v: "0.1", type: "request", id, verb: "chat.send", params: { origin: ORIGIN, text, messageId } };
}

test("a duplicate message id is acknowledged but never dispatched twice", async () => {
	const config = await makeConfig();
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, text) => {
			turns.push(text);
			return "mock reply";
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);

	client.send(chatSend("first", "msg-1", "hello"));
	await waitFor(client.frames, 3);
	client.send(chatSend("second", "msg-1", "hello"));
	await waitFor(client.frames, 4);
	await Bun.sleep(50);

	const dup = client.frames.find((frame) => frame.type === "response" && frame.id === "second");
	expect(dup.result).toMatchObject({ turnId: null, engaged: true });
	expect(turns).toEqual(["hello"]);
	expect(client.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(1);
	expect(database.inboundPendingCount("loopback/loopback/loopback")).toBe(0);
	client.close();
});

test("a message arriving while a turn is in flight is drained afterwards, not dropped", async () => {
	const config = await makeConfig();
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	let release: (() => void) | undefined;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, text) => {
			turns.push(text);
			if (turns.length === 1) await gate;
			return `reply to ${text}`;
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);

	client.send(chatSend("one", "msg-1", "first"));
	await waitFor(client.frames, 2);
	// The first turn is now parked inside gjc.sendTurn; the second message must survive it.
	client.send(chatSend("two", "msg-2", "second"));
	await waitFor(client.frames, 3);
	expect(turns).toEqual(["first"]);
	expect(database.inboundPendingCount("loopback/loopback/loopback")).toBe(1);

	release?.();
	await waitFor(client.frames, 5);

	expect(turns).toEqual(["first", "second"]);
	const replies = client.frames.filter((frame) => frame.event === "chat.message").map((frame) => frame.payload.text);
	expect(replies).toEqual(["reply to first", "reply to second"]);
	expect(database.inboundPendingCount("loopback/loopback/loopback")).toBe(0);
	client.close();
});
