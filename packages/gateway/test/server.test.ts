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
	for (let attempt = 0; attempt < 400 && frames.length < count; attempt++) await Bun.sleep(5);
	expect(frames.length).toBeGreaterThanOrEqual(count);
}

test("requires negotiation then serves status, shutdown, and validates chat params", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async () => "mock reply",
	};
	server = await startUnixServer({
		config,
		database,
		gjc,
		startedAt: "2026-01-01T00:00:00.000Z",
		onStop: () => database.close(),
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "request", id: "before", verb: "gateway.status" });
	await waitFor(client.frames, 1);
	expect(client.frames[0].error.code).toBe("negotiation_required");
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 2);
	expect(client.frames[1].type).toBe("negotiated");
	client.send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
	await waitFor(client.frames, 3);
	expect(client.frames[2].result.schemaVersion).toBe(8);
	expect(client.frames[2].result.startedAt).toBe("2026-01-01T00:00:00.000Z");
	client.send({
		v: "0.1",
		type: "request",
		id: "bad-chat",
		verb: "chat.send",
		params: { origin: { platform: "discord", kind: "channel", conversationId: "x" }, text: "hello" },
	});
	await waitFor(client.frames, 4);
	expect(client.frames[3].error.code).toBe("invalid_params");
	client.send({
		v: "0.1",
		type: "request",
		id: "chat",
		verb: "chat.send",
		params: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" }, text: "hello" },
	});
	await waitFor(client.frames, 6);
	expect(client.frames[4].result.turnId).toBeString();
	expect(client.frames[5].payload).toMatchObject({ text: "mock reply", final: true });
	client.send({ v: "0.1", type: "request", id: "shutdown", verb: "gateway.shutdown" });
	await waitFor(client.frames, 8);
	expect(client.frames[6]).toMatchObject({ type: "response", id: "shutdown", result: { stopping: true } });
	expect(client.frames[7]).toMatchObject({ type: "event", event: "gateway.stopping" });
	client.close();
});

test("a failed platform turn still delivers a visible ledgered failure notice", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async () => {
			throw new Error("gjc turn timed out after 300000ms");
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "dm",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	await waitFor(client.frames, 4);
	expect(client.frames[1].result.engaged).toBe(true);
	const notice = client.frames.find((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(notice.payload.text).toStartWith("[turn failed]");
	expect(notice.payload.deliveryId).toBeString();
	const errorFrame = client.frames.find((frame) => frame.type === "error" && frame.id === "dm");
	expect(errorFrame.error.code).toBe("verb_failed");
	client.close();
});

test("long turns broadcast throttled chat.progress liveness events", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, _text, _preamble, onProgress) => {
			for (let call = 1; call <= 3; call++) {
				await Bun.sleep(5);
				onProgress?.({ toolCalls: call, outputTokens: call * 100 });
			}
			return "done";
		},
	};
	server = await startUnixServer({
		config,
		database,
		gjc,
		progress: { firstAfterMs: 0, intervalMs: 0 },
		onStop: () => database.close(),
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "dm",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	// Wait for the final reply so every progress frame the turn produced has arrived.
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.some((frame) => frame.type === "event" && frame.event === "chat.message")) break;
		await Bun.sleep(5);
	}
	const progress = client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.progress");
	expect(progress.length).toBeGreaterThanOrEqual(2);
	expect(progress[0].payload.origin.conversationId).toBe("c1");
	// The first frame may be a zero-state heartbeat; later frames carry stream data.
	const maxTools = Math.max(...progress.map((frame: any) => frame.payload.toolCalls));
	const maxTokens = Math.max(...progress.map((frame: any) => frame.payload.outputTokens));
	expect(maxTools).toBeGreaterThanOrEqual(1);
	expect(maxTokens).toBeGreaterThanOrEqual(100);
	expect(progress[0].payload.elapsedMs).toBeGreaterThanOrEqual(0);
	const reply = client.frames.find((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(reply.payload.text).toBe("done");
	client.close();
});

test("debounced burst becomes one turn carrying the unread diff with speaker attribution", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		debounceMs: 80,
		channels: { c1: { engagement: "open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: Array<{ text: string; preamble: string }> = [];
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, text, preamble) => {
			turns.push({ text, preamble: preamble ?? "" });
			return "batched reply";
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const origin = { platform: "discord", kind: "channel", conversationId: "c1" };
	const say = (id: string, text: string, authorId: string, authorName: string, mentioned: boolean) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: { origin, text, messageId: id, engagement: { mentioned, group: true, authorId, authorName } },
		});
	say("m1", "first message", "u1", "alice", false);
	await Bun.sleep(10);
	say("m2", "second message", "u2", "bob", false);
	await Bun.sleep(10);
	say("m3", "@bot do the thing", "owner", "bellman", true);
	for (let attempt = 0; attempt < 400 && turns.length === 0; attempt++) await Bun.sleep(10);
	expect(turns).toHaveLength(1);
	const turn = turns[0]!;
	// The two earlier burst messages arrive as the unread diff, the newest as the trigger.
	expect(turn.text).toContain("Unread messages in this conversation");
	expect(turn.text).toContain("alice: first message");
	expect(turn.text).toContain("bob: second message");
	expect(turn.text).toContain("[bellman in discord channel c1]");
	expect(turn.text).toContain("@bot do the thing");
	// Consumed context is not replayed on the next turn.
	say("m4", "follow-up", "owner", "bellman", true);
	for (let attempt = 0; attempt < 400 && turns.length < 2; attempt++) await Bun.sleep(10);
	expect(turns).toHaveLength(2);
	expect(turns[1]!.text).not.toContain("first message");
	expect(turns[1]!.text).toContain("follow-up");
	client.close();
});

test("group turns carry silence guidance: listeners are told to default to [SILENT]", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { c1: { engagement: "open", debounceMs: 0 } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const preambles: string[] = [];
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async (_session, _text, preamble) => {
			preambles.push(preamble ?? "");
			return "[SILENT]";
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "listen",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "channel", conversationId: "c1" },
			text: "people chatting among themselves",
			messageId: "listen-1",
			engagement: { mentioned: false, group: true, authorId: "u1", authorName: "alice" },
		},
	});
	for (let attempt = 0; attempt < 400 && preambles.length === 0; attempt++) await Bun.sleep(5);
	expect(preambles[0]).toContain("You were NOT addressed");
	expect(preambles[0]).toContain("[SILENT]");
	// The silence-token reply suppresses delivery: no chat.message event arrives.
	await Bun.sleep(50);
	expect(client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.message")).toHaveLength(0);
	client.close();
});
