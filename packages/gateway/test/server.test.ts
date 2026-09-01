import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MAX_STALLED_CONTINUATIONS, parseLaneJobRecord } from "@gajaeway/subsession";
import type { GatewayConfig } from "../src/config";
import { memoryRoot } from "../src/memory/doctrine";
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

async function connect(
	socketPath: string,
): Promise<{ send(value: unknown): void; frames: any[]; wireLines: string[]; close(): void }> {
	const frames: any[] = [];
	const wireLines: string[] = [];
	let buffered = Buffer.alloc(0);
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered = Buffer.concat([buffered, Buffer.from(data)]);
				let newline = buffered.indexOf(10);
				while (newline >= 0) {
					const line = buffered.subarray(0, newline).toString("utf8");
					buffered = buffered.subarray(newline + 1);
					if (line) {
						wireLines.push(line);
						frames.push(JSON.parse(line));
					}
					newline = buffered.indexOf(10);
				}
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, wireLines, close: () => socket.end() };
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
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
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
	expect(client.frames[2].result.schemaVersion).toBe(15);
	expect(client.frames[2].result.startedAt).toBe("2026-01-01T00:00:00.000Z");
	expect(client.frames[2].result.contextDiff).toEqual({
		unread: 0,
		expired: 0,
		truncated: 0,
		omittedOldestAt: null,
		omittedNewestAt: null,
		floorAt: null,
	});
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
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
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
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
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

test("large memory.audit and concurrent progress remain independently parseable frames", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-frame-writer-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const root = memoryRoot(directory);
	await mkdir(root, { recursive: true });
	for (let index = 0; index < 2200; index++)
		await writeFile(
			join(root, `orphan-${index.toString().padStart(4, "0")}.md`),
			`orphan ${index} ${"x".repeat(80)}\n`,
		);
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
		sendTurn: async (_session, _text, _preamble, onProgress) => {
			onProgress?.({ toolCalls: 1, outputTokens: 100 });
			await Bun.sleep(5);
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
	client.send({ v: "0.1", type: "request", id: "audit", verb: "memory.audit", params: {} });
	client.send({
		v: "0.1",
		type: "request",
		id: "progress-turn",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	for (let attempt = 0; attempt < 800; attempt++) {
		if (
			client.frames.some((frame) => frame.type === "response" && frame.id === "audit") &&
			client.frames.some((frame) => frame.type === "event" && frame.event === "chat.message")
		)
			break;
		await Bun.sleep(5);
	}
	const audit = client.frames.find((frame) => frame.type === "response" && frame.id === "audit");
	expect(audit).toBeDefined();
	const auditIndex = client.frames.indexOf(audit);
	const auditLine = client.wireLines[auditIndex];
	expect(Buffer.byteLength(`${auditLine}\n`, "utf8")).toBeGreaterThan(219_000);
	expect(JSON.parse(auditLine)).toEqual(audit);
	expect(
		client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.progress").length,
	).toBeGreaterThan(0);
	expect(client.frames.find((frame) => frame.type === "event" && frame.event === "chat.message")?.payload.text).toBe(
		"done",
	);
	expect(client.frames.filter((frame) => frame.type === "response" && frame.id === "audit")).toHaveLength(1);
	expect(client.wireLines).toHaveLength(client.frames.length);
	client.close();
});

test("shutdown quiesces an in-flight turn before final stopping frame", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-shutdown-writer-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	let entered!: () => void;
	let release!: () => void;
	const turnEntered = new Promise<void>((resolve) => (entered = resolve));
	const turnRelease = new Promise<void>((resolve) => (release = resolve));
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
		sendTurn: async () => {
			entered();
			await turnRelease;
			return "late reply";
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "turn",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	await turnEntered;
	client.send({ v: "0.1", type: "request", id: "shutdown", verb: "gateway.shutdown" });
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.some((frame) => frame.type === "response" && frame.id === "shutdown")) break;
		await Bun.sleep(5);
	}
	expect(client.frames.find((frame) => frame.type === "response" && frame.id === "shutdown")).toBeDefined();
	release();
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping")) break;
		await Bun.sleep(5);
	}
	const replyIndex = client.frames.findIndex((frame) => frame.type === "event" && frame.event === "chat.message");
	const stoppingIndex = client.frames.findIndex(
		(frame) => frame.type === "event" && frame.event === "gateway.stopping",
	);
	expect(replyIndex).toBeGreaterThan(1);
	expect(stoppingIndex).toBeGreaterThan(replyIndex);
	expect(client.frames[replyIndex]?.payload.text).toBe("late reply");
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
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
		debounceMs: 80,
		channels: { c1: { engagement: "open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: Array<{ text: string; preamble: string }> = [];
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
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
	expect(turn.text).toContain("alice (author:u1, msg:m1): first message");
	expect(turn.text).toContain("bob (author:u2, msg:m2): second message");
	expect(turn.text).toContain("[bellman | discord channel c1 (author:owner, msg:m3)]");
	expect(turn.text).toContain("@bot do the thing");
	// Consumed context is not replayed on the next turn.
	say("m4", "follow-up", "owner", "bellman", true);
	for (let attempt = 0; attempt < 400 && turns.length < 2; attempt++) await Bun.sleep(10);
	expect(turns).toHaveLength(2);
	expect(turns[1]?.text).not.toContain("first message");
	expect(turns[1]?.text).toContain("follow-up");
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
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "open", debounceMs: 0 } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const preambles: string[] = [];
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
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

test("[REPLY:id] parts thread to the referenced message and strip the directive", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
		sendTurn: async () => "[REPLY:msg-42] threaded answer\n[BREAK]\nplain follow-up",
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
			messageId: "m-1",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.message").length >= 2) break;
		await Bun.sleep(5);
	}
	const messages = client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(messages).toHaveLength(2);
	expect(messages[0].payload).toMatchObject({ text: "threaded answer", replyToMessageId: "msg-42" });
	expect(messages[1].payload.text).toBe("plain follow-up");
	expect(messages[1].payload.replyToMessageId).toBeUndefined();
	client.close();
});

test("work.run runs a named worker session in the requested cwd and returns the text", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const seen: Array<{ key?: string; text?: string; options?: unknown }> = [];
	const gjc: GjcPort = {
		ensureSession: async (key, _epoch, options) => {
			seen.push({ key, options });
			return { sessionId: "worker-session" };
		},
		forgetRebinds: () => {},
		sendTurn: async (_session, text, _preamble, _progress, options) => {
			seen.push({ text, options });
			return "worker result";
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "w",
		verb: "work.run",
		params: { name: "repo-fix", text: "fix the bug", cwd: "/tmp/some-repo" },
	});
	await waitFor(client.frames, 2);
	const response = client.frames.find((frame) => frame.type === "response" && frame.id === "w");
	expect(response.result).toMatchObject({ text: "worker result", sessionKey: "work/task/repo-fix", held: false });
	expect(seen[0]).toMatchObject({
		key: "work/task/repo-fix",
		options: { cwd: "/tmp/some-repo", codingRegister: true },
	});
	expect(seen[1]).toMatchObject({ text: "fix the bug", options: { cwd: "/tmp/some-repo", codingRegister: true } });
	// Invalid names are rejected before touching gjc.
	client.send({ v: "0.1", type: "request", id: "bad", verb: "work.run", params: { name: "../evil", text: "x" } });
	await waitFor(client.frames, 3);
	expect(client.frames.find((frame) => frame.type === "error" && frame.id === "bad")).toBeDefined();
	client.close();
});

test("work.run records a durable lane job and work.jobs projects it (issue #10)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0" }),
		sendTurn: async () => "worker result",
		forgetRebinds: () => {},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "w",
		verb: "work.run",
		params: { name: "Repo.Fix-2", text: "fix the bug" },
	});
	await waitFor(client.frames, 2);
	const run = client.frames.find((frame) => frame.type === "response" && frame.id === "w");
	expect(run.result).toMatchObject({ held: false, text: "worker result" });
	const jobId = run.result.jobId;
	expect(jobId).toBe(`lanejob-${Buffer.from("Repo.Fix-2", "utf8").toString("hex")}`);

	client.send({ v: "0.1", type: "request", id: "jobs", verb: "work.jobs" });
	await waitFor(client.frames, 3);
	const jobs = client.frames.find((frame) => frame.type === "response" && frame.id === "jobs");
	expect(jobs.result.jobs).toHaveLength(1);
	expect(jobs.result.jobs[0].job_id).toBe(jobId);
	expect(jobs.result.jobs[0].lane_key).toBe("work-Repo.Fix-2");
	// The completed ATTEMPT closed; the JOB stays continuable (attempt_ended),
	// never a terminal work-failure.
	expect(jobs.result.jobs[0].state).toBe("attempt_ended");
	// No commit was made, so the pre-existing HEAD must NOT appear as a
	// worker checkpoint: progress is measured against the creation baseline.
	expect(jobs.result.jobs[0].checkpoints).toBe(0);

	// The stored authority carries the closed attempt; the record is restart-safe.
	const raw = database.laneJobJson(jobId);
	expect(typeof raw).toBe("string");
	const parsed = parseLaneJobRecord(raw as string);
	expect(parsed.attempts).toHaveLength(1);
	expect(parsed.attempts[0].endState).toBe("completed");
	expect(parsed.attempts[0].sessionId).toBe("0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0");
	expect(parsed.lane.worktreePath).toBe(process.cwd());

	// A crashed predecessor's open attempt holds the job: the next work.run
	// records the uncertainty and REFUSES to continue without an operator ack.
	database.putLaneJob({
		jobId: parsed.jobId,
		laneKey: "work-Repo.Fix-2",
		state: "running",
		createdAt: parsed.createdAt,
		updatedAt: new Date().toISOString(),
		lane: parsed.lane,
		json: JSON.stringify({
			...parsed,
			attempts: [{ ...parsed.attempts[0], endState: undefined, endedAt: undefined }],
		}),
	});
	const manipulated = JSON.parse(database.laneJobJson(jobId) as string);
	client.send({ v: "0.1", type: "request", id: "w2", verb: "work.run", params: { name: "Repo.Fix-2", text: "again" } });
	async function waitId(id: string): Promise<void> {
		for (let attempt = 0; attempt < 400 && !client.frames.some((f) => f.id === id); attempt++) await Bun.sleep(5);
	}
	await waitId("w2");
	const held = client.frames.find((frame) => frame.id === "w2" && frame.type !== undefined);
	expect(held.type).toBe("response");
	expect(held.result).toMatchObject({ held: true, state: "awaiting_operator" });
	expect(held.result.reason).toMatch(/terminally uncertain/);

	// resume: true is the explicit operator acknowledgement.
	client.send({
		v: "0.1",
		type: "request",
		id: "w3",
		verb: "work.run",
		params: { name: "Repo.Fix-2", text: "again", resume: true },
	});
	await waitId("w3");
	const resumed = client.frames.find((frame) => frame.type === "response" && frame.id === "w3");
	expect(resumed.result).toMatchObject({ held: false, text: "worker result" });
	const revived = parseLaneJobRecord(database.laneJobJson(jobId) as string);
	// Uncertain predecessor closed + resumed attempt: the crash case writes
	// exactly one hold, one continuation.
	expect(revived.attempts).toHaveLength(2);
	expect(revived.attempts[0].endState).toBe("terminal_uncertain");
	expect(revived.escalations.some((entry) => entry.includes("terminal_uncertain"))).toBe(true);
	// Only the LAST attempt is open, and it closed completed.
	expect(revived.attempts.at(-1)?.endState).toBe("completed");
	client.send({ v: "0.1", type: "request", id: "jobs2", verb: "work.jobs" });
	await waitId("jobs2");
	const jobs2 = client.frames.find((frame) => frame.type === "response" && frame.id === "jobs2");
	expect(jobs2.result.jobs[0].state).toBe("attempt_ended");
	client.close();
});

test("a stalled durable job holds the next work.run until resume (production path)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	let turns = 0;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0" }),
		sendTurn: async () => {
			turns += 1;
			return "worker result";
		},
		forgetRebinds: () => {},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({ v: "0.1", type: "request", id: "w", verb: "work.run", params: { name: "stall-out", text: "x" } });
	await waitFor(client.frames, 2);
	expect(client.frames.find((frame) => frame.type === "response" && frame.id === "w")).toBeDefined();
	// Simulate the reconciliation outcome after repeated stalled continuations.
	const first = parseLaneJobRecord(
		database.laneJobJson(`lanejob-${Buffer.from("stall-out", "utf8").toString("hex")}`) as string,
	);
	database.putLaneJob({
		jobId: first.jobId,
		laneKey: `work-stall-out`,
		state: "stalled",
		createdAt: first.createdAt,
		updatedAt: new Date().toISOString(),
		lane: first.lane,
		json: JSON.stringify({ ...first, state: "stalled", stalledContinuations: MAX_STALLED_CONTINUATIONS }),
	});
	client.send({ v: "0.1", type: "request", id: "w2", verb: "work.run", params: { name: "stall-out", text: "x" } });
	async function waitId2(id: string): Promise<void> {
		for (let attempt = 0; attempt < 400 && !client.frames.some((f) => f.id === id && f.type !== undefined); attempt++)
			await Bun.sleep(5);
	}
	await waitId2("w2");
	const held = client.frames.find((frame) => frame.type === "response" && frame.id === "w2");
	expect(held.result).toMatchObject({ held: true, state: "stalled" });
	// The turn must NOT have run while held.
	expect(turns).toBe(1);
	// resume:true is the explicit operator acknowledgement that proceeds.
	client.send({
		v: "0.1",
		type: "request",
		id: "w3",
		verb: "work.run",
		params: { name: "stall-out", text: "x", resume: true },
	});
	await waitId2("w3");
	expect(
		client.frames.find((frame) => frame.type === "response" && frame.id === "w3" && frame.result?.held === false),
	).toBeDefined();
	expect(turns).toBe(2);
	client.close();
});
test("resuming a stalled job clears the hold durably: the next ordinary call is not held", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	let turns = 0;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0" }),
		sendTurn: async () => {
			turns += 1;
			return "worker result";
		},
		forgetRebinds: () => {},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const jobId = `lanejob-${Buffer.from("stall-clear", "utf8").toString("hex")}`;

	// First run succeeds (creates the job).
	client.send({ v: "0.1", type: "request", id: "w1", verb: "work.run", params: { name: "stall-clear", text: "x" } });
	async function waitId3(id: string): Promise<void> {
		for (let attempt = 0; attempt < 400 && !client.frames.some((f) => f.id === id && f.type !== undefined); attempt++)
			await Bun.sleep(5);
	}
	await waitId3("w1");

	// Force the stalled hold, as reconciliation would after repeat stalls.
	const first = parseLaneJobRecord(database.laneJobJson(jobId) as string);
	database.putLaneJob({
		jobId: first.jobId,
		laneKey: "work-stall-clear",
		state: "stalled",
		createdAt: first.createdAt,
		updatedAt: new Date().toISOString(),
		lane: first.lane,
		json: JSON.stringify({ ...first, state: "stalled", stalledContinuations: MAX_STALLED_CONTINUATIONS }),
	});

	// Ordinary call: held.
	client.send({ v: "0.1", type: "request", id: "w2", verb: "work.run", params: { name: "stall-clear", text: "x" } });
	await waitId3("w2");
	expect(client.frames.find((frame) => frame.id === "w2" && frame.type === "response")?.result).toMatchObject({
		held: true,
		state: "stalled",
	});
	expect(turns).toBe(1);

	// resume:true: runs AND durably clears the hold with an audit entry.
	client.send({
		v: "0.1",
		type: "request",
		id: "w3",
		verb: "work.run",
		params: { name: "stall-clear", text: "x", resume: true },
	});
	await waitId3("w3");
	expect(client.frames.find((frame) => frame.id === "w3" && frame.type === "response")?.result).toMatchObject({
		held: false,
		text: "worker result",
	});
	const afterResume = parseLaneJobRecord(database.laneJobJson(jobId) as string);
	expect(afterResume.stalledContinuations).toBe(0);
	expect(afterResume.escalations.some((entry) => entry.includes("operator resume acknowledged"))).toBe(true);

	// The NEXT ordinary call is not held by the old stalled state.
	client.send({ v: "0.1", type: "request", id: "w4", verb: "work.run", params: { name: "stall-clear", text: "x" } });
	await waitId3("w4");
	expect(client.frames.find((frame) => frame.id === "w4" && frame.type === "response")?.result).toMatchObject({
		held: false,
	});
	expect(turns).toBe(3);
	client.close();
});

test("responses larger than one socket buffer arrive intact (backpressure outbox)", async () => {
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
	// A ~700KB reply forces multiple kernel-buffer writes on the unix socket.
	const bigReply = `big:${"x".repeat(700_000)}:end`;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async () => bigReply,
		forgetRebinds: () => {},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "big",
		verb: "chat.send",
		params: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" }, text: "go" },
	});
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.some((frame) => frame.type === "event" && frame.event === "chat.message")) break;
		await Bun.sleep(10);
	}
	const message = client.frames.find((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(message).toBeDefined();
	expect(message.payload.text).toBe(bigReply);
	client.close();
});
