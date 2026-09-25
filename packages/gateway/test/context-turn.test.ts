import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { SessionPort } from "../src/orchestrator/session-port";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, sessionPortFromScript } from "./session-port.fake";

const ORIGIN = { platform: "discord", kind: "channel", conversationId: "context-turn" } as const;
const ORIGIN_KEY = "discord/channel/context-turn";
let directory = "";
let server: GatewayServer | undefined;

afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function config(): Promise<GatewayConfig> {
	directory = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-context-turn-")));
	return {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "context-turn": { engagement: "mention-open" } },
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

async function waitUntil(predicate: () => boolean): Promise<void> {
	for (let attempt = 0; attempt < 400 && !predicate(); attempt++) await Bun.sleep(5);
	expect(predicate()).toBe(true);
}

function send(client: { send(value: unknown): void }, id: string, text: string): void {
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.send",
		params: {
			origin: ORIGIN,
			text,
			messageId: id,
			receivedAt: new Date().toISOString(),
			engagement: { mentioned: true, group: true, authorId: "owner", authorName: "bellman" },
		},
	});
}

async function start(
	gatewayConfig: GatewayConfig,
	database: GatewayDatabase,
	sessionPort: SessionPort,
	seed?: () => void,
): Promise<{ client: Awaited<ReturnType<typeof connect>> }> {
	const { bind, resume } = sessionPort;
	attachTestBrokerOwnership(database, sessionPort, join(gatewayConfig.home, "agent"));
	seed?.();
	server = await startUnixServer({
		config: gatewayConfig,
		database,
		sessionPort,
		onStop: () => {
			// A restart reuses the fake runtime, not wrappers bound to a closed DB.
			sessionPort.bind = bind;
			sessionPort.resume = resume;
			database.close();
		},
	});
	const client = await connect(gatewayConfig.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitUntil(() => client.frames.length >= 1);
	return { client };
}

test("failed turn preserves selected context; later successful text consumes it with trigger attribution once", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const turns: string[] = [];
	let attempts = 0;
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, text) => {
			turns.push(text);
			if (attempts++ === 0) throw new Error("runtime failed before reply");
			return "success";
		},
	});
	const { client } = await start(gatewayConfig, database, sessionPort, () => {
		database.contextRecord({
			messageId: "context-bot",
			originKey: ORIGIN_KEY,
			authorId: "bot-2",
			authorName: "helper-bot",
			body: "current bounded bot context",
		});
	});

	send(client, "trigger-failed", "first owner request");
	await waitUntil(() =>
		client.frames.some((frame) => frame.event === "chat.message" && frame.payload.text.startsWith("[turn failed]")),
	);
	expect(
		database
			.contextUnread(ORIGIN_KEY)
			.map((row) => row.message_id)
			.sort(),
	).toEqual(["context-bot", "trigger-failed"]);

	send(client, "trigger-success", "second owner request");
	await waitUntil(() => turns.length === 2);
	expect(turns[1]).toContain("helper-bot (author:bot-2, msg:context-bot): current bounded bot context");
	expect(turns[1]).toContain("first owner request");
	expect(turns[1]?.match(/second owner request/g)).toHaveLength(1);
	await waitUntil(() => database.contextUnread(ORIGIN_KEY).length === 0);
	client.close();
});

test("successful intentional silence advances the context cursor", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async () => "[SILENT]",
	});
	const { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "silent-trigger", "read this but stay quiet");
	await waitUntil(() => database.contextUnread(ORIGIN_KEY).length === 0);
	expect(client.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
	client.close();
});

test("a delivered intermediate reply followed by runtime failure consumes the selected context and posts no failure notice", async () => {
	// The user saw an answer; the context it was written from is read, and a
	// "[turn failed]" after a visible reply would only be noise. (Under the
	// former mid-work gate this text was suppressed as pre-tool and the test
	// asserted the opposite; the gate is gone - 2026-09-18.)
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, _text, _preamble, _progress, options) => {
			options?.onAssistantText?.("delivered intermediate reply");
			throw new Error("runtime failed after visible reply");
		},
	});
	const { client } = await start(gatewayConfig, database, sessionPort, () => {
		database.contextRecord({ messageId: "context-human", originKey: ORIGIN_KEY, body: "relevant context" });
	});
	send(client, "intermediate-trigger", "owner request");
	await waitUntil(() => client.frames.some((frame) => frame.event === "chat.message"));
	await waitUntil(() => database.contextUnread(ORIGIN_KEY).length === 0);
	const messages = client.frames.filter((frame) => frame.event === "chat.message");
	expect(messages.map((frame) => frame.payload.text)).toEqual(["delivered intermediate reply"]);
	client.close();
});

test("a chatty turn past the mid-work part budget still delivers its final answer and settles", async () => {
	// The per-turn part budget caps mid-work speech; it must never eat the
	// terminal slot. With the interim gate gone a 12-message turn is ordinary.
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, _text, _preamble, _progress, options) => {
			for (let index = 1; index <= 12; index++) options?.onAssistantText?.(`progress note ${index}`);
			return "the final answer";
		},
	});
	const { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "chatty-trigger", "owner request");
	await waitUntil(() =>
		client.frames.some((frame) => frame.event === "chat.message" && frame.payload.text === "the final answer"),
	);
	await waitUntil(() => database.inboundPendingCount(ORIGIN_KEY) === 0);
	const texts = client.frames.filter((frame) => frame.event === "chat.message").map((frame) => frame.payload.text);
	expect(texts.filter((text) => text.startsWith("progress note"))).toHaveLength(10);
	expect(texts.filter((text) => text === "the final answer")).toHaveLength(1);
	expect(database.inboundTurnRow(sessionPort.sends[0]!.opRef)?.terminal_delivery_id).not.toBeNull();
	client.close();
});

test("a turn that wrote its answer and then failed on a hung tool delivers that answer, not a bare failure (#210)", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async () => {
			throw new Error("Agent run failed after execution started.");
		},
	});
	// The reply sits in the transcript; the tail never showed it.
	sessionPort.fetchAssistantSince = async () => ({
		text: "written before the tool timed out",
		pages: 1,
		complete: true,
	});
	const { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "hung-tool-trigger", "owner request");
	await waitUntil(() => database.inboundPendingCount(ORIGIN_KEY) === 0);
	await waitUntil(() => client.frames.some((frame) => frame.event === "chat.message"));
	// A visible answer consumes the context it was written from.
	await waitUntil(() => database.contextUnread(ORIGIN_KEY).length === 0);
	const texts = client.frames.filter((frame) => frame.event === "chat.message").map((frame) => frame.payload.text);
	expect(texts).toEqual(["written before the tool timed out"]);
	client.close();
});

test("a delivered intermediate reaction followed by runtime failure consumes the selected context", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, _text, _preamble, _progress, options) => {
			options?.onAssistantText?.("[REACT:👍]");
			throw new Error("runtime failed after visible reaction");
		},
	});
	const { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "reaction-trigger", "owner request");
	await waitUntil(() => client.frames.some((frame) => frame.event === "chat.message"));
	await waitUntil(() => database.contextUnread(ORIGIN_KEY).length === 0);
	const messages = client.frames.filter((frame) => frame.event === "chat.message");
	expect(messages).toHaveLength(1);
	expect(messages[0]?.payload.reaction).toMatchObject({ targetMessageId: "reaction-trigger", emoji: "👍" });
	client.close();
});

test("/new retires a pre-reset trigger queued behind an in-flight turn", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	let release: (() => void) | undefined;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const turns: string[] = [];
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, text) => {
			turns.push(text);
			if (turns.length === 1) await gate;
			return "ok";
		},
	});
	const { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "running-trigger", "running owner request");
	await waitUntil(() => turns.length === 1);
	send(client, "queued-before-reset", "must never run after reset");
	await waitUntil(() => database.inboundPendingCount(ORIGIN_KEY) === 1);
	send(client, "reset-while-busy", "/new");
	await waitUntil(() => client.frames.some((frame) => frame.type === "response" && frame.id === "reset-while-busy"));
	release?.();
	// The retired turn settles from status once released; the queued-behind
	// message rode into it as a steer and must never become a fresh turn.
	await waitUntil(() => database.inboundPendingCount(ORIGIN_KEY) === 0);
	expect(turns).toHaveLength(1);
	client.close();
});

test("/new persists a floor that excludes pre-reset context after gateway restart", async () => {
	const gatewayConfig = await config();
	let database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const turns: string[] = [];
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, text) => {
			turns.push(text);
			return "ok";
		},
	});
	let { client } = await start(gatewayConfig, database, sessionPort, () => {
		database.contextRecord({ messageId: "pre-reset", originKey: ORIGIN_KEY, body: "old command" });
	});
	send(client, "reset-command", "/new");
	await waitUntil(() => client.frames.some((frame) => frame.type === "response" && frame.id === "reset-command"));
	client.close();
	await server?.stop();
	server = undefined;

	database = await GatewayDatabase.open(gatewayConfig.dbPath);
	({ client } = await start(gatewayConfig, database, sessionPort));
	send(client, "post-reset", "new session request");
	await waitUntil(() => turns.length === 1);
	expect(turns[0]).not.toContain("old command");
	expect(database.contextDiagnostics(ORIGIN_KEY).floorAt).not.toBeNull();
	client.close();
});

test("epoch bootstrap commits on first terminal success and is not repeated after restart", async () => {
	const gatewayConfig = await config();
	let database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const preambles: string[] = [];
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, _text, preamble) => {
			preambles.push(preamble ?? "");
			return "ok";
		},
	});
	let { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "bootstrap-first", "first request");
	await waitUntil(() => preambles.length === 1);
	expect(preambles[0]).toContain("## Session bootstrap");
	expect(database.getSessionBootstrap(ORIGIN_KEY)).toMatchObject({
		epoch: 0,
		lastBootstrappedEpoch: 0,
		byteCount: expect.any(Number),
	});
	send(client, "bootstrap-second", "second request");
	await waitUntil(() => preambles.length === 2);
	expect(preambles[1]).not.toContain("## Session bootstrap");
	client.close();
	await server?.stop();
	server = undefined;

	database = await GatewayDatabase.open(gatewayConfig.dbPath);
	({ client } = await start(gatewayConfig, database, sessionPort));
	send(client, "bootstrap-after-restart", "after restart");
	await waitUntil(() => preambles.length === 3);
	expect(preambles[2]).not.toContain("## Session bootstrap");
	client.close();
});

test("pre-success failure retries the same stable bootstrap and intentional silence commits it", async () => {
	const gatewayConfig = await config();
	let database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const preambles: string[] = [];
	let attempt = 0;
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, _text, preamble) => {
			preambles.push(preamble ?? "");
			if (attempt++ === 0) throw new Error("failed before delivery");
			return "[SILENT]";
		},
	});
	let { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "bootstrap-fail", "first request");
	await waitUntil(() =>
		client.frames.some((frame) => frame.event === "chat.message" && frame.payload.text.startsWith("[turn failed]")),
	);
	expect(database.getSessionBootstrap(ORIGIN_KEY)?.lastBootstrappedEpoch).toBe(-1);
	client.close();
	await server?.stop();
	server = undefined;
	database = await GatewayDatabase.open(gatewayConfig.dbPath);
	({ client } = await start(gatewayConfig, database, sessionPort));
	send(client, "bootstrap-retry", "retry request");
	await waitUntil(() => preambles.length === 2);
	const marker = /bootstrap-id: (session-bootstrap:[a-f0-9]+)/.exec(preambles[0] ?? "")?.[1];
	expect(marker).toBeString();
	expect(preambles[1]).toContain(`bootstrap-id: ${marker}`);
	await waitUntil(() => database.getSessionBootstrap(ORIGIN_KEY)?.lastBootstrappedEpoch === 0);
	expect(
		client.frames.filter((frame) => frame.event === "chat.message" && frame.payload?.text === "[SILENT]"),
	).toHaveLength(0);
	client.close();
});

test("intermediate delivery failure keeps bootstrap pending but consumes the body before stable-marker retry", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const attempts: Array<{ text: string; preamble: string }> = [];
	let first = true;
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, text, preamble, _progress, options) => {
			attempts.push({ text, preamble: preamble ?? "" });
			if (first) {
				first = false;
				options?.onAssistantText?.("visible answer");
				throw new Error("failed after delivery");
			}
			return "ok";
		},
	});
	const { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "bootstrap-visible-fail", "body that must not replay");
	// The visible answer ships (no mid-work gate); the failure after it posts no
	// notice. The turn is over once the trigger row settles.
	await waitUntil(() => client.frames.some((frame) => frame.event === "chat.message"));
	await waitUntil(() => database.inboundPendingCount(ORIGIN_KEY) === 0);
	expect(database.getSessionBootstrap(ORIGIN_KEY)?.lastBootstrappedEpoch).toBe(-1);
	send(client, "bootstrap-visible-retry", "next body");
	await waitUntil(() => attempts.length === 2);
	const marker = /bootstrap-id: (session-bootstrap:[a-f0-9]+)/.exec(attempts[0]?.preamble ?? "")?.[1];
	expect(attempts[1]?.preamble).toContain(`bootstrap-id: ${marker}`);
	expect(attempts[1]?.text).toContain("body that must not replay");
	expect(attempts[1]?.text.match(/next body/g)).toHaveLength(1);
	client.close();
});

test("/new atomically establishes a fresh floor and pending epoch bootstrap", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const preambles: string[] = [];
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, _text, preamble) => {
			preambles.push(preamble ?? "");
			return "ok";
		},
	});
	const { client } = await start(gatewayConfig, database, sessionPort);
	send(client, "before-reset-success", "before reset");
	await waitUntil(() => database.getSessionBootstrap(ORIGIN_KEY)?.lastBootstrappedEpoch === 0);
	send(client, "bootstrap-reset", "/new");
	await waitUntil(() => database.getSessionRecord(ORIGIN_KEY)?.epoch === 1);
	expect(database.getSessionBootstrap(ORIGIN_KEY)).toMatchObject({ epoch: 1, lastBootstrappedEpoch: 0 });
	expect(database.contextDiagnostics(ORIGIN_KEY).floorAt).not.toBeNull();
	send(client, "after-reset-success", "after reset");
	await waitUntil(() => preambles.length === 2);
	expect(preambles[1]).toContain("epoch: 1");
	expect(preambles[1]).toContain("## Session bootstrap");
	client.close();
});

test("persistent sessions do not auto-rotate on a turn count", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const preambles: string[] = [];
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async (_session, _text, preamble) => {
			preambles.push(preamble ?? "");
			return "ok";
		},
	});
	const { client } = await start(gatewayConfig, database, sessionPort);
	for (let turn = 0; turn < 3; turn++) {
		send(client, `persistent-${turn}`, `turn ${turn}`);
		await waitUntil(() => preambles.length === turn + 1);
	}
	expect(database.getSessionRecord(ORIGIN_KEY)?.epoch).toBe(0);
	expect(preambles.filter((preamble) => preamble.includes("## Session bootstrap"))).toHaveLength(1);
	client.close();
});

test("gateway startup prunes old consumed context even when the database was quiet", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const sessionPort = sessionPortFromScript({
		bind: async (key, epoch) => ({ sessionId: `session-${key}-${epoch}` }),
		respond: async () => "ok",
	});
	const { client } = await start(gatewayConfig, database, sessionPort, () => {
		const raw = new Database(gatewayConfig.dbPath);
		raw
			.query(
				"INSERT INTO conversation_context (message_id, origin_key, body, received_at, consumed_at) VALUES (?, ?, ?, ?, ?)",
			)
			.run(
				"quiet-old",
				ORIGIN_KEY,
				"old private body",
				new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString(),
				new Date().toISOString(),
			);
		raw.close();
	});
	const inspected = new Database(gatewayConfig.dbPath, { readonly: true });
	expect(inspected.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM conversation_context").get()?.n).toBe(0);
	inspected.close();
	client.close();
});
