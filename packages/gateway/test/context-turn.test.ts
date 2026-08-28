import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import type { GjcPort } from "../src/orchestrator/gjc-client";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

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
	directory = await mkdtemp(join(tmpdir(), "gajaeway-context-turn-"));
	return {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "context-turn": { engagement: "open-mention-only", debounceMs: 0 } },
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
	gjc: GjcPort,
): Promise<{ client: Awaited<ReturnType<typeof connect>> }> {
	server = await startUnixServer({ config: gatewayConfig, database, gjc, onStop: () => database.close() });
	const client = await connect(gatewayConfig.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitUntil(() => client.frames.length >= 1);
	return { client };
}

test("failed turn preserves selected context; later successful text consumes it with trigger attribution once", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	database.contextRecord({
		messageId: "context-bot",
		originKey: ORIGIN_KEY,
		authorId: "bot-2",
		authorName: "helper-bot",
		body: "current bounded bot context",
	});
	const turns: string[] = [];
	let attempts = 0;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "session" }),
		forgetRebinds: () => {},
		sendTurn: async (_session, text) => {
			turns.push(text);
			if (attempts++ === 0) throw new Error("runtime failed before reply");
			return "success";
		},
	};
	const { client } = await start(gatewayConfig, database, gjc);

	send(client, "trigger-failed", "first owner request");
	await waitUntil(() => client.frames.some((frame) => frame.type === "error" && frame.id === "trigger-failed"));
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
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "session" }),
		forgetRebinds: () => {},
		sendTurn: async () => "[SILENT]",
	};
	const { client } = await start(gatewayConfig, database, gjc);
	send(client, "silent-trigger", "read this but stay quiet");
	await waitUntil(() => database.contextUnread(ORIGIN_KEY).length === 0);
	expect(client.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
	client.close();
});

test("intermediate real assistant delivery followed by runtime failure consumes the selected window", async () => {
	const gatewayConfig = await config();
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	database.contextRecord({ messageId: "context-human", originKey: ORIGIN_KEY, body: "relevant context" });
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "session" }),
		forgetRebinds: () => {},
		sendTurn: async (_session, _text, _preamble, _progress, options) => {
			options?.onAssistantText?.("delivered intermediate reply");
			throw new Error("runtime failed after visible reply");
		},
	};
	const { client } = await start(gatewayConfig, database, gjc);
	send(client, "intermediate-trigger", "owner request");
	await waitUntil(() => client.frames.some((frame) => frame.type === "error" && frame.id === "intermediate-trigger"));
	expect(database.contextUnread(ORIGIN_KEY)).toEqual([]);
	const messages = client.frames.filter((frame) => frame.event === "chat.message");
	expect(messages.map((frame) => frame.payload.text)).toEqual(["delivered intermediate reply"]);
	client.close();
});

test("/new persists a floor that excludes pre-reset context after gateway restart", async () => {
	const gatewayConfig = await config();
	let database = await GatewayDatabase.open(gatewayConfig.dbPath);
	database.contextRecord({ messageId: "pre-reset", originKey: ORIGIN_KEY, body: "old command" });
	const turns: string[] = [];
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "session" }),
		forgetRebinds: () => {},
		sendTurn: async (_session, text) => {
			turns.push(text);
			return "ok";
		},
	};
	let { client } = await start(gatewayConfig, database, gjc);
	send(client, "reset-command", "/new");
	await waitUntil(() => client.frames.some((frame) => frame.type === "response" && frame.id === "reset-command"));
	client.close();
	await server?.stop();
	server = undefined;

	database = await GatewayDatabase.open(gatewayConfig.dbPath);
	({ client } = await start(gatewayConfig, database, gjc));
	send(client, "post-reset", "new session request");
	await waitUntil(() => turns.length === 1);
	expect(turns[0]).not.toContain("old command");
	expect(database.contextDiagnostics(ORIGIN_KEY).floorAt).not.toBeNull();
	client.close();
});
