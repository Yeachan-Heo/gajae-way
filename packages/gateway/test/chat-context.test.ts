import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatContextEntry, EngagementContext, OriginRef } from "@gajaeway/protocol";
import { originKey } from "@gajaeway/protocol";
import type { GatewayConfig } from "../src/config";
import type { GjcPort } from "../src/orchestrator/gjc-client";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

type Frame = {
	readonly type?: string;
	readonly id?: string;
	readonly event?: string;
	readonly result?: unknown;
	readonly payload?: unknown;
	readonly error?: { readonly code?: string };
};
type TestClient = {
	readonly frames: Frame[];
	readonly send: (value: unknown) => void;
	readonly close: () => void;
};

let directory = "";
let server: GatewayServer | undefined;
let activeClient: TestClient | undefined;

afterEach(async () => {
	activeClient?.close();
	activeClient = undefined;
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function connect(socketPath: string): Promise<TestClient> {
	const frames: Frame[] = [];
	let buffered = "";
	const socket = await Bun.connect<undefined>({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as Frame);
			},
		},
	});
	const client = {
		frames,
		send: (value: unknown) => {
			const wire = `${JSON.stringify(value)}\n`;
			for (let offset = 0; offset < wire.length; offset += 8_000) {
				const chunk = wire.slice(offset, offset + 8_000);
				if (offset === 0) socket.write(chunk);
				else setTimeout(() => socket.write(chunk), offset / 8_000);
			}
		},
		close: () => socket.end(),
	};
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(frames, (frame) => frame.type === "negotiated");
	return client;
}

async function waitFor(frames: readonly Frame[], predicate: (frame: Frame) => boolean): Promise<Frame> {
	for (let attempt = 0; attempt < 400; attempt++) {
		const frame = frames.find(predicate);
		if (frame) return frame;
		await Bun.sleep(5);
	}
	throw new Error("timed out waiting for frame");
}

async function start(
	gjc: GjcPort,
	channels?: GatewayConfig["channels"],
): Promise<{ client: TestClient; database: GatewayDatabase }> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-chat-context-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		...(channels ? { channels } : {}),
	};
	const database = await GatewayDatabase.open(config.dbPath);
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	activeClient = await connect(config.socketPath);
	return { client: activeClient, database };
}

function engagement(authorId: string, authorName = authorId): EngagementContext {
	return { mentioned: false, group: true, authorId, authorName };
}

function entry(messageId: string, at: string, text = messageId, authorId = messageId): ChatContextEntry {
	return { messageId, text, engagement: engagement(authorId), at };
}

function sendContext(
	client: TestClient,
	id: string,
	origin: OriginRef,
	entries: readonly ChatContextEntry[],
	maxItems = 20,
	maxCharsPerItem = 200,
): void {
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.context",
		params: { origin, entries, cap: { maxItems, maxCharsPerItem } },
	});
}

function resultOf(frame: Frame): Record<string, unknown> {
	if (typeof frame.result !== "object" || frame.result === null || Array.isArray(frame.result))
		throw new Error("expected an object result");
	return frame.result as Record<string, unknown>;
}

test("records only the newest cap entries atomically and retries are idempotent", async () => {
	let turns = 0;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "unused" }),
		sendTurn: async () => {
			turns++;
			return "unused";
		},
	};
	const { client, database } = await start(gjc);
	const origin: OriginRef = { platform: "discord", kind: "channel", conversationId: "cap" };
	const base = Date.now() - 21_000;
	const entries = Array.from({ length: 21 }, (_, index) =>
		entry(`m${String(index + 1).padStart(2, "0")}`, new Date(base + index * 1_000).toISOString()),
	);
	sendContext(client, "first", origin, entries);
	const first = await waitFor(client.frames, (frame) => frame.type === "response" && frame.id === "first");
	expect(resultOf(first)).toEqual({ recorded: 20, dropped: 1, truncated: 0, engaged: false });
	expect(database.contextUnread(originKey(origin), 100).map((row) => row.message_id)).toEqual(
		entries.slice(1).map((item) => item.messageId),
	);
	sendContext(client, "retry", origin, entries);
	const retry = await waitFor(client.frames, (frame) => frame.type === "response" && frame.id === "retry");
	expect(resultOf(retry)).toEqual({ recorded: 0, dropped: 1, truncated: 0, engaged: false });
	expect(turns).toBe(0);
});

test("separates the request-size ceiling from the cap", async () => {
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "unused" }),
		sendTurn: async () => "unused",
	};
	const { client, database } = await start(gjc);
	const origin: OriginRef = { platform: "discord", kind: "channel", conversationId: "size" };
	const at = new Date().toISOString();
	const entries = Array.from({ length: 200 }, (_, index) => entry(`m${index}`, at));
	sendContext(client, "two-hundred", origin, entries);
	const accepted = await waitFor(client.frames, (frame) => frame.type === "response" && frame.id === "two-hundred");
	expect(resultOf(accepted)).toMatchObject({ recorded: 20, dropped: 180, engaged: false });
	expect(database.contextUnread(originKey(origin), 100)).toHaveLength(20);
	const tooMany = [...entries, entry("m200", at)];
	sendContext(client, "two-hundred-one", origin, tooMany);
	const rejected = await waitFor(client.frames, (frame) => frame.type === "error" && frame.id === "two-hundred-one");
	expect(rejected.error?.code).toBe("invalid_params");
	expect(database.contextUnread(originKey(origin), 100)).toHaveLength(20);
});

test("sorts by at and messageId, truncates bodies, and never dispatches a turn", async () => {
	let turns = 0;
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "unused" }),
		sendTurn: async () => {
			turns++;
			return "unused";
		},
	};
	const { client, database } = await start(gjc, { ordered: { engagement: "open", debounceMs: 0 } });
	const origin: OriginRef = { platform: "discord", kind: "channel", conversationId: "ordered" };
	const sameAt = new Date(Date.now() - 1_000).toISOString();
	const entries = [
		entry("speaker-b", sameAt, "b", "speaker-b"),
		entry("older", new Date(Date.parse(sameAt) - 1_000).toISOString(), "x".repeat(100), "speaker-old"),
		entry("speaker-a", sameAt, "a", "speaker-a"),
	];
	sendContext(client, "ordered", origin, entries, 100, 50);
	const response = await waitFor(client.frames, (frame) => frame.type === "response" && frame.id === "ordered");
	expect(resultOf(response)).toEqual({ recorded: 3, dropped: 0, truncated: 1, engaged: false });
	expect(database.contextUnread(originKey(origin), 100).map((row) => row.message_id)).toEqual([
		"older",
		"speaker-a",
		"speaker-b",
	]);
	expect(database.contextUnread(originKey(origin), 100)[0]?.body).toHaveLength(50);
	sendContext(client, "ordered-retry", origin, entries, 100, 50);
	const retry = await waitFor(client.frames, (frame) => frame.type === "response" && frame.id === "ordered-retry");
	expect(resultOf(retry)).toMatchObject({ recorded: 0, truncated: 1, engaged: false });
	expect(turns).toBe(0);
});

test("accepts the inner future envelope and rejects entries beyond it", async () => {
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "unused" }),
		sendTurn: async () => "unused",
	};
	const { client } = await start(gjc);
	const acceptedOrigin: OriginRef = { platform: "discord", kind: "channel", conversationId: "future-accepted" };
	sendContext(client, "future-59", acceptedOrigin, [entry("future-59", new Date(Date.now() + 59_000).toISOString())]);
	sendContext(client, "future-60", acceptedOrigin, [entry("future-60", new Date(Date.now() + 60_000).toISOString())]);
	const accepted59 = await waitFor(client.frames, (frame) => frame.type === "response" && frame.id === "future-59");
	const accepted60 = await waitFor(client.frames, (frame) => frame.type === "response" && frame.id === "future-60");
	expect(resultOf(accepted59).engaged).toBe(false);
	expect(resultOf(accepted60).engaged).toBe(false);
	const rejectedOrigin: OriginRef = { platform: "discord", kind: "channel", conversationId: "future-rejected" };
	sendContext(client, "future-61", rejectedOrigin, [entry("future-61", new Date(Date.now() + 61_000).toISOString())]);
	const rejected = await waitFor(client.frames, (frame) => frame.type === "error" && frame.id === "future-61");
	expect(rejected.error?.code).toBe("invalid_params");
});
