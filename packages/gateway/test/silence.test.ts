import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isSilenceToken } from "@gajaeway/protocol";
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

async function settle(): Promise<void> {
	for (let attempt = 0; attempt < 60; attempt++) await Bun.sleep(5);
}

/** An open channel: the adapter promotes human messages here, so every one reaches a turn. */
async function openChannelGateway(reply: string): Promise<{ frames: any[]; database: GatewayDatabase }> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-silence-"));
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
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		sendTurn: async () => reply,
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 60 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	client.send({
		v: "0.1",
		type: "request",
		id: "c1",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "channel", conversationId: "chan-1" },
			text: "people chatting about lunch",
			messageId: "m1",
			engagement: { mentioned: true, group: true, authorId: "human-1" },
		},
	});
	await settle();
	return { frames: client.frames, database };
}

test("silence tokens are matched exactly, after trimming and case folding", () => {
	for (const token of ["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY", "  [silent]  ", "no_reply"])
		expect(isSilenceToken(token)).toBe(true);
	for (const text of ["[SILENT] but actually", "silence", "", "I will stay silent", "형님 [SILENT]"])
		expect(isSilenceToken(text)).toBe(false);
});

test("bracketing a silence token never turns it into a delivered message", () => {
	// `[SILENT]` was the only bracketed spelling in the catalog, so `[NO_REPLY]`
	// used to be delivered verbatim into the room.
	for (const token of ["[NO_REPLY]", "[NO REPLY]", "  [no_reply]  ", "[SILENT]", "SILENT"])
		expect(isSilenceToken(token)).toBe(true);
	// Stripping brackets must not turn prose into silence.
	for (const text of ["[reply please]", "[]", "[SILENT] but actually", "[NO_REPLY] just kidding"])
		expect(isSilenceToken(text)).toBe(false);
});

test("a silent turn in an open channel delivers nothing", async () => {
	const { frames } = await openChannelGateway("[SILENT]");
	expect(frames.some((frame) => frame.type === "response" && frame.id === "c1")).toBe(true);
	expect(frames.some((frame) => frame.type === "event" && frame.event === "chat.message")).toBe(false);
});

test("a spoken turn in the same open channel does deliver", async () => {
	const { frames } = await openChannelGateway("형님, 점심은 국밥이 답입니다.");
	const message = frames.find((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(message).toBeDefined();
	expect(message.payload.text).toContain("국밥");
});

test("a silent turn still marks its inbound message done so the queue does not stall", async () => {
	const { database } = await openChannelGateway("[SILENT]");
	expect(database.inboundPendingCount("discord/channel/chan-1")).toBe(0);
});
