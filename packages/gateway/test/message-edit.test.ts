import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, messageEditId, renderMessageEdit, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { sessionPortFromScript } from "./session-port.fake";

/**
 * A message the user edits after the gateway ingested it is streamed into the
 * same session as an update of a `[MESSAGE POINTER: <id>]`: steered into the
 * running turn, or sent as the next turn when idle. Never a fresh message,
 * never lost, never an edit of something the gateway did not see.
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

const ORIGIN = { platform: "discord", kind: "dm", conversationId: "d1", peerId: "owner" } as const;
const ORIGIN_KEY = "discord/dm/d1/peer=owner";
const ENGAGEMENT = { mentioned: false, group: false, authorId: "owner", authorName: "bellman" };

async function connect(socketPath: string) {
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
	return {
		send: (value: unknown) => socket.write(`${JSON.stringify(value)}\n`),
		frames,
		close: () => socket.end(),
		response: (id: string) => frames.find((frame) => frame.type === "response" && frame.id === id),
	};
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 400; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

async function start(respond: (text: string) => Promise<string>) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-message-edit-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
		channels: { c1: { engagement: "open-mention-only" } },
	};
	database = await GatewayDatabase.open(config.dbPath);
	const port = sessionPortFromScript({ respond: (_session, text) => respond(text) });
	server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database?.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await eventually(() => client.frames.length >= 1, "negotiation did not complete");
	const send = (id: string, messageId: string, text: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: { origin: ORIGIN, text, messageId, engagement: ENGAGEMENT },
		});
	const edit = (id: string, messageId: string, text: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.edit",
			params: { origin: ORIGIN, messageId, text, engagement: ENGAGEMENT },
		});
	return { client, port, send, edit };
}

test("an edit during the running turn is steered into it as a [MESSAGE POINTER] update", async () => {
	let release!: () => void;
	const running = new Promise<void>((resolve) => {
		release = resolve;
	});
	const turns: string[] = [];
	const { client, port, send, edit } = await start(async (text) => {
		turns.push(text);
		await running;
		return "ok";
	});
	send("s1", "m-1", "book a table for 2");
	await eventually(() => turns.length === 1, "original was not sent");
	edit("e1", "m-1", "book a table for 4");
	await eventually(() => port.steers.length === 1, "edit was not steered into the running turn");
	expect(port.sends).toHaveLength(1);
	expect(port.steers[0]!.text).toContain("[MESSAGE POINTER: m-1]");
	expect(port.steers[0]!.text).toContain("book a table for 4");
	expect(client.response("e1")?.result).toMatchObject({ engaged: true });
	expect(client.response("e1")?.result.turnId).toBeString();
	// The edit is attributed to the running turn, and the context ledger now
	// carries the new body under the original message id.
	const turn = database!.inboundNonterminalTurns(ORIGIN_KEY)[0]!;
	expect(database!.inboundTurnRows(turn.opRef).map((row) => [row.message_id, row.turn_role])).toEqual([
		["m-1", "trigger"],
		[messageEditId("m-1", "book a table for 4"), "steer"],
	]);
	release();
	await eventually(() => database!.inboundPendingCount(ORIGIN_KEY) === 0, "turn did not complete");
	client.close();
});

test("an edit while idle is sent as the next turn, pointing at the original message", async () => {
	const turns: string[] = [];
	const { client, send, edit } = await start(async (text) => {
		turns.push(text);
		return "ok";
	});
	send("s1", "m-1", "what is 2+2");
	await eventually(() => turns.length === 1, "original was not sent");
	await eventually(() => database!.inboundPendingCount(ORIGIN_KEY) === 0, "first turn did not complete");
	edit("e1", "m-1", "what is 2+3");
	await eventually(() => turns.length === 2, "edit was not sent as the next turn");
	// The speaker header is prepended like any DM turn; the body is the pointer update.
	expect(turns[1]).toEndWith(renderMessageEdit("m-1", "what is 2+3"));
	expect(turns[1]).toContain("[MESSAGE POINTER: m-1]");
	expect(client.response("e1")?.result).toMatchObject({ engaged: true });
	client.close();
});

test("an edit of a message the gateway never ingested is dropped", async () => {
	const turns: string[] = [];
	const { client, edit } = await start(async (text) => {
		turns.push(text);
		return "ok";
	});
	edit("e1", "never-seen", "hello?");
	await eventually(() => client.response("e1") !== undefined, "no response to the edit");
	expect(client.response("e1")?.result).toEqual({ turnId: null, engaged: false });
	await Bun.sleep(50);
	expect(turns).toEqual([]);
	expect(database!.inboundPendingCount(ORIGIN_KEY)).toBe(0);
	client.close();
});

test("the same edit event delivered twice is one update; a further edit is a second update", async () => {
	const turns: string[] = [];
	const { client, send, edit } = await start(async (text) => {
		turns.push(text);
		return "ok";
	});
	send("s1", "m-1", "v1");
	await eventually(() => turns.length === 1, "original was not sent");
	await eventually(() => database!.inboundPendingCount(ORIGIN_KEY) === 0, "first turn did not complete");
	edit("e1", "m-1", "v2");
	await eventually(() => turns.length === 2, "edit was not sent");
	await eventually(() => database!.inboundPendingCount(ORIGIN_KEY) === 0, "edit turn did not complete");
	edit("e1-replay", "m-1", "v2");
	await eventually(() => client.response("e1-replay") !== undefined, "no response to the replayed edit");
	expect(client.response("e1-replay")?.result).toEqual({ turnId: null, engaged: true });
	await Bun.sleep(50);
	expect(turns).toHaveLength(2);
	edit("e2", "m-1", "v3");
	await eventually(() => turns.length === 3, "second edit was not sent");
	expect(turns[2]).toContain("v3");
	expect(messageEditId("m-1", "v2")).not.toBe(messageEditId("m-1", "v3"));
	client.close();
});

test("a steered edit of a context-only message consumes the ORIGINAL message's context row, so the next turn does not replay it as unread", async () => {
	let release!: () => void;
	const running = new Promise<void>((resolve) => {
		release = resolve;
	});
	const turns: string[] = [];
	const { client, port } = await start(async (text) => {
		turns.push(text);
		if (turns.length === 1) await running;
		return "ok";
	});
	// Mention-only channel: while alice's mention runs, bob posts WITHOUT a
	// mention (context only, never a turn or a steer), then edits that post
	// into a mention. Only the edit is steered.
	const channel = { platform: "discord", kind: "channel", conversationId: "c1" } as const;
	const post = (id: string, messageId: string, text: string, authorId: string, mentioned: boolean) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin: channel,
				text,
				messageId,
				engagement: { mentioned, group: true, authorId, authorName: authorId },
			},
		});
	const editIn = (id: string, messageId: string, text: string, authorId: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.edit",
			params: {
				origin: channel,
				messageId,
				text,
				engagement: { mentioned: true, group: true, authorId, authorName: authorId },
			},
		});
	post("s1", "m-1", "@bot start", "alice", true);
	await eventually(() => turns.length === 1, "first turn was not sent");
	post("s2", "m-2", "bot, wrong ping", "bob", false);
	await eventually(() => client.response("s2")?.result?.engaged === false, "bob's post was not recorded as context");
	editIn("e1", "m-2", "@bot right ping", "bob");
	await eventually(() => port.steers.length === 1, "bob's edit was not steered");
	expect(port.steers[0]!.text).toContain("[MESSAGE POINTER: m-2]");
	expect(port.steers[0]!.text).toContain("@bot right ping");
	release();
	await eventually(() => database!.inboundPendingCount("discord/channel/c1") === 0, "first turn did not complete");
	post("s3", "m-3", "@bot next", "alice", true);
	await eventually(() => turns.length === 2, "next turn was not sent");
	// m-2 was read inside turn 1 (as the pointer update): the next turn must
	// not get it again as unread context, in either body.
	expect(turns[1]).not.toContain("right ping");
	expect(turns[1]).not.toContain("wrong ping");
	expect(turns[1]).toContain("@bot next");
	client.close();
});
