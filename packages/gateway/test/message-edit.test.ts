import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajaeway/subsession";
import type { GatewayConfig } from "../src/config";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { type GatewayServer, messageEditId, renderMessageEdit, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort, sessionPortFromScript } from "./session-port.fake";

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

/**
 * A steer the runtime recorded but whose transport tore is finalized like a
 * live one once its acceptance is learnt - at the old turn's terminal, or
 * after a gateway restart - so the next turn never sees it as unread.
 */
class TornUntilTerminalPort extends ScriptedSessionPort {
	torn = true;
	attempts = 0;
	async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]): Promise<void> {
		this.attempts++;
		if (this.torn) throw new GjcCliError("gjc sdk turn.steer exited 1", 1, "socket reset");
		await super.steer(input);
	}
}

async function startWith(port: ScriptedSessionPort, dir?: string) {
	directory = dir ?? (await mkdtemp(join(tmpdir(), "gajaeway-message-edit-")));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	database = await GatewayDatabase.open(config.dbPath);
	server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database?.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await eventually(() => client.frames.length >= 1, "negotiation did not complete");
	return client;
}

const dm = (client: Awaited<ReturnType<typeof connect>>, id: string, messageId: string, text: string) =>
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.send",
		params: { origin: ORIGIN, text, messageId, engagement: ENGAGEMENT },
	});

test("a held steer accepted at the old turn's terminal consumes its context row: the next turn does not replay it", async () => {
	const port = new TornUntilTerminalPort();
	const client = await startWith(port);
	dm(client, "s1", "m-1", "first");
	await eventually(() => port.sends.length === 1, "first turn was not sent");
	dm(client, "s2", "m-2", "torn follow-up");
	await eventually(() => port.attempts >= 3, "steer was not replayed and held");
	expect(database!.inboundSteersHeld(port.sends[0]!.opRef).map((r) => r.message_id)).toEqual(["m-2"]);
	// The runtime had recorded it: the replay at terminal returns acceptance.
	port.torn = false;
	port.complete(port.sends[0]!.opRef, "answer one");
	await eventually(
		() => database!.inboundTurnRow(port.sends[0]!.opRef)?.turn_state === "done",
		"turn did not complete",
	);
	expect(database!.inboundSteersHeld(port.sends[0]!.opRef)).toEqual([]);
	dm(client, "s3", "m-3", "next");
	await eventually(() => port.sends.length === 2, "next turn was not sent");
	expect(port.sends[1]!.text).not.toContain("torn follow-up");
	expect(port.sends[1]!.text).toContain("next");
	port.complete(port.sends[1]!.opRef, "answer two");
	await eventually(() => database!.inboundPendingCount(ORIGIN_KEY) === 0, "next turn did not complete");
	client.close();
});

test("a held steer accepted after a gateway restart consumes its context row: the next turn does not replay it", async () => {
	const port = new TornUntilTerminalPort();
	let client = await startWith(port);
	dm(client, "s1", "m-1", "first");
	await eventually(() => port.sends.length === 1, "first turn was not sent");
	const opRef = port.sends[0]!.opRef;
	dm(client, "s2", "m-2", "torn follow-up");
	await eventually(() => port.attempts >= 3, "steer was not replayed and held");
	port.complete(opRef, "answer one");
	await eventually(() => database!.inboundTurnRow(opRef)?.turn_state === "done", "turn did not complete");
	expect(database!.inboundSteersHeld(opRef).map((r) => r.message_id)).toEqual(["m-2"]);
	// Restart the gateway on the same home; the transport is healthy again and
	// the runtime reports the recorded acceptance on the same clientRef.
	client.close();
	await server?.stop();
	server = undefined;
	port.torn = false;
	client = await startWith(port, directory);
	dm(client, "s3", "m-3", "next");
	await eventually(() => port.sends.length === 2, "next turn was not sent after restart");
	expect(database!.inboundSteersHeld(opRef)).toEqual([]);
	expect(database!.inboundTurnRows(opRef).map((r) => [r.message_id, r.turn_state])).toEqual([
		["m-1", "done"],
		["m-2", "done"],
	]);
	expect(port.sends[1]!.text).not.toContain("torn follow-up");
	expect(port.sends[1]!.text).toContain("next");
	port.complete(port.sends[1]!.opRef, "answer two");
	await eventually(() => database!.inboundPendingCount(ORIGIN_KEY) === 0, "next turn did not complete");
	client.close();
});

test("a crash between steer acceptance and the lifecycle hook cannot leave the message unread: acceptance and context consumption are one transaction", async () => {
	// Direct actor surface with a lifecycle whose onSteerAccepted "crashes":
	// the durable acceptance (steer done + context consumed) has already
	// committed, nothing after it runs, and a fresh manager on the same
	// database must see the context row consumed.
	directory = await mkdtemp(join(tmpdir(), "gajaeway-message-edit-"));
	database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const port = new ScriptedSessionPort();
	const logs: string[] = [];
	const manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "crash",
		repo: join(directory, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			steerContextMessageId: (row) => row.message_id,
			onSteerAccepted: () => {
				throw new Error("simulated crash after durable acceptance");
			},
		}),
		log: (line) => {
			logs.push(line);
		},
	});
	const record = (messageId: string, body: string) => {
		database!.contextRecord({ messageId, originKey: ORIGIN_KEY, authorId: "owner", body });
		expect(
			database!.inboundEnqueue({ messageId, originKey: ORIGIN_KEY, originRefJson: JSON.stringify(ORIGIN), body }),
		).toBe(true);
	};
	try {
		record("m-1", "first");
		await manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "first turn was not sent");
		const opRef = port.sends[0]!.opRef;
		record("m-2", "steered then crash");
		await manager.notifyInbound(ORIGIN_KEY).catch(() => {});
		await eventually(() => logs.some((line) => line.includes("simulated crash")), "crash hook did not fire");
		expect(database.inboundTurnRows(opRef).map((r) => [r.message_id, r.turn_state])).toEqual([
			["m-1", "accepted"],
			["m-2", "done"],
		]);
		// The unread window no longer contains m-2: consumed in the same
		// transaction as the acceptance, before the crash.
		// (m-1 is the running turn's own trigger; it is committed at that turn's terminal.)
		expect(database.contextWindow(ORIGIN_KEY, "probe").rows.map((r) => r.message_id)).toEqual(["m-1"]);
	} finally {
		await manager.stop();
	}
});
