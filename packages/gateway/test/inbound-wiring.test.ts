import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { RelayRefusedError } from "../src/orchestrator/tail-runner";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

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

async function negotiated(socketPath: string): Promise<{ send(value: unknown): void; frames: any[]; close(): void }> {
	const client = await connect(socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	return client;
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 400 && !predicate(); attempt++) await Bun.sleep(5);
	expect(predicate(), message).toBe(true);
}

test("a trigger re-dispatched after its first attach failed still answers the loopback requester", async () => {
	class StaleFirstPort extends ScriptedSessionPort {
		refusals = 0;
		override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
			if (this.refusals === 0) {
				this.refusals += 1;
				throw new RelayRefusedError(input.sessionId, "endpoint_stale", "endpoint is not live");
			}
			return await super.attachTail(input);
		}
	}
	const config = await makeConfig();
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new StaleFirstPort({
		onBind: (input) => `${input.originKey}#${input.epoch}`,
		onSend: (input, scripted) => scripted.complete(input.opRef, "answer on the fresh session"),
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	// Connected first, like an adapter: an answer without its requester context
	// falls back to the first connection.
	const adapter = await negotiated(config.socketPath);
	const requester = await negotiated(config.socketPath);

	requester.send(chatSend("ask", "msg-stale", "hello after a stale endpoint"));
	await eventually(
		() => requester.frames.some((frame) => frame.event === "chat.message" && frame.payload.final === true),
		"the requester never received the final answer",
	);

	expect(sessionPort.refusals).toBe(1);
	expect(sessionPort.sends).toHaveLength(1);
	const turnId = requester.frames.find((frame) => frame.type === "response" && frame.id === "ask").result.turnId;
	const answer = requester.frames.find((frame) => frame.event === "chat.message");
	expect(answer.payload).toMatchObject({ turnId, text: "answer on the fresh session", final: true });
	expect(adapter.frames.filter((frame) => frame.event === "chat.message")).toEqual([]);
	expect(database.inboundPendingCount("loopback/loopback/loopback")).toBe(0);
	adapter.close();
	requester.close();
});

test("a failed loopback turn ends the requester's wait with a final failure notice", async () => {
	const config = await makeConfig();
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({
		onBind: (input) => `${input.originKey}#${input.epoch}`,
		onSend: (input, scripted) => scripted.fail(input.opRef, "provider exploded"),
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const requester = await negotiated(config.socketPath);

	requester.send(chatSend("ask", "msg-fail", "this turn fails"));
	await eventually(
		() => requester.frames.some((frame) => frame.event === "chat.message" && frame.payload.final === true),
		"the requester never learned that its turn failed",
	);

	const turnId = requester.frames.find((frame) => frame.type === "response" && frame.id === "ask").result.turnId;
	const notice = requester.frames.find((frame) => frame.event === "chat.message");
	expect(notice.payload.turnId).toBe(turnId);
	expect(notice.payload.text).toStartWith("[turn failed]");
	expect(database.inboundPendingCount("loopback/loopback/loopback")).toBe(0);
	requester.close();
});

test("a duplicate message id is acknowledged but never dispatched twice", async () => {
	const config = await makeConfig();
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	const sessionPort = new ScriptedSessionPort({
		onBind: (input) => `${input.originKey}#${input.epoch}`,
		onSend: (input, scripted) => {
			turns.push(input.text);
			scripted.complete(input.opRef, "mock reply");
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
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

test("a message arriving while a persistent turn is in flight is steered into that turn", async () => {
	const config = await makeConfig();
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({ onBind: (input) => `${input.originKey}#${input.epoch}` });
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);

	client.send(chatSend("one", "msg-1", "first"));
	await waitFor(client.frames, 2);
	// The actor sends the correction through operator-gated turn.steer instead of
	// waiting for a second legacy CLI turn.
	client.send(chatSend("two", "msg-2", "second"));
	await waitFor(client.frames, 3);
	expect(sessionPort.sends.map((send) => send.text)).toEqual(["first"]);
	expect(sessionPort.steers.map((steer) => steer.text.replace(/^\[Additional message[^\n]*\]\n/, ""))).toEqual([
		"second",
	]);
	sessionPort.complete(sessionPort.sends[0]!.opRef, "reply to first");
	await waitFor(client.frames, 4);

	const replies = client.frames.filter((frame) => frame.event === "chat.message").map((frame) => frame.payload.text);
	expect(replies).toEqual(["reply to first"]);
	expect(database.inboundPendingCount("loopback/loopback/loopback")).toBe(0);
	client.close();
});
