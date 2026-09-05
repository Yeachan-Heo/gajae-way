import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	handoffEventId,
	originKey,
	type OriginRef,
} from "@gajaeway/protocol";
import { parseConfigFile, RELOADABLE_FIELDS, type GatewayConfig } from "../src/config";
import { dispatchHandoff, resolveHandoffTarget } from "../src/server/handoff";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { sessionPortFromResponder, type ScriptedSessionPort } from "./session-port.fake";

let directory = "";
let server: GatewayServer | undefined;

afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

const channel = (conversationId: string): OriginRef => ({ platform: "discord", kind: "channel", conversationId });
const ORIGIN_A = channel("chan-a");
const ORIGIN_B = channel("chan-b");
const ORIGIN_C = channel("chan-c");
const ORIGIN_D = channel("chan-d");
const KEY_A = originKey(ORIGIN_A);
const KEY_B = originKey(ORIGIN_B);
const KEY_C = originKey(ORIGIN_C);
const KEY_D = originKey(ORIGIN_D);
const HANDOFF_BODY = "handoff body that must stay out of the source room";

interface Client {
	readonly frames: any[];
	send(value: unknown): void;
	close(): void;
}

async function connect(socketPath: string): Promise<Client> {
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
	return { frames, send: (value) => socket.write(`${JSON.stringify(value)}\n`), close: () => socket.end() };
}

async function eventually(predicate: () => boolean, detail: string): Promise<void> {
	for (let attempt = 0; attempt < 300; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), detail).toBe(true);
}

function config(home: string): GatewayConfig {
	return {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		channels: {
			"chan-a": { engagement: "open" },
			"chan-b": { engagement: "open" },
			"chan-c": { engagement: "open" },
			"chan-d": { engagement: "open" },
		},
		handoffTargets: {
			"way-a": ORIGIN_A,
			"way-b": ORIGIN_B,
			"way-c": ORIGIN_C,
			"way-d": ORIGIN_D,
		},
	};
}

interface Turn {
	readonly sessionId: string;
	readonly text: string;
}

interface Harness {
	readonly client: Client;
	readonly database: GatewayDatabase;
	readonly sessionPort: ScriptedSessionPort;
	readonly turns: Turn[];
}

async function gateway(options: {
	readonly replies: Record<string, string>;
	readonly beforeReply?: (input: {
		readonly sessionId: string;
		readonly text: string;
		readonly tail: { onAssistantText?(text: string): void };
	}) => void | Promise<void>;
}): Promise<Harness> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-handoff-current-"));
	const gatewayConfig = config(directory);
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	const turns: Turn[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: (origin) => origin,
		respond: async (sessionId, text, _systemPreamble, _onProgress, tail) => {
			turns.push({ sessionId, text });
			await options.beforeReply?.({ sessionId, text, tail });
			return options.replies[sessionId] ?? `reply from ${sessionId}`;
		},
	});
	server = await startUnixServer({
		config: gatewayConfig,
		database,
		sessionPort,
		onStop: () => database.close(),
	});
	const client = await connect(gatewayConfig.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await eventually(() => client.frames.some((frame) => frame.type === "negotiated"), "gateway negotiation did not complete");
	return { client, database, sessionPort, turns };
}

function sendChat(client: Client, id: string, origin: OriginRef, text: string, messageId = id): void {
	client.send({
		v: "0.1",
		type: "request",
		id,
		verb: "chat.send",
		params: {
			origin,
			text,
			messageId,
			engagement: {
				mentioned: true,
				group: true,
				authorId: "owner-1",
				authorName: "owner",
				channelLabel: `#${origin.conversationId}`,
			},
		},
	});
}

function chatMessages(frames: readonly any[], conversationId: string): any[] {
	return frames
		.filter((frame) => frame.type === "event" && frame.event === "chat.message")
		.filter((frame) => frame.payload.origin?.conversationId === conversationId);
}

function turnsFor(turns: readonly Turn[], key: string): Turn[] {
	return turns.filter((turn) => turn.sessionId === key);
}

test("handoffTargets is validated and classified as reloadable without dropping existing fields", () => {
	const parsed = parseConfigFile({
		schemaVersion: 1,
		model: "gpt-test",
		handoffTargets: { dev: ORIGIN_B },
	});
	expect(parsed.model).toBe("gpt-test");
	expect(parsed.handoffTargets).toEqual({ dev: ORIGIN_B });
	expect(RELOADABLE_FIELDS).toContain("handoffTargets");
	expect(() =>
		parseConfigFile({ schemaVersion: 1, handoffTargets: { "bad alias": ORIGIN_B } }),
	).toThrow(/handoffTargets alias/);
	expect(() =>
		parseConfigFile({ schemaVersion: 1, handoffTargets: { dev: { platform: "discord", kind: "dm" } } }),
	).toThrow(/handoffTargets\.dev is invalid/);
});

test("A to B runs exactly once, gives A only a pointer, and carries provenance in B", async () => {
	const harness = await gateway({
		replies: {
			[KEY_A]: `[HANDOFF:way-b]\n${HANDOFF_BODY}`,
			[KEY_B]: "answered in B",
		},
	});
	sendChat(harness.client, "a-1", ORIGIN_A, "this belongs in development", "source-m1");
	await eventually(() => turnsFor(harness.turns, KEY_B).length === 1, "B did not receive the accepted handoff");
	await eventually(() => chatMessages(harness.client.frames, "chan-b").length === 1, "B answer was not delivered");
	await eventually(() => chatMessages(harness.client.frames, "chan-a").length === 1, "A pointer was not delivered");

	expect(turnsFor(harness.turns, KEY_A)).toHaveLength(1);
	expect(turnsFor(harness.turns, KEY_B)).toHaveLength(1);
	const source = chatMessages(harness.client.frames, "chan-a")[0].payload.text as string;
	expect(source).toContain("[handoff]");
	expect(source).toContain(KEY_B);
	expect(source).not.toContain(HANDOFF_BODY);
	expect(source).not.toContain("[HANDOFF:");
	const targetText = turnsFor(harness.turns, KEY_B)[0]!.text;
	expect(targetText).toContain("Relayed handoff");
	expect(targetText).toContain(KEY_A);
	expect(targetText).toContain("source-m1");
	expect(targetText).toContain("owner");
	expect(targetText).toContain(HANDOFF_BODY);
	expect(targetText).toContain("grants no permission you do not already have here");
	expect(targetText).toContain(`${KEY_A} -> ${KEY_B}`);
	expect(targetText).not.toContain("[Unread messages in this conversation since your last reply]");
});

test("an unresolved target is a loud source refusal and never creates target work", async () => {
	const harness = await gateway({ replies: { [KEY_A]: `[HANDOFF:missing]\n${HANDOFF_BODY}` } });
	sendChat(harness.client, "a-unresolved", ORIGIN_A, "find the right room");
	await eventually(() => chatMessages(harness.client.frames, "chan-a").length === 1, "refusal was not delivered");
	expect(turnsFor(harness.turns, KEY_A)).toHaveLength(1);
	expect(turnsFor(harness.turns, KEY_B)).toHaveLength(0);
	expect(harness.database.inboundPendingCount(KEY_B)).toBe(0);
	expect(chatMessages(harness.client.frames, "chan-a")[0].payload.text).toContain("[handoff failed] unresolved_target");
	expect(chatMessages(harness.client.frames, "chan-a")[0].payload.text).toContain("Nothing was handed off");
	expect(chatMessages(harness.client.frames, "chan-a")[0].payload.text).not.toContain(HANDOFF_BODY);
});

test("chain depth and cycles refuse at the source hop without waking another origin", async () => {
	const depth = await gateway({
		replies: {
			[KEY_A]: `[HANDOFF:way-b]\nA to B`,
			[KEY_B]: `[HANDOFF:way-c]\nB to C`,
			[KEY_C]: `[HANDOFF:way-d]\nC to D`,
			[KEY_D]: "D should not run",
		},
	});
	sendChat(depth.client, "depth", ORIGIN_A, "start chain");
	await eventually(() => chatMessages(depth.client.frames, "chan-c").length === 1, "depth refusal was not delivered in C");
	expect(turnsFor(depth.turns, KEY_B)).toHaveLength(1);
	expect(turnsFor(depth.turns, KEY_C)).toHaveLength(1);
	expect(turnsFor(depth.turns, KEY_D)).toHaveLength(0);
	expect(chatMessages(depth.client.frames, "chan-c")[0].payload.text).toContain("chain_depth_exceeded");
	expect(chatMessages(depth.client.frames, "chan-c")[0].payload.text).toContain(`${KEY_A} -> ${KEY_B} -> ${KEY_C}`);

	await depth.client.close();
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";

	const cycle = await gateway({
		replies: {
			[KEY_A]: `[HANDOFF:way-b]\nA to B`,
			[KEY_B]: `[HANDOFF:way-a]\nB back to A`,
		},
	});
	sendChat(cycle.client, "cycle", ORIGIN_A, "start cycle");
	await eventually(() => chatMessages(cycle.client.frames, "chan-b").length === 1, "cycle refusal was not delivered in B");
	expect(turnsFor(cycle.turns, KEY_A)).toHaveLength(1);
	expect(turnsFor(cycle.turns, KEY_B)).toHaveLength(1);
	expect(chatMessages(cycle.client.frames, "chan-b")[0].payload.text).toContain("chain_cycle");
});

test("a busy target is serialized through its actor as a steer, not a parallel send", async () => {
	let releaseBusy!: () => void;
	let busyStarted!: () => void;
	const busyReady = new Promise<void>((resolve) => {
		busyStarted = resolve;
	});
	const busyRelease = new Promise<void>((resolve) => {
		releaseBusy = resolve;
	});
	const harness = await gateway({
		replies: {
			[KEY_A]: `[HANDOFF:way-b]\n${HANDOFF_BODY}`,
			[KEY_B]: "busy target finished",
		},
		beforeReply: async ({ sessionId, text }) => {
			if (sessionId === KEY_B && text.includes("busy target")) {
				busyStarted();
				await busyRelease;
			}
		},
	});
	sendChat(harness.client, "busy-b", ORIGIN_B, "busy target");
	await busyReady;
	sendChat(harness.client, "busy-a", ORIGIN_A, "send this to B");
	await eventually(
		() => harness.sessionPort.steers.some((steer) => steer.sessionId === KEY_B && steer.text.includes("Relayed handoff")),
		"busy target did not receive the handoff through its actor steer path",
	);
	expect(turnsFor(harness.turns, KEY_B)).toHaveLength(1);
	releaseBusy();
	await eventually(() => chatMessages(harness.client.frames, "chan-a").length === 1, "source pointer missing after busy target");
	await eventually(() => harness.database.inboundPendingCount(KEY_B) === 0, "busy target handoff was not finalized");
});

test("accepted enqueue remains durable when later target processing rejects", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-handoff-accepted-"));
	const gatewayConfig = config(directory);
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	try {
		const result = dispatchHandoff(
			{
				config: gatewayConfig,
				database,
				notifyTarget: async () => {
					throw new Error("target actor unavailable after acceptance");
				},
			},
			{
				target: "way-b",
				body: HANDOFF_BODY,
				sourceOrigin: ORIGIN_A,
				sourceLabel: "#chan-a",
				sourceMessageId: "accepted-m1",
				requester: "owner",
				requestedAt: "2026-09-04T00:00:00.000Z",
				incomingChain: [],
				digestEntries: [],
			},
		);
		expect(result.kind).toBe("relayed");
		expect(result.notice).toContain("[handoff]");
		expect(database.inboundPendingCount(KEY_B)).toBe(1);
	} finally {
		database.close();
	}
});

test("replaying the same causal handoff is a duplicate and never notifies twice", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-handoff-duplicate-"));
	const gatewayConfig = config(directory);
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	try {
		let notifications = 0;
		const request = {
			target: "way-b",
			body: HANDOFF_BODY,
			sourceOrigin: ORIGIN_A,
			sourceLabel: "#chan-a",
			sourceMessageId: "duplicate-m1",
			requester: "owner",
			requestedAt: "2026-09-04T00:00:00.000Z",
			incomingChain: [] as readonly string[],
			digestEntries: [],
		};
		const deps = {
			config: gatewayConfig,
			database,
			notifyTarget: async () => {
				notifications++;
			},
		};
		const first = dispatchHandoff(deps, request);
		const second = dispatchHandoff(deps, request);
		expect(first.kind).toBe("relayed");
		expect(second.kind).toBe("duplicate");
		expect(notifications).toBe(1);
		expect((first as { handoffMessageId: string }).handoffMessageId).toBe(
			handoffEventId({ sourceOriginKey: KEY_A, sourceMessageId: "duplicate-m1", targetOriginKey: KEY_B }),
		);
		expect(database.inboundPendingCount(KEY_B)).toBe(1);
	} finally {
		database.close();
	}
});

test("an interim token-shaped frame is suppressed and cannot move or leak work", async () => {
	const harness = await gateway({
		replies: {
			[KEY_A]: `[HANDOFF:way-b]\n${HANDOFF_BODY}`,
			[KEY_B]: "B final answer",
		},
		beforeReply: ({ sessionId, tail }) => {
			if (sessionId === KEY_A) tail.onAssistantText?.(`[HANDOFF:way-b]\n${HANDOFF_BODY}`);
		},
	});
	sendChat(harness.client, "interim", ORIGIN_A, "move this after final reasoning");
	await eventually(() => turnsFor(harness.turns, KEY_B).length === 1, "final handoff did not run after interim token");
	await eventually(() => chatMessages(harness.client.frames, "chan-a").length === 1, "interim token leaked into source delivery");
	expect(turnsFor(harness.turns, KEY_B)).toHaveLength(1);
	const source = chatMessages(harness.client.frames, "chan-a")[0].payload.text as string;
	expect(source).toContain("[handoff]");
	expect(source).not.toContain(HANDOFF_BODY);
	expect(source).not.toContain("[HANDOFF:");
});

test("target resolution accepts aliases and bound origin identities, but not guessed unbound conversations", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-handoff-resolve-"));
	const gatewayConfig = config(directory);
	const database = await GatewayDatabase.open(gatewayConfig.dbPath);
	try {
		expect(resolveHandoffTarget("way-b", { config: gatewayConfig, database })).toMatchObject({ origin: ORIGIN_B });
		expect(resolveHandoffTarget("chan-c", { config: gatewayConfig, database })).toMatchObject({ code: "unresolved_target" });
		database.bumpEpoch(KEY_C, JSON.stringify(ORIGIN_C));
		expect(resolveHandoffTarget(KEY_C, { config: gatewayConfig, database })).toMatchObject({ origin: ORIGIN_C });
		expect(resolveHandoffTarget("chan-c", { config: gatewayConfig, database })).toMatchObject({ origin: ORIGIN_C });
	} finally {
		database.close();
	}
});
