import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { BotAudienceTurnGuard } from "../src/engagement/policy";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = "discord/channel/bot-budget";
const ORIGIN_REF = { platform: "discord", kind: "channel", conversationId: "bot-budget" };

let directory = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;
let server: GatewayServer | undefined;

type TestFrame = {
	readonly id?: string;
	readonly result?: {
		readonly engaged?: boolean;
		readonly engagement?: { readonly botAudienceDeclines: number };
	};
};

async function eventually(predicate: () => boolean, message: string, attempts = 400): Promise<void> {
	for (let attempt = 0; attempt < attempts; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

afterEach(async () => {
	await manager?.stop();
	manager = undefined;
	await server?.stop();
	server = undefined;
	database?.close();
	database = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function connect(
	socketPath: string,
): Promise<{ send(value: unknown): void; frames: TestFrame[]; close(): void }> {
	const frames: TestFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as TestFrame);
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, close: () => socket.end() };
}

async function waitForFrame(frames: TestFrame[], id: string): Promise<TestFrame> {
	for (let attempt = 0; attempt < 400; attempt++) {
		const frame = frames.find((candidate) => candidate.id === id);
		if (frame) return frame;
		await Bun.sleep(5);
	}
	throw new Error(`frame ${id} did not arrive`);
}

function resultOf(frame: TestFrame): NonNullable<TestFrame["result"]> {
	if (!frame.result) throw new Error("response frame has no result");
	return frame.result;
}

async function startBotTurn(claimTerminal: boolean) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-bot-audience-refund-"));
	database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const port = attachTestBrokerOwnership(
		database,
		new ScriptedSessionPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` }),
		join(directory, "agent"),
	);
	const guard = new BotAudienceTurnGuard(database);
	const messageId = "bot-trigger";
	expect(
		database.inboundEnqueue({
			messageId,
			originKey: ORIGIN,
			originRefJson: JSON.stringify(ORIGIN_REF),
			body: "scheduled bot request",
			engagementJson: JSON.stringify({ mentioned: true, group: true, authorId: "bot", authorIsBot: true }),
		}),
	).toBe(true);
	guard.recordBotAdmission(ORIGIN, messageId);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "bot-audience-test",
		repo: join(directory, "workspace"),
		onTurnStart: ({ turn }) => ({
			text: "bot turn",
			onTerminal: () => {
				if (claimTerminal) database?.inboundTurnClaimTerminal(turn.opRef, 0, "answer-delivery");
			},
			onFailure: () => {},
			onSettled: ({ terminalDeliveryId }) => {
				if (terminalDeliveryId === null) guard.releaseUnansweredAdmission(ORIGIN, messageId);
			},
		}),
	});
	await manager.notifyInbound(ORIGIN);
	await eventually(() => port.sends.length === 1, "bot trigger was not sent");
	const firstSend = port.sends[0];
	if (!firstSend) throw new Error("bot trigger send disappeared");
	return { guard, port, opRef: firstSend.opRef };
}

test("an unanswered bot turn refunds its admission and the next bot follow-up is admitted", async () => {
	const { guard, port, opRef } = await startBotTurn(false);
	port.fail(opRef, "prompt deadline exceeded");
	await eventually(() => database?.inboundTurnRow(opRef)?.turn_state === "done", "failed bot turn did not settle");
	expect(guard.canAdmit(ORIGIN)).toBe(true);
	guard.recordBotAdmission(ORIGIN, "bot-follow-up");
	expect(guard.canAdmit(ORIGIN)).toBe(false);
});

test("a bot turn that claims its terminal slot stays spent until a human message", async () => {
	const { guard, port, opRef } = await startBotTurn(true);
	port.complete(opRef, "answered");
	await eventually(() => database?.inboundTurnRow(opRef)?.turn_state === "done", "answered bot turn did not settle");
	expect(guard.canAdmit(ORIGIN)).toBe(false);
	guard.recordHumanMessage(ORIGIN);
	expect(guard.canAdmit(ORIGIN)).toBe(true);
});

test("replaying settlement refunds at most once and never below zero", async () => {
	const { guard, port, opRef } = await startBotTurn(false);
	port.fail(opRef, "prompt deadline exceeded");
	await eventually(() => database?.inboundTurnRow(opRef)?.turn_state === "done", "failed bot turn did not settle");
	guard.releaseUnansweredAdmission(ORIGIN, "bot-trigger");
	expect(guard.canAdmit(ORIGIN)).toBe(true);
	guard.recordBotAdmission(ORIGIN, "new-bot-trigger");
	guard.releaseUnansweredAdmission(ORIGIN, "bot-trigger");
	expect(guard.canAdmit(ORIGIN)).toBe(false);
	guard.releaseUnansweredAdmission(ORIGIN, "new-bot-trigger");
	expect(guard.canAdmit(ORIGIN)).toBe(true);
});

test("guard state survives restart and a human message deletes the durable origin row", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-bot-audience-state-"));
	const dbPath = join(directory, "gateway.db");
	database = await GatewayDatabase.open(dbPath);
	const first = new BotAudienceTurnGuard(database);
	first.recordBotAdmission(ORIGIN, "bot-trigger");
	database.close();
	database = await GatewayDatabase.open(dbPath);
	const restarted = new BotAudienceTurnGuard(database);
	expect(restarted.canAdmit(ORIGIN)).toBe(false);
	restarted.recordHumanMessage(ORIGIN);
	expect(database.metaGet(`bot-audience-state:${ORIGIN}`)).toBeUndefined();
	database.close();
	database = await GatewayDatabase.open(dbPath);
	expect(new BotAudienceTurnGuard(database).canAdmit(ORIGIN)).toBe(true);
});

test("only addressed bot declines increment the durable operator counter", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-bot-audience-declines-"));
	database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const guard = new BotAudienceTurnGuard(database);
	guard.recordBotAudienceDecline(true);
	guard.recordBotAudienceDecline(false);
	expect(guard.botAudienceDeclines()).toBe(1);
	expect(new BotAudienceTurnGuard(database).botAudienceDeclines()).toBe(1);
});

test("the gateway counts addressed declines, ignores unaddressed bot chatter, and projects the counter", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-bot-audience-status-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "discord:bot-budget": { engagement: "open", audience: "all" } },
	};
	database = await GatewayDatabase.open(config.dbPath);
	const port = attachTestBrokerOwnership(
		database,
		new ScriptedSessionPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` }),
		join(directory, "agent"),
	);
	server = await startUnixServer({ config, database, sessionPort: port });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 400 && client.frames.length < 1; attempt++) await Bun.sleep(5);
	client.send({
		v: "0.1",
		type: "request",
		id: "admit",
		verb: "chat.send",
		params: {
			origin: ORIGIN_REF,
			text: "first bot request",
			messageId: "bot-status-1",
			engagement: { mentioned: true, group: true, authorId: "bot", authorIsBot: true },
		},
	});
	expect(resultOf(await waitForFrame(client.frames, "admit")).engaged).toBe(true);
	client.send({
		v: "0.1",
		type: "request",
		id: "addressed-decline",
		verb: "chat.send",
		params: {
			origin: ORIGIN_REF,
			text: "addressed follow-up",
			messageId: "bot-status-2",
			engagement: { mentioned: true, group: true, authorId: "bot", authorIsBot: true },
		},
	});
	expect(resultOf(await waitForFrame(client.frames, "addressed-decline")).engaged).toBe(false);
	client.send({
		v: "0.1",
		type: "request",
		id: "unaddressed-decline",
		verb: "chat.send",
		params: {
			origin: ORIGIN_REF,
			text: "unaddressed chatter",
			messageId: "bot-status-3",
			engagement: { mentioned: false, group: true, authorId: "bot", authorIsBot: true },
		},
	});
	expect(resultOf(await waitForFrame(client.frames, "unaddressed-decline")).engaged).toBe(false);
	client.send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
	expect(resultOf(await waitForFrame(client.frames, "status")).engagement).toEqual({ botAudienceDeclines: 1 });
	const firstSend = port.sends[0];
	if (firstSend) {
		port.fail(firstSend.opRef);
		await eventually(
			() => database?.inboundTurnRow(firstSend.opRef)?.turn_state === "done",
			"status fixture did not settle",
		);
	}
	client.close();
});
