import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { type BotAudienceLimits, BotAudienceTurnGuard } from "../src/engagement/policy";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = "discord/channel/bot-budget";
const ORIGIN_REF = { platform: "discord", kind: "channel", conversationId: "bot-budget" };
/** These cases are about the one-turn budget, so they configure it explicitly; the default is unlimited. */
const CAP_ONE: BotAudienceLimits = { maxConsecutiveTurns: 1, maxTurnsPerWindow: 30 };

let directory = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;
let server: GatewayServer | undefined;

type TestFrame = {
	readonly id?: string;
	readonly result?: {
		readonly engaged?: boolean;
		readonly engagement?: { readonly botAudienceDeclines: number; readonly botAudienceRateLimited: number };
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
	expect(guard.canAdmit(ORIGIN, CAP_ONE).admit).toBe(true);
	guard.recordBotAdmission(ORIGIN, "bot-follow-up");
	expect(guard.canAdmit(ORIGIN, CAP_ONE).admit).toBe(false);
});

test("a bot turn that claims its terminal slot stays spent until a human message", async () => {
	const { guard, port, opRef } = await startBotTurn(true);
	port.complete(opRef, "answered");
	await eventually(() => database?.inboundTurnRow(opRef)?.turn_state === "done", "answered bot turn did not settle");
	expect(guard.canAdmit(ORIGIN, CAP_ONE).admit).toBe(false);
	guard.recordHumanMessage(ORIGIN);
	expect(guard.canAdmit(ORIGIN, CAP_ONE).admit).toBe(true);
});

test("replaying settlement refunds at most once and never below zero", async () => {
	const { guard, port, opRef } = await startBotTurn(false);
	port.fail(opRef, "prompt deadline exceeded");
	await eventually(() => database?.inboundTurnRow(opRef)?.turn_state === "done", "failed bot turn did not settle");
	guard.releaseUnansweredAdmission(ORIGIN, "bot-trigger");
	expect(guard.canAdmit(ORIGIN, CAP_ONE).admit).toBe(true);
	guard.recordBotAdmission(ORIGIN, "new-bot-trigger");
	guard.releaseUnansweredAdmission(ORIGIN, "bot-trigger");
	expect(guard.canAdmit(ORIGIN, CAP_ONE).admit).toBe(false);
	guard.releaseUnansweredAdmission(ORIGIN, "new-bot-trigger");
	expect(guard.canAdmit(ORIGIN, CAP_ONE).admit).toBe(true);
});

test("guard state survives restart; a human message clears the budget and keeps the rate window", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-bot-audience-state-"));
	const dbPath = join(directory, "gateway.db");
	database = await GatewayDatabase.open(dbPath);
	const first = new BotAudienceTurnGuard(database);
	first.recordBotAdmission(ORIGIN, "bot-trigger");
	database.close();
	database = await GatewayDatabase.open(dbPath);
	const restarted = new BotAudienceTurnGuard(database);
	expect(restarted.canAdmit(ORIGIN, CAP_ONE).admit).toBe(false);
	restarted.recordHumanMessage(ORIGIN);
	// The consecutive budget is spent by conversation flow and reset by a human.
	// The runaway window is not, so the row survives until the window itself ages out.
	expect(restarted.consecutiveTurns(ORIGIN)).toBe(0);
	expect(restarted.windowedTurns(ORIGIN)).toBe(1);
	database.close();
	database = await GatewayDatabase.open(dbPath);
	const reopened = new BotAudienceTurnGuard(database);
	expect(reopened.canAdmit(ORIGIN, CAP_ONE).admit).toBe(true);
	reopened.recordHumanMessage(ORIGIN, Date.now() + 2 * 60_000);
	expect(database.metaGet(`bot-audience-state:${ORIGIN}`)).toBeUndefined();
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
		channels: { "discord:bot-budget": { engagement: "open", audience: "all", botAudienceMaxConsecutiveTurns: 1 } },
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
	expect(resultOf(await waitForFrame(client.frames, "status")).engagement).toEqual({
		botAudienceDeclines: 1,
		botAudienceRateLimited: 0,
	});
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

test("an open bot audience runs consecutive bot turns by default and stops only at the rate limit", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-bot-audience-unlimited-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// No consecutive cap: the channel is open to bots, and only the runaway
		// rate limit — set low here — bounds the conversation.
		channels: { "discord:bot-budget": { engagement: "open", audience: "all", botAudienceMaxTurnsPerWindow: 2 } },
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
	const botSend = (id: string, messageId: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin: ORIGIN_REF,
				text: `bot message ${messageId}`,
				messageId,
				engagement: { mentioned: true, group: true, authorId: "bot", authorIsBot: true },
			},
		});
	botSend("bot-1", "bot-loop-1");
	expect(resultOf(await waitForFrame(client.frames, "bot-1")).engaged).toBe(true);
	// Two consecutive bot turns with no human in between: impossible before #252.
	botSend("bot-2", "bot-loop-2");
	expect(resultOf(await waitForFrame(client.frames, "bot-2")).engaged).toBe(true);
	botSend("bot-3", "bot-loop-3");
	expect(resultOf(await waitForFrame(client.frames, "bot-3")).engaged).toBe(false);
	client.send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
	expect(resultOf(await waitForFrame(client.frames, "status")).engagement).toEqual({
		botAudienceDeclines: 1,
		botAudienceRateLimited: 1,
	});
	for (const send of port.sends) {
		port.fail(send.opRef);
		await eventually(
			() => database?.inboundTurnRow(send.opRef)?.turn_state === "done",
			"rate-limit fixture did not settle",
		);
	}
	client.close();
});
