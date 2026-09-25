import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { originKey } from "@gajae-gateway/protocol";
import { MAX_STALLED_CONTINUATIONS, parseLaneJobRecord } from "@gajae-gateway/subsession";
import type { GatewayConfig } from "../src/config";
import { memoryRoot } from "../src/memory/doctrine";
import { deterministicTerminalDeliveryId } from "../src/orchestrator/tail-runner";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import {
	attachTestBrokerOwnership,
	ScriptedSessionPort,
	sessionPortFromResponder,
	sessionPortFromScript,
} from "./session-port.fake";

let directory = "";
let server: GatewayServer | undefined;
const workSessionIds = new Map<string, string>();
afterEach(async () => {
	await server?.stop();
	server = undefined;
	workSessionIds.clear();
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function connect(
	socketPath: string,
): Promise<{ send(value: unknown): void; frames: any[]; wireLines: string[]; close(): void }> {
	const frames: any[] = [];
	const wireLines: string[] = [];
	let buffered = Buffer.alloc(0);
	const socket = await Bun.connect({
		unix: socketPath,
		socket: {
			data(_socket, data) {
				buffered = Buffer.concat([buffered, Buffer.from(data)]);
				let newline = buffered.indexOf(10);
				while (newline >= 0) {
					const line = buffered.subarray(0, newline).toString("utf8");
					buffered = buffered.subarray(newline + 1);
					if (line) {
						wireLines.push(line);
						frames.push(JSON.parse(line));
					}
					newline = buffered.indexOf(10);
				}
			},
		},
	});
	return { send: (value) => socket.write(`${JSON.stringify(value)}\n`), frames, wireLines, close: () => socket.end() };
}

async function waitFor(frames: any[], count: number): Promise<void> {
	for (let attempt = 0; attempt < 400 && frames.length < count; attempt++) await Bun.sleep(5);
	expect(frames.length).toBeGreaterThanOrEqual(count);
}

function bindWorkFixture(key: string, epoch: number, preferred?: string): string {
	const cacheKey = `${key}#${epoch}`;
	let sessionId = workSessionIds.get(cacheKey);
	if (!sessionId) {
		sessionId = preferred && epoch === 0 ? preferred : crypto.randomUUID();
		workSessionIds.set(cacheKey, sessionId);
	}
	return sessionId;
}

async function waitFrame(frames: any[], id: string): Promise<void> {
	for (let attempt = 0; attempt < 600 && !frames.some((frame) => frame.id === id); attempt++) await Bun.sleep(5);
	expect(frames.some((frame) => frame.id === id)).toBe(true);
}

async function startDeliveryServer(
	options: { ownerTarget?: GatewayConfig["ownerTarget"]; deliverySweepIntervalMs?: number } = {},
) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-delivery-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
		...(options.ownerTarget ? { ownerTarget: options.ownerTarget } : {}),
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "the single reply body",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		...(options.deliverySweepIntervalMs === undefined
			? {}
			: { deliverySweepIntervalMs: options.deliverySweepIntervalMs }),
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	return { client, config, database };
}

test("requires negotiation then serves status, shutdown, and validates chat params", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "mock reply",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		startedAt: "2026-01-01T00:00:00.000Z",
		onStop: () => database.close(),
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "request", id: "before", verb: "gateway.status" });
	await waitFor(client.frames, 1);
	expect(client.frames[0].error.code).toBe("negotiation_required");
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 2);
	expect(client.frames[1].type).toBe("negotiated");
	client.send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
	await waitFor(client.frames, 3);
	expect(client.frames[2].result.schemaVersion).toBe(23);
	expect(client.frames[2].result.startedAt).toBe("2026-01-01T00:00:00.000Z");
	expect(client.frames[2].result.contextDiff).toEqual({
		unread: 0,
		expired: 0,
		truncated: 0,
		omittedOldestAt: null,
		omittedNewestAt: null,
		floorAt: null,
	});
	client.send({
		v: "0.1",
		type: "request",
		id: "bad-chat",
		verb: "chat.send",
		params: { origin: { platform: "discord", kind: "channel", conversationId: "x" }, text: "hello" },
	});
	await waitFor(client.frames, 4);
	expect(client.frames[3].error.code).toBe("invalid_params");
	client.send({
		v: "0.1",
		type: "request",
		id: "chat",
		verb: "chat.send",
		params: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" }, text: "hello" },
	});
	await waitFor(client.frames, 6);
	expect(client.frames[4].result.turnId).toBeString();
	expect(client.frames[5].payload).toMatchObject({ text: "mock reply", final: true });
	// The turn also emits its final chat.progress; frames after the reply are
	// matched by identity, not position.
	client.send({ v: "0.1", type: "request", id: "shutdown", verb: "gateway.shutdown" });
	await waitFor(client.frames, 8);
	expect(client.frames.some((frame) => frame.event === "chat.progress" && frame.payload?.final === true)).toBe(true);
	expect(client.frames.find((frame) => frame.id === "shutdown")).toMatchObject({
		type: "response",
		result: { stopping: true },
	});
	expect(client.frames.some((frame) => frame.event === "gateway.stopping")).toBe(true);
	client.close();
});

test("a connected adapter receives a failed delivery again on the periodic sweep", async () => {
	const { client, database } = await startDeliveryServer({ deliverySweepIntervalMs: 100 });
	const origin = { platform: "discord", kind: "dm", conversationId: "delivery-test", peerId: "owner" };
	client.send({
		v: "0.1",
		type: "request",
		id: "create-delivery",
		verb: "chat.send",
		params: { origin, text: "trigger", engagement: { mentioned: false, group: false, authorId: "owner" } },
	});
	await waitFrame(client.frames, "create-delivery");
	for (let attempt = 0; attempt < 400 && !client.frames.some((frame) => frame.event === "chat.message"); attempt++)
		await Bun.sleep(5);
	const initial = client.frames.find(
		(frame) => frame.event === "chat.message" && frame.payload?.text === "the single reply body",
	);
	expect(initial).toBeDefined();
	const deliveryId = initial.payload.deliveryId as string;
	client.send({
		v: "0.1",
		type: "request",
		id: "first-fail",
		verb: "delivery.fail",
		params: { deliveryId, reason: "platform_rejected", ambiguous: false },
	});
	await waitFrame(client.frames, "first-fail");
	expect(database.deliveryRows().find((row) => row.delivery_id === deliveryId)).toMatchObject({
		state: "pending",
		attempts: 1,
	});
	const messagesBeforeRetry = client.frames.filter(
		(frame) => frame.event === "chat.message" && frame.payload?.deliveryId === deliveryId,
	).length;
	for (
		let attempt = 0;
		attempt < 1_000 &&
		client.frames.filter((frame) => frame.event === "chat.message" && frame.payload?.deliveryId === deliveryId)
			.length <= messagesBeforeRetry;
		attempt++
	)
		await Bun.sleep(5);
	const deliveryMessages = client.frames.filter(
		(frame) => frame.event === "chat.message" && frame.payload?.deliveryId === deliveryId,
	);
	expect(deliveryMessages.length).toBeGreaterThan(messagesBeforeRetry);
	const redelivery = deliveryMessages.at(-1);
	expect(redelivery?.payload).toMatchObject({ deliveryId, redelivered: true });
	expect(
		client.frames.filter((frame) => frame.event === "chat.message" && frame.payload?.deliveryId === deliveryId),
	).toHaveLength(2);
	client.close();
});

test("expiry logs and notifies the owner once, while status exposes metadata without payload", async () => {
	const logs: string[] = [];
	const originalError = console.error;
	console.error = (...values: unknown[]) => logs.push(values.map(String).join(" "));
	const ownerTarget = {
		origin: { platform: "discord" as const, kind: "dm" as const, conversationId: "owner", peerId: "owner" },
	};
	try {
		const { client, database } = await startDeliveryServer({ ownerTarget });
		const origin = { platform: "discord", kind: "dm", conversationId: "delivery-test", peerId: "owner" };
		client.send({
			v: "0.1",
			type: "request",
			id: "create-delivery",
			verb: "chat.send",
			params: { origin, text: "trigger", engagement: { mentioned: false, group: false, authorId: "owner" } },
		});
		await waitFrame(client.frames, "create-delivery");
		for (let attempt = 0; attempt < 400 && !client.frames.some((frame) => frame.event === "chat.message"); attempt++)
			await Bun.sleep(5);
		const reply = client.frames.find(
			(frame) => frame.event === "chat.message" && frame.payload?.text === "the single reply body",
		);
		expect(reply).toBeDefined();
		const deliveryId = reply.payload.deliveryId as string;
		for (let attempt = 1; attempt <= 5; attempt++) {
			const id = `fail-${attempt}`;
			client.send({
				v: "0.1",
				type: "request",
				id,
				verb: "delivery.fail",
				params: { deliveryId, reason: "adapter_failure", ambiguous: false },
			});
			await waitFrame(client.frames, id);
		}
		expect(database.deliveryRows().find((row) => row.delivery_id === deliveryId)).toMatchObject({
			state: "expired",
			attempts: 5,
		});
		for (
			let attempt = 0;
			attempt < 400 && !client.frames.some((frame) => frame.payload?.deliveryId?.startsWith("gw-x-"));
			attempt++
		)
			await Bun.sleep(5);
		const notice = client.frames.find(
			(frame) => frame.event === "chat.message" && frame.payload?.deliveryId?.startsWith("gw-x-"),
		);
		expect(notice?.payload).toMatchObject({ origin: ownerTarget.origin });
		expect(notice.payload.text).toContain(`gajaeway ops redeliver ${deliveryId}`);
		const logLine = logs.find((line) => line.startsWith(`delivery_expired deliveryId=${deliveryId}`));
		expect(logLine).toContain("origin=discord/dm/delivery-test/peer=owner attempts=5 reason=adapter_failure");
		expect(logLine).not.toContain("the single reply body");

		client.send({ v: "0.1", type: "request", id: "status-expired", verb: "gateway.status" });
		await waitFrame(client.frames, "status-expired");
		const status = client.frames.find((frame) => frame.id === "status-expired").result;
		expect(status.delivery).toMatchObject({
			expired: 1,
			recentExpired: [{ deliveryId, originKey: "discord/dm/delivery-test/peer=owner", attempts: 5 }],
		});
		expect(JSON.stringify(status.delivery)).not.toContain("the single reply body");

		const noticeId = notice.payload.deliveryId as string;
		for (let attempt = 1; attempt <= 5; attempt++) {
			const id = `fail-notice-${attempt}`;
			client.send({
				v: "0.1",
				type: "request",
				id,
				verb: "delivery.fail",
				params: { deliveryId: noticeId, reason: "owner_unavailable", ambiguous: false },
			});
			await waitFrame(client.frames, id);
		}
		expect(database.deliveryRows().find((row) => row.delivery_id === noticeId)?.state).toBe("expired");
		expect(
			client.frames.filter((frame) => frame.event === "chat.message" && frame.payload?.deliveryId?.startsWith("gw-x-")),
		).toHaveLength(1);
		expect(logs.some((line) => line.startsWith(`delivery_expired deliveryId=${noticeId}`))).toBe(true);
		client.close();
	} finally {
		console.error = originalError;
	}
});

test("ops.redeliver requeues expired rows by id or since and immediately publishes them", async () => {
	const { client, database } = await startDeliveryServer();
	const ledger = new DeliveryLedger(database);
	const origin = { platform: "loopback", kind: "loopback", conversationId: "loopback" } as const;
	const createExpired = (deliveryId: string) => {
		ledger.createPending({
			deliveryId,
			turnId: `turn-${deliveryId}`,
			originKey: originKey(origin),
			payloadJson: JSON.stringify({
				turnId: `turn-${deliveryId}`,
				origin,
				role: "assistant",
				text: `body for ${deliveryId}`,
				final: true,
				deliveryId,
			}),
		});
		for (let attempt = 0; attempt < 5; attempt++) ledger.fail(deliveryId);
	};
	createExpired("expired-by-id");
	createExpired("expired-by-since");
	client.send({ v: "0.1", type: "request", id: "status-before", verb: "gateway.status" });
	await waitFrame(client.frames, "status-before");
	const status = client.frames.find((frame) => frame.id === "status-before").result;
	expect(status.delivery.expired).toBe(2);
	expect(status.delivery.recentExpired.map((row: { deliveryId: string }) => row.deliveryId).sort()).toEqual([
		"expired-by-id",
		"expired-by-since",
	]);
	expect(
		status.delivery.recentExpired.every(
			(row: { originKey: string; attempts: number }) =>
				row.originKey === "loopback/loopback/loopback" && row.attempts === 5,
		),
	).toBe(true);
	expect(JSON.stringify(status.delivery)).not.toContain("body for");

	client.send({
		v: "0.1",
		type: "request",
		id: "requeue-id",
		verb: "ops.redeliver",
		params: { deliveryId: "expired-by-id" },
	});
	await waitFrame(client.frames, "requeue-id");
	expect(client.frames.find((frame) => frame.id === "requeue-id").result).toEqual({ requeued: ["expired-by-id"] });
	expect(
		client.frames.find((frame) => frame.event === "chat.message" && frame.payload?.deliveryId === "expired-by-id")
			?.payload,
	).toMatchObject({
		redelivered: true,
		duplicateWarning: true,
	});

	client.send({
		v: "0.1",
		type: "request",
		id: "requeue-since",
		verb: "ops.redeliver",
		params: { since: new Date(Date.now() - 60_000).toISOString() },
	});
	await waitFrame(client.frames, "requeue-since");
	expect(client.frames.find((frame) => frame.id === "requeue-since").result).toEqual({
		requeued: ["expired-by-since"],
	});

	client.send({
		v: "0.1",
		type: "request",
		id: "unknown-redelivery",
		verb: "ops.redeliver",
		params: { deliveryId: "missing" },
	});
	await waitFrame(client.frames, "unknown-redelivery");
	expect(client.frames.find((frame) => frame.id === "unknown-redelivery").error.code).toBe("invalid_params");
	client.send({ v: "0.1", type: "request", id: "bad-since", verb: "ops.redeliver", params: { since: "not-a-time" } });
	await waitFrame(client.frames, "bad-since");
	expect(client.frames.find((frame) => frame.id === "bad-since").error.code).toBe("invalid_params");
	client.close();
});

test("unauthorized direct messages cannot invoke /new or /model", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-command-auth-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "allowlist",
		mentionAllowlist: ["allowed"],
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "owner", peerId: "owner" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({ onBind: (input) => bindWorkFixture(input.originKey, input.epoch) });
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	const origin = { platform: "discord", kind: "dm", conversationId: "private", peerId: "intruder" };
	const engagement = { mentioned: false, group: false, authorId: "intruder" };
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "model",
		verb: "chat.send",
		params: { origin, text: "/model set forbidden", engagement },
	});
	client.send({
		v: "0.1",
		type: "request",
		id: "new",
		verb: "chat.send",
		params: { origin, text: "/new", engagement },
	});
	await waitFor(client.frames, 3);
	expect(client.frames.filter((frame) => frame.type === "response" && frame.result?.engaged === false)).toHaveLength(2);
	expect(sessionPort.binds).toEqual([]);
	expect(database.getSessionRecord("discord/dm/private/peer=intruder")).toBeUndefined();
	expect(database.conversationModelGet("discord/dm/private/peer=intruder")).toBeUndefined();
	client.close();
});

test("a mention-less /new inside an engaged thread is authorised", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-command-thread-follow-up-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		channels: { "slack:C1": { engagement: "mention-open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({ onBind: (input) => bindWorkFixture(input.originKey, input.epoch) });
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	const origin = {
		platform: "slack" as const,
		kind: "thread" as const,
		conversationId: "C1:1700000000.000100",
		parentId: "C1",
	};
	const originKey = "slack/thread/C1:1700000000.000100/parent=C1";
	database.inboundEnqueue({
		messageId: "thread-opening",
		originKey,
		originRefJson: JSON.stringify(origin),
		body: "@persona start",
	});
	database.inboundBindTurn({
		messageId: "thread-opening",
		originKey,
		epoch: 0,
		opRef: "thread-opening-op",
		sessionId: "thread-opening-session",
	});
	database.inboundTurnAccept("thread-opening-op");
	expect(database.inboundTurnComplete("thread-opening-op")).toBe(1);
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "thread-new",
		verb: "chat.send",
		params: {
			origin,
			text: "/new",
			engagement: { mentioned: false, group: true, authorId: "stranger" },
		},
	});
	await waitFrame(client.frames, "thread-new");
	expect(client.frames.find((frame) => frame.id === "thread-new")).toMatchObject({
		type: "response",
		result: { engaged: true },
	});
	client.close();
});

test("a mention-less /new at the channel root remains refused", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-command-channel-root-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		mentionAllowlist: ["owner"],
		channels: {
			"slack:C1": { engagement: "mention-open" },
			"slack:C2": { engagement: "closed" },
		},
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({ onBind: (input) => bindWorkFixture(input.originKey, input.epoch) });
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	const origin = { platform: "slack" as const, kind: "channel" as const, conversationId: "C1" };
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "channel-new",
		verb: "chat.send",
		params: {
			origin,
			text: "/new",
			engagement: { mentioned: false, group: true, authorId: "stranger" },
		},
	});
	await waitFrame(client.frames, "channel-new");
	expect(client.frames.find((frame) => frame.id === "channel-new")).toMatchObject({
		type: "response",
		result: { turnId: null, engaged: false },
	});
	expect(database.getSessionRecord("slack/channel/C1")).toBeUndefined();
	const closedOrigin = { platform: "slack" as const, kind: "channel" as const, conversationId: "C2" };
	client.send({
		v: "0.1",
		type: "request",
		id: "closed-channel-new",
		verb: "chat.send",
		params: {
			origin: closedOrigin,
			text: "/new",
			engagement: { mentioned: false, group: true, authorId: "stranger" },
		},
	});
	await waitFrame(client.frames, "closed-channel-new");
	expect(client.frames.find((frame) => frame.id === "closed-channel-new")).toMatchObject({
		type: "response",
		result: { turnId: null, engaged: false },
	});
	expect(database.getSessionRecord("slack/channel/C2")).toBeUndefined();
	client.close();
});

test("a failed platform turn still delivers a visible ledgered failure notice", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({
		onBind: (input) => bindWorkFixture(input.originKey, input.epoch),
		onSend: (input, scripted) => scripted.fail(input.opRef, "session operation stalled without terminal evidence"),
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "dm",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			messageId: "failed-message",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	await waitFor(client.frames, 3);
	expect(client.frames[1].result.engaged).toBe(true);
	const notice = client.frames.find((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(notice.payload.text).toStartWith("[turn failed]");
	expect(notice.payload.deliveryId).toBe(deterministicTerminalDeliveryId("discord/dm/c1/peer=p1", "failed-message", 0));
	expect(client.frames.find((frame) => frame.type === "error" && frame.id === "dm")).toBeUndefined();
	client.close();
});

test("long turns broadcast throttled chat.progress liveness events", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, _text, _preamble, onProgress) => {
			for (let call = 1; call <= 3; call++) {
				await Bun.sleep(5);
				onProgress?.({ toolCalls: call, outputTokens: call * 100 });
			}
			return "done";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		progress: { firstAfterMs: 0, intervalMs: 0 },
		onStop: () => database.close(),
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "dm",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	// Wait for the final reply so every progress frame the turn produced has arrived.
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.some((frame) => frame.type === "event" && frame.event === "chat.message")) break;
		await Bun.sleep(5);
	}
	const progress = client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.progress");
	expect(progress.length).toBeGreaterThanOrEqual(2);
	expect(progress[0].payload.origin.conversationId).toBe("c1");
	// The first frame may be a zero-state heartbeat; later frames carry stream data.
	const maxTools = Math.max(...progress.map((frame: any) => frame.payload.toolCalls));
	const maxTokens = Math.max(...progress.map((frame: any) => frame.payload.outputTokens));
	expect(maxTools).toBeGreaterThanOrEqual(1);
	expect(maxTokens).toBeGreaterThanOrEqual(100);
	expect(progress[0].payload.elapsedMs).toBeGreaterThanOrEqual(0);
	const reply = client.frames.find((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(reply.payload.text).toBe("done");
	client.close();
});

test("large memory.audit and concurrent progress remain independently parseable frames", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-frame-writer-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const root = memoryRoot(directory);
	await mkdir(root, { recursive: true });
	for (let index = 0; index < 2200; index++)
		await writeFile(
			join(root, `orphan-${index.toString().padStart(4, "0")}.md`),
			`orphan ${index} ${"x".repeat(80)}\n`,
		);
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, _text, _preamble, onProgress) => {
			onProgress?.({ toolCalls: 1, outputTokens: 100 });
			await Bun.sleep(5);
			return "done";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		progress: { firstAfterMs: 0, intervalMs: 0 },
		onStop: () => database.close(),
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({ v: "0.1", type: "request", id: "audit", verb: "memory.audit", params: {} });
	client.send({
		v: "0.1",
		type: "request",
		id: "progress-turn",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	for (let attempt = 0; attempt < 800; attempt++) {
		if (
			client.frames.some((frame) => frame.type === "response" && frame.id === "audit") &&
			client.frames.some((frame) => frame.type === "event" && frame.event === "chat.message")
		)
			break;
		await Bun.sleep(5);
	}
	const audit = client.frames.find((frame) => frame.type === "response" && frame.id === "audit");
	expect(audit).toBeDefined();
	const auditIndex = client.frames.indexOf(audit);
	const auditLine = client.wireLines[auditIndex];
	expect(Buffer.byteLength(`${auditLine}\n`, "utf8")).toBeGreaterThan(219_000);
	expect(JSON.parse(auditLine)).toEqual(audit);
	expect(
		client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.progress").length,
	).toBeGreaterThan(0);
	expect(client.frames.find((frame) => frame.type === "event" && frame.event === "chat.message")?.payload.text).toBe(
		"done",
	);
	expect(client.frames.filter((frame) => frame.type === "response" && frame.id === "audit")).toHaveLength(1);
	expect(client.wireLines).toHaveLength(client.frames.length);
	client.close();
});

test("shutdown quiesces an in-flight turn before final stopping frame", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-shutdown-writer-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	let entered!: () => void;
	let release!: () => void;
	const turnEntered = new Promise<void>((resolve) => (entered = resolve));
	const turnRelease = new Promise<void>((resolve) => (release = resolve));
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => {
			entered();
			await turnRelease;
			return "late reply";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "turn",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	await turnEntered;
	client.send({ v: "0.1", type: "request", id: "shutdown", verb: "gateway.shutdown" });
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.some((frame) => frame.type === "response" && frame.id === "shutdown")) break;
		await Bun.sleep(5);
	}
	expect(client.frames.find((frame) => frame.type === "response" && frame.id === "shutdown")).toBeDefined();
	release();
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping")) break;
		await Bun.sleep(5);
	}
	const replyIndex = client.frames.findIndex((frame) => frame.type === "event" && frame.event === "chat.message");
	const stoppingIndex = client.frames.findIndex(
		(frame) => frame.type === "event" && frame.event === "gateway.stopping",
	);
	expect(replyIndex).toBeGreaterThan(1);
	expect(stoppingIndex).toBeGreaterThan(replyIndex);
	expect(client.frames[replyIndex]?.payload.text).toBe("late reply");
	client.close();
});

test("unengaged messages before a mention arrive as the unread diff with speaker attribution; the mention is the trigger", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "mention-open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: Array<{ text: string; preamble: string }> = [];
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, text, preamble) => {
			turns.push({ text, preamble: preamble ?? "" });
			return "batched reply";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const origin = { platform: "discord", kind: "channel", conversationId: "c1" };
	const say = (id: string, text: string, authorId: string, authorName: string, mentioned: boolean) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: { origin, text, messageId: id, engagement: { mentioned, group: true, authorId, authorName } },
		});
	say("m1", "first message", "u1", "alice", false);
	await Bun.sleep(10);
	say("m2", "second message", "u2", "bob", false);
	await Bun.sleep(10);
	// m1/m2 are not engaged (no mention in a mention-only channel): they never
	// become turns of their own, only unread context for the one that does.
	say("m3", "@bot do the thing", "owner", "bellman", true);
	for (let attempt = 0; attempt < 400 && turns.length === 0; attempt++) await Bun.sleep(10);
	expect(turns).toHaveLength(1);
	const turn = turns[0]!;
	// The two earlier burst messages arrive as the unread diff, the newest as the trigger.
	expect(turn.text).toContain("Unread messages in this conversation");
	expect(turn.text).toContain("alice (author:u1, msg:m1): first message");
	expect(turn.text).toContain("bob (author:u2, msg:m2): second message");
	expect(turn.text).toContain("[bellman | discord channel c1 (author:owner, msg:m3)]");
	expect(turn.text).toContain("@bot do the thing");
	// Consumed context is not replayed on the next turn.
	say("m4", "follow-up", "owner", "bellman", true);
	for (let attempt = 0; attempt < 400 && turns.length < 2; attempt++) await Bun.sleep(10);
	expect(turns).toHaveLength(2);
	expect(turns[1]?.text).not.toContain("first message");
	expect(turns[1]?.text).toContain("follow-up");
	client.close();
});

test("a DM burst is never coalesced: the first fragment is the turn and the rest are steered into it, none lost", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	let release!: () => void;
	const running = new Promise<void>((resolve) => {
		release = resolve;
	});
	// sessionPortFromScript completes the op off the send path, so the turn is
	// genuinely running (send acknowledged, no terminal) while later fragments arrive.
	const sessionPort = sessionPortFromScript({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, text) => {
			turns.push(text);
			await running;
			return "ok";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const origin = { platform: "discord", kind: "dm", conversationId: "d1", peerId: "owner" };
	for (const [id, text] of [
		["f1", "fragment one"],
		["f2", "fragment two"],
		["f3", "fragment three"],
	] as const) {
		// No messageId: the sender never recorded these in the unread-context ledger.
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: { origin, text, engagement: { mentioned: false, group: false, authorId: "owner" } },
		});
		for (let attempt = 0; attempt < 400 && turns.length === 0; attempt++) await Bun.sleep(5);
	}
	for (let attempt = 0; attempt < 400 && sessionPort.steers.length < 2; attempt++) await Bun.sleep(5);
	try {
		expect(turns).toHaveLength(1);
		expect(turns[0]).toContain("fragment one");
		// Fragments can land in the same millisecond; steer order is arrival order.
		expect(sessionPort.steers.map((steer) => steer.text.split("\n").at(-1))).toEqual([
			"fragment two",
			"fragment three",
		]);
	} finally {
		release();
		client.close();
	}
});

test("an open-channel burst from several authors: the first is the turn, later ones are steered with their speaker labels, and the next turn does not replay them as unread", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	let release!: () => void;
	let running = new Promise<void>((resolve) => {
		release = resolve;
	});
	const sessionPort = sessionPortFromScript({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, text) => {
			turns.push(text);
			await running;
			return "ok";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const origin = { platform: "discord", kind: "channel", conversationId: "c1" };
	const say = (id: string, text: string, authorId: string, authorName: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: { origin, text, messageId: id, engagement: { mentioned: false, group: true, authorId, authorName } },
		});
	say("m1", "first message", "u1", "alice");
	for (let attempt = 0; attempt < 400 && turns.length === 0; attempt++) await Bun.sleep(5);
	say("m2", "second message", "u2", "bob");
	say("m3", "third message", "u3", "carol");
	for (let attempt = 0; attempt < 400 && sessionPort.steers.length < 2; attempt++) await Bun.sleep(5);
	expect(turns).toHaveLength(1);
	expect(turns[0]).toContain("[alice | discord channel c1 (author:u1, msg:m1)]");
	try {
		// Each steer names its speaker and message, like a trigger would.
		expect(sessionPort.steers[0]!.text).toContain("bob");
		expect(sessionPort.steers[0]!.text).toContain("msg:m2");
		expect(sessionPort.steers[0]!.text).toContain("second message");
		expect(sessionPort.steers[1]!.text).toContain("carol");
		expect(sessionPort.steers[1]!.text).toContain("msg:m3");
	} finally {
		release();
	}
	for (let attempt = 0; attempt < 400 && database.inboundPendingCount("discord/channel/c1") > 0; attempt++)
		await Bun.sleep(5);
	// The steered messages were read inside turn 1: the next turn must not get
	// them again as "unread".
	running = Promise.resolve();
	say("m4", "fourth message", "u1", "alice");
	for (let attempt = 0; attempt < 400 && turns.length < 2; attempt++) await Bun.sleep(5);
	expect(turns).toHaveLength(2);
	expect(turns[1]).not.toContain("second message");
	expect(turns[1]).not.toContain("third message");
	expect(turns[1]).toContain("fourth message");
	client.close();
});

test("an inbound ledger write failure fails the mention closed and logs a drop naming the message (#176)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { townhall: { engagement: "mention-open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, text) => {
			turns.push(text);
			return "ok";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	database.inboundEnqueue = () => {
		throw new Error("database is locked");
	};
	const origin = { platform: "discord", kind: "channel", conversationId: "townhall" };
	const logs: string[] = [];
	const original = console.error;
	console.error = (...args: unknown[]) => logs.push(args.map(String).join(" "));
	try {
		server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
		const client = await connect(config.socketPath);
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "mention",
			verb: "chat.send",
			params: {
				origin,
				messageId: "1546123517383286824",
				text: "<@bot> daily handoff",
				engagement: { mentioned: true, group: true, authorId: "quant-gajae" },
			},
		});
		await waitFrame(client.frames, "mention");
		// Fail closed: the adapter sees an error, so the message stays eligible for recovery.
		expect(client.frames.find((frame) => frame.id === "mention")?.type).toBe("error");
		await Bun.sleep(50);
		expect(turns).toEqual([]);
		// And never silent: the drop names the message, the origin, and the reason.
		expect(logs).toContain(
			"inbound_dropped message=1546123517383286824 origin=discord/channel/townhall engaged=true reason=ingest_error: database is locked",
		);
		client.close();
	} finally {
		console.error = original;
	}
});

test("a backlog left pending across an outage is answered on boot, never expired", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, text) => {
			turns.push(text);
			return "ok";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	const origin = { platform: "discord", kind: "dm", conversationId: "d-stale", peerId: "owner" };
	const key = "discord/dm/d-stale/peer=owner";
	const old = new Date(Date.now() - 30 * 60_000).toISOString();
	// Left behind by an outage: pending for 30 minutes before this boot. It was
	// never seen by the model, so it must reach the session now (live: four DMs
	// were deleted behind an 85-minute wedge by the old 10-minute floor).
	expect(
		database.inboundEnqueue({
			messageId: "stale-1",
			originKey: key,
			originRefJson: JSON.stringify(origin),
			body: "from before the outage",
			receivedAt: old,
		}),
	).toBe(true);
	const logs: string[] = [];
	const original = console.error;
	console.error = (...args: unknown[]) => logs.push(args.map(String).join(" "));
	try {
		server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
		const client = await connect(config.socketPath);
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "f",
			verb: "chat.send",
			params: {
				origin,
				text: "fresh after the outage",
				engagement: { mentioned: false, group: false, authorId: "owner" },
			},
		});
		for (let attempt = 0; attempt < 400 && turns.length < 2; attempt++) await Bun.sleep(10);
		expect(turns).toEqual([
			expect.stringContaining("from before the outage"),
			expect.stringContaining("fresh after the outage"),
		]);
		expect(logs.some((line) => line.includes("inbound_expired"))).toBe(false);
		expect(database.inboundPendingCount(key)).toBe(0);
		client.close();
	} finally {
		console.error = original;
	}
});

test("/restart is owner-only and triggers an ordered gateway stop after acknowledging", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "d-owner", peerId: "owner" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "unused",
	});
	const stops: string[] = [];
	const exits: number[] = [];
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		exitProcess: (code) => exits.push(code),
		onStop: () => {
			stops.push("stopped");
			database.close();
		},
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const origin = { platform: "discord", kind: "dm", conversationId: "d-owner", peerId: "owner" };
	// A non-owner is refused.
	client.send({
		v: "0.1",
		type: "request",
		id: "r1",
		verb: "chat.send",
		params: { origin, text: "/restart", engagement: { mentioned: false, group: false, authorId: "stranger" } },
	});
	await waitFor(client.frames, 2);
	expect(client.frames.find((f: any) => f.id === "r1")?.result).toEqual({ turnId: null, engaged: false });
	expect(stops).toEqual([]);
	// The owner gets an ack and the gateway stops shortly after.
	client.send({
		v: "0.1",
		type: "request",
		id: "r2",
		verb: "chat.send",
		params: { origin, text: "/restart", engagement: { mentioned: false, group: false, authorId: "owner" } },
	});
	await waitFor(client.frames, 3);
	expect(client.frames.find((f: any) => f.id === "r2")?.result.engaged).toBe(true);
	for (let attempt = 0; attempt < 400 && stops.length === 0; attempt++) await Bun.sleep(10);
	expect(stops).toEqual(["stopped"]);
	for (let attempt = 0; attempt < 100 && exits.length === 0; attempt++) await Bun.sleep(10);
	expect(exits).toEqual([75]);
	client.close();
	server = undefined;
});

test("group turns name the [SILENT] mechanism but never rule on whether the persona was addressed", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const preambles: string[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, _text, preamble) => {
			preambles.push(preamble ?? "");
			return "[SILENT]";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "listen",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "channel", conversationId: "c1" },
			text: "people chatting among themselves",
			messageId: "listen-1",
			engagement: { mentioned: false, group: true, authorId: "u1", authorName: "alice" },
		},
	});
	for (let attempt = 0; attempt < 400 && preambles.length === 0; attempt++) await Bun.sleep(5);
	// Speaking or not is the persona's call from its own rules/memory; the runtime
	// only tells it how to stay quiet (live: the old "NOT addressed" stamp silenced
	// the persona on people talking to it in an open room).
	expect(preambles[0]).not.toContain("NOT addressed");
	expect(preambles[0]).not.toContain("explicitly addressed");
	expect(preambles[0]).toContain("[SILENT]");
	// The silence-token reply suppresses delivery: no chat.message event arrives.
	await Bun.sleep(50);
	expect(client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.message")).toHaveLength(0);
	client.close();
});

test("[REPLY:id] parts thread to the referenced message and strip the directive", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "[REPLY:msg-42] threaded answer\n[BREAK]\nplain follow-up",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "dm",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			messageId: "m-1",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.message").length >= 2) break;
		await Bun.sleep(5);
	}
	const messages = client.frames.filter((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(messages).toHaveLength(2);
	expect(messages[0].payload).toMatchObject({ text: "threaded answer", replyToMessageId: "msg-42" });
	expect(messages[1].payload.text).toBe("plain follow-up");
	expect(messages[1].payload.replyToMessageId).toBeUndefined();
	client.close();
});

test("work.run runs a named worker session in the requested cwd and returns the text", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({
		onBind: (input) => bindWorkFixture(input.originKey, input.epoch),
		onSend: (input, scripted) => scripted.complete(input.opRef, "worker result"),
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "w",
		verb: "work.run",
		params: { name: "repo-fix", text: "fix the bug", cwd: "/tmp/some-repo" },
	});
	await waitFor(client.frames, 2);
	const response = client.frames.find((frame) => frame.type === "response" && frame.id === "w");
	expect(response.result).toMatchObject({ text: "worker result", sessionKey: "work/task/repo-fix", held: false });
	expect(sessionPort.binds[0]).toMatchObject({
		originKey: "work/task/repo-fix",
		repo: "/tmp/some-repo",
		codingRegister: true,
	});
	expect(sessionPort.sends[0]).toMatchObject({ text: "fix the bug", repo: "/tmp/some-repo", codingRegister: true });
	// Invalid names are rejected before touching the SessionPort.
	client.send({ v: "0.1", type: "request", id: "bad", verb: "work.run", params: { name: "../evil", text: "x" } });
	await waitFor(client.frames, 3);
	expect(client.frames.find((frame) => frame.type === "error" && frame.id === "bad")).toBeDefined();
	client.close();
});

test("work.run records a durable lane job and work.jobs projects it (issue #10)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: async (key, epoch) => ({ sessionId: bindWorkFixture(key, epoch, "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0") }),
		respond: async () => "worker result",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "w",
		verb: "work.run",
		params: { name: "Repo.Fix-2", text: "fix the bug" },
	});
	await waitFor(client.frames, 2);
	const run = client.frames.find((frame) => frame.type === "response" && frame.id === "w");
	expect(run.result).toMatchObject({ held: false, text: "worker result" });
	const jobId = run.result.jobId;
	expect(jobId).toBe(`lanejob-${Buffer.from("Repo.Fix-2", "utf8").toString("hex")}`);

	client.send({ v: "0.1", type: "request", id: "jobs", verb: "work.jobs" });
	await waitFor(client.frames, 3);
	const jobs = client.frames.find((frame) => frame.type === "response" && frame.id === "jobs");
	expect(jobs.result.jobs).toHaveLength(1);
	expect(jobs.result.jobs[0].job_id).toBe(jobId);
	expect(jobs.result.jobs[0].lane_key).toBe("work-Repo.Fix-2");
	// The completed ATTEMPT closed; the JOB stays continuable (attempt_ended),
	// never a terminal work-failure.
	expect(jobs.result.jobs[0].state).toBe("attempt_ended");
	// No commit was made, so the pre-existing HEAD must NOT appear as a
	// worker checkpoint: progress is measured against the creation baseline.
	expect(jobs.result.jobs[0].checkpoints).toBe(0);

	// The stored authority carries the closed attempt; the record is restart-safe.
	const raw = database.laneJobJson(jobId);
	expect(typeof raw).toBe("string");
	const parsed = parseLaneJobRecord(raw as string);
	expect(parsed.attempts).toHaveLength(1);
	expect(parsed.attempts[0].endState).toBe("completed");
	expect(parsed.attempts[0].sessionId).toBe("0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0");
	expect(parsed.lane.worktreePath).toBe(process.cwd());

	// Reopening a settled tombstone by editing history is corruption, not a
	// crash predecessor. Neither an ordinary request nor resume may repair it.
	database.putLaneJob({
		jobId: parsed.jobId,
		laneKey: "work-Repo.Fix-2",
		state: "running",
		createdAt: parsed.createdAt,
		updatedAt: new Date().toISOString(),
		lane: parsed.lane,
		json: JSON.stringify({ ...parsed, attempts: [{ ...parsed.attempts[0], endState: undefined, endedAt: undefined }] }),
	});
	const damaged = database.laneJobJson(jobId);
	for (const resume of [false, true]) {
		const id = `corrupt-${resume}`;
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "work.run",
			params: { name: "Repo.Fix-2", text: "again", resume },
		});
		await waitFrame(client.frames, id);
		expect(client.frames.find((frame) => frame.id === id)).toMatchObject({
			type: "error",
			error: { code: "verb_failed", detail: { reasonCode: "lane_state_corrupt", name: "Repo.Fix-2" } },
		});
	}
	expect(database.laneJobJson(jobId)).toBe(damaged);
	expect(sessionPort.sends).toHaveLength(1);
	client.close();
});

test("work.jobs projects each lane's last commit, accept time and current attempt (issue #67)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const repo = join(directory, "lane-repo");
	await mkdir(repo);
	const git = (...args: string[]) => {
		const result = Bun.spawnSync(["git", "-C", repo, ...args], {
			stdout: "pipe",
			stderr: "pipe",
			env: {
				...process.env,
				GIT_AUTHOR_NAME: "lane",
				GIT_AUTHOR_EMAIL: "lane@example.invalid",
				GIT_COMMITTER_NAME: "lane",
				GIT_COMMITTER_EMAIL: "lane@example.invalid",
				GIT_COMMITTER_DATE: "2026-09-01T00:00:00Z",
			},
		});
		expect(result.exitCode).toBe(0);
		return result.stdout.toString().trim();
	};
	git("init", "--quiet");
	git("commit", "--allow-empty", "--quiet", "-m", "baseline");
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	let committed = false;
	const sessionPort = sessionPortFromResponder({
		bind: async (key, epoch) => ({ sessionId: bindWorkFixture(key, epoch) }),
		respond: async () => {
			// The first worker commits; that HEAD move is the progress signal the row must show.
			if (!committed) git("commit", "--allow-empty", "--quiet", "-m", "lane: land the fix");
			committed = true;
			return "worker result";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "w",
		verb: "work.run",
		params: { name: "row", text: "fix it", cwd: repo },
	});
	await waitFrame(client.frames, "w");
	const run = client.frames.find((frame) => frame.id === "w");
	expect(run.type).toBe("response");
	// A second lane whose worktree has since been removed (Q5: manual cleanup).
	client.send({
		v: "0.1",
		type: "request",
		id: "gone",
		verb: "work.run",
		params: { name: "gone", text: "x", cwd: join(directory, "missing-worktree") },
	});
	await waitFrame(client.frames, "gone");

	client.send({ v: "0.1", type: "request", id: "jobs", verb: "work.jobs" });
	await waitFrame(client.frames, "jobs");
	const jobs = client.frames.find((frame) => frame.id === "jobs").result.jobs;
	const row = jobs.find((job: { lane_key: string }) => job.lane_key === "work-row");
	const record = parseLaneJobRecord(database.laneJobJson(row.job_id) as string);
	expect(row.accepted_at).toBe(record.createdAt);
	expect(row.attempt).toEqual({
		op_ref: run.result.opRef,
		started_at: record.attempts[0].startedAt,
		ended_at: record.attempts[0].endedAt,
	});
	expect(row.last_commit).toEqual({
		sha: git("rev-parse", "HEAD"),
		subject: "lane: land the fix",
		committed_at: "2026-09-01T00:00:00.000Z",
	});
	// No repository evidence is reported as absent, never invented.
	const gone = jobs.find((job: { lane_key: string }) => job.lane_key === "work-gone");
	expect(gone.last_commit).toBeNull();
	client.close();
});

test("a stalled durable job holds the next work.run until resume (production path)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	let turns = 0;
	const sessionPort = sessionPortFromResponder({
		bind: async (key, epoch) => ({ sessionId: bindWorkFixture(key, epoch, "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0") }),
		respond: async () => {
			turns += 1;
			return "worker result";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({ v: "0.1", type: "request", id: "w", verb: "work.run", params: { name: "stall-out", text: "x" } });
	await waitFor(client.frames, 2);
	expect(client.frames.find((frame) => frame.type === "response" && frame.id === "w")).toBeDefined();
	// Simulate the reconciliation outcome after repeated stalled continuations.
	const first = parseLaneJobRecord(
		database.laneJobJson(`lanejob-${Buffer.from("stall-out", "utf8").toString("hex")}`) as string,
	);
	database.putLaneJob({
		jobId: first.jobId,
		laneKey: `work-stall-out`,
		state: "stalled",
		createdAt: first.createdAt,
		updatedAt: new Date().toISOString(),
		lane: first.lane,
		json: JSON.stringify({ ...first, state: "stalled", stalledContinuations: MAX_STALLED_CONTINUATIONS }),
	});
	client.send({ v: "0.1", type: "request", id: "w2", verb: "work.run", params: { name: "stall-out", text: "x" } });
	async function waitId2(id: string): Promise<void> {
		for (let attempt = 0; attempt < 400 && !client.frames.some((f) => f.id === id && f.type !== undefined); attempt++)
			await Bun.sleep(5);
	}
	await waitId2("w2");
	const held = client.frames.find((frame) => frame.type === "response" && frame.id === "w2");
	expect(held.result).toMatchObject({ held: true, state: "stalled" });
	// The turn must NOT have run while held.
	expect(turns).toBe(1);
	// resume:true is the explicit operator acknowledgement that proceeds.
	client.send({
		v: "0.1",
		type: "request",
		id: "w3",
		verb: "work.run",
		params: { name: "stall-out", text: "x", resume: true },
	});
	await waitId2("w3");
	expect(
		client.frames.find((frame) => frame.type === "response" && frame.id === "w3" && frame.result?.held === false),
	).toBeDefined();
	expect(turns).toBe(2);
	client.close();
});
test("resuming a stalled job clears the hold durably: the next ordinary call is not held", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	let turns = 0;
	const sessionPort = sessionPortFromResponder({
		bind: async (key, epoch) => ({ sessionId: bindWorkFixture(key, epoch, "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0") }),
		respond: async () => {
			turns += 1;
			return "worker result";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const jobId = `lanejob-${Buffer.from("stall-clear", "utf8").toString("hex")}`;

	// First run succeeds (creates the job).
	client.send({ v: "0.1", type: "request", id: "w1", verb: "work.run", params: { name: "stall-clear", text: "x" } });
	async function waitId3(id: string): Promise<void> {
		for (let attempt = 0; attempt < 400 && !client.frames.some((f) => f.id === id && f.type !== undefined); attempt++)
			await Bun.sleep(5);
	}
	await waitId3("w1");

	// Force the stalled hold, as reconciliation would after repeat stalls.
	const first = parseLaneJobRecord(database.laneJobJson(jobId) as string);
	database.putLaneJob({
		jobId: first.jobId,
		laneKey: "work-stall-clear",
		state: "stalled",
		createdAt: first.createdAt,
		updatedAt: new Date().toISOString(),
		lane: first.lane,
		json: JSON.stringify({ ...first, state: "stalled", stalledContinuations: MAX_STALLED_CONTINUATIONS }),
	});

	// Ordinary call: held.
	client.send({ v: "0.1", type: "request", id: "w2", verb: "work.run", params: { name: "stall-clear", text: "x" } });
	await waitId3("w2");
	expect(client.frames.find((frame) => frame.id === "w2" && frame.type === "response")?.result).toMatchObject({
		held: true,
		state: "stalled",
	});
	expect(turns).toBe(1);

	// resume:true: runs AND durably clears the hold with an audit entry.
	client.send({
		v: "0.1",
		type: "request",
		id: "w3",
		verb: "work.run",
		params: { name: "stall-clear", text: "x", resume: true },
	});
	await waitId3("w3");
	expect(client.frames.find((frame) => frame.id === "w3" && frame.type === "response")?.result).toMatchObject({
		held: false,
		text: "worker result",
	});
	const afterResume = parseLaneJobRecord(database.laneJobJson(jobId) as string);
	expect(afterResume.stalledContinuations).toBe(0);
	expect(afterResume.escalations.some((entry) => entry.includes("operator resume acknowledged"))).toBe(true);

	// The NEXT ordinary call is not held by the old stalled state.
	client.send({ v: "0.1", type: "request", id: "w4", verb: "work.run", params: { name: "stall-clear", text: "x" } });
	await waitId3("w4");
	expect(client.frames.find((frame) => frame.id === "w4" && frame.type === "response")?.result).toMatchObject({
		held: false,
	});
	expect(turns).toBe(3);
	client.close();
});

test("work.run forwards a model preset to bind and send and rejects invalid models", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({
		onBind: (input) => bindWorkFixture(input.originKey, input.epoch),
		onSend: (input, scripted) => scripted.complete(input.opRef, "model result"),
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "model",
		verb: "work.run",
		params: { name: "model", text: "work", cwd: directory, model: { preset: "muse-gpt" } },
	});
	await waitFor(client.frames, 2);
	expect(client.frames.find((frame) => frame.id === "model")).toMatchObject({
		type: "response",
		result: { text: "model result", held: false },
	});
	expect(sessionPort.binds[0].model).toEqual({ preset: "muse-gpt" });
	expect(sessionPort.sends[0].model).toBeUndefined();
	for (const [index, model] of [{ preset: "" }, 42].entries()) {
		const id = `invalid-model-${index}`;
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "work.run",
			params: { name: "model", text: "work", model },
		});
		await waitFor(client.frames, 3 + index);
		expect(client.frames.find((frame) => frame.id === id)).toMatchObject({
			type: "error",
			error: { code: "invalid_params" },
		});
	}
	expect(sessionPort.binds).toHaveLength(1);
	expect(sessionPort.sends).toHaveLength(1);
	client.close();
});

test("work capacity rejects new lanes, permits reuse, and work.retire frees the slot with jobs activity visible", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		work: { maxLanes: 1 },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({
		onBind: (input) => bindWorkFixture(input.originKey, input.epoch),
		onSend: (input, scripted) => scripted.complete(input.opRef, "worker result"),
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "first",
		verb: "work.run",
		params: { name: "a", text: "work", cwd: directory },
	});
	await waitFor(client.frames, 2);
	const first = client.frames.find((frame) => frame.id === "first");
	expect(first).toMatchObject({ type: "response", result: { held: false, sessionKey: "work/task/a" } });
	const sessionId = database.getSessionRecord("work/task/a")!.sessionId;
	client.send({
		v: "0.1",
		type: "request",
		id: "full",
		verb: "work.run",
		params: { name: "b", text: "work", cwd: directory },
	});
	await waitFor(client.frames, 3);
	expect(client.frames.find((frame) => frame.id === "full")).toMatchObject({
		type: "error",
		error: { code: "lane_capacity", detail: { active: 1, maxLanes: 1, candidates: [{ name: "a" }] } },
	});
	expect(sessionPort.binds).toHaveLength(1);
	expect(sessionPort.sends).toHaveLength(1);
	client.send({
		v: "0.1",
		type: "request",
		id: "reuse",
		verb: "work.run",
		params: { name: "a", text: "again", cwd: directory },
	});
	await waitFor(client.frames, 4);
	expect(client.frames.find((frame) => frame.id === "reuse")).toMatchObject({
		type: "response",
		result: { held: false, sessionKey: "work/task/a" },
	});
	expect(sessionPort.sends).toHaveLength(2);
	expect(sessionPort.sends[1].sessionId).toBe(sessionId);
	client.send({ v: "0.1", type: "request", id: "jobs-active", verb: "work.jobs" });
	await waitFor(client.frames, 5);
	const jobs = client.frames.find((frame) => frame.id === "jobs-active");
	expect(jobs.result.jobs).toHaveLength(1);
	expect(Number.isFinite(Date.parse(jobs.result.jobs[0].last_activity_at))).toBe(true);
	expect(jobs.result.jobs[0]).toMatchObject({
		session_id: sessionId,
		last_activity_at: expect.any(String),
	});
	client.send({ v: "0.1", type: "request", id: "retire", verb: "work.retire", params: { name: "a" } });
	await waitFor(client.frames, 6);
	expect(client.frames.find((frame) => frame.id === "retire")).toMatchObject({
		type: "response",
		result: { retired: true, sessionKey: "work/task/a", sessionId, closed: true },
	});
	expect(sessionPort.closes).toEqual([{ sessionId, repo: directory }]);
	expect(database.getSessionRecord("work/task/a")).toEqual({ sessionId: "", epoch: 1 });
	client.send({
		v: "0.1",
		type: "request",
		id: "freed",
		verb: "work.run",
		params: { name: "b", text: "work", cwd: directory },
	});
	await waitFor(client.frames, 7);
	expect(client.frames.find((frame) => frame.id === "freed")).toMatchObject({
		type: "response",
		result: { held: false, sessionKey: "work/task/b", text: "worker result" },
	});
	expect(sessionPort.sends).toHaveLength(3);
	client.close();
});

test("responses larger than one socket buffer arrive intact (backpressure outbox)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	// A ~700KB reply forces multiple kernel-buffer writes on the unix socket.
	const bigReply = `big:${"x".repeat(700_000)}:end`;
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => bigReply,
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "big",
		verb: "chat.send",
		params: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" }, text: "go" },
	});
	for (let attempt = 0; attempt < 400; attempt++) {
		if (client.frames.some((frame) => frame.type === "event" && frame.event === "chat.message")) break;
		await Bun.sleep(10);
	}
	const message = client.frames.find((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(message).toBeDefined();
	expect(message.payload.text).toBe(bigReply);
	client.close();
});

test("every turn preamble carries the attachment-scope rule", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const preambles: string[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_s, _t, preamble) => {
			preambles.push(preamble ?? "");
			return "ok";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({
		v: "0.1",
		type: "request",
		id: "a",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "d-att", peerId: "owner" },
			text: "hi",
			engagement: { mentioned: false, group: false, authorId: "owner" },
		},
	});
	for (let attempt = 0; attempt < 400 && preambles.length === 0; attempt++) await Bun.sleep(10);
	expect(preambles[0]).toContain("If the current message lists none, it has none");
	client.close();
});

test("a fresh session's first turn carries recent conversation history, a later turn does not", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_s, text) => {
			turns.push(text);
			return "ok";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	const key = "discord/dm/d-hist/peer=owner";
	for (let i = 1; i <= 3; i++)
		database.contextRecord({
			messageId: `old-${i}`,
			originKey: key,
			authorId: "owner",
			authorName: "bellman",
			body: `earlier message ${i}`,
			receivedAt: new Date(Date.now() - 60_000 * (4 - i)).toISOString(),
		});
	database.contextRecord({
		messageId: "old-img",
		originKey: key,
		authorId: "owner",
		authorName: "bellman",
		body: "[image · shot.png · 20.1 KB · https://cdn.discordapp.com/attachments/1/2/shot.png]",
		receivedAt: new Date(Date.now() - 30_000).toISOString(),
	});
	database.contextCommitWindow(key, ["old-1", "old-2", "old-3", "old-img"], 0);
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const origin = { platform: "discord", kind: "dm", conversationId: "d-hist", peerId: "owner" };
	const say = (id: string, text: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin,
				text,
				messageId: id,
				engagement: { mentioned: false, group: false, authorId: "owner", authorName: "bellman" },
			},
		});
	say("n1", "new question");
	for (let attempt = 0; attempt < 400 && turns.length === 0; attempt++) await Bun.sleep(10);
	expect(turns[0]).toContain("[Recent conversation history");
	expect(turns[0]).toContain("earlier message 2");
	// A replayed attachment keeps its label but never its url: the model must not
	// re-fetch a days-old screenshot and treat it as the current message.
	expect(turns[0]).toContain("[image · shot.png · 20.1 KB · past attachment; not part of this message, do not fetch]");
	expect(turns[0]).not.toContain("cdn.discordapp.com");
	say("n2", "follow-up");
	for (let attempt = 0; attempt < 400 && turns.length < 2; attempt++) await Bun.sleep(10);
	expect(turns[1]).not.toContain("[Recent conversation history");
	client.close();
});

test("control tokens never leak: a silence token inside a preamble silences, and [REPLY:id] mid-text is stripped", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "open" } },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const replies = [
		"Pure geopolitics chat, not addressed to me, nothing to add.\n\n[SILENT]",
		"This is a real bug report.\n\n[REPLY:1544704223634260038] 알겠고 인정",
	];
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => replies.shift() ?? "[SILENT]",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const origin = { platform: "discord", kind: "channel", conversationId: "c1" };
	const say = (id: string, text: string) =>
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "chat.send",
			params: {
				origin,
				text,
				messageId: id,
				engagement: { mentioned: true, group: true, authorId: "owner", authorName: "bellman" },
			},
		});
	say("m1", "first");
	await Bun.sleep(400);
	say("m2", "second");
	await Bun.sleep(600);
	const messages = client.frames
		.filter((f: any) => f.type === "event" && f.event === "chat.message" && f.payload?.text)
		.map((f: any) => f.payload);
	expect(messages.map((m: any) => m.text)).toEqual(["This is a real bug report. 알겠고 인정"]);
	expect(messages[0].replyToMessageId).toBe("1544704223634260038");
});
async function workSocketFixture() {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-async-socket-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		work: { maxLanes: 2 },
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const port = new ScriptedSessionPort({ onBind: (input) => bindWorkFixture(input.originKey, input.epoch) });
	attachTestBrokerOwnership(database, port, join(directory, "agent"));
	let openAtShutdown = -1;
	server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		onStop: () => {
			openAtShutdown = database.workAttemptOpen().length;
			database.close();
		},
	});
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	return {
		config,
		database,
		port,
		client,
		get openAtShutdown() {
			return openAtShutdown;
		},
	};
}

test("socket work.start accepts before terminal, status/steer stay available, one completion notice", async () => {
	const f = await workSocketFixture();
	const target = { platform: "discord", kind: "channel", conversationId: "results" };
	f.client.send({
		v: "0.1",
		type: "request",
		id: "start",
		verb: "work.start",
		params: { name: "a", text: "work", cwd: directory, notify: target },
	});
	await waitFrame(f.client.frames, "start");
	const receipt = f.client.frames.find((frame) => frame.id === "start").result;
	expect(receipt.started).toBe(true);
	expect(f.database.workAttemptGet(receipt.opRef)?.settledAt).toBeNull();
	f.client.send({ v: "0.1", type: "request", id: "status", verb: "work.status", params: { name: "a" } });
	f.client.send({
		v: "0.1",
		type: "request",
		id: "steer",
		verb: "work.steer",
		params: { name: "a", text: "correction" },
	});
	f.client.send({
		v: "0.1",
		type: "request",
		id: "overlap",
		verb: "work.start",
		params: { name: "a", text: "again", cwd: directory, resume: true },
	});
	await waitFrame(f.client.frames, "status");
	await waitFrame(f.client.frames, "steer");
	await waitFrame(f.client.frames, "overlap");
	expect(f.client.frames.find((frame) => frame.id === "status").result.op.status).toBe("in_flight");
	expect(f.client.frames.find((frame) => frame.id === "steer").result.clientRef).toBe(f.port.steers[0]?.clientRef);
	expect(f.client.frames.find((frame) => frame.id === "overlap").error.detail.reasonCode).toBe("attempt_open");
	f.port.complete(receipt.opRef, "socket result");
	for (let i = 0; i < 600 && !f.client.frames.some((frame) => frame.event === "chat.message"); i++) await Bun.sleep(5);
	const notices = f.client.frames.filter((frame) => frame.event === "chat.message");
	expect(notices).toHaveLength(1);
	expect(notices[0].payload).toMatchObject({
		turnId: receipt.opRef,
		origin: target,
		text: "[lane a] completed: socket result",
	});
	expect(f.port.sends).toHaveLength(1);
	f.client.close();
});

test("one socket disconnect leaves its work observable while another run waits independently", async () => {
	const f = await workSocketFixture();
	const second = await connect(f.config.socketPath);
	second.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(second.frames, 1);
	f.client.send({
		v: "0.1",
		type: "request",
		id: "run-a",
		verb: "work.run",
		params: { name: "a", text: "work", cwd: directory },
	});
	second.send({
		v: "0.1",
		type: "request",
		id: "run-b",
		verb: "work.run",
		params: { name: "b", text: "work", cwd: directory },
	});
	for (let i = 0; i < 600 && f.port.sends.length < 2; i++) await Bun.sleep(5);
	expect(f.port.sends).toHaveLength(2);
	f.client.close();
	second.send({ v: "0.1", type: "request", id: "observe-a", verb: "work.status", params: { name: "a" } });
	await waitFrame(second.frames, "observe-a");
	expect(second.frames.find((frame) => frame.id === "observe-a").result.attempt.endedAt).toBeUndefined();
	expect(second.frames.some((frame) => frame.id === "run-b")).toBe(false);
	for (const send of f.port.sends) f.port.complete(send.opRef, "continued");
	await waitFrame(second.frames, "run-b");
	expect(second.frames.find((frame) => frame.id === "run-b").result.text).toBe("continued");
	for (let i = 0; i < 600 && f.database.workAttemptOpen().length; i++) await Bun.sleep(5);
	expect(f.database.workAttemptOpen()).toHaveLength(0);
	expect(second.frames.filter((frame) => frame.event === "chat.message")).toHaveLength(0);
	second.close();
});

test("Unix stop detaches a long run before request drain and leaves the attempt durable", async () => {
	const f = await workSocketFixture();
	f.client.send({
		v: "0.1",
		type: "request",
		id: "run",
		verb: "work.run",
		params: { name: "a", text: "long work", cwd: directory },
	});
	for (let i = 0; i < 600 && f.port.sends.length === 0; i++) await Bun.sleep(5);
	expect(f.port.sends).toHaveLength(1);
	let stopped = false;
	const stop = server!.stop().then(() => {
		stopped = true;
	});
	for (let i = 0; i < 600 && !stopped; i++) await Bun.sleep(5);
	expect(stopped).toBe(true);
	await stop;
	expect(f.openAtShutdown).toBe(1);
	expect(f.client.frames.find((frame) => frame.id === "run")).toMatchObject({
		type: "error",
		error: { code: "gateway_shutting_down" },
	});
	f.client.close();
});

test("stdio stop detaches work.run before runtime request drain in an actual child process", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-work-stdio-"));
	const code = `
		import { join } from "node:path";
		import { startStdioServer } from "./packages/gateway/src/server/server.ts";
		import { GatewayDatabase } from "./packages/gateway/src/store/db.ts";
		import { attachTestBrokerOwnership, ScriptedSessionPort } from "./packages/gateway/test/session-port.fake.ts";
		const home = process.env.WORK_TEST_HOME;
		const config = { schemaVersion: 1, home, configPath: join(home, "config.json"), socketPath: join(home, "gateway.sock"), dbPath: join(home, "gateway.db"), logVerbosity: "info" };
		const database = await GatewayDatabase.open(config.dbPath);
		const sessionIds = new Map();
		const port = new ScriptedSessionPort({ onBind: (input) => {
			const key = input.originKey + "#" + input.epoch;
			let sessionId = sessionIds.get(key);
			if (!sessionId) {
				sessionId = crypto.randomUUID();
				sessionIds.set(key, sessionId);
			}
			return sessionId;
		} });
		attachTestBrokerOwnership(database, port, join(home, "agent"));
		startStdioServer({ config, database, sessionPort: port, onStop: () => { database.close(); process.exit(0); } });
	`;
	const child = Bun.spawn([process.execPath, "-e", code], {
		cwd: process.cwd(),
		env: { ...process.env, WORK_TEST_HOME: directory },
		stdin: "pipe",
		stdout: "pipe",
		stderr: "pipe",
	});
	const frames: any[] = [];
	let buffer = "";
	const reading = (async () => {
		for await (const chunk of child.stdout) {
			buffer += Buffer.from(chunk).toString();
			let newline = buffer.indexOf("\n");
			while (newline >= 0) {
				const line = buffer.slice(0, newline);
				buffer = buffer.slice(newline + 1);
				if (line) frames.push(JSON.parse(line));
				newline = buffer.indexOf("\n");
			}
		}
	})();
	const stderr = new Response(child.stderr).text();
	const send = (frame: unknown) => {
		child.stdin.write(`${JSON.stringify(frame)}\n`);
		child.stdin.flush();
	};
	try {
		send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(frames, 1);
		send({
			v: "0.1",
			type: "request",
			id: "run",
			verb: "work.run",
			params: { name: "a", text: "long work", cwd: directory },
		});
		let observable = false;
		for (let i = 0; i < 100 && !observable; i++) {
			const id = `status-${i}`;
			send({ v: "0.1", type: "request", id, verb: "work.status", params: { name: "a" } });
			await waitFrame(frames, id);
			observable = frames.find((frame) => frame.id === id)?.result?.op?.status === "in_flight";
			if (!observable) await Bun.sleep(5);
		}
		expect(observable).toBe(true);
		send({ v: "0.1", type: "request", id: "shutdown", verb: "gateway.shutdown" });
		let exitCode: number | undefined;
		void child.exited.then((code) => {
			exitCode = code;
		});
		for (let i = 0; i < 600 && exitCode === undefined; i++) await Bun.sleep(5);
		expect(exitCode).toBe(0);
		await reading;
		await stderr;
		expect(frames.find((frame) => frame.id === "run")).toMatchObject({
			type: "error",
			error: { code: "gateway_shutting_down" },
		});
		const reopened = await GatewayDatabase.open(join(directory, "gateway.db"));
		try {
			expect(reopened.workAttemptOpen()).toHaveLength(1);
			expect(
				parseLaneJobRecord(reopened.laneJobJson(reopened.workAttemptOpen()[0]!.jobId)!).attempts[0]?.endedAt,
			).toBeUndefined();
		} finally {
			reopened.close();
		}
	} finally {
		child.kill();
		await child.exited;
		await reading;
		await stderr;
	}
});
for (const verb of ["work.start", "work.run"] as const) {
	test(`session.list preserves fresh ${verb} worker identity before and after settlement`, async () => {
		const f = await workSocketFixture();
		const name = "identity-worker";
		f.client.send({ v: "0.1", type: "request", id: "worker", verb, params: { name, text: "work", cwd: directory } });
		for (let i = 0; i < 600 && f.port.sends.length === 0; i++) await Bun.sleep(5);
		expect(f.port.sends).toHaveLength(1);
		const opRef = f.port.sends[0]!.opRef;
		const expectedOrigin = { platform: "work", kind: "task", conversationId: name };
		for (const phase of ["open", "settled"] as const) {
			if (phase === "settled") {
				f.port.complete(opRef, "identity retained");
				for (let i = 0; i < 600 && f.database.workAttemptGet(opRef)?.settledAt === null; i++) await Bun.sleep(5);
				expect(f.database.workAttemptGet(opRef)?.settledAt).not.toBeNull();
				await waitFrame(f.client.frames, "worker");
			} else expect(f.database.workAttemptGet(opRef)?.settledAt).toBeNull();
			const id = `sessions-${phase}`;
			f.client.send({ v: "0.1", type: "request", id, verb: "session.list" });
			await waitFrame(f.client.frames, id);
			const sessions = f.client.frames.find((frame) => frame.id === id).result.sessions;
			expect(sessions).toHaveLength(1);
			expect(sessions[0].origin).toEqual(expectedOrigin);
			expect(sessions[0].lastActivityAt).toBeString();
		}
		f.client.close();
	});
}

test("status reports each connected client's generation and flags an adapter older than the gateway", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-generation-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "mock reply",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		startedAt: "2026-01-02T00:00:00.000Z",
		onStop: () => database.close(),
	});

	// An adapter that survived a gateway-only restart: its process predates the
	// gateway process even though the reconnect itself is fresh.
	const stale = await connect(config.socketPath);
	stale.send({
		v: "0.1",
		type: "hello",
		payload: {
			supportedVersions: ["0.1"],
			clientInfo: { name: "adapter-slack", startedAt: "2026-01-01T00:00:00.000Z" },
		},
	});
	await waitFor(stale.frames, 1);

	const fresh = await connect(config.socketPath);
	fresh.send({
		v: "0.1",
		type: "hello",
		payload: {
			supportedVersions: ["0.1"],
			clientInfo: { name: "adapter-discord", startedAt: "2026-01-02T00:00:30.000Z" },
		},
	});
	await waitFor(fresh.frames, 1);

	// A client that reports no start time is a diagnostic gap, never "stale".
	const anonymous = await connect(config.socketPath);
	anonymous.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(anonymous.frames, 1);

	fresh.send({ v: "0.1", type: "request", id: "status", verb: "gateway.status" });
	await waitFor(fresh.frames, 2);
	const clients = fresh.frames[1].result.clients as Array<{
		name: string;
		startedAt?: string;
		connectedAt: string;
		staleGeneration: boolean;
	}>;
	expect(clients).toHaveLength(3);
	expect(clients.find((client) => client.name === "adapter-slack")?.staleGeneration).toBe(true);
	expect(clients.find((client) => client.name === "adapter-discord")?.staleGeneration).toBe(false);
	const unidentified = clients.find((client) => client.name === "unidentified");
	expect(unidentified?.startedAt).toBeUndefined();
	expect(unidentified?.staleGeneration).toBe(false);
	for (const client of clients) expect(typeof client.connectedAt).toBe("string");

	stale.close();
	fresh.close();
	anonymous.close();
});
