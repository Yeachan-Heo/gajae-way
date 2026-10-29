import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type GatewayReadinessResult,
	type MemoryOperationStatusResult,
	originKey,
	WORK_JOBS_DEFAULT_LIMIT,
	WORK_JOBS_MAX_LIMIT,
} from "@gajae-gateway/protocol";
import { createLaneJobRecord, MAX_STALLED_CONTINUATIONS, parseLaneJobRecord } from "@gajae-gateway/subsession";
import type { GatewayConfig } from "../src/config";
import { ACK_TIMEOUT_MS, DeliveryService } from "../src/delivery/delivery";
import { initializeMemory, memoryGit, memoryRoot } from "../src/memory/doctrine";
import { MonitorRegistry } from "../src/monitors/registry";
import { ModelNotSelectedError } from "../src/orchestrator/session-port";
import { deterministicTerminalDeliveryId } from "../src/orchestrator/tail-runner";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
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

async function connect(socketPath: string): Promise<{
	send(value: unknown): void;
	frames: any[];
	wireLines: string[];
	waitForFrame(id: string): Promise<void>;
	close(): void;
}> {
	const frames: any[] = [];
	const wireLines: string[] = [];
	const frameWaiters = new Map<string, Array<() => void>>();
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
						const frame = JSON.parse(line);
						frames.push(frame);
						if (typeof frame.id === "string") {
							const waiters = frameWaiters.get(frame.id);
							if (waiters) {
								frameWaiters.delete(frame.id);
								for (const resolve of waiters) resolve();
							}
						}
					}
					newline = buffered.indexOf(10);
				}
			},
		},
	});
	const waitForFrame = (id: string): Promise<void> => {
		if (frames.some((frame) => frame.id === id)) return Promise.resolve();
		return new Promise<void>((resolve) => {
			const waiters = frameWaiters.get(id) ?? [];
			waiters.push(resolve);
			frameWaiters.set(id, waiters);
		});
	};
	return {
		send: (value) => socket.write(`${JSON.stringify(value)}\n`),
		frames,
		wireLines,
		waitForFrame,
		close: () => socket.end(),
	};
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
	options: {
		ownerTarget?: GatewayConfig["ownerTarget"];
		deliverySweepIntervalMs?: number;
		bind?: Parameters<typeof sessionPortFromResponder>[0]["bind"];
		attachTail?: ReturnType<typeof sessionPortFromResponder>["attachTail"];
		respond?: Parameters<typeof sessionPortFromResponder>[0]["respond"];
		send?: ReturnType<typeof sessionPortFromResponder>["send"];
		serviceTier?: GatewayConfig["serviceTier"];
		setServiceTier?: ReturnType<typeof sessionPortFromResponder>["setServiceTier"];
	} = {},
) {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-delivery-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
		...(options.ownerTarget ? { ownerTarget: options.ownerTarget } : {}),
		...(options.serviceTier ? { serviceTier: options.serviceTier } : {}),
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: options.bind ?? ((key, epoch) => bindWorkFixture(key, epoch)),
		respond: options.respond ?? (async () => "the single reply body"),
	});
	if (options.attachTail) sessionPort.attachTail = options.attachTail;
	if (options.send) sessionPort.send = options.send;
	if (options.setServiceTier) sessionPort.setServiceTier = options.setServiceTier;
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
	expect(client.frames[2].result.schemaVersion).toBe(39);
	expect(client.frames[2].result.startedAt).toBe("2026-01-01T00:00:00.000Z");
	expect(client.frames[2].result.readiness).toMatchObject({ ready: true, state: "ready", reason: null, failure: null });
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
	expect(database.outbox.rows().find((row) => row.delivery_id === deliveryId)).toMatchObject({
		state: "pending",
		attempts: 1,
		last_error: "other",
	});
	client.send({ v: "0.1", type: "request", id: "status-retry", verb: "gateway.status" });
	await waitFrame(client.frames, "status-retry");
	const retrying = client.frames.find((frame) => frame.id === "status-retry").result.delivery.recentPending;
	expect(retrying).toMatchObject([{ deliveryId, state: "pending", attempts: 1, lastError: "other" }]);
	expect(Date.parse(retrying[0].nextRetryAt)).toBeGreaterThan(Date.now() - 2_000);
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

test("status readiness degrades on sustained delivery failure and recovers after a confirmed delivery", async () => {
	const { client, database } = await startDeliveryServer();
	const origin = { platform: "discord", kind: "dm", conversationId: "readiness-test", peerId: "owner" } as const;
	const delivery = new DeliveryService(database);
	for (let index = 0; index < 3; index++) {
		const deliveryId = `readiness-failed-${index}`;
		expect(delivery.prepare(`turn-${index}`, origin, "reply", undefined, deliveryId)).toBeDefined();
		client.send({
			v: "0.1",
			type: "request",
			id: `fail-${index}`,
			verb: "delivery.fail",
			params: { deliveryId, reason: "upstream rejected token=secret-value", ambiguous: false },
		});
		await waitFrame(client.frames, `fail-${index}`);
	}
	client.send({ v: "0.1", type: "request", id: "degraded-status", verb: "gateway.status" });
	await waitFrame(client.frames, "degraded-status");
	const degraded = client.frames.find((frame) => frame.id === "degraded-status").result.readiness;
	expect(degraded).toMatchObject({ ready: false, state: "degraded", failureThreshold: 3 });
	expect(degraded.reason).toContain("delivery: other");
	expect(degraded.failure).toMatchObject({ category: "delivery", code: "other", consecutiveFailures: 3 });
	expect(JSON.stringify(degraded)).not.toContain("secret-value");

	expect(delivery.prepare("recovered-turn", origin, "reply", undefined, "readiness-recovered")).toBeDefined();
	client.send({
		v: "0.1",
		type: "request",
		id: "confirm-recovered",
		verb: "delivery.confirm",
		params: { deliveryId: "readiness-recovered" },
	});
	await waitFrame(client.frames, "confirm-recovered");
	client.send({ v: "0.1", type: "request", id: "recovered-status", verb: "gateway.status" });
	await waitFrame(client.frames, "recovered-status");
	expect(client.frames.find((frame) => frame.id === "recovered-status").result.readiness).toMatchObject({
		ready: true,
		state: "ready",
		reason: null,
		failure: null,
	});
	client.close();
});

test("session-create readiness details redact absolute paths", async () => {
	const { client } = await startDeliveryServer({
		bind: () => {
			throw new Error("broker bind failed at /private/gajaeway/workspace");
		},
	});
	for (let index = 0; index < 3; index++) {
		const conversationId = `readiness-bind-${index}`;
		client.send({
			v: "0.1",
			type: "request",
			id: `bind-failure-${index}`,
			verb: "chat.send",
			params: { origin: { platform: "loopback", kind: "loopback", conversationId }, text: "hello" },
		});
		await waitFrame(client.frames, `bind-failure-${index}`);
	}
	client.send({ v: "0.1", type: "request", id: "bind-failure-status", verb: "gateway.status" });
	await waitFrame(client.frames, "bind-failure-status");
	const readiness = client.frames.find((frame) => frame.id === "bind-failure-status").result.readiness;
	expect(readiness).toMatchObject({
		ready: false,
		failure: { category: "session_create", code: "persona_bind_failed", consecutiveFailures: 3 },
	});
	expect(readiness.failure.detail).toContain("[path]");
	expect(readiness.failure.detail).not.toContain("/private/gajaeway/workspace");
	client.close();
});

test("create-then-disown attachment failures degrade session-create readiness", async () => {
	let attachmentAttempts = 0;
	const { client } = await startDeliveryServer({
		attachTail: async () => {
			attachmentAttempts += 1;
			throw new Error("session_unavailable: broker disowned new session");
		},
	});
	client.send({
		v: "0.1",
		type: "request",
		id: "create-then-disown",
		verb: "chat.send",
		params: {
			origin: { platform: "loopback", kind: "loopback", conversationId: "readiness-disowned-session" },
			text: "hello",
		},
	});
	await waitFrame(client.frames, "create-then-disown");
	for (let attempt = 0; attempt < 400 && attachmentAttempts < 3; attempt++) await Bun.sleep(5);
	expect(attachmentAttempts).toBeGreaterThanOrEqual(3);
	client.send({ v: "0.1", type: "request", id: "disowned-session-status", verb: "gateway.status" });
	await waitFrame(client.frames, "disowned-session-status");
	const readiness = client.frames.find((frame) => frame.id === "disowned-session-status").result.readiness;
	expect(readiness).toMatchObject({
		ready: false,
		failure: { category: "session_create", code: "session_attach_failed", consecutiveFailures: 3 },
	});
	client.close();
});

test("generic tail attachment failures degrade session-create readiness without rebinding", async () => {
	let attachmentAttempts = 0;
	const { client } = await startDeliveryServer({
		attachTail: async () => {
			attachmentAttempts += 1;
			throw new Error("protocol_error: broker tail timeout");
		},
	});
	for (let index = 0; index < 3; index++) {
		const conversationId = `readiness-generic-attach-${index}`;
		client.send({
			v: "0.1",
			type: "request",
			id: `generic-attach-${index}`,
			verb: "chat.send",
			params: { origin: { platform: "loopback", kind: "loopback", conversationId }, text: "hello" },
		});
		await waitFrame(client.frames, `generic-attach-${index}`);
		for (let attempt = 0; attempt < 400 && attachmentAttempts < index + 1; attempt++) await Bun.sleep(5);
	}
	expect(attachmentAttempts).toBe(3);
	client.send({ v: "0.1", type: "request", id: "generic-attach-status", verb: "gateway.status" });
	await waitFrame(client.frames, "generic-attach-status");
	const readiness = client.frames.find((frame) => frame.id === "generic-attach-status").result.readiness;
	expect(readiness).toMatchObject({
		ready: false,
		failure: { category: "session_create", code: "session_attach_failed", consecutiveFailures: 3 },
	});
	client.close();
});

test("post-send disown ambiguity retains the original operation without degrading session-create readiness", async () => {
	let port: ScriptedSessionPort | undefined;
	let unknown = true;
	let finish!: () => void;
	const terminal = new Promise<void>((resolve) => {
		finish = resolve;
	});
	const conversationId = "readiness-send-disowned-session";
	const key = `loopback/loopback/${conversationId}`;
	const { client, database } = await startDeliveryServer({
		respond: async () => {
			await terminal;
			return "original native operation recovered";
		},
		send: async function (this: ScriptedSessionPort, input) {
			port = this;
			const status = this.status.bind(this);
			this.status = async (query) => {
				const report = await status(query);
				return unknown ? { operationRef: query.opRef, status: { status: "unknown" }, summaryCompleted: false } : report;
			};
			this.liveness = async () => ({ live: undefined, disowned: true });
			// The native prompt was admitted, but its acknowledgement was lost.
			await ScriptedSessionPort.prototype.send.call(this, input);
			throw new Error("session_unavailable: broker disowned new session");
		},
	});
	try {
		client.send({
			v: "0.1",
			type: "request",
			id: "send-disowned-session",
			verb: "chat.send",
			params: { origin: { platform: "loopback", kind: "loopback", conversationId }, text: "hello" },
		});
		await waitFrame(client.frames, "send-disowned-session");
		for (let attempt = 0; attempt < 400 && !port?.statusReads.length; attempt++) await Bun.sleep(5);
		expect(port?.statusReads.length).toBeGreaterThan(0);
		const original = port!.sends[0]!;
		expect(database.inbox.turnRow(original.opRef)).toMatchObject({
			turn_state: "bound",
			turn_op_ref: original.opRef,
			bound_session_id: original.sessionId,
		});
		for (let index = 0; index < 3; index++) {
			const id = `send-disowned-status-${index}`;
			client.send({ v: "0.1", type: "request", id, verb: "gateway.status" });
			await waitFrame(client.frames, id);
			expect(client.frames.find((frame) => frame.id === id).result.readiness).toMatchObject({
				ready: true,
				state: "ready",
				failure: null,
			});
		}
		expect(port!.sends).toHaveLength(1);
		expect(port!.sendAttempts).toHaveLength(1);
		expect(port!.binds).toHaveLength(1);
		expect(database.bindings.getSessionRecord(key)).toEqual({ sessionId: original.sessionId, epoch: 0 });

		unknown = false;
		finish();
		const replies = () =>
			client.frames.filter(
				(frame) => frame.event === "chat.message" && frame.payload?.text === "original native operation recovered",
			);
		for (
			let attempt = 0;
			attempt < 400 && (database.inbox.turnRow(original.opRef)?.turn_state !== "done" || replies().length === 0);
			attempt++
		)
			await Bun.sleep(5);
		expect(database.inbox.turnRow(original.opRef)?.turn_state).toBe("done");
		expect(replies()).toHaveLength(1);
		expect(port!.sends).toHaveLength(1);
		expect(port!.sendAttempts).toHaveLength(1);
		expect(port!.binds).toHaveLength(1);
		expect(database.bindings.getSessionRecord(key)).toEqual({ sessionId: original.sessionId, epoch: 0 });
	} finally {
		// Always provide the real native terminal, including when an assertion fails,
		// so server shutdown never drains an intentionally unresolved operation.
		unknown = false;
		finish();
		client.close();
	}
});

test("turn readiness details redact broker URLs", async () => {
	const { client } = await startDeliveryServer({
		respond: async () => {
			throw new Error("turn failed via https://broker.example/private/session");
		},
	});
	for (let index = 0; index < 3; index++) {
		const conversationId = `readiness-turn-${index}`;
		client.send({
			v: "0.1",
			type: "request",
			id: `turn-failure-${index}`,
			verb: "chat.send",
			params: { origin: { platform: "loopback", kind: "loopback", conversationId }, text: "hello" },
		});
		await waitFrame(client.frames, `turn-failure-${index}`);
		for (
			let attempt = 0;
			attempt < 400 &&
			!client.frames.some(
				(frame) => frame.event === "chat.message" && frame.payload?.origin?.conversationId === conversationId,
			);
			attempt++
		)
			await Bun.sleep(5);
		expect(
			client.frames.some(
				(frame) => frame.event === "chat.message" && frame.payload?.origin?.conversationId === conversationId,
			),
		).toBe(true);
	}
	client.send({ v: "0.1", type: "request", id: "turn-failure-status", verb: "gateway.status" });
	await waitFrame(client.frames, "turn-failure-status");
	const readiness = client.frames.find((frame) => frame.id === "turn-failure-status").result.readiness;
	expect(readiness).toMatchObject({
		ready: false,
		failure: { category: "turn", code: "turn_failed", consecutiveFailures: 3 },
	});
	expect(readiness.failure.detail).toContain("[url]");
	expect(readiness.failure.detail).not.toContain("broker.example");
	client.close();
});

test("pre-send service-tier configuration failures degrade turn readiness", async () => {
	const { client } = await startDeliveryServer({
		serviceTier: "priority",
		setServiceTier: async () => {
			throw new Error("service-tier configuration failed at /private/service-tier/config");
		},
	});
	for (let index = 0; index < 3; index++) {
		const conversationId = `readiness-pre-send-${index}`;
		client.send({
			v: "0.1",
			type: "request",
			id: `pre-send-failure-${index}`,
			verb: "chat.send",
			params: { origin: { platform: "loopback", kind: "loopback", conversationId }, text: "hello" },
		});
		await waitFrame(client.frames, `pre-send-failure-${index}`);
	}
	// chat.send acknowledges admission, not completion of asynchronous preflight.
	let readiness: GatewayReadinessResult | undefined;
	for (let attempt = 0; attempt < 400; attempt++) {
		const id = `pre-send-status-${attempt}`;
		client.send({ v: "0.1", type: "request", id, verb: "gateway.status" });
		await waitFrame(client.frames, id);
		readiness = client.frames.find((frame) => frame.id === id).result.readiness;
		if (readiness?.failure?.consecutiveFailures === 3) break;
		await Bun.sleep(5);
	}
	expect(readiness).toMatchObject({
		ready: false,
		failure: { category: "turn", code: "pre_send_configuration_failed", consecutiveFailures: 3 },
	});
	expect(readiness?.failure?.detail).toContain("[path]");
	expect(readiness?.failure?.detail).not.toContain("/private/service-tier/config");
	client.close();
});

test("confirming a valid note does not clear contract-unmet failures in its monitor batch", async () => {
	const { client, database } = await startDeliveryServer();
	const ledger = new DeliveryService(database);
	const origin = { platform: "loopback", kind: "loopback", conversationId: "loopback" } as const;
	const monitor = new MonitorRegistry(database).add({
		name: "partial-readiness-monitor",
		trigger: { kind: "cron", schedule: "0 */2 * * *" },
		eventTypes: ["readiness.partial"],
		burstPolicy: "serialize",
		enabled: false,
	});
	const createAuthoredEvent = (eventId: string, batchId: string, output: string) => {
		database.transaction((tx) => {
			database.monitors.eventCreate(tx, {
				eventId,
				monitorId: monitor.monitorId,
				eventType: "readiness.partial",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			database.monitors.eventUpdate(tx, eventId, "authored", batchId);
			database.monitors.authoredOutputCreate(tx, eventId, output);
		});
	};
	for (let index = 0; index < 3; index++) {
		const batchId = `failed-readiness-batch-${index}`;
		const eventId = `failed-readiness-event-${index}`;
		const deliveryId = `failed-readiness-delivery-${index}`;
		createAuthoredEvent(eventId, batchId, "failed report");
		expect(ledger.prepare(batchId, origin, "failed report", undefined, deliveryId)).toBeDefined();
		client.send({
			v: "0.1",
			type: "request",
			id: `failed-readiness-${index}`,
			verb: "delivery.fail",
			params: { deliveryId, reason: "discord 503", ambiguous: false },
		});
		await waitFrame(client.frames, `failed-readiness-${index}`);
	}

	const batchId = "partially-failed-readiness-batch";
	const echoEventId = "partially-failed-echo-event";
	const validEventId = "partially-failed-valid-event";
	const deliveryId = "partially-failed-readiness-delivery";
	createAuthoredEvent(echoEventId, batchId, "instruction echo");
	database.transaction((tx) => {
		database.monitors.eventUpdate(tx, echoEventId, "contract_unmet", batchId);
		database.monitors.failureRecord(tx, echoEventId, "authored_echo_suspected", "note echoed monitor contract");
	});
	createAuthoredEvent(validEventId, batchId, "verified update");
	expect(ledger.prepare(batchId, origin, "verified update", undefined, deliveryId)).toBeDefined();
	client.send({
		v: "0.1",
		type: "request",
		id: "partial-batch-confirm",
		verb: "delivery.confirm",
		params: { deliveryId },
	});
	await waitFrame(client.frames, "partial-batch-confirm");
	client.send({ v: "0.1", type: "request", id: "partial-batch-status", verb: "gateway.status" });
	await waitFrame(client.frames, "partial-batch-status");
	expect(client.frames.find((frame) => frame.id === "partial-batch-status").result.readiness).toMatchObject({
		ready: false,
		failure: { category: "monitor_delivery", consecutiveFailures: 3 },
	});
	client.close();
});

test("delivery sweep expirations degrade delivery readiness, including monitor batches", async () => {
	const { client, config, database } = await startDeliveryServer({ deliverySweepIntervalMs: 20 });
	const ledger = new DeliveryService(database);
	const origin = { platform: "loopback", kind: "loopback", conversationId: "loopback" } as const;
	const staleAt = new Date(Date.now() - 25 * 60 * 60_000).toISOString();
	const setStale = (deliveryId: string) => {
		const raw = new Database(config.dbPath);
		try {
			raw.query("UPDATE deliveries SET created_at = ? WHERE delivery_id = ?").run(staleAt, deliveryId);
		} finally {
			raw.close();
		}
	};
	const expire = async (deliveryId: string) => {
		for (let attempt = 0; attempt < 500 && ledger.get(deliveryId)?.state !== "expired"; attempt++) await Bun.sleep(5);
		expect(ledger.get(deliveryId)?.state).toBe("expired");
	};
	const confirm = async (deliveryId: string) => {
		expect(ledger.prepare(`recovery-${deliveryId}`, origin, "reply", undefined, deliveryId)).toBeDefined();
		client.send({
			v: "0.1",
			type: "request",
			id: `confirm-${deliveryId}`,
			verb: "delivery.confirm",
			params: { deliveryId },
		});
		await waitFrame(client.frames, `confirm-${deliveryId}`);
	};

	for (let index = 0; index < 3; index++) {
		const deliveryId = `readiness-expired-${index}`;
		expect(
			ledger.createPending({
				deliveryId,
				turnId: `ordinary-${index}`,
				originKey: originKey(origin),
				payloadJson: "{}",
			}),
		).toBe(true);
		setStale(deliveryId);
	}
	await Promise.all([0, 1, 2].map((index) => expire(`readiness-expired-${index}`)));
	client.send({ v: "0.1", type: "request", id: "delivery-expiry-status", verb: "gateway.status" });
	await waitFrame(client.frames, "delivery-expiry-status");
	expect(client.frames.find((frame) => frame.id === "delivery-expiry-status").result.readiness).toMatchObject({
		ready: false,
		failure: { category: "delivery", code: "delivery_expired", consecutiveFailures: 3 },
	});
	await confirm("clear-expired-delivery-streak");

	const monitor = new MonitorRegistry(database).add({
		name: "readiness-expiry-monitor",
		trigger: { kind: "cron", schedule: "0 */2 * * *" },
		eventTypes: ["readiness.expiry"],
		burstPolicy: "serialize",
		enabled: false,
	});
	for (let index = 0; index < 3; index++) {
		const batchId = `monitor-batch-${index}`;
		const eventId = `monitor-event-${index}`;
		const deliveryId = `monitor-readiness-expired-${index}`;
		database.transaction((tx) => {
			database.monitors.eventCreate(tx, {
				eventId,
				monitorId: monitor.monitorId,
				eventType: "readiness.expiry",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			database.monitors.eventUpdate(tx, eventId, "authored", batchId);
			database.monitors.authoredOutputCreate(tx, eventId, "monitor report");
		});
		expect(
			ledger.createPending({
				deliveryId,
				turnId: batchId,
				originKey: originKey(origin),
				payloadJson: JSON.stringify({
					turnId: batchId,
					origin,
					role: "assistant",
					text: "monitor report",
					final: true,
					deliveryId,
				}),
			}),
		).toBe(true);
		setStale(deliveryId);
		await expire(deliveryId);
		await confirm(`clear-monitor-delivery-${index}`);
	}
	client.send({ v: "0.1", type: "request", id: "monitor-expiry-status", verb: "gateway.status" });
	await waitFrame(client.frames, "monitor-expiry-status");
	expect(client.frames.find((frame) => frame.id === "monitor-expiry-status").result.readiness).toMatchObject({
		ready: false,
		failure: { category: "monitor_delivery", code: "delivery_expired", consecutiveFailures: 3 },
	});
	client.close();
});

test("inflight rows are not re-broadcast by the timed sweep within ack timeout", async () => {
	const { client, database } = await startDeliveryServer({ deliverySweepIntervalMs: 50 });
	const origin = { platform: "discord", kind: "dm", conversationId: "inflight-test", peerId: "owner" };
	client.send({
		v: "0.1",
		type: "request",
		id: "create-inflight",
		verb: "chat.send",
		params: { origin, text: "test inflight", engagement: { mentioned: false, group: false, authorId: "owner" } },
	});
	await waitFrame(client.frames, "create-inflight");
	// Wait for the delivery to arrive
	for (let attempt = 0; attempt < 400 && !client.frames.some((frame) => frame.event === "chat.message"); attempt++)
		await Bun.sleep(5);
	const initial = client.frames.find(
		(frame) => frame.event === "chat.message" && frame.payload?.text === "the single reply body",
	);
	expect(initial).toBeDefined();
	const deliveryId = initial.payload.deliveryId as string;

	// Manually mark the delivery as inflight (simulating an in-flight state)
	const ledger = new DeliveryService(database);
	ledger.markInflight(deliveryId);
	const row = ledger.get(deliveryId);
	const updatedAt = Date.parse(row!.updatedAt);

	// Count chat.message frames with this deliveryId before the sweep
	const messagesBeforeSweep = client.frames.filter(
		(frame) => frame.event === "chat.message" && frame.payload?.deliveryId === deliveryId,
	).length;

	// Wait for multiple sweep cycles (timed sweep runs every 50ms)
	await Bun.sleep(250);

	// Verify no duplicate was broadcast during the ack timeout window
	const messagesAfterWait = client.frames.filter(
		(frame) => frame.event === "chat.message" && frame.payload?.deliveryId === deliveryId,
	).length;
	expect(messagesAfterWait).toBe(messagesBeforeSweep);

	// Manually advance time to after the ack timeout in the ledger
	const nowAfterTimeout = updatedAt + ACK_TIMEOUT_MS + 100;
	const undelivered = ledger.listUndelivered(24 * 60 * 60_000, nowAfterTimeout, false);

	// After ack timeout, the inflight row should be returned
	expect(undelivered).toHaveLength(1);
	expect(undelivered[0]).toMatchObject({ deliveryId, state: "inflight" });

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
		expect(database.outbox.rows().find((row) => row.delivery_id === deliveryId)).toMatchObject({
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
		expect(database.outbox.rows().find((row) => row.delivery_id === noticeId)?.state).toBe("expired");
		expect(
			client.frames.filter((frame) => frame.event === "chat.message" && frame.payload?.deliveryId?.startsWith("gw-x-")),
		).toHaveLength(1);
		expect(logs.some((line) => line.startsWith(`delivery_expired deliveryId=${noticeId}`))).toBe(true);
		client.close();
	} finally {
		console.error = originalError;
	}
});

test("an expired monitor delivery fails its authored events with evidence; a confirmed redrive delivers them (#94)", async () => {
	const { client, database } = await startDeliveryServer();
	const monitor = new MonitorRegistry(database).add({
		name: "watch",
		trigger: { kind: "cron", schedule: "0 */2 * * *" },
		eventTypes: ["memory.canonicalize"],
		burstPolicy: "dedupe",
		enabled: false,
	});
	const batchId = crypto.randomUUID();
	const eventId = crypto.randomUUID();
	database.transaction((tx) => {
		database.monitors.eventCreate(tx, {
			eventId,
			monitorId: monitor.monitorId,
			eventType: "memory.canonicalize",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});
		database.monitors.eventUpdate(tx, eventId, "authored", batchId);
		database.monitors.authoredOutputCreate(tx, eventId, "report");
	});
	const origin = { platform: "loopback", kind: "loopback", conversationId: "loopback" } as const;
	const deliveryId = "monitor-delivery";
	new DeliveryService(database).createPending({
		deliveryId,
		turnId: batchId,
		originKey: originKey(origin),
		payloadJson: JSON.stringify({
			turnId: batchId,
			origin,
			role: "assistant",
			text: "report",
			final: true,
			deliveryId,
		}),
	});
	for (let attempt = 1; attempt <= 5; attempt++) {
		const id = `monitor-fail-${attempt}`;
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "delivery.fail",
			params: { deliveryId, reason: "discord 503", ambiguous: false },
		});
		await waitFrame(client.frames, id);
	}
	const eventStage = () => database.monitors.eventRows().find((row) => row.event_id === eventId)?.stage;
	expect(eventStage()).toBe("failed_no_retry");
	expect(database.monitors.failure(eventId)).toMatchObject({
		code: "delivery_expired",
		detail: `delivery ${deliveryId} expired after 5 attempts: discord_503`,
	});
	expect(database.monitors.authoredOutput(eventId)).toBe("report");

	client.send({ v: "0.1", type: "request", id: "redrive", verb: "ops.redeliver", params: { deliveryId } });
	await waitFrame(client.frames, "redrive");
	client.send({ v: "0.1", type: "request", id: "confirm", verb: "delivery.confirm", params: { deliveryId } });
	await waitFrame(client.frames, "confirm");
	expect(eventStage()).toBe("delivered");
	client.close();
});

test("the sweep expires a stale unsettled row and alerts once even with no adapter connected", async () => {
	const logs: string[] = [];
	const originalError = console.error;
	console.error = (...values: unknown[]) => logs.push(values.map(String).join(" "));
	try {
		directory = await mkdtemp(join(tmpdir(), "gajaeway-delivery-ttl-"));
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: directory,
			gjc: { agentDir: join(directory, "agent") },
			configPath: join(directory, "config.json"),
			socketPath: join(directory, "gateway.sock"),
			dbPath: join(directory, "gateway.db"),
			logVerbosity: "info",
			dmPolicy: "open",
		};
		const database = await Store.open(config.dbPath);
		const sessionPort = sessionPortFromResponder({
			bind: (key, epoch) => bindWorkFixture(key, epoch),
			respond: async () => "unused",
		});
		attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
		const ledger = new DeliveryService(database);
		ledger.createPending({ deliveryId: "orphan", turnId: "turn", originKey: "discord/channel/c", payloadJson: "{}" });
		ledger.fail("orphan", false, "socket hang up");
		ledger.fail("orphan", false, "socket hang up");
		const raw = new Database(config.dbPath);
		raw
			.query("UPDATE deliveries SET created_at = ? WHERE delivery_id = 'orphan'")
			.run(new Date(Date.now() - 25 * 60 * 60_000).toISOString());
		raw.close();
		server = await startUnixServer({
			config,
			database,
			sessionPort,
			onStop: () => database.close(),
			deliverySweepIntervalMs: 20,
		});
		for (let attempt = 0; attempt < 400 && ledger.get("orphan")?.state !== "expired"; attempt++) await Bun.sleep(5);
		expect(ledger.get("orphan")).toMatchObject({
			state: "expired",
			attempts: 2,
			lastError: "network",
			nextRetryAt: null,
		});
		await Bun.sleep(100);
		expect(logs.filter((line) => line.startsWith("delivery_expired deliveryId=orphan"))).toEqual([
			"delivery_expired deliveryId=orphan origin=discord/channel/c attempts=2 reason=age",
		]);
	} finally {
		console.error = originalError;
	}
});

test("ops.redeliver requeues expired rows by id or since and immediately publishes them", async () => {
	const { client, database } = await startDeliveryServer();
	const ledger = new DeliveryService(database);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "allowlist",
		mentionAllowlist: ["allowed"],
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "owner", peerId: "owner" } },
	};
	const database = await Store.open(config.dbPath);
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
	expect(database.bindings.getSessionRecord("discord/dm/private/peer=intruder")).toBeUndefined();
	expect(database.bindings.modelGet("discord/dm/private/peer=intruder")).toBeUndefined();
	client.close();
});

test("a mention-less /new inside an engaged thread is authorised", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-command-thread-follow-up-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		channels: { "slack:C1": { engagement: "mention-open" } },
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = new ScriptedSessionPort({ onBind: (input) => bindWorkFixture(input.originKey, input.epoch) });
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	const origin = {
		platform: "slack" as const,
		kind: "thread" as const,
		conversationId: "C1:1700000000.000100",
		parentId: "C1",
	};
	const originKey = "slack/thread/C1:1700000000.000100/parent=C1";
	database.transaction((tx) =>
		database.inbox.enqueue(tx, {
			messageId: "thread-opening",
			originKey,
			originRefJson: JSON.stringify(origin),
			body: "@persona start",
		}),
	);
	database.transaction((tx) =>
		database.inbox.bindTurn(tx, {
			messageId: "thread-opening",
			originKey,
			epoch: 0,
			opRef: "thread-opening-op",
			sessionId: "thread-opening-session",
		}),
	);
	database.transaction((tx) => database.inbox.turnAccept(tx, "thread-opening-op"));
	expect(database.transaction((tx) => database.inbox.turnComplete(tx, "thread-opening-op"))).toBe(1);
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
		gjc: { agentDir: join(directory, "agent") },
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
	const database = await Store.open(config.dbPath);
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
	expect(database.bindings.getSessionRecord("slack/channel/C1")).toBeUndefined();
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
	expect(database.bindings.getSessionRecord("slack/channel/C2")).toBeUndefined();
	client.close();
});

test("a failed platform turn still delivers a visible ledgered failure notice", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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

test("model refusals notify once without consuming the pending message's eventual answer", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-model-refusal-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await Store.open(config.dbPath);
	const port = new ScriptedSessionPort({
		onBind: (input) => bindWorkFixture(input.originKey, input.epoch),
		onSend: (input, scripted) => scripted.complete(input.opRef, "the original message is answered"),
	});
	const send = port.send.bind(port);
	let attempts = 0;
	port.send = async (input) => {
		attempts++;
		if (attempts <= 2) throw new ModelNotSelectedError(input.opRef);
		return send(input);
	};
	attachTestBrokerOwnership(database, port, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	const origin = { platform: "discord", kind: "dm", conversationId: "model-refusal", peerId: "p1" };
	const key = "discord/dm/model-refusal/peer=p1";
	const engagement = { mentioned: false, group: false, authorId: "p1" };
	const until = async (condition: () => boolean) => {
		for (let attempt = 0; attempt < 600 && !condition(); attempt++) await Bun.sleep(5);
		expect(condition()).toBe(true);
	};
	const notices = () =>
		client.frames.filter(
			(frame) => frame.event === "chat.message" && frame.payload.text.startsWith("[turn failed] model_not_selected:"),
		);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "original",
			verb: "chat.send",
			params: { origin, engagement, messageId: "refused-original", text: "please answer this" },
		});
		await until(() => notices().length === 1 && database.inbox.pendingOldest(key) !== undefined);
		const pending = database.inbox.pendingOldest(key);
		expect(pending).toMatchObject({
			message_id: "refused-original",
			state: "pending",
			turn_state: null,
			turn_op_ref: null,
			bound_session_id: null,
			dispatched_at: null,
			terminal_delivery_id: null,
		});
		expect(notices()[0].payload.text).toEndWith("Send /model <id> or /new");
		expect(notices()[0].payload.deliveryId).not.toBe(deterministicTerminalDeliveryId(key, "refused-original", 0));
		const sessionId = database.bindings.getSessionRecord(key)?.sessionId;
		if (!sessionId) throw new Error("refused message lost its session binding");

		// Successful control alone does not prove the next prompt will be admitted.
		// A second explicit refusal must not create another logical notice.
		client.send({
			v: "0.1",
			type: "request",
			id: "model-again",
			verb: "chat.send",
			params: { origin, engagement, text: "/model next" },
		});
		await until(() => attempts === 2 && database.inbox.pendingOldest(key) !== undefined);
		expect(notices()).toHaveLength(1);
		expect(database.inbox.pendingOldest(key)?.terminal_delivery_id).toBeNull();

		client.send({
			v: "0.1",
			type: "request",
			id: "model-working",
			verb: "chat.send",
			params: { origin, engagement, text: "/model working" },
		});
		await until(() =>
			client.frames.some(
				(frame) => frame.event === "chat.message" && frame.payload.text === "the original message is answered",
			),
		);
		await until(() => database.inbox.turnRow(port.sends[0]!.opRef)?.state === "done");
		expect(attempts).toBe(3);
		expect(port.sends).toHaveLength(1);
		expect(port.sends[0]?.sessionId).toBe(sessionId);
		expect(port.binds).toHaveLength(1);
		expect(notices()).toHaveLength(1);
		expect(database.inbox.turnRow(port.sends[0]!.opRef)?.terminal_delivery_id).not.toBeNull();
	} finally {
		client.close();
	}
});

test("long turns broadcast throttled chat.progress liveness events", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
		gjc: { agentDir: join(directory, "agent") },
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
	const database = await Store.open(config.dbPath);
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
	// Wait for the initial audit response (returns opRef) and chat.message event
	for (let attempt = 0; attempt < 800; attempt++) {
		if (
			client.frames.some((frame) => frame.type === "response" && frame.id === "audit") &&
			client.frames.some((frame) => frame.type === "event" && frame.event === "chat.message")
		)
			break;
		await Bun.sleep(5);
	}
	const auditResponse = client.frames.find((frame) => frame.type === "response" && frame.id === "audit");
	expect(auditResponse).toBeDefined();
	expect(auditResponse?.result?.opRef).toBeDefined();
	const opRef = (auditResponse?.result as { opRef: string }).opRef;

	// Poll for the audit result via memory.status
	let auditStatus: any;
	for (let attempt = 0; attempt < 200; attempt++) {
		client.send({ v: "0.1", type: "request", id: `status-${attempt}`, verb: "memory.status", params: { opRef } });
		await waitFor(client.frames, client.frames.length + 1);
		const statusFrame = client.frames[client.frames.length - 1];
		if (statusFrame?.result?.status === "completed") {
			auditStatus = statusFrame.result;
			break;
		}
		await Bun.sleep(10);
	}
	expect(auditStatus).toBeDefined();
	expect(auditStatus?.status).toBe("completed");
	expect(auditStatus?.result).toBeDefined();

	// Verify the audit result contains large data (the issues list is comprehensive)
	const auditResultJson = JSON.stringify(auditStatus.result);
	expect(Buffer.byteLength(auditResultJson, "utf8")).toBeGreaterThan(219_000);
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

test("memory.autolink returns an operation ID and reports its committed result", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-memory-autolink-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const root = await initializeMemory(directory);
	await mkdir(join(root, "people"), { recursive: true });
	await writeFile(join(root, "people/alice-example.md"), "# Alice Example\n\nCanonical person.");
	await mkdir(join(root, "ops/rules"), { recursive: true });
	await writeFile(join(root, "ops/rules/example.md"), "Alice Example appears here.\n");
	await memoryGit(root, ["add", "-A"]);
	await memoryGit(root, ["commit", "-m", "autolink fixture"]);

	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "unused",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	client.send({ v: "0.1", type: "request", id: "autolink", verb: "memory.autolink" });
	await waitFrame(client.frames, "autolink");
	const accepted = client.frames.find((frame) => frame.id === "autolink");
	const opRef = accepted?.result?.opRef as string;
	expect(opRef).toMatch(/^gw-ml-/);

	let completed: MemoryOperationStatusResult | undefined;
	for (let attempt = 0; attempt < 200; attempt++) {
		const id = `autolink-status-${attempt}`;
		client.send({ v: "0.1", type: "request", id, verb: "memory.status", params: { opRef } });
		await waitFrame(client.frames, id);
		const status = client.frames.find((frame) => frame.id === id)?.result as MemoryOperationStatusResult | undefined;
		if (status?.status === "completed") {
			completed = status;
			break;
		}
		await Bun.sleep(5);
	}
	const report = completed?.result;
	expect(completed?.elapsedMs).toBeNumber();
	expect(report && "commitSha" in report).toBe(true);
	if (!report || !("commitSha" in report)) throw new Error("autolink status omitted its report");
	expect(report.filesChanged).toBe(1);
	expect(report.linksAdded).toBeGreaterThan(0);
	if (typeof report.commitSha !== "string") throw new Error("autolink report omitted its commit SHA");
	expect(report.commitSha).toMatch(/^[0-9a-f]{40}$/);
	expect(report.elapsedMs).toBeNumber();
	expect(await memoryGit(root, ["rev-parse", "HEAD"])).toBe(report.commitSha);
	expect(await Bun.file(join(root, "ops/rules/example.md")).text()).toContain(
		"[Alice Example](../../people/alice-example.md)",
	);
	client.close();
});

test("shutdown quiesces an in-flight turn before final stopping frame", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-shutdown-writer-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
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
	const database = await Store.open(config.dbPath);
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

test("graceful server restart recovers an accepted turn on its original live session", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	const startGateway = async () => {
		const database = await Store.open(config.dbPath);
		attachTestBrokerOwnership(database, port, join(directory, "agent"));
		server = await startUnixServer({ config, database, sessionPort: port, onStop: () => database.close() });
		return database;
	};
	let database = await startGateway();
	const firstClient = await connect(config.socketPath);
	firstClient.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(firstClient.frames, 1);
	firstClient.send({
		v: "0.1",
		type: "request",
		id: "in-flight-turn",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "restart-lifecycle", peerId: "p1" },
			messageId: "restart-lifecycle-message",
			text: "finish after the gateway restarts",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	for (let attempt = 0; attempt < 400 && port.sends.length === 0; attempt++) await Bun.sleep(5);
	const accepted = port.sends[0];
	if (!accepted) throw new Error("gateway did not accept the in-flight turn");
	expect(database.inbox.turnRow(accepted.opRef)).toMatchObject({
		state: "pending",
		turn_state: "accepted",
		bound_session_id: accepted.sessionId,
	});
	const readsBeforeRestart = port.statusReads.length;
	firstClient.close();
	const stoppingServer = server;
	if (!stoppingServer) throw new Error("gateway server disappeared before restart");
	await stoppingServer.stop("test gateway restart");
	server = undefined;
	expect(port.closes).toHaveLength(0);

	database = await startGateway();
	const secondClient = await connect(config.socketPath);
	secondClient.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(secondClient.frames, 1);
	for (
		let attempt = 0;
		attempt < 400 && !port.statusReads.slice(readsBeforeRestart).some((read) => read.opRef === accepted.opRef);
		attempt++
	)
		await Bun.sleep(5);
	expect(port.statusReads.slice(readsBeforeRestart).some((read) => read.opRef === accepted.opRef)).toBe(true);
	expect(port.sends).toHaveLength(1);
	expect(port.sends[0]).toMatchObject({ opRef: accepted.opRef, sessionId: accepted.sessionId });
	expect(database.inbox.turnRow(accepted.opRef)).toMatchObject({ state: "pending", turn_state: "accepted" });

	port.complete(accepted.opRef, "answer after gateway restart");
	for (
		let attempt = 0;
		attempt < 800 &&
		!secondClient.frames.some(
			(frame) => frame.event === "chat.message" && frame.payload?.text === "answer after gateway restart",
		);
		attempt++
	)
		await Bun.sleep(5);
	const replies = secondClient.frames.filter(
		(frame) => frame.event === "chat.message" && frame.payload?.text === "answer after gateway restart",
	);
	expect(replies).toHaveLength(1);
	expect(database.inbox.turnRow(accepted.opRef)).toMatchObject({ state: "done", turn_state: "done" });
	expect(port.sends).toHaveLength(1);
	secondClient.close();
}, 30_000);

test("restart prepare waits for delivered turns and holds new persona dispatches until cancellation", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-drain-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	let turnEntered!: () => void;
	let releaseTurn!: () => void;
	const entered = new Promise<void>((resolve) => (turnEntered = resolve));
	const turnRelease = new Promise<void>((resolve) => (releaseTurn = resolve));
	let responses = 0;
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => {
			responses++;
			if (responses === 1) {
				turnEntered();
				await turnRelease;
			}
			return `reply-${responses}`;
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
		id: "active-turn",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "active", peerId: "p1" },
			text: "finish before restarting",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	await entered;
	client.send({
		v: "0.1",
		type: "request",
		id: "prepare-restart",
		verb: "gateway.restart.prepare",
		params: { restartId: "restart-602" },
	});
	client.send({
		v: "0.1",
		type: "request",
		id: "queued-turn",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "queued", peerId: "p2" },
			text: "wait through restart",
			engagement: { mentioned: false, group: false, authorId: "p2" },
		},
	});
	await client.waitForFrame("queued-turn");
	await Bun.sleep(20);
	expect(responses).toBe(1);
	expect(client.frames.some((frame) => frame.id === "prepare-restart")).toBe(false);

	releaseTurn();
	await waitFrame(client.frames, "active-turn");
	for (
		let attempt = 0;
		attempt < 400 &&
		!client.frames.some(
			(frame) => frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "reply-1",
		);
		attempt++
	)
		await Bun.sleep(5);
	const replyDelivery = client.frames.find(
		(frame) => frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "reply-1",
	);
	if (!replyDelivery || replyDelivery.type !== "event" || !replyDelivery.payload.deliveryId)
		throw new Error("terminal reply delivery was not broadcast");
	client.send({
		v: "0.1",
		type: "request",
		id: "confirm-active-reply",
		verb: "delivery.confirm",
		params: { deliveryId: replyDelivery.payload.deliveryId },
	});
	await client.waitForFrame("confirm-active-reply");
	await client.waitForFrame("prepare-restart");
	const deliveredIndex = client.frames.findIndex(
		(frame) => frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "reply-1",
	);
	const preparedIndex = client.frames.findIndex((frame) => frame.id === "prepare-restart");
	expect(client.frames[preparedIndex]?.result).toEqual({ ready: true, drained: true });
	expect(deliveredIndex).toBeGreaterThan(0);
	expect(preparedIndex).toBeGreaterThan(deliveredIndex);
	expect(responses).toBe(1);
	client.send({
		v: "0.1",
		type: "request",
		id: "recheck-restart-drain",
		verb: "gateway.restart.prepare",
		params: { restartId: "restart-602", waitMs: 1_000 },
	});
	await client.waitForFrame("recheck-restart-drain");
	expect(client.frames.find((frame) => frame.id === "recheck-restart-drain")?.result).toEqual({
		ready: true,
		drained: true,
	});

	client.send({
		v: "0.1",
		type: "request",
		id: "cancel-restart",
		verb: "gateway.restart.cancel",
		params: { restartId: "restart-602" },
	});
	await client.waitForFrame("cancel-restart");
	expect(client.frames.find((frame) => frame.id === "cancel-restart")?.result).toEqual({ cancelled: true });
	await waitFrame(client.frames, "queued-turn");
	for (let attempt = 0; attempt < 400 && responses < 2; attempt++) await Bun.sleep(5);
	expect(responses).toBe(2);
	client.close();
});

test("restart prepare fails closed until an unconfirmed terminal reply is delivered", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-delivery-timeout-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	let turnEntered!: () => void;
	let releaseTurn!: () => void;
	const entered = new Promise<void>((resolve) => (turnEntered = resolve));
	const turnRelease = new Promise<void>((resolve) => (releaseTurn = resolve));
	let responses = 0;
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => {
			responses++;
			if (responses === 1) {
				turnEntered();
				await turnRelease;
			}
			return `reply-${responses}`;
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "active-turn",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "active", peerId: "p1" },
				text: "answer before restart",
				engagement: { mentioned: false, group: false, authorId: "p1" },
			},
		});
		await entered;
		client.send({
			v: "0.1",
			type: "request",
			id: "prepare-timeout",
			verb: "gateway.restart.prepare",
			params: { restartId: "unconfirmed-answer", waitMs: 40 },
		});
		client.send({ v: "0.1", type: "request", id: "drain-observed", verb: "gateway.status" });
		await client.waitForFrame("drain-observed");
		client.send({
			v: "0.1",
			type: "request",
			id: "queued-turn",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "queued", peerId: "p2" },
				text: "resume only after cancellation",
				engagement: { mentioned: false, group: false, authorId: "p2" },
			},
		});
		await client.waitForFrame("queued-turn");
		releaseTurn();
		for (
			let attempt = 0;
			attempt < 400 &&
			!client.frames.some(
				(frame) => frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "reply-1",
			);
			attempt++
		)
			await Bun.sleep(5);
		expect(
			client.frames.some(
				(frame) => frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "reply-1",
			),
		).toBe(true);
		await client.waitForFrame("prepare-timeout");
		expect(client.frames.find((frame) => frame.id === "prepare-timeout")?.error?.code).toBe("verb_failed");
		expect(client.frames.some((frame) => frame.type === "response" && frame.id === "prepare-timeout")).toBe(false);
		expect(responses).toBe(1);
		client.send({
			v: "0.1",
			type: "request",
			id: "cancel-timeout",
			verb: "gateway.restart.cancel",
			params: { restartId: "unconfirmed-answer" },
		});
		await client.waitForFrame("cancel-timeout");
		expect(client.frames.find((frame) => frame.id === "cancel-timeout")?.result).toEqual({ cancelled: true });
		for (let attempt = 0; attempt < 400 && responses < 2; attempt++) await Bun.sleep(5);
		expect(responses).toBe(2);
	} finally {
		releaseTurn();
		client.close();
	}
});

test("restart prepare treats an expired terminal delivery as settled", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-expired-delivery-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	let turnEntered!: () => void;
	let releaseTurn!: () => void;
	const entered = new Promise<void>((resolve) => (turnEntered = resolve));
	const turnRelease = new Promise<void>((resolve) => (releaseTurn = resolve));
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => {
			turnEntered();
			await turnRelease;
			return "expired terminal reply";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "turn-with-expiring-reply",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "expiring", peerId: "p1" },
				text: "complete while restart is draining",
				engagement: { mentioned: false, group: false, authorId: "p1" },
			},
		});
		await entered;
		client.send({
			v: "0.1",
			type: "request",
			id: "prepare-expiring-reply",
			verb: "gateway.restart.prepare",
			params: { restartId: "expiring-reply", waitMs: 1_000 },
		});
		client.send({ v: "0.1", type: "request", id: "observe-expiring-drain", verb: "gateway.status" });
		await client.waitForFrame("observe-expiring-drain");
		releaseTurn();
		for (
			let attempt = 0;
			attempt < 400 &&
			!client.frames.some(
				(frame) =>
					frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "expired terminal reply",
			);
			attempt++
		)
			await Bun.sleep(5);
		const terminalReply = client.frames.find(
			(frame) =>
				frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "expired terminal reply",
		);
		if (!terminalReply || terminalReply.type !== "event" || !terminalReply.payload.deliveryId)
			throw new Error("terminal reply was not broadcast");
		for (let attempt = 1; attempt <= 5; attempt++) {
			const id = `expire-terminal-${attempt}`;
			client.send({
				v: "0.1",
				type: "request",
				id,
				verb: "delivery.fail",
				params: { deliveryId: terminalReply.payload.deliveryId, reason: "adapter_failure", ambiguous: false },
			});
			await client.waitForFrame(id);
		}
		expect(database.outbox.rows().find((row) => row.delivery_id === terminalReply.payload.deliveryId)?.state).toBe(
			"expired",
		);
		await client.waitForFrame("prepare-expiring-reply");
		expect(client.frames.find((frame) => frame.id === "prepare-expiring-reply")?.result).toEqual({
			ready: true,
			drained: true,
		});
		client.send({
			v: "0.1",
			type: "request",
			id: "cancel-expiring-reply",
			verb: "gateway.restart.cancel",
			params: { restartId: "expiring-reply" },
		});
		await client.waitForFrame("cancel-expiring-reply");
		expect(client.frames.find((frame) => frame.id === "cancel-expiring-reply")?.result).toEqual({ cancelled: true });
	} finally {
		releaseTurn();
		client.close();
	}
});

test("expired terminal delivery IDs do not poison later restarts after outbox retention", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-expired-retention-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "terminal reply that expires before a restart",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "create-idle-expiring-terminal",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "expired-before-restart", peerId: "p1" },
				text: "finish before the restart begins",
				engagement: { mentioned: false, group: false, authorId: "p1" },
			},
		});
		await client.waitForFrame("create-idle-expiring-terminal");
		for (
			let attempt = 0;
			attempt < 400 &&
			!client.frames.some(
				(frame) =>
					frame.type === "event" &&
					frame.event === "chat.message" &&
					frame.payload.text === "terminal reply that expires before a restart",
			);
			attempt++
		)
			await Bun.sleep(5);
		const terminalReply = client.frames.find(
			(frame) =>
				frame.type === "event" &&
				frame.event === "chat.message" &&
				frame.payload.text === "terminal reply that expires before a restart",
		);
		if (!terminalReply || terminalReply.type !== "event" || !terminalReply.payload.deliveryId)
			throw new Error("terminal reply was not broadcast");
		const deliveryId = terminalReply.payload.deliveryId;
		const triggerRowQuery = database.core
			.groupDb("inbox")
			.prepare(
				"SELECT turn_state, terminal_delivery_id FROM inbound_messages WHERE origin_key = ? AND body = ? ORDER BY received_at DESC LIMIT 1",
			);
		let triggerRow: { turn_state: string; terminal_delivery_id: string | null } | null = null;
		for (let attempt = 0; attempt < 400; attempt++) {
			triggerRow = triggerRowQuery.get(
				"discord/dm/expired-before-restart/peer=p1",
				"finish before the restart begins",
			) as { turn_state: string; terminal_delivery_id: string | null } | null;
			if (triggerRow?.turn_state === "done" && triggerRow.terminal_delivery_id !== null) break;
			await Bun.sleep(5);
		}
		if (!triggerRow || triggerRow.turn_state !== "done" || triggerRow.terminal_delivery_id === null)
			throw new Error("terminal trigger did not finish with a delivery link");
		expect(Object.values(JSON.parse(triggerRow.terminal_delivery_id) as Record<string, string>)).toContain(deliveryId);
		client.send({
			v: "0.1",
			type: "request",
			id: "blocked-before-expiry",
			verb: "gateway.restart.prepare",
			params: { restartId: "blocked-before-expiry", waitMs: 0 },
		});
		await client.waitForFrame("blocked-before-expiry");
		expect(client.frames.find((frame) => frame.id === "blocked-before-expiry")?.error?.code).toBe("verb_failed");
		client.send({
			v: "0.1",
			type: "request",
			id: "cancel-blocked-before-expiry",
			verb: "gateway.restart.cancel",
			params: { restartId: "blocked-before-expiry" },
		});
		await client.waitForFrame("cancel-blocked-before-expiry");
		expect(client.frames.find((frame) => frame.id === "cancel-blocked-before-expiry")?.result).toEqual({
			cancelled: true,
		});

		for (let attempt = 1; attempt <= 5; attempt++) {
			const id = `expire-idle-terminal-${attempt}`;
			client.send({
				v: "0.1",
				type: "request",
				id,
				verb: "delivery.fail",
				params: { deliveryId, reason: "adapter_failure", ambiguous: false },
			});
			await client.waitForFrame(id);
		}
		expect(database.outbox.get(deliveryId)?.state).toBe("expired");
		const retentionCutoff = new Date(Date.now() + 1_000).toISOString();
		expect(
			database.transaction((tx) =>
				database.outbox.sweep(tx, {
					deliveryCutoff: retentionCutoff,
					historyCutoff: retentionCutoff,
					batch: 100,
				}),
			),
		).toBe(1);
		expect(database.outbox.get(deliveryId)).toBeUndefined();

		client.send({
			v: "0.1",
			type: "request",
			id: "restart-after-expired-retention",
			verb: "gateway.restart.prepare",
			params: { restartId: "restart-after-expired-retention", waitMs: 0 },
		});
		await client.waitForFrame("restart-after-expired-retention");
		expect(client.frames.find((frame) => frame.id === "restart-after-expired-retention")?.result).toEqual({
			ready: true,
			drained: true,
		});
		client.send({
			v: "0.1",
			type: "request",
			id: "cancel-after-expired-retention",
			verb: "gateway.restart.cancel",
			params: { restartId: "restart-after-expired-retention" },
		});
		await client.waitForFrame("cancel-after-expired-retention");
		expect(client.frames.find((frame) => frame.id === "cancel-after-expired-retention")?.result).toEqual({
			cancelled: true,
		});
	} finally {
		client.close();
	}
});

test("restart prepare waits for a terminal delivery whose actor settled before the drain", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-predrain-delivery-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "terminal reply before drain",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "terminal-before-drain",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "predrain", peerId: "p1" },
				text: "send a reply before draining",
				engagement: { mentioned: false, group: false, authorId: "p1" },
			},
		});
		await client.waitForFrame("terminal-before-drain");
		for (
			let attempt = 0;
			attempt < 400 &&
			!client.frames.some(
				(frame) =>
					frame.type === "event" &&
					frame.event === "chat.message" &&
					frame.payload.text === "terminal reply before drain",
			);
			attempt++
		)
			await Bun.sleep(5);
		const terminalReply = client.frames.find(
			(frame) =>
				frame.type === "event" &&
				frame.event === "chat.message" &&
				frame.payload.text === "terminal reply before drain",
		);
		if (!terminalReply || terminalReply.type !== "event" || !terminalReply.payload.deliveryId)
			throw new Error("pre-drain terminal reply was not broadcast");
		await Bun.sleep(30);
		client.send({
			v: "0.1",
			type: "request",
			id: "prepare-predrain-delivery",
			verb: "gateway.restart.prepare",
			params: { restartId: "predrain-delivery", waitMs: 40 },
		});
		await client.waitForFrame("prepare-predrain-delivery");
		expect(client.frames.find((frame) => frame.id === "prepare-predrain-delivery")?.error?.code).toBe("verb_failed");
		expect(client.frames.some((frame) => frame.id === "prepare-predrain-delivery" && frame.result !== undefined)).toBe(
			false,
		);
		client.send({
			v: "0.1",
			type: "request",
			id: "confirm-predrain-reply",
			verb: "delivery.confirm",
			params: { deliveryId: terminalReply.payload.deliveryId },
		});
		await client.waitForFrame("confirm-predrain-reply");
		client.send({
			v: "0.1",
			type: "request",
			id: "recheck-predrain-delivery",
			verb: "gateway.restart.prepare",
			params: { restartId: "predrain-delivery", waitMs: 1_000 },
		});
		await client.waitForFrame("recheck-predrain-delivery");
		expect(client.frames.find((frame) => frame.id === "recheck-predrain-delivery")?.result).toEqual({
			ready: true,
			drained: true,
		});
		client.send({
			v: "0.1",
			type: "request",
			id: "cancel-predrain-delivery",
			verb: "gateway.restart.cancel",
			params: { restartId: "predrain-delivery" },
		});
		await client.waitForFrame("cancel-predrain-delivery");
		expect(client.frames.find((frame) => frame.id === "cancel-predrain-delivery")?.result).toEqual({ cancelled: true });
	} finally {
		client.close();
	}
});

test("restart prepare includes unconfirmed terminal deliveries persisted by the prior gateway", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-durable-delivery-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const firstDatabase = await Store.open(config.dbPath);
	const firstSessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "durable terminal reply",
	});
	attachTestBrokerOwnership(firstDatabase, firstSessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database: firstDatabase,
		sessionPort: firstSessionPort,
		onStop: () => firstDatabase.close(),
	});
	const firstClient = await connect(config.socketPath);
	let client: Awaited<ReturnType<typeof connect>> | undefined;
	try {
		firstClient.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(firstClient.frames, 1);
		firstClient.send({
			v: "0.1",
			type: "request",
			id: "create-durable-terminal",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "durable", peerId: "p1" },
				text: "persist a terminal reply",
				engagement: { mentioned: false, group: false, authorId: "p1" },
			},
		});
		await firstClient.waitForFrame("create-durable-terminal");
		for (
			let attempt = 0;
			attempt < 400 &&
			!firstClient.frames.some(
				(frame) =>
					frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "durable terminal reply",
			);
			attempt++
		)
			await Bun.sleep(5);
		const terminalReply = firstClient.frames.find(
			(frame) =>
				frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "durable terminal reply",
		);
		if (!terminalReply || terminalReply.type !== "event" || !terminalReply.payload.deliveryId)
			throw new Error("terminal reply was not persisted by the first gateway");
		const deliveryId = terminalReply.payload.deliveryId;
		expect(firstDatabase.outbox.unsettledTerminalDeliveryIds()).toContain(deliveryId);
		const oldTriggerAt = new Date(Date.now() - 8 * 24 * 60 * 60 * 1_000).toISOString();
		const triggerRows = firstDatabase.core
			.groupDb("inbox")
			.prepare("UPDATE inbound_messages SET received_at = ? WHERE turn_role = 'trigger' AND turn_state = 'done'");
		expect(firstDatabase.transaction((tx) => triggerRows.run(tx, oldTriggerAt).changes)).toBeGreaterThan(0);
		await Bun.sleep(30);
		firstClient.close();
		await server.stop();
		server = undefined;

		const database = await Store.open(config.dbPath);
		const sessionPort = sessionPortFromResponder({
			bind: (key, epoch) => bindWorkFixture(key, epoch),
			respond: async () => "unused",
		});
		attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
		server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
		client = await connect(config.socketPath);
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "prepare-after-gateway-restart",
			verb: "gateway.restart.prepare",
			params: { restartId: "durable-terminal", waitMs: 40 },
		});
		await client.waitForFrame("prepare-after-gateway-restart");
		expect(client.frames.find((frame) => frame.id === "prepare-after-gateway-restart")?.error?.code).toBe(
			"verb_failed",
		);
		client.send({
			v: "0.1",
			type: "request",
			id: "confirm-durable-terminal",
			verb: "delivery.confirm",
			params: { deliveryId },
		});
		await client.waitForFrame("confirm-durable-terminal");
		client.send({
			v: "0.1",
			type: "request",
			id: "recheck-durable-terminal",
			verb: "gateway.restart.prepare",
			params: { restartId: "durable-terminal", waitMs: 1_000 },
		});
		await client.waitForFrame("recheck-durable-terminal");
		expect(client.frames.find((frame) => frame.id === "recheck-durable-terminal")?.result).toEqual({
			ready: true,
			drained: true,
		});
		client.send({
			v: "0.1",
			type: "request",
			id: "cancel-durable-terminal",
			verb: "gateway.restart.cancel",
			params: { restartId: "durable-terminal" },
		});
		await client.waitForFrame("cancel-durable-terminal");
		expect(client.frames.find((frame) => frame.id === "cancel-durable-terminal")?.result).toEqual({
			cancelled: true,
		});
	} finally {
		firstClient.close();
		client?.close();
		await server?.stop();
		server = undefined;
	}
});

test("gateway restores an indeterminate restart drain from its persisted receipt", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-receipt-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	await writeFile(
		join(directory, "restart-stack.json"),
		JSON.stringify({ id: "interrupted-stack", state: "indeterminate", drainPrepared: true }),
		{ mode: 0o600 },
	);
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "unused",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	let client: Awaited<ReturnType<typeof connect>> | undefined;
	let reconnect: Awaited<ReturnType<typeof connect>> | undefined;
	try {
		client = await connect(config.socketPath);
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "reattach-interrupted-stack",
			verb: "gateway.restart.prepare",
			params: { restartId: "interrupted-stack", waitMs: 0 },
		});
		await client.waitForFrame("reattach-interrupted-stack");
		expect(client.frames.find((frame) => frame.id === "reattach-interrupted-stack")?.result).toEqual({
			ready: true,
			drained: true,
		});
		client.close();
		await Bun.sleep(30);

		reconnect = await connect(config.socketPath);
		reconnect.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(reconnect.frames, 1);
		reconnect.send({
			v: "0.1",
			type: "request",
			id: "recover-interrupted-stack",
			verb: "gateway.restart.cancel",
			params: { restartId: "interrupted-stack" },
		});
		await reconnect.waitForFrame("recover-interrupted-stack");
		expect(reconnect.frames.find((frame) => frame.id === "recover-interrupted-stack")?.result).toEqual({
			cancelled: true,
		});
	} finally {
		client?.close();
		reconnect?.close();
		await server?.stop();
		server = undefined;
	}
});

test("restart prepare settles a silent turn without waiting for a nonexistent delivery", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-restart-silent-turn-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	let turnEntered!: () => void;
	let releaseTurn!: () => void;
	const entered = new Promise<void>((resolve) => (turnEntered = resolve));
	const turnRelease = new Promise<void>((resolve) => (releaseTurn = resolve));
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => {
			turnEntered();
			await turnRelease;
			return "[SILENT]";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "silent-turn",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "silent", peerId: "p1" },
				text: "no reply is needed",
				engagement: { mentioned: false, group: false, authorId: "p1" },
			},
		});
		await entered;
		client.send({
			v: "0.1",
			type: "request",
			id: "prepare-silent-turn",
			verb: "gateway.restart.prepare",
			params: { restartId: "silent-turn-restart", waitMs: 1_000 },
		});
		client.send({ v: "0.1", type: "request", id: "silent-drain-observed", verb: "gateway.status" });
		await client.waitForFrame("silent-drain-observed");
		releaseTurn();
		await client.waitForFrame("prepare-silent-turn");
		expect(client.frames.find((frame) => frame.id === "prepare-silent-turn")?.result).toEqual({
			ready: true,
			drained: true,
		});
		expect(
			client.frames.some(
				(frame) => frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "[SILENT]",
			),
		).toBe(false);
		client.send({
			v: "0.1",
			type: "request",
			id: "cancel-silent-turn",
			verb: "gateway.restart.cancel",
			params: { restartId: "silent-turn-restart" },
		});
		await client.waitForFrame("cancel-silent-turn");
	} finally {
		releaseTurn();
		client.close();
	}
});

test("owner /restart resumes after a stack drain is cancelled and waits for active replies", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-owner-restart-drain-"));
	const ownerOrigin = { platform: "discord", kind: "dm", conversationId: "owner", peerId: "owner" } as const;
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		ownerTarget: { origin: ownerOrigin },
	};
	let turnEntered!: () => void;
	let releaseTurn!: () => void;
	const entered = new Promise<void>((resolve) => (turnEntered = resolve));
	const turnRelease = new Promise<void>((resolve) => (releaseTurn = resolve));
	let responses = 0;
	let exitCode: number | undefined;
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => {
			responses++;
			turnEntered();
			await turnRelease;
			return "active reply delivered";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		exitProcess: (code) => {
			exitCode = code;
		},
	});
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "active-reply",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "active", peerId: "p1" },
				text: "finish this reply before restart",
				engagement: { mentioned: false, group: false, authorId: "p1" },
			},
		});
		await entered;
		client.send({
			v: "0.1",
			type: "request",
			id: "stack-prepare",
			verb: "gateway.restart.prepare",
			params: { restartId: "stack-owner-race", waitMs: 120_000 },
		});
		client.send({ v: "0.1", type: "request", id: "stack-drain-observed", verb: "gateway.status" });
		await client.waitForFrame("stack-drain-observed");
		client.send({
			v: "0.1",
			type: "request",
			id: "owner-restart",
			verb: "chat.send",
			params: {
				origin: ownerOrigin,
				text: "/restart",
				engagement: { mentioned: false, group: false, authorId: "owner" },
			},
		});
		await client.waitForFrame("owner-restart");
		await Bun.sleep(1_600);
		expect(exitCode).toBeUndefined();
		client.send({
			v: "0.1",
			type: "request",
			id: "queued-during-restart",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "queued", peerId: "p2" },
				text: "wait for the next gateway generation",
				engagement: { mentioned: false, group: false, authorId: "p2" },
			},
		});
		await client.waitForFrame("queued-during-restart");
		expect(responses).toBe(1);
		client.send({
			v: "0.1",
			type: "request",
			id: "cancel-stack-drain",
			verb: "gateway.restart.cancel",
			params: { restartId: "stack-owner-race" },
		});
		await client.waitForFrame("cancel-stack-drain");
		expect(client.frames.find((frame) => frame.id === "cancel-stack-drain")?.result).toEqual({
			cancelled: false,
			transferred: true,
		});
		releaseTurn();
		for (
			let attempt = 0;
			attempt < 400 &&
			!client.frames.some(
				(frame) =>
					frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "active reply delivered",
			);
			attempt++
		)
			await Bun.sleep(5);
		const activeReply = client.frames.find(
			(frame) =>
				frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "active reply delivered",
		);
		if (!activeReply || activeReply.type !== "event" || !activeReply.payload.deliveryId)
			throw new Error("owner restart reply was not broadcast");
		client.send({
			v: "0.1",
			type: "request",
			id: "confirm-owner-reply",
			verb: "delivery.confirm",
			params: { deliveryId: activeReply.payload.deliveryId },
		});
		await client.waitForFrame("confirm-owner-reply");
		for (let attempt = 0; attempt < 400 && exitCode === undefined; attempt++) await Bun.sleep(5);
		expect(exitCode).toBe(75);
		expect(responses).toBe(1);
		const replyIndex = client.frames.findIndex(
			(frame) =>
				frame.type === "event" && frame.event === "chat.message" && frame.payload.text === "active reply delivered",
		);
		expect(replyIndex).toBeGreaterThan(0);
	} finally {
		releaseTurn();
		client.close();
	}
});

test("owner /restart cancels instead of restarting before an active reply is confirmed", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-owner-restart-delivery-timeout-"));
	const ownerOrigin = { platform: "discord", kind: "dm", conversationId: "owner", peerId: "owner" } as const;
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		ownerTarget: { origin: ownerOrigin },
	};
	let turnEntered!: () => void;
	let releaseTurn!: () => void;
	const entered = new Promise<void>((resolve) => (turnEntered = resolve));
	const turnRelease = new Promise<void>((resolve) => (releaseTurn = resolve));
	let responses = 0;
	let exitCode: number | undefined;
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => {
			responses++;
			if (responses === 1) {
				turnEntered();
				await turnRelease;
			}
			return `reply-${responses}`;
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		exitProcess: (code) => {
			exitCode = code;
		},
		ownerRestartDrainWaitMs: 20,
	});
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "active-turn",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "active", peerId: "p1" },
				text: "do not restart before my answer arrives",
				engagement: { mentioned: false, group: false, authorId: "p1" },
			},
		});
		await entered;
		client.send({
			v: "0.1",
			type: "request",
			id: "owner-restart",
			verb: "chat.send",
			params: {
				origin: ownerOrigin,
				text: "/restart",
				engagement: { mentioned: false, group: false, authorId: "owner" },
			},
		});
		await client.waitForFrame("owner-restart");
		client.send({
			v: "0.1",
			type: "request",
			id: "queued-turn",
			verb: "chat.send",
			params: {
				origin: { platform: "discord", kind: "dm", conversationId: "queued", peerId: "p2" },
				text: "keep this queued until restart is cancelled",
				engagement: { mentioned: false, group: false, authorId: "p2" },
			},
		});
		await client.waitForFrame("queued-turn");
		await Bun.sleep(5);
		expect(exitCode).toBeUndefined();
		expect(client.frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping")).toBe(false);
		expect(responses).toBe(1);
		await Bun.sleep(100);
		expect(exitCode).toBeUndefined();
		for (let attempt = 0; attempt < 400 && responses < 2; attempt++) await Bun.sleep(5);
		expect(responses).toBe(2);
		releaseTurn();
		await Bun.sleep(20);
		expect(exitCode).toBeUndefined();
	} finally {
		releaseTurn();
		client.close();
	}
});

test("owner /restart takes over a stack supervisor that exceeds its wait deadline", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-owner-restart-timeout-"));
	const ownerOrigin = { platform: "discord", kind: "dm", conversationId: "owner", peerId: "owner" } as const;
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		ownerTarget: { origin: ownerOrigin },
	};
	let exitCode: number | undefined;
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "reply",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		exitProcess: (code) => {
			exitCode = code;
		},
		ownerRestartSupervisorWaitMs: 20,
		ownerRestartSupervisorCancelWaitMs: 1_000,
	});
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "stack-prepare",
			verb: "gateway.restart.prepare",
			params: { restartId: "stalled-stack-supervisor", waitMs: 120_000 },
		});
		await client.waitForFrame("stack-prepare");
		expect(client.frames.find((frame) => frame.id === "stack-prepare")?.result).toEqual({ ready: true, drained: true });
		client.send({
			v: "0.1",
			type: "request",
			id: "owner-restart",
			verb: "chat.send",
			params: {
				origin: ownerOrigin,
				text: "/restart",
				engagement: { mentioned: false, group: false, authorId: "owner" },
			},
		});
		await client.waitForFrame("owner-restart");
		for (
			let attempt = 0;
			attempt < 400 &&
			!client.frames.some((frame) => frame.type === "event" && frame.event === "gateway.restart.superseded");
			attempt++
		)
			await Bun.sleep(5);
		const superseded = client.frames.find(
			(frame) => frame.type === "event" && frame.event === "gateway.restart.superseded",
		);
		expect(superseded?.type === "event" ? superseded.payload : undefined).toEqual({
			restartId: "stalled-stack-supervisor",
			ownerRestartId: expect.any(String),
		});
		expect(exitCode).toBeUndefined();
		expect(client.frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping")).toBe(false);
		client.send({
			v: "0.1",
			type: "request",
			id: "ack-stack-supervisor-cancel",
			verb: "gateway.restart.cancel",
			params: { restartId: "stalled-stack-supervisor" },
		});
		await client.waitForFrame("ack-stack-supervisor-cancel");
		expect(client.frames.find((frame) => frame.id === "ack-stack-supervisor-cancel")?.result).toEqual({
			cancelled: false,
			transferred: true,
		});
		for (
			let attempt = 0;
			attempt < 400 && !client.frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping");
			attempt++
		)
			await Bun.sleep(5);
		expect(client.frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping")).toBe(true);
		client.send({
			v: "0.1",
			type: "request",
			id: "stale-stack-recheck",
			verb: "gateway.restart.prepare",
			params: { restartId: "stalled-stack-supervisor", waitMs: 1_000 },
		});
		await client.waitForFrame("stale-stack-recheck");
		expect(client.frames.find((frame) => frame.id === "stale-stack-recheck")?.error?.code).toBe(
			"gateway_shutting_down",
		);
		for (let attempt = 0; attempt < 500 && exitCode === undefined; attempt++) await Bun.sleep(5);
		expect(exitCode).toBe(75);
	} finally {
		client.close();
	}
});

test("owner /restart fails closed when a superseded stack supervisor does not settle", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-owner-restart-command-stuck-"));
	const ownerOrigin = { platform: "discord", kind: "dm", conversationId: "owner", peerId: "owner" } as const;
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		ownerTarget: { origin: ownerOrigin },
	};
	let exitCode: number | undefined;
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async () => "reply",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({
		config,
		database,
		sessionPort,
		onStop: () => database.close(),
		exitProcess: (code) => {
			exitCode = code;
		},
		ownerRestartSupervisorWaitMs: 20,
		ownerRestartSupervisorCancelWaitMs: 30,
	});
	const client = await connect(config.socketPath);
	try {
		client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
		await waitFor(client.frames, 1);
		client.send({
			v: "0.1",
			type: "request",
			id: "stalled-prepare",
			verb: "gateway.restart.prepare",
			params: { restartId: "unsettled-stack-supervisor", waitMs: 120_000 },
		});
		await client.waitForFrame("stalled-prepare");
		client.send({
			v: "0.1",
			type: "request",
			id: "owner-restart",
			verb: "chat.send",
			params: {
				origin: ownerOrigin,
				text: "/restart",
				engagement: { mentioned: false, group: false, authorId: "owner" },
			},
		});
		await client.waitForFrame("owner-restart");
		for (
			let attempt = 0;
			attempt < 400 &&
			!client.frames.some(
				(frame) =>
					frame.type === "event" &&
					frame.event === "chat.message" &&
					frame.payload.text.includes("did not finish its current service command"),
			);
			attempt++
		)
			await Bun.sleep(5);
		expect(
			client.frames.some(
				(frame) =>
					frame.type === "event" &&
					frame.event === "chat.message" &&
					frame.payload.text.includes("did not finish its current service command"),
			),
		).toBe(true);
		expect(exitCode).toBeUndefined();
		expect(client.frames.some((frame) => frame.type === "event" && frame.event === "gateway.stopping")).toBe(false);
		client.send({
			v: "0.1",
			type: "request",
			id: "release-stalled-drain",
			verb: "gateway.restart.cancel",
			params: { restartId: "unsettled-stack-supervisor" },
		});
		await client.waitForFrame("release-stalled-drain");
		expect(client.frames.find((frame) => frame.id === "release-stalled-drain")?.result).toEqual({
			cancelled: true,
		});
	} finally {
		client.close();
	}
});

test("unengaged messages before a mention arrive as the unread diff with speaker attribution; the mention is the trigger", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "mention-open" } },
	};
	const database = await Store.open(config.dbPath);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "open" } },
	};
	const database = await Store.open(config.dbPath);
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
	for (let attempt = 0; attempt < 400 && database.inbox.pendingCount("discord/channel/c1") > 0; attempt++)
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { townhall: { engagement: "mention-open" } },
	};
	const database = await Store.open(config.dbPath);
	const turns: string[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: (key, epoch) => bindWorkFixture(key, epoch),
		respond: async (_session, text) => {
			turns.push(text);
			return "ok";
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	database.inbox.enqueue = () => {
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: "stale-1",
				originKey: key,
				originRefJson: JSON.stringify(origin),
				body: "from before the outage",
				receivedAt: old,
			}),
		),
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
		expect(database.inbox.pendingCount(key)).toBe(0);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "d-owner", peerId: "owner" } },
	};
	const database = await Store.open(config.dbPath);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "open" } },
	};
	const database = await Store.open(config.dbPath);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
	client.send({
		v: "0.1",
		type: "request",
		id: "bad",
		verb: "work.run",
		params: { cwd: directory, name: "../evil", text: "x" },
	});
	await waitFor(client.frames, 3);
	expect(client.frames.find((frame) => frame.type === "error" && frame.id === "bad")).toBeDefined();
	client.close();
});

test("work.run records a durable lane job and work.jobs projects it (issue #10)", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
	const sessionPort = sessionPortFromResponder({
		bind: async (key, epoch) => ({ sessionId: bindWorkFixture(key, epoch, "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0") }),
		respond: async () => "worker result",
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const runResponse = client.waitForFrame("w");
	client.send({
		v: "0.1",
		type: "request",
		id: "w",
		verb: "work.run",
		params: { cwd: directory, name: "Repo.Fix-2", text: "fix the bug" },
	});
	await runResponse;
	const run = client.frames.find((frame) => frame.type === "response" && frame.id === "w");
	expect(run).toMatchObject({ type: "response", id: "w" });
	expect(run.result).toMatchObject({ held: false, text: "worker result" });
	const jobId = run.result.jobId;
	expect(jobId).toBe(`lanejob-${Buffer.from("Repo.Fix-2", "utf8").toString("hex")}`);

	const jobsResponse = client.waitForFrame("jobs");
	client.send({ v: "0.1", type: "request", id: "jobs", verb: "work.jobs" });
	await jobsResponse;
	const jobs = client.frames.find((frame) => frame.type === "response" && frame.id === "jobs");
	expect(jobs).toMatchObject({ type: "response", id: "jobs" });
	expect(jobs.result.jobs).toHaveLength(1);
	expect(jobs.result.jobs[0].job_id).toBe(jobId);
	expect(jobs.result.jobs[0].lane_key).toBe("work-Repo.Fix-2");
	expect(jobs.result.jobs[0].reports).toEqual({ pending: 0, claimed: 0, held: 0, undeliverable: 0 });
	// The completed ATTEMPT closed; the JOB stays continuable (attempt_ended),
	// never a terminal work-failure.
	expect(jobs.result.jobs[0].state).toBe("attempt_ended");
	// No commit was made, so the pre-existing HEAD must NOT appear as a
	// worker checkpoint: progress is measured against the creation baseline.
	expect(jobs.result.jobs[0].checkpoints).toBe(0);

	// The stored authority carries the closed attempt; the record is restart-safe.
	const raw = database.work.laneJobJson(jobId);
	expect(typeof raw).toBe("string");
	const parsed = parseLaneJobRecord(raw as string);
	expect(parsed.attempts).toHaveLength(1);
	expect(parsed.attempts[0].endState).toBe("completed");
	expect(parsed.attempts[0].sessionId).toBe("0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0");
	expect(parsed.lane.worktreePath).toBe(directory);

	// Reopening a settled tombstone by editing history is corruption, not a
	// crash predecessor. Neither an ordinary request nor resume may repair it.
	database.transaction((tx) =>
		database.work.putLaneJob(tx, {
			jobId: parsed.jobId,
			laneKey: "work-Repo.Fix-2",
			state: "running",
			createdAt: parsed.createdAt,
			updatedAt: new Date().toISOString(),
			lane: parsed.lane,
			json: JSON.stringify({
				...parsed,
				attempts: [{ ...parsed.attempts[0], endState: undefined, endedAt: undefined }],
			}),
		}),
	);
	const damaged = database.work.laneJobJson(jobId);
	for (const resume of [false, true]) {
		const id = `corrupt-${resume}`;
		client.send({
			v: "0.1",
			type: "request",
			id,
			verb: "work.run",
			params: { cwd: directory, name: "Repo.Fix-2", text: "again", resume },
		});
		await waitFrame(client.frames, id);
		expect(client.frames.find((frame) => frame.id === id)).toMatchObject({
			type: "error",
			error: { code: "verb_failed", detail: { reasonCode: "lane_state_corrupt", name: "Repo.Fix-2" } },
		});
	}
	expect(database.work.laneJobJson(jobId)).toBe(damaged);
	expect(sessionPort.sends).toHaveLength(1);
	client.close();
});

test("work.jobs pages newest-first so a home with many jobs stays under the frame cap", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await Store.open(config.dbPath);
	// Each projected job is ~700 bytes, so 1600 of them exceed the 1 MiB frame unpaged.
	const total = 1600;
	database.transaction((tx) => {
		for (let index = 0; index < total; index++) {
			// Pairs share updated_at, so only the job_id tiebreak keeps pages disjoint.
			const at = new Date(Date.UTC(2026, 0, 1) + (index >> 1) * 1000);
			const record = createLaneJobRecord({
				jobId: `lanejob-${index.toString(16).padStart(4, "0")}`,
				branch: "main",
				worktreePath: join(directory, "missing-worktree"),
				now: () => at,
			});
			database.work.putLaneJob(tx, { ...record, laneKey: `work-job${index}`, json: JSON.stringify(record) });
		}
	});
	const sessionPort = sessionPortFromResponder({
		bind: async () => ({ sessionId: "unused" }),
		respond: async () => "",
	});
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	const client = await connect(config.socketPath);
	client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	await waitFor(client.frames, 1);
	const request = async (id: string, params?: unknown) => {
		client.send({ v: "0.1", type: "request", id, verb: "work.jobs", ...(params === undefined ? {} : { params }) });
		await waitFor(client.frames, client.frames.length + 1);
		return client.frames.find((frame) => frame.id === id);
	};

	const first = await request("default");
	expect(first.type).toBe("response");
	expect(first.result.total).toBe(total);
	expect(first.result.offset).toBe(0);
	expect(first.result.jobs).toHaveLength(WORK_JOBS_DEFAULT_LIMIT);
	expect(first.result.jobs[0].lane_key).toBe(`work-job${total - 1}`);

	const tail = await request("tail", { limit: WORK_JOBS_MAX_LIMIT, offset: total - 2 });
	expect(tail.result.jobs.map((job: { lane_key: string }) => job.lane_key)).toEqual(["work-job1", "work-job0"]);
	expect(tail.result.offset).toBe(total - 2);
	const seen = new Set<string>();
	for (let offset = 0; offset < total; offset += WORK_JOBS_MAX_LIMIT) {
		const page = await request(`page-${offset}`, { limit: WORK_JOBS_MAX_LIMIT, offset });
		for (const job of page.result.jobs) seen.add(job.job_id);
	}
	expect(seen.size).toBe(total);

	for (const [id, params] of [
		["zero", { limit: 0 }],
		["over", { limit: WORK_JOBS_MAX_LIMIT + 1 }],
		["negative", { offset: -1 }],
		["fraction", { limit: 1.5 }],
	] as const) {
		const refused = await request(id, params);
		expect(refused.type).toBe("error");
		expect(refused.error.code).toBe("invalid_params");
	}
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
	let committed = false;
	const sessionPort = sessionPortFromResponder({
		bind: async (key, epoch) => ({ sessionId: bindWorkFixture(key, epoch) }),
		respond: async () => {
			// The first worker commits; that HEAD move is the progress signal the row must show.
			// Spawn asynchronously: a synchronous commit stalls the event loop that must deliver
			// the work.run response, which timed out on loaded linux-arm64 runners (issue #67).
			if (!committed) {
				const exitCode = await Bun.spawn(
					["git", "-C", repo, "commit", "--allow-empty", "--quiet", "-m", "lane: land the fix"],
					{
						stdout: "ignore",
						stderr: "ignore",
						env: {
							...process.env,
							GIT_AUTHOR_NAME: "lane",
							GIT_AUTHOR_EMAIL: "lane@example.invalid",
							GIT_COMMITTER_NAME: "lane",
							GIT_COMMITTER_EMAIL: "lane@example.invalid",
							GIT_COMMITTER_DATE: "2026-09-01T00:00:00Z",
						},
					},
				).exited;
				expect(exitCode).toBe(0);
			}
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
	const record = parseLaneJobRecord(database.work.laneJobJson(row.job_id) as string);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
	client.send({
		v: "0.1",
		type: "request",
		id: "w",
		verb: "work.run",
		params: { cwd: directory, name: "stall-out", text: "x" },
	});
	await waitFor(client.frames, 2);
	expect(client.frames.find((frame) => frame.type === "response" && frame.id === "w")).toBeDefined();
	// Simulate the reconciliation outcome after repeated stalled continuations.
	const first = parseLaneJobRecord(
		database.work.laneJobJson(`lanejob-${Buffer.from("stall-out", "utf8").toString("hex")}`) as string,
	);
	database.transaction((tx) =>
		database.work.putLaneJob(tx, {
			jobId: first.jobId,
			laneKey: `work-stall-out`,
			state: "stalled",
			createdAt: first.createdAt,
			updatedAt: new Date().toISOString(),
			lane: first.lane,
			json: JSON.stringify({ ...first, state: "stalled", stalledContinuations: MAX_STALLED_CONTINUATIONS }),
		}),
	);
	client.send({
		v: "0.1",
		type: "request",
		id: "w2",
		verb: "work.run",
		params: { cwd: directory, name: "stall-out", text: "x" },
	});
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
		params: { cwd: directory, name: "stall-out", text: "x", resume: true },
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		// This harness drives DM turns; DMs are authorisation-gated now.
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
	client.send({
		v: "0.1",
		type: "request",
		id: "w1",
		verb: "work.run",
		params: { cwd: directory, name: "stall-clear", text: "x" },
	});
	async function waitId3(id: string): Promise<void> {
		for (let attempt = 0; attempt < 400 && !client.frames.some((f) => f.id === id && f.type !== undefined); attempt++)
			await Bun.sleep(5);
	}
	await waitId3("w1");

	// Force the stalled hold, as reconciliation would after repeat stalls.
	const first = parseLaneJobRecord(database.work.laneJobJson(jobId) as string);
	database.transaction((tx) =>
		database.work.putLaneJob(tx, {
			jobId: first.jobId,
			laneKey: "work-stall-clear",
			state: "stalled",
			createdAt: first.createdAt,
			updatedAt: new Date().toISOString(),
			lane: first.lane,
			json: JSON.stringify({ ...first, state: "stalled", stalledContinuations: MAX_STALLED_CONTINUATIONS }),
		}),
	);

	// Ordinary call: held.
	client.send({
		v: "0.1",
		type: "request",
		id: "w2",
		verb: "work.run",
		params: { cwd: directory, name: "stall-clear", text: "x" },
	});
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
		params: { cwd: directory, name: "stall-clear", text: "x", resume: true },
	});
	await waitId3("w3");
	expect(client.frames.find((frame) => frame.id === "w3" && frame.type === "response")?.result).toMatchObject({
		held: false,
		text: "worker result",
	});
	const afterResume = parseLaneJobRecord(database.work.laneJobJson(jobId) as string);
	expect(afterResume.stalledContinuations).toBe(0);
	expect(afterResume.escalations.some((entry) => entry.includes("operator resume acknowledged"))).toBe(true);

	// The NEXT ordinary call is not held by the old stalled state.
	client.send({
		v: "0.1",
		type: "request",
		id: "w4",
		verb: "work.run",
		params: { cwd: directory, name: "stall-clear", text: "x" },
	});
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await Store.open(config.dbPath);
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
			params: { cwd: directory, name: "model", text: "work", model },
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		work: { maxLanes: 1 },
	};
	const database = await Store.open(config.dbPath);
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
	const sessionId = database.bindings.getSessionRecord("work/task/a")!.sessionId;
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
	expect(database.bindings.getSessionRecord("work/task/a")).toEqual({ sessionId: "", epoch: 1 });
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await Store.open(config.dbPath);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
	};
	const database = await Store.open(config.dbPath);
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
		database.transaction((tx) =>
			database.inbox.contextRecord(tx, {
				messageId: `old-${i}`,
				originKey: key,
				authorId: "owner",
				authorName: "bellman",
				body: `earlier message ${i}`,
				receivedAt: new Date(Date.now() - 60_000 * (4 - i)).toISOString(),
			}),
		);
	database.transaction((tx) =>
		database.inbox.contextRecord(tx, {
			messageId: "old-img",
			originKey: key,
			authorId: "owner",
			authorName: "bellman",
			body: "[image · shot.png · 20.1 KB · https://cdn.discordapp.com/attachments/1/2/shot.png]",
			receivedAt: new Date(Date.now() - 30_000).toISOString(),
		}),
	);
	database.transaction((tx) => database.inbox.contextCommitWindow(tx, key, ["old-1", "old-2", "old-3", "old-img"], 0));
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open" as const,
		channels: { c1: { engagement: "open" } },
	};
	const database = await Store.open(config.dbPath);
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
	const target = { platform: "discord" as const, kind: "channel" as const, conversationId: "results" };
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		work: { maxLanes: 2 },
		ownerTarget: { origin: target },
	};
	const database = await Store.open(config.dbPath);
	const port = new ScriptedSessionPort({ onBind: (input) => bindWorkFixture(input.originKey, input.epoch) });
	attachTestBrokerOwnership(database, port, join(directory, "agent"));
	let openAtShutdown = -1;
	server = await startUnixServer({
		config,
		database,
		sessionPort: port,
		onStop: () => {
			openAtShutdown = database.work.workAttemptOpen().length;
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
	f.client.send({
		v: "0.1",
		type: "request",
		id: "start",
		verb: "work.start",
		params: { name: "a", text: "work", cwd: directory },
	});
	await waitFrame(f.client.frames, "start");
	const receipt = f.client.frames.find((frame) => frame.id === "start").result;
	expect(receipt.started).toBe(true);
	expect(f.database.work.workAttemptGet(receipt.opRef)?.settledAt).toBeNull();
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
		origin: { platform: "discord", kind: "channel", conversationId: "results" },
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
	for (let i = 0; i < 600 && f.database.work.workAttemptOpen().length; i++) await Bun.sleep(5);
	expect(f.database.work.workAttemptOpen()).toHaveLength(0);
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
		import { Store } from "./packages/gateway/src/store/index.ts";
		import { attachTestBrokerOwnership, ScriptedSessionPort } from "./packages/gateway/test/session-port.fake.ts";
		const home = process.env.WORK_TEST_HOME;
		const config = { schemaVersion: 1, home, configPath: join(home, "config.json"), socketPath: join(home, "gateway.sock"), dbPath: join(home, "gateway.db"), logVerbosity: "info", gjc: { agentDir: join(home, "agent") } };
		const database = await Store.open(config.dbPath);
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
		const reopened = await Store.open(join(directory, "gateway.db"));
		try {
			expect(reopened.work.workAttemptOpen()).toHaveLength(1);
			expect(
				parseLaneJobRecord(reopened.work.laneJobJson(reopened.work.workAttemptOpen()[0]!.jobId)!).attempts[0]?.endedAt,
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
				for (let i = 0; i < 600 && f.database.work.workAttemptGet(opRef)?.settledAt === null; i++) await Bun.sleep(5);
				expect(f.database.work.workAttemptGet(opRef)?.settledAt).not.toBeNull();
				await waitFrame(f.client.frames, "worker");
			} else expect(f.database.work.workAttemptGet(opRef)?.settledAt).toBeNull();
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
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await Store.open(config.dbPath);
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

for (const state of ["running", "indeterminate"] as const) {
	test(`#713: an inherited ${state} restart drain stays held and is reported until it is recovered`, async () => {
		directory = await mkdtemp(join(tmpdir(), "gajaeway-server-inherited-drain-"));
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: directory,
			gjc: { agentDir: join(directory, "agent") },
			configPath: join(directory, "config.json"),
			socketPath: join(directory, "gateway.sock"),
			dbPath: join(directory, "gateway.db"),
			logVerbosity: "info",
			dmPolicy: "open" as const,
		};
		const stackPath = join(directory, "restart-stack.json");
		// The supervisor that wrote this receipt is gone; that alone never releases the fence.
		await writeFile(stackPath, JSON.stringify({ id: "inherited-drain", state, drainPrepared: true }), {
			mode: 0o600,
		});
		const database = await Store.open(config.dbPath);
		const sessionPort = sessionPortFromResponder({
			bind: (key, epoch) => bindWorkFixture(key, epoch),
			respond: async () => "reply",
		});
		attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
		let client: Awaited<ReturnType<typeof connect>> | undefined;
		try {
			server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
			client = await connect(config.socketPath);
			client.send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
			await waitFor(client.frames, 1);
			client.send({ v: "0.1", type: "request", id: "held", verb: "gateway.status" });
			await client.waitForFrame("held");
			const held = client.frames.find((frame) => frame.id === "held")?.result.readiness;
			expect(held.ready).toBe(false);
			expect(held.failure).toMatchObject({ code: "restart_drain_held", consecutiveFailures: 1 });
			expect(held.failure.detail).toContain("id=inherited-drain");
			expect(typeof held.failure.at).toBe("string");
			expect(typeof held.windowMs).toBe("number");
			expect(typeof held.failureThreshold).toBe("number");
			expect((JSON.parse(await readFile(stackPath, "utf8")) as { id?: string }).id).toBe("inherited-drain");

			client.send({
				v: "0.1",
				type: "request",
				id: "reattach",
				verb: "gateway.restart.prepare",
				params: { restartId: "inherited-drain", waitMs: 0 },
			});
			await client.waitForFrame("reattach");
			client.send({
				v: "0.1",
				type: "request",
				id: "recover",
				verb: "gateway.restart.cancel",
				params: { restartId: "inherited-drain" },
			});
			await client.waitForFrame("recover");
			expect(client.frames.find((frame) => frame.id === "recover")?.result).toEqual({ cancelled: true });
			client.send({ v: "0.1", type: "request", id: "released", verb: "gateway.status" });
			await client.waitForFrame("released");
			const released = client.frames.find((frame) => frame.id === "released")?.result.readiness;
			expect(released.failure?.code).not.toBe("restart_drain_held");
		} finally {
			client?.close();
			await server?.stop();
			server = undefined;
		}
	});
}
