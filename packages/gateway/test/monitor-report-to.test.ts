import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";
import type { GatewayConfig } from "../src/config";
import { DeliveryService } from "../src/delivery/delivery";
import { FunctionalReadiness } from "../src/functional-readiness";
import { MONITOR_AUTHORING_HEADER, MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, sessionPortFromResponder, sessionPortFromScript } from "./session-port.fake";

const target: OriginRef = { platform: "discord", kind: "channel", conversationId: "report-room" };

function eventsFromPrompt(text: string): Array<{ eventId: string; eventType: string; payload?: unknown }> {
	const match = text.match(/\[.*\]$/s);
	if (!match) throw new Error("prompt has no event array");
	return JSON.parse(match[0]) as Array<{ eventId: string; eventType: string; payload?: unknown }>;
}

async function openTestStore() {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-report-to-"));
	const database = await Store.open(join(home, "gateway.db"));
	return { home, database };
}

test("reportTo survives a store reopen on the current schema", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-report-reopen-"));
	const path = join(home, "gateway.db");
	let database: Store | undefined = await Store.open(path);
	try {
		const monitor = new MonitorRegistry(database).add({
			name: "reopen-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["reopen.changed"],
			reportTo: target,
		});
		const schemaVersion = database.schemaVersion;
		database.close();
		database = await Store.open(path);
		expect(database.schemaVersion).toBe(schemaVersion);
		expect(new MonitorRegistry(database).get(monitor.monitorId)).toMatchObject({
			name: "reopen-watch",
			reportTo: target,
		});
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("reportTo persists and updates, and rejects monitor execution origins", async () => {
	const { home, database } = await openTestStore();
	try {
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "reporting",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["report.changed"],
			reportTo: target,
		});

		expect(registry.get(monitor.monitorId)?.reportTo).toEqual(target);
		expect(registry.update({ monitorId: monitor.monitorId, reportTo: null })?.reportTo).toBeNull();
		expect(registry.get(monitor.monitorId)?.reportTo).toBeUndefined();
		expect(registry.update({ monitorId: monitor.monitorId, reportTo: target })?.reportTo).toEqual(target);
		expect(registry.get(monitor.monitorId)?.reportTo).toEqual(target);
		expect(() =>
			registry.update({
				monitorId: monitor.monitorId,
				reportTo: { platform: "monitor", kind: "eventtype", conversationId: "report.changed" },
			}),
		).toThrow("monitor reportTo must target a chat or loopback conversation origin");
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("an authored monitor result is atomically queued and wakes its reportTo conversation", async () => {
	const { home, database } = await openTestStore();
	let propagator: MonitorPropagator | undefined;
	try {
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "deploy-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["deploy.completed"],
			burstPolicy: "serialize",
			reportTo: target,
		});
		const notifications: string[] = [];
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) =>
				JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "Release r-42 merged." }))),
		});
		propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			notifyInbound: async (key) => {
				notifications.push(key);
			},
		});

		const eventId = await propagator.submitAwaitable(monitor.monitorId, "deploy.completed", { pullRequest: 42 });
		const report = database.inbox.pendingOldest(originKey(target));
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored_no_delivery");
		if (!report) throw new Error("monitor result was not queued for its reportTo origin");
		expect(report).toMatchObject({
			message_id: `monitor-report:${eventId}`,
			source: "lane_report",
			origin_key: originKey(target),
			origin_ref_json: JSON.stringify(target),
			state: "pending",
		});
		expect(report.body.startsWith(`Monitor "deploy-watch" result for deploy.completed (${eventId}):`)).toBe(true);
		expect(report.body.endsWith("Release r-42 merged.")).toBe(true);
		expect(notifications).toEqual([originKey(target)]);
	} finally {
		await propagator?.drain();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("reportTo injects into the conversation even when the monitor also delivers its note", async () => {
	const { home, database } = await openTestStore();
	let propagator: MonitorPropagator | undefined;
	try {
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "delivering-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["deploy.delivered"],
			burstPolicy: "serialize",
			channelTarget: { origin: target },
			reportTo: target,
		});
		const notifications: string[] = [];
		const delivered: unknown[] = [];
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) =>
				JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "Merged and announced." }))),
		});
		propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			deliver: (payload) => delivered.push(payload),
			notifyInbound: async (key) => {
				notifications.push(key);
			},
		});

		const eventId = await propagator.submitAwaitable(monitor.monitorId, "deploy.delivered", {});
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");
		expect(database.outbox.rows()).toHaveLength(1);
		expect(delivered).toHaveLength(1);
		expect(database.inbox.pendingOldest(originKey(target))?.message_id).toBe(`monitor-report:${eventId}`);
		expect(notifications).toEqual([originKey(target)]);
	} finally {
		await propagator?.drain();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("reportTo omits silent and contract-only notes rejected by the delivery path", async () => {
	const { home, database } = await openTestStore();
	let propagator: MonitorPropagator | undefined;
	try {
		const registry = new MonitorRegistry(database);
		const instruction = "Inspect deploys and report only verified changes.";
		const monitor = registry.add({
			name: "filtered-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["deploy.silent", "deploy.echo"],
			burstPolicy: "serialize",
			instruction,
			channelTarget: { origin: target },
			reportTo: target,
		});
		const notifications: string[] = [];
		const readiness = new FunctionalReadiness();
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) =>
				JSON.stringify(
					eventsFromPrompt(text).map(({ eventId, eventType }) => ({
						eventId,
						note: eventType === "deploy.silent" ? "[SILENT]" : instruction,
					})),
				),
		});
		propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			notifyInbound: async (key) => {
				notifications.push(key);
			},
			onReadinessFailure: ({ code, detail, originKey, at }) =>
				readiness.recordFailure({ category: "monitor_delivery", code, detail, origin: originKey, at }),
		});

		const silentEventId = await propagator.submitAwaitable(monitor.monitorId, "deploy.silent", {});
		expect(database.monitors.eventGet(silentEventId)?.stage).toBe("authored_no_delivery");
		expect(database.inbox.pendingByOrigin()).toEqual([]);
		for (let index = 0; index < 3; index++) {
			const echoEventId = await propagator.submitAwaitable(monitor.monitorId, "deploy.echo", { index });
			expect(database.monitors.eventGet(echoEventId)?.stage).toBe("contract_unmet");
			expect(database.monitors.failure(echoEventId)?.code).toBe("authored_echo_suspected");
		}
		expect(readiness.snapshot().ready).toBe(false);
		expect(readiness.snapshot().failure).toMatchObject({
			category: "monitor_delivery",
			code: "authored_echo_suspected",
			consecutiveFailures: 3,
		});
		expect(database.inbox.pendingByOrigin()).toEqual([]);

		const recoveryEventId = crypto.randomUUID();
		database.transaction((tx) => {
			database.monitors.eventCreate(tx, {
				eventId: recoveryEventId,
				monitorId: monitor.monitorId,
				eventType: "deploy.silent",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			database.monitors.eventUpdate(tx, recoveryEventId, "authored");
			database.monitors.authoredOutputCreate(tx, recoveryEventId, "[SILENT]");
		});
		await propagator.reconcile();
		expect(database.monitors.eventGet(recoveryEventId)?.stage).toBe("authored_no_delivery");
		expect(database.inbox.pendingByOrigin()).toEqual([]);
		expect(notifications).toEqual([]);
	} finally {
		await propagator?.drain();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("mixed silent and contract-echo batches preserve monitor readiness failure streaks", async () => {
	const { home, database } = await openTestStore();
	let propagator: MonitorPropagator | undefined;
	try {
		const registry = new MonitorRegistry(database);
		const instruction = "Inspect deploys and report only verified changes.";
		const monitor = registry.add({
			name: "mixed-outcomes",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["deploy.changed"],
			burstPolicy: "coalesce",
			instruction,
			channelTarget: { origin: target },
		});
		const readiness = new FunctionalReadiness();
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) =>
				JSON.stringify(
					eventsFromPrompt(text).map(({ eventId, payload }) => ({
						eventId,
						note: (payload as { readonly kind?: string } | undefined)?.kind === "silent" ? "[SILENT]" : instruction,
					})),
				),
		});
		propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			notifyInbound: async () => {},
			onReadinessFailure: ({ code, detail, originKey, at }) =>
				readiness.recordFailure({ category: "monitor_delivery", code, detail, origin: originKey, at }),
			onReadinessSuccess: () => readiness.recordSuccess("monitor_delivery"),
		});

		for (let index = 0; index < 3; index++) {
			const silentEventId = propagator.submit(monitor.monitorId, "deploy.changed", { kind: "silent", index });
			const echoEventId = propagator.submit(monitor.monitorId, "deploy.changed", { kind: "echo", index });
			for (let attempt = 0; attempt < 400; attempt++) {
				if (
					database.monitors.eventGet(silentEventId)?.stage === "authored_no_delivery" &&
					database.monitors.eventGet(echoEventId)?.stage === "contract_unmet"
				) {
					break;
				}
				await Bun.sleep(5);
			}
			expect(database.monitors.eventGet(silentEventId)?.stage).toBe("authored_no_delivery");
			expect(database.monitors.eventGet(echoEventId)?.stage).toBe("contract_unmet");
		}

		expect(readiness.snapshot().ready).toBe(false);
		expect(readiness.snapshot().failure).toMatchObject({
			category: "monitor_delivery",
			code: "authored_echo_suspected",
			consecutiveFailures: 3,
		});
	} finally {
		await propagator?.drain();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("the runtime routes a monitor report into the target conversation's persona session", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-report-session-"));
	const agentDir = join(home, "agent");
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		gjc: { agentDir },
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await Store.open(config.dbPath);
	let server: GatewayServer | undefined;
	let socket: Awaited<ReturnType<typeof Bun.connect>> | undefined;
	const personaPrompts: string[] = [];
	try {
		const sessionPort = sessionPortFromResponder({
			bind: (key, epoch) => `${key}-session-${epoch}`,
			respond: async (_sessionId, text) => {
				if (text.startsWith(MONITOR_AUTHORING_HEADER))
					return JSON.stringify(
						eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "Completed by monitor." })),
					);
				personaPrompts.push(text);
				return "[SILENT]";
			},
		});
		attachTestBrokerOwnership(database, sessionPort, agentDir);
		const monitor = new MonitorRegistry(database).add({
			name: "runtime-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["runtime.completed"],
			burstPolicy: "serialize",
			reportTo: target,
		});
		server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });

		const frames: Array<Record<string, unknown>> = [];
		let buffered = "";
		const connectedSocket = await Bun.connect({
			unix: config.socketPath,
			socket: {
				data(_socket, data) {
					buffered += Buffer.from(data).toString("utf8");
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		socket = connectedSocket;
		const waitForFrame = async (predicate: (frame: Record<string, unknown>) => boolean, label: string) => {
			for (let attempt = 0; attempt < 400; attempt++) {
				const frame = frames.find(predicate);
				if (frame) return frame;
				await Bun.sleep(5);
			}
			throw new Error(`no ${label} frame`);
		};
		connectedSocket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		await waitForFrame((frame) => frame.type === "negotiated", "negotiation");
		connectedSocket.write(
			`${JSON.stringify({ v: "0.1", type: "request", id: "monitor-test", verb: "monitor.test", params: { monitorId: monitor.monitorId } })}\n`,
		);
		const response = await waitForFrame((frame) => frame.id === "monitor-test", "monitor.test");
		expect(response).toMatchObject({ type: "response", result: { eventId: expect.any(String) } });
		for (
			let attempt = 0;
			attempt < 400 && !personaPrompts.some((text) => text.includes("Completed by monitor."));
			attempt++
		)
			await Bun.sleep(5);
		const targetPrompt = personaPrompts.find((text) => text.includes("Completed by monitor."));
		if (!targetPrompt) throw new Error("monitor report never reached the target persona session");
		expect(targetPrompt).toContain("[Internal report from another Gajae session");
		expect(targetPrompt).toContain('Monitor "runtime-watch" result for runtime.completed');
		expect(database.bindings.getSessionRecord(originKey(target))?.sessionId).toBe(`${originKey(target)}-session-0`);
	} finally {
		socket?.end();
		if (server) await server.stop();
		else database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitors without reportTo do not enqueue or wake an internal report", async () => {
	const { home, database } = await openTestStore();
	let propagator: MonitorPropagator | undefined;
	try {
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "plain-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["plain.changed"],
			burstPolicy: "serialize",
		});
		const notifications: string[] = [];
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) =>
				JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "changed" }))),
		});
		propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			notifyInbound: async (key) => {
				notifications.push(key);
			},
		});

		await propagator.submitAwaitable(monitor.monitorId, "plain.changed", {});
		expect(database.inbox.pendingByOrigin()).toEqual([]);
		expect(notifications).toEqual([]);
	} finally {
		await propagator?.drain();
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});
