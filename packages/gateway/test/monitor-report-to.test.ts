import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef } from "@gajae-gateway/protocol";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";

test("monitor reportTo field is set and persisted", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-report-to-"));
	let database: GatewayDatabase | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const reportTo: OriginRef = {
			platform: "discord",
			kind: "channel",
			conversationId: "123456789",
		};
		const created = registry.add({
			name: "with-report-to",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			reportTo,
			enabled: true,
		});

		const retrieved = registry.get(created.monitorId);
		expect(retrieved?.reportTo).toEqual(reportTo);
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor reportTo can be updated", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-report-to-update-"));
	let database: GatewayDatabase | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const created = registry.add({
			name: "report-to-updatable",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			enabled: true,
		});

		const reportTo: OriginRef = {
			platform: "slack",
			kind: "channel",
			conversationId: "C123456789",
		};
		const updated = registry.update({ monitorId: created.monitorId, reportTo });

		expect(updated?.reportTo).toEqual(reportTo);
		const retrieved = registry.get(created.monitorId);
		expect(retrieved?.reportTo).toEqual(reportTo);
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor reportTo is optional (null)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-no-report-to-"));
	let database: GatewayDatabase | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const created = registry.add({
			name: "no-report-to",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			enabled: true,
		});

		expect(created.reportTo).toBeUndefined();
		const retrieved = registry.get(created.monitorId);
		expect(retrieved?.reportTo).toBeNull();
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor reportTo configuration is persisted and can be updated to null", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-report-to-null-"));
	let database: GatewayDatabase | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const reportTo: OriginRef = {
			platform: "discord",
			kind: "channel",
			conversationId: "123456789",
		};
		const created = registry.add({
			name: "with-report-to",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			reportTo,
			enabled: true,
		});

		const withReportTo = registry.get(created.monitorId);
		expect(withReportTo?.reportTo).toEqual(reportTo);

		// Clear reportTo by updating to null
		const cleared = registry.update({ monitorId: created.monitorId, reportTo: null });
		expect(cleared?.reportTo).toBeNull();
		const retrievedAfterClear = registry.get(created.monitorId);
		expect(retrievedAfterClear?.reportTo).toBeNull();
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor reportTo validates origin ref", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-report-to-validation-"));
	let database: GatewayDatabase | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);

		// Invalid origin ref should throw
		try {
			registry.add({
				name: "invalid-report-to",
				trigger: { kind: "cron", schedule: "0 * * * *" },
				eventTypes: ["test.event"],
				reportTo: {
					platform: "invalid-platform" as never,
					kind: "channel",
					conversationId: "123",
				},
				enabled: true,
			});
			throw new Error("Expected validation error");
		} catch (error) {
			expect((error as Error).message).toContain("unknown platform");
		}
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor event settling with reportTo injects exactly one internal report (delivered path)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-report-delivered-"));
	try {
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);

		const reportToOrigin: OriginRef = {
			platform: "discord",
			kind: "channel",
			conversationId: "channel-delivered",
		};
		const created = registry.add({
			name: "report-delivered",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			reportTo: reportToOrigin,
			enabled: true,
			burstPolicy: "serialize",
		});

		const { originKey } = await import("@gajae-gateway/protocol");
		const targetOriginKey = originKey(reportToOrigin);

		// Set up propagator to process events
		const { DeliveryService } = await import("../src/delivery/delivery");
		const { DeliveryLedger } = await import("../src/store/ledger");
		const { MonitorPropagator } = await import("../src/monitors/propagate");
		const { sessionPortFromScript } = await import("./session-port.fake");

		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "test-session" }),
			respond: async (_id: string, text: string) => {
				const parsed = JSON.parse(text.match(/\[.*\]$/s)?.[0] ?? "[]");
				return JSON.stringify(parsed.map(({ eventId }: { eventId: string }) => ({ eventId, note: "processed" })));
			},
		});

		const memory = {
			enqueue: () => crypto.randomUUID(),
			enqueueExistingId: () => {},
		};

		const propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: memory as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});

		// Submit event and wait for settlement
		const eventId = await propagator.submitAwaitable(created.monitorId, "test.event", { test: "payload" });

		// Verify exactly one report was injected
		const message = database.inboundPendingOldest(targetOriginKey);
		expect(message).toBeDefined();
		expect(message?.source).toBe("lane_report");
		expect(message?.state).toBe("pending");
		const pendingCount = database.inboundPendingCount(targetOriginKey);
		expect(pendingCount).toBe(1);

		// Verify the message id is deterministic from event_id
		const { createHash } = await import("node:crypto");
		const expectedMessageId = `monitor-report-${createHash("sha256").update(eventId).digest("hex")}`;
		expect(message?.message_id).toBe(expectedMessageId);

		propagator.dispose();
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor event settling without reportTo does not create any report", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-no-reportto-"));
	try {
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);

		const created = registry.add({
			name: "no-reportto",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			enabled: true,
			burstPolicy: "serialize",
			// reportTo is explicitly unset
		});

		const { originKey } = await import("@gajae-gateway/protocol");
		const arbitraryOrigin: OriginRef = {
			platform: "discord",
			kind: "channel",
			conversationId: "arbitrary-channel",
		};
		const testOriginKey = originKey(arbitraryOrigin);

		// Set up propagator
		const { DeliveryService } = await import("../src/delivery/delivery");
		const { DeliveryLedger } = await import("../src/store/ledger");
		const { MonitorPropagator } = await import("../src/monitors/propagate");
		const { sessionPortFromScript } = await import("./session-port.fake");

		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "test-session" }),
			respond: async (_id: string, text: string) => {
				const parsed = JSON.parse(text.match(/\[.*\]$/s)?.[0] ?? "[]");
				return JSON.stringify(parsed.map(({ eventId }: { eventId: string }) => ({ eventId, note: "processed" })));
			},
		});

		const memory = {
			enqueue: () => crypto.randomUUID(),
			enqueueExistingId: () => {},
		};

		const propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: memory as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});

		// Submit and settle an event
		await propagator.submitAwaitable(created.monitorId, "test.event", { test: "payload" });

		// Verify NO message was created in any origin
		const pendingCount = database.inboundPendingCount(testOriginKey);
		expect(pendingCount).toBe(0);

		// Also verify no messages in other origins
		expect(database.inboundPendingOrigins()).toHaveLength(0);

		propagator.dispose();
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor report injection is idempotent: re-settling same event adds no duplicate", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-idempotent-"));
	try {
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);

		const reportToOrigin: OriginRef = {
			platform: "discord",
			kind: "channel",
			conversationId: "channel-idempotent",
		};
		const created = registry.add({
			name: "idempotent-test",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			reportTo: reportToOrigin,
			enabled: true,
			burstPolicy: "serialize",
		});

		const { originKey } = await import("@gajae-gateway/protocol");
		const targetOriginKey = originKey(reportToOrigin);

		// Set up propagator
		const { DeliveryService } = await import("../src/delivery/delivery");
		const { DeliveryLedger } = await import("../src/store/ledger");
		const { MonitorPropagator } = await import("../src/monitors/propagate");
		const { sessionPortFromScript } = await import("./session-port.fake");

		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "test-session" }),
			respond: async (_id: string, text: string) => {
				const parsed = JSON.parse(text.match(/\[.*\]$/s)?.[0] ?? "[]");
				return JSON.stringify(parsed.map(({ eventId }: { eventId: string }) => ({ eventId, note: "processed" })));
			},
		});

		const memory = {
			enqueue: () => crypto.randomUUID(),
			enqueueExistingId: () => {},
		};

		const propagator = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: memory as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
		});

		// Submit event and wait for settlement
		const eventId = await propagator.submitAwaitable(created.monitorId, "test.event", { test: "payload" });

		// Verify message was created
		const initialMessage = database.inboundPendingOldest(targetOriginKey);
		expect(initialMessage).toBeDefined();
		const initialCount = database.inboundPendingCount(targetOriginKey);
		expect(initialCount).toBe(1);
		const messageId = initialMessage?.message_id;

		// Get the event to verify it was settled (to either delivered or authored_no_delivery)
		const eventRow = database.monitorEventGet(eventId);
		expect(eventRow).toBeDefined();
		expect(["delivered", "authored_no_delivery"]).toContain(eventRow?.stage ?? "");

		// Try to settle the same event again by calling the inject directly
		// (In practice this would happen through reconcile on a retry)
		if (eventRow) {
			database.inboundEnqueue({
				messageId: messageId!,
				originKey: targetOriginKey,
				originRefJson: JSON.stringify(reportToOrigin),
				body: initialMessage?.body ?? "test",
				receivedAt: new Date().toISOString(),
				source: "lane_report",
			});
		}

		// Verify no duplicate was created (UNIQUE constraint on message_id prevents it)
		const finalCount = database.inboundPendingCount(targetOriginKey);
		expect(finalCount).toBe(1);

		const finalMessage = database.inboundPendingOldest(targetOriginKey);
		expect(finalMessage?.message_id).toBe(messageId);

		propagator.dispose();
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
