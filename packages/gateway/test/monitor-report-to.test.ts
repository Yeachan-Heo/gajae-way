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

test("monitor report is injected as inbound message when reportTo is set and event is settled", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-inject-report-"));
	let database: GatewayDatabase | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);

		const reportToOrigin: OriginRef = {
			platform: "discord",
			kind: "channel",
			conversationId: "channel-123",
		};
		const created = registry.add({
			name: "with-injection",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			reportTo: reportToOrigin,
			enabled: true,
		});

		const { originKey } = await import("@gajae-gateway/protocol");
		const targetOriginKey = originKey(reportToOrigin);

		// Create and settle a monitor event
		const eventId = crypto.randomUUID();
		database.monitorEventCreate({
			eventId,
			monitorId: created.monitorId,
			eventType: "test.event",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});

		// Set the authored output (simulating successful authoring)
		const reportContent = "Monitor report content";
		database.authoredOutputCreate(eventId, reportContent);

		// Update event to authored state
		database.monitorEventUpdate(eventId, "authored", eventId);

		// Simulate injection by calling inboundEnqueue directly
		const { createHash } = await import("node:crypto");
		const messageId = `monitor-report-${createHash("sha256")
			.update(eventId)
			.digest("hex")}`;

		const injected = database.inboundEnqueue({
			messageId,
			originKey: targetOriginKey,
			originRefJson: JSON.stringify(reportToOrigin),
			body: reportContent,
			receivedAt: new Date().toISOString(),
			source: "lane_report",
		});

		expect(injected).toBe(true);

		// Verify the inbound message was created
		const message = database.inboundPendingOldest(targetOriginKey);
		expect(message).toBeDefined();
		expect(message?.message_id).toBe(messageId);
		expect(message?.body).toBe(reportContent);
		expect(message?.origin_key).toBe(targetOriginKey);
		expect(message?.state).toBe("pending");
		expect(message?.source).toBe("lane_report");
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor report is not injected when reportTo is unset", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-no-inject-"));
	let database: GatewayDatabase | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);

		const created = registry.add({
			name: "without-injection",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			enabled: true,
			// reportTo is not set
		});

		const { originKey } = await import("@gajae-gateway/protocol");
		const testOriginKey = originKey({
			platform: "discord",
			kind: "channel",
			conversationId: "channel-456",
		});

		// Create and settle a monitor event
		const eventId = crypto.randomUUID();
		database.monitorEventCreate({
			eventId,
			monitorId: created.monitorId,
			eventType: "test.event",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});

		// Set the authored output
		const reportContent = "Monitor report content";
		database.authoredOutputCreate(eventId, reportContent);

		// Update event to authored state
		database.monitorEventUpdate(eventId, "authored", eventId);

		// Check that no messages were created in the session (0 pending messages)
		const pendingCount = database.inboundPendingCount(testOriginKey);
		expect(pendingCount).toBe(0);
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("monitor report injection is idempotent (same event_id produces same messageId)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-idempotent-"));
	let database: GatewayDatabase | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);

		const reportToOrigin: OriginRef = {
			platform: "discord",
			kind: "channel",
			conversationId: "channel-789",
		};
		const created = registry.add({
			name: "idempotent-test",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["test.event"],
			reportTo: reportToOrigin,
			enabled: true,
		});

		const { originKey } = await import("@gajae-gateway/protocol");
		const targetOriginKey = originKey(reportToOrigin);

		// Create a monitor event
		const eventId = crypto.randomUUID();
		database.monitorEventCreate({
			eventId,
			monitorId: created.monitorId,
			eventType: "test.event",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});

		// Set the authored output
		const reportContent = "Monitor report content";
		database.authoredOutputCreate(eventId, reportContent);

		// Compute messageId deterministically
		const { createHash } = await import("node:crypto");
		const messageId1 = `monitor-report-${createHash("sha256")
			.update(eventId)
			.digest("hex")}`;

		// Inject the report twice
		const injected1 = database.inboundEnqueue({
			messageId: messageId1,
			originKey: targetOriginKey,
			originRefJson: JSON.stringify(reportToOrigin),
			body: reportContent,
			receivedAt: new Date().toISOString(),
			source: "lane_report",
		});

		const injected2 = database.inboundEnqueue({
			messageId: messageId1,
			originKey: targetOriginKey,
			originRefJson: JSON.stringify(reportToOrigin),
			body: reportContent,
			receivedAt: new Date().toISOString(),
			source: "lane_report",
		});

		// First injection should succeed
		expect(injected1).toBe(true);
		// Second injection should fail (duplicate on messageId)
		expect(injected2).toBe(false);

		// Verify only one message exists
		const message = database.inboundPendingOldest(targetOriginKey);
		expect(message).toBeDefined();
		expect(message?.message_id).toBe(messageId1);
		const pendingCount = database.inboundPendingCount(targetOriginKey);
		expect(pendingCount).toBe(1);
	} finally {
		database?.close();
		await rm(home, { recursive: true, force: true });
	}
});
