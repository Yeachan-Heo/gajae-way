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
