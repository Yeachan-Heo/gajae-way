import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import type { SessionPort } from "../src/orchestrator/session-port";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

test("bind failure includes operation details in error report", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-bind-fail-"));
	try {
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "bind-fail-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.bind_fail"],
			burstPolicy: "serialize",
			model: { preset: "deepseekmaxxing" },
		});

		const failingPort: Partial<SessionPort> = {
			bind: async () => {
				throw new GjcCliError("session bind failed: model.profile.set failed", 0, "", { code: "operation_failed" });
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: failingPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.bind_fail", {});

		// Let dispatch attempt and fail
		for (let i = 0; i < 100; i++) {
			const row = database.monitorEventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		// Verify failure was recorded
		const failure = database.monitorFailure(eventId);
		expect(failure).toBeDefined();
		expect(failure?.code).toBe("session_bind_failed");

		// Verify the detail includes operation information
		const detail = failure?.detail || "";
		expect(detail).toContain("bind");
		expect(detail).toContain("operation");
		expect(detail).toContain("hasModel");
		expect(detail).not.toContain("SECRET"); // Should not leak secrets

		// Verify sessionId is null (since bind failed before obtaining it)
		expect(detail).toContain('"sessionId":null');

		// Verify origin information is present
		// Extract operation info JSON from detail string
		// The format is: "dispatch phase failed ({code}): {failureDetail JSON} {operation info JSON}"
		// We need to find the last JSON object
		const lastBraceIndex = detail.lastIndexOf("}");
		let openBraceIndex = lastBraceIndex;
		let depth = 1;
		for (let i = lastBraceIndex - 1; i >= 0 && depth > 0; i--) {
			if (detail[i] === "}") depth++;
			if (detail[i] === "{") depth--;
			if (depth === 0) openBraceIndex = i;
		}
		const operationJsonStr = detail.substring(openBraceIndex, lastBraceIndex + 1);
		const detailObj = JSON.parse(operationJsonStr);
		expect(detailObj).toHaveProperty("phase", "bind");
		expect(detailObj).toHaveProperty("origin");
		expect(detailObj.operation).toBe("bind");
		expect(detailObj.operation_args).toBeDefined();
		expect(typeof detailObj.operation_args.hasModel).toBe("boolean");

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("bind failure events are marked as failed (not failed_no_retry) for re-dispatch", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-refirer-"));
	try {
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "refirer-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.refirer"],
			burstPolicy: "serialize",
		});

		const failingPort: Partial<SessionPort> = {
			bind: async () => {
				throw new GjcCliError("session bind failed", 0, "", { code: "operation_failed" });
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: failingPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.refirer", {});

		// Wait for first dispatch attempt to fail
		for (let i = 0; i < 100; i++) {
			const row = database.monitorEventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		const failedRow = database.monitorEventRows().find((r) => r.event_id === eventId);
		// The critical check: dispatch-phase failures mark event as "failed", not "failed_no_retry"
		// This means reconcile WILL re-fire the event
		expect(failedRow?.stage).toBe("failed");
		// dispatch_attempts starts at 0, incremented when dispatch is retried
		expect((failedRow?.dispatch_attempts ?? -1) >= 0).toBe(true);
		// Verify failure is recorded with operation details
		const failure = database.monitorFailure(eventId);
		expect(failure?.code).toBe("session_bind_failed");
		expect(failure?.detail).toContain("operation");

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("setModel failure includes operation details in error report", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-setmodel-fail-"));
	try {
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "setmodel-fail-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.setmodel_fail"],
			burstPolicy: "serialize",
			model: { preset: "invalid-preset" },
		});

		const failingPort: Partial<SessionPort> = {
			bind: async (input) => ({
				sessionId: input.originKey,
				epoch: 0,
				startupModelApplied: false,
				originKey: input.originKey,
				repo: input.repo,
			}),
			setModel: async () => {
				throw new GjcCliError("session bind failed: model.profile.set failed", 0, "", { code: "invalid_preset" });
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: failingPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(database)),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.setmodel_fail", {});

		// Let dispatch attempt and fail
		for (let i = 0; i < 100; i++) {
			const row = database.monitorEventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		// Verify failure was recorded
		const failure = database.monitorFailure(eventId);
		expect(failure).toBeDefined();
		expect(failure?.code).toBe("session_bind_failed");

		// Verify the detail includes operation information
		const detail = failure?.detail || "";
		expect(detail).toContain("setModel");
		expect(detail).toContain("operation");

		// Verify sessionId is present (since bind succeeded)
		expect(detail).not.toContain('"sessionId":null');

		// Verify operation args are included
		// Extract operation info JSON from detail string
		const lastBraceIndex = detail.lastIndexOf("}");
		let openBraceIndex = lastBraceIndex;
		let depth = 1;
		for (let i = lastBraceIndex - 1; i >= 0 && depth > 0; i--) {
			if (detail[i] === "}") depth++;
			if (detail[i] === "{") depth--;
			if (depth === 0) openBraceIndex = i;
		}
		const operationJsonStr = detail.substring(openBraceIndex, lastBraceIndex + 1);
		const detailObj = JSON.parse(operationJsonStr);
		expect(detailObj.operation).toBe("setModel");
		expect(detailObj.operation_args).toBeDefined();
		expect(detailObj.operation_args?.model).toBe("preset:invalid-preset");

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
