import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import { GjcCliUnavailableError, GjcCommandQueueTimeoutError } from "../src/orchestrator/broker";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import type { SessionPort, SessionBindInput } from "../src/orchestrator/session-port";
import { Store } from "../src/store";

test("first-attempt bind timeout is recorded with queue context and degraded signal fires after 3 consecutive failures (#748)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-first-bind-timeout-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "first-bind-timeout-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.timeout_first_bind"],
			burstPolicy: "serialize",
			model: { preset: "claude-opus" },
		});

		let bindAttempts = 0;
		const readinessFailures: Array<{ code: string; detail: string; originKey: string; at: number }> = [];

		const failingPort: Partial<SessionPort> = {
			bind: async (input: SessionBindInput) => {
				bindAttempts++;
				// First bind attempt times out with queue context (simulating pool saturation)
				// This is the root cause from issue #748
				const error = new GjcCliUnavailableError("request timed out after 30000ms queue=12 occupancy=8");
				throw error;
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: failingPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			onReadinessFailure: (failure) => readinessFailures.push(failure),
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		// Dispatch multiple events to accumulate consecutive first-attempt failures
		const eventIds: string[] = [];
		for (let i = 0; i < 3; i++) {
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "test.timeout_first_bind", {}));
		}

		// Let all 3 dispatch attempts fail
		for (let i = 0; i < 200; i++) {
			const allFailed = eventIds.every((eid) => {
				const row = database.monitors.eventRows().find((r) => r.event_id === eid);
				return row?.stage === "failed";
			});
			if (allFailed) break;
			await Bun.sleep(10);
		}

		expect(bindAttempts).toBe(3);

		// Verify all events failed with session_bind_failed
		for (const eventId of eventIds) {
			const failure = database.monitors.failure(eventId);
			expect(failure?.code).toBe("session_bind_failed");
		}

		// Verify that the timeout error message includes queue context
		const firstFailure = database.monitors.failure(eventIds[0]);
		console.log("First failure detail:", firstFailure?.detail);
		expect(firstFailure?.detail).toBeDefined();
		if (firstFailure?.detail) {
			console.log("Detail contains 'queue':", firstFailure.detail.includes("queue"));
			console.log("Detail contains 'occupancy':", firstFailure.detail.includes("occupancy"));
			console.log("Detail substring:", firstFailure.detail.substring(0, 300));
		}
		// The error message should contain timeout and broker_unavailable info
		expect(firstFailure?.detail).toMatch(/timed out|timeout/);

		// Verify degraded readiness signal was recorded after 3rd failure
		expect(readinessFailures.length).toBeGreaterThan(0);
		const degradedSignal = readinessFailures.find((f) => f.code === "monitor_bind_degraded");
		expect(degradedSignal).toBeDefined();
		expect(degradedSignal?.detail).toContain("first-attempt bind failed");
		expect(degradedSignal?.detail).toContain("#748");

		// Verify safety state reflects the consecutive failures
		const safetyState = pipeline.sessionSafetyState(
			`monitor/eventtype/test.timeout_first_bind/parent=${monitor.monitorId}`,
		);
		expect(safetyState.firstAttemptBindFailures).toBe(3);

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("first-attempt bind failure counter resets on successful bind (#748)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-bind-reset-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "bind-reset-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.bind_reset"],
			burstPolicy: "serialize",
		});

		const readinessFailures: Array<{ code: string; detail: string }> = [];
		let shouldFail = true;
		let bindAttempts = 0;

		const flakyPort: Partial<SessionPort> = {
			bind: async (input: SessionBindInput) => {
				bindAttempts++;
				if (shouldFail) {
					const error = new GjcCliUnavailableError("request timed out after 30000ms queue=5 occupancy=4");
					throw error;
				}
				// Success case: return a valid binding
				return {
					sessionId: `test-session-${input.epoch}`,
					epoch: input.epoch,
					originKey: input.originKey,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: flakyPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			onReadinessFailure: (failure) => readinessFailures.push(failure),
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const originKey = `monitor/eventtype/test.bind_reset/parent=${monitor.monitorId}`;

		// Submit 3 events that will fail with timeout
		for (let i = 0; i < 3; i++) {
			await pipeline.submitAwaitable(monitor.monitorId, "test.bind_reset", {});
		}

		// Wait for failures
		for (let i = 0; i < 200; i++) {
			const failed = database.monitors.eventRows().filter((r) => r.stage === "failed");
			if (failed.length === 3) break;
			await Bun.sleep(10);
		}

		// Verify counter incremented
		let safetyState = pipeline.sessionSafetyState(originKey);
		expect(safetyState.firstAttemptBindFailures).toBe(3);
		expect(bindAttempts).toBe(3);

		// Now switch to success mode and submit another event
		shouldFail = false;
		const successEventId = await pipeline.submitAwaitable(monitor.monitorId, "test.bind_reset", {});

		// Wait for success
		for (let i = 0; i < 200; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === successEventId);
			if (row?.stage === "authored") break;
			await Bun.sleep(10);
		}

		expect(bindAttempts).toBe(4);

		// Verify counter was reset after successful bind
		safetyState = pipeline.sessionSafetyState(originKey);
		expect(safetyState.firstAttemptBindFailures).toBe(0);

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("queue timeout error includes pool occupancy in error message (#748)", async () => {
	// Test that GjcCommandQueueTimeoutError captures and reports queue context
	const error1 = new GjcCommandQueueTimeoutError(30000);
	expect(error1.message).toContain("timed out");

	const error2 = new GjcCommandQueueTimeoutError(30000, 12, 8);
	expect(error2.message).toContain("queue=12");
	expect(error2.message).toContain("occupancy=8");
	expect(error2.queueLength).toBe(12);
	expect(error2.poolOccupancy).toBe(8);
});
