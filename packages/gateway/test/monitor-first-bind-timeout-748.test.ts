import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { FunctionalReadiness } from "../src/functional-readiness";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GjcCliUnavailableError, GjcCommandQueueTimeoutError } from "../src/orchestrator/broker";
import type { SessionBindInput, SessionPort } from "../src/orchestrator/session-port";
import { Store } from "../src/store";

function eventIdsFromPrompt(text: string): string[] {
	const match = text.match(/\[.*\]$/s);
	if (!match) throw new Error("prompt has no event array");
	return (JSON.parse(match[0]) as Array<{ eventId: string }>).map(({ eventId }) => eventId);
}

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
			bind: async (_input: SessionBindInput) => {
				bindAttempts++;
				// First bind attempt times out with queue context (simulating pool saturation)
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
		expect(firstFailure?.detail).toBeDefined();
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

test("first-bind degradation survives successful retries until a first-attempt success (#748)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-bind-retry-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "bind-retry-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.bind_retry"],
			burstPolicy: "serialize",
			channelTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } },
		});

		let failNextBind = false;
		let bindAttempts = 0;
		let requestAttempts = 0;
		let firstBindFailureDetail: string | undefined;
		const readinessFailures: Array<{ code: string; detail: string }> = [];
		const readiness = new FunctionalReadiness();
		const retryingPort: Partial<SessionPort> = {
			bind: async (input: SessionBindInput) => {
				bindAttempts++;
				if (failNextBind) {
					failNextBind = false;
					throw new Error(
						'gjc sdk request failed: broker_unavailable (request timed out after 30000ms queue=12 occupancy=8 cliSubcommand="sdk session" queueWaitMs=12000 operation=session.resume)',
					);
				}
				return {
					sessionId: `retry-session-${input.epoch}`,
					epoch: input.epoch,
					originKey: input.originKey,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			request: async (input) => {
				requestAttempts++;
				return {
					assistant: {
						text: JSON.stringify(
							eventIdsFromPrompt(input.text).map((eventId) => ({ eventId, note: "retry recovered" })),
						),
					},
				} as never;
			},
			runExclusive: async (_, fn) => fn(),
		};
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: retryingPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			onReadinessFailure: (failure) => {
				readinessFailures.push(failure);
				readiness.recordFailure({
					category: failure.code === "monitor_bind_degraded" ? "monitor_bind" : "monitor_delivery",
					code: failure.code,
					detail: failure.detail,
					origin: failure.originKey,
					at: failure.at,
				});
			},
			onReadinessSuccess: () => readiness.recordSuccess("monitor_delivery"),
			onFirstAttemptBindSuccess: () => readiness.recordSuccess("monitor_bind"),
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const sessionOrigin = `monitor/eventtype/test.bind_retry/parent=${monitor.monitorId}`;
		for (let i = 0; i < 5; i++) {
			failNextBind = true;
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.bind_retry", { tick: i });
			expect(database.monitors.eventGet(eventId)?.stage).toBe("failed");
			expect(database.monitors.failure(eventId)?.code).toBe("session_bind_failed");
			if (i === 0) firstBindFailureDetail = database.monitors.failure(eventId)?.detail;
			expect(pipeline.sessionSafetyState(sessionOrigin).firstAttemptBindFailures).toBe(i + 1);

			await pipeline.reconcile();

			expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");
			expect(pipeline.sessionSafetyState(sessionOrigin).firstAttemptBindFailures).toBe(i + 1);
			expect(requestAttempts).toBe(i + 1);

			const event = database.monitors.eventGet(eventId);
			const delivery = database.outbox.unsettledRows().find((row) => row.turnId === event?.batch_id);
			expect(delivery).toBeDefined();
			if (!delivery) throw new Error("retry-success delivery was not enqueued");
			database.transaction((tx) => {
				database.outbox.confirm(tx, delivery.deliveryId);
				database.monitors.settleForTurn(tx, delivery.turnId, "delivered");
			});
			readiness.recordSuccess("delivery");
			readiness.recordSuccess("monitor_delivery");
		}

		expect(bindAttempts).toBe(10);
		expect(requestAttempts).toBe(5);
		expect(readinessFailures.find((failure) => failure.code === "monitor_bind_degraded")).toBeDefined();
		expect(readiness.snapshot()).toMatchObject({
			ready: false,
			failure: { category: "monitor_bind", code: "monitor_bind_degraded" },
		});
		const firstAttemptSuccessId = await pipeline.submitAwaitable(monitor.monitorId, "test.bind_retry", { tick: 5 });
		expect(database.monitors.eventGet(firstAttemptSuccessId)?.stage).toBe("authored");
		expect(pipeline.sessionSafetyState(sessionOrigin).firstAttemptBindFailures).toBe(0);
		expect(readiness.snapshot()).toMatchObject({ ready: true, failure: null });
		expect(firstBindFailureDetail).toContain('"operation":"session.resume"');
		expect(firstBindFailureDetail).toContain('"dispatch_operation":"bind"');
		expect(firstBindFailureDetail).toContain('"cliSubcommand":"sdk session"');
		expect(firstBindFailureDetail).toContain('"queueWaitMs":12000');
		expect(firstBindFailureDetail).toContain('"queueLength":12');
		expect(firstBindFailureDetail).toContain('"poolOccupancy":8');

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
			name: "retry-success-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.retry_success"],
			burstPolicy: "serialize",
		});

		const readinessFailures: Array<{ code: string; detail: string }> = [];
		const sessionSafetyStates: Map<string, { firstAttemptBindFailures: number }> = new Map();

		// Track binding behavior: fail 3 times on first attempts, then succeed
		let bindAttempts = 0;
		const flakyPort: Partial<SessionPort> = {
			bind: async (input: SessionBindInput) => {
				bindAttempts++;
				// Fail the first 3 attempts with a timeout
				if (bindAttempts <= 3) {
					const error = new GjcCliUnavailableError("request timed out after 30000ms queue=5 occupancy=4");
					throw error;
				}
				// Then succeed
				return {
					sessionId: `test-session-${input.epoch}`,
					epoch: input.epoch,
					originKey: input.originKey,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			request: async (input) =>
				({
					assistant: {
						text: JSON.stringify(eventIdsFromPrompt(input.text).map((eventId) => ({ eventId, note: "[SILENT]" }))),
					},
				}) as never,
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: flakyPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			onReadinessFailure: (failure) => {
				readinessFailures.push(failure);
				if (failure.code === "monitor_bind_degraded") {
					sessionSafetyStates.set(failure.originKey, { firstAttemptBindFailures: 3 });
				}
			},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const originKey = `monitor/eventtype/test.retry_success/parent=${monitor.monitorId}`;

		// Submit 3 events that will all fail on first attempt
		for (let i = 0; i < 3; i++) {
			await pipeline.submitAwaitable(monitor.monitorId, "test.retry_success", {});
		}

		// Wait for degraded signal
		for (let i = 0; i < 200; i++) {
			const degraded = readinessFailures.find((f) => f.code === "monitor_bind_degraded");
			if (degraded) break;
			await Bun.sleep(10);
		}

		// Verify degraded signal was set after 3 first-attempt failures
		const degradedSignal = readinessFailures.find((f) => f.code === "monitor_bind_degraded");
		expect(degradedSignal).toBeDefined();
		expect(degradedSignal?.detail).toContain("first-attempt bind failed");

		// After 3 first-attempt failures, the counter should be 3
		let safetyState = pipeline.sessionSafetyState(originKey);
		expect(safetyState.firstAttemptBindFailures).toBe(3);

		// Now submit an event that will succeed (it's the 4th bind attempt)
		const successEventId = await pipeline.submitAwaitable(monitor.monitorId, "test.retry_success", {});

		// Wait for the first-attempt dispatch to succeed without delivery.
		for (let i = 0; i < 200; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === successEventId);
			if (row?.stage === "authored_no_delivery") break;
			await Bun.sleep(10);
		}

		expect(bindAttempts).toBe(4);
		expect(database.monitors.eventGet(successEventId)?.stage).toBe("authored_no_delivery");

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

	const error2 = new GjcCommandQueueTimeoutError(30000, 12, 8, 16);
	expect(error2.message).toContain("queue=12");
	expect(error2.message).toContain("occupancy=8/16");
	expect(error2.queueLength).toBe(12);
	expect(error2.poolOccupancy).toBe(8);
	expect(error2.poolSize).toBe(16);

	const error3 = new GjcCommandQueueTimeoutError(30000, 12, 8, 16, "session.resume");
	expect(error3.message).toContain("operation=session.resume");
	expect(error3.operation).toBe("session.resume");

	const unavailable = new GjcCliUnavailableError("request timed out after 30000ms", "session.create");
	expect(unavailable.message).toContain("operation=session.create");
	expect(unavailable.operation).toBe("session.create");
});
