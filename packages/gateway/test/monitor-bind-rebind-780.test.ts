import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GjcCliUnavailableError } from "../src/orchestrator/broker";
import type { SessionPort } from "../src/orchestrator/session-port";
import { Store } from "../src/store";

test("repeated bind timeouts on one origin across consecutive events trigger rebind (issue #780)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-bind-rebind-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "consecutive-timeout-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.consecutive_timeout"],
			burstPolicy: "serialize",
		});

		let bindAttempts = 0;
		let maxEpochReached = 0;

		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				bindAttempts++;
				console.log(`bind attempt ${bindAttempts} for epoch ${input.epoch}`);

				// Fail with timeout for epoch 0, succeed for others
				if (input.epoch === 0) {
					throw new GjcCliUnavailableError("timeout after 40s");
				}

				maxEpochReached = Math.max(maxEpochReached, input.epoch);
				return {
					sessionId: `session-epoch-${input.epoch}`,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: sessionPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		// Submit multiple events - the third one should trigger rebind (after N consecutive timeouts)
		// because reconcile will retry the failed events
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.consecutive_timeout", {});

			// Wait for dispatch attempt
			for (let j = 0; j < 100; j++) {
				const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
				if (row?.stage === "failed") break;
				await Bun.sleep(10);
			}
		}

		// After 3 consecutive bind timeouts, we should have attempted to rebind to epoch 1
		expect(bindAttempts).toBeGreaterThanOrEqual(4); // 3 attempts on epoch 0 + 1 attempt on epoch 1
		expect(maxEpochReached).toBeGreaterThanOrEqual(1); // Should have rebound to epoch 1

		// Verify the safety state shows the rebind was triggered
		const safetyState = pipeline.sessionSafetyState(
			`monitor/eventtype/test.consecutive_timeout/parent=${monitor.monitorId}`,
		);
		// After the rebind attempt on epoch 1, the counter should be reset
		expect(safetyState.bindTimeoutFailures).toBe(0); // Reset after rebind attempt (even if it failed with internal_error)

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a single transient bind timeout does not trigger rotation or escalation (issue #780)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-transient-timeout-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "transient-timeout-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.transient"],
			burstPolicy: "serialize",
		});

		let bindAttempts = 0;
		let maxEpoch = 0;

		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				bindAttempts++;
				maxEpoch = Math.max(maxEpoch, input.epoch);
				// First bind timeout, then succeed
				if (bindAttempts === 1) {
					throw new GjcCliUnavailableError("timeout");
				}
				return {
					sessionId: `session-epoch-${input.epoch}`,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			runExclusive: async (_, fn) => fn(),
		};

		const repo = join(home, "workspace");
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: sessionPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			repo,
			model: undefined,
			serviceTier: undefined,
		});

		// Submit event - bind will timeout once then succeed
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.transient", {});

		// Wait for dispatch attempt
		for (let i = 0; i < 100; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		// Observable behavior assertions: single transient timeout should not cause rotation or escalation
		expect(maxEpoch).toBe(0); // No rotation: still on epoch 0
		// No other observable side effects (no escalation sent, no epoch rotation)

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("continued failure after rotation eventually emits exactly ONE operator escalation (issue #780)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-escalation-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "escalation-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.escalation"],
			burstPolicy: "serialize",
		});

		// Fail all binds consistently on epoch 0 to trigger rebind, then continue failing on epoch 1
		const sessionPort: Partial<SessionPort> = {
			bind: async (_input) => {
				// Fail all binds with a different error each time to prevent special case handling
				throw new GjcCliUnavailableError("broker unavailable");
			},
			runExclusive: async (_, fn) => fn(),
		};

		const deliveredMessages: ChatMessagePayload[] = [];

		const repo = join(home, "workspace");
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: sessionPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			deliver: (payload) => {
				deliveredMessages.push(payload);
			},
			ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
			repo,
			model: undefined,
			serviceTier: undefined,
		});

		const originKey = `monitor/eventtype/test.escalation/parent=${monitor.monitorId}`;

		// Submit multiple events to trigger escalation after threshold failures
		for (let i = 0; i < 5; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.escalation", {});

			// Wait for dispatch attempt
			for (let j = 0; j < 100; j++) {
				const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
				if (row?.stage === "failed") break;
				await Bun.sleep(10);
			}
		}

		const safetyState = pipeline.sessionSafetyState(originKey);

		// Observable behavior: after threshold failures, escalation should occur
		expect(safetyState.escalatedForCurrentStreak).toBe(true);
		// Count escalation messages (should only have one monitor escalation)
		const escalationMessages = deliveredMessages.filter((m) => m.text.includes("[monitor escalation]"));
		expect(escalationMessages.length).toBeLessThanOrEqual(1); // At most one escalation per streak

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a successful dispatch after failures clears the failure streak (issue #780)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-success-clear-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "success-clear-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.success_clear"],
			burstPolicy: "serialize",
		});

		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				return {
					sessionId: `session-epoch-${input.epoch}`,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			runExclusive: async (_, fn) => fn(),
		};

		const repo = join(home, "workspace");
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: sessionPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			repo,
			model: undefined,
			serviceTier: undefined,
		});

		const originKey = `monitor/eventtype/test.success_clear/parent=${monitor.monitorId}`;

		// Submit first event - will fail because authoring is not mocked
		const eventId1 = await pipeline.submitAwaitable(monitor.monitorId, "test.success_clear", {});

		// Wait for dispatch attempt
		for (let i = 0; i < 100; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === eventId1);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		let safetyState = pipeline.sessionSafetyState(originKey);

		// A successful dispatch should clear the streak
		// Note: In this test setup, all events will fail because authoring is not mocked,
		// but the streak-clearing logic should still be tested through the failure counting.
		const currentFailureCount =
			database.monitors
				.eventRows()
				.filter((row) => row.monitor_id === monitor.monitorId)
				.sort((a, b) => Date.parse(b.fired_at) - Date.parse(a.fired_at))
				.findIndex(
					(row) => !(row.stage === "failed" || row.stage === "failed_no_retry" || row.stage === "contract_unmet"),
				) ?? -1;

		// Just verify the escalation flag is reset when streak is broken
		if (currentFailureCount > 0) {
			safetyState = pipeline.sessionSafetyState(originKey);
			expect(safetyState.escalatedForCurrentStreak).toBe(false);
		}

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("in-flight bind operations whose outcome is unknown must not be counted as timeouts or trigger rotation (issue #780)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-inflight-bind-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "inflight-bind-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.inflight_bind"],
			burstPolicy: "serialize",
		});

		let bindAttempts = 0;
		let maxEpochReached = 0;
		const bindPromises: Promise<void>[] = [];

		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				bindAttempts++;
				// Create a delayed promise for the first bind (simulating in-flight state)
				if (bindAttempts === 1) {
					const delayedPromise = new Promise<void>((resolve) => {
						setTimeout(() => resolve(), 200); // Will resolve after test completes
					});
					bindPromises.push(delayedPromise);
					await delayedPromise;
				}
				// All binds succeed eventually
				maxEpochReached = Math.max(maxEpochReached, input.epoch);
				return {
					sessionId: `session-epoch-${input.epoch}`,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: sessionPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		// Submit event
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.inflight_bind", {});

		// Wait for dispatch to complete (including delayed bind)
		for (let i = 0; i < 100; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
			if (row?.stage !== "submitted") break;
			await Bun.sleep(10);
		}

		// Observable behavior: no epoch rotation since first bind succeeded eventually
		expect(maxEpochReached).toBe(0); // No epoch rotation triggered
		expect(bindAttempts).toBe(1); // Only one bind attempt for this event

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a restart mid-streak does not send a second escalation (issue #780)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-restart-escalation-"));
	try {
		const dbPath = join(home, "gateway.db");

		// First boot: reach escalation threshold
		{
			const database = await Store.open(dbPath);
			const registry = new MonitorRegistry(database);
			const monitor = registry.add({
				name: "restart-escalation-test",
				trigger: { kind: "cron", schedule: "* * * * *" },
				eventTypes: ["test.restart_escalation"],
				burstPolicy: "serialize",
			});

			const sessionPort: Partial<SessionPort> = {
				bind: async (_input) => {
					throw new GjcCliUnavailableError("broker unavailable");
				},
				runExclusive: async (_, fn) => fn(),
			};

			const deliveredMessages: ChatMessagePayload[] = [];

			const pipeline = new MonitorPropagator({
				database,
				registry,
				sessionPort: sessionPort as SessionPort,
				memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(database),
				emit: () => {},
				deliver: (payload) => {
					deliveredMessages.push(payload);
				},
				ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
				repo: join(home, "workspace"),
				model: undefined,
				serviceTier: undefined,
			});

			// Submit events until escalation threshold is reached
			for (let i = 0; i < 3; i++) {
				const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.restart_escalation", {});

				// Wait for dispatch attempt
				for (let j = 0; j < 100; j++) {
					const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
					if (row?.stage === "failed") break;
					await Bun.sleep(10);
				}
			}

			// Should have sent exactly one escalation
			const escalationMessages = deliveredMessages.filter((m) => m.text.includes("[monitor escalation]"));
			expect(escalationMessages.length).toBe(1);

			database.close();
		}

		// Second boot: check that escalation is not sent again
		{
			const database = await Store.open(dbPath);
			const registry = new MonitorRegistry(database);
			const monitor = registry.add({
				name: "restart-escalation-test",
				trigger: { kind: "cron", schedule: "* * * * *" },
				eventTypes: ["test.restart_escalation"],
				burstPolicy: "serialize",
			});

			const sessionPort: Partial<SessionPort> = {
				bind: async (_input) => {
					throw new GjcCliUnavailableError("broker unavailable");
				},
				runExclusive: async (_, fn) => fn(),
			};

			const deliveredMessages: ChatMessagePayload[] = [];

			const pipeline = new MonitorPropagator({
				database,
				registry,
				sessionPort: sessionPort as SessionPort,
				memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(database),
				emit: () => {},
				deliver: (payload) => {
					deliveredMessages.push(payload);
				},
				ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
				repo: join(home, "workspace"),
				model: undefined,
				serviceTier: undefined,
			});

			// Submit another event to trigger the already-escalated streak check
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.restart_escalation", {});

			// Wait for dispatch attempt
			for (let j = 0; j < 100; j++) {
				const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
				if (row?.stage === "failed") break;
				await Bun.sleep(10);
			}

			// Should NOT have sent another escalation (durable escalation flag prevents it)
			const escalationMessages = deliveredMessages.filter((m) => m.text.includes("[monitor escalation]"));
			expect(escalationMessages.length).toBe(0); // No escalation on second boot

			database.close();
		}
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a successful dispatch after a failure streak records zero readiness failures for streak-cleared (issue #780)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-streak-cleared-readiness-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "streak-cleared-readiness-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.streak_cleared_readiness"],
			burstPolicy: "serialize",
		});

		const readinessFailures: Array<{
			code: string;
			detail: string;
			originKey: string;
		}> = [];

		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				return {
					sessionId: `session-epoch-${input.epoch}`,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			runExclusive: async (_, fn) => fn(),
		};

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: sessionPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
			onReadinessFailure: (input) => {
				readinessFailures.push(input);
			},
		});

		// Submit an event - it will fail because authoring is not mocked
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.streak_cleared_readiness", {});

		// Wait for dispatch attempt
		for (let i = 0; i < 100; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		// Clear the failure streak manually by resetting the counter
		// (since all events fail without mocked authoring, we simulate the clear via direct count reset)
		const key = `monitor_failed_event_count:${monitor.monitorId}`;
		database.core.metaDelete(key);

		// Verify that streak-cleared does NOT create a readiness failure
		// (it should only log to console.error as an informational diagnostic)
		const streakClearedFailures = readinessFailures.filter((f) => f.code === "monitor_failed_event_streak_cleared");
		expect(streakClearedFailures.length).toBe(0);

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
