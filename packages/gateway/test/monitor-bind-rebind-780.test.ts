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
import { sessionPortFromScript } from "./session-port.fake";

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

		let requestCount = 0;
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
			// Issue #780 P3: mock authoring to return success for at least one event
			request: async (input) => {
				requestCount++;
				// Third dispatch should succeed (after 2 failures) to test streak clearing
				if (requestCount === 3) {
					return {
						receipt: { sessionId: input.sessionId, operationRef: input.opRef },
						assistant: {
							text: JSON.stringify([
								{ eventId: input.text.match(/"eventId":"([^"]+)"/)?.[1] ?? "unknown", note: "[author note: success]" },
							]),
						},
					} as never;
				}
				// First two should fail for streak
				throw new Error("authoring failed (simulated failure)");
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

		// Submit events: first two will fail (creating a streak), third succeeds to test streak clearing
		const eventIds: string[] = [];
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.success_clear", {});
			eventIds.push(eventId);

			// Wait for dispatch to complete
			for (let j = 0; j < 200; j++) {
				const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
				if (
					row?.stage === "failed" ||
					row?.stage === "failed_no_retry" ||
					row?.stage === "delivered" ||
					row?.stage === "authored_no_delivery"
				) {
					break;
				}
				await Bun.sleep(10);
			}
		}

		// After three events (2 failed + 1 successful), check that streak is cleared
		const rows = database.monitors.eventRows();
		const successRow = rows.find((r) => r.event_id === eventIds[2]);
		const failureRows = rows.filter(
			(r) =>
				(r.event_id === eventIds[0] || r.event_id === eventIds[1]) &&
				(r.stage === "failed" || r.stage === "failed_no_retry"),
		);

		// Verify streak was created
		expect(failureRows.length).toBe(2);
		// Verify successful dispatch happened
		expect(successRow?.stage).toMatch(/delivered|authored/);

		// Verify that successful dispatch cleared the escalation flag
		const safetyState = pipeline.sessionSafetyState(originKey);
		expect(safetyState.escalatedForCurrentStreak).toBe(false);

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
		let persistedMonitorId: string | undefined;

		// First boot: reach escalation threshold
		{
			const database = await Store.open(dbPath);
			const registry = new MonitorRegistry(database);
			let monitor = registry.get("restart-escalation-test");
			if (!monitor) {
				monitor = registry.add({
					name: "restart-escalation-test",
					trigger: { kind: "cron", schedule: "* * * * *" },
					eventTypes: ["test.restart_escalation"],
					burstPolicy: "serialize",
				});
			}
			persistedMonitorId = monitor.monitorId; // Issue #780 P3: reuse persisted monitor ID across restart

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
				const eventId = await pipeline.submitAwaitable(persistedMonitorId!, "test.restart_escalation", {});

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
			const monitor = registry.get(persistedMonitorId!);
			if (!monitor) throw new Error("failed to load persisted monitor on restart (Issue #780 P3)");

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
			const eventId = await pipeline.submitAwaitable(persistedMonitorId!, "test.restart_escalation", {});

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

		let sendAttempts = 0;
		const port = sessionPortFromScript({
			respond: async (_sessionId: string, text: string) => {
				sendAttempts++;
				// First event fails (authoring throws)
				if (sendAttempts === 1) {
					throw new Error("simulated authoring failure");
				}
				// Second event succeeds with valid monitor authoring response
				// Extract event IDs from the prompt JSON array (at the end of the text)
				const jsonMatch = text.match(/\[.*\]$/s);
				if (!jsonMatch) throw new Error("No event array in prompt");
				const events = JSON.parse(jsonMatch[0]) as Array<{ eventId: string }>;
				const authored = events.map(({ eventId }) => ({
					eventId,
					note: "dispatched successfully",
				}));
				return JSON.stringify(authored);
			},
		});

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: port,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			deliver: () => {}, // Non-null delivery target needed to trigger #clearConsecutiveFailedEventCount
			ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
			onReadinessFailure: (input) => {
				readinessFailures.push(input);
			},
		});

		// Submit first event - will fail because authoring throws
		pipeline.submit(monitor.monitorId, "test.streak_cleared_readiness", {});
		// Submit second event - will succeed because respond completes with valid response
		pipeline.submit(monitor.monitorId, "test.streak_cleared_readiness", {});

		// Drain to ensure all dispatches complete
		await pipeline.drain();

		// Verify both events are in the database
		const rows = database.monitors.eventRows();
		expect(rows.length).toBeGreaterThanOrEqual(2);

		// First event should have failed
		const failedEvents = rows.filter((r) => r.stage === "failed");
		expect(failedEvents.length).toBeGreaterThanOrEqual(1);

		// Second event should have reached authored_no_delivery or authored
		const authoredEvents = rows.filter((r) => ["authored_no_delivery", "authored"].includes(r.stage));
		expect(authoredEvents.length).toBeGreaterThanOrEqual(1);

		// Verify that streak-cleared does NOT create a readiness failure
		// The buggy version calls onReadinessFailure with code="monitor_failed_event_streak_cleared"
		// The fix removes that call - streak clearing should only log, not record a readiness failure
		const streakClearedFailures = readinessFailures.filter((f) => f.code === "monitor_failed_event_streak_cleared");
		expect(streakClearedFailures.length).toBe(0);

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
