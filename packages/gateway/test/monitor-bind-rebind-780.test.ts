import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
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
import { MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS } from "../src/store/groups/monitors";
import { attachTestBrokerOwnership, initializeTestBrokerAuthority, sessionPortFromScript } from "./session-port.fake";

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
		const attemptedEpochs: number[] = [];
		let requestAttempts = 0;
		const delivered: ChatMessagePayload[] = [];

		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				bindAttempts++;
				attemptedEpochs.push(input.epoch);
				console.log(`bind attempt ${bindAttempts} for epoch ${input.epoch}`);

				// Fail with timeout for epoch 0, succeed for others
				if (input.epoch === 0) {
					throw new GjcCliUnavailableError("timeout after 40s");
				}

				maxEpochReached = Math.max(maxEpochReached, input.epoch);
				const sessionId = `session-epoch-${input.epoch}`;
				database.transaction((tx) => {
					expect(database.bindings.bindAtEpoch(tx, input.originKey, sessionId, input.epoch)).toBe(true);
				});
				return {
					sessionId,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			// Issue #780 P2 #2: Verify successful delivery after rebind
			request: async (input) => {
				requestAttempts++;
				const eventIdMatch = input.text.match(/"eventId":"([^"]+)"/);
				return {
					receipt: { sessionId: input.sessionId, operationRef: input.opRef },
					assistant: {
						text: JSON.stringify([
							{
								eventId: eventIdMatch ? eventIdMatch[1] : "unknown",
								note: "Rebound session delivered the monitor result.",
							},
						]),
					},
				} as never;
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
			ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
			deliver: (payload) => delivered.push(payload),
		});

		// Submit multiple events - the first three trigger bind timeouts, the fourth succeeds on rebind
		const eventIds: string[] = [];
		for (let i = 0; i < 4; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.consecutive_timeout", {});
			eventIds.push(eventId);

			// Wait for dispatch attempt
			for (let j = 0; j < 200; j++) {
				const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
				if (
					row?.stage === "failed" ||
					row?.stage === "delivered" ||
					row?.stage === "authored" ||
					row?.stage === "authored_no_delivery"
				)
					break;
				await Bun.sleep(10);
			}
		}

		// After 3 consecutive bind timeouts, we should have attempted to rebind to exactly epoch 1 (one rotation)
		expect(bindAttempts).toBe(5);
		expect(maxEpochReached).toBe(1); // Should have rebound to exactly epoch 1 (one rotation)
		expect(attemptedEpochs).toEqual([0, 0, 0, 1, 1]);

		// Issue #780 P2 #2: Verify the fourth event (after rebind) succeeds
		const fourthEventId = eventIds[3];
		if (!fourthEventId) throw new Error("fourth rebind event was not submitted");
		const fourthEventRow = database.monitors.eventRows().find((r) => r.event_id === fourthEventId);
		expect(fourthEventRow?.stage).toMatch(/delivered|authored/);
		expect(database.monitors.authoredOutput(fourthEventId)).toBe("Rebound session delivered the monitor result.");
		expect(delivered.some((payload) => payload.text.includes("Rebound session delivered"))).toBe(true);

		// Verify the safety state shows the rebind was triggered
		const originKey = `monitor/eventtype/test.consecutive_timeout/parent=${monitor.monitorId}`;
		const safetyState = pipeline.sessionSafetyState(originKey);
		// After the rebind attempt on epoch 1, the counter should be reset
		expect(safetyState.bindTimeoutFailures).toBe(0); // Reset after rebind attempt

		// Issue #780 P2 #2: Verify that the session binding persisted at epoch E+1
		const sessionRecord = database.bindings.getSessionRecord(originKey);
		expect(sessionRecord).toEqual({ epoch: 1, sessionId: "session-epoch-1" });

		// Verify successful authoring occurred after rebind
		expect(requestAttempts).toBeGreaterThanOrEqual(1); // At least one successful authoring request

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a stale pending session.create no longer blocks same-process rebind after repeated bind timeouts (#780)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stale-create-rebind-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const agentDir = join(home, "agent");
		const authority = initializeTestBrokerAuthority(database, agentDir);
		const registry = new MonitorRegistry(database);
		const attemptedEpochs: number[] = [];
		let requestAttempts = 0;
		const fakeSessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				attemptedEpochs.push(input.epoch);
				if (input.epoch === 0) throw new GjcCliUnavailableError("broker_unavailable: bind timed out");
				return {
					sessionId: `session-epoch-${input.epoch}`,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				};
			},
			hasPendingSessionCreate: ({ originKey, epoch }) =>
				database.bindings.brokerOperation(authority, "session.create", JSON.stringify([originKey, epoch]))?.state ===
				"pending",
			request: async (input) => {
				requestAttempts++;
				const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1];
				return {
					receipt: { sessionId: input.sessionId, operationRef: input.opRef },
					assistant: { text: JSON.stringify([{ eventId, note: "Fresh epoch is authoring again." }]) },
				} as never;
			},
			resume: async (input) => ({
				sessionId: input.sessionId,
				originKey: input.originKey,
				epoch: input.epoch,
				repo: input.repo,
			}),
			runExclusive: async (_, fn) => fn(),
		};
		const sessionPort = attachTestBrokerOwnership(database, fakeSessionPort as unknown as SessionPort, agentDir);
		const monitor = registry.add({
			name: "stale-create-rebind-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.stale_create_rebind"],
			burstPolicy: "serialize",
		});
		const originKey = `monitor/eventtype/test.stale_create_rebind/parent=${monitor.monitorId}`;
		const staleCreateScope = JSON.stringify([originKey, 0]);
		database.transaction((tx) => {
			database.bindings.beginBrokerOperation(tx, authority, {
				operation: "session.create",
				scope: staleCreateScope,
				operationRef: "gw-bind-stale-create",
				requestJson: JSON.stringify({ cwd: join(home, "workspace") }),
			});
		});

		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		const eventIds: string[] = [];
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.stale_create_rebind", {});
			eventIds.push(eventId);
			for (let attempt = 0; attempt < 200; attempt++) {
				const stage = database.monitors.eventGet(eventId)?.stage;
				if (stage === "failed" || stage === "authored_no_delivery" || stage === "authored") break;
				await Bun.sleep(10);
			}
		}

		expect(attemptedEpochs).toEqual([0, 0, 0, 1]);
		expect(database.bindings.getSessionRecord(originKey)).toEqual({ epoch: 1, sessionId: "session-epoch-1" });
		expect(database.bindings.brokerOperation(authority, "session.create", staleCreateScope)?.state).toBe("pending");
		const recoveredEventId = eventIds[2];
		if (!recoveredEventId) throw new Error("recovery event was not submitted");
		expect(database.monitors.eventGet(recoveredEventId)?.stage).toMatch(/authored|delivered/);
		expect(database.monitors.authoredOutput(recoveredEventId)).toBeDefined();
		expect(requestAttempts).toBe(1);
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
		const escalationMessages: ChatMessagePayload[] = [];
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort: sessionPort as SessionPort,
			memory: { enqueue: () => "intent", enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			deliver: (payload) => {
				if (payload.text.includes("[monitor escalation]")) {
					escalationMessages.push(payload);
				}
			},
			ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
			repo,
			model: undefined,
			serviceTier: undefined,
		});

		// Submit event - bind will timeout once then succeed
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.transient", {});

		// Wait for dispatch attempt
		for (let i = 0; i < 200; i++) {
			const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed" || row?.stage === "delivered" || row?.stage === "authored_no_delivery") break;
			await Bun.sleep(10);
		}

		// Observable behavior assertions: single transient timeout should not cause rotation or escalation
		expect(maxEpoch).toBe(0); // No rotation: still on epoch 0
		expect(escalationMessages.length).toBe(0); // No escalation triggered for single transient failure

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("three bind timeouts trigger exactly one epoch rotation without retryable-stage escalation (issue #780)", async () => {
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

		const attemptedEpochs: number[] = [];
		// Keep timing out after the rebind so the fixture exercises the timeout predicate on both epochs.
		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				attemptedEpochs.push(input.epoch);
				throw new GjcCliUnavailableError("broker_unavailable: bind timed out after 40s");
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

		// Three retryable failures on epoch 0 trigger one rotation; the immediate
		// retry on epoch 1 also times out but must not rotate again yet.
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.escalation", {});

			// Wait for dispatch attempt
			for (let j = 0; j < 200; j++) {
				const row = database.monitors.eventRows().find((r) => r.event_id === eventId);
				if (row?.stage === "failed" || row?.stage === "failed_no_retry") break;
				await Bun.sleep(10);
			}
		}

		expect(attemptedEpochs).toEqual([0, 0, 0, 1]);
		expect(database.bindings.getSessionRecord(originKey)?.epoch).toBe(1);
		const safetyState = pipeline.sessionSafetyState(originKey);

		// Observable behavior: multiple bind timeouts should trigger epoch rotation via rebind logic
		// However, with the P2 #1 fix, escalation only happens on TERMINAL failures, not retryable 'failed' stages.
		// Bind timeouts produce 'failed' stages that are retryable, so no escalation occurs.
		expect(safetyState.escalatedForCurrentStreak).toBe(false);
		expect(deliveredMessages.filter((m) => m.text.includes("[monitor escalation]")).length).toBe(0);

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
			instruction: "Inspect each event and report new evidence.",
			burstPolicy: "serialize",
		});

		const instruction = "Inspect each event and report new evidence.";
		const escalationMessages: ChatMessagePayload[] = [];
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
			request: async (input) => {
				requestCount++;
				const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1];
				const note = requestCount === 4 ? "[SILENT]" : instruction;
				return {
					receipt: { sessionId: input.sessionId, operationRef: input.opRef },
					assistant: { text: JSON.stringify([{ eventId, note }]) },
				} as never;
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
			deliver: (payload) => {
				if (payload.text.includes("[monitor escalation]")) escalationMessages.push(payload);
			},
			ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
			repo,
			model: undefined,
			serviceTier: undefined,
		});

		const originKey = `monitor/eventtype/test.success_clear/parent=${monitor.monitorId}`;

		const escalationKey = `monitor_escalation:${originKey}:${monitor.monitorId}`;
		const dispatchUntil = async (stage: string) => {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.success_clear", {});
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(eventId)?.stage === stage) break;
				await Bun.sleep(10);
			}
			expect(database.monitors.eventGet(eventId)?.stage).toBe(stage);
			return eventId;
		};

		for (let i = 0; i < 3; i++) await dispatchUntil("contract_unmet");
		expect(escalationMessages).toHaveLength(1);
		expect(database.core.metaGet(escalationKey)).toBeDefined();
		expect(pipeline.sessionSafetyState(originKey).escalatedForCurrentStreak).toBe(true);

		await dispatchUntil("authored_no_delivery");
		expect(database.core.metaGet(escalationKey)).toBeUndefined();
		expect(pipeline.sessionSafetyState(originKey).escalatedForCurrentStreak).toBe(false);

		for (let i = 0; i < 3; i++) await dispatchUntil("contract_unmet");
		expect(escalationMessages).toHaveLength(2);
		expect(database.core.metaGet(escalationKey)).toBeDefined();
		expect(requestCount).toBe(7);

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
		let firstBootEscalations = 0;
		let secondBootEscalations = 0;

		// First boot: create terminal failures that trigger exactly one escalation
		{
			const database = await Store.open(dbPath);
			const registry = new MonitorRegistry(database);
			let monitor = registry.get("restart-escalation-test");
			if (!monitor) {
				monitor = registry.add({
					name: "restart-escalation-test",
					trigger: { kind: "cron", schedule: "* * * * *" },
					eventTypes: ["test.restart_escalation"],
					instruction: "Inspect the event and report new evidence.",
					burstPolicy: "serialize",
				});
			}
			persistedMonitorId = monitor.monitorId;

			// Seed terminal failures directly in the database
			for (let i = 0; i < 3; i++) {
				const eventId = randomUUID();
				database.transaction((tx) => {
					database.monitors.eventCreate(tx, {
						eventId,
						monitorId: monitor.monitorId,
						eventType: "test.restart_escalation",
						payloadJson: JSON.stringify({}),
						firedAt: new Date(Date.now() - 1000 * (3 - i)).toISOString(),
					});
					database.monitors.eventUpdate(tx, eventId, "failed_no_retry");
				});
			}

			const sessionPort: Partial<SessionPort> = {
				bind: async (input) => ({
					sessionId: `session-epoch-${input.epoch}`,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				}),
				request: async (input) => {
					const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1];
					return {
						receipt: { sessionId: input.sessionId, operationRef: input.opRef },
						assistant: { text: JSON.stringify([{ eventId, note: "Inspect the event and report new evidence." }]) },
					} as never;
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
					if (payload.text.includes("[monitor escalation]")) {
						deliveredMessages.push(payload);
					}
				},
				ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
				repo: join(home, "workspace"),
				model: undefined,
				serviceTier: undefined,
			});

			// Submit a new event that will trigger the failure check and escalation
			if (!persistedMonitorId) throw new Error("monitor ID not set after first boot");
			const triggerEventId = await pipeline.submitAwaitable(persistedMonitorId, "test.restart_escalation", {});
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(triggerEventId)?.stage === "contract_unmet") break;
				await Bun.sleep(10);
			}
			expect(database.monitors.eventGet(triggerEventId)?.stage).toBe("contract_unmet");

			firstBootEscalations = deliveredMessages.length;
			console.error(`First boot escalations: ${firstBootEscalations}`);
			expect(firstBootEscalations).toBe(1); // Should have exactly one escalation

			database.close();
		}

		// Second boot: with the streak still open, no new escalation should be sent
		{
			const database = await Store.open(dbPath);
			const registry = new MonitorRegistry(database);
			if (!persistedMonitorId) throw new Error("monitor ID not set after first boot");
			const monitor = registry.get(persistedMonitorId);
			if (!monitor) throw new Error("failed to load persisted monitor on restart (Issue #780 P3)");

			const sessionPort: Partial<SessionPort> = {
				bind: async (input) => ({
					sessionId: `session-epoch-${input.epoch}`,
					originKey: input.originKey,
					epoch: input.epoch,
					repo: input.repo,
					startupModelApplied: false,
				}),
				request: async (input) => {
					const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1];
					return {
						receipt: { sessionId: input.sessionId, operationRef: input.opRef },
						assistant: { text: JSON.stringify([{ eventId, note: "Inspect the event and report new evidence." }]) },
					} as never;
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
					if (payload.text.includes("[monitor escalation]")) {
						deliveredMessages.push(payload);
					}
				},
				ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
				repo: join(home, "workspace"),
				model: undefined,
				serviceTier: undefined,
			});

			// Submit another event after restart - should NOT trigger a second escalation
			if (!persistedMonitorId) throw new Error("monitor ID not available for second check");
			const triggerEventId = await pipeline.submitAwaitable(persistedMonitorId, "test.restart_escalation", {});
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(triggerEventId)?.stage === "contract_unmet") break;
				await Bun.sleep(10);
			}
			expect(database.monitors.eventGet(triggerEventId)?.stage).toBe("contract_unmet");

			secondBootEscalations = deliveredMessages.length;
			console.error(`Second boot escalations: ${secondBootEscalations}`);
			expect(secondBootEscalations).toBe(0); // No second escalation (already escalated in first boot)

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

test("three retryable 'failed' events do not trigger escalation (issue #780, P2 #1 - failure-escalation streak fix)", async () => {
	// Regression test: verify that retryable 'failed' stages are excluded from
	// the escalation streak counter. Only terminal failures ('failed_no_retry' and
	// 'contract_unmet') should be counted.
	// The fix is in #countConsecutiveFailures which now checks:
	// if (row.stage !== "failed_no_retry" && row.stage !== "contract_unmet") { return count; }
	// Previously it treated 'failed' as breaking the streak instead of skipping it.

	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retryable-failures-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "retryable-failures-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.retryable_failures"],
			instruction: "Inspect each event and report new evidence.",
			burstPolicy: "serialize",
		});

		const escalationMessages: ChatMessagePayload[] = [];
		const instruction = "Inspect each event and report new evidence.";
		let requestAttempts = 0;

		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => ({
				sessionId: `session-epoch-${input.epoch}`,
				originKey: input.originKey,
				epoch: input.epoch,
				repo: input.repo,
				startupModelApplied: false,
			}),
			request: async (input) => {
				requestAttempts++;
				if (requestAttempts <= 3) throw new Error("retryable authoring failure");
				const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1];
				const note = requestAttempts === 4 ? "[SILENT]" : instruction;
				return {
					receipt: { sessionId: input.sessionId, operationRef: input.opRef },
					assistant: { text: JSON.stringify([{ eventId, note }]) },
				} as never;
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
			deliver: (payload) => {
				if (payload.text.includes("[monitor escalation]")) {
					escalationMessages.push(payload);
				}
			},
			ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
		});

		// Seed a terminal streak without invoking its hook. A retryable dispatch
		// must not cause an alert just because that threshold is already present.
		for (let i = 0; i < 3; i++) {
			const eventId = randomUUID();
			database.transaction((tx) => {
				database.monitors.eventCreate(tx, {
					eventId,
					monitorId: monitor.monitorId,
					eventType: "test.retryable_failures",
					payloadJson: JSON.stringify({}),
					firedAt: new Date(Date.now() - 10_000 + i).toISOString(),
				});
				database.monitors.eventUpdate(tx, eventId, "failed_no_retry");
			});
		}

		const retryableIds: string[] = [];
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.retryable_failures", {});
			retryableIds.push(eventId);
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(eventId)?.stage === "failed") break;
				await Bun.sleep(10);
			}
			const row = database.monitors.eventGet(eventId);
			expect(row?.stage).toBe("failed");
			expect(row?.dispatch_attempts ?? MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS).toBeLessThan(
				MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS,
			);
		}
		expect(escalationMessages).toHaveLength(0);

		// A successful terminal event resets the seeded failure history.
		const recoveryEventId = await pipeline.submitAwaitable(monitor.monitorId, "test.retryable_failures", {});
		for (let attempt = 0; attempt < 200; attempt++) {
			if (database.monitors.eventGet(recoveryEventId)?.stage === "authored_no_delivery") break;
			await Bun.sleep(10);
		}
		expect(database.monitors.eventGet(recoveryEventId)?.stage).toBe("authored_no_delivery");

		// Three actual contract-unmet terminal transitions, after the success
		// boundary, produce exactly one observable escalation.
		const terminalIds: string[] = [];
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.retryable_failures", {});
			terminalIds.push(eventId);
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(eventId)?.stage === "contract_unmet") break;
				await Bun.sleep(10);
			}
			expect(database.monitors.eventGet(eventId)?.stage).toBe("contract_unmet");
		}
		expect(retryableIds).toHaveLength(3);
		expect(terminalIds).toHaveLength(3);
		expect(requestAttempts).toBe(7);
		expect(escalationMessages).toHaveLength(1);
		expect(escalationMessages[0]?.text).toContain("[monitor escalation]");

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
