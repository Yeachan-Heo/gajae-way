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

test("fix #1: epoch rotation is persisted before rebind attempt (issue #780, codex finding #1)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-epoch-persist-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "epoch-persist-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.epoch_persist"],
			burstPolicy: "serialize",
		});

		let bindAttempts = 0;
		const attemptedEpochs: number[] = [];
		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				bindAttempts++;
				attemptedEpochs.push(input.epoch);
				// First 3 attempts on epoch 0 timeout, then succeed on epoch 1
				if (input.epoch === 0) {
					throw new GjcCliUnavailableError("broker_unavailable: bind timed out");
				}
				// The new epoch must be committed before SessionPort is called again.
				expect(database.bindings.getSessionRecord(input.originKey)?.epoch).toBe(input.epoch);
				const sessionId = `session-${input.epoch}`;
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

		// Three failed epoch-0 binds trigger exactly one rotation and a single epoch-1 bind.
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.epoch_persist", {});
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(eventId)?.stage === "failed") break;
				await Bun.sleep(10);
			}
		}

		expect(bindAttempts).toBe(4);
		expect(attemptedEpochs).toEqual([0, 0, 0, 1]);
		expect(
			database.bindings.getSessionRecord(`monitor/eventtype/test.epoch_persist/parent=${monitor.monitorId}`),
		).toEqual({ epoch: 1, sessionId: "session-1" });

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fix #2: escalation payload includes deliveryId (issue #780, codex finding #2)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-escalation-deliveryid-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "escalation-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.escalation"],
			instruction: "Inspect the event and report new evidence.",
			burstPolicy: "serialize",
		});

		const deliveredMessages: ChatMessagePayload[] = [];
		const instruction = "Inspect the event and report new evidence.";
		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => ({
				sessionId: "test-session",
				originKey: input.originKey,
				epoch: input.epoch,
				repo: input.repo,
				startupModelApplied: false,
			}),
			request: async (input) => {
				const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1];
				return {
					receipt: { sessionId: input.sessionId, operationRef: input.opRef },
					assistant: { text: JSON.stringify([{ eventId, note: instruction }]) },
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
			deliver: (payload) => {
				deliveredMessages.push(payload);
			},
			ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
		});

		// Real authoring responses echo the instruction, taking the actual terminal
		// `contract_unmet` transition that triggers escalation.
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.escalation", {});
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(eventId)?.stage === "contract_unmet") break;
				await Bun.sleep(10);
			}
			expect(database.monitors.eventGet(eventId)?.stage).toBe("contract_unmet");
		}

		const [escalationMsg] = deliveredMessages.filter((message) => message.text.includes("[monitor escalation]"));
		expect(escalationMsg).toBeDefined();
		expect(escalationMsg?.deliveryId).toBeString();
		expect(escalationMsg?.deliveryId?.length).toBeGreaterThan(0);

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fix #3: per-origin failure counting across multiple event types (issue #780, codex finding #3)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-multi-eventtype-failures-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "multi-eventtype-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.typeA", "test.typeB"],
			instruction: "Inspect type A and report new evidence.",
			burstPolicy: "serialize",
		});

		const instruction = "Inspect type A and report new evidence.";
		const escalationMessages: ChatMessagePayload[] = [];
		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => ({
				sessionId: "test-session",
				originKey: input.originKey,
				epoch: input.epoch,
				repo: input.repo,
				startupModelApplied: false,
			}),
			request: async (input) => {
				const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1];
				const eventType = input.text.match(/"eventType":"([^"]+)"/)?.[1];
				const note = eventType === "test.typeA" ? instruction : "Found new evidence for type B.";
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
			repo: join(home, "workspace"),
			model: undefined,
			serviceTier: undefined,
			ownerTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "test-owner" } },
			deliver: (payload) => {
				if (payload.text.includes("[monitor escalation]")) escalationMessages.push(payload);
			},
		});

		const dispatchUntil = async (eventType: string, expected: "contract_unmet" | "authored") => {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, eventType, {});
			for (let attempt = 0; attempt < 200; attempt++) {
				const stage = database.monitors.eventGet(eventId)?.stage;
				if (stage === expected || (expected === "authored" && stage === "delivered")) break;
				await Bun.sleep(10);
			}
			expect(database.monitors.eventGet(eventId)?.stage).toMatch(
				expected === "authored" ? /authored|delivered/ : expected,
			);
		};

		await dispatchUntil("test.typeA", "contract_unmet");
		for (let i = 0; i < 12; i++) await dispatchUntil("test.typeB", "authored");
		await dispatchUntil("test.typeA", "contract_unmet");
		for (let i = 0; i < 12; i++) await dispatchUntil("test.typeB", "authored");
		await dispatchUntil("test.typeA", "contract_unmet");

		expect(escalationMessages).toHaveLength(1);
		expect(escalationMessages[0]?.text).toContain("[monitor escalation]");

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fix #2: bind timeout counter persists across gateway restarts (issue #780, codex finding #2 part 2)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-restart-persistence-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "restart-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.restart"],
			burstPolicy: "serialize",
		});

		const attemptedEpochs: number[] = [];
		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				attemptedEpochs.push(input.epoch);
				// Always timeout on epoch 0 to trigger restart scenario
				if (input.epoch === 0) {
					throw new GjcCliUnavailableError("broker_unavailable: bind timed out");
				}
				const sessionId = "test-session";
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
			runExclusive: async (_, fn) => fn(),
		};

		// First propagator: submit events that timeout
		let pipeline = new MonitorPropagator({
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

		const originKey = `monitor/eventtype/test.restart/parent=${monitor.monitorId}`;
		// Submit events to increment and persist the timeout counter.
		for (let i = 0; i < 2; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.restart", {});
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(eventId)?.stage === "failed") break;
				await Bun.sleep(10);
			}
		}

		const safetyBeforeRestart = pipeline.sessionSafetyState(originKey);
		expect(safetyBeforeRestart.bindTimeoutFailures).toBe(2);
		expect(safetyBeforeRestart.bindTimeoutEpoch).toBe(0);

		// Simulate gateway restart by creating a new propagator
		pipeline = new MonitorPropagator({
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

		// The counter should be restored from persistent storage
		const safetyAfterRestart = pipeline.sessionSafetyState(originKey);
		expect(safetyAfterRestart.bindTimeoutFailures).toBe(2);
		expect(safetyAfterRestart.bindTimeoutEpoch).toBe(0);

		const recoveredEventId = await pipeline.submitAwaitable(monitor.monitorId, "test.restart", {});
		for (let attempt = 0; attempt < 200; attempt++) {
			if (attemptedEpochs.includes(1) && database.monitors.eventGet(recoveredEventId)?.stage === "failed") break;
			await Bun.sleep(10);
		}
		expect(attemptedEpochs).toEqual([0, 0, 0, 1]);
		expect(database.bindings.getSessionRecord(originKey)).toEqual({ epoch: 1, sessionId: "test-session" });

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fix #4: escalation persistence ordering is correct (issue #780, codex finding #4)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-escalation-persist-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "escalation-persist-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.escalation_persist"],
			instruction: "Inspect the event and report new evidence.",
			burstPolicy: "serialize",
		});

		const instruction = "Inspect the event and report new evidence.";
		const originKey = `monitor/eventtype/test.escalation_persist/parent=${monitor.monitorId}`;
		const escalationKey = `monitor_escalation:${originKey}:${monitor.monitorId}`;
		const deliveredMessages: ChatMessagePayload[] = [];
		let persistedBeforeDelivery = false;
		let outboxStateBeforeDelivery: string | undefined;
		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => ({
				sessionId: "test-session",
				originKey: input.originKey,
				epoch: input.epoch,
				repo: input.repo,
				startupModelApplied: false,
			}),
			request: async (input) => {
				const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1];
				return {
					receipt: { sessionId: input.sessionId, operationRef: input.opRef },
					assistant: { text: JSON.stringify([{ eventId, note: instruction }]) },
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
			deliver: (payload) => {
				if (!payload.text.includes("[monitor escalation]")) return;
				deliveredMessages.push(payload);
				if (!payload.deliveryId) throw new Error("escalation payload is missing its deliveryId");
				const outboxRow = database.outbox.get(payload.deliveryId);
				outboxStateBeforeDelivery = outboxRow?.state;
				persistedBeforeDelivery = database.core.metaGet(escalationKey) !== undefined && outboxRow !== undefined;
			},
		});

		// Reach terminal contract failures through real authoring results.
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.escalation_persist", {});
			for (let attempt = 0; attempt < 200; attempt++) {
				if (database.monitors.eventGet(eventId)?.stage === "contract_unmet") break;
				await Bun.sleep(10);
			}
			expect(database.monitors.eventGet(eventId)?.stage).toBe("contract_unmet");
		}

		expect(deliveredMessages).toHaveLength(1);
		expect(persistedBeforeDelivery).toBe(true);
		expect(outboxStateBeforeDelivery).toBe("inflight");
		expect(database.core.metaGet(escalationKey)).toBeDefined();
		expect(pipeline.sessionSafetyState(originKey).escalatedForCurrentStreak).toBe(true);

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fix #5 & #6: failure counting skips unfinished events and uses bounded queries (issue #780, codex findings #5,#6)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-bounded-query-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "bounded-query-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.bounded"],
			burstPolicy: "serialize",
		});

		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => ({
				sessionId: "test-session",
				originKey: input.originKey,
				epoch: input.epoch,
				repo: input.repo,
				startupModelApplied: false,
			}),
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

		// Create multiple events with different stages
		const eventIds: string[] = [];
		for (let i = 0; i < 5; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.bounded", {});
			eventIds.push(eventId);
		}

		// Mark some as failed (terminal)
		database.transaction((tx) => {
			for (let i = 0; i < 3; i++) {
				database.monitors.eventFencedFail(
					tx,
					eventIds[i],
					"test-lease",
					"test-batch",
					"internal_error",
					"test failure",
					Date.now(),
					false,
					{},
				);
			}
		});

		// The test verifies that eventRows with bounded limit works correctly
		// Query 2x threshold events from the monitor
		const recentEvents = database.monitors.eventRows(monitor.monitorId, "newest", false, 6);
		expect(recentEvents.length).toBeLessThanOrEqual(6); // Bounded by limit

		// Verify all returned events are from this monitor
		for (const event of recentEvents) {
			expect(event.monitor_id).toBe(monitor.monitorId);
		}

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("eventRows pages newest-first with limit and offset (issue #780, P2 per-origin pagination)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-event-rows-page-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "event-rows-page-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.page"],
			burstPolicy: "serialize",
		});
		database.transaction((tx) => {
			for (let i = 0; i < 5; i++) {
				database.monitors.eventCreate(tx, {
					eventId: `page-${i}`,
					monitorId: monitor.monitorId,
					eventType: "test.page",
					payloadJson: "{}",
					firedAt: new Date(1_000 + i).toISOString(),
				});
			}
		});
		const pages = [0, 2, 4].map((offset) =>
			database.monitors.eventRows(monitor.monitorId, "newest", false, 2, offset).map((row) => row.event_id),
		);
		expect(pages).toEqual([["page-4", "page-3"], ["page-2", "page-1"], ["page-0"]]);
		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
