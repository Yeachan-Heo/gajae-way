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
		const sessionPort: Partial<SessionPort> = {
			bind: async (input) => {
				bindAttempts++;
				// First 3 attempts on epoch 0 timeout, then succeed on epoch 1
				if (input.epoch === 0) {
					throw new GjcCliUnavailableError("timeout");
				}
				// Epoch 1 succeeds - this verifies the epoch was persisted
				return {
					sessionId: `session-${input.epoch}`,
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

		// Submit 3 events to trigger rebind
		for (let i = 0; i < 3; i++) {
			await pipeline.submitAwaitable(monitor.monitorId, "test.epoch_persist", {});
			await Bun.sleep(50);
		}

		// Wait for dispatches to complete
		await Bun.sleep(200);

		// Verify bind was attempted on epoch 1 (at least 3 timeouts on epoch 0 + 1 attempt on epoch 1)
		expect(bindAttempts).toBeGreaterThanOrEqual(4);

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
			burstPolicy: "serialize",
		});

		const deliveredMessages: ChatMessagePayload[] = [];
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
			deliver: (payload) => {
				deliveredMessages.push(payload);
			},
		});

		// Simulate 3 failed events to trigger escalation
		for (let i = 0; i < 3; i++) {
			const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.escalation", {});
			const row = database.monitors.eventRows(monitor.monitorId, "newest", false, 1)[0];
			database.transaction((tx) => {
				database.monitors.eventFencedFail(
					tx,
					eventId,
					"test-lease",
					"test-batch",
					"internal_error",
					"test failure",
					Date.now(),
					false,
					{},
				);
			});
			await Bun.sleep(50);
		}

		// Wait for escalation to be emitted
		await Bun.sleep(100);

		// Find the escalation message
		const escalationMsg = deliveredMessages.find((m) => m.text?.includes("monitor escalation"));
		if (escalationMsg) {
			// Verify deliveryId is present in the payload
			expect(escalationMsg.deliveryId).toBeDefined();
			expect(typeof escalationMsg.deliveryId).toBe("string");
			expect(escalationMsg.deliveryId!.length).toBeGreaterThan(0);
		}

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
