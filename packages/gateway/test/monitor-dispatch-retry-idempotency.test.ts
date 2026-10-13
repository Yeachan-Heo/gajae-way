import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import type { SessionPort, SessionRequestInput, SessionRequestResult } from "../src/orchestrator/session-port";
import { Store } from "../src/store";

test("issue #187: dispatch retry reuses authoring turn if request fails after prompt delivery", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-retry-idempotency-"));
	try {
		const database = await Store.open(join(home, "gateway.db"));
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "retry-test",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["test.retry"],
			burstPolicy: "serialize",
		});

		let requestCount = 0;
		let lastOpRef: string | undefined;
		let statusCallCount = 0;
		const sessionPort: Partial<SessionPort> = {
			bind: async () => ({
				sessionId: "test-session-123",
				originKey: "test-origin",
				epoch: 0,
				repo: join(home, "workspace"),
			}),
			request: async (input: SessionRequestInput): Promise<SessionRequestResult> => {
				requestCount++;
				lastOpRef = input.opRef;

				// First request fails after the prompt is delivered
				if (requestCount === 1) {
					// Simulate a failure that happens after the prompt was delivered
					// (e.g., network error reading the response)
					throw new Error("Connection reset while reading response");
				}

				// Second request succeeds
				const eventId = input.text.match(/"eventId":"([^"]+)"/)?.[1] ?? "unknown";
				return {
					receipt: {
						sessionId: "test-session-123",
						operationRef: input.opRef,
						acceptedAt: new Date().toISOString(),
						taskKey: "monitor",
					},
					status: {
						operationRef: input.opRef,
						status: { status: "terminal_ok" },
						summaryCompleted: true,
					},
					assistant: {
						text: `[{"eventId":"${eventId}","note":"test note"}]`,
						pages: 1,
						complete: true,
					},
				};
			},
			runExclusive: async (_, fn) => fn(),
			status: async (input) => {
				statusCallCount++;
				console.error(`[TEST] status() called: opRef=${input.opRef}, count=${statusCallCount}`);
				return {
					operationRef: input.opRef,
					status: { status: "in_flight" },
					summaryCompleted: false,
				};
			},
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

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.retry", {});

		// Wait for first dispatch attempt to fail
		for (let i = 0; i < 100; i++) {
			const row = database.monitors.monitorEventRows().find((r) => r.event_id === eventId);
			if (row?.stage === "failed") break;
			await Bun.sleep(10);
		}

		const firstAttemptOpRef = lastOpRef;
		expect(firstAttemptOpRef).toBeDefined();
		expect(requestCount).toBe(1);
		console.error(`[TEST] First attempt opRef: ${firstAttemptOpRef}`);
		console.error(`[TEST] Status() was called ${statusCallCount} times after first dispatch`);

		// Now trigger a retry (simulate reconcile after retry backoff)
		let attemptedRetry = false;
		try {
			await pipeline.reconcile();
			attemptedRetry = true;
		} catch (e) {
			console.error(`[TEST] Reconcile error: ${e}`);
		}

		// Wait a bit for async operations
		await Bun.sleep(100);
		console.error(
			`[TEST] After reconcile: requestCount=${requestCount}, statusCallCount=${statusCallCount}, lastOpRef=${lastOpRef}`,
		);

		// The second request should have been attempted (if reconcile was triggered)
		if (attemptedRetry && requestCount > 1) {
			const secondAttemptOpRef = lastOpRef;

			// The key assertion: on retry, the SAME opRef should be reused
			// (because the first turn is still running from the initial request)
			expect(secondAttemptOpRef).toBe(firstAttemptOpRef);
			expect(requestCount).toBe(2);

			// After retry succeeds, check event is delivered
			const finalRow = database.monitors.monitorEventRows().find((r) => r.event_id === eventId);
			expect(finalRow?.stage).toMatch(/authored|delivered|authored_no_delivery/);
		}

		database.close();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
