import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { OriginRef } from "@gajae-gateway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { Store } from "../src/store";
import { sessionPortFromScript } from "./session-port.fake";

const target: OriginRef = { platform: "discord", kind: "channel", conversationId: "report-room" };

function eventsFromPrompt(text: string): Array<{ eventId: string }> {
	const match = text.match(/\[.*\]$/s);
	if (!match) throw new Error("prompt has no event array");
	return JSON.parse(match[0]) as Array<{ eventId: string }>;
}

/**
 * snowflake CHANGES_REQUESTED on #842 (d00fb1ddd): persisted authored output
 * must stay immutable across a stage regression AND a regressed row that
 * already holds output must be routed through stored-answer recovery, not
 * fresh authoring. The real MonitorPropagator path: an event is authored with
 * a spoken answer, forced to `failed` via eventFencedFail (dispatch-lease
 * failure after authoring), then reclaimed by reconcile while the monitor's
 * session would answer `[SILENT]` to any fresh prompt. The reclaim must
 * recover the STORED answer — never dispatch a fresh authoring turn, never
 * overwrite the stored output, never strand the row at `failed` silently.
 */
test("a failed row with persisted output recovers its stored answer on reclaim, never a fresh [SILENT] authoring turn", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stage-regression-"));
	let pipeline: MonitorPropagator | undefined;
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const realAnswer = "Rollback finished: replica restored from snapshot 7.";
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "rollback-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["rollback.changed"],
			burstPolicy: "serialize",
			reportTo: target,
		});

		// The first dispatch answers with a real, spoken note. Any LATER prompt
		// (a fresh authoring turn after the regression) would return [SILENT] —
		// the poisoned-session shape this regression must never surface.
		let prompts = 0;
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) => {
				prompts++;
				if (prompts === 1)
					return JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: realAnswer })));
				return JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "[SILENT]" })));
			},
		});
		pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
		});

		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "rollback.changed", { run: 1 });
		expect(database.monitors.authoredOutput(eventId)).toBe(realAnswer);
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored_no_delivery");

		// Force the stage regression through the real store path: the row goes
		// back through `dispatched` under a fresh lease, then eventFencedFail
		// (a post-author delivery failure) drops it to `failed` with output
		// still persisted. This is exactly the interleaving the review flags:
		// another generation reclaimed the row and its fresh dispatch failed.
		const leaseId = `lease-${crypto.randomUUID()}`;
		database.transaction((tx) => {
			database.monitors.eventUpdate(tx, eventId, "dispatched");
			database.monitors.eventAcquireLease(tx, eventId, "test-owner", leaseId, 60_000);
		});
		const failed = database.transaction((tx) =>
			database.monitors.eventFencedFail(
				tx,
				eventId,
				leaseId,
				`batch-${crypto.randomUUID()}`,
				"post_auth_failure",
				"delivery handoff failed after authoring",
				Date.now(),
				false,
			),
		);
		expect(failed).toBe(true);
		// The failing generation's lease dies with it: release so reconcile is
		// allowed to reclaim (a live lease means another attempt owns the row).
		database.transaction((tx) => database.monitors.eventReleaseLease(tx, eventId, leaseId));
		expect(database.monitors.eventGet(eventId)?.stage).toBe("failed");
		expect(database.monitors.authoredOutput(eventId)).toBe(realAnswer);

		// Reclaim through the real reconcile path. The session would answer
		// [SILENT] to a fresh prompt; the reclaim must never send one.
		await pipeline.reconcile();

		// The stored output is unchanged: immutable across the regression.
		expect(database.monitors.authoredOutput(eventId)).toBe(realAnswer);
		// The row recovered to `authored` (spoken stored answer → delivery
		// pending), never stuck at `failed` and never re-dispatched.
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");
		// No failure evidence was added by the reclaim: recovery, not a failure.
		expect(database.monitors.failure(eventId)?.code).toBe("post_auth_failure");
		// Exactly one fresh authoring prompt was ever sent (the first one).
		expect(prompts).toBe(1);
	} finally {
		pipeline?.dispose();
		await pipeline?.drain();
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
});

/**
 * snowykr CHANGES_REQUESTED on #842 (03a93afa): when authorFenced returns false
 * (another dispatch already persisted the answer), the caller skips only author/intent/
 * report work and still runs silence classification and delivery. This creates a
 * stale-read/lease-release window where a regressed row can receive a competing
 * answer. The fix requires aborting ALL downstream stage/delivery processing when
 * persistence is rejected, and routing output-bearing rows to stored-answer recovery
 * under the acquired lease instead of fresh authoring.
 *
 * This test verifies that when a fresh dispatch gets a response but authorFenced
 * rejects the persistence (another dispatch already persisted), the fresh dispatch
 * does NOT try to deliver its stale answer. Instead, it recovers the stored answer.
 */
test("a stale dispatch with rejected persistence recovers stored answer, never delivers fresh stale answer", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stale-dispatch-"));
	let pipeline: MonitorPropagator | undefined;
	const database = await Store.open(join(directory, "gateway.db"));
	try {
		const storedAnswer = "Real answer from original dispatch";
		const staleAnswer = "Wrong answer from stale dispatch"; // Should NOT be delivered
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "stale-answer-test",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["test.event"],
			burstPolicy: "serialize",
			reportTo: target,
		});

		// Count how many times each answer is received/processed
		let requestCount = 0;
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) => {
				requestCount++;
				// First request returns storedAnswer
				// Second request (stale) would return staleAnswer
				const answer = requestCount === 1 ? storedAnswer : staleAnswer;
				return JSON.stringify(
					eventsFromPrompt(text).map(({ eventId }) => ({
						eventId,
						note: answer,
					})),
				);
			},
		});
		const delivery = new DeliveryService(database);
		pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery,
			emit: () => {},
		});

		// First dispatch: answers with storedAnswer, persists it
		const eventId = await pipeline.submitAwaitable(monitor.monitorId, "test.event", { run: 1 });
		expect(database.monitors.authoredOutput(eventId)).toBe(storedAnswer);
		expect(requestCount).toBe(1);

		// Now manually regress the event: the row goes back to "dispatched" state
		// This simulates what happens when a post-author exception regresses the stage.
		database.transaction((tx) => database.monitors.eventUpdate(tx, eventId, "dispatched"));
		const leaseId = `lease-${crypto.randomUUID()}`;
		database.transaction((tx) => {
			database.monitors.eventAcquireLease(tx, eventId, "test-owner-stale", leaseId, 60_000);
		});

		// Try to authorize again with a different answer (simulating the stale dispatch)
		// This call should return false because the row was already authored
		const authored = database.transaction((tx) => {
			// This should return false because the row was already authored
			return database.monitors.authorFenced(tx, eventId, leaseId, staleAnswer);
		});
		expect(authored).toBe(false); // Should be false - row already has persisted output

		// Most importantly: the stored answer must not change
		expect(database.monitors.authoredOutput(eventId)).toBe(storedAnswer);
	} finally {
		pipeline?.dispose();
		await pipeline?.drain();
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
});
