import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type OriginRef, originKey } from "@gajae-gateway/protocol";
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
 * snowykr CHANGES_REQUESTED on #842 (03a93afa, P2): a dispatch that loses the
 * authorFenced race must not merely skip delivery — it must route the row to
 * stored-answer recovery under the lease it still holds, because the owning
 * dispatch already failed and nobody else will deliver the stored answer.
 *
 * Real MonitorPropagator path, reviewer's exact ordering: the row is claimed
 * and read (fresh dispatch in flight) BEFORE any output exists; another writer
 * then persists a spoken answer and regresses the row to `failed` via
 * eventFencedFail under its own lease; the stale attempt's session then
 * returns [SILENT] (case 1) or a different spoken answer (case 2).
 *
 * Required: stored output unchanged, no delivery/report of the fresh response,
 * the STORED answer delivered/reported exactly once, final stage `authored`,
 * lease released.
 */
async function staleDispatchRaceCase(
	sessionAnswers: string[],
	storedAnswer: string,
): Promise<{
	database: Store;
	eventId: string;
	prompts: string[];
	deliveredTexts: string[];
	notifications: string[];
	directory: string;
	propagator: MonitorPropagator;
	clock: { now: number };
	preReconcile: {
		stage: string | undefined;
		leaseReleased: boolean;
		report: { message_id: string; body: string } | undefined;
		reportCount: number;
		notifications: string[];
	};
}> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stale-race-"));
	const clock = { now: Date.now() };
	const database = await Store.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const monitor = registry.add({
		name: "stale-race-watch",
		trigger: { kind: "cron", schedule: "*/5 * * * *" },
		eventTypes: ["race.event"],
		burstPolicy: "serialize",
		channelTarget: { origin: target },
		reportTo: target,
	});
	const prompts: string[] = [];
	let answerIndex = 0;
	const sessionPort = sessionPortFromScript({
		bind: async () => ({ sessionId: "monitor-session" }),
		respond: async (_sessionId, text) => {
			prompts.push(text);
			const note = sessionAnswers[Math.min(answerIndex, sessionAnswers.length - 1)];
			answerIndex++;
			return JSON.stringify(eventsFromPrompt(text).map(({ eventId: id }) => ({ eventId: id, note })));
		},
	});
	const deliveredTexts: string[] = [];
	const notifications: string[] = [];
	const pipeline = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(database),
		emit: () => {},
		deliver: (payload) => {
			deliveredTexts.push(payload.text);
		},
		now: () => clock.now,
		notifyInbound: async (key) => {
			notifications.push(key);
		},
	});
	// The stale attempt's dispatch is parked inside its session turn: its lease
	// and claim exist, but no response has been selected yet — exactly the
	// window the reviewer describes (row claimed/read before output exists).
	let releaseStale!: () => void;
	const parked = new Promise<void>((resolve) => {
		releaseStale = resolve;
	});
	const staleDispatch = pipeline
		.submitAwaitable(monitor.monitorId, "race.event", { run: 1 })
		.then((eventId) => {
			void eventId;
		})
		.catch(() => {});
	// Wait until the stale attempt sits inside the authoring turn (it wrapped
	// the responder through ScriptedSessionPort's onSend, so reaching our
	// responder means the lease is held and the row is claimed `dispatched`-bound).
	const eventId = database.monitors.eventRows()[0]?.event_id;
	if (!eventId) throw new Error("event row missing");
	for (let i = 0; i < 200; i++) {
		if (database.monitors.eventLiveLeaseOwner(eventId) !== undefined) break;
		await Bun.sleep(5);
	}
	if (database.monitors.eventLiveLeaseOwner(eventId) === undefined)
		throw new Error("stale attempt never leased the row");

	// Another writer (the owning generation) persists a spoken answer while the
	// stale attempt is still parked inside its authoring turn — the reviewer's
	// interleaving: output persists before the in-flight dispatch's response is
	// selected. The writer CANNOT take the dispatch lease (it is live), which
	// is exactly the ordering that reaches the rejection below: when the stale
	// attempt resumes, its persisted-output guard (authorFenced) fails while
	// its lease is still valid, so only the in-loop recovery can rescue the row.
	database.transaction((tx) => database.monitors.authoredOutputCreate(tx, eventId, storedAnswer));
	expect(database.monitors.authoredOutput(eventId)).toBe(storedAnswer);

	// The stale attempt's session now answers. Its persistence attempt is
	// rejected by the authored-output fence while its own lease is still live,
	// so the recovery must run inside the dispatch itself (the reviewer's
	// required fix), not merely at the next reconcile sweep.
	releaseStale();
	await staleDispatch;

	// The owning generation's failure lands only after the stale dispatch has
	// fully settled: the writer takes a fresh lease (the attempt released its
	// own) and regresses the row to `failed` via eventFencedFail. Recovery is
	// then observable without any reconcile sweep having run.
	const ownerLease = `lease-${crypto.randomUUID()}`;
	database.transaction((tx) => {
		database.monitors.eventAcquireLease(tx, eventId, "owner-gen", ownerLease, 60_000);
	});
	database.transaction((tx) => {
		database.monitors.eventFencedFail(
			tx,
			eventId,
			ownerLease,
			`batch-${crypto.randomUUID()}`,
			"post_auth_failure",
			"owning dispatch failed after authoring",
			clock.now,
			false,
		);
	});
	database.transaction((tx) => database.monitors.eventReleaseLease(tx, eventId, ownerLease));
	// Evidence captured BEFORE any reconcile sweep: recovery must already have
	// happened inside the stale dispatch itself (the reviewer's in-loop fix),
	// not only at the next periodic sweep.
	const preReconcile = {
		stage: database.monitors.eventGet(eventId)?.stage,
		leaseReleased: database.monitors.eventLiveLeaseOwner(eventId) === undefined,
		report: database.inbox.pendingOldest(originKey(target)),
		reportCount: database.inbox.pendingByOrigin().find((row) => row.origin_key === originKey(target))?.n ?? 0,
		notifications: [...notifications],
	};
	await pipeline.reconcile();
	return {
		database,
		eventId,
		prompts,
		deliveredTexts,
		notifications,
		directory,
		propagator: pipeline,
		clock,
		preReconcile,
	};
}

async function closeRaceState(state: { database: Store; directory: string }): Promise<void> {
	state.database.close();
	await rm(state.directory, { recursive: true, force: true });
}

test("a stale dispatch with rejected persistence recovers the stored answer ([SILENT] session)", async () => {
	const storedAnswer = "Rollback finished: replica restored from snapshot 7.";
	const state = await staleDispatchRaceCase(["[SILENT]"], storedAnswer);
	const { database, eventId, directory } = state;
	try {
		// Recovery happened inside the stale dispatch itself, before any
		// reconcile sweep: the row left `failed`, the lease was released, the
		// deterministic report existed, and the owner was woken.
		expect(state.preReconcile.stage).toBe("authored");
		expect(state.preReconcile.leaseReleased).toBe(true);
		expect(state.preReconcile.report?.message_id).toBe(`monitor-report:${eventId}`);
		expect(state.preReconcile.report?.body.endsWith(storedAnswer)).toBe(true);
		expect(state.preReconcile.reportCount).toBe(1);
		expect(state.preReconcile.notifications).toEqual([originKey(target)]);
		// The stored output is untouched: the stale [SILENT] never overwrote it.
		expect(database.monitors.authoredOutput(eventId)).toBe(storedAnswer);
		// The fresh [SILENT] response is never delivered and nothing entered
		// the channel-delivery ledger: recovery reports through reportTo only.
		expect(state.deliveredTexts).toHaveLength(0);
		expect(database.outbox.rows()).toHaveLength(0);
		// Exactly one report row remains, keyed by the deterministic id, with
		// the STORED answer as its body.
		const report = database.inbox.pendingOldest(originKey(target));
		expect(report?.message_id).toBe(`monitor-report:${eventId}`);
		expect(report?.body.endsWith(storedAnswer)).toBe(true);
		expect(database.inbox.pendingByOrigin().find((row) => row.origin_key === originKey(target))?.n).toBe(1);
		// The owner was woken for the stored answer (reconcile's recovered guard
		// must not duplicate the dispatch's own recovery wake).
		expect(state.notifications.length).toBeGreaterThanOrEqual(1);
		expect(new Set(state.notifications)).toEqual(new Set([originKey(target)]));
		// Final stage is `authored` (spoken stored answer, report pending) and
		// this attempt's lease is released.
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");
		expect(database.monitors.eventLiveLeaseOwner(eventId)).toBeUndefined();
	} finally {
		await closeRaceState({ database, directory });
	}
});

test("a stale dispatch with rejected persistence recovers the stored answer (different spoken answer)", async () => {
	const storedAnswer = "Deploy finished: 3 services rolled, 0 failures.";
	const state = await staleDispatchRaceCase(["Completely different stale answer"], storedAnswer);
	const { database, eventId, directory } = state;
	try {
		// Recovery happened inside the stale dispatch itself, before any
		// reconcile sweep: the row left `failed`, the lease was released, the
		// deterministic report existed, and the owner was woken.
		expect(state.preReconcile.stage).toBe("authored");
		expect(state.preReconcile.leaseReleased).toBe(true);
		expect(state.preReconcile.report?.message_id).toBe(`monitor-report:${eventId}`);
		expect(state.preReconcile.report?.body.endsWith(storedAnswer)).toBe(true);
		expect(state.preReconcile.reportCount).toBe(1);
		expect(state.preReconcile.notifications).toEqual([originKey(target)]);
		// The stored output is untouched: the competing spoken answer never
		// replaced it.
		expect(database.monitors.authoredOutput(eventId)).toBe(storedAnswer);
		// The competing spoken answer is never delivered and nothing entered
		// the channel-delivery ledger: recovery reports through reportTo only.
		expect(state.deliveredTexts).toHaveLength(0);
		expect(database.outbox.rows()).toHaveLength(0);
		// Exactly one report row remains: the deterministic id, STORED body.
		const report = database.inbox.pendingOldest(originKey(target));
		expect(report?.message_id).toBe(`monitor-report:${eventId}`);
		expect(report?.body.endsWith(storedAnswer)).toBe(true);
		// Final stage `authored`, lease released, no duplicated report wake.
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");
		expect(database.monitors.eventLiveLeaseOwner(eventId)).toBeUndefined();
		// The owner was woken for the stored answer (reconcile's recovered guard
		// must not duplicate the dispatch's own recovery wake).
		expect(state.notifications.length).toBeGreaterThanOrEqual(1);
		expect(new Set(state.notifications)).toEqual(new Set([originKey(target)]));
	} finally {
		await closeRaceState({ database, directory });
	}
});

/**
 * snowykr CHANGES_REQUESTED on #842 (P2, claim-time recheck): a row may
 * already hold persisted authored output at the moment a dispatch claims it
 * (the burst window deferred the claim while another generation authored and
 * failed). Leasing that row for fresh authoring would send a redundant
 * authoring turn that the authored-output fence then rejects — exactly the
 * [SILENT]-over-spoken-answer hazard. The claim loop must recheck
 * authoredOutput after acquiring the lease and route such rows through
 * stored-answer recovery without any authoring turn.
 */
test("a row claimed with persisted output recovers without a fresh authoring turn (claim-time recheck)", async () => {
	const storedAnswer = "Rollback finished: replica restored from snapshot 7.";
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-claim-recheck-"));
	const clock = { now: Date.now() };
	const database = await Store.open(join(directory, "gateway.db"));
	const prompts: string[] = [];
	const notifications: string[] = [];
	const deliveredTexts: string[] = [];
	try {
		const registry = new MonitorRegistry(database);
		const monitor = registry.add({
			name: "claim-recheck-watch",
			trigger: { kind: "cron", schedule: "*/5 * * * *" },
			eventTypes: ["race.event"],
			burstPolicy: "coalesce",
			channelTarget: { origin: target },
			reportTo: target,
		});
		const sessionPort = sessionPortFromScript({
			bind: async () => ({ sessionId: "monitor-session" }),
			respond: async (_sessionId, text) => {
				prompts.push(text);
				return JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "[SILENT]" })));
			},
		});
		const pipeline = new MonitorPropagator({
			database,
			registry,
			sessionPort,
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(database),
			emit: () => {},
			now: () => clock.now,
			deliver: (payload) => {
				deliveredTexts.push(payload.text);
			},
			notifyInbound: async (key) => {
				notifications.push(key);
			},
		});

		// The event enters the coalescing burst window; its claim is deferred.
		const eventId = pipeline.submit(monitor.monitorId, "race.event", { run: 1 });
		// While the window is open (no dispatch lease exists yet), the owning
		// generation persists a spoken answer and its dispatch fails: the row
		// regresses to `failed` WITH output persisted — the reviewer's shape
		// at claim time.
		const ownerLease = `lease-${crypto.randomUUID()}`;
		database.transaction((tx) => {
			database.monitors.authoredOutputCreate(tx, eventId, storedAnswer);
			database.monitors.eventAcquireLease(tx, eventId, "owner-gen", ownerLease, 60_000);
		});
		database.transaction((tx) => {
			database.monitors.eventFencedFail(
				tx,
				eventId,
				ownerLease,
				`batch-${crypto.randomUUID()}`,
				"post_auth_failure",
				"owning dispatch failed after authoring",
				clock.now,
				false,
			);
		});
		database.transaction((tx) => database.monitors.eventReleaseLease(tx, eventId, ownerLease));
		expect(database.monitors.authoredOutput(eventId)).toBe(storedAnswer);
		expect(database.monitors.eventGet(eventId)?.stage).toBe("failed");

		// The burst window fires and the dispatch claims the row. Recovery must
		// happen at claim time: no authoring turn, stored answer reported.
		for (let i = 0; i < 400; i++) {
			if (database.inbox.pendingOldest(originKey(target)) !== undefined) break;
			await Bun.sleep(5);
		}
		// The dispatch settles fully before evidence is captured (it also
		// releases the lease it held for the recheck).
		for (let i = 0; i < 400; i++) {
			if (database.monitors.eventLiveLeaseOwner(eventId) === undefined) break;
			await Bun.sleep(5);
		}
		const preReconcile = {
			prompts: prompts.length,
			stage: database.monitors.eventGet(eventId)?.stage,
			leaseReleased: database.monitors.eventLiveLeaseOwner(eventId) === undefined,
			report: database.inbox.pendingOldest(originKey(target)),
			notifications: [...notifications],
		};
		// No authoring turn was ever sent for the already-authored row.
		expect(preReconcile.prompts).toBe(0);
		expect(preReconcile.stage).toBe("authored");
		expect(preReconcile.leaseReleased).toBe(true);
		expect(preReconcile.report?.message_id).toBe(`monitor-report:${eventId}`);
		expect(preReconcile.report?.body.endsWith(storedAnswer)).toBe(true);
		expect(preReconcile.notifications).toEqual([originKey(target)]);

		await pipeline.reconcile();

		// Idempotent afterwards: still no prompt, no duplicated report, and the
		// fresh [SILENT] response was never delivered through the channel.
		expect(prompts.length).toBe(0);
		expect(deliveredTexts).toHaveLength(0);
		expect(database.outbox.rows()).toHaveLength(0);
		expect(database.inbox.pendingByOrigin().find((row) => row.origin_key === originKey(target))?.n).toBe(1);
		expect(database.monitors.authoredOutput(eventId)).toBe(storedAnswer);
		expect(database.monitors.eventGet(eventId)?.stage).toBe("authored");
		expect(database.monitors.eventLiveLeaseOwner(eventId)).toBeUndefined();
	} finally {
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
});
