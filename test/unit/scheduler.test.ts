import { afterAll, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { createMainAdmissionHandler } from "../../src/main-session/admission";
import { createScheduler, type SchedulerClock } from "../../src/main-session/scheduler";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { loadWayProfile } from "../../src/profile";

const temporaryDirectories: string[] = [];

afterAll(() => {
	for (const directory of temporaryDirectories) fs.rmSync(directory, { recursive: true, force: true });
});

class TestClock implements SchedulerClock {
	#now: number;
	constructor(now: number) {
		this.#now = now;
	}
	now(): number {
		return this.#now;
	}
	advance(ms: number): void {
		this.#now += ms;
	}
	setTimeout(): unknown {
		// Ticks are driven explicitly in these tests.
		return undefined;
	}
	clearTimeout(): void {}
}

function fixture(options: { turnState?: "idle" | "busy" } = {}) {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-scheduler-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const core: WayCoreHandle = loadWayCore().WayCore.open(stateDir);
	const clock = new TestClock(Date.now());
	const systemEvents: string[] = [];
	const submits: unknown[] = [];
	const scheduler = createScheduler({
		core,
		profile,
		admitSystemEvent: async ({ text }) => {
			systemEvents.push(text);
		},
		submitFromRpc: async (params) => {
			submits.push(params);
		},
		turnState: () => options.turnState ?? "idle",
		tickMs: 1_000,
		clock,
		newId: (() => {
			let counter = 0;
			return () => `id-${(counter += 1)}`;
		})(),
	});
	return { core, scheduler, clock, systemEvents, submits, profile };
}

function markFailedClosed(core: WayCoreHandle): void {
	core.gatewayMetaTransaction({
		expected: [],
		puts: [{ key: "failed_closed_reason", value: '"growth_protocol_invalid"' }],
		deletes: [],
	});
}

test("a system_event job admits through the in-process entry point and records ok", async () => {
	const { scheduler, core, clock, systemEvents } = fixture();

	const created = (await scheduler.handleRpc("schedule.create", {
		idempotency_key: "create-1",
		name: "heartbeat",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "scheduled nudge" },
	})) as { job_id: string; next_fire_at_ms: number };

	clock.advance(120_000);
	await scheduler.handleRpc("schedule.run_now", { idempotency_key: "run-1", job_id: created.job_id });

	expect(systemEvents).toEqual(["scheduled nudge"]);
	const runs = core.scheduleRunList(created.job_id);
	expect(runs.runs).toHaveLength(1);
	expect(runs.runs[0]?.outcome).toBe("ok");
});

/**
 * The fail-closed bit is read inside the claim transaction, so losing that race
 * must leave no durable run row at all rather than a claimed-then-refused one.
 */
test("fail-closed winning the claim race inserts no schedule_runs row", async () => {
	const { scheduler, core, clock } = fixture();

	const created = (await scheduler.handleRpc("schedule.create", {
		idempotency_key: "create-2",
		name: "fenced",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "must not run" },
	})) as { job_id: string };

	markFailedClosed(core);
	clock.advance(120_000);

	const claim = core.scheduleDueClaim(clock.now(), "race-run");
	expect(claim.refusedFailedClosed).toBe(true);
	expect(claim.claimed).toBe(false);
	expect(core.scheduleRunList(created.job_id).runs).toEqual([]);
});

/**
 * A long outage must collapse into ONE row carrying the real missed count. N
 * missed occurrences producing N runs would replay a scheduled turn N times.
 */
test("an overdue window collapses into one skipped_overdue run carrying missed_count", async () => {
	const { scheduler, core, clock } = fixture();

	const created = (await scheduler.handleRpc("schedule.create", {
		idempotency_key: "create-3",
		name: "overdue",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "collapse me" },
	})) as { job_id: string };

	// Ten whole intervals elapse while the daemon is down.
	clock.advance(10 * 60_000);
	await scheduler.reconcile();

	const runs = core.scheduleRunList(created.job_id).runs;
	expect(runs).toHaveLength(1);
	expect(runs[0]?.outcome).toBe("skipped_overdue");
	expect(runs[0]?.missedCount).toBeGreaterThanOrEqual(9);
	// The next occurrence is strictly in the future, not backfilled.
	const job = core.scheduleJobGet(created.job_id);
	expect(job.nextFireAtMs ?? 0).toBeGreaterThan(clock.now());
});

/**
 * Window 1: the daemon died after the claim committed but before anything was
 * sent. Inside the durable idempotency window the deterministic key makes a
 * retry safe - if the admission had actually happened, the idempotency record
 * would replay it instead of sending again - so the occurrence is recovered
 * rather than silently skipped.
 */
test("a crash after the claim is recovered inside the idempotency window without duplicating", async () => {
	const { scheduler, core, clock, systemEvents } = fixture();

	const created = (await scheduler.handleRpc("schedule.create", {
		idempotency_key: "create-4",
		name: "window one",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "interrupted work" },
	})) as { job_id: string };

	clock.advance(120_000);
	const claim = core.scheduleDueClaim(clock.now(), "crashed-run");
	expect(claim.claimed).toBe(true);
	expect(core.scheduleInFlightRuns()).toHaveLength(1);

	await scheduler.reconcile();

	expect(core.scheduleInFlightRuns()).toEqual([]);
	// Recovered, and admitted exactly once.
	expect(systemEvents).toEqual(["interrupted work"]);
	const outcomes = core.scheduleRunList(created.job_id).runs.map((run) => run.outcome);
	expect(outcomes).toContain("ok");
	expect(outcomes).not.toContain("interrupted_unknown");
});

/**
 * Past the durable idempotency window the admission record may have expired, so
 * a retry could no longer be suppressed and might send a SECOND broker turn.
 * At-most-once wins: the occurrence is recorded as interrupted and never
 * replayed. A skipped run is visible and recoverable; a duplicated turn
 * corrupts session history irreversibly.
 */
test("a crash older than the idempotency window is never re-admitted", async () => {
	const { scheduler, core, clock, systemEvents } = fixture();

	const created = (await scheduler.handleRpc("schedule.create", {
		idempotency_key: "create-4b",
		name: "expired window",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "must not replay" },
	})) as { job_id: string };

	clock.advance(120_000);
	core.scheduleDueClaim(clock.now(), "stale-run");
	// More than 24h passes before the daemon comes back.
	clock.advance(25 * 60 * 60 * 1_000);

	await scheduler.reconcile();

	expect(core.scheduleInFlightRuns()).toEqual([]);
	expect(systemEvents).toEqual([]);
	const outcomes = core.scheduleRunList(created.job_id).runs.map((run) => run.outcome);
	expect(outcomes).toContain("interrupted_unknown");
});

test("in-flight reconciliation runs before overdue collapse so the occurrence is not double counted", async () => {
	const { scheduler, core, clock } = fixture();

	const created = (await scheduler.handleRpc("schedule.create", {
		idempotency_key: "create-5",
		name: "ordering",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "ordering" },
	})) as { job_id: string };

	clock.advance(120_000);
	core.scheduleDueClaim(clock.now(), "inflight-run");
	clock.advance(10 * 60_000);

	await scheduler.reconcile();

	// The claimed occurrence is resolved on its own row, and the collapse row
	// (if any) is separate - the in-flight run is never folded into missed_count.
	expect(core.scheduleInFlightRuns()).toEqual([]);
	const runs = core.scheduleRunList(created.job_id).runs;
	const claimed = runs.filter((run) => run.runId === "inflight-run");
	expect(claimed).toHaveLength(1);
	expect(claimed[0]?.outcome).not.toBeUndefined();
	expect(claimed[0]?.missedCount).toBe(0);
});

test("a submit job defers instead of steering a live operator turn", async () => {
	const { scheduler, core, clock, submits } = fixture({ turnState: "busy" });

	const created = (await scheduler.handleRpc("schedule.create", {
		idempotency_key: "create-6",
		name: "operator turn",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "submit",
		surface_id: "owner-dm",
		payload: { text: "operator work" },
	})) as { job_id: string };

	clock.advance(120_000);
	await scheduler.handleRpc("schedule.run_now", { idempotency_key: "run-6", job_id: created.job_id });

	expect(submits).toEqual([]);
	const runs = core.scheduleRunList(created.job_id).runs;
	expect(runs[0]?.outcome).toBe("deferred_busy");
});

test("schedule.create rejects a surface_id on a system_event payload and requires one for submit", async () => {
	const { scheduler } = fixture();

	await expect(
		scheduler.handleRpc("schedule.create", {
			idempotency_key: "bad-1",
			name: "bad system event",
			kind: "every",
			spec: String(60_000),
			timezone: "UTC",
			payload_kind: "system_event",
			surface_id: "owner-dm",
			payload: { text: "no surface allowed" },
		}),
	).rejects.toMatchObject({ code: -32602 });

	await expect(
		scheduler.handleRpc("schedule.create", {
			idempotency_key: "bad-2",
			name: "bad submit",
			kind: "every",
			spec: String(60_000),
			timezone: "UTC",
			payload_kind: "submit",
			payload: { text: "needs a surface" },
		}),
	).rejects.toMatchObject({ code: -32602 });
});

test("an ambiguous cron expression is refused with schedule_invalid_spec", async () => {
	const { scheduler } = fixture();

	await expect(
		scheduler.handleRpc("schedule.create", {
			idempotency_key: "bad-3",
			name: "ambiguous",
			kind: "cron",
			spec: "0 9 15 * 1",
			timezone: "UTC",
			payload_kind: "system_event",
			payload: { text: "ambiguous day fields" },
		}),
	).rejects.toMatchObject({ code: 1701 });
});

test("an unknown job id is schedule_not_found", async () => {
	const { scheduler } = fixture();

	await expect(scheduler.handleRpc("schedule.get", { job_id: "missing" })).rejects.toMatchObject({ code: 1700 });
	await expect(scheduler.handleRpc("schedule.runs", { job_id: "missing" })).rejects.toMatchObject({ code: 1700 });
});

test("the durable payload_kind check rejects a third kind", () => {
	const { core } = fixture();

	expect(() =>
		core.scheduleJobUpsert({
			jobId: "bad-kind",
			name: "bad kind",
			kind: "every",
			spec: String(60_000),
			timezone: "UTC",
			payloadKind: "command",
			payloadJson: JSON.stringify({ text: "shell" }),
			maxConsecutiveFailures: 8,
		}),
	).toThrow();
});

/**
 * Window 2, proven against the real durable admission path rather than a stub.
 *
 * The daemon died after the broker accepted but before the run row was
 * finalized. Recovery retries under the SAME deterministic key
 * `sched:<job>:<run>`, so the durable idempotency record replays the stored
 * response instead of dispatching a second broker turn. Without the
 * deterministic key this is exactly where a duplicate turn would appear.
 */
test("a retry after broker acceptance replays instead of dispatching a second turn", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-window2-"));
	temporaryDirectories.push(root);
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDir = path.join(root, "state");
	for (const directory of [corpus, workspace, stateDir]) fs.mkdirSync(directory);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(
		profilePath,
		`[corpus]\npath = "${corpus}"\nworkspace = "${workspace}"\n\n[injection]\nfiles = []\n\n[surfaces.owner]\nid = "owner-dm"\nplatform = "test"\nkind = "dm"\nsession_kind = "main"\n`,
	);
	const profile = loadWayProfile(profilePath);
	const core: WayCoreHandle = loadWayCore().WayCore.open(stateDir);

	// A real admission handler over the real durable store; only the broker
	// target is stubbed, so the idempotency layer is genuinely exercised.
	const brokerDispatches: string[] = [];
	const target = {
		turnState: "idle" as const,
		async admit(
			_deliveredAs: string,
			text: string,
			opRef: string,
			finalizePendingClaim?: () => void,
			recordAttemptIds?: (attemptIds: readonly string[]) => void,
		): Promise<void> {
			brokerDispatches.push(text);
			recordAttemptIds?.([`attempt-${opRef}`]);
			finalizePendingClaim?.();
		},
	};
	// biome-ignore lint/suspicious/noExplicitAny: the stub implements only what admission uses.
	const { admitSystemEvent } = createMainAdmissionHandler(target as any, profile, core);

	const key = "sched:window2-job:window2-run";
	await admitSystemEvent({ text: "window two work", idempotencyKey: key });
	expect(brokerDispatches).toEqual(["window two work"]);

	// The recovery retry uses the identical key.
	await admitSystemEvent({ text: "window two work", idempotencyKey: key });

	// Replayed, not re-dispatched: exactly one broker turn exists.
	expect(brokerDispatches).toEqual(["window two work"]);
	const attributions = core.mainAdmissionAttributions();
	expect(attributions).toHaveLength(1);
	expect(attributions[0]?.origin).toBe("scheduler");
});

/**
 * A retried create must converge on the same job.
 *
 * `idempotency_key` was previously required and then discarded, so a retry
 * without an explicit `job_id` inserted a SECOND job and the schedule silently
 * doubled.
 */
test("a retried schedule.create with the same idempotency key does not create a second job", async () => {
	const { scheduler, core } = fixture();

	const params = {
		idempotency_key: "create-retry-1",
		name: "retried",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "retry me" },
	};

	const first = (await scheduler.handleRpc("schedule.create", params)) as { job_id: string };
	const second = (await scheduler.handleRpc("schedule.create", params)) as { job_id: string };

	expect(second.job_id).toBe(first.job_id);
	expect(core.scheduleJobList({ limit: 200 }).jobs).toHaveLength(1);
});

test("a different idempotency key still creates a distinct job", async () => {
	const { scheduler, core } = fixture();
	const base = {
		name: "distinct-a",
		kind: "every",
		spec: String(60_000),
		timezone: "UTC",
		payload_kind: "system_event",
		payload: { text: "distinct" },
	};

	await scheduler.handleRpc("schedule.create", { ...base, idempotency_key: "key-a" });
	await scheduler.handleRpc("schedule.create", { ...base, name: "distinct-b", idempotency_key: "key-b" });

	expect(core.scheduleJobList({ limit: 200 }).jobs).toHaveLength(2);
});
