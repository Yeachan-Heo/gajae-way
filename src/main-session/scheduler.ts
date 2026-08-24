import { createHash } from "node:crypto";
import type { ScheduleJobRow, ScheduleRunRow, WayCoreHandle } from "../native-loader";
import type { WayProfile } from "../profile";
import { RpcBridgeException } from "../rpc-bridge";
import { canonicalJson } from "./gates";
import {
	assertTimezone,
	countMissedOccurrences,
	nextFireAt,
	parseCron,
	type ScheduleSpec,
	ScheduleSpecError,
} from "./schedule-spec";

export const SCHEDULE_METHOD_PREFIX = "schedule.";
export const DEFAULT_TICK_MS = 15_000;
export const DEFAULT_DEFER_WINDOW_MS = 900_000;
export const DEFAULT_MAX_CONSECUTIVE_FAILURES = 8;
const MAX_BACKOFF_MS = 3_600_000;
const BASE_BACKOFF_MS = 30_000;
/** Mirrors the durable idempotency retention window in the Rust store. */
const IDEMPOTENCY_WINDOW_MS = 24 * 60 * 60 * 1_000;

/** Terminal run outcomes, mirrored from the durable Rust enum. */
export type ScheduleRunOutcome =
	| "ok"
	| "failed"
	| "deferred_busy"
	| "expired_deferred"
	| "missed"
	| "skipped_overdue"
	| "refused_failed_closed"
	| "refused_quarantined"
	| "refused_not_ready"
	| "refused_surface_quarantined"
	| "interrupted_unknown";

/**
 * Refusals are not failures. A long quarantine or a fenced host must not
 * consume a job's failure budget and suspend it; only a broker or host error
 * counts against `failure_count`.
 */
const REFUSAL_OUTCOMES: ReadonlySet<ScheduleRunOutcome> = new Set([
	"refused_failed_closed",
	"refused_quarantined",
	"refused_not_ready",
	"refused_surface_quarantined",
	"deferred_busy",
	"expired_deferred",
	"interrupted_unknown",
]);

export interface SchedulerClock {
	now(): number;
	setTimeout(callback: () => void | Promise<void>, milliseconds: number): unknown;
	clearTimeout(timer: unknown): void;
}

const systemSchedulerClock: SchedulerClock = {
	now: () => Date.now(),
	setTimeout: (callback, milliseconds) => setTimeout(callback, milliseconds),
	clearTimeout: (timer) => clearTimeout(timer as ReturnType<typeof setTimeout>),
};

export interface SchedulerOptions {
	readonly core: WayCoreHandle;
	readonly profile: WayProfile;
	/** In-process scheduler entry point; never an RPC client. */
	readonly admitSystemEvent: (input: { text: string; idempotencyKey: string }) => Promise<unknown>;
	/** Operator-surface submission, used only by `submit` payloads. */
	readonly submitFromRpc: (params: unknown) => Promise<unknown>;
	readonly turnState: () => "idle" | "busy";
	readonly mutationReadinessReason?: () => string | undefined;
	readonly isCorpusQuarantined?: () => boolean;
	readonly isSurfaceQuarantined?: (surfaceId: string) => boolean;
	readonly tickMs?: number;
	readonly deferWindowMs?: number;
	readonly maxConsecutiveFailures?: number;
	readonly enabled?: boolean;
	readonly clock?: SchedulerClock;
	readonly onError?: (error: Error) => void;
	readonly newId?: () => string;
}

interface SchedulePayload {
	readonly text: string;
}

function parsePayload(payloadJson: string): SchedulePayload {
	let parsed: unknown;
	try {
		parsed = JSON.parse(payloadJson) as unknown;
	} catch {
		throw new RpcBridgeException(1701, "schedule_invalid_spec");
	}
	if (typeof parsed !== "object" || parsed === null) throw new RpcBridgeException(1701, "schedule_invalid_spec");
	const text = (parsed as { text?: unknown }).text;
	if (typeof text !== "string" || !text.trim()) throw new RpcBridgeException(1701, "schedule_invalid_spec");
	return { text };
}

function specOf(job: ScheduleJobRow): ScheduleSpec {
	return {
		kind: job.kind as ScheduleSpec["kind"],
		spec: job.spec,
		timezone: job.timezone,
		// The anchor is the job's CREATE-TIME epoch, not its interval. Passing
		// Number(job.spec) here snapped every schedule onto a global grid measured
		// from the Unix epoch, discarding the create time and breaking the no-drift
		// contract nextEveryFire documents.
		...(job.kind === "every" ? { anchorMs: job.createdAtMs } : {}),
	};
}

function backoffFor(failureCount: number): number {
	const exponent = Math.max(0, failureCount - 1);
	return Math.min(BASE_BACKOFF_MS * 2 ** exponent, MAX_BACKOFF_MS);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredString(record: Record<string, unknown>, field: string): string {
	const value = record[field];
	if (typeof value !== "string" || !value.trim()) {
		throw new RpcBridgeException(-32602, `${field} must be a non-empty string.`);
	}
	return value;
}

function optionalPositiveInteger(record: Record<string, unknown>, field: string): number | undefined {
	const value = record[field];
	if (value === undefined) return undefined;
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 1) {
		throw new RpcBridgeException(-32602, `${field} must be a positive safe integer.`);
	}
	return value;
}

function noUnknownKeys(record: Record<string, unknown>, allowed: readonly string[]): void {
	for (const key of Object.keys(record)) {
		if (!allowed.includes(key)) throw new RpcBridgeException(-32602, `unknown parameter: ${key}`);
	}
}

export interface Scheduler {
	handleRpc(method: string, params: unknown): Promise<unknown>;
	/** Resolves in-flight runs, then collapses overdue windows. Order matters. */
	reconcile(): Promise<void>;
	start(): void;
	stop(): void;
}

/**
 * In-process durable scheduler.
 *
 * It never opens a UDS client: routing scheduled work back through the RPC
 * boundary would create a second writer to the main session. It calls the
 * in-process admission entry points directly instead, so every admission
 * traverses exactly one pre-effect claim.
 */

/**
 * Kill seams for the durability drill, matching the existing `process.exit(137)`
 * pattern in src/main.ts. Both are inert outside NODE_ENV=test.
 *
 * Window 1: the claim transaction has committed and nothing has been sent yet.
 * Window 2: the broker has accepted but the run row is not yet finalized.
 * A simulated crash cannot prove that no in-memory scheduler state was
 * load-bearing; only a real process death can.
 */
function killAfterScheduleRunClaimedForE2e(): void {
	if (Bun.env.NODE_ENV !== "test") return;
	if (Bun.env.GAJAEWAY_E2E_KILL_AFTER_SCHEDULE_RUN_CLAIMED === "1") process.exit(137);
}

function killAfterScheduleAdmissionBeforeRunFinalizeForE2e(): void {
	if (Bun.env.NODE_ENV !== "test") return;
	if (Bun.env.GAJAEWAY_E2E_KILL_AFTER_SCHEDULE_ADMISSION_BEFORE_RUN_FINALIZE === "1") process.exit(137);
}

export function createScheduler(options: SchedulerOptions): Scheduler {
	const core = options.core;
	const clock = options.clock ?? systemSchedulerClock;
	const tickMs = options.tickMs ?? DEFAULT_TICK_MS;
	const deferWindowMs = options.deferWindowMs ?? DEFAULT_DEFER_WINDOW_MS;
	const defaultMaxFailures = options.maxConsecutiveFailures ?? DEFAULT_MAX_CONSECUTIVE_FAILURES;
	const enabled = options.enabled ?? true;
	const newId = options.newId ?? (() => crypto.randomUUID());
	const ownerSurfaceIds = new Set(options.profile.ownerSurfaces.map((surface) => surface.id));
	let timer: unknown;
	let stopped = false;
	const deferredSince = new Map<string, number>();

	const journalPayload = (
		job: ScheduleJobRow,
		run: ScheduleRunRow,
		outcome: ScheduleRunOutcome,
		nextFireAtMs: number | undefined,
		missedCount?: number,
	): string =>
		canonicalJson({
			job_id: job.jobId,
			run_id: run.runId,
			trigger: run.trigger,
			payload_kind: job.payloadKind,
			outcome,
			attempt: run.attempt,
			...(missedCount === undefined ? {} : { missed_count: missedCount }),
			...(nextFireAtMs === undefined ? {} : { next_fire_at_ms: nextFireAtMs }),
		});

	const finalize = (
		job: ScheduleJobRow,
		run: ScheduleRunRow,
		outcome: ScheduleRunOutcome,
		nextFireAtMs: number | undefined,
		detail?: string,
	): void => {
		const now = clock.now();
		const refusal = REFUSAL_OUTCOMES.has(outcome);
		const failureCount = refusal ? job.failureCount : job.failureCount + 1;
		const maxFailures = job.maxConsecutiveFailures || defaultMaxFailures;
		let jobState: string | undefined;
		let backoffUntilMs: number | undefined;
		if (outcome === "failed") {
			jobState = failureCount >= maxFailures ? "suspended" : "backoff";
			backoffUntilMs = jobState === "backoff" ? now + backoffFor(failureCount) : undefined;
		} else if (outcome === "ok") {
			jobState = job.kind === "at" ? "completed" : "active";
		}
		core.scheduleRunFinalize({
			runId: run.runId,
			outcome,
			finishedAt: now,
			...(detail === undefined ? {} : { detailJson: canonicalJson({ detail }) }),
			...(jobState === undefined ? {} : { jobState }),
			...(refusal ? {} : { failureCount }),
			...(backoffUntilMs === undefined ? {} : { backoffUntilMs }),
			journalPayloadJson: journalPayload(job, run, outcome, nextFireAtMs),
		});
		// A refusal keeps the occurrence, so the job retries on the next tick
		// rather than silently losing its slot.
		core.scheduleSetNextFire(job.jobId, nextFireAtMs, now);
	};

	const advance = (job: ScheduleJobRow, from: number): number | undefined => {
		try {
			return nextFireAt(specOf(job), from);
		} catch {
			return undefined;
		}
	};

	const runOnce = async (job: ScheduleJobRow, run: ScheduleRunRow): Promise<void> => {
		const now = clock.now();
		const nextFire = advance(job, now);

		const fenceReason = options.mutationReadinessReason?.();
		if (fenceReason) {
			finalize(job, run, "refused_not_ready", job.nextFireAtMs ?? now, fenceReason);
			return;
		}
		if (options.isCorpusQuarantined?.()) {
			finalize(job, run, "refused_quarantined", job.nextFireAtMs ?? now);
			return;
		}

		if (job.payloadKind === "system_event") {
			const payload = parsePayload(job.payloadJson);
			try {
				await options.admitSystemEvent({ text: payload.text, idempotencyKey: admissionKeyFor(job.jobId, run.runId) });
				killAfterScheduleAdmissionBeforeRunFinalizeForE2e();
				deferredSince.delete(job.jobId);
				finalize(job, run, "ok", nextFire);
			} catch (error) {
				finalize(job, run, "failed", nextFire, error instanceof Error ? error.message : String(error));
			}
			return;
		}

		// `submit` payloads defer instead of steering: a scheduled turn must never
		// interleave itself into a live operator turn.
		if (options.turnState() !== "idle") {
			const since = deferredSince.get(job.jobId) ?? now;
			deferredSince.set(job.jobId, since);
			if (now - since >= deferWindowMs) {
				deferredSince.delete(job.jobId);
				finalize(job, run, "expired_deferred", nextFire);
				return;
			}
			finalize(job, run, "deferred_busy", job.nextFireAtMs ?? now);
			return;
		}
		const surfaceId = job.surfaceId;
		if (surfaceId === undefined || !ownerSurfaceIds.has(surfaceId)) {
			finalize(job, run, "refused_surface_quarantined", nextFire, "submit requires a digest-bound owner surface");
			return;
		}
		if (options.isSurfaceQuarantined?.(surfaceId)) {
			finalize(job, run, "refused_surface_quarantined", job.nextFireAtMs ?? now);
			return;
		}
		const payload = parsePayload(job.payloadJson);
		try {
			await options.submitFromRpc({
				text: payload.text,
				surface_id: surfaceId,
				idempotency_key: admissionKeyFor(job.jobId, run.runId),
			});
			deferredSince.delete(job.jobId);
			finalize(job, run, "ok", nextFire);
		} catch (error) {
			finalize(job, run, "failed", nextFire, error instanceof Error ? error.message : String(error));
		}
	};

	const tick = async (): Promise<void> => {
		if (stopped || !enabled) return;
		const claim = core.scheduleDueClaim(clock.now(), newId());
		if (claim.claimed) killAfterScheduleRunClaimedForE2e();
		if (claim.refusedFailedClosed) {
			// The gate is read inside the claim transaction, so nothing was
			// claimed and there is no run row to finalize.
			return;
		}
		if (!claim.claimed || !claim.job || !claim.run) return;
		await runOnce(claim.job, claim.run);
	};

	const scheduleNextTick = (): void => {
		if (stopped || !enabled) return;
		timer = clock.setTimeout(async () => {
			try {
				await tick();
			} catch (error) {
				options.onError?.(error instanceof Error ? error : new Error(String(error)));
			} finally {
				scheduleNextTick();
			}
		}, tickMs);
	};

	const admissionKeyFor = (jobId: string, runId: string): string => `sched:${jobId}:${runId}`;

	const reconcile = async (): Promise<void> => {
		// In-flight runs first: a collapse must never swallow or miscount an
		// occurrence that already has a durable claim.
		const pendingKeys = new Set(core.mainAdmissionOperationsPending().map((pending) => pending.key));
		const now = clock.now();
		for (const run of core.scheduleInFlightRuns()) {
			let job: ScheduleJobRow;
			try {
				job = core.scheduleJobGet(run.jobId);
			} catch {
				continue;
			}
			const key = admissionKeyFor(job.jobId, run.runId);
			// A still-pending admission is owned by reconcilePendingMainAdmissions,
			// which runs before this. Touching it here would race that recovery.
			if (pendingKeys.has(key)) continue;

			// Past the durable idempotency window the admission record may have
			// expired, so a retry could no longer be suppressed and would risk a
			// SECOND broker turn. At-most-once wins: record the occurrence as
			// interrupted and never replay it. A skipped run is visible and
			// recoverable; a duplicated turn corrupts session history.
			if (now - run.claimedAt >= IDEMPOTENCY_WINDOW_MS) {
				core.scheduleRunFinalize({
					runId: run.runId,
					outcome: "interrupted_unknown",
					finishedAt: now,
					journalPayloadJson: journalPayload(job, run, "interrupted_unknown", job.nextFireAtMs),
				});
				continue;
			}

			// Inside the window the deterministic key is the safety property: if
			// the admission already happened, the durable idempotency record
			// replays its response instead of sending again, so this resolves a
			// window-1 crash (claim committed, nothing sent) without risking a
			// duplicate for a window-2 crash (sent, not yet finalized).
			await runOnce(job, run);
		}
		// Collapse only after every in-flight run is resolved above.
		const collapseNow = clock.now();
		for (const job of core.scheduleJobList({ limit: 200 }).jobs) {
			if (job.state !== "active" && job.state !== "backoff") continue;
			const due = job.nextFireAtMs;
			if (due === undefined || due > collapseNow) continue;
			const missed = countMissedOccurrences(specOf(job), due, collapseNow);
			if (missed < 1) continue;
			const nextFire = advance(job, collapseNow);
			core.scheduleOverdueCollapse({
				jobId: job.jobId,
				runId: newId(),
				missedCount: missed,
				...(nextFire === undefined ? {} : { nextFireAtMs: nextFire }),
				nowMs: collapseNow,
				journalPayloadJson: canonicalJson({
					job_id: job.jobId,
					trigger: "overdue",
					payload_kind: job.payloadKind,
					outcome: job.kind === "at" ? "missed" : "skipped_overdue",
					missed_count: missed,
					...(nextFire === undefined ? {} : { next_fire_at_ms: nextFire }),
				}),
			});
		}
	};

	const validateSpec = (kind: string, spec: string, timezone: string, now: number): number | undefined => {
		if (kind !== "at" && kind !== "every" && kind !== "cron") {
			throw new RpcBridgeException(1701, "schedule_invalid_spec");
		}
		try {
			assertTimezone(timezone);
			if (kind === "cron") parseCron(spec);
			if (kind === "every") {
				const interval = Number(spec);
				if (!Number.isSafeInteger(interval) || interval < tickMs)
					throw new ScheduleSpecError("interval below the tick floor");
			}
			if (kind === "at") {
				const instant = Number(spec);
				if (!Number.isSafeInteger(instant) || instant <= now)
					throw new ScheduleSpecError("an at instant must be in the future");
			}
			return nextFireAt({ kind, spec, timezone, ...(kind === "every" ? { anchorMs: now } : {}) }, now);
		} catch (error) {
			if (error instanceof ScheduleSpecError) throw new RpcBridgeException(1701, "schedule_invalid_spec");
			throw error;
		}
	};

	const upsert = (params: unknown, existing: ScheduleJobRow | undefined): unknown => {
		if (!isRecord(params)) throw new RpcBridgeException(-32602, "params must be an object.");
		noUnknownKeys(params, [
			"idempotency_key",
			"job_id",
			"name",
			"kind",
			"spec",
			"timezone",
			"payload_kind",
			"payload",
			"surface_id",
			"max_consecutive_failures",
		]);
		const idempotencyKey = requiredString(params, "idempotency_key");
		const kind = requiredString(params, "kind");
		const spec = requiredString(params, "spec");
		const timezone = requiredString(params, "timezone");
		const payloadKind = requiredString(params, "payload_kind");
		if (payloadKind !== "system_event" && payloadKind !== "submit") {
			throw new RpcBridgeException(-32602, "payload_kind must be system_event or submit.");
		}
		if (existing !== undefined && existing.payloadKind !== payloadKind) {
			throw new RpcBridgeException(-32602, "payload_kind is immutable.");
		}
		const payload = params.payload;
		if (!isRecord(payload)) throw new RpcBridgeException(-32602, "payload must be an object.");
		noUnknownKeys(payload, ["text"]);
		const text = requiredString(payload, "text");
		const surfaceRaw = params.surface_id;
		if (payloadKind === "system_event" && surfaceRaw !== undefined) {
			throw new RpcBridgeException(-32602, "a system_event payload must not carry a surface_id.");
		}
		let surfaceId: string | undefined;
		if (payloadKind === "submit") {
			surfaceId = requiredString(params, "surface_id");
			if (!ownerSurfaceIds.has(surfaceId)) {
				throw new RpcBridgeException(-32602, "surface_id must be a digest-bound owner surface.");
			}
		}
		const now = clock.now();
		const nextFire = validateSpec(kind, spec, timezone, now);
		const job = core.scheduleJobUpsert({
			// A retried create must not insert a second job: with no explicit
			// job_id the identity is derived from the idempotency key so the
			// retry converges on the same row.
			jobId: existing?.jobId ?? requiredStringOrDerived(params, "job_id", idempotencyKey),
			name: requiredString(params, "name"),
			kind,
			spec,
			timezone,
			payloadKind,
			payloadJson: canonicalJson({ text }),
			...(surfaceId === undefined ? {} : { surfaceId }),
			...(nextFire === undefined ? {} : { nextFireAtMs: nextFire }),
			maxConsecutiveFailures: optionalPositiveInteger(params, "max_consecutive_failures") ?? defaultMaxFailures,
		});
		return { job_id: job.jobId, next_fire_at_ms: job.nextFireAtMs ?? null, state: job.state };
	};

	const jobOrThrow = (jobId: string): ScheduleJobRow => {
		try {
			return core.scheduleJobGet(jobId);
		} catch {
			throw new RpcBridgeException(1700, "schedule_not_found");
		}
	};

	const handleRpc = async (method: string, params: unknown): Promise<unknown> => {
		switch (method) {
			case "schedule.create":
				return upsert(params, undefined);
			case "schedule.update": {
				if (!isRecord(params)) throw new RpcBridgeException(-32602, "params must be an object.");
				return upsert(params, jobOrThrow(requiredString(params, "job_id")));
			}
			case "schedule.delete": {
				if (!isRecord(params)) throw new RpcBridgeException(-32602, "params must be an object.");
				noUnknownKeys(params, ["idempotency_key", "job_id"]);
				requiredString(params, "idempotency_key");
				return { deleted: core.scheduleJobDelete(requiredString(params, "job_id")) };
			}
			case "schedule.get": {
				if (!isRecord(params)) throw new RpcBridgeException(-32602, "params must be an object.");
				noUnknownKeys(params, ["job_id"]);
				return jobOrThrow(requiredString(params, "job_id"));
			}
			case "schedule.list": {
				const record = isRecord(params) ? params : {};
				noUnknownKeys(record, ["state", "cursor", "limit"]);
				return core.scheduleJobList({
					...(typeof record.state === "string" ? { state: record.state } : {}),
					...(typeof record.cursor === "string" ? { cursor: record.cursor } : {}),
					...(typeof record.limit === "number" ? { limit: record.limit } : {}),
				});
			}
			case "schedule.runs": {
				if (!isRecord(params)) throw new RpcBridgeException(-32602, "params must be an object.");
				noUnknownKeys(params, ["job_id", "cursor", "limit"]);
				const jobId = requiredString(params, "job_id");
				jobOrThrow(jobId);
				return core.scheduleRunList(
					jobId,
					typeof params.cursor === "string" ? params.cursor : undefined,
					typeof params.limit === "number" ? params.limit : undefined,
				);
			}
			case "schedule.run_now": {
				if (!isRecord(params)) throw new RpcBridgeException(-32602, "params must be an object.");
				noUnknownKeys(params, ["idempotency_key", "job_id"]);
				requiredString(params, "idempotency_key");
				const job = jobOrThrow(requiredString(params, "job_id"));
				core.scheduleSetNextFire(job.jobId, clock.now(), clock.now());
				await tick();
				return { job_id: job.jobId, trigger: "manual" };
			}
			default:
				throw new RpcBridgeException(-32601, `method not found: ${method}`);
		}
	};

	return {
		handleRpc,
		reconcile,
		start: () => {
			if (!enabled) return;
			stopped = false;
			scheduleNextTick();
		},
		stop: () => {
			stopped = true;
			if (timer !== undefined) clock.clearTimeout(timer);
			timer = undefined;
		},
	};
}

/**
 * Resolves the durable job identity.
 *
 * When the caller supplies no `job_id`, it is DERIVED from the idempotency key
 * rather than generated randomly, so a retried `schedule.create` converges on
 * the same row instead of inserting a second job.
 */
function requiredStringOrDerived(record: Record<string, unknown>, field: string, idempotencyKey: string): string {
	const value = record[field];
	if (value === undefined) return `job:${createHash("sha256").update(idempotencyKey).digest("hex").slice(0, 32)}`;
	if (typeof value !== "string" || !value.trim()) {
		throw new RpcBridgeException(-32602, `${field} must be a non-empty string.`);
	}
	return value;
}
