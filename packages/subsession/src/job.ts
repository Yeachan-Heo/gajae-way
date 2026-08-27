/**
 * Durable lane jobs.
 *
 * Contract (issue #10, measured on 2026-08-27): a prompt op is a bounded
 * conversational turn, while delegated work is minutes-to-hours with its own
 * progress and durable artifacts. Four lanes died at exactly 1800s while their
 * workers were still committing - one 51s before its op turned terminal, one
 * seven minutes after. The defect: the supervisor equated op lifetime with work
 * lifetime and assumed the final report would arrive in a reply body.
 *
 * This module therefore models the JOB as the durable unit:
 *   - job identity outlives every prompt op; each op is one attempt;
 *   - progress is read from the REPOSITORY FIRST (HEAD moves on the lane
 *     branch), op state second;
 *   - an op that ends while the job can continue is `attempt_ended`, not job
 *     failure;
 *   - evidence classification reuses issue #9's exact vocabulary:
 *     work_committed_report_lost | work_in_progress_uncommitted | no_work_produced
 *     (no synonyms);
 *   - continuation is ONE deterministic code path against the same live
 *     session, never ad-hoc operator branching;
 *   - unknown or corrupt durable state fails closed;
 *   - resume is progress-gated so restart cannot mint duplicate attempts.
 */

import { assertValidOpRef } from "./send";
import type { PromptStatusBody, SupervisorOpState } from "./status";
import { projectOpState } from "./status";

export class LaneJobError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "LaneJobError";
	}
}

/** Wire/storage version of the persisted LaneJobRecord. */
export const LANE_JOB_SCHEMA_VERSION = 1 as const;

/** Issue #9's exact evidence vocabulary. No synonyms. */
export type WorkEvidenceClass = "work_committed_report_lost" | "work_in_progress_uncommitted" | "no_work_produced";

/**
 * Job states (durable, outlive every op):
 *
 *  - running       an attempt is driving the job right now
 *  - attempt_ended the attempt's accounting closed but the session may still be
 *                  working/continuable; reconcile before continuing
 *  - awaiting_operator terminal_uncertainty holds for a human; automation must
 *                  neither resend nor recreate
 *  - stalled       continuations stopped producing HEAD moves; escalation only
 *  - done          the deliverable landed (e.g. merged / confirmed complete)
 *  - aborted       operator-cancelled; never auto-resumed
 */
export type LaneJobState = "running" | "attempt_ended" | "awaiting_operator" | "stalled" | "done" | "aborted";

const LANE_JOB_STATES = new Set<LaneJobState>([
	"running",
	"attempt_ended",
	"awaiting_operator",
	"stalled",
	"done",
	"aborted",
]);

const SESSION_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** One prompt op driving the job. Attempts are append-only history. */
export type JobAttempt = {
	readonly opRef: string;
	readonly sessionId: string;
	readonly startedAt: string;
	/** Terminal time of the ATTEMPT, when known; the job may well continue. */
	readonly endedAt?: string;
	readonly endState?: SupervisorOpState;
	/** Exact failure envelope code when the attempt failed, e.g. prompt_deadline_exceeded. */
	readonly errorCode?: string;
};

export type JobCheckpoint = {
	readonly sha: string;
	readonly createdAt: string;
};

export type LaneJobRecord = {
	readonly schemaVersion: typeof LANE_JOB_SCHEMA_VERSION;
	/** Stable job identity: `lanejob-<laneSessionKey>`. Outlives every op. */
	readonly jobId: string;
	readonly createdAt: string;
	readonly updatedAt: string;
	readonly state: LaneJobState;
	readonly lane: {
		readonly branch: string;
		readonly worktreePath: string;
	};
	/** Every runtime session this job has ever been driven through. */
	readonly sessions: readonly string[];
	readonly attempts: readonly JobAttempt[];
	/** Authoritative progress signal: each observed HEAD move on the lane branch. */
	readonly checkpoints: readonly JobCheckpoint[];
	/** Continuations that produced no new checkpoint, capped by maxStalledContinuations. */
	readonly stalledContinuations: number;
	/** Where the human-readable report was finally recovered from, once found. */
	readonly reportSource?: "transcript_body" | "commit_messages" | "diff_survey";
	readonly report?: string;
	/** Bounded escalation trail once work stalls or an operator hold trips. */
	readonly escalations: readonly string[];
};

export type CreateLaneJobInput = {
	readonly jobId: string;
	readonly branch: string;
	readonly worktreePath: string;
	readonly sessionId?: string;
	readonly now?: () => Date;
};

/**
 * Creates the initial record. Validation mirrors the parse side exactly, so a
 * record this function produces never fails {@link parseLaneJobRecord}.
 */
export function createLaneJobRecord(input: CreateLaneJobInput): LaneJobRecord {
	const jobId = assertJobId(input.jobId);
	if (!SLASH_SAFE_BRANCH.test(input.branch) || input.branch.length === 0) {
		throw new LaneJobError(`invalid lane branch "${input.branch}"`);
	}
	if (input.worktreePath.length === 0 || !input.worktreePath.startsWith("/")) {
		throw new LaneJobError(`invalid worktree path "${input.worktreePath}"`);
	}
	const at = (input.now ?? (() => new Date()))().toISOString();
	return Object.freeze({
		schemaVersion: LANE_JOB_SCHEMA_VERSION,
		jobId,
		createdAt: at,
		updatedAt: at,
		state: "running",
		lane: Object.freeze({ branch: input.branch, worktreePath: input.worktreePath }),
		sessions: input.sessionId ? Object.freeze([assertSessionIdShape(input.sessionId)]) : Object.freeze([]),
		attempts: Object.freeze([]),
		checkpoints: Object.freeze([]),
		stalledContinuations: 0,
		escalations: Object.freeze([]),
	});
}

function assertJobId(jobId: string): string {
	if (!/^lanejob-[a-z0-9-]{1,128}$/.test(jobId)) {
		throw new LaneJobError(`invalid jobId "${jobId}"`);
	}
	return jobId;
}

function assertSessionIdShape(sessionId: string): string {
	if (!SESSION_ID_PATTERN.test(sessionId)) {
		throw new LaneJobError(`sessionId "${sessionId}" is not a runtime session id`);
	}
	return sessionId;
}

const SLASH_SAFE_BRANCH = /^[A-Za-z0-9._/-]+$/;

/**
 * Parses persisted JSON fail-closed.
 *
 * Unknown schema versions are a migration boundary this build does not own
 * (migration-safe = refuse rather than guess); any structural violation -
 * wrong types, non-session-id entries, unsorted checkpoints, duplicated
 * op-refs - throws instead of being coerced. Callers treat the file as
 * operator-controlled state: recover by hand or via migration code, never by
 * silently resetting it.
 */
export function parseLaneJobRecord(raw: string): LaneJobRecord {
	let value: unknown;
	try {
		value = JSON.parse(raw);
	} catch (error) {
		throw new LaneJobError(
			`lane job state is corrupt (unparseable JSON): ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	if (typeof value !== "object" || value === null) {
		throw new LaneJobError("lane job state must be a JSON object");
	}
	const record = value as Record<string, unknown>;
	if (record.schemaVersion !== LANE_JOB_SCHEMA_VERSION) {
		throw new LaneJobError(
			`unknown lane job schemaVersion ${JSON.stringify(record.schemaVersion)}; refusing to guess across versions`,
		);
	}
	assertJobId(asString(record.jobId));
	assertRequiredTimestamp(record.createdAt, "createdAt");
	const updatedAt = assertRequiredTimestamp(record.updatedAt, "updatedAt");
	if (!LANE_JOB_STATES.has(record.state as LaneJobState)) {
		throw new LaneJobError(`unknown lane job state ${JSON.stringify(record.state)}`);
	}
	const lane = record.lane;
	if (typeof lane !== "object" || lane === null) {
		throw new LaneJobError("lane block missing");
	}
	const laneObj = lane as Record<string, unknown>;
	if (typeof laneObj.branch !== "string" || !SLASH_SAFE_BRANCH.test(laneObj.branch) || laneObj.branch.length === 0) {
		throw new LaneJobError(`invalid lane branch ${JSON.stringify(laneObj.branch)}`);
	}
	if (typeof laneObj.worktreePath !== "string" || !laneObj.worktreePath.startsWith("/")) {
		throw new LaneJobError("lane worktreePath must be an absolute path");
	}
	const sessions = parseSessions(record.sessions);
	const attempts = parseAttempts(record.attempts);
	const checkpoints = parseCheckpoints(record.checkpoints);
	const stalledContinuations = parseCounter(record.stalledContinuations, "stalledContinuations");
	const escalations = parseEscalations(record.escalations);

	return Object.freeze({
		schemaVersion: LANE_JOB_SCHEMA_VERSION,
		jobId: record.jobId as string,
		createdAt: record.createdAt as string,
		updatedAt,
		state: record.state as LaneJobState,
		lane: Object.freeze({ branch: laneObj.branch as string, worktreePath: laneObj.worktreePath as string }),
		sessions,
		attempts,
		checkpoints,
		stalledContinuations,
		...(record.reportSource === undefined ? {} : { reportSource: asReportSource(record.reportSource) }),
		...(typeof record.report === "string" ? { report: record.report } : {}),
		escalations,
	});
}

function asString(value: unknown): string {
	if (typeof value !== "string") {
		throw new LaneJobError(`expected a string field, got ${JSON.stringify(value)}`);
	}
	return value;
}

function assertRequiredTimestamp(value: unknown, field: string): string {
	if (typeof value !== "string" || Number.isNaN(Date.parse(value))) {
		throw new LaneJobError(`${field} must be an ISO timestamp`);
	}
	return value;
}

function parseSessions(value: unknown): readonly string[] {
	if (value === undefined) {
		return Object.freeze([]);
	}
	if (!Array.isArray(value)) {
		throw new LaneJobError("sessions must be an array of runtime session ids");
	}
	return Object.freeze(value.map((entry) => assertSessionIdShape(asString(entry))));
}

function parseAttempts(value: unknown): readonly JobAttempt[] {
	if (value === undefined) {
		return Object.freeze([]);
	}
	if (!Array.isArray(value)) {
		throw new LaneJobError("attempts must be an array");
	}
	const seenOpRefs = new Set<string>();
	return Object.freeze(
		value.map((entry): JobAttempt => {
			if (typeof entry !== "object" || entry === null) {
				throw new LaneJobError("each attempt must be an object");
			}
			const attempt = entry as Record<string, unknown>;
			assertValidOpRef(asString(attempt.opRef));
			assertSessionIdShape(asString(attempt.sessionId));
			assertRequiredTimestamp(attempt.startedAt, "attempt.startedAt");
			if (seenOpRefs.has(attempt.opRef as string)) {
				throw new LaneJobError(`duplicate attempt opRef ${JSON.stringify(attempt.opRef)}`);
			}
			seenOpRefs.add(attempt.opRef as string);
			const rest: Partial<JobAttempt> = {};
			if (attempt.endedAt !== undefined) {
				assertRequiredTimestamp(attempt.endedAt, "attempt.endedAt");
				(rest as Record<string, unknown>).endedAt = attempt.endedAt;
			}
			if (attempt.errorCode !== undefined) {
				(rest as Record<string, unknown>).errorCode = asString(attempt.errorCode);
			}
			if (attempt.endState !== undefined) {
				(rest as Record<string, unknown>).endState = attempt.endState;
			}
			return Object.freeze({
				opRef: attempt.opRef as string,
				sessionId: attempt.sessionId as string,
				startedAt: attempt.startedAt as string,
				...rest,
			} as JobAttempt);
		}),
	);
}

function parseCheckpoints(value: unknown): readonly JobCheckpoint[] {
	if (value === undefined) {
		return Object.freeze([]);
	}
	if (!Array.isArray(value)) {
		throw new LaneJobError("checkpoints must be an array");
	}
	let previous = "";
	return Object.freeze(
		value.map((entry, index): JobCheckpoint => {
			if (typeof entry !== "object" || entry === null) {
				throw new LaneJobError("each checkpoint must be an object");
			}
			const checkpoint = entry as Record<string, unknown>;
			const sha = asString(checkpoint.sha);
			if (!/^[0-9a-f]{40}$/.test(sha)) {
				throw new LaneJobError(`checkpoint ${index} sha is not a full commit sha`);
			}
			const createdAt = assertRequiredTimestamp(checkpoint.createdAt, "checkpoint.createdAt");
			if (sha <= previous) {
				throw new LaneJobError("checkpoints must be strictly increasing distinct commits");
			}
			previous = sha;
			return Object.freeze({ sha, createdAt });
		}),
	);
}

function parseCounter(value: unknown, field: string): number {
	if (value === undefined) {
		return 0;
	}
	if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
		throw new LaneJobError(`${field} must be a non-negative integer`);
	}
	return value;
}

function parseEscalations(value: unknown): readonly string[] {
	if (value === undefined) {
		return Object.freeze([]);
	}
	if (!Array.isArray(value)) {
		throw new LaneJobError("escalations must be an array of strings");
	}
	return Object.freeze(value.map((entry) => asString(entry)));
}

function asReportSource(value: unknown): NonNullable<LaneJobRecord["reportSource"]> {
	if (value !== "transcript_body" && value !== "commit_messages" && value !== "diff_survey") {
		throw new LaneJobError(`unknown reportSource ${JSON.stringify(value)}`);
	}
	return value;
}

/** Repository facts injected by the caller, gathered BEFORE reading op state. */
export type RepositoryFacts = {
	/** Current full SHA of the lane branch head, from `git rev-parse`. */
	readonly headSha?: string | undefined;
	/** Checkpoints recorded at last reconciliation. */
	readonly knownCheckpoints?: readonly JobCheckpoint[];
	/** Worker-modified tracked files in the worktree (`git status --porcelain`). */
	readonly dirtyFiles: number;
	/** When the repository snapshot was taken (checkpoint timestamps derive from it). */
	readonly observedAt: string;
};

/** True when these repository facts show forward motion since `knownCheckpoints`. */
export function hasNewCommit(facts: RepositoryFacts): boolean {
	const known = facts.knownCheckpoints ?? [];
	const last = known.at(-1)?.sha ?? "";
	if (!facts.headSha || !/^[0-9a-f]{40}$/.test(facts.headSha)) {
		return false;
	}
	if (facts.headSha === last) {
		return false;
	}
	return last === "" ? true : !facts.headSha.startsWith(last);
}

export type AttemptOutcomeInput = {
	readonly status: PromptStatusBody;
};

/**
 * Projects ONE attempt's terminal transition onto the job's next state.
 *
 * The key distinction #10 demands: `failed + prompt_deadline_exceeded` on a
 * live-capable session is an ATTEMPT ending (resumable), never the job dying.
 * Uncertain projections (`terminal_uncertain`, `terminal_missing_receipt`)
 * hold the job for an operator instead of being papered over.
 */
export function projectAttemptOutcome(input: AttemptOutcomeInput): { readonly state: LaneJobState } {
	const projected = projectOpState(input.status);
	if (projected === "terminal_uncertain" || projected === "terminal_missing_receipt") {
		return { state: "awaiting_operator" };
	}
	return { state: "attempt_ended" };
}

/**
 * Classifies work-vs-report using #9's exact classes, repository first.
 *
 * evidence precedence given repo facts + the attempt's op projection:
 *  - commits exist -> `work_committed_report_lost` regardless of what the op body said;
 *  - no commits but a dirty tree -> `work_in_progress_uncommitted`;
 *  - neither -> `no_work_produced`.
 */
export function classifyWorkEvidence(facts: RepositoryFacts): WorkEvidenceClass {
	const committed = hasNewCommit(facts) || (facts.knownCheckpoints?.length ?? 0) > 0;
	if (committed) {
		return "work_committed_report_lost";
	}
	if (facts.dirtyFiles > 0) {
		return "work_in_progress_uncommitted";
	}
	return "no_work_produced";
}

export type ContinuationPlan =
	| {
			readonly action: "observe" | "continue_same_session" | "resume_session" | "recreate_session";
			readonly reason: string;
			readonly opRefPolicy: "fresh_op_ref_required";
	  }
	| {
			readonly action: "hold_for_operator";
			readonly reason: string;
	  };

export type PlanContinuationInput = {
	readonly record: LaneJobRecord;
	readonly repository: RepositoryFacts;
	readonly latestAttempt: {
		readonly endState?: SupervisorOpState | undefined;
	} | null;
	readonly session: {
		readonly live: boolean;
		readonly deleted: boolean;
		readonly locatorMatches: boolean;
		readonly ambiguous?: boolean;
		readonly duplicateOwner?: boolean;
	};
	readonly workComplete: boolean;
};

/**
 * Bound on consecutive continuations that produced no new checkpoint. Past it
 * the job goes to `hold_for_operator` (bounded escalation) instead of looping.
 */
export const MAX_STALLED_CONTINUATIONS = 3;

/** Classification of how a reconciliation ended, for state transitions. */
export type ReconciliationClass = "progressed" | "stalled" | "held";

export type TransitionInput = {
	readonly record: LaneJobRecord;
	/** Repository facts gathered BEFORE op state; HEAD vs record.checkpoints decides progress. */
	readonly repository: RepositoryFacts;
	readonly classification: ReconciliationClass;
};

/**
 * The single reducer that applies one reconciliation to a record.
 *
 * Progress-gated continuation lives here, so no caller can mint unbounded
 * attempts: stalled continuations accumulate only when the branch head has not
 * moved, and hitting {@link MAX_STALLED_CONTINUATIONS} trips a hold.
 */
export function applyReconciliation(input: TransitionInput): LaneJobRecord {
	const at = input.repository.observedAt;
	const known = input.record.checkpoints;
	const facts = { ...input.repository, knownCheckpoints: known };
	const progressed = hasNewCommit(facts);
	const checkpoints = progressed
		? Object.freeze([...known, Object.freeze({ sha: facts.headSha as string, createdAt: at })])
		: known;

	const stalledContinuations =
		input.classification === "stalled" && !progressed
			? Math.min(input.record.stalledContinuations + 1, MAX_STALLED_CONTINUATIONS)
			: 0;

	return Object.freeze({
		...input.record,
		checkpoints,
		stalledContinuations,
		updatedAt: at,
	});
}

/** Marks an attempt as ended in the job history. */
export function appendAttempt(record: LaneJobRecord, attempt: JobAttempt): LaneJobRecord {
	if (record.attempts.some((existing) => existing.opRef === attempt.opRef)) {
		throw new LaneJobError(`attempt ${attempt.opRef} already exists on ${record.jobId}; refusing a duplicate`);
	}
	return Object.freeze({
		...record,
		sessions: record.sessions.includes(attempt.sessionId)
			? record.sessions
			: Object.freeze([...record.sessions, attempt.sessionId]),
		attempts: Object.freeze([...record.attempts, Object.freeze({ ...attempt })]),
		state: record.state === "done" || record.state === "aborted" ? record.state : "running",
		updatedAt: attempt.startedAt,
	});
}

export type CloseAttemptInput = {
	readonly record: LaneJobRecord;
	readonly opRef: string;
	readonly endState: SupervisorOpState;
	readonly errorCode?: string;
	readonly endedAt: string;
};

/**
 * Closes one attempt on the job.
 *
 * A deadline/lease kill (`endState === attempt_ended`) is recorded as exactly
 * that - the ATTEMPT's terminal transition - while the job itself moves to
 * `attempt_ended`, i.e. continuable, not failed. `terminal_uncertain` /
 * `terminal_missing_receipt` force the job into `awaiting_operator`.
 */
export function closeAttempt(input: CloseAttemptInput): LaneJobRecord {
	const index = input.record.attempts.findIndex((attempt) => attempt.opRef === input.opRef);
	if (index < 0) {
		throw new LaneJobError(`attempt ${input.opRef} does not exist on ${input.record.jobId}`);
	}
	const prior = input.record.attempts[index];
	if (prior.endedAt !== undefined) {
		throw new LaneJobError(`attempt ${input.opRef} was already closed at ${prior.endedAt}`);
	}
	const updatedAttempt = Object.freeze({
		...prior,
		endedAt: input.endedAt,
		endState: input.endState,
		...(input.errorCode !== undefined ? { errorCode: input.errorCode } : {}),
	});
	const attempts = Object.freeze(input.record.attempts.map((attempt, at) => (at === index ? updatedAttempt : attempt)));
	const uncertain = input.endState === "terminal_uncertain" || input.endState === "terminal_missing_receipt";
	const state: LaneJobState = uncertain ? "awaiting_operator" : "attempt_ended";
	return Object.freeze({
		...input.record,
		attempts,
		state,
		escalations: uncertain
			? Object.freeze([
					...input.record.escalations,
					`${input.endedAt} operator hold: attempt ${input.opRef} ended ${input.endState}`,
				])
			: input.record.escalations,
		updatedAt: input.endedAt,
	});
}

export function planContinuation(input: PlanContinuationInput): ContinuationPlan {
	if (input.record.state === "aborted") {
		return { action: "hold_for_operator", reason: "the job was aborted by an operator; never auto-resumed" };
	}
	if (input.record.state === "done") {
		return { action: "hold_for_operator", reason: "job is done: run retirement/handoff, do not continue" };
	}
	if (input.record.state === "awaiting_operator") {
		return {
			action: "hold_for_operator",
			reason: "genuine terminal uncertainty keeps its operator hold; the job model does not override it",
		};
	}
	if (input.session.ambiguous || !input.session.locatorMatches || input.session.duplicateOwner) {
		return {
			action: "hold_for_operator",
			reason: "session authority is not verifiable; failing closed to an operator hold",
		};
	}

	const stalledOut =
		input.record.stalledContinuations >= MAX_STALLED_CONTINUATIONS &&
		!hasNewCommit({ ...input.repository, knownCheckpoints: input.record.checkpoints });
	if (stalledOut) {
		return {
			action: "hold_for_operator",
			reason: `continuation budget exhausted: ${input.record.stalledContinuations} consecutive continuations produced no HEAD move`,
		};
	}

	if (input.workComplete) {
		return { action: "hold_for_operator", reason: "work is complete: run the retirement sequence" };
	}

	const sessionUsable = input.session.live && !input.session.deleted;

	if (sessionUsable) {
		if (
			input.latestAttempt &&
			(input.latestAttempt.endState === "accepted" || input.latestAttempt.endState === "running")
		) {
			return {
				action: "observe",
				reason: "an active turn drives the job: observe, never auto-send a second prompt",
				opRefPolicy: "fresh_op_ref_required",
			};
		}
		return {
			action: "continue_same_session",
			reason: "one deterministic path: same live session, fresh op-ref continuation",
			opRefPolicy: "fresh_op_ref_required",
		};
	}

	if (input.session.deleted) {
		return { action: "hold_for_operator", reason: "session deleted; recreation needs operator sign-off" };
	}

	if (
		input.latestAttempt == null ||
		!(input.latestAttempt.endState === "accepted" || input.latestAttempt.endState === "running")
	) {
		return {
			action: "resume_session",
			reason: "saved non-live session whose last attempt ended: broker session.resume",
			opRefPolicy: "fresh_op_ref_required",
		};
	}
	return { action: "hold_for_operator", reason: "inconsistent snapshot: re-read inspect -> status -> inspect" };
}
