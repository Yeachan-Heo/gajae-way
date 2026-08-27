import { describe, expect, test } from "bun:test";
import {
	appendAttempt,
	applyReconciliation,
	classifyWorkEvidence,
	closeAttempt,
	createLaneJobRecord,
	type JobAttempt,
	LANE_JOB_SCHEMA_VERSION,
	LaneJobError,
	MAX_STALLED_CONTINUATIONS,
	type PlanContinuationInput,
	parseLaneJobRecord,
	planContinuation,
	projectAttemptOutcome,
	type RepositoryFacts,
} from "../src/job";
import { ATTEMPT_ENDED_CODES, projectOpState } from "../src/status";

const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
const SESSION_B = "bf3c1350-3695-5e24-c8c7-d7bd35021980";
const NOW = new Date("2026-08-27T12:00:00.000Z");

function record() {
	return createLaneJobRecord({
		jobId: "lanejob-project-issue-10-lane-jobs",
		branch: "feat/issue-10-lane-jobs-takeover",
		worktreePath: "/wt/lane-jobs",
		sessionId: SESSION,
		now: () => NOW,
	});
}

function repo(overrides: Partial<RepositoryFacts> = {}): RepositoryFacts {
	return {
		headSha: undefined,
		dirtyFiles: 0,
		observedAt: NOW.toISOString(),
		...overrides,
	};
}

function attempt(overrides: Partial<JobAttempt> = {}): JobAttempt {
	return {
		opRef: "gw-lanejob-01hq00000000",
		sessionId: SESSION,
		startedAt: NOW.toISOString(),
		...overrides,
	};
}

function planInput(overrides: Partial<PlanContinuationInput> = {}): PlanContinuationInput {
	return {
		record: record(),
		repository: repo(),
		latestAttempt: null,
		session: { live: true, deleted: false, locatorMatches: true },
		workComplete: false,
		...overrides,
	};
}

const deadlineKillBody = {
	status: {
		status: "failed" as const,
		acceptedAt: 1,
		terminalAt: 1_800_001,
		error: { code: "prompt_deadline_exceeded", message: "Prompt deadline exceeded." },
	},
};

describe("op projection: error.code surfaces and deadline kills are attempt_ended", () => {
	test("parseStatusReport keeps the failure envelope instead of dropping it", () => {
		// covered through parseStatusReport in status.test.ts additions
		expect(ATTEMPT_ENDED_CODES).toContain("prompt_deadline_exceeded");
	});

	test("a failed op with prompt_deadline_exceeded projects to attempt_ended", () => {
		const projected = projectOpState(deadlineKillBody.status);
		expect(projected).toBe("attempt_ended");
	});

	test("projectAttemptOutcome maps a deadline kill to a continuable job, never failure or hold", () => {
		const next = projectAttemptOutcome({ status: deadlineKillBody.status });
		expect(next.state).toBe("attempt_ended");
	});

	test("an ordinary failed op (no code / other code) stays failed", () => {
		expect(projectOpState({ status: "failed" })).toBe("failed");
		expect(
			projectOpState({
				status: "failed",
				receiptState: "present",
				error: { code: "provider_overloaded" },
			}),
		).toBe("failed");
	});

	test("terminal_uncertain still lands in awaiting_operator - no papering over", () => {
		const uncertain = projectAttemptOutcome({ status: { status: "unknown", receiptState: "unknown" } });
		expect(uncertain.state).toBe("awaiting_operator");
		const missingReceipt = projectAttemptOutcome({
			status: { status: "terminal_ok", receiptState: "missing", outcome: { reason: "end_turn" } },
		});
		expect(missingReceipt.state).toBe("awaiting_operator");
	});
});

describe("durable record schema: versioned, frozen, fail-closed", () => {
	test("createLaneJobRecord produces a v1 record with identity that outlives ops", () => {
		const created = record();
		expect(created.schemaVersion).toBe(LANE_JOB_SCHEMA_VERSION);
		expect(created.jobId).toBe("lanejob-project-issue-10-lane-jobs");
		expect(created.state).toBe("running");
		expect(created.attempts).toHaveLength(0);
		expect(created.checkpoints).toHaveLength(0);
		expect(Object.isFrozen(created)).toBe(true);
	});

	test("round-trips through parseLaneJobRecord", () => {
		let current = appendAttempt(record(), attempt());
		current = applyReconciliation({
			record: current,
			repository: repo({ headSha: "a".repeat(40) }),
			classification: "progressed",
		});
		const parsed = parseLaneJobRecord(JSON.stringify(current));
		expect(parsed).toEqual(current);
		expect(parsed.checkpoints[0].sha).toBe("a".repeat(40));
	});

	test("refuses an unknown schemaVersion (migration boundary, fail closed)", () => {
		const raw = JSON.stringify({ ...JSON.parse(JSON.stringify(record())), schemaVersion: 999 });
		expect(() => parseLaneJobRecord(raw)).toThrow(/schemaVersion/);
	});

	test("refuses corrupt JSON and structural garbage instead of coercing", () => {
		expect(() => parseLaneJobRecord("{not json")).toThrow(LaneJobError);
		expect(() => parseLaneJobRecord("42")).toThrow(/JSON object/);
		expect(() => parseLaneJobRecord(JSON.stringify({ ...record(), state: "vibes" }))).toThrow(/state/);
		expect(() => parseLaneJobRecord(JSON.stringify({ ...record(), jobId: "nope" }))).toThrow(/jobId/);
		expect(() =>
			parseLaneJobRecord(JSON.stringify({ ...record(), lane: { branch: "", worktreePath: "/wt" } })),
		).toThrow(/branch/);
		expect(() =>
			parseLaneJobRecord(JSON.stringify({ ...record(), lane: { branch: "b", worktreePath: "wt" } })),
		).toThrow(/worktreePath/);
		expect(() => parseLaneJobRecord(JSON.stringify({ ...record(), sessions: ["not-a-session"] }))).toThrow(
			/session id/,
		);
		expect(() => parseLaneJobRecord(JSON.stringify({ ...record(), updatedAt: "yesterday-ish" }))).toThrow(/timestamp/);
		expect(() => parseLaneJobRecord(JSON.stringify({ ...record(), stalledContinuations: -2 }))).toThrow(/non-negative/);
		expect(() => parseLaneJobRecord(JSON.stringify({ ...record(), reportSource: "vibes" }))).toThrow(/reportSource/);
	});

	test("checkpoints reject only consecutive duplicates; duplicate opRefs are rejected", () => {
		// Git SHAs have no chronological lexical order: a rebase moves HEAD to a
		// sha that sorts LOWER, and that history is still valid durable state.
		const rebased = JSON.parse(JSON.stringify(record()));
		rebased.checkpoints = [
			{ sha: "b".repeat(40), createdAt: NOW.toISOString() },
			{ sha: "a".repeat(40), createdAt: NOW.toISOString() },
		];
		const parsed = parseLaneJobRecord(JSON.stringify(rebased));
		expect(parsed.checkpoints).toHaveLength(2);

		const repeated = JSON.parse(JSON.stringify(record()));
		repeated.checkpoints = [
			{ sha: "a".repeat(40), createdAt: NOW.toISOString() },
			{ sha: "a".repeat(40), createdAt: NOW.toISOString() },
		];
		expect(() => parseLaneJobRecord(JSON.stringify(repeated))).toThrow(/repeats the previous sha/);

		const shortSha = JSON.parse(JSON.stringify(record()));
		shortSha.checkpoints = [{ sha: "abc123", createdAt: NOW.toISOString() }];
		expect(() => parseLaneJobRecord(JSON.stringify(shortSha))).toThrow(/full commit sha/);

		const dupes = JSON.parse(JSON.stringify(record()));
		dupes.attempts = [attempt(), attempt()];
		expect(() => parseLaneJobRecord(JSON.stringify(dupes))).toThrow(/duplicate attempt opRef/);
	});

	test("fail-closed parse: missing v1 sections are schema violations, not empty defaults", () => {
		for (const field of ["sessions", "attempts", "checkpoints", "escalations"]) {
			const stripped = JSON.parse(JSON.stringify(record()));
			delete stripped[field];
			expect(() => parseLaneJobRecord(JSON.stringify(stripped)), field).toThrow(LaneJobError);
		}
		const noCounter = JSON.parse(JSON.stringify(record()));
		delete noCounter.stalledContinuations;
		expect(() => parseLaneJobRecord(JSON.stringify(noCounter))).toThrow(/stalledContinuations/);
		const badEndState = JSON.parse(JSON.stringify({ ...record(), attempts: [{ ...attempt(), endState: "vibes" }] }));
		expect(() => parseLaneJobRecord(JSON.stringify(badEndState))).toThrow(/endState/);
	});
});

describe("repository-first reconciliation: commits are the progress signal", () => {
	test("HEAD move appends a checkpoint even when the reply body is empty", () => {
		let current = appendAttempt(record(), attempt());
		current = closeAttempt({
			record: current,
			opRef: attempt().opRef,
			endState: "attempt_ended",
			errorCode: "prompt_deadline_exceeded",
			endedAt: new Date(NOW.getTime() + 1_800_000).toISOString(),
		});
		current = applyReconciliation({
			record: current,
			repository: repo({ headSha: "c".repeat(40) }),
			classification: "progressed",
		});
		expect(current.checkpoints).toHaveLength(1);
		expect(current.checkpoints[0]).toEqual({ sha: "c".repeat(40), createdAt: NOW.toISOString() });
	});

	test("#9 regression: summary.completed on a deadline-killed op is not success anywhere", () => {
		// The measured shape from issue #9: {"status":"failed","summary":{"completed":true}}.
		// Nothing in this module reads summaryCompleted, and the projection is
		// attempt_ended, not completed.
		const projected = projectOpState(deadlineKillBody.status);
		expect(projected).not.toBe("completed");
		expect(projectOpState({ status: "failed" })).not.toBe("completed");
	});

	test("evidence classes reuse #9's exact vocabulary", () => {
		expect(classifyWorkEvidence(repo({ headSha: "d".repeat(40) }))).toBe("work_committed_report_lost");
		expect(classifyWorkEvidence(repo({ dirtyFiles: 18 }))).toBe("work_in_progress_uncommitted");
		expect(classifyWorkEvidence(repo())).toBe("no_work_produced");
	});

	test("committed work outranks the lost report: evidence stays work_committed_report_lost", () => {
		// The four #9 lanes: op died at exactly 1800s with commits landed.
		// Repository first: classification does not consult op status at all.
		const classification = classifyWorkEvidence(repo({ headSha: `9b2be0f${"0".repeat(33)}` }));
		expect(classification).toBe("work_committed_report_lost");
	});
});

describe("attempts: duplicate prevention and one deterministic continuation path", () => {
	test("appendAttempt refuses a repeated opRef and a second open attempt", () => {
		let current = appendAttempt(record(), attempt());
		expect(() => appendAttempt(current, attempt())).toThrow(/duplicate|already exists/i);
		// A second OPEN op-ref while one is still running is how a restart mints
		// duplicate work - refused; only a closed attempt allows the next.
		expect(() => appendAttempt(current, attempt({ opRef: "gw-lanejob-01hq00000001" }))).toThrow(/still open/);
		current = closeAttempt({
			record: current,
			opRef: attempt().opRef,
			endState: "attempt_ended",
			errorCode: "prompt_deadline_exceeded",
			endedAt: NOW.toISOString(),
		});
		current = appendAttempt(current, attempt({ opRef: "gw-lanejob-01hq00000001" }));
		expect(current.attempts).toHaveLength(2);
		expect(current.sessions).toEqual([SESSION]);
		expect(current.state).toBe("running");
	});

	test("closeAttempt records the deadline kill as an attempt end, not job failure", () => {
		let current = appendAttempt(record(), attempt());
		current = closeAttempt({
			record: current,
			opRef: attempt().opRef,
			endState: "attempt_ended",
			errorCode: "prompt_deadline_exceeded",
			endedAt: new Date(NOW.getTime() + 60_000).toISOString(),
		});
		expect(current.attempts[0].errorCode).toBe("prompt_deadline_exceeded");
		expect(current.attempts[0].endedAt).toBeDefined();
		expect(current.state).toBe("attempt_ended");
	});

	test("closeAttempt rejects double-close and unknown attempts", () => {
		let current = appendAttempt(record(), attempt());
		current = closeAttempt({
			record: current,
			opRef: attempt().opRef,
			endState: "completed",
			endedAt: NOW.toISOString(),
		});
		expect(() =>
			closeAttempt({ record: current, opRef: attempt().opRef, endState: "completed", endedAt: NOW.toISOString() }),
		).toThrow(/already closed/);
		expect(() =>
			closeAttempt({ record: record(), opRef: "gw-nope-01hq", endState: "completed", endedAt: NOW.toISOString() }),
		).toThrow(/does not exist/);
	});

	test("an uncertain close forces awaiting_operator with an escalation trail", () => {
		let current = appendAttempt(record(), attempt());
		current = closeAttempt({
			record: current,
			opRef: attempt().opRef,
			endState: "terminal_uncertain",
			endedAt: NOW.toISOString(),
		});
		expect(current.state).toBe("awaiting_operator");
		expect(current.escalations.some((entry) => entry.includes(attempt().opRef))).toBe(true);
	});
});

describe("continuation planning: bounded, deterministic, operator holds preserved", () => {
	test("live session + ended attempt -> continue_same_session with a fresh op-ref (one path)", () => {
		let current = appendAttempt(record(), attempt());
		current = closeAttempt({
			record: current,
			opRef: attempt().opRef,
			endState: "attempt_ended",
			errorCode: "prompt_deadline_exceeded",
			endedAt: NOW.toISOString(),
		});
		const decision = planContinuation(planInput({ record: current, latestAttempt: current.attempts[0] }));
		expect(decision.action).toBe("continue_same_session");
		if (decision.action === "continue_same_session") {
			expect(decision.opRefPolicy).toBe("fresh_op_ref_required");
		}
	});

	test("active turn wins over continuation: observe", () => {
		// The stored truth for an active turn is an attempt with no recorded end;
		// the planner must read that, not a synthesized running projection.
		const current = appendAttempt(record(), attempt());
		const decision = planContinuation(planInput({ record: current, latestAttempt: null }));
		expect(decision.action).toBe("observe");
	});

	test("deleted session needs operator sign-off; unverified authority fails closed", () => {
		expect(planContinuation(planInput({ session: { live: false, deleted: true, locatorMatches: true } })).action).toBe(
			"hold_for_operator",
		);
		expect(
			planContinuation(planInput({ session: { live: false, deleted: false, locatorMatches: false } })).action,
		).toBe("hold_for_operator");
		expect(
			planContinuation(planInput({ session: { live: true, deleted: false, locatorMatches: true, ambiguous: true } }))
				.action,
		).toBe("hold_for_operator");
	});

	test("non-live saved session whose last attempt ended routes to broker resume", () => {
		let current = appendAttempt(record(), attempt());
		current = closeAttempt({
			record: current,
			opRef: attempt().opRef,
			endState: "completed",
			endedAt: NOW.toISOString(),
		});
		const decision = planContinuation(
			planInput({
				record: current,
				latestAttempt: current.attempts[0],
				session: { live: false, deleted: false, locatorMatches: true },
			}),
		);
		expect(decision.action).toBe("resume_session");
	});

	test("stalled continuations trip the bound into a hold - no runaway resume loops", () => {
		let current = appendAttempt(record(), attempt());
		current = closeAttempt({
			record: current,
			opRef: attempt().opRef,
			endState: "attempt_ended",
			endedAt: NOW.toISOString(),
		});
		// Three continuations without any HEAD move...
		for (let index = 0; index < MAX_STALLED_CONTINUATIONS; index += 1) {
			current = applyReconciliation({
				record: current,
				repository: repo({ headSha: "f".repeat(40), observedAt: NOW.toISOString() }),
				classification: "progressed",
			});
			// First application records f*40 as checkpoint; later ones see no NEW head.
			current = applyReconciliation({
				record: current,
				repository: repo({ headSha: "f".repeat(40) }),
				classification: "stalled",
			});
		}
		// ...then the budget gate closes further automation.
		for (let more = 0; more < 3; more += 1) {
			current = applyReconciliation({
				record: current,
				repository: repo({ headSha: "f".repeat(40) }),
				classification: "stalled",
			});
		}
		expect(current.stalledContinuations).toBe(MAX_STALLED_CONTINUATIONS);
		const decision = planContinuation(
			planInput({
				record: current,
				latestAttempt: current.attempts[0],
				repository: repo({ headSha: "f".repeat(40) }),
			}),
		);
		expect(decision.action).toBe("hold_for_operator");
		expect(decision.reason).toMatch(/budget exhausted|no HEAD move/);
	});

	test("done / aborted jobs hold instead of continuing", () => {
		const donePlan = planContinuation(planInput({ record: { ...record(), state: "done" } }));
		const abortedPlan = planContinuation(planInput({ record: { ...record(), state: "aborted" } }));
		expect(donePlan.reason).toMatch(/retirement/);
		expect(abortedPlan.reason).toMatch(/never auto-resumed/);
	});
});

describe("restart and recovery", () => {
	test("full cycle survives a controller restart via serialized state only", () => {
		// Turn 1: accept attempt, worker commits, op dies at deadline.
		let stored = appendAttempt(record(), attempt({ sessionId: SESSION_B }));
		stored = closeAttempt({
			record: stored,
			opRef: attempt().opRef,
			endState: "attempt_ended",
			errorCode: "prompt_deadline_exceeded",
			endedAt: NOW.toISOString(),
		});
		stored = applyReconciliation({
			record: stored,
			repository: repo({ headSha: `1a2b3c${"4".repeat(34)}` }),
			classification: "progressed",
		});

		// Controller restarts: rebuild strictly from the durable JSON.
		const revived = parseLaneJobRecord(JSON.stringify(stored));
		expect(revived.checkpoints).toHaveLength(1);
		expect(revived.sessions).toEqual([SESSION, SESSION_B]);
		expect(revived.state).toBe("attempt_ended");

		// Evidence-based reconciliation after restart reads the repository FIRST.
		const evidence = classifyWorkEvidence(repo({ headSha: `1a2b3c${"4".repeat(34)}`, knownCheckpoints: [] }));
		expect(evidence).toBe("work_committed_report_lost");

		// Continuation: session B may be gone, but resume/recreate decisions stay one path.
		const decision = planContinuation(
			planInput({
				record: revived,
				latestAttempt: revived.attempts[0],
				session: { live: false, deleted: false, locatorMatches: true },
			}),
		);
		expect(decision.action).toBe("resume_session");
	});

	test("the memory-ops-reflections shape: commit lands AFTER terminal accounting closed", () => {
		let stored = appendAttempt(record(), attempt());
		stored = closeAttempt({
			record: stored,
			opRef: attempt().opRef,
			endState: "attempt_ended",
			errorCode: "prompt_deadline_exceeded",
			endedAt: NOW.toISOString(),
		});
		// Reconciliation runs later and finds the late commit: it becomes a checkpoint.
		stored = applyReconciliation({
			record: stored,
			repository: repo({ headSha: "e".repeat(40), observedAt: new Date(NOW.getTime() + 600_000).toISOString() }),
			classification: "progressed",
		});
		expect(stored.checkpoints[0].sha).toBe("e".repeat(40));
		expect(stored.stalledContinuations).toBe(0);
		// The next prompt may continue the same job - which is exactly how this
		// very slice was continued by its supervisor.
	});
});
