import { describe, expect, test } from "bun:test";
import {
	assertDeleteNotSupported,
	buildHandoff,
	CENSUS_KEYS,
	canClose,
	contaminationFreeze,
	decideRecovery,
	detectContamination,
	type RecoveryInput,
	type RetirementCensus,
	RetirementError,
} from "../src/recovery";

function input(overrides: {
	session?: Partial<RecoveryInput["session"]>;
	operation?: Partial<RecoveryInput["operation"]>;
	lane?: Partial<RecoveryInput["lane"]>;
	workComplete?: boolean;
}): RecoveryInput {
	return {
		session: {
			live: true,
			deleted: false,
			locatorMatches: true,
			savedAuthorityValid: true,
			...overrides.session,
		},
		operation: {
			status: "in_flight",
			receiptState: "absent",
			supervisorState: "running",
			...overrides.operation,
		},
		lane: {
			sideEffectsVerified: true,
			ownershipUnchanged: true,
			...overrides.lane,
		},
		...(overrides.workComplete === undefined ? {} : { workComplete: overrides.workComplete }),
	};
}

const terminalOk = {
	status: "terminal_ok" as const,
	receiptState: "present" as const,
	supervisorState: "completed" as const,
};

describe("decideRecovery: live + active", () => {
	test("observes instead of sending a second prompt", () => {
		const decision = decideRecovery(input({}));
		expect(decision).toMatchObject({ action: "observe", freezeMutations: false });
		expect(decision.reason).toMatch(/never auto-send/);
	});

	test("an accepted-but-not-started op is still active", () => {
		const decision = decideRecovery(input({ operation: { status: "accepted", supervisorState: "accepted" } }));
		expect(decision.action).toBe("observe");
	});
});

describe("decideRecovery: live + terminal", () => {
	test("continues in the same session with a fresh op-ref", () => {
		expect(decideRecovery(input({ operation: terminalOk })).action).toBe("fresh_turn");
	});

	test("a failed turn on a healthy session is also a fresh turn, not a recreate", () => {
		const decision = decideRecovery(
			input({ operation: { status: "failed", receiptState: "present", supervisorState: "failed" } }),
		);
		expect(decision.action).toBe("fresh_turn");
	});

	test("holds when the prior side effects were never re-examined", () => {
		const decision = decideRecovery(
			input({ operation: terminalOk, lane: { sideEffectsVerified: false, ownershipUnchanged: true } }),
		);
		expect(decision).toMatchObject({ action: "operator_hold", freezeMutations: true });
	});

	test("holds when lane ownership changed underneath", () => {
		const decision = decideRecovery(
			input({ operation: terminalOk, lane: { sideEffectsVerified: true, ownershipUnchanged: false } }),
		);
		expect(decision.action).toBe("operator_hold");
	});

	test("completed work routes to the retirement sequence, not another prompt", () => {
		const decision = decideRecovery(input({ operation: terminalOk, workComplete: true }));
		expect(decision.action).toBe("operator_hold");
		expect(decision.reason).toMatch(/handoff -> close -> verify -> registry retire/);
	});
});

describe("decideRecovery: not live", () => {
	test("resumes a saved, valid session whose op is terminal", () => {
		const decision = decideRecovery(
			input({ session: { live: false, savedAuthorityValid: true }, operation: terminalOk }),
		);
		expect(decision.action).toBe("session_resume");
	});

	test("recreates only when resume is impossible and side effects are known", () => {
		const decision = decideRecovery(
			input({
				session: { live: false, savedAuthorityValid: false },
				operation: terminalOk,
				lane: { sideEffectsVerified: true, ownershipUnchanged: true, resumeImpossible: true },
			}),
		);
		expect(decision.action).toBe("recreate");
		expect(decision.reason).toMatch(/retire predecessor/);
	});

	test("never recreates while side effects are unknown", () => {
		const decision = decideRecovery(
			input({
				session: { live: false },
				operation: terminalOk,
				lane: { sideEffectsVerified: false, ownershipUnchanged: true, resumeImpossible: true },
			}),
		);
		expect(decision).toMatchObject({ action: "operator_hold", freezeMutations: true });
	});

	test("a stopped session with an active op is a stale snapshot to re-read", () => {
		const decision = decideRecovery(input({ session: { live: false } }));
		expect(decision.action).toBe("operator_hold");
		expect(decision.reason).toMatch(/inspect -> status -> inspect/);
	});

	test("a deleted session never auto-recreates", () => {
		const decision = decideRecovery(input({ session: { live: false, deleted: true }, operation: terminalOk }));
		expect(decision).toMatchObject({ action: "operator_hold", freezeMutations: true });
	});

	test("holds when saved authority cannot be validated and resume is not ruled out", () => {
		const decision = decideRecovery(
			input({ session: { live: false, savedAuthorityValid: false }, operation: terminalOk }),
		);
		expect(decision.action).toBe("operator_hold");
	});
});

describe("decideRecovery: uncertainty wins over everything", () => {
	test.each([
		["unknown status", { status: "unknown" as const, supervisorState: "terminal_uncertain" as const }],
		[
			"terminal_missing_receipt",
			{ status: "terminal_ok" as const, supervisorState: "terminal_missing_receipt" as const },
		],
	])("%s holds and freezes", (_label, operation) => {
		const decision = decideRecovery(input({ operation }));
		expect(decision).toMatchObject({ action: "operator_hold", freezeMutations: true });
	});

	test("ambiguous authority holds even with a perfect terminal op", () => {
		const decision = decideRecovery(input({ session: { ambiguous: true }, operation: terminalOk }));
		expect(decision.reason).toMatch(/ambiguous/);
		expect(decision.freezeMutations).toBe(true);
	});

	test("a duplicate worktree owner holds", () => {
		const decision = decideRecovery(input({ session: { duplicateOwner: true }, operation: terminalOk }));
		expect(decision).toMatchObject({ action: "operator_hold" });
	});

	test("a locator mismatch holds", () => {
		const decision = decideRecovery(input({ session: { locatorMatches: false }, operation: terminalOk }));
		expect(decision).toMatchObject({ action: "operator_hold" });
	});
});

describe("detectContamination", () => {
	test("classifies authority, scope and operation signals", () => {
		const signals = detectContamination({
			ambiguousRow: true,
			branchMismatch: true,
			priorOpUnknown: true,
		});
		expect(signals.map((signal) => signal.kind).sort()).toEqual(["authority", "operation", "scope"]);
		expect(contaminationFreeze(signals)).toBe(true);
	});

	test("ordinary operational states are not contamination", () => {
		// dirty worktree, failing tests, provider error, prompt_deadline_exceeded,
		// idle session after a completed turn, expected gate, PR awaiting CI.
		const signals = detectContamination({});
		expect(signals).toEqual([]);
		expect(contaminationFreeze(signals)).toBe(false);
	});

	test("an authority shift observed across the double inspect is contamination", () => {
		const signals = detectContamination({ authorityShifted: true });
		expect(signals[0]).toMatchObject({ kind: "authority", code: "authority_shifted" });
	});

	test("a receipt identity mismatch is operation contamination", () => {
		const signals = detectContamination({ receiptIdentityMismatch: true });
		expect(signals[0]).toMatchObject({ kind: "operation", code: "receipt_identity_mismatch" });
	});
});

describe("retirement", () => {
	const fullCensus: RetirementCensus = {
		inspect: true,
		lastOperationStatus: true,
		workflowGates: true,
		queueMessages: true,
		lastAssistant: true,
		gitState: true,
		prAndCi: true,
	};

	const record = {
		laneKey: "project-feature-subsessions-runtime",
		sessionId: "ad2f2494-2584-4d13-b7b6-c6ac24a1087f",
		worktree: "/wt/subsession-runtime",
		branch: "feat/subsession-runtime",
		baseRef: "origin/main",
		baseSha: "7e0150e0000000000000000000000000000000aa",
		headSha: "fcbb8840000000000000000000000000000000bb",
		lastOperationRef: "gw-pr-a-01hq",
		operationState: "completed" as const,
		receiptState: "present" as const,
		pendingGates: [],
		queueState: "empty",
		prNumber: 5,
		retirementReason: "work delivered",
	};

	test("builds a handoff once the census is complete", () => {
		const handoff = buildHandoff(fullCensus, record, () => new Date(0));
		expect(handoff).toMatchObject({ laneKey: record.laneKey, prNumber: 5 });
		expect(handoff.timestamp).toBe(new Date(0).toISOString());
	});

	test("refuses a handoff assembled from a partial census", () => {
		for (const key of CENSUS_KEYS) {
			const census = { ...fullCensus, [key]: false };
			expect(() => buildHandoff(census, record)).toThrow(RetirementError);
		}
	});

	test("the handoff names the missing census steps", () => {
		expect(() => buildHandoff({ ...fullCensus, prAndCi: false, gitState: false }, record)).toThrow(/gitState, prAndCi/);
	});

	test("close requires a quiet lane and a persisted handoff", () => {
		expect(
			canClose({
				authorityUnambiguous: true,
				terminalUncertain: false,
				activeOperation: false,
				handoffPersisted: true,
			}),
		).toEqual({ allowed: true });
	});

	test.each([
		["authorityUnambiguous", { authorityUnambiguous: false }],
		["terminalUncertain", { terminalUncertain: true }],
		["activeOperation", { activeOperation: true }],
		["handoffPersisted", { handoffPersisted: false }],
	])("close is refused on %s", (_label, overrides) => {
		const result = canClose({
			authorityUnambiguous: true,
			terminalUncertain: false,
			activeOperation: false,
			handoffPersisted: true,
			...overrides,
		});
		expect(result.allowed).toBe(false);
		expect(result.reason).toBeTruthy();
	});

	test("session.delete is refused outright", () => {
		expect(() => assertDeleteNotSupported()).toThrow(/requires an explicit separate grant/);
	});
});
