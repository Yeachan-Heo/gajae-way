/**
 * Recovery and retirement decisions.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26, 4/5). Three actions are
 * kept strictly separate, because collapsing them is how a supervisor ends up
 * with two mutation owners on one branch:
 *
 *   - fresh turn:     a new `turn.prompt` on the SAME live session
 *   - session resume: a dead-but-saved session restarted via broker
 *                     `session.resume`
 *   - recreate:       a brand new session id
 *
 * "work is left" is never sufficient reason for resume or recreate. The order of
 * judgement is always: broker authority -> reconcile the existing opRef ->
 * verify worktree/branch/owner -> only then choose an action.
 */

import type { PromptStatus, ReceiptState, SupervisorOpState } from "./status";

export type RecoveryAction =
	/** Live turn in progress: watch and handle gates. Never auto-send. */
	| "observe"
	/** Same live session, new op-ref. */
	| "fresh_turn"
	/** Broker lifecycle restart of a saved, non-live session. */
	| "session_resume"
	/** New session id, same lane, predecessor retired first. */
	| "recreate"
	/** Freeze mutations and wait for a human. */
	| "operator_hold";

export type RecoveryInput = {
	readonly session: {
		readonly live: boolean;
		readonly deleted: boolean;
		readonly ambiguous?: boolean;
		/** Broker can validate the saved transcript identity. */
		readonly savedAuthorityValid?: boolean;
		/** Session cwd equals the lane worktree. */
		readonly locatorMatches: boolean;
		/** Another live session owns the same worktree. */
		readonly duplicateOwner?: boolean;
	};
	readonly operation: {
		readonly status: PromptStatus;
		readonly receiptState?: ReceiptState;
		readonly supervisorState: SupervisorOpState;
	};
	readonly lane: {
		/** The previous op's repository side effects were re-examined. */
		readonly sideEffectsVerified: boolean;
		readonly ownershipUnchanged: boolean;
		/** Broker resume is impossible, e.g. the saved transcript is damaged. */
		readonly resumeImpossible?: boolean;
	};
	/** The lane's work is finished or no longer needed. */
	readonly workComplete?: boolean;
};

export type RecoveryDecision = {
	readonly action: RecoveryAction;
	readonly freezeMutations: boolean;
	readonly reason: string;
};

const UNCERTAIN_STATES = new Set<SupervisorOpState>(["terminal_uncertain", "terminal_missing_receipt"]);

/**
 * The decision table, in order of precedence.
 *
 * Uncertainty is checked before anything else on purpose: the most dangerous
 * recovery is "it looks dead, start another one", and that is exactly what an
 * unknown status or an ambiguous authority invites.
 */
export function decideRecovery(input: RecoveryInput): RecoveryDecision {
	const { session, operation, lane } = input;

	if (session.ambiguous === true) {
		return hold("broker authority is ambiguous");
	}
	if (!session.locatorMatches) {
		return hold("session locator does not match the lane worktree");
	}
	if (session.duplicateOwner === true) {
		return hold("another live session owns this worktree");
	}
	if (operation.status === "unknown" || UNCERTAIN_STATES.has(operation.supervisorState)) {
		return hold(`operation state ${operation.supervisorState} is not decidable; do not resend, resume or recreate`);
	}

	const sessionUsable = session.live && !session.deleted;
	const operationTerminal = operation.status === "terminal_ok" || operation.status === "failed";

	if (sessionUsable && !operationTerminal) {
		return {
			action: "observe",
			freezeMutations: false,
			reason: "an active turn is running: observe and handle gates, never auto-send a second prompt",
		};
	}

	if (sessionUsable && operationTerminal) {
		if (!lane.sideEffectsVerified) {
			return hold("the previous operation's repository side effects have not been re-examined");
		}
		if (!lane.ownershipUnchanged) {
			return hold("lane ownership changed since the operation was accepted");
		}
		if (input.workComplete === true) {
			return {
				action: "operator_hold",
				freezeMutations: true,
				reason: "work is complete: run the retirement sequence (handoff -> close -> verify -> registry retire)",
			};
		}
		return {
			action: "fresh_turn",
			freezeMutations: false,
			reason: "same live session, new op-ref continuation",
		};
	}

	// Session is not usable from here on.
	if (session.deleted) {
		return hold("session is deleted; resume is not available and recreation needs operator sign-off");
	}
	if (!operationTerminal) {
		return hold(
			"inconsistent snapshot: session is not live while the operation still reads active; re-read inspect -> status -> inspect",
		);
	}
	if (!lane.sideEffectsVerified) {
		return hold("cannot recover a stopped session before its side effects are known");
	}
	if (lane.resumeImpossible !== true && session.savedAuthorityValid === true) {
		return {
			action: "session_resume",
			freezeMutations: false,
			reason: "saved authority is valid and the prior operation is terminal: broker session.resume",
		};
	}
	if (lane.resumeImpossible === true) {
		return {
			action: "recreate",
			freezeMutations: false,
			reason: "resume is impossible and the prior operation is terminal: retire predecessor, recreate successor",
		};
	}
	return hold("saved authority could not be validated for resume");
}

function hold(reason: string): RecoveryDecision {
	return { action: "operator_hold", freezeMutations: true, reason };
}

export type ContaminationClass = "authority" | "scope" | "operation";

export type ContaminationSignal = {
	readonly kind: ContaminationClass;
	readonly code: string;
	readonly detail: string;
};

export type ContaminationInput = {
	readonly ambiguousRow?: boolean;
	readonly multipleStateRoots?: boolean;
	readonly terminalUncertain?: boolean;
	/** Authority, generation or locator changed between the two inspects. */
	readonly authorityShifted?: boolean;
	readonly locatorMismatch?: boolean;
	readonly attachmentUnverified?: boolean;
	readonly cwdOutsideLane?: boolean;
	readonly branchMismatch?: boolean;
	readonly duplicateMutationOwner?: boolean;
	readonly taskIdentityChanged?: boolean;
	readonly foreignChangesMixed?: boolean;
	readonly baseMissingFromHistory?: boolean;
	readonly receiptIdentityMismatch?: boolean;
	readonly priorOpUnknown?: boolean;
	readonly terminalWithoutReceiptOrSideEffect?: boolean;
	readonly incompatiblePendingGate?: boolean;
	readonly unintendedFollowUpQueued?: boolean;
	readonly activeOpRefMismatch?: boolean;
};

/**
 * Contamination means authority or scope can no longer be trusted.
 *
 * Deliberately NOT contamination: a dirty worktree, failing tests, a provider
 * error, `prompt_deadline_exceeded`, an idle session after a completed turn, an
 * expected pending gate, or a PR waiting on CI. Those are ordinary states, and
 * treating them as contamination would freeze the lane for no reason.
 */
export function detectContamination(input: ContaminationInput): readonly ContaminationSignal[] {
	const signals: ContaminationSignal[] = [];
	const add = (kind: ContaminationClass, code: string, detail: string, flag?: boolean) => {
		if (flag === true) {
			signals.push({ kind, code, detail });
		}
	};

	add("authority", "ambiguous_row", "broker row reports ambiguous=true", input.ambiguousRow);
	add("authority", "multiple_state_roots", "one session id claims more than one state root", input.multipleStateRoots);
	add("authority", "terminal_uncertain", "terminalUncertain=true", input.terminalUncertain);
	add(
		"authority",
		"authority_shifted",
		"authority, generation or locator changed between inspect -> status -> inspect",
		input.authorityShifted,
	);
	add(
		"authority",
		"locator_mismatch",
		"same session id but locator.repo is not the expected worktree",
		input.locatorMismatch,
	);
	add(
		"authority",
		"attachment_unverified",
		"row is live but broker attachment verification failed",
		input.attachmentUnverified,
	);

	add("scope", "cwd_outside_lane", "session cwd differs from the lane worktree", input.cwdOutsideLane);
	add("scope", "branch_mismatch", "checked-out branch differs from the lane registry branch", input.branchMismatch);
	add(
		"scope",
		"duplicate_mutation_owner",
		"two mutation owners on the same worktree or branch",
		input.duplicateMutationOwner,
	);
	add(
		"scope",
		"task_identity_changed",
		"session task/PR identity no longer matches lane metadata",
		input.taskIdentityChanged,
	);
	add(
		"scope",
		"foreign_changes_mixed",
		"changes from another issue or PR are mixed in with unclear attribution",
		input.foreignChangesMixed,
	);
	add(
		"scope",
		"base_missing_from_history",
		"expected base is not in HEAD history or worktree identity changed",
		input.baseMissingFromHistory,
	);

	add(
		"operation",
		"receipt_identity_mismatch",
		"stored commandId/turnId do not match the status response identity",
		input.receiptIdentityMismatch,
	);
	add("operation", "prior_op_unknown", "prior operation status is unknown", input.priorOpUnknown);
	add(
		"operation",
		"terminal_without_evidence",
		"terminal with no receipt and no decidable repository side effect",
		input.terminalWithoutReceiptOrSideEffect,
	);
	add(
		"operation",
		"incompatible_pending_gate",
		"an incompatible prior workflow gate is still pending",
		input.incompatiblePendingGate,
	);
	add(
		"operation",
		"unintended_follow_up",
		"unintended follow-up or queued work remains",
		input.unintendedFollowUpQueued,
	);
	add(
		"operation",
		"active_opref_mismatch",
		"the active operation differs from the opRef the supervisor believes it owns",
		input.activeOpRefMismatch,
	);

	return signals;
}

/** Any contamination signal freezes mutations before any repair is attempted. */
export function contaminationFreeze(signals: readonly ContaminationSignal[]): boolean {
	return signals.length > 0;
}

export type LaneLifecycle = "active" | "retiring" | "held" | "retired";

export type RetirementCensus = {
	readonly inspect: boolean;
	readonly lastOperationStatus: boolean;
	readonly workflowGates: boolean;
	readonly queueMessages: boolean;
	readonly lastAssistant: boolean;
	readonly gitState: boolean;
	readonly prAndCi: boolean;
};

export const CENSUS_KEYS: readonly (keyof RetirementCensus)[] = [
	"inspect",
	"lastOperationStatus",
	"workflowGates",
	"queueMessages",
	"lastAssistant",
	"gitState",
	"prAndCi",
];

export type RetireHandoff = {
	readonly laneKey: string;
	readonly sessionId: string;
	readonly worktree: string;
	readonly branch: string;
	readonly baseRef: string;
	readonly baseSha: string;
	readonly headSha: string;
	readonly lastOperationRef: string;
	readonly operationState: SupervisorOpState;
	readonly receiptState: ReceiptState;
	readonly pendingGates: readonly string[];
	readonly queueState: string;
	readonly prNumber?: number;
	readonly retirementReason: string;
	readonly successorSessionId?: string;
	readonly timestamp: string;
};

export class RetirementError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "RetirementError";
	}
}

/**
 * Builds the durable handoff record for a retiring lane.
 *
 * The census must be complete first: a handoff assembled from a partial census
 * is the record that later gets trusted as evidence it never was. Bounded
 * identity and evidence only - never a credential or raw path dump.
 */
export function buildHandoff(
	census: RetirementCensus,
	record: Omit<RetireHandoff, "timestamp">,
	now: () => Date = () => new Date(),
): RetireHandoff {
	const missing = CENSUS_KEYS.filter((key) => census[key] !== true);
	if (missing.length > 0) {
		throw new RetirementError(`final census incomplete: ${missing.join(", ")}`);
	}
	return { ...record, timestamp: now().toISOString() };
}

export type CloseGate = {
	readonly authorityUnambiguous: boolean;
	readonly terminalUncertain: boolean;
	readonly activeOperation: boolean;
	readonly handoffPersisted: boolean;
};

/**
 * `close` is the conservative, recoverable ending; it is allowed only once the
 * lane is quiet and the handoff exists.
 */
export function canClose(gate: CloseGate): { readonly allowed: boolean; readonly reason?: string } {
	if (!gate.authorityUnambiguous) {
		return { allowed: false, reason: "authority is ambiguous" };
	}
	if (gate.terminalUncertain) {
		return { allowed: false, reason: "terminalUncertain is set" };
	}
	if (gate.activeOperation) {
		return { allowed: false, reason: "an operation is still active" };
	}
	if (!gate.handoffPersisted) {
		return { allowed: false, reason: "durable handoff has not been persisted" };
	}
	return { allowed: true };
}

/**
 * `session.delete` is out of scope for this runtime.
 *
 * Close is recoverable; delete reaches saved history and cleanup authority. It
 * is a destructive mutation that needs its own explicit grant, so the runtime
 * refuses it rather than offering a convenient wrapper.
 */
export function assertDeleteNotSupported(): never {
	throw new RetirementError(
		"session.delete is not part of this runtime: close is recoverable, delete is destructive and requires an explicit separate grant",
	);
}
