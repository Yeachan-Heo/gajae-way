/**
 * Prompt status projection.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26, 3/5): the canonical
 * top-level statuses of `gjc sdk session status` are exactly five. `completed`,
 * `cancelled` and `error` are NOT top-level statuses - a cancel arrives as
 * `terminal_ok` with `outcome.reason === "cancelled"`, so the stop reason must be
 * read from `outcome` rather than inferred from the status word.
 *
 * `unknown` is neither running nor terminal: it means the reconciliation record
 * could not be found, so it is held for operator judgement. Auto-closing it is
 * forbidden, and so is auto-treating it as a live turn or resending its op-ref.
 */

import type { ControllerOptions } from "./cli";
import { parseEnvelope } from "./cli";

export type PromptStatus = "accepted" | "in_flight" | "terminal_ok" | "failed" | "unknown";

export type ReceiptState = "absent" | "present" | "missing" | "unknown";

/** Normal stop reasons carried by a `terminal_ok` outcome. */
export type StopReason = "end_turn" | "max_tokens" | "max_turn_requests" | "refusal" | "cancelled";

/** Error payload carried by a `failed` status, e.g. `prompt_deadline_exceeded`. */
export type FailureBody = {
	readonly code?: string;
	readonly message?: string;
};
/**
 * Terminal outcome body. A failed outcome carries the runtime's own bounded
 * classifiers (`phase`, `category`) and its safe code next to the redacted
 * message, and those are the only facts that make a post-start failure
 * triageable, so they are typed here rather than dropped at the boundary.
 */
export type PromptOutcomeBody = {
	readonly kind?: string;
	readonly reason?: string;
	readonly provenance?: string;
	readonly code?: string;
	readonly message?: string;
	readonly providerCode?: string;
	readonly phase?: string;
	readonly category?: string;
};

export type PromptStatusBody = {
	readonly status: PromptStatus;
	readonly commandId?: string;
	readonly turnId?: string;
	readonly clientRef?: string;
	readonly acceptedAt?: number;
	readonly startedAt?: number;
	readonly terminalAt?: number;
	readonly receiptState?: ReceiptState;
	readonly outcome?: PromptOutcomeBody;
	/** Failure detail. A lease/deadline kill is distinctly resumable, not a work-verdict. */
	readonly error?: FailureBody;
};

export type StatusReport = {
	readonly operationRef: string;
	readonly status: PromptStatusBody;
	/**
	 * `summary.completed` is true only for `terminal_ok | failed`. It is a
	 * convenience projection over reaching a terminal transition, NOT a verdict
	 * that the work succeeded, so nothing here treats it as one.
	 */
	readonly summaryCompleted: boolean;
};

export type SupervisorOpState =
	| "accepted"
	| "running"
	| "completed"
	| "cancelled"
	| "stopped_incomplete"
	| "failed"
	| "terminal_missing_receipt"
	| "failed"
	/**
	 * The op reached a terminal failure that does NOT end the job: a lease or
	 * deadline kill (`error.code = prompt_deadline_exceeded`) while the session
	 * and its work can continue. Distinctly resumable; never a hold, never a
	 * success verdict.
	 */
	| "attempt_ended"
	| "terminal_uncertain";
const CANONICAL_STATUSES = new Set<PromptStatus>(["accepted", "in_flight", "terminal_ok", "failed", "unknown"]);

const RECEIPT_STATES = new Set<ReceiptState>(["absent", "present", "missing", "unknown"]);

export function isPromptStatus(value: unknown): value is PromptStatus {
	return typeof value === "string" && CANONICAL_STATUSES.has(value as PromptStatus);
}

/** Parses a `session status` envelope, refusing statuses outside the contract. */
export function parseStatusReport(result: Parameters<typeof parseEnvelope>[0]): StatusReport {
	const payload = parseEnvelope<{
		operationRef?: unknown;
		status?: Record<string, unknown>;
		summary?: { completed?: unknown };
	}>(result, "session status");

	const rawStatus = payload.status?.status;
	if (!isPromptStatus(rawStatus)) {
		throw new Error(`session status returned a non-canonical status: ${JSON.stringify(rawStatus)}`);
	}
	const receiptState = payload.status?.receiptState;
	const outcome = payload.status?.outcome as PromptOutcomeBody | undefined;

	return {
		operationRef: typeof payload.operationRef === "string" ? payload.operationRef : "",
		status: {
			status: rawStatus,
			...pickString(payload.status, "commandId"),
			...pickString(payload.status, "turnId"),
			...pickString(payload.status, "clientRef"),
			...pickNumber(payload.status, "acceptedAt"),
			...pickNumber(payload.status, "startedAt"),
			...pickNumber(payload.status, "terminalAt"),
			...(typeof receiptState === "string" && RECEIPT_STATES.has(receiptState as ReceiptState)
				? { receiptState: receiptState as ReceiptState }
				: {}),
			...(outcome && typeof outcome === "object" ? { outcome } : {}),
			...pickFailure(payload.status?.error),
		},
		summaryCompleted: payload.summary?.completed === true,
	};
}

function pickFailure(source: unknown): { error?: FailureBody } {
	if (typeof source !== "object" || source === null) {
		return {};
	}
	const record = source as Record<string, unknown>;
	const code = typeof record.code === "string" ? { code: record.code } : {};
	const message = typeof record.message === "string" ? { message: record.message } : {};
	return Object.keys(code).length + Object.keys(message).length > 0 ? { error: { ...code, ...message } } : {};
}
function pickString<K extends string>(source: Record<string, unknown> | undefined, key: K): Partial<Record<K, string>> {
	const value = source?.[key];
	return typeof value === "string" ? ({ [key]: value } as Partial<Record<K, string>>) : {};
}

function pickNumber<K extends string>(source: Record<string, unknown> | undefined, key: K): Partial<Record<K, number>> {
	const value = source?.[key];
	return typeof value === "number" ? ({ [key]: value } as Partial<Record<K, number>>) : {};
}
/**
 * The failure codes that mean "the op's accounting ended, the job may continue".
 * `prompt_deadline_exceeded` is the measured #9/#10 shape: a lease kill while
 * worker commits were still landing. Any other (or absent) code stays `failed`.
 */
export const ATTEMPT_ENDED_CODES: readonly string[] = ["prompt_deadline_exceeded"];

export function isAttemptEndedCode(code: string | undefined): boolean {
	return code !== undefined && ATTEMPT_ENDED_CODES.includes(code);
}

/**
 * A `failed` status is still terminal for the OP; the projection layer decides
 * whether it was merely an attempt ending (see {@link isAttemptEndedCode}).
 */
export function isTerminalStatus(status: PromptStatus): boolean {
	return status === "terminal_ok" || status === "failed";
}

/**
 * Projects the canonical status onto the supervisor's operational state.
 *
 * Terminal-but-unaccounted receipts are surfaced separately instead of being
 * folded into success: a terminal transition whose receipt is missing or unknown
 * is exactly the case where a supervisor would otherwise claim a deliverable it
 * cannot show.
 */
export function projectOpState(status: PromptStatusBody): SupervisorOpState {
	if (status.status === "unknown") {
		return "terminal_uncertain";
	}
	if (status.status === "accepted") {
		return "accepted";
	}
	if (status.status === "in_flight") {
		return "running";
	}

	if (status.receiptState === "missing") {
		return "terminal_missing_receipt";
	}
	if (status.receiptState === "unknown") {
		return "terminal_uncertain";
	}
	if (status.status === "failed") {
		return isAttemptEndedCode(status.error?.code) ? "attempt_ended" : "failed";
	}

	switch (status.outcome?.reason) {
		case "end_turn":
			return "completed";
		case "cancelled":
			return "cancelled";
		case "max_tokens":
		case "max_turn_requests":
		case "refusal":
			return "stopped_incomplete";
		default:
			// A terminal transition without a readable reason is not a success.
			return "stopped_incomplete";
	}
}

const HOLD_STATES = new Set<SupervisorOpState>(["terminal_uncertain", "terminal_missing_receipt"]);

/** States that must stop automation and wait for operator judgement. */
export function requiresOperatorHold(state: SupervisorOpState): boolean {
	return HOLD_STATES.has(state);
}

export type SessionAuthority = {
	readonly live: boolean;
	readonly deleted: boolean;
	readonly ambiguous?: boolean;
	readonly locatorMatches?: boolean;
};

export type OperationJudgement = {
	readonly state: SupervisorOpState;
	/** True when a single non-atomic snapshot is not enough to conclude. */
	readonly recheck: boolean;
	readonly hold: boolean;
	readonly detail: string;
};

/**
 * Cross-checks session authority against operation status.
 *
 * A single non-atomic read never declares death: an inconsistent pair (session
 * gone, operation still accepted/in_flight) is reported as a stale snapshot to
 * re-read via `inspect -> status -> inspect`, and an unavailable session with an
 * `unknown` operation never authorises automatic recreation.
 */
export function judgeOperation(session: SessionAuthority, status: PromptStatusBody): OperationJudgement {
	const projected = projectOpState(status);

	if (session.ambiguous === true || session.locatorMatches === false) {
		return {
			state: "terminal_uncertain",
			recheck: true,
			hold: true,
			detail: "session authority is ambiguous or the locator does not match; failing closed",
		};
	}

	const sessionUsable = session.live && !session.deleted;

	if (!sessionUsable && (status.status === "accepted" || status.status === "in_flight")) {
		return {
			state: projected,
			recheck: true,
			hold: false,
			detail:
				"inconsistent snapshot: session is not live while the operation still reads active; re-read inspect -> status -> inspect before any recovery",
		};
	}

	if (status.status === "unknown") {
		return {
			state: "terminal_uncertain",
			recheck: true,
			hold: true,
			detail:
				"no reconciliation record: neither running nor terminal. Do not resend the op-ref and do not recreate the session automatically",
		};
	}

	return {
		state: projected,
		recheck: false,
		hold: requiresOperatorHold(projected),
		detail: sessionUsable
			? `session live, operation ${status.status}`
			: `session stopped, operation ${status.status} retained`,
	};
}

/** Fetches and projects the status of one operation. */
export async function fetchOpState(
	options: ControllerOptions,
	sessionId: string,
	opRef: string,
	timeoutMs?: number,
): Promise<StatusReport> {
	const raw = await options.run([
		"sdk",
		"session",
		...(options.agentDir ? ["--agent-dir", options.agentDir] : []),
		"status",
		sessionId,
		opRef,
		"--repo",
		options.repo,
		...(timeoutMs === undefined ? [] : ["--timeout-ms", String(timeoutMs)]),
	]);
	return parseStatusReport(raw);
}
