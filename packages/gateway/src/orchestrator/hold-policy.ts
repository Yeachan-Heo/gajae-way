/**
 * I5a: the total held-turn state machine. One pure evaluator maps the evidence a
 * sweep gathered (two inspects, raw liveness, queue emptiness, the `turn.result`
 * witness) plus the durable attempt row and the deadline onto exactly one
 * transition. Every path (boot, tick, retired, drain, operator) runs the same
 * evaluator; only the allowed transitions differ per path, and every path is a
 * no-op once a terminal disposition is set.
 *
 * Retry authority is the positive non-admission proof and nothing else:
 *   NA1 `send_state = pre_write_failure`   (zero bytes written / no spawn marker)
 *   NA2 `admission = refused`              (structured refuse correlated to the request)
 *   NA3 the session was never created      (bind failed before inboundBindTurn)
 * `unknown` from `turn.result` - on any incarnation, any number of times - is
 * absence, never proof. `client_ref_conflict` is evidence of admission.
 */

export type HoldPath = "boot" | "tick" | "retired" | "drain" | "operator";

export type HoldReason =
	| "unacknowledged_send"
	| "operation_state_unknown"
	| "authority_disagreement"
	| "authority_unreachable"
	| "status_unavailable"
	| "no_terminal_text";

export type EvidenceCell = "U1" | "U2" | "U3" | "U4" | "U5" | "U6" | "T1" | "T2" | "T3" | "T4";

export interface HoldEvidence {
	/** `turn.result` status for this op; `unreachable` when status itself failed. */
	readonly witness:
		| "unknown"
		| "in_flight"
		| "accepted"
		| "terminal_ok"
		| "failed"
		| "terminal_missing_receipt"
		| "unreachable";
	/** The witness carried usable terminal text (I4b selected it). Only meaningful for terminal_ok. */
	readonly terminalTextUsable?: boolean;
	/** Inspect I1 / I2 outcome. */
	readonly inspect: {
		readonly firstFailed: boolean;
		readonly secondFailed: boolean;
		readonly agree: boolean;
		readonly live: boolean | undefined;
		readonly deletedOrDisowned: boolean;
	};
	readonly queueEmpty: boolean | undefined;
}

export interface HoldSubject {
	readonly turnState: "bound" | "accepted";
	readonly sendState: "pending_write" | "pre_write_failure" | "written_unconfirmed" | "accepted" | undefined;
	readonly admission: "unknown" | "refused" | "accepted" | undefined;
	/** Set when the bind itself failed before any row was bound (NA3). */
	readonly sessionNeverCreated?: boolean;
	readonly terminalDisposition: string | null | undefined;
	readonly holdDeadlineAt: number | undefined;
}

export type HoldTransition =
	| { readonly kind: "noop"; readonly reason: "late_terminal_suppressed" | "read_only" }
	| { readonly kind: "terminalize"; readonly cell: "T3" | "T4" }
	| { readonly kind: "fail"; readonly cell: "T1"; readonly code: "terminal_missing_receipt" }
	| { readonly kind: "requeue"; readonly cell: "U1"; readonly proof: "NA1" | "NA2" | "NA3" }
	| { readonly kind: "hold"; readonly cell: EvidenceCell; readonly reason: HoldReason }
	| { readonly kind: "operation_lost"; readonly cell: EvidenceCell; readonly fence: boolean };

/** NA1-NA3: the only retry authority for a bound row. */
export function nonAdmissionProof(subject: HoldSubject): "NA1" | "NA2" | "NA3" | undefined {
	if (subject.sessionNeverCreated) return "NA3";
	if (subject.sendState === "pre_write_failure") return "NA1";
	if (subject.admission === "refused") return "NA2";
	return undefined;
}

export function classifyEvidence(evidence: HoldEvidence): EvidenceCell {
	const { witness, inspect } = evidence;
	if (witness === "terminal_missing_receipt") return "T1";
	if (witness === "failed") return "T4";
	if (witness === "terminal_ok") return evidence.terminalTextUsable ? "T3" : "T2";
	if (inspect.firstFailed || inspect.secondFailed) return "U5";
	if (!inspect.agree) return "U4";
	// Positive death from agreeing inspects is stronger evidence than an
	// unreachable status: a disowning router on a dead session is U3, not U6.
	if (inspect.deletedOrDisowned || inspect.live === false) return "U3";
	if (witness === "unreachable") return "U6";
	if (inspect.live === true && evidence.queueEmpty === true) return "U1";
	if (inspect.live === true) return "U2";
	// Catch-all: reachable but undecidable evidence is treated as U6.
	return "U6";
}

const HOLD_REASON_BY_CELL: Record<Exclude<EvidenceCell, "T1" | "T3" | "T4">, HoldReason> = {
	U1: "operation_state_unknown",
	U2: "operation_state_unknown",
	U3: "operation_state_unknown",
	U4: "authority_disagreement",
	U5: "authority_unreachable",
	U6: "status_unavailable",
	T2: "no_terminal_text",
};

/**
 * The single transition for one evaluation. `nowMs` is compared with the durable
 * deadline; the deadline is set once at the first hold and never reset.
 */
export function evaluateHold(input: {
	readonly path: HoldPath;
	readonly subject: HoldSubject;
	readonly evidence: HoldEvidence;
	readonly nowMs: number;
}): HoldTransition {
	const { path, subject, evidence, nowMs } = input;
	if (subject.terminalDisposition) return { kind: "noop", reason: "late_terminal_suppressed" };
	const cell = classifyEvidence(evidence);
	if (path === "drain") return { kind: "noop", reason: "read_only" };
	if (cell === "T3" || cell === "T4") return { kind: "terminalize", cell };
	if (cell === "T1") return { kind: "fail", cell, code: "terminal_missing_receipt" };
	const expired = subject.holdDeadlineAt !== undefined && nowMs >= subject.holdDeadlineAt;
	if (expired) return { kind: "operation_lost", cell, fence: cell !== "T2" };
	if (cell === "U1" && subject.turnState === "bound") {
		const proof = nonAdmissionProof(subject);
		if (proof) return { kind: "requeue", cell, proof };
		return { kind: "hold", cell, reason: "unacknowledged_send" };
	}
	return { kind: "hold", cell, reason: HOLD_REASON_BY_CELL[cell] };
}

/** Sweep spacing 60, 120, 240, 480 s then capped at 600 s; `sweeps` = evaluations already done. */
export function nextSweepDelayMs(sweeps: number): number {
	const steps = [60_000, 120_000, 240_000, 480_000];
	return sweeps < steps.length ? steps[Math.max(sweeps, 0)]! : 600_000;
}

/** Body of the Q1 notice; the delivery layer prefixes `[turn failed] `. */
export function operationLostNotice(minutes: number, interimDelivered: boolean): string {
	return interimDelivered
		? `operation lost: no result was confirmed for this request within ${minutes} min; the partial output above may be incomplete. It was not resent.`
		: `operation lost: no result was confirmed for this request within ${minutes} min. It was not resent.`;
}
