/**
 * Operator runtime-cycle contract.
 *
 * This is a projection, not a second writer. Durable truth remains the gateway
 * SQLite journal, lock FSM, admission claims, and consumer checkpoints. The
 * owner console and tests use this module to make that truth legible: UI mode,
 * identity verdict, journal/outbox settlement, and actionable gates.
 *
 * Interaction patterns extracted from grok-bot (architectural reference only):
 * - project a UI phase from transport/readiness rather than inventing local state;
 * - keep send/admission ownership separate from post-accept acknowledgement;
 * - treat restart as dropping uncommitted claims while preserving durable receipts.
 */

export const RUNTIME_CYCLE_PHASES = [
	"unavailable",
	"bootstrap_required",
	"verifying",
	"failed_closed",
	"identity_stale",
	"quarantined",
	"degraded",
	"shutting_down",
	"gating",
	"turning",
	"settling",
	"delivering",
	"idle",
] as const;

export type RuntimeCyclePhase = (typeof RUNTIME_CYCLE_PHASES)[number];

/** Owner-console surface modes, analogous to a connection-phase badge. */
export const OWNER_CONSOLE_UI_MODES = ["loading", "connected", "fenced", "unreachable", "reconnecting"] as const;
export type OwnerConsoleUiMode = (typeof OWNER_CONSOLE_UI_MODES)[number];

export const IDENTITY_VERDICTS = ["absent", "unverified", "exact", "stale", "unknown"] as const;
export type IdentityVerdict = (typeof IDENTITY_VERDICTS)[number];

export const OPERATOR_GATE_KINDS = [
	"start_daemon",
	"bootstrap_adopt",
	"wait_verify",
	"recover_fail_closed",
	"approve_profile",
	"clear_quarantine",
	"re_adopt_session",
	"answer_gate_in_gjc",
	"repair_journal_gap",
	"wait_reconcile",
	"inspect_delivery_lag",
] as const;
export type OperatorGateKind = (typeof OPERATOR_GATE_KINDS)[number];

export type OperatorGateSeverity = "blocking" | "advisory";

export interface OperatorGate {
	readonly kind: OperatorGateKind;
	readonly severity: OperatorGateSeverity;
	readonly action: string;
}

export interface RuntimeCycleObservation {
	readonly health?: unknown;
	readonly status?: unknown;
	/** Profile pin `[main_session].session_id` when the operator has one. */
	readonly expectedSessionId?: string;
	readonly localDelivery?: "fenced" | "ready" | "unavailable" | "stopping";
	readonly now?: number;
	/** Set when the owner socket could not be inspected at all. */
	readonly inspectError?: string;
	/** Open SDK gates observed from the journal, not guessed from health. */
	readonly openGateCount?: number;
}

export interface ConsumerSettlement {
	readonly consumerId: string;
	readonly cursor: string;
	readonly claimed: boolean;
	/** Negative means the consumer is behind the journal head. Undefined if either cursor is unparsable. */
	readonly lag: number | undefined;
}

export interface JournalSettlement {
	readonly headCursor: string;
	readonly degraded: boolean;
	readonly gapDetected: boolean;
	readonly gapCount: number;
	readonly consumers: readonly ConsumerSettlement[];
	readonly behind: boolean;
	readonly claimed: boolean;
	/** A consumer cursor ahead of the journal head is an invariant violation. */
	readonly invariantViolated: boolean;
}

export interface RuntimeCycleIdentity {
	readonly expected?: string;
	readonly adopted?: string;
	readonly resumed: boolean;
	readonly verdict: IdentityVerdict;
}

export interface RuntimeCycleProjection {
	readonly phase: RuntimeCyclePhase;
	readonly uiMode: OwnerConsoleUiMode;
	readonly inputAllowed: boolean;
	readonly identity: RuntimeCycleIdentity;
	readonly settlement: JournalSettlement;
	readonly reconcileFreshness: "fresh" | "stale" | "unknown";
	readonly gates: readonly OperatorGate[];
	readonly reason?: string;
	readonly daemonStatus: string;
	readonly daemonState: string;
	readonly turnState: string;
}

type RecordValue = Record<string, unknown>;

function isRecord(value: unknown): value is RecordValue {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function recordValue(value: unknown): RecordValue {
	return isRecord(value) ? value : {};
}

function optionalString(value: unknown): string | undefined {
	if (typeof value !== "string") return undefined;
	const trimmed = value.trim();
	return trimmed ? trimmed : undefined;
}

function firstString(records: readonly RecordValue[], key: string): string | undefined {
	for (const record of records) {
		const value = optionalString(record[key]);
		if (value) return value;
	}
	return undefined;
}

export function parseJournalCursor(value: string): { readonly generation: number; readonly seq: number } | undefined {
	const match = /^(\d+):(\d+)$/u.exec(value);
	if (!match) return undefined;
	return { generation: Number(match[1]), seq: Number(match[2]) };
}

export function compareJournalCursors(left: string, right: string): number | undefined {
	const a = parseJournalCursor(left);
	const b = parseJournalCursor(right);
	if (!a || !b) return undefined;
	if (a.generation !== b.generation) return a.generation < b.generation ? -1 : 1;
	if (a.seq !== b.seq) return a.seq < b.seq ? -1 : 1;
	return 0;
}

function consumerLag(headCursor: string, consumerCursor: string): number | undefined {
	const compared = compareJournalCursors(consumerCursor, headCursor);
	if (compared === undefined) return undefined;
	const head = parseJournalCursor(headCursor);
	const cursor = parseJournalCursor(consumerCursor);
	if (!head || !cursor || head.generation !== cursor.generation) return compared === 0 ? 0 : compared < 0 ? -1 : 1;
	return cursor.seq - head.seq;
}

function projectSettlement(status: RecordValue): JournalSettlement {
	const journal = recordValue(status.journal);
	const headCursor = optionalString(journal.head_cursor) ?? "unknown";
	const consumersInput = Array.isArray(status.consumers) ? status.consumers : [];
	const consumers: ConsumerSettlement[] = consumersInput.map((candidate) => {
		const checkpoint = recordValue(candidate);
		const cursor = optionalString(checkpoint.cursor) ?? "unknown";
		return {
			consumerId: optionalString(checkpoint.consumer_id) ?? "unknown",
			cursor,
			claimed: typeof checkpoint.claim_id === "string" && checkpoint.claim_id.length > 0,
			lag: headCursor === "unknown" ? undefined : consumerLag(headCursor, cursor),
		};
	});
	const gapCount = typeof status.transcript_delivery_gap_count === "number" ? status.transcript_delivery_gap_count : 0;
	return {
		headCursor,
		degraded: journal.degraded === true,
		gapDetected: status.transcript_delivery_gap_detected === true || gapCount > 0,
		gapCount,
		consumers,
		behind: consumers.some((consumer) => consumer.lag !== undefined && consumer.lag < 0),
		claimed: consumers.some((consumer) => consumer.claimed),
		invariantViolated: consumers.some((consumer) => consumer.lag !== undefined && consumer.lag > 0),
	};
}

function projectIdentity(health: RecordValue, status: RecordValue, expectedSessionId?: string): RuntimeCycleIdentity {
	const main = recordValue(status.main ?? health.main);
	const adopted = optionalString(main.session_id);
	const resumed = main.resumed === true;
	const expected = optionalString(expectedSessionId);
	let verdict: IdentityVerdict = "unknown";
	if (!expected && !adopted) verdict = "absent";
	else if (expected && adopted && expected !== adopted) verdict = "stale";
	else if (expected && adopted && expected === adopted) verdict = resumed ? "exact" : "unverified";
	else if (!expected && adopted) verdict = resumed ? "exact" : "unverified";
	else if (expected && !adopted) verdict = "absent";
	return { expected, adopted, resumed, verdict };
}

function reconcileFreshness(status: RecordValue, now: number): "fresh" | "stale" | "unknown" {
	const reconcile = recordValue(status.reconcile);
	const lastOkAt =
		typeof reconcile.last_ok_at === "number" && Number.isFinite(reconcile.last_ok_at) ? reconcile.last_ok_at : undefined;
	if (lastOkAt === undefined) return "unknown";
	const cycleMs =
		typeof reconcile.cycle_ms === "number" && Number.isFinite(reconcile.cycle_ms) ? reconcile.cycle_ms : 15_000;
	const staleAfterMs = Math.max(cycleMs * 2, 30_000);
	return now - lastOkAt <= staleAfterMs ? "fresh" : "stale";
}

function collectGates(input: {
	readonly phase: RuntimeCyclePhase;
	readonly identity: RuntimeCycleIdentity;
	readonly settlement: JournalSettlement;
	readonly reconcile: "fresh" | "stale" | "unknown";
	readonly reason?: string;
	readonly lockQuarantined: boolean;
	readonly openGateCount: number;
}): OperatorGate[] {
	const gates: OperatorGate[] = [];
	const push = (kind: OperatorGateKind, severity: OperatorGateSeverity, action: string): void => {
		if (gates.some((gate) => gate.kind === kind)) return;
		gates.push({ kind, severity, action });
	};
	if (input.phase === "unavailable") {
		push("start_daemon", "blocking", "Start or repair gajaeway.service; the owner socket did not answer.");
	}
	if (input.phase === "failed_closed") {
		if (input.reason === "profile_drift") {
			push(
				"approve_profile",
				"blocking",
				"Run `gajaeway profile approve --confirm` after reviewing the digest-bound diff.",
			);
		} else {
			push(
				"recover_fail_closed",
				"blocking",
				"Re-verify the fail-closed cause, then run the recover ceremony; do not systemd-restart as repair.",
			);
		}
	}
	if (input.identity.verdict === "stale") {
		push(
			"re_adopt_session",
			"blocking",
			"Correct `[main_session].session_id` or re-run bootstrap --confirm for the exact live session.",
		);
	}
	if (input.phase === "bootstrap_required" || input.identity.verdict === "absent") {
		push("bootstrap_adopt", "advisory", "Adopt the exact live GJC session with `gajaeway bootstrap --confirm`.");
	}
	if (input.phase === "verifying") {
		push("wait_verify", "advisory", "Wait for strict resume to finish complete-tail verification.");
	}
	if (input.lockQuarantined) {
		push("clear_quarantine", "advisory", "Complete Git verification, then `/lock clear-quarantine` with the receipt.");
	}
	if (input.settlement.gapDetected || input.settlement.invariantViolated) {
		push(
			"repair_journal_gap",
			input.settlement.invariantViolated ? "blocking" : "advisory",
			"Inspect transcript_delivery_gap events; never baseline a consumer cursor past an unproven head.",
		);
	}
	if (input.reconcile === "stale") {
		push("wait_reconcile", "advisory", "Broker registry reconcile is stale; check gjc sdk session list and way.status.");
	}
	if (input.settlement.behind || input.settlement.claimed) {
		push("inspect_delivery_lag", "advisory", "Journal consumers are claimed or behind; wait for consumer.commit.");
	}
	if (input.openGateCount > 0) {
		push("answer_gate_in_gjc", "advisory", "Answer the open SDK gate in the attached gjc TUI, not through the gateway.");
	}
	return gates;
}

function phaseFor(input: {
	readonly inspectError?: string;
	readonly daemonStatus: string;
	readonly daemonState: string;
	readonly reason?: string;
	readonly identity: RuntimeCycleIdentity;
	readonly lockQuarantined: boolean;
	readonly turnState: string;
	readonly settlement: JournalSettlement;
	readonly openGateCount: number;
	readonly localDelivery?: RuntimeCycleObservation["localDelivery"];
}): RuntimeCyclePhase {
	if (input.inspectError || input.daemonState === "unavailable") return "unavailable";
	if (input.reason === "host_disposed" || input.localDelivery === "stopping") return "shutting_down";
	if (input.daemonState === "failed_closed") return "failed_closed";
	if (input.identity.verdict === "stale") return "identity_stale";
	if (input.daemonState === "verifying" || input.daemonStatus === "booting") return "verifying";
	if (input.identity.verdict === "absent" && input.daemonStatus === "healthy" && !input.identity.resumed) {
		return "bootstrap_required";
	}
	if (input.daemonStatus !== "healthy") return "degraded";
	if (input.lockQuarantined) return "quarantined";
	if (input.settlement.invariantViolated || input.settlement.degraded || input.settlement.gapDetected) return "settling";
	if (input.openGateCount > 0) return "gating";
	if (input.turnState === "busy") return "turning";
	if (input.settlement.claimed || input.settlement.behind) return "delivering";
	return "idle";
}

function uiModeFor(
	phase: RuntimeCyclePhase,
	localDelivery: RuntimeCycleObservation["localDelivery"],
): OwnerConsoleUiMode {
	if (phase === "unavailable") return "unreachable";
	if (phase === "verifying") return "loading";
	if (
		phase === "failed_closed" ||
		phase === "identity_stale" ||
		phase === "degraded" ||
		phase === "shutting_down"
	) {
		return "fenced";
	}
	if (localDelivery === "unavailable") return "reconnecting";
	if (localDelivery === "fenced") return "loading";
	return "connected";
}

/**
 * Projects owner-visible cycle state from gateway health/status plus local
 * console observations. It never mutates durable state.
 */
export function projectRuntimeCycle(observation: RuntimeCycleObservation): RuntimeCycleProjection {
	const health = recordValue(observation.health);
	const status = recordValue(observation.status);
	const records = [health, status];
	const daemonStatus = firstString(records, "status") ?? "unknown";
	const daemonState = firstString(records, "state") ?? "unknown";
	const reason = firstString(records, "reason") ?? optionalString(observation.inspectError);
	const identity = projectIdentity(health, status, observation.expectedSessionId);
	const settlement = projectSettlement(status);
	const lock = recordValue(status.lock);
	const lockQuarantined = lock.quarantined === true;
	const turnState = optionalString(status.turn_state) ?? "unknown";
	const openGateCount = observation.openGateCount ?? 0;
	const phase = phaseFor({
		inspectError: observation.inspectError,
		daemonStatus,
		daemonState,
		reason,
		identity,
		lockQuarantined,
		turnState,
		settlement,
		openGateCount,
		localDelivery: observation.localDelivery,
	});
	const uiMode = uiModeFor(phase, observation.localDelivery);
	const now = observation.now ?? Date.now();
	const reconcile = reconcileFreshness(status, now);
	const gates = collectGates({
		phase,
		identity,
		settlement,
		reconcile,
		reason,
		lockQuarantined,
		openGateCount,
	});
	const inputAllowed =
		uiMode === "connected" &&
		daemonStatus === "healthy" &&
		phase !== "failed_closed" &&
		phase !== "identity_stale" &&
		phase !== "unavailable" &&
		phase !== "shutting_down" &&
		!settlement.invariantViolated;
	return {
		phase,
		uiMode,
		inputAllowed,
		identity,
		settlement,
		reconcileFreshness: reconcile,
		gates,
		reason,
		daemonStatus,
		daemonState,
		turnState,
	};
}

export function renderRuntimeCycleLines(projection: RuntimeCycleProjection): readonly string[] {
	const expected = projection.identity.expected ?? "none";
	const adopted = projection.identity.adopted ?? "none";
	const gateText =
		projection.gates.length === 0
			? "none"
			: projection.gates.map((gate) => `${gate.severity}:${gate.kind}`).join(",");
	const lagging = projection.settlement.consumers
		.filter((consumer) => consumer.lag !== undefined && consumer.lag < 0)
		.map((consumer) => `${consumer.consumerId}:${consumer.lag}`)
		.join(",");
	return [
		`cycle: phase=${projection.phase} ui=${projection.uiMode} input=${projection.inputAllowed} identity=${projection.identity.verdict}`,
		`identity: expected=${expected} adopted=${adopted} resumed=${projection.identity.resumed}`,
		`settlement: head=${projection.settlement.headCursor} degraded=${projection.settlement.degraded} gap=${projection.settlement.gapDetected} claimed=${projection.settlement.claimed} behind=${projection.settlement.behind} invariant=${projection.settlement.invariantViolated}${lagging ? ` lag=${lagging}` : ""}`,
		`gates: ${gateText}`,
	];
}

/** In-memory turn/delivery machine used to pin settlement invariants in tests. */
export const TURN_CYCLE_STATES = [
	"idle",
	"ingress",
	"admission_claimed",
	"admission_accepted",
	"ack_sent",
	"turning",
	"journaled",
	"outbox_claimed",
	"delivered",
	"failed",
] as const;
export type TurnCycleState = (typeof TURN_CYCLE_STATES)[number];

export type TurnCycleEvent =
	| { readonly type: "ingress"; readonly idempotencyKey: string }
	| { readonly type: "claim"; readonly idempotencyKey: string; readonly opRef: string }
	| { readonly type: "accept"; readonly opRef: string }
	| { readonly type: "ack" }
	| { readonly type: "turn_start" }
	| { readonly type: "journal_append"; readonly cursor: string }
	| { readonly type: "outbox_claim"; readonly consumerId: string; readonly cursor: string }
	| { readonly type: "commit"; readonly consumerId: string; readonly cursor: string }
	| { readonly type: "restart" }
	| { readonly type: "fail"; readonly reason: string };

export interface TurnCycleSnapshot {
	readonly state: TurnCycleState;
	readonly idempotencyKey?: string;
	readonly opRef?: string;
	readonly journalCursor?: string;
	readonly consumerId?: string;
	readonly consumerCursor?: string;
	readonly duplicate: boolean;
	readonly reason?: string;
}

export type TurnCycleApplyResult =
	| { readonly ok: true; readonly snapshot: TurnCycleSnapshot }
	| { readonly ok: false; readonly reason: string; readonly snapshot: TurnCycleSnapshot };

function snapshot(
	state: TurnCycleState,
	base: Omit<TurnCycleSnapshot, "state" | "duplicate">,
	duplicate = false,
	reason?: string,
): TurnCycleSnapshot {
	return {
		state,
		idempotencyKey: base.idempotencyKey,
		opRef: base.opRef,
		journalCursor: base.journalCursor,
		consumerId: base.consumerId,
		consumerCursor: base.consumerCursor,
		duplicate,
		reason,
	};
}

/**
 * Applies one turn-cycle event. Duplicate admission/commit with the same
 * durable key is a replay, never a second effect. Restart drops uncommitted
 * outbox claims and undelivered ingress, and preserves claimed/accepted receipts.
 */
export function applyTurnCycleEvent(current: TurnCycleSnapshot, event: TurnCycleEvent): TurnCycleApplyResult {
	const fail = (reason: string): TurnCycleApplyResult => ({
		ok: false,
		reason,
		snapshot: snapshot("failed", current, false, reason),
	});
	const ok = (next: TurnCycleSnapshot): TurnCycleApplyResult => ({ ok: true, snapshot: next });

	if (current.state === "failed" && event.type !== "restart") return fail(current.reason ?? "failed");

	switch (event.type) {
		case "ingress": {
			if (current.state === "idle") {
				return ok(snapshot("ingress", { idempotencyKey: event.idempotencyKey }));
			}
			if (current.idempotencyKey === event.idempotencyKey) {
				return ok(snapshot(current.state, current, true));
			}
			return fail("ingress_while_active");
		}
		case "claim": {
			if (current.state === "ingress" && current.idempotencyKey === event.idempotencyKey) {
				return ok(snapshot("admission_claimed", { idempotencyKey: event.idempotencyKey, opRef: event.opRef }));
			}
			if (
				current.idempotencyKey === event.idempotencyKey &&
				(current.state === "admission_claimed" ||
					current.state === "admission_accepted" ||
					current.state === "ack_sent" ||
					current.state === "turning" ||
					current.state === "journaled" ||
					current.state === "outbox_claimed" ||
					current.state === "delivered")
			) {
				return ok(snapshot(current.state, current, true));
			}
			return fail("claim_without_ingress");
		}
		case "accept": {
			if (current.state === "admission_claimed" && current.opRef === event.opRef) {
				return ok(snapshot("admission_accepted", current));
			}
			if (current.opRef === event.opRef && current.state !== "idle" && current.state !== "ingress") {
				return ok(snapshot(current.state, current, true));
			}
			return fail("accept_without_claim");
		}
		case "ack": {
			if (current.state === "admission_accepted") return ok(snapshot("ack_sent", current));
			if (current.state === "ack_sent" || current.state === "turning" || current.state === "journaled") {
				return ok(snapshot(current.state, current, true));
			}
			return fail("ack_before_accept");
		}
		case "turn_start": {
			if (current.state === "admission_accepted" || current.state === "ack_sent") {
				return ok(snapshot("turning", current));
			}
			if (current.state === "turning") return ok(snapshot("turning", current, true));
			return fail("turn_start_before_accept");
		}
		case "journal_append": {
			if (current.state !== "turning" && current.state !== "admission_accepted" && current.state !== "ack_sent") {
				return fail("journal_before_turn");
			}
			return ok(
				snapshot("journaled", {
					idempotencyKey: current.idempotencyKey,
					opRef: current.opRef,
					journalCursor: event.cursor,
				}),
			);
		}
		case "outbox_claim": {
			if (current.state === "journaled" && current.journalCursor) {
				const compared = compareJournalCursors(event.cursor, current.journalCursor);
				if (compared === undefined) return fail("cursor_unparsable");
				if (compared > 0) return fail("consumer_ahead_of_journal");
				return ok(
					snapshot("outbox_claimed", {
						...current,
						consumerId: event.consumerId,
						consumerCursor: event.cursor,
					}),
				);
			}
			if (
				current.state === "outbox_claimed" &&
				current.consumerId === event.consumerId &&
				current.consumerCursor === event.cursor
			) {
				return ok(snapshot("outbox_claimed", current, true));
			}
			if (current.state === "delivered" && current.consumerId === event.consumerId) {
				return ok(snapshot("delivered", current, true));
			}
			return fail("outbox_claim_before_journal");
		}
		case "commit": {
			if (
				current.state === "outbox_claimed" &&
				current.consumerId === event.consumerId &&
				current.journalCursor &&
				current.consumerCursor === event.cursor
			) {
				const compared = compareJournalCursors(event.cursor, current.journalCursor);
				if (compared === undefined) return fail("cursor_unparsable");
				if (compared > 0) return fail("consumer_ahead_of_journal");
				return ok(snapshot("delivered", { ...current, consumerCursor: event.cursor }));
			}
			if (
				current.state === "delivered" &&
				current.consumerId === event.consumerId &&
				current.consumerCursor === event.cursor
			) {
				return ok(snapshot("delivered", current, true));
			}
			return fail("commit_without_claim");
		}
		case "restart": {
			if (current.state === "idle" || current.state === "ingress") {
				return ok(snapshot("idle", {}));
			}
			if (current.state === "outbox_claimed") {
				return ok(
					snapshot("journaled", {
						idempotencyKey: current.idempotencyKey,
						opRef: current.opRef,
						journalCursor: current.journalCursor,
					}),
				);
			}
			if (current.state === "ack_sent" || current.state === "turning") {
				return ok(snapshot("admission_accepted", { idempotencyKey: current.idempotencyKey, opRef: current.opRef }));
			}
			if (current.state === "failed") return ok(snapshot("failed", current, false, current.reason));
			return ok(snapshot(current.state, { ...current, consumerId: undefined, consumerCursor: undefined }));
		}
		case "fail":
			return fail(event.reason);
	}
}

export function idleTurnCycle(): TurnCycleSnapshot {
	return snapshot("idle", {});
}
