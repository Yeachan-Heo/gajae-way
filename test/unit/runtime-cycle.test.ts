import { expect, test } from "bun:test";
import { consoleStartupDecision, renderConsoleStatusSummary } from "../../src/console/console";
import {
	applyTurnCycleEvent,
	idleTurnCycle,
	projectRuntimeCycle,
	type TurnCycleSnapshot,
} from "../../src/runtime-cycle";

const healthyHealth = {
	status: "healthy",
	state: "running",
	main: { resumed: true, session_id: "main-session" },
};

const healthyStatus = {
	...healthyHealth,
	turn_state: "idle",
	follow_up_queue_depth: 0,
	transcript_verification: "verified",
	transcript_delivery_gap_detected: false,
	transcript_delivery_gap_count: 0,
	journal: { head_cursor: "7:42", degraded: false },
	lock: {
		held: true,
		holder: { session_id: "main-session" },
		queue_len: 0,
		stuck: false,
		quarantined: false,
	},
	write_mode: true,
	reconcile: { last_ok_at: 10_000, cycle_ms: 5_000, drift_count: 0 },
	consumers: [{ consumer_id: "gajaeway-console", cursor: "7:42" }],
};

function applyAll(events: Parameters<typeof applyTurnCycleEvent>[1][]): TurnCycleSnapshot {
	let snapshot = idleTurnCycle();
	for (const event of events) {
		const result = applyTurnCycleEvent(snapshot, event);
		expect(result.ok).toBe(true);
		snapshot = result.snapshot;
	}
	return snapshot;
}

test("happy path projects an interactive idle cycle with exact identity", () => {
	const projection = projectRuntimeCycle({
		health: healthyHealth,
		status: healthyStatus,
		expectedSessionId: "main-session",
		now: 15_000,
	});
	expect(projection).toMatchObject({
		phase: "idle",
		uiMode: "connected",
		inputAllowed: true,
		identity: { verdict: "exact", adopted: "main-session", expected: "main-session", resumed: true },
		settlement: { behind: false, claimed: false, invariantViolated: false, headCursor: "7:42" },
		reconcileFreshness: "fresh",
	});
	expect(consoleStartupDecision(healthyHealth, healthyStatus, { expectedSessionId: "main-session" })).toEqual({
		interactive: true,
	});
	const summary = renderConsoleStatusSummary(healthyHealth, healthyStatus, 15_000, {
		expectedSessionId: "main-session",
	});
	expect(summary).toContain("cycle: phase=idle ui=connected input=true identity=exact");
	expect(summary).toContain("identity: expected=main-session adopted=main-session resumed=true");
	expect(summary).toContain("settlement: head=7:42");
	expect(summary).toContain("gates: none");
});

test("unavailable daemon fences owner input with a start_daemon gate", () => {
	const health = { status: "unhealthy", state: "unavailable", reason: "daemon_unreachable" };
	const projection = projectRuntimeCycle({ health, status: health, inspectError: "daemon_unreachable" });
	expect(projection.phase).toBe("unavailable");
	expect(projection.uiMode).toBe("unreachable");
	expect(projection.inputAllowed).toBe(false);
	expect(projection.gates.map((gate) => gate.kind)).toContain("start_daemon");
	const decision = consoleStartupDecision(health, health);
	expect(decision.interactive).toBe(false);
	expect(decision.refusal).toContain("unavailable");
	expect(decision.refusal).toContain("daemon_unreachable");
});

test("stale adopted identity is fail-closed for owner input", () => {
	const projection = projectRuntimeCycle({
		health: healthyHealth,
		status: healthyStatus,
		expectedSessionId: "other-session",
		now: 15_000,
	});
	expect(projection.phase).toBe("identity_stale");
	expect(projection.uiMode).toBe("fenced");
	expect(projection.inputAllowed).toBe(false);
	expect(projection.gates.map((gate) => gate.kind)).toContain("re_adopt_session");
	const decision = consoleStartupDecision(healthyHealth, healthyStatus, { expectedSessionId: "other-session" });
	expect(decision.interactive).toBe(false);
	expect(decision.refusal).toContain("does not match the profile pin");
	expect(decision.refusal).toContain("other-session");
});

test("duplicate admission and commit after restart do not create a second effect", () => {
	const claimed = applyAll([
		{ type: "ingress", idempotencyKey: "turn-1" },
		{ type: "claim", idempotencyKey: "turn-1", opRef: "op-1" },
	]);
	expect(claimed.state).toBe("admission_claimed");

	const restartedClaim = applyTurnCycleEvent(claimed, { type: "restart" });
	expect(restartedClaim.ok).toBe(true);
	expect(restartedClaim.snapshot.state).toBe("admission_claimed");

	const replayClaim = applyTurnCycleEvent(restartedClaim.snapshot, {
		type: "claim",
		idempotencyKey: "turn-1",
		opRef: "op-1",
	});
	expect(replayClaim.ok).toBe(true);
	expect(replayClaim.snapshot.duplicate).toBe(true);

	const accepted = applyAll([
		{ type: "ingress", idempotencyKey: "turn-1" },
		{ type: "claim", idempotencyKey: "turn-1", opRef: "op-1" },
		{ type: "accept", opRef: "op-1" },
		{ type: "ack" },
		{ type: "turn_start" },
		{ type: "journal_append", cursor: "1:4" },
		{ type: "outbox_claim", consumerId: "gajaeway-discord", cursor: "1:4" },
	]);
	expect(accepted.state).toBe("outbox_claimed");

	const afterCrash = applyTurnCycleEvent(accepted, { type: "restart" });
	expect(afterCrash.ok).toBe(true);
	expect(afterCrash.snapshot.state).toBe("journaled");
	expect(afterCrash.snapshot.consumerCursor).toBeUndefined();

	const reclaimed = applyTurnCycleEvent(afterCrash.snapshot, {
		type: "outbox_claim",
		consumerId: "gajaeway-discord",
		cursor: "1:4",
	});
	expect(reclaimed.ok).toBe(true);
	const committed = applyTurnCycleEvent(reclaimed.snapshot, {
		type: "commit",
		consumerId: "gajaeway-discord",
		cursor: "1:4",
	});
	expect(committed.ok).toBe(true);
	expect(committed.snapshot.state).toBe("delivered");
	const duplicateCommit = applyTurnCycleEvent(committed.snapshot, {
		type: "commit",
		consumerId: "gajaeway-discord",
		cursor: "1:4",
	});
	expect(duplicateCommit.ok).toBe(true);
	expect(duplicateCommit.snapshot.duplicate).toBe(true);
	expect(duplicateCommit.snapshot.state).toBe("delivered");
});

test("ack before durable admission acceptance is refused", () => {
	const claimed = applyAll([
		{ type: "ingress", idempotencyKey: "turn-1" },
		{ type: "claim", idempotencyKey: "turn-1", opRef: "op-1" },
	]);
	const ack = applyTurnCycleEvent(claimed, { type: "ack" });
	expect(ack.ok).toBe(false);
	if (ack.ok) throw new Error("expected ack_before_accept");
	expect(ack.reason).toBe("ack_before_accept");
});

test("delivery commit ahead of the journal head is an invariant violation", () => {
	const journaled = applyAll([
		{ type: "ingress", idempotencyKey: "turn-1" },
		{ type: "claim", idempotencyKey: "turn-1", opRef: "op-1" },
		{ type: "accept", opRef: "op-1" },
		{ type: "turn_start" },
		{ type: "journal_append", cursor: "1:4" },
	]);
	const ahead = applyTurnCycleEvent(journaled, {
		type: "outbox_claim",
		consumerId: "gajaeway-discord",
		cursor: "1:9",
	});
	expect(ahead.ok).toBe(false);
	if (ahead.ok) throw new Error("expected consumer_ahead_of_journal");
	expect(ahead.reason).toBe("consumer_ahead_of_journal");

	const projection = projectRuntimeCycle({
		health: healthyHealth,
		status: {
			...healthyStatus,
			consumers: [{ consumer_id: "gajaeway-discord", cursor: "7:99", claim_id: "claim-1" }],
		},
		expectedSessionId: "main-session",
	});
	expect(projection.settlement.invariantViolated).toBe(true);
	expect(projection.inputAllowed).toBe(false);
	expect(projection.gates.some((gate) => gate.kind === "repair_journal_gap" && gate.severity === "blocking")).toBe(
		true,
	);
});

test("consumer lag and claimed outbox project the delivering phase without fencing input", () => {
	const projection = projectRuntimeCycle({
		health: healthyHealth,
		status: {
			...healthyStatus,
			consumers: [{ consumer_id: "gajaeway-discord", cursor: "7:40", claim_id: "claim-live" }],
		},
		expectedSessionId: "main-session",
		now: 15_000,
	});
	expect(projection.phase).toBe("delivering");
	expect(projection.inputAllowed).toBe(true);
	expect(projection.settlement.behind).toBe(true);
	expect(projection.settlement.claimed).toBe(true);
	expect(projection.gates.map((gate) => gate.kind)).toContain("inspect_delivery_lag");
});
