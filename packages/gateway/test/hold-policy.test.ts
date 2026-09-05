/**
 * I5a matrix: every evidence cell x every path through the one evaluator, with
 * the never-resend oracle per original trigger. Plus the drills the plan names.
 */
import { expect, test } from "bun:test";
import {
	classifyEvidence,
	evaluateHold,
	type EvidenceCell,
	type HoldEvidence,
	type HoldPath,
	type HoldSubject,
	nextSweepDelayMs,
	nonAdmissionProof,
} from "../src/orchestrator/hold-policy";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { createFakeGjc } from "./fixtures/fake-gjc.mjs";
import { eventually, harness, KEY, ORIGIN } from "./red-first-harness";
import { ScriptedSessionPort } from "./session-port.fake";

const inspect = (over: Partial<HoldEvidence["inspect"]> = {}): HoldEvidence["inspect"] => ({
	firstFailed: false,
	secondFailed: false,
	agree: true,
	live: true,
	deletedOrDisowned: false,
	...over,
});

const CELLS: Record<EvidenceCell, HoldEvidence> = {
	U1: { witness: "unknown", inspect: inspect(), queueEmpty: true },
	U2: { witness: "unknown", inspect: inspect(), queueEmpty: false },
	U3: { witness: "unknown", inspect: inspect({ live: false, deletedOrDisowned: true }), queueEmpty: undefined },
	U4: { witness: "unknown", inspect: inspect({ agree: false }), queueEmpty: true },
	U5: { witness: "unknown", inspect: inspect({ firstFailed: true }), queueEmpty: undefined },
	U6: { witness: "unreachable", inspect: inspect(), queueEmpty: true },
	T1: { witness: "terminal_missing_receipt", inspect: inspect(), queueEmpty: true },
	T2: { witness: "terminal_ok", terminalTextUsable: false, inspect: inspect(), queueEmpty: true },
	T3: { witness: "terminal_ok", terminalTextUsable: true, inspect: inspect(), queueEmpty: true },
	T4: { witness: "failed", inspect: inspect(), queueEmpty: true },
};
const PATHS: HoldPath[] = ["boot", "tick", "retired", "drain", "operator"];

const subject = (over: Partial<HoldSubject> = {}): HoldSubject => ({
	turnState: "accepted",
	sendState: "written_unconfirmed",
	admission: "unknown",
	terminalDisposition: null,
	holdDeadlineAt: 10_000,
	...over,
});

test("classifyEvidence maps every input to its documented cell", () => {
	for (const [cell, evidence] of Object.entries(CELLS)) expect(classifyEvidence(evidence)).toBe(cell as EvidenceCell);
	// catch-all: reachable, agreeing, liveness unknown, queue unknown -> U6
	expect(classifyEvidence({ witness: "unknown", inspect: inspect({ live: undefined }), queueEmpty: undefined })).toBe(
		"U6",
	);
});

test("non-admission proof is exactly NA1 / NA2 / NA3; unknown is never proof", () => {
	expect(nonAdmissionProof(subject({ sendState: "pre_write_failure" }))).toBe("NA1");
	expect(nonAdmissionProof(subject({ admission: "refused" }))).toBe("NA2");
	expect(nonAdmissionProof(subject({ sessionNeverCreated: true }))).toBe("NA3");
	for (const sendState of ["pending_write", "written_unconfirmed", "accepted"] as const)
		expect(nonAdmissionProof(subject({ sendState, admission: "unknown" }))).toBeUndefined();
	expect(nonAdmissionProof(subject({ admission: "accepted" }))).toBeUndefined();
});

test("matrix: every cell x every path yields one transition and never a resend of an accepted row", () => {
	for (const path of PATHS) {
		for (const [cell, evidence] of Object.entries(CELLS)) {
			for (const turnState of ["bound", "accepted"] as const) {
				for (const nowMs of [0, 10_000]) {
					const transition = evaluateHold({ path, evidence, nowMs, subject: subject({ turnState }) });
					// Oracle: an accepted (possibly admitted) row is never requeued by any path.
					if (turnState === "accepted") expect(transition.kind).not.toBe("requeue");
					// Drain is read-only for everything.
					if (path === "drain") expect(transition).toEqual({ kind: "noop", reason: "read_only" });
					// Decidable terminals never hold or get lost.
					if (path !== "drain" && (cell === "T3" || cell === "T4")) expect(transition.kind).toBe("terminalize");
					if (path !== "drain" && cell === "T1") expect(transition.kind).toBe("fail");
					// Undecidable at the deadline -> lost; fence for U1-U6, not for T2.
					if (path !== "drain" && cell.startsWith("U") && nowMs >= 10_000)
						expect(transition).toEqual({ kind: "operation_lost", cell: cell as EvidenceCell, fence: true });
					if (path !== "drain" && cell === "T2" && nowMs >= 10_000)
						expect(transition).toEqual({ kind: "operation_lost", cell: cell as EvidenceCell, fence: false });
					// Undecidable before the deadline -> hold with the documented reason.
					if (path !== "drain" && cell.startsWith("U") && nowMs < 10_000 && !(cell === "U1" && turnState === "bound"))
						expect(transition.kind).toBe("hold");
				}
			}
		}
	}
});

test("U1 bound: requeue only with proof, otherwise hold unacknowledged_send", () => {
	const base = { path: "tick" as const, evidence: CELLS.U1, nowMs: 0 };
	expect(evaluateHold({ ...base, subject: subject({ turnState: "bound" }) })).toEqual({
		kind: "hold",
		cell: "U1",
		reason: "unacknowledged_send",
	});
	expect(evaluateHold({ ...base, subject: subject({ turnState: "bound", sendState: "pre_write_failure" }) })).toEqual({
		kind: "requeue",
		cell: "U1",
		proof: "NA1",
	});
	// U2 (busy) never requeues even with proof: waits until idle.
	expect(
		evaluateHold({
			...base,
			evidence: CELLS.U2,
			subject: subject({ turnState: "bound", sendState: "pre_write_failure" }),
		}).kind,
	).toBe("hold");
});

test("a set disposition makes every path a no-op (late_terminal_suppressed)", () => {
	for (const path of PATHS)
		for (const evidence of Object.values(CELLS))
			expect(
				evaluateHold({ path, evidence, nowMs: 99_999, subject: subject({ terminalDisposition: "operation_lost" }) }),
			).toEqual({
				kind: "noop",
				reason: "late_terminal_suppressed",
			});
});

test("sweep spacing is 60, 120, 240, 480 then 600 s", () => {
	expect([0, 1, 2, 3, 4, 9].map(nextSweepDelayMs)).toEqual([60_000, 120_000, 240_000, 480_000, 600_000, 600_000]);
});

test("torn ack: host accepts, gateway dies before inboundTurnAccept, repeated unknown never requeues; operator requeue refused", async () => {
	let now = Date.now();
	const port = new ScriptedSessionPort();
	const h = await harness(port, { now: () => now, holdTtlMs: 30 * 60_000 });
	const fake = createFakeGjc({ modes: "status:unknown-forever" });
	const run = async (args: readonly string[]) => (await fake(args))!;
	const real = new BrokerSessionPort({
		database: h.database,
		cli: run,
		instanceId: "torn",
		tailRunner: new TailRunner({ run, repo: h.repo }),
	});
	port.status = real.status.bind(real);
	try {
		const binding = await port.bind({ originKey: KEY, epoch: 0, repo: h.repo });
		h.database.putSession(KEY, binding.sessionId);
		h.database.inboundEnqueue({
			messageId: "torn",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "torn",
		});
		h.database.inboundBindTurn({
			messageId: "torn",
			originKey: KEY,
			epoch: 0,
			opRef: "gw-torn",
			sessionId: binding.sessionId,
		});
		// The write happened (spawn marker set, bytes flushed) but the ack never came back.
		h.database.turnAttemptSetSpawnGate("gw-torn");
		h.database.reclassifyPendingWrites();
		expect(h.database.turnAttemptState("gw-torn")).toEqual({ sendState: "written_unconfirmed", admission: "unknown" });
		for (let sweep = 0; sweep < 5; sweep++) {
			await h.manager.recover();
			await h.manager.tick(KEY);
			now += 60_000;
		}
		expect(port.sends).toHaveLength(0);
		expect(h.database.inboundTurnRow("gw-torn")?.turn_state).toBe("bound");
		expect(h.logs.filter((l) => l.includes("recovery_requeue"))).toEqual([]);
		const refused = await h.manager.resolveHold({ opRef: "gw-torn", outcome: "requeue", actor: "test" });
		expect(refused).toEqual({
			ok: false,
			reason:
				"refusing requeue: gw-torn has no positive non-admission proof (send_state=written_unconfirmed, admission=unknown); use delivered or abandon",
		});
		expect(h.database.listEpochMutations({ sinceMs: 0 })).toEqual([]);
	} finally {
		await h.close();
	}
});

test("operator verbs: delivered CAS + audit, abandon with notice, refusal once a disposition is set; requeue with NA1 proof releases the row", async () => {
	const port = new ScriptedSessionPort();
	const h = await harness(port);
	try {
		const binding = await port.bind({ originKey: KEY, epoch: 0, repo: h.repo });
		h.database.putSession(KEY, binding.sessionId);
		for (const id of ["d", "a"]) {
			h.database.inboundEnqueue({ messageId: id, originKey: KEY, originRefJson: JSON.stringify(ORIGIN), body: id });
		}
		h.database.inboundBindTurn({
			messageId: "d",
			originKey: KEY,
			epoch: 0,
			opRef: "gw-d",
			sessionId: binding.sessionId,
		});
		h.database.inboundTurnAccept("gw-d");
		expect(
			await h.manager.resolveHold({ opRef: "gw-d", outcome: "delivered", platformMessageId: "m-1", actor: "op" }),
		).toEqual({
			ok: true,
			disposition: "delivered",
		});
		expect(h.database.holdState("gw-d")?.terminalDisposition).toBe("delivered");
		expect(h.database.holdResolutions("gw-d").map((r) => [r.outcome, r.disposition, r.actor])).toEqual([
			["delivered", "delivered", "op"],
		]);
		expect(await h.manager.resolveHold({ opRef: "gw-d", outcome: "abandon", actor: "op" })).toEqual({
			ok: false,
			reason: "refusing abandon: gw-d already has disposition delivered",
		});
		// Resolving 'd' let the actor dispatch the next pending row ('a') itself.
		await eventually(() => port.sends.length === 1, "next trigger was not dispatched after delivered");
		const opA = port.sends[0]!.opRef;
		expect(h.database.inboundTurnRow(opA)?.message_id).toBe("a");
		expect(await h.manager.resolveHold({ opRef: opA, outcome: "abandon", notify: true, actor: "op" })).toEqual({
			ok: true,
			disposition: "abandoned",
		});
		expect(h.database.inboundTurnRow(opA)?.turn_state).toBe("done");
		expect(h.deliveries.at(-1)?.text).toBe("[turn failed] operation abandoned by the operator. It was not resent.");
		expect(h.manager.state(KEY)).toBe("idle");
		// 'r' is dispatched next and accepted by the scripted port: no proof -> requeue refused.
		h.enqueue("r");
		await h.manager.notifyInbound(KEY);
		await eventually(() => port.sends.length === 2, "third trigger was not dispatched after abandon");
		const opR = port.sends[1]!.opRef;
		expect(h.database.inboundTurnRow(opR)?.message_id).toBe("r");
		expect(await h.manager.resolveHold({ opRef: opR, outcome: "requeue", actor: "op" })).toMatchObject({ ok: false });
		port.complete(opR, "answer r");
		await eventually(() => h.database.inboundPendingCount(KEY) === 0, "r did not complete");
		// A bound row with a proven pre-write failure (NA1) may be requeued.
		h.database.inboundEnqueue({ messageId: "q", originKey: KEY, originRefJson: JSON.stringify(ORIGIN), body: "q" });
		h.database.inboundBindTurn({
			messageId: "q",
			originKey: KEY,
			epoch: 0,
			opRef: "gw-q",
			sessionId: binding.sessionId,
		});
		h.database.turnAttemptMarkPreWriteFailure("gw-q");
		expect(await h.manager.resolveHold({ opRef: "gw-q", outcome: "requeue", actor: "op" })).toEqual({
			ok: true,
			disposition: "requeued (NA1)",
		});
		expect(h.database.inboundTurnRow("gw-q")).toBeUndefined();
	} finally {
		await h.close();
	}
});

test("fence: survives restart, blocks dispatch, clears on F1 (terminal) and rotates on F4 (deadline)", async () => {
	let now = Date.now();
	const port = new ScriptedSessionPort();
	const h = await harness(port, { now: () => now, holdTtlMs: 5 * 60_000 });
	try {
		const binding = await port.bind({ originKey: KEY, epoch: 0, repo: h.repo });
		h.database.putSession(KEY, binding.sessionId);
		h.database.inboundEnqueue({
			messageId: "lost",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "lost",
		});
		h.database.inboundBindTurn({
			messageId: "lost",
			originKey: KEY,
			epoch: 0,
			opRef: "gw-lost",
			sessionId: binding.sessionId,
		});
		h.database.inboundTurnAccept("gw-lost");
		expect(
			h.database.inboundTurnLose({
				opRef: "gw-lost",
				originKey: KEY,
				epoch: 0,
				disposition: "operation_lost",
				failureCode: "operation_lost",
				nowMs: now,
				fence: true,
				fenceTtlMs: 5 * 60_000,
			}),
		).toEqual({ lost: true, interimDelivered: false });
		// Durable: a fresh DB handle sees it.
		const reopened = await GatewayDatabase.open(`${h.home}/gateway.db`);
		expect(reopened.sessionFence(KEY)?.opRef).toBe("gw-lost");
		reopened.close();
		// While fenced, a new pending row is not dispatched.
		h.enqueue("after-fence");
		await h.manager.notifyInbound(KEY);
		await Bun.sleep(30);
		expect(port.sends).toHaveLength(0);
		expect(h.logs.some((l) => l.startsWith(`fence_active origin=${KEY} opRef=gw-lost`))).toBe(true);
		// F1: the lost op turns out terminal after all -> clear, dispatch proceeds.
		port.seedOperation("gw-lost", binding.sessionId, "terminal_ok", "late");
		await h.manager.notifyInbound(KEY);
		await eventually(() => port.sends.length === 1, "fence did not clear on F1");
		expect(h.logs.some((l) => l === `fence_cleared origin=${KEY} opRef=gw-lost rule=F1`)).toBe(true);
		expect(h.database.sessionFence(KEY)).toBeUndefined();
		expect(h.database.listEpochMutations({ sinceMs: 0 })).toEqual([]);
	} finally {
		await h.close();
	}
});

test("fence F4: the fence deadline rotates the epoch with execution_uncertain_fence_expired and clears", async () => {
	let now = Date.now();
	const port = new ScriptedSessionPort();
	const h = await harness(port, { now: () => now });
	try {
		const binding = await port.bind({ originKey: KEY, epoch: 0, repo: h.repo });
		h.database.putSession(KEY, binding.sessionId);
		h.database.inboundEnqueue({
			messageId: "lost",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "lost",
		});
		h.database.inboundBindTurn({
			messageId: "lost",
			originKey: KEY,
			epoch: 0,
			opRef: "gw-lost",
			sessionId: binding.sessionId,
		});
		h.database.inboundTurnAccept("gw-lost");
		h.database.inboundTurnLose({
			opRef: "gw-lost",
			originKey: KEY,
			epoch: 0,
			disposition: "operation_lost",
			failureCode: "operation_lost",
			nowMs: now,
			fence: true,
			fenceTtlMs: 60_000,
		});
		now += 61_000;
		h.enqueue("next");
		await h.manager.notifyInbound(KEY);
		await eventually(() => port.sends.length === 1, "fence did not expire");
		const mutations = h.database.listEpochMutations({ sinceMs: 0 });
		expect(mutations.map((m) => [m.reason, m.opRef])).toEqual([["execution_uncertain_fence_expired", "gw-lost"]]);
		expect(h.logs.some((l) => l === `fence_cleared origin=${KEY} opRef=gw-lost rule=F4`)).toBe(true);
	} finally {
		await h.close();
	}
});

test("interim -> TTL -> one visible notice -> late result and parts suppressed", async () => {
	let now = Date.now();
	const port = new ScriptedSessionPort();
	const h = await harness(port, { now: () => now, holdTtlMs: 30 * 60_000 });
	const fake = createFakeGjc({ modes: "status:unknown-forever" });
	const run = async (args: readonly string[]) => (await fake(args))!;
	const real = new BrokerSessionPort({
		database: h.database,
		cli: run,
		instanceId: "interim",
		tailRunner: new TailRunner({ run, repo: h.repo }),
	});
	port.status = real.status.bind(real);
	try {
		const binding = await port.bind({ originKey: KEY, epoch: 0, repo: h.repo });
		h.database.putSession(KEY, binding.sessionId);
		h.database.inboundEnqueue({
			messageId: "lost",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "lost",
		});
		h.database.inboundBindTurn({
			messageId: "lost",
			originKey: KEY,
			epoch: 0,
			opRef: "gw-lost",
			sessionId: binding.sessionId,
		});
		h.database.inboundTurnAccept("gw-lost");
		h.database.inboundMarkInterimDelivered("gw-lost");
		await h.manager.recover();
		now += 31 * 60_000;
		await h.manager.recover();
		await eventually(() => h.deliveries.length === 1, "loss notice not delivered");
		expect(h.deliveries[0]!.text).toBe(
			"[turn failed] operation lost: no result was confirmed for this request within 30 min; the partial output above may be incomplete. It was not resent.",
		);
		// Restart: the disposition is durable; the closed trigger is never re-adopted
		// and later evidence for it cannot produce a second delivery.
		expect(h.database.holdState("gw-lost")?.terminalDisposition).toBe("operation_lost");
		expect(h.database.inboundTurnRow("gw-lost")?.turn_state).toBe("done");
		port.status = async () => ({
			operationRef: "gw-lost",
			status: { status: "terminal_ok", content: { text: "late answer", truncated: false }, terminalAt: now },
			summaryCompleted: true,
		});
		await h.manager.recover();
		await h.manager.tick(KEY);
		expect(h.database.inboundNonterminalTurns(KEY)).toEqual([]);
		expect(h.deliveries).toHaveLength(1);
		expect(port.sends).toHaveLength(0);
	} finally {
		await h.close();
	}
});
