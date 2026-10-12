import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, type OriginRef, originKey } from "@gajae-gateway/protocol";
import { appendAttempt, closeAttempt, createLaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import { buildDeliveryPayload, DeliveryService } from "../src/delivery/delivery";
import { DatabaseStartupError, Store } from "../src/store";
import {
	type WorkAttemptAdmission,
	type WorkAttemptPatch,
	type WorkAttemptRuntime,
	type WorkAttemptSettlement,
	WorkAttemptStateError,
	resolveLaneReportOrigin,
	workAccepted,
	workAttemptDeliveryId,
	workAttemptReportId,
} from "../src/store/groups/work";
import type { WriteTx } from "../src/store/tx";

const START = "2026-09-08T00:00:00.000Z";
const END = "2026-09-08T00:00:10.000Z";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
const directories: string[] = [];
const handles: Array<{ close(): void }> = [];
afterEach(async () => {
	for (const handle of handles.splice(0)) handle.close();
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

async function fixture() {
	const directory = await mkdtemp(join(tmpdir(), "work-lane-store-"));
	directories.push(directory);
	const path = join(directory, "gateway.db");
	const database = await Store.open(path);
	handles.push(database);
	const canonicalAgentDir = join(directory, "agent");
	const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
	database.transaction((tx) => database.bindings.initializeAuthority(tx, authority, database.inspectBrokerAuthority().populated));
	expect(
		database.transaction((tx) => database.bindings.recordOwnedBinding(tx, { authority, sessionId: SESSION, originKey: "work/task/a", epoch: 0, repo: "/work" })),
	).toBe(true);
	const raw = new Database(path);
	handles.push(raw);
	const record = appendAttempt(
		createLaneJobRecord({
			jobId: "lanejob-61",
			branch: "main",
			worktreePath: "/work",
			sessionId: SESSION,
			now: () => new Date(START),
		}),
		{ opRef: "gw-work-store-a", sessionId: SESSION, startedAt: START },
	);
	const runtime: WorkAttemptRuntime = {
		opRef: "gw-work-store-a",
		jobId: record.jobId,
		laneKey: "work-a",
		sessionKey: "work/task/a",
		sessionId: SESSION,
		epoch: 0,
		cwd: "/work",
		startedAt: START,
		mode: "start",
		sendPhase: "prepared",
		sendEvidence: null,
		terminal: null,
		output: { disposition: "pending", reads: 0, nextReadAt: null, excerpt: null, proof: null, knownSilence: null },
		parent: { kind: "persona", originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN },
		reportId: workAttemptReportId(database.instanceId, record.jobId, "gw-work-store-a"),
		wakeReportId: null,
		noticeHash: null,
		deliveryId: workAttemptDeliveryId(database.instanceId, record.jobId, "gw-work-store-a"),
		decision: "undecided",
		settledAt: null,
		version: 0,
	};
	const closed = closeAttempt({ record, opRef: runtime.opRef, endState: "completed", endedAt: END });
	const settlement: WorkAttemptSettlement = {
		terminal: {
			kind: "broker",
			observedAt: END,
			reasonCode: "end_turn",
			status: { status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } },
		},
		output: { ...runtime.output, disposition: "unavailable" },
		decision: "report",
		settledAt: END,
	};
	const payload = buildDeliveryPayload(
		runtime.opRef,
		LOOPBACK_ORIGIN,
		"[lane a] completed: output_unavailable",
		runtime.deliveryId,
	)!;
	const admission: WorkAttemptAdmission = {
		kind: "persona",
		row: {
			messageId: runtime.reportId,
			originKey: originKey(LOOPBACK_ORIGIN),
			originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
			body: payload.text,
			receivedAt: END,
		},
		fallbackPayload: payload,
	};
	return { path, database, raw, authority, record, runtime, closed, settlement, payload, admission };
}

function touchWorkSession(database: Store, tx: WriteTx, runtime: WorkAttemptRuntime, at: string): void {
	database.bindings.touchWorkSession(
		tx,
		runtime.sessionKey,
		runtime.sessionId,
		runtime.epoch,
		at,
		JSON.stringify({ platform: "work", kind: "task", conversationId: runtime.sessionKey.slice("work/task/".length) }),
	);
}

function prepareAttempt(database: Store, tx: WriteTx, runtime: WorkAttemptRuntime, record: ReturnType<typeof appendAttempt>): void {
	const binding = database.bindings.getSessionRecord(runtime.sessionKey);
	database.work.workAttemptPrepare(
		tx,
		runtime,
		record,
		runtime.mode === "historical" || (binding?.sessionId === runtime.sessionId && binding.epoch === runtime.epoch),
	);
	touchWorkSession(database, tx, runtime, runtime.startedAt);
}

function updateAttempt(
	database: Store,
	tx: WriteTx,
	opRef: string,
	expectedVersion: number,
	patch: WorkAttemptPatch,
): WorkAttemptRuntime | undefined {
	const current = database.work.workAttemptGet(opRef);
	const updated = database.work.workAttemptUpdate(tx, opRef, expectedVersion, patch);
	if (current && updated && !workAccepted(current) && workAccepted(updated) && updated.noticeHash !== null)
		database.core.metaSet(`lane-notice:${updated.sessionKey}`, JSON.stringify({ epoch: updated.epoch, hash: updated.noticeHash }));
	return updated;
}

function settleAttempt(
	database: Store,
	tx: WriteTx,
	opRef: string,
	expectedVersion: number,
	record: ReturnType<typeof closeAttempt>,
	patch: WorkAttemptSettlement,
	admission?: WorkAttemptAdmission,
) {
	const checks = {
		...(admission?.kind === "persona"
			? {
					personaSessionExists: database.bindings.getSessionRecord(admission.row.originKey) !== undefined,
					personaMessageExists: database.inbox.messageExists(admission.row.messageId),
				}
			: {}),
		...(admission?.kind === "lane"
			? { laneParentAvailable: database.work.laneParentAvailable(admission.report.parentName) }
			: {}),
	};
	const result = database.work.workAttemptSettle(tx, opRef, expectedVersion, record, patch, admission, checks);
	if (!result) return undefined;
	touchWorkSession(database, tx, result.runtime, result.runtime.settledAt!);
	if (result.runtime.decision === "reported" && admission?.kind === "persona") {
		if (!database.inbox.enqueue(tx, {
			messageId: admission.row.messageId,
			originKey: admission.row.originKey,
			originRefJson: resolveLaneReportOrigin(result.runtime.parent, admission.row.originRefJson),
			body: admission.row.body,
			receivedAt: result.runtime.settledAt!,
			source: "lane_report",
		})) throw new Error("lane report inbound conflict");
	}
	for (const payload of [result.fallbackPayload, result.childFallback]) {
		if (!payload) continue;
		database.outbox.enqueue(tx, {
			deliveryId: payload.deliveryId!,
			turnId: payload.turnId,
			originKey: originKey(payload.origin),
			payloadJson: JSON.stringify(payload),
		});
	}
	return result;
}
function seedLinkedWakeReport(
	f: Awaited<ReturnType<typeof fixture>>,
	root: { readonly originKey: string; readonly origin: OriginRef } | null,
): string {
	const childName = "child";
	const childOpRef = "gw-child-op";
	const childJobId = `lanejob-${Buffer.from(childName, "utf8").toString("hex")}`;
	const reportId = workAttemptReportId(f.database.instanceId, childJobId, childOpRef);
	f.raw
		.query(
			"INSERT INTO lane_reports (report_id, parent_name, child_name, child_op_ref, body, root_json, state, claim_kind, claim_ref, claim_target_op_ref, claim_seq, hold_reason, consumed_op_ref, created_at, updated_at) VALUES (?, 'a', ?, ?, ?, ?, 'claimed', 'wake', ?, NULL, 1, NULL, NULL, ?, ?)",
		)
		.run(
			reportId,
			childName,
			childOpRef,
			"[lane child] completed: child output",
			root === null ? null : JSON.stringify(root),
			f.runtime.opRef,
			START,
			START,
		);
	return reportId;
}

describe("work attempt durable transactions", () => {
	for (const mode of ["start", "run"] as const) {
		test(`${mode} publishes worker metadata and activity atomically at prepare and settlement`, async () => {
			const f = await fixture();
			const runtime = { ...f.runtime, mode, parent: mode === "start" ? f.runtime.parent : null };
			const settlement = { ...f.settlement, decision: mode === "start" ? ("report" as const) : ("no_target" as const) };
			const admission = mode === "start" ? f.admission : undefined;
			const expectedDecision = mode === "start" ? "fallback" : "no_target";
			const identity = () => f.database.bindings.sessionIdentityRows().find((row) => row.origin_key === runtime.sessionKey);
			const workerOrigin = JSON.stringify({ platform: "work", kind: "task", conversationId: "a" });
			// An AFTER fault proves both assignments roll back even after the UPDATE executes.
			f.raw.exec("CREATE TRIGGER fault AFTER UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
			expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record))).toThrow();
			expect(identity()).toMatchObject({ origin_ref_json: null, last_activity_at: null });
			expect(f.database.work.workAttemptGet(runtime.opRef)).toBeUndefined();
			expect(f.database.work.laneJobJson(runtime.jobId)).toBeUndefined();
			f.raw.exec("DROP TRIGGER fault");
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			expect(identity()).toMatchObject({ origin_ref_json: workerOrigin, last_activity_at: START });
			// Simulate missing legacy metadata to prove settlement restores it too.
			f.raw.query("UPDATE sessions SET origin_ref_json = NULL WHERE origin_key = ?").run(runtime.sessionKey);
			f.raw.exec("CREATE TRIGGER fault AFTER UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
			expect(() => f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, settlement, admission))).toThrow();
			expect(identity()).toMatchObject({ origin_ref_json: null, last_activity_at: START });
			expect(f.database.work.workAttemptGet(runtime.opRef)?.decision).toBe("undecided");
			expect(f.database.outbox.rows()).toHaveLength(0);
			f.raw.exec("DROP TRIGGER fault");
			expect(f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, settlement, admission))?.runtime.decision).toBe(
				expectedDecision,
			);
			expect(identity()).toMatchObject({ origin_ref_json: workerOrigin, last_activity_at: END });
		});

		test(`${mode} stale epoch settlement cannot retag or refresh a successor binding`, async () => {
			const f = await fixture();
			const runtime = { ...f.runtime, mode, parent: mode === "start" ? f.runtime.parent : null };
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			const successorOrigin = JSON.stringify(LOOPBACK_ORIGIN);
			// Keep sessionId identical: the epoch predicate must independently fence the write.
			f.raw
				.query("UPDATE sessions SET epoch = epoch + 1, origin_ref_json = ?, last_activity_at = ? WHERE origin_key = ?")
				.run(successorOrigin, START, runtime.sessionKey);
			const before = f.database.bindings.sessionIdentityRows();
			expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record))).toThrow();
			const requested = mode === "start" ? ("report" as const) : ("no_target" as const);
			const expected = mode === "start" ? "fallback" : "no_target";
			expect(
				f.database.transaction((tx) => settleAttempt(f.database, tx, 
					runtime.opRef,
					0,
					f.closed,
					{ ...f.settlement, decision: requested },
					mode === "start" ? f.admission : undefined,
				))?.runtime.decision,
			).toBe(expected);
			expect(f.database.bindings.sessionIdentityRows()).toEqual(before);
		});
	}
	for (const table of ["lane_jobs", "work_attempt_runtime", "sessions"]) {
		test(`prepare rolls back history/runtime/activity when ${table} write fails`, async () => {
			const f = await fixture();
			const action = table === "sessions" ? "UPDATE" : "INSERT";
			f.raw.exec(`CREATE TRIGGER fault BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'fault'); END`);
			expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record))).toThrow();
			expect(f.database.work.laneJobJson(f.runtime.jobId)).toBeUndefined();
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toBeUndefined();
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBeNull();
			f.raw.exec("DROP TRIGGER fault");
			f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(START);
		});
	}

	for (const table of ["work_attempt_runtime", "lane_jobs", "sessions", "deliveries"]) {
		test(`settlement rolls back all effects when ${table} write fails`, async () => {
			const f = await fixture();
			f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
			const action = table === "deliveries" ? "INSERT" : "UPDATE";
			f.raw.exec(`CREATE TRIGGER fault BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'fault'); END`);
			expect(() => f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission))).toThrow();
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
			expect(parseLaneJobRecord(f.database.work.laneJobJson(f.runtime.jobId)!).attempts[0]?.endedAt).toBeUndefined();
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(START);
			expect(f.database.outbox.rows()).toHaveLength(0);
			f.raw.exec("DROP TRIGGER fault");
			expect(
				f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission))?.runtime.decision,
			).toBe("fallback");
			expect(f.database.outbox.rows()).toHaveLength(1);
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(END);
		});
	}

	test("CAS refuses stale settlement and duplicate decisions; pruning does not regenerate", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const accepted = f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		}));
		expect(accepted?.version).toBe(1);
		expect(f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission))).toBeUndefined();
		expect(f.database.outbox.rows()).toHaveLength(0);
		expect(f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 1, f.closed, f.settlement, f.admission))?.runtime.version).toBe(
			2,
		);
		const ledger = new DeliveryService(f.database);
		expect(ledger.confirm(f.runtime.deliveryId)).toBe("transitioned");
		expect(ledger.prune(0, Date.now() + 1000)).toBe(1);
		const reopened = await Store.open(f.path);
		handles.push(reopened);
		expect(reopened.transaction((tx) => settleAttempt(reopened, tx, f.runtime.opRef, 2, f.closed, f.settlement, f.admission))).toBeUndefined();
		expect(reopened.transaction((tx) => updateAttempt(reopened, tx, f.runtime.opRef, 2, {}))).toBeUndefined();
		expect(reopened.work.workAttemptGet(f.runtime.opRef)?.decision).toBe("fallback");
		expect(reopened.outbox.rows()).toHaveLength(0);
	});

	test("duplicate op and different open attempt cannot rewrite lane history", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record))).toThrow();
		const opRef = "gw-work-store-b";
		const other = {
			...f.runtime,
			opRef,
			deliveryId: workAttemptDeliveryId(f.database.instanceId, f.runtime.jobId, opRef),
		};
		const record = { ...f.record, attempts: [{ ...f.record.attempts[0]!, opRef }] };
		expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, other, record))).toThrow();
		expect(f.database.work.workAttemptOpen()).toEqual([f.runtime]);
		expect(f.database.work.workAttemptOpen(1, f.runtime.opRef)).toEqual([]);
		expect(() => f.database.work.workAttemptOpen(1001)).toThrow();
		// SQL also protects against a second writer bypassing the accessor.
		expect(() =>
			f.raw
				.query(
					"INSERT INTO work_attempt_runtime SELECT ?, job_id, lane_key, session_id, version, settled_at, ?, record_json FROM work_attempt_runtime",
				)
				.run(opRef, other.deliveryId),
		).toThrow();
	});

	test("conflicting deterministic ledger identity aborts the entire settlement", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		new DeliveryService(f.database).prepare(
			"other-turn",
			LOOPBACK_ORIGIN,
			"other obligation",
			undefined,
			f.runtime.deliveryId,
		);
		expect(() => f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission))).toThrow();
		expect(f.database.work.workAttemptGet(f.runtime.opRef)?.decision).toBe("undecided");
		expect(f.database.outbox.rows()[0]?.turn_id).toBe("other-turn");
	});

	test("same-transaction delivery seam requires ownership, suppresses silence and checks duplicate payloads", async () => {
		const f = await fixture();
		const delivery = new DeliveryService(f.database);
		expect(() => delivery.persistInTransaction(undefined as never, f.payload)).toThrow("inactive_tx");
		expect(f.database.transaction((tx) => delivery.persistInTransaction(tx, f.payload))).toBe(true);
		expect(f.database.transaction((tx) => delivery.persistInTransaction(tx, f.payload))).toBe(false);
		expect(() => f.database.transaction((tx) => delivery.persistInTransaction(tx, { ...f.payload, text: "changed" }))).toThrow();
		expect(buildDeliveryPayload("turn", LOOPBACK_ORIGIN, "NO_REPLY", "silent")).toBeUndefined();
	});

	test("known silence survives reopen and suppresses exactly once", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const proof = {
			opRef: f.runtime.opRef,
			sessionId: SESSION,
			epoch: 0,
			observedAtMs: Date.parse(END),
			source: "turn.result" as const,
			attribution: "operation_ref" as const,
			fullness: "original" as const,
			clientRef: f.runtime.opRef,
			repo: "/work",
			terminalAt: Date.parse(END),
			contentVersion: 1 as const,
			byteLength: 8,
		};
		const output = { ...f.runtime.output, disposition: "silent" as const, knownSilence: proof, proof, excerpt: null };
		expect(f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, { output }))?.version).toBe(1);
		expect(f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, { output }))).toBeUndefined();
		const reopened = await Store.open(f.path);
		handles.push(reopened);
		expect(reopened.work.workAttemptGet(f.runtime.opRef)?.output.knownSilence).toEqual(proof);
		expect(() => reopened.transaction((tx) => updateAttempt(reopened, tx, f.runtime.opRef, 1, { output: f.runtime.output }))).toThrow();
		const suppressed = { ...f.settlement, output, decision: "suppressed" as const };
		expect(reopened.transaction((tx) => settleAttempt(reopened, tx, f.runtime.opRef, 1, f.closed, suppressed))?.runtime.decision).toBe("suppressed");
		expect(reopened.transaction((tx) => settleAttempt(reopened, tx, f.runtime.opRef, 2, f.closed, suppressed))).toBeUndefined();
		expect(reopened.outbox.rows()).toHaveLength(0);
	});

	test("output claims persist three-read budget; stale/wrong proof and oversized excerpt fail closed", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		let runtime = f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, { terminal: f.settlement.terminal }))!;
		for (let reads = 1; reads <= 3; reads++)
			runtime = f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
				output: { ...runtime.output, reads, nextReadAt: END },
			}))!;
		expect(runtime.output.reads).toBe(3);
		expect(() =>
			f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, runtime.version, { output: { ...runtime.output, reads: 4 } })),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, runtime.version, { output: { ...runtime.output, reads: 0 } })),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
				output: { ...runtime.output, excerpt: "界".repeat(1000) },
			})),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
				output: {
					...runtime.output,
					knownSilence: {
						opRef: "gw-other",
						sessionId: SESSION,
						epoch: 0,
						observedAtMs: Date.parse(END),
						source: "turn.result",
						attribution: "operation_ref",
						fullness: "original",
						clientRef: runtime.opRef,
						repo: "/work",
						terminalAt: Date.parse(END),
						contentVersion: 1,
						byteLength: 8,
					},
				},
			})),
		).toThrow();
	});

	test("terminal evidence is write-once except a proven missing-to-present receipt (#248)", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const missing = {
			kind: "broker" as const,
			observedAt: END,
			reasonCode: "terminal_missing_receipt",
			status: { status: "terminal_ok" as const, receiptState: "missing" as const, outcome: { reason: "end_turn" } },
		};
		const present = {
			...missing,
			reasonCode: "end_turn",
			status: { ...missing.status, receiptState: "present" as const },
		};
		const runtime = f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, { terminal: missing }))!;
		const proof = {
			opRef: f.runtime.opRef,
			sessionId: SESSION,
			epoch: 0,
			observedAtMs: Date.parse(END),
			source: "turn.result" as const,
			attribution: "operation_ref" as const,
			fullness: "original" as const,
			clientRef: f.runtime.opRef,
			repo: "/work",
			terminalAt: Date.parse(END),
			contentVersion: 1 as const,
			byteLength: 8,
		};
		const available = { ...runtime.output, disposition: "available" as const, excerpt: "PR ready", proof };
		// No proven body: the receipt cannot be upgraded.
		expect(() => f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, runtime.version, { terminal: present }))).toThrow();
		// Any other terminal rewrite remains forbidden, even with a proven body.
		expect(() =>
			f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
				terminal: { ...present, status: { ...present.status, outcome: { reason: "refusal" } } },
				output: available,
			})),
		).toThrow();
		const upgraded = f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
			terminal: present,
			output: available,
		}))!;
		expect(upgraded.terminal).toEqual(present);
		// present never regresses to missing.
		expect(() => f.database.transaction((tx) => updateAttempt(f.database, tx, upgraded.opRef, upgraded.version, { terminal: missing }))).toThrow();
	});

	test("corrupt runtime projection or history throws rather than returning empty", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.raw.query("UPDATE work_attempt_runtime SET version = 8 WHERE op_ref = ?").run(f.runtime.opRef);
		expect(() => f.database.work.workAttemptGet(f.runtime.opRef)).toThrow(WorkAttemptStateError);
		expect(() => f.database.work.workAttemptOpen()).toThrow(WorkAttemptStateError);
		f.raw.query("UPDATE work_attempt_runtime SET version = 0, record_json = '{' WHERE op_ref = ?").run(f.runtime.opRef);
		expect(() => f.database.work.workAttemptOpenByLane(f.runtime.laneKey)).toThrow(WorkAttemptStateError);
	});

	test("run is response-only and final decisions cannot bypass settlement", async () => {
		const f = await fixture();
		expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, mode: "run" }, f.record))).toThrow();
		const runtime = { ...f.runtime, mode: "run" as const, parent: null };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		expect(() => f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, 0, f.settlement))).toThrow();
		expect(() => f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, f.settlement, f.admission))).toThrow();
		const patch = { ...f.settlement, decision: "no_target" as const };
		expect(() => f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, patch, f.admission))).toThrow();
		expect(f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, patch))?.runtime.decision).toBe("no_target");
		expect(f.database.outbox.rows()).toHaveLength(0);
	});

	test("wake_unaccepted validator rejects fabricated upstream decisions", async () => {
		for (const decision of ["reported", "fallback", "no_target", "suppressed"] as const) {
			const f = await fixture();
			const linkedId = seedLinkedWakeReport(f, {
				originKey: originKey(LOOPBACK_ORIGIN),
				origin: LOOPBACK_ORIGIN,
			});
			const runtime = { ...f.runtime, wakeReportId: linkedId };
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			const terminal = { kind: "local" as const, observedAt: END, reasonCode: "session_dead" };
			const invalid = {
				...runtime,
				parent: decision === "no_target" ? null : runtime.parent,
				version: 1,
				terminal,
				output: { ...runtime.output, disposition: "unavailable" as const },
				decision,
				settledAt: END,
			};
			f.raw
				.query("UPDATE work_attempt_runtime SET record_json = ?, version = 1, settled_at = ? WHERE op_ref = ?")
				.run(JSON.stringify(invalid), END, runtime.opRef);
			expect(() => f.database.work.workAttemptGet(runtime.opRef)).toThrow(WorkAttemptStateError);
		}

		const noLink = await fixture();
		noLink.database.transaction((tx) => prepareAttempt(noLink.database, tx, noLink.runtime, noLink.record));
		const noLinkInvalid = {
			...noLink.runtime,
			version: 1,
			terminal: { kind: "local" as const, observedAt: END, reasonCode: "session_dead" },
			output: { ...noLink.runtime.output, disposition: "unavailable" as const },
			decision: "wake_unaccepted" as const,
			settledAt: END,
		};
		noLink.raw
			.query("UPDATE work_attempt_runtime SET record_json = ?, version = 1, settled_at = ? WHERE op_ref = ?")
			.run(JSON.stringify(noLinkInvalid), END, noLink.runtime.opRef);
		expect(() => noLink.database.work.workAttemptGet(noLink.runtime.opRef)).toThrow(WorkAttemptStateError);
	});

	test("wake_unaccepted settles only the linked child transition", async () => {
		for (const [root, reasonCode, expectedState] of [
			[{ originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN }, "send_rejected", "fallback"],
			[null, "send_rejected", "undeliverable"],
			[{ originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN }, "session_dead", "held"],
		] as const) {
			const f = await fixture();
			const linkedId = seedLinkedWakeReport(f, root);
			const runtime = { ...f.runtime, parent: null, wakeReportId: linkedId };
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			const terminal = { kind: "local" as const, observedAt: END, reasonCode };
			const staged = f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, 0, {
				terminal,
				output: { ...runtime.output, disposition: "unavailable" },
			}))!;
			const closed = closeAttempt({
				record: f.record,
				opRef: runtime.opRef,
				endState: reasonCode === "send_rejected" ? "failed" : "terminal_uncertain",
				errorCode: reasonCode,
				endedAt: END,
			});
			const result = f.database.transaction((tx) => settleAttempt(f.database, tx, staged.opRef, staged.version, closed, {
				terminal,
				output: staged.output,
				decision: "wake_unaccepted",
				settledAt: END,
			}));
			expect(result?.runtime.decision).toBe("wake_unaccepted");
			expect(f.database.work.workAttemptGet(runtime.opRef)?.decision).toBe("wake_unaccepted");
			expect(f.database.inbox.turnRow(runtime.opRef)).toBeUndefined();
			expect(f.raw.query("SELECT 1 FROM lane_reports WHERE child_op_ref = ?").get(runtime.opRef)).toBeNull();
			expect(f.database.outbox.rows().some((row) => row.delivery_id === runtime.deliveryId)).toBe(false);
			const child = f.database.work.laneReportGet(linkedId)!;
			expect(child.state).toBe(expectedState);
			if (expectedState === "fallback") {
				const childDeliveryId = workAttemptDeliveryId(
					f.database.instanceId,
					`lanejob-${Buffer.from("child", "utf8").toString("hex")}`,
					"gw-child-op",
				);
				expect(result?.childFallback?.deliveryId).toBe(childDeliveryId);
				expect(f.database.outbox.rows()).toMatchObject([
					{ delivery_id: childDeliveryId, turn_id: "gw-child-op", origin_key: originKey(LOOPBACK_ORIGIN) },
				]);
			} else {
				expect(result?.childFallback).toBeUndefined();
				expect(f.database.outbox.rows()).toHaveLength(0);
			}
			if (expectedState === "held") expect(child.hold_reason).toBe("wake_acceptance_uncertain");
		}
	});

	test("accepted wake consumes its child and reports normally to its inherited persona", async () => {
		const f = await fixture();
		const root = { originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN };
		const linkedId = seedLinkedWakeReport(f, root);
		const personaSessionId = crypto.randomUUID();
		expect(
			f.database.transaction((tx) => f.database.bindings.recordOwnedBinding(tx, {
				authority: f.authority,
				sessionId: personaSessionId,
				originKey: root.originKey,
				epoch: 0,
				repo: "/work",
			})),
		).toBe(true);
		const runtime = { ...f.runtime, wakeReportId: linkedId };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		const accepted = f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		}))!;
		expect(f.database.work.laneReportGet(linkedId)?.state).toBe("consumed");
		const result = f.database.transaction((tx) => settleAttempt(f.database, tx, accepted.opRef, accepted.version, f.closed, f.settlement, f.admission))!;
		expect(result.runtime.decision).toBe("reported");
		expect(f.database.work.laneReportGet(linkedId)?.consumed_op_ref).toBe(runtime.opRef);
		expect(f.database.inbox.pendingOldest(root.originKey)).toMatchObject({
			message_id: runtime.reportId,
			source: "lane_report",
		});
		expect(result.childFallback).toBeUndefined();
		expect(f.database.outbox.rows()).toHaveLength(0);
	});
	test("wake with proven output and missing receipt is accepted and reports normally", async () => {
		const f = await fixture();
		const root = { originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN };
		const linkedId = seedLinkedWakeReport(f, root);
		const personaSessionId = crypto.randomUUID();
		f.database.transaction((tx) => f.database.bindings.recordOwnedBinding(tx, {
			authority: f.authority,
			sessionId: personaSessionId,
			originKey: root.originKey,
			epoch: 0,
			repo: "/work",
		}));
		const runtime = { ...f.runtime, wakeReportId: linkedId };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		const proof = {
			opRef: runtime.opRef,
			sessionId: runtime.sessionId,
			epoch: runtime.epoch,
			observedAtMs: Date.parse(END),
			source: "turn.result" as const,
			attribution: "operation_ref" as const,
			fullness: "original" as const,
			clientRef: runtime.opRef,
			repo: runtime.cwd,
			terminalAt: Date.parse(END),
			contentVersion: 1 as const,
			byteLength: Buffer.byteLength("proven wake output"),
		};
		const provenOutput = {
			...runtime.output,
			disposition: "available" as const,
			excerpt: "proven wake output",
			proof,
		};
		const accepted = f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, 0, { output: provenOutput }))!;
		expect(accepted.sendPhase).toBe("prepared");
		expect(f.database.work.laneReportGet(linkedId)?.state).toBe("consumed");
		const patch = {
			...f.settlement,
			output: { ...provenOutput, disposition: "unavailable" as const, excerpt: null },
			decision: "report" as const,
		};
		const result = f.database.transaction((tx) => settleAttempt(f.database, tx, accepted.opRef, accepted.version, f.closed, patch, f.admission))!;
		expect(result.runtime.decision).toBe("reported");
		expect(result.runtime.output.proof).toEqual(proof);
		expect(f.database.work.laneReportGet(linkedId)?.consumed_op_ref).toBe(runtime.opRef);
		expect(f.database.inbox.pendingOldest(root.originKey)).toMatchObject({
			message_id: runtime.reportId,
			source: "lane_report",
		});
	});
	test("wrong bound identity and history cannot be prepared", async () => {
		const f = await fixture();
		expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, epoch: 1 }, f.record))).toThrow();
		expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, cwd: "/different" }, f.record))).toThrow();
		expect(() => f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, deliveryId: "arbitrary" }, f.record))).toThrow();
		expect(f.database.work.workAttemptOpen()).toHaveLength(0);
		expect(f.database.work.laneJobJson(f.runtime.jobId)).toBeUndefined();
	});

	test("v24 migration rewrites a quarantined v23 runtime and restores quarantine triggers", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const settled = f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission))!;
		expect(settled.runtime.decision).toBe("fallback");
		const targetAuthority = {
			canonicalAgentDir: join(f.path, "..", "target-agent"),
			identity: "target-broker",
		};
		f.database.transaction((tx) => f.database.bindings.cutoverBrokerAuthority(tx, {
			expectedAuthority: f.authority,
			targetAuthority,
			evidence: "test v23 quarantine migration",
			disposition: "quarantine",
		}));
		const current = f.database.work.workAttemptGet(f.runtime.opRef)!;
		const legacy: Record<string, unknown> = {
			...current,
			target: current.parent?.kind === "persona" ? current.parent.origin : null,
			decision: "enqueued",
		};
		delete legacy.parent;
		delete legacy.reportId;
		delete legacy.wakeReportId;
		delete legacy.noticeHash;
		f.raw.exec(
			"DROP TRIGGER work_attempt_runtime_quarantine_update; DROP TRIGGER work_attempt_runtime_quarantine_delete;",
		);
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
			.run(JSON.stringify(legacy), f.runtime.opRef);
		f.raw.exec(`
			CREATE TRIGGER work_attempt_runtime_quarantine_update BEFORE UPDATE ON work_attempt_runtime
			WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
			BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
			CREATE TRIGGER work_attempt_runtime_quarantine_delete BEFORE DELETE ON work_attempt_runtime
			WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
			BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
		`);
		f.raw.exec(
			"DROP TABLE lane_reports; ALTER TABLE inbound_messages DROP COLUMN source; DELETE FROM schema_migrations WHERE version >= 24",
		);

		const migrated = await Store.open(f.path);
		handles.push(migrated);
		expect(migrated.schemaVersion).toBe(32);
		expect(migrated.work.workAttemptGet(f.runtime.opRef)).toMatchObject({
			decision: "fallback",
			parent: { kind: "persona", origin: LOOPBACK_ORIGIN, originKey: originKey(LOOPBACK_ORIGIN) },
			reportId: workAttemptReportId(migrated.instanceId, f.runtime.jobId, f.runtime.opRef),
			wakeReportId: null,
			noticeHash: null,
		});
		const triggers = new Set(
			f.raw
				.query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'trigger'")
				.all()
				.map((row) => row.name),
		);
		expect(triggers.has("work_attempt_runtime_quarantine_update")).toBe(true);
		expect(triggers.has("work_attempt_runtime_quarantine_delete")).toBe(true);
		expect(() =>
			f.raw.query("UPDATE work_attempt_runtime SET version = version + 1 WHERE op_ref = ?").run(f.runtime.opRef),
		).toThrow();
	});

	test("v24 corrupt runtime migration rolls schema changes back", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.raw.exec(
			"DROP TABLE lane_reports; ALTER TABLE inbound_messages DROP COLUMN source; DELETE FROM schema_migrations WHERE version >= 24",
		);
		f.raw.query("UPDATE work_attempt_runtime SET record_json = '{' WHERE op_ref = ?").run(f.runtime.opRef);
		await expect(Store.open(f.path)).rejects.toBeInstanceOf(DatabaseStartupError);
		expect(f.database.schemaVersion).toBe(23);
		expect(f.raw.query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'lane_reports'").get()).toBeNull();
		expect(
			f.raw
				.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
				.all()
				.some((row) => row.name === "source"),
		).toBe(false);
	});
	test("v21 migration preserves historical open history and adopts without parent", async () => {
		const f = await fixture();
		f.database.transaction((tx) => f.database.work.putLaneJob(tx, { ...f.record, laneKey: f.runtime.laneKey, json: JSON.stringify(f.record) }));
		// Preserve the fixture's explicit provenance while replaying v21 through v26.
		// Restoring a snapshot is not initialization/adoption of a populated database.
		f.raw.exec(`CREATE TEMP TABLE saved_authority AS SELECT * FROM broker_authority;
			CREATE TEMP TABLE saved_bindings AS SELECT * FROM broker_owned_bindings;`);
		for (const table of ["inbound_messages", "lane_jobs", "work_attempt_runtime", "monitor_events", "authored_outputs"])
			for (const action of ["update", "delete"]) f.raw.exec(`DROP TRIGGER ${table}_quarantine_${action}`);
		for (const table of ["broker_owned_bindings", "broker_cutovers", "broker_quarantine", "broker_retired_sessions"])
			for (const action of ["update", "delete"]) f.raw.exec(`DROP TRIGGER ${table}_immutable_${action}`);
		for (const table of [
			"broker_authority",
			"broker_owned_bindings",
			"broker_tail_cursors",
			"broker_cutovers",
			"broker_quarantine",
			"broker_retired_sessions",
		])
			f.raw.exec(`DROP TABLE ${table}`);
		f.raw.exec(`
ALTER TABLE memory_intents DROP COLUMN quarantine_reason;
ALTER TABLE memory_intents DROP COLUMN attempts;
DROP TABLE work_attempt_runtime;
ALTER TABLE inbound_messages DROP COLUMN source;
DROP TABLE lane_reports;
DELETE FROM schema_migrations WHERE version >= 21;
`);
		const migrated = await Store.open(f.path);
		handles.push(migrated);
		f.raw.exec(`INSERT INTO broker_authority SELECT * FROM saved_authority;
			INSERT INTO broker_owned_bindings SELECT * FROM saved_bindings;
			DROP TABLE saved_authority;
			DROP TABLE saved_bindings;`);
		migrated.bindings.assertAuthority(f.authority);
		expect(migrated.bindings.assertOwnedSession(SESSION, f.runtime.cwd, f.authority)).toMatchObject({
			originKey: f.runtime.sessionKey,
			epoch: f.runtime.epoch,
		});
		expect(migrated.schemaVersion).toBe(32);
		expect(migrated.work.laneJobJson(f.runtime.jobId)).toBe(JSON.stringify(f.record));
		const historical = { ...f.runtime, mode: "historical" as const, sendPhase: "uncertain" as const, parent: null };
		migrated.transaction((tx) => prepareAttempt(migrated, tx, historical, f.record));
		expect(migrated.work.workAttemptGet(f.runtime.opRef)?.parent).toBeNull();
		expect(
			migrated.transaction((tx) => settleAttempt(migrated, tx, f.runtime.opRef, 0, f.closed, { ...f.settlement, decision: "no_target" }))?.runtime
				.decision,
		).toBe("no_target");
		expect(migrated.outbox.rows()).toHaveLength(0);
	});
});
