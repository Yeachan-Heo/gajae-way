import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, type OriginRef, originKey } from "@gajae-gateway/protocol";
import { appendAttempt, closeAttempt, createLaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import { bootGateway } from "../src/boot";
import { buildDeliveryPayload, DeliveryService } from "../src/delivery/delivery";
import { readPinnedGjcVersion } from "../src/orchestrator/broker";
import { DatabaseStartupError, Store } from "../src/store";
import {
	resolveLaneReportOrigin,
	type WorkAttemptAdmission,
	type WorkAttemptPatch,
	type WorkAttemptRuntime,
	type WorkAttemptSettlement,
	WorkAttemptStateError,
	workAccepted,
	workAttemptDeliveryId,
	workAttemptReportId,
} from "../src/store/groups/work";
import type { WriteTx } from "../src/store/tx";

const START = "2026-09-08T00:00:00.000Z";
const END = "2026-09-08T00:00:10.000Z";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
const PARENT_SESSION = "bcb7f8a3-30e5-4d76-9e7e-32d2e2f51f9a";
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
	const canonicalAgentDir = join(await realpath(directory), "agent");
	const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
	database.transaction((tx) =>
		database.bindings.initializeAuthority(tx, authority, database.inspectBrokerAuthority().populated),
	);
	expect(
		database.transaction((tx) =>
			database.bindings.recordOwnedBinding(tx, {
				authority,
				sessionId: SESSION,
				originKey: "work/task/a",
				epoch: 0,
				repo: "/work",
			}),
		),
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
	return { directory, path, database, raw, authority, record, runtime, closed, settlement, payload, admission };
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

function prepareAttempt(
	database: Store,
	tx: WriteTx,
	runtime: WorkAttemptRuntime,
	record: ReturnType<typeof appendAttempt>,
): void {
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
		database.core.metaSet(
			`lane-notice:${updated.sessionKey}`,
			JSON.stringify({ epoch: updated.epoch, hash: updated.noticeHash }),
		);
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
		if (
			!database.inbox.enqueue(tx, {
				messageId: admission.row.messageId,
				originKey: admission.row.originKey,
				originRefJson: resolveLaneReportOrigin(result.runtime.parent, admission.row.originRefJson),
				body: admission.row.body,
				receivedAt: result.runtime.settledAt!,
				source: "lane_report",
			})
		)
			throw new Error("lane report inbound conflict");
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

describe("work attempt boot reconciliation", () => {
	test("reconciliation restores a settled attempt when only its database column is torn", async () => {
		const f = await fixture();
		const settledAt = "2026-09-08T00:00:20.000Z";
		const settlement = { ...f.settlement, settledAt };
		const closed = closeAttempt({
			record: f.record,
			opRef: f.runtime.opRef,
			endState: "completed",
			endedAt: settledAt,
		});
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.database.transaction((tx) => settleAttempt(f.database, tx, f.runtime.opRef, 0, closed, settlement, f.admission));
		const deliveries = f.database.outbox.rows();
		f.raw.query("UPDATE work_attempt_runtime SET settled_at = NULL WHERE op_ref = ?").run(f.runtime.opRef);

		const reconciled = f.database.reconcileWorkAttempts(() => {});
		expect(reconciled).toBe(1);
		expect(f.database.work.workAttemptGet(f.runtime.opRef)?.settledAt).toBe(settledAt);
		expect(f.database.work.workAttemptGet(f.runtime.opRef)?.terminal?.observedAt).toBe(END);
		expect(f.database.outbox.rows()).toEqual(deliveries);
	});

	test("reconciliation prefers the matching history and SQL timestamp over torn runtime JSON", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.database.transaction((tx) =>
			settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
		);
		const runtime = f.database.work.workAttemptGet(f.runtime.opRef);
		if (runtime === undefined) throw new Error("settled work attempt is missing");
		const tornRuntime = { ...runtime, settledAt: "2026-09-08T00:00:30.000Z" };
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
			.run(JSON.stringify(tornRuntime), runtime.opRef);

		expect(() => f.database.work.workAttemptGet(runtime.opRef)).toThrow(WorkAttemptStateError);
		expect(f.database.reconcileWorkAttempts(() => {})).toBe(1);
		expect(f.database.work.workAttemptGet(runtime.opRef)?.settledAt).toBe(END);
		const historyJson = f.database.work.laneJobJson(runtime.jobId);
		expect(historyJson).toBeDefined();
		if (historyJson === undefined) throw new Error("reconciled lane history is missing");
		expect(parseLaneJobRecord(historyJson).attempts[0]?.endedAt).toBe(END);
	});

	test("reconciliation quarantines conflicting settled timestamps without corroboration", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.database.transaction((tx) =>
			settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
		);
		const runtime = f.database.work.workAttemptGet(f.runtime.opRef);
		if (runtime === undefined) throw new Error("settled work attempt is missing");
		const tornRuntime = { ...runtime, settledAt: "2026-09-08T00:00:20.000Z" };
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = ?, settled_at = ? WHERE op_ref = ?")
			.run(JSON.stringify(tornRuntime), "2026-09-08T00:00:30.000Z", runtime.opRef);

		expect(() => f.database.work.workAttemptGet(runtime.opRef)).toThrow(WorkAttemptStateError);
		expect(f.database.reconcileWorkAttempts(() => {})).toBe(0);
		expect(f.database.bindings.isQuarantined("work", runtime.jobId)).toBe(true);
	});

	test("reconciliation quarantines conflicting runtime and history timestamps with a NULL SQL settlement", async () => {
		const f = await fixture();
		const runtimeSettledAt = "2026-09-08T00:00:20.000Z";
		const historySettledAt = "2026-09-08T00:00:30.000Z";
		const runtime: WorkAttemptRuntime = {
			...f.runtime,
			terminal: { kind: "local", observedAt: runtimeSettledAt, reasonCode: "recovery_indeterminate" },
			output: { ...f.runtime.output, disposition: "unavailable" },
			decision: "recovery_indeterminate",
			settledAt: runtimeSettledAt,
		};
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
			.run(JSON.stringify(runtime), runtime.opRef);
		const history = closeAttempt({
			record: f.record,
			opRef: runtime.opRef,
			endState: "terminal_uncertain",
			errorCode: "recovery_indeterminate",
			endedAt: historySettledAt,
		});
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, {
				...history,
				laneKey: runtime.laneKey,
				json: JSON.stringify(history),
			}),
		);

		expect(() => f.database.work.workAttemptGet(runtime.opRef)).toThrow(WorkAttemptStateError);
		expect(f.database.reconcileWorkAttempts(() => {})).toBe(0);
		expect(f.database.bindings.isQuarantined("work", runtime.jobId)).toBe(true);
		expect(f.database.inspectBrokerAuthority().openWork).toBe(0);
	});

	test("reconciliation quarantines settled attempts with mismatched history identity", async () => {
		for (const [field, value] of [
			["sessionId", "59bd7e80-cfbb-429a-b06b-bcf28cf75890"],
			["startedAt", "2026-09-08T00:00:01.000Z"],
		] as const) {
			const f = await fixture();
			f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
			);
			const historyJson = f.database.work.laneJobJson(f.runtime.jobId);
			if (historyJson === undefined) throw new Error("settled lane history is missing");
			const history = parseLaneJobRecord(historyJson);
			const corrupted = {
				...history,
				attempts: history.attempts.map((attempt) =>
					attempt.opRef === f.runtime.opRef ? { ...attempt, [field]: value } : attempt,
				),
			};
			f.raw
				.query("UPDATE lane_jobs SET record_json = ? WHERE job_id = ?")
				.run(JSON.stringify(corrupted), f.runtime.jobId);

			expect(() => f.database.work.workAttemptGet(f.runtime.opRef)).toThrow(WorkAttemptStateError);
			expect(f.database.reconcileWorkAttempts(() => {})).toBe(0);
			expect(f.database.bindings.isQuarantined("work", f.runtime.jobId)).toBe(true);
			expect(f.database.inspectBrokerAuthority().openWork).toBe(0);
		}
	});

	test("reconciliation quarantines settled attempts with mismatched top-level history identity", async () => {
		for (const field of ["jobId", "worktreePath"] as const) {
			const f = await fixture();
			f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
			);
			const historyJson = f.database.work.laneJobJson(f.runtime.jobId);
			if (historyJson === undefined) throw new Error("settled lane history is missing");
			const history = parseLaneJobRecord(historyJson);
			const corrupted =
				field === "jobId"
					? { ...history, jobId: "lanejob-62" }
					: { ...history, lane: { ...history.lane, worktreePath: "/other" } };
			f.raw
				.query("UPDATE lane_jobs SET record_json = ? WHERE job_id = ?")
				.run(JSON.stringify(corrupted), f.runtime.jobId);

			expect(() => f.database.work.workAttemptGet(f.runtime.opRef)).toThrow(WorkAttemptStateError);
			expect(f.database.reconcileWorkAttempts(() => {})).toBe(0);
			expect(f.database.bindings.isQuarantined("work", f.runtime.jobId)).toBe(true);
		}
	});

	test("reconciliation settles a stale database column without replaying the open attempt", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.raw.query("UPDATE work_attempt_runtime SET settled_at = ? WHERE op_ref = ?").run(END, f.runtime.opRef);

		const reconciled = f.database.reconcileWorkAttempts();
		expect(reconciled).toBe(1);
		expect(f.database.work.workAttemptGet(f.runtime.opRef)).toMatchObject({
			decision: "recovery_indeterminate",
			settledAt: END,
			terminal: { reasonCode: "recovery_indeterminate" },
		});
		const historyJson = f.database.work.laneJobJson(f.runtime.jobId);
		expect(historyJson).toBeDefined();
		if (historyJson === undefined) throw new Error("reconciled lane history is missing");
		expect(parseLaneJobRecord(historyJson).attempts[0]?.endedAt).toBe(END);
		expect(f.database.work.workAttemptOpen()).toEqual([]);
	});

	test("reconciliation repairs a settled runtime whose lane history is still open", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const runtime: WorkAttemptRuntime = {
			...f.runtime,
			terminal: { kind: "local", observedAt: END, reasonCode: "recovery_indeterminate" },
			output: { ...f.runtime.output, disposition: "unavailable" },
			decision: "recovery_indeterminate",
			settledAt: END,
		};
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = ?, settled_at = ? WHERE op_ref = ?")
			.run(JSON.stringify(runtime), END, runtime.opRef);

		expect(() => f.database.work.workAttemptGet(runtime.opRef)).toThrow(WorkAttemptStateError);
		const reconciled = f.database.reconcileWorkAttempts(() => {});

		expect(reconciled).toBe(1);
		expect(f.database.work.workAttemptGet(runtime.opRef)).toMatchObject({
			decision: "recovery_indeterminate",
			settledAt: END,
		});
		const historyJson = f.database.work.laneJobJson(runtime.jobId);
		expect(historyJson).toBeDefined();
		if (historyJson === undefined) throw new Error("reconciled lane history is missing");
		expect(parseLaneJobRecord(historyJson).attempts[0]?.endedAt).toBe(END);
	});

	test("reconciliation preserves a definitive completion when lane history is open", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.database.transaction((tx) =>
			settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
		);
		f.raw
			.query("UPDATE lane_jobs SET state = ?, record_json = ?, updated_at = ? WHERE job_id = ?")
			.run(f.record.state, JSON.stringify(f.record), f.record.updatedAt, f.runtime.jobId);

		expect(() => f.database.work.workAttemptGet(f.runtime.opRef)).toThrow(WorkAttemptStateError);
		expect(f.database.reconcileWorkAttempts(() => {})).toBe(1);
		const historyJson = f.database.work.laneJobJson(f.runtime.jobId);
		expect(historyJson).toBeDefined();
		if (historyJson === undefined) throw new Error("reconciled lane history is missing");
		expect(parseLaneJobRecord(historyJson)).toMatchObject({
			state: "attempt_ended",
			attempts: [{ endedAt: END, endState: "completed" }],
		});
	});

	test("reconciliation quarantines invalid settled candidates", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.database.transaction((tx) =>
			settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
		);
		f.raw.query("UPDATE work_attempt_runtime SET version = version + 1 WHERE op_ref = ?").run(f.runtime.opRef);

		expect(f.database.reconcileWorkAttempts(() => {})).toBe(0);
		expect(f.database.bindings.isQuarantined("work", f.runtime.jobId)).toBe(true);
		expect(f.database.work.laneParentAvailable("a")).toBe(false);
	});

	test("reconciliation quarantines only the job whose row indicates a problem", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.database.transaction((tx) =>
			settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
		);
		const unrelated = createLaneJobRecord({
			jobId: "lanejob-62",
			branch: "main",
			worktreePath: "/other",
			sessionId: SESSION,
			now: () => new Date(START),
		});
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, {
				...unrelated,
				laneKey: "work-b",
				json: JSON.stringify(unrelated),
			}),
		);
		f.raw.query("UPDATE work_attempt_runtime SET job_id = ? WHERE op_ref = ?").run(unrelated.jobId, f.runtime.opRef);

		expect(f.database.reconcileWorkAttempts(() => {})).toBe(0);
		expect(f.database.bindings.isQuarantined("work", f.runtime.jobId)).toBe(false);
		expect(f.database.bindings.isQuarantined("work", unrelated.jobId)).toBe(true);
	});

	test("quarantining a settled candidate skips other repairs in the same lane", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.database.transaction((tx) =>
			settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
		);
		const secondOpRef = "gw-work-store-b";
		const secondStartedAt = "2026-09-08T00:00:20.000Z";
		const secondSettledAt = "2026-09-08T00:00:30.000Z";
		const secondRecord = appendAttempt(f.closed, {
			opRef: secondOpRef,
			sessionId: SESSION,
			startedAt: secondStartedAt,
		});
		const secondRuntime: WorkAttemptRuntime = {
			...f.runtime,
			opRef: secondOpRef,
			startedAt: secondStartedAt,
			reportId: workAttemptReportId(f.database.instanceId, f.runtime.jobId, secondOpRef),
			deliveryId: workAttemptDeliveryId(f.database.instanceId, f.runtime.jobId, secondOpRef),
		};
		f.database.transaction((tx) => prepareAttempt(f.database, tx, secondRuntime, secondRecord));
		const tornSettledRuntime: WorkAttemptRuntime = {
			...secondRuntime,
			terminal: { kind: "local", observedAt: secondSettledAt, reasonCode: "recovery_indeterminate" },
			output: { ...secondRuntime.output, disposition: "unavailable" },
			decision: "recovery_indeterminate",
			settledAt: secondSettledAt,
		};
		f.raw
			.query("UPDATE work_attempt_runtime SET record_json = ?, settled_at = ?, version = version + 1 WHERE op_ref = ?")
			.run(JSON.stringify(tornSettledRuntime), secondSettledAt, secondOpRef);
		f.raw.query("UPDATE work_attempt_runtime SET settled_at = NULL WHERE op_ref = ?").run(f.runtime.opRef);

		expect(f.database.reconcileWorkAttempts(() => {})).toBe(0);
		expect(f.database.bindings.isQuarantined("work", f.runtime.jobId)).toBe(true);
		const first = f.raw.query("SELECT settled_at FROM work_attempt_runtime WHERE op_ref = ?").get(f.runtime.opRef) as {
			settled_at: string | null;
		};
		expect(first.settled_at).toBeNull();
	});

	test("reconciliation preserves every repaired attempt in a shared lane history", async () => {
		const f = await fixture();
		const secondOpRef = "gw-work-store-b";
		const secondStartedAt = "2026-09-08T00:00:20.000Z";
		const secondEndedAt = "2026-09-08T00:00:30.000Z";
		const firstReconciledAt = "2026-09-08T00:00:11.000Z";
		const secondReconciledAt = "2026-09-08T00:00:31.000Z";
		const firstHistory = closeAttempt({
			record: f.record,
			opRef: f.runtime.opRef,
			endState: "terminal_uncertain",
			errorCode: "recovery_indeterminate",
			endedAt: END,
		});
		const secondRecord = appendAttempt(firstHistory, {
			opRef: secondOpRef,
			sessionId: SESSION,
			startedAt: secondStartedAt,
		});
		const secondHistory = closeAttempt({
			record: secondRecord,
			opRef: secondOpRef,
			endState: "terminal_uncertain",
			errorCode: "recovery_indeterminate",
			endedAt: secondEndedAt,
		});
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, {
				...secondHistory,
				laneKey: f.runtime.laneKey,
				json: JSON.stringify(secondHistory),
			}),
		);
		const tornRuntime = (opRef: string, startedAt: string, settledAt: string): WorkAttemptRuntime => ({
			...f.runtime,
			opRef,
			startedAt,
			terminal: { kind: "local", observedAt: settledAt, reasonCode: "recovery_indeterminate" },
			output: { ...f.runtime.output, disposition: "unavailable" },
			reportId: workAttemptReportId(f.database.instanceId, f.runtime.jobId, opRef),
			deliveryId: workAttemptDeliveryId(f.database.instanceId, f.runtime.jobId, opRef),
			decision: "recovery_indeterminate",
			settledAt,
		});
		for (const [runtime, columnSettledAt] of [
			[tornRuntime(f.runtime.opRef, START, firstReconciledAt), END],
			[tornRuntime(secondOpRef, secondStartedAt, secondReconciledAt), secondEndedAt],
		] as const)
			f.raw
				.query(
					"INSERT INTO work_attempt_runtime (op_ref, job_id, lane_key, session_id, version, settled_at, delivery_id, record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
				)
				.run(
					runtime.opRef,
					runtime.jobId,
					runtime.laneKey,
					runtime.sessionId,
					runtime.version,
					columnSettledAt,
					runtime.deliveryId,
					JSON.stringify(runtime),
				);

		const reconciled = f.database.reconcileWorkAttempts(() => {});
		expect(reconciled).toBe(2);
		expect(f.database.work.workAttemptGet(f.runtime.opRef)?.settledAt).toBe(END);
		expect(f.database.work.workAttemptGet(secondOpRef)?.settledAt).toBe(secondEndedAt);
		const historyJson = f.database.work.laneJobJson(f.runtime.jobId);
		expect(historyJson).toBeDefined();
		if (historyJson === undefined) throw new Error("reconciled lane history is missing");
		expect(parseLaneJobRecord(historyJson).attempts.map((attempt) => attempt.endedAt)).toEqual([END, secondEndedAt]);
		expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(secondEndedAt);
	});

	for (const parentSessionExists of [false, true]) {
		test(`reconciliation reports an indeterminate persona child when parent session exists=${parentSessionExists}`, async () => {
			const f = await fixture();
			const parentOriginKey = originKey(LOOPBACK_ORIGIN);
			if (parentSessionExists)
				expect(
					f.database.transaction((tx) =>
						f.database.bindings.recordOwnedBinding(tx, {
							authority: f.authority,
							sessionId: PARENT_SESSION,
							originKey: parentOriginKey,
							epoch: 0,
							repo: "/work",
						}),
					),
				).toBe(true);
			f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
			f.database.transaction((tx) =>
				f.database.work.putLaneJob(tx, {
					...f.closed,
					laneKey: f.runtime.laneKey,
					json: JSON.stringify(f.closed),
				}),
			);

			expect(f.database.reconcileWorkAttempts(() => {})).toBe(1);
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toMatchObject({
				decision: "recovery_indeterminate",
				settledAt: END,
			});
			if (parentSessionExists) {
				expect(f.database.inbox.messageExists(f.runtime.reportId)).toBe(true);
				expect(f.database.outbox.rows()).toEqual([]);
			} else {
				const fallback = f.database.outbox.rows()[0];
				expect(fallback).toMatchObject({ delivery_id: f.runtime.deliveryId, origin_key: parentOriginKey });
				expect(JSON.parse(fallback?.payload_json ?? "{}")).toMatchObject({
					turnId: f.runtime.opRef,
					text: expect.stringContaining("recovery_indeterminate"),
				});
			}
		});
	}

	for (const parentAvailable of [false, true]) {
		test(`reconciliation reports an indeterminate lane child when parent is available=${parentAvailable}`, async () => {
			const f = await fixture();
			const parentName = parentAvailable ? "parent" : "missing-parent";
			const root = parentAvailable ? null : { originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN };
			const runtime: WorkAttemptRuntime = {
				...f.runtime,
				parent: { kind: "lane", name: parentName, root },
			};
			if (parentAvailable) {
				const parentJob = createLaneJobRecord({
					jobId: `lanejob-${Buffer.from(parentName, "utf8").toString("hex")}`,
					branch: "main",
					worktreePath: "/parent",
					sessionId: SESSION,
					now: () => new Date(START),
				});
				f.database.transaction((tx) =>
					f.database.work.putLaneJob(tx, {
						...parentJob,
						laneKey: `work-${parentName}`,
						json: JSON.stringify(parentJob),
					}),
				);
			}
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			f.database.transaction((tx) =>
				f.database.work.putLaneJob(tx, {
					...f.closed,
					laneKey: runtime.laneKey,
					json: JSON.stringify(f.closed),
				}),
			);

			expect(f.database.reconcileWorkAttempts(() => {})).toBe(1);
			if (parentAvailable) {
				expect(f.database.work.laneReportsByParent(parentName)).toMatchObject([
					{
						report_id: runtime.reportId,
						child_op_ref: runtime.opRef,
						state: "pending",
						body: expect.stringContaining("recovery_indeterminate"),
					},
				]);
				expect(f.database.outbox.rows()).toEqual([]);
			} else {
				expect(f.database.work.laneReportsByParent(parentName)).toEqual([]);
				expect(f.database.outbox.rows()).toMatchObject([
					{ delivery_id: runtime.deliveryId, origin_key: originKey(LOOPBACK_ORIGIN) },
				]);
			}
		});
	}

	test("boot heals a torn work-history settlement before normal recovery starts", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, {
				...f.closed,
				laneKey: f.runtime.laneKey,
				json: JSON.stringify(f.closed),
			}),
		);
		await mkdir(f.authority.canonicalAgentDir, { recursive: true });
		await Bun.write(
			join(f.directory, "config.json"),
			JSON.stringify({
				schemaVersion: 1,
				dbPath: f.path,
				gjc: { agentDir: f.authority.canonicalAgentDir },
			}),
		);
		const server = await bootGateway({
			home: f.directory,
			broker: {
				executable: "/test-only/gjc",
				command: async (args) => ({
					exitCode: 0,
					stdout:
						args[0] === "--version"
							? `gjc/${readPinnedGjcVersion()}\n`
							: JSON.stringify({ ok: true, result: { sessions: [] } }),
					stderr: "",
				}),
				healthProbe: async () => true,
				discovery: async () => ({
					pid: 1,
					url: "ws://127.0.0.1:1",
					token: "test-only",
					heartbeatAt: Date.now(),
				}),
				healthIntervalMs: 60_000,
				log: () => {},
			},
		});
		try {
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toMatchObject({
				decision: "recovery_indeterminate",
				settledAt: END,
				terminal: { reasonCode: "recovery_indeterminate" },
			});
			expect(f.database.work.workAttemptOpen()).toEqual([]);
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(END);
		} finally {
			await server.stop("test shutdown");
		}
	});

	test("a torn unaccepted wake is held for operator recovery instead of discarded", async () => {
		const f = await fixture();
		const wakeReportId = seedLinkedWakeReport(f, null);
		const runtime = { ...f.runtime, parent: null, wakeReportId };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		const history = closeAttempt({
			record: f.record,
			opRef: runtime.opRef,
			endState: "terminal_uncertain",
			errorCode: "recovery_indeterminate",
			endedAt: END,
		});
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...history, laneKey: runtime.laneKey, json: JSON.stringify(history) }),
		);

		const lines: string[] = [];
		const reconciled = f.database.reconcileWorkAttempts((line) => lines.push(line));
		expect(reconciled).toBe(1);
		expect(f.database.work.workAttemptGet(runtime.opRef)).toMatchObject({
			decision: "wake_unaccepted",
			settledAt: END,
			terminal: { reasonCode: "recovery_indeterminate" },
		});
		expect(f.database.work.laneReportGet(wakeReportId)).toMatchObject({
			state: "held",
			hold_reason: "wake_acceptance_uncertain",
			claim_ref: runtime.opRef,
		});
		expect(f.database.work.workAttemptOpen()).toEqual([]);
		expect(f.database.outbox.rows()).toEqual([]);
		expect(lines).toEqual(["work_attempt_runtime_reconciled count=1"]);
	});

	for (const scenario of [
		{ evidence: "output proof", disposition: "available", decision: "recovery_indeterminate" },
		{ evidence: "known silence", disposition: "silent", decision: "suppressed" },
	] as const) {
		test(`reconciliation preserves ${scenario.evidence} with a linked wake report`, async () => {
			const f = await fixture();
			const wakeReportId = seedLinkedWakeReport(f, null);
			const runtime = { ...f.runtime, parent: null, wakeReportId };
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
				byteLength: scenario.evidence === "known silence" ? 0 : 15,
			};
			const output =
				scenario.evidence === "known silence"
					? { ...runtime.output, disposition: "silent" as const, knownSilence: proof }
					: { ...runtime.output, disposition: "available" as const, excerpt: "verified output", proof };
			const updated = f.database.transaction((tx) =>
				f.database.work.workAttemptUpdate(tx, runtime.opRef, 0, {
					terminal: { kind: "local", observedAt: END, reasonCode: "end_turn" },
					output,
				}),
			);
			expect(updated).toBeDefined();
			if (updated === undefined) throw new Error("durable output evidence was not saved");
			f.database.transaction((tx) =>
				f.database.work.putLaneJob(tx, {
					...f.closed,
					laneKey: runtime.laneKey,
					json: JSON.stringify(f.closed),
				}),
			);

			const reconciled = f.database.reconcileWorkAttempts(() => {});

			expect(reconciled).toBe(1);
			expect(f.database.work.workAttemptGet(runtime.opRef)).toMatchObject({
				decision: scenario.decision,
				settledAt: END,
				output: { disposition: scenario.disposition },
			});
			expect(f.database.work.laneReportGet(wakeReportId)).toMatchObject({
				state: "consumed",
				consumed_op_ref: runtime.opRef,
			});
			expect(f.database.work.workAttemptOpen()).toEqual([]);
		});
	}

	for (const scenario of [
		{ state: "pending", parentHeld: false, root: null },
		{
			state: "fallback",
			parentHeld: true,
			root: { originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN },
		},
		{ state: "undeliverable", parentHeld: true, root: null },
	] as const) {
		test(`reconciliation preserves an already-settled wake report in ${scenario.state}`, async () => {
			const f = await fixture();
			const wakeReportId = seedLinkedWakeReport(f, scenario.root);
			const runtime = { ...f.runtime, parent: null, wakeReportId };
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			const record = scenario.parentHeld
				? closeAttempt({ record: f.record, opRef: runtime.opRef, endState: "terminal_uncertain", endedAt: END })
				: f.closed;
			const settlement: WorkAttemptSettlement = {
				terminal: { kind: "local", observedAt: END, reasonCode: "send_rejected" },
				output: { ...runtime.output, disposition: "unavailable" },
				decision: "wake_unaccepted",
				settledAt: END,
			};
			f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, record, settlement));
			const deliveries = f.database.outbox.rows();
			expect(f.database.work.laneReportGet(wakeReportId)?.state).toBe(scenario.state);

			f.raw.query("UPDATE work_attempt_runtime SET settled_at = NULL WHERE op_ref = ?").run(runtime.opRef);
			const reconciled = f.database.reconcileWorkAttempts(() => {});

			expect(reconciled).toBe(1);
			expect(f.database.work.workAttemptGet(runtime.opRef)).toMatchObject({
				decision: "wake_unaccepted",
				settledAt: END,
			});
			expect(f.database.work.laneReportGet(wakeReportId)?.state).toBe(scenario.state);
			expect(f.database.outbox.rows()).toEqual(deliveries);
			expect(f.database.work.workAttemptOpen()).toEqual([]);
		});
	}

	for (const scenario of [
		{ state: "pending", reportState: "claimed", parentHeld: false, root: null },
		{
			state: "fallback",
			reportState: "held",
			parentHeld: true,
			root: { originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN },
		},
		{ state: "undeliverable", reportState: "claimed", parentHeld: true, root: null },
	] as const) {
		test(`reconciliation settles a definitively rejected wake report in ${scenario.state}`, async () => {
			const f = await fixture();
			const wakeReportId = seedLinkedWakeReport(f, scenario.root);
			const runtime = { ...f.runtime, parent: null, wakeReportId };
			const record = scenario.parentHeld
				? closeAttempt({ record: f.record, opRef: runtime.opRef, endState: "terminal_uncertain", endedAt: END })
				: f.closed;
			const settlement: WorkAttemptSettlement = {
				terminal: { kind: "local", observedAt: END, reasonCode: "send_rejected" },
				output: { ...runtime.output, disposition: "unavailable" },
				decision: "wake_unaccepted",
				settledAt: END,
			};
			f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
			f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, record, settlement));
			f.raw
				.query(
					"UPDATE lane_reports SET state = ?, claim_kind = 'wake', claim_ref = ?, claim_target_op_ref = NULL, hold_reason = ?, consumed_op_ref = NULL WHERE report_id = ?",
				)
				.run(
					scenario.reportState,
					runtime.opRef,
					scenario.reportState === "held" ? "wake_acceptance_uncertain" : null,
					wakeReportId,
				);
			f.raw.query("UPDATE work_attempt_runtime SET settled_at = NULL WHERE op_ref = ?").run(runtime.opRef);
			if (scenario.state === "fallback") {
				const delivery = f.database.outbox.rows()[0];
				expect(delivery).toBeDefined();
				if (!delivery) throw new Error("expected the settled fallback delivery");
				f.raw.query("DELETE FROM deliveries WHERE delivery_id = ?").run(delivery.delivery_id);
			}

			const reconciled = f.database.reconcileWorkAttempts(() => {});

			expect(reconciled).toBe(1);
			expect(f.database.work.laneReportGet(wakeReportId)).toMatchObject({ state: scenario.state });
			expect(f.database.work.workAttemptGet(runtime.opRef)).toMatchObject({
				decision: "wake_unaccepted",
				settledAt: END,
			});
			expect(f.database.outbox.rows()).toHaveLength(scenario.state === "fallback" ? 1 : 0);
		});
	}

	for (const scenario of [
		{ state: "pending", parentHeld: false, root: null },
		{
			state: "fallback",
			parentHeld: true,
			root: { originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN },
		},
	] as const) {
		test(`reconciliation completes a rejected wake as ${scenario.state}`, async () => {
			const f = await fixture();
			const wakeReportId = seedLinkedWakeReport(f, scenario.root);
			const openRuntime = { ...f.runtime, parent: null, wakeReportId };
			f.database.transaction((tx) => prepareAttempt(f.database, tx, openRuntime, f.record));
			const history = scenario.parentHeld
				? closeAttempt({
						record: f.record,
						opRef: openRuntime.opRef,
						endState: "terminal_uncertain",
						errorCode: "recovery_indeterminate",
						endedAt: END,
					})
				: f.closed;
			f.database.transaction((tx) =>
				f.database.work.putLaneJob(tx, {
					...history,
					laneKey: openRuntime.laneKey,
					json: JSON.stringify(history),
				}),
			);
			const settledRuntime: WorkAttemptRuntime = {
				...openRuntime,
				terminal: { kind: "local", observedAt: END, reasonCode: "send_rejected" },
				output: { ...openRuntime.output, disposition: "unavailable" },
				decision: "wake_unaccepted",
				settledAt: END,
			};
			f.raw
				.query("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
				.run(JSON.stringify(settledRuntime), openRuntime.opRef);
			expect(f.database.work.laneReportGet(wakeReportId)?.state).toBe("claimed");

			const reconciled = f.database.reconcileWorkAttempts(() => {});

			expect(reconciled).toBe(1);
			expect(f.database.work.laneReportGet(wakeReportId)).toMatchObject({ state: scenario.state });
			const deliveries = f.database.outbox.rows();
			expect(deliveries).toHaveLength(scenario.state === "fallback" ? 1 : 0);
			if (scenario.state === "fallback") {
				const delivery = deliveries[0];
				expect(delivery).toBeDefined();
				if (delivery === undefined) throw new Error("reconciled wake fallback delivery is missing");
				expect(JSON.parse(delivery.payload_json)).toMatchObject({ text: "[lane child] completed: child output" });
			}
			expect(f.database.reconcileWorkAttempts(() => {})).toBe(0);
			expect(f.database.outbox.rows()).toEqual(deliveries);
		});
	}
});

describe("work attempt durable transactions", () => {
	for (const mode of ["start", "run"] as const) {
		test(`${mode} publishes worker metadata and activity atomically at prepare and settlement`, async () => {
			const f = await fixture();
			const runtime = { ...f.runtime, mode, parent: mode === "start" ? f.runtime.parent : null };
			const settlement = { ...f.settlement, decision: mode === "start" ? ("report" as const) : ("no_target" as const) };
			const admission = mode === "start" ? f.admission : undefined;
			const expectedDecision = mode === "start" ? "fallback" : "no_target";
			const identity = () =>
				f.database.bindings.sessionIdentityRows().find((row) => row.origin_key === runtime.sessionKey);
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
			expect(() =>
				f.database.transaction((tx) =>
					settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, settlement, admission),
				),
			).toThrow();
			expect(identity()).toMatchObject({ origin_ref_json: null, last_activity_at: START });
			expect(f.database.work.workAttemptGet(runtime.opRef)?.decision).toBe("undecided");
			expect(f.database.outbox.rows()).toHaveLength(0);
			f.raw.exec("DROP TRIGGER fault");
			expect(
				f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, settlement, admission))
					?.runtime.decision,
			).toBe(expectedDecision);
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
				f.database.transaction((tx) =>
					settleAttempt(
						f.database,
						tx,
						runtime.opRef,
						0,
						f.closed,
						{ ...f.settlement, decision: requested },
						mode === "start" ? f.admission : undefined,
					),
				)?.runtime.decision,
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
			expect(() =>
				f.database.transaction((tx) =>
					settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
				),
			).toThrow();
			expect(f.database.work.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
			expect(parseLaneJobRecord(f.database.work.laneJobJson(f.runtime.jobId)!).attempts[0]?.endedAt).toBeUndefined();
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(START);
			expect(f.database.outbox.rows()).toHaveLength(0);
			f.raw.exec("DROP TRIGGER fault");
			expect(
				f.database.transaction((tx) =>
					settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
				)?.runtime.decision,
			).toBe("fallback");
			expect(f.database.outbox.rows()).toHaveLength(1);
			expect(f.database.bindings.workLaneRows()[0]?.last_activity_at).toBe(END);
		});
	}

	test("CAS refuses stale settlement and duplicate decisions; pruning does not regenerate", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const accepted = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, f.runtime.opRef, 0, {
				sendPhase: "accepted",
				sendEvidence: { source: "receipt", observedAt: START },
			}),
		);
		expect(accepted?.version).toBe(1);
		expect(
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
			),
		).toBeUndefined();
		expect(f.database.outbox.rows()).toHaveLength(0);
		expect(
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, f.runtime.opRef, 1, f.closed, f.settlement, f.admission),
			)?.runtime.version,
		).toBe(2);
		const ledger = new DeliveryService(f.database);
		expect(ledger.confirm(f.runtime.deliveryId)).toBe("transitioned");
		expect(ledger.prune(0, Date.now() + 1000)).toBe(1);
		const reopened = await Store.open(f.path);
		handles.push(reopened);
		expect(
			reopened.transaction((tx) =>
				settleAttempt(reopened, tx, f.runtime.opRef, 2, f.closed, f.settlement, f.admission),
			),
		).toBeUndefined();
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
		expect(() =>
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
			),
		).toThrow();
		expect(f.database.work.workAttemptGet(f.runtime.opRef)?.decision).toBe("undecided");
		expect(f.database.outbox.rows()[0]?.turn_id).toBe("other-turn");
	});

	test("same-transaction delivery seam requires ownership, suppresses silence and checks duplicate payloads", async () => {
		const f = await fixture();
		const delivery = new DeliveryService(f.database);
		expect(() => delivery.persistInTransaction(undefined as never, f.payload)).toThrow("inactive_tx");
		expect(f.database.transaction((tx) => delivery.persistInTransaction(tx, f.payload))).toBe(true);
		expect(f.database.transaction((tx) => delivery.persistInTransaction(tx, f.payload))).toBe(false);
		expect(() =>
			f.database.transaction((tx) => delivery.persistInTransaction(tx, { ...f.payload, text: "changed" })),
		).toThrow();
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
		expect(f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, { output }))?.version).toBe(
			1,
		);
		expect(
			f.database.transaction((tx) => updateAttempt(f.database, tx, f.runtime.opRef, 0, { output })),
		).toBeUndefined();
		const reopened = await Store.open(f.path);
		handles.push(reopened);
		expect(reopened.work.workAttemptGet(f.runtime.opRef)?.output.knownSilence).toEqual(proof);
		expect(() =>
			reopened.transaction((tx) => updateAttempt(reopened, tx, f.runtime.opRef, 1, { output: f.runtime.output })),
		).toThrow();
		const suppressed = { ...f.settlement, output, decision: "suppressed" as const };
		expect(
			reopened.transaction((tx) => settleAttempt(reopened, tx, f.runtime.opRef, 1, f.closed, suppressed))?.runtime
				.decision,
		).toBe("suppressed");
		expect(
			reopened.transaction((tx) => settleAttempt(reopened, tx, f.runtime.opRef, 2, f.closed, suppressed)),
		).toBeUndefined();
		expect(reopened.outbox.rows()).toHaveLength(0);
	});

	test("output claims persist three-read budget; stale/wrong proof and oversized excerpt fail closed", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		let runtime = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, f.runtime.opRef, 0, { terminal: f.settlement.terminal }),
		)!;
		for (let reads = 1; reads <= 3; reads++)
			runtime = f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
					output: { ...runtime.output, reads, nextReadAt: END },
				}),
			)!;
		expect(runtime.output.reads).toBe(3);
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, { output: { ...runtime.output, reads: 4 } }),
			),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, { output: { ...runtime.output, reads: 0 } }),
			),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
					output: { ...runtime.output, excerpt: "界".repeat(1000) },
				}),
			),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
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
				}),
			),
		).toThrow();
	});

	test("terminal evidence is write-once except a proven missing-to-present receipt (#248)", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const missing = {
			kind: "broker" as const,
			observedAt: END,
			reasonCode: "terminal_missing_receipt",
			status: {
				status: "terminal_ok" as const,
				receiptState: "missing" as const,
				clientRef: f.runtime.opRef,
				terminalAt: Date.parse(END),
				outcome: { reason: "end_turn" },
			},
		};
		const present = {
			...missing,
			reasonCode: "end_turn",
			status: { ...missing.status, receiptState: "present" as const },
		};
		const runtime = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, f.runtime.opRef, 0, { terminal: missing }),
		)!;
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
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, { terminal: present }),
			),
		).toThrow();
		// Any other terminal rewrite remains forbidden, even with a proven body.
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
					terminal: { ...present, status: { ...present.status, outcome: { reason: "refusal" } } },
					output: available,
				}),
			),
		).toThrow();
		const upgraded = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, runtime.opRef, runtime.version, {
				terminal: present,
				output: available,
			}),
		)!;
		expect(upgraded.terminal).toEqual(present);
		// present never regresses to missing.
		expect(() =>
			f.database.transaction((tx) =>
				updateAttempt(f.database, tx, upgraded.opRef, upgraded.version, { terminal: missing }),
			),
		).toThrow();
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
		expect(() =>
			f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, mode: "run" }, f.record)),
		).toThrow();
		const runtime = { ...f.runtime, mode: "run" as const, parent: null };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		expect(() =>
			f.database.transaction((tx) => updateAttempt(f.database, tx, runtime.opRef, 0, f.settlement)),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, f.settlement, f.admission),
			),
		).toThrow();
		const patch = { ...f.settlement, decision: "no_target" as const };
		expect(() =>
			f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, patch, f.admission)),
		).toThrow();
		expect(
			f.database.transaction((tx) => settleAttempt(f.database, tx, runtime.opRef, 0, f.closed, patch))?.runtime
				.decision,
		).toBe("no_target");
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
			const staged = f.database.transaction((tx) =>
				updateAttempt(f.database, tx, runtime.opRef, 0, {
					terminal,
					output: { ...runtime.output, disposition: "unavailable" },
				}),
			)!;
			const closed = closeAttempt({
				record: f.record,
				opRef: runtime.opRef,
				endState: reasonCode === "send_rejected" ? "failed" : "terminal_uncertain",
				errorCode: reasonCode,
				endedAt: END,
			});
			const result = f.database.transaction((tx) =>
				settleAttempt(f.database, tx, staged.opRef, staged.version, closed, {
					terminal,
					output: staged.output,
					decision: "wake_unaccepted",
					settledAt: END,
				}),
			);
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
			f.database.transaction((tx) =>
				f.database.bindings.recordOwnedBinding(tx, {
					authority: f.authority,
					sessionId: personaSessionId,
					originKey: root.originKey,
					epoch: 0,
					repo: "/work",
				}),
			),
		).toBe(true);
		const runtime = { ...f.runtime, wakeReportId: linkedId };
		f.database.transaction((tx) => prepareAttempt(f.database, tx, runtime, f.record));
		const accepted = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, runtime.opRef, 0, {
				sendPhase: "accepted",
				sendEvidence: { source: "receipt", observedAt: START },
			}),
		)!;
		expect(f.database.work.laneReportGet(linkedId)?.state).toBe("consumed");
		const result = f.database.transaction((tx) =>
			settleAttempt(f.database, tx, accepted.opRef, accepted.version, f.closed, f.settlement, f.admission),
		)!;
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
		f.database.transaction((tx) =>
			f.database.bindings.recordOwnedBinding(tx, {
				authority: f.authority,
				sessionId: personaSessionId,
				originKey: root.originKey,
				epoch: 0,
				repo: "/work",
			}),
		);
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
		const accepted = f.database.transaction((tx) =>
			updateAttempt(f.database, tx, runtime.opRef, 0, { output: provenOutput }),
		)!;
		expect(accepted.sendPhase).toBe("prepared");
		expect(f.database.work.laneReportGet(linkedId)?.state).toBe("consumed");
		const patch = {
			...f.settlement,
			output: { ...provenOutput, disposition: "unavailable" as const, excerpt: null },
			decision: "report" as const,
		};
		const result = f.database.transaction((tx) =>
			settleAttempt(f.database, tx, accepted.opRef, accepted.version, f.closed, patch, f.admission),
		)!;
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
		expect(() =>
			f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, epoch: 1 }, f.record)),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) => prepareAttempt(f.database, tx, { ...f.runtime, cwd: "/different" }, f.record)),
		).toThrow();
		expect(() =>
			f.database.transaction((tx) =>
				prepareAttempt(f.database, tx, { ...f.runtime, deliveryId: "arbitrary" }, f.record),
			),
		).toThrow();
		expect(f.database.work.workAttemptOpen()).toHaveLength(0);
		expect(f.database.work.laneJobJson(f.runtime.jobId)).toBeUndefined();
	});

	test("v24 migration rewrites a quarantined v23 runtime and restores quarantine triggers", async () => {
		const f = await fixture();
		f.database.transaction((tx) => prepareAttempt(f.database, tx, f.runtime, f.record));
		const settled = f.database.transaction((tx) =>
			settleAttempt(f.database, tx, f.runtime.opRef, 0, f.closed, f.settlement, f.admission),
		)!;
		expect(settled.runtime.decision).toBe("fallback");
		const targetAuthority = {
			canonicalAgentDir: join(f.path, "..", "target-agent"),
			identity: "target-broker",
		};
		f.database.transaction((tx) =>
			f.database.bindings.cutoverBrokerAuthority(tx, {
				expectedAuthority: f.authority,
				targetAuthority,
				evidence: "test v23 quarantine migration",
				disposition: "quarantine",
			}),
		);
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
		expect(migrated.schemaVersion).toBe(38);
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
	test("v35 migration adds awaiting_checks and restores lane-job quarantine triggers", async () => {
		const f = await fixture();
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...f.record, laneKey: f.runtime.laneKey, json: JSON.stringify(f.record) }),
		);
		f.raw.exec(`
CREATE TABLE lane_jobs_v34 (
	job_id TEXT PRIMARY KEY,
	lane_key TEXT NOT NULL UNIQUE,
	branch TEXT NOT NULL,
	worktree_path TEXT NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('running','attempt_ended','awaiting_operator','stalled','done','aborted')),
	record_json TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
INSERT INTO lane_jobs_v34 SELECT * FROM lane_jobs;
DROP TABLE lane_jobs;
ALTER TABLE lane_jobs_v34 RENAME TO lane_jobs;
CREATE TRIGGER lane_jobs_quarantine_update BEFORE UPDATE ON lane_jobs
	WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
	BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
CREATE TRIGGER lane_jobs_quarantine_delete BEFORE DELETE ON lane_jobs
	WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
	BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
INSERT INTO broker_quarantine (kind, subject_id, cutover_id) VALUES ('work', '${f.record.jobId}', 'cutover-v35');
DELETE FROM schema_migrations WHERE version > 34;
`);

		const migrated = await Store.open(f.path);
		handles.push(migrated);
		expect(migrated.schemaVersion).toBe(38);
		expect(migrated.work.laneJobJson(f.record.jobId)).toBe(JSON.stringify(f.record));
		expect(() =>
			f.raw.query("UPDATE lane_jobs SET updated_at = ? WHERE job_id = ?").run(END, f.record.jobId),
		).toThrow();
		expect(() => f.raw.query("DELETE FROM lane_jobs WHERE job_id = ?").run(f.record.jobId)).toThrow();
		const syntheticOpRef = "gw-checks-a-abababababab";
		f.raw
			.query(
				"INSERT INTO lane_reports (report_id, parent_name, child_name, child_op_ref, body, root_json, state, claim_kind, claim_ref, claim_target_op_ref, claim_seq, hold_reason, consumed_op_ref, created_at, updated_at) VALUES (?, ?, ?, ?, ?, NULL, 'pending', NULL, NULL, NULL, 0, NULL, NULL, ?, ?)",
			)
			.run(`lane-report-${"a".repeat(64)}`, "parent", "a", syntheticOpRef, "checks passed", END, END);
		expect(migrated.work.readOpenWorkRows("replayable").laneReports).toEqual([]);
		expect(migrated.work.readOpenWorkRows("quarantined").laneReports).toMatchObject([
			{ child_name: "a", cutover_id: "cutover-v35", state: "pending" },
		]);
		f.raw
			.query(
				"INSERT INTO lane_jobs (job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				"lanejob-awaiting-checks",
				"work/task/awaiting-checks",
				"fix/awaiting-checks",
				f.runtime.cwd,
				"awaiting_checks",
				"{}",
				END,
				END,
			);
	});
	test("v21 migration preserves historical open history and adopts without parent", async () => {
		const f = await fixture();
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, { ...f.record, laneKey: f.runtime.laneKey, json: JSON.stringify(f.record) }),
		);
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
		expect(migrated.schemaVersion).toBe(38);
		expect(migrated.work.laneJobJson(f.runtime.jobId)).toBe(JSON.stringify(f.record));
		const historical = { ...f.runtime, mode: "historical" as const, sendPhase: "uncertain" as const, parent: null };
		migrated.transaction((tx) => prepareAttempt(migrated, tx, historical, f.record));
		expect(migrated.work.workAttemptGet(f.runtime.opRef)?.parent).toBeNull();
		expect(
			migrated.transaction((tx) =>
				settleAttempt(migrated, tx, f.runtime.opRef, 0, f.closed, { ...f.settlement, decision: "no_target" }),
			)?.runtime.decision,
		).toBe("no_target");
		expect(migrated.outbox.rows()).toHaveLength(0);
	});
});
