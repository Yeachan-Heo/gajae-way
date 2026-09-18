import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN } from "@gajae-gateway/protocol";
import { appendAttempt, closeAttempt, createLaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import { buildDeliveryPayload, DeliveryService } from "../src/delivery/delivery";
import {
	GatewayDatabase,
	type WorkAttemptRuntime,
	type WorkAttemptSettlement,
	WorkAttemptStateError,
	workAttemptDeliveryId,
} from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

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
	const database = await GatewayDatabase.open(path);
	handles.push(database);
	const canonicalAgentDir = join(directory, "agent");
	const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
	database.assertBrokerAuthority(authority, { initializeEmpty: true });
	expect(
		database.recordOwnedBinding({ authority, sessionId: SESSION, originKey: "work/task/a", epoch: 0, repo: "/work" }),
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
		target: LOOPBACK_ORIGIN,
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
		decision: "enqueued",
		settledAt: END,
	};
	const payload = buildDeliveryPayload(
		runtime.opRef,
		LOOPBACK_ORIGIN,
		"[lane a] completed: output_unavailable",
		runtime.deliveryId,
	)!;
	return { path, database, raw, authority, record, runtime, closed, settlement, payload };
}

describe("work attempt durable transactions", () => {
	for (const mode of ["start", "run"] as const) {
		test(`${mode} publishes worker metadata and activity atomically at prepare and settlement`, async () => {
			const f = await fixture();
			const runtime = { ...f.runtime, mode, target: mode === "start" ? f.runtime.target : null };
			const settlement = {
				...f.settlement,
				decision: mode === "start" ? ("enqueued" as const) : ("no_target" as const),
			};
			const payload = mode === "start" ? f.payload : undefined;
			const identity = () => f.database.sessionIdentityRows().find((row) => row.origin_key === runtime.sessionKey);
			const workerOrigin = JSON.stringify({ platform: "work", kind: "task", conversationId: "a" });
			// An AFTER fault proves both assignments roll back even after the UPDATE executes.
			f.raw.exec("CREATE TRIGGER fault AFTER UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
			expect(() => f.database.workAttemptPrepare(runtime, f.record)).toThrow();
			expect(identity()).toMatchObject({ origin_ref_json: null, last_activity_at: null });
			expect(f.database.workAttemptGet(runtime.opRef)).toBeUndefined();
			expect(f.database.laneJobJson(runtime.jobId)).toBeUndefined();
			f.raw.exec("DROP TRIGGER fault");
			f.database.workAttemptPrepare(runtime, f.record);
			expect(identity()).toMatchObject({ origin_ref_json: workerOrigin, last_activity_at: START });
			// Simulate missing legacy metadata to prove settlement restores it too.
			f.raw.query("UPDATE sessions SET origin_ref_json = NULL WHERE origin_key = ?").run(runtime.sessionKey);
			f.raw.exec("CREATE TRIGGER fault AFTER UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
			expect(() => f.database.workAttemptSettle(runtime.opRef, 0, f.closed, settlement, payload)).toThrow();
			expect(identity()).toMatchObject({ origin_ref_json: null, last_activity_at: START });
			expect(f.database.workAttemptGet(runtime.opRef)?.decision).toBe("undecided");
			expect(f.database.deliveryRows()).toHaveLength(0);
			f.raw.exec("DROP TRIGGER fault");
			expect(f.database.workAttemptSettle(runtime.opRef, 0, f.closed, settlement, payload)?.decision).toBe(
				settlement.decision,
			);
			expect(identity()).toMatchObject({ origin_ref_json: workerOrigin, last_activity_at: END });
		});

		test(`${mode} stale epoch settlement cannot retag or refresh a successor binding`, async () => {
			const f = await fixture();
			const runtime = { ...f.runtime, mode, target: mode === "start" ? f.runtime.target : null };
			f.database.workAttemptPrepare(runtime, f.record);
			const successorOrigin = JSON.stringify(LOOPBACK_ORIGIN);
			// Keep sessionId identical: the epoch predicate must independently fence the write.
			f.raw
				.query("UPDATE sessions SET epoch = epoch + 1, origin_ref_json = ?, last_activity_at = ? WHERE origin_key = ?")
				.run(successorOrigin, START, runtime.sessionKey);
			const before = f.database.sessionIdentityRows();
			expect(() => f.database.workAttemptPrepare(runtime, f.record)).toThrow();
			const decision = mode === "start" ? ("enqueued" as const) : ("no_target" as const);
			expect(
				f.database.workAttemptSettle(
					runtime.opRef,
					0,
					f.closed,
					{ ...f.settlement, decision },
					mode === "start" ? f.payload : undefined,
				)?.decision,
			).toBe(decision);
			expect(f.database.sessionIdentityRows()).toEqual(before);
		});
	}
	for (const table of ["lane_jobs", "work_attempt_runtime", "sessions"]) {
		test(`prepare rolls back history/runtime/activity when ${table} write fails`, async () => {
			const f = await fixture();
			const action = table === "sessions" ? "UPDATE" : "INSERT";
			f.raw.exec(`CREATE TRIGGER fault BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'fault'); END`);
			expect(() => f.database.workAttemptPrepare(f.runtime, f.record)).toThrow();
			expect(f.database.laneJobJson(f.runtime.jobId)).toBeUndefined();
			expect(f.database.workAttemptGet(f.runtime.opRef)).toBeUndefined();
			expect(f.database.workLaneRows()[0]?.last_activity_at).toBeNull();
			f.raw.exec("DROP TRIGGER fault");
			f.database.workAttemptPrepare(f.runtime, f.record);
			expect(f.database.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
			expect(f.database.workLaneRows()[0]?.last_activity_at).toBe(START);
		});
	}

	for (const table of ["work_attempt_runtime", "lane_jobs", "sessions", "deliveries"]) {
		test(`settlement rolls back all effects when ${table} write fails`, async () => {
			const f = await fixture();
			f.database.workAttemptPrepare(f.runtime, f.record);
			const action = table === "deliveries" ? "INSERT" : "UPDATE";
			f.raw.exec(`CREATE TRIGGER fault BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'fault'); END`);
			expect(() => f.database.workAttemptSettle(f.runtime.opRef, 0, f.closed, f.settlement, f.payload)).toThrow();
			expect(f.database.workAttemptGet(f.runtime.opRef)).toEqual(f.runtime);
			expect(parseLaneJobRecord(f.database.laneJobJson(f.runtime.jobId)!).attempts[0]?.endedAt).toBeUndefined();
			expect(f.database.workLaneRows()[0]?.last_activity_at).toBe(START);
			expect(f.database.deliveryRows()).toHaveLength(0);
			f.raw.exec("DROP TRIGGER fault");
			expect(f.database.workAttemptSettle(f.runtime.opRef, 0, f.closed, f.settlement, f.payload)?.decision).toBe(
				"enqueued",
			);
			expect(f.database.deliveryRows()).toHaveLength(1);
			expect(f.database.workLaneRows()[0]?.last_activity_at).toBe(END);
		});
	}

	test("CAS refuses stale settlement and duplicate decisions; pruning does not regenerate", async () => {
		const f = await fixture();
		f.database.workAttemptPrepare(f.runtime, f.record);
		const accepted = f.database.workAttemptUpdate(f.runtime.opRef, 0, {
			sendPhase: "accepted",
			sendEvidence: { source: "receipt", observedAt: START },
		});
		expect(accepted?.version).toBe(1);
		expect(f.database.workAttemptSettle(f.runtime.opRef, 0, f.closed, f.settlement, f.payload)).toBeUndefined();
		expect(f.database.deliveryRows()).toHaveLength(0);
		expect(f.database.workAttemptSettle(f.runtime.opRef, 1, f.closed, f.settlement, f.payload)?.version).toBe(2);
		const ledger = new DeliveryLedger(f.database);
		expect(ledger.confirm(f.runtime.deliveryId)).toBe("transitioned");
		expect(ledger.prune(0, Date.now() + 1000)).toBe(1);
		const reopened = await GatewayDatabase.open(f.path);
		handles.push(reopened);
		expect(reopened.workAttemptSettle(f.runtime.opRef, 2, f.closed, f.settlement, f.payload)).toBeUndefined();
		expect(reopened.workAttemptUpdate(f.runtime.opRef, 2, {})).toBeUndefined();
		expect(reopened.workAttemptGet(f.runtime.opRef)?.decision).toBe("enqueued");
		expect(reopened.deliveryRows()).toHaveLength(0);
	});

	test("duplicate op and different open attempt cannot rewrite lane history", async () => {
		const f = await fixture();
		f.database.workAttemptPrepare(f.runtime, f.record);
		expect(() => f.database.workAttemptPrepare(f.runtime, f.record)).toThrow();
		const opRef = "gw-work-store-b";
		const other = {
			...f.runtime,
			opRef,
			deliveryId: workAttemptDeliveryId(f.database.instanceId, f.runtime.jobId, opRef),
		};
		const record = { ...f.record, attempts: [{ ...f.record.attempts[0]!, opRef }] };
		expect(() => f.database.workAttemptPrepare(other, record)).toThrow();
		expect(f.database.workAttemptOpen()).toEqual([f.runtime]);
		expect(f.database.workAttemptOpen(1, f.runtime.opRef)).toEqual([]);
		expect(() => f.database.workAttemptOpen(1001)).toThrow();
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
		f.database.workAttemptPrepare(f.runtime, f.record);
		new DeliveryService(new DeliveryLedger(f.database)).prepare(
			"other-turn",
			LOOPBACK_ORIGIN,
			"other obligation",
			undefined,
			f.runtime.deliveryId,
		);
		expect(() => f.database.workAttemptSettle(f.runtime.opRef, 0, f.closed, f.settlement, f.payload)).toThrow();
		expect(f.database.workAttemptGet(f.runtime.opRef)?.decision).toBe("undecided");
		expect(f.database.deliveryRows()[0]?.turn_id).toBe("other-turn");
	});

	test("same-transaction delivery seam requires ownership, suppresses silence and checks duplicate payloads", async () => {
		const f = await fixture();
		const delivery = new DeliveryService(new DeliveryLedger(f.database));
		expect(() => delivery.persistInTransaction(f.payload)).toThrow("requires a database transaction");
		expect(f.database.withTransaction(() => delivery.persistInTransaction(f.payload))).toBe(true);
		expect(f.database.withTransaction(() => delivery.persistInTransaction(f.payload))).toBe(false);
		expect(() =>
			f.database.withTransaction(() => delivery.persistInTransaction({ ...f.payload, text: "changed" })),
		).toThrow();
		expect(buildDeliveryPayload("turn", LOOPBACK_ORIGIN, "NO_REPLY", "silent")).toBeUndefined();
	});

	test("known silence and checkpoint commit atomically, survive reopen and suppress exactly once", async () => {
		const f = await fixture();
		f.database.workAttemptPrepare(f.runtime, f.record);
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
		f.raw.exec("CREATE TRIGGER fault BEFORE INSERT ON broker_tail_cursors BEGIN SELECT RAISE(ABORT, 'fault'); END");
		expect(() => f.database.workAttemptUpdate(f.runtime.opRef, 0, { output }, "cursor-final")).toThrow();
		expect(f.database.workAttemptGet(f.runtime.opRef)?.output.knownSilence).toBeNull();
		expect(f.database.tailCursorGet(SESSION)).toBeUndefined();
		f.raw.exec("DROP TRIGGER fault");
		f.database.workAttemptUpdate(f.runtime.opRef, 0, { output }, "cursor-final");
		const reopened = await GatewayDatabase.open(f.path);
		handles.push(reopened);
		expect(reopened.workAttemptGet(f.runtime.opRef)?.output.knownSilence).toEqual(proof);
		expect(reopened.tailCursorGet(SESSION)).toBe("cursor-final");
		expect(() => reopened.workAttemptUpdate(f.runtime.opRef, 1, { output: f.runtime.output })).toThrow();
		const suppressed = { ...f.settlement, output, decision: "suppressed" as const };
		expect(reopened.workAttemptSettle(f.runtime.opRef, 1, f.closed, suppressed)?.decision).toBe("suppressed");
		expect(reopened.workAttemptSettle(f.runtime.opRef, 2, f.closed, suppressed)).toBeUndefined();
		expect(reopened.deliveryRows()).toHaveLength(0);
	});

	test("output claims persist three-read budget; stale/wrong proof and oversized excerpt fail closed", async () => {
		const f = await fixture();
		f.database.workAttemptPrepare(f.runtime, f.record);
		let runtime = f.database.workAttemptUpdate(f.runtime.opRef, 0, { terminal: f.settlement.terminal })!;
		for (let reads = 1; reads <= 3; reads++)
			runtime = f.database.workAttemptUpdate(runtime.opRef, runtime.version, {
				output: { ...runtime.output, reads, nextReadAt: END },
			})!;
		expect(runtime.output.reads).toBe(3);
		expect(() =>
			f.database.workAttemptUpdate(runtime.opRef, runtime.version, { output: { ...runtime.output, reads: 4 } }),
		).toThrow();
		expect(() =>
			f.database.workAttemptUpdate(runtime.opRef, runtime.version, { output: { ...runtime.output, reads: 0 } }),
		).toThrow();
		expect(() =>
			f.database.workAttemptUpdate(runtime.opRef, runtime.version, {
				output: { ...runtime.output, excerpt: "界".repeat(1000) },
			}),
		).toThrow();
		expect(() =>
			f.database.workAttemptUpdate(runtime.opRef, runtime.version, {
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
		).toThrow();
	});

	test("corrupt runtime projection or history throws rather than returning empty", async () => {
		const f = await fixture();
		f.database.workAttemptPrepare(f.runtime, f.record);
		f.raw.query("UPDATE work_attempt_runtime SET version = 8 WHERE op_ref = ?").run(f.runtime.opRef);
		expect(() => f.database.workAttemptGet(f.runtime.opRef)).toThrow(WorkAttemptStateError);
		expect(() => f.database.workAttemptOpen()).toThrow(WorkAttemptStateError);
		f.raw.query("UPDATE work_attempt_runtime SET version = 0, record_json = '{' WHERE op_ref = ?").run(f.runtime.opRef);
		expect(() => f.database.workAttemptOpenByLane(f.runtime.laneKey)).toThrow(WorkAttemptStateError);
	});

	test("run is response-only and final decisions cannot bypass settlement", async () => {
		const f = await fixture();
		expect(() => f.database.workAttemptPrepare({ ...f.runtime, mode: "run" }, f.record)).toThrow();
		const runtime = { ...f.runtime, mode: "run" as const, target: null };
		f.database.workAttemptPrepare(runtime, f.record);
		expect(() => f.database.workAttemptUpdate(runtime.opRef, 0, f.settlement)).toThrow();
		expect(() => f.database.workAttemptSettle(runtime.opRef, 0, f.closed, f.settlement, f.payload)).toThrow();
		const patch = { ...f.settlement, decision: "no_target" as const };
		expect(() => f.database.workAttemptSettle(runtime.opRef, 0, f.closed, patch, f.payload)).toThrow();
		expect(f.database.workAttemptSettle(runtime.opRef, 0, f.closed, patch)?.decision).toBe("no_target");
		expect(f.database.deliveryRows()).toHaveLength(0);
	});

	test("wrong bound identity and history cannot be prepared", async () => {
		const f = await fixture();
		expect(() => f.database.workAttemptPrepare({ ...f.runtime, epoch: 1 }, f.record)).toThrow();
		expect(() => f.database.workAttemptPrepare({ ...f.runtime, cwd: "/different" }, f.record)).toThrow();
		expect(() => f.database.workAttemptPrepare({ ...f.runtime, deliveryId: "arbitrary" }, f.record)).toThrow();
		expect(f.database.workAttemptOpen()).toHaveLength(0);
		expect(f.database.laneJobJson(f.runtime.jobId)).toBeUndefined();
	});

	test("v21 migration preserves historical open history and adopts without target", async () => {
		const f = await fixture();
		f.database.putLaneJob({ ...f.record, laneKey: f.runtime.laneKey, json: JSON.stringify(f.record) });
		// Preserve the fixture's explicit provenance while replaying v21 and v22.
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
		f.raw.exec("DROP TABLE work_attempt_runtime; DELETE FROM schema_migrations WHERE version >= 21");
		const migrated = await GatewayDatabase.open(f.path);
		handles.push(migrated);
		f.raw.exec(`INSERT INTO broker_authority SELECT * FROM saved_authority;
			INSERT INTO broker_owned_bindings SELECT * FROM saved_bindings;
			DROP TABLE saved_authority;
			DROP TABLE saved_bindings;`);
		migrated.assertBrokerAuthority(f.authority);
		expect(migrated.assertOwnedSession(SESSION, f.runtime.cwd, f.authority)).toMatchObject({
			originKey: f.runtime.sessionKey,
			epoch: f.runtime.epoch,
		});
		expect(migrated.schemaVersion).toBe(22);
		expect(migrated.laneJobJson(f.runtime.jobId)).toBe(JSON.stringify(f.record));
		const historical = { ...f.runtime, mode: "historical" as const, sendPhase: "uncertain" as const, target: null };
		migrated.workAttemptPrepare(historical, f.record);
		expect(migrated.workAttemptGet(f.runtime.opRef)?.target).toBeNull();
		expect(
			migrated.workAttemptSettle(f.runtime.opRef, 0, f.closed, { ...f.settlement, decision: "no_target" })?.decision,
		).toBe("no_target");
		expect(migrated.deliveryRows()).toHaveLength(0);
	});
});
