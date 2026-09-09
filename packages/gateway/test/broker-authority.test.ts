import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendAttempt, closeAttempt, createLaneJobRecord } from "@gajaeway/subsession";
import { type BrokerAuthority, GatewayDatabase, type WorkAttemptRuntime, workAttemptDeliveryId } from "../src/store/db";

const GLOBAL: BrokerAuthority = { canonicalAgentDir: "/home/operator/.gjc/agent", identity: "global-user" };
const PRIVATE: BrokerAuthority = { canonicalAgentDir: "/srv/gateway/agent", identity: "retired-private" };
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
const NEXT_SESSION = "bd2f2494-2584-4d13-b7b6-c6ac24a1087f";
const ORIGIN = "discord/channel/authority";
const START = "2026-09-08T00:00:00.000Z";
const directories: string[] = [];
const handles: Array<{ close(): void }> = [];

afterEach(async () => {
	for (const handle of handles.splice(0)) handle.close();
	for (const directory of directories.splice(0)) await rm(directory, { recursive: true, force: true });
});

async function fixture() {
	const directory = await mkdtemp(join(tmpdir(), "broker-authority-"));
	directories.push(directory);
	const path = join(directory, "gateway.db");
	const database = await GatewayDatabase.open(path);
	const raw = new Database(path);
	handles.push(database, raw);
	return { database, raw, path };
}

function binding(authority = GLOBAL, sessionId = SESSION, epoch = 0) {
	return { sessionId, originKey: ORIGIN, epoch, repo: "/work", authority };
}

function seedAccepted(database: GatewayDatabase) {
	database.putSession(ORIGIN, SESSION);
	database.inboundEnqueue({ messageId: "accepted", originKey: ORIGIN, originRefJson: "{}", body: "do not replay" });
	database.inboundBindTurn({
		messageId: "accepted",
		originKey: ORIGIN,
		epoch: 0,
		opRef: "gw-old-accepted",
		sessionId: SESSION,
	});
	database.inboundTurnAccept("gw-old-accepted");
	database.inboundEnqueue({ messageId: "pending", originKey: ORIGIN, originRefJson: "{}", body: "old pending" });
	database.inboundEnqueue({ messageId: "held", originKey: ORIGIN, originRefJson: "{}", body: "old ambiguous steer" });
	database.inboundSteerIssued({ messageId: "held", epoch: 0, opRef: "gw-old-accepted" });
}

function seedLane(database: GatewayDatabase) {
	const record = appendAttempt(
		createLaneJobRecord({
			jobId: "lanejob-authority",
			branch: "main",
			worktreePath: "/work",
			sessionId: SESSION,
			now: () => new Date(START),
		}),
		{ opRef: "gw-authority-work", sessionId: SESSION, startedAt: START },
	);
	database.putLaneJob({ ...record, laneKey: "work-authority", json: JSON.stringify(record) });
	return record;
}

function cutover(database: GatewayDatabase) {
	return database.cutoverBrokerAuthority({
		expectedAuthority: null,
		targetAuthority: GLOBAL,
		evidence: "Operator authorized archive and quarantine; no runtime operations.",
		disposition: "quarantine",
	});
}

describe("durable single broker authority", () => {
	test("boot requires explicit initialization and never replaces the active identity", async () => {
		const { database, path } = await fixture();
		expect(() => database.assertBrokerAuthority(GLOBAL)).toThrow("cutover_required");
		database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true });
		const reopened = await GatewayDatabase.open(path);
		handles.push(reopened);
		reopened.assertBrokerAuthority(GLOBAL);
		expect(() => reopened.assertBrokerAuthority(PRIVATE, { initializeEmpty: true })).toThrow("authority_mismatch");
		expect(() =>
			reopened.assertBrokerAuthority({ ...GLOBAL, canonicalAgentDir: "/home/operator/../operator/.gjc/agent" }),
		).toThrow("invalid_authority");
	});

	test("populated legacy history cannot become global ownership", async () => {
		const { database, raw } = await fixture();
		database.putSession(ORIGIN, SESSION);
		raw
			.query("INSERT INTO session_tail_cursors(session_id, cursor, updated_at) VALUES (?, ?, ?)")
			.run(SESSION, "private-cursor", START);
		expect(() => database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true })).toThrow("cutover_required");
		expect(database.getSession(ORIGIN)).toBe(SESSION);
		expect(() => database.assertOwnedSession(SESSION, "/work", GLOBAL)).toThrow("cutover_required");
		cutover(database);
		expect(() => database.assertOwnedSession(SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(() => database.recordOwnedBinding(binding(GLOBAL, SESSION, 1))).toThrow("unowned_session");
		expect(() => database.tailCursorGet(SESSION)).toThrow("unowned_session");
		database.recordOwnedBinding(binding(GLOBAL, NEXT_SESSION, 1));
		expect(database.tailCursorGet(NEXT_SESSION)).toBeUndefined();
		expect(raw.query("SELECT cursor FROM session_tail_cursors").get()).toEqual({ cursor: "private-cursor" });
	});

	test("context-only legacy databases also require cutover", async () => {
		const { database } = await fixture();
		database.contextRecord({ messageId: "context-only", originKey: ORIGIN, body: "preserve context" });
		expect(() => database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true })).toThrow("cutover_required");
	});

	test("ownership requires immutable create provenance, not same cwd or arbitrary binding", async () => {
		const { database, raw } = await fixture();
		database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true });
		expect(database.recordOwnedBinding(binding())).toBe(true);
		expect(database.recordOwnedBinding(binding())).toBe(true);
		expect(database.assertOwnedSession(SESSION, "/work", GLOBAL)).toEqual(binding());
		expect(() => database.assertOwnedSession(NEXT_SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(() => database.assertOwnedSession(SESSION, "/other", GLOBAL)).toThrow("unowned_session");
		expect(() => database.assertOwnedSession(SESSION, "/work", PRIVATE)).toThrow("authority_mismatch");
		expect(() => database.recordOwnedBinding({ ...binding(), originKey: "discord/channel/unrelated" })).toThrow(
			"unowned_session",
		);
		expect(() => database.putSession(ORIGIN, NEXT_SESSION)).toThrow("unowned_session");
		expect(() => database.putSessionAtEpoch(ORIGIN, NEXT_SESSION, 0)).toThrow("unowned_session");
		expect(() => raw.exec("UPDATE broker_owned_bindings SET repo = '/other'")).toThrow("immutable broker provenance");
		expect(() => raw.exec("DELETE FROM broker_owned_bindings")).toThrow("immutable broker provenance");
	});

	test("epoch races reject stale creates while retired owned sessions remain readable", async () => {
		const { database } = await fixture();
		database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true });
		database.recordOwnedBinding(binding());
		database.tailCursorCommit(SESSION, "owned-cursor");
		database.rebindEpoch(ORIGIN);
		expect(database.recordOwnedBinding(binding(GLOBAL, NEXT_SESSION, 0))).toBe(false);
		expect(() => database.assertOwnedSession(NEXT_SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(database.recordOwnedBinding(binding(GLOBAL, NEXT_SESSION, 1))).toBe(true);
		expect(database.assertOwnedSession(SESSION, "/work", GLOBAL).epoch).toBe(0);
		expect(database.tailCursorGet(SESSION)).toBe("owned-cursor");
		expect(database.tailCursorGet(NEXT_SESSION)).toBeUndefined();
	});

	test("binding and cursor transactions roll back without phantom ownership/checkpoints", async () => {
		const { database, raw } = await fixture();
		database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true });
		raw.exec("CREATE TRIGGER fault AFTER INSERT ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
		expect(() => database.recordOwnedBinding(binding())).toThrow("fault");
		expect(database.getSession(ORIGIN)).toBeUndefined();
		expect(() => database.assertOwnedSession(SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		raw.exec("DROP TRIGGER fault");
		database.recordOwnedBinding(binding());
		database.tailCursorCommit(SESSION, "before");
		expect(() =>
			database.withTransaction(() => {
				database.tailCursorCommit(SESSION, "after");
				throw new Error("rollback");
			}),
		).toThrow("rollback");
		expect(database.tailCursorGet(SESSION)).toBe("before");
	});

	test("empty runtime with an open lane is not quiescent", async () => {
		const { database } = await fixture();
		const record = seedLane(database);
		expect(database.workAttemptOpen()).toEqual([]);
		expect(database.inspectBrokerAuthority().openWork).toBeGreaterThan(0);
		expect(() =>
			database.cutoverBrokerAuthority({ expectedAuthority: null, targetAuthority: GLOBAL, evidence: "inspect" }),
		).toThrow("old_work_open");
		cutover(database);
		expect(database.laneJobJson(record.jobId)).toBe(JSON.stringify(record));
		expect(database.isBrokerQuarantined("work", record.jobId)).toBe(true);
		expect(database.laneJobRows()).toEqual([]);
		expect(database.laneJobRows(true)).toHaveLength(1);
		expect(() => database.putLaneJob({ ...record, laneKey: "work-authority", json: JSON.stringify(record) })).toThrow(
			"quarantined",
		);
	});

	test("failed awaiting-operator history is an explicit hold even with no open attempt", async () => {
		const { database } = await fixture();
		const opened = seedLane(database);
		const record = {
			...closeAttempt({
				record: opened,
				opRef: "gw-authority-work",
				endState: "failed",
				endedAt: "2026-09-08T00:01:00.000Z",
			}),
			state: "awaiting_operator" as const,
		};
		database.putLaneJob({ ...record, laneKey: "work-authority", json: JSON.stringify(record) });
		expect(record.attempts.every((attempt) => attempt.endedAt !== undefined)).toBe(true);
		expect(database.inspectBrokerAuthority().openWork).toBe(1);
		expect(() =>
			database.cutoverBrokerAuthority({
				expectedAuthority: null,
				targetAuthority: GLOBAL,
				evidence: "no active runtime is insufficient",
			}),
		).toThrow("old_work_open");
		cutover(database);
		expect(database.laneJobRows()).toEqual([]);
		expect(database.laneJobJson(record.jobId)).toBe(JSON.stringify(record));
	});

	test("retired terminal-trigger steers cannot re-enter the client-ref replay selector", async () => {
		const { database } = await fixture();
		seedAccepted(database);
		database.inboundTurnComplete("gw-old-accepted");
		expect(database.inboundSteersHeldAfterTerminal(ORIGIN)).toHaveLength(1);
		cutover(database);
		expect(database.inboundSteersHeldAfterTerminal(ORIGIN)).toEqual([]);
		expect(database.inboundTurnRows("gw-old-accepted").find((row) => row.message_id === "held")?.turn_state).toBe(
			"bound",
		);
	});

	test("accepted, pending and ambiguous old inputs remain archived holds, never replayed", async () => {
		const { database, raw, path } = await fixture();
		seedAccepted(database);
		database.contextRecord({ messageId: "context", originKey: ORIGIN, body: "keep unread" });
		database.metaSet("delivery_dedup", "keep");
		database.deliveryCreate({ id: "delivery-old", turnId: "gw-old-accepted", originKey: ORIGIN, payloadJson: "{}" });
		raw
			.query(
				"INSERT INTO monitor_events(event_id, monitor_id, event_type, payload_json, fired_at, stage, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
			)
			.run("monitor-old", "monitor", "test", "{}", START, "authored", START);
		raw.query("UPDATE monitor_events SET batch_id = ? WHERE event_id = ?").run("gw-old-accepted", "monitor-old");
		const instance = database.instanceId;
		const inboundBefore = raw.query("SELECT * FROM inbound_messages ORDER BY message_id").all();
		const deliveriesBefore = raw.query("SELECT * FROM deliveries").all();
		const contextBefore = raw.query("SELECT * FROM conversation_context").all();
		expect(() =>
			database.cutoverBrokerAuthority({ expectedAuthority: null, targetAuthority: GLOBAL, evidence: "no disposition" }),
		).toThrow("old_work_open");
		const id = cutover(database);
		const snapshot = JSON.parse(database.brokerCutoverSnapshot(id)!);
		expect(snapshot.sessions[0].gjc_session_id).toBe(SESSION);
		expect(snapshot.inbound_messages).toHaveLength(3);
		expect(database.getSessionRecord(ORIGIN)).toEqual({ sessionId: "", epoch: 1 });
		expect(raw.query("SELECT * FROM inbound_messages ORDER BY message_id").all()).toEqual(inboundBefore);
		expect(raw.query("SELECT * FROM deliveries").all()).toEqual(deliveriesBefore);
		expect(raw.query("SELECT * FROM conversation_context").all()).toEqual(contextBefore);
		expect(database.instanceId).toBe(instance);
		expect(database.metaGet("delivery_dedup")).toBe("keep");
		expect(database.isBrokerQuarantined("monitor", "monitor-old")).toBe(true);
		expect(database.monitorEventRows()).toEqual([]);
		expect(database.monitorEventRows(undefined, "oldest")).toEqual([]);
		expect(database.monitorEventRows("monitor", "oldest")).toEqual([]);
		expect(database.monitorEventRows(undefined, "newest", true)).toHaveLength(1);
		expect(() => database.monitorEventAcquireLease("monitor-old", "owner", "lease", 1000)).toThrow("quarantined");
		expect(() => database.monitorEventUpdate("monitor-old", "admitted")).toThrow("quarantined");
		expect(() =>
			database.monitorEventFencedAuthorWithIntent("monitor-old", "", "new text", false, "test", "new-intent", "{}"),
		).toThrow("quarantined");
		expect(database.monitorEventSettle("monitor-old", "delivered")).toBe(false);
		expect(database.deliveryConfirmWithSettle("delivery-old", "delivered")).toBe("transitioned");
		expect(database.deliveryRows()[0]?.state).toBe("confirmed");
		for (const messageId of ["accepted", "pending", "held"])
			expect(database.isBrokerQuarantined("inbound", messageId)).toBe(true);
		expect(database.inboundNonterminalTurns(ORIGIN)).toEqual([]);
		expect(database.inboundNonterminalOrigins()).toEqual([]);
		expect(database.inboundPendingOrigins()).toEqual([]);
		expect(database.inboundPendingOldest(ORIGIN)).toBeUndefined();
		expect(database.inboundNonterminalTurns(ORIGIN, 0)).toEqual([]);
		expect(database.inboundNonterminalTurnCount()).toBe(0);
		expect(database.inboundPendingCount(ORIGIN)).toBe(0);
		expect(database.inboundPendingByOrigin()).toEqual([]);
		expect(database.inboundSteersHeld("gw-old-accepted")).toEqual([]);
		expect(database.inboundSteersHeldAfterTerminal(ORIGIN)).toEqual([]);
		expect(database.inboundDiscardBefore(ORIGIN, "2099-01-01T00:00:00.000Z")).toEqual([]);
		expect(database.inboundTurnRow("gw-old-accepted")?.turn_state).toBe("accepted");
		expect(database.inboundTurnRows("gw-old-accepted")).toHaveLength(2);
		expect(() => database.inboundTurnComplete("gw-old-accepted")).toThrow("quarantined");
		expect(() => database.inboundSteerRefused("held", "gw-old-accepted")).toThrow("quarantined");
		expect(() =>
			database.inboundBindTurn({
				messageId: "pending",
				originKey: ORIGIN,
				epoch: 1,
				opRef: "gw-new",
				sessionId: NEXT_SESSION,
			}),
		).toThrow("quarantined");
		expect(() => database.inboundTurnRequeue("gw-old-accepted")).toThrow("quarantined");
		expect(() => database.inboundSteerIssued({ messageId: "held", epoch: 1, opRef: "new" })).toThrow("quarantined");
		expect(
			database.inboundEnqueue({ messageId: "accepted", originKey: ORIGIN, originRefJson: "{}", body: "duplicate" }),
		).toBe(false);
		database.inboundEnqueue({ messageId: "new", originKey: ORIGIN, originRefJson: "{}", body: "new input" });
		expect(database.inboundPendingOldest(ORIGIN)?.message_id).toBe("new");
		const reopened = await GatewayDatabase.open(path);
		handles.push(reopened);
		expect(reopened.isBrokerQuarantined("inbound", "accepted")).toBe(true);
		expect(reopened.inboundNonterminalTurns(ORIGIN)).toEqual([]);
		expect(reopened.brokerCutoverSnapshot(id)).toBe(database.brokerCutoverSnapshot(id));
	});

	test("open worker runtime and historical lane stay intact but non-executable after quarantine", async () => {
		const { database } = await fixture();
		database.putSession("work/task/authority", SESSION);
		const record = seedLane(database);
		const runtime: WorkAttemptRuntime = {
			opRef: "gw-authority-work",
			jobId: record.jobId,
			laneKey: "work-authority",
			sessionKey: "work/task/authority",
			sessionId: SESSION,
			epoch: 0,
			cwd: "/work",
			startedAt: START,
			mode: "historical",
			sendPhase: "uncertain",
			sendEvidence: null,
			terminal: null,
			output: { disposition: "pending", reads: 0, nextReadAt: null, excerpt: null, proof: null, knownSilence: null },
			target: null,
			deliveryId: workAttemptDeliveryId(database.instanceId, record.jobId, "gw-authority-work"),
			decision: "undecided",
			settledAt: null,
			version: 0,
		};
		database.workAttemptPrepare(runtime, record);
		cutover(database);
		expect(database.workAttemptGet(runtime.opRef)).toEqual(runtime);
		expect(database.workAttemptOpen()).toEqual([]);
		expect(database.workAttemptOpenByLane("work-authority")).toBeUndefined();
		expect(() => database.workAttemptUpdate(runtime.opRef, 0, {})).toThrow("quarantined");
		expect(() => database.workAttemptPrepare(runtime, record)).toThrow("quarantined");
		expect(database.laneJobJson(record.jobId)).toBe(JSON.stringify(record));
	});

	test("cutover snapshot, holds, epoch and authority roll back as one unit", async () => {
		const { database, raw } = await fixture();
		seedAccepted(database);
		const before = raw.query("SELECT * FROM sessions").all();
		raw.exec("CREATE TRIGGER fault AFTER UPDATE ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
		expect(() => cutover(database)).toThrow("fault");
		expect(raw.query("SELECT * FROM sessions").all()).toEqual(before);
		expect(raw.query("SELECT * FROM broker_cutovers").all()).toEqual([]);
		expect(raw.query("SELECT * FROM broker_quarantine").all()).toEqual([]);
		expect(raw.query("SELECT * FROM broker_retired_sessions").all()).toEqual([]);
		expect(database.inspectBrokerAuthority().authority).toBeNull();
		expect(database.inboundNonterminalTurns(ORIGIN)).toHaveLength(1);
	});

	test("wrong expected authority and authority resurrection are rejected", async () => {
		const { database } = await fixture();
		database.assertBrokerAuthority(PRIVATE, { initializeEmpty: true });
		database.recordOwnedBinding(binding(PRIVATE));
		database.tailCursorCommit(SESSION, "private-qualified");
		expect(() => cutover(database)).toThrow("authority_mismatch");
		database.cutoverBrokerAuthority({
			expectedAuthority: PRIVATE,
			targetAuthority: GLOBAL,
			evidence: "authorized",
			disposition: "quarantine",
		});
		expect(() => database.assertOwnedSession(SESSION, "/work", PRIVATE)).toThrow("authority_mismatch");
		expect(() => database.assertOwnedSession(SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(() => database.tailCursorGet(SESSION)).toThrow("unowned_session");
		expect(() =>
			database.cutoverBrokerAuthority({
				expectedAuthority: GLOBAL,
				targetAuthority: PRIVATE,
				evidence: "unsafe resurrection",
				disposition: "quarantine",
			}),
		).toThrow("authority_mismatch");
	});

	test("corrupt historical job JSON is not swallowed even with explicit quarantine", async () => {
		const { database, raw } = await fixture();
		seedLane(database);
		raw.exec("UPDATE lane_jobs SET record_json = '{' ");
		expect(() => database.inspectBrokerAuthority()).toThrow();
		expect(() => database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true })).toThrow();
		expect(() => cutover(database)).toThrow();
		expect(raw.query("SELECT * FROM broker_authority").all()).toEqual([]);
		expect(raw.query("SELECT * FROM broker_cutovers").all()).toEqual([]);
	});

	test("established authority checks isolate unrelated corrupt history while inspection remains fail-closed", async () => {
		const { database, raw } = await fixture();
		database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true });
		database.recordOwnedBinding(binding());
		seedLane(database);
		raw.exec("UPDATE lane_jobs SET record_json = '{'");
		expect(() => database.assertBrokerAuthority(GLOBAL)).not.toThrow();
		expect(() => database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true })).not.toThrow();
		expect(database.assertOwnedSession(SESSION, "/work", GLOBAL)).toEqual(binding());
		expect(() => database.assertBrokerAuthority(PRIVATE)).toThrow("authority_mismatch");
		expect(() => database.assertOwnedSession(NEXT_SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(() => database.inspectBrokerAuthority()).toThrow();
		expect(() =>
			database.cutoverBrokerAuthority({
				expectedAuthority: GLOBAL,
				targetAuthority: PRIVATE,
				evidence: "validate history",
				disposition: "quarantine",
			}),
		).toThrow();
		expect(database.laneJobJson("lanejob-authority")).toBe("{");
		expect(raw.query("SELECT * FROM broker_cutovers").all()).toEqual([]);
	});

	test("corrupt authority is rejected rather than initialized over", async () => {
		const { database, raw } = await fixture();
		database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true });
		raw.query("UPDATE broker_authority SET authority_key = ?").run("{");
		expect(() => database.inspectBrokerAuthority()).toThrow();
		expect(() => database.assertBrokerAuthority(GLOBAL, { initializeEmpty: true })).toThrow();
	});
});
