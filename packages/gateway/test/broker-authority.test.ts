import { Database } from "bun:sqlite";
import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { appendAttempt, closeAttempt, createLaneJobRecord } from "@gajae-gateway/subsession";
import type { BrokerAuthority } from "../src/store/groups/bindings";
import { Store } from "../src/store";
import { type WorkAttemptRuntime, workAttemptDeliveryId, workAttemptReportId } from "../src/store/groups/work";
import { retryInboundTurn } from "../src/orchestrator/persona-session";

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
	const database = await Store.open(path);
	const raw = new Database(path);
	handles.push(database, raw);
	return { database, raw, path };
}

function binding(authority = GLOBAL, sessionId = SESSION, epoch = 0) {
	return { sessionId, originKey: ORIGIN, epoch, repo: "/work", authority };
}

function seedAccepted(database: Store) {
	database.transaction((tx) => database.bindings.bind(tx, ORIGIN, SESSION));
	database.transaction((tx) => database.inbox.enqueue(tx, { messageId: "accepted", originKey: ORIGIN, originRefJson: "{}", body: "do not replay" }));
	database.transaction((tx) => database.inbox.bindTurn(tx, {
		messageId: "accepted",
		originKey: ORIGIN,
		epoch: 0,
		opRef: "gw-old-accepted",
		sessionId: SESSION,
	}));
	database.transaction((tx) => database.inbox.turnAccept(tx, "gw-old-accepted"));
	database.transaction((tx) => database.inbox.enqueue(tx, { messageId: "pending", originKey: ORIGIN, originRefJson: "{}", body: "old pending" }));
	database.transaction((tx) => database.inbox.enqueue(tx, { messageId: "held", originKey: ORIGIN, originRefJson: "{}", body: "old ambiguous steer" }));
	database.transaction((tx) => database.inbox.steerIssued(tx, { messageId: "held", epoch: 0, opRef: "gw-old-accepted" }));
}

function seedLane(database: Store) {
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
	database.transaction((tx) => database.work.putLaneJob(tx, { ...record, laneKey: "work-authority", json: JSON.stringify(record) }));
	return record;
}

function cutover(database: Store) {
	return database.transaction((tx) => database.bindings.cutoverBrokerAuthority(tx, {
		expectedAuthority: null,
		targetAuthority: GLOBAL,
		evidence: "Operator authorized archive and quarantine; no runtime operations.",
		disposition: "quarantine",
	}));
}

describe("durable single broker authority", () => {
	test("boot requires explicit initialization and never replaces the active identity", async () => {
		const { database, path } = await fixture();
		expect(() => database.bindings.assertAuthority(GLOBAL)).toThrow("cutover_required");
		database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated));
		const reopened = await Store.open(path);
		handles.push(reopened);
		reopened.bindings.assertAuthority(GLOBAL);
		expect(() => reopened.transaction((tx) => reopened.bindings.initializeAuthority(tx, PRIVATE, reopened.inspectBrokerAuthority().populated))).toThrow("authority_mismatch");
		expect(() =>
			reopened.bindings.assertAuthority({ ...GLOBAL, canonicalAgentDir: "/home/operator/../operator/.gjc/agent" }),
		).toThrow("invalid_authority");
	});

	test("populated legacy history cannot become global ownership", async () => {
		const { database, raw } = await fixture();
		database.transaction((tx) => database.bindings.bind(tx, ORIGIN, SESSION));
		raw
			.query("INSERT INTO session_tail_cursors(session_id, cursor, updated_at) VALUES (?, ?, ?)")
			.run(SESSION, "private-cursor", START);
		expect(() => database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated))).toThrow("cutover_required");
		expect(database.bindings.getSession(ORIGIN)).toBe(SESSION);
		expect(() => database.bindings.assertOwnedSession(SESSION, "/work", GLOBAL)).toThrow("cutover_required");
		cutover(database);
		expect(() => database.bindings.assertOwnedSession(SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(() => database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding(GLOBAL, SESSION, 1)))).toThrow("unowned_session");
		database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding(GLOBAL, NEXT_SESSION, 1)));
		expect(raw.query("SELECT cursor FROM session_tail_cursors").get()).toEqual({ cursor: "private-cursor" });
	});

	test("context-only legacy databases also require cutover", async () => {
		const { database } = await fixture();
		database.transaction((tx) => database.inbox.contextRecord(tx, { messageId: "context-only", originKey: ORIGIN, body: "preserve context" }));
		expect(() => database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated))).toThrow("cutover_required");
	});

	test("ownership requires immutable create provenance, not same cwd or arbitrary binding", async () => {
		const { database, raw } = await fixture();
		database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated));
		expect(database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding()))).toBe(true);
		expect(database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding()))).toBe(true);
		expect(database.bindings.assertOwnedSession(SESSION, "/work", GLOBAL)).toEqual(binding());
		expect(() => database.bindings.assertOwnedSession(NEXT_SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(() => database.bindings.assertOwnedSession(SESSION, "/other", GLOBAL)).toThrow("unowned_session");
		expect(() => database.bindings.assertOwnedSession(SESSION, "/work", PRIVATE)).toThrow("authority_mismatch");
		expect(() => database.transaction((tx) => database.bindings.recordOwnedBinding(tx, { ...binding(), originKey: "discord/channel/unrelated" }))).toThrow(
			"unowned_session",
		);
		expect(() => database.transaction((tx) => database.bindings.bind(tx, ORIGIN, NEXT_SESSION))).toThrow("unowned_session");
		expect(() => database.transaction((tx) => database.bindings.bindAtEpoch(tx, ORIGIN, NEXT_SESSION, 0))).toThrow("unowned_session");
		expect(() => raw.exec("UPDATE broker_owned_bindings SET repo = '/other'")).toThrow("immutable broker provenance");
		expect(() => raw.exec("DELETE FROM broker_owned_bindings")).toThrow("immutable broker provenance");
	});

	test("epoch races reject stale creates while retired owned sessions remain readable", async () => {
		const { database } = await fixture();
		database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated));
		database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding()));
		database.transaction((tx) => database.bindings.rotateEpoch(tx, ORIGIN));
		expect(database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding(GLOBAL, NEXT_SESSION, 0)))).toBe(false);
		expect(() => database.bindings.assertOwnedSession(NEXT_SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding(GLOBAL, NEXT_SESSION, 1)))).toBe(true);
		expect(database.bindings.assertOwnedSession(SESSION, "/work", GLOBAL).epoch).toBe(0);
	});

	test("binding transactions roll back without phantom ownership", async () => {
		const { database, raw } = await fixture();
		database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated));
		raw.exec("CREATE TRIGGER fault AFTER INSERT ON sessions BEGIN SELECT RAISE(ABORT, 'fault'); END");
		expect(() => database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding()))).toThrow("fault");
		expect(database.bindings.getSession(ORIGIN)).toBeUndefined();
		expect(() => database.bindings.assertOwnedSession(SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		raw.exec("DROP TRIGGER fault");
		database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding()));
	});

	test("empty runtime with an open lane is not quiescent", async () => {
		const { database } = await fixture();
		const record = seedLane(database);
		expect(database.work.workAttemptOpen()).toEqual([]);
		expect(database.inspectBrokerAuthority().openWork).toBeGreaterThan(0);
		expect(() =>
			database.transaction((tx) => database.bindings.cutoverBrokerAuthority(tx, { expectedAuthority: null, targetAuthority: GLOBAL, evidence: "inspect" })),
		).toThrow("old_work_open");
		cutover(database);
		expect(database.work.laneJobJson(record.jobId)).toBe(JSON.stringify(record));
		expect(database.bindings.isQuarantined("work", record.jobId)).toBe(true);
		expect(database.work.laneJobRows()).toEqual([]);
		expect(database.work.laneJobRows(true)).toHaveLength(1);
		expect(() => database.transaction((tx) => database.work.putLaneJob(tx, { ...record, laneKey: "work-authority", json: JSON.stringify(record) }))).toThrow(
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
		database.transaction((tx) => database.work.putLaneJob(tx, { ...record, laneKey: "work-authority", json: JSON.stringify(record) }));
		expect(record.attempts.every((attempt) => attempt.endedAt !== undefined)).toBe(true);
		expect(database.inspectBrokerAuthority().openWork).toBe(1);
		expect(() =>
			database.transaction((tx) => database.bindings.cutoverBrokerAuthority(tx, {
				expectedAuthority: null,
				targetAuthority: GLOBAL,
				evidence: "no active runtime is insufficient",
			})),
		).toThrow("old_work_open");
		cutover(database);
		expect(database.work.laneJobRows()).toEqual([]);
		expect(database.work.laneJobJson(record.jobId)).toBe(JSON.stringify(record));
	});

	test("retired terminal-trigger steers cannot re-enter the client-ref replay selector", async () => {
		const { database } = await fixture();
		seedAccepted(database);
		database.transaction((tx) => database.inbox.turnComplete(tx, "gw-old-accepted"));
		expect(database.inbox.steersHeldAfterTerminal(ORIGIN)).toHaveLength(1);
		cutover(database);
		expect(database.inbox.steersHeldAfterTerminal(ORIGIN)).toEqual([]);
		expect(database.inbox.turnRows("gw-old-accepted").find((row) => row.message_id === "held")?.turn_state).toBe(
			"bound",
		);
	});

	test("accepted, pending and ambiguous old inputs remain archived holds, never replayed", async () => {
		const { database, raw, path } = await fixture();
		seedAccepted(database);
		database.transaction((tx) => database.inbox.contextRecord(tx, { messageId: "context", originKey: ORIGIN, body: "keep unread" }));
		database.core.metaSet("delivery_dedup", "keep");
		database.transaction((tx) => database.outbox.create(tx, {
			deliveryId: "delivery-old",
			turnId: "gw-old-accepted",
			originKey: ORIGIN,
			payloadJson: "{}",
		}));
		database.transaction((tx) => {
			database.monitors.eventCreate(tx, {
				eventId: "monitor-old",
				monitorId: "monitor",
				eventType: "test",
				payloadJson: "{}",
				firedAt: START,
			});
			database.monitors.eventUpdate(tx, "monitor-old", "authored", "gw-old-accepted");
		});
		const instance = database.instanceId;
		const inboundBefore = raw.query("SELECT * FROM inbound_messages ORDER BY message_id").all();
		const deliveriesBefore = raw.query("SELECT * FROM deliveries").all();
		const contextBefore = raw.query("SELECT * FROM conversation_context").all();
		expect(() =>
			database.transaction((tx) => database.bindings.cutoverBrokerAuthority(tx, { expectedAuthority: null, targetAuthority: GLOBAL, evidence: "no disposition" })),
		).toThrow("old_work_open");
		const id = cutover(database);
		const snapshot = JSON.parse(database.bindings.cutoverSnapshot(id)!);
		expect(snapshot.sessions[0].gjc_session_id).toBe(SESSION);
		expect(snapshot.inbound_messages).toHaveLength(3);
		expect(database.bindings.getSessionRecord(ORIGIN)).toEqual({ sessionId: "", epoch: 1 });
		expect(raw.query("SELECT * FROM inbound_messages ORDER BY message_id").all()).toEqual(inboundBefore);
		expect(raw.query("SELECT * FROM deliveries").all()).toEqual(deliveriesBefore);
		expect(raw.query("SELECT * FROM conversation_context").all()).toEqual(contextBefore);
		expect(database.instanceId).toBe(instance);
		expect(database.core.metaGet("delivery_dedup")).toBe("keep");
		expect(database.bindings.isQuarantined("monitor", "monitor-old")).toBe(true);
		expect(database.monitors.eventRows()).toEqual([]);
		expect(database.monitors.eventRows(undefined, "oldest")).toEqual([]);
		expect(database.monitors.eventRows("monitor", "oldest")).toEqual([]);
		expect(database.monitors.eventRows(undefined, "newest", true)).toHaveLength(1);
		expect(() => database.transaction((tx) => database.monitors.eventAcquireLease(tx, "monitor-old", "owner", "lease", 1000))).toThrow("quarantined");
		expect(() => database.transaction((tx) => database.monitors.eventUpdate(tx, "monitor-old", "admitted"))).toThrow("quarantined");
		expect(() =>
			database.transaction((tx) => database.monitors.authorFenced(tx, "monitor-old", "", "new text")),
		).toThrow("quarantined");
		expect(database.transaction((tx) => database.monitors.eventSettle(tx, "monitor-old", "delivered"))).toBe(false);
		expect(database.transaction((tx) => {
			const delivery = database.outbox.get("delivery-old");
			if (!delivery) return "unknown";
			if (delivery.state === "confirmed" || delivery.state === "expired") {
				if (delivery.state === "confirmed") database.monitors.settleForTurn(tx, delivery.turnId, "delivered");
				return "already_terminal";
			}
			database.outbox.confirm(tx, delivery.deliveryId);
			database.monitors.settleForTurn(tx, delivery.turnId, "delivered");
			return "transitioned";
		})).toBe("transitioned");
		expect(database.outbox.rows()[0]?.state).toBe("confirmed");
		for (const messageId of ["accepted", "pending", "held"])
			expect(database.bindings.isQuarantined("inbound", messageId)).toBe(true);
		expect(database.inbox.nonterminalTurns(ORIGIN)).toEqual([]);
		expect(database.inbox.nonterminalOrigins()).toEqual([]);
		expect(database.inbox.pendingOrigins()).toEqual([]);
		expect(database.inbox.pendingOldest(ORIGIN)).toBeUndefined();
		expect(database.inbox.nonterminalTurns(ORIGIN, 0)).toEqual([]);
		expect(database.inbox.nonterminalTurnCount()).toBe(0);
		expect(database.inbox.pendingCount(ORIGIN)).toBe(0);
		expect(database.inbox.pendingByOrigin()).toEqual([]);
		expect(database.inbox.steersHeld("gw-old-accepted")).toEqual([]);
		expect(database.inbox.steersHeldAfterTerminal(ORIGIN)).toEqual([]);
		expect(database.transaction((tx) => database.inbox.discardBefore(tx, ORIGIN, "2099-01-01T00:00:00.000Z"))).toEqual([]);
		expect(database.inbox.turnRow("gw-old-accepted")?.turn_state).toBe("accepted");
		expect(database.inbox.turnRows("gw-old-accepted")).toHaveLength(2);
		expect(() => database.transaction((tx) => database.inbox.turnComplete(tx, "gw-old-accepted"))).toThrow("quarantined");
		expect(() => database.transaction((tx) => database.inbox.steerRefused(tx, "held", "gw-old-accepted"))).toThrow("quarantined");
		expect(() =>
			database.transaction((tx) => database.inbox.bindTurn(tx, {
				messageId: "pending",
				originKey: ORIGIN,
				epoch: 1,
				opRef: "gw-new",
				sessionId: NEXT_SESSION,
			})),
		).toThrow("quarantined");
		expect(() => database.transaction((tx) => retryInboundTurn(database, tx, "gw-old-accepted"))).toThrow("quarantined");
		expect(() => database.transaction((tx) => database.inbox.steerIssued(tx, { messageId: "held", epoch: 1, opRef: "new" }))).toThrow("quarantined");
		expect(
			database.transaction((tx) => database.inbox.enqueue(tx, { messageId: "accepted", originKey: ORIGIN, originRefJson: "{}", body: "duplicate" })),
		).toBe(false);
		database.transaction((tx) => database.inbox.enqueue(tx, { messageId: "new", originKey: ORIGIN, originRefJson: "{}", body: "new input" }));
		expect(database.inbox.pendingOldest(ORIGIN)?.message_id).toBe("new");
		const reopened = await Store.open(path);
		handles.push(reopened);
		expect(reopened.bindings.isQuarantined("inbound", "accepted")).toBe(true);
		expect(reopened.inbox.nonterminalTurns(ORIGIN)).toEqual([]);
		expect(reopened.bindings.cutoverSnapshot(id)).toBe(database.bindings.cutoverSnapshot(id));
	});

	test("open worker runtime and historical lane stay intact but non-executable after quarantine", async () => {
		const { database } = await fixture();
		database.transaction((tx) => database.bindings.bind(tx, "work/task/authority", SESSION));
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
			parent: null,
			reportId: workAttemptReportId(database.instanceId, record.jobId, "gw-authority-work"),
			wakeReportId: null,
			noticeHash: null,
			deliveryId: workAttemptDeliveryId(database.instanceId, record.jobId, "gw-authority-work"),
			decision: "undecided",
			settledAt: null,
			version: 0,
		};
		database.transaction((tx) => database.work.workAttemptPrepare(tx, runtime, record));
		cutover(database);
		expect(database.work.workAttemptGet(runtime.opRef)).toEqual(runtime);
		expect(database.work.workAttemptOpen()).toEqual([]);
		expect(database.work.workAttemptOpenByLane("work-authority")).toBeUndefined();
		expect(() => database.transaction((tx) => database.work.workAttemptUpdate(tx, runtime.opRef, 0, {}))).toThrow("quarantined");
		expect(() => database.transaction((tx) => database.work.workAttemptPrepare(tx, runtime, record))).toThrow("quarantined");
		expect(database.work.laneJobJson(record.jobId)).toBe(JSON.stringify(record));
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
		expect(database.inbox.nonterminalTurns(ORIGIN)).toHaveLength(1);
	});

	test("wrong expected authority and authority resurrection are rejected", async () => {
		const { database } = await fixture();
		database.transaction((tx) => database.bindings.initializeAuthority(tx, PRIVATE, database.inspectBrokerAuthority().populated));
		database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding(PRIVATE)));
		expect(() => cutover(database)).toThrow("authority_mismatch");
		database.transaction((tx) => database.bindings.cutoverBrokerAuthority(tx, {
			expectedAuthority: PRIVATE,
			targetAuthority: GLOBAL,
			evidence: "authorized",
			disposition: "quarantine",
		}));
		expect(() => database.bindings.assertOwnedSession(SESSION, "/work", PRIVATE)).toThrow("authority_mismatch");
		expect(() => database.bindings.assertOwnedSession(SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(() =>
			database.transaction((tx) => database.bindings.cutoverBrokerAuthority(tx, {
				expectedAuthority: GLOBAL,
				targetAuthority: PRIVATE,
				evidence: "unsafe resurrection",
				disposition: "quarantine",
			})),
		).toThrow("authority_mismatch");
	});

	test("corrupt historical job JSON is not swallowed even with explicit quarantine", async () => {
		const { database, raw } = await fixture();
		seedLane(database);
		raw.exec("UPDATE lane_jobs SET record_json = '{' ");
		expect(() => database.inspectBrokerAuthority()).toThrow();
		expect(() => database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated))).toThrow();
		expect(() => cutover(database)).toThrow();
		expect(raw.query("SELECT * FROM broker_authority").all()).toEqual([]);
		expect(raw.query("SELECT * FROM broker_cutovers").all()).toEqual([]);
	});

	test("established authority checks isolate unrelated corrupt history while inspection remains fail-closed", async () => {
		const { database, raw } = await fixture();
		database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated));
		database.transaction((tx) => database.bindings.recordOwnedBinding(tx, binding()));
		seedLane(database);
		raw.exec("UPDATE lane_jobs SET record_json = '{'");
		expect(() => database.bindings.assertAuthority(GLOBAL)).not.toThrow();
		expect(() => database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, () => database.inspectBrokerAuthority().populated))).not.toThrow();
		expect(database.bindings.assertOwnedSession(SESSION, "/work", GLOBAL)).toEqual(binding());
		expect(() => database.bindings.assertAuthority(PRIVATE)).toThrow("authority_mismatch");
		expect(() => database.bindings.assertOwnedSession(NEXT_SESSION, "/work", GLOBAL)).toThrow("unowned_session");
		expect(() => database.inspectBrokerAuthority()).toThrow();
		expect(() =>
			database.transaction((tx) => database.bindings.cutoverBrokerAuthority(tx, {
				expectedAuthority: GLOBAL,
				targetAuthority: PRIVATE,
				evidence: "validate history",
				disposition: "quarantine",
			})),
		).toThrow();
		expect(database.work.laneJobJson("lanejob-authority")).toBe("{");
		expect(raw.query("SELECT * FROM broker_cutovers").all()).toEqual([]);
	});

	test("corrupt authority is rejected rather than initialized over", async () => {
		const { database, raw } = await fixture();
		database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated));
		raw.query("UPDATE broker_authority SET authority_key = ?").run("{");
		expect(() => database.inspectBrokerAuthority()).toThrow();
		expect(() => database.transaction((tx) => database.bindings.initializeAuthority(tx, GLOBAL, database.inspectBrokerAuthority().populated))).toThrow();
	});
});
