import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, originKey } from "@gajae-gateway/protocol";
import { appendAttempt, closeAttempt, createLaneJobRecord } from "@gajae-gateway/subsession";
import { buildDeliveryPayload } from "../src/delivery/delivery";
import { setContextFloor } from "../src/orchestrator/persona-session";
import { Store } from "../src/store";
import type { WorkAttemptAdmission, WorkAttemptRuntime, WorkAttemptSettlement } from "../src/store/groups/work";
import { workAttemptDeliveryId, workAttemptReportId } from "../src/store/groups/work";
import type { WriteTx } from "../src/store/tx";

function touchWorkSession(database: Store, tx: WriteTx, runtime: WorkAttemptRuntime, at: string): void {
	database.bindings.touchWorkSession(
		tx,
		runtime.sessionKey,
		runtime.sessionId,
		runtime.epoch,
		at,
		JSON.stringify({
			platform: "work",
			kind: "task",
			conversationId: runtime.sessionKey.slice("work/task/".length),
		}),
	);
}

function prepareWorkAttempt(
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

function settleWorkAttempt(
	database: Store,
	tx: WriteTx,
	opRef: string,
	expectedVersion: number,
	record: ReturnType<typeof closeAttempt>,
	settlement: WorkAttemptSettlement,
	admission: WorkAttemptAdmission,
) {
	const personaKey = admission.kind === "persona" ? admission.row.originKey : undefined;
	const messageId = admission.kind === "persona" ? admission.row.messageId : undefined;
	const result = database.work.workAttemptSettle(tx, opRef, expectedVersion, record, settlement, admission, {
		...(personaKey !== undefined
			? { personaSessionExists: database.bindings.getSessionRecord(personaKey) !== undefined }
			: {}),
		...(messageId !== undefined ? { personaMessageExists: database.inbox.messageExists(messageId) } : {}),
	});
	if (!result) return undefined;
	touchWorkSession(database, tx, result.runtime, result.runtime.settledAt!);
	if (result.runtime.decision === "reported" && admission.kind === "persona") {
		if (
			!database.inbox.enqueue(tx, {
				messageId: admission.row.messageId,
				originKey: admission.row.originKey,
				originRefJson: admission.row.originRefJson,
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

async function withDatabase(operation: (database: Store, raw: Database) => void): Promise<void> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-store-composites-"));
	const path = join(directory, "gateway.db");
	const database = await Store.open(path);
	const raw = new Database(path);
	try {
		operation(database, raw);
	} finally {
		raw.close();
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
}

test("binding authority and session writes roll back together", async () => {
	await withDatabase((database, raw) => {
		const authority = { canonicalAgentDir: "/agent", identity: "gjc:/agent" };
		database.transaction((tx) =>
			database.bindings.initializeAuthority(tx, authority, database.inspectBrokerAuthority().populated),
		);
		raw.exec(
			`CREATE TRIGGER injected_session_failure BEFORE INSERT ON sessions BEGIN SELECT RAISE(ABORT, 'injected session failure'); END`,
		);
		expect(() =>
			database.transaction((tx) =>
				database.bindings.recordOwnedBinding(tx, {
					authority,
					sessionId: "session-binding-1",
					originKey: "discord/channel/composite",
					epoch: 0,
					repo: "/repo",
				}),
			),
		).toThrow("injected session failure");
		expect(
			raw.query("SELECT session_id FROM broker_owned_bindings WHERE session_id = 'session-binding-1'").get(),
		).toBeNull();
		expect(
			raw.query("SELECT origin_key FROM sessions WHERE origin_key = 'discord/channel/composite'").get(),
		).toBeNull();
	});
});

test("/new session, context-floor and inbound-discard effects roll back together", async () => {
	await withDatabase((database, raw) => {
		const key = "loopback/loopback/loopback";
		database.transaction((tx) => database.bindings.bind(tx, key, "session-before-new"));
		database.transaction((tx) =>
			database.inbox.enqueue(tx, {
				messageId: "composite-pending-message",
				originKey: key,
				originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
				body: "pending",
				receivedAt: "2026-09-01T00:00:00.000Z",
			}),
		);
		expect(() =>
			database.transaction((tx) => {
				database.bindings.rotateEpoch(tx, key, JSON.stringify(LOOPBACK_ORIGIN));
				setContextFloor(database, tx, key, "2026-09-02T00:00:00.000Z");
				database.inbox.discardBefore(tx, key, "2026-09-02T00:00:00.000Z");
				throw new Error("injected reset failure");
			}),
		).toThrow("injected reset failure");
		expect(database.bindings.getSessionRecord(key)).toEqual({ sessionId: "session-before-new", epoch: 0 });
		expect(
			raw.query("SELECT state FROM inbound_messages WHERE message_id = 'composite-pending-message'").get(),
		).toMatchObject({
			state: "pending",
		});
		expect(raw.query("SELECT origin_key FROM conversation_context_state WHERE origin_key = ?").get(key)).toBeNull();
	});
});

test("server final-reply activity and recall writes roll back together", async () => {
	await withDatabase((database, raw) => {
		const key = originKey(LOOPBACK_ORIGIN);
		database.transaction((tx) => database.bindings.bind(tx, key, "session-final-reply"));
		expect(() =>
			database.transaction((tx) => {
				database.bindings.touch(tx, key, JSON.stringify(LOOPBACK_ORIGIN));
				database.inbox.addRecall(tx, key, JSON.stringify(LOOPBACK_ORIGIN), "user: hi\nassistant: hello");
				throw new Error("injected final-reply failure");
			}),
		).toThrow("injected final-reply failure");
		expect(raw.query("SELECT last_activity_at FROM sessions WHERE origin_key = ?").get(key)).toMatchObject({
			last_activity_at: null,
		});
		expect(raw.query("SELECT id FROM recall_snippets WHERE origin_key = ?").get(key)).toBeNull();
	});
});

test("monitor cron slot claim and event admission roll back together", async () => {
	await withDatabase((database, raw) => {
		const slotAt = "2026-10-01T00:00:00.000Z";
		raw.exec(
			`CREATE TRIGGER injected_monitor_failure BEFORE INSERT ON monitor_events BEGIN SELECT RAISE(ABORT, 'injected event failure'); END`,
		);
		expect(() =>
			database.transaction((tx) =>
				database.monitors.slotClaimWithEvent(tx, {
					monitorId: "composite-monitor",
					slotAt,
					eventId: "composite-slot-event",
					eventType: "cron.composite",
					payloadJson: "{}",
				}),
			),
		).toThrow("injected event failure");
		expect(
			raw
				.query("SELECT monitor_id FROM monitor_slots WHERE monitor_id = ? AND slot_at = ?")
				.get("composite-monitor", slotAt),
		).toBeNull();
		expect(raw.query("SELECT event_id FROM monitor_events WHERE event_id = 'composite-slot-event'").get()).toBeNull();
	});
});

test("work attempt preparation rolls back history, runtime and activity together", async () => {
	await withDatabase((database, raw) => {
		const startedAt = "2026-09-08T00:00:00.000Z";
		const sessionId = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
		const record = appendAttempt(
			createLaneJobRecord({
				jobId: "lanejob-composite",
				branch: "main",
				worktreePath: "/work",
				sessionId,
				now: () => new Date(startedAt),
			}),
			{ opRef: "gw-composite-work", sessionId, startedAt },
		);
		database.transaction((tx) => database.bindings.bindAtEpoch(tx, "work/task/composite", sessionId, 0));
		const runtime: WorkAttemptRuntime = {
			opRef: "gw-composite-work",
			jobId: record.jobId,
			laneKey: "work-composite",
			sessionKey: "work/task/composite",
			sessionId,
			epoch: 0,
			cwd: "/work",
			startedAt,
			mode: "start",
			sendPhase: "prepared",
			sendEvidence: null,
			terminal: null,
			output: { disposition: "pending", reads: 0, nextReadAt: null, excerpt: null, proof: null, knownSilence: null },
			parent: { kind: "persona", originKey: originKey(LOOPBACK_ORIGIN), origin: LOOPBACK_ORIGIN },
			reportId: workAttemptReportId(database.instanceId, record.jobId, "gw-composite-work"),
			wakeReportId: null,
			noticeHash: null,
			deliveryId: workAttemptDeliveryId(database.instanceId, record.jobId, "gw-composite-work"),
			decision: "undecided",
			settledAt: null,
			version: 0,
		};
		raw.exec(
			`CREATE TRIGGER injected_runtime_failure BEFORE INSERT ON work_attempt_runtime BEGIN SELECT RAISE(ABORT, 'injected runtime failure'); END`,
		);
		expect(() => database.transaction((tx) => prepareWorkAttempt(database, tx, runtime, record))).toThrow(
			"injected runtime failure",
		);
		expect(raw.query("SELECT job_id FROM lane_jobs WHERE job_id = ?").get(record.jobId)).toBeNull();
		expect(raw.query("SELECT op_ref FROM work_attempt_runtime WHERE op_ref = ?").get(runtime.opRef)).toBeNull();
		expect(
			raw.query("SELECT last_activity_at FROM sessions WHERE origin_key = 'work/task/composite'").get(),
		).toMatchObject({
			last_activity_at: null,
		});
	});
});

test("settling a persona work attempt and admitting inbound output roll back across groups", async () => {
	await withDatabase((database, raw) => {
		const startedAt = "2026-09-08T00:00:00.000Z";
		const settledAt = "2026-09-08T00:00:10.000Z";
		const sessionId = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
		const opRef = "gw-composite-settle";
		const sessionKey = "work/task/settle";
		const personaKey = originKey(LOOPBACK_ORIGIN);
		const record = appendAttempt(
			createLaneJobRecord({
				jobId: "lanejob-composite-settle",
				branch: "main",
				worktreePath: "/work",
				sessionId,
				now: () => new Date(startedAt),
			}),
			{ opRef, sessionId, startedAt },
		);
		database.transaction((tx) => database.bindings.bindAtEpoch(tx, sessionKey, sessionId, 0));
		database.transaction((tx) => database.bindings.bindAtEpoch(tx, personaKey, "persona-session", 0));
		const runtime: WorkAttemptRuntime = {
			opRef,
			jobId: record.jobId,
			laneKey: "work-settle",
			sessionKey,
			sessionId,
			epoch: 0,
			cwd: "/work",
			startedAt,
			mode: "start",
			sendPhase: "prepared",
			sendEvidence: null,
			terminal: null,
			output: { disposition: "pending", reads: 0, nextReadAt: null, excerpt: null, proof: null, knownSilence: null },
			parent: { kind: "persona", originKey: personaKey, origin: LOOPBACK_ORIGIN },
			reportId: workAttemptReportId(database.instanceId, record.jobId, opRef),
			wakeReportId: null,
			noticeHash: null,
			deliveryId: workAttemptDeliveryId(database.instanceId, record.jobId, opRef),
			decision: "undecided",
			settledAt: null,
			version: 0,
		};
		database.transaction((tx) => prepareWorkAttempt(database, tx, runtime, record));
		const closed = closeAttempt({ record, opRef, endState: "completed", endedAt: settledAt });
		const settlement: WorkAttemptSettlement = {
			terminal: {
				kind: "broker",
				observedAt: settledAt,
				reasonCode: "end_turn",
				status: { status: "terminal_ok", receiptState: "present", outcome: { reason: "end_turn" } },
			},
			output: { ...runtime.output, disposition: "unavailable" },
			decision: "report",
			settledAt,
		};
		const payload = buildDeliveryPayload(
			opRef,
			LOOPBACK_ORIGIN,
			"[lane settle] completed: output_unavailable",
			runtime.deliveryId,
		)!;
		const admission: WorkAttemptAdmission = {
			kind: "persona",
			row: {
				messageId: runtime.reportId,
				originKey: personaKey,
				originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
				body: payload.text,
				receivedAt: settledAt,
			},
			fallbackPayload: payload,
		};
		raw.exec(
			`CREATE TRIGGER injected_inbound_failure BEFORE INSERT ON inbound_messages BEGIN SELECT RAISE(ABORT, 'injected inbound failure'); END`,
		);
		expect(() =>
			database.transaction((tx) => settleWorkAttempt(database, tx, opRef, 0, closed, settlement, admission)),
		).toThrow("injected inbound failure");
		expect(database.work.workAttemptGet(opRef)).toMatchObject({ version: 0, settledAt: null, decision: "undecided" });
		expect(raw.query("SELECT record_json FROM lane_jobs WHERE job_id = ?").get(record.jobId)).toMatchObject({
			record_json: JSON.stringify(record),
		});
		expect(raw.query("SELECT last_activity_at FROM sessions WHERE origin_key = ?").get(sessionKey)).toMatchObject({
			last_activity_at: startedAt,
		});
		expect(raw.query("SELECT message_id FROM inbound_messages WHERE message_id = ?").get(runtime.reportId)).toBeNull();
	});
});
