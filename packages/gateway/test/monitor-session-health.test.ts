import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { eventTypeOrigin, originKey } from "@gajaeway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import {
	buildMonitorSessionDigest,
	EmptyAuthoringResponseError,
	isContextExhaustionFailure,
	MONITOR_CONTEXT_FAILURE_THRESHOLD,
	MONITOR_DIGEST_MAX_LENGTH,
	MONITOR_DIGEST_MAX_NOTES,
} from "../src/monitors/session-health";
import { GjcRuntimeError } from "../src/orchestrator/rebind";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

/**
 * Issue #68. gjc owns compaction (its `-p --mode json` path runs through
 * AgentSession.prompt() with compaction enabled), so the gateway must NOT roll a
 * monitor session on a turn ceiling. It observes health and falls back only on
 * proven compaction failure: consecutive context-family authoring failures.
 *
 * Harness shape mirrors monitor-instruction.test.ts, with a scripted gjc port so
 * each turn's outcome (healthy / context-family failure / malformed response) is
 * chosen per turn.
 */
type TurnOutcome = "ok" | "context_too_large" | "empty" | "bad_json" | "runtime_other";

async function harness(directory: string, outcome: (turn: number) => TurnOutcome, contextFailureThreshold?: number) {
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const turns: Array<{ sessionId: string; prompt: string }> = [];
	const pipeline = new MonitorPropagator({
		database,
		registry,
		gjc: {
			// One session id per epoch, exactly as a real bind behaves, so a roll is
			// observable as a new transcript.
			ensureSession: async (_key: string, epoch = 0) => ({ sessionId: `event-session-e${epoch}` }),
			forgetRebinds: () => {},
			sendTurn: async (sessionId: string, text: string) => {
				const index = turns.length;
				turns.push({ sessionId, prompt: text });
				switch (outcome(index)) {
					case "context_too_large":
						// The shape a real over-budget turn arrives in: a coded runtime error.
						throw new GjcRuntimeError("gjc turn exited 1: context_too_large: too long", {
							code: "context_too_large",
							message: "too long",
						});
					case "runtime_other":
						throw new GjcRuntimeError("gjc turn exited 1: tool_failed: nope", {
							code: "tool_failed",
							message: "nope",
						});
					case "empty":
						return "";
					case "bad_json":
						return "I could not do that.";
					default: {
						const marker = "entry per event: ";
						const payload = text.slice(text.indexOf(marker) + marker.length);
						return JSON.stringify(
							(JSON.parse(payload) as Array<{ eventId: string }>).map(({ eventId }) => ({
								eventId,
								note: `note for ${eventId}`,
							})),
						);
					}
				}
			},
		},
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
		...(contextFailureThreshold === undefined ? {} : { contextFailureThreshold }),
	});
	return { database, registry, pipeline, turns };
}

function serializeMonitor(registry: MonitorRegistry, eventType: string, instruction?: string) {
	return registry.add({
		name: `monitor-${eventType}`,
		trigger: { kind: "cron", schedule: "*/10 * * * *" },
		eventTypes: [eventType],
		burstPolicy: "serialize",
		...(instruction === undefined ? {} : { instruction }),
	});
}

test("context-family failures are classified by code only", () => {
	expect(isContextExhaustionFailure(new GjcRuntimeError("x", { code: "context_too_large", message: "m" }))).toBe(true);
	expect(isContextExhaustionFailure(new EmptyAuthoringResponseError())).toBe(true);
	expect(isContextExhaustionFailure(new GjcRuntimeError("x", { code: "tool_failed", message: "m" }))).toBe(false);
	// No code, or a mere message resembling one, is NOT context exhaustion: wording
	// is not a contract, and guessing would roll healthy sessions.
	expect(isContextExhaustionFailure(new GjcRuntimeError("x", { message: "context_too_large" }))).toBe(false);
	expect(isContextExhaustionFailure(new Error("context_too_large"))).toBe(false);
});

test("buildMonitorSessionDigest carries the contract plus bounded recent notes", () => {
	const digest = buildMonitorSessionDigest({
		monitorName: "deploy-watch",
		instruction: "Post the oldest blocked release with its owner.",
		notes: Array.from({ length: MONITOR_DIGEST_MAX_NOTES + 4 }, (_, index) => ({
			eventType: "deploy.blocked",
			firedAt: `2026-08-30T0${index}:00:00.000Z`,
			note: `note ${index}`,
		})),
	});
	expect(digest).toContain("Session recovery");
	expect(digest).toContain("Monitor: deploy-watch");
	expect(digest).toContain("Standing instruction: Post the oldest blocked release with its owner.");
	expect(digest).toContain("note 0");
	expect(digest).toContain(`note ${MONITOR_DIGEST_MAX_NOTES - 1}`);
	expect(digest).not.toContain(`note ${MONITOR_DIGEST_MAX_NOTES}`);
	expect(digest.split("\n").filter((line) => line.startsWith("- 2026-"))).toHaveLength(MONITOR_DIGEST_MAX_NOTES);
	// A digest line must never look like the JSON array the turn has to answer with.
	expect(digest).not.toContain("[");
});

test("the digest stays under its ceiling even with maximal instruction and notes", () => {
	const digest = buildMonitorSessionDigest({
		monitorName: "x".repeat(500),
		instruction: "i".repeat(4000),
		notes: Array.from({ length: MONITOR_DIGEST_MAX_NOTES }, (_, index) => ({
			eventType: "e".repeat(200),
			firedAt: "2026-08-30T00:00:00.000Z",
			note: "n".repeat(5000) + String(index),
		})),
	});
	expect(digest.length).toBeLessThanOrEqual(MONITOR_DIGEST_MAX_LENGTH);
	expect(digest).toContain("Session recovery");
	expect(digest).toContain("Standing instruction:");
});

test("a monitor without an instruction produces a digest with no instruction line", () => {
	const digest = buildMonitorSessionDigest({ monitorName: "plain", instruction: undefined, notes: [] });
	expect(digest).not.toContain("Standing instruction");
	expect(digest).toContain("Recent authored notes: none yet.");
});

test("a healthy 10-minute monitor is never rolled over 25 hours, and its turns are recorded", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-health-healthy-"));
	try {
		const { database, registry, pipeline, turns } = await harness(directory, () => "ok");
		const monitor = serializeMonitor(registry, "heartbeat.tick", "Report the oldest unacknowledged alert.");
		const sessionKey = originKey(eventTypeOrigin("heartbeat.tick"));
		const ticks = 150; // 25 hours at one tick per 10 minutes
		const eventIds: string[] = [];
		for (let tick = 0; tick < ticks; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "heartbeat.tick", { tick }));
		// gjc owns compaction: a working session keeps its context, whatever the count.
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		// Turn count is observability and it is actually recorded (it used to sit at
		// 0 forever because the authoring path never counted a turn).
		expect(database.sessionTurnCount(sessionKey)).toBe(ticks);
		expect(turns.every((turn) => !turn.prompt.includes("Session recovery"))).toBe(true);
		for (const eventId of eventIds) expect(database.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("two consecutive context failures roll the session and seed the new one with the digest", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-health-roll-"));
	try {
		// Turns 0-1 healthy (so there are notes to carry), turns 2-3 context-family.
		const { database, registry, pipeline, turns } = await harness(
			directory,
			(turn) => (turn === 2 || turn === 3 ? "context_too_large" : "ok"),
			2,
		);
		const instruction = "Summarise the deploy queue in one line.";
		const monitor = serializeMonitor(registry, "digest.tick", instruction);
		const sessionKey = originKey(eventTypeOrigin("digest.tick"));
		const healthyIds: string[] = [];
		for (let tick = 0; tick < 2; tick += 1)
			healthyIds.push(await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick }));
		// First context failure: recorded, but NOT rolled — one failure is not proof.
		await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 2 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		// Second consecutive context failure reaches the threshold.
		const secondFailureId = await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 3 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		// Public-safe evidence: coded failure, no raw runtime text.
		const failure = database.monitorFailure(secondFailureId);
		expect(failure?.code).toBe("authoring_context_exhausted");
		expect(failure?.detail).not.toContain("too long");
		// The roll fires on the NEXT dispatch, which is also the turn that carries
		// the digest into the fresh session.
		await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 4 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		const rolled = turns.at(-1)!;
		expect(rolled.sessionId).toBe("event-session-e1");
		expect(rolled.prompt).toContain("Session recovery");
		expect(rolled.prompt).toContain(`Standing instruction: ${instruction}`);
		for (const eventId of healthyIds) expect(rolled.prompt).toContain(`note for ${eventId}`);
		expect(rolled.prompt).toContain('Respond ONLY with a JSON array containing exactly one {"eventId","note"}');
		const rolledEventId = rolled.prompt.match(/"eventId":"([^"]+)"/)![1]!;
		expect(database.authoredOutput(rolledEventId)).toBe(`note for ${rolledEventId}`);
		// One roll only: the healthy turn cleared the streak.
		await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 5 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(turns.at(-1)!.prompt).not.toContain("Session recovery");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("empty authoring responses count as context-family evidence and reach the same threshold", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-health-empty-"));
	try {
		const { database, registry, pipeline, turns } = await harness(directory, (turn) => (turn < 2 ? "empty" : "ok"), 2);
		const monitor = serializeMonitor(registry, "empty.tick");
		const sessionKey = originKey(eventTypeOrigin("empty.tick"));
		const first = await pipeline.submitAwaitable(monitor.monitorId, "empty.tick", { tick: 0 });
		expect(database.monitorFailure(first)?.code).toBe("authoring_context_exhausted");
		await pipeline.submitAwaitable(monitor.monitorId, "empty.tick", { tick: 1 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		await pipeline.submitAwaitable(monitor.monitorId, "empty.tick", { tick: 2 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		// No instruction on this monitor: the digest keeps its other sections.
		const rolled = turns.at(-1)!;
		expect(rolled.prompt).toContain("Session recovery");
		expect(rolled.prompt).not.toContain("Standing instruction");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("non-context failures never roll the session, however often they repeat", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-health-badjson-"));
	try {
		const { database, registry, pipeline, turns } = await harness(directory, (turn) =>
			turn % 2 === 0 ? "bad_json" : "runtime_other",
		);
		const monitor = serializeMonitor(registry, "malformed.tick");
		const sessionKey = originKey(eventTypeOrigin("malformed.tick"));
		const ids: string[] = [];
		for (let tick = 0; tick < 10; tick += 1)
			ids.push(await pipeline.submitAwaitable(monitor.monitorId, "malformed.tick", { tick }));
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		// Recorded as response/turn defects, never as context exhaustion.
		const codes = new Set(ids.map((id) => database.monitorFailure(id)?.code));
		expect(codes.has("authoring_context_exhausted")).toBe(false);
		expect([...codes].sort()).toEqual(["authoring_response_invalid", "authoring_turn_failed"]);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a non-context failure between two context failures resets the streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-health-interleaved-"));
	try {
		// context, bad_json, context, context → the roll may only fire after the
		// second consecutive pair, i.e. on the dispatch after turn 3.
		const script: TurnOutcome[] = ["context_too_large", "bad_json", "context_too_large", "ok"];
		const { database, registry, pipeline } = await harness(directory, (turn) => script[turn] ?? "ok", 2);
		const monitor = serializeMonitor(registry, "interleaved.tick");
		const sessionKey = originKey(eventTypeOrigin("interleaved.tick"));
		for (let tick = 0; tick < 4; tick += 1)
			await pipeline.submitAwaitable(monitor.monitorId, "interleaved.tick", { tick });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("no event is lost or authored twice across a fallback roll boundary", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-health-boundary-"));
	try {
		const { database, registry, pipeline, turns } = await harness(
			directory,
			(turn) => (turn === 1 || turn === 2 ? "context_too_large" : "ok"),
			2,
		);
		const monitor = serializeMonitor(registry, "boundary.tick");
		const sessionKey = originKey(eventTypeOrigin("boundary.tick"));
		const eventIds: string[] = [];
		for (let tick = 0; tick < 6; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "boundary.tick", { tick }));
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		// A reconcile sweep across the boundary re-authors the two failed events and
		// nothing else: no event is dropped, none is authored twice.
		await pipeline.reconcile();
		for (const eventId of eventIds) {
			// Only the event PAYLOAD counts as an authoring request; a rolled session's
			// digest legitimately quotes older notes.
			const requested = turns.filter((turn) => turn.prompt.includes(`"eventId":"${eventId}"`));
			expect(requested.length).toBeGreaterThanOrEqual(1);
			expect(database.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		}
		// Exactly one authored output and one durable row per event.
		expect(database.monitorEventRows(monitor.monitorId)).toHaveLength(eventIds.length);
		const authoredOnce = eventIds.filter((id) => database.authoredOutput(id) !== undefined);
		expect(authoredOnce).toHaveLength(eventIds.length);
		// Every event settled terminally; none is stuck mid-dispatch.
		for (const row of database.monitorEventRows(monitor.monitorId))
			expect(["authored", "authored_no_delivery", "delivered"]).toContain(row.stage);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("the default threshold demands a run of failures, not a single blip", () => {
	expect(MONITOR_CONTEXT_FAILURE_THRESHOLD).toBeGreaterThan(1);
});
