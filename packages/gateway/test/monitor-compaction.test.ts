import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { eventTypeOrigin, originKey } from "@gajaeway/protocol";
import { DeliveryService } from "../src/delivery/delivery";
import {
	buildMonitorCompactionDigest,
	MONITOR_DIGEST_MAX_LENGTH,
	MONITOR_DIGEST_MAX_NOTES,
	MONITOR_SESSION_TURN_LIMIT,
} from "../src/monitors/compaction";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

/**
 * Issue #68: monitor authoring sessions never counted a turn, so they grew until
 * the turn returned empty text and dispatch settled as internal_error. These
 * tests simulate the real 10-minute monitor cadence over more than a day.
 *
 * Harness shape mirrors monitor-instruction.test.ts, with a gjc port that also
 * accumulates a per-session transcript so "bounded context" is measured rather
 * than asserted.
 */
async function harness(directory: string, sessionTurnLimit?: number) {
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const turns: Array<{ sessionId: string; prompt: string }> = [];
	const pipeline = new MonitorPropagator({
		database,
		registry,
		gjc: {
			// One session id per epoch: exactly what a real bind does, so a roll is
			// observable as a new transcript.
			ensureSession: async (_key: string, epoch = 0) => ({ sessionId: `event-session-e${epoch}` }),
			forgetRebinds: () => {},
			sendTurn: async (sessionId: string, text: string) => {
				turns.push({ sessionId, prompt: text });
				// The event array is the tail after the response-contract marker, so a
				// prompt prefix (guidance, compaction digest) can never be misparsed.
				const marker = "entry per event: ";
				const payload = text.slice(text.indexOf(marker) + marker.length);
				return JSON.stringify(
					(JSON.parse(payload) as Array<{ eventId: string }>).map(({ eventId }) => ({
						eventId,
						note: `note for ${eventId}`,
					})),
				);
			},
		},
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
		...(sessionTurnLimit === undefined ? {} : { sessionTurnLimit }),
	});
	return { database, registry, pipeline, turns };
}

test("buildMonitorCompactionDigest carries the contract plus bounded recent notes", () => {
	const digest = buildMonitorCompactionDigest({
		monitorName: "deploy-watch",
		instruction: "Post the oldest blocked release with its owner.",
		notes: Array.from({ length: MONITOR_DIGEST_MAX_NOTES + 4 }, (_, index) => ({
			eventType: "deploy.blocked",
			firedAt: `2026-08-30T0${index}:00:00.000Z`,
			note: `note ${index}`,
		})),
	});
	expect(digest).toContain("Context compaction");
	expect(digest).toContain("Monitor: deploy-watch");
	expect(digest).toContain("Standing instruction: Post the oldest blocked release with its owner.");
	// Newest notes only, oldest dropped.
	expect(digest).toContain("note 0");
	expect(digest).toContain(`note ${MONITOR_DIGEST_MAX_NOTES - 1}`);
	expect(digest).not.toContain(`note ${MONITOR_DIGEST_MAX_NOTES}`);
	expect(digest.split("\n").filter((line) => line.startsWith("- 2026-"))).toHaveLength(MONITOR_DIGEST_MAX_NOTES);
	// A digest line must never look like the JSON array the turn has to answer with.
	expect(digest).not.toContain("[");
});

test("the digest stays under its ceiling even with maximal instruction and notes", () => {
	const digest = buildMonitorCompactionDigest({
		monitorName: "x".repeat(500),
		instruction: "i".repeat(4000),
		notes: Array.from({ length: MONITOR_DIGEST_MAX_NOTES }, (_, index) => ({
			eventType: "e".repeat(200),
			firedAt: "2026-08-30T00:00:00.000Z",
			note: "n".repeat(5000) + String(index),
		})),
	});
	expect(digest.length).toBeLessThanOrEqual(MONITOR_DIGEST_MAX_LENGTH);
	// The contract survives truncation; notes are what gets dropped.
	expect(digest).toContain("Context compaction");
	expect(digest).toContain("Standing instruction:");
});

test("a monitor without an instruction produces a digest with no instruction line", () => {
	const digest = buildMonitorCompactionDigest({ monitorName: "plain", instruction: undefined, notes: [] });
	expect(digest).not.toContain("Standing instruction");
	expect(digest).toContain("Recent authored notes: none yet.");
});

test("a 10-minute monitor over 25 hours stays bounded, rolls on the threshold, and keeps authoring valid JSON", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-compaction-day-"));
	try {
		const limit = 6;
		const { database, registry, pipeline, turns } = await harness(directory, limit);
		const monitor = registry.add({
			name: "heartbeat",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["heartbeat.tick"],
			burstPolicy: "serialize",
			instruction: "Report the oldest unacknowledged alert.",
		});
		const sessionKey = originKey(eventTypeOrigin("heartbeat.tick"));
		// 25 hours at one tick per 10 minutes.
		const ticks = 150;
		const eventIds: string[] = [];
		for (let tick = 0; tick < ticks; tick += 1) {
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "heartbeat.tick", { tick }));
			// The counter is bounded at every step, not just at the end.
			expect(database.sessionTurnCount(sessionKey)).toBeLessThanOrEqual(limit);
		}
		expect(turns).toHaveLength(ticks);
		// Rolls happen on the threshold: `limit` turns per session, nothing more.
		const perSession = new Map<string, number>();
		for (const turn of turns) perSession.set(turn.sessionId, (perSession.get(turn.sessionId) ?? 0) + 1);
		expect(Math.max(...perSession.values())).toBe(limit);
		expect(perSession.size).toBe(Math.ceil(ticks / limit));
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(Math.floor((ticks - 1) / limit));
		// Context is bounded: the per-session transcript never exceeds `limit`
		// prompts, so prefill cannot grow with the monitor's lifetime.
		const bytesPerSession = new Map<string, number>();
		for (const turn of turns)
			bytesPerSession.set(turn.sessionId, (bytesPerSession.get(turn.sessionId) ?? 0) + turn.prompt.length);
		const longestPrompt = Math.max(...turns.map((turn) => turn.prompt.length));
		expect(Math.max(...bytesPerSession.values())).toBeLessThanOrEqual(limit * longestPrompt);
		// Every authoring response stayed valid: an output per event, terminal stage.
		for (const eventId of eventIds) expect(database.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		const stages = new Set(
			database.monitorEventRows(monitor.monitorId).map((row) => `${row.event_id}:${row.stage}`.split(":")[1]),
		);
		expect([...stages]).toEqual(["authored_no_delivery"]);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("no event is lost or authored twice across a roll boundary", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-compaction-boundary-"));
	try {
		const limit = 3;
		const { database, registry, pipeline, turns } = await harness(directory, limit);
		const monitor = registry.add({
			name: "boundary",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["boundary.tick"],
			burstPolicy: "serialize",
		});
		const eventIds: string[] = [];
		for (let tick = 0; tick < limit * 4; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "boundary.tick", { tick }));
		// A reconcile sweep across the boundary must not re-author anything.
		await pipeline.reconcile();
		for (const eventId of eventIds) {
			// Only the event PAYLOAD counts as an authoring request; the digest of a
			// rolled session legitimately quotes older notes.
			const requested = turns.filter((turn) => turn.prompt.includes(`"eventId":"${eventId}"`));
			expect(requested).toHaveLength(1);
			expect(database.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		}
		expect(turns).toHaveLength(eventIds.length);
		expect(database.monitorEventRows(monitor.monitorId)).toHaveLength(eventIds.length);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("the first prompt of a rolled session carries the digest, earlier prompts do not", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-compaction-digest-"));
	try {
		const limit = 2;
		const { database, registry, pipeline, turns } = await harness(directory, limit);
		const instruction = "Summarise the deploy queue in one line.";
		const monitor = registry.add({
			name: "digest-carrier",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["digest.tick"],
			burstPolicy: "serialize",
			instruction,
		});
		const firstIds: string[] = [];
		for (let tick = 0; tick < limit; tick += 1)
			firstIds.push(await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick }));
		// Pre-roll prompts keep the exact original shape.
		for (const turn of turns) {
			expect(turn.prompt.startsWith(`Author monitor events. ${instruction} Respond ONLY with a JSON array`)).toBe(true);
			expect(turn.prompt).not.toContain("Context compaction");
		}
		await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: limit });
		const rolled = turns.at(-1)!;
		expect(rolled.sessionId).toBe("event-session-e1");
		expect(rolled.prompt).toContain("Context compaction");
		// (a) the instruction contract and (b) the recent authored notes.
		expect(rolled.prompt).toContain(`Standing instruction: ${instruction}`);
		for (const eventId of firstIds) expect(rolled.prompt).toContain(`note for ${eventId}`);
		// The response contract is still the last thing in the prompt.
		expect(rolled.prompt).toContain('Respond ONLY with a JSON array containing exactly one {"eventId","note"}');
		// The rolled turn itself still authored its own event.
		const rolledEventId = rolled.prompt.match(/"eventId":"([^"]+)"/)![1]!;
		expect(database.authoredOutput(rolledEventId)).toBe(`note for ${rolledEventId}`);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a monitor with no instruction rolls with a digest that omits the instruction line", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-compaction-noinstr-"));
	try {
		const limit = 2;
		const { database, registry, pipeline, turns } = await harness(directory, limit);
		const monitor = registry.add({
			name: "plain",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["plain.tick"],
			burstPolicy: "serialize",
		});
		for (let tick = 0; tick <= limit; tick += 1)
			await pipeline.submitAwaitable(monitor.monitorId, "plain.tick", { tick });
		const rolled = turns.at(-1)!;
		expect(rolled.prompt).toContain("Context compaction");
		expect(rolled.prompt).not.toContain("Standing instruction");
		expect(rolled.prompt.startsWith("Author monitor events.\nContext compaction")).toBe(true);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("the default monitor ceiling is separate from the chat path and low enough for a daily monitor", () => {
	// A 10-minute monitor authors ~144 turns a day; the ceiling must roll several
	// times within that, and must not be the chat-path 50.
	expect(MONITOR_SESSION_TURN_LIMIT).toBeLessThan(50);
	expect(144 / MONITOR_SESSION_TURN_LIMIT).toBeGreaterThan(2);
});
