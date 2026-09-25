import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { eventTypeOrigin, originKey } from "@gajae-gateway/protocol";
import { GjcCliError } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import {
	buildMonitorCompactionDigest,
	type CompactionPort,
	classifyAuthoringFailure,
	classifyExecutorFailure,
	decideSessionRoll,
	isAsideTimeoutFailure,
	isOrphanedExecutorFailure,
	MONITOR_BUSY_FAILURE_ROLL_THRESHOLD,
	MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD,
	MONITOR_DIGEST_MAX_LENGTH,
	MONITOR_DIGEST_MAX_NOTES,
	type NativeCompactionStatus,
	unavailableCompactionPort,
} from "../src/monitors/compaction";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { sessionPortFromScript } from "./session-port.fake";

/**
 * Issue #68, final design: native gjc auto-compaction is the primary defence
 * (the `-p --mode json` path goes through AgentSession.prompt()), and this
 * module is the SAFETY NET for the case where it silently stops working.
 *
 * So the load-bearing assertions here are as much about what does NOT happen —
 * a healthy monitor is never rolled, no matter how many turns it takes — as
 * about the last-resort roll itself.
 */

/** A stub port with a scripted status, recording the sessions it was asked about. */
function stubPort(status: NativeCompactionStatus): CompactionPort & { calls: string[] } {
	const calls: string[] = [];
	return {
		calls,
		run: async (sessionId: string) => {
			calls.push(sessionId);
			return { status };
		},
	};
}

/** Valid authoring response for the events carried in `prompt`. */
function echoNotes(prompt: string): string {
	// The event array is the tail after the response-contract marker, so a prompt
	// prefix (guidance, compaction digest) can never be misparsed.
	const marker = "entry per event: ";
	const payload = prompt.slice(prompt.indexOf(marker) + marker.length);
	return JSON.stringify(
		(JSON.parse(payload) as Array<{ eventId: string }>).map(({ eventId }) => ({
			eventId,
			note: `note for ${eventId}`,
		})),
	);
}

async function harness(
	directory: string,
	options: {
		contextFailureRollThreshold?: number;
		protocolFailureRollThreshold?: number;
		compaction?: CompactionPort;
		/** Per-turn response. Defaults to a valid note per claimed event. */
		respond?: (prompt: string, index: number) => string;
		/**
		 * When true for a session, the runtime refuses the prompt with the exact
		 * `busy` envelope the real port surfaces after its bounded busy wait: the
		 * turn never starts, so it is not recorded in `turns`.
		 */
		busy?: (sessionId: string) => boolean;
		/** Propagator clock; reconcile reclaims a `failed` slot only after its retry backoff (#179). */
		now?: () => number;
	} = {},
) {
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const turns: Array<{ sessionId: string; prompt: string }> = [];
	const sessionPort = sessionPortFromScript({
		// One session id per epoch: exactly what a real bind does, so a roll is
		// observable as a new transcript.
		bind: async (_key: string, epoch = 0) => ({ sessionId: `event-session-e${epoch}` }),
		respond: async (sessionId: string, text: string) => {
			const index = turns.length;
			turns.push({ sessionId, prompt: text });
			return options.respond ? options.respond(text, index) : echoNotes(text);
		},
	});
	const request = sessionPort.request.bind(sessionPort);
	sessionPort.request = async (input) => {
		if (options.busy?.(input.sessionId))
			throw new GjcCliError("gjc sdk turn.prompt reported failure", 0, "", { code: "busy" });
		return request(input);
	};
	const pipeline = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
		...(options.compaction === undefined ? {} : { compaction: options.compaction }),
		...(options.contextFailureRollThreshold === undefined
			? {}
			: { contextFailureRollThreshold: options.contextFailureRollThreshold }),
		...(options.protocolFailureRollThreshold === undefined
			? {}
			: { protocolFailureRollThreshold: options.protocolFailureRollThreshold }),
		...(options.now ? { now: options.now } : {}),
	});
	return { database, registry, pipeline, turns, sessionPort };
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

test("the default compaction port is honest about doing nothing", async () => {
	expect((await unavailableCompactionPort.run("event-session-e0")).status).toBe("unavailable");
});

test("authoring failures split across exactly three axes, and only context evidence is context", () => {
	for (const message of [
		"authoring response is empty",
		"session send failed: context_too_large",
		"maximum context length exceeded",
		"prompt is too long for this model",
		"provider returned a 0-token completion",
	])
		expect(classifyAuthoringFailure(new Error(message))).toBe("context");
	for (const message of [
		"authoring response is not an array",
		"Unexpected token < in JSON at position 0",
		"JSON Parse error: Unexpected identifier",
		"authoring response omits event abc",
		"authoring response duplicates event abc",
	])
		expect(classifyAuthoringFailure(new Error(message))).toBe("protocol");
	for (const message of [
		"aside collection worker timed out after 300s",
		"aside_timeout",
		"external tool failed: gh exited 1",
		"memory lock held, run skipped",
		"session bind failed: socket closed",
		"something nobody has seen before",
		"aside exec collection exceeded 360s; child killed but Aside daemon task still running",
	])
		expect(classifyAuthoringFailure(new Error(message))).toBe("executor");
	// The aside worker's cap is recognised as its own executor failure, and is
	// never allowed to look like context exhaustion even when the message also
	// mentions a long prompt.
	expect(isAsideTimeoutFailure(new Error("aside collection timed out after 300000ms"))).toBe(true);
	expect(isAsideTimeoutFailure(new Error("context_too_large"))).toBe(false);
	expect(classifyAuthoringFailure(new Error("aside collection worker timed out after 300s; prompt is too long"))).toBe(
		"executor",
	);
});

test("the executor class names its sub-kinds, with an orphaned external run outranking the timeout", () => {
	// The drill shape: the child died on the wrapper's cap, the daemon job did
	// not, and the next tick would stack another one.
	for (const message of [
		"aside exec collection exceeded 360s; child killed but Aside daemon task still running",
		"child process terminated at 300s, external work still running",
		"orphaned_executor: aside session left open",
		"wrapper killed the child; daemon job is still active",
	]) {
		expect(isOrphanedExecutorFailure(new Error(message))).toBe(true);
		expect(classifyExecutorFailure(new Error(message))).toBe("executor_orphaned_external_work");
		// Still executor class: the class axis stays at three.
		expect(classifyAuthoringFailure(new Error(message))).toBe("executor");
	}
	// A plain timeout with nothing left behind is a timeout, not an orphan.
	expect(isOrphanedExecutorFailure(new Error("aside collection worker timed out after 300000ms"))).toBe(false);
	expect(classifyExecutorFailure(new Error("aside collection worker timed out after 300000ms"))).toBe(
		"executor_timeout",
	);
	expect(classifyExecutorFailure(new Error("external tool failed: gh exited 1"))).toBe("executor_failed");
	// And an orphan report that also mentions context stays executor: a stacked
	// external job is not evidence about this session's context.
	expect(classifyAuthoringFailure(new Error("child killed but daemon task still running; prompt is too long"))).toBe(
		"executor",
	);
});

test("the roll decision needs both the failure streak and a non-successful native compaction", () => {
	const threshold = MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD;
	// Below the streak, nothing rolls — whatever native compaction reported.
	for (const nativeCompaction of ["unavailable", "failed", "skipped", "succeeded"] as const)
		expect(
			decideSessionRoll({ consecutiveContextFailures: threshold - 1, threshold, nativeCompaction }),
		).toBeUndefined();
	// At the streak, a native compaction that did not help names the reason.
	expect(decideSessionRoll({ consecutiveContextFailures: threshold, threshold, nativeCompaction: "unavailable" })).toBe(
		"context_failures_native_compaction_unavailable",
	);
	expect(decideSessionRoll({ consecutiveContextFailures: threshold, threshold, nativeCompaction: "failed" })).toBe(
		"context_failures_native_compaction_failed",
	);
	expect(decideSessionRoll({ consecutiveContextFailures: threshold, threshold, nativeCompaction: "skipped" })).toBe(
		"context_failures_native_compaction_skipped",
	);
	// Native compaction succeeded: the session was repaired, so it is kept.
	expect(
		decideSessionRoll({ consecutiveContextFailures: threshold * 10, threshold, nativeCompaction: "succeeded" }),
	).toBeUndefined();
});

test("a healthy 10-minute monitor is never rolled, however far past any turn threshold it runs", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-healthy-"));
	try {
		const compaction = stubPort("unavailable");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
		});
		const monitor = registry.add({
			name: "heartbeat",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["heartbeat.tick"],
			burstPolicy: "serialize",
			instruction: "Report the oldest unacknowledged alert.",
		});
		const sessionKey = originKey(eventTypeOrigin("heartbeat.tick"));
		// 25 hours at one tick per 10 minutes — far past the 24 the old
		// implementation rolled on, and past the chat path's 50.
		const ticks = 150;
		const eventIds: string[] = [];
		for (let tick = 0; tick < ticks; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "heartbeat.tick", { tick }));
		expect(turns).toHaveLength(ticks);
		// One session, one epoch: not a single roll while the monitor was answering.
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		// Native compaction was never even asked: nothing failed.
		expect(compaction.calls).toHaveLength(0);
		// Turn count is still recorded — it is the observational signal.
		expect(database.sessionTurnCount(sessionKey)).toBe(ticks);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.turns).toBe(ticks);
		expect(state.contextFailures).toBe(0);
		expect(state.lastRoll).toBeUndefined();
		// And every turn authored its event.
		for (const eventId of eventIds) expect(database.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("consecutive context failures with native compaction unavailable roll, and the digest carries continuity", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-rolls-"));
	try {
		const compaction = stubPort("unavailable");
		let empty = false;
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			// An empty authoring response is exactly what the production incident
			// looked like: the session overflowed and answered with nothing.
			respond: (prompt) => (empty ? "" : echoNotes(prompt)),
		});
		const instruction = "Summarise the deploy queue in one line.";
		const monitor = registry.add({
			name: "digest-carrier",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["digest.tick"],
			burstPolicy: "serialize",
			instruction,
		});
		const sessionKey = originKey(eventTypeOrigin("digest.tick"));
		const healthyIds: string[] = [];
		for (let tick = 0; tick < 3; tick += 1)
			healthyIds.push(await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick }));
		// Pre-failure prompts keep the exact original shape.
		for (const turn of turns) {
			expect(turn.prompt.startsWith(`Author monitor events. ${instruction} Respond ONLY with a JSON array`)).toBe(true);
			expect(turn.prompt).not.toContain("Context compaction");
		}
		empty = true;
		const failedIds: string[] = [];
		for (let tick = 0; tick < 2; tick += 1)
			failedIds.push(await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 100 + tick }));
		// One failure is not enough; two in a row with an unavailable port arms the net.
		expect(compaction.calls).toEqual(["event-session-e0", "event-session-e0"]);
		for (const eventId of failedIds) {
			const row = database.monitorEventRows(monitor.monitorId).find((candidate) => candidate.event_id === eventId);
			expect(row?.stage).toBe("failed");
			expect(database.monitorFailure(eventId)?.code).toBe("authoring_context_exhausted");
		}
		const armed = pipeline.sessionSafetyState(sessionKey);
		expect(armed.contextFailures).toBe(2);
		expect(armed.nativeCompaction).toBe("unavailable");
		expect(armed.pendingRoll).toBe("context_failures_native_compaction_unavailable");
		// Still epoch 0: the roll happens at the next dispatch boundary, never
		// mid-batch.
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		empty = false;
		await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 200 });
		const rolled = turns.at(-1)!;
		expect(rolled.sessionId).toBe("event-session-e1");
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(rolled.prompt).toContain("Context compaction");
		// The digest carries (a) the standing contract and (b) recent authored notes.
		expect(rolled.prompt).toContain(`Standing instruction: ${instruction}`);
		for (const eventId of healthyIds) expect(rolled.prompt).toContain(`note for ${eventId}`);
		// The response contract is still the last thing in the prompt.
		expect(rolled.prompt).toContain('Respond ONLY with a JSON array containing exactly one {"eventId","note"}');
		const rolledEventId = rolled.prompt.match(/"eventId":"([^"]+)"/)![1]!;
		expect(database.authoredOutput(rolledEventId)).toBe(`note for ${rolledEventId}`);
		// Armed state consumed exactly once: the next healthy turn does not roll again.
		const after = pipeline.sessionSafetyState(sessionKey);
		expect(after.pendingRoll).toBeUndefined();
		expect(after.lastRoll).toBe("context_failures_native_compaction_unavailable");
		expect(after.contextFailures).toBe(0);
		await pipeline.submitAwaitable(monitor.monitorId, "digest.tick", { tick: 201 });
		expect(turns.at(-1)!.sessionId).toBe("event-session-e1");
		expect(turns.at(-1)!.prompt).not.toContain("Context compaction");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a compaction port that reports success keeps the session, however long the failure streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-native-ok-"));
	try {
		const compaction = stubPort("succeeded");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			respond: () => "",
		});
		const monitor = registry.add({
			name: "native-ok",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["native.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("native.tick"));
		for (let tick = 0; tick < 6; tick += 1) await pipeline.submitAwaitable(monitor.monitorId, "native.tick", { tick });
		// Native compaction was asked every time and said it handled it, so the
		// safety net stays holstered.
		expect(compaction.calls).toHaveLength(6);
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.contextFailures).toBe(6);
		expect(state.nativeCompaction).toBe("succeeded");
		expect(state.pendingRoll).toBeUndefined();
		expect(state.lastRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a native compaction that fails outright rolls with its own structured reason", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-native-failed-"));
	try {
		let empty = true;
		const { database, registry, pipeline } = await harness(directory, {
			contextFailureRollThreshold: 1,
			compaction: stubPort("failed"),
			respond: (prompt) => (empty ? "" : echoNotes(prompt)),
		});
		const monitor = registry.add({
			name: "native-failed",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["nf.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("nf.tick"));
		await pipeline.submitAwaitable(monitor.monitorId, "nf.tick", { tick: 0 });
		expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("context_failures_native_compaction_failed");
		empty = false;
		await pipeline.submitAwaitable(monitor.monitorId, "nf.tick", { tick: 1 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(pipeline.sessionSafetyState(sessionKey).lastRoll).toBe("context_failures_native_compaction_failed");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

// Reversal of a former invariant ("a protocol-class failure never rolls"), on
// measured evidence: on the jip-gajae host 2026-09-02 an off-contract monitor
// session failed 19/19 ticks in one day while a sibling monitor on the same
// gateway delivered fine. Nothing remediated it and the answer text may not be
// logged, so it was both unfixable and undiagnosable. A protocol failure still
// NEVER feeds the context streak and never requests compaction — it now has its
// own streak and its own coded reason instead.
test("a protocol-class failure names the violated rule and rolls only after its own streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-badjson-"));
	try {
		const compaction = stubPort("unavailable");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			protocolFailureRollThreshold: 3,
			compaction,
			respond: () => "sure! here are your notes:",
		});
		const monitor = registry.add({
			name: "bad-json",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["bad.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("bad.tick"));
		const eventIds: string[] = [];
		for (let tick = 0; tick < 2; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "bad.tick", { tick }));
		// Below its own threshold nothing is touched: a single off-contract answer
		// is not proof the session is broken.
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		let state = pipeline.sessionSafetyState(sessionKey);
		expect(state.protocolFailures).toBe(2);
		expect(state.pendingRoll).toBeUndefined();
		// The coded sub-kind is the whole diagnosis, because the offending answer
		// text is never logged.
		expect(state.lastProtocolReason).toBe("protocol_unparseable_json");
		eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "bad.tick", { tick: 2 }));
		// Every attempt is still reported as a contract failure, and context-class
		// machinery is never engaged.
		for (const eventId of eventIds) {
			expect(database.monitorFailure(eventId)?.code).toBe("authoring_response_invalid");
		}
		expect(compaction.calls).toHaveLength(0);
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		state = pipeline.sessionSafetyState(sessionKey);
		expect(state.contextFailures).toBe(0);
		expect(state.executorFailures).toBe(0);
		expect(state.staleContextFailures).toBe(0);
		expect(state.pendingRoll).toBe("protocol_failures_off_contract");
		// The roll itself lands at the next dispatch boundary, never mid-batch.
		await pipeline.submitAwaitable(monitor.monitorId, "bad.tick", { tick: 3 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		expect(pipeline.sessionSafetyState(sessionKey).lastRoll).toBe("protocol_failures_off_contract");
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("no event is lost or authored twice across a roll boundary", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-boundary-"));
	try {
		let empty = false;
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction: stubPort("unavailable"),
			respond: (prompt) => (empty ? "" : echoNotes(prompt)),
		});
		const monitor = registry.add({
			name: "boundary",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["boundary.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("boundary.tick"));
		const healthy: string[] = [];
		for (let tick = 0; tick < 3; tick += 1)
			healthy.push(await pipeline.submitAwaitable(monitor.monitorId, "boundary.tick", { tick }));
		empty = true;
		const failed: string[] = [];
		for (let tick = 0; tick < 2; tick += 1)
			failed.push(await pipeline.submitAwaitable(monitor.monitorId, "boundary.tick", { tick: 100 + tick }));
		empty = false;
		// Dispatches straddling the roll, then the recovery sweep that re-authors
		// only the events that actually failed.
		for (let tick = 0; tick < 3; tick += 1)
			healthy.push(await pipeline.submitAwaitable(monitor.monitorId, "boundary.tick", { tick: 200 + tick }));
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		await pipeline.reconcile();
		// No loss: every event, including the two that failed, ends with its note.
		for (const eventId of [...healthy, ...failed]) expect(database.authoredOutput(eventId)).toBe(`note for ${eventId}`);
		// No duplicate: a successful event is requested exactly once, and a failed
		// one exactly twice (its failure plus its single recovery). Only the event
		// PAYLOAD counts as a request; a rolled session's digest legitimately
		// quotes older notes.
		for (const eventId of healthy)
			expect(turns.filter((turn) => turn.prompt.includes(`"eventId":"${eventId}"`))).toHaveLength(1);
		for (const eventId of failed)
			expect(turns.filter((turn) => turn.prompt.includes(`"eventId":"${eventId}"`))).toHaveLength(2);
		expect(database.monitorEventRows(monitor.monitorId)).toHaveLength(healthy.length + failed.length);
		expect(turns).toHaveLength(healthy.length + failed.length * 2);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a monitor with no instruction is unchanged, and rolls with a digest that omits the instruction line", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-noinstr-"));
	try {
		let empty = false;
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 1,
			compaction: stubPort("unavailable"),
			respond: (prompt) => (empty ? "" : echoNotes(prompt)),
		});
		const monitor = registry.add({
			name: "plain",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["plain.tick"],
			burstPolicy: "serialize",
		});
		await pipeline.submitAwaitable(monitor.monitorId, "plain.tick", { tick: 0 });
		// Unchanged prompt shape for an instruction-less monitor: no guidance, no
		// digest, no leading blank.
		expect(turns[0]!.prompt.startsWith("Author monitor events. Respond ONLY with a JSON array")).toBe(true);
		empty = true;
		await pipeline.submitAwaitable(monitor.monitorId, "plain.tick", { tick: 1 });
		empty = false;
		await pipeline.submitAwaitable(monitor.monitorId, "plain.tick", { tick: 2 });
		const rolled = turns.at(-1)!;
		expect(rolled.prompt).toContain("Context compaction");
		expect(rolled.prompt).not.toContain("Standing instruction");
		expect(rolled.prompt.startsWith("Author monitor events.\nContext compaction")).toBe(true);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a healthy answer resets the streak, so isolated context failures never roll", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-streak-reset-"));
	try {
		const compaction = stubPort("unavailable");
		let fail = false;
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			respond: (prompt) => {
				if (!fail) return echoNotes(prompt);
				throw new Error("session send failed: context_too_large");
			},
		});
		const monitor = registry.add({
			name: "flaky",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["flaky.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("flaky.tick"));
		// A single context_too_large, then a healthy turn — twice. The failures are
		// never consecutive, so nothing may arm.
		for (const tick of [0, 1, 2, 3]) {
			fail = tick % 2 === 0;
			await pipeline.submitAwaitable(monitor.monitorId, "flaky.tick", { tick });
			const midway = pipeline.sessionSafetyState(sessionKey);
			// Exactly one failure is on the books right after a failure, and the
			// following healthy answer wipes it.
			expect(midway.contextFailures).toBe(fail ? 1 : 0);
			expect(midway.pendingRoll).toBeUndefined();
		}
		// Both failures were treated as real context failures (native compaction
		// was asked each time) — they simply never became a streak.
		expect(compaction.calls).toHaveLength(2);
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(0);
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.contextFailures).toBe(0);
		expect(state.lastRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a context failure on a replayed stale event does not feed the current session's streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stale-"));
	try {
		const compaction = stubPort("unavailable");
		let mode: "ok" | "executor" | "context" = "executor";
		const { database, registry, pipeline, turns } = await harness(directory, {
			// 1 makes the assertion sharp: any COUNTED context failure would arm
			// immediately, so a still-disarmed net proves the stale failure was
			// dropped rather than merely under threshold.
			contextFailureRollThreshold: 1,
			compaction,
			respond: (prompt) => {
				if (mode === "ok") return echoNotes(prompt);
				if (mode === "executor") throw new Error("external tool failed: gh exited 1");
				throw new Error("session send failed: context_too_large");
			},
		});
		const monitor = registry.add({
			name: "stale",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["stale.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("stale.tick"));
		// Park an event at `failed` with an executor failure: retryable, and no
		// context evidence on the books.
		const staleId = await pipeline.submitAwaitable(monitor.monitorId, "stale.tick", { tick: 0 });
		expect(database.monitorFailure(staleId)?.code).toBe("executor_failed");
		expect(pipeline.sessionSafetyState(sessionKey).contextFailures).toBe(0);
		// Now the session starts answering with a context rejection, and reconcile
		// replays that old event into it.
		mode = "context";
		await pipeline.reconcile();
		const replayRow = database.monitorEventRows(monitor.monitorId).find((row) => row.event_id === staleId);
		expect(replayRow?.dispatch_attempts).toBeGreaterThan(0);
		expect(database.monitorFailure(staleId)?.code).toBe("authoring_context_exhausted");
		// The replayed failure is recorded, but as stale: no streak, no arm, no roll.
		const afterReplay = pipeline.sessionSafetyState(sessionKey);
		expect(afterReplay.staleContextFailures).toBe(1);
		expect(afterReplay.contextFailures).toBe(0);
		expect(afterReplay.pendingRoll).toBeUndefined();
		// No session row is even written when every turn throws, so "epoch 0" is
		// the absence of a roll; the bound session id says the same thing directly.
		expect(database.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		// A native compaction is never requested on behalf of a stale event either.
		expect(compaction.calls).toHaveLength(0);
		// The same failure from the session's OWN fresh work still arms: the guard
		// narrows the trigger, it does not disable it.
		await pipeline.submitAwaitable(monitor.monitorId, "stale.tick", { tick: 1 });
		const afterFresh = pipeline.sessionSafetyState(sessionKey);
		expect(afterFresh.contextFailures).toBe(1);
		expect(afterFresh.pendingRoll).toBe("context_failures_native_compaction_unavailable");
		expect(compaction.calls).toEqual(["event-session-e0"]);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("an aside-worker timeout streak never rolls: an executor failure is not context evidence", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-aside-"));
	try {
		const compaction = stubPort("unavailable");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			// The real shape: the aside collection worker gives up at its 300s cap.
			respond: () => {
				throw new Error("aside collection worker timed out after 300000ms");
			},
		});
		const monitor = registry.add({
			name: "aside",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["aside.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("aside.tick"));
		const eventIds: string[] = [];
		for (let tick = 0; tick < 5; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "aside.tick", { tick }));
		// Its own failure code, never aggregated with context exhaustion.
		for (const eventId of eventIds) expect(database.monitorFailure(eventId)?.code).toBe("aside_timeout");
		expect(compaction.calls).toHaveLength(0);
		expect(database.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.executorFailures).toBe(5);
		expect(state.contextFailures).toBe(0);
		expect(state.protocolFailures).toBe(0);
		expect(state.pendingRoll).toBeUndefined();
		expect(state.lastRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

/** The measured drill message: child dead on the wrapper's cap, daemon job alive. */
const ORPHANED_MESSAGE =
	"aside exec collection exceeded 360s; child killed but Aside daemon task still running (3 sessions, 10 tabs)";

test("an orphaned-executor streak never rolls and never touches the context streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-orphaned-"));
	try {
		const compaction = stubPort("unavailable");
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			respond: () => {
				throw new Error(ORPHANED_MESSAGE);
			},
		});
		const monitor = registry.add({
			name: "orphan",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["orphan.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("orphan.tick"));
		const eventIds: string[] = [];
		for (let tick = 0; tick < 5; tick += 1)
			eventIds.push(await pipeline.submitAwaitable(monitor.monitorId, "orphan.tick", { tick }));
		// Its own code, never aggregated with the plain timeout or with context.
		for (const eventId of eventIds) expect(database.monitorFailure(eventId)?.code).toBe("orphaned_executor");
		// The session is left completely alone: no compaction request, no roll.
		expect(compaction.calls).toHaveLength(0);
		expect(database.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		expect(turns.some((turn) => turn.prompt.includes("Context compaction"))).toBe(false);
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.contextFailures).toBe(0);
		expect(state.orphanedExecutorFailures).toBe(5);
		expect(state.executorFailures).toBe(5);
		expect(state.protocolFailures).toBe(0);
		// The operator gets a coded reason, which is the whole point of this class.
		expect(state.lastExecutorReason).toBe("executor_orphaned_external_work");
		expect(state.pendingRoll).toBeUndefined();
		expect(state.lastRoll).toBeUndefined();
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("interleaved orphaned-executor and context failures advance only the context streak", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-orphan-mixed-"));
	try {
		const compaction = stubPort("unavailable");
		let mode: "orphaned" | "context" = "orphaned";
		const { database, registry, pipeline, turns } = await harness(directory, {
			contextFailureRollThreshold: 2,
			compaction,
			respond: () => {
				if (mode === "orphaned") throw new Error(ORPHANED_MESSAGE);
				throw new Error("session send failed: context_too_large");
			},
		});
		const monitor = registry.add({
			name: "mixed",
			trigger: { kind: "cron", schedule: "*/10 * * * *" },
			eventTypes: ["mixed.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("mixed.tick"));
		// orphaned, context, orphaned, context: the orphans neither advance nor
		// reset the streak, so the two context failures still reach the threshold.
		const expectedContextStreak = [0, 1, 1, 2];
		const sequence: Array<"orphaned" | "context"> = ["orphaned", "context", "orphaned", "context"];
		for (const [index, step] of sequence.entries()) {
			mode = step;
			await pipeline.submitAwaitable(monitor.monitorId, "mixed.tick", { tick: index });
			expect(pipeline.sessionSafetyState(sessionKey).contextFailures).toBe(expectedContextStreak[index]);
		}
		const state = pipeline.sessionSafetyState(sessionKey);
		expect(state.orphanedExecutorFailures).toBe(2);
		expect(state.executorFailures).toBe(2);
		// Native compaction was requested for the context failures only.
		expect(compaction.calls).toHaveLength(2);
		// The threshold was reached by context evidence alone, so the net is armed.
		expect(state.pendingRoll).toBe("context_failures_native_compaction_unavailable");
		// Still not rolled: that happens at the next dispatch boundary.
		expect(database.getSessionRecord(sessionKey)?.epoch ?? 0).toBe(0);
		expect(new Set(turns.map((turn) => turn.sessionId))).toEqual(new Set(["event-session-e0"]));
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a session that stays busy across dispatches is rolled so the next slot lands on a live session (#263)", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-monitor-busy-"));
	try {
		const compaction = stubPort("unavailable");
		// The live shape: the epoch-0 session is wedged on a turn it never
		// finishes and refuses every prompt with `busy`; a fresh session answers.
		let clock = Date.now();
		const { database, registry, pipeline, turns, sessionPort } = await harness(directory, {
			contextFailureRollThreshold: 1,
			compaction,
			busy: (sessionId) => sessionId === "event-session-e0",
			now: () => clock,
		});
		const monitor = registry.add({
			name: "threads",
			trigger: { kind: "cron", schedule: "0 * * * *" },
			eventTypes: ["threads.tick"],
			burstPolicy: "serialize",
		});
		const sessionKey = originKey(eventTypeOrigin("threads.tick"));
		expect(MONITOR_BUSY_FAILURE_ROLL_THRESHOLD).toBe(2);
		const first = await pipeline.submitAwaitable(monitor.monitorId, "threads.tick", { tick: 0 });
		expect(database.monitorFailure(first)?.code).toBe("session_busy");
		let state = pipeline.sessionSafetyState(sessionKey);
		expect(state.busyFailures).toBe(1);
		expect(state.pendingRoll).toBeUndefined();
		// Reconcile replays the failed slot into the same session and it is busy
		// again. A replay still counts: the refusal is about the session, not the
		// payload, and this is exactly the retry loop that burned every slot.
		await pipeline.reconcile();
		expect(database.monitorFailure(first)?.code).toBe("session_busy");
		state = pipeline.sessionSafetyState(sessionKey);
		expect(state.busyFailures).toBe(2);
		expect(state.pendingRoll).toBe("session_busy_stalled");
		// Not context evidence, not contract evidence: nothing else moved.
		expect(state.contextFailures).toBe(0);
		expect(state.staleContextFailures).toBe(0);
		expect(state.executorFailures).toBe(0);
		expect(state.protocolFailures).toBe(0);
		expect(compaction.calls).toHaveLength(0);
		expect(turns).toHaveLength(0);
		// The next slot rolls at the dispatch boundary, binds a fresh session and
		// delivers, instead of aiming at the stalled session again.
		const next = await pipeline.submitAwaitable(monitor.monitorId, "threads.tick", { tick: 1 });
		expect(database.getSessionRecord(sessionKey)?.epoch).toBe(1);
		state = pipeline.sessionSafetyState(sessionKey);
		expect(state.lastRoll).toBe("session_busy_stalled");
		expect(state.busyFailures).toBe(0);
		expect(database.monitorEventRows(monitor.monitorId).find((row) => row.event_id === next)?.stage).toBe(
			"authored_no_delivery",
		);
		expect(turns.map((turn) => turn.sessionId)).toEqual(["event-session-e1"]);
		// The stalled host is ended so it stops occupying the runtime.
		await Bun.sleep(0);
		expect(sessionPort.closes.map((entry) => entry.sessionId)).toEqual(["event-session-e0"]);
		// The stranded slot is replayed into the live session too, once its
		// retry backoff has elapsed (#179: the second retry waits 10 minutes).
		clock += 10 * 60_000 + 1;
		await pipeline.reconcile();
		expect(database.monitorEventRows(monitor.monitorId).find((row) => row.event_id === first)?.stage).toBe(
			"authored_no_delivery",
		);
		database.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
