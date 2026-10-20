import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { monitorSessionOrigin, originKey } from "@gajae-gateway/protocol";
import type { CliRunner, StatusReport } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import {
	BrokerSessionPort,
	LastAssistantUnreadableError,
	SessionTerminalError,
	sessionRequestStep,
} from "../src/orchestrator/session-port";
import { RelayClosedError, RelayHelloError, TailRunner } from "../src/orchestrator/tail-runner";
import { type MonitorEventStage, Store } from "../src/store";
import { MONITOR_EVENT_MAX_OPEN_AGE_MS } from "../src/store/groups/monitors";
import {
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	noRelay,
	type SessionPortResponder,
	sessionPortFromScript,
} from "./session-port.fake";

let home = "";
let database: Store | undefined;
const propagators: MonitorPropagator[] = [];

afterEach(async () => {
	for (const propagator of propagators.splice(0)) await propagator.drain();
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

function eventIdsFromPrompt(prompt: string): string[] {
	const start = prompt.lastIndexOf('[{"eventId"');
	if (start < 0) return [];
	return (JSON.parse(prompt.slice(start)) as Array<{ eventId: string }>).map(({ eventId }) => eventId);
}

async function harness(
	options: {
		readonly now?: () => number;
		readonly respond?: SessionPortResponder;
		readonly initializeAuthority?: boolean;
		readonly overlap?: "skip" | "queue";
	} = {},
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stuck-"));
	const db = await Store.open(join(home, "gateway.db"));
	database = db;
	const authority = options.initializeAuthority ? initializeTestBrokerAuthority(db, join(home, "agent")) : undefined;
	const registry = new MonitorRegistry(db);
	const monitor = registry.add({
		name: "stuck recovery",
		trigger: { kind: "cron", schedule: "0 * * * *" },
		eventTypes: ["test.tick"],
		burstPolicy: "serialize",
		overlap: options.overlap ?? "skip",
		enabled: true,
	});
	const sessionPort = sessionPortFromScript({
		bind: async () => ({ sessionId: "s1" }),
		respond:
			options.respond ??
			(async (_sessionId, prompt) =>
				JSON.stringify(eventIdsFromPrompt(prompt).map((eventId) => ({ eventId, note: "recovered" })))),
	});
	const emitted: Array<{ eventId: string; stage: string }> = [];
	const propagator = new MonitorPropagator({
		database: db,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(db),
		emit: ({ eventId, stage }) => emitted.push({ eventId, stage }),
		...(options.now ? { now: options.now } : {}),
	});
	propagators.push(propagator);
	return {
		db,
		monitor,
		propagator,
		sessionPort,
		emitted,
		authority,
		databasePath: join(home, "gateway.db"),
		sessionOriginKey: originKey(monitorSessionOrigin(monitor.monitorId, "test.tick")),
	};
}

function seedEvent(db: Store, monitorId: string, stage: MonitorEventStage, firedAt = new Date().toISOString()): string {
	const eventId = crypto.randomUUID();
	db.transaction((tx) => {
		db.monitors.eventCreate(tx, {
			eventId,
			monitorId,
			eventType: "test.tick",
			payloadJson: "{}",
			firedAt,
		});
		if (stage !== "admitted") db.monitors.eventUpdate(tx, eventId, stage);
	});
	return eventId;
}

const stageOf = (db: Store, eventId: string) => db.monitors.eventGet(eventId)?.stage;

test("overlap skip supersedes failed predecessors without leases or saved turns", async () => {
	const { db, monitor } = await harness();
	const predecessor = seedEvent(db, monitor.monitorId, "failed");
	let nextSkippedBy: string | undefined;
	db.transaction((tx) => {
		nextSkippedBy = db.monitors.eventCreate(tx, {
			eventId: crypto.randomUUID(),
			monitorId: monitor.monitorId,
			eventType: "test.tick",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
			overlap: "skip",
		});
	});
	expect(stageOf(db, predecessor)).toBe("failed_no_retry");
	expect(db.monitors.failure(predecessor)?.code).toBe("superseded");
	expect(nextSkippedBy).toBeUndefined();
});

test("overlap skip preserves a pointer-bearing failed predecessor for saved-turn recovery", async () => {
	const { db, monitor } = await harness();
	const predecessor = seedEvent(db, monitor.monitorId, "failed");
	const pointer = JSON.stringify({ sessionId: "s1", opRef: "gw-prior", eventIds: [predecessor] });
	db.core.metaSet(`monitor_authoring_turn:${predecessor}`, pointer);
	let nextSkippedBy: string | undefined;
	let successor = "";
	db.transaction((tx) => {
		successor = crypto.randomUUID();
		nextSkippedBy = db.monitors.eventCreate(tx, {
			eventId: successor,
			monitorId: monitor.monitorId,
			eventType: "test.tick",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
			overlap: "skip",
		});
	});
	expect(stageOf(db, predecessor)).toBe("failed");
	expect(db.core.metaGet(`monitor_authoring_turn:${predecessor}`)).toBe(pointer);
	expect(nextSkippedBy).toBe(predecessor);
	expect(stageOf(db, successor)).toBe("skipped");
});

test("overlap skip does not supersede a failed predecessor with a live dispatch lease", async () => {
	const { db, monitor } = await harness();
	const predecessor = seedEvent(db, monitor.monitorId, "failed");
	db.transaction((tx) => db.monitors.eventAcquireLease(tx, predecessor, "retrying", "retry-lease", 60_000));
	let skippedBy: string | undefined;
	db.transaction((tx) => {
		skippedBy = db.monitors.eventCreate(tx, {
			eventId: crypto.randomUUID(),
			monitorId: monitor.monitorId,
			eventType: "test.tick",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
			overlap: "skip",
		});
	});
	expect(stageOf(db, predecessor)).toBe("failed");
	expect(skippedBy).toBe(predecessor);
});

test("overlap skip preserves a failed predecessor that already has authored output", async () => {
	const { db, monitor } = await harness();
	const predecessor = seedEvent(db, monitor.monitorId, "failed");
	db.transaction((tx) => db.monitors.authoredOutputCreate(tx, predecessor, "recoverable note"));
	let skippedBy: string | undefined;
	db.transaction((tx) => {
		skippedBy = db.monitors.eventCreate(tx, {
			eventId: crypto.randomUUID(),
			monitorId: monitor.monitorId,
			eventType: "test.tick",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
			overlap: "skip",
		});
	});
	expect(stageOf(db, predecessor)).toBe("failed");
	expect(db.monitors.authoredOutput(predecessor)).toBe("recoverable note");
	expect(skippedBy).toBe(predecessor);
});

test("stale reaping measures updated_at and skips saved turns, live leases, and authored output", async () => {
	const now = Date.now();
	const { db, monitor, propagator, databasePath } = await harness({ now: () => now });
	const oldFiredAt = new Date(now - 2 * MONITOR_EVENT_MAX_OPEN_AGE_MS).toISOString();
	const stale = seedEvent(db, monitor.monitorId, "admitted", oldFiredAt);
	const staleBatched = seedEvent(db, monitor.monitorId, "batched", oldFiredAt);
	const staleDispatched = seedEvent(db, monitor.monitorId, "dispatched", oldFiredAt);
	const staleFailed = seedEvent(db, monitor.monitorId, "failed", oldFiredAt);
	const catchUp = seedEvent(db, monitor.monitorId, "admitted", oldFiredAt);
	const pointerBearing = seedEvent(db, monitor.monitorId, "failed", oldFiredAt);
	const leased = seedEvent(db, monitor.monitorId, "dispatched", oldFiredAt);
	const authored = seedEvent(db, monitor.monitorId, "batched", oldFiredAt);
	const pointer = JSON.stringify({ sessionId: "s1", opRef: "gw-saved", eventIds: [pointerBearing] });
	db.core.metaSet(`monitor_authoring_turn:${pointerBearing}`, pointer);
	db.transaction((tx) => {
		db.monitors.eventAcquireLease(tx, leased, "test", "live-lease", 60 * 60_000, now);
		db.monitors.authoredOutputCreate(tx, authored, "already authored");
	});
	const oldUpdatedAt = new Date(now - MONITOR_EVENT_MAX_OPEN_AGE_MS - 1).toISOString();
	const raw = new Database(databasePath);
	try {
		for (const eventId of [stale, staleBatched, staleDispatched, staleFailed, pointerBearing, leased, authored])
			raw.query("UPDATE monitor_events SET updated_at = ? WHERE event_id = ?").run(oldUpdatedAt, eventId);
		raw.query("UPDATE monitor_events SET updated_at = ? WHERE event_id = ?").run(new Date(now).toISOString(), catchUp);
	} finally {
		raw.close();
	}

	expect(propagator.reapStale()).toBe(4);
	for (const eventId of [stale, staleBatched, staleDispatched, staleFailed]) {
		expect(stageOf(db, eventId)).toBe("failed_no_retry");
		expect(db.monitors.failure(eventId)?.code).toBe("stale_open_event");
	}
	expect(stageOf(db, catchUp)).toBe("admitted");
	expect(stageOf(db, pointerBearing)).toBe("failed");
	expect(db.core.metaGet(`monitor_authoring_turn:${pointerBearing}`)).toBe(pointer);
	expect(stageOf(db, leased)).toBe("dispatched");
	expect(stageOf(db, authored)).toBe("batched");
});

test("a reentrant reconcile still reaps stale rows before the guard", async () => {
	let markStarted!: () => void;
	let release!: () => void;
	const started = new Promise<void>((resolve) => (markStarted = resolve));
	const parked = new Promise<void>((resolve) => (release = resolve));
	const { db, monitor, propagator, databasePath } = await harness({
		respond: async (_sessionId, prompt) => {
			markStarted();
			await parked;
			return JSON.stringify(eventIdsFromPrompt(prompt).map((eventId) => ({ eventId, note: "done" })));
		},
	});
	seedEvent(db, monitor.monitorId, "admitted");
	const activeSweep = propagator.reconcile();
	await started;
	const stale = seedEvent(db, monitor.monitorId, "admitted");
	const raw = new Database(databasePath);
	try {
		raw
			.query("UPDATE monitor_events SET updated_at = ? WHERE event_id = ?")
			.run(new Date(Date.now() - MONITOR_EVENT_MAX_OPEN_AGE_MS - 1).toISOString(), stale);
	} finally {
		raw.close();
	}
	try {
		await propagator.reconcile();
		expect(stageOf(db, stale)).toBe("failed_no_retry");
	} finally {
		release();
		await activeSweep;
	}
});

test("settle and retry preserve authoring pointers, and settle refuses a live lease", async () => {
	const { db, monitor, propagator } = await harness();
	const eventId = seedEvent(db, monitor.monitorId, "failed");
	const pointer = JSON.stringify({ sessionId: "s1", opRef: "gw-reuse", eventIds: [eventId] });
	db.core.metaSet(`monitor_authoring_turn:${eventId}`, pointer);
	const settled = propagator.settle(eventId);
	expect(settled).toEqual({ eventId, done: true, previousStage: "failed" });
	expect(stageOf(db, eventId)).toBe("failed_no_retry");
	expect(db.core.metaGet(`monitor_authoring_turn:${eventId}`)).toBe(pointer);

	const busy = seedEvent(db, monitor.monitorId, "admitted");
	db.transaction((tx) => db.monitors.eventAcquireLease(tx, busy, "test", "busy-lease", 60_000));
	expect(propagator.settle(busy)).toEqual({ eventId: busy, done: false, reason: "dispatch_in_flight" });
	expect(stageOf(db, busy)).toBe("admitted");

	const retryable = seedEvent(db, monitor.monitorId, "failed_no_retry", "2020-01-01T00:00:00.000Z");
	const retryPointer = JSON.stringify({ sessionId: "s1", opRef: "gw-retry", eventIds: [retryable] });
	db.core.metaSet(`monitor_authoring_turn:${retryable}`, retryPointer);
	const retried = db.transaction((tx) => db.monitors.eventOperatorRetry(tx, retryable, Date.now()));
	expect(retried).toEqual({ retried: true, previousStage: "failed_no_retry" });
	expect(db.monitors.eventGet(retryable)?.fired_at).toBe("2020-01-01T00:00:00.000Z");
	expect(db.monitors.eventDispatchAttempts(retryable)).toBe(0);
	expect(db.core.metaGet(`monitor_authoring_turn:${retryable}`)).toBe(retryPointer);

	const authored = seedEvent(db, monitor.monitorId, "failed");
	db.transaction((tx) => db.monitors.authoredOutputCreate(tx, authored, "already authored"));
	expect(propagator.settle(authored)).toEqual({ eventId: authored, done: false, reason: "already_authored" });
	expect(stageOf(db, authored)).toBe("failed");
});

test("dispatch lease acquisition cannot reopen an event settled from an old reconcile snapshot", async () => {
	const { db, monitor } = await harness();
	const eventId = seedEvent(db, monitor.monitorId, "admitted");
	const settled = db.transaction((tx) => db.monitors.eventOperatorSettle(tx, eventId));
	expect(settled).toEqual({ settled: true, previousStage: "admitted" });
	const acquired = db.transaction((tx) =>
		db.monitors.eventAcquireLease(tx, eventId, "stale-reconcile", "late-lease", 60_000),
	);
	expect(acquired).toBe(false);
	expect(stageOf(db, eventId)).toBe("failed_no_retry");
});

test("retry immediately dispatches an old failed_no_retry event instead of reaping it", async () => {
	const { db, monitor, propagator, sessionPort, databasePath } = await harness();
	const eventId = seedEvent(db, monitor.monitorId, "failed_no_retry", "2020-01-01T00:00:00.000Z");
	const raw = new Database(databasePath);
	try {
		raw
			.query("UPDATE monitor_events SET updated_at = ? WHERE event_id = ?")
			.run(new Date(Date.now() - MONITOR_EVENT_MAX_OPEN_AGE_MS - 1).toISOString(), eventId);
	} finally {
		raw.close();
	}
	expect(propagator.retry(eventId)).toEqual({ eventId, done: true, previousStage: "failed_no_retry" });
	for (let attempt = 0; attempt < 200 && sessionPort.sends.length === 0; attempt++) await Bun.sleep(5);
	expect(sessionPort.sends).toHaveLength(1);
	expect(db.monitors.eventDispatchAttempts(eventId)).toBe(1);
	expect(stageOf(db, eventId)).not.toBe("failed_no_retry");
	expect(db.monitors.failure(eventId)?.code).not.toBe("stale_open_event");
});

test("unreadable last_assistant results are not protocol failures", async () => {
	const { db, monitor, propagator, sessionPort, sessionOriginKey } = await harness();
	sessionPort.request = async () => {
		throw new LastAssistantUnreadableError("last_assistant_not_json");
	};
	const eventId = await propagator.submitAwaitable(monitor.monitorId, "test.tick", {});
	expect(db.monitors.failure(eventId)?.code).toBe("authoring_result_unreadable");
	expect(propagator.sessionSafetyState(sessionOriginKey).protocolFailures).toBe(0);
});

test("an ambiguous send-stage relay close does not roll the session", async () => {
	const { db, monitor, propagator, sessionPort, authority, sessionOriginKey } = await harness({
		initializeAuthority: true,
	});
	expect(authority).toBeDefined();
	await createOwnedSessionFixture(db, authority!, {
		sessionId: "s1",
		originKey: sessionOriginKey,
		epoch: 0,
		repo: process.cwd(),
	});
	const brokerPort = new BrokerSessionPort({
		database: db,
		authority: authority!,
		cli: (async () => ({ exitCode: 0, stdout: "", stderr: "" })) as CliRunner,
		tailRunner: new TailRunner({ stream: noRelay, repo: process.cwd() }),
	});
	let step: string | undefined;
	const relay = {
		beginTurn: () => {},
		setTurnRunning: () => {},
		close: async () => {},
	};
	brokerPort.attachTail = async () => relay as never;
	brokerPort.send = async () => {
		throw new RelayClosedError("s1", "connection closed after prompt submission");
	};
	brokerPort.status = async () => ({ status: { status: "unknown" } }) as never;
	const brokerRequest = brokerPort.request.bind(brokerPort);
	sessionPort.request = async (input) => {
		try {
			return await brokerRequest(input);
		} catch (error) {
			step = sessionRequestStep(error);
			throw error;
		}
	};
	const eventId = await propagator.submitAwaitable(monitor.monitorId, "test.tick", {});
	expect(step).toBe("send");
	expect(db.monitors.failure(eventId)?.code).toBe("authoring_turn_failed");
	expect(propagator.sessionSafetyState(sessionOriginKey).pendingRoll).toBeUndefined();
});

test("a generic session_not_found diagnostic does not roll the session", async () => {
	const { monitor, propagator, sessionPort, sessionOriginKey } = await harness();
	sessionPort.request = async () => {
		throw Object.assign(new Error("session_not_found"), { code: "session_not_found" });
	};
	await propagator.submitAwaitable(monitor.monitorId, "test.tick", {});
	expect(propagator.sessionSafetyState(sessionOriginKey).pendingRoll).toBeUndefined();
});

test("a relay hello failure is positive pre-send evidence and arms a session roll", async () => {
	const { db, monitor, propagator, sessionPort, sessionOriginKey } = await harness();
	sessionPort.request = async () => {
		throw new RelayHelloError(100, "stream_open");
	};
	const eventId = await propagator.submitAwaitable(monitor.monitorId, "test.tick", {});
	expect(db.monitors.failure(eventId)?.code).toBe("session_host_unavailable");
	expect(propagator.sessionSafetyState(sessionOriginKey).pendingRoll).toBe("session_host_unavailable");
});

/**
 * The terminal submission failure a session wedged on a prior agent run reports:
 * the runtime took the prompt, no run ever started, and the cause is the agent
 * runtime itself. Measured live (memory.canonicalize, every slot from
 * 2026-10-13T21:30Z): `Timed out waiting for prior agent run to finish before
 * prompting.`
 */
function wedgedSubmissionReport(outcome: Record<string, string> = {}): StatusReport {
	return {
		operationRef: "gw-p-wedged-submission",
		status: {
			status: "failed",
			commandId: "command-wedged-submission",
			turnId: "turn-wedged-submission",
			terminalAt: Date.now(),
			error: { code: "internal", message: "Prompt submission failed." },
			outcome: {
				kind: "failed",
				code: "prompt_failed",
				message: "Prompt submission failed.",
				phase: "submission",
				category: "agent_runtime",
				provenance: "agent_failed",
				providerCode: "internal",
				...outcome,
			},
		},
		summaryCompleted: true,
	};
}

test("a wedged prompt submission is positive pre-send evidence and arms a session roll", async () => {
	const { db, monitor, propagator, sessionPort, sessionOriginKey } = await harness();
	sessionPort.request = async () => {
		throw new SessionTerminalError(wedgedSubmissionReport());
	};
	const eventId = await propagator.submitAwaitable(monitor.monitorId, "test.tick", {});
	expect(db.monitors.failure(eventId)?.code).toBe("session_submission_wedged");
	expect(propagator.sessionSafetyState(sessionOriginKey).pendingRoll).toBe("session_submission_wedged");
});

test("a post-start agent failure stays on its session and never rolls", async () => {
	const { db, monitor, propagator, sessionPort, sessionOriginKey } = await harness();
	sessionPort.request = async () => {
		throw new SessionTerminalError(wedgedSubmissionReport({ phase: "post_start" }));
	};
	const eventId = await propagator.submitAwaitable(monitor.monitorId, "test.tick", {});
	expect(db.monitors.failure(eventId)?.code).toBe("authoring_turn_failed");
	expect(propagator.sessionSafetyState(sessionOriginKey).pendingRoll).toBeUndefined();
});

test("a cancelled prompt submission never rolls the session", async () => {
	const { db, monitor, propagator, sessionPort, sessionOriginKey } = await harness();
	sessionPort.request = async () => {
		throw new SessionTerminalError(wedgedSubmissionReport({ providerCode: "aborted" }));
	};
	const eventId = await propagator.submitAwaitable(monitor.monitorId, "test.tick", {});
	expect(db.monitors.failure(eventId)?.code).toBe("authoring_turn_failed");
	expect(propagator.sessionSafetyState(sessionOriginKey).pendingRoll).toBeUndefined();
});

test("only consecutive prior-run-busy terminal failures enter the recovery streak", async () => {
	const { db, monitor, propagator, sessionPort, sessionOriginKey } = await harness({ overlap: "queue" });
	const outcomes = [
		{
			reason: "Timed out waiting for prior agent run to finish before prompting.",
			code: "session_submission_wedged",
			streak: 1,
		},
		{ reason: "The model is temporarily unavailable.", code: "authoring_turn_failed", streak: 0 },
		{
			reason: "Timed out waiting for prior agent run to finish before prompting.",
			code: "session_submission_wedged",
			streak: 1,
		},
	];
	let nextOutcome = 0;
	sessionPort.request = async () => {
		const outcome = outcomes[nextOutcome++];
		if (!outcome) throw new Error("unexpected extra request");
		const cause = Object.assign(new Error("sdk_prompt_terminal_failed"), {
			code: "sdk_prompt_terminal_failed",
			reason: outcome.reason,
		});
		throw new Error("monitor prompt terminal failure", { cause });
	};
	for (const outcome of outcomes) {
		const eventId = await propagator.submitAwaitable(monitor.monitorId, "test.tick", {});
		expect(db.monitors.failure(eventId)?.code).toBe(outcome.code);
		expect(propagator.sessionSafetyState(sessionOriginKey)).toMatchObject({
			priorRunBusyFailures: outcome.streak,
			pendingRoll: undefined,
		});
	}
	expect(db.bindings.getSessionRecord(sessionOriginKey)?.epoch ?? 0).toBe(0);
});
