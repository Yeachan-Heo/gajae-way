import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { monitorSessionOrigin, originKey } from "@gajae-gateway/protocol";
import { GjcCliError } from "@gajae-gateway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { LastAssistantUnreadableError, type SessionRequestInput } from "../src/orchestrator/session-port";
import { RelayHelloError } from "../src/orchestrator/tail-runner";
import {
	GatewayDatabase,
	MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS,
	MONITOR_EVENT_MAX_OPEN_AGE_MS,
	MONITOR_EVENT_RETRY_BACKOFF_MS,
} from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import { sessionPortFromScript } from "./session-port.fake";

/**
 * mk-gajae 2026-10-08: an overlap=skip monitor stopped firing because one
 * event sat `failed` (or stranded `batched`) and every later fire was
 * `skipped` behind it; the request failures behind it were all recorded as
 * phase `request` / `internal_error`, and the operator had no CLI verb to end
 * or retry the stuck event.
 */

let home = "";
let database: GatewayDatabase | undefined;
let pipelines: MonitorPropagator[] = [];
afterEach(async () => {
	for (const pipeline of pipelines) await pipeline.drain();
	pipelines = [];
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

type RequestOverride = (input: SessionRequestInput, next: () => Promise<unknown>) => Promise<unknown>;

async function harness(options: { now?: () => number; request?: RequestOverride } = {}) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-stuck-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const monitor = registry.add({
		name: "probe",
		trigger: { kind: "cron", schedule: "0 * * * *" },
		eventTypes: ["probe.tick"],
		burstPolicy: "serialize",
		overlap: "skip",
	});
	const turns: string[] = [];
	const sessionPort = sessionPortFromScript({
		bind: async (_key: string, epoch = 0) => ({ sessionId: `probe-session-e${epoch}` }),
		respond: async (sessionId: string, text: string) => {
			turns.push(sessionId);
			const events = JSON.parse(text.match(/\[.*\]$/s)?.[0] ?? "[]") as Array<{ eventId: string }>;
			return JSON.stringify(events.map(({ eventId }) => ({ eventId, note: "ok" })));
		},
	});
	const request = sessionPort.request.bind(sessionPort);
	if (options.request) {
		const override = options.request;
		sessionPort.request = async (input) => (await override(input, () => request(input))) as never;
	}
	const emitted: Array<{ eventId: string; stage: string }> = [];
	const pipeline = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: (event) => emitted.push({ eventId: event.eventId, stage: event.stage }),
		...(options.now ? { now: options.now } : {}),
	});
	pipelines.push(pipeline);
	const sessionKey = originKey(monitorSessionOrigin(monitor.monitorId, "probe.tick"));
	return { db: database, monitor, pipeline, turns, emitted, sessionKey };
}

const stageOf = (db: GatewayDatabase, id: string) => db.monitorEventGet(id)?.stage;

test("an exhausted retry budget settles failed_no_retry and the next skip fire is admitted", async () => {
	let clock = Date.now();
	let failing = true;
	const { db, monitor, pipeline } = await harness({
		now: () => clock,
		request: async (_input, next) => {
			if (failing) throw new Error("request exploded");
			return await next();
		},
	});
	const first = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	expect(stageOf(db, first)).toBe("failed");
	for (const backoff of MONITOR_EVENT_RETRY_BACKOFF_MS.slice(1)) {
		clock += backoff + 1;
		await pipeline.reconcile();
	}
	expect(db.monitorEventDispatchAttempts(first)).toBe(MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS - 1);
	clock += 24 * 60 * 60_000;
	await pipeline.reconcile();
	await pipeline.reconcile();
	expect(stageOf(db, first)).toBe("failed_no_retry");
	failing = false;
	const next = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	expect(db.monitorEventGet(next)?.skipped_by).toBeNull();
	expect(stageOf(db, next)).toBe("authored_no_delivery");
});

test("a failed predecessor waiting out its backoff no longer skips the next fire: it is superseded", async () => {
	let failing = true;
	const { db, monitor, pipeline, emitted } = await harness({
		request: async (_input, next) => {
			if (failing) throw new Error("request exploded");
			return await next();
		},
	});
	const first = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	expect(stageOf(db, first)).toBe("failed");
	failing = false;
	const next = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	expect(db.monitorEventGet(next)?.skipped_by).toBeNull();
	expect(stageOf(db, next)).toBe("authored_no_delivery");
	expect(stageOf(db, first)).toBe("failed_no_retry");
	expect(db.monitorFailure(first)?.code).toBe("superseded");
	expect(db.monitorFailure(first)?.detail).toContain(next);
	// The silent-path stage is emitted, so `monitors test --wait` sees it.
	expect(emitted).toContainEqual({ eventId: next, stage: "authored_no_delivery" });
});

test("a live in-flight predecessor still skips, and a leased failed row is not superseded", async () => {
	const { db, monitor } = await harness();
	const running = crypto.randomUUID();
	db.monitorEventCreate({
		eventId: running,
		monitorId: monitor.monitorId,
		eventType: "probe.tick",
		payloadJson: "{}",
		firedAt: new Date().toISOString(),
		overlap: "skip",
	});
	db.monitorEventUpdate(running, "failed");
	expect(db.monitorEventAcquireLease(running, "gateway:1", "lease-1", 60_000, Date.now())).toBe(true);
	const skipped = crypto.randomUUID();
	expect(
		db.monitorEventCreate({
			eventId: skipped,
			monitorId: monitor.monitorId,
			eventType: "probe.tick",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
			overlap: "skip",
		}),
	).toBe(running);
	expect(stageOf(db, skipped)).toBe("skipped");
	expect(stageOf(db, running)).toBe("failed");
});

test("boot reconcile reaps open events older than the open-age bound, live leases excepted", async () => {
	const { db, monitor, pipeline, emitted, turns } = await harness();
	const old = new Date(Date.now() - MONITOR_EVENT_MAX_OPEN_AGE_MS - 60_000).toISOString();
	const seed = (stage: string) => {
		const id = crypto.randomUUID();
		db.monitorEventCreate({
			eventId: id,
			monitorId: monitor.monitorId,
			eventType: "probe.tick",
			payloadJson: "{}",
			firedAt: old,
		});
		db.monitorEventUpdate(id, stage as never, stage === "batched" ? "dead-batch" : null);
		return id;
	};
	const batched = seed("batched");
	const admitted = seed("admitted");
	const leased = seed("failed");
	expect(db.monitorEventAcquireLease(leased, "gateway:other", "lease-x", 60_000, Date.now())).toBe(true);
	await pipeline.reconcile();
	for (const id of [batched, admitted]) {
		expect(stageOf(db, id)).toBe("failed_no_retry");
		expect(db.monitorFailure(id)?.code).toBe("stale_open_event");
		expect(emitted).toContainEqual({ eventId: id, stage: "failed_no_retry" });
	}
	expect(stageOf(db, leased)).toBe("failed");
	// Reaped rows are never redispatched.
	expect(turns).toEqual([]);
	// And they no longer block an overlap=skip fire.
	db.monitorEventReleaseLease(leased, "lease-x");
	const next = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	expect(db.monitorEventGet(next)?.skipped_by).toBeNull();
});

test("operator settle ends a stuck event; operator retry redispatches with a fresh budget", async () => {
	let failing = true;
	const { db, monitor, pipeline } = await harness({
		request: async (_input, next) => {
			if (failing) throw new Error("request exploded");
			return await next();
		},
	});
	const stuck = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	expect(stageOf(db, stuck)).toBe("failed");
	expect(pipeline.settle("no-such-event")).toEqual({ eventId: "no-such-event", done: false, reason: "unknown_event" });
	expect(pipeline.settle(stuck)).toEqual({ eventId: stuck, done: true, previousStage: "failed" });
	expect(stageOf(db, stuck)).toBe("failed_no_retry");
	expect(db.monitorFailure(stuck)?.code).toBe("operator_settled");
	expect(pipeline.settle(stuck)).toEqual({ eventId: stuck, done: false, reason: "stage_failed_no_retry" });

	failing = false;
	expect(pipeline.retry(stuck)).toEqual({ eventId: stuck, done: true, previousStage: "failed_no_retry" });
	for (let i = 0; i < 200 && stageOf(db, stuck) !== "authored_no_delivery"; i++) await Bun.sleep(5);
	expect(stageOf(db, stuck)).toBe("authored_no_delivery");
	expect(pipeline.retry(stuck)).toEqual({ eventId: stuck, done: false, reason: "stage_authored_no_delivery" });
});

test("operator actions refuse an event whose dispatch lease is live", async () => {
	const { db, monitor, pipeline } = await harness();
	const id = crypto.randomUUID();
	db.monitorEventCreate({
		eventId: id,
		monitorId: monitor.monitorId,
		eventType: "probe.tick",
		payloadJson: "{}",
		firedAt: new Date().toISOString(),
	});
	db.monitorEventUpdate(id, "failed");
	expect(db.monitorEventAcquireLease(id, "gateway:1", "lease-1", 60_000, Date.now())).toBe(true);
	expect(pipeline.settle(id)).toEqual({ eventId: id, done: false, reason: "dispatch_in_flight" });
	expect(pipeline.retry(id)).toEqual({ eventId: id, done: false, reason: "dispatch_in_flight" });
	expect(stageOf(db, id)).toBe("failed");
});

test("an unreachable session host is classified and rolls the monitor session to a fresh epoch", async () => {
	const { db, monitor, pipeline, turns, sessionKey } = await harness({
		request: async (input, next) => {
			if (input.sessionId === "probe-session-e0")
				throw Object.assign(new RelayHelloError(15_000, "stream_ended"), { requestStep: "attach" });
			return await next();
		},
	});
	const first = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	const failure = db.monitorFailure(first);
	expect(failure?.code).toBe("session_host_unavailable");
	expect(failure?.detail).toContain('"class":"RelayHelloError"');
	expect(failure?.detail).toContain('"step":"attach"');
	expect(failure?.detail).toContain('"code":"relay_hello_missing"');
	expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBe("session_host_unavailable");
	const next = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	expect(db.getSessionRecord(sessionKey)?.epoch).toBe(1);
	expect(pipeline.sessionSafetyState(sessionKey).lastRoll).toBe("session_host_unavailable");
	expect(stageOf(db, next)).toBe("authored_no_delivery");
	expect(turns).toEqual(["probe-session-e1"]);
});

test("a terminal_ok turn whose answer cannot be read back names the step instead of internal_error", async () => {
	const { db, monitor, pipeline, sessionKey } = await harness({
		request: async () => {
			throw Object.assign(new LastAssistantUnreadableError("last_assistant_null_item"), {
				requestStep: "last_assistant",
			});
		},
	});
	const id = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	const failure = db.monitorFailure(id);
	expect(failure?.code).toBe("authoring_result_unreadable");
	expect(failure?.detail).toContain('"class":"LastAssistantUnreadableError"');
	expect(failure?.detail).toContain('"step":"last_assistant"');
	expect(failure?.detail).toContain('"code":"last_assistant_null_item"');
	// Not a host problem: the session answered, so it is not rolled.
	expect(pipeline.sessionSafetyState(sessionKey).pendingRoll).toBeUndefined();
});

test("a plain Error inside the request carries its step and is not internal_error", async () => {
	const { db, monitor, pipeline } = await harness({
		request: async () => {
			throw Object.assign(new Error("SECRET body"), { requestStep: "status" });
		},
	});
	const id = await pipeline.submitAwaitable(monitor.monitorId, "probe.tick", {});
	const failure = db.monitorFailure(id);
	expect(failure?.code).toBe("authoring_turn_failed");
	expect(failure?.detail).toContain('"step":"status"');
	expect(failure?.detail).not.toContain("SECRET");
});

test("an unresolvable monitor model is classified model_unavailable and names the selector", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-model-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const monitor = registry.add({
		name: "model-probe",
		trigger: { kind: "cron", schedule: "0 * * * *" },
		eventTypes: ["probe.model"],
		burstPolicy: "serialize",
		model: "og/not-in-models-yml",
	});
	const sessionPort = sessionPortFromScript({
		bind: async () => {
			throw new GjcCliError("SECRET raw", 0, "", { code: "model_not_selected", message: "SECRET" });
		},
		respond: async () => "unused",
	});
	const pipeline = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
	});
	pipelines.push(pipeline);
	const id = await pipeline.submitAwaitable(monitor.monitorId, "probe.model", {});
	const failure = database.monitorFailure(id);
	expect(failure?.code).toBe("model_unavailable");
	expect(failure?.detail).toContain('"code":"model_not_selected"');
	expect(failure?.detail).toContain('"model":"og/not-in-models-yml"');
	expect(failure?.detail).not.toContain("SECRET");
	expect(stageOf(database, id)).toBe("failed");
});
