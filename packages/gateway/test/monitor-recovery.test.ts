import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajaeway/subsession";
import { DeliveryService } from "../src/delivery/delivery";
import { MemoryClosureQueue } from "../src/memory/closure";
import { initializeMemory } from "../src/memory/doctrine";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { MonitorRuntime } from "../src/monitors/runtime";
import { cronSlotsBetween, startCron } from "../src/monitors/triggers/cron";
import { GjcRuntimeError } from "../src/orchestrator/rebind";
import { SessionTerminalError } from "../src/orchestrator/session-port";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase, MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";
import type { SessionPortResponder } from "./session-port.fake";
import { sessionPortFromScript } from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;
let propagators: MonitorPropagator[] = [];
afterEach(async () => {
	for (const propagator of propagators) await propagator.drain();
	propagators = [];
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

function fakeSessionPort(respond: SessionPortResponder) {
	return sessionPortFromScript({ bind: async () => ({ sessionId: "s1" }), respond });
}

async function harness(
	respond: SessionPortResponder,
	options: { ownerTarget?: { origin: { platform: "loopback"; kind: "loopback"; conversationId: "loopback" } } } = {},
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const registry = new MonitorRegistry(database);
	const monitor = registry.add({
		name: "canonicalize",
		trigger: { kind: "cron", schedule: "30 */6 * * *" },
		eventTypes: ["memory.canonicalize"],
		burstPolicy: "dedupe",
		enabled: true,
	});
	const sessionPort = fakeSessionPort(respond);
	const propagator = new MonitorPropagator({
		database,
		registry,
		sessionPort,
		memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
		...(options.ownerTarget ? { ownerTarget: options.ownerTarget } : {}),
	});
	propagators.push(propagator);
	return { propagator, monitor, database, registry, sessionPort };
}

function eventsFromPrompt(text: string): Array<{ eventId: string }> {
	const match = text.match(/\[.*\]$/s);
	if (!match) throw new Error("prompt has no event array");
	return JSON.parse(match[0]) as Array<{ eventId: string }>;
}

const stage = (db: GatewayDatabase, id: string) => db.monitorEventRows().find((row) => row.event_id === id)?.stage;

function seedEvent(db: GatewayDatabase, monitorId: string, stage: string, batchId: string | null = null): string {
	const eventId = crypto.randomUUID();
	db.monitorEventCreate({
		eventId,
		monitorId,
		eventType: "memory.canonicalize",
		payloadJson: JSON.stringify({ at: new Date().toISOString() }),
		firedAt: new Date().toISOString(),
	});
	db.monitorEventUpdate(eventId, stage as never, batchId);
	return eventId;
}

function backdateMonitor(db: GatewayDatabase, monitorId: string, createdAt: Date): void {
	db.monitorSetCreatedAt(monitorId, createdAt.toISOString());
}

test("monitor.inspect exposes quarantined accepted and failed history without recovery sends", async () => {
	let sends = 0;
	const {
		database: db,
		monitor,
		propagator,
		sessionPort,
	} = await harness(async () => {
		sends++;
		throw new Error("quarantined history must not send");
	});
	// Dispatched is the durable monitor stage for an accepted authoring turn.
	const accepted = seedEvent(db, monitor.monitorId, "dispatched", "old-accepted-batch");
	const failed = seedEvent(db, monitor.monitorId, "failed", "old-failed-batch");
	db.cutoverBrokerAuthority({
		expectedAuthority: null,
		targetAuthority: { canonicalAgentDir: "/tmp/monitor-inspection-global", identity: "shared-broker" },
		evidence: "Test operator authorized historical monitor quarantine",
		disposition: "quarantine",
	});
	const history = db.monitorEventRows(monitor.monitorId, "newest", true);
	expect(history).toHaveLength(2);
	expect(db.monitorEventRows()).toEqual([]);
	expect(db.monitorEventRows(undefined, "oldest")).toEqual([]);
	expect(db.monitorEventRows(monitor.monitorId, "oldest")).toEqual([]);
	await propagator.reconcile();
	expect(sends).toBe(0);
	const socketPath = join(home, "inspect.sock");
	const server = await startUnixServer({
		config: {
			schemaVersion: 1,
			home,
			configPath: join(home, "config.json"),
			socketPath,
			dbPath: join(home, "gateway.db"),
			logVerbosity: "info",
		},
		database: db,
		sessionPort,
		onStop: () => {},
	});
	let socket: Awaited<ReturnType<typeof Bun.connect>> | undefined;
	try {
		const frames: Array<Record<string, unknown>> = [];
		let buffered = "";
		socket = await Bun.connect({
			unix: socketPath,
			socket: {
				data(_socket, data) {
					buffered += Buffer.from(data).toString();
					const lines = buffered.split("\n");
					buffered = lines.pop() ?? "";
					for (const line of lines) if (line) frames.push(JSON.parse(line));
				},
			},
		});
		const inspect = async (id: string) => {
			socket!.write(
				`${JSON.stringify({ v: "0.1", type: "request", id, verb: "monitor.inspect", params: { monitorId: monitor.monitorId } })}\n`,
			);
			for (let attempt = 0; attempt < 400; attempt++) {
				const frame = frames.find((entry) => entry.id === id);
				if (frame) {
					expect(frame.type).toBe("response");
					return (frame.result as { recentEvents: Array<Record<string, unknown>> }).recentEvents;
				}
				await Bun.sleep(5);
			}
			throw new Error(`no monitor.inspect response for ${id}`);
		};
		socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
		const rows = await inspect("history");
		expect(rows).toHaveLength(2);
		for (const [eventId, stage] of [
			[accepted, "dispatched"],
			[failed, "failed"],
		]) {
			expect(rows.find((row) => row.eventId === eventId)).toMatchObject({
				stage,
				quarantined: true,
				reason: "broker_authority_quarantined",
			});
		}
		// Terminal current-authority events exercise the existing history bound without dispatch.
		for (let index = 0; index < 101; index++) seedEvent(db, monitor.monitorId, "authored_no_delivery");
		const bounded = await inspect("bounded");
		expect(bounded).toHaveLength(100);
		expect(bounded.map((row) => row.eventId)).toEqual(
			db
				.monitorEventRows(monitor.monitorId, "newest", true)
				.slice(0, 100)
				.map((row) => row.event_id),
		);
		for (const row of bounded.filter((row) => row.eventId !== accepted && row.eventId !== failed)) {
			expect(row.quarantined).toBeUndefined();
			expect(row.reason).toBeUndefined();
		}
		await propagator.reconcile();
		expect(sends).toBe(0);
		expect(
			db.monitorEventRows(monitor.monitorId, "newest", true).filter((row) => [accepted, failed].includes(row.event_id)),
		).toEqual(history);
	} finally {
		await server.stop();
		socket?.end();
	}
});
describe("monitor crash-boundary state machine", () => {
	test("admitted→batched→dispatched→authored via a live dispatch", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "authored note" }))),
		);
		const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
		for (let attempt = 0; attempt < 100 && stage(db, eventId) !== "authored_no_delivery"; attempt++)
			await Bun.sleep(10);
		expect(stage(db, eventId)).toBe("authored_no_delivery");
		expect(db.authoredOutput(eventId)).toBe("authored note");
	});

	test("restart crossing the batched stage: reconcile reclaims a stranded batched row exactly once", async () => {
		let turns = 0;
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) => {
			turns++;
			return JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: `note ${turns}` })));
		});
		const stranded = seedEvent(db, monitor.monitorId, "batched", crypto.randomUUID());
		await propagator.reconcile();
		// No target configured: the reclaimed event settles terminal, not authored-forever.
		expect(stage(db, stranded)).toBe("authored_no_delivery");
		expect(turns).toBe(1);
		// Idempotent: a second sweep does not re-author it.
		await propagator.reconcile();
		expect(turns).toBe(1);
	});

	test("dispatched-stranded rows are reclaimed by reconcile", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "recovered" }))),
		);
		const stranded = seedEvent(db, monitor.monitorId, "dispatched", crypto.randomUUID());
		await propagator.reconcile();
		expect(stage(db, stranded)).toBe("authored_no_delivery");
	});

	test("legacy origin-invalid event types terminalize without invoking the session port", async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-invalid-event-"));
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const db = database;
		const registry = new MonitorRegistry(db);
		const monitor = registry.add({
			name: "valid-monitor",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["valid.event"],
			burstPolicy: "serialize",
		});
		let turns = 0;
		const propagator = new MonitorPropagator({
			database: db,
			registry,
			sessionPort: fakeSessionPort(async () => {
				turns++;
				return "[]";
			}),
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(db)),
			emit: () => {},
		});
		propagators.push(propagator);
		const eventId = crypto.randomUUID();
		db.monitorEventCreate({
			eventId,
			monitorId: monitor.monitorId,
			eventType: "broken/type",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});

		await propagator.reconcile();

		expect(stage(db, eventId)).toBe("failed_no_retry");
		expect(db.monitorFailure(eventId)).toMatchObject({
			code: "event_type_invalid",
			detail: "dispatch setup failed (event_type_invalid)",
		});
		expect(turns).toBe(0);
		await propagator.reconcile();
		expect(turns).toBe(0);
	});

	test("malformed persisted monitors are isolated and their events terminalize once", async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-invalid-record-"));
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const db = database;
		const validRegistry = new MonitorRegistry(db);
		const valid = validRegistry.add({
			name: "healthy",
			trigger: { kind: "cron", schedule: "* * * * *" },
			eventTypes: ["healthy.event"],
		});
		const invalidId = crypto.randomUUID();
		db.monitorCreate({
			id: invalidId,
			name: "legacy-invalid",
			triggerJson: "{not-json",
			eventTypesJson: JSON.stringify(["broken/type"]),
			burstPolicy: "serialize",
			channelTargetJson: null,
			enabled: true,
			instruction: null,
			modelJson: null,
			serviceTier: null,
		});
		const registry = new MonitorRegistry(db);
		expect(registry.list().map((record) => record.monitorId)).toEqual([valid.monitorId]);
		const eventId = crypto.randomUUID();
		db.monitorEventCreate({
			eventId,
			monitorId: invalidId,
			eventType: "broken/type",
			payloadJson: "{}",
			firedAt: new Date().toISOString(),
		});
		let turns = 0;
		const propagator = new MonitorPropagator({
			database: db,
			registry,
			sessionPort: fakeSessionPort(async () => {
				turns++;
				return "[]";
			}),
			memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
			delivery: new DeliveryService(new DeliveryLedger(db)),
			emit: () => {},
		});
		propagators.push(propagator);

		await propagator.reconcile();

		expect(stage(db, eventId)).toBe("failed_no_retry");
		expect(db.monitorFailure(eventId)).toMatchObject({
			code: "monitor_invalid",
			detail: "dispatch setup failed (monitor_invalid)",
		});
		expect(turns).toBe(0);
		await propagator.reconcile();
		expect(db.monitorEventRows().filter((row) => row.event_id === eventId)).toHaveLength(1);
		expect(turns).toBe(0);
	});

	test("authored + confirmed delivery → delivered; failure keeps it authored", async () => {
		const { monitor, database: db } = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		const batchId = crypto.randomUUID();
		const eventId = seedEvent(db, monitor.monitorId, "authored", batchId);
		const delivery = new DeliveryService(new DeliveryLedger(db));
		const payload = delivery.prepare(
			batchId,
			{ platform: "loopback", kind: "loopback", conversationId: "loopback" },
			"note",
		);
		expect(payload).toBeDefined();
		const payload0 = payload as { deliveryId: string };
		const deliveryId = payload0.deliveryId;
		delivery.markInflight(deliveryId);
		// Not delivered before adapter confirmation.
		expect(stage(db, eventId)).toBe("authored");
		// Adapter confirms → delivered.
		expect(delivery.confirm(deliveryId)).toBe("transitioned");
		db.withTransaction(() => db.monitorEventUpdate(eventId, "delivered"));
		expect(stage(db, eventId)).toBe("delivered");
		// Ambiguous failure is distinguishable in the ledger, never delivered.
		const payload2 = delivery.prepare(
			`${batchId}-2`,
			{ platform: "loopback", kind: "loopback", conversationId: "loopback" },
			"note",
		) as { deliveryId: string };
		const deliveryId2 = payload2.deliveryId as string;
		delivery.markInflight(deliveryId2);
		delivery.fail(deliveryId2, true);
		const row = db.deliveryRows().find((entry) => entry.delivery_id === deliveryId2);
		expect(row?.state).toBe("failed_ambiguous");
		expect(stage(db, eventId)).toBe("delivered");
	});

	test("no channel target and no owner target settles authored_no_delivery", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "no target note" }))),
		);
		const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
		for (let attempt = 0; attempt < 100 && stage(db, eventId) !== "authored_no_delivery"; attempt++)
			await Bun.sleep(10);
		expect(stage(db, eventId)).toBe("authored_no_delivery");
		// Terminal: reconcile never redispatches it.
		const turnsBefore = db.monitorEventRows().length;
		await propagator.reconcile();
		expect(db.monitorEventRows().length).toBe(turnsBefore);
		expect(stage(db, eventId)).toBe("authored_no_delivery");
	});

	for (const [label, error, expected] of [
		["CLI exit", new GjcCliError("SECRET_RAW", 42, "SECRET_STDERR"), '"exitCode":42'],
		[
			"runtime code",
			new GjcRuntimeError("SECRET_RAW", { code: "deadline_exceeded", message: "SECRET_RUNTIME" }),
			'"code":"deadline_exceeded"',
		],
		[
			"terminal status",
			new SessionTerminalError({
				operationRef: "SECRET_OP",
				status: { status: "failed", error: { code: "internal_error", message: "SECRET_TERMINAL" } },
			} as never),
			'"terminal":"failed"',
		],
		[
			"untrusted fields",
			Object.assign(new Error("SECRET_RAW"), {
				code: "SECRET_CODE",
				signal: "SECRET_SIGNAL",
				exitCode: -1,
				stack: "at packages/SECRET_PATH.ts:1",
			}),
			'"class":"Error"',
		],
		["missing fields", null, '"class":"unknown"'],
	] as const) {
		test(`injected ${label} persists safe diagnostics and request context`, async () => {
			const { propagator, monitor, database: db, sessionPort } = await harness(async () => "unused");
			sessionPort.request = async () => {
				throw error;
			};
			const eventId = seedEvent(db, monitor.monitorId, "failed");
			await propagator.reconcile();
			const detail = db.monitorFailure(eventId)?.detail ?? "";
			expect(detail).toContain(expected);
			expect(detail).toContain('"phase":"request"');
			expect(detail).toContain('"sessionId":"s1"');
			expect(detail).toContain('"origin":"monitor/eventtype/memory.canonicalize"');
			expect(detail).toContain('"attempt":2');
			expect(detail).not.toContain("SECRET");
			if (label === "untrusted fields" || label === "missing fields") {
				expect(detail).not.toContain('"exitCode"');
				expect(detail).not.toContain('"signal"');
			}
			expect(stage(db, eventId)).toBe("failed");
		});
	}
	test("reconcile reclaim budget: an always-failing event lands on failed_no_retry", async () => {
		let turns = 0;
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async () => {
			turns++;
			throw new Error("turn exploded");
		});
		const eventId = seedEvent(db, monitor.monitorId, "failed");
		for (let sweep = 0; sweep < MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS + 2; sweep++) await propagator.reconcile();
		expect(stage(db, eventId)).toBe("failed_no_retry");
		// Bounded: no more dispatch attempts after the budget.
		const attemptsAfter = turns;
		await propagator.reconcile();
		expect(turns).toBe(attemptsAfter);
		const failure = db.monitorFailure(eventId);
		expect(failure).toBeDefined();
		// Public-safe: no raw error body persisted.
		expect(failure?.detail).toBeDefined();
		expect(failure?.detail).not.toContain("turn exploded");
		expect(failure?.code.length ?? 0).toBeGreaterThan(0);
	});

	test("concurrent reconcile sweeps collapse into one", async () => {
		let turns = 0;
		let release: (() => void) | undefined;
		const parked = new Promise<void>((resolve) => {
			release = resolve;
		});
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) => {
			turns++;
			await parked;
			return JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" })));
		});
		const stranded = seedEvent(db, monitor.monitorId, "batched", crypto.randomUUID());
		const first = propagator.reconcile();
		const second = propagator.reconcile();
		// The second sweep must be a no-op while the first is mid-flight.
		await second;
		expect(turns).toBe(1);
		release?.();
		await first;
		expect(stage(db, stranded)).toBe("authored_no_delivery");
	});

	test("drain waits for a claimed monitor dispatch before teardown", async () => {
		let release: (() => void) | undefined;
		const parked = new Promise<void>((resolve) => {
			release = resolve;
		});
		let turns = 0;
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) => {
			turns++;
			await parked;
			return JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "drained" })));
		});
		const eventId = seedEvent(db, monitor.monitorId, "batched", crypto.randomUUID());
		const reconcile = propagator.reconcile();
		for (let attempt = 0; attempt < 100 && turns === 0; attempt++) await Bun.sleep(5);
		expect(turns).toBe(1);
		let drained = false;
		const drain = propagator.drain().then(() => {
			drained = true;
		});
		await Bun.sleep(20);
		expect(drained).toBe(false);
		release?.();
		await Promise.all([reconcile, drain]);
		expect(drained).toBe(true);
		expect(stage(db, eventId)).toBe("authored_no_delivery");
		expect(() => propagator.submit(monitor.monitorId, "memory.canonicalize", {})).toThrow(
			"monitor propagator is closing",
		);
	});

	test("durable failure detail keeps only allowlisted classes causes and frames", async () => {
		class SecretVendorTokenGhp123 extends Error {}
		const hostile = new SecretVendorTokenGhp123("ghp_attacker-secret prompt=https://secret.example/token");
		(hostile as Error & { code: string }).code = "ghp_attacker_secret";
		hostile.stack = "SecretVendorTokenGhp123: ghp_attacker-secret\n    at steal (/Users/private/token.ts:99:1)";
		let thrown: Error = hostile;
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async () => {
			throw thrown;
		});
		const hostileId = seedEvent(db, monitor.monitorId, "admitted");
		await propagator.reconcile();
		const hostileDetail = db.monitorFailure(hostileId)?.detail ?? "";
		expect(hostileDetail).toContain('"class":"Error"');
		expect(hostileDetail).toContain('"frame":"#dispatchBatch"');
		expect(hostileDetail).not.toContain("ghp_");
		expect(hostileDetail).not.toContain("/Users/");

		const closed = new Error("Database has closed: ghp_attacker-secret");
		closed.stack = "Error: Database has closed\n    at withTransaction (/Users/private/gateway.db:3:4)";
		thrown = closed;
		const closedId = seedEvent(db, monitor.monitorId, "admitted");
		await propagator.reconcile();
		const detail = db.monitorFailure(closedId)?.detail ?? "";
		expect(detail).toContain('"class":"Error"');
		expect(detail).toContain('"cause":"database_closed"');
		expect(detail).toContain('"frame":"#dispatchBatch"');
		expect(detail).not.toContain("ghp_");
		expect(detail).not.toContain("/Users/");
	});

	test("unknown stage writes are rejected fail-closed", async () => {
		const { monitor, database: db } = await harness(async () => "[]");
		const eventId = seedEvent(db, monitor.monitorId, "admitted");
		expect(() => db.monitorEventUpdate(eventId, "corrupt" as never)).toThrow(/unknown monitor event stage/);
		expect(stage(db, eventId)).toBe("admitted");
	});
});

describe("cron slot catch-up", () => {
	// Catch-up tests use synthetic 2026-08-27 clocks; backdate the monitor so its
	// creation instant precedes the scheduled slots under test.
	test("catch-up fires every due slot across a restart window with exact timestamps, deduped", () => {
		const fired: string[] = [];
		// Window crossing the 06:30 slot.
		const from = new Date(2026, 7, 27, 5, 0);
		const now = new Date(2026, 7, 27, 8, 0);
		const count = cronSlotsBetween("30 6 * * *", from, now, 8, (slot) => {
			fired.push(slot.toISOString());
			return true;
		});
		expect(count).toBe(1);
		expect(fired).toEqual([new Date(2026, 7, 27, 6, 30).toISOString()]);
	});

	test("catch-up budget caps a long outage", () => {
		const fired: number[] = [];
		// 24h of hourly slots, budget 8.
		const from = new Date(2026, 7, 26, 0, 0);
		const now = new Date(2026, 7, 27, 0, 0);
		const count = cronSlotsBetween("0 * * * *", from, now, 8, (slot) => {
			fired.push(slot.getHours());
			return true;
		});
		expect(count).toBe(8);
		expect(fired).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
	});

	test("*/30 resume at 07:30 after 06:00 tick fires missed 06:30/07:00 plus 07:30 (always scan)", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		backdateMonitor(db, monitor.monitorId, new Date(2026, 7, 26, 0, 0));
		const fired: string[] = [];
		const clock = { value: new Date(2026, 7, 27, 6, 0) };
		const stop = startCron(
			"*/30 * * * *",
			(slot) => {
				const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
				if (id) {
					fired.push(slot.toISOString());
					return true;
				}
				return false;
			},
			{ now: () => clock.value, intervalMs: 1 },
		);
		// First tick at 06:00 — fresh process, window scan (nothing due before 06:00
		// inside the last hour except 05:30, which is inside the window! It fires:
		// the process has no durable record of it and it is genuinely missed).
		await Bun.sleep(5);
		const initial = fired.length;
		expect(initial).toBeGreaterThanOrEqual(1);
		// Suspend: next tick at 07:30 — the CURRENT minute matches, but the
		// suspended window also contains 06:30 and 07:00, which must fire.
		clock.value = new Date(2026, 7, 27, 7, 30);
		await Bun.sleep(5);
		const sixThirty = new Date(2026, 7, 27, 6, 30).toISOString();
		const seven = new Date(2026, 7, 27, 7, 0).toISOString();
		const sevenThirty = new Date(2026, 7, 27, 7, 30).toISOString();
		expect(fired).toContain(sevenThirty);
		// Exactly-once per slot across the whole run:
		expect(fired.filter((slot) => slot === sixThirty).length).toBeLessThanOrEqual(1);
		// The missed 06:30/07:00 slots (within the 1h window of 07:30) fired:
		if (sixThirty >= new Date(clock.value.getTime() - 60 * 60 * 1000).toISOString()) {
			expect(fired).toContain(sixThirty);
		}
		expect(fired).toContain(seven);
		expect(db.monitorEventRows(monitor.monitorId).length).toBe(fired.length);
		stop();
	});

	test("runtime startup catch-up respects monitor.createdAt (integration)", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-runtime-catchup-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "startup",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "serialize",
				enabled: true,
			});
			const closure = new MemoryClosureQueue(db, raceHome);
			await initializeMemory(raceHome);
			const propagator = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async (_id, text) =>
						JSON.stringify(eventsFromPrompt(text).map(({ eventId: id }) => ({ eventId: id, note: "note" }))),
				}),
				memory: closure,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {},
				emit: () => {},
			});
			// First tick at 06:31: window contains the 06:30 slot. Monitor was created
			// at 06:29 (backdated) → catch-up admits the slot...
			db.monitorSetCreatedAt(monitor.monitorId, new Date(2026, 7, 27, 6, 29).toISOString());
			const runtimeOld = new MonitorRuntime({} as never, registry, propagator, {
				now: () => new Date(2026, 7, 27, 6, 31),
			});
			await runtimeOld.start();
			await runtimeOld.stop();
			await closure.drain();
			expect(db.monitorSlotExists(monitor.monitorId, new Date(2026, 7, 27, 6, 30).toISOString())).toBe(true);
			// Second scenario: a FRESH monitor created at 07:00 must NOT backfill the
			// 06:30 slot on startup (scan window includes it, clamp rejects).
			const monitorNew = registry.add({
				name: "fresh",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "serialize",
				enabled: true,
			});
			const runtimeNew = new MonitorRuntime({} as never, registry, propagator, {
				now: () => new Date(2026, 7, 27, 6, 31),
			});
			await runtimeNew.start();
			await runtimeNew.stop();
			await closure.drain();
			expect(db.monitorSlotExists(monitorNew.monitorId, new Date(2026, 7, 27, 6, 30).toISOString())).toBe(false);
			await propagator.drain();
			await closure.drain();
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});

	test("fresh monitor does not backfill pre-creation slots; restarted old monitor does", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		// The monitor was just created (createdAt ≈ now). A slot scheduled 30
		// minutes BEFORE creation must not be synthesized by catch-up...
		const beforeCreation = new Date(Date.now() - 30 * 60_000);
		const id = propagator.submitSlot(
			monitor.monitorId,
			"memory.canonicalize",
			{ at: beforeCreation.toISOString() },
			beforeCreation,
		);
		expect(id).toBeNull();
		expect(db.monitorSlotExists(monitor.monitorId, beforeCreation.toISOString())).toBe(false);
		// ...while a slot after creation on a RESTARTED (old) monitor still fires:
		const afterCreation = new Date(Date.now() + 30 * 60_000);
		const ok = propagator.submitSlot(
			monitor.monitorId,
			"memory.canonicalize",
			{ at: afterCreation.toISOString() },
			afterCreation,
		);
		expect(ok).not.toBeNull();
	});

	test("budget counts only NEW admissions: 12 claimed duplicates do not starve a missed later slot", () => {
		const claimed = new Set<string>();
		// 12 already-claimed slots (fire returns false = duplicate admission)...
		for (let hour = 1; hour <= 12; hour++) claimed.add(new Date(2026, 7, 26, hour, 0).toISOString());
		const admitted: number[] = [];
		const from = new Date(2026, 7, 26, 0, 0);
		const mid = new Date(2026, 7, 26, 13, 0);
		// ...budget of 8 must still reach the unclaimed 13:00 slot.
		const count = cronSlotsBetween("0 * * * *", from, mid, 8, (slot) => {
			const key = slot.toISOString();
			if (claimed.has(key)) return false;
			claimed.add(key);
			admitted.push(slot.getHours());
			return true;
		});
		expect(count).toBe(1);
		expect(admitted).toEqual([13]);
	});

	test("startCron first tick scans the window; slot claims are exactly-once via submitSlot", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		backdateMonitor(db, monitor.monitorId, new Date(2026, 7, 26, 0, 0));
		const fired: string[] = [];
		// Process "starts" at 06:31 after being down across the 06:30 slot.
		const stop = startCron(
			"30 6 * * *",
			(slot) => {
				const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
				if (id) {
					fired.push(slot.toISOString());
					return true;
				}
				return false;
			},
			{ now: () => new Date(2026, 7, 27, 6, 31) },
		);
		stop();
		expect(fired).toEqual([new Date(2026, 7, 27, 6, 30).toISOString()]);
		// Slot row + event row linked (atomic admission).
		const admitted = db.monitorEventRows(monitor.monitorId);
		expect(admitted).toHaveLength(1);
		expect(admitted[0]?.fired_at).toBe(new Date(2026, 7, 27, 6, 30).toISOString());
		// Second process starting at the same minute: slot already claimed, no refire.
		const fired2: string[] = [];
		const stop2 = startCron(
			"30 6 * * *",
			(slot) => {
				const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
				if (id) {
					fired2.push(slot.toISOString());
					return true;
				}
				return false;
			},
			{ now: () => new Date(2026, 7, 27, 6, 31) },
		);
		stop2();
		expect(fired2).toEqual([]);
		expect(db.monitorEventRows(monitor.monitorId)).toHaveLength(1);
	});

	test("startCron skips slots older than the catch-up window", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		backdateMonitor(db, monitor.monitorId, new Date(2026, 7, 26, 0, 0));
		const fired: string[] = [];
		// Process starts at 20:00; the 06:30 slot from the same day is far outside the
		// bounded window and must NOT fire.
		const stop = startCron(
			"30 6 * * *",
			(slot) => {
				const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
				if (id) {
					fired.push(slot.toISOString());
					return true;
				}
				return false;
			},
			{ now: () => new Date(2026, 7, 27, 20, 0) },
		);
		stop();
		expect(fired).toEqual([]);
		expect(db.monitorSlotExists(monitor.monitorId, new Date(2026, 7, 27, 6, 30).toISOString())).toBe(false);
	});

	test("suspension of +60m onto the same wall-minute still fires missed slots (minute EPOCH)", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		backdateMonitor(db, monitor.monitorId, new Date(2026, 7, 26, 0, 0));
		const clock = { value: new Date(2026, 7, 27, 5, 30) };
		const fired: string[] = [];
		const stop = startCron(
			"30 6 * * *",
			(slot) => {
				const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
				if (id) {
					fired.push(slot.toISOString());
					return true;
				}
				return false;
			},
			{ now: () => clock.value, intervalMs: 1 },
		);
		// First tick at 05:30 — window scan, nothing due yet.
		expect(fired).toEqual([]);
		// Suspend: jump exactly +60m to 06:30 (same wall-minute as... different minute
		// here, but the next jump lands on the same minute-of-hour to defeat
		// minute-of-hour dedupe): 06:30 → 07:30.
		clock.value = new Date(2026, 7, 27, 6, 30);
		await Bun.sleep(1);
		// One more 30s-tick at 06:30 would double-fire without slot claims; nothing new.
		clock.value = new Date(2026, 7, 27, 6, 30, 30);
		await Bun.sleep(1);
		// +60m suspension: 06:30:30 → 07:30:30. Minute-of-hour changed 30→30? NO —
		// minuteEpoch advanced, and the window contains the 06:30 slot... but that
		// already fired. A second hourly-style monitor scenario: use a "30 6" schedule;
		// the window (06:30:30, 07:30:30] contains NO 06:30 slot, so nothing fires.
		clock.value = new Date(2026, 7, 27, 7, 30, 30);
		await Bun.sleep(1);
		expect(fired).toEqual([new Date(2026, 7, 27, 6, 30).toISOString()]);
		expect(db.monitorEventRows(monitor.monitorId)).toHaveLength(1);
		stop();
	});

	test("slot admission is atomic: claim+event commit together, duplicate claims never double-admit", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		backdateMonitor(db, monitor.monitorId, new Date(2026, 7, 26, 0, 0));
		const slotAt = new Date(2026, 7, 27, 6, 30);
		// The ONLY production admission path is the atomic claim+event transaction:
		// there is no intermediate "claimed but unadmitted" state a crash can leave.
		const first = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slotAt.toISOString() }, slotAt);
		expect(first).not.toBeNull();
		expect(db.monitorEventRows(monitor.monitorId)).toHaveLength(1);
		// A second claim attempt for the same slot (restart catch-up overlap) is a
		// no-op: no second event, no duplicate firing.
		const second = propagator.submitSlot(
			monitor.monitorId,
			"memory.canonicalize",
			{ at: slotAt.toISOString() },
			slotAt,
		);
		expect(second).toBeNull();
		expect(db.monitorEventRows(monitor.monitorId)).toHaveLength(1);
	});

	test("slot payload carries the exact scheduled timestamp", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		backdateMonitor(db, monitor.monitorId, new Date(2026, 7, 26, 0, 0));
		const slotAt = new Date(2026, 7, 27, 6, 30);
		const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slotAt.toISOString() }, slotAt);
		expect(id).not.toBeNull();
		const rows = db.monitorEventRows(monitor.monitorId);
		const payloadJson = rows[0]?.payload_json;
		expect(payloadJson).toBeDefined();
		expect(JSON.parse(payloadJson as string).at).toBe(slotAt.toISOString());
		// fired_at itself is the scheduled slot time (blocker 3), not submit-time now.
		expect(rows[0]?.fired_at).toBe(slotAt.toISOString());
	});
});

describe("durable dispatch leases (restart-concurrent authoring)", () => {
	test("lease claim is exclusive; a second claim before expiry fails", async () => {
		const { monitor, database: db } = await harness(async () => "[]");
		const eventId = seedEvent(db, monitor.monitorId, "batched", crypto.randomUUID());
		const now = Date.now();
		expect(db.monitorEventAcquireLease(eventId, "proc-A", "lease-A1", 60_000, now)).toBe(true);
		// Process B (restart) cannot steal a LIVE lease...
		expect(db.monitorEventAcquireLease(eventId, "proc-B", "lease-B1", 60_000, now + 1000)).toBe(false);
		// ...only after it expires.
		expect(db.monitorEventAcquireLease(eventId, "proc-B", "lease-B1", 60_000, now + 61_000)).toBe(true);
		// The stale attempt A1 can no longer release or hold the lease.
		db.monitorEventReleaseLease(eventId, "lease-A1");
		expect(db.monitorEventLeaseHeld(eventId, "lease-A1", now + 61_000)).toBe(false);
		expect(db.monitorEventLeaseHeld(eventId, "lease-B1", now + 61_000)).toBe(true);
	});

	test("an event with a live lease is not reclaimed by a new process's reconcile", async () => {
		let turns = 0;
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) => {
			turns++;
			return JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" })));
		});
		const eventId = seedEvent(db, monitor.monitorId, "batched", crypto.randomUUID());
		// Another process still holds a live lease on this event (its gjc turn may
		// still be running there).
		expect(db.monitorEventAcquireLease(eventId, "proc-old", "lease-old", 60_000)).toBe(true);
		await propagator.reconcile();
		expect(turns).toBe(0);
		expect(stage(db, eventId)).toBe("batched");
		// Once the lease expires, reclaim succeeds.
		await Bun.sleep(5);
		// Expire by acquiring with a now past the TTL.
		expect(db.monitorEventAcquireLease(eventId, "proc-new", "lease-new", 60_000, Date.now() + 61_000)).toBe(true);
	});

	test("authored monitor intent is processed to receipted in the same live process, exactly once", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-same-run-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "same-run",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "serialize",
				channelTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } },
				enabled: true,
			});
			await initializeMemory(raceHome);
			const closure = new MemoryClosureQueue(db, raceHome);
			let deliveries = 0;
			const propagator = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async (_id, text) =>
						JSON.stringify(eventsFromPrompt(text).map(({ eventId: id }) => ({ eventId: id, note: "note" }))),
				}),
				memory: closure,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {
					deliveries++;
				},
				emit: () => {},
			});
			propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "same-run" });
			// Wait deterministically for the async serialize dispatch to admit the
			// intent (bounded poll, no fixed-sleep assumption), then drain the
			// closure queue: the intent must reach receipted in this live process.
			for (let attempt = 0; attempt < 400; attempt++) {
				if (db.memoryIntentRows().some((row) => row.kind === "monitor-event")) break;
				await Bun.sleep(5);
			}
			await closure.drain();
			const intents = db.memoryIntentRows().filter((row) => row.kind === "monitor-event");
			expect(intents).toHaveLength(1);
			expect(intents[0]?.state).toBe("receipted");
			// Duplicate/reconcile re-admission must not create a second intent.
			const before = db.memoryIntentRows().length;
			const row = db
				.monitorEventRows()
				.find((candidate) => candidate.event_id !== "" && candidate.stage === "authored");
			expect(row).toBeDefined();
			const eventRow = row as {
				event_id: string;
				event_type: string;
				fired_at: string;
			};
			db.monitorEventFencedAuthorWithIntent(
				eventRow.event_id,
				"",
				"note",
				false,
				eventRow.event_type,
				`monitor-event-intent:${eventRow.event_id}`,
				JSON.stringify({ platform: "monitor", kind: "eventtype", conversationId: "memory.canonicalize" }),
			);
			expect(db.memoryIntentRows().length).toBe(before);
			expect(deliveries).toBe(1);
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});

	test("heartbeat renewal is lease-guarded and uses the injectable clock", async () => {
		const { monitor, database: db } = await harness(async () => "[]");
		const eventId = seedEvent(db, monitor.monitorId, "batched", crypto.randomUUID());
		const now = Date.now();
		// TTL 60s claimed at `now`; renewed at now+50s with the same TTL extends to
		// now+110s (renewal takes effect from the renewal instant).
		expect(db.monitorEventAcquireLease(eventId, "proc-A", "lease-A", 60_000, now)).toBe(true);
		expect(db.monitorEventLiveLeaseOwner(eventId, now + 50_000)).toBe("lease-A");
		expect(db.monitorEventRenewLease(eventId, "lease-A", 60_000, now + 50_000)).toBe(true);
		expect(db.monitorEventLiveLeaseOwner(eventId, now + 100_000)).toBe("lease-A");
		expect(db.monitorEventLiveLeaseOwner(eventId, now + 110_001)).toBeUndefined();
		// A stale attempt (expired lease) cannot renew:
		expect(db.monitorEventRenewLease(eventId, "lease-A", 60_000, now + 200_000)).toBe(false);
	});

	test("stale attempt completion cannot overwrite a newer claim's outcome", async () => {
		const { monitor, database: db } = await harness(async () => "[]");
		const eventId = seedEvent(db, monitor.monitorId, "batched", crypto.randomUUID());
		const now = Date.now();
		// Attempt A claims and starts a long authoring turn...
		expect(db.monitorEventAcquireLease(eventId, "proc-A", "lease-A", 60_000, now)).toBe(true);
		// ...A's process dies; the lease expires; process B steals the claim.
		expect(db.monitorEventAcquireLease(eventId, "proc-B", "lease-B", 60_000, now + 61_000)).toBe(true);
		// A's authoring turn finally completes and tries to write its result. The
		// stale attempt does NOT hold the lease anymore:
		expect(db.monitorEventLeaseHeld(eventId, "lease-A", now + 61_000)).toBe(false);
		// A lease-guarded release is a no-op on B's claim:
		db.monitorEventReleaseLease(eventId, "lease-A");
		expect(db.monitorEventLiveLeaseOwner(eventId, now + 61_000)).toBe("lease-B");
		// B completes and releases cleanly:
		db.monitorEventReleaseLease(eventId, "lease-B");
		expect(db.monitorEventLiveLeaseOwner(eventId, now + 61_000)).toBeUndefined();
	});
});

describe("durable dispatch leases — concurrent attempts (true overlap)", () => {
	test("all fences rejected: leases released, no heartbeat leak, no dispatch", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-fences-rej-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "fences",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "serialize",
				enabled: true,
			});
			let responseRan = false;
			const propagator = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async () => {
						responseRan = true;
						return "[]";
					},
				}),
				memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {},
				emit: () => {},
				// Force the initial fenced batching to fail for every row while the
				// acquire succeeds — exercising the all-fences-rejected branch.
				fencedUpdate: () => false,
			});
			propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "x" });
			await Bun.sleep(80);
			expect(responseRan).toBe(false);
			// Every acquire was rolled back: no live leases remain (no leak).
			expect(db.monitorLeaseLiveCount()).toBe(0);
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});

	test("claim race: the production loser performs no writes of any kind", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-claim-race-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "race-claim",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "serialize",
				enabled: true,
			});
			let turnsA = 0;
			let turnsB = 0;
			let deliveriesB = 0;
			let bAcquireCalls = 0;
			let bFencedBatchCalls = 0;
			const propagatorA = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async (_id, text) => {
						turnsA++;
						return JSON.stringify(eventsFromPrompt(text).map(({ eventId: id }) => ({ eventId: id, note: "note-A" })));
					},
				}),
				memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {},
				emit: () => {},
			});
			// Loser B: its lease-acquire ALWAYS returns false (deterministic claim
			// loss via the injected seam — production dispatch path unchanged). The
			// seam counters prove the REAL #dispatchBatch path was reached.
			const propagatorB = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async () => {
						turnsB++;
						return "[]";
					},
				}),
				memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {
					deliveriesB++;
				},
				emit: () => {},
				acquireLease: () => {
					bAcquireCalls++;
					return false;
				},
				fencedUpdate: () => {
					bFencedBatchCalls++;
					return false;
				},
			});
			// Seed ONE recoverable event with NO live lease (crash-mid-dispatch
			// shape). Both dispatchers reconcile; B's atomic claim deterministically
			// loses via the injected seam.
			const seeded = crypto.randomUUID();
			db.monitorEventCreate({
				eventId: seeded,
				monitorId: monitor.monitorId,
				eventType: "memory.canonicalize",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			// B loses: reconcile reaches #dispatchBatch (seam called exactly once),
			// the false acquire pushes nothing, and no downstream write occurs.
			await propagatorB.reconcile();
			expect(bAcquireCalls).toBe(1);
			expect(bFencedBatchCalls).toBe(0);
			expect(turnsB).toBe(0);
			expect(deliveriesB).toBe(0);
			const seededRow = db.monitorEventRows().find((candidate) => candidate.event_id === seeded);
			expect(seededRow?.stage).toBe("admitted");
			expect(db.authoredOutput(seeded)).toBeUndefined();
			expect(db.monitorFailure(seeded)).toBeUndefined();
			expect(db.memoryIntentRows().filter((row) => row.payload_json.includes(seeded))).toHaveLength(0);
			expect(db.deliveryRows()).toHaveLength(0);
			// A then claims and completes the seeded event through its own chain.
			await propagatorA.reconcile();
			expect(turnsA).toBe(1);
			expect(db.monitorEventRows().find((candidate) => candidate.event_id === seeded)?.stage).toBe(
				"authored_no_delivery",
			);
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});

	test("partial authoring response: omitted dispatched events never become delivered", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-partial-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "partial",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "coalesce",
				enabled: true,
			});
			const closure = new MemoryClosureQueue(db, raceHome);
			await initializeMemory(raceHome);
			let deliveries = 0;
			const propagator = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async () =>
						// Malicious/partial response: omits the second event entirely.
						JSON.stringify([]),
				}),
				memory: closure,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {
					deliveries++;
				},
				emit: () => {},
			});
			const first = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: 1 });
			const second = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: 2 });
			await Bun.sleep(400);
			await closure.drain();
			// The partial response is a structured failure: rows failed (recoverable),
			// never authored, and no delivery admitted.
			expect(deliveries).toBe(0);
			expect(db.deliveryRows()).toHaveLength(0);
			expect(db.monitorFailure(first)).toBeDefined();
			expect(db.monitorFailure(second)).toBeDefined();
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});

	test("atomic confirm+settle repairs legacy split state; crash window closed", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-atomic-confirm-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "atomic-confirm",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				enabled: true,
			});
			const batchId = crypto.randomUUID();
			const eventId = crypto.randomUUID();
			db.monitorEventCreate({
				eventId,
				monitorId: monitor.monitorId,
				eventType: "memory.canonicalize",
				payloadJson: "{}",
				firedAt: new Date().toISOString(),
			});
			db.monitorEventUpdate(eventId, "authored", batchId);
			const delivery = new DeliveryService(new DeliveryLedger(db));
			const payload = delivery.prepare(
				batchId,
				{ platform: "loopback", kind: "loopback", conversationId: "loopback" },
				"note",
			);
			const deliveryId = payload!.deliveryId as string;
			delivery.markInflight(deliveryId);
			// Atomic path: one call transitions ledger AND settles events.
			const outcome = db.deliveryConfirmWithSettle(deliveryId, "delivered");
			expect(outcome).toBe("transitioned");
			expect(db.deliveryRows().find((row) => row.delivery_id === deliveryId)?.state).toBe("confirmed");
			expect(stage(db, eventId)).toBe("delivered");
			// Legacy split-state repair: simulate confirmed ledger + authored event.
			db.monitorEventUpdate(eventId, "authored");
			expect(stage(db, eventId)).toBe("authored");
			// Idempotent re-confirm repairs the legacy split:
			expect(db.deliveryConfirmWithSettle(deliveryId, "delivered")).toBe("already_terminal");
			expect(stage(db, eventId)).toBe("delivered");
			// Expired + late confirm leaves events authored (unchanged behavior).
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});

	test("submitAwaitable rejects non-serialize policies (honest seam contract)", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-await-seam-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "coalesced",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "coalesce",
				enabled: true,
			});
			const propagator = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async (_id, text) =>
						JSON.stringify(eventsFromPrompt(text).map(({ eventId: id }) => ({ eventId: id, note: "n" }))),
				}),
				memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {},
				emit: () => {},
			});
			expect(propagator.submitAwaitable(monitor.monitorId, "memory.canonicalize", { at: "x" })).rejects.toThrow(
				/only supports burstPolicy 'serialize'/,
			);
			// And an unknown/disabled monitor is still rejected:
			await expect(propagator.submitAwaitable("nope", "memory.canonicalize", {})).rejects.toThrow(
				/unknown or disabled monitor/,
			);
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});

	test("stale attempt A cannot overwrite B's outcome, enqueue, or re-deliver after losing its lease", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-lease-race-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "race",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "serialize",
				channelTarget: { origin: { platform: "loopback", kind: "loopback", conversationId: "loopback" } },
				enabled: true,
			});

			// Attempt A parks inside its real propagator dispatch (the response waits,
			// then returns an A note only after B has completed — the stale completion
			// path the fencing must neutralize). A uses the awaitable seam so we hold
			// the exact original dispatch promise.
			let releaseA!: () => void;
			const aParked = new Promise<void>((resolve) => {
				releaseA = resolve;
			});
			let markAReturned!: () => void;
			const aReturned = new Promise<void>((resolve) => {
				markAReturned = resolve;
			});
			let aUnblocked = false;
			let deliveriesA = 0;
			const propagatorA = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async () => {
						releaseA();
						await new Promise<void>((resolve) => {
							const check = setInterval(() => {
								if (aUnblocked) {
									clearInterval(check);
									resolve();
								}
							}, 2);
						});
						const note = JSON.stringify([{ eventId, note: "A note" }]);
						markAReturned();
						return note;
					},
				}),
				memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {
					deliveriesA++;
				},
				emit: () => {},
			});
			let eventId = "";
			const aDispatch: Promise<string> = propagatorA
				.submitAwaitable(monitor.monitorId, "memory.canonicalize", { at: "a" })
				.then((submitted) => {
					eventId = submitted;
					return submitted;
				});
			await aParked;
			await Bun.sleep(10);
			eventId = db.monitorEventRows()[0]?.event_id ?? eventId;
			const leaseA = db.monitorEventLiveLeaseOwner(eventId);
			expect(leaseA).toBeString();

			// A's lease "expires": attempt B (clock shifted past A's TTL) finds no
			// live lease, acquires its own via the SAME awaitable seam, and runs the
			// full real dispatch to completion.
			const bClockShift = 10 * 60_000 + 5_000;
			let deliveriesB = 0;
			const propagatorB = new MonitorPropagator({
				database: db,
				registry,
				sessionPort: sessionPortFromScript({
					bind: async () => ({ sessionId: "s" }),
					respond: async (_id, text) =>
						JSON.stringify(eventsFromPrompt(text).map(({ eventId: id }) => ({ eventId: id, note: "B note" }))),
				}),
				memory: { enqueue: () => crypto.randomUUID(), enqueueExistingId: () => {} } as never,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {
					deliveriesB++;
				},
				emit: () => {},
				now: () => Date.now() + bClockShift,
			});
			// B's production recovery path: reconcile claims the expired event and
			// dispatches it (real chain; asserted below by deliveries/intents).
			await propagatorB.reconcile();
			const row = db.monitorEventRows().find((candidate) => candidate.event_id === eventId);
			expect(row?.stage).toBe("authored");
			expect(db.authoredOutput(eventId)).toBe("B note");
			expect(deliveriesB).toBe(1);

			// Unblock A: its stale attempt completes with an A note. Await A's exact
			// original dispatch promise — every write is lease-fenced (no-op).
			aUnblocked = true;
			await aReturned;
			await aDispatch;
			const freshRow = db.monitorEventRows().find((candidate) => candidate.event_id === eventId);
			expect(freshRow?.stage).toBe("authored");
			expect(db.authoredOutput(eventId)).toBe("B note");
			// Exactly one deterministic memory intent (B's; A fenced out):
			const intents = db.memoryIntentRows().filter((intent) => intent.payload_json.includes(eventId));
			expect(intents).toHaveLength(1);
			expect(intents[0]?.id).toBe(`monitor-event-intent:${eventId}`);
			expect(intents[0]?.payload_json).toContain("B note");
			// Exactly one ledger delivery row (B's); A emitted none:
			expect(db.deliveryRows()).toHaveLength(1);
			expect(deliveriesA).toBe(0);
			expect(deliveriesB).toBe(1);
			// No A failure evidence:
			expect(db.monitorFailure(eventId)).toBeUndefined();
			// Lease state explicit: B released on completion → owner undefined.
			expect(db.monitorEventLiveLeaseOwner(eventId, Date.now() + bClockShift)).toBeUndefined();
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});
});
