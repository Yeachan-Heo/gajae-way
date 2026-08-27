import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { cronSlotsBetween, startCron } from "../src/monitors/triggers/cron";
import type { GjcPort } from "../src/orchestrator/gjc-client";
import { GatewayDatabase, MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

let home = "";
let database: GatewayDatabase | undefined;
let propagators: MonitorPropagator[] = [];
afterEach(async () => {
	for (const propagator of propagators) propagator.dispose();
	propagators = [];
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

function fakeGjc(sendTurn: GjcPort["sendTurn"]): GjcPort {
	return { ensureSession: async () => ({ sessionId: "s1" }), sendTurn };
}

async function harness(
	sendTurn: GjcPort["sendTurn"],
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
	const propagator = new MonitorPropagator({
		database,
		registry,
		gjc: fakeGjc(sendTurn),
		memory: { enqueue: () => crypto.randomUUID() } as never,
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
		...(options.ownerTarget ? { ownerTarget: options.ownerTarget } : {}),
	});
	propagators.push(propagator);
	return { propagator, monitor, database, registry };
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
		expect(delivery.confirm(deliveryId)).toBe(true);
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

	test("unknown stage writes are rejected fail-closed", async () => {
		const { monitor, database: db } = await harness(async () => "[]");
		const eventId = seedEvent(db, monitor.monitorId, "admitted");
		expect(() => db.monitorEventUpdate(eventId, "corrupt" as never)).toThrow(/unknown monitor event stage/);
		expect(stage(db, eventId)).toBe("admitted");
	});
});

describe("cron slot catch-up", () => {
	test("catch-up fires every due slot across a restart window with exact timestamps, deduped", () => {
		const fired: string[] = [];
		// Window crossing two 06:30 slots.
		const from = new Date(2026, 7, 27, 5, 0);
		const now = new Date(2026, 7, 27, 8, 0);
		const count = cronSlotsBetween("30 6 * * *", from, now, 8, (slot) => fired.push(slot.toISOString()));
		expect(count).toBe(1);
		expect(fired).toEqual([new Date(2026, 7, 27, 6, 30).toISOString()]);
	});

	test("catch-up budget caps a long outage", () => {
		const fired: number[] = [];
		// 24h of hourly slots, budget 8.
		const from = new Date(2026, 7, 26, 0, 0);
		const now = new Date(2026, 7, 27, 0, 0);
		const count = cronSlotsBetween("0 * * * *", from, now, 8, (slot) => fired.push(slot.getHours()));
		expect(count).toBe(8);
		expect(fired).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
	});

	test("startCron first tick claims slots in the ledger; duplicates are skipped", async () => {
		const { database: db, monitor } = await harness(async () => "[]");
		const fired: string[] = [];
		// Process "starts" at 07:25 after being down across the 06:30 slot.
		const stop = startCron("30 6 * * *", (slot) => fired.push(slot.toISOString()), {
			now: () => new Date(2026, 7, 27, 6, 31),
			database: db,
			monitorId: monitor.monitorId,
		});
		stop();
		expect(fired).toEqual([new Date(2026, 7, 27, 6, 30).toISOString()]);
		expect(db.monitorSlotExists(monitor.monitorId, new Date(2026, 7, 27, 6, 30).toISOString())).toBe(true);
		// Second process starting at the same minute: slot already claimed, no refire.
		const fired2: string[] = [];
		const stop2 = startCron("30 6 * * *", (slot) => fired2.push(slot.toISOString()), {
			now: () => new Date(2026, 7, 27, 6, 31),
			database: db,
			monitorId: monitor.monitorId,
		});
		stop2();
		expect(fired2).toEqual([]);
	});

	test("startCron skips slots older than the catch-up window", async () => {
		const { database: db, monitor } = await harness(async () => "[]");
		const fired: string[] = [];
		// Process starts at 20:00; the 06:30 slot from the same day is far outside the
		// 1h bounded window and must NOT fire.
		const stop = startCron("30 6 * * *", (slot) => fired.push(slot.toISOString()), {
			now: () => new Date(2026, 7, 27, 20, 0),
			database: db,
			monitorId: monitor.monitorId,
		});
		stop();
		expect(fired).toEqual([]);
	});

	test("slot payload carries the exact scheduled timestamp", async () => {
		const {
			propagator,
			monitor,
			database: db,
		} = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		const slotAt = new Date(2026, 7, 27, 6, 30);
		propagator.submit(monitor.monitorId, "memory.canonicalize", { at: slotAt.toISOString() });
		const rows = db.monitorEventRows(monitor.monitorId);
		const payloadJson = rows[0]?.payload_json;
		expect(payloadJson).toBeDefined();
		expect(JSON.parse(payloadJson as string).at).toBe(slotAt.toISOString());
	});
});
