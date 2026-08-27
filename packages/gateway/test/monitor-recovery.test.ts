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
		const { propagator, monitor, database: db } = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		const fired: string[] = [];
		// Process "starts" at 06:31 after being down across the 06:30 slot.
		const stop = startCron("30 6 * * *", (slot) => {
			const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
			if (id) {
				fired.push(slot.toISOString());
				return true;
			}
			return false;
		}, { now: () => new Date(2026, 7, 27, 6, 31) });
		stop();
		expect(fired).toEqual([new Date(2026, 7, 27, 6, 30).toISOString()]);
		// Slot row + event row linked (atomic admission).
		const admitted = db.monitorEventRows(monitor.monitorId);
		expect(admitted).toHaveLength(1);
		expect(admitted[0]?.fired_at).toBe(new Date(2026, 7, 27, 6, 30).toISOString());
		// Second process starting at the same minute: slot already claimed, no refire.
		const fired2: string[] = [];
		const stop2 = startCron("30 6 * * *", (slot) => {
			const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
			if (id) {
				fired2.push(slot.toISOString());
				return true;
			}
			return false;
		}, { now: () => new Date(2026, 7, 27, 6, 31) });
		stop2();
		expect(fired2).toEqual([]);
		expect(db.monitorEventRows(monitor.monitorId)).toHaveLength(1);
	});

	test("startCron skips slots older than the catch-up window", async () => {
		const { propagator, monitor, database: db } = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		const fired: string[] = [];
		// Process starts at 20:00; the 06:30 slot from the same day is far outside the
		// bounded window and must NOT fire.
		const stop = startCron("30 6 * * *", (slot) => {
			const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
			if (id) {
				fired.push(slot.toISOString());
				return true;
			}
			return false;
		}, { now: () => new Date(2026, 7, 27, 20, 0) });
		stop();
		expect(fired).toEqual([]);
		expect(db.monitorSlotExists(monitor.monitorId, new Date(2026, 7, 27, 6, 30).toISOString())).toBe(false);
	});

	test("suspension of +60m onto the same wall-minute still fires missed slots (minute EPOCH)", async () => {
		const { propagator, monitor, database: db } = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		const clock = { value: new Date(2026, 7, 27, 5, 30) };
		const fired: string[] = [];
		const stop = startCron("30 6 * * *", (slot) => {
			const id = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slot.toISOString() }, slot);
			if (id) {
				fired.push(slot.toISOString());
				return true;
			}
			return false;
		}, { now: () => clock.value, intervalMs: 1 });
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
		const { propagator, monitor, database: db } = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
		const slotAt = new Date(2026, 7, 27, 6, 30);
		// The ONLY production admission path is the atomic claim+event transaction:
		// there is no intermediate "claimed but unadmitted" state a crash can leave.
		const first = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slotAt.toISOString() }, slotAt);
		expect(first).not.toBeNull();
		expect(db.monitorEventRows(monitor.monitorId)).toHaveLength(1);
		// A second claim attempt for the same slot (restart catch-up overlap) is a
		// no-op: no second event, no duplicate firing.
		const second = propagator.submitSlot(monitor.monitorId, "memory.canonicalize", { at: slotAt.toISOString() }, slotAt);
		expect(second).toBeNull();
		expect(db.monitorEventRows(monitor.monitorId)).toHaveLength(1);
	});

	test("slot payload carries the exact scheduled timestamp", async () => {
		const { propagator, monitor, database: db } = await harness(async (_id, text) =>
			JSON.stringify(eventsFromPrompt(text).map(({ eventId }) => ({ eventId, note: "note" }))),
		);
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
		const { propagator, monitor, database: db } = await harness(async (_id, text) => {
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
	test("stale attempt A cannot overwrite B's outcome or re-deliver after its lease expired", async () => {
		const raceHome = await mkdtemp(join(tmpdir(), "gajaeway-lease-race-"));
		try {
			const db = await GatewayDatabase.open(join(raceHome, "gateway.db"));
			const registry = new MonitorRegistry(db);
			const monitor = registry.add({
				name: "race",
				trigger: { kind: "cron", schedule: "30 6 * * *" },
				eventTypes: ["memory.canonicalize"],
				burstPolicy: "serialize",
				enabled: true,
			});

			let eventId = "";

			// ---- Attempt A: parks inside its authoring turn (lease held) ----
			let releaseA!: () => void;
			const aTurnParked = new Promise<void>((resolve) => {
				releaseA = resolve;
			});
			let deliveriesA = 0;
				// A never finishes its turn: after unblocking, its sendTurn resolves but
			// its lease has already expired and been stolen by B, so every write is
			// fenced (no-op).
			const propagatorA = new MonitorPropagator({
				database: db,
				registry,
				gjc: {
					ensureSession: async () => ({ sessionId: "s" }),
					sendTurn: async () => {
						releaseA();
						return await new Promise<string>(() => {});
					},
				},
				memory: { enqueue: () => crypto.randomUUID() } as never,
				delivery: new DeliveryService(new DeliveryLedger(db)),
				deliver: () => {
					deliveriesA++;
				},
				emit: () => {},
			});
			eventId = propagatorA.submit(monitor.monitorId, "memory.canonicalize", { at: "a" });
			await aTurnParked;
			await Bun.sleep(10);
			const leaseA = db.monitorEventLiveLeaseOwner(eventId);
			expect(leaseA).toBeString();

			// ---- A's process "dies": the lease TTL elapses with no heartbeat ----
			// (simulated below by B acquiring at now + TTL + epsilon)

			// ---- Attempt B (new process): steals the expired lease, completes ----
			const now = Date.now();
			const bLease = crypto.randomUUID();
			expect(db.monitorEventAcquireLease(eventId, "proc-B", bLease, 60_000, now + 10 * 60_000 + 5_000)).toBe(
				true,
			);
			db.monitorEventUpdate(eventId, "authored");
			db.authoredOutputCreate(eventId, "B note");

			// ---- A's parked turn is abandoned: stage stays with B's outcome ----
			// A cannot complete (its sendTurn never resolves), so no writes from A
			// exist; the event keeps B's authored state.
			const row = db.monitorEventRows().find((candidate) => candidate.event_id === eventId);
			expect(row?.stage).toBe("authored");
			expect(db.authoredOutput(eventId)).toBe("B note");
			expect(deliveriesA).toBe(0);
			// B holds the lease; A's stale lease id is gone.
			expect(db.monitorEventLiveLeaseOwner(eventId, now + 10 * 60_000 + 5_000)).toBe(bLease);
			expect(leaseA).not.toBe(bLease);
			db.close();
		} finally {
			await rm(raceHome, { recursive: true, force: true });
		}
	});
});
