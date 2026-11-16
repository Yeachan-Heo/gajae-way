import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MemoryClosureQueue } from "../src/memory/closure";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import { Store } from "../src/store";
import type { SessionPortResponder } from "./session-port.fake";
import { sessionPortFromScript } from "./session-port.fake";

let home = "";
let database: Store | undefined;
afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function harness(respond: SessionPortResponder) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-monitor-"));
	database = await Store.open(join(home, "gateway.db"));
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
		sessionPort: sessionPortFromScript({ bind: async () => ({ sessionId: "s1" }), respond }),
		memory: new MemoryClosureQueue(database, home),
		delivery: new DeliveryService(database),
		emit: () => {},
	});
	return { propagator, monitor, database };
}

const stage = (db: Store, id: string) => db.monitors.eventRows().find((row) => row.event_id === id)?.stage;

test("an event stranded at batched by a dead process is recovered by reconcile", async () => {
	const {
		propagator,
		monitor,
		database: db,
	} = await harness(async () => JSON.stringify([{ eventId: "PLACEHOLDER", note: "done" }]));

	// Simulate the crash: the row is left at `batched` with no authored output, exactly the
	// state a gateway restart mid-dispatch leaves behind.
	const eventId = crypto.randomUUID();
	db.transaction((tx) => {
		db.monitors.eventCreate(tx, {
			eventId,
			monitorId: monitor.monitorId,
			eventType: "memory.canonicalize",
			payloadJson: JSON.stringify({ at: new Date().toISOString() }),
			firedAt: new Date().toISOString(),
		});
		db.monitors.eventUpdate(tx, eventId, "batched", crypto.randomUUID());
	});
	expect(stage(db, eventId)).toBe("batched");
	expect(db.monitors.authoredOutput(eventId)).toBeUndefined();

	await propagator.reconcile();

	// Recovery re-dispatched it: it is no longer parked at batched.
	expect(stage(db, eventId)).not.toBe("batched");
});

test("reconcile does not touch an event this process is still dispatching", async () => {
	let release: (() => void) | undefined;
	const parked = new Promise<void>((resolve) => {
		release = resolve;
	});
	let turns = 0;
	const {
		propagator,
		monitor,
		database: db,
	} = await harness(async () => {
		turns++;
		await parked;
		return JSON.stringify([{ eventId: "x", note: "done" }]);
	});

	const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
	// Let the 250ms burst window elapse so the dispatch is genuinely in flight.
	for (let attempt = 0; attempt < 80 && stage(db, eventId) !== "batched"; attempt++) await Bun.sleep(10);
	expect(stage(db, eventId)).toBe("batched");
	expect(turns).toBe(1);

	await propagator.reconcile();
	expect(turns).toBe(1);

	release?.();
	await Bun.sleep(50);
});

test("a failed event is still recovered, as before", async () => {
	const {
		propagator,
		monitor,
		database: db,
	} = await harness(async () => {
		throw new Error("turn exploded");
	});
	const eventId = propagator.submit(monitor.monitorId, "memory.canonicalize", { at: "now" });
	for (let attempt = 0; attempt < 80 && stage(db, eventId) !== "failed"; attempt++) await Bun.sleep(10);
	expect(stage(db, eventId)).toBe("failed");
	await propagator.reconcile();
	expect(stage(db, eventId)).toBe("failed");
});

test("a mid-batch fence failure never leaves a coalesced authoring pointer half-retired", async () => {
	// A crash between per-row fence transactions used to retire the shared
	// authoring pointer for only part of a coalesced batch. Recovery then reads a
	// pointer that no longer covers the batch, rejects it, and re-authors events
	// whose work already ran. The fence and the whole batch's pointer retirement
	// must commit (or roll back) together.
	const fenceHome = await mkdtemp(join(tmpdir(), "gajaeway-monitor-fence-"));
	const db = await Store.open(join(fenceHome, "gateway.db"));
	const propagatorRef: { current?: MonitorPropagator } = {};
	try {
		const registry = new MonitorRegistry(db);
		const monitor = registry.add({
			name: "fence-batch",
			trigger: { kind: "cron", schedule: "30 6 * * *" },
			eventTypes: ["memory.canonicalize"],
			burstPolicy: "coalesce",
			enabled: true,
		});
		const submitted: string[] = [];
		const port = sessionPortFromScript({
			bind: async () => ({ sessionId: "s" }),
			respond: async () => JSON.stringify(submitted.map((eventId) => ({ eventId, note: "done" }))),
		});
		const propagator = new MonitorPropagator({
			database: db,
			registry,
			sessionPort: port,
			memory: new MemoryClosureQueue(db, fenceHome),
			delivery: new DeliveryService(db),
			emit: () => {},
		});
		propagatorRef.current = propagator;

		const original = db.monitors.eventFencedUpdate.bind(db.monitors);
		let fenceCalls = 0;
		db.monitors.eventFencedUpdate = (tx, eventId, leaseId, stage, batchId, nowMs) => {
			if (stage === "dispatched") {
				fenceCalls += 1;
				if (fenceCalls === 2) throw new Error("injected crash between per-row fences");
			}
			return original(tx, eventId, leaseId, stage, batchId, nowMs);
		};

		submitted.push(propagator.submit(monitor.monitorId, "memory.canonicalize", { at: 1 }));
		submitted.push(propagator.submit(monitor.monitorId, "memory.canonicalize", { at: 2 }));
		for (let attempt = 0; attempt < 200 && fenceCalls < 2; attempt++) await Bun.sleep(10);
		expect(fenceCalls).toBe(2);
		await propagator.drain();

		const pointers = submitted.map((eventId) => db.core.metaGet(`monitor_authoring_turn:${eventId}`));
		expect(pointers.every((pointer) => pointer !== undefined)).toBe(true);
		expect(new Set(pointers).size).toBe(1);
		const rows = db.monitors.eventsByIds(submitted);
		expect(rows).toHaveLength(2);
		expect(new Set(rows.map((row) => row.batch_id)).size).toBe(1);
	} finally {
		await propagatorRef.current?.drain().catch(() => {});
		db.close();
		await rm(fenceHome, { recursive: true, force: true });
	}
});
