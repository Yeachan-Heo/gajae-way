import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DeliveryService } from "../src/delivery/delivery";
import { MemoryClosureQueue } from "../src/memory/closure";
import { MonitorPropagator } from "../src/monitors/propagate";
import { MonitorRegistry } from "../src/monitors/registry";
import type { GjcPort } from "../src/orchestrator/gjc-client";
import { GatewayDatabase } from "../src/store/db";
import { DeliveryLedger } from "../src/store/ledger";

let home = "";
let database: GatewayDatabase | undefined;
afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function harness(sendTurn: GjcPort["sendTurn"]) {
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
		gjc: { ensureSession: async () => ({ sessionId: "s1" }), sendTurn, forgetRebinds: () => {} },
		memory: new MemoryClosureQueue(database, home),
		delivery: new DeliveryService(new DeliveryLedger(database)),
		emit: () => {},
	});
	return { propagator, monitor, database };
}

const stage = (db: GatewayDatabase, id: string) => db.monitorEventRows().find((row) => row.event_id === id)?.stage;

test("an event stranded at batched by a dead process is recovered by reconcile", async () => {
	const {
		propagator,
		monitor,
		database: db,
	} = await harness(async () => JSON.stringify([{ eventId: "PLACEHOLDER", note: "done" }]));

	// Simulate the crash: the row is left at `batched` with no authored output, exactly the
	// state a gateway restart mid-dispatch leaves behind.
	const eventId = crypto.randomUUID();
	db.monitorEventCreate({
		eventId,
		monitorId: monitor.monitorId,
		eventType: "memory.canonicalize",
		payloadJson: JSON.stringify({ at: new Date().toISOString() }),
		firedAt: new Date().toISOString(),
	});
	db.monitorEventUpdate(eventId, "batched", crypto.randomUUID());
	expect(stage(db, eventId)).toBe("batched");
	expect(db.authoredOutput(eventId)).toBeUndefined();

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
