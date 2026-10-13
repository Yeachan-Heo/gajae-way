import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { LOOPBACK_ORIGIN, originKey } from "@gajae-gateway/protocol";
import { MonitorRegistry } from "../src/monitors/registry";
import { HISTORY_RETENTION_MS, Store } from "../src/store";

const NOW = new Date("2026-10-03T00:00:00.000Z");
const OLD = new Date(NOW.getTime() - HISTORY_RETENTION_MS - 86_400_000).toISOString();
const RECENT = new Date(NOW.getTime() - 86_400_000).toISOString();

async function withDb(run: (database: Store, raw: Database) => void | Promise<void>) {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-retention-"));
	const path = join(directory, "gateway.db");
	const database = await Store.open(path);
	const raw = new Database(path);
	try {
		await run(database, raw);
	} finally {
		raw.close();
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
}

function count(raw: Database, table: string): number {
	return raw.query<{ n: number }, []>(`SELECT COUNT(*) AS n FROM ${table}`).get()?.n ?? 0;
}

test("retention deletes old terminal inbound rows and keeps pending, recent and quarantined ones", async () => {
	await withDb((database, raw) => {
		const key = originKey(LOOPBACK_ORIGIN);
		for (const id of ["old-done", "old-pending", "recent-done", "old-quarantined"]) {
			database.transaction((tx) =>
				database.inbox.enqueue(tx, {
					messageId: id,
					originKey: key,
					originRefJson: JSON.stringify(LOOPBACK_ORIGIN),
					body: id,
					receivedAt: id.startsWith("recent") ? RECENT : OLD,
				}),
			);
		}
		raw.exec(
			"UPDATE inbound_messages SET state = 'done' WHERE message_id IN ('old-done','recent-done','old-quarantined')",
		);
		raw.exec("INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES ('inbound','old-quarantined','c1')");

		const result = database.sweep(NOW);

		expect(result.deleted.inbound_messages).toBe(1);
		expect(
			raw
				.query<{ message_id: string }, []>("SELECT message_id FROM inbound_messages ORDER BY message_id")
				.all()
				.map((r) => r.message_id),
		).toEqual(["old-pending", "old-quarantined", "recent-done"]);
	});
});

test("retention drops settled monitor events with their outputs and failures, keeps open ones", async () => {
	await withDb((database, raw) => {
		database.transaction((tx) => {
			for (const id of ["ev-old-delivered", "ev-old-open", "ev-recent-delivered"]) {
				database.monitors.eventCreate(tx, {
					eventId: id,
					monitorId: "m1",
					eventType: "t",
					payloadJson: "{}",
					firedAt: OLD,
				});
				database.monitors.authoredOutputCreate(tx, id, "x");
				database.monitors.failureRecord(tx, id, "c", "d");
			}
		});
		raw
			.query("UPDATE monitor_events SET stage = 'delivered', updated_at = ? WHERE event_id = 'ev-old-delivered'")
			.run(OLD);
		raw
			.query("UPDATE monitor_events SET stage = 'delivered', updated_at = ? WHERE event_id = 'ev-recent-delivered'")
			.run(RECENT);
		raw.query("UPDATE monitor_events SET updated_at = ? WHERE event_id = 'ev-old-open'").run(OLD);

		const result = database.sweep(NOW);

		expect(result.deleted.monitor_events).toBe(1);
		expect(
			raw
				.query<{ event_id: string }, []>("SELECT event_id FROM monitor_events ORDER BY event_id")
				.all()
				.map((r) => r.event_id),
		).toEqual(["ev-old-open", "ev-recent-delivered"]);
		expect(count(raw, "authored_outputs")).toBe(2);
		expect(count(raw, "monitor_failures")).toBe(2);
	});
});

test("retention keeps each monitor's newest slot even when it is past the window", async () => {
	await withDb((database, raw) => {
		const older = new Date(NOW.getTime() - HISTORY_RETENTION_MS - 2 * 86_400_000).toISOString();
		for (const slot of [older, OLD]) {
			raw
				.query("INSERT INTO monitor_slots(monitor_id, slot_at, event_id, created_at) VALUES ('m1', ?, NULL, ?)")
				.run(slot, slot);
		}
		database.sweep(NOW);
		expect(new MonitorRegistry(database).cronCursor("m1")).toBe(OLD);
		expect(count(raw, "monitor_slots")).toBe(1);
	});
});

test("retention prunes confirmed deliveries after a week but never unsettled ones", async () => {
	await withDb((database, raw) => {
		const insert = raw.query(
			"INSERT INTO deliveries(delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, 't', 'o', '{}', ?, 0, ?, ?)",
		);
		const eightDays = new Date(NOW.getTime() - 8 * 86_400_000).toISOString();
		insert.run("conf-old", "confirmed", eightDays, eightDays);
		insert.run("conf-new", "confirmed", RECENT, RECENT);
		insert.run("pend-old", "pending", OLD, OLD);
		insert.run("exp-old", "expired", OLD, OLD);

		database.sweep(NOW);

		expect(
			raw
				.query<{ delivery_id: string }, []>("SELECT delivery_id FROM deliveries ORDER BY delivery_id")
				.all()
				.map((r) => r.delivery_id),
		).toEqual(["conf-new", "pend-old"]);
	});
});

test("retention is bounded per batch and reports when more remains", async () => {
	await withDb((database, raw) => {
		const insert = raw.query(
			"INSERT INTO deliveries(delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, 't', 'o', '{}', 'expired', 0, ?, ?)",
		);
		for (let i = 0; i < 7; i++) insert.run(`d${i}`, OLD, OLD);

		const first = database.sweep(NOW, 3);
		expect(first.deleted.deliveries).toBe(3);
		expect(first.more).toBe(true);
		expect(count(raw, "deliveries")).toBe(4);

		database.sweep(NOW, 3);
		const last = database.sweep(NOW, 3);
		expect(last.more).toBe(false);
		expect(count(raw, "deliveries")).toBe(0);
		database.core.maintain();
	});
});

function seedDelivery(raw: Database, id: string, turn: string, state: string, at = RECENT) {
	raw
		.query(
			"INSERT INTO deliveries(delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, 'o', '{}', ?, 0, ?, ?)",
		)
		.run(id, turn, state, at, at);
}

function seedEvent(database: Store, id: string, stage: string, batch: string | null, firedAt: string) {
	database.transaction((tx) => {
		database.monitors.eventCreate(tx, { eventId: id, monitorId: "m1", eventType: "t", payloadJson: "{}", firedAt });
		database.monitors.eventUpdate(tx, id, stage as never, batch);
	});
}

test("targeted ledger reads agree with the unsettled/terminal split", async () => {
	await withDb((database, raw) => {
		seedDelivery(raw, "p", "t1", "pending", "2026-10-01T00:00:00.000Z");
		seedDelivery(raw, "i", "t2", "inflight", "2026-10-02T00:00:00.000Z");
		seedDelivery(raw, "c", "t3", "confirmed");
		seedDelivery(raw, "e1", "t4", "expired", "2026-09-01T00:00:00.000Z");
		seedDelivery(raw, "e2", "t5", "expired", "2026-09-02T00:00:00.000Z");

		expect(database.outbox.unsettledRows().map((row) => row.deliveryId)).toEqual(["p", "i"]);
		expect(database.outbox.get("c")?.state).toBe("confirmed");
		expect(database.outbox.get("missing")).toBeUndefined();
		expect(
			database.outbox
				.getMany(["c", "c", "p", "missing"])
				.map((row) => row.deliveryId)
				.sort(),
		).toEqual(["c", "p"]);
		const counts = database.outbox.counts();
		expect(counts).toMatchObject({ pending: 2, expired: 2 });
		expect(counts.recentExpired.map((row) => row.deliveryId)).toEqual(["e2", "e1"]);
	});
});

test("reconcile reads select exactly the stranded and open monitor events", async () => {
	await withDb((database, raw) => {
		seedDelivery(raw, "d-confirmed", "b-confirmed", "confirmed");
		seedDelivery(raw, "d-expired", "b-expired", "expired");
		seedDelivery(raw, "d-pending", "b-pending", "pending");
		seedDelivery(raw, "d-clean", "b-clean", "confirmed");
		seedEvent(database, "ev-1", "authored", "b-confirmed", "2026-10-01T00:00:01.000Z");
		seedEvent(database, "ev-2", "authored", "b-expired", "2026-10-01T00:00:02.000Z");
		seedEvent(database, "ev-3", "authored", "b-pending", "2026-10-01T00:00:03.000Z");
		seedEvent(database, "ev-4", "delivered", "b-clean", "2026-10-01T00:00:04.000Z");
		seedEvent(database, "ev-5", "admitted", null, "2026-10-01T00:00:00.500Z");

		expect(
			database.outbox
				.terminalForTurns(database.monitors.authoredBatchIds())
				.map((row) => row.delivery_id)
				.sort(),
		).toEqual(["d-confirmed", "d-expired"]);
		// Oldest first; terminal events never appear.
		expect(database.monitors.eventsOpen().map((r) => r.event_id)).toEqual(["ev-5", "ev-1", "ev-2", "ev-3"]);
		expect(database.monitors.eventsByBatch("b-confirmed").map((r) => r.event_id)).toEqual(["ev-1"]);
		// Newest first, matching monitorEventRows() order.
		expect(database.monitors.eventsByIds(["ev-1", "ev-4", "nope"]).map((r) => r.event_id)).toEqual(["ev-4", "ev-1"]);
		expect(database.monitors.eventGet("ev-2")?.stage).toBe("authored");
		expect(database.monitors.eventRows("m1", "newest", true, 2).map((r) => r.event_id)).toEqual(["ev-4", "ev-3"]);
	});
});

test("quarantined events stay invisible to replay reads but visible with includeQuarantined", async () => {
	await withDb((database, raw) => {
		seedEvent(database, "ev-q", "admitted", "b-q", "2026-10-01T00:00:00.000Z");
		raw.exec("INSERT INTO broker_quarantine(kind, subject_id, cutover_id) VALUES ('monitor','ev-q','c1')");
		expect(database.monitors.eventGet("ev-q")).toBeUndefined();
		expect(database.monitors.eventGet("ev-q", true)?.event_id).toBe("ev-q");
		expect(database.monitors.eventsOpen()).toEqual([]);
		expect(database.monitors.eventsByBatch("b-q")).toEqual([]);
		expect(database.monitors.eventsByIds(["ev-q"])).toEqual([]);
	});
});

test("memory intent probes cover deterministic ids, legacy payloads and the open set", async () => {
	await withDb((database) => {
		database.transaction((tx) => {
			database.memory.createIntent(tx, { id: "monitor-event-intent:ev-a", kind: "monitor-event", payloadJson: "{}" });
			database.memory.createIntent(tx, {
				id: "legacy-1",
				kind: "monitor-event",
				payloadJson: '{"identity":"monitor-event:ev-b"}',
			});
			database.memory.createIntent(tx, { id: "other", kind: "daily_capture", payloadJson: '{"x":"ev-c"}' });
		});
		expect(database.memory.coversEvent("ev-a")).toBe(true);
		expect(database.memory.coversEvent("ev-b")).toBe(true);
		expect(database.memory.coversEvent("ev-c")).toBe(false);
		expect(database.memory.get("legacy-1")?.state).toBe("queued");
		expect(database.memory.get("nope")).toBeUndefined();
		database.transaction((tx) => database.memory.beginAttempt(tx, "other"));
		expect(
			database.memory
				.openRows()
				.map((r) => r.id)
				.sort(),
		).toEqual(["legacy-1", "monitor-event-intent:ev-a", "other"]);
	});
});

test("authored-note digest returns newest notes that actually have output", async () => {
	await withDb((database, _raw) => {
		for (const [id, at] of [
			["n1", "2026-10-01T00:00:01.000Z"],
			["n2", "2026-10-01T00:00:02.000Z"],
			["n3", "2026-10-01T00:00:03.000Z"],
		] as const)
			seedEvent(database, id, "delivered", null, at);
		database.transaction((tx) => {
			database.monitors.authoredOutputCreate(tx, "n1", "first");
			database.monitors.authoredOutputCreate(tx, "n3", "third");
		});
		expect(database.monitors.recentAuthoredNotes("m1", 5).map((r) => r.note)).toEqual(["third", "first"]);
		expect(database.monitors.recentAuthoredNotes("m1", 1).map((r) => r.note)).toEqual(["third"]);
	});
});
