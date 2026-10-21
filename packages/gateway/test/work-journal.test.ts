import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HISTORY_RETENTION_MS, Store } from "../src/store";

const OLD = "2020-01-01T00:00:00.000Z";

async function withDb(run: (path: string, database: Store, raw: Database) => void | Promise<void>) {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-journal-"));
	const path = join(directory, "gateway.db");
	const database = await Store.open(path);
	const raw = new Database(path);
	try {
		await run(path, database, raw);
	} finally {
		raw.close();
		database.close();
		await rm(directory, { recursive: true, force: true });
	}
}

function projections(database: Store) {
	return { ...database.work.projectionRows(), deliveries: database.outbox.projectionRows() };
}

function events(raw: Database, entity: string, key: string): string[] {
	return raw
		.query<{ event_type: string }, [string, string]>(
			"SELECT event_type FROM work_journal WHERE entity = ? AND entity_key = ? ORDER BY seq",
		)
		.all(entity, key)
		.map((row) => row.event_type);
}

function wipeAndReplay(database: Store, raw: Database) {
	const live = projections(database);
	raw.exec("DELETE FROM work_attempt_runtime; DELETE FROM lane_jobs; DELETE FROM deliveries;");
	expect(database.replayJournal()).toEqual({
		attempts: live.attempts.length,
		laneJobs: live.laneJobs.length,
		deliveries: live.deliveries.length,
	});
	expect(projections(database)).toEqual(live);
	return live;
}

function enqueue(database: Store, deliveryId: string) {
	database.transaction((tx) =>
		database.outbox.enqueue(tx, {
			deliveryId,
			turnId: `turn-${deliveryId}`,
			originKey: "loopback/dm/journal",
			payloadJson: JSON.stringify({ text: deliveryId }),
		}),
	);
}

test("every outbox transition is journaled and replay restores state, including deleted rows", async () => {
	await withDb((_path, database, raw) => {
		for (const id of ["d1", "d2", "d3", "d4"]) enqueue(database, id);
		database.transaction((tx) => {
			database.outbox.markInflight(tx, "d1");
			database.outbox.confirm(tx, "d1");
			database.outbox.fail(tx, "d2", true, "socket hang up");
			database.outbox.fail(tx, "d3", false, "rate limited 429");
		});
		database.transaction((tx) => database.outbox.requeue(tx, "d2"));
		database.transaction((tx) => database.outbox.expireStale(tx, 1, Date.now() + 1_000));
		database.transaction((tx) => database.outbox.requeueSince(tx, OLD));
		database.transaction((tx) => database.outbox.prune(tx, 0, Date.now() + 1_000));

		expect(events(raw, "delivery", "d1")).toEqual([
			"delivery_queued",
			"delivery_inflight",
			"delivery_confirmed",
			"delivery_deleted",
		]);
		expect(events(raw, "delivery", "d2")).toEqual([
			"delivery_queued",
			"delivery_failed",
			"delivery_requeued",
			"delivery_expired",
			"delivery_requeued",
		]);
		expect(events(raw, "delivery", "d3")).toEqual([
			"delivery_queued",
			"delivery_failed",
			"delivery_expired",
			"delivery_requeued",
		]);
		expect(database.journal.latest("delivery").get("d1")).toBeNull();
		expect(database.journal.latest("delivery").get("d3")).toMatchObject({
			state: "pending",
			last_error: "rate_limited",
		});

		const live = wipeAndReplay(database, raw);
		expect(live.deliveries.map((row) => row.delivery_id)).toEqual(["d2", "d3", "d4"]);
		// A row the journal never recorded, or recorded as deleted, does not survive replay.
		raw.exec(
			"INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES ('d1', 't', 'o', '{}', 'pending', 0, 'x', 'x'), ('ghost', 't', 'o', '{}', 'pending', 0, 'x', 'x')",
		);
		raw.exec("UPDATE deliveries SET attempts = 9 WHERE delivery_id = 'd4'");
		expect(database.replayJournal()).toEqual({ attempts: 0, laneJobs: 0, deliveries: 3 });
		expect(projections(database)).toEqual(live);
	});
});

test("retention sweep journals deliveries it deletes", async () => {
	await withDb((_path, database, raw) => {
		enqueue(database, "old");
		database.transaction((tx) => database.outbox.confirm(tx, "old"));
		raw.exec("UPDATE deliveries SET updated_at = '2020-01-01T00:00:00.000Z' WHERE delivery_id = 'old'");
		expect(database.sweep(new Date()).deleted.deliveries).toBe(1);
		expect(events(raw, "delivery", "old").at(-1)).toBe("delivery_deleted");
		wipeAndReplay(database, raw);
	});
});

test("v36 seeds pre-journal projection rows as baseline events that replay alone", async () => {
	await withDb(async (path, database, raw) => {
		const record = JSON.stringify({ jobId: "lanejob-6a", state: "running" });
		raw.exec(`
INSERT INTO lane_jobs (job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at)
	VALUES ('lanejob-6a', 'work-j', 'gw/j', '/tmp/j', 'awaiting_checks', '${record}', '${OLD}', '2020-01-02T00:00:00.000Z');
INSERT INTO work_attempt_runtime (op_ref, job_id, lane_key, session_id, version, settled_at, delivery_id, record_json)
	VALUES ('gw-j-abababababab', 'lanejob-6a', 'work-j', 'session', 3, NULL, 'delivery-j', '{"opRef":"gw-j-abababababab"}');
INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at, last_error)
	VALUES ('delivery-j', 'turn', 'loopback/dm/j', '{"text":"j"}', 'failed_ambiguous', 2, '${OLD}', '${OLD}', 'network');
DROP TABLE work_journal;
DELETE FROM schema_migrations WHERE version >= 36;
`);
		database.close();
		const migrated = await Store.open(path);
		try {
			expect(migrated.schemaVersion).toBe(38);
			expect(events(raw, "lane_job", "lanejob-6a")).toEqual(["baseline"]);
			expect(events(raw, "attempt", "gw-j-abababababab")).toEqual(["baseline"]);
			expect(events(raw, "delivery", "delivery-j")).toEqual(["baseline"]);
			const live = wipeAndReplay(migrated, raw);
			expect(live.laneJobs[0]).toMatchObject({ state: "awaiting_checks", record_json: record });
			expect(live.attempts[0]).toMatchObject({ version: 3, settled_at: null });
			expect(live.deliveries[0]).toMatchObject({ attempts: 2, last_error: "network" });
		} finally {
			migrated.close();
		}
	});
});

test("journal compaction keeps only what replay needs", async () => {
	await withDb((_path, database, raw) => {
		for (const id of ["keep", "gone"]) enqueue(database, id);
		database.transaction((tx) => {
			database.outbox.markInflight(tx, "keep");
			database.outbox.fail(tx, "keep", true, "socket hang up");
			database.outbox.confirm(tx, "gone");
		});
		database.transaction((tx) => database.outbox.prune(tx, 0, Date.now() + 1_000));
		enqueue(database, "gone-again-later");
		raw.exec(`UPDATE work_journal SET recorded_at = '${OLD}' WHERE entity_key IN ('keep', 'gone')`);
		const now = new Date(Date.parse(OLD) + HISTORY_RETENTION_MS + 86_400_000 * 365 * 10);

		expect(database.sweep(now).deleted.work_journal).toBe(4);
		expect(events(raw, "delivery", "keep")).toEqual(["delivery_failed"]);
		// The tombstone outlives the snapshots it supersedes, so replay never resurrects the row.
		expect(events(raw, "delivery", "gone")).toEqual(["delivery_deleted"]);
		expect(database.sweep(now).deleted.work_journal).toBe(1);
		expect(events(raw, "delivery", "gone")).toEqual([]);
		expect(events(raw, "delivery", "gone-again-later")).toEqual(["delivery_queued"]);
		wipeAndReplay(database, raw);
	});
});
