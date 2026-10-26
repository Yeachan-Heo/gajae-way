import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HISTORY_RETENTION_MS, Store } from "../src/store";
import { ATTEMPT_UPDATED_SWEEP_SQL, HISTORY_SWEEP_SQL } from "../src/store/groups/journal";

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

test("aggressive attempt_updated sweep deletes superseded rows correctly", async () => {
	await withDb((_path, database, raw) => {
		// Directly insert simulated attempt_updated journal entries to test the sweep
		const opRef = "op-update-test";
		const now = new Date();
		const oldTime = new Date(now.getTime() - 8 * 24 * 60 * 60 * 1000); // 8 days ago

		raw.exec(
			`INSERT INTO work_journal (entity, entity_key, event_type, row_json, recorded_at)
			 VALUES 
			 ('attempt', '${opRef}', 'attempt_started', '{"op_ref":"${opRef}"}', '${oldTime.toISOString()}'),
			 ('attempt', '${opRef}', 'attempt_updated', '{"op_ref":"${opRef}"}', '${oldTime.toISOString()}'),
			 ('attempt', '${opRef}', 'attempt_updated', '{"op_ref":"${opRef}"}', '${oldTime.toISOString()}'),
			 ('attempt', '${opRef}', 'attempt_updated', '{"op_ref":"${opRef}"}', '${now.toISOString()}')`,
		);

		const beforeSweep = raw
			.prepare(
				"SELECT COUNT(*) as cnt FROM work_journal WHERE entity = 'attempt' AND entity_key = ? AND event_type = 'attempt_updated'",
			)
			.get(opRef) as { cnt: number };
		expect(beforeSweep.cnt).toBe(3);

		// Run the aggressive sweep
		database.sweep(now);

		const afterSweep = raw
			.prepare(
				"SELECT COUNT(*) as cnt FROM work_journal WHERE entity = 'attempt' AND entity_key = ? AND event_type = 'attempt_updated'",
			)
			.get(opRef) as { cnt: number };
		// Should have deleted the old superseded rows, keeping only the latest
		expect(afterSweep.cnt).toBe(1);

		// Verify the remaining row is the latest one
		const remaining = raw
			.prepare(
				"SELECT recorded_at FROM work_journal WHERE entity = 'attempt' AND entity_key = ? AND event_type = 'attempt_updated'",
			)
			.get(opRef) as { recorded_at: string };
		expect(remaining.recorded_at).toBe(now.toISOString());
	});
});

test("terminal attempt events are not deleted by attempt_updated sweep", async () => {
	await withDb((_path, database, raw) => {
		// Insert simulated journal entries with a terminal event
		const opRef = "op-terminal";
		const now = new Date();
		const oldTime = new Date(now.getTime() - 8 * 24 * 60 * 60 * 1000); // 8 days ago

		raw.exec(
			`INSERT INTO work_journal (entity, entity_key, event_type, row_json, recorded_at)
			 VALUES 
			 ('attempt', '${opRef}', 'attempt_started', '{"op_ref":"${opRef}"}', '${oldTime.toISOString()}'),
			 ('attempt', '${opRef}', 'attempt_updated', '{"op_ref":"${opRef}"}', '${oldTime.toISOString()}'),
			 ('attempt', '${opRef}', 'attempt_terminal', '{"op_ref":"${opRef}"}', '${oldTime.toISOString()}')`,
		);

		const beforeSweep = raw
			.prepare(
				"SELECT COUNT(*) as cnt FROM work_journal WHERE entity = 'attempt' AND entity_key = ? AND event_type = 'attempt_terminal'",
			)
			.get(opRef) as { cnt: number };
		expect(beforeSweep.cnt).toBe(1);

		// Run the sweep at 8 days (after attempt_updated cutoff but within 30-day window)
		database.sweep(now);

		const afterSweep = raw
			.prepare(
				"SELECT COUNT(*) as cnt FROM work_journal WHERE entity = 'attempt' AND entity_key = ? AND event_type = 'attempt_terminal'",
			)
			.get(opRef) as { cnt: number };
		// Terminal event should still be there (it's not deleted by the aggressive sweep)
		expect(afterSweep.cnt).toBe(1);

		const updateAfterSweep = raw
			.prepare(
				"SELECT COUNT(*) as cnt FROM work_journal WHERE entity = 'attempt' AND entity_key = ? AND event_type = 'attempt_updated'",
			)
			.get(opRef) as { cnt: number };
		// But the old update should be deleted by the standard sweep
		expect(updateAfterSweep.cnt).toBe(0);
	});
});

test("journal sweeps walk a bounded rowid range in seq order instead of sorting every candidate", async () => {
	await withDb((_path, _database, raw) => {
		// Regression: a production journal of 3M attempt_updated rows made the index-driven form of
		// this statement scan and sort every attempt row inside one write transaction; the gateway
		// stopped answering its socket and ignored SIGTERM. The bounded form stays O(batch).
		for (const sql of [ATTEMPT_UPDATED_SWEEP_SQL, HISTORY_SWEEP_SQL]) {
			const plan = raw
				.query<{ detail: string }, [number, string, number]>(`EXPLAIN QUERY PLAN ${sql}`)
				.all(1, OLD, 10)
				.map((row) => row.detail);
			expect(plan).toContain("SEARCH j USING INTEGER PRIMARY KEY (rowid<?)");
			expect(plan.some((detail) => detail.includes("TEMP B-TREE"))).toBe(false);
			expect(plan.some((detail) => /SCAN j\b/.test(detail))).toBe(false);
			expect(plan.some((detail) => detail.includes("COVERING INDEX work_journal_entity_key"))).toBe(true);
		}
	});
});

test("attempt_updated sweep never deletes rows recorded after the cutoff, even below an out-of-order bound", async () => {
	await withDb((_path, database, raw) => {
		const now = new Date();
		const old = new Date(now.getTime() - 2 * 60 * 60 * 1000).toISOString();
		const recent = now.toISOString();
		// A clock step backwards leaves a recent row at a lower seq than an old one.
		raw.exec(
			`INSERT INTO work_journal (entity, entity_key, event_type, row_json, recorded_at) VALUES
			 ('attempt', 'op-skew', 'attempt_updated', '{}', '${recent}'),
			 ('attempt', 'op-skew', 'attempt_updated', '{}', '${old}'),
			 ('attempt', 'op-skew', 'attempt_updated', '{}', '${recent}')`,
		);
		expect(database.sweep(now).deleted.work_journal).toBe(1);
		expect(
			raw
				.query<{ recorded_at: string }, []>(
					"SELECT recorded_at FROM work_journal WHERE entity_key = 'op-skew' ORDER BY seq",
				)
				.all()
				.map((row) => row.recorded_at),
		).toEqual([recent, recent]);
	});
});

test("journal sweeps are no-ops when nothing is older than the cutoff", async () => {
	await withDb((_path, database, raw) => {
		const now = new Date();
		raw.exec(
			`INSERT INTO work_journal (entity, entity_key, event_type, row_json, recorded_at) VALUES
			 ('attempt', 'op-new', 'attempt_updated', '{}', '${now.toISOString()}'),
			 ('attempt', 'op-new', 'attempt_updated', '{}', '${now.toISOString()}')`,
		);
		expect(database.sweep(now).deleted.work_journal).toBe(0);
		expect(events(raw, "attempt", "op-new")).toEqual(["attempt_updated", "attempt_updated"]);
	});
});

test("sweep respects deletion budget between journal and attemptUpdated sweeps", async () => {
	await withDb((_path, database, raw) => {
		// Create old attempts with many updates
		const now = new Date();
		// Older than HISTORY_RETENTION_MS, so both the 30-day sweep and the attempt_updated sweep
		// have at least `batch` eligible rows; only a shared budget keeps the total at one batch.
		const oldTime = new Date(now.getTime() - 31 * 24 * 60 * 60 * 1000); // 31 days ago
		const opRef = "op-budget-test";

		// Create 100 old attempt_updated entries
		raw.exec(
			`INSERT INTO work_journal (entity, entity_key, event_type, row_json, recorded_at)
			 VALUES
			 ('attempt', '${opRef}', 'attempt_started', '{}', '${oldTime.toISOString()}'),
			 ${Array.from({ length: 100 }, (_, _i) => `('attempt', '${opRef}', 'attempt_updated', '{}', '${oldTime.toISOString()}')`).join(",")}`,
		);

		const beforeCount = raw.prepare("SELECT COUNT(*) as cnt FROM work_journal WHERE entity = 'attempt'").get() as {
			cnt: number;
		};
		expect(beforeCount.cnt).toBe(101); // 1 started + 100 updates

		// Sweep with a small batch size
		const result = database.sweep(now, 10);

		// Total deleted should not exceed batch size (budget is per transaction)
		const totalDeleted = result.deleted.work_journal || 0;
		expect(totalDeleted).toBe(10);

		// Should indicate more work to do
		expect(result.more).toBe(true);

		const afterCount = raw.prepare("SELECT COUNT(*) as cnt FROM work_journal WHERE entity = 'attempt'").get() as {
			cnt: number;
		};
		expect(afterCount.cnt).toBe(beforeCount.cnt - totalDeleted);
	});
});
