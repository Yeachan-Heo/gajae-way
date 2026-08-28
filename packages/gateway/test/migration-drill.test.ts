import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase } from "../src/store/db";

test("migrates a migration-001 database to the latest schema", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-migration-drill-"));
	const path = join(directory, "gateway.db");
	try {
		const legacy = new Database(path);
		legacy.exec(
			"CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); CREATE TABLE sessions (origin_key TEXT PRIMARY KEY, gjc_session_id TEXT NOT NULL, created_at TEXT NOT NULL); INSERT INTO schema_migrations VALUES (1, '2026-01-01T00:00:00.000Z')",
		);
		legacy.close();

		const database = await GatewayDatabase.open(path);
		expect(database.schemaVersion).toBe(12);
		database.close();

		const migrated = new Database(path, { readonly: true });
		const tables = migrated
			.query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'table'")
			.all()
			.map((row) => row.name);
		for (const table of ["deliveries", "recall_snippets", "meta", "monitors", "monitor_events", "authored_outputs"])
			expect(tables).toContain(table);
		for (const table of ["lane_jobs", "monitor_failures", "monitor_slots", "dispatch_leases"])
			expect(tables).toContain(table);
		expect(
			migrated.query<{ value: string }, []>("SELECT value FROM meta WHERE key = 'instance_id'").get()?.value,
		).toBeString();
		migrated.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("upgrades a deployed lane-jobs schema 10 database to combined schema 12 without losing jobs", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-migration-v10-"));
	const path = join(directory, "gateway.db");
	try {
		const latest = await GatewayDatabase.open(path);
		latest.putLaneJob({
			jobId: "lanejob-test",
			laneKey: "work/task/test",
			state: "running",
			createdAt: "2026-08-28T00:00:00.000Z",
			updatedAt: "2026-08-28T00:00:00.000Z",
			lane: { branch: "feat/test", worktreePath: "/tmp/test" },
			json: '{"schemaVersion":1}',
		});
		latest.close();

		// Recreate the exact pre-monitor-recovery shape: schema 10 already has
		// lane_jobs, while monitor_events still uses the legacy stage contract.
		const v10 = new Database(path);
		v10.exec(`
DROP TABLE dispatch_leases;
DROP TABLE monitor_failures;
DROP TABLE monitor_slots;
CREATE TABLE monitor_events_v10 (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','failed')), batch_id TEXT, updated_at TEXT NOT NULL);
INSERT INTO monitor_events_v10 (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, updated_at) SELECT event_id, monitor_id, event_type, payload_json, fired_at, CASE WHEN stage IN ('authored_no_delivery','failed_no_retry') THEN 'failed' ELSE stage END, batch_id, updated_at FROM monitor_events;
DROP TABLE monitor_events;
ALTER TABLE monitor_events_v10 RENAME TO monitor_events;
DELETE FROM schema_migrations WHERE version > 10;
`);
		v10.close();

		const upgraded = await GatewayDatabase.open(path);
		expect(upgraded.schemaVersion).toBe(12);
		expect(upgraded.laneJobJson("lanejob-test")).toBe('{"schemaVersion":1}');
		const tables = new Set(
			new Database(path, { readonly: true })
				.query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'table'")
				.all()
				.map((row) => row.name),
		);
		for (const table of ["lane_jobs", "monitor_failures", "monitor_slots", "dispatch_leases"])
			expect(tables.has(table)).toBe(true);
		upgraded.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("online backup copy retains a session row", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-backup-drill-"));
	const path = join(directory, "gateway.db");
	const backupPath = join(directory, "gateway-backup.db");
	try {
		const database = await GatewayDatabase.open(path);
		database.withTransaction(() => database.putSession("loopback/loopback/loopback", "session-1"));
		database.backupInto(backupPath);
		database.close();

		const backup = new Database(backupPath, { readonly: true });
		expect(
			backup
				.query<{ gjc_session_id: string }, []>(
					"SELECT gjc_session_id FROM sessions WHERE origin_key = 'loopback/loopback/loopback'",
				)
				.get(),
		).toEqual({ gjc_session_id: "session-1" });
		backup.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
