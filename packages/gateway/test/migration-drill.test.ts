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
		expect(database.schemaVersion).toBe(15);
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
		expect(upgraded.schemaVersion).toBe(15);
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

test("upgrades live schema 12 through bootstrap schema 14 without losing current data", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-migration-v12-"));
	const path = join(directory, "gateway.db");
	try {
		const latest = await GatewayDatabase.open(path);
		latest.putLaneJob({
			jobId: "lanejob-v12",
			laneKey: "work/task/v12",
			state: "running",
			createdAt: "2026-08-28T00:00:00.000Z",
			updatedAt: "2026-08-28T00:00:00.000Z",
			lane: { branch: "fix/v12", worktreePath: "/tmp/v12" },
			json: '{"schemaVersion":1}',
		});
		latest.metaSet("rebind_budget:discord/channel/c1", '{"used":2,"lifetime":7}');
		latest.putSession("discord/channel/c1", "session-v12");
		latest.close();

		const v12 = new Database(path);
		v12.exec(`
INSERT INTO monitors (monitor_id, name, trigger_json, event_types_json, burst_policy, channel_target_json, enabled, created_at) VALUES ('monitor-v12', 'v12', '{"kind":"cron","schedule":"0 * * * *"}', '["v12.event"]', 'coalesce', NULL, 1, '2026-08-28T00:00:00.000Z');
INSERT INTO monitor_events (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at) VALUES ('event-v12', 'monitor-v12', 'v12.event', '{}', '2026-08-28T00:00:00.000Z', 'admitted', NULL, 0, '2026-08-28T00:00:00.000Z');
INSERT INTO monitor_slots (monitor_id, slot_at, created_at, event_id) VALUES ('monitor-v12', '2026-08-28T00:00:00.000Z', '2026-08-28T00:00:00.000Z', 'event-v12');
DROP TABLE conversation_context_state;
DELETE FROM schema_migrations WHERE version = 13;
`);
		v12.close();

		const upgraded = await GatewayDatabase.open(path);
		expect(upgraded.schemaVersion).toBe(15);
		expect(upgraded.laneJobJson("lanejob-v12")).toBe('{"schemaVersion":1}');
		expect(upgraded.metaGet("rebind_budget:discord/channel/c1")).toBe('{"used":2,"lifetime":7}');
		expect(upgraded.monitorSlotExists("monitor-v12", "2026-08-28T00:00:00.000Z")).toBe(true);
		expect(upgraded.getSessionBootstrap("missing")).toBeUndefined();
		expect(upgraded.getSessionRecord("discord/channel/c1")).toEqual({ sessionId: "session-v12", epoch: 0 });
		expect(upgraded.getSessionBootstrap("discord/channel/c1")).toMatchObject({
			epoch: 0,
			lastBootstrappedEpoch: -1,
			appliedAt: null,
			includedSections: [],
			byteCount: 0,
			truncated: false,
		});
		upgraded.close();

		const preserved = new Database(path, { readonly: true });
		expect(preserved.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM monitors").get()?.n).toBe(1);
		expect(preserved.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM monitor_events").get()?.n).toBe(1);
		preserved.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("upgrades a schema 14 monitors table to 15 without losing existing monitors", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-migration-v14-"));
	const path = join(directory, "gateway.db");
	try {
		const latest = await GatewayDatabase.open(path);
		latest.close();

		// Recreate the deployed schema-14 monitors table: no `instruction` column,
		// one live monitor row.
		const v14 = new Database(path);
		v14.exec(`
DROP TABLE monitors;
CREATE TABLE monitors (monitor_id TEXT PRIMARY KEY, name TEXT NOT NULL, trigger_json TEXT NOT NULL, event_types_json TEXT NOT NULL, burst_policy TEXT NOT NULL, channel_target_json TEXT, enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL);
INSERT INTO monitors (monitor_id, name, trigger_json, event_types_json, burst_policy, channel_target_json, enabled, created_at) VALUES ('monitor-v14', 'v14', '{"kind":"cron","schedule":"0 * * * *"}', '["v14.event"]', 'coalesce', NULL, 1, '2026-08-28T00:00:00.000Z');
DELETE FROM schema_migrations WHERE version > 14;
`);
		v14.close();

		const upgraded = await GatewayDatabase.open(path);
		expect(upgraded.schemaVersion).toBe(15);
		const rows = upgraded.monitorRows();
		expect(rows).toHaveLength(1);
		// The pre-existing monitor survives and reads back with no instruction.
		expect(rows[0]?.monitor_id).toBe("monitor-v14");
		expect(rows[0]?.instruction).toBeNull();
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
