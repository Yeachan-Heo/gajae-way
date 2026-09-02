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
		expect(database.schemaVersion).toBe(18);
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
		expect(upgraded.schemaVersion).toBe(18);
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
		expect(upgraded.schemaVersion).toBe(18);
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
		expect(upgraded.schemaVersion).toBe(18);
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

test("upgrades a schema 15 database to 16 and keeps a per-conversation model override usable", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-migrate-16-"));
	const path = join(directory, "gateway.db");
	const latest = await GatewayDatabase.open(path);
	latest.close();

	// Recreate a deployed schema-15 database: no conversation_model table.
	const v15 = new Database(path);
	v15.exec(`
DROP TABLE IF EXISTS conversation_model;
DELETE FROM schema_migrations WHERE version > 15;
`);
	v15.close();

	const upgraded = await GatewayDatabase.open(path);
	expect(upgraded.schemaVersion).toBe(18);
	upgraded.conversationModelSet("discord:c1", { preset: "gpt-heavy" }, "owner");
	expect(upgraded.conversationModelGet("discord:c1")?.selection).toEqual({ preset: "gpt-heavy" });
	upgraded.close();
});

test("migration 17 adds the durable batch binding and the rollback drill refuses live work before allowing a verified-zero down-marker", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-migrate-17-"));
	const path = join(directory, "gateway.db");
	const backupPath = join(directory, "gateway-before-downmark.db");
	try {
		const database = await GatewayDatabase.open(path);
		expect(database.schemaVersion).toBe(18);
		expect(database.putSessionAtEpoch("discord:dm:1", "session-v17", 0)).toBe(true);
		database.backupInto(backupPath);
		database.close();

		const raw = new Database(path);
		const columns = raw
			.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
			.all()
			.map((column) => column.name);
		for (const column of [
			"batch_key",
			"batch_role",
			"batch_epoch",
			"batch_state",
			"attributed_op_ref",
			"accepted_at",
			"bound_session_id",
		])
			expect(columns).toContain(column);
		const tableSql = raw
			.query<{ sql: string }, []>("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'inbound_messages'")
			.get()?.sql;
		expect(tableSql).toContain("state IN ('pending','processing','done')");
		raw.exec(`
INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at)
VALUES ('v17-batch', 'discord:dm:1', '{}', 'body', NULL, 'pending', '2026-09-01T00:00:00.000Z');
UPDATE inbound_messages SET batch_key = 'batch', batch_role = 'trigger', batch_epoch = 0, batch_state = 'accepted', attributed_op_ref = 'gw-p-0123456789abcdef0123456789abcdef' WHERE message_id = 'v17-batch';
`);
		const live = raw
			.query<{ n: number }, []>(
				"SELECT COUNT(*) AS n FROM inbound_messages WHERE batch_role = 'trigger' AND batch_state IN ('settled', 'accepted')",
			)
			.get()?.n;
		expect(live).toBe(1);
		expect(() => {
			if (live !== 0) throw new Error("refusing schema down-marker while nonterminal batches remain");
			raw.exec("DELETE FROM schema_migrations WHERE version >= 17");
		}).toThrow("refusing schema down-marker");
		expect(
			raw.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM schema_migrations WHERE version = 17").get()?.n,
		).toBe(1);

		raw.exec("UPDATE inbound_messages SET state = 'done', batch_state = 'done' WHERE message_id = 'v17-batch'");
		const drained = raw
			.query<{ n: number }, []>(
				"SELECT COUNT(*) AS n FROM inbound_messages WHERE batch_role = 'trigger' AND batch_state IN ('settled', 'accepted')",
			)
			.get()?.n;
		expect(drained).toBe(0);
		raw.exec("DELETE FROM schema_migrations WHERE version >= 17");
		raw.close();

		const v16Guard = new Database(path, { readonly: true });
		expect(
			v16Guard.query<{ version: number }, []>("SELECT MAX(version) AS version FROM schema_migrations").get()?.version,
		).toBe(16);
		v16Guard.close();
		const backup = new Database(backupPath, { readonly: true });
		expect(
			backup.query<{ version: number }, []>("SELECT MAX(version) AS version FROM schema_migrations").get()?.version,
		).toBe(18);
		backup.close();

		const reopened = await GatewayDatabase.open(path);
		expect(reopened.schemaVersion).toBe(18);
		expect(reopened.inboundBatchRows("batch")[0]).toMatchObject({ state: "done", batch_state: "done" });
		reopened.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("migration 18 adds the pre-send dispatch stamp and the terminal reply slot without touching existing rows", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-migrate-18-"));
	const path = join(directory, "gateway.db");
	try {
		const latest = await GatewayDatabase.open(path);
		latest.close();
		// Recreate a deployed schema-17 database: batch columns present, no dispatch/terminal columns.
		const v17 = new Database(path);
		v17.exec(`
DELETE FROM schema_migrations WHERE version > 17;
INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at)
VALUES ('v18-live', 'discord:dm:1', '{}', 'body', NULL, 'pending', '2026-09-02T00:00:00.000Z');
UPDATE inbound_messages SET batch_key = 'b18', batch_role = 'trigger', batch_epoch = 0, batch_state = 'accepted', attributed_op_ref = 'gw-p-0123456789abcdef0123456789abcdef', bound_session_id = 's-old' WHERE message_id = 'v18-live';
`);
		v17.close();
		const upgraded = await GatewayDatabase.open(path);
		expect(upgraded.schemaVersion).toBe(18);
		const row = upgraded.inboundBatchRows("b18")[0];
		expect(row).toMatchObject({ bound_session_id: "s-old", dispatched_at: null, terminal_delivery_id: null });
		// A pre-migration batch that was ALREADY bound never gets a recovery-time
		// floor: that stamp could postdate the answer the old daemon wrote.
		expect(upgraded.inboundBatchBindSession("b18", "s-old", "2026-09-02T00:00:05.000Z")).toBe(true);
		expect(upgraded.inboundBatchDispatchedAt("b18")).toBeUndefined();
		// A never-bound batch is stamped on its first bind only; a re-bind keeps it.
		const v18 = new Database(path);
		v18.exec(`
INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at)
VALUES ('v18-fresh', 'discord:dm:2', '{}', 'body', NULL, 'pending', '2026-09-02T00:00:00.000Z');
UPDATE inbound_messages SET batch_key = 'b18f', batch_role = 'trigger', batch_epoch = 0, batch_state = 'settled', attributed_op_ref = 'gw-p-0123456789abcdef0123456789abcdee' WHERE message_id = 'v18-fresh';
`);
		v18.close();
		expect(upgraded.inboundBatchBindSession("b18f", "s-new", "2026-09-02T00:00:05.000Z")).toBe(true);
		expect(upgraded.inboundBatchDispatchedAt("b18f")).toBe("2026-09-02T00:00:05.000Z");
		expect(upgraded.inboundBatchBindSession("b18f", "s-new", "2026-09-02T00:01:00.000Z")).toBe(true);
		expect(upgraded.inboundBatchDispatchedAt("b18f")).toBe("2026-09-02T00:00:05.000Z");
		// A fresh-turn requeue releases the floor and the terminal claims with the rows.
		expect(upgraded.inboundBatchClaimTerminal("b18f", 0, "gw-t-x")).toBe("gw-t-x");
		expect(upgraded.inboundBatchRequeueFreshTurn("b18f")).toBe(1);
		expect(upgraded.inboundBatchDispatchedAt("b18f")).toBeUndefined();
		const released = upgraded.inboundBatchRows("b18f");
		expect(released).toEqual([]);
		// Terminal slots are per part and first-claim wins.
		expect(upgraded.inboundBatchClaimTerminal("b18", 0, "gw-t-a")).toBe("gw-t-a");
		expect(upgraded.inboundBatchClaimTerminal("b18", 0, "gw-t-b")).toBe("gw-t-a");
		expect(upgraded.inboundBatchClaimTerminal("b18", 1, "gw-t-c")).toBe("gw-t-c");
		upgraded.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
