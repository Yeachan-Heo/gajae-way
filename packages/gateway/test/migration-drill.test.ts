import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GatewayDatabase } from "../src/store/db";

/** Remove v22/v23 completely before replaying historical DDL; missing objects are fixture errors. */
function dropBrokerAuthoritySchema(database: Database): void {
	for (const table of ["inbound_messages", "lane_jobs", "work_attempt_runtime", "monitor_events", "authored_outputs"])
		for (const action of ["update", "delete"]) database.exec(`DROP TRIGGER ${table}_quarantine_${action}`);
	for (const table of ["broker_owned_bindings", "broker_cutovers", "broker_quarantine", "broker_retired_sessions"])
		for (const action of ["update", "delete"]) database.exec(`DROP TRIGGER ${table}_immutable_${action}`);
	database.exec(
		"ALTER TABLE memory_intents DROP COLUMN quarantine_reason; ALTER TABLE memory_intents DROP COLUMN attempts",
	);
	for (const table of [
		"broker_authority",
		"broker_owned_bindings",
		"broker_tail_cursors",
		"broker_cutovers",
		"broker_quarantine",
		"broker_retired_sessions",
	])
		database.exec(`DROP TABLE ${table}`);
}

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
		expect(database.schemaVersion).toBe(23);
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
		dropBrokerAuthoritySchema(v10);
		v10.exec(`
DROP TABLE work_attempt_runtime;
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
		expect(upgraded.schemaVersion).toBe(23);
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
		dropBrokerAuthoritySchema(v12);
		v12.exec(`
INSERT INTO monitors (monitor_id, name, trigger_json, event_types_json, burst_policy, channel_target_json, enabled, created_at) VALUES ('monitor-v12', 'v12', '{"kind":"cron","schedule":"0 * * * *"}', '["v12.event"]', 'coalesce', NULL, 1, '2026-08-28T00:00:00.000Z');
INSERT INTO monitor_events (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at) VALUES ('event-v12', 'monitor-v12', 'v12.event', '{}', '2026-08-28T00:00:00.000Z', 'admitted', NULL, 0, '2026-08-28T00:00:00.000Z');
INSERT INTO monitor_slots (monitor_id, slot_at, created_at, event_id) VALUES ('monitor-v12', '2026-08-28T00:00:00.000Z', '2026-08-28T00:00:00.000Z', 'event-v12');
DROP TABLE conversation_context_state;
DROP TABLE work_attempt_runtime;
DELETE FROM schema_migrations WHERE version > 12;
`);
		v12.close();

		const upgraded = await GatewayDatabase.open(path);
		expect(upgraded.schemaVersion).toBe(23);
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
		dropBrokerAuthoritySchema(v14);
		v14.exec(`
DROP TABLE work_attempt_runtime;
DROP TABLE monitors;
CREATE TABLE monitors (monitor_id TEXT PRIMARY KEY, name TEXT NOT NULL, trigger_json TEXT NOT NULL, event_types_json TEXT NOT NULL, burst_policy TEXT NOT NULL, channel_target_json TEXT, enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL);
INSERT INTO monitors (monitor_id, name, trigger_json, event_types_json, burst_policy, channel_target_json, enabled, created_at) VALUES ('monitor-v14', 'v14', '{"kind":"cron","schedule":"0 * * * *"}', '["v14.event"]', 'coalesce', NULL, 1, '2026-08-28T00:00:00.000Z');
DELETE FROM schema_migrations WHERE version > 14;
`);
		v14.close();

		const upgraded = await GatewayDatabase.open(path);
		expect(upgraded.schemaVersion).toBe(23);
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
	dropBrokerAuthoritySchema(v15);
	v15.exec(`
DROP TABLE work_attempt_runtime;
DROP TABLE IF EXISTS conversation_model;
DELETE FROM schema_migrations WHERE version > 15;
`);
	v15.close();

	const upgraded = await GatewayDatabase.open(path);
	expect(upgraded.schemaVersion).toBe(23);
	upgraded.conversationModelSet("discord:c1", { preset: "gpt-heavy" }, "owner");
	expect(upgraded.conversationModelGet("discord:c1")?.selection).toEqual({ preset: "gpt-heavy" });
	upgraded.close();
});

test("migration 19 rebuilds a genuine schema-18 batch table as turns: bound/accepted triggers survive with their floor and terminal claims, unsent members return to pending, steers stay attributed, nothing is deleted", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gw-migrate-19-"));
	const path = join(directory, "gateway.db");
	try {
		const latest = await GatewayDatabase.open(path);
		expect(latest.schemaVersion).toBe(23);
		latest.close();
		// Rebuild a deployed schema-18 database from its real DDL (v16 base + the
		// v17 ALTERs + the v18 ALTERs), then seed the shapes an upgrade meets.
		const raw = new Database(path);
		dropBrokerAuthoritySchema(raw);
		raw.exec(`
DROP TABLE work_attempt_runtime;
DROP TABLE inbound_messages;
CREATE TABLE inbound_messages (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL);
CREATE INDEX inbound_messages_claim ON inbound_messages (origin_key, state, received_at);
ALTER TABLE inbound_messages ADD COLUMN batch_key TEXT;
ALTER TABLE inbound_messages ADD COLUMN batch_role TEXT CHECK(batch_role IS NULL OR batch_role IN ('trigger', 'member', 'steer'));
ALTER TABLE inbound_messages ADD COLUMN batch_epoch INTEGER;
ALTER TABLE inbound_messages ADD COLUMN batch_state TEXT CHECK(batch_state IS NULL OR batch_state IN ('settled', 'accepted', 'done'));
ALTER TABLE inbound_messages ADD COLUMN attributed_op_ref TEXT;
ALTER TABLE inbound_messages ADD COLUMN accepted_at TEXT;
ALTER TABLE inbound_messages ADD COLUMN bound_session_id TEXT;
CREATE UNIQUE INDEX inbound_messages_nonterminal_trigger ON inbound_messages (origin_key, batch_epoch) WHERE batch_role = 'trigger' AND batch_state IN ('settled', 'accepted');
ALTER TABLE inbound_messages ADD COLUMN dispatched_at TEXT;
ALTER TABLE inbound_messages ADD COLUMN terminal_delivery_id TEXT;
DELETE FROM schema_migrations WHERE version > 18;
INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at, batch_key, batch_role, batch_epoch, batch_state, attributed_op_ref, accepted_at, bound_session_id, dispatched_at, terminal_delivery_id) VALUES
 ('live-trigger', 'o1', '{}', 'q1', NULL, 'pending', '2026-09-02T00:00:00.000Z', 'b1', 'trigger', 3, 'accepted', 'gw-p-live', '2026-09-02T00:00:02.000Z', 's-live', '2026-09-02T00:00:01.000Z', '{"0":"gw-t-x"}'),
 ('live-member', 'o1', '{}', 'q1b', NULL, 'pending', '2026-09-02T00:00:00.500Z', 'b1', 'member', 3, 'accepted', 'gw-p-live', '2026-09-02T00:00:02.000Z', 's-live', '2026-09-02T00:00:01.000Z', NULL),
 ('live-steer', 'o1', '{}', 'more', NULL, 'done', '2026-09-02T00:00:03.000Z', 'b1', 'steer', 3, 'done', 'gw-p-live', '2026-09-02T00:00:03.500Z', NULL, NULL, NULL),
 ('settled-bound', 'o2', '{}', 'q2', NULL, 'pending', '2026-09-02T00:01:00.000Z', 'b2', 'trigger', 0, 'settled', 'gw-p-sb', NULL, 's-2', '2026-09-02T00:01:00.100Z', NULL),
 ('settled-unbound', 'o3', '{}', 'q3', NULL, 'pending', '2026-09-02T00:02:00.000Z', 'b3', 'trigger', 0, 'settled', 'gw-p-su', NULL, NULL, NULL, NULL),
 ('finished', 'o4', '{}', 'q4', NULL, 'done', '2026-09-02T00:03:00.000Z', 'b4', 'trigger', 1, 'done', 'gw-p-done', '2026-09-02T00:03:01.000Z', 's-4', '2026-09-02T00:03:00.500Z', '{"0":"gw-t-y"}'),
 ('settled-member', 'o2', '{}', 'q2b', NULL, 'pending', '2026-09-02T00:01:00.200Z', 'b2', 'member', 0, 'settled', 'gw-p-sb', NULL, 's-2', '2026-09-02T00:01:00.100Z', NULL),
 ('legacy-processing', 'o5', '{}', 'claimed by the pre-actor path', NULL, 'processing', '2026-09-02T00:04:00.000Z', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
 ('wedged-unbatched', 'o1', '{}', 'stuck behind the wedge for 85 minutes', NULL, 'pending', '2026-09-01T22:00:00.000Z', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
`);
		expect(
			raw.query<{ version: number }, []>("SELECT MAX(version) AS version FROM schema_migrations").get()?.version,
		).toBe(18);
		raw.close();

		const upgraded = await GatewayDatabase.open(path);
		expect(upgraded.schemaVersion).toBe(23);
		const after = new Database(path, { readonly: true });
		const columns = after
			.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
			.all()
			.map((column) => column.name);
		const total = after.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM inbound_messages").get()?.n;
		after.close();
		for (const gone of ["batch_key", "batch_role", "batch_epoch", "batch_state", "attributed_op_ref", "accepted_at"])
			expect(columns).not.toContain(gone);
		for (const kept of [
			"turn_role",
			"turn_epoch",
			"turn_state",
			"turn_op_ref",
			"bound_session_id",
			"dispatched_at",
			"terminal_delivery_id",
		])
			expect(columns).toContain(kept);
		expect(total).toBe(9);

		// Accepted trigger: turn intact, floor and terminal claim preserved.
		expect(upgraded.inboundTurnRow("gw-p-live")).toMatchObject({
			message_id: "live-trigger",
			state: "pending",
			turn_role: "trigger",
			turn_epoch: 3,
			turn_state: "accepted",
			bound_session_id: "s-live",
			dispatched_at: "2026-09-02T00:00:01.000Z",
			terminal_delivery_id: '{"0":"gw-t-x"}',
		});
		expect(upgraded.inboundTurnDispatchedAt("gw-p-live")).toBe("2026-09-02T00:00:01.000Z");
		expect(upgraded.inboundTurnClaimTerminal("gw-p-live", 0, "gw-t-late")).toBe("gw-t-x");
		// The steer AND the accepted member stay attributed to that turn as done
		// input: both were in the prompt the runtime holds, so neither is ever
		// sent again.
		expect(upgraded.inboundTurnRows("gw-p-live").map((row) => [row.message_id, row.turn_role, row.state])).toEqual([
			["live-trigger", "trigger", "pending"],
			["live-member", "steer", "done"],
			["live-steer", "steer", "done"],
		]);
		expect(upgraded.inboundPendingOldest("o1")).toMatchObject({ message_id: "wedged-unbatched", turn_state: null });
		expect(upgraded.inboundPendingCount("o1")).toBe(2);
		// Settled + bound -> bound turn; its never-sent member returns to plain
		// pending (the model never saw it); settled + never bound -> plain pending.
		expect(upgraded.inboundNonterminalTurns("o2")).toEqual([
			{
				originKey: "o2",
				epoch: 0,
				state: "bound",
				opRef: "gw-p-sb",
				sessionId: "s-2",
				triggerMessageId: "settled-bound",
			},
		]);
		// The settled+bound member rides with its trigger (pending, attributed) and
		// is decided with it: never sent again on its own, never lost.
		expect(upgraded.inboundPendingOldest("o2")).toBeUndefined();
		expect(
			upgraded.inboundTurnRows("gw-p-sb").map((row) => [row.message_id, row.turn_role, row.state, row.turn_state]),
		).toEqual([
			["settled-bound", "trigger", "pending", "bound"],
			["settled-member", "steer", "pending", "bound"],
		]);
		// Recovery proves the send landed: both become done input together.
		expect(upgraded.inboundTurnAccept("gw-p-sb")).toBe(true);
		expect(upgraded.inboundTurnRows("gw-p-sb").map((row) => [row.message_id, row.state, row.turn_state])).toEqual([
			["settled-bound", "pending", "accepted"],
			["settled-member", "done", "done"],
		]);
		expect(upgraded.inboundTurnComplete("gw-p-sb")).toBe(1);
		expect(upgraded.inboundPendingCount("o2")).toBe(0);
		// A legacy processing row is pending again, not stranded.
		expect(upgraded.inboundPendingOldest("o5")).toMatchObject({ message_id: "legacy-processing", state: "pending" });
		expect(upgraded.inboundNonterminalTurns("o3")).toEqual([]);
		expect(upgraded.inboundPendingOldest("o3")).toMatchObject({ message_id: "settled-unbound", turn_op_ref: null });
		// A finished turn is history and stays done.
		expect(upgraded.inboundTurnRow("gw-p-done")).toMatchObject({ state: "done", turn_state: "done" });
		expect(upgraded.inboundNonterminalTurnCount()).toBe(1);
		// The uniqueness the actor relies on survives the rebuild.
		expect(() =>
			upgraded.inboundBindTurn({
				messageId: "wedged-unbatched",
				originKey: "o1",
				epoch: 3,
				opRef: "gw-p-dup",
				sessionId: "s-live",
			}),
		).toThrow("already has a nonterminal turn");
		upgraded.close();
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
