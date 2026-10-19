import type { Database, SQLQueryBindings } from "bun:sqlite";
import { type OriginRef, originKey, validateOriginRef } from "@gajae-gateway/protocol";
import { DatabaseStartupError } from "./errors";
import { JOURNAL_PROJECTIONS, type JournalEntity, journalSnapshotSql } from "./groups/journal";
import type { WorkAttemptRuntime, WorkParent } from "./groups/work";
import { validateWorkRuntime, workAssert, workAttemptReportId } from "./groups/work";

export type MigrationStatement<T> = Omit<ReturnType<Database["query"]>, "all" | "get"> & {
	all(...params: SQLQueryBindings[]): T[];
	get(...params: SQLQueryBindings[]): T | null;
};
export const LATEST_SCHEMA_VERSION = 38;

export interface MigrationHost {
	readonly database: {
		statement<T = unknown>(sql: string): MigrationStatement<T>;
		execute(sql: string): void;
	};
	readonly schemaVersion: number;
	readonly instanceId: string;
	withTransaction(operation: () => void): void;
	assertNotQuarantined(kind: string, id: string): void;
}

export function runMigrations(host: MigrationHost): void {
	// schema_migrations and broker_authority tables are created in open() before this method is called.
	const current = host.schemaVersion;
	if (current > LATEST_SCHEMA_VERSION) {
		throw new DatabaseStartupError(
			"newer_schema",
			`database schema ${current} is newer than supported schema ${LATEST_SCHEMA_VERSION}`,
		);
	}
	if (current < 1) {
		host.withTransaction(() => {
			host.database.execute(
				"CREATE TABLE sessions (origin_key TEXT PRIMARY KEY, gjc_session_id TEXT NOT NULL, created_at TEXT NOT NULL)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(1, new Date().toISOString());
		});
	}
	if (current < 2) {
		host.withTransaction(() => {
			host.database.execute(
				"CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, turn_id TEXT NOT NULL, origin_key TEXT NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','inflight','confirmed','failed_ambiguous','expired')), attempts INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(2, new Date().toISOString());
		});
	}

	if (current < 3) {
		host.withTransaction(() => {
			host.database.execute(
				"ALTER TABLE sessions ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0; ALTER TABLE sessions ADD COLUMN last_activity_at TEXT; ALTER TABLE sessions ADD COLUMN origin_ref_json TEXT; CREATE TABLE recall_snippets (id INTEGER PRIMARY KEY AUTOINCREMENT, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, text TEXT NOT NULL, at TEXT NOT NULL)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(3, new Date().toISOString());
		});
	}

	if (current < 4) {
		host.withTransaction(() => {
			host.database.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)");
			host.database.statement("INSERT INTO meta (key, value) VALUES ('instance_id', ?)").run(crypto.randomUUID());
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(4, new Date().toISOString());
		});
	}
	if (current < 5) {
		host.withTransaction(() => {
			host.database.execute(
				"CREATE TABLE memory_intents (id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('queued','written','committed','receipted','quarantined')), created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(5, new Date().toISOString());
		});
	}
	if (current < 6) {
		host.withTransaction(() => {
			host.database.execute(
				"CREATE TABLE monitors (monitor_id TEXT PRIMARY KEY, name TEXT NOT NULL, trigger_json TEXT NOT NULL, event_types_json TEXT NOT NULL, burst_policy TEXT NOT NULL, channel_target_json TEXT, enabled INTEGER NOT NULL, created_at TEXT NOT NULL); CREATE TABLE monitor_events (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','failed')), batch_id TEXT, updated_at TEXT NOT NULL); CREATE TABLE authored_outputs (event_id TEXT PRIMARY KEY, output_text TEXT NOT NULL, authored_at TEXT NOT NULL)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(6, new Date().toISOString());
		});
	}
	if (current < 7) {
		host.withTransaction(() => {
			// Inbound messages must be durable before dispatch: a message arriving while a turn is
			// in flight was previously processed transiently and lost outright.
			host.database.execute(
				"CREATE TABLE inbound_messages (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL); CREATE INDEX inbound_messages_claim ON inbound_messages (origin_key, state, received_at)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(7, new Date().toISOString());
		});
	}
	if (current < 8) {
		host.withTransaction(() => {
			// Declined messages are still context, never commands: every inbound platform
			// message lands here so an engaged turn can read the unread diff since the
			// persona's last reply in that conversation.
			host.database.execute(
				"CREATE TABLE conversation_context (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, author_id TEXT, author_name TEXT, body TEXT NOT NULL, received_at TEXT NOT NULL, consumed_at TEXT); CREATE INDEX conversation_context_unread ON conversation_context (origin_key, consumed_at, received_at)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(8, new Date().toISOString());
		});
	}
	if (current < 9) {
		host.withTransaction(() => {
			// v9 retained an observational monitor turn count. Persistent SDK sessions
			// now rely on native compaction; no gateway turn ceiling consumes this column.
			host.database.execute("ALTER TABLE sessions ADD COLUMN turn_count INTEGER NOT NULL DEFAULT 0");
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(9, new Date().toISOString());
		});
	}
	if (current < 10) {
		host.withTransaction(() => {
			// Issue #10: durable lane jobs. One row per job; the record JSON is the
			// fail-closed authority (schema-validated on read by @gajae-gateway/subsession),
			// while the status column stays a plain indexed projection for operators.
			host.database.execute(
				"CREATE TABLE lane_jobs (job_id TEXT PRIMARY KEY, lane_key TEXT NOT NULL UNIQUE, branch TEXT NOT NULL, worktree_path TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('running','attempt_ended','awaiting_operator','stalled','done','aborted')), record_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(10, new Date().toISOString());
		});
	}
	if (current < 11) {
		host.withTransaction(() => {
			// Monitor recovery (issue #29): extend the monitor event state contract,
			// preserve every legacy row, and add bounded operator evidence/slot tables.
			// Existing schema-10 lane_jobs deployments reach this step without loss.
			host.database.execute(
				`CREATE TABLE monitor_events_new (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','authored_no_delivery','failed','failed_no_retry')), batch_id TEXT, dispatch_attempts INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
INSERT INTO monitor_events_new (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at) SELECT event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, 0, updated_at FROM monitor_events;
DROP TABLE monitor_events;
ALTER TABLE monitor_events_new RENAME TO monitor_events;
CREATE TABLE monitor_failures (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, code TEXT NOT NULL, detail TEXT NOT NULL, failed_at TEXT NOT NULL);
CREATE INDEX monitor_failures_event ON monitor_failures (event_id, failed_at);
CREATE TABLE monitor_slots (monitor_id TEXT NOT NULL, slot_at TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (monitor_id, slot_at));`,
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(11, new Date().toISOString());
		});
	}
	if (current < 12) {
		host.withTransaction(() => {
			// Durable dispatch ownership and atomic scheduled-slot admission.
			host.database.execute(
				`CREATE TABLE dispatch_leases (event_id TEXT PRIMARY KEY, owner TEXT NOT NULL, lease_id TEXT NOT NULL, acquired_at TEXT NOT NULL, expires_at TEXT NOT NULL);
CREATE INDEX dispatch_leases_expiry ON dispatch_leases (expires_at);
ALTER TABLE monitor_slots ADD COLUMN event_id TEXT;`,
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(12, new Date().toISOString());
		});
	}
	if (current < 13) {
		host.withTransaction(() => {
			// Issue #35: durable reset floors plus aggregate-only omission evidence.
			// Existing context rows remain intact and unread until the bounded policy
			// classifies them; lane jobs, monitor tables, and meta counters are untouched.
			host.database.execute(
				"CREATE TABLE IF NOT EXISTS conversation_context_state (origin_key TEXT PRIMARY KEY, floor_at TEXT, floor_row_id INTEGER NOT NULL DEFAULT 0, expired_count INTEGER NOT NULL DEFAULT 0, truncated_count INTEGER NOT NULL DEFAULT 0, omitted_oldest_at TEXT, omitted_newest_at TEXT, last_omitted_at TEXT, pending_expired_count INTEGER NOT NULL DEFAULT 0, pending_truncated_count INTEGER NOT NULL DEFAULT 0, pending_omitted_oldest_at TEXT, pending_omitted_newest_at TEXT, omission_revision INTEGER NOT NULL DEFAULT 0)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(13, new Date().toISOString());
		});
	}
	if (current < 14) {
		host.withTransaction(() => {
			// Issue #37: durable once-per-origin-epoch bootstrap projection. Pending
			// state is the monotonic inequality epoch > last_bootstrapped_epoch, so
			// every existing and newly bumped epoch starts pending without a second
			// state machine that can drift from sessions. Bodies are never persisted.
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(sessions)")
					.all()
					.map((row) => row.name),
			);
			const additions = [
				["last_bootstrapped_epoch", "INTEGER NOT NULL DEFAULT -1"],
				["bootstrap_applied_at", "TEXT"],
				["bootstrap_sections_json", "TEXT NOT NULL DEFAULT '[]'"],
				["bootstrap_byte_count", "INTEGER NOT NULL DEFAULT 0"],
				["bootstrap_truncated", "INTEGER NOT NULL DEFAULT 0 CHECK(bootstrap_truncated IN (0,1))"],
				["bootstrap_diagnostics_json", "TEXT NOT NULL DEFAULT '[]'"],
			] as const;
			for (const [name, declaration] of additions)
				if (!columns.has(name)) host.database.execute(`ALTER TABLE sessions ADD COLUMN ${name} ${declaration}`);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(14, new Date().toISOString());
		});
	}
	if (current < 15) {
		host.withTransaction(() => {
			// Per-monitor authoring instruction. Nullable additive column: every
			// existing monitor keeps firing with no instruction and the authoring
			// prompt falls back to the built-in maintenance guidance.
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(monitors)")
					.all()
					.map((row) => row.name),
			);
			if (!columns.has("instruction")) host.database.execute("ALTER TABLE monitors ADD COLUMN instruction TEXT");
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(15, new Date().toISOString());
		});
	}
	if (current < 16) {
		host.withTransaction(() => {
			// Issue #20: per-conversation model override. This lives beside session
			// state rather than in config.json, because config is the owner's file
			// (a slash command must not race their editor) and `model` is not a
			// reloadable field, so a config write would not be read until restart.
			host.database.execute(
				"CREATE TABLE IF NOT EXISTS conversation_model (origin_key TEXT PRIMARY KEY, selection_json TEXT NOT NULL, set_by TEXT, updated_at TEXT NOT NULL)",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(16, new Date().toISOString());
		});
	}
	if (current < 17) {
		host.withTransaction(() => {
			// Issue #92: lifecycle state and its durable session/cursor binding are
			// additive. Do not rewrite the legacy state column or its v16 CHECK.
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(inbound_messages)")
					.all()
					.map((row) => row.name),
			);
			const additions = [
				["batch_key", "TEXT"],
				["batch_role", "TEXT CHECK(batch_role IS NULL OR batch_role IN ('trigger', 'member', 'steer'))"],
				["batch_epoch", "INTEGER"],
				["batch_state", "TEXT CHECK(batch_state IS NULL OR batch_state IN ('settled', 'accepted', 'done'))"],
				["attributed_op_ref", "TEXT"],
				["accepted_at", "TEXT"],
				["bound_session_id", "TEXT"],
			] as const;
			for (const [name, declaration] of additions)
				if (!columns.has(name)) host.database.execute(`ALTER TABLE inbound_messages ADD COLUMN ${name} ${declaration}`);
			host.database.execute(
				"CREATE UNIQUE INDEX IF NOT EXISTS inbound_messages_nonterminal_trigger ON inbound_messages (origin_key, batch_epoch) WHERE batch_role = 'trigger' AND batch_state IN ('settled', 'accepted')",
			);
			host.database.execute(
				"CREATE TABLE IF NOT EXISTS session_tail_cursors (session_id TEXT PRIMARY KEY, cursor TEXT NOT NULL, updated_at TEXT NOT NULL)",
			);
			host.database.execute(
				"UPDATE inbound_messages SET bound_session_id = (SELECT gjc_session_id FROM sessions WHERE sessions.origin_key = inbound_messages.origin_key AND sessions.epoch = inbound_messages.batch_epoch AND sessions.gjc_session_id <> '') WHERE bound_session_id IS NULL AND batch_state IN ('settled', 'accepted')",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(17, new Date().toISOString());
		});
	}
	if (current < 18) {
		host.withTransaction(() => {
			// dispatched_at: wall-clock stamped at bind time, BEFORE the send, so a
			// tail-less reconcile has a transcript floor that can never postdate
			// the answer (accepted_at is stamped after the CLI returns and can).
			// terminal_delivery_id: which ledger delivery satisfied this batch's
			// one terminal reply slot; a later terminal for the same batch is a
			// no-op regardless of text.
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(inbound_messages)")
					.all()
					.map((row) => row.name),
			);
			for (const [name, declaration] of [
				["dispatched_at", "TEXT"],
				["terminal_delivery_id", "TEXT"],
			] as const)
				if (!columns.has(name)) host.database.execute(`ALTER TABLE inbound_messages ADD COLUMN ${name} ${declaration}`);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(18, new Date().toISOString());
		});
	}
	if (current < 19) {
		host.withTransaction(() => {
			// The batch model is gone: no settle window, no coalesced members, no
			// expiry. One trigger row = one turn; steers attach to the running
			// turn's op-ref. Rebuilt table because SQLite cannot drop CHECKed
			// columns in place. Preserved per row: bound_session_id (reattach
			// after restart), dispatched_at (the attribution floor) and
			// terminal_delivery_id (restart-safe per-part terminal claim).
			// A v18 nonterminal batch maps to a nonterminal turn on its trigger.
			// Its `member` rows were part of that batch's ONE prompt: members of
			// an accepted batch (the runtime holds the op, they were sent) become
			// done input attributed to the turn, like steers. Members of a
			// settled+bound batch sit in the send/ack crash window - the prompt
			// may or may not have reached the runtime - so they ride with the
			// trigger as pending `steer`/`bound` rows and are decided WITH it by
			// recovery (accepted -> done input; send proven absent -> released
			// to plain pending together). Members of a settled+unbound batch
			// were never sent and go back to plain pending. Nothing is deleted.
			// Legacy `processing` rows (the pre-actor claim path, whose startup
			// normalisation is gone) return to pending.
			host.database.execute(
				"CREATE TABLE inbound_messages_v19 (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL, turn_role TEXT CHECK(turn_role IS NULL OR turn_role IN ('trigger', 'steer')), turn_epoch INTEGER, turn_state TEXT CHECK(turn_state IS NULL OR turn_state IN ('bound', 'accepted', 'done')), turn_op_ref TEXT, bound_session_id TEXT, dispatched_at TEXT, terminal_delivery_id TEXT)",
			);
			host.database.execute(
				"INSERT INTO inbound_messages_v19 (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at) SELECT message_id, origin_key, origin_ref_json, body, engagement_json, CASE state WHEN 'processing' THEN 'pending' ELSE state END, received_at FROM inbound_messages",
			);
			// Triggers that were bound (session chosen) keep their turn; an unbound
			// settled trigger never had an operation and returns to plain pending.
			host.database.execute(
				"UPDATE inbound_messages_v19 SET turn_role = 'trigger', turn_epoch = src.batch_epoch, turn_state = CASE src.batch_state WHEN 'settled' THEN 'bound' ELSE src.batch_state END, turn_op_ref = src.attributed_op_ref, bound_session_id = src.bound_session_id, dispatched_at = src.dispatched_at, terminal_delivery_id = src.terminal_delivery_id FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'trigger' AND src.attributed_op_ref IS NOT NULL AND (src.batch_state <> 'settled' OR src.bound_session_id IS NOT NULL)",
			);
			host.database.execute(
				"UPDATE inbound_messages_v19 SET turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'done', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'steer'",
			);
			host.database.execute(
				"UPDATE inbound_messages_v19 SET state = 'done', turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'done', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'member' AND src.batch_state IN ('accepted', 'done')",
			);
			host.database.execute(
				"UPDATE inbound_messages_v19 SET turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'bound', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'member' AND src.batch_state = 'settled' AND src.bound_session_id IS NOT NULL AND src.state = 'pending'",
			);
			host.database.execute("DROP TABLE inbound_messages");
			host.database.execute("ALTER TABLE inbound_messages_v19 RENAME TO inbound_messages");
			host.database.execute("CREATE INDEX inbound_messages_claim ON inbound_messages (origin_key, state, received_at)");
			host.database.execute(
				"CREATE UNIQUE INDEX inbound_messages_nonterminal_trigger ON inbound_messages (origin_key, turn_epoch) WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted')",
			);
			host.database.execute(
				"CREATE UNIQUE INDEX inbound_messages_turn_trigger ON inbound_messages (turn_op_ref) WHERE turn_role = 'trigger'",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(19, new Date().toISOString());
		});
	}
	if (current < 20) {
		host.withTransaction(() => {
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(monitors)")
					.all()
					.map((row) => row.name),
			);
			if (!columns.has("model_json")) host.database.execute("ALTER TABLE monitors ADD COLUMN model_json TEXT");
			if (!columns.has("service_tier")) host.database.execute("ALTER TABLE monitors ADD COLUMN service_tier TEXT");
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(20, new Date().toISOString());
		});
	}
	if (current < 21) {
		host.withTransaction(() => {
			// Existing lane history is preserved verbatim. Recovery explicitly adopts
			// historical open attempts without inventing a target or dispatch proof.
			host.database.execute(`CREATE TABLE work_attempt_runtime (
				op_ref TEXT PRIMARY KEY, job_id TEXT NOT NULL, lane_key TEXT NOT NULL,
				session_id TEXT NOT NULL, version INTEGER NOT NULL CHECK(version >= 0),
				settled_at TEXT, delivery_id TEXT NOT NULL UNIQUE,
				record_json TEXT NOT NULL CHECK(length(record_json) <= 16384));
				CREATE UNIQUE INDEX work_attempt_open_lane ON work_attempt_runtime(lane_key) WHERE settled_at IS NULL;`);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(21, new Date().toISOString());
		});
	}
	if (current < 22) {
		host.withTransaction(() => {
			host.database.execute(`CREATE TABLE IF NOT EXISTS broker_authority (
				singleton INTEGER PRIMARY KEY CHECK(singleton = 1), authority_key TEXT NOT NULL);
				CREATE TABLE broker_owned_bindings (
					authority_key TEXT NOT NULL, session_id TEXT NOT NULL, origin_key TEXT NOT NULL,
					epoch INTEGER NOT NULL CHECK(epoch >= 0), repo TEXT NOT NULL, created_at TEXT NOT NULL,
					PRIMARY KEY(authority_key, session_id), UNIQUE(authority_key, origin_key, epoch));
				CREATE TABLE broker_tail_cursors (
					authority_key TEXT NOT NULL, session_id TEXT NOT NULL, cursor TEXT NOT NULL, updated_at TEXT NOT NULL,
					PRIMARY KEY(authority_key, session_id));
				CREATE TABLE broker_cutovers (
					id TEXT PRIMARY KEY, old_authority TEXT, target_authority TEXT NOT NULL UNIQUE,
					evidence TEXT NOT NULL, disposition TEXT NOT NULL CHECK(disposition IN ('quiescent','quarantine')),
					snapshot_json TEXT NOT NULL, created_at TEXT NOT NULL);
				CREATE TABLE broker_quarantine (
					kind TEXT NOT NULL CHECK(kind IN ('inbound','work','monitor')), subject_id TEXT NOT NULL,
					cutover_id TEXT NOT NULL, PRIMARY KEY(kind, subject_id));
				CREATE TABLE broker_retired_sessions (session_id TEXT PRIMARY KEY, cutover_id TEXT NOT NULL);`);
			for (const table of [
				"broker_owned_bindings",
				"broker_cutovers",
				"broker_quarantine",
				"broker_retired_sessions",
			]) {
				for (const action of ["UPDATE", "DELETE"])
					host.database.execute(
						`CREATE TRIGGER ${table}_immutable_${action.toLowerCase()} BEFORE ${action} ON ${table} BEGIN SELECT RAISE(ABORT, 'immutable broker provenance'); END`,
					);
			}
			for (const [kind, table, column] of [
				["inbound", "inbound_messages", "message_id"],
				["work", "lane_jobs", "job_id"],
				["work", "work_attempt_runtime", "job_id"],
				["monitor", "monitor_events", "event_id"],
				["monitor", "authored_outputs", "event_id"],
			] as const) {
				for (const action of ["UPDATE", "DELETE"])
					host.database.execute(`CREATE TRIGGER ${table}_quarantine_${action.toLowerCase()} BEFORE ${action} ON ${table}
					WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = '${kind}' AND subject_id = OLD.${column})
					BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END`);
			}
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(22, new Date().toISOString());
		});
	}
	if (current < 23) {
		host.withTransaction(() => {
			host.database.execute(
				"ALTER TABLE memory_intents ADD COLUMN quarantine_reason TEXT; ALTER TABLE memory_intents ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0 AND typeof(attempts) = 'integer')",
			);
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(23, new Date().toISOString());
		});
	}
	if (current < 24) {
		host.withTransaction(() => {
			host.database.execute(`
				ALTER TABLE inbound_messages ADD COLUMN source TEXT NOT NULL DEFAULT 'platform' CHECK(source IN ('platform','lane_report'));
				CREATE TABLE lane_reports (
					report_id TEXT PRIMARY KEY,
					parent_name TEXT NOT NULL,
					child_name TEXT NOT NULL,
					child_op_ref TEXT NOT NULL UNIQUE,
					body TEXT NOT NULL CHECK(length(CAST(body AS BLOB)) <= 2048),
					root_json TEXT,
					state TEXT NOT NULL CHECK(state IN ('pending','claimed','consumed','fallback','held','undeliverable')),
					claim_kind TEXT CHECK(claim_kind IS NULL OR claim_kind IN ('steer','wake')),
					claim_ref TEXT,
					claim_target_op_ref TEXT,
					claim_seq INTEGER NOT NULL DEFAULT 0 CHECK(claim_seq >= 0),
					hold_reason TEXT,
					consumed_op_ref TEXT,
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL,
					CHECK(state NOT IN ('claimed','held') OR (claim_kind IS NOT NULL AND claim_ref IS NOT NULL)),
					CHECK(state <> 'pending' OR (claim_kind IS NULL AND claim_ref IS NULL AND claim_target_op_ref IS NULL)),
					CHECK(claim_kind IS NULL OR claim_seq > 0),
					CHECK(claim_kind IS NULL OR claim_kind <> 'steer' OR claim_target_op_ref IS NOT NULL),
					CHECK(claim_kind IS NULL OR claim_kind <> 'wake' OR claim_target_op_ref IS NULL),
					CHECK((state = 'consumed' AND consumed_op_ref IS NOT NULL) OR (state <> 'consumed' AND consumed_op_ref IS NULL))
				);
				CREATE INDEX lane_reports_parent_state ON lane_reports(parent_name, state, created_at);
				DROP TRIGGER IF EXISTS work_attempt_runtime_quarantine_update;
				DROP TRIGGER IF EXISTS work_attempt_runtime_quarantine_delete;
			`);
			for (const row of host.database
				.statement<{ op_ref: string; job_id: string; record_json: string }>(
					"SELECT op_ref, job_id, record_json FROM work_attempt_runtime",
				)
				.all()) {
				try {
					const legacy = JSON.parse(row.record_json) as Record<string, unknown>;
					workAssert(legacy && typeof legacy === "object" && !Array.isArray(legacy));
					const target = legacy.target;
					let parent: WorkParent | null = null;
					if (target !== null) {
						validateOriginRef(target as OriginRef);
						const origin = structuredClone(target as OriginRef);
						parent = { kind: "persona", originKey: originKey(origin), origin };
					}
					delete legacy.target;
					legacy.parent = parent;
					legacy.reportId = workAttemptReportId(host.instanceId, row.job_id, row.op_ref);
					legacy.wakeReportId = null;
					legacy.noticeHash = null;
					if (legacy.decision === "enqueued") legacy.decision = "fallback";
					const runtime = legacy as unknown as WorkAttemptRuntime;
					validateWorkRuntime(runtime, host.instanceId);
					workAssert(runtime.opRef === row.op_ref && runtime.jobId === row.job_id);
					host.database
						.statement("UPDATE work_attempt_runtime SET record_json = ? WHERE op_ref = ?")
						.run(JSON.stringify(runtime), row.op_ref);
				} catch {
					throw new DatabaseStartupError(
						"migration_corrupt",
						`schema v24 could not migrate work attempt ${row.op_ref}`,
					);
				}
			}
			host.database.execute(`
				CREATE TRIGGER work_attempt_runtime_quarantine_update BEFORE UPDATE ON work_attempt_runtime
				WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
				BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
				CREATE TRIGGER work_attempt_runtime_quarantine_delete BEFORE DELETE ON work_attempt_runtime
				WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
				BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
			`);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(24, new Date().toISOString());
		});
	}
	if (current < 25) {
		host.withTransaction(() => {
			// Track each epoch's AGENTS.md baseline. Only the digest is persisted;
			// prompt content remains in the workspace and is re-read on each turn.
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(sessions)")
					.all()
					.map((row) => row.name),
			);
			if (!columns.has("agents_md_epoch"))
				host.database.execute("ALTER TABLE sessions ADD COLUMN agents_md_epoch INTEGER NOT NULL DEFAULT -1");
			if (!columns.has("agents_md_digest"))
				host.database.execute("ALTER TABLE sessions ADD COLUMN agents_md_digest TEXT");
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(25, new Date().toISOString());
		});
	}
	if (current < 26) {
		host.withTransaction(() => {
			// Issue #171: the allowlisted classification of the latest failed attempt.
			const columns = host.database
				.statement<{ name: string }>("PRAGMA table_info(deliveries)")
				.all()
				.map((row) => row.name);
			if (!columns.includes("last_error")) host.database.execute("ALTER TABLE deliveries ADD COLUMN last_error TEXT");
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(26, new Date().toISOString());
		});
	}
	if (current < 27) {
		host.withTransaction(() => {
			// Issue #82: declared procedure files are re-read per firing and each
			// authoring event records the procedure version it was given.
			const columns = (table: string) =>
				new Set(
					host.database
						.statement<{ name: string }>(`PRAGMA table_info(${table})`)
						.all()
						.map((row) => row.name),
				);
			if (!columns("monitors").has("procedure_files_json"))
				host.database.execute("ALTER TABLE monitors ADD COLUMN procedure_files_json TEXT");
			if (!columns("monitor_events").has("procedure_json"))
				host.database.execute("ALTER TABLE monitor_events ADD COLUMN procedure_json TEXT");
			host.database
				.statement("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
				.run(27, new Date().toISOString());
		});
	}
	if (current < 28) {
		host.withTransaction(() => {
			// Issue #83: per-monitor overlap policy and the terminal `skipped` stage.
			// Rebuild monitor_events because its stage CHECK constraint must widen.
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(monitors)")
					.all()
					.map((row) => row.name),
			);
			if (!columns.has("overlap"))
				host.database.execute(
					"ALTER TABLE monitors ADD COLUMN overlap TEXT NOT NULL DEFAULT 'queue' CHECK(overlap IN ('queue','skip'))",
				);
			host.database.execute(`CREATE TABLE monitor_events_v28 (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','authored_no_delivery','failed','failed_no_retry','skipped')), batch_id TEXT, dispatch_attempts INTEGER NOT NULL DEFAULT 0, skipped_by TEXT, updated_at TEXT NOT NULL, procedure_json TEXT);
INSERT INTO monitor_events_v28 (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at, procedure_json) SELECT event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at, procedure_json FROM monitor_events ORDER BY rowid;
DROP TABLE monitor_events;
ALTER TABLE monitor_events_v28 RENAME TO monitor_events;
CREATE INDEX monitor_events_monitor_stage ON monitor_events (monitor_id, stage);`);
			for (const action of ["UPDATE", "DELETE"])
				host.database.execute(`CREATE TRIGGER monitor_events_quarantine_${action.toLowerCase()} BEFORE ${action} ON monitor_events
				WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'monitor' AND subject_id = OLD.event_id)
				BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END`);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(28, new Date().toISOString());
		});
	}
	if (current < 29) {
		host.withTransaction(() => {
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(monitor_failures)")
					.all()
					.map((row) => row.name),
			);
			const additions = [
				[
					"protocol_reason",
					"TEXT CHECK(protocol_reason IS NULL OR protocol_reason IN ('protocol_response_not_array', 'protocol_entry_missing_field', 'protocol_unknown_event', 'protocol_duplicate_event', 'protocol_omitted_event', 'protocol_unparseable_json', 'protocol_off_contract'))",
				],
				[
					"response_byte_length",
					"INTEGER CHECK(response_byte_length IS NULL OR (typeof(response_byte_length) = 'integer' AND response_byte_length BETWEEN 0 AND 9007199254740991))",
				],
				[
					"response_entry_count",
					"INTEGER CHECK(response_entry_count IS NULL OR (typeof(response_entry_count) = 'integer' AND response_entry_count BETWEEN 0 AND 9007199254740991))",
				],
			] as const;
			for (const [name, declaration] of additions)
				if (!columns.has(name)) host.database.execute(`ALTER TABLE monitor_failures ADD COLUMN ${name} ${declaration}`);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(29, new Date().toISOString());
		});
	}
	if (current < 30) {
		// Hot-path lookup indexes (deliveries by turn, monitor events by batch / open / authored)
		// and the age indexes behind the retention sweep. Predicates are spelled exactly as the
		// queries spell them so SQLite can prove they imply the partial index.
		host.withTransaction(() => {
			host.database.execute(`
				CREATE INDEX IF NOT EXISTS inbound_messages_gc ON inbound_messages(received_at) WHERE state = 'done';
				CREATE INDEX IF NOT EXISTS deliveries_gc ON deliveries(state, updated_at);
				CREATE INDEX IF NOT EXISTS deliveries_turn ON deliveries(turn_id);
				CREATE INDEX IF NOT EXISTS monitor_events_gc ON monitor_events(updated_at) WHERE stage IN ('delivered','authored_no_delivery','failed_no_retry','skipped');
				CREATE INDEX IF NOT EXISTS monitor_events_open ON monitor_events(fired_at) WHERE stage NOT IN ('delivered','authored_no_delivery','failed_no_retry','skipped');
				CREATE INDEX IF NOT EXISTS monitor_events_batch ON monitor_events(batch_id);
				CREATE INDEX IF NOT EXISTS monitor_events_authored ON monitor_events(batch_id) WHERE stage = 'authored';
				CREATE INDEX IF NOT EXISTS monitor_events_monitor ON monitor_events(monitor_id, fired_at);
				CREATE INDEX IF NOT EXISTS lane_reports_gc ON lane_reports(updated_at) WHERE state IN ('consumed','fallback','undeliverable');
				CREATE INDEX IF NOT EXISTS monitor_slots_gc ON monitor_slots(slot_at);
				CREATE INDEX IF NOT EXISTS memory_intents_open ON memory_intents(created_at) WHERE state <> 'receipted';
			`);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(30, new Date().toISOString());
		});
	}
	if (current < 31) {
		// Migration 25 was edited after it shipped to add the AGENTS.md baseline
		// columns, so every database that recorded 25 before that edit is at a
		// version that claims the columns exist while they do not. Backfill them
		// here instead of re-running 25, which is already recorded.
		host.withTransaction(() => {
			const columns = new Set(
				host.database
					.statement<{ name: string }>("PRAGMA table_info(sessions)")
					.all()
					.map((row) => row.name),
			);
			if (!columns.has("agents_md_epoch"))
				host.database.execute("ALTER TABLE sessions ADD COLUMN agents_md_epoch INTEGER NOT NULL DEFAULT -1");
			if (!columns.has("agents_md_digest"))
				host.database.execute("ALTER TABLE sessions ADD COLUMN agents_md_digest TEXT");
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(31, new Date().toISOString());
		});
	}
	if (current < 32) {
		// Issue #367: Add index to conversation_context (consumed_at, received_at) to support
		// efficient batched retention DELETE and unconsumed GROUP BY scan in contextMaintain().
		host.withTransaction(() => {
			host.database.execute(
				"CREATE INDEX IF NOT EXISTS conversation_context_retention ON conversation_context (consumed_at, received_at)",
			);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(32, new Date().toISOString());
		});
	}
	if (current < 33) {
		// Issue #369: Add the terminal contract_unmet stage while retaining every monitor index.
		host.withTransaction(() => {
			host.database.execute(`CREATE TABLE monitor_events_v33 (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','authored_no_delivery','failed','failed_no_retry','skipped','contract_unmet')), batch_id TEXT, dispatch_attempts INTEGER NOT NULL DEFAULT 0, skipped_by TEXT, updated_at TEXT NOT NULL, procedure_json TEXT);
INSERT INTO monitor_events_v33 (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, skipped_by, updated_at, procedure_json) SELECT event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, skipped_by, updated_at, procedure_json FROM monitor_events ORDER BY rowid;
DROP TABLE monitor_events;
ALTER TABLE monitor_events_v33 RENAME TO monitor_events;
CREATE INDEX monitor_events_monitor_stage ON monitor_events (monitor_id, stage);
CREATE INDEX monitor_events_gc ON monitor_events(updated_at) WHERE stage IN ('delivered','authored_no_delivery','failed_no_retry','skipped','contract_unmet');
CREATE INDEX monitor_events_open ON monitor_events(fired_at) WHERE stage NOT IN ('delivered','authored_no_delivery','failed_no_retry','skipped','contract_unmet');
CREATE INDEX monitor_events_batch ON monitor_events(batch_id);
CREATE INDEX monitor_events_authored ON monitor_events(batch_id) WHERE stage = 'authored';
CREATE INDEX monitor_events_monitor ON monitor_events(monitor_id, fired_at);`);
			for (const action of ["UPDATE", "DELETE"])
				host.database.execute(`CREATE TRIGGER monitor_events_quarantine_${action.toLowerCase()} BEFORE ${action} ON monitor_events
				WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'monitor' AND subject_id = OLD.event_id)
				BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END`);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(33, new Date().toISOString());
		});
	}
	if (current < 34) {
		host.withTransaction(() => {
			host.database.execute(`CREATE TABLE IF NOT EXISTS cross_session_watches (
				watch_id TEXT PRIMARY KEY,
				watcher_origin_key TEXT NOT NULL,
				watcher_origin_ref_json TEXT NOT NULL,
				watched_origin_key TEXT NOT NULL,
				watched_origin_ref_json TEXT NOT NULL,
				handoff_chain_json TEXT NOT NULL,
				reason TEXT NOT NULL,
				expires_at TEXT NOT NULL,
				state TEXT NOT NULL CHECK(state IN ('pending','fired','expired')),
				registered_at TEXT NOT NULL,
				triggered_message_id TEXT,
				triggered_at TEXT,
				CHECK((state = 'fired' AND triggered_message_id IS NOT NULL AND triggered_at IS NOT NULL) OR (state <> 'fired' AND triggered_message_id IS NULL AND triggered_at IS NULL))
			);
			CREATE UNIQUE INDEX IF NOT EXISTS cross_session_watches_pending_pair ON cross_session_watches(watcher_origin_key, watched_origin_key) WHERE state = 'pending';
			CREATE INDEX IF NOT EXISTS cross_session_watches_pending_target ON cross_session_watches(watched_origin_key, expires_at) WHERE state = 'pending';
			CREATE INDEX IF NOT EXISTS cross_session_watches_retention ON cross_session_watches(state, expires_at, triggered_at);`);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(34, new Date().toISOString());
		});
	}
	if (current < 35) {
		// Issue #481: Add awaiting_checks without rewriting the already-deployed v33/v34 migrations.
		host.withTransaction(() => {
			host.database.execute(`
				CREATE TABLE lane_jobs_v35 (
					job_id TEXT PRIMARY KEY,
					lane_key TEXT NOT NULL UNIQUE,
					branch TEXT NOT NULL,
					worktree_path TEXT NOT NULL,
					state TEXT NOT NULL CHECK(state IN ('running','attempt_ended','awaiting_operator','awaiting_checks','stalled','done','aborted')),
					record_json TEXT NOT NULL,
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL
				);
				INSERT INTO lane_jobs_v35 SELECT * FROM lane_jobs;
				DROP TABLE lane_jobs;
				ALTER TABLE lane_jobs_v35 RENAME TO lane_jobs;
			`);
			host.database.execute(`
				CREATE TRIGGER lane_jobs_quarantine_update BEFORE UPDATE ON lane_jobs
				WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
				BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
				CREATE TRIGGER lane_jobs_quarantine_delete BEFORE DELETE ON lane_jobs
				WHEN EXISTS (SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = OLD.job_id)
				BEGIN SELECT RAISE(ABORT, 'broker authority: quarantined'); END;
			`);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(35, new Date().toISOString());
		});
	}
	if (current < 36) {
		// Issue #567: durable journal of work-attempt, lane-job and outbox row snapshots. Existing
		// rows are seeded as baseline events so replay from the journal alone covers pre-v36 state.
		// A database whose v36 record was removed restarts its journal from the current projections.
		host.withTransaction(() => {
			host.database.execute(
				`DROP TABLE IF EXISTS work_journal;
				CREATE TABLE work_journal (
					seq INTEGER PRIMARY KEY AUTOINCREMENT,
					entity TEXT NOT NULL CHECK(entity IN ('attempt','lane_job','delivery')),
					entity_key TEXT NOT NULL,
					event_type TEXT NOT NULL CHECK(event_type IN (
						'baseline',
						'attempt_started','attempt_accepted','attempt_terminal','attempt_updated','attempt_settled','attempt_reconciled',
						'lane_job_running','lane_job_attempt_ended','lane_job_awaiting_operator','lane_job_awaiting_checks','lane_job_stalled','lane_job_done','lane_job_aborted',
						'delivery_queued','delivery_inflight','delivery_confirmed','delivery_failed','delivery_expired','delivery_requeued','delivery_deleted'
					)),
					row_json TEXT CHECK(row_json IS NULL OR json_valid(row_json)),
					recorded_at TEXT NOT NULL
				);
				CREATE INDEX work_journal_entity_key ON work_journal(entity, entity_key, seq);
				CREATE INDEX work_journal_recorded_at ON work_journal(recorded_at);`,
			);
			const seededAt = new Date().toISOString();
			for (const entity of ["lane_job", "attempt", "delivery"] as const satisfies readonly JournalEntity[]) {
				const { table, key } = JOURNAL_PROJECTIONS[entity];
				host.database
					.statement(
						`INSERT INTO work_journal (entity, entity_key, event_type, row_json, recorded_at) SELECT ?, ${key}, 'baseline', ${journalSnapshotSql(entity)}, ? FROM ${table} ORDER BY ${key}`,
					)
					.run(entity, seededAt);
			}
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(36, new Date().toISOString());
		});
	}
	if (current < 37) {
		// Issue #568: broker mutation journal. A row is written as `pending` in its
		// own transaction BEFORE the broker call and resolved after it, so a crash
		// between the two leaves an intent that the retry replays under the same
		// broker-side reference instead of minting a second operation.
		host.withTransaction(() => {
			host.database.execute(`
				CREATE TABLE IF NOT EXISTS broker_operations (
					authority_key TEXT NOT NULL,
					operation TEXT NOT NULL CHECK(operation IN ('session.create','turn.prompt','session.close')),
					scope TEXT NOT NULL,
					operation_ref TEXT NOT NULL,
					request_json TEXT NOT NULL,
					state TEXT NOT NULL CHECK(state IN ('pending','complete')),
					result_json TEXT,
					created_at TEXT NOT NULL,
					completed_at TEXT,
					PRIMARY KEY(authority_key, operation, scope),
					CHECK((state = 'complete' AND result_json IS NOT NULL AND completed_at IS NOT NULL) OR (state = 'pending' AND result_json IS NULL AND completed_at IS NULL))
				);
				CREATE INDEX IF NOT EXISTS broker_operations_completed ON broker_operations(state, completed_at);
			`);
		host.database
			.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
			.run(37, new Date().toISOString());
	});
	}
	if (current < 38) {
		// Issue #625: thread muting. When a bot replies with [SHUTUP], the thread
		// is muted, suppressing LLM turns for unmentioned follow-ups until the next
		// mention or reply to the bot. Persisted across restarts.
		host.withTransaction(() => {
			host.database.execute(`
				CREATE TABLE IF NOT EXISTS thread_mutes (
					thread_origin_key TEXT PRIMARY KEY,
					muted_at TEXT NOT NULL,
					CHECK(thread_origin_key LIKE 'slack/thread/%' OR thread_origin_key LIKE 'discord/thread/%')
				);
			`);
			host.database
				.statement("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
				.run(38, new Date().toISOString());
		});
	}
}
