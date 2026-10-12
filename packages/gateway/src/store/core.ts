import { Database } from "bun:sqlite";
import { lstatSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import type { OwnedTable, OwnerGroup } from "./ownership";
import { TABLE_OWNER } from "./ownership";
import { collectOpenWork as collectOpenWorkReport, type OpenWorkRows, type OpenWorkScope } from "./open-work";
import { BindingsGroup } from "./groups/bindings";
import { DatabaseStartupError, BrokerAuthorityError } from "./errors";
import { LATEST_SCHEMA_VERSION, runMigrations, type MigrationStatement } from "./migrations";
import { classifySql } from "./sql-classify";
import {
	mintWriteTx,
	StoreOwnershipError,
	StoreTransactionError,
	writeTxStore,
	type WriteTx,
} from "./tx";

import { WorkAttemptStateError, WorkGroup } from "./groups/work";

import { parseLaneJobRecord } from "@gajae-gateway/subsession";

type RawStatement = ReturnType<Database["query"]>;

export interface StoreOpenOptions {
	readonly canonicalAgentDir?: string;
	readonly backupsDir?: string;
}

export interface GroupStatement extends Omit<RawStatement, "all" | "get" | "run"> {
	all(...params: Parameters<RawStatement["all"]>): ReturnType<RawStatement["all"]>;
	allWithWrite(tx: WriteTx, ...params: Parameters<RawStatement["all"]>): ReturnType<RawStatement["all"]>;
	get(...params: Parameters<RawStatement["get"]>): ReturnType<RawStatement["get"]>;
	getWithWrite(tx: WriteTx, ...params: Parameters<RawStatement["get"]>): ReturnType<RawStatement["get"]>;
	run(tx: WriteTx, ...params: Parameters<RawStatement["run"]>): ReturnType<RawStatement["run"]>;
}

export interface GroupDb {
	prepare(sql: string): GroupStatement;
	query(sql: string): GroupStatement;
}

const AUTHORITY_CENSUS_TABLES = [
	"sessions",
	"deliveries",
	"recall_snippets",
	"meta",
	"memory_intents",
	"monitors",
	"monitor_events",
	"authored_outputs",
	"inbound_messages",
	"conversation_context",
	"lane_jobs",
	"monitor_failures",
	"monitor_slots",
	"dispatch_leases",
	"conversation_context_state",
	"conversation_model",
	"session_tail_cursors",
	"work_attempt_runtime",
	"lane_reports",
	"broker_owned_bindings",
	"broker_tail_cursors",
	"broker_retired_sessions",
	"broker_quarantine",
	"broker_cutovers",
] as const;

export class StoreCore {
	readonly #database: Database;
	readonly #backupsDir: string | undefined;
	readonly #work: WorkGroup;
	#serial = 0;
	#active: WriteTx | undefined;


	private constructor(database: Database, backupsDir?: string) {
		this.#database = database;
		this.#backupsDir = backupsDir;
		this.#work = new WorkGroup(this);
	}


	static async open(path: string, options: StoreOpenOptions = {}): Promise<StoreCore> {
		await mkdir(dirname(path), { recursive: true, mode: 0o700 });
		const database = new Database(path);
		const core = new StoreCore(database, options.backupsDir);
		try {
			database.exec(
				"PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA synchronous = NORMAL; PRAGMA journal_size_limit = 67108864;",
			);
			database.exec(
				"CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); CREATE TABLE IF NOT EXISTS broker_authority (singleton INTEGER PRIMARY KEY CHECK(singleton = 1), authority_key TEXT NOT NULL);",
			);
			if (options.canonicalAgentDir !== undefined) core.assertInitialAuthority(options.canonicalAgentDir);
			core.migrate();
			core.integrityCheck();
			return core;
		} catch (error) {
			core.close();
			throw error;
		}
	}

	static async openReadonly(path: string): Promise<StoreCore> {
		return new StoreCore(new Database(path, { readonly: true }));
	}

	static async openAdministrative(path: string): Promise<StoreCore> {
		const info = lstatSync(path);
		if (!info.isFile() || info.isSymbolicLink())
			throw new DatabaseStartupError("schema_mismatch", `database path is not a regular file: ${path}`);
		const database = new Database(path);
		const core = new StoreCore(database);
		try {
			database.exec("PRAGMA busy_timeout = 5000; PRAGMA synchronous = NORMAL; PRAGMA journal_size_limit = 67108864;");
			if (core.schemaVersion !== LATEST_SCHEMA_VERSION)
				throw new DatabaseStartupError(
					"schema_mismatch",
					`database schema ${core.schemaVersion} does not match ${LATEST_SCHEMA_VERSION}`,
				);
			const detail = core.quickCheckDetail();
			if (detail !== "ok") throw new DatabaseStartupError("quick_check_failed", `SQLite quick_check failed: ${detail}`);
			return core;
		} catch (error) {
			core.close();
			throw error;
		}
	}

	get schemaVersion(): number {
		try {
			return (
				this.#database
					.query<{ version: number }, []>("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations")
					.get()?.version ?? 0
			);
		} catch (error) {
			if ((error as Error).message.includes("no such table: schema_migrations")) return 0;
			throw error;
		}
	}

	get instanceId(): string {
		const row = this.#database.query<{ value: string }, [string]>("SELECT value FROM meta WHERE key = ?").get("instance_id");
		if (!row) throw new DatabaseStartupError("integrity_check_failed", "meta.instance_id missing after migration");
		return row.value;
	}


	transaction<T>(operation: (tx: WriteTx) => T): T {
		if (this.#active !== undefined) throw new StoreTransactionError("nested");
		if (operation.constructor.name === "AsyncFunction") throw new StoreTransactionError("async_callback");

		this.#database.exec("BEGIN IMMEDIATE");
		let began = true;
		const tx = mintWriteTx(this, ++this.#serial);
		this.#active = tx;
		try {
			const result = operation(tx);
			if (isThenable(result)) {
				this.#database.exec("ROLLBACK");
				began = false;
				throw new StoreTransactionError("async_callback");
			}
			this.#database.exec("COMMIT");
			began = false;
			return result;
		} catch (error) {
			if (began) {
				try {
					this.#database.exec("ROLLBACK");
				} catch {
					// Preserve the callback or commit error; the active token is cleared below.
				}
			}
			throw error;
		} finally {
			this.#active = undefined;
		}
	}

	assertActive(tx: WriteTx): void {
		const store = writeTxStore(tx);
		if (store === undefined) throw new StoreTransactionError("inactive_tx");
		if (store !== this) throw new StoreTransactionError("foreign_store");
		if (this.#active !== tx) throw new StoreTransactionError("inactive_tx");
	}

	groupDb(group: OwnerGroup): GroupDb {
		const prepare = (sql: string): GroupStatement => {
			const classification = classifySql(sql);
			if (!classification) throw new StoreOwnershipError({ reason: "unclassified" });
			if (classification.kind === "write" && TABLE_OWNER[classification.table as keyof typeof TABLE_OWNER] !== group)
				throw new StoreOwnershipError({ reason: "not_owner", group, table: classification.table });
			const statement = this.#database.query(sql);
			const assertWritable = () => {
				if (classification.kind === "write") throw new StoreTransactionError("inactive_tx");
			};
			const runWithTx = (tx: WriteTx, params: Parameters<RawStatement["run"]>) => {
				this.assertActive(tx);
				return statement.run(...params);
			};
			return {
				all: (...params) => {
					assertWritable();
					return statement.all(...params);
				},
				allWithWrite: (tx, ...params) => {
					this.assertActive(tx);
					return statement.all(...params);
				},
				get: (...params) => {
					assertWritable();
					return statement.get(...params);
				},
				getWithWrite: (tx, ...params) => {
					this.assertActive(tx);
					return statement.get(...params);
				},
				run: (tx, ...params) => runWithTx(tx, params),
			} as GroupStatement;
		};
		return Object.freeze({ prepare, query: prepare });
	}

	metaGet(key: string): string | undefined {
		return this.#database.query<{ value: string }, [string]>("SELECT value FROM meta WHERE key = ?").get(key)?.value;
	}

	metaKeysInRange(start: string, end: string): string[] {
		return this.#database
			.query<{ key: string }, [string, string]>("SELECT key FROM meta WHERE key >= ? AND key < ?")
			.all(start, end)
			.map((row) => row.key);
	}

	metaDeleteRange(tx: WriteTx, start: string, end: string): void {
		this.assertActive(tx);
		this.#database.query("DELETE FROM meta WHERE key >= ? AND key < ?").run(start, end);
	}

	metaSet(key: string, value: string): void {
		this.#database
			.query("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
			.run(key, value);
	}

	metaDelete(key: string): void {
		this.#database.query("DELETE FROM meta WHERE key = ?").run(key);
	}

	writeBackup(path: string): void {
		this.#database.query("VACUUM INTO ?").run(path);
	}

	integrityCheckDetail(): string {
		return this.#database.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()?.integrity_check ?? "unknown";
	}

	integrityCheck(): void {
		const detail = this.integrityCheckDetail();
		if (detail !== "ok") throw new DatabaseStartupError("integrity_check_failed", `SQLite integrity_check failed: ${detail}`);
	}

	quickCheckDetail(): string {
		const rows = this.#database.query<{ quick_check: string }, []>("PRAGMA quick_check").all();
		return rows.length === 0 ? "unknown" : rows.map((row) => row.quick_check).join("; ");
	}

	tableHasRows(table: OwnedTable): boolean {
		const exists = this.#database
			.query<{ name: string }, [string]>("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
			.get(table);
		if (!exists) return false;
		const where = table === "meta" ? " WHERE key <> 'instance_id'" : "";
		return this.#database.query(`SELECT 1 FROM ${table}${where} LIMIT 1`).get() !== null;
	}

	tableRowCounts(): Readonly<Record<string, number>> {
		const tables = this.#database
			.query<{ name: string }, []>("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
			.all();
		const counts: Record<string, number> = {};
		for (const { name } of tables) {
			const identifier = `"${name.replaceAll('"', '""')}"`;
			counts[name] = this.#database.query<{ count: number }, []>(`SELECT COUNT(*) AS count FROM ${identifier}`).get()?.count ?? 0;
		}
		return counts;
	}

	maintain(): void {
		this.#database.exec("PRAGMA wal_checkpoint(PASSIVE); PRAGMA optimize;");
	}



	collectOpenWork(scope: OpenWorkScope) {
		return collectOpenWorkReport(
			{
				readOpenWorkRows: (requestedScope) => this.readOpenWorkRows(requestedScope),
				workAttemptGet: (opRef) => this.#work.workAttemptGet(opRef),
			},
			{ scope },
		);
	}

	readOpenWorkRows(scope: OpenWorkScope): OpenWorkRows {
		const quarantine = (kind: string, table: string, id: string): string => {
			if (scope === "all") return "1 = 1";
			const exists = scope === "quarantined" ? "EXISTS" : "NOT EXISTS";
			return `${exists} (SELECT 1 FROM broker_quarantine q WHERE q.kind = '${kind}' AND q.subject_id = ${table}.${id})`;
		};
		const read = <T>(sql: string): T[] => {
			try {
				return this.#database.query<T, []>(sql).all();
			} catch (error) {
				if ((error as Error).message.includes("no such table:")) return [];
				throw error;
			}
		};
		const work = this.#work.readOpenWorkRows(scope);
		return {
			inbound: read<OpenWorkRows["inbound"][number]>(
				`SELECT message_id, origin_key, origin_ref_json, body, source, state, turn_role, turn_state, turn_op_ref, bound_session_id, received_at, (SELECT cutover_id FROM broker_quarantine q WHERE q.kind = 'inbound' AND q.subject_id = inbound_messages.message_id) AS cutover_id FROM inbound_messages WHERE (state <> 'done' OR turn_state IN ('bound','accepted')) AND ${quarantine("inbound", "inbound_messages", "message_id")} ORDER BY received_at, message_id`,
			),
			...work,
			monitors: read<OpenWorkRows["monitors"][number]>(
				`SELECT monitor_events.event_id, monitor_events.monitor_id, monitors.name AS monitor_name, monitor_events.event_type, monitor_events.stage, monitor_events.batch_id, monitor_events.fired_at, (SELECT cutover_id FROM broker_quarantine q WHERE q.kind = 'monitor' AND q.subject_id = monitor_events.event_id) AS cutover_id FROM monitor_events LEFT JOIN monitors ON monitors.monitor_id = monitor_events.monitor_id WHERE monitor_events.stage NOT IN ('delivered','authored_no_delivery','failed_no_retry','skipped') AND ${quarantine("monitor", "monitor_events", "event_id")} ORDER BY monitor_events.fired_at, monitor_events.event_id`,
			),
		};
	}


	assertNotQuarantined(kind: string, id: string): void {
		if (this.#database.query("SELECT 1 FROM broker_quarantine WHERE kind = ? AND subject_id = ?").get(kind, id))
			throw new BrokerAuthorityError("quarantined");
	}

	close(): void {
		this.#database.close();
	}

	migrate(): void {
		const core = this;
		runMigrations({
			database: {
				statement: <T = unknown>(sql: string) =>
					this.#database.query(sql) as unknown as MigrationStatement<T>,
				execute: (sql) => this.#database.exec(sql),
			},
			get schemaVersion() {
				return core.schemaVersion;
			},
			get instanceId() {
				return core.instanceId;
			},
			withTransaction: (operation) => this.transaction(() => operation()),
			assertNotQuarantined: (kind, id) => this.assertNotQuarantined(kind, id),
		});
	}

	private authorityCensusPopulated(): boolean {
		let populated = false;
		for (const table of AUTHORITY_CENSUS_TABLES) {
			const exists = this.#database
				.query<{ name: string }, [string]>("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?")
				.get(table);
			if (!exists) continue;
			const where = table === "meta" ? " WHERE key <> 'instance_id'" : "";
			if (this.#database.query(`SELECT 1 FROM ${table}${where} LIMIT 1`).get()) populated = true;
		}
		try {
			for (const row of this.#database.query<{ epoch: number }, []>("SELECT epoch FROM sessions").all()) {
				if (!Number.isSafeInteger(row.epoch) || row.epoch < 0 || row.epoch >= Number.MAX_SAFE_INTEGER)
					throw new Error("invalid session epoch");
			}
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		try {
			for (const row of this.#work.laneJobHistoryRows()) {
				const job = parseLaneJobRecord(row.record_json);
				if (job.jobId !== row.job_id) throw new WorkAttemptStateError();
				this.#database.query("SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = ?").get(job.jobId);
			}
			for (const row of this.#work.workAttemptRows()) {
				const runtime = this.#work.workAttemptGet(row.op_ref)!;
				if (runtime.settledAt === null)
					this.#database.query("SELECT 1 FROM broker_quarantine WHERE kind = 'work' AND subject_id = ?").get(runtime.jobId);
			}
			this.#work.laneReportOpenCount();
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		try {
			this.#database
				.query<{ n: number }, []>(
					"SELECT COUNT(*) AS n FROM inbound_messages WHERE (state <> 'done' OR turn_state IN ('bound','accepted')) AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'inbound' AND q.subject_id = inbound_messages.message_id)",
				)
				.get();
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		try {
			this.#database
				.query<{ n: number }, []>(
					"SELECT COUNT(*) AS n FROM monitor_events WHERE stage NOT IN ('delivered','authored_no_delivery','failed_no_retry','skipped') AND NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'monitor' AND q.subject_id = monitor_events.event_id)",
				)
				.get();
		} catch (error) {
			if (!(error as Error).message.includes("no such table")) throw error;
		}
		return populated;
	}

	private assertInitialAuthority(canonicalAgentDir: string): void {
		const bindings = new BindingsGroup(this);
		this.transaction((tx) => bindings.assertInitialAuthority(tx, canonicalAgentDir, () => this.authorityCensusPopulated()));
	}
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
	return (
		(value !== null && (typeof value === "object" || typeof value === "function")) &&
		typeof (value as { then?: unknown }).then === "function"
	);
}

// Capability-shaped helper used by typecheck fixtures and future owner commands.
export function runGroupWrite(group: GroupDb, tx: WriteTx, sql: string): ReturnType<RawStatement["run"]> {
	return group.prepare(sql).run(tx);
}
