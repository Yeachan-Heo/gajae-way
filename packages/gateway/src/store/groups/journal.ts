import type { LaneJobState } from "@gajae-gateway/subsession";
import type { GroupDb, StoreCore } from "../core";
import type { WriteTx } from "../tx";
import type { DeliveryDbRow } from "./outbox";
import type { LaneJobDbRow, WorkAttemptDbRow } from "./work";

/**
 * Durable append-only journal for the work-attempt, lane-job and outbox projections.
 *
 * Every projection write appends, in the same transaction, the full post-write row of the
 * entity it touched (or a tombstone when the row is gone). Replaying the latest event per
 * entity therefore reproduces the projection exactly, without consulting the projection.
 */

export type JournalEntity = "attempt" | "lane_job" | "delivery";

export type WorkJournalEventType =
	| "baseline"
	| "attempt_started"
	| "attempt_accepted"
	| "attempt_terminal"
	| "attempt_updated"
	| "attempt_settled"
	| `lane_job_${LaneJobState}`
	| "delivery_queued"
	| "delivery_inflight"
	| "delivery_confirmed"
	| "delivery_failed"
	| "delivery_expired"
	| "delivery_requeued"
	| "delivery_deleted";

export interface JournalRows {
	readonly attempt: WorkAttemptDbRow;
	readonly lane_job: LaneJobDbRow;
	readonly delivery: DeliveryDbRow;
}

/** Projection table, key column and snapshot columns per entity; shared with the v36 baseline seed. */
export const JOURNAL_PROJECTIONS: {
	readonly [E in JournalEntity]: {
		readonly table: string;
		readonly key: keyof JournalRows[E] & string;
		readonly columns: readonly (keyof JournalRows[E] & string)[];
	};
} = {
	attempt: {
		table: "work_attempt_runtime",
		key: "op_ref",
		columns: ["op_ref", "job_id", "lane_key", "session_id", "version", "settled_at", "delivery_id", "record_json"],
	},
	lane_job: {
		table: "lane_jobs",
		key: "job_id",
		columns: ["job_id", "lane_key", "branch", "worktree_path", "state", "record_json", "created_at", "updated_at"],
	},
	delivery: {
		table: "deliveries",
		key: "delivery_id",
		columns: [
			"delivery_id",
			"turn_id",
			"origin_key",
			"payload_json",
			"state",
			"attempts",
			"created_at",
			"updated_at",
			"last_error",
		],
	},
};

/** SQLite expression producing the journal snapshot of one projection row. */
export function journalSnapshotSql(entity: JournalEntity): string {
	return `json_object(${JOURNAL_PROJECTIONS[entity].columns.map((column) => `'${column}', ${column}`).join(", ")})`;
}

const RECORD_SQL: Readonly<Record<JournalEntity, string>> = Object.fromEntries(
	(Object.keys(JOURNAL_PROJECTIONS) as JournalEntity[]).map((entity) => {
		const { table, key } = JOURNAL_PROJECTIONS[entity];
		return [
			entity,
			`INSERT INTO work_journal (entity, entity_key, event_type, row_json, recorded_at) SELECT ?, ?, ?, (SELECT ${journalSnapshotSql(entity)} FROM ${table} WHERE ${key} = ?), ?`,
		];
	}),
) as Record<JournalEntity, string>;

export class JournalGroup {
	readonly #db: GroupDb;

	constructor(core: StoreCore) {
		this.#db = core.groupDb("journal");
	}

	/** Journal the current row of `key` (a tombstone when absent); call after the projection write in the same tx. */
	record(tx: WriteTx, entity: JournalEntity, key: string, eventType: WorkJournalEventType): void {
		this.#db.prepare(RECORD_SQL[entity]).run(tx, entity, key, eventType, key, new Date().toISOString());
	}

	/** Latest journaled row per entity key; `null` means the row was deleted. */
	latest<E extends JournalEntity>(entity: E): Map<string, JournalRows[E] | null> {
		const rows = this.#db
			.prepare(
				"SELECT entity_key, row_json FROM work_journal AS j WHERE entity = ? AND seq = (SELECT MAX(seq) FROM work_journal WHERE entity = j.entity AND entity_key = j.entity_key) ORDER BY entity_key",
			)
			.all(entity) as Array<{ entity_key: string; row_json: string | null }>;
		return new Map(
			rows.map((row) => [row.entity_key, row.row_json === null ? null : (JSON.parse(row.row_json) as JournalRows[E])]),
		);
	}

	/**
	 * Compact history older than the cutoff without changing what replay produces: superseded
	 * snapshots go first, and a tombstone goes only once it is the sole event left for its key.
	 */
	sweep(tx: WriteTx, cutoffs: { readonly historyCutoff: string; readonly batch: number }): number {
		return this.#db
			.prepare(
				`DELETE FROM work_journal WHERE seq IN (
					SELECT seq FROM work_journal AS j WHERE recorded_at < ? AND (
						EXISTS (SELECT 1 FROM work_journal AS later WHERE later.entity = j.entity AND later.entity_key = j.entity_key AND later.seq > j.seq)
						OR (row_json IS NULL AND NOT EXISTS (SELECT 1 FROM work_journal AS earlier WHERE earlier.entity = j.entity AND earlier.entity_key = j.entity_key AND earlier.seq < j.seq))
					) ORDER BY seq LIMIT ?
				)`,
			)
			.run(tx, cutoffs.historyCutoff, cutoffs.batch).changes;
	}

	/**
	 * Aggressively compact superseded attempt_updated rows older than the cutoff, keeping the
	 * newest row per key always and terminal/transition events on the standard 30-day window.
	 * This deletes non-latest attempt_updated rows to save space without affecting what replay
	 * needs (only the latest snapshot per entity key).
	 *
	 * Readers:
	 * - latest() only returns the latest row per key, so non-latest rows are safe to delete
	 * - replayProjection() uses latest(), so it's unaffected
	 * - Recovery/audit flows use latest(), so they're unaffected
	 */
	sweepAttemptUpdated(tx: WriteTx, cutoffs: { readonly updateCutoff: string; readonly batch: number }): number {
		return this.#db
			.prepare(
				`DELETE FROM work_journal WHERE seq IN (
					SELECT seq FROM work_journal AS j
					WHERE entity = 'attempt' AND event_type = 'attempt_updated'
						AND recorded_at < ?
						AND EXISTS (
							SELECT 1 FROM work_journal AS later
							WHERE later.entity = j.entity AND later.entity_key = j.entity_key AND later.seq > j.seq
						)
			ORDER BY seq LIMIT ?
			)`,
			)
			.run(tx, cutoffs.updateCutoff, cutoffs.batch).changes;
	}

	/** Measure work_journal row count and on-disk footprint. Returns 0 for pre-v36 databases without work_journal table. */
	measure(): { rowCount: number; byteSize: number } {
		try {
			const stats = this.#db.prepare("SELECT COUNT(*) as cnt FROM work_journal").get() as { cnt: number };
			const rowCount = stats.cnt;

			// Measure the on-disk footprint of work_journal (table + its indexes) with the dbstat
			// virtual table. Only SQLite builds without SQLITE_ENABLE_DBSTAT_VTAB fall back to an
			// estimate (row_json bytes + ~80 bytes of per-row column overhead).
			let byteSize: number;
			try {
				const dbstatResult = this.#db
					.prepare(
						"SELECT COALESCE(SUM(pgsize), 0) AS size FROM dbstat WHERE name IN (SELECT name FROM sqlite_schema WHERE tbl_name = 'work_journal')",
					)
					.get() as { size: number };
				byteSize = dbstatResult.size;
			} catch {
				const sizeStats = this.#db
					.prepare("SELECT COALESCE(SUM(LENGTH(CAST(row_json AS BLOB))), 0) AS json_size FROM work_journal")
					.get() as { json_size: number };
				byteSize = sizeStats.json_size + rowCount * 80;
			}

			return { rowCount, byteSize };
		} catch (error) {
			// Pre-v36 databases don't have work_journal table; report 0 for both metrics
			if ((error as Error).message.includes("no such table")) {
				return { rowCount: 0, byteSize: 0 };
			}
			throw error;
		}
	}
}

/**
 * Keys whose projection row differs column-for-column from the journaled snapshot, including
 * rows the journal never saw and journaled rows missing from the projection.
 */
export function staleJournalKeys<E extends JournalEntity>(
	entity: E,
	rows: readonly JournalRows[E][],
	journaled: ReadonlyMap<string, JournalRows[E] | null>,
): string[] {
	const { key, columns } = JOURNAL_PROJECTIONS[entity];
	const current = new Map(rows.map((row) => [String(row[key]), row]));
	const stale = new Set<string>();
	for (const [rowKey, row] of current) {
		const snapshot = journaled.get(rowKey);
		if (!snapshot || columns.some((column) => row[column] !== snapshot[column])) stale.add(rowKey);
	}
	for (const [rowKey, snapshot] of journaled) if (snapshot && !current.has(rowKey)) stale.add(rowKey);
	return [...stale].sort();
}
