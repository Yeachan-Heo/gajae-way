import type { DeliveryErrorCode } from "@gajae-gateway/protocol";
import type { GroupDb, StoreCore } from "../core";
import type { WriteTx } from "../tx";

export type DeliveryState = "pending" | "inflight" | "confirmed" | "failed_ambiguous" | "expired";
export type LedgerOutcome = "unknown" | "transitioned" | "already_terminal";
const MAX_DEFINITIVE_FAILURES = 5;
const RETRY_BACKOFF_BASE_MS = 2_000;
const RETRY_BACKOFF_CAP_MS = 5 * 60_000;
export const ACK_TIMEOUT_MS = 60 * 1_000;

const DELIVERY_ERROR_PATTERNS: readonly (readonly [DeliveryErrorCode, RegExp])[] = [
	["rate_limited", /rate.?limit|\b429\b|too many requests/i],
	["timeout", /time.?out|deadline/i],
	["network", /network|socket|econn|enotfound|eai_again|hang up|fetch failed|connection/i],
	["not_found", /unknown (message|channel|guild|user)|not.?found|\b404\b/i],
	["forbidden", /forbidden|missing (access|permissions)|not.?allowed|disabled|blocked|cannot|\b403\b/i],
	["invalid_request", /invalid|bad request|malformed|\b400\b/i],
];

export function classifyDeliveryError(reason: string | undefined): DeliveryErrorCode {
	if (reason) for (const [code, pattern] of DELIVERY_ERROR_PATTERNS) if (pattern.test(reason)) return code;
	return "other";
}

export interface DeliveryDbRow {
	readonly delivery_id: string;
	readonly turn_id: string;
	readonly origin_key: string;
	readonly payload_json: string;
	readonly state: string;
	readonly attempts: number;
	readonly created_at: string;
	readonly updated_at: string;
	readonly last_error: string | null;
}

export interface DeliveryRow {
	readonly deliveryId: string;
	readonly turnId: string;
	readonly originKey: string;
	readonly payloadJson: string;
	readonly state: DeliveryState;
	readonly attempts: number;
	readonly createdAt: string;
	readonly updatedAt: string;
	readonly lastError: DeliveryErrorCode | null;
	readonly nextRetryAt: string | null;
}

export interface ExpiredDeliveryRow {
	readonly deliveryId: string;
	readonly originKey: string;
	readonly attempts: number;
	readonly expiredAt: string;
	readonly lastError: DeliveryErrorCode | null;
}

export interface UnsettledDeliveryRow {
	readonly deliveryId: string;
	readonly originKey: string;
	readonly state: Exclude<DeliveryState, "confirmed" | "expired">;
	readonly attempts: number;
	readonly lastError: DeliveryErrorCode | null;
	readonly nextRetryAt: string | null;
	readonly createdAt: string;
}

const DELIVERY_COLUMNS =
	"delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at, last_error";

/** Persistence and retry policy for the durable delivery outbox. */
export class OutboxGroup {
	readonly #db: GroupDb;

	constructor(core: StoreCore) {
		this.#db = core.groupDb("outbox");
	}

	/** Strict deduplicating enqueue used by composites: a reused id must name the same obligation. */
	enqueue(
		tx: WriteTx,
		row: {
			readonly deliveryId: string;
			readonly turnId: string;
			readonly originKey: string;
			readonly payloadJson: string;
		},
		now = Date.now(),
	): boolean {
		const existing = this.#db
			.prepare("SELECT turn_id, origin_key, payload_json FROM deliveries WHERE delivery_id = ?")
			.get(row.deliveryId) as { turn_id: string; origin_key: string; payload_json: string } | null;
		if (existing) {
			if (
				existing.turn_id !== row.turnId ||
				existing.origin_key !== row.originKey ||
				existing.payload_json !== row.payloadJson
			)
				throw new Error("delivery id conflicts with a different obligation");
			return false;
		}
		const at = new Date(now).toISOString();
		this.#db
			.prepare(
				"INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
			)
			.run(tx, row.deliveryId, row.turnId, row.originKey, row.payloadJson, at, at);
		return true;
	}

	/** Ordinary delivery creation is idempotent; a duplicate id is left unchanged. */
	create(
		tx: WriteTx,
		row: {
			readonly deliveryId: string;
			readonly turnId: string;
			readonly originKey: string;
			readonly payloadJson: string;
		},
		now = Date.now(),
	): boolean {
		const at = new Date(now).toISOString();
		return (
			this.#db
				.prepare(
					"INSERT OR IGNORE INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
				)
				.run(tx, row.deliveryId, row.turnId, row.originKey, row.payloadJson, at, at).changes === 1
		);
	}

	markInflight(tx: WriteTx, deliveryId: string): void {
		const row = this.get(deliveryId);
		if (!row || row.state === "confirmed" || row.state === "expired") return;
		this.#update(tx, deliveryId, "inflight", row.attempts);
	}

	/** Terminal states are monotonic: late duplicate confirms/fails cannot rewrite them. */
	confirm(tx: WriteTx, deliveryId: string): LedgerOutcome {
		const row = this.get(deliveryId);
		if (!row) return "unknown";
		if (row.state === "confirmed" || row.state === "expired") return "already_terminal";
		this.#update(tx, deliveryId, "confirmed");
		return "transitioned";
	}

	fail(tx: WriteTx, deliveryId: string, ambiguous = false, reason?: string): LedgerOutcome {
		const row = this.get(deliveryId);
		if (!row) return "unknown";
		if (row.state === "confirmed" || row.state === "expired") return "already_terminal";
		const attempts = row.attempts + 1;
		const state: DeliveryState = ambiguous
			? "failed_ambiguous"
			: attempts >= MAX_DEFINITIVE_FAILURES
				? "expired"
				: "pending";
		this.#update(tx, deliveryId, state, attempts, classifyDeliveryError(reason));
		return "transitioned";
	}

	list(filters: { readonly since?: string; readonly state?: string; readonly limit?: number } = {}): DeliveryRow[] {
		const limit = filters.limit ?? 50;
		if (!Number.isSafeInteger(limit) || limit < 1 || limit > 500)
			throw new RangeError("delivery list limit must be between 1 and 500");
		const clauses: string[] = [];
		const params: (string | number)[] = [];
		if (filters.since !== undefined) {
			clauses.push("created_at >= ?");
			params.push(filters.since);
		}
		if (filters.state !== undefined) {
			clauses.push("state = ?");
			params.push(filters.state);
		}
		params.push(limit);
		const where = clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
		const rows = this.#db
			.prepare(`SELECT ${DELIVERY_COLUMNS} FROM deliveries${where} ORDER BY created_at DESC, delivery_id DESC LIMIT ?`)
			.all(...params) as DeliveryDbRow[];
		return rows.map((row) => this.#toRow(row));
	}

	/** Fresh unsettled rows, with the retry backoff and adapter-ack timeout applied. */
	listUndelivered(freshnessMs: number, now = Date.now(), ignoreBackoff = false): DeliveryRow[] {
		return this.unsettledRows().filter(
			(row) =>
				now - Date.parse(row.createdAt) <= freshnessMs &&
				(ignoreBackoff || row.attempts === 0 || now - Date.parse(row.updatedAt) >= retryBackoffMs(row.attempts)) &&
				(row.state !== "inflight" || ignoreBackoff || now - Date.parse(row.updatedAt) >= ACK_TIMEOUT_MS),
		);
	}

	expireStale(tx: WriteTx, freshnessMs: number, now = Date.now()): ExpiredDeliveryRow[] {
		const before = new Date(now - freshnessMs).toISOString();
		const expiredAt = new Date(now).toISOString();
		const rows = this.#db
			.prepare(
				"SELECT delivery_id, origin_key, attempts, last_error FROM deliveries WHERE state NOT IN ('confirmed', 'expired') AND created_at < ?",
			)
			.all(before) as Array<{ delivery_id: string; origin_key: string; attempts: number; last_error: string | null }>;
		if (rows.length === 0) return [];
		this.#db
			.prepare(
				"UPDATE deliveries SET state = 'expired', updated_at = ? WHERE state NOT IN ('confirmed', 'expired') AND created_at < ?",
			)
			.run(tx, expiredAt, before);
		return rows.map((row) => ({
			deliveryId: row.delivery_id,
			originKey: row.origin_key,
			attempts: row.attempts,
			expiredAt,
			lastError: row.last_error as DeliveryErrorCode | null,
		}));
	}

	requeue(tx: WriteTx, deliveryId: string): string[] {
		const changed = this.#db
			.prepare(
				"UPDATE deliveries SET state = 'pending', attempts = 0, updated_at = ? WHERE delivery_id = ? AND state IN ('expired', 'failed_ambiguous', 'pending')",
			)
			.run(tx, new Date().toISOString(), deliveryId).changes;
		return changed === 1 ? [deliveryId] : [];
	}

	requeueSince(tx: WriteTx, since: string): string[] {
		const rows = this.#db
			.prepare(
				"SELECT delivery_id FROM deliveries WHERE state IN ('expired', 'failed_ambiguous', 'pending') AND updated_at >= ? ORDER BY updated_at, delivery_id",
			)
			.all(since) as Array<{ delivery_id: string }>;
		if (rows.length === 0) return [];
		this.#db
			.prepare(
				"UPDATE deliveries SET state = 'pending', attempts = 0, updated_at = ? WHERE state IN ('expired', 'failed_ambiguous', 'pending') AND updated_at >= ?",
			)
			.run(tx, new Date().toISOString(), since);
		return rows.map((row) => row.delivery_id);
	}

	getMany(deliveryIds: readonly string[]): DeliveryRow[] {
		return [...new Set(deliveryIds)].flatMap((id) => {
			const row = this.get(id);
			return row ? [row] : [];
		});
	}

	prune(tx: WriteTx, deliveredOlderThanMs: number, now = Date.now()): number {
		return this.#db
			.prepare("DELETE FROM deliveries WHERE state = 'confirmed' AND updated_at < ?")
			.run(tx, new Date(now - deliveredOlderThanMs).toISOString()).changes;
	}

	counts(now = Date.now()): {
		pending: number;
		oldestPendingAgeMs: number | null;
		expired: number;
		recentExpired: readonly ExpiredDeliveryRow[];
		recentPending: readonly UnsettledDeliveryRow[];
	} {
		const open = this.#db
			.prepare(
				"SELECT COUNT(*) AS n, MIN(created_at) AS oldest FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous')",
			)
			.get() as { n: number; oldest: string | null };
		const expiredCount = (
			this.#db.prepare("SELECT COUNT(*) AS n FROM deliveries WHERE state = 'expired'").get() as { n: number }
		).n;
		const expiredRows = this.#db
			.prepare(
				"SELECT delivery_id, origin_key, attempts, updated_at, last_error FROM deliveries WHERE state = 'expired' ORDER BY updated_at DESC, delivery_id LIMIT 5",
			)
			.all() as Array<{
			delivery_id: string;
			origin_key: string;
			attempts: number;
			updated_at: string;
			last_error: string | null;
		}>;
		const pendingRows = this.#db
			.prepare(
				`SELECT ${DELIVERY_COLUMNS} FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous') ORDER BY created_at LIMIT 5`,
			)
			.all() as DeliveryDbRow[];
		return {
			pending: open.n,
			oldestPendingAgeMs: open.oldest === null ? null : now - Date.parse(open.oldest),
			expired: expiredCount,
			recentExpired: expiredRows.map((row) => ({
				deliveryId: row.delivery_id,
				originKey: row.origin_key,
				attempts: row.attempts,
				expiredAt: row.updated_at,
				lastError: row.last_error as DeliveryErrorCode | null,
			})),
			recentPending: pendingRows.map((raw) => {
				const row = this.#toRow(raw);
				return {
					deliveryId: row.deliveryId,
					originKey: row.originKey,
					state: row.state as UnsettledDeliveryRow["state"],
					attempts: row.attempts,
					lastError: row.lastError,
					nextRetryAt: row.nextRetryAt,
					createdAt: row.createdAt,
				};
			}),
		};
	}

	get(deliveryId: string): DeliveryRow | undefined {
		const row = this.#db
			.prepare(`SELECT ${DELIVERY_COLUMNS} FROM deliveries WHERE delivery_id = ?`)
			.get(deliveryId) as DeliveryDbRow | null;
		return row ? this.#toRow(row) : undefined;
	}

	/** Raw rows for persistence diagnostics and compatibility-free test assertions. */
	rows(): DeliveryDbRow[] {
		return this.#db.prepare(`SELECT ${DELIVERY_COLUMNS} FROM deliveries ORDER BY created_at`).all() as DeliveryDbRow[];
	}

	stateCounts(): Array<{ state: string; n: number }> {
		return this.#db.prepare("SELECT state, COUNT(*) AS n FROM deliveries GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	unsettledByOrigin(nowMs: number): Array<{ origin_key: string; n: number; oldest_ms: number }> {
		return this.#db
			.prepare(
				"SELECT origin_key, COUNT(*) AS n, MAX((? - CAST(strftime('%s', created_at) AS INTEGER)) * 1000) AS oldest_ms FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous') GROUP BY origin_key",
			)
			.all(Math.floor(nowMs / 1000)) as Array<{ origin_key: string; n: number; oldest_ms: number }>;
	}

	confirmedConversation(
		conversationId: string,
		floorAt: string,
		limit: number,
	): Array<{ created_at: string; payload_json: string }> {
		return this.#db
			.prepare(
				"SELECT created_at, payload_json FROM deliveries WHERE state = 'confirmed' AND json_extract(payload_json, '$.origin.conversationId') = ? AND json_extract(payload_json, '$.reaction') IS NULL AND created_at >= ? ORDER BY created_at DESC LIMIT ?",
			)
			.all(conversationId, floorAt, limit) as Array<{ created_at: string; payload_json: string }>;
	}

	terminalForTurns(turnIds: readonly string[]): Array<{ delivery_id: string; turn_id: string; state: string }> {
		const ids = [...new Set(turnIds)];
		if (ids.length === 0) return [];
		const placeholders = ids.map(() => "?").join(", ");
		return this.#db
			.prepare(
				`SELECT delivery_id, turn_id, state FROM deliveries WHERE turn_id IN (${placeholders}) AND state IN ('confirmed','expired')`,
			)
			.all(...ids) as Array<{ delivery_id: string; turn_id: string; state: string }>;
	}

	sweep(tx: WriteTx, cutoffs: { deliveryCutoff: string; historyCutoff: string; batch: number }): number {
		return this.#db
			.prepare(
				"DELETE FROM deliveries WHERE rowid IN (SELECT rowid FROM deliveries WHERE (state = 'confirmed' AND updated_at < ?) OR (state = 'expired' AND updated_at < ?) LIMIT ?)",
			)
			.run(tx, cutoffs.deliveryCutoff, cutoffs.historyCutoff, cutoffs.batch).changes;
	}

	#update(
		tx: WriteTx,
		deliveryId: string,
		state: DeliveryState,
		attempts?: number,
		lastError?: DeliveryErrorCode,
	): void {
		this.#db
			.prepare(
				"UPDATE deliveries SET state = ?, attempts = COALESCE(?, attempts), last_error = COALESCE(?, last_error), updated_at = ? WHERE delivery_id = ?",
			)
			.run(tx, state, attempts ?? null, lastError ?? null, new Date().toISOString(), deliveryId);
	}

	unsettledRows(): DeliveryRow[] {
		return (
			this.#db
				.prepare(
					`SELECT ${DELIVERY_COLUMNS} FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous') ORDER BY created_at`,
				)
				.all() as DeliveryDbRow[]
		).map((row) => this.#toRow(row));
	}

	#toRow(row: DeliveryDbRow): DeliveryRow {
		const state = row.state as DeliveryState;
		return {
			deliveryId: row.delivery_id,
			turnId: row.turn_id,
			originKey: row.origin_key,
			payloadJson: row.payload_json,
			state,
			attempts: row.attempts,
			createdAt: row.created_at,
			updatedAt: row.updated_at,
			lastError: row.last_error as DeliveryErrorCode | null,
			nextRetryAt:
				state === "confirmed" || state === "expired"
					? null
					: new Date(Date.parse(row.updated_at) + retryBackoffMs(row.attempts)).toISOString(),
		};
	}
}

function retryBackoffMs(attempts: number): number {
	if (attempts === 0) return 0;
	return Math.min(RETRY_BACKOFF_BASE_MS * 2 ** Math.max(0, attempts - 1), RETRY_BACKOFF_CAP_MS);
}
