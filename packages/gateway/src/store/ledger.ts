import type { DeliveryErrorCode } from "@gajae-gateway/protocol";
import type { GatewayDatabase } from "./db";

export type DeliveryState = "pending" | "inflight" | "confirmed" | "failed_ambiguous" | "expired";
/** Outcome of a settlement attempt against the ledger. */
export type LedgerOutcome = "unknown" | "transitioned" | "already_terminal";
const MAX_DEFINITIVE_FAILURES = 5;
const RETRY_BACKOFF_BASE_MS = 2_000;
const RETRY_BACKOFF_CAP_MS = 5 * 60_000;

const DELIVERY_ERROR_PATTERNS: readonly (readonly [DeliveryErrorCode, RegExp])[] = [
	["rate_limited", /rate.?limit|\b429\b|too many requests/i],
	["timeout", /time.?out|deadline/i],
	["network", /network|socket|econn|enotfound|eai_again|hang up|fetch failed|connection/i],
	["not_found", /unknown (message|channel|guild|user)|not.?found|\b404\b/i],
	["forbidden", /forbidden|missing (access|permissions)|not.?allowed|disabled|blocked|cannot|\b403\b/i],
	["invalid_request", /invalid|bad request|malformed|\b400\b/i],
];

/** Maps an adapter's free-text failure reason onto the allowlisted code; the raw text is never stored. */
export function classifyDeliveryError(reason: string | undefined): DeliveryErrorCode {
	if (reason) for (const [code, pattern] of DELIVERY_ERROR_PATTERNS) if (pattern.test(reason)) return code;
	return "other";
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
	/** Classification of the most recent failed attempt; kept across redrive and confirm. */
	readonly lastError: DeliveryErrorCode | null;
	/** When the sweep next retries this row; null once the row is terminal. */
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

export class DeliveryLedger {
	readonly #database: GatewayDatabase;
	constructor(database: GatewayDatabase) {
		this.#database = database;
	}
	createPending(row: { deliveryId: string; turnId: string; originKey: string; payloadJson: string }): boolean {
		return this.#database.withTransaction(() =>
			this.#database.deliveryCreate({
				id: row.deliveryId,
				turnId: row.turnId,
				originKey: row.originKey,
				payloadJson: row.payloadJson,
			}),
		);
	}
	/** Explicit participation in the same database's caller-owned transaction. */
	createPendingInTransaction(row: {
		deliveryId: string;
		turnId: string;
		originKey: string;
		payloadJson: string;
	}): boolean {
		return this.#database.deliveryCreateInTransaction({
			id: row.deliveryId,
			turnId: row.turnId,
			originKey: row.originKey,
			payloadJson: row.payloadJson,
		});
	}
	markInflight(deliveryId: string): void {
		const row = this.get(deliveryId);
		if (!row || row.state === "confirmed" || row.state === "expired") return;
		this.#database.withTransaction(() => this.#database.deliveryUpdate(deliveryId, "inflight", row.attempts));
	}
	/**
	 * Terminal-state transitions are explicit (red-team blocker 2 / round 3):
	 * - `confirmed` is terminal: a late duplicate confirm is a no-op; a fail can
	 *   NEVER rewrite a confirmed row (the platform told us it was delivered).
	 * - `expired` is terminal: a late confirm cannot resurrect it (the platform
	 *   connection is gone; a duplicate would be re-acked through the redelivery
	 *   path only if the row were still live). Late-fail on expired is a no-op.
	 * Returns "unknown" (no such delivery), "transitioned" (applied now), or
	 * "already_terminal" (idempotent no-op on a settled row).
	 */
	confirm(deliveryId: string): LedgerOutcome {
		const row = this.get(deliveryId);
		if (!row) return "unknown";
		if (row.state === "confirmed" || row.state === "expired") return "already_terminal";
		this.#database.withTransaction(() => this.#database.deliveryUpdate(deliveryId, "confirmed"));
		return "transitioned";
	}
	fail(deliveryId: string, ambiguous = false, reason?: string): LedgerOutcome {
		const row = this.get(deliveryId);
		if (!row) return "unknown";
		// Terminal states never rewrite: confirmed stays delivered, expired stays
		// expired. A late duplicate fail after confirm is recorded as a no-op.
		if (row.state === "confirmed" || row.state === "expired") return "already_terminal";
		const attempts = row.attempts + 1;
		const state: DeliveryState = ambiguous
			? "failed_ambiguous"
			: attempts >= MAX_DEFINITIVE_FAILURES
				? "expired"
				: "pending";
		this.#database.withTransaction(() =>
			this.#database.deliveryUpdate(deliveryId, state, attempts, classifyDeliveryError(reason)),
		);
		return "transitioned";
	}
	/**
	 * Unsettled, fresh rows. `ignoreBackoff` is for a newly negotiated adapter:
	 * the backoff paces retries over a transport that already failed them, and a
	 * new connection is a new transport.
	 */
	listUndelivered(freshnessMs: number, now = Date.now(), ignoreBackoff = false): DeliveryRow[] {
		return this.rows().filter(
			(row) =>
				!["confirmed", "expired"].includes(row.state) &&
				now - Date.parse(row.createdAt) <= freshnessMs &&
				(ignoreBackoff || now >= Date.parse(row.nextRetryAt as string)),
		);
	}
	expireStale(freshnessMs: number, now = Date.now()): ExpiredDeliveryRow[] {
		const before = new Date(now - freshnessMs).toISOString();
		const expiredAt = new Date(now).toISOString();
		return this.#database
			.withTransaction(() => this.#database.deliveryExpireBefore(before, expiredAt))
			.map((row) => ({
				deliveryId: row.delivery_id,
				originKey: row.origin_key,
				attempts: row.attempts,
				expiredAt: row.updated_at,
				lastError: row.last_error as DeliveryErrorCode | null,
			}));
	}
	requeue(deliveryId: string): string[] {
		return this.#database.withTransaction(() => this.#database.deliveryRequeueById(deliveryId));
	}
	requeueSince(since: string): string[] {
		const timestamp = Date.parse(since);
		if (!Number.isFinite(timestamp)) throw new TypeError("since must be an ISO timestamp");
		const normalizedSince = new Date(timestamp).toISOString();
		return this.#database.withTransaction(() => this.#database.deliveryRequeueSince(normalizedSince));
	}
	getMany(deliveryIds: readonly string[]): DeliveryRow[] {
		const wanted = new Set(deliveryIds);
		return this.rows().filter((row) => wanted.has(row.deliveryId));
	}
	prune(deliveredOlderThanMs: number, now = Date.now()): number {
		return this.#database.withTransaction(() =>
			this.#database.deliveryPrune(new Date(now - deliveredOlderThanMs).toISOString()),
		);
	}
	counts(now = Date.now()): {
		pending: number;
		oldestPendingAgeMs: number | null;
		expired: number;
		recentExpired: readonly ExpiredDeliveryRow[];
		recentPending: readonly UnsettledDeliveryRow[];
	} {
		const allRows = this.rows();
		const rows = allRows.filter((row) => !["confirmed", "expired"].includes(row.state));
		const expiredRows = allRows
			.filter((row) => row.state === "expired")
			.sort(
				(left, right) =>
					right.updatedAt.localeCompare(left.updatedAt) || left.deliveryId.localeCompare(right.deliveryId),
			);
		return {
			pending: rows.length,
			oldestPendingAgeMs: rows.length ? Math.max(...rows.map((row) => now - Date.parse(row.createdAt))) : null,
			expired: expiredRows.length,
			recentExpired: expiredRows.slice(0, 5).map((row) => ({
				deliveryId: row.deliveryId,
				originKey: row.originKey,
				attempts: row.attempts,
				expiredAt: row.updatedAt,
				lastError: row.lastError,
			})),
			recentPending: rows.slice(0, 5).map((row) => ({
				deliveryId: row.deliveryId,
				originKey: row.originKey,
				state: row.state as UnsettledDeliveryRow["state"],
				attempts: row.attempts,
				lastError: row.lastError,
				nextRetryAt: row.nextRetryAt,
				createdAt: row.createdAt,
			})),
		};
	}
	get(deliveryId: string): DeliveryRow | undefined {
		return this.rows().find((row) => row.deliveryId === deliveryId);
	}
	private rows(): DeliveryRow[] {
		return this.#database.deliveryRows().map((row) => {
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
		});
	}
}

/** Delay after the last ledger transition before the sweep retries; a never-failed row is due at once. */
function retryBackoffMs(attempts: number): number {
	if (attempts === 0) return 0;
	return Math.min(RETRY_BACKOFF_BASE_MS * 2 ** Math.max(0, attempts - 1), RETRY_BACKOFF_CAP_MS);
}
