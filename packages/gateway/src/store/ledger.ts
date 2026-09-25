import type { GatewayDatabase } from "./db";

export type DeliveryState = "pending" | "inflight" | "confirmed" | "failed_ambiguous" | "expired";
export const DELIVERY_FAILURE_REASONS = [
	"adapter_error",
	"rate_limited",
	"permission_denied",
	"target_unavailable",
	"transport_error",
] as const;
export type DeliveryFailureReason = (typeof DELIVERY_FAILURE_REASONS)[number];
/** Outcome of a settlement attempt against the ledger. */
export type LedgerOutcome = "unknown" | "transitioned" | "already_terminal";
const MAX_DEFINITIVE_FAILURES = 5;
const RETRY_BACKOFF_BASE_MS = 2_000;
const RETRY_BACKOFF_CAP_MS = 5 * 60_000;

export interface DeliveryRow {
	readonly deliveryId: string;
	readonly turnId: string;
	readonly originKey: string;
	readonly payloadJson: string;
	readonly state: DeliveryState;
	readonly lastError: DeliveryFailureReason | null;
	readonly attempts: number;
	readonly createdAt: string;
	readonly updatedAt: string;
}

export interface ExpiredDeliveryRow {
	readonly deliveryId: string;
	readonly originKey: string;
	readonly attempts: number;
	readonly expiredAt: string;
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
	fail(deliveryId: string, ambiguous = false, reason?: unknown): LedgerOutcome {
		const row = this.get(deliveryId);
		if (!row) return "unknown";
		// Terminal states never rewrite: confirmed stays delivered, expired stays
		// expired. A late duplicate fail after confirm is recorded as a no-op.
		if (row.state === "confirmed" || row.state === "expired") return "already_terminal";
		const attempts = row.attempts + 1;
		// Ambiguous failures still count as attempts but remain unsettled; ordinary
		// sweeps apply the existing backoff and a reconnect replays them immediately.
		const state: DeliveryState = ambiguous
			? "failed_ambiguous"
			: attempts >= MAX_DEFINITIVE_FAILURES
				? "expired"
				: "pending";
		const lastError = classifyDeliveryFailure(reason);
		this.#database.withTransaction(() => this.#database.deliveryUpdate(deliveryId, state, attempts, lastError));
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
				(ignoreBackoff || row.attempts === 0 || now - Date.parse(row.updatedAt) >= retryBackoffMs(row.attempts)),
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
			})),
		};
	}
	get(deliveryId: string): DeliveryRow | undefined {
		return this.rows().find((row) => row.deliveryId === deliveryId);
	}
	private rows(): DeliveryRow[] {
		return this.#database.deliveryRows().map((row) => ({
			deliveryId: row.delivery_id,
			turnId: row.turn_id,
			originKey: row.origin_key,
			payloadJson: row.payload_json,
			state: row.state as DeliveryState,
			lastError: row.last_error as DeliveryFailureReason | null,
			attempts: row.attempts,
			createdAt: row.created_at,
			updatedAt: row.updated_at,
		}));
	}
}

/** Classifies untrusted adapter text without allowing it into persistent storage. */
export function classifyDeliveryFailure(reason: unknown): DeliveryFailureReason {
	if (typeof reason !== "string") return "adapter_error";
	if (/\b(rate[\s_-]?limit(?:ed|ing)?|too many requests|http\s*429|status(?:\s*code)?\s*429)\b/i.test(reason))
		return "rate_limited";
	if (
		/\b(?:401|403|permission|permissions|forbidden|missing[\s_-](?:access|permissions?)|unauthori[sz]ed|not[\s_-](?:authorized|authed|allowed)|invalid[\s_-]auth|auth(?:entication|orization)?(?:[\s_-](?:failed|error|denied|invalid))?|access[\s_-]denied)\b/i.test(
			reason,
		)
	)
		return "permission_denied";
	if (
		/\b404\b|\b(?:unknown|missing|no[\s_-]such|not[\s_-]found|cannot[\s_-]find)[\s_-]+(?:message|channel|chat|user|target|recipient)\b|\b(?:message|channel|chat|user|target|recipient)[\s_-]+(?:not[\s_-]found|missing|unavailable)\b/i.test(
			reason,
		)
	)
		return "target_unavailable";
	if (
		/\b(transport|socket|econn[a-z]+|etimedout|timeout|timed out|time[\s_-]out|network|fetch failed|connection[\s_-](?:reset|closed|refused|lost))\b/i.test(
			reason,
		)
	)
		return "transport_error";
	return "adapter_error";
}

function retryBackoffMs(attempts: number): number {
	return Math.min(RETRY_BACKOFF_BASE_MS * 2 ** Math.max(0, attempts - 1), RETRY_BACKOFF_CAP_MS);
}
