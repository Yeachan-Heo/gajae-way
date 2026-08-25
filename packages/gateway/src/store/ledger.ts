import type { GatewayDatabase } from "./db";

export type DeliveryState = "pending" | "inflight" | "confirmed" | "failed_ambiguous" | "expired";
export interface DeliveryRow {
	readonly deliveryId: string;
	readonly turnId: string;
	readonly originKey: string;
	readonly payloadJson: string;
	readonly state: DeliveryState;
	readonly attempts: number;
	readonly createdAt: string;
	readonly updatedAt: string;
}

export class DeliveryLedger {
	readonly #database: GatewayDatabase;
	constructor(database: GatewayDatabase) {
		this.#database = database;
	}
	createPending(row: { deliveryId: string; turnId: string; originKey: string; payloadJson: string }): void {
		this.#database.withTransaction(() =>
			this.#database.deliveryCreate({
				id: row.deliveryId,
				turnId: row.turnId,
				originKey: row.originKey,
				payloadJson: row.payloadJson,
			}),
		);
	}
	markInflight(deliveryId: string): void {
		this.#database.withTransaction(() =>
			this.#database.deliveryUpdate(deliveryId, "inflight", this.get(deliveryId)?.attempts ?? 0),
		);
	}
	confirm(deliveryId: string): boolean {
		const row = this.get(deliveryId);
		if (!row) return false;
		this.#database.withTransaction(() => this.#database.deliveryUpdate(deliveryId, "confirmed"));
		return true;
	}
	fail(deliveryId: string, ambiguous = false): boolean {
		const row = this.get(deliveryId);
		if (!row) return false;
		const attempts = row.attempts + 1;
		const state: DeliveryState = ambiguous ? "failed_ambiguous" : attempts >= 3 ? "expired" : "pending";
		this.#database.withTransaction(() => this.#database.deliveryUpdate(deliveryId, state, attempts));
		return true;
	}
	listUndelivered(freshnessMs: number, now = Date.now()): DeliveryRow[] {
		return this.rows().filter(
			(row) =>
				!["confirmed", "expired"].includes(row.state) &&
				now - Date.parse(row.createdAt) <= freshnessMs &&
				(row.state === "failed_ambiguous" ||
					row.attempts === 0 ||
					now - Date.parse(row.updatedAt) >= retryBackoffMs(row.attempts)),
		);
	}
	prune(deliveredOlderThanMs: number, now = Date.now()): number {
		return this.#database.withTransaction(() =>
			this.#database.deliveryPrune(new Date(now - deliveredOlderThanMs).toISOString()),
		);
	}
	counts(now = Date.now()): { pending: number; oldestPendingAgeMs: number | null } {
		const rows = this.rows().filter((row) => !["confirmed", "expired"].includes(row.state));
		return {
			pending: rows.length,
			oldestPendingAgeMs: rows.length ? Math.max(...rows.map((row) => now - Date.parse(row.createdAt))) : null,
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
			attempts: row.attempts,
			createdAt: row.created_at,
			updatedAt: row.updated_at,
		}));
	}
}

function retryBackoffMs(attempts: number): number {
	return 1_000 * 2 ** Math.max(0, attempts - 1);
}
