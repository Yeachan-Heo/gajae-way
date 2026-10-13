import type { GroupDb, StoreCore } from "../core";
import type { WriteTx } from "../tx";

export interface MemoryIntentDbRow {
	readonly id: string;
	readonly kind: string;
	readonly payload_json: string;
	readonly state: "queued" | "written" | "committed" | "receipted" | "quarantined";
	readonly attempts: number;
	readonly quarantine_reason: string | null;
}

/** Persistence for durable memory-closure intents. */
export class MemoryGroup {
	readonly #db: GroupDb;

	constructor(core: StoreCore) {
		this.#db = core.groupDb("memory");
	}

	intentCounts(): Array<{ state: string; n: number }> {
		return this.#db.prepare("SELECT state, COUNT(*) AS n FROM memory_intents GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	createIntent(tx: WriteTx, row: { id: string; kind: string; payloadJson: string }): void {
		const now = new Date().toISOString();
		this.#db
			.prepare(
				"INSERT INTO memory_intents (id, kind, payload_json, state, created_at, updated_at) VALUES (?, ?, ?, 'queued', ?, ?)",
			)
			.run(tx, row.id, row.kind, row.payloadJson, now, now);
	}

	/** Insertion-order view preserves admission order within the same millisecond. */
	rowsByRowid(): MemoryIntentDbRow[] {
		return this.#db
			.prepare("SELECT id, kind, payload_json, state, attempts, quarantine_reason FROM memory_intents ORDER BY rowid")
			.all() as MemoryIntentDbRow[];
	}

	beginAttempt(tx: WriteTx, id: string): void {
		this.#db
			.prepare("UPDATE memory_intents SET attempts = attempts + 1, updated_at = ? WHERE id = ?")
			.run(tx, new Date().toISOString(), id);
	}

	quarantine(tx: WriteTx, id: string, reason: string): void {
		if (!reason.trim()) throw new Error("memory intent quarantine reason is required");
		this.#db
			.prepare(
				"UPDATE memory_intents SET state = 'quarantined', quarantine_reason = COALESCE(quarantine_reason, ?), updated_at = ? WHERE id = ?",
			)
			.run(tx, reason, new Date().toISOString(), id);
	}

	update(tx: WriteTx, id: string, state: MemoryIntentDbRow["state"]): void {
		this.#db
			.prepare("UPDATE memory_intents SET state = ?, updated_at = ? WHERE id = ?")
			.run(tx, state, new Date().toISOString(), id);
	}

	rows(): MemoryIntentDbRow[] {
		return this.#db
			.prepare(
				"SELECT id, kind, payload_json, state, attempts, quarantine_reason FROM memory_intents ORDER BY created_at, id",
			)
			.all() as MemoryIntentDbRow[];
	}

	/** Every row not yet receipted, in admission order; recovery does not need receipted history. */
	openRows(): MemoryIntentDbRow[] {
		return this.#db
			.prepare(
				"SELECT id, kind, payload_json, state, attempts, quarantine_reason FROM memory_intents WHERE state <> 'receipted' ORDER BY created_at, id",
			)
			.all() as MemoryIntentDbRow[];
	}

	get(id: string): MemoryIntentDbRow | undefined {
		return (
			(this.#db
				.prepare("SELECT id, kind, payload_json, state, attempts, quarantine_reason FROM memory_intents WHERE id = ?")
				.get(id) as MemoryIntentDbRow | null) ?? undefined
		);
	}

	/** Deterministic id probe plus legacy payload scan for pre-deterministic ids. */
	coversEvent(eventId: string): boolean {
		if (this.get(`monitor-event-intent:${eventId}`)) return true;
		return (
			this.#db
				.prepare(
					"SELECT 1 AS one FROM memory_intents WHERE kind = 'monitor-event' AND instr(payload_json, ?) > 0 LIMIT 1",
				)
				.get(eventId) !== null
		);
	}
}
