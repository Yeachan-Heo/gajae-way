/**
 * Correlates gateway turn ids with their input modality.
 *
 * The registry deliberately resolves only voice entries. Missing or text entries therefore
 * fail closed to the caller's text path rather than risking accidental speech.
 */

export type TurnModality = "voice" | "text";

export interface ModalityEntry {
	readonly turnId: string;
	readonly modality: TurnModality;
	readonly announced: boolean;
	readonly parts: number;
	readonly registeredAtMs: number;
	readonly lastSeenAtMs: number;
}

export interface ModalityRegistration {
	readonly turnId: string;
	readonly modality: TurnModality;
	readonly atMs: number;
	readonly announced?: boolean;
	readonly parts?: number;
}

export interface ModalityRegistrationState {
	readonly modality: TurnModality;
	readonly announced?: boolean;
	readonly parts?: number;
}

export interface ModalitySweepResult {
	readonly cleared: number;
	readonly turnIds: readonly string[];
}

type MutableModalityEntry = {
	turnId: string;
	modality: TurnModality;
	announced: boolean;
	parts: number;
	registeredAtMs: number;
	lastSeenAtMs: number;
};

function validateTimestamp(atMs: number): void {
	if (!Number.isFinite(atMs)) throw new RangeError("atMs must be finite");
}

export class ModalityRegistry {
	private readonly values = new Map<string, MutableModalityEntry>();

	/**
	 * Registers once. A duplicate registration preserves parts and announce state, while
	 * refreshing liveness for a redelivered fragment.
	 */
	register(turnId: string, state: ModalityRegistrationState, atMs: number): boolean;
	register(turnId: string, modality: TurnModality, atMs: number): boolean;
	register(registration: ModalityRegistration): boolean;
	register(
		turnIdOrRegistration: string | ModalityRegistration,
		modalityOrState?: TurnModality | ModalityRegistrationState,
		atMs?: number,
	): boolean {
		const turnId = typeof turnIdOrRegistration === "string" ? turnIdOrRegistration : turnIdOrRegistration.turnId;
		const selectedModality =
			typeof turnIdOrRegistration === "string"
				? typeof modalityOrState === "string"
					? modalityOrState
					: modalityOrState?.modality
				: turnIdOrRegistration.modality;
		const selectedAtMs = typeof turnIdOrRegistration === "string" ? atMs : turnIdOrRegistration.atMs;
		if (turnId.length === 0) throw new TypeError("turnId must be a non-empty string");
		if (selectedModality !== "voice" && selectedModality !== "text") {
			throw new TypeError("modality must be voice or text");
		}
		if (selectedAtMs === undefined) throw new TypeError("atMs is required");
		validateTimestamp(selectedAtMs);

		const existing = this.values.get(turnId);
		if (existing !== undefined) {
			existing.lastSeenAtMs = Math.max(existing.lastSeenAtMs, selectedAtMs);
			return false;
		}
		this.values.set(turnId, {
			turnId,
			modality: selectedModality,
			announced: false,
			parts: 0,
			registeredAtMs: selectedAtMs,
			lastSeenAtMs: selectedAtMs,
		});
		return true;
	}

	/** Returns a snapshot for diagnostics without exposing mutable map state. */
	get(turnId: string): ModalityEntry | undefined {
		const entry = this.values.get(turnId);
		return entry === undefined ? undefined : { ...entry };
	}

	/**
	 * Announces a voice turn at most once. Text and unknown turns return false.
	 */
	announce(turnId: string): boolean {
		const entry = this.values.get(turnId);
		if (entry === undefined || entry.modality !== "voice" || entry.announced) return false;
		entry.announced = true;
		return true;
	}

	/** Records any delivery, including `final`; final fragments never delete the entry. */
	recordPart(turnId: string, atMs: number, final = false): boolean {
		void final;
		validateTimestamp(atMs);
		const entry = this.values.get(turnId);
		if (entry === undefined) return false;
		entry.parts += 1;
		entry.lastSeenAtMs = Math.max(entry.lastSeenAtMs, atMs);
		return true;
	}

	markPart(turnId: string, atMs: number, final = false): boolean {
		return this.recordPart(turnId, atMs, final);
	}

	observeDelivery(turnId: string, final: boolean, atMs: number): boolean {
		return this.recordPart(turnId, atMs, final);
	}

	/** Explicit turn-end signal; this is the normal deletion path. */
	end(turnId: string): boolean {
		return this.values.delete(turnId);
	}

	/** Alias matching the wire event name. */
	turnEnd(turnId: string): boolean {
		return this.end(turnId);
	}

	onTurnEnd(turnId: string): boolean {
		return this.end(turnId);
	}

	resolve(turnId: string): "voice" | undefined {
		return this.values.get(turnId)?.modality === "voice" ? "voice" : undefined;
	}

	/**
	 * Removes entries that have not received a delivery within the caller's TTL window.
	 * The comparison is inclusive at the expiry boundary.
	 */
	sweep(nowMs: number, ttlMs: number): ModalitySweepResult {
		validateTimestamp(nowMs);
		if (!Number.isFinite(ttlMs) || ttlMs < 0) throw new RangeError("ttlMs must be finite and non-negative");
		const turnIds: string[] = [];
		for (const [turnId, entry] of this.values) {
			if (nowMs - entry.lastSeenAtMs >= ttlMs) {
				this.values.delete(turnId);
				turnIds.push(turnId);
			}
		}
		return { cleared: turnIds.length, turnIds };
	}

	get size(): number {
		return this.values.size;
	}
}
