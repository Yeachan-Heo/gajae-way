/**
 * Fail-closed origin turn tracking for voice admission.
 *
 * The gateway accepts a turn before the adapter sees its identifier. This book therefore
 * records every accepted turn it can identify and keeps an exact debt count for accepted
 * turns that arrive after the bounded list is full.
 */

import type { AdmitOutcome, OriginTurnBookCounters, OutstandingTurn, SettleResult, SweepResult } from "./ports";

export interface OriginTurnBookOptions {
	readonly maxEntries: number;
}

/** The gateway response fields relevant to adapter-side admission. */
export interface TurnAdmission {
	readonly engaged?: boolean;
	readonly turnId?: string | null;
	/** Slash-command callers set this to false because no turn is executed. */
	readonly admit?: boolean;
}

export interface AdmissibleTurn extends TurnAdmission {
	readonly engaged: true;
	readonly turnId: string;
}

/**
 * Encodes the caller contract: only an engaged response with a string turn id is admitted.
 * Policy declines, duplicate acknowledgements, and command-only responses are excluded.
 */
export function shouldAdmitTurn(input: TurnAdmission): input is AdmissibleTurn {
	return input.admit !== false && input.engaged === true && typeof input.turnId === "string";
}

export type TurnGateDecision = "hold" | "trigger";

export type TurnGateSource = boolean | { readonly isBusy: () => boolean };

type MutableCounters = {
	tracked: number;
	saturatedAdmits: number;
	settled: number;
	unknownTerminal: number;
	stale: number;
	debt: number;
};

export class OriginTurnBook {
	private readonly maxEntries: number;
	private entries: OutstandingTurn[] = [];
	private nextSequence = 0;
	private debtValue = 0;
	private saturatedSinceMs: number | undefined;
	private readonly counterValues: MutableCounters = {
		tracked: 0,
		saturatedAdmits: 0,
		settled: 0,
		unknownTerminal: 0,
		stale: 0,
		debt: 0,
	};

	constructor(options: OriginTurnBookOptions | number) {
		const maxEntries = typeof options === "number" ? options : options.maxEntries;
		if (!Number.isSafeInteger(maxEntries) || maxEntries < 1) {
			throw new RangeError("maxEntries must be a positive safe integer");
		}
		this.maxEntries = maxEntries;
	}

	/**
	 * Adds a gateway-accepted turn without ever evicting an earlier accepted turn.
	 * Once the tracked list is full, the scalar debt preserves busy knowledge exactly.
	 */
	admit(entry: Omit<OutstandingTurn, "seq">, nowMs: number): AdmitOutcome {
		const trackedEntry: OutstandingTurn = { ...entry, seq: this.nextSequence };
		this.nextSequence += 1;
		if (this.entries.length < this.maxEntries) {
			this.entries.push(trackedEntry);
			this.counterValues.tracked += 1;
			return "tracked";
		}

		this.debtValue += 1;
		this.counterValues.saturatedAdmits += 1;
		this.counterValues.debt = this.debtValue;
		if (this.saturatedSinceMs === undefined) {
			this.saturatedSinceMs = nowMs;
		}
		return "saturated";
	}

	/** Busy includes both tracked entries and every accepted turn represented by debt. */
	isBusy(): boolean {
		return this.entries.length > 0 || this.debtValue > 0;
	}

	/** Whether one or more accepted turns are represented by saturation debt. */
	get saturated(): boolean {
		return this.debtValue > 0;
	}

	get debt(): number {
		return this.debtValue;
	}

	get outstanding(): readonly OutstandingTurn[] {
		return this.entries.slice();
	}

	get counters(): OriginTurnBookCounters {
		return {
			tracked: this.counterValues.tracked,
			saturatedAdmits: this.counterValues.saturatedAdmits,
			settled: this.counterValues.settled,
			unknownTerminal: this.counterValues.unknownTerminal,
			stale: this.counterValues.stale,
			debt: this.debtValue,
		};
	}

	/** Uses the same fail-closed decision as the standalone helper. */
	decision(): TurnGateDecision {
		return decideTurnGate(this);
	}

	/**
	 * A known terminal clears its whole prefix, because debounce folded those entries into
	 * the terminal turn. An unknown terminal consumes exactly one FIFO entry or one debt.
	 */
	settle(turnId: string, nowMs: number): SettleResult {
		void nowMs;
		const knownIndex = this.entries.findIndex((entry) => entry.turnId === turnId);
		if (knownIndex >= 0) {
			const cleared = this.entries.splice(0, knownIndex + 1);
			this.counterValues.settled += cleared.length;
			return { cleared, debtCleared: 0, unknownTerminal: false };
		}

		if (this.entries.length > 0) {
			const oldest = this.entries.shift();
			if (oldest === undefined) {
				return { cleared: [], debtCleared: 0, unknownTerminal: false };
			}
			this.counterValues.unknownTerminal += 1;
			return { cleared: [oldest], debtCleared: 0, unknownTerminal: true };
		}

		if (this.debtValue > 0) {
			this.debtValue -= 1;
			this.counterValues.debt = this.debtValue;
			this.counterValues.unknownTerminal += 1;
			if (this.debtValue === 0) {
				this.saturatedSinceMs = undefined;
			}
			return { cleared: [], debtCleared: 1, unknownTerminal: true };
		}

		return { cleared: [], debtCleared: 0, unknownTerminal: false };
	}

	/**
	 * Removes tracked entries strictly older than the supplied TTL. Saturation debt is
	 * released as one safety-valve operation once its saturation window has elapsed.
	 */
	sweep(nowMs: number, ttlMs: number): SweepResult {
		if (!Number.isFinite(nowMs) || !Number.isFinite(ttlMs) || ttlMs < 0) {
			throw new RangeError("nowMs and ttlMs must be finite, with ttlMs non-negative");
		}

		const retained: OutstandingTurn[] = [];
		const cleared: OutstandingTurn[] = [];
		for (const entry of this.entries) {
			if (nowMs - entry.acceptedAtMs > ttlMs) {
				cleared.push(entry);
			} else {
				retained.push(entry);
			}
		}
		this.entries = retained;
		this.counterValues.stale += cleared.length;

		let debtCleared = 0;
		if (this.debtValue > 0 && this.saturatedSinceMs !== undefined && nowMs >= this.saturatedSinceMs + ttlMs) {
			debtCleared = this.debtValue;
			this.debtValue = 0;
			this.counterValues.debt = 0;
			this.counterValues.stale += debtCleared;
			this.saturatedSinceMs = undefined;
		}

		return { cleared, debtCleared };
	}
}

/** Routes an utterance to context while any accepted turn remains busy. */
export function decideTurnGate(source: TurnGateSource): TurnGateDecision {
	const busy = typeof source === "boolean" ? source : source.isBusy();
	return busy ? "hold" : "trigger";
}
