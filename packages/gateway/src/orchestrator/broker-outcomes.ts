/**
 * I6a: application outcomes observed at the envelope boundary, scoped to the
 * daemon identity that produced them. The health probe (endpoint hello) proves
 * liveness only; a daemon that answers hello but refuses every session op with a
 * daemon-resource error is *serviceability*-dead, and only broker-scope
 * outcomes may strike it. Per-session cursor/pin errors are the caller's
 * business and never count.
 */

export type OutcomeScope = "session" | "broker";
export type OutcomeMessageClass = "capacity" | "cursor" | "other";

export interface DaemonIdentity {
	readonly pid: number;
	/** Discovery url (host:port) at call time; a replaced daemon has a different identity. */
	readonly url: string;
}

export interface BrokerOutcome {
	readonly scope: OutcomeScope;
	readonly op: string;
	readonly code: string | undefined;
	readonly messageClass: OutcomeMessageClass;
	readonly sessionId?: string;
	readonly daemon: DaemonIdentity | undefined;
	readonly generation: number;
	readonly at: number;
	readonly ok: boolean;
}

/** Codes that are always per-session (cursor/pin) and never daemon evidence. */
const SESSION_LOCAL_CODES = new Set(["snapshot_capacity_exceeded", "invalid_cursor", "cursor_expired"]);
const CAPACITY_MESSAGE = /cursor capacity is exhausted|reconciliation_capacity/i;

/** Classify a failed envelope; `hasCursor` marks a cursor-bearing request whose invalid_input is local. */
export function classifyFailure(input: {
	op: string;
	code: string | undefined;
	message: string | undefined;
	hasCursor: boolean;
}): { scope: OutcomeScope; messageClass: OutcomeMessageClass } {
	const message = input.message ?? "";
	if (CAPACITY_MESSAGE.test(message) || input.code === "reconciliation_capacity")
		return { scope: "broker", messageClass: "capacity" };
	if (input.code && SESSION_LOCAL_CODES.has(input.code)) return { scope: "session", messageClass: "cursor" };
	if (input.code === "invalid_input" && input.hasCursor) return { scope: "session", messageClass: "cursor" };
	return { scope: "session", messageClass: "other" };
}

export interface ServiceabilityOptions {
	readonly strikesToRetire?: number;
	readonly windowMs?: number;
	readonly cooldownMs?: number;
	/** Distinct live sessions reporting `session_unavailable` before that counts as broker-scope. */
	readonly unavailableSessionsToStrike?: number;
	readonly now?: () => number;
}

export interface ServiceabilityVerdict {
	readonly retire: boolean;
	readonly strikes: number;
	readonly reason?: string;
}

/**
 * Strike accounting keyed by daemon identity. Strikes come only from broker-scope
 * outcomes of the current generation; they reset only when the same op class
 * succeeds against the same daemon; a retired identity enters a cooldown during
 * which it cannot be struck again.
 */
export class ServiceabilityTracker {
	readonly #strikesToRetire: number;
	readonly #windowMs: number;
	readonly #cooldownMs: number;
	readonly #unavailableThreshold: number;
	readonly #now: () => number;
	#identity: string | undefined;
	#strikeTimes: number[] = [];
	#unavailableSessions = new Map<string, number>();
	#cooldownUntil = new Map<string, number>();

	constructor(options: ServiceabilityOptions = {}) {
		this.#strikesToRetire = options.strikesToRetire ?? 3;
		this.#windowMs = options.windowMs ?? 60_000;
		this.#cooldownMs = options.cooldownMs ?? 10 * 60_000;
		this.#unavailableThreshold = options.unavailableSessionsToStrike ?? 2;
		this.#now = options.now ?? (() => Date.now());
	}

	get strikes(): number {
		return this.#strikeTimes.length;
	}

	/** Records one outcome; returns whether the daemon must be retired now. */
	observe(outcome: BrokerOutcome, currentGeneration: number, liveSessions: ReadonlySet<string>): ServiceabilityVerdict {
		if (outcome.generation !== currentGeneration || !outcome.daemon) return { retire: false, strikes: this.strikes };
		const identity = `${outcome.daemon.pid}|${outcome.daemon.url}`;
		if (identity !== this.#identity) {
			this.#identity = identity;
			this.#strikeTimes = [];
			this.#unavailableSessions = new Map();
		}
		const now = this.#now();
		if (outcome.ok) {
			// Reset only on a matching success: the same op class against the same daemon.
			if (outcome.scope === "broker" || outcome.op === "session.list" || outcome.op === "session.inspect") {
				this.#strikeTimes = [];
				this.#unavailableSessions = new Map();
			}
			return { retire: false, strikes: 0 };
		}
		let broker = outcome.scope === "broker";
		if (!broker && outcome.code === "session_unavailable" && outcome.sessionId && liveSessions.has(outcome.sessionId)) {
			this.#unavailableSessions.set(outcome.sessionId, now);
			for (const [id, at] of this.#unavailableSessions)
				if (now - at > this.#windowMs) this.#unavailableSessions.delete(id);
			broker = this.#unavailableSessions.size >= this.#unavailableThreshold;
		}
		if (!broker) return { retire: false, strikes: this.strikes };
		const until = this.#cooldownUntil.get(identity);
		if (until !== undefined && now < until) return { retire: false, strikes: this.strikes, reason: "cooldown" };
		this.#strikeTimes = this.#strikeTimes.filter((at) => now - at <= this.#windowMs);
		this.#strikeTimes.push(now);
		if (this.#strikeTimes.length >= this.#strikesToRetire) {
			this.#cooldownUntil.set(identity, now + this.#cooldownMs);
			const strikes = this.#strikeTimes.length;
			this.#strikeTimes = [];
			return { retire: true, strikes, reason: outcome.messageClass };
		}
		return { retire: false, strikes: this.strikes };
	}
}
