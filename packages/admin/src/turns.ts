/**
 * In-flight turn tracker.
 *
 * `chat.progress` is fire-and-forget to whoever happens to be connected and no
 * verb lists turns in flight (gap G1), so this is the only place the console can
 * learn that work is happening. Two rules keep it honest:
 *
 * - liveness expires. The gateway heartbeats every 15s, so three missed
 *   heartbeats (45s) demotes a row to `stalled` with its last-known counters
 *   instead of animating a lie.
 * - finished turns persist for ten minutes with their outcome. Work that
 *   vanishes the instant it completes cannot be verified by an owner who was
 *   away from the desk.
 */

import {
	type ChatMessagePayload,
	type ChatProgressPayload,
	isSilenceToken,
	type OriginRef,
} from "@gajae-gateway/protocol";

/** Per-turn ceiling the gateway enforces on every `gjc` child. */
export const TURN_CEILING_MS = 300_000;
/** Three missed 15s heartbeats. */
export const TURN_STALL_MS = 45_000;
/** How long a finished turn stays on the Live work panel. */
export const TURN_RETENTION_MS = 600_000;

export type TurnState = "running" | "stalled" | "finished";

export type TrackedTurn = {
	readonly turnId: string;
	readonly origin: OriginRef;
	readonly startedAt: number;
	readonly lastEventAt: number;
	readonly elapsedMs: number;
	readonly toolCalls: number;
	readonly outputTokens: number;
	/** The last activity the gateway reported (tool name + intent, thinking, writing); operator-only detail. */
	readonly activity?: ChatProgressPayload["activity"];
	/**
	 * True once at least one `chat.progress` heartbeat has been seen. A turn that
	 * finishes inside the gateway's first 15s heartbeat window is only ever seen
	 * as a final message, and reporting its counters as `0 tool calls, 0s` would
	 * be asserting evidence we never received.
	 */
	readonly observed: boolean;
	readonly finishedAt?: number;
	readonly outcome?: "replied" | "silent" | "failed";
	readonly deliveryId?: string;
};

/** A turn's terminal shape, derived once so the panel and the SSE delta agree. */
function outcomeOf(message: ChatMessagePayload): TrackedTurn["outcome"] {
	if (message.text.trim().startsWith("[turn failed]")) return "failed";
	return isSilenceToken(message.text) ? "silent" : "replied";
}

export class TurnTracker {
	readonly #turns = new Map<string, TrackedTurn>();
	readonly #now: () => number;

	constructor(now: () => number = Date.now) {
		this.#now = now;
	}

	/** Ingest a progress heartbeat. Returns true when the panel needs repainting. */
	progress(payload: ChatProgressPayload): boolean {
		const at = this.#now();
		const existing = this.#turns.get(payload.turnId);
		this.#turns.set(payload.turnId, {
			turnId: payload.turnId,
			origin: payload.origin,
			startedAt: existing?.startedAt ?? at - payload.elapsedMs,
			lastEventAt: at,
			elapsedMs: payload.elapsedMs,
			toolCalls: payload.toolCalls,
			outputTokens: payload.outputTokens,
			...(payload.activity
				? { activity: payload.activity }
				: existing?.activity
					? { activity: existing.activity }
					: {}),
			observed: true,
		});
		return true;
	}

	/** Ingest a chat message; only a final message terminates a turn. */
	final(message: ChatMessagePayload): boolean {
		if (!message.final) return false;
		const at = this.#now();
		const existing = this.#turns.get(message.turnId);
		this.#turns.set(message.turnId, {
			turnId: message.turnId,
			origin: message.origin,
			startedAt: existing?.startedAt ?? at,
			lastEventAt: at,
			elapsedMs: existing ? at - existing.startedAt : 0,
			toolCalls: existing?.toolCalls ?? 0,
			outputTokens: existing?.outputTokens ?? 0,
			...(existing?.activity ? { activity: existing.activity } : {}),
			observed: existing?.observed ?? false,
			finishedAt: at,
			outcome: outcomeOf(message),
			...(message.deliveryId === undefined ? {} : { deliveryId: message.deliveryId }),
		});
		return true;
	}

	/** Drop finished turns past the retention window. Returns true when something was dropped. */
	prune(): boolean {
		const at = this.#now();
		let dropped = false;
		for (const [turnId, turn] of this.#turns) {
			if (turn.finishedAt !== undefined && at - turn.finishedAt > TURN_RETENTION_MS) {
				this.#turns.delete(turnId);
				dropped = true;
			}
		}
		return dropped;
	}

	/** Forget everything: the stream dropped, so nothing known is trustworthy as live. */
	clear(): void {
		this.#turns.clear();
	}

	stateOf(turn: TrackedTurn): TurnState {
		if (turn.finishedAt !== undefined) return "finished";
		return this.#now() - turn.lastEventAt > TURN_STALL_MS ? "stalled" : "running";
	}

	/** Running and stalled turns first (oldest first), then finished ones newest first. */
	list(): readonly TrackedTurn[] {
		const live: TrackedTurn[] = [];
		const done: TrackedTurn[] = [];
		for (const turn of this.#turns.values()) (turn.finishedAt === undefined ? live : done).push(turn);
		live.sort((a, b) => a.startedAt - b.startedAt);
		done.sort((a, b) => (b.finishedAt ?? 0) - (a.finishedAt ?? 0));
		return [...live, ...done];
	}

	get activeCount(): number {
		let count = 0;
		for (const turn of this.#turns.values()) if (turn.finishedAt === undefined) count += 1;
		return count;
	}
}
