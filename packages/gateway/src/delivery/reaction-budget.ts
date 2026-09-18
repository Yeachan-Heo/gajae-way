import { REACTIONS_PER_MESSAGE_CAP, REACTIONS_PER_TURN_CAP } from "@gajae-gateway/protocol";

export type ReactionRejection =
	| { readonly reason: "turn_cap"; readonly detail: string }
	| { readonly reason: "message_cap"; readonly detail: string }
	| { readonly reason: "duplicate"; readonly detail: string };

/**
 * Rate safety for outbound reactions, on top of the protocol allowlist.
 *
 * Two caps, both enforced here so the reply-token path and the chat.react verb
 * cannot drift apart: at most REACTIONS_PER_TURN_CAP reactions per turn, and at
 * most REACTIONS_PER_MESSAGE_CAP reactions on any single target message. The same
 * emoji on the same message is a duplicate and is rejected rather than re-sent,
 * because reacting is idempotent on the platform: a second delivery would burn a
 * platform request and a ledger row to achieve nothing.
 *
 * Rejections are returned, never swallowed: the caller turns them into a protocol
 * error (verb path) or a logged error plus verbatim text (reply-token path). A
 * silent no-op is not an option — an owner who never sees the reaction must be
 * able to find out why from the daemon log or the verb response.
 *
 * State is in-memory and bounded: caps protect a live turn and a live message, and
 * a restart legitimately starts fresh (the platform state is idempotent anyway).
 */
export class ReactionBudget {
	readonly #turns = new Map<string, number>();
	readonly #messages = new Map<string, Set<string>>();
	readonly #limit: number;

	constructor(limit = 2_000) {
		this.#limit = limit;
	}

	/**
	 * Claims budget for one reaction. `turnId` is omitted for reactions requested
	 * directly through chat.react, which has no turn to bound; the per-message cap
	 * still applies there.
	 */
	claim(input: {
		readonly turnId?: string;
		readonly originKey: string;
		readonly targetMessageId: string;
		readonly emoji: string;
	}): ReactionRejection | undefined {
		const messageKey = `${input.originKey}\u0000${input.targetMessageId}`;
		const applied = this.#messages.get(messageKey);
		if (applied?.has(input.emoji))
			return {
				reason: "duplicate",
				detail: `${input.emoji} is already applied to message ${input.targetMessageId}`,
			};
		if (applied && applied.size >= REACTIONS_PER_MESSAGE_CAP)
			return {
				reason: "message_cap",
				detail: `message ${input.targetMessageId} already carries ${applied.size} reaction(s), cap is ${REACTIONS_PER_MESSAGE_CAP}`,
			};
		const used = input.turnId ? (this.#turns.get(input.turnId) ?? 0) : 0;
		if (input.turnId && used >= REACTIONS_PER_TURN_CAP)
			return {
				reason: "turn_cap",
				detail: `turn ${input.turnId} already emitted ${used} reaction(s), cap is ${REACTIONS_PER_TURN_CAP}`,
			};
		if (input.turnId) this.#remember(this.#turns, input.turnId, used + 1);
		this.#remember(this.#messages, messageKey, (applied ?? new Set<string>()).add(input.emoji));
		return undefined;
	}

	/** Bounded LRU insert: newest key last, oldest evicted past the limit. */
	#remember<T>(store: Map<string, T>, key: string, value: T): void {
		store.delete(key);
		store.set(key, value);
		if (store.size > this.#limit) store.delete(store.keys().next().value as string);
	}
}
