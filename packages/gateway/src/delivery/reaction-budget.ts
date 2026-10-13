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
 * A remove-then-add transition (e.g. fire -> check) does not count the add against
 * REACTIONS_PER_MESSAGE_CAP: removal frees the slot. REACTIONS_PER_TURN_CAP is
 * still enforced, as is the allowlist and duplicate detection.
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
	/** Tracks reactions added by the persona for authorization on remove. */
	readonly #owned = new Map<string, Set<string>>();
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
		readonly emojiName: string;
	}): ReactionRejection | undefined {
		const messageKey = `${input.originKey}\u0000${input.targetMessageId}`;
		const emojiKey = input.emoji || input.emojiName;
		const applied = this.#messages.get(messageKey);
		if (applied?.has(emojiKey))
			return {
				reason: "duplicate",
				detail: `${emojiKey} is already applied to message ${input.targetMessageId}`,
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
		this.#remember(this.#messages, messageKey, (applied ?? new Set<string>()).add(emojiKey));
		// Mark as owned by the persona so remove can verify ownership
		this.#remember(this.#owned, messageKey, (this.#owned.get(messageKey) ?? new Set<string>()).add(emojiKey));
		return undefined;
	}

	/**
	 * Releases budget for a removed reaction. Returns true if the reaction was owned
	 * by the persona (already added and tracked), false if the persona cannot remove it.
	 */
	release(input: {
		readonly originKey: string;
		readonly targetMessageId: string;
		readonly emoji: string;
		readonly emojiName: string;
	}): boolean {
		const messageKey = `${input.originKey}\u0000${input.targetMessageId}`;
		const emojiKey = input.emoji || input.emojiName;
		const owned = this.#owned.get(messageKey);
		if (!owned?.has(emojiKey)) return false;
		// Remove from owned and applied
		owned.delete(emojiKey);
		const applied = this.#messages.get(messageKey);
		if (applied) {
			applied.delete(emojiKey);
		}
		return true;
	}

	/** Bounded LRU insert: newest key last, oldest evicted past the limit. */
	#remember<T>(store: Map<string, T>, key: string, value: T): void {
		store.delete(key);
		store.set(key, value);
		if (store.size > this.#limit) store.delete(store.keys().next().value as string);
	}
}
