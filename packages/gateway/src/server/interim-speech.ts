/**
 * Mid-work speech: what the persona says while a turn is still running.
 *
 * Every assistant message the tail observes before the turn's final answer is
 * delivered, with one exception - a near-repeat of the message just delivered.
 *
 * This module used to be a gate (issue #71): a structural "nothing before the
 * first tool" rule, a per-language narration classifier, a per-turn cap of 2
 * and a 45-second spacing rule. Measured 2026-09-18 on the live deployment,
 * that gate suppressed 232 of 232 mid-turn messages in a day, and in one
 * thread the user sent four messages in 70 s, the persona answered each with a
 * `[REPLY:…]` part within seconds, and the gate delivered one of them two
 * minutes late and dropped the other three. A reply the user is waiting for
 * cannot be "narration", and a rule that cannot tell the two apart is not a
 * guard, it is a hole. The persona's own instructions decide what it says
 * mid-turn; the gateway delivers it.
 *
 * Duplicate suppression stays because it removes a failure the model does
 * produce (the same status line re-emitted with a bumped counter) and never
 * removes an answer.
 */

export type InterimSuppressionReason =
	/** Nothing left after trimming. */
	| "empty"
	/** Near-identical to the previous mid-work message of this turn. */
	| "duplicate";

export type InterimSpeechDecision =
	| { readonly deliver: true }
	| { readonly deliver: false; readonly reason: InterimSuppressionReason };

/** Punctuation/whitespace/case-insensitive form used for near-duplicate comparison. */
function normalized(text: string): string {
	return text
		.toLowerCase()
		.replace(/[\s\p{P}\p{S}]+/gu, "")
		.trim();
}

/**
 * Near-identical means: identical after normalization, or one is a prefix of
 * the other and the shorter covers at least 80% of the longer. That catches the
 * observed failure (the same status line re-emitted with a bumped counter)
 * without needing edit-distance machinery.
 */
export function isNearDuplicate(previous: string, next: string): boolean {
	const a = normalized(previous);
	const b = normalized(next);
	if (!a || !b) return false;
	if (a === b) return true;
	const [shorter, longer] = a.length <= b.length ? [a, b] : [b, a];
	if (!longer.startsWith(shorter)) return false;
	return shorter.length / longer.length >= 0.8;
}

/** Per-turn state: only the last delivered text, for duplicate suppression. */
export class InterimSpeechGate {
	#delivered = 0;
	#lastDeliveredText: string | undefined;

	/**
	 * Decides whether one streamed assistant message ships to the channel.
	 * Only call this for MID-WORK messages: the final answer is delivered by the
	 * caller unconditionally and must never pass through here.
	 */
	admit(text: string): InterimSpeechDecision {
		const trimmed = text.trim();
		if (!trimmed) return { deliver: false, reason: "empty" };
		if (this.#lastDeliveredText !== undefined && isNearDuplicate(this.#lastDeliveredText, trimmed))
			return { deliver: false, reason: "duplicate" };
		this.#delivered++;
		this.#lastDeliveredText = trimmed;
		return { deliver: true };
	}

	get deliveredCount(): number {
		return this.#delivered;
	}
}
