/**
 * Mid-work speech gate (issue #71).
 *
 * A long agentic turn streams one assistant message per model step, and every
 * one of them used to ship to the channel. Live finding: a single turn arrived
 * as 5 chat messages within 90s (tool counter 6→12→18→26), and the fragments
 * were procedural narration — "채널이랑 직전 지시를 더 볼게요". That reads as a
 * work log, not as a person chatting.
 *
 * The fix is NOT silence: a persona that says nothing for three minutes and
 * then drops a wall of text is just as inhuman. Mid-work speech stays, but it
 * must be what a person actually says in chat: something they found, a short
 * reaction, a heads-up that this will take a while, or a question that unblocks
 * them.
 *
 * DESIGN INTENT: tail-framed assistant text is filtered before outward delivery.
 * This module is the backstop — a model that narrates routine activity must not
 * be able to flood the room. Therefore the rules here are deliberately few and
 * conservative: they suppress only what is unmistakably process narration and
 * default to delivering anything else.
 * Tightening this into an exhaustive regex zoo would start eating real replies,
 * which is the worse failure: a suppressed finding is invisible forever, while
 * a leaked narration line is merely noise.
 */

export interface InterimSpeechLimits {
	/** Hard cap on delivered mid-work messages within one turn. */
	readonly maxPerTurn: number;
	/** Minimum spacing between delivered mid-work messages; the first one is never delayed. */
	readonly minGapMs: number;
}

export const DEFAULT_INTERIM_SPEECH_LIMITS: InterimSpeechLimits = {
	maxPerTurn: 2,
	minGapMs: 45_000,
};

export type InterimSuppressionReason =
	/** Nothing left after trimming. */
	| "empty"
	/** Pure process narration: reading files, checking the channel, querying the DB. */
	| "procedural"
	/** Near-identical to the previous mid-work message of this turn. */
	| "duplicate"
	/** Turn already spent its mid-work budget. */
	| "turn-cap"
	/** Too soon after the previous mid-work message. */
	| "rate"
	/**
	 * Emitted before the turn ran a single tool. A message at that point cannot
	 * contain a finding - nothing has been looked at yet - so it is by
	 * construction the model thinking out loud. This is the language-independent
	 * rule: the content regexes below only ever caught the polite Korean intent
	 * endings, so plain declarative narration ("...판단한다", "...찍고 한 방만 친다")
	 * shipped to the channel unchanged (measured 2026-08-31 in #playground-ko).
	 */
	| "pre-tool";

export type InterimSpeechDecision =
	| { readonly deliver: true }
	| { readonly deliver: false; readonly reason: InterimSuppressionReason };

/** What the turn has done so far, at the moment this message was streamed. */
export interface InterimSpeechContext {
	/** Tool executions started in this turn before this message. */
	readonly toolCallsSoFar?: number;
}

/**
 * Sentence split that works for both languages present in this product.
 * Korean chat sentences frequently carry no terminal punctuation at all, so a
 * newline or `!`/`?`/`…`/`.` is all that is treated as a boundary; an
 * unpunctuated Korean line stays one sentence, which is what we want. A `.`
 * counts only before whitespace or end of text, so "reading server.ts now" is
 * one sentence rather than a filename split in half.
 */
function sentences(text: string): string[] {
	return text
		.split(/[\n!?…]+|\.(?:\s+|$)/)
		.map((part) => part.trim())
		.filter((part) => part.length > 0);
}

/**
 * A sentence is process narration when it announces or narrates INSPECTION
 * work rather than reporting anything. Two rules, one per language family:
 *
 *  - Korean: an inspection verb stem (보다/확인/조회/검색/읽다/살펴보다/점검/찾다/
 *    뒤지다) combined with an intent or in-progress ending (~ㄹ게요, ~겠습니다,
 *    ~려고요, ~중, ~고 있어요). "채널이랑 직전 지시를 더 볼게요" and
 *    "로그부터 보겠습니다" match; "로그에 500이 3분마다 찍혀요" does not, because
 *    it has no inspection verb — it is a finding.
 *  - English: the same shape — an intent marker (let me / I'll / going to /
 *    next / first) or a progressive form in front of an inspection verb
 *    (check / look / read / inspect / review / scan / search / query / grep /
 *    open / examine / dig into / pull up).
 *
 * Anything reporting a RESULT survives, even when it names a file or the DB,
 * because the verb is what distinguishes "I will look at the logs" from "the
 * logs say X".
 */
// 볼/봐/본 are separate syllables from the 보 stem, so the inflected forms are listed.
const KOREAN_INSPECTION = /(보|볼|봐|본|확인|조회|검색|읽|살펴|점검|찾아|뒤져|훑어|파악)/;
const KOREAN_INTENT_ENDING = /(게요|게용|겠습니다|겠어요|겠네요|려고요|려구요|볼까|중이|중입니다|고 있|부터요)/;
/**
 * Plain declarative (반말) narration: "판단한다", "먼저 찍는다", "막는다". Present-tense
 * only - a past form ("확인했다", "봤다") is a report of something already found and
 * must survive. Kept separate from the polite endings above because those were the
 * only shapes the original rule covered.
 */
const KOREAN_PLAIN_INTENT = /(한다|본다|는다|간다|긴다|친다|린다|잡는다|막는다|넣는다)$/;
const KOREAN_PAST = /(았|었|했|봤|왔|졌)다/;
const ENGLISH_INSPECTION =
	/\b(check(ing)?|look(ing)?|read(ing)?|inspect(ing)?|review(ing)?|scan(ning)?|search(ing)?|quer(y|ying)|grep(ping)?|open(ing)?|examin(e|ing)|dig(ging)?|pull(ing)? up|verify(ing)?)\b/i;
const ENGLISH_INTENT =
	/\b(let me|i'?ll|i will|i'?m going to|i am going to|going to|gonna|about to|next|first|now|starting|start(ing)? by|then)\b/i;
const ENGLISH_PROGRESSIVE = /\b(check|look|read|inspect|review|scan|search|quer|grep|open|examin|digg|pull)\w*ing\b/i;
/**
 * "Looking good", "reading fine" are reactions, not narration: a progressive
 * inspection verb followed by an evaluative adjective is a verdict about what
 * was already seen. One explicit carve-out beats loosening the main rule.
 */
const ENGLISH_PROGRESSIVE_VERDICT =
	/\b(check|look|read|inspect|review|scan|search|quer|grep|open|examin|digg|pull)\w*ing\s+(good|great|fine|ok|okay|solid|clean|promising|bad|rough|off|weird|sane)\b/i;

function isProceduralSentence(sentence: string): boolean {
	if (KOREAN_INSPECTION.test(sentence) && KOREAN_INTENT_ENDING.test(sentence)) return true;
	if (KOREAN_INSPECTION.test(sentence) && KOREAN_PLAIN_INTENT.test(sentence) && !KOREAN_PAST.test(sentence))
		return true;
	if (ENGLISH_PROGRESSIVE.test(sentence)) return !ENGLISH_PROGRESSIVE_VERDICT.test(sentence);
	return ENGLISH_INTENT.test(sentence) && ENGLISH_INSPECTION.test(sentence);
}

/**
 * True when the WHOLE message is process narration. A message that mixes a
 * narration line with a real finding is delivered: dropping it would lose the
 * finding, and the narration line costs one clause.
 */
export function isProceduralNarration(text: string): boolean {
	const parts = sentences(text);
	if (parts.length === 0) return false;
	return parts.every((sentence) => isProceduralSentence(sentence));
}

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

/**
 * Per-turn admission state for mid-work speech.
 *
 * KNOWN LIMITATION (issue #71 requirement 3): the intended rule is "do not
 * deliver a mid-work message whose text also appears in the final answer".
 * That is undecidable at delivery time — the final answer does not exist yet,
 * and a message already sent to Discord cannot be recalled. So the implemented
 * approximation is consecutive near-duplicate suppression, which removes the
 * repetition that was actually observed. Overlap between a mid-work message and
 * the later final answer is therefore still possible; the turn cap of 2 bounds
 * how bad that can get. See the duplicate tests for the pinned behavior.
 */
export class InterimSpeechGate {
	readonly #limits: InterimSpeechLimits;
	#delivered = 0;
	#lastDeliveredAt: number | undefined;
	#lastDeliveredText: string | undefined;

	constructor(limits?: Partial<InterimSpeechLimits>) {
		this.#limits = { ...DEFAULT_INTERIM_SPEECH_LIMITS, ...limits };
	}

	/** Number of mid-work messages this turn actually delivered. */
	get deliveredCount(): number {
		return this.#delivered;
	}

	/**
	 * Decides whether one streamed assistant message ships to the channel.
	 * Only call this for MID-WORK messages: the final answer is delivered by the
	 * caller unconditionally and must never pass through here.
	 */
	admit(text: string, nowMs: number, context?: InterimSpeechContext): InterimSpeechDecision {
		const trimmed = text.trim();
		if (!trimmed) return { deliver: false, reason: "empty" };
		// Structural gate first: cheaper than the regexes and it does not care what
		// language or register the narration used.
		if (context?.toolCallsSoFar === 0) return { deliver: false, reason: "pre-tool" };
		// Content before rate: a suppressed narration line must not consume the
		// turn's budget, otherwise four narration steps would starve one real
		// finding arriving later in the same turn.
		if (isProceduralNarration(trimmed)) return { deliver: false, reason: "procedural" };
		if (this.#lastDeliveredText !== undefined && isNearDuplicate(this.#lastDeliveredText, trimmed))
			return { deliver: false, reason: "duplicate" };
		if (this.#delivered >= this.#limits.maxPerTurn) return { deliver: false, reason: "turn-cap" };
		// The first mid-work message is never delayed: silence at the start of a
		// long turn is exactly what this feature exists to avoid.
		if (this.#lastDeliveredAt !== undefined && nowMs - this.#lastDeliveredAt < this.#limits.minGapMs)
			return { deliver: false, reason: "rate" };
		this.#delivered++;
		this.#lastDeliveredAt = nowMs;
		this.#lastDeliveredText = trimmed;
		return { deliver: true };
	}
}
