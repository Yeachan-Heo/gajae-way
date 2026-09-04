/**
 * The outbound control-token vocabulary — the single place every token the
 * persona may write is spelled.
 *
 * Four tokens steer a reply: `[SILENT]` (and its `NO_REPLY` spellings) suppress
 * it, `[BREAK]` splits it into human-sized parts, `[REPLY:<message id>]` routes
 * it at one message, and `[REACT:<emoji>]` turns it into a platform reaction.
 * All four are internal protocol: on every platform-facing path they are
 * honoured wherever they appear in a part and are never visible in delivered
 * text, in a voice reply, or in a monitor note.
 *
 * That invariant is stated here rather than at each call site because every time
 * one path anchored a token at the start of a message while another matched it
 * anywhere, the room got the raw syntax. Live examples, in order of discovery:
 * `'...nothing to add.\n\n[SILENT]'` posted verbatim, then a narration line
 * before `[REPLY:...]` delivering both the aside and the wrong routing, then the
 * same shape before `[REACT:👍]`, then a monitor note that went out reading
 * `'[REPLY:1544933092823928892] ... [BREAK] [REPLY:...] ...'` in one unthreaded
 * message. The answer is one vocabulary, one scanner, and no token spelling
 * anywhere else in the codebase.
 *
 * Matching is case-insensitive for every token, for the same reason
 * `isSilenceToken` unbrackets: the spelling drifts, the intent does not.
 *
 * Two exceptions are deliberate:
 *
 * - The LOOPBACK console is not a platform surface. It delivers the raw reply so
 *   an operator can see exactly what the model produced, so tokens are neither
 *   honoured nor stripped there (only silence still suppresses the turn).
 * - A MONITOR note has no reaction target, so that path honours `[REPLY:<id>]`
 *   and `[BREAK]` but never a reaction.
 *
 * The GRAMMAR is deliberately narrow, and the two ways of falling outside it are
 * treated differently:
 *
 * - An argument may not contain the whitespace its own token rejects: never a
 *   line break, and for `[REPLY:...]` no whitespace at all. A closed fragment
 *   that breaks this is not a token — it cannot be honoured — but it must not be
 *   delivered either. A short one is deleted outright; a long one keeps its text
 *   and loses only its `[NAME:` prefix, because deleting paragraphs of a real
 *   answer is the worse failure while leaving the syntax visible is still a leak.
 * - An UNCLOSED fragment (`[REACT:👍` with no bracket at all) is
 *   indistinguishable from prose and ships as written.
 */

/**
 * Silence tokens (spec fact 22, Hermes pattern): a part carrying one of these
 * is suppressed while the turn stays in the session transcript.
 *
 * Matching is bracket-insensitive. `[SILENT]` was the only bracketed spelling in
 * the original list, so an owner or persona writing the equally natural
 * `[NO_REPLY]` produced a literal message in the room instead of silence.
 * Brackets are decoration, not meaning: strip one optional surrounding pair
 * before comparing.
 */
export const SILENCE_TOKENS = ["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY"] as const;

/**
 * How far the sweep of a grammar-breaking fragment may DELETE.
 *
 * An unbounded delete would let one mistyped `[REACT:` swallow every paragraph up
 * to the next `]` in a real answer. Past the bound only the `[NAME:` prefix is
 * removed, so the text survives and the syntax still does not: an earlier version
 * left the whole fragment in place, and a pile of nested over-bound wrappers then
 * shipped raw once inner layers were deleted.
 */
const BROKEN_TOKEN_MAX_CHARS = 200;

/**
 * Platform message ids are opaque to us, but they are not arbitrary strings:
 * Discord snowflakes and Telegram message ids are short and alphanumeric.
 * Bounding them here keeps a hostile or hallucinated id from becoming an
 * oversized frame or from smuggling newlines into anything that renders an id.
 *
 * It lives beside the tokens because both tokens that carry an id — a reaction
 * target and a reply target — must agree on what an id may be. `[REPLY:...]`
 * routing metadata went out unvalidated and unbounded until it did.
 */
const PLATFORM_MESSAGE_ID = /^[A-Za-z0-9._:-]{1,64}$/;

export function isPlatformMessageId(value: string): boolean {
	return PLATFORM_MESSAGE_ID.test(value);
}

function unbracket(text: string): string {
	return text.startsWith("[") && text.endsWith("]") && text.length > 2 ? text.slice(1, -1).trim() : text;
}

function escapeForRegExp(text: string): string {
	return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Any silence token in its bracketed spelling, anywhere in the text. Built from
 * the list so a new spelling cannot be added without containment learning it.
 */
const BRACKETED_SILENCE = new RegExp(
	`\\[\\s*(?:${[...new Set(SILENCE_TOKENS.map((token) => escapeForRegExp(unbracket(token))))].join("|")})\\s*\\]`,
	"i",
);

/** True when the whole text is nothing but a silence token. */
export function isSilenceToken(text: string): boolean {
	const normalized = unbracket(text.trim()).toUpperCase();
	return (SILENCE_TOKENS as readonly string[]).some((token) => unbracket(token).toUpperCase() === normalized);
}

/**
 * True when the text carries a silence token at all: bracketed anywhere, or
 * bare on a line of its own.
 *
 * Models routinely wrap the token in a reasoning preamble, so silence is decided
 * by what a part CONTAINS, not by what it equals. An UNBRACKETED token only
 * counts when it owns its line: `SILENT` inside a sentence is a word, and eating
 * a real answer is worse than a leak.
 *
 * A BRACKETED silence token smuggled inside a grammar-breaking fragment is not a
 * directive: the fragment's sweep swallows it, so it never reaches this check.
 * A BARE one is different and deliberately still counts. `[REPLY:bad x]\nSILENT\n…`
 * leaves that `SILENT` owning its own line once the fragment is deleted, and
 * widening the sweep far enough to eat it would eat the prose next to it too —
 * which is a regression this code already shipped once. So the reply is silenced,
 * exactly as it would be if the persona had written the bare token on its own.
 */
export function containsSilenceToken(text: string): boolean {
	if (BRACKETED_SILENCE.test(text)) return true;
	// Every line terminator ends a line, not just LF: `preamble\rSILENT` owns its
	// line exactly as `preamble\nSILENT` does, and splitting on `\r?\n` alone
	// delivered the word SILENT into the room as text.
	return text.split(/\r\n|[\n\r\u2028\u2029]/).some((line) => line.trim().length > 0 && isSilenceToken(line));
}

/**
 * What the scanner found: a real token, garbage that must not be delivered, or —
 * when the garbage is too long to delete safely — just its control prefix.
 */
type SpanKind = "react" | "reply" | "break" | "broken" | "prefix";

interface Span {
	readonly kind: SpanKind;
	/** Index of the opening `[`. */
	readonly start: number;
	/** Index just past the closing `]`. */
	readonly end: number;
	/** Raw argument between `:` and `]`; empty for `[BREAK]`. */
	readonly argument: string;
}

const REACT_PREFIX = "[REACT:";
const REPLY_PREFIX = "[REPLY:";
const BREAK_TOKEN = "[BREAK]";

function isLineTerminator(code: number): boolean {
	return code === 0x0a || code === 0x0d || code === 0x2028 || code === 0x2029;
}

/** JS `\s`, without paying for a regex per character. */
function isWhitespace(code: number): boolean {
	return (
		code === 0x20 ||
		(code >= 0x09 && code <= 0x0d) ||
		code === 0xa0 ||
		code === 0x1680 ||
		(code >= 0x2000 && code <= 0x200a) ||
		code === 0x2028 ||
		code === 0x2029 ||
		code === 0x202f ||
		code === 0x205f ||
		code === 0x3000 ||
		code === 0xfeff
	);
}

/**
 * Finds every control span in one linear pass.
 *
 * This is a hand-written scanner rather than a global regex on purpose. Matching
 * a token ANYWHERE in the message (which is the whole point — a leading anchor is
 * what leaked in the first place) made the regex form quadratic on a reply full
 * of unterminated `[REACT:` prefixes: measured 0.5s at 70k characters and 2.1s at
 * 140k, which is the Bun event loop stalled by one hostile or runaway reply.
 *
 * Linear because the index of every `]`, line terminator and whitespace run is
 * collected once, and the cursors into those lists only ever move forward: a
 * rejected candidate costs O(1) instead of another scan to the end of the text.
 * An argument therefore stays unbounded, so a 10000-character message id is
 * still recognised and stripped instead of being posted into the room.
 */
function scanControlSpans(text: string): readonly Span[] {
	const openers: number[] = [];
	const closers: number[] = [];
	const lineFeeds: number[] = [];
	const whitespace: number[] = [];
	for (let index = 0; index < text.length; index++) {
		const code = text.charCodeAt(index);
		if (code === 0x5b) openers.push(index);
		else if (code === 0x5d) closers.push(index);
		if (code === 0x0a) lineFeeds.push(index);
		if (isWhitespace(code)) whitespace.push(index);
	}
	const spans: Span[] = [];
	let closerCursor = 0;
	let lineFeedCursor = 0;
	let whitespaceCursor = 0;
	/** First entry at or after `from`; cursors never rewind, so this is amortized O(1). */
	const firstAtOrAfter = (list: readonly number[], cursor: number, from: number): number => {
		let moved = cursor;
		while (moved < list.length && (list[moved] as number) < from) moved++;
		return moved;
	};
	for (const opener of openers) {
		// Skip openers swallowed by a span already accepted.
		if (spans.length > 0 && opener < (spans[spans.length - 1] as Span).end) continue;
		const head = text.slice(opener, opener + 7).toUpperCase();
		if (head === BREAK_TOKEN) {
			spans.push({ kind: "break", start: opener, end: opener + BREAK_TOKEN.length, argument: "" });
			continue;
		}
		const isReact = head === REACT_PREFIX;
		if (!isReact && head !== REPLY_PREFIX) continue;
		const argumentStart = opener + 7;
		closerCursor = firstAtOrAfter(closers, closerCursor, argumentStart);
		// No closing bracket left anywhere: no later candidate can find one either,
		// which is what keeps a run of unterminated prefixes from being rescanned.
		if (closerCursor >= closers.length) break;
		const closer = closers[closerCursor] as number;
		const argument = text.slice(argumentStart, closer);
		lineFeedCursor = firstAtOrAfter(lineFeeds, lineFeedCursor, argumentStart);
		whitespaceCursor = firstAtOrAfter(whitespace, whitespaceCursor, argumentStart);
		const hasLineFeed = lineFeedCursor < lineFeeds.length && (lineFeeds[lineFeedCursor] as number) < closer;
		const hasWhitespace = whitespaceCursor < whitespace.length && (whitespace[whitespaceCursor] as number) < closer;
		// Each token rejects exactly the whitespace ITS OWN grammar rejects, and
		// nothing more. A REACT argument may hold spaces, tabs and even a stray CR —
		// those only make the emoji unresolvable, so it stays a recognised token that
		// is logged and stripped — while a line feed genuinely ends the token. A REPLY
		// argument may hold no whitespace at all, so any of it breaks the token: CR,
		// tab, form feed and the unicode line separators included, which is how
		// `[REPLY:bad\rargument]` was still reaching the room.
		const broken = isReact ? hasLineFeed : hasWhitespace || argument.length === 0;
		if (!broken) {
			spans.push({ kind: isReact ? "react" : "reply", start: opener, end: closer + 1, argument });
			continue;
		}
		// A broken fragment ends at its first bracket — except when that bracket is
		// itself part of smuggled control syntax. `[REACT:bad\n[BREAK]\n[SILENT]\nx]`
		// closes on the `[BREAK]`, which left a bare `[SILENT]` behind and deleted the
		// persona's whole reply as if it had asked for silence. So the span is extended
		// past a further bracket ONLY when the text in between carries control syntax:
		// swallowing garbage is right, swallowing the answer is not.
		const end = extendOverSmuggled(text, closers, closerCursor, argumentStart, closer) + 1;
		// Short enough to delete outright; otherwise only the control prefix goes, so
		// the text survives without the syntax. Deleting a long span whole is what the
		// bound protects against, and leaving it whole is what leaked.
		if (end - 1 - argumentStart <= BROKEN_TOKEN_MAX_CHARS) spans.push({ kind: "broken", start: opener, end, argument });
		else spans.push({ kind: "prefix", start: opener, end: argumentStart, argument });
	}
	return spans;
}

/**
 * The furthest closing bracket this broken fragment should own: the last one
 * within the delete bound whose intervening text is nothing but more control
 * syntax. Returns `closer` unchanged when the next bracket guards real text.
 */
function extendOverSmuggled(
	text: string,
	closers: readonly number[],
	cursor: number,
	argumentStart: number,
	closer: number,
): number {
	let end = closer;
	for (let index = cursor + 1; index < closers.length; index++) {
		const candidate = closers[index] as number;
		if (candidate - argumentStart > BROKEN_TOKEN_MAX_CHARS) break;
		const between = text.slice(end + 1, candidate + 1);
		if (!SMUGGLED_CONTROL.test(between)) break;
		end = candidate;
	}
	return end;
}

/**
 * A fragment's tail made of nothing but COMPLETE bracketed control tokens.
 *
 * The brackets are required on both sides. An earlier version made them optional
 * and so treated ordinary prose that merely began `REACT:` as smuggled syntax:
 * `[REPLY:bad x] REACT: this is a real answer]` was reduced to nothing but its
 * tail. Only a fully bracketed token may be swallowed on the way to a further
 * closing bracket.
 */
const SMUGGLED_CONTROL = new RegExp(
	`^(?:\\s*\\[(?:${[...new Set(SILENCE_TOKENS.map((token) => escapeForRegExp(unbracket(token))))].join("|")}|BREAK|REACT:[^\\]]*|REPLY:[^\\]]*)\\]\\s*)+$`,
	"i",
);

/** Every `[REACT:...]` occurrence, in order, with its raw argument. */
export function reactionTokens(text: string): readonly string[] {
	return scanControlSpans(text)
		.filter((span) => span.kind === "react")
		.map((span) => span.argument);
}

/**
 * The first `[REPLY:<message id>]` target in the text that a platform could
 * actually be asked to thread, if any.
 *
 * The id is VALIDATED here, not just extracted. A monitor note carrying a
 * 100000-character target put that whole string into outbound delivery metadata:
 * the visible text was clean, but the payload was junk and the delivery could
 * never land. An unusable target is dropped and the reply is delivered unthreaded
 * — the same trade as a reaction: a bad token costs the routing, never the reply.
 *
 * Nesting follows the delivered text, and that split is deliberate:
 *
 * - Inside a SHORT broken fragment, which is deleted whole, a nested token goes
 *   with it — neither honoured nor delivered. Reading routing out of the middle of
 *   garbage is how a malformed monitor note got threaded at an id it never meant.
 * - Inside an OVER-BOUND fragment, only the `[NAME:` prefix is removed and the
 *   body stays as text, so a nested token is still honoured and stripped. It has
 *   to be: whatever survives into the room must be sanitized, and sanitizing a
 *   token means consuming it.
 */
export function replyTarget(text: string): string | undefined {
	return scanControlSpans(text).find((span) => span.kind === "reply" && isPlatformMessageId(span.argument))?.argument;
}

/**
 * Splits a message on `[BREAK]` lines into the parts the persona intended.
 *
 * Only a token that owns its line splits; one written inline is stripped as
 * ordinary control syntax by `stripControlTokens`, because a mid-sentence
 * `[BREAK]` is a mistake, not a part boundary.
 */
export function breakParts(text: string): readonly string[] {
	const parts: string[] = [];
	let cut = 0;
	for (const span of scanControlSpans(text)) {
		if (span.kind !== "break") continue;
		// Owning its line means: only whitespace back to a line terminator. CRLF is
		// ONE terminator, not two, or the split left a stray `\r` on the part before.
		let before = span.start;
		while (before > 0 && isWhitespace(text.charCodeAt(before - 1)) && !isLineTerminator(text.charCodeAt(before - 1)))
			before--;
		if (before === 0 || !isLineTerminator(text.charCodeAt(before - 1))) continue;
		let boundary = before - 1;
		if (text.charCodeAt(boundary) === 0x0a && boundary > 0 && text.charCodeAt(boundary - 1) === 0x0d) boundary--;
		let after = span.end;
		while (after < text.length && isWhitespace(text.charCodeAt(after)) && !isLineTerminator(text.charCodeAt(after)))
			after++;
		if (after < text.length && isLineTerminator(text.charCodeAt(after))) {
			const consumed = text.charCodeAt(after);
			after++;
			if (consumed === 0x0d && after < text.length && text.charCodeAt(after) === 0x0a) after++;
		}
		parts.push(text.slice(cut, boundary));
		cut = after;
	}
	parts.push(text.slice(cut));
	return parts;
}

/**
 * Removes every `[REACT:...]` occurrence, together with the whitespace around
 * it so the words either side do not fuse. Used by the reaction parser.
 */
export function stripReactionTokens(text: string): string {
	return removeUntilStable(text, (kind) => kind === "react");
}

/**
 * Removes closed fragments that broke the grammar, and nothing else.
 *
 * Exposed separately because ORDER matters on every delivery path. A fragment can
 * hide a line-owned `[BREAK]` or a `[SILENT]` inside its argument
 * (`[REACT:bad\n[BREAK]\nargument]`, `[REPLY:bad\r[SILENT]\rargument]`), so a
 * caller that splits, judges silence, or reads a reply target BEFORE sweeping
 * either posted both halves raw or, worse, threw the persona's real answer away
 * as if it had asked for silence. Sweep the whole text first, then interpret it.
 */
export function stripBrokenTokens(text: string): string {
	return removeUntilStable(text, (kind) => kind === "broken" || kind === "prefix");
}

/**
 * Removes every routing/reaction/part token from text that is about to be
 * delivered, plus any closed fragment that broke the grammar. Silence is NOT
 * stripped: a part carrying a silence token is dropped whole by
 * `containsSilenceToken`, never trimmed into a message.
 */
export function stripControlTokens(text: string): string {
	return removeUntilStable(text, () => true);
}

/**
 * Strips repeatedly until the text stops changing.
 *
 * One pass is a reduction, not a fixed point: deleting a span can pull a closing
 * bracket into range of an earlier fragment that was previously past the sweep
 * bound, so a second pass legitimately finds more. That mattered because the
 * delivery ledger strips again at its own boundary — without converging here,
 * the text stored for redelivery could differ from the text already sent.
 *
 * Each round strictly shortens the text, so this terminates; the cap is only
 * there so a future grammar change cannot turn this into a spin.
 */
function removeUntilStable(text: string, wanted: (kind: SpanKind) => boolean): string {
	let current = remove(text, wanted);
	for (let round = 0; round < 8; round++) {
		const next = remove(current, wanted);
		if (next === current) return current;
		current = next;
	}
	return current;
}

/**
 * Deletes the selected spans together with the whitespace around them, and puts
 * back as much line structure as was consumed.
 *
 * A single space was not enough: the whitespace around a token can be the very
 * line break that makes the next `[BREAK]` own its line, so collapsing it fused
 * two intentional messages into one. The replacement is the WIDER of the two
 * sides, not their sum — a token alone on its line has a break before and after,
 * but it separated one line from the next, not two paragraphs.
 */
function remove(text: string, wanted: (kind: SpanKind) => boolean): string {
	const spans = scanControlSpans(text).filter((span) => wanted(span.kind));
	if (spans.length === 0) return text.trim();
	let out = "";
	let cut = 0;
	for (const span of spans) {
		let before = span.start;
		while (before > cut && isWhitespace(text.charCodeAt(before - 1))) before--;
		let after = span.end;
		while (after < text.length && isWhitespace(text.charCodeAt(after))) after++;
		// CRLF counts once: counting both characters turned one line break into a
		// paragraph break every time the persona's runtime used Windows newlines.
		const lines = (from: number, to: number) => {
			let count = 0;
			for (let index = from; index < to; index++) {
				if (!isLineTerminator(text.charCodeAt(index))) continue;
				count++;
				if (text.charCodeAt(index) === 0x0d && text.charCodeAt(index + 1) === 0x0a) index++;
			}
			return Math.min(count, 2);
		};
		const kept = Math.max(lines(before, span.start), lines(span.end, after));
		out += text.slice(cut, before) + (kept === 0 ? " " : "\n".repeat(kept));
		cut = after;
	}
	return (out + text.slice(cut)).trim();
}
