/**
 * Differential test for the hand-written control-token scanner.
 *
 * The scanner replaced a set of global regexes because matching a token ANYWHERE
 * made the regex form quadratic on hostile input (13.3s for 350k characters of
 * unterminated `[REACT:` prefixes). Speed is worthless if the rewrite changed
 * what counts as a token, and the risky part is not the grammar but the
 * OPTIMIZATION: monotonic cursors into precollected index lists, plus an early
 * break once no closing bracket remains.
 *
 * So the reference here is deliberately the naive version of the same grammar —
 * a plain O(n²) loop with `indexOf` and substring scans, no cursors, no early
 * break. Structurally different, obviously correct, and slow. On randomized
 * input the two must agree exactly; where they disagree, the fast one is wrong.
 *
 * The grammar being modelled, unified across all four tokens (leftmost span
 * wins, and a span consumes everything inside it):
 *
 *   `[BREAK]`
 *   `[REACT:` argument `]`  — broken by a line feed in the argument
 *   `[REPLY:` argument `]`  — broken by ANY whitespace, or an empty argument
 *
 * A broken span is swept rather than honoured, and only up to 200 characters;
 * past that it is left alone so a mistyped token cannot eat a real answer.
 */
import { expect, test } from "bun:test";
import { breakParts, reactionTokens, replyTarget, stripControlTokens } from "../src/control-tokens";

const BROKEN_MAX = 200;

type Kind = "react" | "reply" | "break" | "broken" | "prefix";
interface Span {
	kind: Kind;
	start: number;
	end: number;
	argument: string;
}

/** Naive reference scan: no cursors, no early break, no cleverness. */
function naiveScan(text: string): Span[] {
	const spans: Span[] = [];
	let index = 0;
	while (index < text.length) {
		if (text[index] !== "[") {
			index++;
			continue;
		}
		const head = text.slice(index, index + 7).toUpperCase();
		if (head === "[BREAK]") {
			spans.push({ kind: "break", start: index, end: index + 7, argument: "" });
			index += 7;
			continue;
		}
		const isReact = head === "[REACT:";
		if (!isReact && head !== "[REPLY:") {
			index++;
			continue;
		}
		const argumentStart = index + 7;
		const closer = text.indexOf("]", argumentStart);
		if (closer === -1) {
			index++;
			continue;
		}
		const argument = text.slice(argumentStart, closer);
		const broken = isReact ? argument.includes("\n") : argument.length === 0 || /\s/.test(argument);
		if (!broken) {
			spans.push({ kind: isReact ? "react" : "reply", start: index, end: closer + 1, argument });
			index = closer + 1;
			continue;
		}
		// A broken fragment reaches past its first bracket while the text in between is
		// nothing but more control syntax, so a smuggled `[SILENT]` cannot survive it.
		let end = closer;
		for (;;) {
			const next = text.indexOf("]", end + 1);
			if (next === -1 || next - argumentStart > BROKEN_MAX) break;
			if (
				!/^(?:\s*\[(?:SILENT|NO_REPLY|NO REPLY|BREAK|REACT:[^\]]*|REPLY:[^\]]*)\]\s*)+$/i.test(
					text.slice(end + 1, next + 1),
				)
			)
				break;
			end = next;
		}
		if (end - argumentStart <= BROKEN_MAX) {
			spans.push({ kind: "broken", start: index, end: end + 1, argument });
			index = end + 1;
			continue;
		}
		// Over the delete bound: only the control prefix goes, so the text survives
		// without the syntax.
		spans.push({ kind: "prefix", start: index, end: argumentStart, argument });
		index = argumentStart;
	}
	return spans;
}

function isWhitespace(value: string): boolean {
	return value.length > 0 && /^\s$/.test(value);
}

function isLineTerminator(value: string): boolean {
	return value === "\n" || value === "\r" || value === "\u2028" || value === "\u2029";
}

/** Naive reference for the whitespace-restoring removal. */
function naiveRemove(text: string, wanted: (kind: Kind) => boolean): string {
	const spans = naiveScan(text).filter((span) => wanted(span.kind));
	if (spans.length === 0) return text.trim();
	let out = "";
	let cut = 0;
	for (const span of spans) {
		let before = span.start;
		while (before > cut && isWhitespace(text[before - 1] as string)) before--;
		let after = span.end;
		while (after < text.length && isWhitespace(text[after] as string)) after++;
		const lines = (from: number, to: number) =>
			Math.min(
				(
					text
						.slice(from, to)
						.replace(/\r\n/g, "\n")
						.match(/[\n\r\u2028\u2029]/g) ?? []
				).length,
				2,
			);
		const kept = Math.max(lines(before, span.start), lines(span.end, after));
		out += text.slice(cut, before) + (kept === 0 ? " " : "\n".repeat(kept));
		cut = after;
	}
	return (out + text.slice(cut)).trim();
}

/** Naive reference for splitting on line-owned `[BREAK]`. */
function naiveBreakParts(text: string): string[] {
	const parts: string[] = [];
	let cut = 0;
	for (const span of naiveScan(text)) {
		if (span.kind !== "break") continue;
		let before = span.start;
		while (before > 0 && isWhitespace(text[before - 1] as string) && !isLineTerminator(text[before - 1] as string))
			before--;
		if (before === 0 || !isLineTerminator(text[before - 1] as string)) continue;
		let after = span.end;
		while (after < text.length && isWhitespace(text[after] as string) && !isLineTerminator(text[after] as string))
			after++;
		if (after < text.length && isLineTerminator(text[after] as string)) {
			const consumed = text[after] as string;
			after++;
			if (consumed === "\r" && text[after] === "\n") after++;
		}
		let boundary = before - 1;
		if (text[boundary] === "\n" && boundary > 0 && text[boundary - 1] === "\r") boundary--;
		parts.push(text.slice(cut, boundary));
		cut = after;
	}
	parts.push(text.slice(cut));
	return parts;
}

/** Deterministic PRNG, so a failure is reproducible from its seed. */
function random(seed: number): () => number {
	let state = seed >>> 0;
	return () => {
		state = (state * 1664525 + 1013904223) >>> 0;
		return state / 0x1_0000_0000;
	};
}

const PIECES = [
	"[REACT:",
	"[REPLY:",
	"[BREAK]",
	"[react:",
	"[reply:",
	"[break]",
	"]",
	"[",
	"👍",
	"thumbsup",
	"@1544305635179936821",
	"@",
	":",
	"본문",
	" ",
	"\t",
	"\n",
	"\r\n",
	"\r",
	"\u2028",
	"\u2029",
	"\u0000",
	"[SILENT]",
	"nonsense",
	"x".repeat(210),
	"x",
];

function generate(next: () => number): string {
	const length = 1 + Math.floor(next() * 16);
	let text = "";
	for (let index = 0; index < length; index++) text += PIECES[Math.floor(next() * PIECES.length)] as string;
	return text;
}

test("the optimized scanner agrees with the naive one on reaction tokens", () => {
	const next = random(0x5eed);
	for (let iteration = 0; iteration < 20_000; iteration++) {
		const text = generate(next);
		const expected = naiveScan(text)
			.filter((span) => span.kind === "react")
			.map((span) => span.argument);
		expect(reactionTokens(text), JSON.stringify(text)).toEqual(expected);
	}
});

test("the optimized scanner agrees with the naive one on the reply target", () => {
	const next = random(0xc0ffee);
	for (let iteration = 0; iteration < 20_000; iteration++) {
		const text = generate(next);
		// A target a platform could never be asked to thread is not honoured.
		const expected = naiveScan(text).find(
			(span) => span.kind === "reply" && /^[A-Za-z0-9._:-]{1,64}$/.test(span.argument),
		)?.argument;
		expect(replyTarget(text), JSON.stringify(text)).toBe(expected);
	}
});

test("the optimized stripper agrees with the naive one, and converges", () => {
	const next = random(0xbeef);
	for (let iteration = 0; iteration < 20_000; iteration++) {
		const text = generate(next);
		const stripped = stripControlTokens(text);
		// One naive pass is only a reduction: deleting a span can pull a closing
		// bracket into range of a fragment that was past the sweep bound, so the
		// reference is iterated to the same fixed point the implementation reaches.
		let expected = naiveRemove(text, () => true);
		for (let round = 0; round < 8; round++) {
			const again = naiveRemove(expected, () => true);
			if (again === expected) break;
			expected = again;
		}
		expect(stripped, JSON.stringify(text)).toBe(expected);
		expect(stripControlTokens(stripped), JSON.stringify(text)).toBe(stripped);
	}
});

test("the optimized splitter agrees with the naive one", () => {
	const next = random(0xfeed);
	for (let iteration = 0; iteration < 20_000; iteration++) {
		const text = generate(next);
		expect(breakParts(text), JSON.stringify(text)).toEqual(naiveBreakParts(text));
	}
});
