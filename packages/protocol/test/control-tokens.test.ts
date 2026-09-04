import { describe, expect, test } from "bun:test";
import {
	breakParts,
	containsSilenceToken,
	isSilenceToken,
	replyTarget,
	SILENCE_TOKENS,
	stripBrokenTokens,
	stripControlTokens,
	stripReactionTokens,
} from "../src/control-tokens";

describe("silence containment", () => {
	test("every silence spelling counts, bracketed and anywhere", () => {
		// The gap this closes: containment used to know only `[SILENT]`, so a
		// preamble in front of `[NO_REPLY]` posted the reasoning into the room.
		for (const token of SILENCE_TOKENS) {
			expect(containsSilenceToken(`...nothing to add.\n\n[${token.replace(/^\[|\]$/g, "")}]`)).toBe(true);
			expect(containsSilenceToken(`형님 [${token.replace(/^\[|\]$/g, "")}]`)).toBe(true);
		}
		for (const text of ["[Silent]", "[silent]", "[  NO_REPLY  ]", "[no reply]", "말할 게 없네 [SILENT]"])
			expect(containsSilenceToken(text)).toBe(true);
	});

	test("a bare token counts only when it owns its line", () => {
		expect(containsSilenceToken("판단 끝.\n\nNO_REPLY")).toBe(true);
		expect(containsSilenceToken("  no_reply  ")).toBe(true);
		// Every line terminator ends a line, not just LF. Splitting on `\r?\n` alone
		// delivered the bare word SILENT into the room as text.
		for (const terminator of ["\n", "\r", "\r\n", "\u2028", "\u2029"])
			expect(containsSilenceToken(`preamble${terminator}SILENT`)).toBe(true);
		// A word inside a sentence is a word: eating a real answer is worse than a leak.
		for (const text of ["I will stay silent", "silence", "그 방은 silent 모드야", ""])
			expect(containsSilenceToken(text)).toBe(false);
	});

	test("containment subsumes the exact-match contract", () => {
		for (const token of ["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY", "  [silent]  ", "no_reply"]) {
			expect(isSilenceToken(token)).toBe(true);
			expect(containsSilenceToken(token)).toBe(true);
		}
	});

	test("a reaction token is not silence", () => {
		expect(containsSilenceToken("[REACT:👍]")).toBe(false);
	});
});

describe("control-token stripping", () => {
	test("no token survives into delivered text", () => {
		expect(stripControlTokens("[REPLY:123] 확인했습니다")).toBe("확인했습니다");
		expect(stripControlTokens("확인 [REACT:👍] 했습니다")).toBe("확인 했습니다");
		expect(stripControlTokens("판단 끝.\n\n[REPLY:1544305635179495434] 혼난거 아니고")).toBe(
			"판단 끝.\n\n혼난거 아니고",
		);
		expect(stripControlTokens("한 줄 [BREAK] 두 줄")).toBe("한 줄 두 줄");
		// Case-insensitive, exactly like the silence tokens.
		expect(stripControlTokens("[reply:9] [react:🔥] [break] 넵")).toBe("넵");
	});

	test("stripping is total: nothing bracketed is left behind", () => {
		for (const text of [
			"[REPLY:1][REACT:👍][BREAK] body",
			"body [REACT:nonsense]",
			"[REACT:🚀] launch",
			"위에 한 줄\n[REACT:👍@42]\n아래 한 줄",
		]) {
			const stripped = stripControlTokens(text);
			expect(stripped).not.toContain("[REACT");
			expect(stripped).not.toContain("[REPLY");
			expect(stripped).not.toContain("[BREAK");
		}
	});

	test("prose that merely looks like a token is untouched", () => {
		for (const text of ["[REACT:👍", "reply to me", "[]", "a [ b ] c"]) expect(stripControlTokens(text)).toBe(text);
	});

	test("the reaction-only stripper leaves routing tokens for the caller", () => {
		expect(stripReactionTokens("[REPLY:12] [REACT:👍] 넵")).toBe("[REPLY:12] 넵");
	});

	test("stripping puts back the line structure it consumed", () => {
		// The blocker this closes: collapsing the whitespace around a token to a single
		// space ate the newline that makes the next [BREAK] own its line, so two
		// intentional messages were delivered fused into one.
		expect(stripReactionTokens("one\n[REACT:👍]\n[BREAK]\ntwo")).toBe("one\n[BREAK]\ntwo");
		expect(breakParts(stripReactionTokens("one\n[REACT:👍]\n[BREAK]\ntwo"))).toEqual(["one", "two"]);
		// A paragraph break survives for the same reason; an inline token still yields a space.
		expect(stripControlTokens("첫 문단\n\n[REACT:👍]\n\n둘째 문단")).toBe("첫 문단\n\n둘째 문단");
		expect(stripControlTokens("한 줄\n[REPLY:9]\n다음 줄")).toBe("한 줄\n다음 줄");
		expect(stripControlTokens("확인 [REACT:👍] 했습니다")).toBe("확인 했습니다");
	});

	test("a closed fragment that breaks the token grammar is swept, not delivered", () => {
		// It cannot be honoured — the argument is no legal emoji or message id — but
		// leaving it in place posted raw syntax into chat, voice and monitor notes.
		for (const text of [
			"[REACT:bad\nargument] body",
			"[REPLY:bad\nargument] body",
			"[react:x\ny] body",
			// A REPLY argument may hold NO whitespace, so every separator its grammar
			// rejects breaks it. A newline-only sweep let CR, tab, form feed, a plain
			// space and the unicode line separators sail straight through.
			"[REPLY:bad\rargument] body",
			"[REPLY:bad\targument] body",
			"[REPLY:bad\fargument] body",
			"[REPLY:bad argument] body",
			"[REPLY:bad\u2028argument] body",
			"[REPLY:bad\u2029argument] body",
		]) {
			const stripped = stripControlTokens(text);
			expect(stripped).toBe("body");
			expect(stripped).not.toContain("[");
		}
		// A REACT argument MAY hold spaces and tabs, so those do not make it broken:
		// it is a recognised-but-unusable token, and the token pass removes it.
		expect(stripControlTokens("[REACT:👍@ ] body")).toBe("body");
		// ...and neither does a stray CR: it only makes the emoji unresolvable, so the
		// token is still recognised, logged and stripped. Tightening this to "any line
		// terminator" pushed a 500-character CR argument over the sweep bound and put
		// the whole thing back in the room.
		expect(stripControlTokens(`[REACT:x\r${"9".repeat(500)}] body`)).toBe("body");
		expect(stripControlTokens("[REACT:x\u2028y] body")).toBe("body");
	});

	test("an over-bound fragment keeps its text but still loses its prefix", () => {
		// The bound limits only how much may be DELETED: one broken `[REACT:` plus a
		// bracket three paragraphs later must not take the paragraphs with it. Leaving
		// the whole fragment in place was still a leak, and a pile of nested over-bound
		// wrappers shipped raw as soon as the inner layers were deleted.
		const long = `[REACT:${"가".repeat(400)}\n${"나".repeat(400)}] 본문`;
		const stripped = stripControlTokens(long);
		expect(stripped).toBe(`${"가".repeat(400)}\n${"나".repeat(400)}] 본문`);
		expect(stripped).not.toContain("[REACT");
	});

	test("nested over-bound wrappers cannot outlast the sweep", () => {
		const filler = `${"x".repeat(179)} `;
		const nested = `${"[REPLY:".repeat(32)}${filler}[REACT:👍]${"]".repeat(32)} 본문`;
		const stripped = stripControlTokens(nested);
		expect(stripped).not.toContain("[REPLY:");
		expect(stripped).not.toContain("[REACT:");
		// Converged: stripping again changes nothing.
		expect(stripControlTokens(stripped)).toBe(stripped);
	});

	test("the broken-fragment sweep leaves well-formed tokens for the caller", () => {
		// It runs before the [BREAK] split, so it must not touch a valid [REPLY:id]:
		// stripping the routing target before the caller read it would silently
		// unthread every reply.
		expect(stripBrokenTokens("[REPLY:123] 확인")).toBe("[REPLY:123] 확인");
		expect(stripBrokenTokens("[REACT:👍] 확인")).toBe("[REACT:👍] 확인");
		expect(stripBrokenTokens("[REACT:bad\nargument] 확인")).toBe("확인");
		expect(stripBrokenTokens("[REPLY:bad\rargument] 확인")).toBe("확인");
		expect(stripBrokenTokens("[REPLY:bad argument] 확인")).toBe("확인");
		// A REACT argument legitimately holds a space, so the sweep must leave it for
		// the reaction parser instead of deciding it is broken.
		expect(stripBrokenTokens("[REACT:👍@ ] 확인")).toBe("[REACT:👍@ ] 확인");
	});

	test("a broken fragment that hides a [BREAK] is swept before the split, not after", () => {
		// Splitting first cut the fragment in half, so neither part still held a whole
		// fragment and both halves were delivered raw.
		const hostile = "[REACT:bad\n[BREAK]\nargument] 본문";
		expect(breakParts(stripBrokenTokens(hostile)).map((part) => stripControlTokens(part))).toEqual(["argument] 본문"]);
		for (const part of breakParts(stripBrokenTokens(hostile))) expect(part).not.toContain("[REACT");
	});

	test("a broken fragment cannot smuggle a silence token and delete a real answer", () => {
		// The worst failure this closes: the fragment closed early on its own inner
		// `[BREAK]`, leaving a bare `[SILENT]` in the part, so containment threw the
		// persona's whole reply away — the markers either side of the garbage included.
		const deliver = (text: string) =>
			breakParts(stripBrokenTokens(text))
				.map((part) => part.trim())
				.filter((part) => part.length > 0 && !containsSilenceToken(part))
				.map((part) => stripControlTokens(part));
		expect(deliver("앞 문장\n[REACT:bad\n[BREAK]\n[SILENT]\nargument] 뒤 문장")).toEqual([
			"앞 문장\nargument] 뒤 문장",
		]);
		expect(deliver("앞 문장\n[REPLY:bad\t[BREAK]\t[NO_REPLY]\targument] 뒤 문장")).toEqual([
			"앞 문장\nargument] 뒤 문장",
		]);
		for (const separator of ["\n", "\r", "\t", " ", "\u2028", "\u2029"])
			expect(deliver(`앞 문장\n[REPLY:bad${separator}[SILENT]${separator}argument] 뒤 문장`)).toEqual([
				"앞 문장\nargument] 뒤 문장",
			]);
		// A real silence directive still suppresses, and so does one behind a preamble.
		expect(deliver("형님 [SILENT]")).toEqual([]);
		expect(deliver("할 말 없음.\n\n[NO_REPLY]")).toEqual([]);
	});

	test("the fragment swallows smuggled control syntax but never the answer next to it", () => {
		// Extending past the fragment's first bracket is only allowed while the text in
		// between is more control syntax. Prose that merely contains a bracket keeps it.
		expect(stripControlTokens("[REACT:bad\nx] 본문 [배열] 끝")).toBe("본문 [배열] 끝");
		expect(stripControlTokens("[REACT:bad\nx] 본문]")).toBe("본문]");
		expect(stripControlTokens("[REACT:bad\n[BREAK]\n[SILENT]\nx] 본문 [배열] 끝")).toBe("x] 본문 [배열] 끝");
		// Only a COMPLETE bracketed token may be swallowed. Making the brackets
		// optional ate ordinary prose that merely began `REACT:`.
		expect(stripControlTokens("[REPLY:bad x] REACT: this is a real answer] tail")).toBe(
			"REACT: this is a real answer] tail",
		);
		expect(stripControlTokens("[REPLY:bad x] REPLY: 실제 답변입니다] 끝")).toBe("REPLY: 실제 답변입니다] 끝");
	});

	test("nesting follows the delivered text, and the two fragment kinds differ", () => {
		// Inside a SHORT broken fragment, which is deleted whole, a nested token goes
		// with it: routing must never be read out of the middle of garbage.
		expect(stripControlTokens("[REPLY:bad x][REPLY:evil-target] 본문")).toBe("본문");
		expect(replyTarget("[REPLY:bad x][REPLY:evil-target] 본문")).toBeUndefined();
		// Inside an OVER-BOUND fragment only the prefix goes, so the body stays as
		// text — and a token surviving in that text must still be consumed, or its
		// syntax would reach the room.
		const overBound = `[REPLY:${"x ".repeat(101)}[REPLY:good-target]] 본문`;
		expect(replyTarget(overBound)).toBe("good-target");
		expect(stripControlTokens(overBound)).not.toContain("[REPLY:");
	});

	test("a bare silence token surviving a swept fragment still silences", () => {
		// It owns its own line once the fragment is deleted, so it means what a bare
		// token always means. Widening the sweep to eat it would eat the prose beside
		// it — the regression this code already shipped once.
		expect(containsSilenceToken("[REPLY:bad x]\nNO_REPLY\nargument] visible")).toBe(true);
		// A BRACKETED one is swallowed with the fragment instead, so it never counts.
		expect(containsSilenceToken(stripBrokenTokens("[REPLY:bad x][SILENT] 본문"))).toBe(false);
	});

	test("a reply full of unterminated prefixes cannot stall the event loop", () => {
		// Matching a token ANYWHERE made the old global-regex form quadratic: a reply
		// of unterminated `[REACT:` prefixes measured 0.5s at 70k characters and 13.3s
		// at 350k, which is the gateway's event loop stopped by one runaway reply. The
		// linear scanner does the same 350k in single-digit milliseconds.
		for (const hostile of ["[REACT:".repeat(50_000), `${"[REACT:".repeat(50_000)}\n]`]) {
			const started = performance.now();
			stripControlTokens(hostile);
			expect(performance.now() - started).toBeLessThan(500);
		}
	});
});

describe("routing and part tokens", () => {
	test("a reply target is found wherever it appears", () => {
		expect(replyTarget("[REPLY:123] 확인")).toBe("123");
		expect(replyTarget("판단 끝.\n\n[REPLY:456] 확인")).toBe("456");
		expect(replyTarget("[reply:789] 확인")).toBe("789");
		expect(replyTarget("no target here")).toBeUndefined();
	});

	test("only a break token that owns its line splits the message", () => {
		expect(breakParts("first\n[BREAK]\nsecond")).toEqual(["first", "second"]);
		expect(breakParts("first\n  [break]  \nsecond")).toEqual(["first", "second"]);
		expect(breakParts("inline [BREAK] token")).toEqual(["inline [BREAK] token"]);
	});
});
