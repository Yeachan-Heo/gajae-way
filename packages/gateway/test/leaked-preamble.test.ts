import { describe, expect, test } from "bun:test";
import { isSilenceToken } from "@gajaeway/protocol";
import { stripLeakedPreamble } from "../src/delivery/leaked-preamble";

describe("stripLeakedPreamble", () => {
	test("strips the live 2026-09-01 leak before a [REPLY:] token", () => {
		const leaked =
			"형님이 나오라고 부르셨으니 반응은 해야지.\n\n[REPLY:1544305635179495434] 혼난거 아니고 그냥 듣고 있었음";
		expect(stripLeakedPreamble(leaked)).toBe("[REPLY:1544305635179495434] 혼난거 아니고 그냥 듣고 있었음");
	});

	test("a narrated [SILENT] collapses back to silence", () => {
		const leaked = "Directed at 개발가재, not me. Staying out.\n\n[SILENT]";
		const stripped = stripLeakedPreamble(leaked);
		expect(stripped).toBe("[SILENT]");
		expect(isSilenceToken(stripped)).toBe(true);
	});

	test("bracketless and lowercase silence spellings are caught too", () => {
		expect(isSilenceToken(stripLeakedPreamble("아무 말 안 하는게 맞겠다\n\nNO_REPLY"))).toBe(true);
		expect(isSilenceToken(stripLeakedPreamble("이건 내 차례 아님\n\nno reply"))).toBe(true);
	});

	test("strips an English narration line before [REACT:]", () => {
		const leaked = "No fact to add here, an ack is enough.\n\n[REACT:👍] ok";
		expect(stripLeakedPreamble(leaked)).toBe("[REACT:👍] ok");
	});

	test("mid-line tokens stay plain text (RT-TOKEN-02 / RT-TOKEN-04 contract)", () => {
		expect(stripLeakedPreamble("아 이건 답해야지. [REPLY:123] 본문")).toBe("아 이건 답해야지. [REPLY:123] 본문");
		expect(stripLeakedPreamble("확인 [REACT:👍] 했습니다")).toBe("확인 [REACT:👍] 했습니다");
		expect(stripLeakedPreamble("[SILENT][REACT:🔥]")).toBe("[SILENT][REACT:🔥]");
	});

	test("strips a multi-line aside within budget", () => {
		const leaked = "형님이 부르셨다.\n답은 짧게.\n\n[REPLY:9] 넵";
		expect(stripLeakedPreamble(leaked)).toBe("[REPLY:9] 넵");
	});

	test("leaves a clean message untouched", () => {
		expect(stripLeakedPreamble("[REPLY:1] 본문")).toBe("[REPLY:1] 본문");
		expect(stripLeakedPreamble("[SILENT]")).toBe("[SILENT]");
		expect(stripLeakedPreamble("그냥 평범한 답장입니다")).toBe("그냥 평범한 답장입니다");
	});

	test("never eats real content: long block before the token is kept", () => {
		const long = `${"본문 ".repeat(120)}\n\n[REPLY:1] 뒤에 붙은 답`;
		expect(stripLeakedPreamble(long)).toBe(long);
	});

	test("never eats real content: too many lines before the token", () => {
		const many = "한 줄\n두 줄\n세 줄\n네 줄\n\n[REPLY:1] 답";
		expect(stripLeakedPreamble(many)).toBe(many);
	});

	test("a [BREAK] before the token means the earlier text was a real part", () => {
		const parted = "첫 번째 답\n\n[BREAK]\n\n[REPLY:1] 두 번째 답";
		expect(stripLeakedPreamble(parted)).toBe(parted);
	});

	test("text that merely mentions a token later is not truncated", () => {
		const explaining = "파트를 [REPLY:<id>]로 열면 스레드 답장이 됩니다";
		// The token here has no closing-id shape our parser would route on, but even
		// if it matches, the preceding text is the answer: budget keeps it only when
		// it is aside-sized, and this is the whole point of the conservative guard.
		expect(stripLeakedPreamble(explaining).endsWith("스레드 답장이 됩니다")).toBe(true);
	});
});
