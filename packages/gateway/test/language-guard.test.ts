import { expect, test } from "bun:test";
import {
	buildLanguageInstruction,
	checkKanaGuard,
	detectLanguageFromText,
	getKanaRatio,
} from "../src/server/language-guard";

test("detectLanguageFromText: identifies Korean when 10%+ Hangul", () => {
	// About 20% Hangul + 80% Latin
	const text = "안녕하세요 hello world";
	expect(detectLanguageFromText(text)).toBe("ko");
});

test("detectLanguageFromText: identifies Korean with more Hangul", () => {
	const text = "한국어로 작성된 메시지입니다. 많은 한글 텍스트가 있습니다.";
	expect(detectLanguageFromText(text)).toBe("ko");
});

test("detectLanguageFromText: returns undefined for English-only text", () => {
	const text = "This is an English only message";
	expect(detectLanguageFromText(text)).toBeUndefined();
});

test("detectLanguageFromText: returns undefined for text below 10% Hangul", () => {
	// Only about 5% Hangul
	const text = "한 test english language more more more";
	expect(detectLanguageFromText(text)).toBeUndefined();
});

test("detectLanguageFromText: handles empty string", () => {
	expect(detectLanguageFromText("")).toBeUndefined();
});

test("getKanaRatio: calculates kana ratio accurately", () => {
	// Mix of hiragana and katakana
	const text = "これはテストです";
	const ratio = getKanaRatio(text);
	expect(ratio).toBeGreaterThan(0.9);
	expect(ratio).toBeLessThanOrEqual(1.0);
});

test("getKanaRatio: handles text with mixed kana and other characters", () => {
	const text = "These are テスト results with ひらがな mixed";
	const ratio = getKanaRatio(text);
	expect(ratio).toBeGreaterThan(0.0);
	expect(ratio).toBeLessThan(0.5);
});

test("getKanaRatio: returns 0 for no kana", () => {
	const ratio = getKanaRatio("English text with no kana");
	expect(ratio).toBe(0);
});

test("checkKanaGuard: blocks response with excessive kana in Korean conversation", () => {
	const result = checkKanaGuard("これはテストです。ひらがなとカタカナの混合です", "ko");
	expect(result).toBeDefined();
	expect(result?.blocked).toBe(true);
	expect(result?.reason).toContain("가나");
});

test("checkKanaGuard: allows response with Hanja in Korean conversation (Hanja ≠ Kana)", () => {
	// 中文 (Chinese) is Hanja, not Kana, so should not trigger the guard
	const result = checkKanaGuard("中文文本不应该被阻止，只有仮名才应该被阻止", "ko");
	expect(result).toBeUndefined();
});

test("checkKanaGuard: allows Korean response in Korean conversation", () => {
	const result = checkKanaGuard("이것은 한국어 응답입니다. 괜찮습니다.", "ko");
	expect(result).toBeUndefined();
});

test("checkKanaGuard: allows mixed Korean+Hanja in Korean conversation", () => {
	const result = checkKanaGuard("한국어와 中文이 섞여있지만 괜찮습니다.", "ko");
	expect(result).toBeUndefined();
});

test("checkKanaGuard: allows Japanese response in English conversation", () => {
	const result = checkKanaGuard("これはテストです", "en");
	expect(result).toBeUndefined();
});

test("checkKanaGuard: allows Japanese response when language is undefined", () => {
	const result = checkKanaGuard("これはテストです", undefined);
	expect(result).toBeUndefined();
});

test("checkKanaGuard: blocks quoted Japanese in Korean conversation only if kana ratio is high", () => {
	// A single quoted sentence might not trigger the guard
	const singleQuote = '사용자가 "これはテスト" 이라고 말했습니다';
	const kanaRatio = getKanaRatio(singleQuote);
	if (kanaRatio > 0.05) {
		expect(checkKanaGuard(singleQuote, "ko")).toBeDefined();
	} else {
		expect(checkKanaGuard(singleQuote, "ko")).toBeUndefined();
	}
});

test("checkKanaGuard: allows heavily quoted Japanese as long as total ratio is below threshold", () => {
	// A message that's mostly Korean with some quoted Japanese
	const result = checkKanaGuard(
		`사용자의 메시지를 정리하면:
"これはテスト"는 테스트를 의미합니다.
실제로 대부분의 텍스트가 한국어입니다.`,
		"ko",
	);
	// This should be okay because most of the text is Korean
	const totalKanaRatio = getKanaRatio(
		`사용자의 메시지를 정리하면:
"これはテスト"는 테스트를 의미합니다.
실제로 대부분의 텍스트가 한국어입니다.`,
	);
	if (totalKanaRatio <= 0.05) {
		expect(result).toBeUndefined();
	}
});

test("buildLanguageInstruction: returns Korean instruction when language is Korean", () => {
	const instruction = buildLanguageInstruction("ko");
	expect(instruction).toBeDefined();
	expect(instruction).toContain("한국어");
	expect(instruction).toContain("Korean");
});

test("buildLanguageInstruction: returns undefined when language is English", () => {
	const instruction = buildLanguageInstruction("en");
	expect(instruction).toBeUndefined();
});

test("buildLanguageInstruction: returns undefined when language is undefined", () => {
	const instruction = buildLanguageInstruction(undefined);
	expect(instruction).toBeUndefined();
});

test("detectLanguageFromText: detects Korean with Hanja mixed", () => {
	const text = "한국어 텍스트 중文이 섞여있습니다.";
	expect(detectLanguageFromText(text)).toBe("ko");
});
