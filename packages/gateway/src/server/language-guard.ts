/**
 * Language detection and kana guard for internal turns.
 * Issue #852: Prevent Japanese responses in Korean conversations.
 */

/** Hangul character range (U+AC00–U+D7A3) */
const HANGUL_RANGE = /[\uAC00-\uD7A3]/g;
/** Kana characters (Hiragana U+3040–U+309F, Katakana U+30A0–U+30FF) */
const KANA_RANGE = /[\u3040-\u309F\u30A0-\u30FF]/g;

/**Threshold for language detection: 10%+ Hangul characters indicates Korean */
const HANGUL_THRESHOLD = 0.1;
/** Threshold for kana detection: more than 5% kana in Korean text is suspicious */
const KANA_THRESHOLD = 0.05;

export type DetectedLanguage = "ko" | "en" | undefined;

/**
 * Detect language from text based on character distribution.
 * Returns "ko" if 10%+ Hangul, otherwise undefined (assume English/other).
 */
export function detectLanguageFromText(text: string): DetectedLanguage {
	if (!text || text.length === 0) return undefined;

	const hangulMatches = text.match(HANGUL_RANGE) ?? [];
	const hangulRatio = hangulMatches.length / text.length;

	return hangulRatio >= HANGUL_THRESHOLD ? "ko" : undefined;
}

/**
 * Calculate the ratio of kana characters in text.
 * Hanja (Chinese characters) in Korean text are legitimate and should not trigger the guard.
 */
export function getKanaRatio(text: string): number {
	if (!text || text.length === 0) return 0;

	const kanaMatches = text.match(KANA_RANGE) ?? [];
	return kanaMatches.length / text.length;
}

/**
 * Check if a response has suspicious kana content given the conversation language.
 * Returns a rewrite request if Korean conversation has too much kana, otherwise undefined.
 */
export function checkKanaGuard(
	responseBody: string,
	conversationLanguage: DetectedLanguage,
): { readonly blocked: true; readonly reason: string } | undefined {
	if (conversationLanguage !== "ko") return undefined;

	const kanaRatio = getKanaRatio(responseBody);
	if (kanaRatio > KANA_THRESHOLD) {
		return {
			blocked: true,
			reason: `응답에 가나(일본어 문자)가 ${(kanaRatio * 100).toFixed(1)}% 포함되어 있습니다. 이 대화는 한국어로 진행 중입니다. 한국어로만 작성해주세요.`,
		};
	}

	return undefined;
}

/**
 * Build a language instruction for the system prompt when the conversation is in Korean.
 * This ensures internal turns (lane reports, monitors) respect the conversation language.
 */
export function buildLanguageInstruction(conversationLanguage: DetectedLanguage): string | undefined {
	if (conversationLanguage !== "ko") return undefined;

	return "이 대화는 한국어로 진행 중입니다. 모든 답변(내부 보고 포함)은 반드시 한국어로만 작성해주세요. Do not respond in Japanese or any language other than Korean.";
}
