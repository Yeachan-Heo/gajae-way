/**
 * No spoken length cap by default (owner decision, 2026-08-29).
 *
 * A cap was the wrong instinct: someone listening instead of reading cannot go
 * fetch the rest from the text, so truncating the audio truncates the answer for
 * exactly the person the audio is for. Quota is the cheaper thing to spend.
 * `voice.maxSpokenChars` still exists for a deployment that wants a bound.
 */
export const DEFAULT_MAX_SPOKEN_CHARS = Number.POSITIVE_INFINITY;

/**
 * Reduces a reply to something worth listening to.
 *
 * A spoken reply is not a rendered document: urls, code fences, table pipes and
 * heading markers are noise read aloud and cost quota per character. Removing
 * them is not cosmetic — a url alone can be a third of a short reply.
 */
export function speakable(text: string, maxChars: number = DEFAULT_MAX_SPOKEN_CHARS): string {
	const stripped = text
		// Fenced code first: its contents must not survive as loose lines.
		.replace(/```[\s\S]*?```/g, " ")
		// An UNTERMINATED fence runs to the end. Without this the leftover ``` fell
		// through to the inline-code rule below, which consumed two of the three
		// backticks and left one to be spoken, while the code itself was read aloud
		// and billed.
		.replace(/```[\s\S]*$/, " ")
		.replace(/`([^`]*)`/g, "$1")
		// A markdown link keeps its label and loses its target.
		.replace(/\[([^\]]*)\]\((?:[^)]*)\)/g, "$1")
		.replace(/https?:\/\/\S+/g, " ")
		.replace(/^\s*[|>#-]+\s*/gm, " ")
		.replace(/\*\*?([^*]*)\*\*?/g, "$1")
		.replace(/[ \t]+/g, " ")
		.replace(/\s*\n\s*/g, "\n")
		.trim();
	if (stripped.length <= maxChars) return stripped;
	// Cut on a sentence boundary when one is near the cap, so the voice does not
	// stop mid-word. The text message carries the remainder either way.
	const window = cutToCodePoint(stripped, maxChars);
	const boundary = Math.max(window.lastIndexOf(". "), window.lastIndexOf("? "), window.lastIndexOf("다. "));
	return (boundary > maxChars * 0.6 ? window.slice(0, boundary + 1) : window).trim();
}

/**
 * Slices to at most `limit` UTF-16 units without splitting a surrogate pair.
 *
 * A bare `slice` can cut between the halves of an astral character and leave a
 * lone surrogate, which is not valid text to hand a provider — and the cap lands
 * at an arbitrary offset by definition, so it will eventually happen.
 */
function cutToCodePoint(text: string, limit: number): string {
	if (text.length <= limit) return text;
	const code = text.charCodeAt(limit - 1);
	// A high surrogate at the boundary means its pair is the character being cut.
	const end = code >= 0xd800 && code <= 0xdbff ? limit - 1 : limit;
	return text.slice(0, end);
}
