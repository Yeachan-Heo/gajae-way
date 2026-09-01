/**
 * Leaked control-line preamble ("찐빠") stripping.
 *
 * Observed live (2026-09-01, #playground-ko): a persona emitted its own
 * reasoning line BEFORE the control token —
 *
 *   형님이 나오라고 부르셨으니 반응은 해야지.
 *
 *   [REPLY:1544305635179495434] 혼난거 아니고 …
 *
 * Every parser downstream anchors its token at the START of the message or
 * part (`/^\[REPLY:…\]/`, `isSilenceToken(whole)`), so one stray narration
 * line does two kinds of damage at once: the routing/silence directive is
 * ignored, AND the model's private aside is delivered into the room as text.
 * `[SILENT]` behind a narration line is the worst case — the persona asked
 * for silence and the room got the reasoning instead.
 *
 * This strips that leading block back off. It is deliberately conservative:
 * dropping a real answer is a worse failure than leaking a preamble, so a
 * block is only removed when ALL of these hold.
 *
 * 1. A REPLY/REACT directive, or a bare silence token, starts its own line
 *    (or follows narration on one line) somewhere after the first character.
 * 2. Nothing before it is itself a directive — `[BREAK]` in the block means
 *    the preceding text is genuine delivered content, not an aside.
 * 3. The block is narration-sized: at most `MAX_PREAMBLE_LINES` non-empty
 *    lines and `MAX_PREAMBLE_CHARS` characters.
 *
 * Tokens are matched bracket-tolerantly and case-insensitively for the same
 * reason `isSilenceToken` unbrackets: the spelling drifts, the intent does not.
 */
import { isSilenceToken } from "@gajaeway/protocol";

const MAX_PREAMBLE_LINES = 3;
const MAX_PREAMBLE_CHARS = 300;

/** `[REPLY:<id>]` / `[REACT:<emoji>]` / `[REACT:<emoji>@<id>]`, brackets optional-ish. */
const DIRECTIVE_AT_LINE_START = /^\s*\[(?:REPLY|REACT):[^\]\r\n]*\]/i;
/** The same tokens anywhere, used to find a mid-line leak and to veto stripping. */
const DIRECTIVE_ANYWHERE = /\[(?:REPLY|REACT):[^\]\r\n]*\]/i;
const BREAK_ANYWHERE = /\[BREAK\]/i;

/** A line that is nothing but a silence token (`[SILENT]`, `NO_REPLY`, …). */
function isBareSilenceLine(line: string): boolean {
	const trimmed = line.trim();
	return trimmed.length > 0 && isSilenceToken(trimmed);
}

function withinBudget(block: string): boolean {
	if (block.length > MAX_PREAMBLE_CHARS) return false;
	if (BREAK_ANYWHERE.test(block) || DIRECTIVE_ANYWHERE.test(block)) return false;
	const nonEmpty = block.split(/\r?\n/).filter((line) => line.trim().length > 0);
	return nonEmpty.length > 0 && nonEmpty.length <= MAX_PREAMBLE_LINES;
}

/**
 * Returns `message` with a leaked narration block before the first control
 * token removed, or `message` unchanged when no safe strip applies.
 */
export function stripLeakedPreamble(message: string): string {
	const lines = message.split(/\r?\n/);
	for (let index = 0; index < lines.length; index++) {
		const line = lines[index] as string;
		const startsWithDirective = DIRECTIVE_AT_LINE_START.test(line);
		const silenceLine = isBareSilenceLine(line);
		// Nothing to strip: the token already opens the message.
		if (index === 0 && (startsWithDirective || silenceLine)) return message;
		if (index > 0 && (startsWithDirective || silenceLine)) {
			const block = lines.slice(0, index).join("\n");
			if (!withinBudget(block)) return message;
			// A silence directive means the whole turn was meant to stay quiet:
			// hand back the bare token so `isSilenceToken` upstream matches.
			if (silenceLine) return line.trim();
			return lines.slice(index).join("\n").trim();
		}
		// Deliberately NOT handled: a token in the MIDDLE of a line
		// ("확인 [REACT:👍] 했습니다", "[SILENT][REACT:🔥]"). That shape is an
		// established contract — mid-reply tokens are plain text (RT-TOKEN-02,
		// RT-TOKEN-04) — and a sentence quoting a token is far more common than
		// a leak that forgets its own newline. The leak always came with the
		// newline the persona was told to write.
	}
	return message;
}
