import { REACTION_ALLOWLIST, type ReactionRef } from "@gajaeway/protocol";

/**
 * Telegram's reaction surface, mapped onto the protocol allowlist.
 *
 * Facts below are from https://core.telegram.org/bots/api (fetched 2026-08-27):
 * - `setMessageReaction(chat_id, message_id, reaction: Array of ReactionType)`
 *   changes the chosen reactions on a message and returns True. "Service
 *   messages of some types can't be reacted to", bots can't use paid reactions,
 *   and an omitted/empty `reaction` array clears the bot's own reaction.
 * - `ReactionTypeEmoji.emoji` is NOT free-form: the server accepts exactly the
 *   73 emoji listed below and rejects anything else. That is a hard platform
 *   limit, not a policy we can widen from our side.
 */
export const TELEGRAM_REACTION_SET: readonly string[] = [
	"❤",
	"👍",
	"👎",
	"🔥",
	"🥰",
	"👏",
	"😁",
	"🤔",
	"🤯",
	"😱",
	"🤬",
	"😢",
	"🎉",
	"🤩",
	"🤮",
	"💩",
	"🙏",
	"👌",
	"🕊",
	"🤡",
	"🥱",
	"🥴",
	"😍",
	"🐳",
	"❤‍🔥",
	"🌚",
	"🌭",
	"💯",
	"🤣",
	"⚡",
	"🍌",
	"🏆",
	"💔",
	"🤨",
	"😐",
	"🍓",
	"🍾",
	"💋",
	"🖕",
	"😈",
	"😴",
	"😭",
	"🤓",
	"👻",
	"👨‍💻",
	"👀",
	"🎃",
	"🙈",
	"😇",
	"😨",
	"🤝",
	"✍",
	"🤗",
	"🫡",
	"🎅",
	"🎄",
	"☃",
	"💅",
	"🤪",
	"🗿",
	"🆒",
	"💘",
	"🙉",
	"🦄",
	"😘",
	"💊",
	"🙊",
	"😎",
	"👾",
	"🤷‍♂",
	"🤷",
	"🤷‍♀",
	"😡",
];

// Telegram spells its reactions with the bare codepoints (❤ is U+2764 with no
// U+FE0F), while personas and other platforms happily emit the presentation
// selector. Normalizing it away on both sides is what makes ❤️ land as ❤.
const VARIATION_SELECTOR = /\uFE0F/g;

const TELEGRAM_EMOJI = new Map(TELEGRAM_REACTION_SET.map((emoji) => [emoji.replace(VARIATION_SELECTOR, ""), emoji]));

/** Allowlist entries Telegram genuinely cannot express, with the reason an operator will read. */
const UNSUPPORTED_ALLOWLIST = new Set(
	REACTION_ALLOWLIST.filter((entry) => !TELEGRAM_EMOJI.has(entry.unicode.replace(VARIATION_SELECTOR, ""))).map(
		(entry) => entry.unicode,
	),
);

/**
 * Maps one outbound reaction onto the emoji Telegram will accept, or explains
 * why it cannot be expressed at all.
 *
 * IMPOSSIBLE-CASE POLICY — REPORTED FAILURE. When the emoji is outside
 * Telegram's set the adapter reports `delivery.fail` (ambiguous: false); it
 * never degrades the reaction into a text message and never silently no-ops.
 * A reaction is chosen BY the persona as a substitute for speaking, so turning
 * it into text would put words into a chat the persona deliberately chose not
 * to speak in, and a no-op would hide the gap entirely. A definitive
 * `delivery.fail` makes the gap visible in the gateway ledger and in
 * `gateway.status` pending counts, and the ledger's bounded retry/expiry then
 * stops retrying an impossibility forever.
 */
export function telegramReactionFor(reaction: ReactionRef): { emoji: string } | { unsupported: string } {
	const normalized = reaction.emoji.replace(VARIATION_SELECTOR, "");
	const emoji = TELEGRAM_EMOJI.get(normalized);
	if (emoji) return { emoji };
	const known = UNSUPPORTED_ALLOWLIST.has(reaction.emoji) || UNSUPPORTED_ALLOWLIST.has(normalized);
	return {
		unsupported: `Telegram cannot react with ${reaction.emoji} (${reaction.emojiName}): its Bot API accepts only ${TELEGRAM_REACTION_SET.length} server-provided reaction emoji and ${reaction.emoji} is not one of them${known ? "" : ", and it is outside the reaction allowlist"}`,
	};
}
