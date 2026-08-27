import type { OriginPlatform } from "./origin";

/**
 * Emoji reactions (both directions) for the gajaeway profile.
 *
 * Reactions are a deliberately narrow surface: a bounded allowlist, a per-turn
 * cap and a per-message cap, so a persona cannot turn a room into a reaction
 * storm and an adapter cannot be asked to post arbitrary platform markup.
 *
 * The allowlist is platform-neutral unicode with a stable short name. The name
 * is what an adapter uses to look for a *custom guild* emoji of the same name
 * (Discord `<:name:id>`); the unicode is the fallback that always works and is
 * the only spelling that may ever reach a plain text message.
 */

/** One allowlisted reaction: a stable short name plus the unicode that always works. */
export interface ReactionEmoji {
	readonly name: string;
	readonly unicode: string;
}

/**
 * The bounded reaction set. Anything outside it is rejected with an error, never
 * silently dropped. Grows additively — adding an entry is a protocol change.
 */
export const REACTION_ALLOWLIST: readonly ReactionEmoji[] = [
	{ name: "thumbsup", unicode: "👍" },
	{ name: "thumbsdown", unicode: "👎" },
	{ name: "check", unicode: "✅" },
	{ name: "cross", unicode: "❌" },
	{ name: "eyes", unicode: "👀" },
	{ name: "pray", unicode: "🙏" },
	{ name: "fire", unicode: "🔥" },
	{ name: "tada", unicode: "🎉" },
	{ name: "laugh", unicode: "🤣" },
	{ name: "heart", unicode: "❤" },
	{ name: "thinking", unicode: "🤔" },
	{ name: "salute", unicode: "🫡" },
	{ name: "lobster", unicode: "🦞" },
];

/**
 * Reaction caps. Both are per turn: `REACTIONS_PER_TURN_CAP` bounds how many
 * reactions one reply may produce at all, `REACTIONS_PER_MESSAGE_CAP` bounds how
 * many land on any single target message. One acknowledgement per message is the
 * point; a pile of emoji on one message is noise.
 */
export const REACTIONS_PER_TURN_CAP = 3;
export const REACTIONS_PER_MESSAGE_CAP = 1;

/** Adding or retracting a reaction on the platform side. */
export type ReactionAction = "add" | "remove";

/**
 * An outbound reaction bound to one specific platform message. There is no
 * "react to the last message": the target id is required everywhere, because
 * "last" is ambiguous the moment anyone else speaks.
 */
export interface ReactionRef {
	/** Platform message id being reacted to. */
	readonly targetMessageId: string;
	/** Allowlisted unicode emoji; the spelling that is safe to render anywhere. */
	readonly emoji: string;
	/** Allowlist short name, so an adapter can prefer a same-named custom guild emoji. */
	readonly emojiName: string;
}

const BY_UNICODE = new Map(REACTION_ALLOWLIST.map((entry) => [entry.unicode, entry]));
const BY_NAME = new Map(REACTION_ALLOWLIST.map((entry) => [entry.name, entry]));
// U+FE0F: personas and platforms disagree about the emoji presentation selector
// (❤ vs ❤️), and Telegram's documented reaction set uses the bare codepoint.
// Normalizing it away keeps one canonical spelling per reaction.
const VARIATION_SELECTOR = /\uFE0F/g;

/**
 * Resolves any accepted spelling (`👍`, `👍️`, `thumbsup`, `:thumbsup:`) to its
 * allowlist entry. Returns undefined for everything else — the caller decides
 * whether that is a protocol error or a fall-back-to-text situation.
 */
export function resolveReactionEmoji(input: string): ReactionEmoji | undefined {
	const trimmed = input.trim();
	if (!trimmed) return undefined;
	const unicode = BY_UNICODE.get(trimmed.replace(VARIATION_SELECTOR, ""));
	if (unicode) return unicode;
	const name = trimmed.startsWith(":") && trimmed.endsWith(":") && trimmed.length > 2 ? trimmed.slice(1, -1) : trimmed;
	return BY_NAME.get(name.toLowerCase());
}

/**
 * Platform reaction capability.
 *
 * The allowlist says what a persona may ASK for; this says what a platform can
 * actually deliver. Telegram's Bot API accepts only its own server-provided
 * reaction set, so three allowlist entries are unreachable there and asking for
 * one is a guaranteed dead delivery — the persona would believe it acknowledged
 * while the human saw nothing. The Telegram adapter owns the authoritative
 * 73-emoji set and its tests assert that this list is exactly the set of
 * allowlist entries that set rejects, so the two cannot drift apart silently.
 */
const TELEGRAM_INCAPABLE = new Set(["check", "cross", "lobster"]);

export function platformSupportsReaction(platform: OriginPlatform, emojiName: string): boolean {
	return platform === "telegram" ? !TELEGRAM_INCAPABLE.has(emojiName) : true;
}

/** The allowlist a specific platform can actually deliver. */
export function reactionAllowlistFor(platform: OriginPlatform): readonly ReactionEmoji[] {
	return REACTION_ALLOWLIST.filter((entry) => platformSupportsReaction(platform, entry.name));
}

/**
 * Human-readable allowlist for error messages and persona guidance: the caller
 * gets told what IS accepted, on the platform it is actually speaking to.
 *
 * `platform` is required, and typed as the closed OriginPlatform union rather
 * than a string. An unqualified description is the exact text that invited a
 * Telegram persona to acknowledge with an emoji Telegram never accepts, so there
 * is no default arm to fall into, and a mis-spelled platform is a compile error
 * rather than a silent fallback to the full list.
 */
export function reactionAllowlistDescription(platform: OriginPlatform): string {
	return reactionAllowlistFor(platform)
		.map((entry) => `${entry.unicode} (${entry.name})`)
		.join(", ");
}

/**
 * The reaction reply mode (third mode next to text and the silence token).
 *
 * A reply may open with one or more `[REACT:<emoji-or-name>]` tokens, optionally
 * targeting a specific message with `@<message id>`:
 *
 *   [REACT:👍]                      react to the message that triggered the turn
 *   [REACT:thumbsup@123456789]      react to one named message
 *   [REACT:👀] still looking into it  react AND say something
 *
 * With nothing left after the tokens, the turn acknowledges with a reaction and
 * sends no message at all. Parsing is all-or-nothing: a malformed or
 * non-allowlisted token makes the whole reply plain text, so a bad token can
 * only ever cost the reaction, never the reply.
 */
export interface ReactionReply {
	readonly reactions: readonly {
		readonly emoji: string;
		readonly emojiName: string;
		readonly targetMessageId?: string;
	}[];
	/** Reply text after the tokens; empty means reaction-only (no message). */
	readonly body: string;
}

/**
 * Platform message ids are opaque to us, but they are not arbitrary strings:
 * Discord snowflakes and Telegram message ids are short and alphanumeric. Bounding
 * them here keeps a hostile or hallucinated id from becoming an oversized frame or
 * from smuggling newlines into anything that renders an id.
 */
const PLATFORM_MESSAGE_ID = /^[A-Za-z0-9._:-]{1,64}$/;

export function isPlatformMessageId(value: string): boolean {
	return PLATFORM_MESSAGE_ID.test(value);
}

const REACT_TOKEN = /^\s*\[REACT:([^\]\n]*)\]/;

/**
 * Parses leading reaction tokens. Returns undefined when the reply carries no
 * token at all OR when a token is malformed/not allowlisted — both cases mean
 * "deliver the text verbatim".
 */
export function parseReactionReply(text: string): ReactionReply | undefined {
	let rest = text;
	const reactions: { emoji: string; emojiName: string; targetMessageId?: string }[] = [];
	for (let match = rest.match(REACT_TOKEN); match; match = rest.match(REACT_TOKEN)) {
		const [token, argument = ""] = match;
		const at = argument.lastIndexOf("@");
		const emojiPart = at === -1 ? argument : argument.slice(0, at);
		const targetMessageId = at === -1 ? undefined : argument.slice(at + 1).trim();
		const resolved = resolveReactionEmoji(emojiPart);
		// Malformed: unknown emoji, an explicit `@` with no target id after it, or a
		// target that cannot be a platform message id.
		if (!resolved || (at !== -1 && !isPlatformMessageId(targetMessageId ?? ""))) return undefined;
		reactions.push({
			emoji: resolved.unicode,
			emojiName: resolved.name,
			...(targetMessageId ? { targetMessageId } : {}),
		});
		rest = rest.slice(token.length);
	}
	if (reactions.length === 0) return undefined;
	return { reactions, body: rest.trim() };
}
