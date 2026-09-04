import { isPlatformMessageId, reactionTokens, stripReactionTokens } from "./control-tokens";
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
 * A reply may carry one or more `[REACT:<emoji-or-name>]` tokens, optionally
 * targeting a specific message with `@<message id>`:
 *
 *   [REACT:👍]                      react to the message that triggered the turn
 *   [REACT:thumbsup@123456789]      react to one named message
 *   [REACT:👀] still looking into it  react AND say something
 *
 * A token is honoured wherever it appears and is ALWAYS removed from `body`:
 * anchoring it at the start of the message is what posted `[REACT:👍]` into the
 * room verbatim whenever the persona wrote a reasoning line above it, exactly as
 * a leading-anchored `[SILENT]` once did.
 *
 * With nothing left after the tokens, the turn acknowledges with a reaction and
 * sends no message at all. A token that cannot be honoured — unknown emoji,
 * `@` with no usable target — is reported in `skipped` and stripped anyway: a
 * bad token costs the reaction, never the reply, and never leaks its own syntax
 * into the room.
 */
export interface ReactionReply {
	readonly reactions: readonly {
		readonly emoji: string;
		readonly emojiName: string;
		readonly targetMessageId?: string;
	}[];
	/** Reply text with every REACTION token removed; empty means reaction-only (no message). */
	readonly body: string;
	/** Raw arguments of tokens that could not be resolved, for the caller to log. */
	readonly skipped: readonly string[];
}

/**
 * Parses every reaction token in a reply. Returns undefined only when the reply
 * carries no token at all, which means "deliver the text as it stands".
 *
 * A token that cannot be honoured lands in `skipped` instead of forcing the raw
 * text out: the caller logs it and delivers `body`, so the reply survives and
 * the control syntax stays internal.
 */
export function parseReactionReply(text: string): ReactionReply | undefined {
	const arguments_ = reactionTokens(text);
	if (arguments_.length === 0) return undefined;
	const reactions: { emoji: string; emojiName: string; targetMessageId?: string }[] = [];
	const skipped: string[] = [];
	for (const argument of arguments_) {
		const at = argument.lastIndexOf("@");
		const emojiPart = at === -1 ? argument : argument.slice(0, at);
		const targetMessageId = at === -1 ? undefined : argument.slice(at + 1).trim();
		const resolved = resolveReactionEmoji(emojiPart);
		// Unhonourable: unknown emoji, an explicit `@` with no target id after it,
		// or a target that cannot be a platform message id.
		if (!resolved || (at !== -1 && !isPlatformMessageId(targetMessageId ?? ""))) {
			skipped.push(argument);
			continue;
		}
		reactions.push({
			emoji: resolved.unicode,
			emojiName: resolved.name,
			...(targetMessageId ? { targetMessageId } : {}),
		});
	}
	return { reactions, body: stripReactionTokens(text), skipped };
}
