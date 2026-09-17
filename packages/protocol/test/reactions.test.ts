import { describe, expect, test } from "bun:test";
import {
	isPlatformMessageId,
	isSilenceToken,
	parseReactionReply,
	platformSupportsReaction,
	REACTION_ALLOWLIST,
	REACTIONS_PER_MESSAGE_CAP,
	REACTIONS_PER_TURN_CAP,
	reactionAllowlistDescription,
	reactionAllowlistFor,
	resolveReactionEmoji,
} from "../src/index";

describe("reaction allowlist", () => {
	test("every accepted spelling of an allowlisted emoji resolves to one canonical entry", () => {
		expect(resolveReactionEmoji("👍")).toEqual({ name: "thumbsup", unicode: "👍" });
		expect(resolveReactionEmoji("thumbsup")).toEqual({ name: "thumbsup", unicode: "👍" });
		expect(resolveReactionEmoji(":thumbsup:")).toEqual({ name: "thumbsup", unicode: "👍" });
		expect(resolveReactionEmoji("  ThumbsUp  ")).toEqual({ name: "thumbsup", unicode: "👍" });
	});

	test("the emoji presentation selector is normalized away so ❤️ and ❤ are one reaction", () => {
		// Telegram's documented reaction set uses the bare U+2764; personas and Discord
		// both emit the U+FE0F form. One canonical spelling or the caps leak.
		expect(resolveReactionEmoji("❤️")).toEqual({ name: "heart", unicode: "❤" });
		expect(resolveReactionEmoji("❤")).toEqual({ name: "heart", unicode: "❤" });
	});

	test("anything outside the bounded set is rejected, not coerced", () => {
		for (const input of ["🚀", "", "   ", ":rocket:", "shrug", "👍👍", "<:lobster:123>"])
			expect(resolveReactionEmoji(input)).toBeUndefined();
	});

	test("the rejection description names what IS accepted, per platform", () => {
		const discord = reactionAllowlistDescription("discord");
		for (const entry of REACTION_ALLOWLIST) {
			expect(discord).toContain(entry.unicode);
			expect(discord).toContain(entry.name);
		}
		// Telegram cannot express these three, so it must never be told to try. The
		// authoritative 73-emoji set lives in the Telegram adapter, whose tests assert
		// this list matches it exactly in both directions.
		const telegram = reactionAllowlistDescription("telegram");
		for (const [unicode, name] of [
			["✅", "check"],
			["❌", "cross"],
			["🦞", "lobster"],
		] as const) {
			expect(telegram).not.toContain(unicode);
			expect(telegram).not.toContain(name);
			expect(platformSupportsReaction("telegram", name)).toBe(false);
			expect(platformSupportsReaction("discord", name)).toBe(true);
		}
		expect(reactionAllowlistFor("telegram")).toHaveLength(REACTION_ALLOWLIST.length - 3);
		// Slack reacts by emoji name and has a name for every allowlist entry.
		expect(reactionAllowlistFor("slack")).toEqual(REACTION_ALLOWLIST);
		for (const entry of REACTION_ALLOWLIST) expect(platformSupportsReaction("slack", entry.name)).toBe(true);
	});

	test("a slack channel:ts pair is a platform message id", () => {
		expect(isPlatformMessageId("C0123456789:1726543210.123456")).toBe(true);
		expect(isPlatformMessageId("C0123456789:1726543210.123456\n")).toBe(false);
	});

	test("caps are bounded and small", () => {
		expect(REACTIONS_PER_TURN_CAP).toBe(3);
		expect(REACTIONS_PER_MESSAGE_CAP).toBe(1);
	});
});

describe("reaction reply token", () => {
	test("a reply that is only a token is a reaction with no message", () => {
		expect(parseReactionReply("[REACT:👍]")).toEqual({
			reactions: [{ emoji: "👍", emojiName: "thumbsup" }],
			body: "",
		});
	});

	test("text after the token is still delivered", () => {
		expect(parseReactionReply("[REACT:👀] 보고 있습니다")).toEqual({
			reactions: [{ emoji: "👀", emojiName: "eyes" }],
			body: "보고 있습니다",
		});
	});

	test("a token may target one specific message", () => {
		expect(parseReactionReply("[REACT:thumbsup@1418812345678]")).toEqual({
			reactions: [{ emoji: "👍", emojiName: "thumbsup", targetMessageId: "1418812345678" }],
			body: "",
		});
	});

	test("several leading tokens are all parsed", () => {
		const parsed = parseReactionReply("[REACT:👍][REACT:🎉@42] done");
		expect(parsed?.reactions).toEqual([
			{ emoji: "👍", emojiName: "thumbsup" },
			{ emoji: "🎉", emojiName: "tada", targetMessageId: "42" },
		]);
		expect(parsed?.body).toBe("done");
	});

	test("a malformed or disallowed token is not a reaction at all, so the text ships verbatim", () => {
		// undefined here means "deliver the text as-is"; the reply is never dropped.
		for (const text of [
			"[REACT:🚀] launch",
			"[REACT:]",
			"[REACT: ]",
			"[REACT:👍@]",
			"[REACT:👍",
			"react [REACT:👍]",
			"plain text",
			"",
			// A target that cannot be a platform message id: oversized, spaced, control char.
			`[REACT:👍@${"9".repeat(65)}]`,
			"[REACT:👍@msg 1]",
			"[REACT:👍@msg\u00001]",
		])
			expect(parseReactionReply(text)).toBeUndefined();
	});

	test("platform message ids are bounded to what a platform actually issues", () => {
		for (const id of ["1418812345678", "m1", "abc.def:1", "x-1_", "9".repeat(64)])
			expect(isPlatformMessageId(id)).toBe(true);
		for (const id of ["", " ", "9".repeat(65), "m 1", "m\n1", "m\u00001", "<script>"])
			expect(isPlatformMessageId(id)).toBe(false);
	});

	test("the silence-token contract is untouched by reaction parsing", () => {
		for (const token of ["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY", "  [silent]  ", "no_reply"]) {
			expect(isSilenceToken(token)).toBe(true);
			expect(parseReactionReply(token)).toBeUndefined();
		}
		expect(isSilenceToken("[REACT:👍]")).toBe(false);
	});
});
