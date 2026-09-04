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
			skipped: [],
		});
	});

	test("text after the token is still delivered", () => {
		expect(parseReactionReply("[REACT:👀] 보고 있습니다")).toEqual({
			reactions: [{ emoji: "👀", emojiName: "eyes" }],
			body: "보고 있습니다",
			skipped: [],
		});
	});

	test("a token may target one specific message", () => {
		expect(parseReactionReply("[REACT:thumbsup@1418812345678]")).toEqual({
			reactions: [{ emoji: "👍", emojiName: "thumbsup", targetMessageId: "1418812345678" }],
			body: "",
			skipped: [],
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

	test("a token is honoured and stripped wherever it appears, not only at the start", () => {
		// The leak this closes: a model writes its reasoning above the token, and a
		// leading-anchored parser posted `[REACT:👍]` into the room as text.
		const preamble = parseReactionReply("형님이 부르셨으니 반응은 해야지.\n\n[REACT:👍]");
		expect(preamble?.reactions).toEqual([{ emoji: "👍", emojiName: "thumbsup" }]);
		expect(preamble?.body).toBe("형님이 부르셨으니 반응은 해야지.");
		for (const text of ["확인 [REACT:👍] 했습니다", "react [REACT:👍]", "본문 [REACT:🔥@9] 끝"])
			expect(parseReactionReply(text)?.body).not.toContain("[REACT");
		expect(parseReactionReply("확인 [REACT:👍] 했습니다")).toEqual({
			reactions: [{ emoji: "👍", emojiName: "thumbsup" }],
			body: "확인 했습니다",
			skipped: [],
		});
	});

	test("token spelling is case-insensitive, like the silence tokens", () => {
		expect(parseReactionReply("[react:👍] 넵")).toEqual({
			reactions: [{ emoji: "👍", emojiName: "thumbsup" }],
			body: "넵",
			skipped: [],
		});
	});

	test("a token that cannot be honoured is reported and stripped, and the reply still ships", () => {
		// A bad token costs the reaction, never the reply — and never leaks its syntax.
		for (const [text, body, skipped] of [
			["[REACT:🚀] launch", "launch", "🚀"],
			["[REACT:nonsense] 발사합니다", "발사합니다", "nonsense"],
			["[REACT:] 발사합니다", "발사합니다", ""],
			["[REACT: ] 발사합니다", "발사합니다", " "],
			["[REACT:👍@] 발사합니다", "발사합니다", "👍@"],
			[`[REACT:👍@${"9".repeat(65)}]`, "", `👍@${"9".repeat(65)}`],
			["[REACT:👍@msg 1]", "", "👍@msg 1"],
			["[REACT:👍@msg\u00001]", "", "👍@msg\u00001"],
		] as const) {
			const parsed = parseReactionReply(text);
			expect(parsed?.reactions).toEqual([]);
			expect(parsed?.body).toBe(body);
			expect(parsed?.skipped).toEqual([skipped]);
		}
	});

	test("text with no closed token at all is left completely alone", () => {
		// undefined means "deliver the text as it stands"; the reply is never dropped.
		for (const text of ["[REACT:👍", "plain text", ""]) expect(parseReactionReply(text)).toBeUndefined();
	});

	test("a hostile argument is inert: nothing is executed, nothing is eaten", () => {
		// The argument is resolved by allowlist lookup, never by pattern matching, so
		// regex metacharacters are ordinary unknown emoji.
		for (const text of ["[REACT:.*] hi", "[REACT:(?:👍)] hi", "[REACT:^$|\\d] hi"]) {
			const parsed = parseReactionReply(text);
			expect(parsed?.reactions).toEqual([]);
			expect(parsed?.body).toBe("hi");
		}
		// Stray brackets around a token survive as ordinary text: they are not control
		// syntax, and widening the match to swallow them would start eating prose.
		expect(parseReactionReply("[REACT:👍]]] hi")?.body).toBe("]] hi");
		expect(parseReactionReply("[REACT:[REACT:👍]]")?.body).toBe("]");
		// A newline inside the argument means no token: it cannot be a reaction, and a
		// reply that merely mentions the word survives untouched.
		expect(parseReactionReply("[REACT:👍\n형님]")).toBeUndefined();
	});

	test("a long reply with scattered tokens stays linear", () => {
		const huge = `${"가".repeat(50_000)}\n[REACT:👍]\n${"나".repeat(50_000)}`;
		const started = performance.now();
		const parsed = parseReactionReply(huge);
		expect(performance.now() - started).toBeLessThan(250);
		expect(parsed?.reactions).toEqual([{ emoji: "👍", emojiName: "thumbsup" }]);
		expect(parsed?.body).not.toContain("[REACT");
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
