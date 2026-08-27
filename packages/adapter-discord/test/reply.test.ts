import { expect, test } from "bun:test";
import { resolveReplyContext } from "../src/reply";

const BOT_ID = "self-bot";

test("a reply with the referenced author resolved reports every part", () => {
	expect(
		resolveReplyContext(
			{
				reference: { messageId: "900", type: 0 },
				mentions: { repliedUser: { id: "author-2", username: "yeachanheo", globalName: "형님" } },
			},
			BOT_ID,
		),
	).toEqual({ messageId: "900", authorId: "author-2", authorName: "형님", fromSelf: false });
});

test("a reply with only the reference id keeps the relationship instead of dropping it", () => {
	expect(resolveReplyContext({ reference: { messageId: "900" } }, BOT_ID)).toEqual({ messageId: "900" });
});

test("an unresolved referenced author leaves ownership undecided rather than false", () => {
	expect(resolveReplyContext({ reference: { messageId: "900" }, mentions: {} }, BOT_ID)?.fromSelf).toBeUndefined();
});

test("a reply to our own message is flagged from the same bot id as mention detection", () => {
	expect(
		resolveReplyContext(
			{ reference: { messageId: "42" }, mentions: { repliedUser: { id: BOT_ID, username: "gajaeway" } } },
			BOT_ID,
		),
	).toEqual({ messageId: "42", authorId: BOT_ID, authorName: "gajaeway", fromSelf: true });
});

test("the handle is used only when the referenced author has no display name", () => {
	expect(
		resolveReplyContext(
			{ reference: { messageId: "7" }, mentions: { repliedUser: { id: "a", username: "handle", globalName: "  " } } },
			BOT_ID,
		)?.authorName,
	).toBe("handle");
});

test("a plain message with no reference is not a reply", () => {
	expect(resolveReplyContext({}, BOT_ID)).toBeUndefined();
	expect(resolveReplyContext({ reference: null }, BOT_ID)).toBeUndefined();
});

test("a forward is a reference but not a reply", () => {
	expect(resolveReplyContext({ reference: { messageId: "900", type: 1 } }, BOT_ID)).toBeUndefined();
});

test("a reference without a usable message id is not a reply", () => {
	expect(resolveReplyContext({ reference: { messageId: "" } }, BOT_ID)).toBeUndefined();
	expect(resolveReplyContext({ reference: { messageId: null } }, BOT_ID)).toBeUndefined();
});

test("ownership stays undecided when the bot id is unknown", () => {
	expect(
		resolveReplyContext({ reference: { messageId: "1" }, mentions: { repliedUser: { id: "x" } } }, "")?.fromSelf,
	).toBeUndefined();
});
