import { expect, test } from "bun:test";
import { telegramMessageOrigin } from "../src/origin";
import { resolveTelegramReplyContext } from "../src/reply";

const BOT_USER_ID = "77";

test("a reply carries the referenced id, author and text from the same update", () => {
	expect(
		resolveTelegramReplyContext(
			{ message_id: 41, from: { id: 12, username: "yeachanheo", first_name: "형님" }, text: "did you push it?" },
			BOT_USER_ID,
		),
	).toEqual({
		messageId: "41",
		authorId: "12",
		authorName: "yeachanheo",
		fromSelf: false,
		excerpt: "did you push it?",
	});
});

test("a reply to our own message is flagged from the same bot user id as mention detection", () => {
	expect(
		resolveTelegramReplyContext(
			{ message_id: 41, from: { id: 77, first_name: "gajaeway" }, text: "pushed" },
			BOT_USER_ID,
		)?.fromSelf,
	).toBe(true);
});

test("the first name is used when the referenced author has no username", () => {
	expect(resolveTelegramReplyContext({ message_id: 4, from: { id: 12, first_name: "형님" } }, BOT_USER_ID)).toEqual({
		messageId: "4",
		authorId: "12",
		authorName: "형님",
		fromSelf: false,
	});
});

test("an anonymous referenced message leaves ownership undecided", () => {
	expect(resolveTelegramReplyContext({ message_id: 4 }, BOT_USER_ID)).toEqual({ messageId: "4" });
});

test("the excerpt is collapsed to one line and bounded", () => {
	const excerpt = resolveTelegramReplyContext(
		{ message_id: 4, from: { id: 12 }, text: `${"a".repeat(250)}\n\nsecond   line` },
		BOT_USER_ID,
	)?.excerpt as string;
	expect(excerpt).not.toContain("\n");
	expect(excerpt.length).toBe(201);
	expect(excerpt.endsWith("…")).toBe(true);
});

test("truncation counts code points so an emoji is never split", () => {
	const excerpt = resolveTelegramReplyContext({ message_id: 4, from: { id: 12 }, text: "😀".repeat(300) }, BOT_USER_ID)
		?.excerpt as string;
	expect(excerpt).toBe(`${"😀".repeat(200)}…`);
	expect(excerpt).not.toContain("\ufffd");
});

test("a whitespace-only referenced text yields no excerpt", () => {
	expect(resolveTelegramReplyContext({ message_id: 4, text: "   \n " }, BOT_USER_ID)?.excerpt).toBeUndefined();
});

test("a message without reply_to_message is not a reply", () => {
	expect(resolveTelegramReplyContext(undefined, BOT_USER_ID)).toBeUndefined();
});

test("a forum-topic message is topic routing, not a reply", () => {
	const topic = {
		chat: { id: -100, type: "supergroup", title: "GAJAE" },
		from: { id: 12, username: "yeachanheo" },
		message_thread_id: 5,
		is_topic_message: true,
	} as const;
	// The thread id routes the message to a topic; every message in that topic
	// carries it, so it must never be read as an answer to the opening post.
	expect(telegramMessageOrigin(topic).kind).toBe("topic");
	expect(
		resolveTelegramReplyContext((topic as { reply_to_message?: undefined }).reply_to_message, BOT_USER_ID),
	).toBeUndefined();
});
