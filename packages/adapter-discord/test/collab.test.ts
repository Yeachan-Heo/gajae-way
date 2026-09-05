import { expect, test } from "bun:test";
import { decideInbound } from "../src/main";

const SELF = { id: "self-bot" };
const OPEN = { "chan-1": { engagement: "open" as const } };
const BOT_OPEN = { "chan-1": { engagement: "open" as const, botEngagement: "open" as const } };
const BOT_REPLY = { "chan-1": { engagement: "open" as const, botEngagement: "reply-or-mention" as const } };
const channel = (id = "chan-1") => ({ id, type: 0 });
const dmChannel = { id: "dm-1", type: 1 };

const message = (over: Record<string, unknown> = {}) =>
	({
		id: "m1",
		content: "hello",
		author: { id: "human-1", bot: false },
		channel: channel(),
		...over,
	}) as never;

test("our own messages are ignored so the persona cannot answer itself", () => {
	expect(decideInbound(message({ author: { id: "self-bot", bot: true } }), SELF, OPEN)).toBeUndefined();
});

test("another bot is heard instead of being dropped", () => {
	const decision = decideInbound(message({ author: { id: "other-bot", bot: true } }), SELF, OPEN);
	expect(decision).toBeDefined();
	expect(decision?.authorId).toBe("other-bot");
});

test("an open channel promotes a human message to a mention", () => {
	expect(decideInbound(message(), SELF, OPEN)?.mentioned).toBe(true);
});

test("an open channel never promotes a bot message, so two bots cannot loop", () => {
	const decision = decideInbound(message({ author: { id: "other-bot", bot: true } }), SELF, OPEN);
	expect(decision?.mentioned).toBe(false);
});

test("a bot that explicitly mentions us still earns a turn", () => {
	const decision = decideInbound(
		message({ author: { id: "other-bot", bot: true }, content: "<@self-bot> your turn" }),
		SELF,
		OPEN,
	);
	expect(decision?.mentioned).toBe(true);
});

test("bot open explicitly promotes other bots but never our own bot", () => {
	expect(decideInbound(message({ author: { id: "other-bot", bot: true } }), SELF, BOT_OPEN)?.mentioned).toBe(true);
	expect(decideInbound(message({ author: { id: "self-bot", bot: true } }), SELF, BOT_OPEN)).toBeUndefined();
});

test("reply-or-mention promotes only a resolved native reply to us", () => {
	const reply = {
		author: { id: "other-bot", bot: true },
		reference: { messageId: "parent" },
		mentions: { repliedUser: { id: "self-bot" } },
	};
	expect(decideInbound(message(reply), SELF, BOT_REPLY)?.mentioned).toBe(true);
	expect(
		decideInbound(message({ ...reply, mentions: { repliedUser: { id: "someone-else" } } }), SELF, BOT_REPLY)?.mentioned,
	).toBe(false);
	expect(
		decideInbound(
			message({ author: { id: "other-bot", bot: true }, reference: { messageId: "unknown" } }),
			SELF,
			BOT_REPLY,
		)?.mentioned,
	).toBe(false);
});

test("threads inherit parent bot policy and may override it field-by-field", () => {
	const inThread = message({
		author: { id: "other-bot", bot: true },
		channel: { id: "thread-1", parentId: "chan-1", isThread: () => true },
	});
	expect(decideInbound(inThread, SELF, BOT_OPEN)?.mentioned).toBe(true);
	expect(
		decideInbound(inThread, SELF, {
			...BOT_OPEN,
			"thread-1": { botEngagement: "reply-or-mention" },
		})?.mentioned,
	).toBe(false);
});

test("outside an open channel a human still needs to mention us", () => {
	expect(decideInbound(message({ channel: channel("chan-2") }), SELF, OPEN)?.mentioned).toBe(false);
});

test("a message without an id is ignored", () => {
	expect(decideInbound(message({ id: undefined }), SELF, OPEN)).toBeUndefined();
});

test("group is set for channel origins and unset for direct messages", () => {
	expect(decideInbound(message(), SELF, OPEN)?.group).toBe(true);
	expect(decideInbound(message({ channel: dmChannel }), SELF, undefined)?.group).toBe(false);
});
