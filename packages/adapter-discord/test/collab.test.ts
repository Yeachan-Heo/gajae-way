import { expect, test } from "bun:test";
import { decideInbound } from "../src/main";

const SELF = { id: "self-bot" };
const CHANNELS = { "chan-1": { engagement: "open" as const, audience: "all" as const } };
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
	expect(decideInbound(message({ author: { id: "self-bot", bot: true } }), SELF, CHANNELS)).toBeUndefined();
});

test("other bots are forwarded with bot identity for the gateway's canonical policy", () => {
	const decision = decideInbound(message({ author: { id: "other-bot", bot: true } }), SELF, CHANNELS);
	expect(decision).toMatchObject({ authorId: "other-bot", authorIsBot: true, mentioned: false });
});

test("adapter channel mode never fabricates a mention", () => {
	expect(decideInbound(message(), SELF, CHANNELS)?.mentioned).toBe(false);
	expect(decideInbound(message({ author: { id: "other-bot", bot: true } }), SELF, CHANNELS)?.mentioned).toBe(false);
});

test("a real mention addresses the bot for human and bot authors", () => {
	for (const author of [
		{ id: "human-1", bot: false },
		{ id: "other-bot", bot: true },
	]) {
		expect(decideInbound(message({ author, content: "<@self-bot> your turn" }), SELF, CHANNELS)?.mentioned).toBe(true);
	}
});

test("a native reply to our message is addressed, including for a bot author", () => {
	for (const author of [
		{ id: "human-1", bot: false },
		{ id: "other-bot", bot: true },
	]) {
		const decision = decideInbound(
			message({
				author,
				reference: { messageId: "outbound-1", type: 0 },
				mentions: { has: () => false, repliedUser: { id: "self-bot", username: "gajaeway" } },
			}),
			SELF,
			CHANNELS,
		);
		expect(decision?.mentioned).toBe(true);
		expect(decision?.replyTo).toMatchObject({ messageId: "outbound-1", fromSelf: true });
	}
});

test("a native reply to somebody else is context, not an addressed signal", () => {
	const decision = decideInbound(
		message({
			reference: { messageId: "other-1", type: 0 },
			mentions: { has: () => false, repliedUser: { id: "human-2", username: "other" } },
		}),
		SELF,
		CHANNELS,
	);
	expect(decision?.mentioned).toBe(false);
	expect(decision?.replyTo).toMatchObject({ messageId: "other-1", fromSelf: false });
});

test("a message without an id is ignored", () => {
	expect(decideInbound(message({ id: undefined }), SELF, CHANNELS)).toBeUndefined();
});

test("group is set for channel origins and unset for direct messages", () => {
	expect(decideInbound(message(), SELF, CHANNELS)?.group).toBe(true);
	expect(decideInbound(message({ channel: dmChannel }), SELF, undefined)?.group).toBe(false);
});
