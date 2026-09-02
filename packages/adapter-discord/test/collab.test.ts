import { expect, test } from "bun:test";
import { decideInbound } from "../src/main";

const SELF = { id: "self-bot" };
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
	expect(decideInbound(message({ author: { id: "self-bot", bot: true } }), SELF)).toBeUndefined();
});

test("another bot is heard instead of being dropped", () => {
	const decision = decideInbound(message({ author: { id: "other-bot", bot: true } }), SELF);
	expect(decision).toBeDefined();
	expect(decision?.authorId).toBe("other-bot");
});

test("an open channel is not promoted by the adapter", () => {
	expect(decideInbound(message(), SELF)?.mentioned).toBe(false);
});

test("an open channel never promotes a bot message, so two bots cannot loop", () => {
	const decision = decideInbound(message({ author: { id: "other-bot", bot: true } }), SELF);
	expect(decision?.mentioned).toBe(false);
});

test("a bot that explicitly mentions us still earns a turn", () => {
	const decision = decideInbound(
		message({ author: { id: "other-bot", bot: true }, content: "<@self-bot> your turn" }),
		SELF,
	);
	expect(decision?.mentioned).toBe(true);
});

test("outside an open channel a human still needs to mention us", () => {
	expect(decideInbound(message({ channel: channel("chan-2") }), SELF)?.mentioned).toBe(false);
});

test("a message without an id is ignored", () => {
	expect(decideInbound(message({ id: undefined }), SELF)).toBeUndefined();
});

test("group is set for channel origins and unset for direct messages", () => {
	expect(decideInbound(message(), SELF)?.group).toBe(true);
	expect(decideInbound(message({ channel: dmChannel }), SELF)?.group).toBe(false);
});
