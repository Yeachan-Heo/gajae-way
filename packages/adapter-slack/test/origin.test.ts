import { expect, test } from "bun:test";
import { isSlackDmChannel, parseSlackMessageId, slackMessageId, slackMessageOrigin } from "../src/origin";

test("Slack origins distinguish direct messages, thread replies, and roots", () => {
	expect(slackMessageOrigin({ channel: "C1", ts: "1.2" })).toEqual({
		platform: "slack",
		kind: "channel",
		conversationId: "C1",
	});
	expect(slackMessageOrigin({ channel: "C1", ts: "1.2", thread_ts: "1.2" }).kind).toBe("channel");
	expect(slackMessageOrigin({ channel: "C1", ts: "2.3", thread_ts: "1.2" })).toEqual({
		platform: "slack",
		kind: "thread",
		conversationId: "C1:1.2",
		parentId: "C1",
	});
	for (const event of [
		{ channel: "D1", user: "U1", thread_ts: "1.2" },
		{ channel: "C1", channel_type: "im", bot_id: "B1" },
	]) {
		expect(slackMessageOrigin(event)).toEqual({
			platform: "slack",
			kind: "dm",
			conversationId: event.channel,
			peerId: event.user ?? event.bot_id,
		});
	}
	expect(() => slackMessageOrigin({ channel: "D1" })).toThrow("Slack DM D1 has no peer id");
	expect(isSlackDmChannel("G1", "mpim")).toBe(false);
});

test("Slack message identifiers round trip and reject malformed or unsafe parts", () => {
	expect(parseSlackMessageId(slackMessageId("C1", "123.456"))).toEqual({ channel: "C1", ts: "123.456" });
	for (const id of ["C1", ":1.2", "C1:abc", "C1:1.2:3", "C 1:1.2", `${"C".repeat(64)}:1.2`])
		expect(parseSlackMessageId(id)).toBeUndefined();
	for (const [channel, ts] of [
		["", "1.2"],
		["C1", ""],
		["C\n1", "1.2"],
		["C1", "abc"],
		["C:1", "1.2"],
	])
		expect(() => slackMessageId(channel as string, ts as string)).toThrow("Slack");
});
