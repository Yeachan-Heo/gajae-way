import { expect, test } from "bun:test";
import { REACTION_ALLOWLIST } from "@gajaeway/protocol";
import {
	describeSlackReaction,
	reactionFromSlackName,
	SLACK_REACTION_NAMES,
	type SlackReactionEvent,
	slackReactionFor,
} from "../src/reactions";

test("Every protocol reaction has a Slack mapping and round trips", () => {
	for (const { name, unicode } of REACTION_ALLOWLIST) {
		expect(Object.hasOwn(SLACK_REACTION_NAMES, name)).toBe(true);
		expect(reactionFromSlackName(slackReactionFor({ emojiName: name }))).toEqual({ emoji: unicode, emojiName: name });
	}
	expect(reactionFromSlackName("thumbsup::skin-tone-3")?.emojiName).toBe("thumbsup");
	expect(reactionFromSlackName("thumbsdown")?.emojiName).toBe("thumbsdown");
	expect(reactionFromSlackName("custom")).toBeUndefined();
	for (const name of ["custom", "constructor", "toString"])
		expect(() => slackReactionFor({ emojiName: name })).toThrow(`Slack has no reaction name for ${name}`);
});

const event: SlackReactionEvent = {
	type: "reaction_added",
	user: "U1",
	reaction: "custom",
	item: { type: "message", channel: "C1", ts: "1.2" },
	event_ts: "2.3",
};

test("Slack reactions describe routing, unknown emoji, engagement, and removal", () => {
	expect(describeSlackReaction(event, "BOT", { userName: () => "Alice", channelName: () => "general" })).toEqual({
		origin: { platform: "slack", kind: "channel", conversationId: "C1" },
		targetMessageId: "C1:1.2",
		emoji: ":custom:",
		action: "add",
		engagement: { mentioned: false, group: true, authorId: "U1", authorName: "Alice", channelLabel: "general" },
	});
	const direct = describeSlackReaction(
		{ ...event, type: "reaction_removed", reaction: "+1", item: { ...event.item, channel: "D1" } },
		"BOT",
	);
	expect(direct?.origin).toEqual({ platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" });
	expect(direct?.action).toBe("remove");
	expect(direct?.emoji).toBe("👍");
	expect(direct?.engagement.group).toBe(false);
	expect(describeSlackReaction(event, "U1")).toBeUndefined();
	for (const item of [
		{ ...event.item, type: "file" },
		{ ...event.item, channel: "" },
		{ ...event.item, ts: "" },
	])
		expect(describeSlackReaction({ ...event, item }, "BOT")).toBeUndefined();
});
