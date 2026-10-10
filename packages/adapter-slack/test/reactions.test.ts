import { expect, test } from "bun:test";
import { REACTION_ALLOWLIST } from "@gajae-gateway/protocol";
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

	// Custom emoji names are passed through to Slack, which validates existence
	expect(reactionFromSlackName("custom")).toEqual({ emoji: ":custom:", emojiName: "custom:custom" });
	expect(slackReactionFor({ emojiName: "custom" })).toBe("custom");

	// Prototype pollution attempts are rejected at parse time (by resolveReactionEmoji)
	// and never reach slackReactionFor, so the adapter never sees them
});

test("Slack reaction names are Slack's own short names, unknown names are custom emoji", () => {
	// Pinned to names `reactions.add` accepts. `rofl` is a Discord/GitHub alias
	// that Slack doesn't recognize; it's treated as a custom emoji name.
	expect(SLACK_REACTION_NAMES).toEqual({
		thumbsup: "+1",
		thumbsdown: "-1",
		check: "white_check_mark",
		cross: "x",
		eyes: "eyes",
		pray: "pray",
		fire: "fire",
		tada: "tada",
		laugh: "rolling_on_the_floor_laughing",
		heart: "heart",
		thinking: "thinking_face",
		salute: "saluting_face",
		lobster: "lobster",
	});
	expect(reactionFromSlackName("rolling_on_the_floor_laughing")).toEqual({ emoji: "🤣", emojiName: "laugh" });
	// Slack doesn't have `rofl`, so it's treated as a custom emoji
	expect(reactionFromSlackName("rofl")).toEqual({ emoji: ":rofl:", emojiName: "custom:rofl" });
});

const event: SlackReactionEvent = {
	type: "reaction_added",
	user: "U1",
	reaction: "custom",
	item: { type: "message", channel: "C1", ts: "1.2" },
	event_ts: "2.3",
};

test("Custom emoji names and allowlist entries are both handled correctly", () => {
	// Allowlist entries map to Slack's native names
	expect(slackReactionFor({ emojiName: "thumbsup" })).toBe("+1");
	expect(slackReactionFor({ emojiName: "check" })).toBe("white_check_mark");

	// Custom emoji names are passed through unchanged for Slack to validate
	expect(slackReactionFor({ emojiName: "custom_emoji" })).toBe("custom_emoji");
	expect(slackReactionFor({ emojiName: "gajae-salute" })).toBe("gajae-salute");

	// Inbound custom emoji are prefixed with 'custom:' to distinguish from allowlist names
	expect(reactionFromSlackName("custom_emoji")).toEqual({ emoji: ":custom_emoji:", emojiName: "custom:custom_emoji" });
	expect(reactionFromSlackName("gajae-salute")).toEqual({ emoji: ":gajae-salute:", emojiName: "custom:gajae-salute" });
});

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
