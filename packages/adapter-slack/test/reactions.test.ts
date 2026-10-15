import { expect, test } from "bun:test";
import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import { REACTION_ALLOWLIST } from "@gajae-gateway/protocol";
import type { GatewayClientLike } from "../src/main";
import { settleSlackReaction } from "../src/main";
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
	expect(reactionFromSlackName("custom")).toEqual({ emoji: "custom:custom", emojiName: "custom:custom" });
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
	expect(reactionFromSlackName("rofl")).toEqual({ emoji: "custom:rofl", emojiName: "custom:rofl" });
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
	expect(reactionFromSlackName("custom_emoji")).toEqual({
		emoji: "custom:custom_emoji",
		emojiName: "custom:custom_emoji",
	});
	expect(reactionFromSlackName("gajae-salute")).toEqual({
		emoji: "custom:gajae-salute",
		emojiName: "custom:gajae-salute",
	});
});

test("Slack reactions describe routing, unknown emoji, engagement, and removal", () => {
	expect(describeSlackReaction(event, "BOT", { userName: () => "Alice", channelName: () => "general" })).toEqual({
		origin: { platform: "slack", kind: "channel", conversationId: "C1" },
		targetMessageId: "C1:1.2",
		emoji: "custom:custom",
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

test("settleSlackReaction with action 'add' calls api.addReaction", async () => {
	const requests: { verb: string; args: unknown[] }[] = [];
	const api = {
		addReaction: async (channel: string, ts: string, name: string) => {
			requests.push({ verb: "addReaction", args: [channel, ts, name] });
		},
		removeReaction: async (_channel: string, _ts: string, _name: string) => {
			requests.push({ verb: "removeReaction", args: [_channel, _ts, _name] });
		},
	};
	const gatewayRequests: { verb: string; params: unknown }[] = [];
	const gateway: Pick<GatewayClientLike, "request"> = {
		request: async <T = unknown>(verb: string, params?: unknown): Promise<T> => {
			gatewayRequests.push({ verb, params });
			return {} as T;
		},
	};
	const message: ChatMessagePayload = {
		turnId: "turn-1",
		origin: { platform: "slack", kind: "channel", conversationId: "C1" },
		role: "assistant",
		text: "👍",
		final: true,
		deliveryId: "delivery-1",
		reaction: {
			targetMessageId: "C1:1.2",
			emoji: "👍",
			emojiName: "thumbsup",
			action: "add",
		},
	};

	await settleSlackReaction(gateway, api, message);

	expect(requests).toEqual([{ verb: "addReaction", args: ["C1", "1.2", "+1"] }]);
	expect(gatewayRequests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);

	await settleSlackReaction(gateway, api, {
		...message,
		deliveryId: "delivery-2",
		text: "",
		reaction: {
			targetMessageId: "C1:1.2",
			emoji: "",
			emojiName: "gajae-salute",
			action: "add",
		},
	});
	expect(requests).toEqual([
		{ verb: "addReaction", args: ["C1", "1.2", "+1"] },
		{ verb: "addReaction", args: ["C1", "1.2", "gajae-salute"] },
	]);
	expect(gatewayRequests).toEqual([
		{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } },
		{ verb: "delivery.confirm", params: { deliveryId: "delivery-2" } },
	]);
});

test("settleSlackReaction with action 'remove' calls api.removeReaction and not addReaction", async () => {
	const requests: { verb: string; args: unknown[] }[] = [];
	const api = {
		addReaction: async (_channel: string, _ts: string, _name: string) => {
			requests.push({ verb: "addReaction", args: [_channel, _ts, _name] });
		},
		removeReaction: async (channel: string, ts: string, name: string) => {
			requests.push({ verb: "removeReaction", args: [channel, ts, name] });
		},
	};
	const gatewayRequests: { verb: string; params: unknown }[] = [];
	const gateway: Pick<GatewayClientLike, "request"> = {
		request: async <T = unknown>(verb: string, params?: unknown): Promise<T> => {
			gatewayRequests.push({ verb, params });
			return {} as T;
		},
	};
	const message: ChatMessagePayload = {
		turnId: "turn-1",
		origin: { platform: "slack", kind: "channel", conversationId: "C1" },
		role: "assistant",
		text: "👍",
		final: true,
		deliveryId: "delivery-1",
		reaction: {
			targetMessageId: "C1:1.2",
			emoji: "👍",
			emojiName: "thumbsup",
			action: "remove",
		},
	};

	await settleSlackReaction(gateway, api, message);

	expect(requests).toEqual([{ verb: "removeReaction", args: ["C1", "1.2", "+1"] }]);
	expect(gatewayRequests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});
