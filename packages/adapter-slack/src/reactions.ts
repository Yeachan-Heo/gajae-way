import { type EngagementContext, type OriginRef, REACTION_ALLOWLIST, type ReactionRef } from "@gajaeway/protocol";
import { slackMessageId, slackMessageOrigin } from "./origin";

export const SLACK_REACTION_NAMES: Readonly<Record<string, string>> = {
	thumbsup: "+1",
	thumbsdown: "-1",
	check: "white_check_mark",
	cross: "x",
	eyes: "eyes",
	pray: "pray",
	fire: "fire",
	tada: "tada",
	laugh: "rofl",
	heart: "heart",
	thinking: "thinking_face",
	salute: "saluting_face",
	lobster: "lobster",
};

const reverseNames = new Map(
	REACTION_ALLOWLIST.map(({ name, unicode }) => [SLACK_REACTION_NAMES[name], { emoji: unicode, emojiName: name }]),
);

export function slackReactionFor(reaction: Pick<ReactionRef, "emojiName">): string {
	const name = Object.hasOwn(SLACK_REACTION_NAMES, reaction.emojiName)
		? SLACK_REACTION_NAMES[reaction.emojiName]
		: undefined;
	if (!name) throw new Error(`Slack has no reaction name for ${reaction.emojiName}`);
	return name;
}

export function reactionFromSlackName(
	name: string,
): { readonly emoji: string; readonly emojiName: string } | undefined {
	const base = name.replace(/::skin-tone-\d+$/, "");
	return reverseNames.get(base === "thumbsup" ? "+1" : base === "thumbsdown" ? "-1" : base);
}

export interface SlackReactionEvent {
	readonly type: "reaction_added" | "reaction_removed";
	readonly user: string;
	readonly reaction: string;
	readonly item: { readonly type: string; readonly channel: string; readonly ts: string };
	readonly item_user?: string;
	readonly event_ts: string;
}

export interface SlackReactionDescription {
	readonly origin: OriginRef;
	readonly targetMessageId: string;
	readonly emoji: string;
	readonly action: "add" | "remove";
	readonly engagement: EngagementContext;
}

export function describeSlackReaction(
	event: SlackReactionEvent,
	botUserId: string,
	names?: { userName(id: string): string | undefined; channelName(id: string): string | undefined },
): SlackReactionDescription | undefined {
	if (event.item.type !== "message" || event.user === botUserId || !event.item.channel || !event.item.ts)
		return undefined;
	// Slack reactions carry no thread_ts, so thread reactions resolve to their parent channel.
	const origin = slackMessageOrigin({ channel: event.item.channel, user: event.user });
	const authorName = names?.userName(event.user);
	const channelLabel = names?.channelName(event.item.channel);
	return {
		origin,
		targetMessageId: slackMessageId(event.item.channel, event.item.ts),
		emoji: reactionFromSlackName(event.reaction)?.emoji ?? `:${event.reaction}:`,
		action: event.type === "reaction_added" ? "add" : "remove",
		engagement: {
			mentioned: false,
			group: origin.kind !== "dm",
			authorId: event.user,
			...(authorName ? { authorName } : {}),
			...(channelLabel ? { channelLabel } : {}),
		},
	};
}
