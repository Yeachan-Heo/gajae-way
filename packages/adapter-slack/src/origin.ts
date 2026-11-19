import { isPlatformMessageId, type OriginRef } from "@gajae-gateway/protocol";

/** Raw Slack fields keep routing independent of a platform SDK. */
export interface SlackMessageOriginShape {
	readonly channel: string;
	readonly channel_type?: string;
	readonly user?: string;
	readonly bot_id?: string;
	readonly ts?: string;
	readonly thread_ts?: string;
	readonly parent_user_id?: string;
}

export function isSlackDmChannel(channel: string, channelType?: string): boolean {
	return channelType === "im" || channel.startsWith("D");
}

export function slackMessageOrigin(event: SlackMessageOriginShape): OriginRef {
	if (isSlackDmChannel(event.channel, event.channel_type)) {
		const peerId = event.user ?? event.bot_id;
		if (!peerId) throw new Error(`Slack DM ${event.channel} has no peer id`);
		return { platform: "slack", kind: "dm", conversationId: event.channel, peerId };
	}
	if (event.thread_ts && event.thread_ts !== event.ts) {
		return {
			platform: "slack",
			kind: "thread",
			conversationId: slackMessageId(event.channel, event.thread_ts),
			parentId: event.channel,
		};
	}
	return { platform: "slack", kind: "channel", conversationId: event.channel };
}

/** Resolve the session origin after engagement and command handling are known. */
export function resolveSlackMessageOrigin(message: SlackMessageOriginShape, mentioned: boolean): OriginRef {
	const origin = slackMessageOrigin(message);
	// Every reply keeps its own thread session, including a thread rooted at the bot's own
	// message: folding those into the channel session piled every work thread the bot opened
	// into one session, so threads leaked into each other and compacted together.
	if (origin.kind !== "channel") return origin;
	if (!mentioned || !message.ts) return origin;
	return {
		platform: "slack",
		kind: "thread",
		conversationId: slackMessageId(message.channel, message.ts),
		parentId: message.channel,
	};
}

export function slackMessageId(channel: string, ts: string): string {
	const id = `${channel}:${ts}`;
	if (!parseSlackMessageId(id)) throw new Error("Slack message id requires a safe channel and timestamp");
	return id;
}

export function parseSlackMessageId(id: string): { readonly channel: string; readonly ts: string } | undefined {
	if (!isPlatformMessageId(id)) return undefined;
	const match = /^([^:]+):(\d+\.\d+)$/.exec(id);
	return match ? { channel: match[1] as string, ts: match[2] as string } : undefined;
}
