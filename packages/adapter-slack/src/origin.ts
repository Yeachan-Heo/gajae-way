import { isPlatformMessageId, type OriginRef } from "@gajae-gateway/protocol";

/** Raw Slack fields keep routing independent of a platform SDK. */
export interface SlackMessageOriginShape {
	readonly channel: string;
	readonly channel_type?: string;
	readonly user?: string;
	readonly bot_id?: string;
	readonly ts?: string;
	readonly thread_ts?: string;
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

/**
 * When a mentioned message arrives in a channel (not DM, not thread), convert it to a thread-rooted origin.
 * Implements Hermes-like contract: channel mention → thread + new session.
 * In Slack, the message's own timestamp becomes the thread root.
 */
export function maybeThreadOnMention(origin: OriginRef, mentioned: boolean, messageTs: string | undefined): OriginRef {
	// Only thread channel mentions: origin must be a channel, not DM or thread, and bot must be mentioned.
	if (origin.kind !== "channel" || !mentioned || !messageTs) return origin;

	// Convert channel origin to thread origin using message's own ts as thread root.
	return {
		platform: origin.platform,
		kind: "thread",
		conversationId: slackMessageId(origin.conversationId, messageTs),
		parentId: origin.conversationId,
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
