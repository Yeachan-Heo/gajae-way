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
export function resolveSlackMessageOrigin(
	message: SlackMessageOriginShape,
	botUserId: string,
	mentioned: boolean,
): OriginRef {
	const origin = slackMessageOrigin(message);
	if (origin.kind === "dm") return origin;
	if (message.thread_ts && message.thread_ts !== message.ts) {
		return message.parent_user_id === botUserId
			? { platform: "slack", kind: "channel", conversationId: message.channel }
			: origin;
	}
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
