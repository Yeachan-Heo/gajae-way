import type { OriginRef } from "@gajae-gateway/protocol";

/** The Discord message fields needed for origin normalization; intentionally discord.js-free. */
export interface DiscordMessageOriginShape {
	readonly author: { readonly id: string };
	readonly channel: {
		readonly id: string;
		readonly parentId?: string | null;
		isDMBased?: () => boolean;
		isThread?: () => boolean;
		readonly type?: number | string;
		readonly name?: string | null;
	};
}

/** Normalize a Discord message into the canonical public OriginRef. */
export function discordMessageOrigin(message: DiscordMessageOriginShape): OriginRef {
	const channel = message.channel;
	if (channel.isDMBased?.() || channel.type === 1 || channel.type === "DM") {
		return {
			platform: "discord",
			kind: "dm",
			conversationId: channel.id,
			peerId: message.author.id,
		};
	}
	if (
		channel.isThread?.() ||
		channel.type === 10 ||
		channel.type === 11 ||
		channel.type === 12 ||
		channel.type === "THREAD"
	) {
		if (!channel.parentId) throw new Error(`Discord thread ${channel.id} has no parent channel id`);
		const threadVisibility =
			channel.type === 10 || channel.type === 11 ? "public" : channel.type === 12 ? "private" : undefined;
		return {
			platform: "discord",
			kind: "thread",
			conversationId: channel.id,
			parentId: channel.parentId,
			...(threadVisibility ? { threadVisibility } : {}),
		};
	}
	return { platform: "discord", kind: "channel", conversationId: channel.id };
}
