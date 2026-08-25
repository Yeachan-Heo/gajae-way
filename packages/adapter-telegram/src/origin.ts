import type { OriginRef } from "@gajaeway/protocol";

export interface TelegramMessageOriginShape {
	readonly chat: { readonly id: number | string; readonly type: string };
	readonly from?: { readonly id: number | string };
	readonly message_thread_id?: number;
	readonly is_topic_message?: boolean;
}

/** Normalize an inbound Telegram message into its isolated canonical origin. */
export function telegramMessageOrigin(message: TelegramMessageOriginShape): OriginRef {
	const chatId = String(message.chat.id);
	if (message.chat.type === "private") {
		if (!message.from) throw new Error(`Telegram private chat ${chatId} has no sender id`);
		return { platform: "telegram", kind: "dm", conversationId: chatId, peerId: String(message.from.id) };
	}
	if (
		(message.chat.type === "group" || message.chat.type === "supergroup") &&
		message.is_topic_message &&
		message.message_thread_id !== undefined
	) {
		return {
			platform: "telegram",
			kind: "topic",
			conversationId: `${chatId}.${message.message_thread_id}`,
			parentId: chatId,
		};
	}
	if (message.chat.type === "group" || message.chat.type === "supergroup") {
		return { platform: "telegram", kind: "channel", conversationId: chatId };
	}
	throw new Error(`Unsupported Telegram chat type: ${message.chat.type}`);
}
