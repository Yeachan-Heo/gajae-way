/**
 * Reply resolution for inbound Telegram metadata.
 *
 * Telegram embeds the whole referenced message in `reply_to_message`, so the id,
 * author and text all arrive with the update and never need a follow-up call.
 *
 * `message_thread_id` is deliberately not consulted: in a forum it is topic
 * routing that every message in the topic carries, so treating it as a reply
 * would claim every topic message answers the topic's opening post.
 *
 * Kept free of any gateway/SDK import so the rules stay unit testable.
 */

import type { ReplyContext } from "@gajaeway/protocol";

/** Referenced text is context, not the message being answered: keep the header readable. */
const EXCERPT_LIMIT = 200;

export interface TelegramReplyMessageShape {
	readonly message_id?: number | string;
	readonly from?: { readonly id?: number | string; readonly username?: string; readonly first_name?: string };
	readonly text?: string;
}

/**
 * Builds the reply metadata for an inbound message, or undefined when
 * `reply_to_message` is absent.
 *
 * `fromSelf` is decided against the same bot user id mention detection uses, and
 * stays absent when the referenced message has no sender (e.g. an anonymous
 * channel post) so an unknown author never reads as "not ours".
 */
export function resolveTelegramReplyContext(
	reply: TelegramReplyMessageShape | undefined,
	botUserId: string,
): ReplyContext | undefined {
	if (!reply) return undefined;
	const messageId = idOf(reply.message_id);
	if (!messageId) return undefined;

	const authorId = idOf(reply.from?.id);
	const authorName = nonBlank(reply.from?.username) ?? nonBlank(reply.from?.first_name);
	const excerpt = excerptOf(reply.text);
	return {
		messageId,
		...(authorId ? { authorId } : {}),
		...(authorName ? { authorName } : {}),
		...(authorId && botUserId ? { fromSelf: authorId === botUserId } : {}),
		...(excerpt ? { excerpt } : {}),
	};
}

function idOf(value: number | string | undefined): string | undefined {
	return value === undefined ? undefined : nonBlank(String(value));
}

function excerptOf(text: string | undefined): string | undefined {
	const collapsed = nonBlank(text?.replace(/\s+/g, " ").trim());
	if (!collapsed) return undefined;
	// Truncate by code point: slicing UTF-16 units splits emoji into lone surrogates.
	const points = [...collapsed];
	return points.length > EXCERPT_LIMIT ? `${points.slice(0, EXCERPT_LIMIT).join("")}…` : collapsed;
}

function nonBlank(value: string | undefined): string | undefined {
	return typeof value === "string" && value.trim() !== "" ? value : undefined;
}
