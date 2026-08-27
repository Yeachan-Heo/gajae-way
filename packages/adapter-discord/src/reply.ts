/**
 * Reply resolution for inbound Discord metadata.
 *
 * Discord splits a reply across two places: `message.reference` always carries
 * the referenced message id, while the referenced *author* only appears in
 * `message.mentions.repliedUser` when that message was already resolved. Fetching
 * the referenced message would put a network round-trip in front of every inbound
 * message, so this module reports exactly what arrived and leaves the rest absent
 * rather than dropping the relationship because one part is missing.
 *
 * `message.reference` is also used for forwards, which are not replies — a
 * non-default reference type is ignored instead of being reported as an answer.
 *
 * Kept free of any `discord.js` import so the rules stay unit testable without
 * the gateway client's dependency tree.
 */

import type { ReplyContext } from "@gajaeway/protocol";
import { type AuthorLike, resolveDisplayName } from "./author";

/** `MessageReferenceType.Default`; type 1 is `Forward`, which is not a reply. */
const REFERENCE_TYPE_REPLY = 0;

export type MessageReferenceLike = {
	readonly messageId?: string | null;
	/** Absent on older payloads, where every reference is a reply. */
	readonly type?: number | null;
} | null;

export type ReplyMessageLike = {
	readonly reference?: MessageReferenceLike;
	/** Author of the replied-to message, present only when discord.js resolved it. */
	readonly mentions?: { readonly repliedUser?: AuthorLike | null } | null;
};

/**
 * Builds the reply metadata for an inbound message, or undefined when it is not
 * a reply.
 *
 * `fromSelf` is decided by comparing the referenced author id against the same
 * bot id mention detection uses, and stays absent when the referenced author was
 * not resolved — an unknown author must not read as "not ours".
 */
export function resolveReplyContext(message: ReplyMessageLike, botId: string): ReplyContext | undefined {
	const reference = message.reference;
	if (!reference) return undefined;
	if (typeof reference.type === "number" && reference.type !== REFERENCE_TYPE_REPLY) return undefined;
	const messageId = nonBlank(reference.messageId);
	if (!messageId) return undefined;

	const repliedUser = message.mentions?.repliedUser ?? undefined;
	const authorId = nonBlank(repliedUser?.id);
	const authorName = resolveDisplayName(repliedUser);
	return {
		messageId,
		...(authorId ? { authorId } : {}),
		...(authorName ? { authorName } : {}),
		...(authorId && botId ? { fromSelf: authorId === botId } : {}),
	};
}

function nonBlank(value: string | null | undefined): string | undefined {
	return typeof value === "string" && value.trim() !== "" ? value : undefined;
}
