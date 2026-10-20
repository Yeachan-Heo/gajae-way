import { constants } from "node:fs";
import { open, realpath } from "node:fs/promises";
import { acquireLease, exitOnLeaseHeld, type Lease, LeaseHeldError } from "@gajae-gateway/home";

import {
	type ChatMessagePayload,
	type EngagementContext,
	type FileRef,
	type OriginRef,
	OUTBOUND_FILE_MAX_BYTES,
	originKey,
} from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import {
	describeInboundBody,
	pruneTelegramAttachments,
	type TelegramAttachmentCarrier,
	type TelegramAttachmentSaver,
	TelegramAttachmentStore,
	type TelegramFileApi,
	telegramAttachment,
} from "./attachments";
import { adapterHome, type LoadedTelegramAdapterConfig, loadTelegramAdapterConfig } from "./config";
import { type TelegramMessageOriginShape, telegramMessageOrigin } from "./origin";
import { telegramReactionFor } from "./reactions";
import { resolveTelegramReplyContext, type TelegramReplyMessageShape } from "./reply";
import { TelegramAdapterState } from "./state";

const TELEGRAM_MESSAGE_LIMIT = 4_096;
/**
 * sendPhoto refuses photos over 10 MB (https://core.telegram.org/bots/api#sendphoto).
 * Decimal megabytes keep us under the limit whichever unit Telegram means;
 * anything larger goes out as a document, which Telegram accepts up to 50 MB.
 */
export const TELEGRAM_PHOTO_MAX_BYTES = 10_000_000;
/**
 * Deliveries settle one at a time per chat, so a Bot API request that never
 * answers would hold back every later reply to that chat. Each request is
 * abandoned after this long; the delivery then fails as ambiguous.
 */
export const TELEGRAM_REQUEST_TIMEOUT_MS = 30_000;
/** Uploads carry up to 50 MB, so they get longer than a JSON call. */
export const TELEGRAM_UPLOAD_TIMEOUT_MS = 120_000;
/** getUpdates long-polls for this long before answering with no updates. */
const TELEGRAM_POLL_SECONDS = 30;
const ATTACHMENT_PRUNE_INTERVAL_MS = 6 * 60 * 60 * 1000;

export interface GatewayClientLike {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void;
	close?(): Promise<void>;
}

export interface TelegramMessage extends TelegramMessageOriginShape, TelegramAttachmentCarrier {
	readonly message_id: number;
	readonly text?: string;
	/** Media messages carry their text here instead of in `text`. */
	readonly caption?: string;
	readonly reply_to_message?: TelegramReplyMessageShape;
}

/** One entry of Telegram's `ReactionType` union; bots only ever set/read `type: "emoji"`. */
export interface TelegramReactionType {
	readonly type: string;
	readonly emoji?: string;
	readonly custom_emoji_id?: string;
}

/**
 * `MessageReactionUpdated`: a user changed their reactions on one message.
 *
 * OPERATOR NOTE (https://core.telegram.org/bots/api, fetched 2026-08-27): this
 * update is delivered ONLY if the bot is an administrator in the chat AND
 * "message_reaction" is explicitly listed in `allowed_updates`. It is not in the
 * default update set, and Telegram never sends it for reactions set by bots. If
 * inbound reactions never arrive, check bot admin rights first.
 */
export interface TelegramMessageReactionUpdated {
	readonly chat: TelegramMessageOriginShape["chat"];
	readonly message_id: number;
	readonly user?: { readonly id: number | string; readonly username?: string; readonly first_name?: string };
	readonly actor_chat?: { readonly id: number | string; readonly title?: string };
	readonly date: number;
	readonly old_reaction: readonly TelegramReactionType[];
	readonly new_reaction: readonly TelegramReactionType[];
}

export interface TelegramUpdate {
	readonly update_id: number;
	readonly message?: TelegramMessage;
	readonly message_reaction?: TelegramMessageReactionUpdated;
}

export type FetchLike = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

export class TelegramApiError extends Error {
	constructor(
		readonly status: number,
		message: string,
	) {
		super(message);
		this.name = "TelegramApiError";
	}
}

/** One local file to upload; `name` is the file name Telegram shows. */
export interface TelegramUpload {
	readonly name: string;
	readonly bytes: Uint8Array;
}

/** Where a reply lands: the chat, and the forum topic inside it when there is one. */
export interface TelegramUploadTarget {
	readonly chatId: string;
	readonly messageThreadId?: number;
	readonly caption?: string;
}

export interface TelegramBotApiTimeouts {
	readonly requestMs: number;
	readonly uploadMs: number;
}

export class TelegramBotApi implements TelegramFileApi {
	constructor(
		readonly token: string,
		readonly fetcher: FetchLike = fetch,
		readonly timeouts: TelegramBotApiTimeouts = {
			requestMs: TELEGRAM_REQUEST_TIMEOUT_MS,
			uploadMs: TELEGRAM_UPLOAD_TIMEOUT_MS,
		},
	) {}

	/**
	 * The timeout covers the whole exchange, response body included, and is
	 * combined with any caller signal.
	 */
	async call<T>(
		method: string,
		parameters: Record<string, unknown> = {},
		signal?: AbortSignal,
		timeoutMs: number = this.timeouts.requestMs,
	): Promise<T> {
		const response = await this.fetcher(`https://api.telegram.org/bot${this.token}/${method}`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify(parameters),
			signal: withTimeout(timeoutMs, signal),
		});
		return this.#result<T>(method, response);
	}

	/** Posts one photo (JPEG/PNG/WebP, at most {@link TELEGRAM_PHOTO_MAX_BYTES}) into the chat or topic. */
	sendPhoto(target: TelegramUploadTarget, file: TelegramUpload): Promise<{ readonly message_id?: number }> {
		return this.#upload("sendPhoto", "photo", target, file);
	}

	/** Posts any file as a document into the chat or topic. */
	sendDocument(target: TelegramUploadTarget, file: TelegramUpload): Promise<{ readonly message_id?: number }> {
		return this.#upload("sendDocument", "document", target, file);
	}

	/**
	 * multipart/form-data upload: the bytes and their file name travel in one
	 * request, so nothing is left half-posted on failure. The request url embeds
	 * the bot token, and a transport error may echo it, so such errors are rethrown
	 * with the token removed; they stay plain Errors so the delivery is still
	 * treated as ambiguous (the upload may have landed).
	 */
	async #upload<T>(
		method: "sendPhoto" | "sendDocument",
		field: "photo" | "document",
		target: TelegramUploadTarget,
		file: TelegramUpload,
	): Promise<T> {
		const form = new FormData();
		form.append("chat_id", target.chatId);
		if (target.messageThreadId !== undefined) form.append("message_thread_id", String(target.messageThreadId));
		if (target.caption !== undefined) form.append("caption", target.caption);
		form.append(field, new Blob([file.bytes as Uint8Array<ArrayBuffer>]), file.name);
		let response: Response;
		try {
			response = await this.fetcher(`https://api.telegram.org/bot${this.token}/${method}`, {
				method: "POST",
				body: form,
				signal: withTimeout(this.timeouts.uploadMs),
			});
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			const detail = this.token ? message.replaceAll(this.token, "<redacted>") : message;
			throw new Error(`Telegram ${method} request failed: ${detail}`);
		}
		return this.#result<T>(method, response);
	}

	async #result<T>(method: string, response: Response): Promise<T> {
		let body: unknown;
		try {
			body = await response.json();
		} catch {
			throw new TelegramApiError(response.status, `Telegram ${method} returned an invalid response`);
		}
		if (!response.ok || !isTelegramResult(body)) {
			const description =
				isObject(body) && typeof body.description === "string" ? body.description : `Telegram ${method} failed`;
			throw new TelegramApiError(response.status, description);
		}
		return body.result as T;
	}

	getUpdates(offset?: number): Promise<TelegramUpdate[]> {
		// `allowed_updates` REPLACES Telegram's default set, so "message" has to be
		// listed explicitly to keep the existing text path working while opting into
		// "message_reaction" (which is never in the default set).
		return this.call(
			"getUpdates",
			{
				timeout: TELEGRAM_POLL_SECONDS,
				allowed_updates: ["message", "message_reaction"],
				...(offset === undefined ? {} : { offset }),
			},
			undefined,
			// The long poll legitimately holds the request open for its whole window.
			TELEGRAM_POLL_SECONDS * 1_000 + this.timeouts.requestMs,
		);
	}

	sendMessage(chatId: string, text: string, messageThreadId?: number): Promise<unknown> {
		return this.call("sendMessage", {
			chat_id: chatId,
			text,
			...(messageThreadId === undefined ? {} : { message_thread_id: messageThreadId }),
		});
	}

	setMessageReaction(chatId: string, messageId: string, emoji?: string): Promise<unknown> {
		// setMessageReaction sets the bot's chosen reactions on one message and returns
		// True; a single-element array is exactly one reaction, and bots may not use
		// paid reactions. An empty array removes all reactions.
		// https://core.telegram.org/bots/api (fetched 2026-08-27)
		return this.call("setMessageReaction", {
			chat_id: chatId,
			message_id: Number(messageId),
			reaction: emoji ? [{ type: "emoji", emoji }] : [],
		});
	}

	getFile(fileId: string, signal: AbortSignal): Promise<{ readonly file_path?: string; readonly file_size?: number }> {
		return this.call("getFile", { file_id: fileId }, signal);
	}

	/** The url embeds the bot token: callers must never log or forward it. */
	downloadFile(filePath: string, signal: AbortSignal): Promise<Response> {
		return this.fetcher(`https://api.telegram.org/file/bot${this.token}/${filePath}`, { signal });
	}
}

function withTimeout(timeoutMs: number, signal?: AbortSignal): AbortSignal {
	const timeout = AbortSignal.timeout(timeoutMs);
	return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

export function chunkTelegramMessage(text: string): string[] {
	if (text.length === 0) return [""];
	const chunks: string[] = [];
	for (let offset = 0; offset < text.length; offset += TELEGRAM_MESSAGE_LIMIT)
		chunks.push(text.slice(offset, offset + TELEGRAM_MESSAGE_LIMIT));
	return chunks;
}

export function deliveryFailureIsAmbiguous(error: unknown): boolean {
	// Telegram's HTTP Bot API errors are definitive non-delivery; thrown fetch/timeout errors are not.
	return !(error instanceof TelegramApiError);
}

export async function settleTelegramDelivery(
	gateway: Pick<GatewayClientLike, "request">,
	bot: Pick<TelegramBotApi, "sendMessage">,
	state: TelegramAdapterState,
	message: ChatMessagePayload,
): Promise<void> {
	if (message.origin.platform !== "telegram" || !message.deliveryId) return;
	const deliveryId = message.deliveryId;
	try {
		const route = state.routeFor(message.origin);
		if (!route)
			throw new TelegramApiError(400, `No persisted Telegram reply route for ${message.origin.conversationId}`);
		const text = message.duplicateWarning ? `[recovered - may be a duplicate] ${message.text}` : message.text;
		for (const chunk of chunkTelegramMessage(text)) await bot.sendMessage(route.chatId, chunk, route.messageThreadId);
		await gateway.request("delivery.confirm", { deliveryId });
	} catch (error) {
		await gateway.request("delivery.fail", {
			deliveryId,
			reason: error instanceof Error ? error.message : String(error),
			ambiguous: deliveryFailureIsAmbiguous(error),
		});
	}
}

/**
 * Settles a reaction delivery: react to the target message, post nothing, then
 * confirm or fail the SAME delivery ledger entry a text message would use.
 *
 * Every failure path — an emoji outside Telegram's 73-emoji reaction set, a chat
 * that rejects the reaction (reactions disabled/not permitted, a service message
 * that "can't be reacted to"), or a missing persisted reply route — reports
 * `delivery.fail` with `ambiguous: false`, never a text fallback and never a
 * silent no-op. See the IMPOSSIBLE-CASE POLICY comment in ./reactions.
 */
export async function settleTelegramReaction(
	gateway: Pick<GatewayClientLike, "request">,
	bot: Pick<TelegramBotApi, "setMessageReaction">,
	state: TelegramAdapterState,
	message: ChatMessagePayload,
): Promise<void> {
	if (message.origin.platform !== "telegram" || !message.deliveryId || !message.reaction) return;
	const deliveryId = message.deliveryId;
	const reaction = message.reaction;
	const action = reaction.action ?? "add";
	try {
		const route = state.routeFor(message.origin);
		if (!route)
			throw new TelegramApiError(400, `No persisted Telegram reply route for ${message.origin.conversationId}`);
		if (action === "remove") {
			// Remove the reaction by passing no emoji
			await bot.setMessageReaction(route.chatId, reaction.targetMessageId, undefined);
		} else {
			const mapped = telegramReactionFor(reaction);
			// TelegramApiError keeps `deliveryFailureIsAmbiguous` honest: an emoji Telegram
			// refuses is definitively not delivered, exactly like a rejected API call.
			if ("unsupported" in mapped) throw new TelegramApiError(400, mapped.unsupported);
			await bot.setMessageReaction(route.chatId, reaction.targetMessageId, mapped.emoji);
		}
		await gateway.request("delivery.confirm", { deliveryId });
	} catch (error) {
		await gateway.request("delivery.fail", {
			deliveryId,
			reason: error instanceof Error ? error.message : String(error),
			ambiguous: deliveryFailureIsAmbiguous(error),
		});
	}
}

/**
 * Settles a file delivery (a persona `MEDIA:<path>` line the gateway checked):
 * upload the file into the same chat and forum topic the text reply goes to,
 * post nothing else, then confirm or fail the delivery's own ledger entry.
 *
 * Photos (see {@link telegramPhotoEligible}) go through sendPhoto so they render
 * inline; everything else, and any photo Telegram refuses as a photo, goes
 * through sendDocument. A refused photo posted nothing, so the document retry
 * cannot duplicate it. A transport failure is never retried here: the upload may
 * have landed, so it is reported ambiguous and the ledger decides.
 */
export async function settleTelegramFile(
	gateway: Pick<GatewayClientLike, "request">,
	bot: Partial<Pick<TelegramBotApi, "sendPhoto" | "sendDocument">>,
	state: TelegramAdapterState,
	message: ChatMessagePayload,
): Promise<void> {
	if (message.origin.platform !== "telegram" || !message.deliveryId || !message.file) return;
	const deliveryId = message.deliveryId;
	try {
		if (!bot.sendPhoto || !bot.sendDocument)
			throw new TelegramApiError(400, "this Telegram client cannot upload files");
		const route = state.routeFor(message.origin);
		if (!route)
			throw new TelegramApiError(400, `No persisted Telegram reply route for ${message.origin.conversationId}`);
		const bytes = await readOutboundFile(message.file);
		const upload: TelegramUpload = { name: message.file.name, bytes };
		const target: TelegramUploadTarget = {
			chatId: route.chatId,
			...(route.messageThreadId === undefined ? {} : { messageThreadId: route.messageThreadId }),
			...(message.duplicateWarning ? { caption: `[recovered - may be a duplicate] ${message.text}` } : {}),
		};
		let sent = false;
		if (telegramPhotoEligible(bytes)) {
			try {
				await bot.sendPhoto(target, upload);
				sent = true;
			} catch (error) {
				if (!telegramRefusedPhoto(error)) throw error;
			}
		}
		if (!sent) await bot.sendDocument(target, upload);
		await gateway.request("delivery.confirm", { deliveryId });
	} catch (error) {
		await gateway.request("delivery.fail", {
			deliveryId,
			reason: error instanceof Error ? error.message : String(error),
			ambiguous: deliveryFailureIsAmbiguous(error),
		});
	}
}

/**
 * True for bytes sendPhoto can show inline: a JPEG, PNG or WebP (decided by
 * magic bytes, not the file name, because Telegram decodes the content) within
 * the photo size limit. GIF is deliberately excluded: sendPhoto keeps only a
 * still frame, while a GIF sent with sendDocument is shown as an animation.
 */
export function telegramPhotoEligible(bytes: Uint8Array): boolean {
	if (bytes.byteLength > TELEGRAM_PHOTO_MAX_BYTES) return false;
	const startsWith = (offset: number, signature: readonly number[]) =>
		signature.every((byte, index) => bytes[offset + index] === byte);
	return (
		startsWith(0, [0xff, 0xd8, 0xff]) ||
		startsWith(0, [0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]) ||
		(startsWith(0, [0x52, 0x49, 0x46, 0x46]) && startsWith(8, [0x57, 0x45, 0x42, 0x50]))
	);
}

/**
 * A 400 whose description names a photo/image processing error
 * (`PHOTO_INVALID_DIMENSIONS`, `PHOTO_SAVE_FILE_INVALID`, `IMAGE_PROCESS_FAILED`, ...):
 * Telegram refused these bytes as a photo, not the chat or the request. Other
 * 400s (chat or topic not found, missing rights) would fail a document too.
 */
function telegramRefusedPhoto(error: unknown): boolean {
	return (
		error instanceof TelegramApiError &&
		error.status === 400 &&
		/\b(?:PHOTO_[A-Z_]+|IMAGE_PROCESS_FAILED)\b/.test(error.message)
	);
}

/**
 * Reads the checked file again at delivery time and refuses it if it is no
 * longer the file the gateway approved: the open must not follow a symlink, the
 * descriptor must still resolve to the checked path, and the size must match
 * exactly. Every refusal is a definitive non-delivery (nothing was posted).
 */
async function readOutboundFile(file: FileRef): Promise<Uint8Array> {
	if (!Number.isSafeInteger(file.size) || file.size < 1 || file.size > OUTBOUND_FILE_MAX_BYTES)
		throw new TelegramApiError(400, `${file.name} has an invalid checked size`);
	let handle: Awaited<ReturnType<typeof open>>;
	try {
		handle = await open(file.path, constants.O_RDONLY | constants.O_NOFOLLOW);
	} catch (error) {
		throw new TelegramApiError(
			400,
			`cannot open ${file.name}: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	try {
		const descriptorPath =
			process.platform === "linux"
				? `/proc/self/fd/${handle.fd}`
				: process.platform === "darwin"
					? `/dev/fd/${handle.fd}`
					: undefined;
		if (!descriptorPath || (await realpath(descriptorPath)) !== file.path)
			throw new TelegramApiError(400, `${file.name} changed after the gateway checked it`);
		const stat = await handle.stat();
		if (!stat.isFile() || stat.size !== file.size || stat.size > OUTBOUND_FILE_MAX_BYTES)
			throw new TelegramApiError(400, `${file.name} changed after the gateway checked it`);
		const bytes = Buffer.allocUnsafe(file.size + 1);
		let length = 0;
		while (length < bytes.length) {
			const read = await handle.read(bytes, length, bytes.length - length, null);
			if (read.bytesRead === 0) break;
			length += read.bytesRead;
		}
		if (length !== file.size) throw new TelegramApiError(400, `${file.name} changed while the adapter read it`);
		return bytes.subarray(0, length);
	} catch (error) {
		if (error instanceof TelegramApiError) throw error;
		throw new TelegramApiError(
			400,
			`cannot read ${file.name}: ${error instanceof Error ? error.message : String(error)}`,
		);
	} finally {
		await handle.close().catch(() => {});
	}
}

/**
 * Settles each conversation's deliveries one at a time, in the order the gateway
 * sent them. One reply can fan out into a text delivery and several file
 * deliveries; settled concurrently, a small upload overtakes a large one and the
 * chat shows them out of order. Different conversations still settle in parallel,
 * and a failed settlement never blocks the deliveries queued behind it.
 */
export class TelegramDeliveryOrder {
	readonly #chains = new Map<string, Promise<void>>();

	constructor(private readonly log: Pick<Console, "error"> = console) {}

	run(key: string, task: () => Promise<void>): void {
		const previous = this.#chains.get(key) ?? Promise.resolve();
		const next = previous
			.then(task)
			.catch((error: unknown) =>
				this.log.error(
					`Telegram delivery settlement request failed: ${error instanceof Error ? error.message : String(error)}`,
				),
			);
		this.#chains.set(key, next);
		void next.then(() => {
			if (this.#chains.get(key) === next) this.#chains.delete(key);
		});
	}

	/** Resolves once every queued delivery, including ones queued while waiting, has settled. */
	async drain(): Promise<void> {
		while (this.#chains.size > 0) await Promise.all([...this.#chains.values()]);
	}
}

/**
 * The Telegram conversation a delivery lands in: its persisted chat and forum
 * topic, so origins that share a destination share one order. An origin without
 * a route fails settlement anyway; it is still ordered by its own key.
 */
export function telegramDeliveryKey(state: TelegramAdapterState, origin: OriginRef): string {
	const route = state.routeFor(origin);
	return route ? `route:${route.chatId}:${route.messageThreadId ?? ""}` : `origin:${originKey(origin)}`;
}

export function subscribeTelegramDeliveries(
	gateway: GatewayClientLike,
	bot: Pick<TelegramBotApi, "sendMessage" | "setMessageReaction"> &
		Partial<Pick<TelegramBotApi, "sendPhoto" | "sendDocument">>,
	state: TelegramAdapterState,
	order: TelegramDeliveryOrder = new TelegramDeliveryOrder(),
): () => void {
	return gateway.onChatMessage((message) => {
		if (message.origin.platform !== "telegram" || !message.deliveryId) return;
		// A file delivery's text is only the "📎 name" fallback; it must never be
		// posted as a message when the upload itself is what was asked for.
		order.run(telegramDeliveryKey(state, message.origin), () =>
			message.reaction
				? settleTelegramReaction(gateway, bot, state, message)
				: message.file
					? settleTelegramFile(gateway, bot, state, message)
					: settleTelegramDelivery(gateway, bot, state, message),
		);
	});
}

export class TelegramAdapter {
	constructor(
		readonly state: TelegramAdapterState,
		readonly botUsername: string,
		readonly botUserId: string,
		readonly config: Pick<LoadedTelegramAdapterConfig, "chats">,
		readonly attachments: TelegramAttachmentSaver,
	) {}

	async handleUpdate(gateway: Pick<GatewayClientLike, "request">, update: TelegramUpdate): Promise<boolean> {
		if (!(await this.state.acceptUpdate(update.update_id))) return false;
		// An inbound reaction is engagement metadata, never a turn: it is reported via
		// engagement.reaction and must never reach chat.send.
		const reaction = describeTelegramReaction(update.message_reaction, this.botUserId);
		if (reaction) {
			await gateway.request("engagement.reaction", reaction);
			return true;
		}
		const message = update.message;
		if (!message?.from) return true;
		// A photo or file has no `text`; forwarding text alone dropped it without a trace.
		const content = message.text ?? message.caption ?? "";
		const attachment = telegramAttachment(message);
		if (content === "" && !attachment) return true;
		const origin = telegramMessageOrigin(message);
		await this.state.rememberOrigin(origin, origin.kind === "topic" ? message.message_thread_id : undefined);
		const baseEngagement = engagementForMessage(message, origin, this.botUsername, this.botUserId);
		const engagement =
			origin.kind !== "dm" && this.config.chats?.[origin.parentId ?? origin.conversationId]?.engagement === "open"
				? { ...baseEngagement, mentioned: true }
				: baseEngagement;
		const stored = attachment
			? await this.attachments.save(attachment, String(message.chat.id), message.message_id)
			: undefined;
		await gateway.request("chat.send", {
			origin,
			text: describeInboundBody(content, attachment, stored),
			engagement,
		});
		return true;
	}
}

export interface TelegramReactionEvent {
	readonly origin: OriginRef;
	readonly targetMessageId: string;
	readonly emoji: string;
	readonly action: "add" | "remove";
	readonly engagement: EngagementContext;
}

/**
 * Diffs `old_reaction` against `new_reaction` to describe what the reacting user
 * just did. An emoji that appeared is an "add", one that disappeared is a
 * "remove"; nothing changed (or a non-emoji custom/paid reaction) yields
 * undefined. One update that SWAPS a reaction contains both an add and a remove:
 * the addition is reported, because the emoji the user just chose is the current
 * signal and the retraction it replaced is not news. Reactions authored by our
 * own bot account are ignored so the persona never reacts to itself.
 */
export function describeTelegramReaction(
	update: TelegramMessageReactionUpdated | undefined,
	botUserId: string,
): TelegramReactionEvent | undefined {
	if (!update) return undefined;
	const actorId = String(update.user?.id ?? update.actor_chat?.id ?? "");
	if (!actorId || actorId === botUserId) return undefined;
	const before = new Set(emojiReactions(update.old_reaction));
	const after = emojiReactions(update.new_reaction);
	const added = after.find((emoji) => !before.has(emoji));
	for (const emoji of after) before.delete(emoji);
	const removed = [...before][0];
	const emoji = added ?? removed;
	if (!emoji) return undefined;
	const action = added ? ("add" as const) : ("remove" as const);
	// MessageReactionUpdated carries no message_thread_id, so a forum topic reaction
	// resolves to its parent chat origin — the finest grain Telegram gives us here.
	const origin = telegramMessageOrigin({ chat: update.chat, from: update.user ?? { id: actorId } });
	const authorName = update.user?.username ?? update.user?.first_name ?? update.actor_chat?.title;
	return {
		origin,
		targetMessageId: String(update.message_id),
		emoji,
		action,
		engagement: {
			mentioned: false,
			group: origin.kind !== "dm",
			authorId: actorId,
			...(authorName ? { authorName } : {}),
			...(update.chat.title ? { channelLabel: update.chat.title } : {}),
		},
	};
}

function emojiReactions(reactions: readonly TelegramReactionType[]): string[] {
	return reactions.flatMap((reaction) => (reaction.type === "emoji" && reaction.emoji ? [reaction.emoji] : []));
}

export function engagementForMessage(
	message: TelegramMessage,
	origin: OriginRef,
	botUsername: string,
	botUserId: string,
): EngagementContext {
	const replyTo = resolveTelegramReplyContext(message.reply_to_message, botUserId);
	// A reply to our own message is the same "addressed to us" signal as an @mention,
	// so it keeps reading as one — derived from replyTo instead of a second identity check.
	const mentioned =
		(message.text ?? message.caption)?.toLocaleLowerCase().includes(`@${botUsername.toLocaleLowerCase()}`) === true ||
		replyTo?.fromSelf === true;
	const authorName = message.from?.username ?? message.from?.first_name;
	return {
		mentioned,
		group: origin.kind !== "dm",
		authorId: String(message.from?.id ?? ""),
		...(message.from?.is_bot ? { authorIsBot: true } : {}),
		...(authorName ? { authorName } : {}),
		...(message.chat.title ? { channelLabel: message.chat.title } : {}),
		...(replyTo ? { replyTo } : {}),
	};
}

export async function startTelegramAdapter(config: LoadedTelegramAdapterConfig): Promise<void> {
	const state = await TelegramAdapterState.load(adapterHome());
	const bot = new TelegramBotApi(config.token);
	const identity = await bot.call<{ id: number | string; username?: string }>("getMe");
	if (!identity.username) throw new Error("Telegram bot account has no username");
	const attachmentDirectory = `${adapterHome()}/attachments/telegram`;
	const pruneAttachments = () =>
		void pruneTelegramAttachments(attachmentDirectory).catch((error) =>
			console.error(`Telegram attachment prune failed: ${error instanceof Error ? error.message : String(error)}`),
		);
	pruneAttachments();
	setInterval(pruneAttachments, ATTACHMENT_PRUNE_INTERVAL_MS).unref();
	const adapter = new TelegramAdapter(
		state,
		identity.username,
		String(identity.id),
		config,
		new TelegramAttachmentStore(attachmentDirectory, bot),
	);
	const gateway = new ReconnectingGateway(config.gatewaySocket ?? defaultGatewaySocket(), bot, state);
	await gateway.connectInitial();

	for (;;) {
		try {
			for (const update of await bot.getUpdates(state.updateId === undefined ? undefined : state.updateId + 1))
				await adapter.handleUpdate(gateway, update);
		} catch {
			await Bun.sleep(1_000);
		}
	}
}

class ReconnectingGateway implements GatewayClientLike {
	#client: GajaewayClient | undefined;
	#reconnecting = false;
	#attempt = 0;
	#deliveryOff: (() => void) | undefined;
	// Shared across reconnects so a delivery replayed on a new connection still
	// waits behind the conversation's in-flight settlement.
	readonly #deliveryOrder = new TelegramDeliveryOrder();
	#handlers = new Set<(message: ChatMessagePayload) => void>();

	constructor(
		readonly socketPath: string,
		readonly bot: TelegramBotApi,
		readonly state: TelegramAdapterState,
	) {}

	async connect(): Promise<void> {
		await this.#connect(false);
	}

	async connectInitial(): Promise<void> {
		await this.#connect(true);
	}

	async #connect(initial: boolean): Promise<void> {
		try {
			const options = {
				clientName: "adapter-telegram",
				onRetry: (attempt: number, delayMs: number, error: unknown) =>
					console.warn(
						`gateway_socket_waiting client=adapter-telegram path=${this.socketPath} attempt=${attempt} retry_in_ms=${delayMs} detail=${error instanceof Error ? error.message : String(error)}`,
					),
			};
			const client = initial
				? await GajaewayClient.connectSocketWithBackoff(this.socketPath, options)
				: await GajaewayClient.connectSocket(this.socketPath, options);
			this.#client = client;
			this.#attempt = 0;
			this.#deliveryOff?.();
			this.#deliveryOff = subscribeTelegramDeliveries(client, this.bot, this.state, this.#deliveryOrder);
			this.monitor(client);
		} catch {
			this.scheduleReconnect();
		}
	}

	async request<T = unknown>(verb: string, params?: unknown): Promise<T> {
		if (!this.#client) throw new Error("gateway is not connected");
		try {
			return await this.#client.request<T>(verb, params);
		} catch (error) {
			this.scheduleReconnect();
			throw error;
		}
	}

	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void {
		this.#handlers.add(handler);
		return () => this.#handlers.delete(handler);
	}

	private monitor(client: GajaewayClient): void {
		setTimeout(() => {
			if (this.#client !== client) return;
			void client.request("gateway.status").then(
				() => this.monitor(client),
				() => this.scheduleReconnect(),
			);
		}, 30_000);
	}

	private scheduleReconnect(): void {
		if (this.#reconnecting) return;
		this.#reconnecting = true;
		this.#client = undefined;
		this.#deliveryOff?.();
		const delay = Math.min(30_000, 500 * 2 ** Math.min(this.#attempt++, 6));
		const jitter = Math.floor(Math.random() * Math.max(1, delay / 4));
		setTimeout(() => {
			this.#reconnecting = false;
			void this.connect();
		}, delay + jitter);
	}
}

function defaultGatewaySocket(): string {
	return `${adapterHome()}/gateway.sock`;
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isTelegramResult(value: unknown): value is { readonly ok: true; readonly result: unknown } {
	return isObject(value) && value.ok === true && "result" in value;
}

export async function runTelegramAdapterWithLease<T>(home: string, start: (lease: Lease) => Promise<T>): Promise<T> {
	let lease: Lease;
	try {
		lease = await acquireLease(home, "adapter-telegram");
		console.info(`lease_acquired name=adapter-telegram pid=${process.pid}`);
	} catch (error) {
		if (!(error instanceof LeaseHeldError))
			console.error(
				`lease_failed name=adapter-telegram detail=${error instanceof Error ? error.message : String(error)}`,
			);
		throw error;
	}
	try {
		return await start(lease);
	} finally {
		await lease.release();
	}
}

if (import.meta.main) {
	void runTelegramAdapterWithLease(adapterHome(), async (lease) => {
		const shutdown = (): void => {
			void lease.release().then(
				() => process.exit(0),
				(error) => {
					console.error(
						`Telegram adapter lease release failed: ${error instanceof Error ? error.message : String(error)}`,
					);
					process.exit(1);
				},
			);
		};
		process.once("SIGINT", shutdown);
		process.once("SIGTERM", shutdown);
		await startTelegramAdapter(await loadTelegramAdapterConfig());
		await new Promise<void>(() => {});
	}).catch((error) => {
		if (error instanceof LeaseHeldError) exitOnLeaseHeld(error);
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	});
}
