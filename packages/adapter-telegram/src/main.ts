import type { ChatMessagePayload, EngagementContext, OriginRef } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { type LoadedTelegramAdapterConfig, loadTelegramAdapterConfig } from "./config";
import { type TelegramMessageOriginShape, telegramMessageOrigin } from "./origin";
import { telegramReactionFor } from "./reactions";
import { resolveTelegramReplyContext, type TelegramReplyMessageShape } from "./reply";
import { TelegramAdapterState } from "./state";

const TELEGRAM_MESSAGE_LIMIT = 4_096;

export interface GatewayClientLike {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void;
	close?(): Promise<void>;
}

export interface TelegramMessage extends TelegramMessageOriginShape {
	readonly message_id: number;
	readonly text?: string;
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

export class TelegramBotApi {
	constructor(
		readonly token: string,
		readonly fetcher: FetchLike = fetch,
	) {}

	async call<T>(method: string, parameters: Record<string, unknown> = {}): Promise<T> {
		const response = await this.fetcher(`https://api.telegram.org/bot${this.token}/${method}`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify(parameters),
		});
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
		return this.call("getUpdates", {
			timeout: 30,
			allowed_updates: ["message", "message_reaction"],
			...(offset === undefined ? {} : { offset }),
		});
	}

	sendMessage(chatId: string, text: string, messageThreadId?: number): Promise<unknown> {
		return this.call("sendMessage", {
			chat_id: chatId,
			text,
			...(messageThreadId === undefined ? {} : { message_thread_id: messageThreadId }),
		});
	}

	setMessageReaction(chatId: string, messageId: string, emoji: string): Promise<unknown> {
		// setMessageReaction sets the bot's chosen reactions on one message and returns
		// True; a single-element array is exactly one reaction, and bots may not use
		// paid reactions. https://core.telegram.org/bots/api (fetched 2026-08-27)
		return this.call("setMessageReaction", {
			chat_id: chatId,
			message_id: Number(messageId),
			reaction: [{ type: "emoji", emoji }],
		});
	}
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
	try {
		const mapped = telegramReactionFor(reaction);
		// TelegramApiError keeps `deliveryFailureIsAmbiguous` honest: an emoji Telegram
		// refuses is definitively not delivered, exactly like a rejected API call.
		if ("unsupported" in mapped) throw new TelegramApiError(400, mapped.unsupported);
		const route = state.routeFor(message.origin);
		if (!route)
			throw new TelegramApiError(400, `No persisted Telegram reply route for ${message.origin.conversationId}`);
		await bot.setMessageReaction(route.chatId, reaction.targetMessageId, mapped.emoji);
		await gateway.request("delivery.confirm", { deliveryId });
	} catch (error) {
		await gateway.request("delivery.fail", {
			deliveryId,
			reason: error instanceof Error ? error.message : String(error),
			ambiguous: deliveryFailureIsAmbiguous(error),
		});
	}
}

export function subscribeTelegramDeliveries(
	gateway: GatewayClientLike,
	bot: Pick<TelegramBotApi, "sendMessage" | "setMessageReaction">,
	state: TelegramAdapterState,
	log: Pick<Console, "error"> = console,
): () => void {
	return gateway.onChatMessage((message) => {
		const settled = message.reaction
			? settleTelegramReaction(gateway, bot, state, message)
			: settleTelegramDelivery(gateway, bot, state, message);
		void settled.catch((error) =>
			log.error(
				`Telegram delivery settlement request failed: ${error instanceof Error ? error.message : String(error)}`,
			),
		);
	});
}

export class TelegramAdapter {
	constructor(
		readonly state: TelegramAdapterState,
		readonly botUsername: string,
		readonly botUserId: string,
		readonly config: Pick<LoadedTelegramAdapterConfig, "chats">,
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
		if (!message?.from || !message.text) return true;
		const origin = telegramMessageOrigin(message);
		await this.state.rememberOrigin(origin, origin.kind === "topic" ? message.message_thread_id : undefined);
		const baseEngagement = engagementForMessage(message, origin, this.botUsername, this.botUserId);
		const engagement =
			origin.kind !== "dm" && this.config.chats?.[origin.parentId ?? origin.conversationId]?.engagement === "open"
				? { ...baseEngagement, mentioned: true }
				: baseEngagement;
		await gateway.request("chat.send", {
			origin,
			text: message.text,
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
		message.text?.toLocaleLowerCase().includes(`@${botUsername.toLocaleLowerCase()}`) === true ||
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
	const adapter = new TelegramAdapter(state, identity.username, String(identity.id), config);
	const gateway = new ReconnectingGateway(config.gatewaySocket ?? defaultGatewaySocket(), bot, state);
	await gateway.connect();
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
	#handlers = new Set<(message: ChatMessagePayload) => void>();

	constructor(
		readonly socketPath: string,
		readonly bot: TelegramBotApi,
		readonly state: TelegramAdapterState,
	) {}

	async connect(): Promise<void> {
		try {
			const client = await GajaewayClient.connectSocket(this.socketPath);
			this.#client = client;
			this.#attempt = 0;
			this.#deliveryOff?.();
			this.#deliveryOff = subscribeTelegramDeliveries(client, this.bot, this.state);
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

function adapterHome(): string {
	return process.env.GAJAEWAY_HOME ?? `${process.env.HOME ?? "~"}/.gajaeway`;
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

if (import.meta.main) {
	loadTelegramAdapterConfig()
		.then(startTelegramAdapter)
		.catch((error) => {
			console.error(error instanceof Error ? error.message : String(error));
			process.exitCode = 1;
		});
}
