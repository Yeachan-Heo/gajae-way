import { join } from "node:path";
import type { ChatMessagePayload, EngagementContext, OriginRef } from "@gajaeway/protocol";
import { ProtocolError } from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";
import { adapterHome, loadTelegramAdapterConfig } from "./config";
import { type TelegramMessageOriginShape, telegramMessageOrigin } from "./origin";
import { telegramReactionFor } from "./reactions";
import { resolveTelegramReplyContext, type TelegramReplyMessageShape } from "./reply";
import { TelegramAdapterState } from "./state";

const TELEGRAM_MESSAGE_LIMIT = 4_096;
const TELEGRAM_BACKOFF_INITIAL_MS = 1_000;
const TELEGRAM_BACKOFF_MAX_MS = 30_000;

export interface GatewayClientLike {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void;
	close?(): void | Promise<void>;
}

export interface AdapterHandle {
	readonly stop: () => Promise<void>;
	readonly settled: Promise<void>;
}

/** Structural copy of the composition-owned generation contract. */
export interface Generation {
	readonly id: number;
	readonly signal: AbortSignal;
	readonly port: GatewayClientLike & { open(): Promise<unknown> };
	track<T>(task: Promise<T>): Promise<T>;
	sleep(ms: number): Promise<void>;
}

export interface TelegramAdapterInput {
	readonly token: string;
}

export type OpenGatewayClient = GatewayClientLike & { open(): Promise<unknown> };

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

	async call<T>(method: string, parameters: Record<string, unknown> = {}, signal?: AbortSignal): Promise<T> {
		const response = await this.fetcher(`https://api.telegram.org/bot${this.token}/${method}`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify(parameters),
			signal,
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

	getUpdates(offset?: number, signal?: AbortSignal): Promise<TelegramUpdate[]> {
		// `allowed_updates` REPLACES Telegram's default set, so "message" has to be
		// listed explicitly to keep the existing text path working while opting into
		// "message_reaction" (which is never in the default set).
		return this.call(
			"getUpdates",
			{
				timeout: 30,
				allowed_updates: ["message", "message_reaction"],
				...(offset === undefined ? {} : { offset }),
			},
			signal,
		);
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

export type TaskTracker = <T>(task: Promise<T>) => Promise<T>;

const identityTrack: TaskTracker = (task) => task;

export function subscribeTelegramDeliveries(
	gateway: GatewayClientLike,
	bot: Pick<TelegramBotApi, "sendMessage" | "setMessageReaction">,
	state: TelegramAdapterState,
	log: Pick<Console, "error"> = console,
	track: TaskTracker = identityTrack,
	shouldHandle: () => boolean = () => true,
): () => void {
	return gateway.onChatMessage((message) => {
		if (!shouldHandle()) return;
		const settled = message.reaction
			? settleTelegramReaction(gateway, bot, state, message)
			: settleTelegramDelivery(gateway, bot, state, message);
		void track(settled).catch((error) =>
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
		readonly log: Pick<Console, "error"> = console,
	) {}

	async handleUpdate(gateway: Pick<GatewayClientLike, "request">, update: TelegramUpdate): Promise<boolean> {
		if (!this.state.isNew(update.update_id)) return false;
		try {
			// An inbound reaction is engagement metadata, never a turn: it is reported via
			// engagement.reaction and must never reach chat.send.
			const reaction = describeTelegramReaction(update.message_reaction, this.botUserId);
			if (reaction) await gateway.request("engagement.reaction", reaction);
			else {
				const message = update.message;
				if (message?.from && message.text) {
					const origin = telegramMessageOrigin(message);
					await this.state.rememberOrigin(origin, origin.kind === "topic" ? message.message_thread_id : undefined);
					await gateway.request("chat.send", {
						origin,
						text: message.text,
						engagement: engagementForMessage(message, origin, this.botUsername, this.botUserId),
						messageId: `telegram:${this.botUserId}:update:${update.update_id}`,
					});
				}
			}
			await this.state.commit(update.update_id);
			return true;
		} catch (error) {
			if (error instanceof ProtocolError && error.code === "invalid_params") {
				this.log.error(`telegram_update_rejected update_id=${update.update_id} code=${error.code}`);
				await this.state.commit(update.update_id);
				return true;
			}
			throw error;
		}
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

/** Starts a generation-owned poller using a production Telegram Bot API client. */
export async function startTelegramAdapter(
	input: TelegramAdapterInput,
	gateway: OpenGatewayClient,
	home: string,
	gen: Generation,
): Promise<AdapterHandle> {
	return startTelegramAdapterWithBot(input, gateway, home, gen, new TelegramBotApi(input.token));
}

/** Test seam for the generation lifecycle; production callers use `startTelegramAdapter`. */
export async function startTelegramAdapterWithBot(
	_input: TelegramAdapterInput,
	gateway: OpenGatewayClient,
	home: string,
	gen: Generation,
	bot: Pick<TelegramBotApi, "call" | "getUpdates" | "sendMessage" | "setMessageReaction">,
	log: Pick<Console, "error"> = console,
): Promise<AdapterHandle> {
	const state = await TelegramAdapterState.load(home);
	const stopController = new AbortController();
	const signal = combinedSignal(gen.signal, stopController.signal);
	let deliveryOff: (() => void) | undefined;
	let stopPromise: Promise<void> | undefined;
	let opened = false;
	let adapter: TelegramAdapter | undefined;
	const tasks = new Set<Promise<unknown>>();

	const track: TaskTracker = <T>(task: Promise<T>): Promise<T> => {
		const tracked = gen.track(task);
		tasks.add(tracked);
		void tracked.finally(() => tasks.delete(tracked)).catch(() => {});
		return tracked;
	};

	const poller = track(
		(async () => {
			let attempt = 0;
			while (!signal.aborted) {
				try {
					if (!adapter) {
						const identity = await bot.call<{ id: number | string; username?: string }>("getMe", {}, signal);
						if (!identity.username) throw new Error("Telegram bot account has no username");
						adapter = new TelegramAdapter(state, identity.username, String(identity.id), log);
					}
					if (!opened) {
						deliveryOff ??= subscribeTelegramDeliveries(gateway, bot, state, log, track, () => !signal.aborted);
						await gateway.open();
						opened = true;
					}
					const offset = state.updateId === undefined ? undefined : state.updateId + 1;
					for (const update of await bot.getUpdates(offset, signal)) {
						if (signal.aborted) break;
						await adapter.handleUpdate(gateway, update);
					}
					attempt = 0;
				} catch (error) {
					if (signal.aborted || isAbortError(error)) return;
					if (isFatalTelegramError(error)) throw error;
					if (error instanceof TelegramApiError && error.status === 409) log.error("telegram_poll_conflict");
					try {
						await awaitWithAbort(gen.sleep(telegramBackoffDelay(attempt++)), signal);
					} catch (sleepError) {
						if (signal.aborted || isAbortError(sleepError)) return;
						throw sleepError;
					}
				}
			}
		})(),
	);
	// A start failure can reject before the supervisor observes the handle. Keep the
	// process from reporting that rejection twice while preserving `settled` for it.
	void poller.catch(() => {});

	return {
		settled: poller,
		stop: () => {
			stopPromise ??= (async () => {
				stopController.abort();
				deliveryOff?.();
				deliveryOff = undefined;
				await poller.catch(() => {});
				await Promise.allSettled([...tasks]);
				await gateway.close?.();
			})();
			return stopPromise;
		},
	};
}

export function telegramBackoffDelay(attempt: number, random: () => number = Math.random): number {
	const base = Math.min(TELEGRAM_BACKOFF_MAX_MS, TELEGRAM_BACKOFF_INITIAL_MS * 2 ** Math.max(0, attempt));
	return Math.max(1, Math.round(base * (0.75 + random() * 0.5)));
}

function isFatalTelegramError(error: unknown): error is TelegramApiError {
	return error instanceof TelegramApiError && (error.status === 401 || error.status === 404);
}

function combinedSignal(...signals: readonly AbortSignal[]): AbortSignal {
	if (signals.some((signal) => signal.aborted)) return AbortSignal.abort();
	const controller = new AbortController();
	for (const signal of signals) signal.addEventListener("abort", () => controller.abort(), { once: true });
	return controller.signal;
}

function abortableSleep(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise((resolve, reject) => {
		if (signal.aborted) {
			reject(abortError());
			return;
		}
		const timer = setTimeout(() => {
			signal.removeEventListener("abort", onAbort);
			resolve();
		}, ms);
		const onAbort = () => {
			clearTimeout(timer);
			signal.removeEventListener("abort", onAbort);
			reject(abortError());
		};
		signal.addEventListener("abort", onAbort, { once: true });
	});
}

function abortError(): Error {
	return Object.assign(new Error("The operation was aborted"), { name: "AbortError" });
}

function isAbortError(error: unknown): boolean {
	return error instanceof Error && error.name === "AbortError";
}

function awaitWithAbort<T>(task: Promise<T>, signal: AbortSignal): Promise<T> {
	return new Promise((resolve, reject) => {
		const onAbort = () => {
			signal.removeEventListener("abort", onAbort);
			reject(abortError());
		};
		if (signal.aborted) {
			void task.catch(() => {});
			onAbort();
			return;
		}
		signal.addEventListener("abort", onAbort, { once: true });
		void task.then(
			(value) => {
				signal.removeEventListener("abort", onAbort);
				resolve(value);
			},
			(error: unknown) => {
				signal.removeEventListener("abort", onAbort);
				reject(error);
			},
		);
	});
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isTelegramResult(value: unknown): value is { readonly ok: true; readonly result: unknown } {
	return isObject(value) && value.ok === true && "result" in value;
}

if (import.meta.main) {
	void (async () => {
		const config = await loadTelegramAdapterConfig();
		const client = await GajaewayClient.connectSocket(config.gatewaySocket ?? join(adapterHome(), "gateway.sock"));
		const gateway: OpenGatewayClient = {
			request: (verb, params) => client.request(verb, params),
			onChatMessage: (handler) => client.on("chat.message", (payload) => handler(payload as ChatMessagePayload)),
			open: async () => ({ replayed: 0 }),
			close: () => client.close(),
		};
		const controller = new AbortController();
		const gen: Generation = {
			id: 1,
			signal: controller.signal,
			port: gateway,
			track: (task) => task,
			sleep: (ms) => abortableSleep(ms, controller.signal),
		};
		const handle = await startTelegramAdapter({ token: config.token }, gateway, adapterHome(), gen);
		const stop = (): void => {
			controller.abort();
			void handle.stop();
		};
		process.once("SIGINT", stop);
		process.once("SIGTERM", stop);
		await handle.settled;
	})().catch((error) => {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	});
}
