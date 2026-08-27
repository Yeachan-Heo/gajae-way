import type { ChatMessagePayload, EngagementContext, OriginRef } from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";
import { type LoadedTelegramAdapterConfig, loadTelegramAdapterConfig } from "./config";
import { type TelegramMessageOriginShape, telegramMessageOrigin } from "./origin";
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
	readonly reply_to_message?: { readonly from?: { readonly id: number | string } };
}

export interface TelegramUpdate {
	readonly update_id: number;
	readonly message?: TelegramMessage;
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
		return this.call("getUpdates", { timeout: 30, ...(offset === undefined ? {} : { offset }) });
	}

	sendMessage(chatId: string, text: string, messageThreadId?: number): Promise<unknown> {
		return this.call("sendMessage", {
			chat_id: chatId,
			text,
			...(messageThreadId === undefined ? {} : { message_thread_id: messageThreadId }),
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

export function subscribeTelegramDeliveries(
	gateway: GatewayClientLike,
	bot: Pick<TelegramBotApi, "sendMessage">,
	state: TelegramAdapterState,
	log: Pick<Console, "error"> = console,
): () => void {
	return gateway.onChatMessage((message) => {
		void settleTelegramDelivery(gateway, bot, state, message).catch((error) =>
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
		const message = update.message;
		if (!message?.from || !message.text) return true;
		const origin = telegramMessageOrigin(message);
		await this.state.rememberOrigin(origin, origin.kind === "topic" ? message.message_thread_id : undefined);
		const baseEngagement = engagementForMessage(message, origin, this.botUsername, this.botUserId);
		const engagement =
			origin.kind !== "dm" && this.config.chats?.[origin.parentId ?? origin.conversationId]?.engagement === "open"
				? { ...baseEngagement, mentioned: true }
				: baseEngagement;
		await gateway.request("chat.send", { origin, text: message.text, engagement });
		return true;
	}
}

export function engagementForMessage(
	message: TelegramMessage,
	origin: OriginRef,
	botUsername: string,
	botUserId: string,
): EngagementContext {
	const mentioned =
		message.text?.toLocaleLowerCase().includes(`@${botUsername.toLocaleLowerCase()}`) === true ||
		String(message.reply_to_message?.from?.id ?? "") === botUserId;
	const authorName = message.from?.username ?? message.from?.first_name;
	return {
		mentioned,
		group: origin.kind !== "dm",
		authorId: String(message.from?.id ?? ""),
		...(message.from?.is_bot ? { authorIsBot: true } : {}),
		...(authorName ? { authorName } : {}),
		...(message.chat.title ? { channelLabel: message.chat.title } : {}),
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
