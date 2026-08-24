import type { AdapterPlatform, InboundMessage, SendResult } from "../runtime/protocol";

export const TELEGRAM_API_BASE_URL = "https://api.telegram.org";
const DEFAULT_LONG_POLL_SECONDS = 25;
const RATE_LIMIT_MAX_RETRIES = 3;
const RATE_LIMIT_MAX_WAIT_MS = 5_000;

export type TelegramFetch = (input: string, init?: RequestInit) => Promise<Response>;

export class TelegramPlatformError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "TelegramPlatformError";
	}
}

export interface TelegramPlatformOptions {
	readonly token: string;
	readonly apiBaseUrl?: string;
	readonly fetch?: TelegramFetch;
	/** Test seam so rate-limit backoff never waits in real time. */
	readonly sleep?: (milliseconds: number) => Promise<void>;
	readonly longPollSeconds?: number;
}

/**
 * Dependency-free Telegram Bot API driver.
 *
 * Declares `at_least_once`: the Bot API exposes no send-idempotency key, so the
 * shared runtime guarantees the send-before-commit ordering invariant and
 * suppresses duplicates within one process lifetime, but a crash or two
 * overlapping adapter processes can still duplicate. `ack` is a no-op because
 * Telegram has no per-message acknowledgement primitive; `typing` maps to
 * sendChatAction.
 */
export class TelegramPlatform implements AdapterPlatform {
	readonly dedupe = "at_least_once" as const;
	readonly #token: string;
	readonly #apiBaseUrl: string;
	readonly #fetch: TelegramFetch;
	readonly #sleep: (milliseconds: number) => Promise<void>;
	readonly #longPollSeconds: number;
	readonly #disconnectHandlers = new Set<(reason: string) => void>();
	#offset = 0;
	#running = false;
	#loop: Promise<void> | undefined;

	constructor(options: TelegramPlatformOptions) {
		if (!options.token.trim()) throw new TelegramPlatformError("Telegram bot token must not be empty.");
		this.#token = options.token;
		this.#apiBaseUrl = (options.apiBaseUrl ?? TELEGRAM_API_BASE_URL).replace(/\/+$/u, "");
		this.#fetch = options.fetch ?? globalThis.fetch;
		this.#sleep = options.sleep ?? ((milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds)));
		this.#longPollSeconds = options.longPollSeconds ?? DEFAULT_LONG_POLL_SECONDS;
	}

	async start(onMessage: (message: InboundMessage) => Promise<void>): Promise<void> {
		if (this.#running) return;
		this.#running = true;
		this.#loop = this.poll(onMessage);
	}

	async stop(): Promise<void> {
		this.#running = false;
		const loop = this.#loop;
		this.#loop = undefined;
		if (loop) await loop.catch(() => undefined);
	}

	onDisconnect(handler: (reason: string) => void): void {
		this.#disconnectHandlers.add(handler);
	}

	async send(chatId: string, text: string, _options: { readonly nonce?: string }): Promise<SendResult> {
		// A nonce is deliberately ignored: Telegram has no equivalent, and
		// pretending otherwise would imply a guarantee the platform cannot keep.
		const result = await this.call("sendMessage", { chat_id: chatId, text });
		const messageId = isRecord(result) ? result.message_id : undefined;
		return typeof messageId === "number" || typeof messageId === "string" ? { platformMsgId: String(messageId) } : {};
	}

	async ack(_chatId: string, _platformMsgId: string): Promise<void> {
		// Telegram exposes no per-message acknowledgement primitive.
	}

	async typing(chatId: string): Promise<void> {
		await this.call("sendChatAction", { chat_id: chatId, action: "typing" });
	}

	/** Single poll pass, exposed so tests drive the loop deterministically. */
	async pollOnce(onMessage: (message: InboundMessage) => Promise<void>): Promise<number> {
		const result = await this.call("getUpdates", {
			offset: this.#offset,
			timeout: this.#longPollSeconds,
			allowed_updates: ["message"],
		});
		if (!Array.isArray(result)) return 0;
		let handled = 0;
		for (const update of result) {
			if (!isRecord(update)) continue;
			const updateId = typeof update.update_id === "number" ? update.update_id : undefined;
			// Advance past every update, including ones we do not route, or the
			// same batch is returned forever.
			if (updateId !== undefined) this.#offset = Math.max(this.#offset, updateId + 1);
			const inbound = toInboundMessage(update.message);
			if (!inbound) continue;
			await onMessage(inbound);
			handled += 1;
		}
		return handled;
	}

	private async poll(onMessage: (message: InboundMessage) => Promise<void>): Promise<void> {
		while (this.#running) {
			try {
				await this.pollOnce(onMessage);
			} catch (error) {
				if (!this.#running) return;
				const reason = error instanceof Error ? error.message : String(error);
				for (const handler of this.#disconnectHandlers) handler(reason);
				await this.#sleep(1_000);
			}
		}
	}

	private async call(method: string, body: Record<string, unknown>): Promise<unknown> {
		let response = await this.request(method, body);
		// 429 is expected traffic on a bot API, not an exceptional case.
		for (let attempt = 0; response.status === 429 && attempt < RATE_LIMIT_MAX_RETRIES; attempt += 1) {
			const waitMs = await retryAfterMs(response);
			if (waitMs === undefined) break;
			await this.#sleep(waitMs);
			response = await this.request(method, body);
		}
		const text = await response.text();
		let parsed: unknown;
		try {
			parsed = text ? (JSON.parse(text) as unknown) : undefined;
		} catch {
			throw new TelegramPlatformError(`Telegram ${method} returned invalid JSON.`);
		}
		if (!response.ok || !isRecord(parsed) || parsed.ok !== true) {
			const description =
				isRecord(parsed) && typeof parsed.description === "string" ? parsed.description : response.statusText;
			throw new TelegramPlatformError(`Telegram ${method} failed with ${response.status}: ${description}`);
		}
		return parsed.result;
	}

	private async request(method: string, body: Record<string, unknown>): Promise<Response> {
		return await this.#fetch(`${this.#apiBaseUrl}/bot${this.#token}/${method}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(body),
		});
	}
}

export function toInboundMessage(value: unknown): InboundMessage | undefined {
	if (!isRecord(value)) return undefined;
	const chat = isRecord(value.chat) ? value.chat : undefined;
	const from = isRecord(value.from) ? value.from : undefined;
	const chatId = chat?.id;
	const messageId = value.message_id;
	const text = value.text;
	if (typeof text !== "string" || !text.trim()) return undefined;
	if (typeof chatId !== "number" && typeof chatId !== "string") return undefined;
	if (typeof messageId !== "number" && typeof messageId !== "string") return undefined;
	return {
		platformMsgId: String(messageId),
		chatId: String(chatId),
		text,
		senderId: from?.id === undefined ? "" : String(from.id),
		...(from?.is_bot === true ? { authorBot: true } : {}),
	};
}

async function retryAfterMs(response: Response): Promise<number | undefined> {
	const header = response.headers.get("retry-after");
	let seconds = header === null ? Number.NaN : Number(header);
	if (!Number.isFinite(seconds)) {
		try {
			const parsed = JSON.parse(await response.clone().text()) as unknown;
			const parameters = isRecord(parsed) && isRecord(parsed.parameters) ? parsed.parameters : undefined;
			const candidate = parameters?.retry_after;
			if (typeof candidate === "number" && Number.isFinite(candidate)) seconds = candidate;
		} catch {
			return undefined;
		}
	}
	if (!Number.isFinite(seconds) || seconds < 0) return undefined;
	return Math.min(Math.ceil(seconds * 1_000), RATE_LIMIT_MAX_WAIT_MS);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
