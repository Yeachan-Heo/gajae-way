import * as fs from "node:fs";
import * as path from "node:path";
import type {
	DiscordCurrentUser,
	DiscordFetch,
	DiscordMessage,
	DiscordMessageHandler,
	DiscordPlatform,
	DiscordSendOptions,
} from "../discord/platform";

export interface TelegramPlatformOptions {
	readonly token: string;
	readonly apiBaseUrl?: string;
	readonly fetch?: DiscordFetch;
	readonly stateDir: string;
	readonly pollTimeoutSeconds?: number;
	readonly now?: () => number;
	onDiagnostic?(message: string): void;
}

interface TelegramUpdateState {
	offset: number;
	sends: Record<string, string>;
}

const TELEGRAM_API = "https://api.telegram.org";
const MAX_MESSAGE_LENGTH = 4_096;
const MAX_CHUNKS = 32;
const MAX_TRACKED_SENDS = 5_000;
const TRUNCATION = "\n… [Telegram message truncated: chunk limit reached]";
export function telegramFormatOutboundText(text: string): string {
	return text.replace(/^#{1,6}\s+(.+)$/gm, "**$1**");
}


export class TelegramPlatformError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "TelegramPlatformError";
	}
}

/** Telegram long-polling transport with durable update offset and send ledger. */
export class TelegramPlatform implements DiscordPlatform {
	readonly #token: string;
	readonly #apiBaseUrl: string;
	readonly #fetch: DiscordFetch;
	readonly #statePath: string;
	readonly #pollTimeoutSeconds: number;
	readonly #now: () => number;
	readonly #diagnostic: (message: string) => void;
	readonly #handlers = new Set<DiscordMessageHandler>();
	#state: TelegramUpdateState;
	#running = false;
	#pollTask: Promise<void> | undefined;
	#currentUser: Promise<DiscordCurrentUser> | undefined;

	constructor(options: TelegramPlatformOptions) {
		this.#token = requiredToken(options.token);
		this.#apiBaseUrl = (options.apiBaseUrl ?? TELEGRAM_API).replace(/\/+$/, "");
		this.#fetch = options.fetch ?? globalThis.fetch;
		if (!options.stateDir) throw new TelegramPlatformError("Telegram stateDir is required for durable offset and send settlement.");
		this.#statePath = path.join(options.stateDir, "telegram-offset.json");
		this.#pollTimeoutSeconds = options.pollTimeoutSeconds ?? 25;
		this.#now = options.now ?? Date.now;
		this.#diagnostic = options.onDiagnostic ?? (() => undefined);
		this.#state = readState(this.#statePath);
	}

	async connect(): Promise<void> {
		if (this.#running) return;
		this.#running = true;
		this.#pollTask = this.pollLoop();
	}

	async disconnect(): Promise<void> {
		this.#running = false;
		await this.#pollTask;
		this.#pollTask = undefined;
	}

	onMessage(callback: DiscordMessageHandler): () => void {
		this.#handlers.add(callback);
		return () => this.#handlers.delete(callback);
	}

	async getCurrentUser(): Promise<DiscordCurrentUser> {
		if (this.#currentUser) return await this.#currentUser;
		const request = this.call("getMe", {}).then(result => {
			const user = record(result);
			if (typeof user.id !== "number" && typeof user.id !== "string") throw new TelegramPlatformError("Telegram getMe returned no user id.");
			return { id: String(user.id), ...(typeof user.username === "string" ? { username: user.username } : {}) };
		});
		this.#currentUser = request;
		try {
			return await request;
		} catch (error) {
			if (this.#currentUser === request) this.#currentUser = undefined;
			throw error;
		}
	}

	async send(channelId: string, text: string, nonce: string, options?: DiscordSendOptions): Promise<string> {
		if (!text.trim()) throw new TelegramPlatformError("Telegram message text must not be empty.");
		const chunks = splitTelegram(telegramFormatOutboundText(text));
		let first: string | undefined;
		for (const [index, chunk] of chunks.entries()) {
			const key = `${options?.dedupeKey ?? nonce}:chunk:${index}`;
			const existing = this.#state.sends[key];
			if (existing) {
				first ??= existing;
				continue;
			}
			const result = record(await this.call("sendMessage", {
				chat_id: channelId,
				text: chunk,
				...(index === 0 && options?.replyTo ? { reply_to_message_id: options.replyTo.messageId } : {}),
			}));
			const message = record(result);
			const id = String(message.message_id ?? "");
			if (!id) throw new TelegramPlatformError("Telegram sendMessage returned no message id.");
			this.#state.sends[key] = id;
			writeState(this.#statePath, this.#state);
			first ??= id;
		}
		if (!first) throw new TelegramPlatformError("Telegram sendMessage produced no message id.");
		return first;
	}

	async ackTyping(channelId: string): Promise<void> {
		await this.call("sendChatAction", { chat_id: channelId, action: "typing" });
	}

	async resolveThreadParent(_channelId: string): Promise<string | undefined> {
		return undefined;
	}

	async resolveMessageAuthor(_channelId: string, _messageId: string): Promise<string | undefined> {
		return undefined;
	}

	async react(_channelId: string, _messageId: string, _emoji: string): Promise<void> {
		return;
	}

	private async pollLoop(): Promise<void> {
		while (this.#running) {
			try {
				const updates = await this.call("getUpdates", { offset: this.#state.offset, timeout: this.#pollTimeoutSeconds, allowed_updates: ["message"] });
				for (const updateValue of Array.isArray(updates) ? updates : []) {
					const update = record(updateValue);
					const updateId = Number(update.update_id);
					if (!Number.isSafeInteger(updateId)) continue;
					const message = telegramMessage(update.message, this.#now());
					if (message) for (const handler of [...this.#handlers]) await handler(message);
					this.#state.offset = updateId + 1;
					writeState(this.#statePath, this.#state);
				}
			} catch (error) {
				this.#diagnostic(`telegram polling failed: ${error instanceof Error ? error.message : String(error)}`);
				await new Promise(resolve => setTimeout(resolve, 250));
			}
			if (this.#running) await new Promise(resolve => setTimeout(resolve, 10));
		}
	}

	private async call(method: string, body: Record<string, unknown>): Promise<unknown> {
		const response = await this.#fetch(`${this.#apiBaseUrl}/bot${this.#token}/${method}`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify(body),
		});
		const payload = await response.json().catch(() => undefined);
		if (!response.ok || !record(payload).ok) throw new TelegramPlatformError(`Telegram ${method} failed with HTTP ${response.status}.`);
		return record(payload).result;
	}
}

export async function validateTelegramToken(token: string, fetchImpl: DiscordFetch = globalThis.fetch, apiBaseUrl = TELEGRAM_API): Promise<DiscordCurrentUser> {
	const response = await fetchImpl(`${apiBaseUrl.replace(/\/+$/, "")}/bot${requiredToken(token)}/getMe`, { method: "POST" });
	const payload = await response.json().catch(() => undefined);
	if (!response.ok || !record(payload).ok) throw new TelegramPlatformError("Telegram getMe failed.");
	const user = record(record(payload).result);
	return { id: String(user.id), ...(typeof user.username === "string" ? { username: user.username } : {}) };
}

function splitTelegram(text: string): readonly string[] {
	const chunks: string[] = [];
	let remaining = text;
	while (remaining && chunks.length < MAX_CHUNKS) {
		if (remaining.length <= MAX_MESSAGE_LENGTH) {
			chunks.push(remaining);
			remaining = "";
			break;
		}
		let cut = remaining.slice(0, MAX_MESSAGE_LENGTH).lastIndexOf("\n\n");
		if (cut < 1) cut = remaining.slice(0, MAX_MESSAGE_LENGTH).lastIndexOf("\n");
		if (cut < 1) cut = remaining.slice(0, MAX_MESSAGE_LENGTH).lastIndexOf(" ");
		if (cut < 1) cut = MAX_MESSAGE_LENGTH;
		chunks.push(remaining.slice(0, cut));
		remaining = remaining.slice(cut).trimStart();
	}
	if (remaining) {
		const last = chunks.at(-1) ?? "";
		chunks[chunks.length - 1] = `${last.slice(0, MAX_MESSAGE_LENGTH - TRUNCATION.length)}${TRUNCATION}`;
	}
	return chunks;
}

function requiredToken(token: string): string {
	const value = token.trim();
	if (!value || /\s/.test(value)) throw new TelegramPlatformError("Telegram bot token is invalid.");
	return value;
}

function telegramMessage(value: unknown, acceptedAt: number): DiscordMessage | undefined {
	const message = record(value);
	const chat = record(message.chat);
	const from = record(message.from);
	if (!message.message_id || (typeof chat.id !== "number" && typeof chat.id !== "string")) return undefined;
	const text = typeof message.text === "string" ? message.text : "";
	return {
		id: String(message.message_id),
		channelId: String(chat.id),
		text,
		...(from.id === undefined ? {} : { authorId: String(from.id) }),
		...(from.is_bot === undefined ? {} : { authorBot: from.is_bot === true }),
		...(record(message.reply_to_message).message_id === undefined ? {} : { messageReference: { channelId: String(chat.id), messageId: String(record(message.reply_to_message).message_id) } }),
		acceptedAt,
	};
}

function record(value: unknown): Record<string, any> {
	return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, any> : {};
}

/**
 * An ABSENT state file is a legitimate fresh start. A present-but-unreadable or
 * malformed one is NOT: silently resetting to offset 0 with an empty send ledger
 * would make Telegram re-deliver already-handled updates AND discard the chunk
 * dedupe records that stand in for Discord's `enforce_nonce`, producing duplicate
 * persona turns and duplicate posts. Corruption therefore fails closed.
 */
function readState(filePath: string): TelegramUpdateState {
	let raw: string;
	try {
		raw = fs.readFileSync(filePath, "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException)?.code === "ENOENT") return { offset: 0, sends: {} };
		throw new TelegramPlatformError(
			`Telegram durable state at ${filePath} could not be read: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	let value: unknown;
	try {
		value = JSON.parse(raw);
	} catch (error) {
		throw new TelegramPlatformError(
			`Telegram durable state at ${filePath} is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new TelegramPlatformError(`Telegram durable state at ${filePath} is not an object.`);
	}
	const state = value as Record<string, unknown>;
	if (!Number.isSafeInteger(state.offset) || (state.offset as number) < 0) {
		throw new TelegramPlatformError(`Telegram durable state at ${filePath} has an invalid update offset.`);
	}
	const sendsValue = state.sends;
	if (typeof sendsValue !== "object" || sendsValue === null || Array.isArray(sendsValue)) {
		throw new TelegramPlatformError(`Telegram durable state at ${filePath} has an invalid send ledger.`);
	}
	const sends: Record<string, string> = {};
	for (const [key, entry] of Object.entries(sendsValue as Record<string, unknown>)) {
		if (typeof entry !== "string" || !entry) {
			throw new TelegramPlatformError(`Telegram durable state at ${filePath} has a malformed send-ledger entry for ${key}.`);
		}
		sends[key] = entry;
	}
	return { offset: state.offset as number, sends };
}

/**
 * Bounds the send ledger. Discord delegates duplicate suppression to
 * `enforce_nonce` server-side; Telegram has no equivalent, so these records are
 * the only dedupe authority and would otherwise grow without limit while being
 * fully rewritten on every chunk. Oldest insertions are dropped first, well past
 * any realistic redelivery window.
 */
function pruneSends(state: TelegramUpdateState): void {
	const keys = Object.keys(state.sends);
	if (keys.length <= MAX_TRACKED_SENDS) return;
	for (const key of keys.slice(0, keys.length - MAX_TRACKED_SENDS)) delete state.sends[key];
}

function writeState(filePath: string, state: TelegramUpdateState): void {
	pruneSends(state);
	fs.mkdirSync(path.dirname(filePath), { recursive: true });
	const temporary = `${filePath}.${process.pid}.tmp`;
	fs.writeFileSync(temporary, JSON.stringify(state));
	fs.renameSync(temporary, filePath);
}
