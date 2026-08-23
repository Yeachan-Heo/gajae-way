import { createHash } from "node:crypto";

export interface DiscordMessageReference {
	readonly channelId: string;
	readonly messageId: string;
}

export interface DiscordMessage {
	/** Discord's immutable MESSAGE_CREATE snowflake. */
	readonly id: string;
	readonly channelId: string;
	readonly text: string;
	readonly authorId?: string;
	readonly authorBot?: boolean;
	/** User ids from Discord's `mentions` array; roles/everyone are excluded. */
	readonly mentionedUserIds?: readonly string[];
	/** The MESSAGE_CREATE reference, when the inbound message is a reply. */
	readonly messageReference?: DiscordMessageReference;
	/** Available only when Discord included `referenced_message` in the dispatch. */
	readonly referencedMessageAuthorId?: string;
	/** Milliseconds when this adapter accepted the gateway dispatch. */
	readonly acceptedAt: number;
}


export type DiscordMessageHandler = (message: DiscordMessage) => void | Promise<void>;
export interface DiscordSendOptions {
	/** Reply target for the first outbound chunk; Discord may fall back to a plain post when it is gone. */
	readonly replyTo?: DiscordMessageReference;
	/** Durable event key used to derive retries for chunks after the first. */
	readonly dedupeKey?: string;
	/** Internal adapter hint: the caller already applied presentation formatting. */
	readonly formatted?: boolean;
}


/**
 * The only Discord boundary used by routing and egress. It intentionally has
 * no gateway, SDK, broker, or persistence concerns so the in-process fixture
 * can stand in for the real network implementation.
 */
export interface DiscordPlatform {
	connect(): Promise<void>;
	disconnect(): Promise<void>;
	onMessage(callback: DiscordMessageHandler): () => void;
	getCurrentUser(): Promise<DiscordCurrentUser>;
	send(channelId: string, text: string, nonce: string, options?: DiscordSendOptions): Promise<string>;
	ackTyping(channelId: string): Promise<void>;
	resolveThreadParent(channelId: string): Promise<string | undefined>;
	resolveMessageAuthor(channelId: string, messageId: string): Promise<string | undefined>;
	react(channelId: string, messageId: string, emoji: string): Promise<void>;
}


export class DiscordPlatformError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "DiscordPlatformError";
	}
}

export interface DiscordCurrentUser {
	readonly id: string;
	readonly username?: string;
}

export type DiscordFetch = (input: string, init?: RequestInit) => Promise<Response>;

export interface DiscordGatewayPlatformOptions {
	readonly token: string;
	readonly apiBaseUrl?: string;
	readonly gatewayUrl?: string;
	readonly fetch?: DiscordFetch;
	readonly webSocketFactory?: (url: string) => GatewaySocket;
	readonly now?: () => number;
	readonly onDiagnostic?: (message: string) => void;
	readonly reconnectBaseMs?: number;
	readonly reconnectMaxMs?: number;
}

interface GatewaySocket {
	readonly readyState: number;
	send(data: string): void;
	close(code?: number, reason?: string): void;
	addEventListener(type: string, callback: (event: unknown) => void): void;
	removeEventListener(type: string, callback: (event: unknown) => void): void;
}

interface GatewayEnvelope {
	readonly op: number;
	readonly d?: unknown;
	readonly s?: number | null;
	readonly t?: string | null;
}

interface Deferred<T> {
	readonly promise: Promise<T>;
	resolve(value: T): void;
	reject(error: Error): void;
	settled(): boolean;
}

const DISCORD_API_BASE_URL = "https://discord.com/api/v10";
const DEFAULT_RECONNECT_BASE_MS = 1_000;
const DEFAULT_RECONNECT_MAX_MS = 30_000;
const GATEWAY_CONNECT_TIMEOUT_MS = 15_000;
const GUILD_MESSAGES_INTENT = 1 << 9;
const DIRECT_MESSAGES_INTENT = 1 << 12;
const MESSAGE_CONTENT_INTENT = 1 << 15;

export const DISCORD_MAX_MESSAGE_LENGTH = 2_000;
export const TELEGRAM_MAX_MESSAGE_LENGTH = 4_096;
export const DISCORD_MAX_CHUNKS_PER_MESSAGE = 32;
export const DISCORD_TRUNCATION_MARKER = "\n… [Discord message truncated: chunk limit reached]";

export type AdapterPlatformName = "discord" | "telegram";
export type PlatformFormattingKind = "discord" | "telegram";

export interface PlatformCurrentUser {
	readonly id: string;
	readonly username?: string;
}

export interface PlatformMessageChunks {
	readonly chunks: readonly string[];
	readonly truncated: boolean;
}


export type DiscordMessageChunks = PlatformMessageChunks;

/**
 * Splits outbound text at paragraph, line, sentence, and finally hard boundaries.
 * Fenced blocks are closed and re-opened across chunks so each Discord post is
 * independently renderable. The original text is never mutated in the journal.
 */
export function splitDiscordMessage(text: string, maxChunks = DISCORD_MAX_CHUNKS_PER_MESSAGE): DiscordMessageChunks {
	if (!Number.isSafeInteger(maxChunks) || maxChunks < 1) throw new DiscordPlatformError("Discord message chunk bound must be a positive safe integer.");
	if (!text) return { chunks: [], truncated: false };
	const hasFence = /(^|\n)\s*(`{3,}|~{3,})/.test(text);
	const rawLimit = hasFence ? DISCORD_MAX_MESSAGE_LENGTH - 32 : DISCORD_MAX_MESSAGE_LENGTH;
	const rawChunks: string[] = [];
	let remaining = text;
	while (remaining && rawChunks.length < maxChunks) {
		const { prefix, remainder } = takeDiscordChunk(remaining, rawLimit);
		rawChunks.push(prefix);
		remaining = remainder;
	}
	const truncated = remaining.length > 0;
	if (truncated) {
		const marker = DISCORD_TRUNCATION_MARKER;
		const last = rawChunks.at(-1) ?? "";
		const available = Math.max(0, DISCORD_MAX_MESSAGE_LENGTH - marker.length);
		rawChunks[rawChunks.length - 1] = `${discordSlice(last, available)}${marker}`;
	}
	const chunks: string[] = [];
	let fence: DiscordFenceState | undefined;
	for (const raw of rawChunks) {
		let chunk = raw;
		if (fence) chunk = `${fence.openingLine}\n${chunk}`;
		const nextFence = discordFenceTransition(raw, fence);
		if (nextFence) chunk = `${chunk}\n${nextFence.closingMarker}`;
		if (chunk.length > DISCORD_MAX_MESSAGE_LENGTH) {
			// The reserved fence budget keeps this path rare; preserve the marker and
			// hard-trim only the presentation chunk, never the durable source text.
			const markerIndex = chunk.lastIndexOf(DISCORD_TRUNCATION_MARKER);
			chunk = markerIndex >= 0 && truncated
				? `${discordSlice(chunk.slice(0, markerIndex), Math.max(0, DISCORD_MAX_MESSAGE_LENGTH - DISCORD_TRUNCATION_MARKER.length))}${DISCORD_TRUNCATION_MARKER}`
				: discordSlice(chunk, DISCORD_MAX_MESSAGE_LENGTH);
		}
		chunks.push(chunk);
		fence = nextFence;
	}
	if (fence && chunks.length > 0) {
		const closing = `\n${fence.closingMarker}`;
		const last = chunks[chunks.length - 1] ?? "";
		const budget = Math.max(0, DISCORD_MAX_MESSAGE_LENGTH - closing.length);
		const markerIndex = last.lastIndexOf(DISCORD_TRUNCATION_MARKER);
		const content = markerIndex >= 0 && truncated
			? `${discordSlice(last.slice(0, markerIndex), Math.max(0, budget - DISCORD_TRUNCATION_MARKER.length))}${DISCORD_TRUNCATION_MARKER}`
			: discordSlice(last, budget);
		chunks[chunks.length - 1] = `${content}${closing}`;
	}
	return { chunks, truncated };
}

/** Applies Discord's presentation-only markdown rules without touching journal text. */
export function formatDiscordOutboundText(text: string): string {
	return wrapMultipleBareLinks(renderDiscordTables(text));
}

function renderDiscordTables(text: string): string {
	const lines = text.split("\n");
	const output: string[] = [];
	let fence: string | undefined;
	for (let index = 0; index < lines.length; index += 1) {
		const line = lines[index] ?? "";
		const fenceMatch = /^\s*(`{3,}|~{3,})/.exec(line);
		if (fenceMatch) {
			const marker = fenceMatch[1] as string;
			if (fence === undefined) fence = marker[0];
			else if (marker[0] === fence[0]) fence = undefined;
			output.push(line);
			continue;
		}
		const separator = lines[index + 1];
		if (fence === undefined && separator !== undefined && line.includes("|") && isDiscordTableSeparator(separator)) {
			const headers = discordTableCells(line);
			const rows: string[] = [];
			index += 2;
			while (index < lines.length && (lines[index]?.includes("|") ?? false)) {
				const cells = discordTableCells(lines[index] ?? "");
				if (cells.length > 0) rows.push(`- ${headers.map((header, cellIndex) => `${header}: ${cells[cellIndex] ?? ""}`).join("; ")}`);
				index += 1;
			}
			index -= 1;
			if (rows.length === 0) output.push(...headers.map(header => `- ${header}`));
			else output.push(...rows);
			continue;
		}
		output.push(line);
	}
	return output.join("\n");
}

function isDiscordTableSeparator(line: string): boolean {
	const cells = discordTableCells(line);
	return cells.length >= 2 && cells.every(cell => /^:?-{3,}:?$/.test(cell));
}

function discordTableCells(line: string): string[] {
	const trimmed = line.trim();
	const withoutEdges = trimmed.startsWith("|") ? trimmed.slice(1) : trimmed;
	const normalized = withoutEdges.endsWith("|") ? withoutEdges.slice(0, -1) : withoutEdges;
	return normalized.split("|").map(cell => cell.trim()).filter((cell, index, cells) => cell.length > 0 || index < cells.length - 1);
}

function wrapMultipleBareLinks(text: string): string {
	const pattern = /https?:\/\/[^\s<>]+/g;
	const matches = [...text.matchAll(pattern)];
	if (matches.length < 2) return text;
	return text.replace(pattern, (raw, offset: number) => {
		const before = text[offset - 1];
		const trailing = raw.match(/[.,!?;:)]+$/)?.[0] ?? "";
		const url = trailing ? raw.slice(0, -trailing.length) : raw;
		const markdownLink = before === "(" && text.slice(0, offset).endsWith("](");
		if (before === "<" || markdownLink || !url) return raw;
		return `<${url}>${trailing}`;
	});
}

interface DiscordFenceState {
	readonly openingLine: string;
	readonly closingMarker: string;
}

function discordSlice(text: string, limit: number): string {
	if (limit <= 0) return "";
	const sliced = text.slice(0, limit);
	const last = sliced.charCodeAt(sliced.length - 1);
	return last >= 0xd800 && last <= 0xdbff ? sliced.slice(0, -1) : sliced;
}

function takeDiscordChunk(text: string, limit: number): { prefix: string; remainder: string } {
	const candidate = discordSlice(text, limit);
	if (candidate.length >= text.length) return { prefix: text, remainder: "" };
	const paragraph = candidate.lastIndexOf("\n\n");
	if (paragraph >= 0) return splitAt(text, candidate, paragraph + 2);
	const line = candidate.lastIndexOf("\n");
	if (line >= 0) return splitAt(text, candidate, line + 1);
	const sentence = /[.!?](?:[\"')\]]*)\s+/g;
	let sentenceEnd = -1;
	for (const match of candidate.matchAll(sentence)) sentenceEnd = (match.index ?? 0) + match[0].length;
	if (sentenceEnd > 0) return splitAt(text, candidate, sentenceEnd);
	const whitespace = candidate.search(/\s+[^\s]*$/);
	if (whitespace > 0) {
		const whitespaceEnd = whitespace + (candidate.slice(whitespace).match(/\s+/)?.[0].length ?? 1);
		return splitAt(text, candidate, whitespaceEnd);
	}
	return splitAt(text, candidate, candidate.length);
}

function splitAt(source: string, candidate: string, codeUnitIndex: number): { prefix: string; remainder: string } {
	const prefix = candidate.slice(0, codeUnitIndex);
	return { prefix, remainder: source.slice(prefix.length) };
}

function discordFenceTransition(raw: string, incoming: DiscordFenceState | undefined): DiscordFenceState | undefined {
	let state = incoming;
	for (const line of raw.split("\n")) {
		const match = /^\s*(`{3,}|~{3,})(.*)$/.exec(line);
		if (!match) continue;
		const marker = match[1] as string;
		if (state) {
			if (marker[0] === state.closingMarker[0] && marker.length >= state.closingMarker.length) state = undefined;
		} else {
			state = { openingLine: line, closingMarker: marker[0]!.repeat(marker.length) };
		}
	}
	return state;
}


/** Validates a bot token without opening a gateway connection. */
export async function validateDiscordToken(
	token: string,
	fetchImpl: DiscordFetch = globalThis.fetch,
	apiBaseUrl = DISCORD_API_BASE_URL,
): Promise<DiscordCurrentUser> {
	const normalizedToken = requiredToken(token);
	const response = await fetchImpl(`${normalizeBaseUrl(apiBaseUrl)}/users/@me`, {
		headers: { Authorization: `Bot ${normalizedToken}` },
	});
	if (!response.ok) throw await restError("GET", "/users/@me", response);
	const body = await response.json();
	if (!isRecord(body) || typeof body.id !== "string" || !body.id) {
		throw new DiscordPlatformError("Discord GET /users/@me returned no user id.");
	}
	return { id: body.id, ...(typeof body.username === "string" ? { username: body.username } : {}) };
}

/**
 * Minimal dependency-free Discord implementation: gateway IDENTIFY,
 * HEARTBEAT, RESUME and MESSAGE_CREATE, plus REST send/typing/reaction calls.
 */
export class DiscordGatewayPlatform implements DiscordPlatform {
	readonly #token: string;
	readonly #apiBaseUrl: string;
	readonly #configuredGatewayUrl?: string;
	readonly #fetch: DiscordFetch;
	readonly #webSocketFactory: (url: string) => GatewaySocket;
	readonly #now: () => number;
	readonly #onDiagnostic: ((message: string) => void) | undefined;
	readonly #reconnectBaseMs: number;
	readonly #reconnectMaxMs: number;
	readonly #handlers = new Set<DiscordMessageHandler>();
	readonly #threadParentCache = new Map<string, Promise<string | undefined>>();
	#currentUser: Promise<DiscordCurrentUser> | undefined;
	readonly #messageAuthorCache = new Map<string, Promise<string | undefined>>();


	#socket: GatewaySocket | undefined;
	#sessionId: string | undefined;
	#resumeGatewayUrl: string | undefined;
	#sequence: number | null = null;
	#heartbeatTimer: ReturnType<typeof setInterval> | undefined;
	#heartbeatIntervalMs = 0;
	#heartbeatAcknowledged = true;
	#reconnectTimer: ReturnType<typeof setTimeout> | undefined;
	#reconnectDelayMs: number;
	#wanted = false;
	#ready: Deferred<void> | undefined;
	#initialConnect: Promise<void> | undefined;

	constructor(options: DiscordGatewayPlatformOptions) {
		this.#token = requiredToken(options.token);
		this.#apiBaseUrl = normalizeBaseUrl(options.apiBaseUrl ?? DISCORD_API_BASE_URL);
		this.#configuredGatewayUrl = options.gatewayUrl;
		this.#fetch = options.fetch ?? globalThis.fetch;
		this.#webSocketFactory = options.webSocketFactory ?? defaultWebSocketFactory;
		this.#now = options.now ?? Date.now;
		this.#onDiagnostic = options.onDiagnostic;
		this.#reconnectBaseMs = positiveInteger(options.reconnectBaseMs ?? DEFAULT_RECONNECT_BASE_MS, "reconnectBaseMs");
		this.#reconnectMaxMs = positiveInteger(options.reconnectMaxMs ?? DEFAULT_RECONNECT_MAX_MS, "reconnectMaxMs");
		if (this.#reconnectMaxMs < this.#reconnectBaseMs) {
			throw new DiscordPlatformError("reconnectMaxMs must be at least reconnectBaseMs.");
		}
		this.#reconnectDelayMs = this.#reconnectBaseMs;
	}

	async connect(): Promise<void> {
		this.#wanted = true;
		if (this.#socket && this.#ready?.settled()) return;
		if (!this.#initialConnect) {
			this.#initialConnect = this.openGateway().finally(() => {
				this.#initialConnect = undefined;
			});
		}
		await this.#initialConnect;
	}

	async disconnect(): Promise<void> {
		this.#wanted = false;
		if (this.#reconnectTimer) clearTimeout(this.#reconnectTimer);
		this.#reconnectTimer = undefined;
		this.clearHeartbeat();
		const ready = this.#ready;
		this.#ready = undefined;
		if (ready && !ready.settled()) ready.reject(new DiscordPlatformError("Discord gateway disconnected."));
		const socket = this.#socket;
		this.#socket = undefined;
		if (socket) socket.close(1_000, "adapter shutdown");
	}

	onMessage(callback: DiscordMessageHandler): () => void {
		this.#handlers.add(callback);
		return () => this.#handlers.delete(callback);
	}

	/** Returns the bot identity through the same verified GET /users/@me path as --check. */
	async getCurrentUser(): Promise<DiscordCurrentUser> {
		const existing = this.#currentUser;
		if (existing) return await existing;
		const lookup = validateDiscordToken(this.#token, this.#fetch, this.#apiBaseUrl);
		this.#currentUser = lookup;
		try {
			return await lookup;
		} catch (error) {
			if (this.#currentUser === lookup) this.#currentUser = undefined;
			throw error;
		}
	}


	async send(channelId: string, text: string, nonce: string, options?: DiscordSendOptions): Promise<string> {
		validateChannelId(channelId);
		if (!text.trim()) throw new DiscordPlatformError("Discord message text must not be empty.");
		if (!nonce) throw new DiscordPlatformError("Discord message nonce must not be empty.");
		const outboundText = options?.formatted ? text : formatDiscordOutboundText(text);
		const chunkSet = splitDiscordMessage(outboundText);
		if (chunkSet.truncated) {
			try {
				this.#onDiagnostic?.("Discord outbound message exceeded the chunk bound; sent a truncated presentation.");
			} catch {
				// Diagnostics must not interfere with durable delivery.
			}
		}
		const chunks = chunkSet.chunks;
		let firstMessageId: string | undefined;
		for (const [index, chunk] of chunks.entries()) {
			const chunkNonce = index === 0 ? nonce : discordChunkNonceFromBase(options?.dedupeKey ?? nonce, index);
			const messageId = await this.sendChunk(channelId, chunk, chunkNonce, index === 0 ? options?.replyTo : undefined);
			firstMessageId ??= messageId;
		}
		if (!firstMessageId) throw new DiscordPlatformError("Discord message text must not be empty.");
		return firstMessageId;
	}

	private async sendChunk(channelId: string, text: string, nonce: string, replyTo: DiscordMessageReference | undefined): Promise<string> {
		if (replyTo !== undefined) {
			validateChannelId(replyTo.channelId);
			validateMessageId(replyTo.messageId);
		}
		const body = {
			content: text,
			nonce,
			enforce_nonce: true,
			...(replyTo === undefined
				? {}
				: { message_reference: { channel_id: replyTo.channelId, message_id: replyTo.messageId, fail_if_not_exists: false } }),
		};
		let response: unknown;
		try {
			response = await this.rest("POST", `/channels/${encodeURIComponent(channelId)}/messages`, body);
		} catch (error) {
			if (replyTo !== undefined && isMissingReplyReference(error)) {
				response = await this.rest("POST", `/channels/${encodeURIComponent(channelId)}/messages`, {
					content: text,
					nonce,
					enforce_nonce: true,
				});
			} else {
				throw error;
			}
		}
		if (!isRecord(response) || typeof response.id !== "string" || !response.id) {
			throw new DiscordPlatformError("Discord message send response did not include an id.");
		}
		return response.id;
	}

	async ackTyping(channelId: string): Promise<void> {
		validateChannelId(channelId);
		await this.rest("POST", `/channels/${encodeURIComponent(channelId)}/typing`);
	}

	/**
	 * Resolves parentage only for a possible thread through a read-only REST
	 * lookup. Successful thread and non-thread results are cached per channel.
	 */
	async resolveThreadParent(channelId: string): Promise<string | undefined> {
		validateChannelId(channelId);
		const existing = this.#threadParentCache.get(channelId);
		if (existing) return await existing;
		const lookup = this.readThreadParent(channelId);
		this.#threadParentCache.set(channelId, lookup);
		try {
			return await lookup;
		} catch (error) {
			if (this.#threadParentCache.get(channelId) === lookup) this.#threadParentCache.delete(channelId);
			throw error;
		}
	}

	/** Resolves a reply target's author through cached read-only Discord REST. */
	async resolveMessageAuthor(channelId: string, messageId: string): Promise<string | undefined> {
		validateChannelId(channelId);
		validateMessageId(messageId);
		const key = `${channelId}:${messageId}`;
		const existing = this.#messageAuthorCache.get(key);
		if (existing) return await existing;
		const lookup = this.readMessageAuthor(channelId, messageId);
		this.#messageAuthorCache.set(key, lookup);
		try {
			return await lookup;
		} catch (error) {
			if (this.#messageAuthorCache.get(key) === lookup) this.#messageAuthorCache.delete(key);
			throw error;
		}
	}



	async react(channelId: string, messageId: string, emoji: string): Promise<void> {
		validateChannelId(channelId);
		if (!messageId) throw new DiscordPlatformError("Discord message id must not be empty.");
		if (!emoji) throw new DiscordPlatformError("Discord reaction emoji must not be empty.");
		await this.rest(
			"PUT",
			`/channels/${encodeURIComponent(channelId)}/messages/${encodeURIComponent(messageId)}/reactions/${encodeURIComponent(emoji)}/@me`,
		);
	}

	private async readThreadParent(channelId: string): Promise<string | undefined> {
		const channel = await this.rest("GET", `/channels/${encodeURIComponent(channelId)}`);
		if (!isRecord(channel) || !isDiscordThreadType(channel.type) || typeof channel.parent_id !== "string" || !/^\d+$/.test(channel.parent_id)) {
			return undefined;
		}
		return channel.parent_id;
	}

	private async readMessageAuthor(channelId: string, messageId: string): Promise<string | undefined> {
		const message = await this.rest("GET", `/channels/${encodeURIComponent(channelId)}/messages/${encodeURIComponent(messageId)}`);
		const author = isRecord(message) && isRecord(message.author) ? message.author : undefined;
		return typeof author?.id === "string" && author.id ? author.id : undefined;
	}


	private async rest(method: string, resource: string, body?: Record<string, unknown>): Promise<unknown> {
		const response = await this.#fetch(`${this.#apiBaseUrl}${resource}`, {
			method,
			headers: {
				Authorization: `Bot ${this.#token}`,
				...(body ? { "Content-Type": "application/json" } : {}),
			},
			...(body ? { body: JSON.stringify(body) } : {}),
		});
		if (!response.ok) throw await restError(method, resource, response);
		if (response.status === 204) return undefined;
		const text = await response.text();
		if (!text) return undefined;
		try {
			return JSON.parse(text) as unknown;
		} catch {
			throw new DiscordPlatformError(`Discord ${method} ${resource} returned invalid JSON.`);
		}
	}

	private async gatewayUrl(): Promise<string> {
		if (this.#resumeGatewayUrl) return this.#resumeGatewayUrl;
		if (this.#configuredGatewayUrl) return this.#configuredGatewayUrl;
		const discovered = await this.rest("GET", "/gateway/bot");
		if (!isRecord(discovered) || typeof discovered.url !== "string" || !discovered.url) {
			throw new DiscordPlatformError("Discord GET /gateway/bot returned no gateway URL.");
		}
		return discovered.url;
	}

	private async openGateway(): Promise<void> {
		const gatewayUrl = await this.gatewayUrl();
		const ready = deferred<void>();
		this.#ready = ready;
		let timer: ReturnType<typeof setTimeout> | undefined;
		const timeout = new Promise<never>((_, reject) => {
			timer = setTimeout(() => reject(new DiscordPlatformError("Timed out waiting for Discord gateway READY.")), GATEWAY_CONNECT_TIMEOUT_MS);
		});
		try {
			const socket = this.#webSocketFactory(gatewayUrlWithEncoding(gatewayUrl));
			this.#socket = socket;
			socket.addEventListener("message", event => this.onGatewayMessage(socket, event));
			socket.addEventListener("close", event => this.onGatewayClose(socket, event));
			socket.addEventListener("error", () => {
				// Discord closes the socket with the actionable status. Keep this
				// handler intentionally quiet to avoid treating transient errors as a
				// separate terminal state.
			});
			await Promise.race([ready.promise, timeout]);
			this.#reconnectDelayMs = this.#reconnectBaseMs;
		} catch (error) {
			if (this.#socket) this.#socket.close(4_000, "gateway startup failed");
			throw error;
		} finally {
			if (timer) clearTimeout(timer);
		}
	}

	private onGatewayMessage(socket: GatewaySocket, event: unknown): void {
		if (socket !== this.#socket) return;
		const raw = isRecord(event) && typeof event.data === "string" ? event.data : undefined;
		if (!raw) return;
		let envelope: GatewayEnvelope;
		try {
			envelope = JSON.parse(raw) as GatewayEnvelope;
		} catch {
			socket.close(4_000, "invalid gateway JSON");
			return;
		}
		if (typeof envelope.s === "number") this.#sequence = envelope.s;
		switch (envelope.op) {
			case 0:
				this.onDispatch(envelope);
				break;
			case 1:
				this.heartbeat();
				break;
			case 7:
				socket.close(4_000, "Discord requested reconnect");
				break;
			case 9:
				if (envelope.d !== true) {
					this.#sessionId = undefined;
					this.#resumeGatewayUrl = undefined;
					this.#sequence = null;
				}
				socket.close(4_000, "Discord invalidated session");
				break;
			case 10:
				this.onHello(envelope.d);
				break;
			case 11:
				this.#heartbeatAcknowledged = true;
				break;
			default:
				break;
		}
	}

	private onHello(data: unknown): void {
		if (!isRecord(data) || typeof data.heartbeat_interval !== "number" || data.heartbeat_interval <= 0) {
			this.#socket?.close(4_000, "invalid gateway hello");
			return;
		}
		const heartbeatIntervalMs = Math.floor(data.heartbeat_interval);
		this.clearHeartbeat();
		this.#heartbeatIntervalMs = heartbeatIntervalMs;
		this.#heartbeatAcknowledged = true;
		this.#heartbeatTimer = setInterval(() => this.heartbeat(), heartbeatIntervalMs);
		this.heartbeat();
		if (this.#sessionId && this.#sequence !== null) {
			this.sendGateway({ op: 6, d: { token: this.#token, session_id: this.#sessionId, seq: this.#sequence } });
			return;
		}
		this.sendGateway({
			op: 2,
			d: {
				token: this.#token,
				intents: GUILD_MESSAGES_INTENT | DIRECT_MESSAGES_INTENT | MESSAGE_CONTENT_INTENT,
				properties: { os: process.platform, browser: "gajaeway", device: "gajaeway" },
			},
		});
	}

	private onDispatch(envelope: GatewayEnvelope): void {
		if (envelope.t === "READY" && isRecord(envelope.d)) {
			if (typeof envelope.d.session_id === "string") this.#sessionId = envelope.d.session_id;
			if (typeof envelope.d.resume_gateway_url === "string") this.#resumeGatewayUrl = envelope.d.resume_gateway_url;
			this.#ready?.resolve();
			return;
		}
		if (envelope.t === "RESUMED") {
			this.#ready?.resolve();
			return;
		}
		if (envelope.t !== "MESSAGE_CREATE" || !isRecord(envelope.d)) return;
		const message = discordMessageFromDispatch(envelope.d, this.#now());
		if (!message) return;
		for (const callback of this.#handlers) {
			Promise.resolve(callback(message)).catch(error => {
				console.error(`gajaeway-discord message handler failed: ${error instanceof Error ? error.message : String(error)}`);
			});
		}
	}

	private heartbeat(): void {
		if (!this.#socket) return;
		if (!this.#heartbeatAcknowledged) {
			this.#socket.close(4_000, "Discord heartbeat ACK missing");
			return;
		}
		this.#heartbeatAcknowledged = false;
		this.sendGateway({ op: 1, d: this.#sequence });
	}

	private sendGateway(payload: unknown): void {
		try {
			this.#socket?.send(JSON.stringify(payload));
		} catch {
			this.#socket?.close(4_000, "gateway send failed");
		}
	}

	private onGatewayClose(socket: GatewaySocket, event: unknown): void {
		if (socket !== this.#socket) return;
		this.#socket = undefined;
		this.clearHeartbeat();
		const code = isRecord(event) && typeof event.code === "number" ? event.code : undefined;
		const ready = this.#ready;
		this.#ready = undefined;
		if (ready && !ready.settled()) ready.reject(new DiscordPlatformError(`Discord gateway closed${code ? ` (${code})` : ""}.`));
		if (!this.#wanted || isNonRetryableGatewayClose(code)) return;
		this.scheduleReconnect();
	}

	private scheduleReconnect(): void {
		if (!this.#wanted || this.#reconnectTimer) return;
		const delay = this.#reconnectDelayMs;
		this.#reconnectDelayMs = Math.min(this.#reconnectDelayMs * 2, this.#reconnectMaxMs);
		this.#reconnectTimer = setTimeout(() => {
			this.#reconnectTimer = undefined;
			void this.openGateway().catch(() => this.scheduleReconnect());
		}, delay);
	}

	private clearHeartbeat(): void {
		if (this.#heartbeatTimer) clearInterval(this.#heartbeatTimer);
		this.#heartbeatTimer = undefined;
		this.#heartbeatIntervalMs = 0;
		this.#heartbeatAcknowledged = true;
	}
}

function isDiscordThreadType(value: unknown): boolean {
	return value === 10 || value === 11 || value === 12;
}

function defaultWebSocketFactory(url: string): GatewaySocket {
	if (typeof WebSocket === "undefined") throw new DiscordPlatformError("This runtime does not provide WebSocket support.");
	return new WebSocket(url) as unknown as GatewaySocket;
}

function discordMessageFromDispatch(data: Record<string, unknown>, acceptedAt: number): DiscordMessage | undefined {
	if (typeof data.id !== "string" || !data.id || typeof data.channel_id !== "string" || !data.channel_id) return undefined;
	if (typeof data.content !== "string") return undefined;
	const author = isRecord(data.author) ? data.author : undefined;
	const mentions = Array.isArray(data.mentions)
		? data.mentions
			.map(mention => (isRecord(mention) && typeof mention.id === "string" && mention.id ? mention.id : undefined))
			.filter((id): id is string => id !== undefined)
		: [];
	const messageReference = discordMessageReference(data.message_reference, data.channel_id);
	const referencedMessage = isRecord(data.referenced_message) ? data.referenced_message : undefined;
	const referencedAuthor = referencedMessage && isRecord(referencedMessage.author) ? referencedMessage.author : undefined;
	return {
		id: data.id,
		channelId: data.channel_id,
		text: data.content,
		...(typeof author?.id === "string" ? { authorId: author.id } : {}),
		...(typeof author?.bot === "boolean" ? { authorBot: author.bot } : {}),
		...(mentions.length > 0 ? { mentionedUserIds: mentions } : {}),
		...(messageReference === undefined ? {} : { messageReference }),
		...(typeof referencedAuthor?.id === "string" && referencedAuthor.id ? { referencedMessageAuthorId: referencedAuthor.id } : {}),
		acceptedAt,
	};
}

function discordMessageReference(value: unknown, fallbackChannelId: string): DiscordMessageReference | undefined {
	if (!isRecord(value) || !isDiscordSnowflake(value.message_id)) return undefined;
	const channelId = isDiscordSnowflake(value.channel_id) ? value.channel_id : fallbackChannelId;
	if (!isDiscordSnowflake(channelId)) return undefined;
	return { channelId, messageId: value.message_id };
}


function requiredToken(token: string): string {
	const normalized = token.trim();
	if (!normalized) throw new DiscordPlatformError("Discord bot token must not be empty.");
	if (/\s/.test(normalized)) throw new DiscordPlatformError("Discord bot token must not contain whitespace.");
	return normalized;
}

function validateChannelId(channelId: string): void {
	if (!channelId || !/^\d+$/.test(channelId)) {
		throw new DiscordPlatformError("Discord channel id must be a non-empty snowflake.");
	}
}

function validateMessageId(messageId: string): void {
	if (!isDiscordSnowflake(messageId)) throw new DiscordPlatformError("Discord message id must be a non-empty snowflake.");
}

function isDiscordSnowflake(value: unknown): value is string {
	return typeof value === "string" && /^\d+$/.test(value);
}

function normalizeBaseUrl(baseUrl: string): string {
	const normalized = baseUrl.replace(/\/+$/, "");
	if (!/^https:\/\//.test(normalized)) throw new DiscordPlatformError("Discord API base URL must use HTTPS.");
	return normalized;
}

function gatewayUrlWithEncoding(url: string): string {
	const parsed = new URL(url);
	parsed.searchParams.set("v", "10");
	parsed.searchParams.set("encoding", "json");
	return parsed.toString();
}

function positiveInteger(value: number, name: string): number {
	if (!Number.isSafeInteger(value) || value < 1) throw new DiscordPlatformError(`${name} must be a positive safe integer.`);
	return value;
}

function isNonRetryableGatewayClose(code: number | undefined): boolean {
	return code === 4_004 || code === 4_010 || code === 4_011 || code === 4_012 || code === 4_013 || code === 4_014;
}

function discordChunkNonceFromBase(baseNonce: string, index: number): string {
	return createHash("sha256").update(`${baseNonce}:chunk:${index}`).digest("hex").slice(0, 24);
}

function isMissingReplyReference(error: unknown): boolean {
	return error instanceof DiscordPlatformError && /HTTP 404|10008|unknown message/i.test(error.message);
}

async function restError(method: string, resource: string, response: Response): Promise<DiscordPlatformError> {
	let detail = "";
	try {
		const body = (await response.text()).trim();
		if (body) detail = `: ${body.slice(0, 512)}`;
	} catch {
		// The status is sufficient when an intermediary supplied no readable body.
	}
	return new DiscordPlatformError(`Discord ${method} ${resource} failed with HTTP ${response.status}${detail}`);
}

function deferred<T>(): Deferred<T> {
	let resolved = false;
	let resolvePromise!: (value: T) => void;
	let rejectPromise!: (error: Error) => void;
	const promise = new Promise<T>((resolve, reject) => {
		resolvePromise = value => {
			if (resolved) return;
			resolved = true;
			resolve(value);
		};
		rejectPromise = error => {
			if (resolved) return;
			resolved = true;
			reject(error);
		};
	});
	return { promise, resolve: resolvePromise, reject: rejectPromise, settled: () => resolved };
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
