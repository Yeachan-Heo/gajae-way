export interface DiscordMessage {
	/** Discord's immutable MESSAGE_CREATE snowflake. */
	readonly id: string;
	readonly channelId: string;
	readonly text: string;
	readonly authorId?: string;
	readonly authorBot?: boolean;
	/** Milliseconds when this adapter accepted the gateway dispatch. */
	readonly acceptedAt: number;
}

export type DiscordMessageHandler = (message: DiscordMessage) => void | Promise<void>;

/**
 * The only Discord boundary used by routing and egress. It intentionally has
 * no gateway, SDK, broker, or persistence concerns so the in-process fixture
 * can stand in for the real network implementation.
 */
export interface DiscordPlatform {
	connect(): Promise<void>;
	disconnect(): Promise<void>;
	onMessage(callback: DiscordMessageHandler): () => void;
	send(channelId: string, text: string, nonce: string): Promise<string>;
	ackTyping(channelId: string): Promise<void>;
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
const DIRECT_MESSAGES_INTENT = 1 << 12;
const MESSAGE_CONTENT_INTENT = 1 << 15;

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
	readonly #reconnectBaseMs: number;
	readonly #reconnectMaxMs: number;
	readonly #handlers = new Set<DiscordMessageHandler>();
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

	async send(channelId: string, text: string, nonce: string): Promise<string> {
		validateChannelId(channelId);
		if (!text.trim()) throw new DiscordPlatformError("Discord message text must not be empty.");
		if (text.length > 2_000) throw new DiscordPlatformError("Discord message text exceeds the 2000-character platform limit.");
		if (!nonce) throw new DiscordPlatformError("Discord message nonce must not be empty.");
		const response = await this.rest("POST", `/channels/${encodeURIComponent(channelId)}/messages`, {
			content: text,
			nonce,
			enforce_nonce: true,
		});
		if (!isRecord(response) || typeof response.id !== "string" || !response.id) {
			throw new DiscordPlatformError("Discord message send response did not include an id.");
		}
		return response.id;
	}

	async ackTyping(channelId: string): Promise<void> {
		validateChannelId(channelId);
		await this.rest("POST", `/channels/${encodeURIComponent(channelId)}/typing`);
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
		this.#heartbeatIntervalMs = Math.floor(data.heartbeat_interval);
		this.#heartbeatAcknowledged = true;
		this.clearHeartbeat();
		this.#heartbeatTimer = setInterval(() => this.heartbeat(), this.#heartbeatIntervalMs);
		this.heartbeat();
		if (this.#sessionId && this.#sequence !== null) {
			this.sendGateway({ op: 6, d: { token: this.#token, session_id: this.#sessionId, seq: this.#sequence } });
			return;
		}
		this.sendGateway({
			op: 2,
			d: {
				token: this.#token,
				intents: DIRECT_MESSAGES_INTENT | MESSAGE_CONTENT_INTENT,
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

function defaultWebSocketFactory(url: string): GatewaySocket {
	if (typeof WebSocket === "undefined") throw new DiscordPlatformError("This runtime does not provide WebSocket support.");
	return new WebSocket(url) as unknown as GatewaySocket;
}

function discordMessageFromDispatch(data: Record<string, unknown>, acceptedAt: number): DiscordMessage | undefined {
	if (typeof data.id !== "string" || !data.id || typeof data.channel_id !== "string" || !data.channel_id) return undefined;
	if (typeof data.content !== "string") return undefined;
	const author = isRecord(data.author) ? data.author : undefined;
	return {
		id: data.id,
		channelId: data.channel_id,
		text: data.content,
		...(typeof author?.id === "string" ? { authorId: author.id } : {}),
		...(typeof author?.bot === "boolean" ? { authorBot: author.bot } : {}),
		acceptedAt,
	};
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
