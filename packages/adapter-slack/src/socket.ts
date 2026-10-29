export interface SlackEnvelope {
	readonly envelope_id: string;
	readonly type: string;
	readonly payload?: unknown;
	readonly accepts_response_payload?: boolean;
	readonly retry_attempt?: number;
	readonly retry_reason?: string;
}

export interface SlackEventsApiPayload {
	readonly event_id?: string;
	readonly event?: Record<string, unknown>;
	readonly team_id?: string;
}

export interface SlackSlashCommand {
	readonly command: string;
	readonly text: string;
	readonly user_id: string;
	readonly user_name?: string;
	readonly channel_id: string;
	readonly channel_name?: string;
	readonly team_id?: string;
	readonly trigger_id: string;
	readonly response_url: string;
}

export interface WebSocketLike {
	send(data: string): void;
	close(code?: number, reason?: string): void;
	onopen: ((event: unknown) => void) | null;
	onmessage: ((event: { readonly data: unknown }) => void) | null;
	onclose: ((event: { readonly code?: number; readonly reason?: string }) => void) | null;
	onerror: ((event: unknown) => void) | null;
	/** Bun's client socket raises protocol ping/pong as events; a seam without them relies on frames alone. */
	addEventListener?(type: "ping" | "pong", listener: (event: unknown) => void): void;
	/** Client-initiated keepalive probe; a healthy peer answers with a pong. */
	ping?(data?: string): void;
}

export interface SocketModeHandlers {
	onEvent(event: Record<string, unknown>, envelope: SlackEnvelope): void | Promise<void>;
	onSlashCommand(command: SlackSlashCommand, envelope: SlackEnvelope): void | Promise<void>;
	onConnected?(): void;
	onDisconnected?(reason: string): void;
}

export interface SocketModeOptions {
	readonly factory?: (url: string) => WebSocketLike;
	readonly sleep?: (ms: number) => Promise<void>;
	readonly log?: Pick<Console, "log" | "error">;
	readonly random?: () => number;
	/**
	 * Force-close a socket that has carried no frame, ping, or pong for this long so
	 * the reconnect loop replaces it. Slack pings roughly every 10s and the adapter
	 * probes at half the window itself, so a healthy link never trips it.
	 */
	readonly livenessTimeoutMs?: number;
	readonly now?: () => number;
	readonly setInterval?: (work: () => void, ms: number) => unknown;
	readonly clearInterval?: (timer: unknown) => void;
	/** Bound on `apps.connections.open`; a hung call must not stall the reconnect loop. */
	readonly openTimeoutMs?: number;
	readonly setTimeout?: (work: () => void, ms: number) => unknown;
	readonly clearTimeout?: (timer: unknown) => void;
}

/**
 * A half-open TCP connection stays ESTABLISHED forever after a network blip:
 * no close frame arrives, Slack's periodic `disconnect` never does either,
 * and the adapter sits silent (live, 2026-10: 30+ hours with zero events).
 * Fifteen minutes is far beyond Slack's ~10s ping cadence yet short enough
 * that an outage is noticed within one planning horizon.
 */
export const DEFAULT_SOCKET_LIVENESS_TIMEOUT_MS = 15 * 60_000;
export const MIN_SOCKET_LIVENESS_TIMEOUT_MS = 30_000;
const LIVENESS_CHECK_MAX_MS = 60_000;
/**
 * `apps.connections.open` answers in well under a second. Without a bound, one
 * request hung on a dead keep-alive connection after a network drop parked the
 * reconnect loop forever: no socket, so no liveness watchdog, and no retry
 * (#710: "closed unexpectedly", then 18 hours of nothing).
 */
export const DEFAULT_SOCKET_OPEN_TIMEOUT_MS = 30_000;

export class SlackSocketMode {
	private ready?: Promise<void>;
	private cancel?: () => void;
	private retire?: (reason: string) => void;
	private generation = 0;
	private failures = 0;
	private isConnected = false;
	private opened = 0;
	private connectedAt: number | undefined;
	private lastEventAt: number | undefined;
	private readonly seen = new Map<string, string | undefined>();
	private retryTimer?: ReturnType<typeof setTimeout>;
	private readonly factory: (url: string) => WebSocketLike;
	private readonly sleep: (ms: number) => Promise<void>;
	private readonly log: Pick<Console, "log" | "error">;
	private readonly random: () => number;
	private readonly livenessTimeoutMs: number;
	private readonly now: () => number;
	private readonly setInterval: (work: () => void, ms: number) => unknown;
	private readonly clearInterval: (timer: unknown) => void;
	private readonly openTimeoutMs: number;
	private readonly setTimer: (work: () => void, ms: number) => unknown;
	private readonly clearTimer: (timer: unknown) => void;

	constructor(
		private readonly openConnection: (signal?: AbortSignal) => Promise<{ readonly url: string }>,
		private readonly handlers: SocketModeHandlers,
		options: SocketModeOptions = {},
	) {
		// The DOM WebSocket's handler signatures are contravariant in the event type, so the
		// structural cast is the honest way to say "a real socket satisfies the seam".
		this.factory = options.factory ?? ((url) => new WebSocket(url) as unknown as WebSocketLike);
		this.sleep =
			options.sleep ??
			((ms) =>
				new Promise((resolve) => {
					this.retryTimer = setTimeout(resolve, ms);
				}));
		this.log = options.log ?? console;
		this.random = options.random ?? Math.random;
		this.livenessTimeoutMs = options.livenessTimeoutMs ?? DEFAULT_SOCKET_LIVENESS_TIMEOUT_MS;
		this.now = options.now ?? Date.now;
		this.setInterval = options.setInterval ?? ((work, ms) => setInterval(work, ms));
		this.clearInterval = options.clearInterval ?? ((timer) => clearInterval(timer as ReturnType<typeof setInterval>));
		this.openTimeoutMs = options.openTimeoutMs ?? DEFAULT_SOCKET_OPEN_TIMEOUT_MS;
		this.setTimer = options.setTimeout ?? ((work, ms) => setTimeout(work, ms));
		this.clearTimer = options.clearTimeout ?? ((timer) => clearTimeout(timer as ReturnType<typeof setTimeout>));
	}

	get connected(): boolean {
		return this.isConnected;
	}
	get connections(): number {
		return this.opened;
	}
	/** When the current connection said hello; undefined while disconnected. */
	get connectedSince(): number | undefined {
		return this.isConnected ? this.connectedAt : undefined;
	}
	/** When the last events_api envelope arrived (pings, hello, and slash_commands excluded; the deaf-socket detector uses this to track message delivery). */
	get lastEnvelopeAt(): number | undefined {
		return this.lastEventAt;
	}

	/** Replace the current connection through the ordinary reconnect path. */
	restart(reason: string): void {
		this.retire?.(reason);
	}

	start(): Promise<void> {
		if (this.ready) return this.ready;
		const generation = ++this.generation;
		let ready!: () => void;
		this.ready = new Promise((resolve) => {
			ready = resolve;
		});
		const cancelled = new Promise<void>((resolve) => {
			this.cancel = resolve;
		});
		void this.run(generation, ready, cancelled);
		return this.ready;
	}

	stop(): void {
		++this.generation;
		this.cancel?.();
		clearTimeout(this.retryTimer);
		this.retire?.("Slack socket stopped");
		this.ready = undefined;
		this.isConnected = false;
	}

	/** One owner performs reconnection; duplicate close/error callbacks cannot fork it. */
	private async run(generation: number, ready: () => void, cancelled: Promise<void>): Promise<void> {
		while (generation === this.generation) {
			try {
				let openTimer: unknown;
				const openAbort = new AbortController();
				const connection = await Promise.race([
					this.openConnection(openAbort.signal),
					cancelled,
					new Promise<never>((_, reject) => {
						openTimer = this.setTimer(() => {
							openAbort.abort();
							reject(new SocketOpenTimeoutError(this.openTimeoutMs));
						}, this.openTimeoutMs);
					}),
				]).finally(() => this.clearTimer(openTimer));
				if (!connection || generation !== this.generation) break;
				const socket = this.factory(connection.url);
				await Promise.race([
					new Promise<void>((resolve) => {
						let retired = false;
						let opened = false;
						const createdAt = this.now();
						let lastActivityAt = createdAt;
						let probed = false;
						const touch = () => {
							lastActivityAt = this.now();
							probed = false;
						};
						const watchdog = this.setInterval(
							() => {
								// Pings can keep a socket that never said hello "active"; it still delivers nothing.
								const unannounced = this.now() - createdAt;
								if (!this.isConnected && unannounced >= this.livenessTimeoutMs) {
									this.log.log(
										`Slack socket never connected: no hello after ${Math.round(unannounced / 1000)}s; forcing reconnect.`,
									);
									retire("Slack socket never connected");
									return;
								}
								const quiet = this.now() - lastActivityAt;
								if (quiet >= this.livenessTimeoutMs) {
									this.log.log(
										`Slack socket stale: nothing received for ${Math.round(quiet / 1000)}s; forcing reconnect.`,
									);
									retire("Slack socket stale");
									return;
								}
								// A quiet workspace is not a dead link: ask before concluding anything.
								if (quiet >= this.livenessTimeoutMs / 2 && !probed && typeof socket.ping === "function") {
									probed = true;
									try {
										socket.ping();
									} catch {
										this.log.error("Slack socket ping failed");
									}
								}
							},
							Math.max(1_000, Math.min(LIVENESS_CHECK_MAX_MS, Math.floor(this.livenessTimeoutMs / 4))),
						);
						const retire = (reason: string) => {
							if (retired) return;
							retired = true;
							this.clearInterval(watchdog);
							socket.onopen = socket.onmessage = socket.onclose = socket.onerror = null;
							this.isConnected = false;
							this.retire = undefined;
							try {
								socket.close();
							} catch {
								this.log.error("Slack socket close failed");
							}
							try {
								this.handlers.onDisconnected?.(reason);
							} catch {
								this.log.error("Slack disconnect handler failed");
							}
							resolve();
						};
						this.retire = retire;
						socket.onopen = () => {
							touch();
							if (!opened) {
								opened = true;
								++this.opened;
							}
							ready();
						};
						socket.onclose = (event) => retire(event.reason || `Slack socket closed (${event.code ?? 0})`);
						socket.onerror = () => retire("Slack socket error");
						socket.onmessage = (event) => {
							touch();
							void this.receive(socket, event.data, retire);
						};
						if (typeof socket.addEventListener === "function") {
							socket.addEventListener("ping", touch);
							socket.addEventListener("pong", touch);
						}
					}),
					cancelled,
				]);
			} catch (error) {
				this.log.error(
					`Slack socket connection failed${error instanceof SocketOpenTimeoutError ? `: ${error.message}` : ""}`,
				);
			}
			if (generation !== this.generation) break;
			const delay = Math.min(30_000, 500 * 2 ** Math.min(this.failures++, 6)) * (0.5 + this.random() * 0.5);
			// Resolve startup even when Slack is unavailable; the loop owns retries.
			let waiting: Promise<void>;
			try {
				waiting = this.sleep(delay);
			} catch {
				ready();
				break;
			}
			ready();
			try {
				await Promise.race([waiting, cancelled]);
			} catch {
				this.log.error("Slack reconnect delay failed");
				break;
			}
		}
		ready();
	}

	private async receive(socket: WebSocketLike, data: unknown, retire: (reason: string) => void): Promise<void> {
		let frame: Record<string, unknown>;
		try {
			const parsed: unknown = JSON.parse(typeof data === "string" ? data : String(data));
			if (!isObject(parsed)) throw new Error("Slack frame is not an object");
			frame = parsed;
		} catch {
			this.log.error("Slack socket received an invalid frame");
			return;
		}
		try {
			// Slack retries unacknowledged envelopes; ACK must precede any application work.
			if (typeof frame.envelope_id === "string") socket.send(JSON.stringify({ envelope_id: frame.envelope_id }));
			if (frame.type === "hello") {
				this.failures = 0;
				if (!this.isConnected) {
					this.isConnected = true;
					this.connectedAt = this.now();
					this.lastEventAt = undefined;
					this.handlers.onConnected?.();
				}
				return;
			}
			if (frame.type === "disconnect") {
				retire("Slack requested reconnect");
				return;
			}
			if (typeof frame.envelope_id !== "string") {
				this.log.log("Slack socket ignored a non-envelope frame");
				return;
			}
			const payload = isObject(frame.payload) ? frame.payload : undefined;
			const eventId =
				frame.type === "events_api" && typeof payload?.event_id === "string" ? payload.event_id : undefined;
			let duplicate = this.seen.has(frame.envelope_id);
			// Keep 1024 envelopes, not 512 events with two separately counted identity keys.
			if (eventId !== undefined) {
				for (const [id, seenEventId] of this.seen) {
					if (seenEventId !== eventId) continue;
					duplicate = true;
					this.seen.delete(id);
					this.seen.set(id, seenEventId);
					break;
				}
			}
			this.seen.delete(frame.envelope_id);
			this.seen.set(frame.envelope_id, eventId);
			while (this.seen.size > 1024) this.seen.delete(this.seen.keys().next().value as string);
			if (typeof frame.retry_attempt === "number" && frame.retry_attempt > 0 && duplicate) return;
			const envelope = frame as unknown as SlackEnvelope;
			if (frame.type === "events_api" && isObject(payload?.event)) {
				this.lastEventAt = this.now();
				await this.handlers.onEvent(payload.event, envelope);
			} else if (frame.type === "slash_commands" && payload) {
				await this.handlers.onSlashCommand(payload as unknown as SlackSlashCommand, envelope);
			} else {
				this.log.log(`Slack socket ignored envelope type ${String(frame.type)}`);
			}
		} catch {
			this.log.error("Slack socket envelope handling failed");
		}
	}
}

class SocketOpenTimeoutError extends Error {
	constructor(ms: number) {
		super(`connections.open timed out after ${ms}ms`);
		this.name = "SocketOpenTimeoutError";
	}
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
