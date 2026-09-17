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
}

export class SlackSocketMode {
	private ready?: Promise<void>;
	private cancel?: () => void;
	private retire?: (reason: string) => void;
	private generation = 0;
	private failures = 0;
	private isConnected = false;
	private opened = 0;
	private readonly seen = new Map<string, string | undefined>();
	private retryTimer?: ReturnType<typeof setTimeout>;
	private readonly factory: (url: string) => WebSocketLike;
	private readonly sleep: (ms: number) => Promise<void>;
	private readonly log: Pick<Console, "log" | "error">;
	private readonly random: () => number;

	constructor(
		private readonly openConnection: () => Promise<{ readonly url: string }>,
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
	}

	get connected(): boolean {
		return this.isConnected;
	}
	get connections(): number {
		return this.opened;
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
				const connection = await Promise.race([this.openConnection(), cancelled]);
				if (!connection || generation !== this.generation) break;
				const socket = this.factory(connection.url);
				await Promise.race([
					new Promise<void>((resolve) => {
						let retired = false;
						let opened = false;
						const retire = (reason: string) => {
							if (retired) return;
							retired = true;
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
							if (!opened) {
								opened = true;
								++this.opened;
							}
							ready();
						};
						socket.onclose = (event) => retire(event.reason || `Slack socket closed (${event.code ?? 0})`);
						socket.onerror = () => retire("Slack socket error");
						socket.onmessage = (event) => {
							void this.receive(socket, event.data, retire);
						};
					}),
					cancelled,
				]);
			} catch {
				this.log.error("Slack socket connection failed");
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

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
