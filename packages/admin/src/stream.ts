/**
 * Server-sent-event fan-out.
 *
 * The admin process holds one persistent gateway connection and fans it out to N
 * browser `EventSource` clients. SSE rather than a WebSocket on purpose:
 * bidirectionality would be a liability, because every mutation must travel the
 * audited POST path and never an unlogged socket frame.
 *
 * The admin process is explicitly **not** an event store. The gateway offers no
 * event replay, so `Last-Event-ID` is not honoured with a delta chain; a
 * reconnecting client is sent a freshly recomputed `snapshot` and replaces
 * wholesale. Correct-by-reconstruction beats a delta chain with a hole in it.
 */

/** Idle-timeout guard, and the browser's own liveness proof. */
export const KEEPALIVE_MS = 15_000;
/** Normal reconnect interval handed to `EventSource`. */
export const RETRY_MS = 3_000;
/** Backed-off interval used while the gateway itself is unreachable. */
export const RETRY_DEGRADED_MS = 15_000;

type Client = {
	readonly write: (chunk: string) => void;
	readonly close: () => void;
};

export type StreamHubOptions = {
	/** Full state for a client that just connected or reconnected. */
	readonly snapshot: () => Promise<unknown>;
	/** True while the admin process can talk to the gateway. */
	readonly gatewayReachable: () => boolean;
};

export class StreamHub {
	readonly #clients = new Set<Client>();
	readonly #options: StreamHubOptions;
	#keepalive: ReturnType<typeof setInterval> | undefined;
	#id = 0;

	constructor(options: StreamHubOptions) {
		this.#options = options;
	}

	get clientCount(): number {
		return this.#clients.size;
	}

	/** Open one SSE response. The initial payload is always a full snapshot. */
	connect(): Response {
		const encoder = new TextEncoder();
		let client: Client | undefined;

		const body = new ReadableStream<Uint8Array>({
			start: async (controller) => {
				let open = true;
				client = {
					write: (chunk) => {
						if (!open) return;
						try {
							controller.enqueue(encoder.encode(chunk));
						} catch {
							open = false;
							if (client) this.#clients.delete(client);
						}
					},
					close: () => {
						if (!open) return;
						open = false;
						try {
							controller.close();
						} catch {
							// Already closed by the peer.
						}
					},
				};
				this.#clients.add(client);
				this.#startKeepalive();

				const retry = this.#options.gatewayReachable() ? RETRY_MS : RETRY_DEGRADED_MS;
				client.write(`retry: ${retry}\n\n`);
				client.write(this.#frame("snapshot", await this.#options.snapshot()));
			},
			cancel: () => {
				if (client) this.#clients.delete(client);
				this.#stopKeepaliveWhenIdle();
			},
		});

		return new Response(body, {
			headers: {
				"content-type": "text/event-stream; charset=utf-8",
				"cache-control": "no-store",
				connection: "keep-alive",
				"x-accel-buffering": "no",
			},
		});
	}

	broadcast(event: string, data: unknown): void {
		if (this.#clients.size === 0) return;
		const frame = this.#frame(event, data);
		for (const client of this.#clients) client.write(frame);
	}

	/** Push a freshly recomputed snapshot to everyone: the reconciliation backstop. */
	async reconcile(): Promise<void> {
		if (this.#clients.size === 0) return;
		this.broadcast("snapshot", await this.#options.snapshot());
	}

	close(): void {
		for (const client of this.#clients) client.close();
		this.#clients.clear();
		if (this.#keepalive) clearInterval(this.#keepalive);
		this.#keepalive = undefined;
	}

	#frame(event: string, data: unknown): string {
		this.#id += 1;
		return `id: ${this.#id}\nevent: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
	}

	#startKeepalive(): void {
		if (this.#keepalive) return;
		this.#keepalive = setInterval(() => {
			for (const client of this.#clients) client.write(": keepalive\n\n");
		}, KEEPALIVE_MS);
		this.#keepalive.unref?.();
	}

	#stopKeepaliveWhenIdle(): void {
		if (this.#clients.size > 0 || !this.#keepalive) return;
		clearInterval(this.#keepalive);
		this.#keepalive = undefined;
	}
}
