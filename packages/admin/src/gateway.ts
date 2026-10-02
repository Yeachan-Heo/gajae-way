import { GajaewayClient } from "@gajae-gateway/sdk";

type GatewayEventHandler = (payload: unknown) => void;

const INITIAL_RETRY_DELAY_MS = 500;
const MAX_RETRY_DELAY_MS = 30_000;

export type AdminGatewayRetryOptions = {
	readonly random?: () => number;
	readonly setTimeout?: typeof setTimeout;
	readonly clearTimeout?: typeof clearTimeout;
};

export class AdminGateway {
	static async connect(socketPath: string, retryOptions: AdminGatewayRetryOptions = {}): Promise<AdminGateway> {
		const gateway = new AdminGateway(socketPath, retryOptions);
		const client = await GajaewayClient.connectSocket(socketPath);
		gateway.#attach(client);
		return gateway;
	}

	#client?: GajaewayClient;
	#closed = false;
	#connecting?: Promise<void>;
	#connectionAbort?: AbortController;
	#retryTimer?: ReturnType<typeof setTimeout>;
	#retryAttempt = 0;
	#reconnectNeeded = false;
	#disconnectOff?: () => void;
	#connectionHandlers = new Set<(connected: boolean) => void>();
	#handlers = new Map<string, Set<GatewayEventHandler>>();
	#eventOffs = new Map<string, Map<GatewayEventHandler, () => void>>();
	readonly #socketPath: string;
	readonly #random: () => number;
	readonly #setTimeout: typeof setTimeout;
	readonly #clearTimeout: typeof clearTimeout;

	private constructor(socketPath: string, retryOptions: AdminGatewayRetryOptions) {
		this.#socketPath = socketPath;
		this.#random = retryOptions.random ?? Math.random;
		this.#setTimeout = retryOptions.setTimeout ?? setTimeout;
		this.#clearTimeout = retryOptions.clearTimeout ?? clearTimeout;
	}

	get connected(): boolean {
		return this.#client !== undefined;
	}

	onConnectionChange(handler: (connected: boolean) => void): () => void {
		handler(this.connected);
		if (this.#closed) return () => {};
		this.#connectionHandlers.add(handler);
		return () => this.#connectionHandlers.delete(handler);
	}

	async request<T = unknown>(verb: string, params?: unknown): Promise<T> {
		const client = this.#client;
		if (!client) throw new Error("gateway is not connected");
		return client.request<T>(verb, params);
	}

	on(event: string, handler: GatewayEventHandler): () => void {
		const handlers = this.#handlers.get(event) ?? new Set<GatewayEventHandler>();
		handlers.add(handler);
		this.#handlers.set(event, handlers);
		if (this.#client) this.#subscribe(this.#client, event, handler);
		return () => {
			handlers.delete(handler);
			const eventOffs = this.#eventOffs.get(event);
			eventOffs?.get(handler)?.();
			eventOffs?.delete(handler);
			if (eventOffs?.size === 0) this.#eventOffs.delete(event);
			if (handlers.size === 0) this.#handlers.delete(event);
		};
	}

	async close(): Promise<void> {
		if (this.#closed) return;
		this.#closed = true;
		if (this.#retryTimer !== undefined) this.#clearTimeout(this.#retryTimer);
		this.#retryTimer = undefined;
		this.#connectionAbort?.abort();
		const connecting = this.#connecting;
		const client = this.#client;
		this.#client = undefined;
		this.#disconnectOff?.();
		this.#disconnectOff = undefined;
		this.#detachEvents();
		if (client) this.#notifyConnection();
		this.#connectionHandlers.clear();
		await Promise.all([client?.close(), connecting]);
	}

	#attach(client: GajaewayClient): void {
		if (this.#closed) {
			void client.close().catch(() => {});
			return;
		}
		const recovered = this.#reconnectNeeded;
		this.#client = client;
		this.#disconnectOff = client.onDisconnect(() => this.#disconnected(client));
		if (this.#client !== client) return;
		for (const [event, handlers] of this.#handlers)
			for (const handler of handlers) this.#subscribe(client, event, handler);
		this.#retryAttempt = 0;
		this.#reconnectNeeded = false;
		if (recovered) console.error("Admin gateway reconnected.");
		this.#notifyConnection();
	}

	#notifyConnection(): void {
		for (const handler of this.#connectionHandlers) handler(this.connected);
	}

	#subscribe(client: GajaewayClient, event: string, handler: GatewayEventHandler): void {
		const offs = this.#eventOffs.get(event) ?? new Map<GatewayEventHandler, () => void>();
		if (offs.has(handler)) return;
		offs.set(
			handler,
			client.on(event, (payload) => handler(payload)),
		);
		this.#eventOffs.set(event, offs);
	}

	#detachEvents(): void {
		for (const handlers of this.#eventOffs.values()) for (const off of handlers.values()) off();
		this.#eventOffs.clear();
	}

	#disconnected(client: GajaewayClient): void {
		if (this.#client !== client) return;
		this.#client = undefined;
		this.#disconnectOff?.();
		this.#disconnectOff = undefined;
		this.#detachEvents();
		this.#reconnectNeeded = true;
		console.error("Admin gateway disconnected; reconnecting.");
		this.#scheduleRetry();
		this.#notifyConnection();
	}

	#scheduleRetry(): void {
		if (this.#closed || this.#client || this.#connecting || this.#retryTimer !== undefined) return;
		const delay = Math.min(MAX_RETRY_DELAY_MS, INITIAL_RETRY_DELAY_MS * 2 ** Math.min(this.#retryAttempt++, 6));
		const jitter = Math.floor(this.#random() * Math.max(1, delay / 4));
		this.#retryTimer = this.#setTimeout(
			() => {
				this.#retryTimer = undefined;
				this.#reconnect();
			},
			Math.min(MAX_RETRY_DELAY_MS, delay + jitter),
		);
		this.#retryTimer.unref?.();
	}

	#reconnect(): void {
		if (this.#closed || this.#client || this.#connecting) return;
		const controller = new AbortController();
		this.#connectionAbort = controller;
		this.#connecting = GajaewayClient.connectSocket(this.#socketPath, { signal: controller.signal })
			.then(async (client) => {
				if (this.#closed || controller.signal.aborted) {
					await client.close();
					return;
				}
				this.#attach(client);
			})
			.catch(() => {})
			.finally(() => {
				if (this.#connectionAbort === controller) this.#connectionAbort = undefined;
				this.#connecting = undefined;
				if (!this.#client && !this.#closed) this.#scheduleRetry();
			});
	}
}
