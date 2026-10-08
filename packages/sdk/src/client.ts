import type {
	ChatReactParams,
	ChatReactResult,
	ChatSendResult,
	GatewayStatusResult,
	OpsCycleResult,
	WorkRunParams,
	WorkRunResult,
} from "@gajae-gateway/protocol";
import {
	type ChatMessagePayload,
	type ChatProgressPayload,
	encodeFrame,
	type Frame,
	FrameDecoder,
	type FrameWriterSink,
	LOOPBACK_ORIGIN,
	OrderedFrameWriter,
	type OriginRef,
	PROFILE_VERSION,
	ProtocolError,
} from "@gajae-gateway/protocol";

type EventHandler = (payload: unknown, frame: Frame) => void;
/** Held pre-subscription events per event name; a delivery replay is bounded by the ledger anyway. */
const UNDELIVERED_EVENT_LIMIT = 1_000;

export interface StdioTransport {
	readable: ReadableStream<Uint8Array> | AsyncIterable<Uint8Array>;
	writable: WritableStream<Uint8Array> | { write(data: Uint8Array | string): unknown };
}

export interface GajaewayClientOptions {
	requestTimeoutMs?: number;
	/** Identifies this process in `gateway.status` generation diagnostics. */
	clientName?: string;
	/** Cancels an in-progress socket connection or negotiation. */
	signal?: AbortSignal;
}

/** Process start time, so the gateway can compare client and gateway generations. */
export function processStartedAt(): string {
	return new Date(Date.now() - process.uptime() * 1000).toISOString();
}

interface Transport {
	/** Encoding errors throw synchronously; transport failures reject asynchronously. */
	write(frame: Frame): Promise<void>;
	close(): void | Promise<void>;
}

interface Pending {
	resolve: (value: unknown) => void;
	reject: (error: Error) => void;
	timer: ReturnType<typeof setTimeout>;
}

export class GajaewayClient {
	static async connectSocket(path: string, options?: GajaewayClientOptions): Promise<GajaewayClient> {
		const client = new GajaewayClient(undefined, options);
		const decoder = new TextDecoder();
		const socketSink: FrameWriterSink = {
			write: (_bytes: Uint8Array) => 0,
			close: () => {},
		};
		let writer: OrderedFrameWriter;
		const signal = options?.signal;
		if (signal?.aborted) throw new Error("client connection aborted");
		let rejectAbort!: (reason: Error) => void;
		const aborted = signal
			? new Promise<never>((_resolve, reject) => {
					rejectAbort = reject;
				})
			: undefined;
		const onAbort = () => {
			const error = new Error("client connection aborted");
			client.#fail(error);
			rejectAbort(error);
		};
		signal?.addEventListener("abort", onAbort, { once: true });
		const connecting = Bun.connect<undefined>({
			unix: path,
			socket: {
				open(socket) {
					socketSink.write = (bytes: Uint8Array) => socket.write(bytes);
					socketSink.close = () => socket.end();
					writer = new OrderedFrameWriter(socketSink, (error) => {
						client.#fail(error as Error);
					});
				},
				data(_socket, data) {
					client.#receive(decoder.decode(data, { stream: true }));
				},
				close() {
					client.#fail(new Error("gateway connection closed"));
				},
				error(_socket, error) {
					client.#fail(error);
				},
				drain(_socket) {
					writer?.drain();
				},
			},
		});
		let socket: Awaited<typeof connecting>;
		try {
			socket = await (aborted ? Promise.race([connecting, aborted]) : connecting);
		} catch (error) {
			signal?.removeEventListener("abort", onAbort);
			void connecting.then(
				(lateSocket) => lateSocket.end(),
				() => {},
			);
			client.#fail(error instanceof Error ? error : new Error(String(error)));
			throw error;
		}
		client.#transport = {
			write: (frame) => {
				writer.write(frame);
				return writer.settled();
			},
			close: () => {
				writer?.close();
			},
		};
		if (client.#disconnectError) {
			client.#transport = undefined;
			socket.end();
			signal?.removeEventListener("abort", onAbort);
			throw client.#disconnectError;
		}
		try {
			await client.#negotiate();
			return client;
		} catch (error) {
			await client.close();
			throw error;
		} finally {
			signal?.removeEventListener("abort", onAbort);
		}
	}

	static async connectStdio(transport: StdioTransport, options?: GajaewayClientOptions): Promise<GajaewayClient> {
		const client = new GajaewayClient(undefined, options);
		client.#transport = {
			write: (frame) => {
				const data = encodeFrame(frame);
				return (async () => {
					const writable = transport.writable;
					if ("getWriter" in writable) {
						const writer = writable.getWriter();
						try {
							await writer.write(new TextEncoder().encode(data));
						} finally {
							writer.releaseLock();
						}
					} else await writable.write(data);
				})();
			},
			close: () => {
				if ("getWriter" in transport.writable) transport.writable.getWriter().releaseLock();
			},
		};
		void client.#readStdio(transport.readable);
		await client.#negotiate();
		return client;
	}

	#transport?: Transport;
	#decoder = new FrameDecoder();
	#pending = new Map<string, Pending>();
	#events = new Map<string, Set<EventHandler>>();
	#disconnectHandlers = new Set<(error: Error) => void>();
	#disconnectError?: Error;
	/**
	 * Events that arrived before anyone subscribed to them. The gateway writes
	 * `negotiated` and the pending-delivery replay back to back, and one socket
	 * read can carry both, so replayed `chat.message` frames are dispatched
	 * before `connectSocket` even resolves - before an adapter has had any
	 * chance to call `onChatMessage`. Dropping them would strand those ledger
	 * rows until the next reconnect. They are held per event and handed to the
	 * first subscriber, bounded so a client that never subscribes cannot grow
	 * without limit.
	 */
	readonly #undelivered = new Map<string, Array<{ payload: unknown; frame: Frame }>>();
	#negotiated?: Promise<void>;
	#requestTimeoutMs: number;
	#clientName: string;
	#id = 0;

	private constructor(transport?: Transport, options?: GajaewayClientOptions) {
		this.#transport = transport;
		this.#requestTimeoutMs = options?.requestTimeoutMs ?? 30_000;
		this.#clientName = options?.clientName ?? "@gajae-gateway/sdk";
	}

	on(event: string, handler: EventHandler): () => void {
		const handlers = this.#events.get(event) ?? new Set<EventHandler>();
		handlers.add(handler);
		this.#events.set(event, handlers);
		const held = this.#undelivered.get(event);
		if (held) {
			this.#undelivered.delete(event);
			for (const entry of held) handler(entry.payload, entry.frame);
		}
		return () => handlers.delete(handler);
	}

	/** Subscribe to the terminal transport disconnect; this is not a protocol event. */
	onDisconnect(handler: (error: Error) => void): () => void {
		if (this.#disconnectError) {
			handler(this.#disconnectError);
			return () => {};
		}
		this.#disconnectHandlers.add(handler);
		return () => this.#disconnectHandlers.delete(handler);
	}

	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void {
		return this.on("chat.message", (payload) => handler(payload as ChatMessagePayload));
	}

	onChatProgress(handler: (progress: ChatProgressPayload) => void): () => void {
		return this.on("chat.progress", (payload) => handler(payload as ChatProgressPayload));
	}

	async request<T = unknown>(verb: string, params?: unknown): Promise<T> {
		const transport = this.#transport;
		if (!transport) throw new Error("client is not connected");
		const id = `${++this.#id}`;
		const promise = new Promise<T>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.#pending.delete(id);
				reject(new ProtocolError("verb_failed", `request timed out after ${this.#requestTimeoutMs}ms`));
			}, this.#requestTimeoutMs);
			this.#pending.set(id, { resolve: resolve as (value: unknown) => void, reject, timer });
		});
		try {
			void transport.write({ v: PROFILE_VERSION, type: "request", id, verb, params }).catch((error: unknown) => {
				this.#fail(error instanceof Error ? error : new Error(String(error)));
			});
		} catch (error) {
			const pending = this.#pending.get(id);
			if (pending) {
				clearTimeout(pending.timer);
				this.#pending.delete(id);
				pending.reject(error instanceof Error ? error : new Error(String(error)));
			}
		}
		return promise;
	}

	status(): Promise<GatewayStatusResult> {
		return this.request("gateway.status");
	}
	shutdown(): Promise<{ readonly stopping: true }> {
		return this.request("gateway.shutdown");
	}
	chatSend(origin: OriginRef, text: string): Promise<ChatSendResult> {
		return this.request("chat.send", { origin, text });
	}
	/** React to one specific message; the target id is required by the verb. */
	chatReact(params: ChatReactParams): Promise<ChatReactResult> {
		return this.request("chat.react", params);
	}
	workRun(params: WorkRunParams): Promise<WorkRunResult> {
		return this.request("work.run", params);
	}
	opsCycle(): Promise<OpsCycleResult> {
		return this.request("ops.cycle");
	}

	async close(): Promise<void> {
		const transport = this.#fail(new Error("client closed"), false);
		await transport?.close();
	}

	async #negotiate(): Promise<void> {
		if (this.#negotiated) return this.#negotiated;
		if (this.#disconnectError) throw this.#disconnectError;
		this.#negotiated = new Promise<void>((resolve, reject) => {
			let settled = false;
			let offNegotiated = () => {};
			let offError = () => {};
			let offDisconnect = () => {};
			let timer: ReturnType<typeof setTimeout> | undefined;
			const finish = (error?: Error): void => {
				if (settled) return;
				settled = true;
				clearTimeout(timer);
				offNegotiated();
				offError();
				offDisconnect();
				if (error) reject(error);
				else resolve();
			};
			offNegotiated = this.on("__negotiated", () => finish());
			offError = this.on("__negotiation_error", (payload) => finish(payload as Error));
			offDisconnect = this.onDisconnect((error) => finish(error));
			const transport = this.#transport;
			if (!transport) {
				finish(new Error("client is not connected"));
				return;
			}
			timer = setTimeout(() => {
				this.#fail(new Error(`gateway negotiation timed out after ${this.#requestTimeoutMs}ms`));
			}, this.#requestTimeoutMs);
			void transport
				.write({
					v: PROFILE_VERSION,
					type: "hello",
					payload: {
						supportedVersions: [PROFILE_VERSION],
						clientInfo: { name: this.#clientName, startedAt: processStartedAt() },
					},
				})
				.catch((error) => this.#fail(error instanceof Error ? error : new Error(String(error))));
		});
		return this.#negotiated;
	}

	async #readStdio(readable: StdioTransport["readable"]): Promise<void> {
		const decoder = new TextDecoder();
		try {
			for await (const chunk of readable as AsyncIterable<Uint8Array>)
				this.#receive(decoder.decode(chunk, { stream: true }));
			this.#fail(new Error("gateway connection closed"));
		} catch (error) {
			this.#fail(error instanceof Error ? error : new Error(String(error)));
		}
	}

	#receive(chunk: string): void {
		if (this.#disconnectError) return;
		let frames: Frame[];
		try {
			frames = this.#decoder.feed(chunk);
		} catch (error) {
			this.#fail(error as Error);
			return;
		}
		for (const frame of frames) {
			if (this.#disconnectError) break;
			if (frame.type === "negotiated") {
				this.#emit("__negotiated", frame.payload, frame);
				continue;
			}
			if (frame.type === "error") {
				const error = new ProtocolError(frame.error.code, frame.error.message, frame.error.detail);
				if (frame.id) {
					const pending = this.#pending.get(frame.id);
					if (pending) {
						clearTimeout(pending.timer);
						this.#pending.delete(frame.id);
						pending.reject(error);
					}
				} else {
					if (error.code === "malformed_frame" || error.code === "payload_too_large") {
						this.#fail(error);
					} else {
						this.#emit("__negotiation_error", error, frame);
					}
				}
				continue;
			}
			if (frame.type === "response") {
				const pending = this.#pending.get(frame.id);
				if (pending) {
					clearTimeout(pending.timer);
					this.#pending.delete(frame.id);
					pending.resolve(frame.result);
				}
				continue;
			}
			if (frame.type === "event") {
				this.#emit(frame.event, frame.payload, frame);
			}
		}
	}

	#emit(event: string, payload: unknown, frame: Frame): void {
		const handlers = this.#events.get(event);
		if (!handlers || handlers.size === 0) {
			// Internal negotiation signals are awaited by a subscriber that is
			// installed before the hello is written; only protocol events are held.
			if (event.startsWith("__")) return;
			const held = this.#undelivered.get(event) ?? [];
			held.push({ payload, frame });
			if (held.length > UNDELIVERED_EVENT_LIMIT) held.shift();
			this.#undelivered.set(event, held);
			return;
		}
		for (const handler of handlers) handler(payload, frame);
	}
	#fail(error: Error, closeTransport = true): Transport | undefined {
		if (this.#disconnectError) return undefined;
		this.#disconnectError = error;
		const transport = this.#transport;
		this.#transport = undefined;
		for (const pending of this.#pending.values()) {
			clearTimeout(pending.timer);
			pending.reject(error);
		}
		this.#pending.clear();
		if (closeTransport && transport) {
			try {
				void Promise.resolve(transport.close()).catch(() => {});
			} catch {}
		}
		for (const handler of this.#disconnectHandlers) {
			try {
				handler(error);
			} catch (callbackError) {
				console.error("SDK disconnect subscriber failed", callbackError);
			}
		}
		this.#disconnectHandlers.clear();
		return transport;
	}
}

export { LOOPBACK_ORIGIN };
