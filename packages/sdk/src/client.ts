import type {
	ChatReactParams,
	ChatReactResult,
	ChatSendResult,
	GatewayStatusResult,
	OpsCycleResult,
	WorkRunParams,
	WorkRunResult,
} from "@gajaeway/protocol";
import {
	type ChatMessagePayload,
	type ChatProgressPayload,
	encodeFrame,
	type Frame,
	FrameDecoder,
	LOOPBACK_ORIGIN,
	type OriginRef,
	PROFILE_VERSION,
	ProtocolError,
} from "@gajaeway/protocol";

type EventHandler = (payload: unknown, frame: Frame) => void;

export interface StdioTransport {
	readable: ReadableStream<Uint8Array> | AsyncIterable<Uint8Array>;
	writable: WritableStream<Uint8Array> | { write(data: Uint8Array | string): unknown };
}

export interface GajaewayClientOptions {
	requestTimeoutMs?: number;
}

export interface RequestOptions {
	timeoutMs?: number;
}

interface Transport {
	write(data: string): Promise<void>;
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
		const socket = await Bun.connect<undefined>({
			unix: path,
			socket: {
				open() {},
				data(_socket, data) {
					client.#receive(new TextDecoder().decode(data));
				},
				close() {
					client.#fail(new Error("gateway connection closed"));
				},
				error(_socket, error) {
					client.#fail(error);
				},
			},
		});
		client.#transport = {
			write: async (data) => {
				socket.write(data);
			},
			close: () => {
				socket.end();
			},
		};
		await client.#negotiate();
		return client;
	}

	static async connectStdio(transport: StdioTransport, options?: GajaewayClientOptions): Promise<GajaewayClient> {
		const client = new GajaewayClient(undefined, options);
		client.#transport = {
			write: async (data) => {
				const writable = transport.writable;
				if ("getWriter" in writable) {
					const writer = writable.getWriter();
					try {
						await writer.write(new TextEncoder().encode(data));
					} finally {
						writer.releaseLock();
					}
				} else await writable.write(data);
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
	#negotiated?: Promise<void>;
	#requestTimeoutMs: number;
	#id = 0;

	private constructor(transport?: Transport, options?: GajaewayClientOptions) {
		this.#transport = transport;
		this.#requestTimeoutMs = options?.requestTimeoutMs ?? 30_000;
	}

	on(event: string, handler: EventHandler): () => void {
		const handlers = this.#events.get(event) ?? new Set<EventHandler>();
		handlers.add(handler);
		this.#events.set(event, handlers);
		return () => handlers.delete(handler);
	}

	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void {
		return this.on("chat.message", (payload) => handler(payload as ChatMessagePayload));
	}

	onChatProgress(handler: (progress: ChatProgressPayload) => void): () => void {
		return this.on("chat.progress", (payload) => handler(payload as ChatProgressPayload));
	}

	async request<T = unknown>(verb: string, params?: unknown, options?: RequestOptions): Promise<T> {
		if (!this.#transport) throw new Error("client is not connected");
		const id = `${++this.#id}`;
		const timeoutMs = options?.timeoutMs ?? this.#requestTimeoutMs;
		if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error("request timeout must be a positive number");
		const promise = new Promise<T>((resolve, reject) => {
			const timer = setTimeout(() => {
				this.#pending.delete(id);
				reject(
					new ProtocolError("verb_failed", `${verb} timed out after ${timeoutMs}ms; server completion is unknown`),
				);
			}, timeoutMs);
			this.#pending.set(id, { resolve: resolve as (value: unknown) => void, reject, timer });
		});
		await this.#transport.write(encodeFrame({ v: PROFILE_VERSION, type: "request", id, verb, params }));
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
		await this.#transport?.close();
		this.#fail(new Error("client closed"));
	}

	async #negotiate(): Promise<void> {
		if (this.#negotiated) return this.#negotiated;
		this.#negotiated = new Promise<void>((resolve, reject) => {
			const off = this.on("__negotiated", (_payload) => {
				off();
				resolve();
			});
			const offErr = this.on("__negotiation_error", (payload) => {
				offErr();
				reject(payload as Error);
			});
			void this.#transport
				?.write(
					encodeFrame({
						v: PROFILE_VERSION,
						type: "hello",
						payload: {
							supportedVersions: [PROFILE_VERSION],
							clientInfo: { name: "@gajaeway/sdk" },
						},
					}),
				)
				.catch(reject);
		});
		return this.#negotiated;
	}

	async #readStdio(readable: StdioTransport["readable"]): Promise<void> {
		for await (const chunk of readable as AsyncIterable<Uint8Array>) this.#receive(new TextDecoder().decode(chunk));
	}

	#receive(chunk: string): void {
		let frames: Frame[];
		try {
			frames = this.#decoder.feed(chunk);
		} catch (error) {
			this.#fail(error as Error);
			return;
		}
		for (const frame of frames) {
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
				} else this.#emit("__negotiation_error", error, frame);
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
		for (const handler of this.#events.get(event) ?? []) handler(payload, frame);
	}
	#fail(error: Error): void {
		for (const pending of this.#pending.values()) {
			clearTimeout(pending.timer);
			pending.reject(error);
		}
		this.#pending.clear();
	}
}

export { LOOPBACK_ORIGIN };
