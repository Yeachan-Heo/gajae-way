import { createConnection, type Socket } from "node:net";

export type JsonRpcError = {
	readonly code: number;
	readonly message: string;
	readonly data?: unknown;
};

export type JsonRpcResponse = {
	readonly jsonrpc: "2.0";
	readonly id: string | number | null;
	readonly result?: unknown;
	readonly error?: JsonRpcError;
};

export interface RpcRequestOptions {
	readonly signal?: AbortSignal;
	readonly timeoutMs?: number;
}

export interface JsonRpcClient {
	request(method: string, params?: unknown, options?: RpcRequestOptions): Promise<JsonRpcResponse>;
	close(): void;
}

type PendingRequest = {
	readonly resolve: (value: JsonRpcResponse) => void;
	readonly reject: (error: Error) => void;
	readonly cleanup: () => void;
};

/**
 * Small NDJSON-over-UDS JSON-RPC client shared by adapters and integration
 * tests. It deliberately owns no retry or durable state; callers choose their
 * own idempotency and reconnect policy.
 */
export class RpcClient implements JsonRpcClient {
	readonly socket: Socket;
	#buffer = "";
	readonly #pending = new Map<string, PendingRequest>();
	#nextId = 1;
	#closed = false;

	private constructor(socket: Socket) {
		this.socket = socket;
		socket.setEncoding("utf8");
		socket.on("data", chunk => this.onData(String(chunk)));
		socket.on("error", error => this.rejectAll(error));
		socket.on("close", () => this.rejectAll(new Error("RPC socket closed")));
	}

	static connect(socketPath: string): Promise<RpcClient> {
		return new Promise((resolve, reject) => {
			const socket = createConnection(socketPath);
			let settled = false;
			const onError = (error: Error) => {
				if (settled) return;
				settled = true;
				socket.off("connect", onConnect);
				reject(error);
			};
			const onConnect = () => {
				if (settled) return;
				settled = true;
				socket.off("error", onError);
				resolve(new RpcClient(socket));
			};
			socket.once("error", onError);
			socket.once("connect", onConnect);
		});
	}

	request(method: string, params: unknown = {}, options: RpcRequestOptions = {}): Promise<JsonRpcResponse> {
		if (this.#closed) return Promise.reject(new Error("RPC socket is closed"));
		if (!method) return Promise.reject(new Error("RPC method must not be empty"));
		if (options.signal?.aborted) return Promise.reject(abortError());
		const id = this.#nextId++;
		const key = JSON.stringify(id);
		const request = `${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`;
		return new Promise((resolve, reject) => {
			let timeout: ReturnType<typeof setTimeout> | undefined;
			const onAbort = () => finish(() => reject(abortError()));
			const cleanup = () => {
				if (timeout) clearTimeout(timeout);
				options.signal?.removeEventListener("abort", onAbort);
			};
			const finish = (callback: () => void) => {
				const pending = this.#pending.get(key);
				if (!pending) return;
				this.#pending.delete(key);
				pending.cleanup();
				callback();
			};
			if (options.timeoutMs !== undefined) {
				if (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs <= 0) {
					reject(new Error("RPC timeoutMs must be a positive safe integer"));
					return;
				}
				timeout = setTimeout(() => finish(() => reject(new Error(`RPC request timed out: ${method}`))), options.timeoutMs);
			}
			options.signal?.addEventListener("abort", onAbort, { once: true });
			this.#pending.set(key, { resolve, reject, cleanup });
			this.socket.write(request, error => {
				if (!error) return;
				finish(() => reject(error));
			});
		});
	}

	notify(method: string, params: unknown = {}): void {
		if (this.#closed) throw new Error("RPC socket is closed");
		this.socket.write(`${JSON.stringify({ jsonrpc: "2.0", method, params })}\n`);
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		this.socket.end();
	}

	private onData(chunk: string): void {
		this.#buffer += chunk;
		for (;;) {
			const newline = this.#buffer.indexOf("\n");
			if (newline < 0) return;
			const line = this.#buffer.slice(0, newline);
			this.#buffer = this.#buffer.slice(newline + 1);
			if (!line.trim()) continue;
			let response: JsonRpcResponse;
			try {
				response = JSON.parse(line) as JsonRpcResponse;
			} catch {
				this.rejectAll(new Error("RPC server returned invalid JSON"));
				this.close();
				return;
			}
			const key = JSON.stringify(response.id);
			const pending = this.#pending.get(key);
			if (!pending) continue;
			this.#pending.delete(key);
			pending.cleanup();
			pending.resolve(response);
		}
	}

	private rejectAll(error: Error): void {
		for (const [key, pending] of this.#pending) {
			this.#pending.delete(key);
			pending.cleanup();
			pending.reject(error);
		}
	}
}

export function rpcResult<T>(response: JsonRpcResponse, method: string): T {
	if (response.error) throw new RpcResponseError(method, response.error);
	if (!("result" in response)) throw new Error(`RPC ${method} returned neither result nor error`);
	return response.result as T;
}

export class RpcResponseError extends Error {
	readonly code: number;
	readonly data?: unknown;

	constructor(method: string, error: JsonRpcError) {
		super(`RPC ${method} failed: ${error.code} ${error.message}`);
		this.name = "RpcResponseError";
		this.code = error.code;
		this.data = error.data;
	}
}

function abortError(): Error {
	const error = new Error("RPC request aborted");
	error.name = "AbortError";
	return error;
}
