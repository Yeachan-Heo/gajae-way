import { RpcClient, type JsonRpcClient, type JsonRpcResponse, type RpcRequestOptions } from "../rpc-client";

/** Bounded reconnect backoff so a restarting gateway is waited out, not hammered. */
const RECONNECT_BASE_DELAY_MS = 250;
const RECONNECT_MAX_DELAY_MS = 5_000;

function isSocketLoss(error: unknown): boolean {
	const message = error instanceof Error ? error.message : String(error);
	return (
		message.includes("RPC socket closed") ||
		message.includes("RPC socket is closed") ||
		message.includes("ended by the other party") ||
		message.includes("ECONNRESET") ||
		message.includes("ECONNREFUSED") ||
		message.includes("EPIPE") ||
		message.includes("ENOENT")
	);
}

/**
 * A JSON-RPC client that re-dials the gateway socket when the connection is
 * lost.
 *
 * The plain client deliberately owns no reconnect policy, which left the adapter
 * permanently useless after any gateway restart: every later request threw
 * "socket closed", the outbox loop logged that forever, and the chat surface went
 * silent until an operator noticed and restarted the process by hand.
 *
 * Reconnecting is safe for delivery because the journal consumer commits only
 * after a confirmed send, and replayed sends reuse deterministic nonces plus the
 * durable per-chunk ledger, so a retried event cannot double-post. A request that
 * was in flight when the socket died is reported to the caller rather than
 * silently retried, because the adapter cannot know whether it was applied.
 */
export class ReconnectingRpcClient implements JsonRpcClient {
	readonly #socketPath: string;
	readonly #connect: (socketPath: string) => Promise<JsonRpcClient>;
	readonly #onDiagnostic: ((message: string) => void) | undefined;
	#client: JsonRpcClient | undefined;
	#closed = false;
	#attempts = 0;

	private constructor(
		socketPath: string,
		client: JsonRpcClient,
		connect: (socketPath: string) => Promise<JsonRpcClient>,
		onDiagnostic?: (message: string) => void,
	) {
		this.#socketPath = socketPath;
		this.#client = client;
		this.#connect = connect;
		this.#onDiagnostic = onDiagnostic;
	}

	static async connect(
		socketPath: string,
		options: { connect?: (socketPath: string) => Promise<JsonRpcClient>; onDiagnostic?: (message: string) => void } = {},
	): Promise<ReconnectingRpcClient> {
		const connect = options.connect ?? (path => RpcClient.connect(path) as Promise<JsonRpcClient>);
		const client = await connect(socketPath);
		return new ReconnectingRpcClient(socketPath, client, connect, options.onDiagnostic);
	}

	async request(method: string, params: unknown = {}, options: RpcRequestOptions = {}): Promise<JsonRpcResponse> {
		if (this.#closed) throw new Error("RPC socket is closed");
		const client = this.#client ?? (await this.reconnect());
		try {
			const response = await client.request(method, params, options);
			this.#attempts = 0;
			return response;
		} catch (error) {
			if (this.#closed || !isSocketLoss(error)) throw error;
			// Drop the dead client so the NEXT request re-dials. This request is not
			// retried: its outcome is unknown, and the caller owns idempotency.
			this.#client = undefined;
			try {
				client.close();
			} catch {
				// Closing a dead socket is best-effort.
			}
			this.diagnostic(`gateway rpc connection lost during ${method}; will reconnect`);
			throw error;
		}
	}

	close(): void {
		if (this.#closed) return;
		this.#closed = true;
		try {
			this.#client?.close();
		} catch {
			// Best-effort teardown.
		}
		this.#client = undefined;
	}

	private async reconnect(): Promise<JsonRpcClient> {
		for (;;) {
			if (this.#closed) throw new Error("RPC socket is closed");
			this.#attempts += 1;
			const delay = Math.min(RECONNECT_BASE_DELAY_MS * 2 ** Math.min(this.#attempts - 1, 5), RECONNECT_MAX_DELAY_MS);
			await new Promise(resolve => setTimeout(resolve, delay));
			if (this.#closed) throw new Error("RPC socket is closed");
			try {
				const client = await this.#connect(this.#socketPath);
				this.#client = client;
				this.#attempts = 0;
				this.diagnostic("gateway rpc connection re-established");
				return client;
			} catch (error) {
				this.diagnostic(`gateway rpc reconnect failed: ${error instanceof Error ? error.message : String(error)}`);
			}
		}
	}

	private diagnostic(message: string): void {
		try {
			this.#onDiagnostic?.(message);
		} catch {
			// Diagnostics must never break delivery.
		}
	}
}
