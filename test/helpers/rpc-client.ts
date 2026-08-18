import { createConnection, type Socket } from "node:net";

export type JsonRpcResponse = {
	jsonrpc: "2.0";
	id: string | number | null;
	result?: unknown;
	error?: { code: number; message: string; data?: unknown };
};

/** Minimal NDJSON-over-UDS client for integration tests. */
export class RpcClient {
	readonly socket: Socket;
	private buffer = "";
	private readonly pending = new Map<string, { resolve: (value: JsonRpcResponse) => void; reject: (error: Error) => void }>();
	private nextId = 1;

	private constructor(socket: Socket) {
		this.socket = socket;
		socket.setEncoding("utf8");
		socket.on("data", chunk => this.onData(chunk));
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
			socket.on("error", onError);
			socket.once("connect", onConnect);
		});
	}

	request(method: string, params: unknown = {}): Promise<JsonRpcResponse> {
		const id = this.nextId++;
		const key = JSON.stringify(id);
		const request = JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n";
		return new Promise((resolve, reject) => {
			this.pending.set(key, { resolve, reject });
			this.socket.write(request, error => {
				if (!error) return;
				this.pending.delete(key);
				reject(error);
			});
		});
	}

	notify(method: string, params: unknown = {}): void {
		this.socket.write(JSON.stringify({ jsonrpc: "2.0", method, params }) + "\n");
	}

	close(): void {
		this.socket.end();
	}

	private onData(chunk: string): void {
		this.buffer += chunk;
		for (;;) {
			const newline = this.buffer.indexOf("\n");
			if (newline < 0) return;
			const line = this.buffer.slice(0, newline);
			this.buffer = this.buffer.slice(newline + 1);
			if (!line.trim()) continue;
			const response = JSON.parse(line) as JsonRpcResponse;
			const pending = this.pending.get(JSON.stringify(response.id));
			if (!pending) continue;
			this.pending.delete(JSON.stringify(response.id));
			pending.resolve(response);
		}
	}

	private rejectAll(error: Error): void {
		for (const pending of this.pending.values()) pending.reject(error);
		this.pending.clear();
	}
}
