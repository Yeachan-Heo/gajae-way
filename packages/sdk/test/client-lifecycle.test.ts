import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PROFILE_VERSION } from "@gajae-gateway/protocol";
import { GajaewayClient } from "../src/client";

type Peer = { end(): void; write(data: string): unknown };

async function withSocket(
	configure: (socket: Peer, frame: { type: string; id?: string; verb?: string }) => void,
	body: (path: string, peers: Set<Peer>) => Promise<void>,
): Promise<void> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-sdk-lifecycle-"));
	const path = join(directory, "gateway.sock");
	const peers = new Set<Peer>();
	const listener = Bun.listen<{ buffer: string }>({
		unix: path,
		socket: {
			open(socket) {
				socket.data = { buffer: "" };
				peers.add(socket);
			},
			data(socket, data) {
				socket.data.buffer += Buffer.from(data).toString("utf8");
				let newline = socket.data.buffer.indexOf("\n");
				while (newline >= 0) {
					const line = socket.data.buffer.slice(0, newline);
					socket.data.buffer = socket.data.buffer.slice(newline + 1);
					configure(socket, JSON.parse(line));
					newline = socket.data.buffer.indexOf("\n");
				}
			},
			close(socket) {
				peers.delete(socket);
			},
		},
	});
	try {
		await body(path, peers);
	} finally {
		listener.stop(true);
		await rm(directory, { recursive: true, force: true });
	}
}

function negotiated(): string {
	return `${JSON.stringify({
		v: PROFILE_VERSION,
		type: "negotiated",
		payload: { profileVersion: PROFILE_VERSION, capabilities: [] },
	})}\n`;
}

test("invalid request params do not leak pending work or disconnect the client", async () => {
	let held: { socket: Peer; id: string } | undefined;
	let requests = 0;
	await withSocket(
		(socket, frame) => {
			if (frame.type === "hello") socket.write(negotiated());
			if (frame.type !== "request") return;
			if (!frame.id) throw new Error("request missing id");
			requests++;
			if (frame.verb === "held") held = { socket, id: frame.id };
			else socket.write(`${JSON.stringify({ v: PROFILE_VERSION, type: "response", id: frame.id, result: "ok" })}\n`);
		},
		async (path) => {
			const client = await GajaewayClient.connectSocket(path, { requestTimeoutMs: 500 });
			let disconnects = 0;
			client.onDisconnect(() => disconnects++);
			const pending = client.request("held");
			const outcome = pending.catch((error: Error) => error);
			await until(() => held !== undefined);
			const circular: { self?: unknown } = {};
			circular.self = circular;
			for (const params of [circular, { value: 1n }]) {
				await expect(client.request("invalid", params)).rejects.toThrow();
				expect(disconnects).toBe(0);
			}
			expect(await client.request<string>("valid")).toBe("ok");
			if (!held) throw new Error("held request missing");
			held.socket.write(
				`${JSON.stringify({ v: PROFILE_VERSION, type: "response", id: held.id, result: "held ok" })}\n`,
			);
			expect(await outcome).toBe("held ok");
			expect(requests).toBe(2);
			expect(disconnects).toBe(0);
			await client.close();
		},
	);
});

test("stdio serialization errors reject only the invalid request", async () => {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	const client = await GajaewayClient.connectStdio({
		readable: new ReadableStream<Uint8Array>({
			start(value) {
				controller = value;
			},
		}),
		writable: {
			write(data) {
				const frame = JSON.parse(String(data));
				controller.enqueue(
					new TextEncoder().encode(
						frame.type === "hello"
							? negotiated()
							: `${JSON.stringify({ v: PROFILE_VERSION, type: "response", id: frame.id, result: "ok" })}\n`,
					),
				);
			},
		},
	});
	let disconnects = 0;
	client.onDisconnect(() => disconnects++);
	try {
		const circular: { self?: unknown } = {};
		circular.self = circular;
		for (const params of [circular, { value: 1n }]) {
			await expect(client.request("invalid", params)).rejects.toThrow();
			expect(disconnects).toBe(0);
		}
		expect(await client.request("valid")).toBe("ok");
	} finally {
		controller.close();
		await client.close();
	}
});

test("throwing disconnect subscribers cannot prevent later notifications or explicit socket close", async () => {
	await withSocket(
		(socket, frame) => {
			if (frame.type === "hello") socket.write(negotiated());
		},
		async (path, peers) => {
			const client = await GajaewayClient.connectSocket(path);
			const failure = new Error("subscriber failed");
			const diagnostic = spyOn(console, "error").mockImplementation(() => {});
			let notifications = 0;
			client.onDisconnect(() => {
				throw failure;
			});
			client.onDisconnect(() => notifications++);
			try {
				await client.close();
				await until(() => peers.size === 0);
				await client.close();
				expect(notifications).toBe(1);
				expect(diagnostic).toHaveBeenCalledWith("SDK disconnect subscriber failed", failure);
			} finally {
				diagnostic.mockRestore();
				await client.close();
			}
		},
	);
});

test("a transport write failure is terminal and rejects later work", async () => {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	let writes = 0;
	const client = await GajaewayClient.connectStdio({
		readable: new ReadableStream<Uint8Array>({
			start(value) {
				controller = value;
			},
		}),
		writable: {
			write() {
				if (++writes === 1) controller.enqueue(new TextEncoder().encode(negotiated()));
				else throw new Error("write failed");
			},
		},
	});
	let disconnects = 0;
	client.onDisconnect(() => disconnects++);
	await expect(client.request("valid")).rejects.toThrow("write failed");
	expect(disconnects).toBe(1);
	await expect(client.request("after failure")).rejects.toThrow("client is not connected");
	expect(writes).toBe(2);
	controller.close();
	await client.close();
});

test("stdio EOF rejects the caller even while its request write is unresolved", async () => {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	let releaseWrite!: () => void;
	let writes = 0;
	const client = await GajaewayClient.connectStdio({
		readable: new ReadableStream<Uint8Array>({
			start(value) {
				controller = value;
			},
		}),
		writable: {
			write() {
				if (++writes === 1) {
					controller.enqueue(new TextEncoder().encode(negotiated()));
					return;
				}
				return new Promise<void>((resolve) => {
					releaseWrite = resolve;
				});
			},
		},
	});
	const pending = client.request("held").catch((error: Error) => error);
	try {
		controller.close();
		const outcome = await Promise.race([pending, Bun.sleep(100).then(() => "write still blocked")]);
		expect(outcome).toBeInstanceOf(Error);
		expect((outcome as Error).message).toBe("gateway connection closed");
	} finally {
		releaseWrite();
		await pending;
		await client.close();
	}
});

test("stdio EOF rejects pending work once and leaves the client terminal", async () => {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	let writes = 0;
	const client = await GajaewayClient.connectStdio({
		readable: new ReadableStream<Uint8Array>({
			start(value) {
				controller = value;
			},
		}),
		writable: {
			write() {
				writes++;
				if (writes === 1) controller.enqueue(new TextEncoder().encode(negotiated()));
			},
		},
	});
	let disconnects = 0;
	const disconnected = new Promise<Error>((resolve) => {
		client.onDisconnect((error) => {
			disconnects++;
			resolve(error);
		});
	});
	const outcome = client.request("held").catch((error: Error) => error);
	controller.close();
	const error = await outcome;
	if (!(error instanceof Error)) throw new Error("pending request unexpectedly resolved");
	expect(error.message).toBe("gateway connection closed");
	expect(await disconnected).toBe(error);
	expect(disconnects).toBe(1);
	await expect(client.request("after EOF")).rejects.toThrow("client is not connected");
	expect(writes).toBe(2);
	await client.close();
});

test("a stdio read error rejects pending work with the original error", async () => {
	let controller!: ReadableStreamDefaultController<Uint8Array>;
	let writes = 0;
	const client = await GajaewayClient.connectStdio({
		readable: new ReadableStream<Uint8Array>({
			start(value) {
				controller = value;
			},
		}),
		writable: {
			write() {
				writes++;
				if (writes === 1) controller.enqueue(new TextEncoder().encode(negotiated()));
			},
		},
	});
	const failure = new Error("stdio read failed");
	let disconnects = 0;
	const disconnected = new Promise<Error>((resolve) => {
		client.onDisconnect((error) => {
			disconnects++;
			resolve(error);
		});
	});
	const outcome = client.request("held").catch((error: Error) => error);
	controller.error(failure);
	expect(await outcome).toBe(failure);
	expect(await disconnected).toBe(failure);
	expect(disconnects).toBe(1);
	await expect(client.request("after read error")).rejects.toThrow("client is not connected");
	expect(writes).toBe(2);
	await client.close();
});

async function until(predicate: () => boolean): Promise<void> {
	const deadline = Date.now() + 1_000;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error("timed out waiting for socket fixture");
		await Bun.sleep(5);
	}
}

test("a socket disconnect rejects pending work, invalidates transport, and notifies once", async () => {
	let requests = 0;
	await withSocket(
		(socket, frame) => {
			if (frame.type === "hello") socket.write(negotiated());
			if (frame.type === "request") requests++;
		},
		async (path, peers) => {
			const client = await GajaewayClient.connectSocket(path, { requestTimeoutMs: 500 });
			let disconnects = 0;
			const disconnected = new Promise<Error>((resolve) => {
				client.onDisconnect((error) => {
					disconnects++;
					resolve(error);
				});
			});
			const pending = client.request("chat.send", { text: "must not replay" });
			await until(() => requests === 1);
			for (const peer of peers) peer.end();
			expect(await pending.catch((error: Error) => error.message)).toBe("gateway connection closed");
			expect((await disconnected).message).toBe("gateway connection closed");
			expect(disconnects).toBe(1);
			await expect(client.request("chat.send", { text: "after disconnect" })).rejects.toThrow(
				"client is not connected",
			);
			expect(requests).toBe(1);
			await client.close();
		},
	);
});

test("socket close during negotiation rejects and releases the connection", async () => {
	await withSocket(
		(socket, frame) => {
			if (frame.type === "hello") socket.end();
		},
		async (path, peers) => {
			await expect(GajaewayClient.connectSocket(path)).rejects.toThrow("gateway connection closed");
			await until(() => peers.size === 0);
		},
	);
});

test("a silent peer cannot wedge negotiation indefinitely", async () => {
	await withSocket(
		() => {},
		async (path, peers) => {
			await expect(GajaewayClient.connectSocket(path, { requestTimeoutMs: 30 })).rejects.toThrow(
				"gateway negotiation timed out after 30ms",
			);
			await until(() => peers.size === 0);
		},
	);
});

test("an abort during negotiation rejects promptly and releases the connection", async () => {
	let sawHello!: () => void;
	const hello = new Promise<void>((resolve) => {
		sawHello = resolve;
	});
	await withSocket(
		(_socket, frame) => {
			if (frame.type === "hello") sawHello();
		},
		async (path, peers) => {
			const controller = new AbortController();
			const connecting = GajaewayClient.connectSocket(path, { signal: controller.signal });
			await hello;
			controller.abort();
			await expect(connecting).rejects.toThrow("client connection aborted");
			await until(() => peers.size === 0);
		},
	);
});
