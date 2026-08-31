import { describe, expect, test } from "bun:test";
import { encodeFrame, PROFILE_VERSION } from "@gajaeway/protocol";
import { GajaewayClient } from "../src/client";

/**
 * `onTransportTerminal` contract (plan step 8a).
 *
 * The gateway releases the terminal gjc lease the instant a connection dies. If
 * the CLI cannot observe that, the released lease plus a still-running native
 * TUI is a concurrent-resume hazard. `on()` only sees protocol event frames and
 * the failure path only rejects PENDING requests, so this callback is the only
 * signal available to a consumer that is just waiting on a spawned child.
 *
 * A fake stdio transport is used so the three abrupt paths can be driven
 * deterministically without a real socket.
 */

interface FakeTransport {
	readonly client: GajaewayClient;
	/** Push a raw wire chunk at the client (drives the decoder). */
	feed(chunk: string): void;
	/** Simulate abrupt transport death the way socket close/error does. */
	killTransport(error: Error): void;
	readonly writes: string[];
	closeCalls: number;
}

/**
 * Build a connected client over a controllable stdio transport.
 *
 * `connectStdio` negotiates, so the readable must emit a `negotiated` frame
 * before the returned promise resolves.
 */
async function connectFake(): Promise<FakeTransport> {
	const writes: string[] = [];
	let closeCalls = 0;
	let pushChunk: ((chunk: string) => void) | undefined;

	const readable = (async function* () {
		const queue: string[] = [];
		let resolveNext: (() => void) | undefined;
		const ended = false;
		pushChunk = (chunk: string) => {
			queue.push(chunk);
			resolveNext?.();
			resolveNext = undefined;
		};
		// The generator ends only when the client is torn down; these tests drive
		// termination through the failure paths, not by ending the stream.
		while (!ended || queue.length > 0) {
			if (queue.length === 0) {
				await new Promise<void>((resolve) => {
					resolveNext = resolve;
				});
				continue;
			}
			const next = queue.shift();
			if (next !== undefined) yield new TextEncoder().encode(next);
		}
	})();

	const writable = {
		getWriter() {
			return {
				write(bytes: Uint8Array) {
					writes.push(new TextDecoder().decode(bytes));
					return Promise.resolve();
				},
				releaseLock() {},
			};
		},
	};

	const connecting = GajaewayClient.connectStdio({
		readable: readable as never,
		writable: writable as never,
		close: () => {
			closeCalls += 1;
		},
	} as never);

	// The client writes `hello` first; answer with `negotiated` so connect resolves.
	await Bun.sleep(1);
	pushChunk?.(
		encodeFrame({
			v: PROFILE_VERSION,
			type: "negotiated",
			payload: { profileVersion: PROFILE_VERSION, capabilities: [], serverInfo: { name: "test" } },
		} as never),
	);
	const client = await connecting;

	return {
		client,
		feed: (chunk: string) => pushChunk?.(chunk),
		killTransport: (error: Error) => {
			// Decoder failure is one of the three real abrupt paths and is the only
			// one drivable deterministically without a socket. The socket `close` /
			// `error` paths are proven separately against a real unix socket below.
			void error;
			pushChunk?.("{ this is not a valid frame\n");
		},
		get writes() {
			return writes;
		},
		get closeCalls() {
			return closeCalls;
		},
	} as unknown as FakeTransport;
}

/** Serve one unix connection and answer `hello` with `negotiated`. */
async function serveOneUnixConnection(socketPath: string): Promise<{
	stop: () => void;
	dropClientConnection: () => void;
}> {
	let accepted: { write: (data: string) => void; terminate: () => void } | undefined;
	const server = Bun.listen<undefined>({
		unix: socketPath,
		socket: {
			open(socket) {
				accepted = {
					write: (data: string) => {
						socket.write(data);
					},
					terminate: () => socket.end(),
				};
			},
			data(_socket, data) {
				if (!new TextDecoder().decode(data).includes('"hello"')) return;
				accepted?.write(
					encodeFrame({
						v: PROFILE_VERSION,
						type: "negotiated",
						payload: { profileVersion: PROFILE_VERSION, capabilities: [], serverInfo: { name: "test" } },
					} as never),
				);
			},
		},
	});
	await Bun.sleep(1);
	return {
		stop: () => server.stop(true),
		dropClientConnection: () => accepted?.terminate(),
	};
}

describe("GajaewayClient.onTransportTerminal", () => {
	test("fires exactly once on abrupt transport failure", async () => {
		const fake = await connectFake();
		const seen: Error[] = [];
		fake.client.onTransportTerminal((error) => seen.push(error));

		fake.killTransport(new Error("boom"));
		await Bun.sleep(1);

		expect(seen.length).toBe(1);
		expect(seen[0]).toBeInstanceOf(Error);
	});

	test("does not fire for an intentional close()", async () => {
		const fake = await connectFake();
		let fired = 0;
		fake.client.onTransportTerminal(() => {
			fired += 1;
		});

		await fake.client.close();
		await Bun.sleep(1);

		expect(fired).toBe(0);
	});

	test("a transport failure after close() still does not fire", async () => {
		const fake = await connectFake();
		let fired = 0;
		fake.client.onTransportTerminal(() => {
			fired += 1;
		});

		await fake.client.close();
		// A real socket emits its `close` callback after an intentional end; that
		// must stay suppressed rather than killing the operator's child.
		fake.killTransport(new Error("late close"));
		await Bun.sleep(1);

		expect(fired).toBe(0);
	});

	test("the returned unsubscribe prevents delivery", async () => {
		const fake = await connectFake();
		let fired = 0;
		const off = fake.client.onTransportTerminal(() => {
			fired += 1;
		});

		off();
		fake.killTransport(new Error("boom"));
		await Bun.sleep(1);

		expect(fired).toBe(0);
	});

	test("a second failure does not re-fire", async () => {
		const fake = await connectFake();
		let fired = 0;
		fake.client.onTransportTerminal(() => {
			fired += 1;
		});

		fake.killTransport(new Error("first"));
		await Bun.sleep(1);
		fake.killTransport(new Error("second"));
		await Bun.sleep(1);

		expect(fired).toBe(1);
	});

	test("pending-request rejection behavior is unchanged", async () => {
		const fake = await connectFake();
		let fired = 0;
		fake.client.onTransportTerminal(() => {
			fired += 1;
		});

		const pending = fake.client.request("gateway.status");
		const rejected = pending.then(
			() => "resolved",
			(error: Error) => `rejected:${error.message}`,
		);

		fake.killTransport(new Error("transport died"));
		await Bun.sleep(1);

		expect(await rejected).toStartWith("rejected:");
		// The notification is additive: pending rejection still happened, and the
		// terminal handler also ran exactly once.
		expect(fired).toBe(1);
	});

	test("the raw socket and transport are not reachable from the public surface", async () => {
		const fake = await connectFake();

		const surface = new Set<string>();
		for (
			let proto: object | null = fake.client;
			proto && proto !== Object.prototype;
			proto = Object.getPrototypeOf(proto)
		) {
			for (const key of Object.getOwnPropertyNames(proto)) surface.add(key);
		}

		expect(surface.has("onTransportTerminal")).toBe(true);
		for (const forbidden of ["socket", "transport", "rawSocket", "getTransport", "getSocket"]) {
			expect(surface.has(forbidden)).toBe(false);
		}
		// Own enumerable properties must not leak the transport either.
		expect(Object.keys(fake.client)).toEqual([]);
	});

	test("a real unix socket close routes into the same notification", async () => {
		// This is the path that actually matters in production: the daemon dies and
		// the client's Bun socket `close` callback fires. It must reach the same
		// one-shot notification, or the CLI can never kill an orphaned TUI.
		const dir = await Bun.$`mktemp -d`.text();
		const socketPath = `${dir.trim()}/terminal-probe.sock`;
		const server = await serveOneUnixConnection(socketPath);
		try {
			const client = await GajaewayClient.connectSocket(socketPath);
			const seen: Error[] = [];
			client.onTransportTerminal((error) => seen.push(error));

			server.dropClientConnection();
			await Bun.sleep(20);

			expect(seen.length).toBe(1);
		} finally {
			server.stop();
			await Bun.$`rm -rf ${dir.trim()}`.quiet();
		}
	});

	test("a real unix socket close after an intentional close() stays suppressed", async () => {
		const dir = await Bun.$`mktemp -d`.text();
		const socketPath = `${dir.trim()}/terminal-probe-2.sock`;
		const server = await serveOneUnixConnection(socketPath);
		try {
			const client = await GajaewayClient.connectSocket(socketPath);
			let fired = 0;
			client.onTransportTerminal(() => {
				fired += 1;
			});

			// Ordinary teardown: the socket `close` callback still fires afterwards,
			// and it must not be reported as transport death.
			await client.close();
			await Bun.sleep(20);

			expect(fired).toBe(0);
		} finally {
			server.stop();
			await Bun.$`rm -rf ${dir.trim()}`.quiet();
		}
	});
});
