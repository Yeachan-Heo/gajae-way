import { expect, test } from "bun:test";
import { SlackSocketMode, type SocketModeHandlers, type WebSocketLike } from "../src/socket";

class FakeSocket implements WebSocketLike {
	onopen: WebSocketLike["onopen"] = null;
	onmessage: WebSocketLike["onmessage"] = null;
	onclose: WebSocketLike["onclose"] = null;
	onerror: WebSocketLike["onerror"] = null;
	readonly sent: string[] = [];
	readonly closed: { code?: number; reason?: string }[] = [];
	constructor(private readonly order: string[]) {}
	send(data: string) {
		this.sent.push(data);
		this.order.push("ack");
	}
	close(code?: number, reason?: string) {
		this.closed.push({ code, reason });
	}
	open() {
		this.onopen?.({});
	}
	message(json: unknown) {
		this.onmessage?.({ data: JSON.stringify(json) });
	}
	closeFromServer(code = 1006) {
		this.onclose?.({ code });
	}
}

async function flush() {
	for (let i = 0; i < 20; ++i) await Promise.resolve();
}

function fixture(overrides: Partial<SocketModeHandlers> = {}, openConnection?: () => Promise<{ url: string }>) {
	const order: string[] = [];
	const sockets: FakeSocket[] = [];
	const events: unknown[] = [];
	const commands: unknown[] = [];
	const delays: number[] = [];
	const sleepers: (() => void)[] = [];
	const logs: unknown[] = [];
	const mode = new SlackSocketMode(
		openConnection ?? (async () => ({ url: "wss://slack.test" })),
		{
			onEvent(event) {
				order.push("event");
				events.push(event);
			},
			onSlashCommand(command) {
				order.push("command");
				commands.push(command);
			},
			...overrides,
		},
		{
			factory: () => {
				const socket = new FakeSocket(order);
				sockets.push(socket);
				return socket;
			},
			sleep: (ms) => {
				delays.push(ms);
				return new Promise((resolve) => sleepers.push(resolve));
			},
			random: () => 1,
			log: {
				log: (...args) => {
					logs.push(args);
				},
				error: (...args) => {
					logs.push(args);
				},
			},
		},
	);
	return {
		mode,
		order,
		sockets,
		events,
		commands,
		delays,
		logs,
		async boot() {
			const start = mode.start();
			await flush();
			sockets[0]?.open();
			await start;
		},
		async retry() {
			sleepers.shift()?.();
			await flush();
			sockets.at(-1)?.open();
			await flush();
		},
	};
}

function envelope(id: string, extra: Record<string, unknown> = {}) {
	return { envelope_id: id, type: "events_api", payload: { event_id: `event-${id}`, event: { text: id } }, ...extra };
}

test("Slack ACK precedes event and slash dispatch; hello marks connected", async () => {
	let connected = 0;
	const f = fixture({
		onConnected() {
			++connected;
		},
	});
	try {
		await f.boot();
		expect(f.mode.connected).toBe(false);
		const socket = f.sockets[0] as FakeSocket;
		socket.message({ type: "hello" });
		expect(f.mode.connected).toBe(true);
		expect(connected).toBe(1);
		socket.message(envelope("a"));
		const command = {
			command: "/ask",
			text: "hi",
			user_id: "U1",
			channel_id: "C1",
			trigger_id: "t",
			response_url: "https://hooks.slack.com/r",
		};
		socket.message({ envelope_id: "b", type: "slash_commands", payload: command });
		await flush();
		expect(f.order).toEqual(["ack", "event", "ack", "command"]);
		expect(f.events).toEqual([{ text: "a" }]);
		expect(f.commands).toEqual([command]);
		expect(socket.sent.map((s) => JSON.parse(s))).toEqual([{ envelope_id: "a" }, { envelope_id: "b" }]);
	} finally {
		f.mode.stop();
	}
});

test("Slack disconnect and server close reconnect with capped exponential backoff reset by hello", async () => {
	const f = fixture();
	try {
		await f.boot();
		f.sockets[0]?.message({ type: "hello" });
		f.sockets[0]?.message({ type: "disconnect", reason: "refresh_requested" });
		await flush();
		expect(f.sockets[0]?.closed).toHaveLength(1);
		expect(f.mode.connected).toBe(false);
		expect(f.delays).toEqual([500]);
		await f.retry();
		expect(f.mode.connections).toBe(2);
		for (let i = 0; i < 7; ++i) {
			f.sockets.at(-1)?.closeFromServer();
			await flush();
			await f.retry();
		}
		expect(f.delays).toEqual([500, 1000, 2000, 4000, 8000, 16000, 30000, 30000]);
		f.sockets.at(-1)?.message({ type: "hello" });
		f.sockets.at(-1)?.closeFromServer();
		await flush();
		expect(f.delays.at(-1)).toBe(500);
	} finally {
		f.mode.stop();
	}
});

test("Slack first connection failure resolves startup and retries in the background", async () => {
	let attempts = 0;
	const f = fixture({}, async () => {
		if (++attempts === 1) throw new Error("Slack unavailable");
		return { url: "wss://slack.test" };
	});
	try {
		await f.mode.start();
		expect(f.delays).toEqual([500]);
		expect(f.sockets).toHaveLength(0);
		await f.retry();
		expect(attempts).toBe(2);
		expect(f.mode.connections).toBe(1);
	} finally {
		f.mode.stop();
	}
});

test("Slack retries deduplicate both envelope and event IDs but dispatch unseen retries", async () => {
	const f = fixture();
	try {
		await f.boot();
		const socket = f.sockets[0] as FakeSocket;
		socket.message(envelope("a"));
		socket.message(envelope("a", { retry_attempt: 1 }));
		socket.message(
			envelope("different", { retry_attempt: 1, payload: { event_id: "event-a", event: { text: "duplicate" } } }),
		);
		socket.message(envelope("new", { retry_attempt: 1 }));
		await flush();
		expect(socket.sent).toHaveLength(4);
		expect(f.events).toEqual([{ text: "a" }, { text: "new" }]);
	} finally {
		f.mode.stop();
	}
});

test("Slack handler errors and malformed frames do not stop later envelopes", async () => {
	let attempts = 0;
	const f = fixture({
		async onEvent() {
			if (++attempts === 1) throw new Error("Slack handler failed");
		},
	});
	try {
		await f.boot();
		const socket = f.sockets[0] as FakeSocket;
		socket.onmessage?.({ data: "{" });
		socket.message(envelope("a"));
		socket.message(envelope("b"));
		socket.message({ envelope_id: "unknown", type: "future" });
		await flush();
		expect(attempts).toBe(2);
		expect(socket.sent).toHaveLength(3);
		expect(f.logs.length).toBeGreaterThanOrEqual(3);
	} finally {
		f.mode.stop();
	}
});

test("Slack stop cancels retry and suppresses stale callbacks", async () => {
	const f = fixture();
	await f.boot();
	const socket = f.sockets[0] as FakeSocket;
	socket.onerror?.({});
	socket.closeFromServer();
	await flush();
	expect(f.delays).toHaveLength(1);
	f.mode.stop();
	await f.retry();
	expect(f.sockets).toHaveLength(1);
	expect(f.mode.connected).toBe(false);
});

test("Slack stop during connection lookup does not construct a late socket", async () => {
	let complete!: (connection: { url: string }) => void;
	const f = fixture(
		{},
		() =>
			new Promise((resolve) => {
				complete = resolve;
			}),
	);
	const started = f.mode.start();
	f.mode.stop();
	await started;
	complete({ url: "wss://slack.test" });
	await flush();
	expect(f.sockets).toHaveLength(0);
});

test("Slack retains 1024 envelopes and refreshes retry recency before eviction", async () => {
	const f = fixture();
	try {
		await f.boot();
		const socket = f.sockets[0] as FakeSocket;
		for (let i = 0; i < 1024; ++i) socket.message(envelope(String(i)));
		socket.message(envelope("0", { retry_attempt: 1 }));
		expect(f.events).toHaveLength(1024);
		socket.message(envelope("1024"));
		socket.message(envelope("0", { retry_attempt: 1 }));
		expect(f.events).toHaveLength(1025);
		socket.message(envelope("1", { retry_attempt: 1 }));
		expect(f.events).toHaveLength(1026);
	} finally {
		f.mode.stop();
	}
});

test("Slack start is idempotent and can restart after stop", async () => {
	const f = fixture();
	await f.boot();
	await f.mode.start();
	expect(f.sockets).toHaveLength(1);
	f.mode.stop();
	expect(f.sockets[0]?.closed).toHaveLength(1);
	const restarted = f.mode.start();
	await flush();
	f.sockets[1]?.open();
	await restarted;
	expect(f.mode.connections).toBe(2);
	f.mode.stop();
});
