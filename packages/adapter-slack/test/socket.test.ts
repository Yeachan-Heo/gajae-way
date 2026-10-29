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

/** A socket whose TCP link is half-open: it accepts pings and never answers, closes, or errors. */
class HalfOpenSocket extends FakeSocket {
	readonly pings: number[] = [];
	readonly listeners = new Map<string, (event: unknown) => void>();
	constructor(
		order: string[],
		private readonly clock: () => number,
	) {
		super(order);
	}
	addEventListener(type: "ping" | "pong", listener: (event: unknown) => void) {
		this.listeners.set(type, listener);
	}
	ping() {
		this.pings.push(this.clock());
	}
	pong() {
		this.listeners.get("pong")?.({});
	}
}

function livenessFixture(livenessTimeoutMs = 15 * 60_000) {
	let now = 1_000_000;
	const order: string[] = [];
	const sockets: HalfOpenSocket[] = [];
	const logs: string[] = [];
	const disconnects: string[] = [];
	const delays: number[] = [];
	const sleepers: (() => void)[] = [];
	const ticks: { work: () => void; ms: number; cleared: boolean }[] = [];
	const mode = new SlackSocketMode(
		async () => ({ url: "wss://slack.test" }),
		{
			onEvent() {},
			onSlashCommand() {},
			onDisconnected(reason) {
				disconnects.push(reason);
			},
		},
		{
			factory: () => {
				const socket = new HalfOpenSocket(order, () => now);
				sockets.push(socket);
				return socket;
			},
			sleep: (ms) => {
				delays.push(ms);
				return new Promise((resolve) => sleepers.push(resolve));
			},
			random: () => 1,
			log: { log: (line) => logs.push(String(line)), error: (line) => logs.push(String(line)) },
			livenessTimeoutMs,
			now: () => now,
			setInterval: (work, ms) => {
				const tick = { work, ms, cleared: false };
				ticks.push(tick);
				return tick;
			},
			clearInterval: (timer) => {
				(timer as { cleared: boolean }).cleared = true;
			},
		},
	);
	/** Advance the clock and run every live watchdog tick, as a real interval would. */
	const elapse = (ms: number) => {
		now += ms;
		for (const tick of ticks) if (!tick.cleared) tick.work();
	};
	return {
		mode,
		sockets,
		logs,
		disconnects,
		delays,
		sleepers,
		ticks,
		elapse,
		async boot() {
			const start = mode.start();
			await flush();
			sockets[0]?.open();
			await start;
		},
	};
}

test("Slack half-open socket with no traffic is force-closed and the reconnect loop replaces it", async () => {
	const f = livenessFixture();
	try {
		await f.boot();
		const socket = f.sockets[0] as HalfOpenSocket;
		socket.message({ type: "hello" });
		expect(f.ticks).toHaveLength(1);
		expect(f.ticks[0]?.ms).toBe(60_000);
		// Quiet for half the window: the adapter probes rather than concluding anything.
		f.elapse(8 * 60_000);
		expect(socket.pings).toHaveLength(1);
		expect(socket.closed).toHaveLength(0);
		// The peer never answers; the full window elapses and the socket is retired.
		f.elapse(8 * 60_000);
		expect(socket.closed).toHaveLength(1);
		expect(f.mode.connected).toBe(false);
		expect(f.disconnects).toEqual(["Slack socket stale"]);
		expect(f.logs.some((line) => line.includes("Slack socket stale"))).toBe(true);
		expect(f.ticks[0]?.cleared).toBe(true);
		await flush();
		// The ordinary reconnect path owns the replacement: backoff, then a fresh socket.
		expect(f.delays).toEqual([500]);
		f.sleepers.shift()?.();
		await flush();
		f.sockets[1]?.open();
		await flush();
		expect(f.mode.connections).toBe(2);
		expect(f.ticks).toHaveLength(2);
	} finally {
		f.mode.stop();
	}
});

test("Slack pongs and frames keep a quiet socket alive", async () => {
	const f = livenessFixture();
	try {
		await f.boot();
		const socket = f.sockets[0] as HalfOpenSocket;
		socket.message({ type: "hello" });
		f.elapse(8 * 60_000);
		expect(socket.pings).toHaveLength(1);
		socket.pong();
		f.elapse(8 * 60_000);
		expect(socket.closed).toHaveLength(0);
		expect(socket.pings).toHaveLength(2);
		socket.message(envelope("keepalive"));
		f.elapse(14 * 60_000);
		expect(socket.closed).toHaveLength(0);
		expect(f.mode.connected).toBe(true);
	} finally {
		f.mode.stop();
	}
});

test("#710: a connections.open that never answers is abandoned and the reconnect loop opens a fresh socket", async () => {
	const timers: { work: () => void; ms: number; cleared: boolean }[] = [];
	const delays: number[] = [];
	const sleepers: (() => void)[] = [];
	const logs: string[] = [];
	const sockets: FakeSocket[] = [];
	let opens = 0;
	const mode = new SlackSocketMode(
		// The first request hangs forever, as one stuck on a dead keep-alive connection does.
		() => (++opens === 1 ? new Promise<{ url: string }>(() => {}) : Promise.resolve({ url: "wss://slack.test" })),
		{ onEvent() {}, onSlashCommand() {} },
		{
			factory: () => {
				const socket = new FakeSocket([]);
				sockets.push(socket);
				return socket;
			},
			sleep: (ms) => {
				delays.push(ms);
				return new Promise((resolve) => sleepers.push(resolve));
			},
			random: () => 1,
			log: { log: (line) => logs.push(String(line)), error: (line) => logs.push(String(line)) },
			openTimeoutMs: 30_000,
			setTimeout: (work, ms) => {
				const timer = { work, ms, cleared: false };
				timers.push(timer);
				return timer;
			},
			clearTimeout: (timer) => {
				(timer as { cleared: boolean }).cleared = true;
			},
		},
	);
	try {
		const start = mode.start();
		await flush();
		expect(opens).toBe(1);
		expect(timers.map((timer) => timer.ms)).toEqual([30_000]);
		timers[0]!.work();
		await flush();
		await start;
		expect(logs).toContain("Slack socket connection failed: connections.open timed out after 30000ms");
		expect(delays).toEqual([500]);
		sleepers.shift()?.();
		await flush();
		expect(opens).toBe(2);
		expect(sockets).toHaveLength(1);
		// An answered request clears its own bound.
		expect(timers).toHaveLength(2);
		expect(timers[1]!.cleared).toBe(true);
		sockets[0]!.open();
		sockets[0]!.message({ type: "hello" });
		expect(mode.connected).toBe(true);
	} finally {
		mode.stop();
	}
});

test("#710: a socket kept busy by pings but never greeted with hello is replaced after the liveness window", async () => {
	const f = livenessFixture();
	try {
		await f.boot();
		const socket = f.sockets[0] as HalfOpenSocket;
		expect(f.mode.connected).toBe(false);
		// Slack pings every few seconds, so the plain quiet check never fires.
		for (let minute = 0; minute < 14; minute++) {
			f.elapse(60_000);
			socket.listeners.get("ping")?.({});
		}
		expect(f.disconnects).toEqual([]);
		f.elapse(60_000);
		expect(f.disconnects).toEqual(["Slack socket never connected"]);
		expect(f.logs.some((line) => line.startsWith("Slack socket never connected: no hello after 900s"))).toBe(true);
		await flush();
		expect(f.delays).toEqual([500]);
	} finally {
		f.mode.stop();
	}
});

test("#710: slash_commands envelopes never count as Events API delivery", async () => {
	const f = livenessFixture();
	try {
		await f.boot();
		const socket = f.sockets[0] as HalfOpenSocket;
		socket.message({ type: "hello" });
		socket.message({
			envelope_id: "s1",
			type: "slash_commands",
			payload: { command: "/status", text: "", user_id: "U1", channel_id: "C1" },
		});
		await flush();
		expect(f.mode.lastEnvelopeAt).toBeUndefined();
	} finally {
		f.mode.stop();
	}
});

test("#710: the socket reports when it said hello and when the last envelope arrived, and restart reconnects", async () => {
	const f = livenessFixture();
	try {
		await f.boot();
		const socket = f.sockets[0] as HalfOpenSocket;
		expect(f.mode.connectedSince).toBeUndefined();
		expect(f.mode.lastEnvelopeAt).toBeUndefined();
		socket.message({ type: "hello" });
		expect(f.mode.connectedSince).toBe(1_000_000);
		f.elapse(5_000);
		socket.message(envelope("e1"));
		await flush();
		expect(f.mode.lastEnvelopeAt).toBe(1_005_000);
		f.mode.restart("Slack events possibly disabled");
		expect(f.disconnects).toEqual(["Slack events possibly disabled"]);
		expect(f.mode.connected).toBe(false);
		expect(f.mode.connectedSince).toBeUndefined();
		await flush();
		expect(f.delays).toEqual([500]);
		f.sleepers.shift()?.();
		await flush();
		f.sockets[1]?.open();
		f.sockets[1]?.message({ type: "hello" });
		expect(f.mode.connections).toBe(2);
		expect(f.mode.connectedSince).toBe(1_005_000);
	} finally {
		f.mode.stop();
	}
});
