/**
 * Green 7 (plan I8): the reconnect window. Modes:
 *   A  socket down until t=45 000, ECONNREFUSED from t=0, max jitter -> reconnect <= 5 s after it returns
 *   B  socket down until t=8 000, zero jitter                          -> reconnect <= 2 s after it returns
 *   C  gateway.stopping announced at t=0, socket usable at t=8 000      -> reconnect <= 2 s after it returns
 * HEAD values (red 7 characterization, kept permanently in red-first/): A 31 868 ms, B 7 500 ms, C >= 30 000 ms.
 * The real ReconnectingGateway runs under a virtual clock; only the socket and timers are seams.
 */
import { expect, test } from "bun:test";
import { ReconnectingGateway } from "../src/main";

type Mode = { usableAt: number; random: number; stoppingAt?: number };

async function drive(input: Mode) {
	let mode = input;
	let now = 0;
	const timers: Array<{ at: number; work: () => void; id: number }> = [];
	let nextId = 1;
	const attemptsAt: number[] = [];
	const logs: string[] = [];
	let firstConnectAt = -1;
	const originalRandom = Math.random;
	Math.random = () => mode.random;
	const originalLog = console.log;
	console.log = (line: unknown) => {
		logs.push(String(line));
	};
	const handlers = new Map<string, () => void>();
	const fakeClient = {
		on(event: string, handler: () => void) {
			handlers.set(event, handler);
			return () => handlers.delete(event);
		},
		onEvent(event: string, handler: () => void) {
			handlers.set(event, handler);
			return () => handlers.delete(event);
		},
		onChatMessage: () => () => {},
		onProgress: () => () => {},
		request: async () => ({}),
		close: async () => {},
	};
	const gateway = new ReconnectingGateway(
		"/tmp/fixture.sock",
		{} as never,
		{ channels: {}, credentialFile: "" } as never,
		undefined,
		undefined,
		"/tmp/none",
		() => undefined,
		undefined,
		async () => {},
	);
	gateway.seams = {
		now: () => now,
		setTimeout: (work, delayMs) => {
			const id = nextId++;
			timers.push({ at: now + delayMs, work, id });
			return id;
		},
		clearTimeout: (id) => {
			const index = timers.findIndex((timer) => timer.id === id);
			if (index >= 0) timers.splice(index, 1);
		},
		connectSocket: async () => {
			attemptsAt.push(now);
			if (now >= mode.usableAt) {
				if (firstConnectAt < 0) firstConnectAt = now;
				return fakeClient as never;
			}
			throw Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED", syscall: "connect" });
		},
	};
	// Mute the side effects connect() wires up after success; only the link matters here.
	(gateway as unknown as { monitor: () => void }).monitor = () => {};
	(gateway as unknown as { recoverMissedMessages: () => Promise<void> }).recoverMissedMessages = async () => {};
	const originalError = console.error;
	console.error = () => {};
	try {
		if (mode.stoppingAt !== undefined) {
			// Mode C: the adapter is connected (socket was usable), then the gateway
			// announces its stop at t=stoppingAt and the socket is gone until usableAt.
			const usableAt = mode.usableAt;
			mode = { ...mode, usableAt: 0 };
			await gateway.connect();
			mode = { ...mode, usableAt };
			attemptsAt.length = 0;
			firstConnectAt = -1;
			now = mode.stoppingAt ?? 0;
			handlers.get("gateway.stopping")?.();
		} else {
			await gateway.connect();
		}
		for (let step = 0; step < 500 && firstConnectAt < 0; step++) {
			timers.sort((a, b) => a.at - b.at);
			const next = timers.shift();
			if (!next) break;
			now = Math.max(now, next.at);
			next.work();
			await Bun.sleep(0);
		}
		return { attemptsAt, firstConnectAt, logs };
	} finally {
		Math.random = originalRandom;
		console.log = originalLog;
		console.error = originalError;
	}
}

test("green 7 A: socket back at 45 s with max jitter -> reconnect within 5 s (HEAD: 31 868 ms)", async () => {
	const { firstConnectAt, logs } = await drive({ usableAt: 45_000, random: 1 - Number.EPSILON });
	expect(firstConnectAt).toBeGreaterThanOrEqual(45_000);
	expect(firstConnectAt - 45_000).toBeLessThanOrEqual(5_000);
	expect(logs.some((line) => /reconnect window opened \(ECONNREFUSED\)/.test(line))).toBe(true);
	expect(logs.some((line) => /reconnecting in (250|500|1000|2000)ms\./.test(line))).toBe(true);
});

test("green 7 B: socket back at 8 s with zero jitter -> reconnect within 2 s (HEAD: 7 500 ms)", async () => {
	const { firstConnectAt } = await drive({ usableAt: 8_000, random: 0 });
	expect(firstConnectAt - 8_000).toBeLessThanOrEqual(2_000);
});

test("green 7 C: gateway.stopping at t=0, socket back at 8 s -> reconnect within 2 s (HEAD: >= 30 000 via the monitor)", async () => {
	const { firstConnectAt, logs } = await drive({ usableAt: 8_000, random: 1 - Number.EPSILON, stoppingAt: 0 });
	expect(firstConnectAt).toBeGreaterThanOrEqual(8_000);
	expect(firstConnectAt - 8_000).toBeLessThanOrEqual(2_000);
	expect(logs.some((line) => /reconnect window opened \(gateway\.stopping\)/.test(line))).toBe(true);
});

test("after the 120 s window the ordinary curve resumes (no unbounded fast retry)", async () => {
	const { attemptsAt } = await drive({ usableAt: 200_000, random: 0 });
	const late = attemptsAt.filter((at) => at > 120_000);
	const gaps = late.slice(1).map((at, i) => at - late[i]!);
	expect(gaps.some((gap) => gap >= 4_000)).toBe(true);
});
