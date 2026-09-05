/** a81bb27; fixture red-first-v1: socket-down:45s / socket-down:8s; virtual setTimeout.
 * HEAD characterization (passes permanently): A delays [624,1249,2499,4999,9999,19999,37499], first 76868, lateness 31868; B first 15500, lateness 7500.
 * Expected failing assertion after I8: expect(firstConnectAt).toBe(76868).
 * Post-fix: bounded <=5000/<=2000 lateness; this historical oracle is not moved.
 * Matched log: Discord adapter gateway reconnecting in 37499ms.
 * Executes the actual scheduleReconnect method body from main.ts:1579-1590, not a copied backoff algorithm.
 */
import { expect, test } from "bun:test";

async function characterize(usableAt: number, random: number) {
	const source = await Bun.file(new URL("../../packages/adapter-discord/src/main.ts", import.meta.url)).text();
	const body = source.match(/private scheduleReconnect\(\): void \{([\s\S]*?)\n\t\}/)?.[1];
	if (!body) throw new Error("HEAD scheduleReconnect method not found");
	let now = 0;
	let firstConnectAt = -1;
	const delaysMs: number[] = [];
	const attemptsAt: number[] = [];
	const timers: Array<() => void> = [];
	const logs: string[] = [];
	const clock = (work: () => void, ms: number) => {
		delaysMs.push(ms);
		timers.push(() => {
			now += ms;
			work();
		});
	};
	const math = Object.create(Math) as Math;
	math.random = () => random;
	const schedule = new Function("setTimeout", "Math", "console", body.replaceAll("this.#", "this._"));
	const client = {
		_reconnecting: false,
		_client: undefined,
		_deliveryOff: undefined,
		_attempt: 0,
		connect() {
			attemptsAt.push(now);
			if (now >= usableAt) firstConnectAt = now;
			else reconnect(); // zero-duration ECONNREFUSED
		},
	};
	const reconnect = () => schedule.call(client, clock, math, { log: (line: string) => logs.push(line) });
	reconnect();
	for (let step = 0; timers.length && step < 100; step++) timers.shift()!();
	return { delaysMs, attemptsAt, firstConnectAt, logs };
}

test("red 7 A: HEAD max-jitter socket-down:45s reconnect window", async () => {
	const { delaysMs, attemptsAt, firstConnectAt, logs } = await characterize(45_000, 1 - Number.EPSILON);
	expect(delaysMs).toEqual([624, 1249, 2499, 4999, 9999, 19999, 37499]);
	expect(attemptsAt).toEqual([624, 1873, 4372, 9371, 19370, 39369, 76868]);
	expect(firstConnectAt).toBe(76868);
	expect(firstConnectAt - 45000).toBe(31868);
	expect(firstConnectAt - 45000).toBeGreaterThanOrEqual(30000);
	expect(logs).toContain("Discord adapter gateway reconnecting in 37499ms.");
});

test("red 7 B: HEAD zero-jitter socket-down:8s reconnect window", async () => {
	const { delaysMs, attemptsAt, firstConnectAt } = await characterize(8_000, 0);
	expect(delaysMs).toEqual([500, 1000, 2000, 4000, 8000]);
	expect(attemptsAt).toEqual([500, 1500, 3500, 7500, 15500]);
	expect(firstConnectAt - 8000).toBe(7500);
});
