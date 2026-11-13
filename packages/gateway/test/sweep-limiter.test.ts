import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import {
	inSweep,
	SWEEP_READ_TIMEOUT_MS,
	SweepLimiter,
	SweepTurns,
	sweepControl,
} from "../src/orchestrator/sweep-limiter";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import { createOwnedSessionFixture, initializeTestBrokerAuthority, noRelay } from "./session-port.fake";

let home = "";
let database: Store | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

function deferred<T = void>() {
	let resolve!: (value: T) => void;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

test("sweep reads share one slot process-wide; nested scopes join, control leaves the scope", async () => {
	const limiter = new SweepLimiter();
	let inFlight = 0;
	let peak = 0;
	const read = () =>
		limiter.read(async () => {
			inFlight++;
			peak = Math.max(peak, inFlight);
			await Bun.sleep(5);
			inFlight--;
		});
	await Promise.all(Array.from({ length: 12 }, () => limiter.run(read)));
	expect(peak).toBe(1);

	const seen: boolean[] = [];
	await limiter.run(async () => {
		seen.push(inSweep());
		await limiter.run(async () => seen.push(inSweep()));
		await sweepControl(async () => seen.push(inSweep()));
		seen.push(inSweep());
	});
	expect(seen).toEqual([true, true, false, true]);
	expect(inSweep()).toBe(false);
});

test("a sweep turn is lent to the next unit during a control operation and reclaimed first in line", async () => {
	const limiter = new SweepLimiter();
	const turns = new SweepTurns();
	const order: string[] = [];
	const control = deferred();
	const first = await turns.take();
	const unitA = limiter.runTurn(first, async () => {
		order.push("A:read");
		await sweepControl(async () => {
			order.push("A:control-start");
			await control.promise;
			order.push("A:control-end");
		});
		order.push("A:after-control");
	});
	// B takes the turn while A is inside its control operation.
	const second = await turns.take();
	order.push("B:took-turn");
	// A newcomer queued behind B must not overtake A's reclaim.
	const newcomer = turns.take().then((turn) => {
		order.push("C:took-turn");
		turn.release();
	});
	control.resolve();
	await Bun.sleep(1);
	expect(order).toEqual(["A:read", "A:control-start", "B:took-turn", "A:control-end"]);
	second.release();
	await unitA;
	first.release();
	await newcomer;
	expect(order).toEqual([
		"A:read",
		"A:control-start",
		"B:took-turn",
		"A:control-end",
		"A:after-control",
		"C:took-turn",
	]);
	// Releasing a lent turn leaks nothing: the slot is free again.
	expect(turns.tryTake()).toBeDefined();
});

test("a turn released while lent does not reclaim the slot", async () => {
	const turns = new SweepTurns();
	const limiter = new SweepLimiter();
	const turn = await turns.take();
	const control = deferred();
	const unit = limiter.runTurn(turn, async () => await sweepControl(async () => await control.promise));
	await Bun.sleep(1);
	turn.release();
	control.resolve();
	await unit;
	const next = turns.tryTake();
	expect(next).toBeDefined();
	expect(turns.tryTake()).toBeUndefined();
	next?.release();
});

async function ownedPort(cli: CliRunner, generation: () => number = () => 0) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-sweep-limiter-"));
	database = await Store.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	const sessionIds = Array.from({ length: 6 }, (_, index) => `00000000-0000-0000-0000-00000000000${index}`);
	for (const [index, sessionId] of sessionIds.entries())
		await createOwnedSessionFixture(database, authority, { sessionId, repo, originKey: `origin-${index}`, epoch: 0 });
	const port = new BrokerSessionPort({
		authority,
		database,
		cli,
		tailRunner: new TailRunner({ stream: noRelay, repo }),
		sleep: async () => {},
		brokerGeneration: generation,
	});
	return { port, repo, sessionIds };
}

function liveInspect(): CliResult {
	return {
		exitCode: 0,
		stdout: JSON.stringify({ ok: true, result: { session: { live: true } } }),
		stderr: "",
	};
}

test("CLI reads inside a sweep run one at a time at background priority under the sweep bound", async () => {
	type Call = { readonly priority?: string; readonly timeoutMs?: number };
	const calls: Call[] = [];
	let inFlight = 0;
	let peak = 0;
	const cli: CliRunner = async (args, options) => {
		if (!args.includes("inspect")) throw new Error(`unexpected command ${args.join(" ")}`);
		calls.push({ ...(options?.priority ? { priority: options.priority } : {}), timeoutMs: options?.timeoutMs });
		inFlight++;
		peak = Math.max(peak, inFlight);
		await Bun.sleep(5);
		inFlight--;
		return liveInspect();
	};
	const { port, repo, sessionIds } = await ownedPort(cli);
	const limiter = new SweepLimiter();

	await Promise.all(sessionIds.map((sessionId) => limiter.run(async () => await port.liveness({ sessionId, repo }))));
	expect(peak).toBe(1);
	expect(calls).toHaveLength(sessionIds.length);
	for (const call of calls) expect(call).toEqual({ priority: "background", timeoutMs: SWEEP_READ_TIMEOUT_MS });

	// The same reads outside a sweep are ordinary interactive work and stay concurrent.
	calls.length = 0;
	peak = 0;
	await Promise.all(sessionIds.map(async (sessionId) => await port.liveness({ sessionId, repo })));
	expect(peak).toBe(sessionIds.length);
	for (const call of calls) expect(call.priority).toBeUndefined();
});

test("a sweep read that straddles a broker replacement is unknown, never evidence", async () => {
	let generation = 1;
	let liveCalls = 0;
	const cli: CliRunner = async (args) => {
		if (args.includes("inspect")) {
			liveCalls++;
			generation++;
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { live: false } } }),
				stderr: "",
			};
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const { port, repo, sessionIds } = await ownedPort(cli, () => generation);
	const sessionId = sessionIds[0]!;
	const limiter = new SweepLimiter();
	// `live: false` read across the generation change must not reach the caller as "not live".
	expect(await limiter.run(async () => await port.liveness({ sessionId, repo }))).toEqual({
		live: undefined,
		disowned: false,
	});
	// A strict read surfaces the broker-unavailable class its callers already treat as unknown.
	await expect(limiter.run(async () => await port.inspect({ sessionId, repo }))).rejects.toThrow("broker_unavailable");
	// Outside a sweep the read is returned as the broker gave it.
	expect((await port.liveness({ sessionId, repo })).live).toBe(false);
	expect(liveCalls).toBe(3);
});

test("a control operation inside a sweep never waits for the sweep read slot", async () => {
	const readGate = deferred();
	const closed: Array<string | undefined> = [];
	const cli: CliRunner = async (args, options) => {
		if (args.includes("inspect")) {
			await readGate.promise;
			return liveInspect();
		}
		if (args.includes("session.close")) {
			closed.push(options?.priority);
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: {} }), stderr: "" };
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const { port, repo, sessionIds } = await ownedPort(cli);
	const limiter = new SweepLimiter();
	// One sweep read holds the slot...
	const reading = limiter.run(async () => await port.liveness({ sessionId: sessionIds[0]!, repo }));
	await Bun.sleep(1);
	// ...while another sweep's close (a control operation) proceeds as interactive work.
	await limiter.run(async () => await port.close({ sessionId: sessionIds[1]!, repo }));
	expect(closed).toEqual([undefined]);
	readGate.resolve();
	expect((await reading).live).toBe(true);
});
