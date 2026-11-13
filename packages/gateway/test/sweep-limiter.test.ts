import { afterEach, expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import {
	inSweep,
	SWEEP_READ_TIMEOUT_MS,
	SweepLimiter,
	SweepTurns,
	singleFlight,
	sweepControl,
	sweepHandOn,
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

test("a control operation hands the turn to the next unit; the unit continues without it", async () => {
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
	control.resolve();
	// A finishes without waiting for the turn back, while B still holds it.
	await unitA;
	expect(order).toEqual(["A:read", "A:control-start", "B:took-turn", "A:control-end", "A:after-control"]);
	expect(first.held).toBe(false);
	expect(second.held).toBe(true);
	// Releasing a handed-on turn is a no-op; only the holder frees the slot.
	first.release();
	expect(turns.tryTake()).toBeUndefined();
	second.release();
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

test("a persona unit keeps the read slot across its reads; other sweeps read between units", async () => {
	const limiter = new SweepLimiter();
	const turns = new SweepTurns();
	const order: string[] = [];
	const read = (label: string) =>
		limiter.read(async () => {
			order.push(label);
			await Bun.sleep(2);
		});
	const turn = await turns.take();
	const unit = limiter.runTurn(turn, async () => {
		await read("unit-1");
		await Bun.sleep(5); // non-read work between reads: the slot stays with the unit
		await read("unit-2");
		await read("unit-3");
	});
	await Bun.sleep(1);
	const other = limiter.run(async () => await read("lane"));
	await unit;
	turn.release();
	await other;
	expect(order).toEqual(["unit-1", "unit-2", "unit-3", "lane"]);
});

test("a unit gives the read slot up with its turn and a finished unit leaks no slot", async () => {
	const limiter = new SweepLimiter();
	const turns = new SweepTurns();
	const order: string[] = [];
	const control = Promise.withResolvers<void>();
	const turn = await turns.take();
	let late: Promise<void> | undefined;
	const unit = limiter.runTurn(turn, async () => {
		await limiter.read(async () => void order.push("unit-1"));
		await sweepControl(async () => {
			order.push("control");
			await control.promise;
		});
		await limiter.read(async () => void order.push("unit-2"));
		// A fire-and-forget read that outlives the unit.
		late = Bun.sleep(5).then(async () => await limiter.read(async () => void order.push("late")));
	});
	await Bun.sleep(1);
	// While the unit is inside its control operation another sweep can read.
	await limiter.run(async () => await limiter.read(async () => void order.push("lane-during-control")));
	control.resolve();
	await unit;
	turn.release();
	await late;
	await limiter.run(async () => await limiter.read(async () => void order.push("after")));
	expect(order).toEqual(["unit-1", "control", "lane-during-control", "unit-2", "late", "after"]);
});

test("sweepHandOn keeps the sweep scope while handing the turn on", async () => {
	const limiter = new SweepLimiter();
	const turns = new SweepTurns();
	const turn = await turns.take();
	const seen: boolean[] = [];
	const step = Promise.withResolvers<void>();
	const unit = limiter.runTurn(turn, async () => {
		await sweepHandOn(async () => {
			seen.push(inSweep());
			await step.promise;
		});
	});
	await Bun.sleep(1);
	const next = turns.tryTake();
	expect(next).toBeDefined();
	step.resolve();
	await unit;
	next?.release();
	turn.release();
	expect(seen).toEqual([true]);
});

test("singleFlight skips a run while one is in flight and a failed run clears the guard", async () => {
	const logs: string[] = [];
	let runs = 0;
	const gate = Promise.withResolvers<void>();
	let fail = true;
	const tick = singleFlight(
		"persona",
		async () => {
			runs++;
			await gate.promise;
			if (fail) throw new Error("sweep failed");
		},
		(line) => logs.push(line),
	);
	tick();
	tick();
	expect(runs).toBe(1);
	expect(logs).toEqual(["reconcile_sweep_skipped subsystem=persona reason=in_flight"]);
	gate.resolve();
	await Bun.sleep(1);
	fail = false;
	tick();
	expect(runs).toBe(2);
});

test("BrokerSessionPort control methods leave the sweep scope; host termination hands the turn on but keeps its reads in scope", async () => {
	const file = await readFile(join(import.meta.dir, "../src/orchestrator/session-port.ts"), "utf8");
	const source = file.slice(file.indexOf("export class BrokerSessionPort"));
	const control = [
		"bind",
		"resume",
		"send",
		"steer",
		"setModel",
		"setServiceTier",
		"close",
		"attachTail",
		"runCompaction",
		"runExclusive",
		"request",
	];
	for (const method of control) {
		const at = source.search(new RegExp(`\\n\\t(async )?${method}(<T>)?\\(`));
		expect(at, `${method} not found`).toBeGreaterThan(0);
		const body = source.slice(at, source.indexOf("\n\t}\n", at));
		expect(body, `${method} must leave sweep scope first`).toMatch(
			new RegExp(`\\{\\n(\\t\\t//[^\\n]*\\n)*\\t\\tif \\(inSweep\\(\\)\\) return (await )?sweepControl\\(`),
		);
	}
	const terminate = source.slice(source.search(/\n\tasync terminateHost\(/));
	expect(terminate.slice(0, 600)).toContain("return await sweepHandOn(");
});

test("a sweep read keeps a caller's own longer budget", async () => {
	const timeouts: Array<{ readonly op: string; readonly timeoutMs?: number }> = [];
	const cli: CliRunner = async (args, options) => {
		timeouts.push({ op: args.includes("turn.result") ? "turn.result" : "inspect", timeoutMs: options?.timeoutMs });
		if (args.includes("inspect")) return liveInspect();
		return { exitCode: 0, stdout: JSON.stringify({ ok: false, error: { code: "not_found" } }), stderr: "" };
	};
	const { port, repo, sessionIds } = await ownedPort(cli);
	const limiter = new SweepLimiter();
	const sessionId = sessionIds[0]!;
	// turn.result brings a 15s budget: a sweep must not cut it to 10s and turn
	// a slow-but-answering read into a spent output attempt.
	await limiter.run(
		async () =>
			await port.fetchWorkerOutput({ sessionId, repo, opRef: "gw-w-sweep-budget", notBeforeMs: Date.now() - 1_000 }),
	);
	await limiter.run(async () => await port.liveness({ sessionId, repo }));
	expect(timeouts).toEqual([
		{ op: "turn.result", timeoutMs: 15_000 },
		{ op: "inspect", timeoutMs: SWEEP_READ_TIMEOUT_MS },
	]);
});

test("a unit that handed its turn on never deadlocks with the unit that took it", async () => {
	const limiter = new SweepLimiter();
	const turns = new SweepTurns();
	const order: string[] = [];
	let inFlight = 0;
	let peak = 0;
	const read = (label: string, ms = 2) =>
		limiter.read(async () => {
			inFlight++;
			peak = Math.max(peak, inFlight);
			order.push(label);
			await Bun.sleep(ms);
			inFlight--;
		});
	const first = await turns.take();
	const unitA = limiter.runTurn(first, async () => {
		await read("a-1");
		// Host termination: the turn goes on, and the termination still reads and
		// then waits through its grace while the next unit runs.
		await sweepHandOn(async () => {
			await read("a-handed-on");
			await Bun.sleep(20);
		});
		await read("a-2");
	});
	const second = await turns.take();
	const unitB = limiter
		.runTurn(second, async () => {
			await read("b-1", 10);
			await read("b-2", 10);
			await read("b-3", 10);
		})
		.finally(() => second.release());
	const outcome = await Promise.race([
		Promise.all([unitA, unitB]).then(() => "settled"),
		Bun.sleep(2_000).then(() => "deadlock"),
	]);
	first.release();
	expect(outcome).toBe("settled");
	expect(peak).toBe(1);
	expect(order.slice(0, 3)).toEqual(["a-1", "a-handed-on", "b-1"]);
	// A's later read is let in at one of B's read boundaries, not after B's whole unit.
	expect(order.indexOf("a-2")).toBeLessThan(order.indexOf("b-3"));
});
