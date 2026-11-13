import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import {
	currentSweep,
	inSweep,
	SWEEP_MAILBOX_WAIT_MS,
	sweepControl,
	sweepHandOn,
} from "../src/orchestrator/sweep-limiter";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

let home = "";
let database: Store | undefined;
let manager: PersonaSessionManager | undefined;

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

/**
 * Counts the CLI-shaped reads a recovery/reconciliation sweep makes through the
 * port. Like BrokerSessionPort's CLI runner, a read made in sweep scope takes the
 * sweep read slot.
 */
class CountingReadPort extends ScriptedSessionPort {
	delayMs = 0;
	/** Delay only sweep-scope reads, so a measurement isolates waiting caused by sweeps. */
	sweepOnlyDelay = false;
	inFlight = 0;
	peak = 0;
	readonly reads: string[] = [];
	readonly scopes: string[] = [];

	async #counted<T>(kind: string, sessionId: string, read: () => Promise<T>): Promise<T> {
		const sweep = currentSweep();
		this.scopes.push(`${kind}:${sessionId}:${sweep ? "sweep" : "interactive"}`);
		const run = async () => {
			this.reads.push(`${kind}:${sessionId}`);
			this.inFlight++;
			this.peak = Math.max(this.peak, this.inFlight);
			try {
				if (this.delayMs > 0 && (sweep || !this.sweepOnlyDelay)) await Bun.sleep(this.delayMs);
				return await read();
			} finally {
				this.inFlight--;
			}
		};
		return sweep ? await sweep.read(run) : await run();
	}

	override async inspect(input: Parameters<ScriptedSessionPort["inspect"]>[0]) {
		return await this.#counted("inspect", input.sessionId, async () => await super.inspect(input));
	}

	override async liveness(input: Parameters<ScriptedSessionPort["liveness"]>[0]) {
		return await this.#counted("liveness", input.sessionId, async () => await super.liveness(input));
	}

	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (input.relay !== undefined) return await super.status(input);
		return await this.#counted("status", input.sessionId, async () => await super.status(input));
	}

	override async runningJobs(input: Parameters<ScriptedSessionPort["runningJobs"]>[0]) {
		return await this.#counted("jobs", input.sessionId, async () => await super.runningJobs(input));
	}

	/** Grace a termination waits through after its inspect read (the real port's signal grace). */
	terminationGraceMs = 0;

	// Control operations and host termination behave like BrokerSessionPort's in a
	// sweep: control leaves the sweep scope, termination lends the turn and keeps
	// its own inspect read in scope.
	override async terminateHost(input: Parameters<ScriptedSessionPort["terminateHost"]>[0]) {
		return await sweepHandOn(async () => {
			await this.#counted("inspect", input.sessionId, async () => undefined);
			if (this.terminationGraceMs > 0) await Bun.sleep(this.terminationGraceMs);
			return await super.terminateHost(input);
		});
	}

	override async bind(input: Parameters<ScriptedSessionPort["bind"]>[0]) {
		return await sweepControl(async () => await super.bind(input));
	}

	override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
		return await sweepControl(async () => await super.send(input));
	}

	override async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]) {
		return await sweepControl(async () => await super.steer(input));
	}

	override async resume(input: Parameters<ScriptedSessionPort["resume"]>[0]) {
		return await sweepControl(async () => await super.resume(input));
	}

	override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
		return await sweepControl(async () => await super.attachTail(input));
	}

	resetCounts(): void {
		this.reads.length = 0;
		this.scopes.length = 0;
		this.peak = 0;
	}
}

type SweepOrigin = { readonly key: string; readonly ref: Record<string, string> };

function sweepOrigin(index: number): SweepOrigin {
	const conversationId = `sweep-${index}`;
	return {
		key: `loopback/loopback/${conversationId}`,
		ref: { platform: "loopback", kind: "loopback", conversationId },
	};
}

function enqueueAt(origin: SweepOrigin, messageId: string, body: string): void {
	const accepted = database?.transaction((tx) =>
		database?.inbox.enqueue(tx, {
			messageId,
			originKey: origin.key,
			originRefJson: JSON.stringify(origin.ref),
			body,
			source: "platform",
		}),
	);
	expect(accepted).toBe(true);
}

function newManager(
	port: ScriptedSessionPort,
	extra: {
		setTimeout?: (work: () => void, delayMs: number) => unknown;
		log?: (line: string) => void;
		brokerGeneration?: () => number;
		unresolvedHoldMs?: number;
		hostIdleRetireMs?: number;
	} = {},
): PersonaSessionManager {
	return new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		maxLiveHosts: 32,
		...(extra.brokerGeneration ? { brokerGeneration: extra.brokerGeneration } : {}),
		...(extra.unresolvedHoldMs ? { unresolvedHoldMs: extra.unresolvedHoldMs } : {}),
		...(extra.hostIdleRetireMs ? { hostIdleRetireMs: extra.hostIdleRetireMs } : {}),
		...(extra.setTimeout ? { setTimeout: extra.setTimeout, clearTimeout: () => {} } : {}),
		log: extra.log ?? (() => {}),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
}

/** Leaves one accepted, still-running turn per origin, then restarts the manager over the same store. */
async function restartWithRunningTurns(
	port: CountingReadPort,
	count: number,
	extra: Parameters<typeof newManager>[1] = {},
): Promise<SweepOrigin[]> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-sweep-"));
	database = await Store.open(join(home, "gateway.db"));
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	manager = newManager(port);
	const origins = Array.from({ length: count }, (_, index) => sweepOrigin(index));
	for (const origin of origins) {
		enqueueAt(origin, `running-${origin.key}`, "still running");
		await manager.notifyInbound(origin.key);
	}
	expect(port.sends).toHaveLength(count);
	await manager.stop();
	manager = newManager(port, extra);
	port.resetCounts();
	return origins;
}

function readsFor(port: CountingReadPort, sessionId: string): string[] {
	return port.reads.filter((read) => read.endsWith(`:${sessionId}`));
}

test("restart recovery reads one origin at a time instead of fanning out per origin", async () => {
	const port = new CountingReadPort();
	const origins = await restartWithRunningTurns(port, 6);
	port.delayMs = 5;
	await manager!.recover();
	// Every origin's authority reads ran (inspect -> status -> inspect -> liveness)...
	for (const send of port.sends) expect(readsFor(port, send.sessionId).length).toBeGreaterThanOrEqual(4);
	// ...but never two at once: the former Promise.all fan-out read every origin concurrently.
	expect(port.peak).toBe(1);
	// Recovery still adopts every running turn and resends nothing.
	expect(port.sends).toHaveLength(origins.length);
	for (const origin of origins) expect(manager!.state(origin.key)).toBe("turn-running");
});

test("recovery time is the serial read time, not a stall: 20 restarted origins", async () => {
	const port = new CountingReadPort();
	const origins = await restartWithRunningTurns(port, 20);
	port.delayMs = 10;
	const startedAt = performance.now();
	await manager!.recover();
	const elapsedMs = performance.now() - startedAt;
	for (const origin of origins) expect(manager!.state(origin.key)).toBe("turn-running");
	expect(port.peak).toBe(1);
	// Recovery cost is reads x read latency (4 authority reads plus the adopted turn's
	// reconcile per origin): at a 1s gjc CLI read, 20 origins x 5 reads ~ 100s per
	// post-restart sweep. Nothing else may wait: allow 2x scheduling slack.
	const perOrigin = port.reads.length / origins.length;
	expect(perOrigin).toBeGreaterThanOrEqual(4);
	expect(perOrigin).toBeLessThanOrEqual(6);
	// The contract's bound: 20 origins within 120 serial reads (<= 120s at a 1s gjc
	// CLI read), and no stall beyond the reads themselves.
	expect(port.reads.length).toBeLessThanOrEqual(120);
	expect(elapsedMs).toBeLessThan(port.reads.length * port.delayMs * 2 + 500);
}, 30_000);

test("overlapping recovery calls join one sweep", async () => {
	const port = new CountingReadPort();
	await restartWithRunningTurns(port, 3);
	const first = manager!.recover();
	const second = manager!.recover();
	expect(second).toBe(first);
	await first;
	// One sweep: each running turn's authority was read once (inspect -> status -> inspect -> liveness).
	for (const send of port.sends)
		expect(readsFor(port, send.sessionId).filter((read) => read.startsWith("inspect:"))).toHaveLength(2);
	// A call after the sweep settled starts a new one.
	const third = manager!.recover();
	expect(third).not.toBe(first);
	await third;
});

test("a message for an origin the sweep has not reached is admitted without waiting for the sweep", async () => {
	const port = new CountingReadPort();
	const origins = await restartWithRunningTurns(port, 4);
	port.delayMs = 20;
	const sweeping = manager!.recover();
	const fresh = sweepOrigin(99);
	enqueueAt(fresh, "fresh-message", "answer me now");
	await manager!.notifyInbound(fresh.key);
	expect(port.sends.some((send) => send.text === "answer me now")).toBe(true);
	// The sweep is still working through the restarted origins when the reply was sent.
	expect(port.reads.length).toBeLessThan(origins.length * 4);
	await sweeping;
	expect(port.peak).toBe(1);
});

test("a busy origin is deferred: no read, no apply, its late closure is a no-op, and the next sweep visits it", async () => {
	const port = new CountingReadPort();
	const timers: Array<{ work: () => void; delayMs: number }> = [];
	const logs: string[] = [];
	const origins = await restartWithRunningTurns(port, 2, {
		setTimeout: (work, delayMs) => {
			timers.push({ work, delayMs });
			return timers.length;
		},
		log: (line) => logs.push(line),
	});
	const [busy, idle] = origins as [SweepOrigin, SweepOrigin];
	const busySession = port.sends[0]!.sessionId;
	// Occupy the busy origin's mailbox with a model switch blocked on the port.
	const gate = Promise.withResolvers<void>();
	let blocked = false;
	const setModel = port.setModel.bind(port);
	port.setModel = async (input) => {
		blocked = true;
		await gate.promise;
		return await setModel(input);
	};
	const occupied = manager!.rebindModel(busy.key, "openai/gpt-test");
	await eventually(() => blocked, "the model switch did not reach the port");
	// The switch's own binding check is a user-path read; count only the sweep from here.
	port.resetCounts();
	const sweeping = manager!.recover();
	await eventually(
		() => timers.some((timer) => timer.delayMs === SWEEP_MAILBOX_WAIT_MS),
		"the busy mailbox did not arm the deferral timer",
	);
	for (const timer of timers.filter((candidate) => candidate.delayMs === SWEEP_MAILBOX_WAIT_MS)) timer.work();
	await sweeping;
	expect(logs.some((line) => line.startsWith(`sweep_origin_deferred origin=${busy.key} driver=recover`))).toBe(true);
	expect(readsFor(port, busySession)).toEqual([]);
	expect(port.reads.length).toBeGreaterThan(0);
	expect(manager!.state(idle.key)).toBe("turn-running");
	gate.resolve();
	await occupied;
	await eventually(
		() => logs.some((line) => line.startsWith(`sweep_closure_abandoned origin=${busy.key}`)),
		"the deferred closure did not run as a no-op",
	);
	expect(readsFor(port, busySession)).toEqual([]);
	await manager!.recover();
	expect(readsFor(port, busySession).length).toBeGreaterThan(0);
	expect(port.sends).toHaveLength(2);
}, 30_000);

test("broker-generation reconciliation reads one origin at a time and still fences every turn", async () => {
	const port = new CountingReadPort();
	const logs: string[] = [];
	const origins = await restartWithRunningTurns(port, 5, { log: (line) => logs.push(line) });
	await manager!.recover();
	port.resetCounts();
	port.delayMs = 5;
	await manager!.onBrokerGeneration(1);
	for (const origin of origins)
		expect(logs.some((line) => line.startsWith(`broker_generation_fenced originKey=${origin.key} `))).toBe(true);
	expect(port.reads.filter((read) => read.startsWith("status:")).length).toBeGreaterThanOrEqual(origins.length);
	expect(port.peak).toBe(1);
	expect(port.sends).toHaveLength(origins.length);
	// One line per pass names its sweep reads (the cutover's real-consumer check reads probes>0).
	const pass = logs.find((line) => line.startsWith("persona_recovery_sweep driver=broker_generation "));
	expect(pass).toMatch(/ origins=5 started=5 deferred=0 probes=([1-9]\d*) handed_on=\d+ duration_ms=\d+$/);
}, 30_000);

test("a message for an origin whose sweep unit is running is admitted within 2s while other sweeps contend", async () => {
	const port = new CountingReadPort();
	const origins = await restartWithRunningTurns(port, 3);
	port.delayMs = 300;
	port.sweepOnlyDelay = true;
	// Other sweep drivers (lane, monitor, work observers) keep the read slot busy.
	let contending = true;
	const limiter = manager!.sweepLimiter;
	const contenders = Array.from({ length: 3 }, async () => {
		while (contending) await limiter.run(async () => await limiter.read(async () => await Bun.sleep(300)));
	});
	const sweeping = manager!.recover();
	const target = origins[0]!;
	await eventually(
		() => port.reads.some((read) => read.endsWith(`:${port.sends[0]!.sessionId}`)),
		"the sweep did not reach the first origin",
	);
	enqueueAt(target, "message-during-unit", "answer while recovering");
	const startedAt = performance.now();
	await manager!.notifyInbound(target.key);
	const admittedMs = performance.now() - startedAt;
	contending = false;
	await sweeping;
	await Promise.all(contenders);
	// The running turn takes the message as a steer once its unit releases the mailbox.
	expect(port.steers.some((steer) => steer.text.includes("answer while recovering"))).toBe(true);
	expect(admittedMs).toBeLessThan(2_000);
	expect(port.peak).toBe(1);
}, 30_000);

test("every periodic persona sweep reads in sweep scope; user-path reads stay interactive", async () => {
	const port = new CountingReadPort();
	const origins = await restartWithRunningTurns(port, 2);
	await manager!.recover();
	await manager!.tick();
	await manager!.onBrokerGeneration(1);
	await manager!.sweepIdleHosts();
	const periodic = port.scopes.slice();
	expect(periodic.length).toBeGreaterThan(0);
	expect(periodic.every((entry) => entry.endsWith(":sweep"))).toBe(true);
	port.resetCounts();
	// A user message's binding check is ordinary work.
	const fresh = sweepOrigin(42);
	enqueueAt(fresh, "fresh-user", "hello");
	await manager!.notifyInbound(fresh.key);
	enqueueAt(origins[1]!, "steer-user", "more");
	await manager!.notifyInbound(origins[1]!.key);
	expect(port.scopes.every((entry) => entry.endsWith(":interactive"))).toBe(true);
	expect(inSweep()).toBe(false);
});

test("the generation fence leaves bound.brokerGeneration alone: a fenced old-generation turn gets no transcript failure probe", async () => {
	const port = new CountingReadPort();
	let generation = 0;
	const origins = await restartWithRunningTurns(port, 2, { brokerGeneration: () => generation });
	await manager!.recover();
	generation = 1;
	await manager!.onBrokerGeneration(1);
	const probesBefore = port.failureEvidenceProbes.length;
	for (const send of port.sends) port.fail(send.opRef, "scripted failure");
	for (const origin of origins) await manager!.reconcile(origin.key);
	// Settlement gates compare the bound's generation with the broker's: a fenced
	// turn still carries generation 0, so no failed-turn transcript probe runs for it.
	expect(port.failureEvidenceProbes.length).toBe(probesBefore);
});

test("an escalation burst after a restart still reads one at a time", async () => {
	const port = new CountingReadPort();
	const logs: string[] = [];
	const origins = await restartWithRunningTurns(port, 6, { log: (line) => logs.push(line), unresolvedHoldMs: 1 });
	// Every host is gone and every operation is unknown: each origin escalates its held turn.
	for (const send of port.sends) {
		port.forgetOperation(send.opRef);
		port.setSessionState(send.sessionId, { live: false });
	}
	await Bun.sleep(5);
	port.delayMs = 5;
	await manager!.recover();
	expect(port.reads.length).toBeGreaterThanOrEqual(origins.length * 4);
	expect(port.peak).toBe(1);
	expect(port.sends).toHaveLength(origins.length);
	// The burst really escalated every origin.
	expect(logs.filter((line) => line.startsWith("recovery_hold_escalated ")).length).toBe(origins.length);
}, 30_000);

test("idle-host retirement of several origins lends the turn through each termination and settles at one read at a time", async () => {
	const port = new CountingReadPort();
	const logs: string[] = [];
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-sweep-"));
	database = await Store.open(join(home, "gateway.db"));
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	manager = newManager(port, { log: (line) => logs.push(line), hostIdleRetireMs: 1 });
	const origins = Array.from({ length: 4 }, (_, index) => sweepOrigin(index));
	for (const origin of origins) {
		enqueueAt(origin, `idle-${origin.key}`, "answer once");
		await manager.notifyInbound(origin.key);
	}
	for (const send of port.sends) port.complete(send.opRef, "done");
	for (const origin of origins) await manager.tick(origin.key);
	await eventually(
		() => port.sends.every((send) => database!.inbox.turnRow(send.opRef)?.turn_state === "done"),
		"turns did not settle",
	);
	await Bun.sleep(5);
	port.resetCounts();
	port.delayMs = 5;
	port.terminationGraceMs = 20;
	const outcome = await Promise.race([
		manager.sweepIdleHosts().then((retired) => `retired=${retired}`),
		Bun.sleep(5_000).then(() => "hung"),
	]);
	expect(outcome).toBe(`retired=${origins.length}`);
	expect(port.peak).toBe(1);
	expect(logs.filter((line) => line.startsWith("persona_host_retired ")).length).toBe(origins.length);
}, 30_000);

test("a busy origin's generation fence still applies while its reconcile is deferred", async () => {
	const port = new CountingReadPort();
	const timers: Array<{ work: () => void; delayMs: number }> = [];
	const logs: string[] = [];
	const origins = await restartWithRunningTurns(port, 2, {
		setTimeout: (work, delayMs) => {
			timers.push({ work, delayMs });
			return timers.length;
		},
		log: (line) => logs.push(line),
	});
	await manager!.recover();
	const busy = origins[0]!;
	const gate = Promise.withResolvers<void>();
	let blocked = false;
	const setModel = port.setModel.bind(port);
	port.setModel = async (input) => {
		blocked = true;
		await gate.promise;
		return await setModel(input);
	};
	const occupied = manager!.rebindModel(busy.key, "openai/gpt-test");
	await eventually(() => blocked, "the model switch did not reach the port");
	const fencing = manager!.onBrokerGeneration(1);
	await eventually(
		() => timers.some((timer) => timer.delayMs === SWEEP_MAILBOX_WAIT_MS),
		"the busy mailbox did not arm the deferral timer",
	);
	for (const timer of timers.filter((candidate) => candidate.delayMs === SWEEP_MAILBOX_WAIT_MS)) timer.work();
	gate.resolve();
	await occupied;
	await fencing;
	expect(
		logs.some((line) => line.startsWith(`sweep_origin_deferred origin=${busy.key} driver=broker_generation`)),
	).toBe(true);
	// Deferred reconcile, applied fence: every origin's relay was fenced.
	for (const origin of origins)
		expect(logs.some((line) => line.startsWith(`broker_generation_fenced originKey=${origin.key} `))).toBe(true);
	expect(
		logs.some((line) =>
			line.startsWith("persona_recovery_sweep driver=broker_generation origins=2 started=1 deferred=1 "),
		),
	).toBe(true);
}, 30_000);

test("stopping the manager mid-sweep releases the sweep turn and settles the sweep", async () => {
	const port = new CountingReadPort();
	await restartWithRunningTurns(port, 4);
	port.delayMs = 20;
	const sweeping = manager!.recover();
	await eventually(() => port.reads.length > 0, "the sweep did not start");
	await manager!.stop();
	await expect(Promise.race([sweeping.then(() => "settled"), Bun.sleep(2_000).then(() => "hung")])).resolves.toBe(
		"settled",
	);
}, 30_000);
