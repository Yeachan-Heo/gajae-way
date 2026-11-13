import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { SWEEP_MAILBOX_WAIT_MS } from "../src/orchestrator/sweep-limiter";
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

/** Counts the CLI-shaped reads a recovery/reconciliation sweep makes through the port. */
class CountingReadPort extends ScriptedSessionPort {
	delayMs = 0;
	inFlight = 0;
	peak = 0;
	readonly reads: string[] = [];

	async #counted<T>(kind: string, sessionId: string, read: () => Promise<T>): Promise<T> {
		this.reads.push(`${kind}:${sessionId}`);
		this.inFlight++;
		this.peak = Math.max(this.peak, this.inFlight);
		try {
			if (this.delayMs > 0) await Bun.sleep(this.delayMs);
			return await read();
		} finally {
			this.inFlight--;
		}
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

	resetCounts(): void {
		this.reads.length = 0;
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
	extra: { setTimeout?: (work: () => void, delayMs: number) => unknown; log?: (line: string) => void } = {},
): PersonaSessionManager {
	return new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
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
});

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
});
