import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { BrokerSession } from "@gajaeway/subsession";
import { OpRefRejectedError } from "@gajaeway/subsession";
import { parseConfigFile } from "../src/config";
import { PersonaSessionManager, personaBatchKey, personaBatchOpRef } from "../src/orchestrator/persona-session";
import type { SessionSendInput, SessionSteerInput } from "../src/orchestrator/session-port";
import type { TailAttachInput } from "../src/orchestrator/tail-runner";
import { GatewayDatabase, type InboundBatch } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "issue92-redteam" } as const;
const ORIGIN_KEY = "loopback/loopback/issue92-redteam";
const SOURCE_ROOT = join(import.meta.dir, "../src");

type Fixture = {
	readonly home: string;
	readonly database: GatewayDatabase;
	readonly manager: PersonaSessionManager;
	readonly port: ScriptedSessionPort;
	readonly batches: Map<string, InboundBatch>;
	readonly discarded: string[];
	readonly logs: string[];
	readonly terminal: string[];
	close(): Promise<void>;
};

type FixtureOptions = {
	readonly port?: ScriptedSessionPort;
	readonly settleWindowMs?: number;
	readonly now?: () => number;
	readonly setTimeout?: (work: () => void, delayMs: number) => unknown;
	readonly clearTimeout?: (timer: unknown) => void;
	readonly brokerGeneration?: () => number;
};

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 400; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function required<T>(value: T | undefined, message: string): T {
	if (value === undefined) throw new Error(message);
	return value;
}

async function fixture(options: FixtureOptions = {}): Promise<Fixture> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-issue92-redteam-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = options.port ?? new ScriptedSessionPort();
	const batches = new Map<string, InboundBatch>();
	const discarded: string[] = [];
	const logs: string[] = [];
	const terminal: string[] = [];
	const manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "issue92-redteam",
		repo: join(home, "workspace"),
		settleWindowMs: options.settleWindowMs ?? 0,
		...(options.now ? { now: options.now } : {}),
		...(options.setTimeout ? { setTimeout: options.setTimeout } : {}),
		...(options.clearTimeout ? { clearTimeout: options.clearTimeout } : {}),
		...(options.brokerGeneration ? { brokerGeneration: options.brokerGeneration } : {}),
		onTurnStart: ({ batch, rows }) => {
			batches.set(batch.batchKey, batch);
			return {
				text: rows.map((row) => row.body).join("\n"),
				onTerminal: ({ text }) => {
					terminal.push(text);
				},
			};
		},
		onInboundDiscard: (messageIds) => {
			discarded.push(...messageIds);
		},
		log: (line) => {
			logs.push(line);
		},
	});
	return {
		home,
		database,
		manager,
		port,
		batches,
		discarded,
		logs,
		terminal,
		async close() {
			await manager.stop();
			database.close();
			await rm(home, { recursive: true, force: true });
		},
	};
}

function enqueue(target: Fixture, messageId: string, body: string, receivedAt = new Date().toISOString()): void {
	expect(
		target.database.inboundEnqueue({
			messageId,
			originKey: ORIGIN_KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			receivedAt,
		}),
	).toBe(true);
}

function assertCoverage(target: Fixture, messageIds: readonly string[]): void {
	const rows = [...target.batches.values()].flatMap((batch) => target.database.inboundBatchRows(batch.batchKey));
	expect(rows.map((row) => row.message_id).sort()).toEqual([...messageIds].sort());
	expect(new Set(rows.map((row) => row.message_id)).size).toBe(messageIds.length);
	for (const row of rows)
		expect(row).toMatchObject({
			state: "done",
			batch_state: "done",
			attributed_op_ref: expect.any(String),
		});
	expect(target.database.inboundPendingCount(ORIGIN_KEY)).toBe(0);
}

class RetentionGapPort extends ScriptedSessionPort {
	readonly tailInputs = new Map<string, TailAttachInput[]>();

	async attachTail(input: TailAttachInput) {
		const inputs = this.tailInputs.get(input.sessionId) ?? [];
		inputs.push(input);
		this.tailInputs.set(input.sessionId, inputs);
		return await super.attachTail(input);
	}

	async emitRetentionGap(sessionId: string): Promise<void> {
		for (const input of this.tailInputs.get(sessionId) ?? [])
			await input.onRetentionGap?.({ sessionId, resync: { revision: 1, generation: 2, seq: 3 } });
	}
}

class FailFirstSteerPort extends ScriptedSessionPort {
	steerAttempts = 0;

	async steer(input: SessionSteerInput): Promise<void> {
		this.steerAttempts++;
		if (this.steerAttempts === 1) throw new Error("broker socket disappeared after steer dispatch");
		await super.steer(input);
	}
}

class IntermittentSteerPort extends ScriptedSessionPort {
	readonly #failAttempts: ReadonlySet<number>;
	steerAttempts = 0;

	constructor(failAttempts: ReadonlySet<number>) {
		super();
		this.#failAttempts = failAttempts;
	}

	async steer(input: SessionSteerInput): Promise<void> {
		this.steerAttempts++;
		if (this.#failAttempts.has(this.steerAttempts)) throw new Error(`scripted steer disconnect ${this.steerAttempts}`);
		await super.steer(input);
	}
}

class ConflictUnknownPort extends ScriptedSessionPort {
	async send(input: SessionSendInput): Promise<never> {
		this.sendAttempts.push(input);
		throw new OpRefRejectedError(input.opRef, "client_ref_conflict", { code: "client_ref_conflict" });
	}
}

class IdleRecoveryPort extends ScriptedSessionPort {
	readonly calls: string[] = [];

	async resume(input: Parameters<ScriptedSessionPort["resume"]>[0]) {
		this.calls.push(`resume:${input.sessionId}`);
		return await super.resume(input);
	}

	async send(input: SessionSendInput) {
		this.calls.push(`send:${input.sessionId}`);
		return await super.send(input);
	}
}

class InspectUnavailableIdleRecoveryPort extends IdleRecoveryPort {
	async inspect(input: Parameters<ScriptedSessionPort["inspect"]>[0]): Promise<BrokerSession | undefined> {
		await super.inspect(input);
		throw new Error("scripted inspect transport outage");
	}
}

async function sourceFiles(directory: string): Promise<string[]> {
	const entries = await readdir(directory, { withFileTypes: true });
	const files: string[] = [];
	for (const entry of entries) {
		const path = join(directory, entry.name);
		if (entry.isDirectory()) files.push(...(await sourceFiles(path)));
		else if (entry.isFile() && path.endsWith(".ts")) files.push(path);
	}
	return files;
}

test("red-team: a strict tail retention gap does not infer terminal or create another turn", async () => {
	const port = new RetentionGapPort();
	const target = await fixture({ port });
	try {
		enqueue(target, "gap-trigger", "keep the turn open");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "initial batch did not start");
		const send = required(port.sends[0], "initial gap send missing");
		const batch = target.batches.values().next().value as InboundBatch;

		await port.emitRetentionGap(send.sessionId);
		await eventually(
			() => target.logs.some((line) => line.startsWith(`retention_gap origin=${ORIGIN_KEY}`)),
			"gap was not observed",
		);
		expect(target.manager.state(ORIGIN_KEY)).toBe("turn-running");
		expect(port.sends).toHaveLength(1);
		expect(target.database.inboundBatchRows(batch.batchKey)).toEqual(
			expect.arrayContaining([expect.objectContaining({ state: "pending", batch_state: "accepted" })]),
		);

		port.complete(send.opRef, "terminal tail evidence");
		await eventually(
			() => target.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"gap case did not complete after terminal evidence",
		);
		assertCoverage(target, ["gap-trigger"]);
	} finally {
		await target.close();
	}
});

test("red-team: broker death during a steer leaves the row unconsumed and sends it once after terminal recovery", async () => {
	const port = new FailFirstSteerPort();
	const target = await fixture({ port });
	try {
		enqueue(target, "steer-trigger", "initial prompt");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "initial batch did not start");
		const first = required(port.sends[0], "initial steer send missing");

		enqueue(target, "steer-after-death", "must remain durable");
		await target.manager.notifyInbound(ORIGIN_KEY);
		expect(port.steerAttempts).toBe(1);
		expect(port.steers).toEqual([]);
		expect(target.database.inboundPendingOldest(ORIGIN_KEY)?.message_id).toBe("steer-after-death");

		await target.manager.onBrokerGeneration(2);
		port.complete(first.opRef, "first terminal");
		await eventually(() => port.sends.length === 2, "failed steer row was not re-fired as one later batch");
		const second = required(port.sends[1], "recovered steer send missing");
		expect(second.text).toBe("must remain durable");
		expect(port.sendAttempts.map((input) => input.opRef)).toHaveLength(2);
		port.complete(second.opRef, "second terminal");
		await eventually(
			() => target.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"broker-death sequence left pending rows",
		);
		assertCoverage(target, ["steer-trigger", "steer-after-death"]);
	} finally {
		await target.close();
	}
});

test("red-team: fixed-from-first settling fires at two seconds under 500ms fragments and includes the exact boundary", async () => {
	const base = Date.parse("2026-09-02T00:00:00.000Z");
	let now = base;
	const delays: number[] = [];
	const port = new ScriptedSessionPort();
	const target = await fixture({
		port,
		settleWindowMs: 2_000,
		now: () => now,
		setTimeout: (_work, delayMs) => {
			delays.push(delayMs);
			return delays.length;
		},
		clearTimeout: () => {},
	});
	try {
		for (const [offset, id] of [
			[0, "fragment-0"],
			[500, "fragment-1"],
			[1_000, "fragment-2"],
			[1_500, "fragment-3"],
			[2_000, "fragment-at-boundary"],
		] as const) {
			now = base + offset;
			enqueue(target, id, id, new Date(now).toISOString());
			await target.manager.notifyInbound(ORIGIN_KEY);
		}
		expect(delays).toEqual([2_000]);
		await target.manager.tick(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "continuous fragments starved the fixed settle window");
		expect(required(port.sends[0], "coalesced send missing").text.split("\n")).toEqual([
			"fragment-0",
			"fragment-1",
			"fragment-2",
			"fragment-3",
			"fragment-at-boundary",
		]);

		now = base + 2_500;
		enqueue(target, "fragment-after-fire", "fragment-after-fire", new Date(now).toISOString());
		await target.manager.notifyInbound(ORIGIN_KEY);
		expect(port.sends).toHaveLength(1);
		expect(port.steers).toEqual([expect.objectContaining({ text: "fragment-after-fire" })]);
		port.complete(required(port.sends[0], "coalesced send missing").opRef, "one coalesced response");
		await eventually(() => target.database.inboundPendingCount(ORIGIN_KEY) === 0, "coalesced fragments did not settle");
		expect(target.terminal).toEqual(["one coalesced response"]);
		assertCoverage(target, [
			"fragment-0",
			"fragment-1",
			"fragment-2",
			"fragment-3",
			"fragment-at-boundary",
			"fragment-after-fire",
		]);
	} finally {
		await target.close();
	}
});

test("red-team: restart after broker acceptance but before durable acceptance reconciles the same op-ref without another send", async () => {
	const port = new ScriptedSessionPort();
	const target = await fixture({ port });
	try {
		const receivedAt = "2026-09-02T00:00:00.000Z";
		const cutoff = receivedAt;
		const repo = join(target.home, "workspace");
		const binding = await port.bind({ originKey: ORIGIN_KEY, epoch: 0, repo });
		expect(target.database.putSessionAtEpoch(ORIGIN_KEY, binding.sessionId, 0)).toBe(true);
		enqueue(target, "crash-window", "accepted before attribution", receivedAt);
		const batchKey = personaBatchKey(ORIGIN_KEY, 0, "crash-window", cutoff);
		const opRef = personaBatchOpRef("issue92-redteam", ORIGIN_KEY, 0, "crash-window", cutoff);
		target.database.inboundSettleBatch({ originKey: ORIGIN_KEY, epoch: 0, cutoff, batchKey, opRef });
		expect(target.database.inboundBatchBindSession(batchKey, binding.sessionId)).toBe(true);
		port.seedAcceptedSend({ sessionId: binding.sessionId, repo, text: "accepted before attribution", opRef });

		await target.manager.recover();
		expect(port.sendAttempts).toHaveLength(1);
		expect(target.database.inboundBatchRows(batchKey)).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ state: "pending", batch_state: "accepted", attributed_op_ref: opRef }),
			]),
		);
		port.complete(opRef, "reconciled terminal");
		await eventually(
			() => target.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"accepted crash-window batch did not reconcile",
		);
		assertCoverage(target, ["crash-window"]);
	} finally {
		await target.close();
	}
});

test("red-team: /new preserves an accepted batch, discards only unbatched pre-floor work, and fences stale output", async () => {
	const base = Date.parse("2026-09-02T00:00:00.000Z");
	let now = base;
	const port = new ScriptedSessionPort();
	const target = await fixture({ port, now: () => now });
	try {
		enqueue(target, "old-trigger", "old accepted", new Date(base).toISOString());
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "old batch did not start");
		const old = required(port.sends[0], "old epoch send missing");
		const oldBatch = target.batches.values().next().value as InboundBatch;

		enqueue(target, "discard-before-new", "must be discarded", new Date(base + 1).toISOString());
		now = base + 2;
		await target.manager.reset(ORIGIN_KEY, JSON.stringify(ORIGIN), new Date(now).toISOString());
		expect(target.discarded).toEqual(["discard-before-new"]);
		expect(target.database.inboundBatchRows(oldBatch.batchKey)).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ message_id: "old-trigger", state: "pending", batch_state: "accepted" }),
			]),
		);

		now = base + 3;
		enqueue(target, "new-trigger", "new epoch", new Date(now).toISOString());
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 2, "new epoch was blocked by accepted retired work");
		const fresh = required(port.sends[1], "new epoch send missing");
		port.complete(old.opRef, "stale old output");
		port.complete(fresh.opRef, "fresh output");
		await eventually(
			() => target.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"/new sequence did not reconcile both batches",
		);
		expect(target.terminal).toEqual(["fresh output"]);
		expect(target.database.inboundBatchRows(oldBatch.batchKey)).toEqual(
			expect.arrayContaining([expect.objectContaining({ state: "done", batch_state: "done" })]),
		);
		assertCoverage(target, ["old-trigger", "new-trigger"]);
	} finally {
		await target.close();
	}
});

test("red-team: a stall on a retired hold does not abort it, block the next epoch, or deliver stale output", async () => {
	const port = new ScriptedSessionPort();
	const target = await fixture({ port });
	try {
		enqueue(target, "retired-trigger", "old generation");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "retired candidate did not start");
		const old = required(port.sends[0], "retired send missing");

		await target.manager.reset(ORIGIN_KEY, JSON.stringify(ORIGIN));
		await Bun.sleep(30);
		port.emitStall(old.sessionId, 120_000);
		await eventually(
			() => target.logs.some((line) => line.includes("reason=stall")),
			"retired stall did not become a durable hold",
		);
		await target.manager.recover();
		enqueue(target, "post-retired-stall", "new generation can proceed");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 2, "retired hold blocked new generation");
		const fresh = required(port.sends[1], "post-stall send missing");
		port.complete(fresh.opRef, "fresh generation output");
		port.complete(old.opRef, "stale generation output");
		await eventually(
			() => target.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"retired hold did not reconcile terminal",
		);
		expect(target.terminal).toEqual(["fresh generation output"]);
		assertCoverage(target, ["retired-trigger", "post-retired-stall"]);
	} finally {
		await target.close();
	}
});

test("red-team: unknown send outcomes are non-steerable holds that retain subsequent rows", async () => {
	const port = new ConflictUnknownPort();
	const target = await fixture({ port });
	try {
		enqueue(target, "conflict-trigger", "operation identity is unknown");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(
			() => target.logs.some((line) => line.startsWith(`recovery_client_ref_conflict origin=${ORIGIN_KEY}`)),
			"client_ref_conflict was not reconciled",
		);
		const batch = target.batches.values().next().value as InboundBatch;
		expect(target.database.inboundBatchRows(batch.batchKey)).toEqual(
			expect.arrayContaining([expect.objectContaining({ message_id: "conflict-trigger", batch_state: "settled" })]),
		);
		expect(target.logs.some((line) => line.includes("reason=operation_state_unknown"))).toBe(true);
		expect(port.sends).toEqual([]);

		enqueue(target, "steered-while-unknown", "this must not mutate an operator hold");
		await target.manager.notifyInbound(ORIGIN_KEY);
		expect(port.steers).toEqual([]);
		expect(target.database.inboundPendingOldest(ORIGIN_KEY)).toMatchObject({
			message_id: "steered-while-unknown",
			batch_role: null,
			batch_state: null,
		});
	} finally {
		await target.close();
	}
});

test("red-team: a terminal tail frame arriving during the status grace wins once without status-fallback delivery", async () => {
	const base = Date.parse("2026-09-02T00:00:00.000Z");
	const now = base;
	const timers: Array<{ readonly work: () => void; readonly delayMs: number }> = [];
	const port = new ScriptedSessionPort();
	const target = await fixture({
		port,
		settleWindowMs: 0,
		now: () => now,
		setTimeout: (work, delayMs) => {
			timers.push({ work, delayMs });
			return timers.length;
		},
		clearTimeout: () => {},
	});
	try {
		enqueue(target, "grace-tail-trigger", "tail frame must win", new Date(now).toISOString());
		await target.manager.notifyInbound(ORIGIN_KEY);
		await target.manager.tick(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "grace test did not start its first turn");
		const send = required(port.sends[0], "grace test send missing");
		const batch = required(target.batches.values().next().value, "grace test batch missing");

		port.seedOperation(send.opRef, send.sessionId, "terminal_ok", "status reported terminal first");
		await target.manager.tick(ORIGIN_KEY);
		const grace = required(
			timers.find((timer) => timer.delayMs === 250),
			"terminal grace was not scheduled",
		);
		expect(target.manager.state(ORIGIN_KEY)).toBe("turn-running");
		expect(target.terminal).toEqual([]);
		expect(target.logs.some((line) => line.includes("reason=tail_terminal_evidence_unavailable"))).toBe(true);
		expect(target.database.inboundBatchRows(batch.batchKey)[0]).toMatchObject({
			state: "pending",
			batch_state: "accepted",
		});

		port.complete(send.opRef, "tail terminal arrived during grace");
		await eventually(() => target.terminal.length === 1, "terminal tail did not settle the held batch");
		expect(target.terminal).toEqual(["tail terminal arrived during grace"]);
		expect(target.logs.some((line) => line.startsWith("terminal_status_reconciled"))).toBe(false);
		expect(target.database.inboundBatchRows(batch.batchKey)[0]).toMatchObject({ state: "done", batch_state: "done" });

		grace.work();
		await Bun.sleep(20);
		expect(target.terminal).toEqual(["tail terminal arrived during grace"]);
		expect(target.logs.some((line) => line.startsWith("terminal_status_reconciled"))).toBe(false);
	} finally {
		await target.close();
	}
});

test("red-team: slow tail keeps a status-terminal turn non-steerable until the bounded grace, then status settles it once", async () => {
	const base = Date.parse("2026-09-02T00:01:00.000Z");
	let now = base;
	const timers: Array<{ readonly work: () => void; readonly delayMs: number }> = [];
	const port = new ScriptedSessionPort();
	const target = await fixture({
		port,
		settleWindowMs: 0,
		now: () => now,
		setTimeout: (work, delayMs) => {
			timers.push({ work, delayMs });
			return timers.length;
		},
		clearTimeout: () => {},
	});
	try {
		enqueue(target, "slow-tail-trigger", "status is terminal but tail is late", new Date(now).toISOString());
		await target.manager.notifyInbound(ORIGIN_KEY);
		await target.manager.tick(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "slow-tail test did not start its first turn");
		const first = required(port.sends[0], "slow-tail first send missing");
		const firstBatch = required(target.batches.values().next().value, "slow-tail first batch missing");

		port.seedOperation(first.opRef, first.sessionId, "terminal_ok", "status-only terminal");
		await target.manager.tick(ORIGIN_KEY);
		const grace = required(
			timers.find((timer) => timer.delayMs === 250),
			"slow-tail terminal grace was not scheduled",
		);
		expect(target.terminal).toEqual([]);
		expect(target.manager.state(ORIGIN_KEY)).toBe("turn-running");
		expect(target.database.inboundBatchRows(firstBatch.batchKey)[0]).toMatchObject({
			state: "pending",
			batch_state: "accepted",
		});

		now++;
		enqueue(
			target,
			"arrived-during-grace",
			"must remain pending until original status settles",
			new Date(now).toISOString(),
		);
		await target.manager.notifyInbound(ORIGIN_KEY);
		expect(port.steers).toEqual([]);
		expect(target.database.inboundPendingOldest(ORIGIN_KEY)).toMatchObject({
			message_id: "arrived-during-grace",
			batch_role: null,
			batch_state: null,
		});
		expect(target.terminal).toEqual([]);

		grace.work();
		await eventually(() => target.terminal.length === 1, "status fallback did not settle after bounded grace");
		expect(target.terminal).toEqual(["status-only terminal"]);
		expect(target.logs.filter((line) => line.startsWith("terminal_status_reconciled"))).toHaveLength(1);
		expect(target.database.inboundBatchRows(firstBatch.batchKey)[0]).toMatchObject({
			state: "done",
			batch_state: "done",
		});

		port.complete(first.opRef, "late tail must not deliver twice");
		await Bun.sleep(20);
		expect(target.terminal).toEqual(["status-only terminal"]);
		await target.manager.tick(ORIGIN_KEY);
		await eventually(() => port.sends.length === 2, "pending grace-period row did not start its next batch");
		const second = required(port.sends[1], "slow-tail second send missing");
		port.complete(second.opRef, "next turn terminal");
		await eventually(
			() => target.database.inboundPendingCount(ORIGIN_KEY) === 0,
			"slow-tail sequence left a row orphaned",
		);
		assertCoverage(target, ["slow-tail-trigger", "arrived-during-grace"]);
	} finally {
		await target.close();
	}
});

test("red-team: stopping during terminal-status grace cancels the grace callback so nothing touches a closed database", async () => {
	const port = new ScriptedSessionPort();
	const target = await fixture({ port, settleWindowMs: 0 });
	let closed = false;
	try {
		enqueue(target, "shutdown-grace-trigger", "do not touch storage after stop");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "shutdown-grace turn did not start");
		const send = required(port.sends[0], "shutdown-grace send missing");
		port.seedOperation(send.opRef, send.sessionId, "terminal_ok", "status terminal before shutdown");
		await target.manager.tick(ORIGIN_KEY);
		expect(target.logs.some((line) => line.includes("reason=tail_terminal_evidence_unavailable"))).toBe(true);

		await target.close();
		closed = true;
		// The grace timer is owned by the actor and cancelled at stop(); a stale
		// callback must never reconcile against the closed database.
		await Bun.sleep(400);
		expect(target.logs.some((line) => line.startsWith("persona_reconcile_grace_failed"))).toBe(false);
		expect(target.logs.some((line) => line.includes("Cannot use a closed database"))).toBe(false);
	} finally {
		if (!closed) await target.close();
	}
});

test("red-team coverage fuzz: exact-boundary fragments plus steer-failure and broker-generation interleavings leave no orphaned ids", async () => {
	for (let seed = 0; seed < 12; seed++) {
		const base = Date.parse("2026-09-02T00:00:00.000Z") + seed * 10_000;
		let now = base;
		let generation = 1;
		const port = new IntermittentSteerPort(new Set([1 + (seed % 2), 3 + (seed % 3)]));
		const target = await fixture({
			port,
			settleWindowMs: 100,
			now: () => now,
			setTimeout: () => 0,
			clearTimeout: () => {},
			brokerGeneration: () => generation,
		});
		const ids = Array.from({ length: 8 }, (_, index) => `fuzz-${seed}-${index}`);
		try {
			for (const [index, offset] of [0, 25, 50, 75, 100].entries()) {
				now = base + offset;
				const id = required(ids[index], `seed ${seed} initial id ${index} missing`);
				enqueue(target, id, id, new Date(now).toISOString());
				await target.manager.notifyInbound(ORIGIN_KEY);
			}
			await target.manager.tick(ORIGIN_KEY);
			await eventually(() => port.sends.length === 1, `seed ${seed} did not fire at the first fixed cutoff`);

			for (let index = 5; index < ids.length; index++) {
				if ((seed + index) % 2 === 0) {
					generation++;
					await target.manager.onBrokerGeneration(generation);
				}
				now++;
				const id = required(ids[index], `seed ${seed} post-fire id ${index} missing`);
				enqueue(target, id, id, new Date(now).toISOString());
				await target.manager.notifyInbound(ORIGIN_KEY);
			}

			let completedSends = 0;
			while (target.database.inboundPendingCount(ORIGIN_KEY) > 0) {
				if (completedSends < port.sends.length) {
					const send = required(port.sends[completedSends], `seed ${seed} send ${completedSends} missing`);
					port.complete(send.opRef, `terminal-${seed}-${completedSends}`);
					completedSends++;
					await eventually(
						() =>
							target.manager.state(ORIGIN_KEY) !== "turn-running" ||
							target.database.inboundPendingCount(ORIGIN_KEY) === 0,
						`seed ${seed} did not reconcile terminal send ${completedSends}`,
					);
				}
				if (target.database.inboundPendingCount(ORIGIN_KEY) === 0) break;
				now += 100;
				await target.manager.tick(ORIGIN_KEY);
				await eventually(
					() => port.sends.length > completedSends,
					`seed ${seed} did not dispatch its remaining pending rows`,
				);
			}
			assertCoverage(target, ids);
		} finally {
			await target.close();
		}
	}
});

test("red-team non-goal probes: no running-turn abort or coexistence control remains, and inbound persistence has only ratified batch metadata", async () => {
	const source = (await Promise.all((await sourceFiles(SOURCE_ROOT)).map((path) => readFile(path, "utf8")))).join("\n");
	for (const forbidden of ["turn.abort", "turn.replace", "--resume", "keyed-queue"])
		expect(source).not.toContain(forbidden);
	for (const field of ["legacyTurn", "persistentTurn", "turnPath", "turnMode", "turnTransport"] as const) {
		const parsed = parseConfigFile({ schemaVersion: 1, [field]: true });
		expect(parsed).not.toHaveProperty(field);
	}

	const home = await mkdtemp(join(tmpdir(), "gajaeway-issue92-persistence-"));
	const path = join(home, "gateway.db");
	try {
		const database = await GatewayDatabase.open(path);
		database.close();
		const raw = new Database(path, { readonly: true });
		const columns = raw
			.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
			.all()
			.map((column) => column.name);
		raw.close();
		expect(columns.filter((column) => /transcript|receipt|pending_op_ref/i.test(column))).toEqual([]);
		expect(columns).toEqual(
			expect.arrayContaining([
				"batch_key",
				"batch_role",
				"batch_epoch",
				"batch_state",
				"attributed_op_ref",
				"accepted_at",
				"bound_session_id",
			]),
		);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("red-team evidence: compaction observation is wired from production monitor composition through the SessionPort", async () => {
	const [tailRunner, sessionPort, propagator, server] = await Promise.all(
		[
			"../src/orchestrator/tail-runner.ts",
			"../src/orchestrator/session-port.ts",
			"../src/monitors/propagate.ts",
			"../src/server/server.ts",
		].map((relativePath) => readFile(join(import.meta.dir, relativePath), "utf8")),
	);
	expect(tailRunner).toContain("recordCompactionReceipt");
	expect(sessionPort).toContain("runCompaction(input");
	expect(sessionPort).toContain("recordCompactionReceipt");
	expect(propagator).toContain("this.#compaction = options.compaction ?? unavailableCompactionPort");
	expect(propagator).not.toContain("this.#sessionPort.runCompaction");
	expect(server).toContain("new MonitorPropagator({");
	// AC7: the production composition root supplies the ONE compaction seam.
	expect(server).toContain("compaction: {");
	expect(server).toContain("sessionPort.runCompaction({");
});

test("red-team: an idle binding whose session died is resumed exactly once before the next send", async () => {
	const port = new IdleRecoveryPort({ onSend: (input, scripted) => scripted.complete(input.opRef, "reply") });
	const target = await fixture({ port, settleWindowMs: 0 });
	try {
		enqueue(target, "idle-1", "first turn");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "first turn did not send");
		const sessionId = required(port.sends[0], "first send missing").sessionId;
		await eventually(() => target.database.inboundPendingCount(ORIGIN_KEY) === 0, "first turn did not complete");
		// Broker restarted while idle: saved authority exists but the host is dead.
		port.setSessionState(sessionId, { live: false });
		enqueue(target, "idle-2", "second turn");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 2, "second turn did not send");
		expect(required(port.sends[1], "second send missing").sessionId).toBe(sessionId);
		// Contract: dead+saved => session.resume, then ONE send; never a send to a dead binding.
		expect(port.calls.filter((call) => call.startsWith("resume:"))).toEqual([`resume:${sessionId}`]);
		expect(port.calls.indexOf(`resume:${sessionId}`)).toBeLessThan(port.calls.lastIndexOf(`send:${sessionId}`));
	} finally {
		await target.close();
	}
});

test("red-team: an inspect outage on an idle binding never blocks the send and never fabricates a resume", async () => {
	const port = new InspectUnavailableIdleRecoveryPort({
		onSend: (input, scripted) => scripted.complete(input.opRef, "reply"),
	});
	const target = await fixture({ port, settleWindowMs: 0 });
	try {
		enqueue(target, "outage-1", "first turn");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "first turn did not send");
		await eventually(() => target.database.inboundPendingCount(ORIGIN_KEY) === 0, "first turn did not complete");
		enqueue(target, "outage-2", "second turn");
		await target.manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 2, "second turn was blocked by the inspect outage");
		expect(port.calls.filter((call) => call.startsWith("resume:"))).toEqual([]);
	} finally {
		await target.close();
	}
});
