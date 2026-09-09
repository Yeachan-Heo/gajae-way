import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type BrokerSession, GjcCliError, type StatusReport } from "@gajaeway/subsession";
import { PersonaSessionManager, personaTurnOpRef } from "../src/orchestrator/persona-session";
import { GatewayDatabase } from "../src/store/db";
import {
	attachTestBrokerOwnership,
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	ScriptedSessionPort,
} from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "restart" } as const;
const KEY = "loopback/loopback/restart";

let home = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;

class AuthorityShiftingPort extends ScriptedSessionPort {
	#shift = false;
	#inspects = 0;

	shiftOnRecovery(): void {
		this.#shift = true;
		this.#inspects = 0;
	}

	async inspect(input: { sessionId: string; repo: string }) {
		const session = await super.inspect(input);
		if (!this.#shift || ++this.#inspects !== 2 || !session) return session;
		return { ...session, pid: 99_999 };
	}
}

async function eventually(predicate: () => boolean, message: string, attempts = 200): Promise<void> {
	for (let attempt = 0; attempt < attempts; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function makeManager(port: ScriptedSessionPort, logs: string[], terminal: string[] = []): PersonaSessionManager {
	return new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "restart-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				terminal.push(text);
			},
		}),
		log: (line) => logs.push(line),
	});
}

function enqueue(messageId: string, body: string, receivedAt = new Date().toISOString()): void {
	expect(
		database?.inboundEnqueue({
			messageId,
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			receivedAt,
		}),
	).toBe(true);
}

async function startAccepted(port: ScriptedSessionPort, logs: string[]): Promise<{ opRef: string; sessionId: string }> {
	manager = makeManager(port, logs);
	enqueue("m-1", "restart me");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "initial turn did not reach an accepted send");
	const send = port.sends[0]!;
	const turn = database!.inboundNonterminalTurns(KEY)[0]!;
	expect(turn).toMatchObject({ state: "accepted", opRef: send.opRef, sessionId: send.sessionId });
	return { opRef: send.opRef, sessionId: send.sessionId };
}

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("restart observes an accepted nonterminal turn on the same live session without another send", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const accepted = await startAccepted(port, logs);
	await manager!.stop();
	const terminal: string[] = [];
	manager = makeManager(port, logs, terminal);

	await manager.recover();
	expect(port.sends).toHaveLength(1);
	expect(port.inspections.filter((entry) => entry.sessionId === accepted.sessionId)).toHaveLength(2);
	expect(database.inboundTurnRows(accepted.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });
	port.complete(accepted.opRef, "transcript survived restart");
	await eventually(
		() => database?.inboundTurnRows(accepted.opRef)[0]?.turn_state === "done",
		"observed turn did not reach terminal completion",
	);
	expect(terminal).toEqual(["transcript survived restart"]);
});

test("client_ref_conflict reconciles the same deterministic op-ref without a second accepted send, including after restart", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const repo = join(home, "workspace");
	const opRef = personaTurnOpRef("restart-test", KEY, 0, "m-conflict");
	port.seedAcceptedSend({ sessionId: "session-1", repo, text: "replay exactly once", opRef });
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "restart-test",
		repo,
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
		log: (line) => logs.push(line),
	});
	enqueue("m-conflict", "replay exactly once");
	await manager.notifyInbound(KEY);
	await eventually(
		() => logs.includes(`recovery_client_ref_conflict origin=${KEY} epoch=0 opRef=${opRef}`),
		"client_ref_conflict was not reconciled through status",
	);
	const turn = database.inboundNonterminalTurns(KEY)[0]!;
	expect(turn).toMatchObject({ opRef, state: "accepted", sessionId: "session-1" });
	await manager.stop();
	manager = makeManager(port, logs);
	await manager.recover();
	expect(port.sendAttempts.map((send) => send.opRef)).toEqual([opRef, opRef]);
	expect(port.sends).toHaveLength(1);
	port.complete(opRef, "conflict reconciled");
	await eventually(
		() => database?.inboundTurnRows(turn.opRef)[0]?.turn_state === "done",
		"conflict-reconciled turn did not complete",
	);
});

test("a bound turn whose operation is unknown stays held and is never resent", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const repo = join(home, "workspace");
	const binding = await port.bind({ originKey: KEY, epoch: 0, repo });
	expect(database.getSessionRecord(KEY)).toEqual({ sessionId: binding.sessionId, epoch: 0 });
	const opRef = personaTurnOpRef("restart-test", KEY, 0, "m-unknown");
	enqueue("m-unknown", "do not replay unknown");
	database.inboundBindTurn({
		messageId: "m-unknown",
		originKey: KEY,
		epoch: 0,
		opRef,
		sessionId: binding.sessionId,
	});
	manager = makeManager(port, logs);
	await manager.recover();
	expect(port.sendAttempts).toEqual([]);
	expect(database.inboundTurnRows(opRef)[0]).toMatchObject({
		turn_state: "bound",
		bound_session_id: binding.sessionId,
	});
	expect(logs.some((line) => line.startsWith(`recovery_hold origin=${KEY} epoch=0 opRef=${opRef}`))).toBe(true);
});

test("dead plus active is a sticky hold and never silently resumes or resends", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const accepted = await startAccepted(port, logs);
	await manager!.stop();
	port.setSessionState(accepted.sessionId, { live: false });
	manager = makeManager(port, logs);

	await manager.recover();
	expect(port.resumes).toEqual([]);
	expect(port.sends).toHaveLength(1);
	expect(database.inboundTurnRows(accepted.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(logs.some((line) => line.startsWith(`recovery_hold origin=${KEY} epoch=0 opRef=${accepted.opRef}`))).toBe(
		true,
	);
});

test("an inspect authority shift holds the turn rather than trusting a stale active operation", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new AuthorityShiftingPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const accepted = await startAccepted(port, logs);
	await manager!.stop();
	port.shiftOnRecovery();
	manager = makeManager(port, logs);

	await manager.recover();
	expect(port.sends).toHaveLength(1);
	expect(port.resumes).toEqual([]);
	expect(database.inboundTurnRows(accepted.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(logs.some((line) => line.includes("reason=broker authority is ambiguous"))).toBe(true);
});

test("dead terminal saved authority resumes, holds once, then reconciles from status when tail evidence is unavailable", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const accepted = await startAccepted(port, logs);
	port.seedOperation(accepted.opRef, accepted.sessionId, "terminal_ok", "saved transcript");
	await manager!.stop();
	port.setSessionState(accepted.sessionId, { live: false });
	const terminal: string[] = [];
	manager = makeManager(port, logs, terminal);

	await manager.recover();
	expect(port.resumes).toEqual([
		{ sessionId: accepted.sessionId, repo: join(home, "workspace"), originKey: KEY, epoch: 0 },
	]);
	// Exactly one send ever: the recovered terminal turn is reconciled, never resent.
	expect(port.sends).toHaveLength(1);
	// A verified terminal operation no longer waits for a live tail connection.
	await eventually(
		() => database?.inboundTurnRows(accepted.opRef)[0]?.turn_state === "done",
		"saved terminal turn did not reconcile from its original operation result",
	);
	expect(port.workerOutputReads.some((read) => read.opRef === accepted.opRef)).toBe(true);
	expect(
		logs.some((line) => line.includes("terminal_status_reconciled") && line.includes("tail_evidence=unavailable")),
	).toBe(true);
	expect(database.inboundTurnRows(accepted.opRef)[0]).toMatchObject({ state: "done", turn_state: "done" });
	expect(terminal).toEqual(["saved transcript"]);
});

test("resume-impossible remains an explicit hold without terminal tail evidence", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const accepted = await startAccepted(port, logs);
	port.seedOperation(accepted.opRef, accepted.sessionId, "terminal_ok", "unavailable session transcript");
	await manager!.stop();
	port.setSessionState(accepted.sessionId, { live: false });
	port.failResume(accepted.sessionId);
	manager = makeManager(port, logs);

	await manager.recover();
	expect(database.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: accepted.sessionId });
	expect(database.inboundTurnRows(accepted.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(logs.some((line) => line.includes("reason=tail_terminal_evidence_unavailable"))).toBe(true);
});

test("a retired hold reattaches after restart and does not block the new epoch", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const accepted = await startAccepted(port, logs);
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	await manager!.stop();
	manager = makeManager(port, logs);

	await manager.recover();
	expect(database.inboundNonterminalTurns(KEY)[0]).toMatchObject({
		epoch: 0,
		state: "accepted",
		sessionId: accepted.sessionId,
	});
	enqueue("m-new", "new epoch work");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 2, "retired hold blocked new-epoch send");
	const newSend = port.sends[1]!;
	port.complete(newSend.opRef, "new epoch reply");
	port.complete(accepted.opRef, "retired reply");
	await eventually(
		() => database?.inboundNonterminalTurns(KEY).length === 0,
		"retired turn did not reconcile after its reattached tail reached terminal",
	);
});

test("a recovered failed operation settles without replay and accepts genuinely new input", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const terminal: string[] = [];
	const accepted = await startAccepted(port, logs);
	await manager!.stop();
	port.seedOperation(accepted.opRef, accepted.sessionId, "failed", "interrupted");
	manager = makeManager(port, logs, terminal);

	await manager.recover();
	await eventually(() => database!.inboundNonterminalTurns(KEY).length === 0, "failed turn did not settle");
	expect(port.sends).toHaveLength(1);
	expect(database.inboundPendingCount(KEY)).toBe(0);
	expect(database.inboundTurnRow(accepted.opRef)?.turn_state).toBe("done");
	expect(terminal).toEqual([]);

	enqueue("new-input", "new work after the failed turn");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 2, "new user input did not dispatch");
	const next = port.sends[1]!;
	expect(next.opRef).not.toBe(accepted.opRef);
	expect(next.text).toBe("new work after the failed turn");
	port.complete(next.opRef, "new reply");
	await eventually(() => terminal.length === 1, "new input did not complete");
	expect(terminal).toEqual(["new reply"]);
	expect(port.sends.filter((send) => send.text === "restart me")).toHaveLength(1);
});

class UnreadableStorePort extends ScriptedSessionPort {
	// The runtime cannot answer for this session at all (e.g. a pre-cutover
	// store the current gjc no longer reads): inspect and status both throw.
	async inspect(_input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined> {
		throw new Error("session.list returned a malformed session row.");
	}
	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		if (this.sends.length === 0) throw new Error("SDK session is unavailable through the session Router.");
		return await super.status(input);
	}
}

test("a bound, unaccepted turn on a session the runtime cannot answer for is released and re-fired on a fresh session", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new UnreadableStorePort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	// Durable state left by the previous binary: a bound session record and a
	// bound (never accepted) turn pointing at a session the new runtime cannot read.
	await createOwnedSessionFixture(database, initializeTestBrokerAuthority(database, join(home, "canonical-agent")), {
		originKey: KEY,
		epoch: 0,
		repo: join(home, "workspace"),
		sessionId: "pre-cutover-session",
	});
	const opRef = personaTurnOpRef("restart-test", KEY, 0, "m-1");
	enqueue("m-1", "hello after cutover");
	database.inboundBindTurn({
		messageId: "m-1",
		originKey: KEY,
		epoch: 0,
		opRef,
		sessionId: "pre-cutover-session",
	});
	manager = makeManager(port, logs);

	await manager.recover();
	expect(logs.some((line) => line.startsWith(`recovery_requeue_unaccepted origin=${KEY}`))).toBe(true);
	await eventually(() => port.sends.length === 1, "released turn was not re-dispatched on a fresh session");
	expect(port.sends[0]!.text).toBe("hello after cutover");
	expect(port.sends[0]!.opRef).not.toBe(opRef);
	expect(port.sends[0]!.sessionId).not.toBe("pre-cutover-session");
});

class ColdBindFlakyPort extends ScriptedSessionPort {
	bindAttempts = 0;
	async bind(input: Parameters<ScriptedSessionPort["bind"]>[0]) {
		this.bindAttempts++;
		if (this.bindAttempts === 1) throw new Error("gjc sdk request failed: spawn_failed");
		return await super.bind(input);
	}
}

test("a dispatch whose bind initially fails is retried after a backoff without stranding the pending row", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ColdBindFlakyPort({
		onBind: (input) => `session-${input.epoch + 1}`,
		onSend: (input, scripted) => scripted.complete(input.opRef, "bound later"),
	});
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const terminal: string[] = [];
	manager = makeManager(port, logs, terminal);
	enqueue("m-1", "hello under load");
	await manager.notifyInbound(KEY);
	await eventually(
		() => logs.some((line) => line.startsWith(`persona_bind_failed origin=${KEY}`)),
		"bind failure was not recorded",
	);
	// The retry is a real DISPATCH_FAILURE_RETRY_MS (2s) backoff timer, not a
	// synchronous re-bind on the same admission.
	expect(port.sends).toEqual([]);
	await eventually(() => port.sends.length === 1, "turn was not dispatched after the bind retry", 1_000);
	expect(port.bindAttempts).toBe(2);
	await eventually(() => terminal.length === 1, "retried turn did not complete");
	expect(database.inboundPendingCount(KEY)).toBe(0);
	expect(database.inboundNonterminalTurns(KEY)).toEqual([]);
});

class DisownedButInspectablePort extends ScriptedSessionPort {
	// gjc >= 0.16.0 on a rebooted host: inspect still answers (live=false) but the
	// Router disowns the id on status/send.
	async inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined> {
		const s = await super.inspect(input);
		return s ? { ...s, live: false } : s;
	}
	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		if (this.sends.length === 0) throw new Error("gjc sdk request failed: session_unavailable");
		return await super.status(input);
	}
}

test("a bound turn whose id the Router disowns is released and re-fired even when inspect still answers", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new DisownedButInspectablePort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	// Bind once through the fake so "stale-session" is a known scripted session,
	// then leave its durable record pointing at it as a pre-reboot binding.
	const first = await port.bind({ originKey: KEY, epoch: 0, repo: join(home, "workspace") });
	const staleId = first.sessionId;
	expect(database.getSessionRecord(KEY)).toEqual({ sessionId: staleId, epoch: 0 });
	const opRef = personaTurnOpRef("restart-test", KEY, 0, "m-1");
	enqueue("m-1", "hello");
	database.inboundBindTurn({
		messageId: "m-1",
		originKey: KEY,
		epoch: 0,
		opRef,
		sessionId: staleId,
	});
	manager = makeManager(port, logs);
	await manager.recover();
	expect(logs.some((line) => line.startsWith(`recovery_requeue_unaccepted origin=${KEY}`))).toBe(true);
	await eventually(() => port.sends.length === 1, "disowned turn was not re-fired");
	// The origin was rebound (epoch bumped) and the release re-fired under a fresh ref.
	expect(database.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(port.sends[0]!.opRef).not.toBe(opRef);
});

class PoisonedBindEpochPort extends ScriptedSessionPort {
	bindAttempts = 0;
	readonly bindEpochs: number[] = [];

	constructor() {
		super({
			onBind: (input) => `session-${input.epoch}`,
			onSend: (input, scripted) => scripted.complete(input.opRef, "recovered after epoch rotation"),
		});
	}

	async bind(input: Parameters<ScriptedSessionPort["bind"]>[0]) {
		this.bindAttempts++;
		this.bindEpochs.push(input.epoch);
		if (this.bindAttempts === 1 || this.bindAttempts === 3)
			throw new Error("gjc sdk request failed: terminal_uncertain");
		if (this.bindAttempts === 2) throw new Error("gjc command timed out after 30000ms");
		return await super.bind(input);
	}
}

test("a poisoned terminal_uncertain bind epoch is rotated after the bounded burst; the pending row is sent once on the new epoch", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new PoisonedBindEpochPort();
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const terminal: string[] = [];
	const timers: Array<{ readonly work: () => void; readonly delayMs: number }> = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "restart-test",
		repo: join(home, "workspace"),
		setTimeout: (work, delayMs) => {
			const timer = { work, delayMs };
			timers.push(timer);
			return timer;
		},
		clearTimeout: () => {},
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				terminal.push(text);
			},
		}),
		log: (line) => logs.push(line),
	});
	enqueue("poisoned", "must survive the poisoned session-create key");
	await manager.notifyInbound(KEY);
	expect(port.bindEpochs).toEqual([0]);
	expect(timers.map((timer) => timer.delayMs)).toEqual([2_000]);

	// Attempt 2: ordinary timeout on the same epoch -> exponential backoff.
	timers[0]!.work();
	await eventually(() => port.bindAttempts === 2, "second bind did not run");
	expect(port.bindEpochs).toEqual([0, 0]);
	expect(timers.map((timer) => timer.delayMs)).toEqual([2_000, 4_000]);

	// Attempt 3: terminal_uncertain again. Nothing was ever sent, so the actor
	// advances epoch 0 -> 1, leaves the row pending, and returns to the base delay.
	timers[1]!.work();
	await eventually(() => port.bindAttempts === 3, "third bind did not run");
	expect(database.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database.inboundPendingOldest(KEY)).toMatchObject({ message_id: "poisoned", turn_state: null });
	expect(port.sends).toEqual([]);
	expect(timers.map((timer) => timer.delayMs)).toEqual([2_000, 4_000, 2_000]);
	expect(
		logs.some(
			(line) =>
				line.startsWith(`persona_bind_epoch_rotated origin=${KEY} epoch=0 nextEpoch=1`) &&
				line.includes("reason=terminal_uncertain"),
		),
	).toBe(true);

	// Attempt 4 uses the fresh epoch/idempotency key and succeeds exactly once.
	timers[2]!.work();
	await eventually(() => port.sends.length === 1, "fresh epoch did not dispatch");
	expect(port.bindEpochs).toEqual([0, 0, 0, 1]);
	expect(port.sends[0]!.text).toBe("must survive the poisoned session-create key");
	await eventually(() => terminal.length === 1, "recovered turn did not complete");
	expect(terminal).toEqual(["recovered after epoch rotation"]);
	expect(database.inboundPendingCount(KEY)).toBe(0);
});

class ModelSetFlakyPort extends ScriptedSessionPort {
	modelAttempts = 0;

	constructor() {
		super({
			onBind: (input) => `session-${input.epoch + 1}`,
			onSend: (input, scripted) => scripted.complete(input.opRef, "model recovered"),
		});
	}

	async setModel(input: Parameters<ScriptedSessionPort["setModel"]>[0]) {
		this.modelAttempts++;
		if (this.modelAttempts === 1) throw new Error('model "removed/model" not found');
		return await super.setModel(input);
	}
}

test("model.set failure happens before send: the row returns to pending and retries instead of becoming an ambiguous bound hold", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ModelSetFlakyPort();
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const terminal: string[] = [];
	const timers: Array<{ readonly work: () => void; readonly delayMs: number }> = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "restart-test",
		repo: join(home, "workspace"),
		setTimeout: (work, delayMs) => {
			const timer = { work, delayMs };
			timers.push(timer);
			return timer;
		},
		clearTimeout: () => {},
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			effectiveModel: "removed/model",
			onTerminal: ({ text }) => {
				terminal.push(text);
			},
		}),
		log: (line) => logs.push(line),
	});
	enqueue("model-failure", "do not strand me behind model.set");
	await manager.notifyInbound(KEY);
	expect(port.modelAttempts).toBe(1);
	expect(port.sendAttempts).toEqual([]);
	expect(database.inboundPendingOldest(KEY)).toMatchObject({ message_id: "model-failure", turn_state: null });
	expect(database.inboundNonterminalTurns(KEY)).toEqual([]);
	expect(timers.map((timer) => timer.delayMs)).toEqual([2_000]);
	expect(logs.some((line) => line.startsWith(`persona_model_failed origin=${KEY}`))).toBe(true);

	timers[0]!.work();
	await eventually(() => port.sends.length === 1, "model retry did not dispatch");
	expect(port.modelAttempts).toBe(2);
	expect(port.sends[0]!.text).toBe("do not strand me behind model.set");
	await eventually(() => terminal.length === 1, "model-recovered turn did not complete");
	expect(database.inboundPendingCount(KEY)).toBe(0);
});

class ModelSetSessionGonePort extends ScriptedSessionPort {
	modelAttempts = 0;

	constructor() {
		super({
			onBind: (input) => `session-${input.epoch}`,
			onSend: (input, scripted) => scripted.complete(input.opRef, "fresh session model recovered"),
		});
	}

	async setModel(input: Parameters<ScriptedSessionPort["setModel"]>[0]) {
		this.modelAttempts++;
		if (this.modelAttempts === 1)
			throw new GjcCliError("gjc sdk model.set reported failure: session_unavailable", 0, "", {
				code: "session_unavailable",
				message: `SDK session ${input.sessionId} is unavailable through the session Router.`,
			});
		return await super.setModel(input);
	}
}

test("model.set session_unavailable rotates the epoch immediately before retrying the still-pending row", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ModelSetSessionGonePort();
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const terminal: string[] = [];
	const timers: Array<{ readonly work: () => void; readonly delayMs: number }> = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "restart-test",
		repo: join(home, "workspace"),
		setTimeout: (work, delayMs) => {
			const timer = { work, delayMs };
			timers.push(timer);
			return timer;
		},
		clearTimeout: () => {},
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			effectiveModel: "openai/gpt-5.6-sol",
			onTerminal: ({ text }) => {
				terminal.push(text);
			},
		}),
		log: (line) => logs.push(line),
	});
	enqueue("model-session-gone", "retry me on a fresh session");
	await manager.notifyInbound(KEY);
	expect(port.binds.map((bind) => bind.epoch)).toEqual([0]);
	expect(port.sendAttempts).toEqual([]);
	expect(database.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database.inboundPendingOldest(KEY)).toMatchObject({ message_id: "model-session-gone", turn_state: null });
	expect(timers.map((timer) => timer.delayMs)).toEqual([2_000]);
	expect(
		logs.some(
			(line) =>
				line.startsWith(`persona_model_failed origin=${KEY} epoch=0 nextEpoch=1`) &&
				line.includes("session_unavailable"),
		),
	).toBe(true);

	timers[0]!.work();
	await eventually(() => port.sends.length === 1, "fresh session did not receive the prompt");
	expect(port.binds.map((bind) => bind.epoch)).toEqual([0, 1]);
	expect(port.sends[0]!.text).toBe("retry me on a fresh session");
	await eventually(() => terminal.length === 1, "fresh session turn did not complete");
	expect(database.inboundPendingCount(KEY)).toBe(0);
});
