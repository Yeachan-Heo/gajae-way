import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type BrokerSession, GjcCliError, type StatusReport } from "@gajae-gateway/subsession";
import { PersonaSessionManager, personaTurnOpRef } from "../src/orchestrator/persona-session";
import { Store } from "../src/store";
import {
	attachTestBrokerOwnership,
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	ScriptedSessionPort,
} from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "restart" } as const;
const KEY = "loopback/loopback/restart";

let home = "";
let database: Store | undefined;
let manager: PersonaSessionManager | undefined;

const fixtureBindings = new WeakMap<ScriptedSessionPort, Pick<ScriptedSessionPort, "bind" | "resume">>();

function registerFixtureBindings(port: ScriptedSessionPort): void {
	const original = fixtureBindings.get(port) ?? { bind: port.bind.bind(port), resume: port.resume.bind(port) };
	fixtureBindings.set(port, original);
	port.bind = original.bind;
	port.resume = original.resume;
	attachTestBrokerOwnership(database!, port, join(home, "canonical-agent"));
}

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

function makeManager(
	port: ScriptedSessionPort,
	logs: string[],
	terminal: string[] = [],
	failures: string[] = [],
): PersonaSessionManager {
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
			onFailure: ({ error }) => {
				failures.push(error.message);
			},
		}),
		log: (line) => logs.push(line),
	});
}

function enqueue(messageId: string, body: string, receivedAt = new Date().toISOString()): void {
	enqueueOrigin(KEY, JSON.stringify(ORIGIN), messageId, body, receivedAt);
}

function enqueueOrigin(
	originKey: string,
	originRefJson: string,
	messageId: string,
	body: string,
	receivedAt = new Date().toISOString(),
): void {
	expect(
		database?.transaction((tx) =>
			database?.inbox.enqueue(tx, {
				messageId,
				originKey,
				originRefJson,
				body,
				receivedAt,
			}),
		),
	).toBe(true);
}

async function startAccepted(port: ScriptedSessionPort, logs: string[]): Promise<{ opRef: string; sessionId: string }> {
	manager = makeManager(port, logs);
	enqueue("m-1", "restart me");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "initial turn did not reach an accepted send");
	const send = port.sends[0]!;
	const turn = database!.inbox.nonterminalTurns(KEY)[0]!;
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
	database = await Store.open(join(home, "gateway.db"));
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
	expect(database.inbox.turnRows(accepted.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });
	port.complete(accepted.opRef, "transcript survived restart");
	await eventually(
		() => database?.inbox.turnRows(accepted.opRef)[0]?.turn_state === "done",
		"observed turn did not reach terminal completion",
	);
	expect(terminal).toEqual(["transcript survived restart"]);
});

test("bounded shutdown keeps an accepted turn alive past the old cutoff and defers new input until restart", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const terminal: string[] = [];
	manager = makeManager(port, logs, terminal);
	enqueue("m-drain", "finish before gateway stops");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "initial turn did not reach an accepted send");
	const first = port.sends[0];
	if (!first) throw new Error("accepted send disappeared during shutdown test");

	let drainFinished = false;
	manager.beginDrain();
	const draining = manager.drain().then(() => {
		drainFinished = true;
	});
	enqueue("m-during-drain", "dispatch after restart");
	await manager.notifyInbound(KEY);

	// Exceed the old five-second shutdown window while the accepted operation is still running.
	await Bun.sleep(5_100);
	expect(drainFinished).toBe(false);
	expect(port.sends).toHaveLength(1);
	expect(database.inbox.pendingOldest(KEY)).toMatchObject({
		message_id: "m-during-drain",
		turn_role: null,
		turn_state: null,
	});
	port.complete(first.opRef, "answer completed during shutdown");
	await draining;
	await eventually(
		() => database?.inbox.turnRow(first.opRef)?.turn_state === "done",
		"shutdown drain did not settle the accepted turn",
	);
	expect(terminal).toEqual(["answer completed during shutdown"]);

	await manager.stop();
	manager = makeManager(port, logs, terminal);
	await manager.recover();
	await eventually(() => port.sends.length === 2, "queued input was not resumed after restart");
	const second = port.sends[1];
	if (!second) throw new Error("queued send disappeared during restart test");
	expect(second.text).toBe("dispatch after restart");
	port.complete(second.opRef, "queued answer");
	await eventually(() => terminal.length === 2, "queued input did not finish after restart");
	expect(terminal).toEqual(["answer completed during shutdown", "queued answer"]);
	expect(port.sends.filter((send) => send.opRef === first.opRef)).toHaveLength(1);
}, 15_000);

test("restart drain waits for all active turns and holds new persona dispatches", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-drain-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: ({ originKey, epoch }) => `${originKey}-${epoch}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const terminal: string[] = [];
	manager = makeManager(port, logs, terminal);
	const otherOrigin = { ...ORIGIN, conversationId: "other" };
	const otherKey = "loopback/loopback/other";
	const queuedOrigin = { ...ORIGIN, conversationId: "queued" };
	const queuedKey = "loopback/loopback/queued";

	enqueue("active-1", "first active turn");
	enqueueOrigin(otherKey, JSON.stringify(otherOrigin), "active-2", "second active turn");
	await Promise.all([manager.notifyInbound(KEY), manager.notifyInbound(otherKey)]);
	await eventually(() => port.sends.length === 2, "both active turns did not start");
	const [firstSend, secondSend] = port.sends;
	if (!firstSend || !secondSend) throw new Error("both active sends were not recorded");

	const draining = manager.drainForRestart("restart-602");
	enqueueOrigin(queuedKey, JSON.stringify(queuedOrigin), "queued-1", "held until restart");
	await manager.notifyInbound(queuedKey);
	await Bun.sleep(20);
	expect(port.sends).toHaveLength(2);
	expect(await Promise.race([draining.then(() => true), Bun.sleep(10).then(() => false)])).toBe(false);

	port.complete(firstSend.opRef, "first delivered");
	await eventually(() => database?.inbox.turnRow(firstSend.opRef)?.turn_state === "done", "first turn did not deliver");
	expect(terminal).toContain("first delivered");
	expect(await Promise.race([draining.then(() => true), Bun.sleep(10).then(() => false)])).toBe(false);

	port.complete(secondSend.opRef, "second delivered");
	await expect(draining).resolves.toBe("drained");
	await eventually(() => terminal.length === 2, "restart drain completed before all answers were delivered");
	expect(terminal).toEqual(expect.arrayContaining(["first delivered", "second delivered"]));
	expect(port.sends).toHaveLength(2);
	expect(database.inbox.pendingOldest(queuedKey)?.message_id).toBe("queued-1");

	await expect(manager.cancelRestartDrain("restart-602")).resolves.toBe(true);
	await eventually(() => port.sends.length === 3, "cancelled restart drain did not resume pending dispatch");
});

test("restart drain reaches its deadline and keeps new dispatches held for durable recovery", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-drain-timeout-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: ({ originKey, epoch }) => `${originKey}-${epoch}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const currentManager = makeManager(port, []);
	manager = currentManager;
	enqueue("active-timeout", "provider did not finish");
	await currentManager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before drain timeout");

	await expect(currentManager.drainForRestart("restart-timeout", 20)).resolves.toBe("timed_out");
	expect(currentManager.restartDrainActive).toBe(true);
	enqueue("queued-after-timeout", "resume after restart");
	await currentManager.notifyInbound(KEY);
	expect(port.sends).toHaveLength(1);
	expect(database.inbox.pendingOldest(KEY)?.message_id).toBe("queued-after-timeout");
});

test("a transferred restart drain rejects assertions from the prior supervisor", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-transfer-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: ({ originKey, epoch }) => `${originKey}-${epoch}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	manager = makeManager(port, []);
	await expect(manager.drainForRestart("stack-supervisor")).resolves.toBe("drained");
	expect(manager.transferRestartDrain("stack-supervisor", "owner-restart")).toBe(true);
	await expect(manager.drainForRestart("stack-supervisor", 0)).rejects.toThrow(
		"restart drain already owned by owner-restart",
	);
	expect(manager.restartDrainId).toBe("owner-restart");
	await expect(manager.drainForRestart("owner-restart", 0)).resolves.toBe("drained");
	await expect(manager.cancelRestartDrain("owner-restart")).resolves.toBe(true);
});

test("client_ref_conflict reconciles the same deterministic op-ref without a second accepted send, including after restart", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
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
	const turn = database.inbox.nonterminalTurns(KEY)[0]!;
	expect(turn).toMatchObject({ opRef, state: "accepted", sessionId: "session-1" });
	await manager.stop();
	manager = makeManager(port, logs);
	await manager.recover();
	expect(port.sendAttempts.map((send) => send.opRef)).toEqual([opRef, opRef]);
	expect(port.sends).toHaveLength(1);
	port.complete(opRef, "conflict reconciled");
	await eventually(
		() => database?.inbox.turnRows(turn.opRef)[0]?.turn_state === "done",
		"conflict-reconciled turn did not complete",
	);
});

test("a bound turn whose operation is unknown stays held and is never resent", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const repo = join(home, "workspace");
	const binding = await port.bind({ originKey: KEY, epoch: 0, repo });
	expect(database.bindings.getSessionRecord(KEY)).toEqual({ sessionId: binding.sessionId, epoch: 0 });
	const opRef = personaTurnOpRef("restart-test", KEY, 0, "m-unknown");
	enqueue("m-unknown", "do not replay unknown");
	database!.transaction((tx) =>
		database!.inbox.bindTurn(tx, {
			messageId: "m-unknown",
			originKey: KEY,
			epoch: 0,
			opRef,
			sessionId: binding.sessionId,
		}),
	);
	// The prompt was handed to the port: its outcome is what recovery must prove.
	database!.transaction((tx) => database!.inbox.markSendAttempted(tx, opRef));
	manager = makeManager(port, logs);
	await manager.recover();
	expect(port.sendAttempts).toEqual([]);
	expect(database.inbox.turnRows(opRef)[0]).toMatchObject({
		turn_state: "bound",
		bound_session_id: binding.sessionId,
	});
	expect(logs.some((line) => line.startsWith(`recovery_hold origin=${KEY} epoch=0 opRef=${opRef}`))).toBe(true);
});

test("dead plus active is a sticky hold and never silently resumes or resends", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
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
	expect(database.inbox.turnRows(accepted.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(logs.some((line) => line.startsWith(`recovery_hold origin=${KEY} epoch=0 opRef=${accepted.opRef}`))).toBe(
		true,
	);
});

test("an inspect authority shift holds the turn rather than trusting a stale active operation", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
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
	expect(database.inbox.turnRows(accepted.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(logs.some((line) => line.includes("reason=broker authority is ambiguous"))).toBe(true);
});

test("dead terminal saved authority resumes, holds once, then reconciles from status when tail evidence is unavailable", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
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
		() => database?.inbox.turnRows(accepted.opRef)[0]?.turn_state === "done",
		"saved terminal turn did not reconcile from its original operation result",
	);
	expect(port.workerOutputReads.some((read) => read.opRef === accepted.opRef)).toBe(true);
	expect(
		logs.some((line) => line.includes("terminal_status_reconciled") && line.includes("tail_evidence=unavailable")),
	).toBe(true);
	expect(database.inbox.turnRows(accepted.opRef)[0]).toMatchObject({ state: "done", turn_state: "done" });
	expect(terminal).toEqual(["saved transcript"]);
});

test("resume-impossible remains an explicit hold without terminal tail evidence", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
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
	expect(database.bindings.getSessionRecord(KEY)).toMatchObject({ epoch: 0, sessionId: accepted.sessionId });
	expect(database.inbox.turnRows(accepted.opRef)[0]).toMatchObject({ state: "pending", turn_state: "accepted" });
	expect(logs.some((line) => line.includes("reason=tail_terminal_evidence_unavailable"))).toBe(true);
});

test("a retired hold reattaches after restart and does not block the new epoch", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const accepted = await startAccepted(port, logs);
	await manager!.reset(KEY, JSON.stringify(ORIGIN));
	await manager!.stop();
	manager = makeManager(port, logs);

	await manager.recover();
	expect(database.inbox.nonterminalTurns(KEY)[0]).toMatchObject({
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
		() => database?.inbox.nonterminalTurns(KEY).length === 0,
		"retired turn did not reconcile after its reattached tail reached terminal",
	);
});

test("a recovered failed operation settles without replay and accepts genuinely new input", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const terminal: string[] = [];
	const accepted = await startAccepted(port, logs);
	await manager!.stop();
	port.seedOperation(accepted.opRef, accepted.sessionId, "failed", "interrupted");
	manager = makeManager(port, logs, terminal);

	await manager.recover();
	await eventually(() => database!.inbox.nonterminalTurns(KEY).length === 0, "failed turn did not settle");
	expect(port.sends).toHaveLength(1);
	expect(database.inbox.pendingCount(KEY)).toBe(0);
	expect(database.inbox.turnRow(accepted.opRef)?.turn_state).toBe("done");
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
	unavailable = true;
	async inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined> {
		if (this.unavailable) throw new Error("session.list returned a malformed session row.");
		return await super.inspect(input);
	}
	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		if (this.unavailable) throw new Error("SDK session is unavailable through the session Router.");
		return await super.status(input);
	}
}

test("a bound turn on an unreadable session retains its original identity across Store reopen until exact terminal recovery", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new UnreadableStorePort({ onBind: (input) => `session-${input.epoch + 1}` });
	registerFixtureBindings(port);
	const logs: string[] = [];
	// A bound row does not prove that the native producer never accepted it.
	await createOwnedSessionFixture(database, initializeTestBrokerAuthority(database, join(home, "canonical-agent")), {
		originKey: KEY,
		epoch: 0,
		repo: join(home, "workspace"),
		sessionId: "pre-cutover-session",
	});
	const opRef = personaTurnOpRef("restart-test", KEY, 0, "m-1");
	enqueue("m-1", "hello after cutover");
	database!.transaction((tx) =>
		database!.inbox.bindTurn(tx, {
			messageId: "m-1",
			originKey: KEY,
			epoch: 0,
			opRef,
			sessionId: "pre-cutover-session",
		}),
	);
	// The prompt was handed to the port: its outcome is what recovery must prove.
	database!.transaction((tx) => database!.inbox.markSendAttempted(tx, opRef));
	port.setSessionState("pre-cutover-session", { repo: join(home, "workspace"), live: true });
	port.seedAcceptedSend({
		sessionId: "pre-cutover-session",
		repo: join(home, "workspace"),
		opRef,
		text: "hello after cutover",
	});
	const terminal: string[] = [];
	const failures: string[] = [];
	manager = makeManager(port, logs, terminal, failures);
	await manager.recover();
	await manager.stop();
	database.close();
	database = await Store.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = makeManager(port, logs, terminal, failures);
	for (let sweep = 0; sweep < 3; sweep++) {
		await manager.recover();
		await manager.tick(KEY);
	}
	expect(database.inbox.turnRow(opRef)).toMatchObject({
		state: "pending",
		turn_state: "bound",
		turn_op_ref: opRef,
		bound_session_id: "pre-cutover-session",
		turn_epoch: 0,
		terminal_delivery_id: null,
	});
	expect(database.bindings.getSessionRecord(KEY)).toEqual({ sessionId: "pre-cutover-session", epoch: 0 });
	expect(port.sendAttempts.map((send) => send.opRef)).toEqual([opRef]);
	expect(port.binds).toHaveLength(0);
	expect(port.resumes).toEqual([]);
	expect(port.closes).toEqual([]);
	expect(terminal).toEqual([]);
	expect(failures).toEqual([]);
	port.unavailable = false;
	port.complete(opRef, "original unreadable-session answer");
	await manager.recover();
	await manager.tick(KEY);
	await eventually(
		() => database!.inbox.turnRow(opRef)?.turn_state === "done",
		"original unreadable turn did not settle",
	);
	await manager.recover();
	await manager.tick(KEY);
	expect(terminal).toEqual(["original unreadable-session answer"]);
	expect(failures).toEqual([]);
	expect(port.workerOutputReads.map((read) => [read.sessionId, read.opRef])).toEqual([["pre-cutover-session", opRef]]);
	expect(port.sends).toHaveLength(1);
	expect(port.sendAttempts.map((send) => send.opRef)).toEqual([opRef]);
	expect(database.bindings.getSessionRecord(KEY)).toEqual({ sessionId: "pre-cutover-session", epoch: 0 });
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
	database = await Store.open(join(home, "gateway.db"));
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
	expect(database.inbox.pendingCount(KEY)).toBe(0);
	expect(database.inbox.nonterminalTurns(KEY)).toEqual([]);
});

class DisownedButInspectablePort extends ScriptedSessionPort {
	// gjc >= 0.16.0 on a rebooted host: inspect still answers (live=false) but the
	// Router disowns the id on status/send.
	unavailable = true;
	async inspect(input: { sessionId: string; repo: string }): Promise<BrokerSession | undefined> {
		const s = await super.inspect(input);
		return s && this.unavailable ? { ...s, live: false } : s;
	}
	async status(input: { sessionId: string; repo: string; opRef: string }): Promise<StatusReport> {
		if (this.unavailable) throw new Error("gjc sdk request failed: session_unavailable");
		return await super.status(input);
	}
}

test("a bound turn whose id the Router disowns retains its original identity across Store reopen until exact terminal recovery", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-restart-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new DisownedButInspectablePort({ onBind: (input) => `session-${input.epoch + 1}` });
	registerFixtureBindings(port);
	const logs: string[] = [];
	// Bind once through the fake so "stale-session" is a known scripted session,
	// then leave its durable record pointing at it as a pre-reboot binding.
	const first = await port.bind({ originKey: KEY, epoch: 0, repo: join(home, "workspace") });
	const staleId = first.sessionId;
	expect(database.bindings.getSessionRecord(KEY)).toEqual({ sessionId: staleId, epoch: 0 });
	const opRef = personaTurnOpRef("restart-test", KEY, 0, "m-1");
	enqueue("m-1", "hello");
	database!.transaction((tx) =>
		database!.inbox.bindTurn(tx, {
			messageId: "m-1",
			originKey: KEY,
			epoch: 0,
			opRef,
			sessionId: staleId,
		}),
	);
	// The prompt was handed to the port: its outcome is what recovery must prove.
	database!.transaction((tx) => database!.inbox.markSendAttempted(tx, opRef));
	// Lost Router ownership is not native producer-exit evidence.
	port.seedAcceptedSend({ sessionId: staleId, repo: join(home, "workspace"), opRef, text: "hello" });
	port.failResume(staleId, "session_unavailable");
	const terminal: string[] = [];
	const failures: string[] = [];
	manager = makeManager(port, logs, terminal, failures);
	await manager.recover();
	await manager.stop();
	database.close();
	database = await Store.open(join(home, "gateway.db"));
	registerFixtureBindings(port);
	manager = makeManager(port, logs, terminal, failures);
	for (let sweep = 0; sweep < 3; sweep++) {
		await manager.recover();
		await manager.tick(KEY);
	}
	expect(database.inbox.turnRow(opRef)).toMatchObject({
		state: "pending",
		turn_state: "bound",
		turn_op_ref: opRef,
		bound_session_id: staleId,
		turn_epoch: 0,
		terminal_delivery_id: null,
	});
	expect(database.bindings.getSessionRecord(KEY)).toEqual({ sessionId: staleId, epoch: 0 });
	expect(port.sendAttempts.map((send) => send.opRef)).toEqual([opRef]);
	expect(port.binds).toHaveLength(1);
	expect(port.resumes).toEqual([]);
	expect(port.closes).toEqual([]);
	expect(terminal).toEqual([]);
	expect(failures).toEqual([]);
	port.unavailable = false;
	port.complete(opRef, "original disowned-session answer");
	await manager.recover();
	await manager.tick(KEY);
	await eventually(
		() => database!.inbox.turnRow(opRef)?.turn_state === "done",
		"original disowned turn did not settle",
	);
	await manager.recover();
	await manager.tick(KEY);
	expect(terminal).toEqual(["original disowned-session answer"]);
	expect(failures).toEqual([]);
	expect(port.workerOutputReads.map((read) => [read.sessionId, read.opRef])).toEqual([[staleId, opRef]]);
	expect(port.sends).toHaveLength(1);
	expect(port.sendAttempts.map((send) => send.opRef)).toEqual([opRef]);
	expect(database.bindings.getSessionRecord(KEY)).toEqual({ sessionId: staleId, epoch: 0 });
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
	database = await Store.open(join(home, "gateway.db"));
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
	expect(database.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database.inbox.pendingOldest(KEY)).toMatchObject({ message_id: "poisoned", turn_state: null });
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
	expect(database.inbox.pendingCount(KEY)).toBe(0);
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
	database = await Store.open(join(home, "gateway.db"));
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
	expect(database.inbox.pendingOldest(KEY)).toMatchObject({ message_id: "model-failure", turn_state: null });
	expect(database.inbox.nonterminalTurns(KEY)).toEqual([]);
	expect(timers.map((timer) => timer.delayMs)).toEqual([2_000]);
	expect(logs.some((line) => line.startsWith(`persona_model_failed origin=${KEY}`))).toBe(true);

	timers[0]!.work();
	await eventually(() => port.sends.length === 1, "model retry did not dispatch");
	expect(port.modelAttempts).toBe(2);
	expect(port.sends[0]!.text).toBe("do not strand me behind model.set");
	await eventually(() => terminal.length === 1, "model-recovered turn did not complete");
	expect(database.inbox.pendingCount(KEY)).toBe(0);
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
	database = await Store.open(join(home, "gateway.db"));
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
	expect(database.bindings.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database.inbox.pendingOldest(KEY)).toMatchObject({ message_id: "model-session-gone", turn_state: null });
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
	expect(database.inbox.pendingCount(KEY)).toBe(0);
});

/**
 * #685 crash injection: the gateway dies while a bound trigger waits at a
 * pre-send await. A second Store handle on the same file plays the restarted
 * process; the dead actor's promise never settles.
 */
async function crashAt(stage: "start_turn" | "attach" | "send"): Promise<{
	port: ScriptedSessionPort;
	opRef: string;
	restart: (terminal: string[], logs: string[]) => Promise<void>;
}> {
	home = await mkdtemp(join(tmpdir(), `gajaeway-crash-${stage}-`));
	const path = join(home, "gateway.db");
	const crashed = await Store.open(path);
	database = crashed;
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(crashed, port, join(home, "canonical-agent"));
	let reached = false;
	const never = () => {
		reached = true;
		return new Promise<never>(() => undefined);
	};
	const attachTail = port.attachTail.bind(port);
	const send = port.send.bind(port);
	let hang = true;
	if (stage === "attach") port.attachTail = async (input) => (hang ? never() : attachTail(input));
	if (stage === "send")
		port.send = async (input) => {
			if (!hang) return await send(input);
			port.sendAttempts.push(input);
			return never();
		};
	const dead = new PersonaSessionManager({
		database: crashed,
		port,
		instanceId: "restart-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => (stage === "start_turn" ? never() : { text: trigger.body }),
		log: () => undefined,
	});
	enqueue("m-crash", "answer me once");
	void dead.notifyInbound(KEY).catch(() => undefined);
	await eventually(() => reached, `${stage} was not reached`);
	const opRef = crashed.inbox.nonterminalTurns(KEY)[0]!.opRef;
	expect(crashed.inbox.turnRow(opRef)?.turn_state).toBe("bound");
	return {
		port,
		opRef,
		restart: async (terminal, logs) => {
			hang = false;
			database = await Store.open(path);
			registerFixtureBindings(port);
			manager = makeManager(port, logs, terminal);
			await manager.recover();
		},
	};
}

for (const stage of ["start_turn", "attach"] as const)
	test(`#685: a crash at ${stage} before any send is answered exactly once after restart, without operator action`, async () => {
		const { port, opRef, restart } = await crashAt(stage);
		const terminal: string[] = [];
		const logs: string[] = [];
		await restart(terminal, logs);
		expect(logs.some((line) => line.startsWith("recovery_requeue") && line.includes("reason=never_sent"))).toBe(true);
		await eventually(() => port.sends.length === 1, "requeued trigger was not sent");
		expect(port.sends[0]).toMatchObject({ text: "answer me once" });
		expect(port.sends[0]!.opRef).not.toBe(opRef);
		port.complete(port.sends[0]!.opRef, "answered once");
		await eventually(() => terminal.length === 1, "requeued trigger was not answered");
		expect(terminal).toEqual(["answered once"]);
		expect(port.sends).toHaveLength(1);
	});

test("#685: a crash after the send-attempt marker is held for reconcile and never resent", async () => {
	const { port, opRef, restart } = await crashAt("send");
	expect(database!.inbox.turnRow(opRef)?.send_attempted_at).not.toBeNull();
	port.status = async (input) => ({
		operationRef: input.opRef,
		status: { status: "unknown" },
		summaryCompleted: false,
	});
	const terminal: string[] = [];
	const logs: string[] = [];
	await restart(terminal, logs);
	await manager!.tick(KEY);
	expect(logs.some((line) => line.includes("reason=never_sent"))).toBe(false);
	expect(logs.some((line) => line.startsWith("recovery_hold") && line.includes(`opRef=${opRef}`))).toBe(true);
	expect(port.sends).toHaveLength(0);
	expect(port.sendAttempts).toHaveLength(1);
	expect(database!.inbox.turnRow(opRef)).toMatchObject({ turn_state: "bound", turn_op_ref: opRef });
	expect(terminal).toEqual([]);
});
