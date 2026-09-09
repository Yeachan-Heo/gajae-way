import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajaeway/subsession";
import { PersonaSessionManager, personaTurnOpRef } from "../src/orchestrator/persona-session";
import type { TailAttachInput } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort, steerRefused } from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;
let latestOpRef = "";

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
	latestOpRef = "";
});

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "persona" } as const;
const KEY = "loopback/loopback/persona";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

function enqueue(messageId: string, body: string): void {
	const accepted = database?.inboundEnqueue({
		messageId,
		originKey: KEY,
		originRefJson: JSON.stringify(ORIGIN),
		body,
	});
	expect(accepted).toBe(true);
}

/** Only successful scripted creation grants ownership; unknown seeded ids stay unowned. */
function registerFixtureBindings(port: ScriptedSessionPort): void {
	const fixtureDatabase = database!;
	const canonicalAgentDir = join(home, "agent");
	const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
	const bind = port.bind.bind(port);
	port.bind = async (input) => {
		const binding = await bind(input);
		expect(fixtureDatabase.recordOwnedBinding({ ...binding, authority })).toBe(true);
		return binding;
	};
}

async function harness(
	port: ScriptedSessionPort,
	hooks: {
		terminal?: (text: string) => void;
		retired?: () => void;
		released?: (opRef: string) => void;
		failure?: (message: string) => void;
	} = {},
	log?: (line: string) => void,
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const canonicalAgentDir = join(home, "agent");
	database.assertBrokerAuthority(
		{ canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` },
		{ initializeEmpty: true },
	);
	registerFixtureBindings(port);
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		...(log ? { log } : {}),
		onTurnStart: ({ trigger, turn }) => {
			latestOpRef = turn.opRef;
			return {
				text: trigger.body,
				onTerminal: ({ text }) => hooks.terminal?.(text),
				onFailure: ({ error }) => hooks.failure?.(error.message),
				onRetired: () => hooks.retired?.(),
				onReleased: ({ turn: released }) => hooks.released?.(released.opRef),
			};
		},
	});
}

test("actor immediately dispatches durable inbound with one deterministic caller op-ref, then completes on tail terminal", async () => {
	const port = new ScriptedSessionPort({
		onSend: (input, scripted) => scripted.complete(input.opRef, "persona reply"),
	});
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) });
	enqueue("m-1", "hello");
	await manager?.notifyInbound(KEY);
	await eventually(() => terminal.length === 1, "tail terminal did not reach lifecycle");

	expect(port.sends).toHaveLength(1);
	const send = port.sends[0]!;
	expect(send.text).toBe("hello");
	expect(send.opRef).toBe(latestOpRef);
	expect(send.opRef).toMatch(/^gw-p-[0-9a-f]{32}$/);
	expect(terminal).toEqual(["persona reply"]);
	expect(database?.inboundPendingCount(KEY)).toBe(0);
	expect(database?.inboundTurnRow(latestOpRef)).toMatchObject({ state: "done", turn_state: "done" });
});

test("a message admitted while a persistent turn is running becomes an operator-gated steer", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "first");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "initial persistent send did not start");
	expect(port.sends).toHaveLength(1);
	enqueue("m-2", "correction");
	await manager?.notifyInbound(KEY);

	expect(port.steers).toHaveLength(1);
	expect(port.steers[0]).toMatchObject({ sessionId: port.sends[0]!.sessionId });
	expect(port.steers[0]!.text.endsWith("\ncorrection")).toBe(true);
	expect(database?.inboundTurnRows(latestOpRef)).toEqual(
		expect.arrayContaining([
			expect.objectContaining({
				message_id: "m-2",
				turn_role: "steer",
				turn_state: "done",
				turn_op_ref: latestOpRef,
			}),
		]),
	);
	port.complete(port.sends[0]!.opRef, "done");
	await eventually(
		() => database?.inboundTurnRow(latestOpRef)?.turn_state === "done",
		"accepted turn did not reconcile terminal",
	);
});

/**
 * Live 2026-09-07 (playground-ko on three hosts at once): native compaction
 * did not keep the window bounded, the provider rejected every prompt with
 * `prompt is too long: 1042682 tokens > 1000000`, and the gateway delivered
 * `[turn failed] Prompt submission failed.` for each message until an operator
 * sent `/new`. A failed turn on an exhausted session is rotated instead.
 */
test("a failed turn on a session at the context ceiling rotates the epoch and re-dispatches on a fresh session", async () => {
	const port = new ScriptedSessionPort({ onBind: (input) => `session-e${input.epoch}` });
	const terminal: string[] = [];
	const failures: string[] = [];
	const released: string[] = [];
	const logs: string[] = [];
	await harness(
		port,
		{
			terminal: (text) => terminal.push(text),
			failure: (message) => failures.push(message),
			released: (opRef) => released.push(opRef),
		},
		(line) => logs.push(line),
	);
	enqueue("m-1", "hello at the ceiling");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "first turn did not start");
	const first = port.sends[0]!;
	expect(first.sessionId).toBe("session-e0");
	port.contextPercent.set("session-e0", 102.7);
	port.fail(first.opRef, "Prompt submission failed.");

	await eventually(() => port.sends.length === 2, "the trigger was not re-dispatched on a fresh session");
	const second = port.sends[1]!;
	expect(second.text).toBe("hello at the ceiling");
	expect(second.sessionId).toBe("session-e1");
	expect(second.opRef).not.toBe(first.opRef);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(released).toEqual([first.opRef]);
	// The user never saw a failure notice: the rotation is the remedy.
	expect(failures).toEqual([]);
	expect(
		logs.some((line) =>
			line.startsWith(`session_rotated_context_exhausted origin=${KEY} epoch=0 nextEpoch=1 opRef=${first.opRef}`),
		),
	).toBe(true);

	port.complete(second.opRef, "answered on the fresh session");
	await eventually(() => terminal.length === 1, "replacement turn did not complete");
	expect(terminal).toEqual(["answered on the fresh session"]);
	expect(database?.inboundPendingCount(KEY)).toBe(0);
});

test("a failed turn on a session with room left is reported as a failure, not rotated", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];
	await harness(port, { failure: (message) => failures.push(message) });
	enqueue("m-1", "ordinary failure");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const first = port.sends[0]!;
	port.contextPercent.set(first.sessionId, 41);
	port.fail(first.opRef, "provider hiccup");
	await eventually(() => failures.length === 1, "failure did not reach the lifecycle");
	expect(failures).toEqual(["provider hiccup"]);
	expect(port.sends).toHaveLength(1);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(0);
	expect(port.contextProbes).toEqual([first.sessionId]);
});

test("a failed turn whose context occupancy is unknown is reported as a failure, never rotated on a guess", async () => {
	const port = new ScriptedSessionPort();
	const failures: string[] = [];
	await harness(port, { failure: (message) => failures.push(message) });
	enqueue("m-1", "unknown occupancy");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	port.fail(port.sends[0]!.opRef, "Prompt submission failed.");
	await eventually(() => failures.length === 1, "failure did not reach the lifecycle");
	expect(port.sends).toHaveLength(1);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(0);
});

test("bounded shutdown reconciliation leaves a nonterminal accepted turn durable", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "still running");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before shutdown");
	await manager?.drain(0);
	expect(manager?.state(KEY)).toBe("turn-running");
	expect(database?.inboundTurnRow(latestOpRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
});

test("stop drains an admitted generation callback and fences queued and delayed tail callbacks", async () => {
	const port = new ScriptedSessionPort();
	let tailInput: TailAttachInput | undefined;
	const attach = port.attachTail.bind(port);
	port.attachTail = async (input) => {
		tailInput = input;
		return attach(input);
	};
	const terminal: string[] = [];
	const logs: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) }, (line) => logs.push(line));
	enqueue("stop-generation", "keep accepted work durable");
	await manager!.notifyInbound(KEY);
	await manager!.drain(0);
	expect(logs.some((line) => line.startsWith("shutdown_hold "))).toBe(true);
	const before = database!.inboundTurnRow(latestOpRef);
	let release!: () => void;
	const blocked = new Promise<void>((resolve) => {
		release = resolve;
	});
	let entered!: () => void;
	const started = new Promise<void>((resolve) => {
		entered = resolve;
	});
	let statusCalls = 0;
	const status = port.status.bind(port);
	port.status = async (input) => {
		statusCalls++;
		entered();
		await blocked;
		return status(input);
	};
	const generation = manager!.onBrokerGeneration(1);
	await started;
	const queuedGeneration = manager!.onBrokerGeneration(2);
	const queuedTail = tailInput!.onStall?.({
		sessionId: port.sends[0]!.sessionId,
		brokerGeneration: 0,
		elapsedMs: 100_000,
	});
	let stopped = false;
	const stopping = manager!.stop().then(() => {
		stopped = true;
	});
	await Promise.resolve();
	expect(stopped).toBe(false);
	release();
	await Promise.all([generation, queuedGeneration, queuedTail, stopping]);
	expect(statusCalls).toBe(1);
	expect(database!.inboundTurnRow(latestOpRef)).toEqual(before);
	database!.close();
	database = undefined;
	const callbacks = tailInput!;
	await manager!.onBrokerGeneration(3);
	await manager!.tick(KEY);
	await manager!.reconcile(KEY);
	await callbacks.onCursorCommitted?.("late-cursor");
	await callbacks.onStall?.({ sessionId: port.sends[0]!.sessionId, brokerGeneration: 0, elapsedMs: 100_000 });
	// A saved callback can outlive the tail handle and database.
	await callbacks.onFrame?.({
		kind: "transcript",
		rawKind: "transcript",
		payload: { role: "assistant", opRef: latestOpRef },
		assistantText: "late answer",
		steerEcho: false,
		idle: false,
	});
	await callbacks.onRetentionGap?.({
		sessionId: port.sends[0]!.sessionId,
		resync: { revision: 1, generation: 0, seq: 1 },
	});
	expect(statusCalls).toBe(1);
	expect(port.sends).toHaveLength(1);
	expect(port.steers).toHaveLength(0);
	expect(terminal).toEqual([]);
});

test("/new retires an accepted turn, fences its late output, and preserves turn recovery until terminal", async () => {
	const port = new ScriptedSessionPort();
	let retired = 0;
	const terminal: string[] = [];
	await harness(port, { retired: () => retired++, terminal: (text) => terminal.push(text) });
	enqueue("m-1", "old turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before reset");
	const first = port.sends[0]!;
	await manager?.reset(KEY, JSON.stringify(ORIGIN));

	expect(retired).toBe(1);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(database?.inboundTurnRow(latestOpRef)).toMatchObject({ state: "pending", turn_state: "accepted" });
	port.complete(first.opRef, "stale output");
	await eventually(
		() => database?.inboundTurnRow(latestOpRef)?.turn_state === "done",
		"retired turn did not reconcile",
	);
	expect(terminal).toEqual([]);
});

test("a retired stalled turn detaches into a durable hold and reconciles terminal without stale delivery", async () => {
	const port = new ScriptedSessionPort();
	const terminal: string[] = [];
	await harness(port, { terminal: (text) => terminal.push(text) });
	enqueue("m-1", "old turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before retired stall");
	const send = port.sends[0]!;
	const opRef = latestOpRef;
	await manager?.reset(KEY, JSON.stringify(ORIGIN));
	port.emitStall(send.sessionId);
	await manager?.recover();
	port.complete(send.opRef, "must remain fenced");
	await eventually(
		() => database?.inboundTurnRow(opRef)?.turn_state === "done",
		"retired hold did not reconcile terminal",
	);
	expect(terminal).toEqual([]);
});

/**
 * Live 2026-09-05: a send that tore on Router startup left a BOUND turn with an
 * unknown op. The next message's steer was refused, so the session was replaced
 * under the turn (retired, answerWanted). The old session then died, but a
 * retired turn was exempt from every release rule: held forever, its lifecycle
 * never told the turn was over, and the trigger never re-dispatched.
 */
class GhostSendPort extends ScriptedSessionPort {
	ghostOpRef: string | undefined;
	ghostSessionId: string | undefined;
	ghostDead = false;

	constructor() {
		super({ onBind: (input) => `session-e${input.epoch}` });
	}

	override async send(input: Parameters<ScriptedSessionPort["send"]>[0]) {
		if (this.ghostOpRef === undefined) {
			this.ghostOpRef = input.opRef;
			this.ghostSessionId = input.sessionId;
			this.sendAttempts.push(input);
			throw new GjcCliError("gjc sdk session send reported failure", 0, "", {
				code: "timeout",
				message: "SDK session Router startup timed out.",
			});
		}
		return await super.send(input);
	}

	override async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]) {
		if (input.sessionId === this.ghostSessionId)
			throw steerRefused("session is unavailable through the session Router");
		await super.steer(input);
	}

	override async status(input: Parameters<ScriptedSessionPort["status"]>[0]) {
		if (input.opRef === this.ghostOpRef && this.ghostDead)
			throw Object.assign(new Error("session_unavailable"), { code: "session_unavailable" });
		return await super.status(input);
	}

	async liveness(input: { sessionId: string; repo: string }) {
		return { live: !(input.sessionId === this.ghostSessionId && this.ghostDead), disowned: false };
	}
}

test("a retired turn whose send never landed is released once its session dies, ends its lifecycle, and rides the replacement turn", async () => {
	const port = new GhostSendPort();
	const released: string[] = [];
	const logs: string[] = [];
	await harness(port, { released: (opRef) => released.push(opRef) }, (line) => logs.push(line));
	enqueue("m-1", "torn send");
	await manager?.notifyInbound(KEY);
	const ghostOpRef = port.ghostOpRef!;
	expect(logs.some((line) => line.startsWith(`persona_send_ambiguous origin=${KEY} opRef=${ghostOpRef}`))).toBe(true);
	expect(database?.inboundTurnRow(ghostOpRef)).toMatchObject({ state: "pending", turn_state: "bound" });

	// A second message: the ghost session refuses the steer, the epoch rotates
	// and the new row becomes the replacement turn on a fresh session.
	enqueue("m-2", "replacement turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "replacement turn did not start on the new epoch");
	expect(
		logs.some((line) => line.startsWith(`session_rebound_after_steer_failure origin=${KEY} epoch=0 nextEpoch=1`)),
	).toBe(true);
	const replacement = port.sends[0]!;
	expect(replacement.text).toBe("replacement turn");
	expect(replacement.sessionId).not.toBe(port.ghostSessionId);
	expect(released).toEqual([]);

	// The ghost session dies. The retired hold must release: trigger back to
	// pending, lifecycle told, and — since a turn is running — steered into it.
	port.ghostDead = true;
	await manager?.tick(KEY);
	expect(released).toEqual([ghostOpRef]);
	expect(
		logs.some((line) =>
			line.startsWith(`recovery_requeue_unaccepted origin=${KEY} epoch=0 nextEpoch=1 opRef=${ghostOpRef}`),
		),
	).toBe(true);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(port.steers).toEqual([
		expect.objectContaining({ sessionId: replacement.sessionId, text: expect.stringContaining("torn send") }),
	]);
	expect(database?.inboundTurnRow(ghostOpRef)).toBeUndefined();
	expect(database?.inboundTurnRows(replacement.opRef)).toEqual(
		expect.arrayContaining([expect.objectContaining({ message_id: "m-1", turn_role: "steer", turn_state: "done" })]),
	);

	// A later sweep is a no-op: nothing is held on the ghost any more.
	await manager?.tick(KEY);
	expect(released).toEqual([ghostOpRef]);
	port.complete(replacement.opRef, "answered both");
	await eventually(() => database?.inboundPendingCount(KEY) === 0, "replacement turn did not complete");
});

/**
 * Live 2026-09-05 (every main cutover from a schema-16 home): a BOUND trigger
 * on disk pointed at a session the new broker has never heard of. Status came
 * back adoptable, and the tail attach then threw
 * `session tail failed: session_unavailable`, which crashed the actor's recovery
 * and wedged the origin ("already has a nonterminal turn") until the row was
 * hand-edited.
 */
test("startup recovery releases a bound turn whose tail attach is disowned instead of wedging the origin", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "orphaned by cutover");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const orphan = port.sends[0]!;
	const opRef = latestOpRef;
	await manager?.stop();
	// The row as the old runtime left it: bound, never acknowledged.
	database?.inboundTurnRequeue(opRef);
	database?.inboundBindTurn({ messageId: "m-1", originKey: KEY, epoch: 0, opRef, sessionId: orphan.sessionId });
	expect(database?.inboundTurnRow(opRef)).toMatchObject({ turn_state: "bound" });

	const logs: string[] = [];
	class DisowningTailPort extends ScriptedSessionPort {
		override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
			if (input.sessionId === orphan.sessionId) throw new Error("session tail failed: session_unavailable");
			return await super.attachTail(input);
		}
	}
	const disowning = new DisowningTailPort({ onBind: (input) => `fresh-e${input.epoch}` });
	registerFixtureBindings(disowning);
	disowning.seedOperation(opRef, orphan.sessionId, "in_flight");
	// inspect/status still describe the session as live and the op as running
	// (live shape: the id is indexed but the tail router disowns it).
	disowning.setSessionState(orphan.sessionId, { repo: join(home, "workspace"), live: true, deleted: false });
	manager = new PersonaSessionManager({
		database: database!,
		port: disowning,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
	await manager.recover();
	expect(logs.filter((line) => line.startsWith("recovery_turn_failed"))).toEqual([]);
	expect(logs.some((line) => line.includes(`opRef=${opRef}`) && line.includes("reason=tail_attach_disowned"))).toBe(
		true,
	);
	await eventually(() => disowning.sends.length === 1, "released trigger was not re-dispatched");
	expect(disowning.sends[0]!.text).toBe("orphaned by cutover");
	expect(disowning.sends[0]!.sessionId).not.toBe(orphan.sessionId);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(manager.state(KEY)).toBe("turn-running");
});

test("startup recovery releases a retired bound turn the broker disowns instead of holding it forever", async () => {
	const port = new GhostSendPort();
	await harness(port);
	enqueue("m-1", "torn send");
	await manager?.notifyInbound(KEY);
	const ghostOpRef = port.ghostOpRef!;
	enqueue("m-2", "replacement turn");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "replacement turn did not start");
	const replacement = port.sends[0]!;
	port.complete(replacement.opRef, "answered");
	await eventually(
		() => database?.inboundTurnRow(replacement.opRef)?.turn_state === "done",
		"replacement did not close",
	);
	// Exactly the live shape: epoch rotated, ghost trigger still bound on a dead session.
	expect(database?.inboundTurnRow(ghostOpRef)).toMatchObject({ state: "pending", turn_state: "bound", turn_epoch: 0 });
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	await manager?.stop();

	port.ghostDead = true;
	const logs: string[] = [];
	const released: string[] = [];
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onReleased: ({ turn }) => {
				released.push(turn.opRef);
			},
		}),
	});
	await manager.recover();
	expect(
		logs.some((line) => line.includes(`opRef=${ghostOpRef}`) && line.includes("reason=retired_router_disowned")),
	).toBe(true);
	// Never adopted, so no lifecycle to release; the row simply became the next turn under the current epoch.
	expect(released).toEqual([]);
	await eventually(() => port.sends.length === 2, "released ghost trigger was not re-dispatched");
	expect(port.sends[1]!.text).toBe("torn send");
	expect(port.sends[1]!.opRef).not.toBe(ghostOpRef);
	expect(database?.getSessionRecord(KEY)?.epoch).toBe(1);
	expect(logs.filter((line) => line.includes(`opRef=${ghostOpRef}`) && line.startsWith("recovery_hold"))).toEqual([]);
});

test("startup recovery reconstructs an accepted durable turn and reconciles its terminal tail", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "recover me");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "accepted turn did not start before restart");
	const oldManager = manager!;
	const opRef = port.sends[0]!.opRef;
	await oldManager.stop();
	const terminal: string[] = [];
	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onTerminal: ({ text }) => {
				terminal.push(text);
			},
		}),
	});
	await manager.recover();
	port.complete(opRef, "recovered reply");
	await eventually(() => terminal.length === 1, "recovered actor did not deliver terminal output");
	expect(terminal).toEqual(["recovered reply"]);
	expect(database?.inboundTurnRow(opRef)).toMatchObject({ state: "done", turn_state: "done" });
});

test("turn op-refs are SDK-safe even when platform ids contain unsafe bytes", async () => {
	const opRef = personaTurnOpRef("instance", "discord/channel/room", 4, "message id / with spaces");
	expect(opRef).toMatch(/^gw-p-[0-9a-f]{32}$/);
});

test("notifyInbound immediately starts a turn while idle", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	enqueue("m-1", "live policy");
	await manager?.notifyInbound(KEY);

	expect(manager?.state(KEY)).toBe("turn-running");
	expect(port.sends).toEqual([expect.objectContaining({ text: "live policy", opRef: latestOpRef })]);
});

test("two messages 50ms apart start one turn and steer the second", async () => {
	const port = new ScriptedSessionPort();
	await harness(port);
	const now = Date.now();
	expect(
		database?.inboundEnqueue({
			messageId: "m-1",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "first fragment",
			receivedAt: new Date(now).toISOString(),
		}),
	).toBe(true);
	expect(
		database?.inboundEnqueue({
			messageId: "m-2",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "second fragment",
			receivedAt: new Date(now + 50).toISOString(),
		}),
	).toBe(true);

	await manager?.notifyInbound(KEY);

	expect(manager?.state(KEY)).toBe("turn-running");
	expect(port.sends).toEqual([expect.objectContaining({ text: "first fragment", opRef: latestOpRef })]);
	expect(port.steers).toEqual([
		expect.objectContaining({
			sessionId: port.sends[0]!.sessionId,
			text: expect.stringMatching(/^\[Additional message[^\n]*\]\nsecond fragment$/),
		}),
	]);
	expect(database?.inboundTurnRows(latestOpRef)).toEqual([
		expect.objectContaining({
			message_id: "m-1",
			state: "pending",
			turn_role: "trigger",
			turn_state: "accepted",
		}),
		expect.objectContaining({
			message_id: "m-2",
			state: "done",
			turn_role: "steer",
			turn_state: "done",
			turn_op_ref: latestOpRef,
		}),
	]);

	port.complete(latestOpRef, "done");
	await eventually(
		() =>
			database?.inboundTurnRows(latestOpRef).every((row) => row.state === "done" && row.turn_state === "done") === true,
		"turn rows did not complete after terminal tail evidence",
	);
});

test("recovery and stop never scan or delete unrelated shared broker sessions", async () => {
	class SharedPort extends ScriptedSessionPort {
		readonly index = [
			{ sessionId: "unrelated-saved", live: false, lastActivityMs: 0 },
			{ sessionId: "unrelated-live", live: true, lastActivityMs: 0 },
			{ sessionId: "unrelated-unknown-age", live: false, lastActivityMs: undefined },
		];
		indexScans = 0;
		readonly deleted: string[] = [];
		async listSessions() {
			this.indexScans++;
			return this.index.map((row) => ({
				...row,
				cwd: "/shared-workspace",
				sessionPath: `/shared-sessions/${row.sessionId}.jsonl`,
			}));
		}
		async deleteSession(input: { sessionId: string }) {
			this.deleted.push(input.sessionId);
			return { deleted: true as const };
		}
	}
	const port = new SharedPort();
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));
	enqueue("m-1", "recover my pending message");
	await manager!.recover();
	await eventually(() => port.sends.length === 1, "pending recovery did not dispatch");
	const send = port.sends[0]!;
	expect(port.indexScans).toBe(0);
	expect(port.deleted).toEqual([]);
	await manager!.stop();
	expect(port.indexScans).toBe(0);
	expect(port.deleted).toEqual([]);

	manager = new PersonaSessionManager({
		database: database!,
		port,
		instanceId: "instance-test",
		repo: join(home, "workspace"),
		log: (line) => logs.push(line),
	});
	await manager.recover();
	expect(manager.state(KEY)).toBe("turn-running");
	expect(port.sends).toHaveLength(1);
	port.complete(send.opRef, "recovered reply");
	await eventually(
		() => database?.inboundTurnRow(send.opRef)?.turn_state === "done",
		"recovered turn did not complete",
	);
	await manager.stop();
	expect(port.indexScans).toBe(0);
	expect(port.deleted).toEqual([]);
	expect(logs.some((line) => line.startsWith("session_gc"))).toBe(false);
});
