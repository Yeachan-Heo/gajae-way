import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajaeway/subsession";
import { PersonaSessionManager, personaTurnOpRef } from "../src/orchestrator/persona-session";
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

async function harness(
	port: ScriptedSessionPort,
	hooks: { terminal?: (text: string) => void; retired?: () => void; released?: (opRef: string) => void } = {},
	log?: (line: string) => void,
) {
	home = await mkdtemp(join(tmpdir(), "gajaeway-persona-session-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
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

test("a retired turn whose send tore is held under its own deadline when its session dies; the replacement turn is unaffected", async () => {
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

	// The ghost session dies. I5a (U3): the torn send is ABSENCE of evidence, not
	// proof it never landed; the retired hold stays held under its own durable
	// deadline instead of being replayed into the replacement turn.
	port.ghostDead = true;
	await manager?.tick(KEY);
	expect(released).toEqual([]);
	expect(logs.some((line) => line.startsWith(`recovery_requeue_unaccepted origin=${KEY}`))).toBe(false);
	expect(
		logs.some((line) => line.includes(`opRef=${ghostOpRef}`) && line.includes("reason=operation_state_unknown")),
	).toBe(true);
	expect(database?.inboundTurnRow(ghostOpRef)).toMatchObject({ turn_state: "bound" });
	expect(port.steers).toEqual([]);
	expect(database?.holdState(ghostOpRef)?.deadlineAt).toBeDefined();
	// The replacement turn is unaffected and completes normally.
	port.complete(replacement.opRef, "answered the second message");
	await eventually(
		() => database?.inboundTurnRow(replacement.opRef)?.turn_state === "done",
		"replacement turn did not complete",
	);
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
