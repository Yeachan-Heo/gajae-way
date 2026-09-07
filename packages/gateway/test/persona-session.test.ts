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

/**
 * Live 2026-09-06 (jip): every epoch rotation leaves the previous session saved
 * in the broker index and nothing removed them. 149 indexed for 55 referenced
 * pushed `session.list` past one page; the broker's 32-cursor budget leaked one
 * per early-stopped traversal and every id-resolving CLI call then failed with
 * `cursor capacity is exhausted`.
 */
test("session GC deletes indexed sessions no origin or pending turn references, and nothing else", async () => {
	class IndexedPort extends ScriptedSessionPort {
		readonly index: Array<{ sessionId: string; live: boolean; lastActivityMs: number | undefined }> = [];
		readonly deleted: string[] = [];
		refuse = new Set<string>();
		async listSessions() {
			return this.index.map((row) => ({
				...row,
				cwd: "/corpus",
				sessionPath: `/agent/sessions/b/2026_${row.sessionId}.jsonl`,
			}));
		}
		async deleteSession(input: { sessionId: string }) {
			if (this.refuse.has(input.sessionId))
				return { deleted: false as const, code: "cleanup_pending", message: "cleanup pending" };
			this.deleted.push(input.sessionId);
			this.index.splice(
				this.index.findIndex((row) => row.sessionId === input.sessionId),
				1,
			);
			return { deleted: true as const };
		}
	}
	const port = new IndexedPort();
	const logs: string[] = [];
	await harness(port, {}, (line) => logs.push(line));
	enqueue("m-1", "bind me");
	await manager?.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "turn did not start");
	const current = port.sends[0]!.sessionId;
	const old = Date.now() - 2 * 60 * 60_000;
	port.index.push(
		{ sessionId: current, live: true, lastActivityMs: old }, // referenced: keep
		{ sessionId: "rotated-away", live: false, lastActivityMs: old }, // orphan: delete
		{ sessionId: "still-live-elsewhere", live: true, lastActivityMs: old }, // live: keep
		{ sessionId: "just-rotated", live: false, lastActivityMs: Date.now() - 60_000 }, // too fresh: keep
		{ sessionId: "no-heartbeat", live: false, lastActivityMs: undefined }, // orphan, unknown age: delete
		{ sessionId: "broker-refuses", live: false, lastActivityMs: old }, // refused by broker: logged, kept
	);
	port.refuse.add("broker-refuses");

	const result = await manager!.collectSessions();
	expect(port.deleted.sort()).toEqual(["no-heartbeat", "rotated-away"]);
	expect(result).toEqual({ indexed: 6, deleted: 2, refused: 1 });
	expect(port.index.map((row) => row.sessionId).sort()).toEqual(
		["broker-refuses", "just-rotated", "still-live-elsewhere", current].sort(),
	);
	expect(logs.filter((line) => line.startsWith("session_gc"))).toEqual([
		"session_gc indexed=6 referenced=1 deleted=2 refused=1 refusals=cleanup_pending:1",
	]);

	// Idempotent: a second sweep with nothing collectable is silent.
	const again = await manager!.collectSessions();
	expect(again).toEqual({ indexed: 4, deleted: 0, refused: 1 });
});
