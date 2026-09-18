import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajae-gateway/subsession";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort, steerRefused } from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "steering" } as const;
const ORIGIN_KEY = "loopback/loopback/steering";

let home = "";
let database: GatewayDatabase | undefined;
let manager: PersonaSessionManager | undefined;

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

class SteeringTranscriptPort extends ScriptedSessionPort {
	async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]): Promise<void> {
		await super.steer(input);
		this.emitSteerEcho(input.sessionId, input.text, `steer-${input.clientRef}`);
	}
}

function enqueue(messageId: string, body: string): void {
	expect(
		database?.inboundEnqueue({
			messageId,
			originKey: ORIGIN_KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			receivedAt: new Date().toISOString(),
		}),
	).toBe(true);
}

afterEach(async () => {
	await manager?.stop();
	database?.close();
	manager = undefined;
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("a mid-turn message issues one steer, keeps one send, and is attributed in the running tail transcript", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-steering-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = attachTestBrokerOwnership(
		database,
		new SteeringTranscriptPort({
			onBind: (input) => `session-${input.originKey}-${input.epoch}`,
		}),
		join(home, "agent"),
	);
	const observedTailText: string[] = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "steering-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: `[trigger:${trigger.message_id}] ${trigger.body}`,
			onFrame: ({ frame }) => {
				const content = frame.payload.content;
				if (Array.isArray(content))
					for (const item of content)
						if (typeof item === "object" && item !== null && typeof (item as { text?: unknown }).text === "string")
							observedTailText.push((item as { text: string }).text);
			},
		}),
	});

	enqueue("trigger", "draft the release note");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length === 1, "initial send did not become accepted");
	const running = port.sends[0]!;
	const batch = database.inboundNonterminalTurns(ORIGIN_KEY)[0]!;

	enqueue("correction", "mention the rollback caveat");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(
		() => observedTailText.some((text) => text.endsWith("\nmention the rollback caveat")),
		"steer echo did not reach the running tail",
	);

	expect(port.sends).toHaveLength(1);
	expect(port.steers).toEqual([
		expect.objectContaining({
			sessionId: running.sessionId,
			text: expect.stringMatching(/^\[Additional message[^\n]*\]\nmention the rollback caveat$/),
		}),
	]);
	expect(port.tailFrames(running.sessionId)).toEqual(
		expect.arrayContaining([
			expect.objectContaining({
				steerEcho: true,
				payload: { role: "user", content: [{ text: expect.stringMatching(/\nmention the rollback caveat$/) }] },
			}),
		]),
	);
	expect(database.inboundTurnRows(batch.opRef)).toEqual(
		expect.arrayContaining([
			expect.objectContaining({
				message_id: "correction",
				turn_role: "steer",
				turn_state: "done",
				turn_op_ref: running.opRef,
			}),
		]),
	);

	port.complete(running.opRef, "done");
	await eventually(
		() =>
			database?.inboundTurnRows(batch.opRef).every((row) => row.state === "done" && row.turn_state === "done") === true,
		"running batch did not complete after its tail terminal event",
	);
});

test("a message arriving after a consumer-visible reply waits for the next turn instead of steering the answered turn", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-post-reply-boundary-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = attachTestBrokerOwnership(
		database,
		new ScriptedSessionPort({
			onBind: (input) => `session-${input.originKey}-${input.epoch}`,
		}),
		join(home, "agent"),
	);
	const visible: string[] = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "post-reply-boundary-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
			onFrame: ({ frame }) => {
				if (!frame.assistantText) return false;
				visible.push(frame.assistantText);
				return true;
			},
		}),
	});

	enqueue("first", "first request");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length === 1, "first turn did not start");
	const first = port.sends[0]!;
	port.emitAssistant(first.sessionId, "first visible reply", "visible-first", first.opRef);
	await eventually(() => visible.length === 1, "reply did not become consumer-visible");

	enqueue("follow-up", "sent after the reply");
	await manager.notifyInbound(ORIGIN_KEY);
	expect(port.steers).toHaveLength(0);
	expect(port.sends).toHaveLength(1);
	expect(database.inboundPendingOldest(ORIGIN_KEY)?.message_id).toBe("follow-up");

	port.complete(first.opRef, "first visible reply");
	await eventually(() => port.sends.length === 2, "follow-up did not start as a fresh turn after settlement");
	expect(port.sends[1]?.text).toBe("sent after the reply");
	expect(port.steers).toHaveLength(0);
});

/**
 * A steer whose CLI died after the request landed (torn envelope, non-zero
 * exit) is NOT a refusal: gjc keeps the clientRef, so a replay returns the
 * recorded outcome. Treating it as a refusal would rebind the session and send
 * the same message again as a new turn on the replacement.
 */
class TornSteerPort extends ScriptedSessionPort {
	readonly attempts: string[] = [];
	async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]): Promise<void> {
		this.attempts.push(input.clientRef);
		// First attempt: the request is recorded, then the transport tears.
		if (this.attempts.length === 1) {
			await super.steer(input);
			throw new GjcCliError("gjc sdk turn.steer exited 137", 137, "killed");
		}
		// Replay of the same clientRef: gjc reports the recorded acceptance.
		if (this.attempts.length === 2) return;
		await super.steer(input);
	}
}

test("a torn steer transport is replayed on the same clientRef, never rebinds the session, and the message is steered once", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-steering-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = attachTestBrokerOwnership(
		database,
		new TornSteerPort({
			onBind: (input) => `session-${input.originKey}-${input.epoch}`,
		}),
		join(home, "agent"),
	);
	const logs: string[] = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "steering-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
		log: (line) => {
			logs.push(line);
		},
	});
	enqueue("trigger", "first");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length === 1, "initial send missing");
	enqueue("torn", "second");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.attempts.length === 2, "torn steer was not replayed");
	expect(port.attempts[0]).toBe(port.attempts[1]);
	expect(logs.some((line) => line.startsWith("steer_ambiguous") && line.includes("action=replay"))).toBe(true);
	expect(logs.some((line) => line.startsWith("session_rebound_after_steer_failure"))).toBe(false);
	expect(port.sends).toHaveLength(1);
	expect(
		database.inboundTurnRows(database.inboundNonterminalTurns(ORIGIN_KEY)[0]!.opRef).map((r) => r.message_id),
	).toEqual(["trigger", "torn"]);
	expect(database.inboundPendingOldest(ORIGIN_KEY)).toBeUndefined();
});

test("a steer transport that stays torn HOLDS the row - the message may already be in the turn, so it is never re-sent elsewhere", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-steering-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	class DeadTransportPort extends ScriptedSessionPort {
		attempts = 0;
		constructor() {
			super({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
		}
		async steer(): Promise<void> {
			this.attempts++;
			throw new GjcCliError("gjc sdk turn.steer exited 1", 1, "socket reset");
		}
	}
	const port = new DeadTransportPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const logs: string[] = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "steering-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
		log: (line) => {
			logs.push(line);
		},
	});
	enqueue("trigger", "first");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length === 1, "initial send missing");
	enqueue("maybe-landed", "second");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => logs.some((line) => line.startsWith("steer_hold")), "torn steer was not held");
	// Original + bounded replays on the SAME clientRef, then hold: no rebind, no second send.
	expect(port.attempts).toBe(3);
	expect(logs.filter((line) => line.startsWith("steer_ambiguous"))).toHaveLength(2);
	expect(logs.some((line) => line.startsWith("session_rebound_after_steer_failure"))).toBe(false);
	expect(port.sends).toHaveLength(1);
	// Held = durably attributed to the running turn, not an ordinary pending row.
	expect(database.inboundPendingOldest(ORIGIN_KEY)).toBeUndefined();
	const opRef = port.sends[0]!.opRef;
	expect(database.inboundSteersHeld(opRef).map((r) => r.message_id)).toEqual(["maybe-landed"]);
	// The next admission retries the same clientRef; once the transport answers
	// (here: the recorded acceptance), the row is attributed without a duplicate.
	port.steer = async () => {};
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => database?.inboundSteersHeld(opRef).length === 0, "held steer was not retried");
	expect(database.inboundTurnRows(opRef).map((r) => [r.message_id, r.turn_state])).toEqual([
		["trigger", "accepted"],
		["maybe-landed", "done"],
	]);
	expect(port.sends).toHaveLength(1);
});

test("a held steer survives the old turn's terminal and a restart: it is never dispatched as a new turn while its outcome is unknown", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-steering-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	class TornPort extends ScriptedSessionPort {
		torn = true;
		attempts = 0;
		constructor() {
			super({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
		}
		async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]): Promise<void> {
			this.attempts++;
			if (this.torn) throw new GjcCliError("gjc sdk turn.steer exited 1", 1, "socket reset");
			await super.steer(input);
		}
	}
	const port = new TornPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const logs: string[] = [];
	const make = () =>
		new PersonaSessionManager({
			database: database!,
			port,
			instanceId: "steering-test",
			repo: join(home, "workspace"),
			onTurnStart: ({ trigger }) => ({ text: trigger.body }),
			log: (line) => {
				logs.push(line);
			},
		});
	manager = make();
	enqueue("trigger", "first");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length === 1, "initial send missing");
	const opRef = port.sends[0]!.opRef;
	enqueue("maybe-landed", "second");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => logs.some((line) => line.startsWith("steer_hold")), "torn steer was not held");
	// The old turn ends while the steer is still unresolved: it is NOT sent as
	// a new turn (it may already be inside the answer) and stays held.
	port.complete(opRef, "answer one");
	await eventually(() => database?.inboundTurnRow(opRef)?.turn_state === "done", "old turn did not complete");
	expect(logs.some((line) => line.includes("reason=unresolved_at_terminal"))).toBe(true);
	expect(port.sends).toHaveLength(1);
	expect(database.inboundSteersHeld(opRef).map((r) => r.message_id)).toEqual(["maybe-landed"]);
	expect(database.inboundPendingOldest(ORIGIN_KEY)).toBeUndefined();
	// A restart finds the hold in SQLite and does not dispatch it either.
	await manager.stop();
	manager = make();
	await manager.recover();
	await Bun.sleep(50);
	expect(port.sends).toHaveLength(1);
	expect(database.inboundSteersHeld(opRef).map((r) => r.message_id)).toEqual(["maybe-landed"]);
	// A definitive refusal on the same clientRef releases it: it is then an
	// ordinary pending message and becomes the next turn, exactly once.
	port.torn = false;
	port.steer = async () => {
		throw steerRefused("no running turn");
	};
	enqueue("later", "third");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length >= 2, "held steer was not released");
	expect(port.sends[1]!.text).toBe("second");
	expect(port.sends[1]!.sessionId).toBe(port.sends[0]!.sessionId);
	expect(port.binds).toHaveLength(1);
});

for (const outcome of ["accepted", "refused"] as const) {
	test(`recovery alone resolves a terminal turn's only held steer as ${outcome} on the original clientRef`, async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-held-only-recovery-"));
		const dbPath = join(home, "gateway.db");
		database = await GatewayDatabase.open(dbPath);
		class HeldPort extends ScriptedSessionPort {
			decidable = false;
			readonly attempts: Parameters<ScriptedSessionPort["steer"]>[0][] = [];
			async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]): Promise<void> {
				this.attempts.push(input);
				if (!this.decidable) throw new GjcCliError("gjc sdk turn.steer exited 1", 1, "socket reset");
				if (outcome === "refused") throw steerRefused("no running turn");
			}
		}
		const port = new HeldPort({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
		const bind = port.bind.bind(port);
		const resume = port.resume.bind(port);
		attachTestBrokerOwnership(database, port, join(home, "agent"));
		const make = () =>
			new PersonaSessionManager({
				database: database!,
				port,
				instanceId: "held-only-recovery-test",
				repo: join(home, "workspace"),
				onTurnStart: ({ trigger }) => ({ text: trigger.body }),
				log: () => {},
			});
		manager = make();
		enqueue("trigger", "first");
		await manager.notifyInbound(ORIGIN_KEY);
		await eventually(() => port.sends.length === 1, "initial send missing");
		const running = port.sends[0]!;
		enqueue("held-only", "second");
		await manager.notifyInbound(ORIGIN_KEY);
		expect(database.inboundSteersHeld(running.opRef).map((row) => row.message_id)).toEqual(["held-only"]);
		port.complete(running.opRef, "first answer");
		await eventually(() => database?.inboundTurnRow(running.opRef)?.turn_state === "done", "trigger did not settle");
		await manager.stop();
		const epoch = database.getSessionRecord(ORIGIN_KEY)?.epoch;
		database.close();
		database = await GatewayDatabase.open(dbPath);
		port.bind = bind;
		port.resume = resume;
		attachTestBrokerOwnership(database, port, join(home, "agent"));
		expect(database.inboundNonterminalOrigins()).toEqual([]);
		expect(database.inboundPendingOrigins()).toEqual([]);
		expect(database.inboundHeldSteerOrigins()).toEqual([ORIGIN_KEY]);
		const attemptsBeforeRecovery = port.attempts.length;
		const originalAttempt = port.attempts[0]!;
		port.decidable = true;
		manager = make();
		// No new message or keyed tick: the held-only origin must be enumerated at boot.
		await manager.recover();
		expect(port.attempts).toHaveLength(attemptsBeforeRecovery + 1);
		expect(port.attempts.at(-1)).toEqual(originalAttempt);
		expect(originalAttempt.sessionId).toBe(running.sessionId);
		expect(database.inboundSteersHeld(running.opRef)).toEqual([]);
		expect(database.inboundHeldSteerOrigins()).toEqual([]);
		if (outcome === "accepted") {
			expect(database.inboundTurnRows(running.opRef)).toEqual(
				expect.arrayContaining([
					expect.objectContaining({
						message_id: "held-only",
						state: "done",
						turn_state: "done",
						turn_op_ref: running.opRef,
					}),
				]),
			);
			expect(port.sends).toHaveLength(1);
		} else {
			await eventually(() => port.sends.length === 2, "confirmed refusal did not dispatch the deferred message");
			expect(port.sends[1]?.text).toBe("second");
			expect(port.sends[1]?.sessionId).toBe(running.sessionId);
		}
		expect(port.binds).toHaveLength(1);
		expect(database.getSessionRecord(ORIGIN_KEY)?.epoch).toBe(epoch);
		expect(database.inboundPendingOldest(ORIGIN_KEY)).toBeUndefined();
	});
}

test("a torn steer whose replay returns a definitive refusal waits for terminal then sends once on the same session", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-steering-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	class TornThenRefusedPort extends ScriptedSessionPort {
		attempts = 0;
		constructor() {
			super({ onBind: (input) => `session-${input.originKey}-${input.epoch}` });
		}
		async steer(input: Parameters<ScriptedSessionPort["steer"]>[0]): Promise<void> {
			this.steers.push(input);
			this.attempts++;
			if (this.attempts === 1) throw new GjcCliError("gjc sdk turn.steer exited 137", 137, "killed");
			throw steerRefused("no running turn");
		}
	}
	const port = new TornThenRefusedPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const logs: string[] = [];
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "steering-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
		log: (line) => {
			logs.push(line);
		},
	});
	enqueue("trigger", "first");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length === 1, "initial send missing");
	const running = port.sends[0]!;
	enqueue("refused", "second");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(
		() => port.attempts === 2 && database?.inboundPendingOldest(ORIGIN_KEY)?.message_id === "refused",
		"refused message was not returned to pending",
	);
	expect(port.attempts).toBe(2);
	expect(port.steers[0]!.clientRef).toBe(port.steers[1]!.clientRef);
	expect(port.steers.every((steer) => steer.sessionId === running.sessionId)).toBe(true);
	expect(port.binds).toHaveLength(1);
	expect(port.sends).toHaveLength(1);
	expect(database.inboundTurnRow(running.opRef)?.turn_state).toBe("accepted");
	expect(logs.filter((line) => line.startsWith("session_rebound_after_steer_failure"))).toHaveLength(0);
	port.complete(running.opRef, "first answer");
	await eventually(() => port.sends.length === 2, "refused message did not start after current terminal");
	expect(port.sends[1]!.text).toBe("second");
	expect(port.sends[1]!.sessionId).toBe(running.sessionId);
	expect(port.sends[1]!.opRef).not.toBe(running.opRef);
	expect(port.binds).toHaveLength(1);
	expect(database.inboundPendingOldest(ORIGIN_KEY)).toBeUndefined();
});
