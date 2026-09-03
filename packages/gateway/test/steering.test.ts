import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcCliError } from "@gajaeway/subsession";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort, steerRefused } from "./session-port.fake";

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
	const port = new SteeringTranscriptPort();
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
	const port = new TornSteerPort();
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
			super({ onBind: (input) => `session-${input.epoch}` });
		}
		async steer(): Promise<void> {
			this.attempts++;
			throw new GjcCliError("gjc sdk turn.steer exited 1", 1, "socket reset");
		}
	}
	const port = new DeadTransportPort();
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
	expect(database.inboundPendingOldest(ORIGIN_KEY)).toMatchObject({ message_id: "maybe-landed", turn_state: null });
	// The next admission retries the same clientRef; once the transport answers
	// (here: the recorded acceptance), the row is attributed without a duplicate.
	port.steer = async () => {};
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => database?.inboundPendingOldest(ORIGIN_KEY) === undefined, "held steer was not retried");
	expect(port.sends).toHaveLength(1);
});

test("a torn steer whose replay returns a definitive refusal rebinds once and sends the message once", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-steering-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	class TornThenRefusedPort extends ScriptedSessionPort {
		attempts = 0;
		constructor() {
			super({ onBind: (input) => `session-${input.epoch}` });
		}
		async steer(): Promise<void> {
			this.attempts++;
			if (this.attempts === 1) throw new GjcCliError("gjc sdk turn.steer exited 137", 137, "killed");
			throw steerRefused("no running turn");
		}
	}
	const port = new TornThenRefusedPort();
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
	enqueue("refused", "second");
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length === 2, "refused message was not sent on the replacement session");
	expect(port.attempts).toBe(2);
	expect(logs.filter((line) => line.startsWith("session_rebound_after_steer_failure"))).toHaveLength(1);
	expect(port.sends[1]!.text).toBe("second");
	expect(port.sends[1]!.sessionId).not.toBe(port.sends[0]!.sessionId);
});
