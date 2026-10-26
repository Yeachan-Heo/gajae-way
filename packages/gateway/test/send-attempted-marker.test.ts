import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager, retryInboundTurn } from "../src/orchestrator/persona-session";
import { Store } from "../src/store";
import {
	attachTestBrokerOwnership,
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	ScriptedSessionPort,
} from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "send-marker" } as const;
const KEY = "loopback/loopback/send-marker";

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

function enqueue(messageId: string, body: string): void {
	const accepted = database?.transaction((tx) =>
		database?.inbox.enqueue(tx, {
			messageId,
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
		}),
	);
	expect(accepted).toBe(true);
}

test("send_attempted marker distinguishes never-sent from send-ambiguous: never-sent is requeued on recovery", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-send-marker-never-sent-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const repo = join(home, "workspace");

	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "send-marker-test",
		repo,
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
		}),
		log: (line) => logs.push(line),
	});

	enqueue("m-never-sent", "requeue this message");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "send was not attempted");
	const send = port.sends[0]!;

	// Verify the turn is bound
	const boundTurn = database.inbox.turnRow(send.opRef);
	expect(boundTurn?.turn_state).toBe("bound");
	expect(boundTurn?.send_attempted_at).not.toBeNull();

	await manager.stop();

	// Simulate crash before the marker was written by manually clearing it
	database.transaction((tx) => {
		database
			.core
			.db()
			.prepare("UPDATE inbound_messages SET send_attempted_at = NULL WHERE turn_op_ref = ?")
			.run(tx, send.opRef);
	});

	// Reopen the database and recover
	const port2 = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	const logs2: string[] = [];
	manager = new PersonaSessionManager({
		database,
		port: port2,
		instanceId: "send-marker-test",
		repo,
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
		}),
		log: (line) => logs2.push(line),
	});

	await manager.recover();

	// Verify the turn was requeued (recovery_requeue log)
	expect(logs2.some((line) => line.startsWith("recovery_requeue origin=" + KEY))).toBe(true);

	// Verify a new send attempt was made
	await eventually(() => port2.sends.length === 1, "requeued message was not sent");
	const newSend = port2.sends[0]!;
	expect(newSend.opRef).not.toBe(send.opRef);

	// Complete the new send
	port2.complete(newSend.opRef, "recovery answer");
	await eventually(
		() => database?.inbox.turnRow(newSend.opRef)?.turn_state === "done",
		"recovered turn did not settle",
	);
});

test("send_attempted marker prevents resend after marker is written: goes through reconcile path", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-send-marker-reconcile-"));
	database = await Store.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));
	const logs: string[] = [];
	const repo = join(home, "workspace");

	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "send-marker-test",
		repo,
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
		}),
		log: (line) => logs.push(line),
	});

	enqueue("m-sent-ambiguous", "do not requeue this");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "send was not attempted");
	const send = port.sends[0]!;

	// Verify the turn is bound and marker is written
	const boundTurn = database.inbox.turnRow(send.opRef);
	expect(boundTurn?.turn_state).toBe("bound");
	expect(boundTurn?.send_attempted_at).not.toBeNull();

	await manager.stop();

	// Reopen and recover
	const port2 = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	const logs2: string[] = [];
	manager = new PersonaSessionManager({
		database,
		port: port2,
		instanceId: "send-marker-test",
		repo,
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
		}),
		log: (line) => logs2.push(line),
	});

	await manager.recover();

	// Verify the turn goes through reconcile (recovery_hold log), not requeue
	expect(logs2.some((line) => line.includes("recovery_hold") && line.includes("operation_state_unknown"))).toBe(true);
	expect(logs2.some((line) => line.includes("recovery_requeue"))).toBe(false);

	// Verify no new send attempt was made
	expect(port2.sends).toHaveLength(0);
	expect(port2.sendAttempts).toHaveLength(0);

	// The turn should remain bound
	const heldTurn = database.inbox.turnRow(send.opRef);
	expect(heldTurn?.turn_state).toBe("bound");
});

test("turnRequeue clears send_attempted marker so requeued turn is treated as never-sent", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-send-marker-clear-"));
	database = await Store.open(join(home, "gateway.db"));
	const logs: string[] = [];
	const repo = join(home, "workspace");
	const port = new ScriptedSessionPort({ onBind: (input) => `session-${input.epoch + 1}` });
	attachTestBrokerOwnership(database, port, join(home, "canonical-agent"));

	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "send-marker-test",
		repo,
		onTurnStart: ({ trigger }) => ({
			text: trigger.body,
		}),
		log: (line) => logs.push(line),
	});

	enqueue("m-requeue-clear", "requeue test");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "send was not attempted");
	const send = port.sends[0]!;

	// Verify marker is written
	const beforeRequeue = database.inbox.turnRow(send.opRef);
	expect(beforeRequeue?.send_attempted_at).not.toBeNull();

	// Requeue the turn
	database.transaction((tx) => {
		retryInboundTurn(database, tx, send.opRef);
	});

	// Verify marker is cleared after requeue
	const afterRequeue = database.inbox.turnRow(send.opRef);
	expect(afterRequeue?.send_attempted_at).toBeNull();
	expect(afterRequeue?.turn_state).toBeNull(); // Turn state cleared too
	expect(afterRequeue?.turn_op_ref).toBeNull(); // OpRef cleared too
});

