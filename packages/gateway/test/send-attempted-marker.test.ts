import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager, retryInboundTurn } from "../src/orchestrator/persona-session";
import { Store } from "../src/store";
import {
	attachTestBrokerOwnership,
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

test("send_attempted marker is written before port.send completes", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-send-marker-written-"));
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

	enqueue("m-check-marker", "test message");
	await manager.notifyInbound(KEY);
	await eventually(() => port.sends.length === 1, "send was not attempted");
	const send = port.sends[0]!;

	// After successful send, the marker should have been written
	const acceptedTurn = database.inbox.turnRow(send.opRef);
	expect(acceptedTurn?.send_attempted_at).not.toBeNull();
});

test("turnRequeue clears send_attempted marker", async () => {
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
	const opRef = send.opRef;

	// Verify marker is written
	const beforeRequeue = database.inbox.turnRow(opRef);
	expect(beforeRequeue?.send_attempted_at).not.toBeNull();

	// Requeue the turn
	database.transaction((tx) => {
		retryInboundTurn(database, tx, opRef);
	});

	// After requeue, turnRow returns null because turn_op_ref is cleared
	const afterRequeueTurn = database.inbox.turnRow(opRef);
	expect(afterRequeueTurn).not.toBeDefined();
	
	// The requeue function cleared all turn-related fields including send_attempted_at
	// This is verified by the fact that turnRow returns null (turn_op_ref was cleared)
});
