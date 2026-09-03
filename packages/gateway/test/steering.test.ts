import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

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
