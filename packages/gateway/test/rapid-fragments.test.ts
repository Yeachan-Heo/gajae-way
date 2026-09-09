import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, ScriptedSessionPort } from "./session-port.fake";

const ORIGIN = { platform: "loopback", kind: "loopback", conversationId: "coalesce" } as const;
const ORIGIN_KEY = "loopback/loopback/coalesce";

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

function enqueue(messageId: string, authorName: string, body: string, receivedAt: string): void {
	expect(
		database?.inboundEnqueue({
			messageId,
			originKey: ORIGIN_KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body,
			engagementJson: JSON.stringify({ authorName }),
			receivedAt,
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

test("rapid fragments are never coalesced: the first is sent at once and every later one is steered into that turn with its speaker label", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-rapid-fragments-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const port = new ScriptedSessionPort();
	attachTestBrokerOwnership(database, port, join(home, "agent"));
	const base = Date.now();
	const label = (row: { engagement_json: string | null; message_id: string; body: string }) => {
		const engagement = JSON.parse(row.engagement_json ?? "{}") as { authorName?: string };
		return `[speaker:${engagement.authorName ?? "unknown"} message:${row.message_id}] ${row.body}`;
	};
	manager = new PersonaSessionManager({
		database,
		port,
		instanceId: "fragments-test",
		repo: join(home, "workspace"),
		onTurnStart: ({ trigger }) => ({ text: label(trigger) }),
	});

	enqueue("m-1", "Ari", "first fragment", new Date(base).toISOString());
	await manager.notifyInbound(ORIGIN_KEY);
	await eventually(() => port.sends.length === 1, "first fragment was not sent immediately");
	expect(manager.state(ORIGIN_KEY)).toBe("turn-running");
	enqueue("m-2", "Bea", "second fragment", new Date(base + 25).toISOString());
	await manager.notifyInbound(ORIGIN_KEY);
	enqueue("m-3", "Cy", "third fragment", new Date(base + 100).toISOString());
	await manager.notifyInbound(ORIGIN_KEY);

	const send = port.sends[0]!;
	expect(send.text).toBe("[speaker:Ari message:m-1] first fragment");
	expect(port.sends).toHaveLength(1);
	expect(port.steers.map((steer) => steer.text.split("\n").at(-1))).toEqual(["second fragment", "third fragment"]);
	const turn = database.inboundNonterminalTurns(ORIGIN_KEY)[0]!;
	expect(turn.triggerMessageId).toBe("m-1");
	expect(database.inboundTurnRows(turn.opRef).map((row) => [row.message_id, row.turn_role, row.turn_state])).toEqual([
		["m-1", "trigger", "accepted"],
		["m-2", "steer", "done"],
		["m-3", "steer", "done"],
	]);

	port.complete(send.opRef, "done");
	await eventually(() => database?.inboundPendingCount(ORIGIN_KEY) === 0, "trigger remained pending after terminal");
	expect(port.sends).toHaveLength(1);
});
