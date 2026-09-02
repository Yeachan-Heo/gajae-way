import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BrokerSupervisor } from "../src/orchestrator/broker";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

test("persistent-session control-plane logs use grep-stable fields", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-observability-"));
	const lines: string[] = [];
	const original = console.error;
	console.error = (...parts: unknown[]) => lines.push(parts.map(String).join(" "));
	let database: GatewayDatabase | undefined;
	let manager: PersonaSessionManager | undefined;
	let broker: BrokerSupervisor | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const port = new ScriptedSessionPort();
		const originKey = "loopback/loopback/observability";
		const origin = { platform: "loopback", kind: "loopback", conversationId: "observability" };
		manager = new PersonaSessionManager({
			database,
			port,
			instanceId: "observability",
			repo: join(home, "workspace"),
			settleWindowMs: 0,
			onTurnStart: ({ rows }) => ({ text: rows.map((row) => row.body).join("\n") }),
		});
		expect(
			database.inboundEnqueue({
				messageId: "m-1",
				originKey,
				originRefJson: JSON.stringify(origin),
				body: "first",
				receivedAt: new Date().toISOString(),
			}),
		).toBe(true);
		await manager.notifyInbound(originKey);
		await eventually(() => port.sends.length === 1, "initial persistent operation was not sent");
		const first = port.sends[0]!;
		expect(
			database.inboundEnqueue({
				messageId: "m-2",
				originKey,
				originRefJson: JSON.stringify(origin),
				body: "steer this",
				receivedAt: new Date().toISOString(),
			}),
		).toBe(true);
		await manager.notifyInbound(originKey);
		await eventually(() => port.steers.length === 1, "steer was not delivered");
		port.emitStall(first.sessionId, 4_321);
		await eventually(() => lines.some((line) => line.startsWith(`stall_alert originKey=${originKey} sessionId=${first.sessionId} silentMs=4321`)), "stall alert was not logged");
		await manager.reset(originKey, JSON.stringify(origin));
		await eventually(() => lines.some((line) => line.includes(`retired_hold originKey=${originKey} batchKey=`)), "retired batch hold was not logged");
		expect(lines.some((line) => line.startsWith(`steer_delivered originKey=${originKey} opRef=${first.opRef} messageId=m-2`))).toBe(true);

		const runner = new TailRunner({
			run: async () => ({ exitCode: 0, stdout: JSON.stringify({ ok: true, result: { items: [], terminal: true } }), stderr: "" }),
			repo: join(home, "workspace"),
		});
		runner.recordCompactionReceipt({ sessionId: first.sessionId, originKey, result: { started: true } });
		expect(lines.some((line) => line === `compaction_event sessionId=${first.sessionId} originKey=${originKey} source=control_receipt result=started`)).toBe(true);

		let healthy = true;
		broker = new BrokerSupervisor({
			ssotAgentDir: null,
			home,
			instanceId: "observability",
			healthProbe: async () => healthy,
			healthIntervalMs: 5,
			restartBackoff: { initialMs: 1, maxMs: 1 },
		});
		await broker.start();
		healthy = false;
		await eventually(
			() => lines.some((line) => line.startsWith("broker_restart generation=2 backoffMs=1 reason=")),
			"broker restart was not logged",
		);
	} finally {
		await manager?.stop();
		await broker?.stop();
		database?.close();
		console.error = original;
		await rm(home, { recursive: true, force: true });
	}
});
