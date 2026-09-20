import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GlobalGjcClient } from "../src/orchestrator/broker";
import { PersonaSessionManager } from "../src/orchestrator/persona-session";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";
import { noRelay, ScriptedSessionPort } from "./session-port.fake";

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
	let broker: GlobalGjcClient | undefined;
	try {
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const ownedDatabase = database;
		const canonicalAgentDir = join(home, "fake-user-agent");
		const authority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
		ownedDatabase.assertBrokerAuthority(authority, { initializeEmpty: true });
		const port = new ScriptedSessionPort();
		const bind = port.bind.bind(port);
		port.bind = async (input) => {
			const binding = await bind(input);
			expect(ownedDatabase.recordOwnedBinding({ ...binding, authority })).toBe(true);
			return binding;
		};
		const originKey = "loopback/loopback/observability";
		const origin = { platform: "loopback", kind: "loopback", conversationId: "observability" };
		manager = new PersonaSessionManager({
			database,
			port,
			instanceId: "observability",
			repo: join(home, "workspace"),
			onTurnStart: ({ trigger }) => ({ text: trigger.body }),
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
		await eventually(
			() =>
				lines.some((line) =>
					line.startsWith(`stall_alert originKey=${originKey} sessionId=${first.sessionId} silentMs=4321`),
				),
			"stall alert was not logged",
		);
		await manager.reset(originKey, JSON.stringify(origin));
		await eventually(
			() => lines.some((line) => line.includes(`retired_hold originKey=${originKey} epoch=`)),
			"retired turn hold was not logged",
		);
		expect(
			lines.some((line) =>
				line.startsWith(`steer_delivered originKey=${originKey} opRef=${first.opRef} messageId=m-2`),
			),
		).toBe(true);

		const runner = new TailRunner({
			stream: noRelay,
			repo: join(home, "workspace"),
		});
		runner.recordCompactionReceipt({ sessionId: first.sessionId, originKey, result: { started: true } });
		expect(
			lines.some(
				(line) =>
					line ===
					`compaction_event sessionId=${first.sessionId} originKey=${originKey} source=control_receipt result=started`,
			),
		).toBe(true);

		let healthy = true;
		let probes = 0;
		let commands = 0;
		const generations: number[] = [];
		broker = new GlobalGjcClient({
			executable: "/fake/gjc",
			agentDir: join(home, "fake-user-agent"),
			cwd: home,
			discovery: async () => ({ pid: 1234, url: "ws://127.0.0.1:12345", token: "fake-token", heartbeatAt: Date.now() }),
			command: async () => {
				commands++;
				return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" };
			},
			healthProbe: async () => {
				probes++;
				return healthy;
			},
			healthIntervalMs: 5,
			reconnectBackoff: { initialMs: 1, maxMs: 1 },
		});
		broker.onGeneration((generation) => generations.push(generation));
		await broker.start();
		healthy = false;
		await eventually(
			() => lines.some((line) => line.includes("global broker unavailable; observing without repair")),
			"read-only global broker outage observation was not logged",
		);
		const outageProbes = probes;
		healthy = true;
		await eventually(() => probes > outageProbes, "same-incarnation recovery was not observed");
		await broker.stop();
		await broker.start();
		expect(broker.generation).toBe(1);
		expect(generations).toEqual([1]);
		expect(commands).toBe(0);
		expect(lines.some((line) => line.startsWith("broker_restart"))).toBe(false);
	} finally {
		await manager?.stop();
		await broker?.stop();
		database?.close();
		console.error = original;
		await rm(home, { recursive: true, force: true });
	}
});
