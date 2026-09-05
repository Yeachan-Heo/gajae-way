/** a81bb27; fixture fake-gjc version 1: daemon:capacity-exhausted, healthy endpoint hello.
 * Failing assertion: expect(broker.generation).toBe(2), then 100 healthy probes expect(broker.generation).toBe(2).
 * HEAD: 1; post-fix: 2 and stays 2. Matched log: session.list cursor capacity is exhausted.
 */
import { expect, test } from "bun:test";
import { BrokerSupervisor } from "../../packages/gateway/src/orchestrator/broker";
import { BrokerSessionPort } from "../../packages/gateway/src/orchestrator/session-port";
import { TailRunner } from "../../packages/gateway/src/orchestrator/tail-runner";
import { createFakeGjc } from "../../packages/gateway/test/fixtures/fake-gjc.mjs";
import { harness, eventually } from "./harness";

test("red 4: routed capacity failures retire a hello-healthy poisoned daemon exactly once", async () => {
	const h = await harness();
	let probes = 0;
	const fake = createFakeGjc({ modes: "daemon:capacity-exhausted" });
	const broker = new BrokerSupervisor({ home: h.home, instanceId: "red-capacity", ssotAgentDir: null,
		command: async (args) => (await fake(args))!, healthProbe: async () => { probes++; return true; }, healthIntervalMs: 1 });
	const tailRunner = new TailRunner({ run: broker.cli });
	const port = new BrokerSessionPort({ database: h.database, cli: broker.cli, instanceId: "red-capacity", tailRunner });
	try {
		await broker.start();
		const outcomes: string[] = [];
		for (let i = 0; i < 3; i++) await port.inspect({ sessionId: "poisoned", repo: h.home }).catch((error) => { outcomes.push(String(error)); });
		const after = probes;
		await eventually(() => probes > after, "probe interval did not elapse");
		console.info("red4", { generation: broker.generation, outcomes });
		expect(broker.generation).toBe(2);
		const start = probes;
		await eventually(() => probes >= start + 100, "100 healthy probes did not run");
		expect(broker.generation).toBe(2);
	} finally { await broker.stop(); await h.close(); }
});
