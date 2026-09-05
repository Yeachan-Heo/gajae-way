/** a81bb27; fixture red-first-v1: status:unknown-forever, live-idle inspect, no transcript.
 * Failing assertions: expect(row.turn_state).toBe("done"); expect(deliveries.filter(d => /operation lost/.test(d.text))).toHaveLength(1); expect(promptCalls).toHaveLength(0).
 * HEAD: accepted, 0 deliveries; post-fix: done, 1 delivery, 0 sends.
 * Matched logs: recovery_hold ... not decidable ... sweeps=N; reason=operation_state_unknown.
 */
import { expect, test } from "bun:test";
import { ScriptedSessionPort } from "../../packages/gateway/test/session-port.fake";
import { harness, KEY, ORIGIN } from "./harness";
import { createFakeGjc } from "../../packages/gateway/test/fixtures/fake-gjc.mjs";
import { BrokerSessionPort } from "../../packages/gateway/src/orchestrator/session-port";
import { TailRunner } from "../../packages/gateway/src/orchestrator/tail-runner";

test("red 3: an accepted unknown operation becomes lost after 31 minutes without replay", async () => {
	let now = Date.now();
	const port = new ScriptedSessionPort();
	const h = await harness(port, { now: () => now });
	const fake = createFakeGjc({ modes: "status:unknown-forever" });
	const run = async (args: readonly string[]) => (await fake(args))!;
	const real = new BrokerSessionPort({
		database: h.database,
		cli: run,
		instanceId: "red-hold",
		tailRunner: new TailRunner({ run }),
	});
	port.status = real.status.bind(real);
	try {
		const binding = await port.bind({ originKey: KEY, epoch: 0, repo: `${h.home}/workspace` });
		h.database.putSession(KEY, binding.sessionId);
		h.database.inboundEnqueue({
			messageId: "lost",
			originKey: KEY,
			originRefJson: JSON.stringify(ORIGIN),
			body: "lost",
		});
		h.database.inboundBindTurn({
			messageId: "lost",
			originKey: KEY,
			epoch: 0,
			opRef: "gw-red-lost",
			sessionId: binding.sessionId,
		});
		h.database.inboundTurnAccept("gw-red-lost");
		await h.manager.recover();
		now += 31 * 60_000;
		await h.manager.recover();
		await h.manager.tick(KEY);
		const row = h.database.inboundTurnRow("gw-red-lost")!;
		const deliveries = h.deliveries;
		const promptCalls = port.sends;
		console.info("red3", { state: row.turn_state, deliveries, logs: h.logs });
		expect(row.turn_state).toBe("done");
		expect(deliveries.filter((d) => /operation lost/.test(d.text))).toHaveLength(1);
		expect(promptCalls).toHaveLength(0);
	} finally {
		await h.close();
	}
});
