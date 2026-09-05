/** a81bb27; fixture red-first-v1: terminal tail frame without assistant frame, terminal_ok witness.
 * Failing assertion: expect(logs.filter(l => l.includes("terminal_text_fallback"))).toHaveLength(0).
 * HEAD: 1; post-fix: 0. Matched log: terminal_text_fallback ... source=session.last_assistant.
 */
import { expect, test } from "bun:test";
import { ScriptedSessionPort } from "../../packages/gateway/test/session-port.fake";
import { eventually, harness, KEY } from "./harness";
import { createFakeGjc } from "../../packages/gateway/test/fixtures/fake-gjc.mjs";
import { BrokerSessionPort } from "../../packages/gateway/src/orchestrator/session-port";
import { TailRunner } from "../../packages/gateway/src/orchestrator/tail-runner";

test("red 8b: terminal turn.result does not silently use session.last_assistant", async () => {
	const port = new ScriptedSessionPort({ onSend: (input, scripted) => scripted.completeWithoutAnswerFrame(input.opRef, "AUTHORITATIVE") });
	const h = await harness(port);
	const fake = createFakeGjc({ modes: "status:content" });
	const run = async (args: readonly string[]) => (await fake(args))!;
	const real = new BrokerSessionPort({ database: h.database, cli: run, instanceId: "red-fallback", tailRunner: new TailRunner({ run }) });
	port.status = real.status.bind(real);
	try {
		h.enqueue("fallback");
		await h.manager.notifyInbound(KEY);
		await eventually(() => h.deliveries.length > 0, "terminal frame never reconciled");
		const logs = h.logs;
		console.info("red8b", logs);
		expect(logs.filter(l => l.includes("terminal_text_fallback"))).toHaveLength(0);
	} finally { await h.close(); }
});
