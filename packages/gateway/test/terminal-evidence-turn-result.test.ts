/** a81bb27; fixture fake-gjc version 1: status:content + silent tail + transcript:rows OTHER.
 * Failing assertion: expect(deliveredText).toBe("AUTHORITATIVE"); expect(logs).toContain("terminal_text_source=turn_result").
 * HEAD: OTHER; post-fix: AUTHORITATIVE. Matched logs: tail_terminal_evidence_unavailable; terminal_status_reconciled.
 */
import { expect, test } from "bun:test";
import { ScriptedSessionPort } from "./session-port.fake";
import { createFakeGjc } from "./fixtures/fake-gjc.mjs";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { eventually, harness, KEY } from "./red-first-harness";

test("red 8: turn.result is authoritative even when the attached tail stays silent", async () => {
	const port = new ScriptedSessionPort();
	const h = await harness(port);
	const rows = [{ id: "assistant-other", ts: new Date().toISOString(), revision: 1, role: "assistant", body: "OTHER" }];
	const fake = createFakeGjc({ modes: `status:content,transcript:rows=${JSON.stringify(rows)}` });
	const run = async (args: readonly string[]) => (await fake(args))!;
	const real = new BrokerSessionPort({
		database: h.database,
		cli: run,
		instanceId: "red-terminal",
		tailRunner: new TailRunner({ run, repo: h.repo }),
	});
	port.status = real.status.bind(real);
	try {
		h.enqueue("terminal-result");
		await h.manager.notifyInbound(KEY);
		await h.manager.tick(KEY);
		await eventually(() => h.deliveries.length > 0, "terminal status never reconciled");
		const deliveredText = h.deliveries[0]!.text;
		const logs = h.logs;
		console.info("red8", { deliveredText, logs });
		expect(deliveredText).toBe("AUTHORITATIVE");
		expect(logs).toContain("terminal_text_source=turn_result");
	} finally {
		await h.close();
	}
});
