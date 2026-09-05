/** a81bb27; fixture fake-gjc version 1: inspect:cwd-locator; negative inspect:cwd-mismatch.
 * Failing assertions: expect(resumeCalls).toHaveLength(1); expect(sessionIdAfter).toBe(sessionIdBefore); expect(epochAfter).toBe(epochBefore).
 * Negative: expect(resumeCalls).toHaveLength(0). HEAD: zero resumes, rotated epoch; post-fix: one resume, same id/epoch.
 * Matched log: model.profile.set ... session_unavailable; persona_model_failed ... nextEpoch=.
 */
import { expect, test } from "bun:test";
import { GjcCliError } from "../../packages/subsession/src/index";
import { BrokerSessionPort } from "../../packages/gateway/src/orchestrator/session-port";
import { TailRunner } from "../../packages/gateway/src/orchestrator/tail-runner";
import { createFakeGjc } from "../../packages/gateway/test/fixtures/fake-gjc.mjs";
import { ScriptedSessionPort } from "../../packages/gateway/test/session-port.fake";
import { harness, KEY } from "./harness";

async function scenario(mode: string) {
	const port = new ScriptedSessionPort();
	const h = await harness(port, { onTurnStart: ({ trigger }) => ({ text: trigger.body, effectiveModel: "fixture/model" }) });
	const fake = createFakeGjc({ modes: mode });
	const run = async (args: readonly string[]) => (await fake(args))!;
	const real = new BrokerSessionPort({ database: h.database, cli: run, instanceId: "red-resume", tailRunner: new TailRunner({ run }) });
	port.inspect = real.inspect.bind(real);
	const resumeCalls: unknown[] = [];
	port.resume = async (input) => { resumeCalls.push(input); return await real.resume(input); };
	port.setModel = async () => {
		if (!resumeCalls.length) throw new GjcCliError("model.profile.set failed: session_unavailable", 0, "", { code: "session_unavailable" });
		return { changed: true };
	};
	h.database.putSession(KEY, "saved-session");
	const before = h.database.getSessionRecord(KEY)!;
	try {
		h.enqueue("restart");
		await h.manager.notifyInbound(KEY);
		const after = h.database.getSessionRecord(KEY)!;
		console.info("red5", { mode, resumeCalls: resumeCalls.length, before, after, logs: h.logs });
		return { resumeCalls, sessionIdBefore: before.sessionId, sessionIdAfter: after.sessionId, epochBefore: before.epoch, epochAfter: after.epoch };
	} finally { await h.close(); }
}

test("red 5: resume saved cwd-locator authority without changing identity", async () => {
	const { resumeCalls, sessionIdAfter, sessionIdBefore, epochAfter, epochBefore } = await scenario("inspect:cwd-locator");
	expect(resumeCalls).toHaveLength(1);
	expect(sessionIdAfter).toBe(sessionIdBefore);
	expect(epochAfter).toBe(epochBefore);
});

test("red 5 negative: never resume a mismatching cwd locator", async () => {
	const { resumeCalls } = await scenario("inspect:cwd-mismatch");
	expect(resumeCalls).toHaveLength(0);
});
