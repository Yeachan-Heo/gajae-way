/** a81bb27; fixture red-first-v1: cursor:invalid_input; live binding and committed cursor.
 * Failing assertions: expect(promptCalls).toHaveLength(1); expect(epochAfter).toBe(epochBefore); await expect(second).resolves.toBeUndefined().
 * HEAD: 0 sends, second InboundTurnConflictError; post-fix: 1, equal epoch, resolves.
 * Matched logs: session tail failed: invalid_input; already has a nonterminal turn.
 */
import { expect, test } from "bun:test";
import { ScriptedSessionPort } from "../../packages/gateway/test/session-port.fake";
import { harness, KEY } from "./harness";
import { createFakeGjc, runFakeGjc } from "../../packages/gateway/test/fixtures/fake-gjc.mjs";
import { TailRunner } from "../../packages/gateway/src/orchestrator/tail-runner";

test("red 1: rejected checkpoint reattaches without stranding the bound trigger", async () => {
	const port = new ScriptedSessionPort();
	const fake = createFakeGjc({ modes: "cursor:invalid_input" });
	const tails = new TailRunner({ run: (args) => runFakeGjc(args, fake) });
	port.attachTail = tails.attach.bind(tails);
	const h = await harness(port);
	try {
		const binding = await port.bind({ originKey: KEY, epoch: 0, repo: `${h.home}/workspace` });
		h.database.putSession(KEY, binding.sessionId);
		h.database.tailCursorCommit(binding.sessionId, "stale-connection-cursor");
		const epochBefore = h.database.getSessionRecord(KEY)!.epoch;
		h.enqueue("first");
		await h.manager.notifyInbound(KEY).catch((error) => h.logs.push(String(error)));
		h.enqueue("second");
		const second = h.manager.notifyInbound(KEY);
		await second.catch((error) => h.logs.push(String(error)));
		const promptCalls = port.sends;
		const epochAfter = h.database.getSessionRecord(KEY)!.epoch;
		console.info("red1", { promptCalls: promptCalls.length, epochBefore, epochAfter, logs: h.logs });
		expect(promptCalls).toHaveLength(1);
		expect(epochAfter).toBe(epochBefore);
		await expect(second).resolves.toBeUndefined();
	} finally { await h.close(); }
});
