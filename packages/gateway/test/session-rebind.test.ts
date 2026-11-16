import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PersonaSessionManager, personaTurnOpRef } from "../src/orchestrator/persona-session";
import {
	extractRuntimeError,
	formatFailureChatNotice,
	formatFailureNotice,
	GjcRuntimeError,
	isRebindableCode,
	REBINDABLE_ERROR_CODES,
	rebindableCodeOf,
	redactSecrets,
} from "../src/orchestrator/rebind";
import { Store } from "../src/store";
import { ScriptedSessionPort } from "./session-port.fake";

let home = "";
let database: Store | undefined;

async function openDatabase(): Promise<Store> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-bind-"));
	database = await Store.open(join(home, "gateway.db"));
	return database;
}

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test.each([
	["resource_gone", "session endpoint record is gone"],
	["spawn_failed", "SDK startup did not complete before readiness cutoff"],
	["terminal_uncertain", "cleanup could not be proven"],
	["managed_append_identity_mismatch", "bound session identity changed"],
])("%s is bind-rebindable only from its exact structured code", (code, message) => {
	expect(isRebindableCode(code)).toBe(true);
	expect(rebindableCodeOf(new GjcRuntimeError("wrapped", { code, message }))).toBe(code);
});

test("rebind classification is narrow and the measured code set is pinned", () => {
	expect(isRebindableCode("RESOURCE_GONE")).toBe(false);
	expect(isRebindableCode("resource_gone_x")).toBe(false);
	expect(
		rebindableCodeOf(
			new GjcRuntimeError("lookalike", { code: "invalid_params", message: "session endpoint record is gone" }),
		),
	).toBeUndefined();
	// resume_unusable (#96) stays classified: the per-turn `--resume` spawn it was
	// measured on is gone, but a bind-time create/resume can still report it.
	expect([...REBINDABLE_ERROR_CODES].sort()).toEqual([
		"idempotency_conflict",
		"managed_append_identity_mismatch",
		"resource_gone",
		"resume_unusable",
		"spawn_failed",
		"terminal_uncertain",
	]);
});

test("runtime parsing accepts only declared SDK error envelopes", () => {
	expect(extractRuntimeError('noise\n{"ok":false,"error":{"code":" resource_gone\u200b","message":"gone"}}')).toEqual({
		code: "resource_gone",
		message: "gone",
	});
	expect(
		extractRuntimeError('{"type":"tool_execution_end","error":{"code":"spawn_failed","message":"tool failed"}}'),
	).toBeUndefined();
	expect(extractRuntimeError('{"ok":true,"error":{"code":"resource_gone","message":"not a failure"}}')).toBeUndefined();

	expect(
		extractRuntimeError(
			'COMMAND ["sdk","serve"]\nERROR {"code":"broker_unavailable","message":"The SDK broker is unavailable."}\nCOMPLETE true',
		),
	).toEqual({ code: "broker_unavailable", message: "The SDK broker is unavailable." });
	expect(extractRuntimeError('note ERROR {"code":"broker_unavailable"}')).toBeUndefined();
	expect(extractRuntimeError("ERROR {not json")).toBeUndefined();
});

test("bind diagnostics preserve codes while redacting credential-bearing details", () => {
	const secret = "sk-live-0123456789abcdef";
	const notice = formatFailureNotice(
		new GjcRuntimeError("wrapped", {
			code: "spawn_failed",
			message: `Authorization: Bearer ${secret}; api_key=${secret}`,
		}),
	);
	expect(notice).toContain("spawn_failed");
	expect(notice).toContain("[redacted]");
	expect(notice).not.toContain(secret);
	expect(redactSecrets('{"secrets":["first","second"]}')).toBe('{"secrets":[redacted]}');
});

test("the chat notice says nothing was answered and what to do, keeping the redacted diagnosis last", () => {
	// The live redacted pre-start failure (2026-10-12): code and message carry no cause.
	const submission = new GjcRuntimeError("internal: Prompt submission failed.", {
		code: "internal",
		message: "Prompt submission failed.",
	});
	expect(formatFailureChatNotice(submission)).toBe(
		"[turn failed] This message was not answered. Please send it again in a moment.\n(diagnosis: internal: Prompt submission failed.)",
	);
	// The operator log line is unchanged.
	expect(formatFailureNotice(submission)).toBe("[turn failed] internal: Prompt submission failed.");
	const rebindable = new GjcRuntimeError("wrapped", { code: "spawn_failed", message: "SDK startup did not complete" });
	expect(formatFailureChatNotice(rebindable)).toBe(
		"[turn failed] This message was not answered. Send /new to start a fresh conversation, then send it again.\n(diagnosis: spawn_failed: SDK startup did not complete)",
	);
	const quota = new GjcRuntimeError("wrapped", {
		code: "provider_quota_exhausted",
		message: "model provider quota/billing is exhausted (HTTP 402); switch the model preset",
	});
	expect(formatFailureChatNotice(quota)).toBe(
		"[turn failed] This message was not answered.\n(diagnosis: provider_quota_exhausted: model provider quota/billing is exhausted (HTTP 402); switch the model preset)",
	);
	const secret = "sk-live-0123456789abcdef";
	const leaky = formatFailureChatNotice(
		new GjcRuntimeError("wrapped", { code: "internal", message: `api_key=${secret}` }),
	);
	expect(leaky).not.toContain(secret);
});
async function until(check: () => boolean): Promise<void> {
	for (let i = 0; i < 200 && !check(); i++) await new Promise((resolve) => setTimeout(resolve, 5));
	expect(check()).toBe(true);
}

test("missing recovery evidence holds a bound turn instead of replaying it", async () => {
	const db = await openDatabase();
	const port = new ScriptedSessionPort();
	const originKey = "loopback/loopback/tail-evidence";
	const origin = { platform: "loopback", kind: "loopback", conversationId: "tail-evidence" } as const;
	const repo = join(home, "workspace");
	const binding = await port.bind({ originKey, epoch: 0, repo });
	expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, originKey, binding.sessionId, 0))).toBe(true);
	const receivedAt = new Date().toISOString();
	const opRef = personaTurnOpRef("bind-test", originKey, 0, "m-1");
	expect(
		db.transaction((tx) =>
			db.inbox.enqueue(tx, {
				messageId: "m-1",
				originKey,
				originRefJson: JSON.stringify(origin),
				body: "never replay this without evidence",
				receivedAt,
			}),
		),
	).toBe(true);
	db.transaction((tx) => {
		db.inbox.bindTurn(tx, { messageId: "m-1", originKey, epoch: 0, opRef, sessionId: binding.sessionId });
		// The send was attempted (#685): only reconcile evidence may settle it.
		expect(db.inbox.markSendAttempted(tx, opRef)).toBe(true);
	});
	const manager = new PersonaSessionManager({
		database: db,
		port,
		instanceId: "bind-test",
		repo,
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
	try {
		await manager.recover();
		expect(port.sendAttempts).toEqual([]);
		expect(db.inbox.turnRow(opRef)).toMatchObject({
			turn_state: "bound",
			bound_session_id: binding.sessionId,
		});
	} finally {
		await manager.stop();
	}
});

for (const stage of ["start_turn", "attach"] as const) {
	test(`a ${stage} failure after bindTurn returns the trigger to pending and a later retry answers it (#675)`, async () => {
		const db = await openDatabase();
		let failures = 1;
		const port = new (class extends ScriptedSessionPort {
			override async attachTail(input: Parameters<ScriptedSessionPort["attachTail"]>[0]) {
				if (stage === "attach" && failures > 0) {
					failures--;
					throw new Error("broker_unavailable (client stopped or broker unavailable)");
				}
				return await super.attachTail(input);
			}
		})();
		const originKey = `loopback/loopback/pre-send-${stage}`;
		const origin = { platform: "loopback", kind: "loopback", conversationId: `pre-send-${stage}` } as const;
		const repo = join(home, "workspace");
		const binding = await port.bind({ originKey, epoch: 0, repo });
		expect(db.transaction((tx) => db.bindings.bindAtEpoch(tx, originKey, binding.sessionId, 0))).toBe(true);
		expect(
			db.transaction((tx) =>
				db.inbox.enqueue(tx, {
					messageId: "m-1",
					originKey,
					originRefJson: JSON.stringify(origin),
					body: "still answer me",
					receivedAt: new Date().toISOString(),
				}),
			),
		).toBe(true);
		const timers: Array<{ work: () => void; delay: number }> = [];
		const logs: string[] = [];
		const manager = new PersonaSessionManager({
			database: db,
			port,
			instanceId: "bind-test",
			repo,
			log: (line: string) => logs.push(line),
			setTimeout: (work: () => void, delay: number) => {
				const timer = { work, delay };
				timers.push(timer);
				return timer;
			},
			clearTimeout: (timer: unknown) => {
				const i = timers.indexOf(timer as { work: () => void; delay: number });
				if (i >= 0) timers.splice(i, 1);
			},
			onTurnStart: ({ trigger }) => {
				if (stage === "start_turn" && failures > 0) {
					failures--;
					throw new Error("lifecycle setup failed");
				}
				return { text: trigger.body };
			},
		});
		try {
			await manager.recover();
			expect(port.sendAttempts).toEqual([]);
			expect(db.inbox.pendingOldest(originKey)?.message_id).toBe("m-1");
			expect(logs.some((line) => line.includes(`send_pre_send_requeue stage=${stage}`))).toBe(true);
			// A generic failure never rotates the epoch (no authority to replace the session).
			expect(db.bindings.getSession(originKey)).toBe(binding.sessionId);
			const retry = timers.find((timer) => timer.delay > 0);
			expect(retry).toBeDefined();
			timers.splice(timers.indexOf(retry!), 1);
			retry!.work();
			await until(() => port.sendAttempts.length === 1);
			expect(port.sendAttempts[0]?.text).toContain("still answer me");
		} finally {
			await manager.stop();
		}
	});
}
