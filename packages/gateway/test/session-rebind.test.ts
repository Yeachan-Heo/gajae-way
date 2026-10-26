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
test("bound turn without prompt intent is requeued for recovery instead of held (#675)", async () => {
	// Issue #675: when a turn is bound to a session but the prompt intent row
	// doesn't exist (prompt was never sent before a broker outage), the turn should
	// be requeued instead of held indefinitely. This prevents stranded turns that
	// can never complete when the broker becomes unavailable before dispatch.
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
				body: "turn without prompt intent row",
				receivedAt,
			}),
		),
	).toBe(true);
	db.transaction((tx) =>
		db.inbox.bindTurn(tx, { messageId: "m-1", originKey, epoch: 0, opRef, sessionId: binding.sessionId }),
	);
	const manager = new PersonaSessionManager({
		database: db,
		port,
		instanceId: "bind-test",
		repo,
		onTurnStart: ({ trigger }) => ({ text: trigger.body }),
	});
	try {
		await manager.recover();
		// The turn without a prompt intent was requeued for re-dispatch
		// (not held bound indefinitely)
		const turn = db.inbox.turnRow(opRef);
		if (turn) {
			// If turn is still in inbox, it should not be bound anymore
			expect(turn.turn_state).not.toBe("bound");
		}
	} finally {
		await manager.stop();
	}
});
