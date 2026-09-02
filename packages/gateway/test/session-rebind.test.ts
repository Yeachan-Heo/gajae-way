import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcClient } from "../src/orchestrator/gjc-client";
import { PersonaSessionManager, personaBatchKey, personaBatchOpRef } from "../src/orchestrator/persona-session";
import {
	extractRuntimeError,
	formatFailureNotice,
	GjcRuntimeError,
	isRebindableCode,
	REBINDABLE_ERROR_CODES,
	rebindableCodeOf,
	redactSecrets,
} from "../src/orchestrator/rebind";
import { GatewayDatabase } from "../src/store/db";
import { ScriptedSessionPort } from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;

function child(stdout: string, stderr = "", exitCode = 0): ReturnType<typeof Bun.spawn> {
	return {
		stdout: new Response(stdout).body,
		stderr: new Response(stderr).body,
		exited: Promise.resolve(exitCode),
		kill: () => {},
	} as unknown as ReturnType<typeof Bun.spawn>;
}

async function openDatabase(): Promise<GatewayDatabase> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-bind-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
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

test("bind-time condemned session.create rebinds exactly once with the next epoch", async () => {
	const db = await openDatabase();
	const commands: string[][] = [];
	const spawn = ((options: { readonly cmd: readonly string[] }) => {
		commands.push([...options.cmd]);
		return commands.length === 1
			? child(
					JSON.stringify({ ok: false, error: { code: "resource_gone", message: "saved session no longer exists" } }) +
						"\n",
					"",
					1,
				)
			: child(JSON.stringify({ ok: true, result: { sessionId: "session-e1" } }) + "\n");
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(db, home, { spawn, log: () => {} });

	await expect(client.ensureSession("discord/dm/rebind", 0)).resolves.toEqual({ sessionId: "session-e1" });
	expect(db.getSessionRecord("discord/dm/rebind")).toEqual({ sessionId: "session-e1", epoch: 1 });
	expect(commands).toHaveLength(2);
	expect(commands.every((command) => command.includes("session.create"))).toBe(true);
});

test("prose-only create failure never triggers a bind-time rebind", async () => {
	const db = await openDatabase();
	let calls = 0;
	const client = new GjcClient(db, home, {
		spawn: (() => {
			calls++;
			return child("not JSON\n", 'Error: Session "dead-beef" not found\n', 1);
		}) as unknown as typeof Bun.spawn,
		log: () => {},
	});

	await expect(client.ensureSession("discord/dm/prose", 0)).rejects.toThrow("gjc session.create exited 1");
	expect(calls).toBe(1);
	expect(db.getSessionRecord("discord/dm/prose")).toBeUndefined();
});

test("concurrent bind callers share one in-flight session.create", async () => {
	const db = await openDatabase();
	let release!: () => void;
	const exited = new Promise<number>((resolve) => {
		release = () => resolve(0);
	});
	let calls = 0;
	const client = new GjcClient(db, home, {
		spawn: (() => {
			calls++;
			return {
				stdout: new Response(JSON.stringify({ ok: true, result: { sessionId: "shared-session" } }) + "\n").body,
				stderr: new Response("").body,
				exited,
				kill: () => release(),
			} as unknown as ReturnType<typeof Bun.spawn>;
		}) as unknown as typeof Bun.spawn,
		log: () => {},
	});

	const first = client.ensureSession("discord/dm/shared", 0);
	const second = client.ensureSession("discord/dm/shared", 0);
	await Bun.sleep(1);
	expect(calls).toBe(1);
	release();
	await expect(Promise.all([first, second])).resolves.toEqual([
		{ sessionId: "shared-session" },
		{ sessionId: "shared-session" },
	]);
});

test("missing recovery evidence holds a settled batch instead of replaying a turn", async () => {
	const db = await openDatabase();
	const port = new ScriptedSessionPort();
	const originKey = "loopback/loopback/tail-evidence";
	const origin = { platform: "loopback", kind: "loopback", conversationId: "tail-evidence" } as const;
	const repo = join(home, "workspace");
	const binding = await port.bind({ originKey, epoch: 0, repo });
	expect(db.putSessionAtEpoch(originKey, binding.sessionId, 0)).toBe(true);
	const receivedAt = "2026-09-03T00:00:00.000Z";
	const cutoff = receivedAt;
	const batchKey = personaBatchKey(originKey, 0, "m-1", cutoff);
	const opRef = personaBatchOpRef("bind-test", originKey, 0, "m-1", cutoff);
	expect(
		db.inboundEnqueue({
			messageId: "m-1",
			originKey,
			originRefJson: JSON.stringify(origin),
			body: "never replay this without evidence",
			receivedAt,
		}),
	).toBe(true);
	db.inboundSettleBatch({ originKey, epoch: 0, cutoff, batchKey, opRef });
	db.inboundBatchBindSession(batchKey, binding.sessionId);
	const manager = new PersonaSessionManager({
		database: db,
		port,
		instanceId: "bind-test",
		repo,
		settleWindowMs: 0,
		onTurnStart: ({ rows }) => ({ text: rows.map((row) => row.body).join("\n") }),
	});
	try {
		await manager.recover();
		expect(port.sendAttempts).toEqual([]);
		expect(db.inboundBatchRows(batchKey)[0]).toMatchObject({
			batch_state: "settled",
			bound_session_id: binding.sessionId,
		});
	} finally {
		await manager.stop();
	}
});
