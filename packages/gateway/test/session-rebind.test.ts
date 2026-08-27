import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { GatewayConfig } from "../src/config";
import { GjcClient, type GjcPort, GjcTurnStream } from "../src/orchestrator/gjc-client";
import {
	DEFAULT_REBIND_CAP,
	extractRuntimeError,
	formatFailureNotice,
	GjcRuntimeError,
	isRebindableCode,
	RebindCapExceededError,
	rebindableCodeOf,
	redactSecrets,
} from "../src/orchestrator/rebind";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";

/**
 * Session rebinding (#13) and runtime error surfacing (#14).
 *
 * Regression anchor: a resident host went mute for ~2 hours because the gateway
 * kept re-sending ONE idempotency key the runtime had already condemned, and the
 * user only ever saw a single opaque line.
 */

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

/** A finished child process with canned streams; the process seam under test. */
function fakeChild(stdout: string, stderr: string, exitCode: number): ReturnType<typeof Bun.spawn> {
	return {
		stdout: new Response(stdout).body,
		stderr: new Response(stderr).body,
		exited: Promise.resolve(exitCode),
		kill: () => {},
	} as unknown as ReturnType<typeof Bun.spawn>;
}

/** `{"ok":false,…}` exactly as the gjc SDK renders a condemned lifecycle op. */
function createFailure(code: string, message: string): string {
	return `${JSON.stringify({ ok: false, operation: "session.create", error: { code, message } })}\n`;
}

function createSuccess(sessionId: string): string {
	return `${JSON.stringify({ ok: true, operation: "session.create", result: { sessionId, endpointGeneration: 1 } })}\n`;
}

interface Harness {
	readonly client: GjcClient;
	readonly database: GatewayDatabase;
	readonly logs: string[];
	readonly commands: string[][];
}

async function harness(
	responses: Array<{ stdout?: string; stderr?: string; exitCode?: number }>,
	rebindCap = DEFAULT_REBIND_CAP,
): Promise<Harness> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-rebind-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const logs: string[] = [];
	const commands: string[][] = [];
	let call = 0;
	const spawn = ((options: { cmd: string[] }) => {
		commands.push(options.cmd);
		const response = responses[call++] ?? { stdout: "", stderr: "no canned response", exitCode: 1 };
		return fakeChild(response.stdout ?? "", response.stderr ?? "", response.exitCode ?? 0);
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 5_000, directory, undefined, {
		rebindCap,
		spawn,
		log: (line) => logs.push(line),
	});
	return { client, database, logs, commands };
}

const REBINDABLE = [
	["resource_gone", "session endpoint record is gone"],
	["spawn_failed", "SDK startup did not complete before readiness cutoff"],
	["terminal_uncertain", "Lifecycle startup cleanup could not be proven; retained artifacts require reconciliation"],
	["managed_append_identity_mismatch", "bound session identity no longer matches the cwd"],
] as const;

test.each(REBINDABLE)("%s is classified rebindable from its structured code", (code, message) => {
	expect(isRebindableCode(code)).toBe(true);
	expect(rebindableCodeOf(new GjcRuntimeError(`wrapped: ${message}`, { code, message }))).toBe(code);
});

test("an unrelated code is a genuine failure, not a rebindable one", () => {
	expect(isRebindableCode("unsupported_state_version")).toBe(false);
	expect(isRebindableCode("invalid_params")).toBe(false);
	expect(isRebindableCode(undefined)).toBe(false);
	// Message text is never a classifier: the same wording without a rebindable
	// code stays a genuine failure.
	const lookalike = new GjcRuntimeError("session endpoint record is gone", {
		code: "unsupported_state_version",
		message: "session endpoint record is gone",
	});
	expect(rebindableCodeOf(lookalike)).toBeUndefined();
	expect(rebindableCodeOf(new Error("spawn_failed"))).toBeUndefined();
});

test("the structured error code is parsed out of the runtime envelope, not the message", () => {
	expect(
		extractRuntimeError(`noise line\n${createFailure("resource_gone", "session endpoint record is gone")}`),
	).toEqual({ code: "resource_gone", message: "session endpoint record is gone" });
	expect(
		extractRuntimeError('{"type":"error","error":{"code":"managed_append_identity_mismatch","message":"cwd"}}'),
	).toEqual({ code: "managed_append_identity_mismatch", message: "cwd" });
	expect(extractRuntimeError(createSuccess("s1"))).toBeUndefined();
	expect(extractRuntimeError("not json at all")).toBeUndefined();
});

test("code normalization is narrow: transport noise is stripped, case is not folded", () => {
	// The same code with transport noise around it is the same code, and
	// classification agrees with what the notice shows.
	expect(extractRuntimeError('{"ok":false,"error":{"code":" resource_gone\u200b","message":"gone"}}')).toEqual({
		code: "resource_gone",
		message: "gone",
	});
	expect(rebindableCodeOf(new GjcRuntimeError("w", { code: " resource_gone ", message: "gone" }))).toBe(
		"resource_gone",
	);
	// A different code is never assumed to be one of the four.
	expect(isRebindableCode("RESOURCE_GONE")).toBe(false);
	expect(isRebindableCode("resource_gone_x")).toBe(false);
	expect(isRebindableCode("resource_gone")).toBe(true);
});

/** An assistant reply frame, i.e. a turn that completed. */
function turnReply(text: string): string {
	return `${JSON.stringify({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text }] } })}\n`;
}

/** A turn-level structured error frame, as the ndjson stream renders one. */
function turnFailure(code: string, message: string): string {
	return `${JSON.stringify({ type: "error", error: { code, message } })}\n`;
}

test("only a DECLARED error frame is read as the failure, first one wins", () => {
	// A per-tool failure reports a tool that failed, not a condemned session key.
	expect(
		extractRuntimeError('{"type":"tool_execution_end","error":{"code":"spawn_failed","message":"tool died"}}'),
	).toBeUndefined();
	// An ok:true envelope carrying an error field is not an error either.
	expect(extractRuntimeError('{"ok":true,"error":{"code":"resource_gone","message":"x"}}')).toBeUndefined();
	// First declared error wins, so a later incidental one cannot overwrite it.
	const stream = new GjcTurnStream();
	stream.feed(turnFailure("terminal_uncertain", "the real cause"));
	stream.feed(turnFailure("resource_gone", "a later frame"));
	expect(stream.runtimeError).toEqual({ code: "terminal_uncertain", message: "the real cause" });
});

test("a per-tool error frame does not rebind: a failed tool is not a condemned key", async () => {
	const { client, database, logs } = await harness([
		{ stdout: createSuccess("session-e0") },
		{
			stdout: `${JSON.stringify({ type: "tool_execution_end", error: { code: "spawn_failed", message: "tool died" } })}\n`,
			stderr: "the child exited for an unrelated reason",
			exitCode: 1,
		},
	]);
	try {
		const { sessionId } = await client.ensureSession("discord:dm:c1");
		await expect(client.sendTurn(sessionId, "hello")).rejects.toThrow(/unrelated reason/);
		expect(logs).toEqual([]);
		expect(database.getSessionRecord("discord:dm:c1")?.epoch).toBe(0);
	} finally {
		database.close();
	}
});

test("a recurring turn-level rebindable failure spends the cap and then fails explicitly", async () => {
	// A fresh key always creates, so only a completed TURN may clear the budget.
	// Otherwise the cap is unreachable and the epoch grows one bump per message.
	const identityMismatch = {
		stdout: turnFailure("managed_append_identity_mismatch", "bound session identity no longer matches the cwd"),
		exitCode: 1,
	};
	const { client, database, logs } = await harness([
		{ stdout: createSuccess("session-e0") },
		// Each attempt: the turn fails, the rebind creates a fresh session, and the
		// replay against that fresh session fails the same way.
		identityMismatch,
		{ stdout: createSuccess("session-e1") },
		identityMismatch,
		identityMismatch,
		{ stdout: createSuccess("session-e2") },
		identityMismatch,
		identityMismatch,
		{ stdout: createSuccess("session-e3") },
		identityMismatch,
		identityMismatch,
	]);
	// Resolve the binding the way the server does: epoch read from the store.
	const currentSession = async () =>
		(await client.ensureSession("discord:dm:c1", database.getSessionRecord("discord:dm:c1")?.epoch ?? 0)).sessionId;
	try {
		for (let attempt = 1; attempt <= DEFAULT_REBIND_CAP; attempt++) {
			await expect(client.sendTurn(await currentSession(), "hello")).rejects.toMatchObject({
				code: "managed_append_identity_mismatch",
			});
			expect(logs).toHaveLength(attempt);
		}
		expect(database.getSessionRecord("discord:dm:c1")?.epoch).toBe(DEFAULT_REBIND_CAP);
		await expect(client.sendTurn(await currentSession(), "hello")).rejects.toMatchObject({
			name: "RebindCapExceededError",
			code: "rebind_cap_exceeded",
		});
		// Bounded: the 4th attempt minted no further epoch.
		expect(logs).toHaveLength(DEFAULT_REBIND_CAP);
		expect(database.getSessionRecord("discord:dm:c1")?.epoch).toBe(DEFAULT_REBIND_CAP);
	} finally {
		database.close();
	}
});

test("a completed turn restores the budget, and an explicit /new does too", async () => {
	const condemned = { stdout: createFailure("resource_gone", "session endpoint record is gone"), exitCode: 1 };
	const { client, database, logs } = await harness([
		condemned,
		{ stdout: createSuccess("session-e1") },
		{ stdout: turnReply("healthy again") },
		condemned,
		{ stdout: createSuccess("session-e2") },
	]);
	try {
		const first = await client.ensureSession("discord:dm:c1");
		expect(logs).toHaveLength(1);
		// A completed turn is the proof of health that clears the budget.
		expect(await client.sendTurn(first.sessionId, "hello")).toBe("healthy again");
		client.forgetRebinds("discord:dm:c1");
		// Budget restored, so a later rebindable failure is still allowed to rebind.
		await client.ensureSession("discord:dm:c2");
		expect(logs).toHaveLength(2);
		expect(database.getSessionRecord("discord:dm:c2")?.epoch).toBe(1);
	} finally {
		database.close();
	}
});

test("a rebind whose REPLAY succeeds still spends the budget: no unbounded epoch growth", async () => {
	// The dangerous shape, and the one the live incident had: a turn-level code
	// that recurs on every message while the replay against the fresh session
	// succeeds. If a replayed turn cleared the budget, each message would cost one
	// epoch and reset the counter, so the cap would never fire.
	directory = await mkdtemp(join(tmpdir(), "gajaeway-rebind-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const logs: string[] = [];
	let failNextTurn = false;
	let created = 0;
	const spawn = ((options: { cmd: string[] }) => {
		if (options.cmd.includes("session.create")) return fakeChild(createSuccess(`session-${created++}`), "", 0);
		if (failNextTurn) {
			failNextTurn = false;
			return fakeChild(turnFailure("managed_append_identity_mismatch", "identity moved"), "", 1);
		}
		return fakeChild(turnReply("ok"), "", 0);
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
	try {
		const failures: string[] = [];
		for (let message = 0; message < 10; message++) {
			failNextTurn = true;
			const epoch = database.getSessionRecord("discord:dm:c1")?.epoch ?? 0;
			const { sessionId } = await client.ensureSession("discord:dm:c1", epoch);
			await client.sendTurn(sessionId, "hello").catch((error: unknown) => {
				failures.push(error instanceof Error ? error.name : String(error));
			});
		}
		// Bounded at the cap, not one bump per message.
		expect(database.getSessionRecord("discord:dm:c1")?.epoch).toBe(DEFAULT_REBIND_CAP);
		expect(logs).toHaveLength(DEFAULT_REBIND_CAP);
		expect(logs[DEFAULT_REBIND_CAP - 1]).toContain(`${DEFAULT_REBIND_CAP}/${DEFAULT_REBIND_CAP}`);
		// Past the cap every further message fails loudly instead of growing the epoch.
		expect(failures.filter((name) => name === "RebindCapExceededError")).toHaveLength(7);
	} finally {
		database.close();
	}
});

test("a turn that already ran a tool is NOT replayed after the rebind", async () => {
	// The persona has full tool access; replaying a half-executed turn re-runs
	// real side effects, so the gateway rebinds but surfaces the failure.
	const startedWork = `${JSON.stringify({ type: "tool_execution_start" })}\n${turnFailure("managed_append_identity_mismatch", "identity moved")}`;
	const { client, database, logs, commands } = await harness([
		{ stdout: createSuccess("session-e0") },
		{ stdout: startedWork, exitCode: 1 },
		{ stdout: createSuccess("session-e1") },
	]);
	try {
		const { sessionId } = await client.ensureSession("discord:dm:c1");
		await expect(client.sendTurn(sessionId, "hello")).rejects.toThrow(
			/not replayed because the turn had already produced 1 tool call/,
		);
		// Rebound so the NEXT turn works, but the turn itself was not re-run.
		expect(logs).toHaveLength(1);
		expect(database.getSessionRecord("discord:dm:c1")).toEqual({ sessionId: "session-e1", epoch: 1 });
		expect(commands).toHaveLength(3);
	} finally {
		database.close();
	}
});

test.each(REBINDABLE)("a %s create failure bumps the epoch once, persists it, and retries", async (code, message) => {
	const { client, database, logs, commands } = await harness([
		{ stdout: createFailure(code, message), exitCode: 1 },
		{ stdout: createSuccess("session-after-rebind") },
	]);
	try {
		expect(await client.ensureSession("discord:dm:c1")).toEqual({ sessionId: "session-after-rebind" });
		// Exactly one bump: e0 was condemned, e1 is the retry.
		expect(database.getSessionRecord("discord:dm:c1")).toEqual({ sessionId: "session-after-rebind", epoch: 1 });
		expect(commands).toHaveLength(2);
		expect(commands[0]).toContain("gajaeway-discord-dm-c1-e0".replace("gajaeway-", `gajaeway-${database.instanceId}-`));
		expect(commands[1]).toContain("gajaeway-discord-dm-c1-e1".replace("gajaeway-", `gajaeway-${database.instanceId}-`));
		expect(logs).toHaveLength(1);
	} finally {
		database.close();
	}
});

test("the rebind log line names the causing code and both epochs", async () => {
	const { client, database, logs } = await harness([
		{ stdout: createFailure("terminal_uncertain", "cleanup could not be proven"), exitCode: 1 },
		{ stdout: createSuccess("session-e1") },
	]);
	try {
		await client.ensureSession("discord:dm:c1");
		expect(logs[0]).toBe(
			"gateway session rebind 1/3 origin=discord:dm:c1 cause=terminal_uncertain epoch 0 -> 1 (lifetime 1)",
		);
	} finally {
		database.close();
	}
});

test("the lifetime total is logged, so growth that alternates with clean turns is visible", async () => {
	// The cap bounds CONSECUTIVE rebinds, and an unaided clean turn legitimately
	// clears it. That leaves a failure alternating with clean turns growing the
	// epoch while every consecutive count reads 1/3 — invisible unless the
	// lifetime total is reported, which is the harm the guard exists to prevent.
	directory = await mkdtemp(join(tmpdir(), "gajaeway-rebind-"));
	const database = await GatewayDatabase.open(join(directory, "gateway.db"));
	const logs: string[] = [];
	let failNextTurn = false;
	let created = 0;
	const spawn = ((options: { cmd: string[] }) => {
		if (options.cmd.includes("session.create")) return fakeChild(createSuccess(`session-${created++}`), "", 0);
		if (failNextTurn) {
			failNextTurn = false;
			return fakeChild(turnFailure("managed_append_identity_mismatch", "identity moved"), "", 1);
		}
		return fakeChild(turnReply("ok"), "", 0);
	}) as unknown as typeof Bun.spawn;
	const client = new GjcClient(database, 5_000, directory, undefined, { spawn, log: (line) => logs.push(line) });
	const current = async () =>
		(await client.ensureSession("discord:dm:c1", database.getSessionRecord("discord:dm:c1")?.epoch ?? 0)).sessionId;
	try {
		for (let round = 0; round < 3; round++) {
			// A failing message, then a clean one that resets the consecutive count.
			failNextTurn = true;
			await client.sendTurn(await current(), "hello").catch(() => undefined);
			await client.sendTurn(await current(), "hello");
		}
		// The consecutive count keeps reading 1/3 — the shape the cap cannot bound.
		expect(logs.every((line) => line.includes("rebind 1/3"))).toBe(true);
		// The lifetime total escalates, so an operator can actually see it.
		expect(logs.map((line) => line.match(/\(lifetime (\d+)\)/)?.[1])).toEqual(["1", "2", "3"]);
	} finally {
		database.close();
	}
});

test("the 4th rebind attempt fails explicitly instead of growing the epoch further", async () => {
	// Every create fails rebindably, so each attempt spends one rebind.
	const condemned = { stdout: createFailure("resource_gone", "session endpoint record is gone"), exitCode: 1 };
	const { client, database, logs } = await harness(new Array(12).fill(condemned));
	try {
		for (let attempt = 1; attempt <= DEFAULT_REBIND_CAP; attempt++) {
			await expect(client.ensureSession("discord:dm:c1")).rejects.toMatchObject({ code: "resource_gone" });
			expect(logs).toHaveLength(attempt);
		}
		expect(database.getSessionRecord("discord:dm:c1")?.epoch).toBe(DEFAULT_REBIND_CAP);
		// The 4th is refused loudly: silent unbounded epoch growth is worse than mute.
		await expect(client.ensureSession("discord:dm:c1")).rejects.toMatchObject({
			name: "RebindCapExceededError",
			code: "rebind_cap_exceeded",
		});
		expect(logs).toHaveLength(DEFAULT_REBIND_CAP);
		expect(database.getSessionRecord("discord:dm:c1")?.epoch).toBe(DEFAULT_REBIND_CAP);
	} finally {
		database.close();
	}
});

test("a turn-level managed_append_identity_mismatch rebinds and replays the turn once", async () => {
	const { client, database, logs, commands } = await harness([
		{ stdout: createSuccess("session-e0") },
		{
			stdout: `{"type":"error","error":{"code":"managed_append_identity_mismatch","message":"bound session identity no longer matches the cwd"}}\n`,
			exitCode: 1,
		},
		{ stdout: createSuccess("session-e1") },
		{
			stdout: `${JSON.stringify({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "reply after rebind" }] } })}\n`,
		},
	]);
	try {
		const { sessionId } = await client.ensureSession("discord:dm:c1");
		expect(sessionId).toBe("session-e0");
		expect(await client.sendTurn(sessionId, "hello")).toBe("reply after rebind");
		expect(logs[0]).toBe(
			"gateway session rebind 1/3 origin=discord:dm:c1 cause=managed_append_identity_mismatch epoch 0 -> 1 (lifetime 1)",
		);
		expect(database.getSessionRecord("discord:dm:c1")).toEqual({ sessionId: "session-e1", epoch: 1 });
		// The replayed turn resumes the NEW session, never the condemned one.
		expect(commands[3]).toContain("session-e1");
	} finally {
		database.close();
	}
});

test("a non-rebindable turn failure is surfaced with its code and never rebinds", async () => {
	const { client, database, logs } = await harness([
		{ stdout: createSuccess("session-e0") },
		{
			stdout: `{"ok":false,"error":{"code":"unsupported_state_version","message":"state version 9 is unsupported"}}\n`,
			exitCode: 1,
		},
	]);
	try {
		const { sessionId } = await client.ensureSession("discord:dm:c1");
		await expect(client.sendTurn(sessionId, "hello")).rejects.toMatchObject({ code: "unsupported_state_version" });
		expect(logs).toEqual([]);
		expect(database.getSessionRecord("discord:dm:c1")?.epoch).toBe(0);
	} finally {
		database.close();
	}
});

test("a successful create and turn are unchanged: no rebind, no epoch bump, one spawn each", async () => {
	const { client, database, logs, commands } = await harness([
		{ stdout: createSuccess("session-happy") },
		{
			stdout: `${JSON.stringify({ type: "message_end", message: { role: "assistant", content: [{ type: "text", text: "plain reply" }] } })}\n`,
		},
	]);
	try {
		const { sessionId } = await client.ensureSession("discord:dm:c1");
		expect(sessionId).toBe("session-happy");
		expect(await client.sendTurn(sessionId, "hello")).toBe("plain reply");
		// Cached binding: a second ensureSession spawns nothing new.
		expect(await client.ensureSession("discord:dm:c1")).toEqual({ sessionId: "session-happy" });
		expect(commands).toHaveLength(2);
		expect(logs).toEqual([]);
		expect(database.getSessionRecord("discord:dm:c1")).toEqual({ sessionId: "session-happy", epoch: 0 });
	} finally {
		database.close();
	}
});

test("the failure notice carries the runtime code and message, with /new only when a rebind helps", () => {
	expect(
		formatFailureNotice(
			new GjcRuntimeError("gjc turn exited 1", {
				code: "spawn_failed",
				message: "SDK startup did not complete before readiness cutoff",
			}),
		),
	).toBe(
		"[turn failed] spawn_failed: SDK startup did not complete before readiness cutoff Send /new to rebind this conversation.",
	);
	// Not rebindable: the diagnosis survives verbatim, the hint does not.
	const genuine = formatFailureNotice(
		new GjcRuntimeError("gjc turn exited 1", {
			code: "unsupported_state_version",
			message: "state version 9 is unsupported",
		}),
	);
	expect(genuine).toBe("[turn failed] unsupported_state_version: state version 9 is unsupported");
	expect(genuine).not.toContain("/new");
	expect(formatFailureNotice(new Error("gjc turn timed out after 300000ms"))).toBe(
		"[turn failed] gjc turn timed out after 300000ms",
	);
});

test("a long runtime message is truncated but never erased", () => {
	const notice = formatFailureNotice(
		new GjcRuntimeError("wrapped", { code: "spawn_failed", message: `${"x".repeat(400)} tail` }),
	);
	expect(notice).toStartWith("[turn failed] spawn_failed: xxx");
	expect(notice).toContain("…");
	expect(notice.length).toBeLessThan(340);
});

test("secret-looking values are redacted while the diagnostic code survives", () => {
	const notice = formatFailureNotice(
		new GjcRuntimeError("wrapped", {
			code: "spawn_failed",
			message: 'startup rejected: token=sk-ant-api03-abcdefghijklmnop and authorization="Bearer ghp_ABCDEFGHIJKLMNOP"',
		}),
	);
	expect(notice).toContain("spawn_failed");
	expect(notice).not.toContain("sk-ant-api03-abcdefghijklmnop");
	expect(notice).not.toContain("ghp_ABCDEFGHIJKLMNOP");
	expect(notice).toContain("[redacted]");
	// Nothing but secrets: a runtime code is the whole diagnosis, never sensitive.
	expect(redactSecrets("unsupported_state_version: state version 9 is unsupported")).toBe(
		"unsupported_state_version: state version 9 is unsupported",
	);
});

test("a control-character-split secret cannot be reassembled past the redactor", () => {
	// Normalization must run BEFORE redaction: stripping the NUL first would let
	// the key escape every pattern and then be reassembled in the clear.
	const inCode = formatFailureNotice(
		new GjcRuntimeError("wrapped", { code: "sk_live_\u0000deadbeefcafebabe0123", message: "startup failed" }),
	);
	expect(inCode).not.toContain("deadbeefcafebabe0123");
	const inMessage = formatFailureNotice(
		new GjcRuntimeError("wrapped", { code: "spawn_failed", message: "token=sk-ant\u200b-api03-abcdefghijklmnop" }),
	);
	expect(inMessage).not.toContain("api03-abcdefghijklmnop");
	expect(inMessage).toContain("[redacted]");
});

test("a word character before a vendor prefix does not defeat redaction", () => {
	// `\b` used to anchor these rules, so `MYKEYsk_live_<secret>` leaked — and
	// because control characters are stripped first, one zero-width space was
	// enough to manufacture that adjacency deliberately.
	for (const message of [
		"MYKEY\u200bsk_live_deadbeefcafebabe0123456789",
		"MYKEYsk_live_deadbeefcafebabe0123456789",
		"AWS_SECRET_ACCESS_KEY_sk_live_deadbeefcafebabe0123456789",
		"pat\u0000ghp_abcdefghijklmnopqrstuvwx",
	]) {
		const notice = formatFailureNotice(new GjcRuntimeError("w", { code: "spawn_failed", message }));
		expect(notice).toContain("[redacted]");
		expect(notice).not.toContain("deadbeefcafebabe0123456789");
		expect(notice).not.toContain("abcdefghijklmnopqrstuvwx");
	}
});

test("a quoted secret containing whitespace is redacted whole, not half", () => {
	const notice = formatFailureNotice(
		new GjcRuntimeError("w", {
			code: "spawn_failed",
			message: 'api_key: "deadbeefcafebabe0123456789 wJalrXUtnFEMIKSECRETDENGbPxRfiCYEXAMPLEKEY"',
		}),
	);
	expect(notice).not.toContain("deadbeefcafebabe0123456789");
	expect(notice).not.toContain("wJalrXUtnFEMIKSECRETDENGbPxRfiCYEXAMPLEKEY");
	expect(notice).toContain("[redacted]");
});

test("prose after an auth-ish word is diagnosis, not a secret, and survives", () => {
	// Redacting `oauth: exchange rejected` loses the whole diagnosis to protect
	// nothing; only credential-shaped values are erased.
	expect(
		formatFailureNotice(new GjcRuntimeError("w", { code: "tool_denied", message: "oauth: exchange rejected" })),
	).toBe("[turn failed] tool_denied: oauth: exchange rejected");
	expect(redactSecrets("authorization: denied by policy")).toBe("authorization: denied by policy");
	expect(redactSecrets("authorization: hunter2hunter2hunter2")).toContain("[redacted]");
});

test("an ordinary diagnostic that merely contains an auth-ish key word survives", () => {
	// The affixed key word matches far more than credentials; redacting
	// `max_tokens=200000` loses the diagnosis to protect nothing.
	for (const text of [
		"max_tokens=200000 exceeded",
		"auth_expired: retry the handshake",
		"oauth: exchange rejected",
		"token_count=1024",
		'authorization: "expired"',
	])
		expect(redactSecrets(text)).toBe(text);
	// A credential in exactly those shapes is still erased.
	expect(redactSecrets("AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY")).toContain("[redacted]");
	expect(redactSecrets('api_key: "deadbeefcafebabe0123456789 wJalrXUtnFEMIKSECRETDENGbPxRfi"')).toContain("[redacted]");
});

test("an unterminated final frame counts as work, so the turn is not replayed", async () => {
	// A child killed mid-write leaves its last frame in the parser buffer, where
	// it is never parsed; replaying then re-runs the side effect the guard exists
	// to prevent.
	const { client, database, logs, commands } = await harness([
		{ stdout: createSuccess("session-e0") },
		// tool_execution_start with NO trailing newline, failure on stderr.
		{
			stdout: `${JSON.stringify({ type: "tool_execution_start", name: "bash" })}`,
			stderr: JSON.stringify({
				ok: false,
				error: { code: "managed_append_identity_mismatch", message: "identity moved" },
			}),
			exitCode: 1,
		},
		{ stdout: createSuccess("session-e1") },
	]);
	try {
		const { sessionId } = await client.ensureSession("discord:dm:c1");
		await expect(client.sendTurn(sessionId, "hello")).rejects.toThrow(/unterminated frame/);
		expect(logs).toHaveLength(1);
		expect(database.getSessionRecord("discord:dm:c1")).toEqual({ sessionId: "session-e1", epoch: 1 });
		// create, failed turn, rebind create — and no replay.
		expect(commands).toHaveLength(3);
	} finally {
		database.close();
	}
});

test("an affixed secret key name is still redacted", () => {
	// The key word carries affixes in real environments, so requiring it to sit
	// immediately next to the delimiter missed the most common shape of all.
	for (const message of [
		"AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY refused",
		"GITHUB_TOKEN=ghs_abcdefghijklmnopqrstuvwxyz rejected",
		"my_api_key_value: hunter2hunter2hunter2",
	]) {
		const notice = formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message }));
		expect(notice).toContain("[redacted]");
		for (const leak of [
			"wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY",
			"abcdefghijklmnopqrstuvwxyz",
			"hunter2hunter2hunter2",
		])
			expect(notice).not.toContain(leak);
	}
	// A diagnostic that merely mentions state is untouched.
	expect(redactSecrets("unsupported_state_version at schema 8")).toBe("unsupported_state_version at schema 8");
});

test("a message of only control characters does not pass for a diagnosis", () => {
	// A SIGKILLed child can dump a partial binary buffer; .trim() does not remove
	// C0 controls, so this used to deliver a visually bare notice.
	expect(formatFailureNotice(new GjcRuntimeError("", { message: "\u0000\u0001\u0002" }))).toBe(
		"[turn failed] the runtime produced no diagnosis",
	);
});

test("the no-replay failure reports its real evidence and does NOT advise /new", () => {
	// The automatic rebind already happened; advising /new here would bump a
	// second epoch and discard the freshly bound transcript.
	const notice = formatFailureNotice(
		new GjcRuntimeError("gjc turn exited 1 (rebound to e1)", {
			code: "turn_not_replayed",
			message:
				"managed_append_identity_mismatch: the turn had already produced assistant text, so it was rebound to e1 but not replayed",
		}),
	);
	expect(notice).toContain("turn_not_replayed");
	expect(notice).toContain("assistant text");
	expect(notice).not.toContain("/new");
});

test("bare header tokens and underscore-form vendor keys are redacted too", () => {
	const cases = [
		"startup rejected by Authorization: Bearer abcDEF123456ghiJKL",
		"proxy said Basic YWRtaW46c3VwZXJzZWNyZXQx",
		"key sk_live_deadbeefcafebabe0123456789 was refused",
		"credential=hunter2hunter2hunter2 rejected",
	];
	for (const message of cases) {
		const notice = formatFailureNotice(new GjcRuntimeError("wrapped", { code: "spawn_failed", message }));
		expect(notice).toContain("spawn_failed");
		expect(notice).toContain("[redacted]");
		for (const leak of [
			"abcDEF123456ghiJKL",
			"YWRtaW46c3VwZXJzZWNyZXQx",
			"deadbeefcafebabe0123456789",
			"hunter2hunter2hunter2",
		])
			expect(notice).not.toContain(leak);
	}
});

test("a secret pasted into the CODE field is redacted, and a runaway code is bounded", () => {
	const leaked = formatFailureNotice(
		new GjcRuntimeError("wrapped", { code: "sk_live_deadbeefcafebabe0123456789", message: "startup failed" }),
	);
	expect(leaked).not.toContain("deadbeefcafebabe0123456789");
	expect(leaked).toContain("[redacted]");
	const runaway = formatFailureNotice(
		new GjcRuntimeError("wrapped", { code: "x".repeat(4000), message: "startup failed" }),
	);
	// Bounded for a chat surface rather than shipping a 4000-character code.
	expect(runaway.length).toBeLessThan(400);
	expect(runaway).toContain("…");
});

test("the server's /new handler really resets the rebind budget", async () => {
	// The wiring that makes the cap-exceeded remedy real: without it the notice
	// tells the user to send /new and nothing changes.
	directory = await mkdtemp(join(tmpdir(), "gajaeway-rebind-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const forgotten: string[] = [];
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: (originKey) => forgotten.push(originKey),
		sendTurn: async () => "mock reply",
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	const socket = await Bun.connect({ unix: config.socketPath, socket: { data: () => {} } });
	socket.write(`${JSON.stringify({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } })}\n`);
	await Bun.sleep(60);
	socket.write(
		`${JSON.stringify({
			v: "0.1",
			type: "request",
			id: "new",
			verb: "chat.send",
			params: { origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" }, text: "/new" },
		})}\n`,
	);
	for (let attempt = 0; attempt < 200 && forgotten.length === 0; attempt++) await Bun.sleep(5);
	expect(forgotten).toEqual(["discord/dm/c1/peer=p1"]);
	socket.end();
});

test("an unfamiliar code shape is still REPORTED, but never classified rebindable", () => {
	const error = new GjcRuntimeError("wrapped", { code: "Runtime.Error-42", message: "state moved" });
	expect(formatFailureNotice(error)).toBe("[turn failed] Runtime.Error-42: state moved");
	expect(rebindableCodeOf(error)).toBeUndefined();
	expect(extractRuntimeError('{"ok":false,"error":{"code":"Runtime.Error-42","message":"state moved"}}')).toEqual({
		code: "Runtime.Error-42",
		message: "state moved",
	});
});

test("an empty runtime message never erases the notice to a bare [turn failed]", () => {
	// A killed or crashed child produces exactly this: an exit code and no message.
	const killed = formatFailureNotice(new GjcRuntimeError("gjc turn exited 137: ", { message: "" }));
	expect(killed).toBe("[turn failed] gjc turn exited 137:");
	const nothing = formatFailureNotice(new GjcRuntimeError("", { message: "   " }));
	expect(nothing).toBe("[turn failed] the runtime produced no diagnosis");
	expect(formatFailureNotice(new Error(""))).toBe("[turn failed] the runtime produced no diagnosis");
});

test("the cap-exceeded failure keeps a usable remedy instead of shipping a dead end", () => {
	const notice = formatFailureNotice(
		new RebindCapExceededError("discord:dm:c1", DEFAULT_REBIND_CAP, "resource_gone", 3),
	);
	expect(notice).toContain("rebind_cap_exceeded");
	expect(notice).toContain("Send /new to rebind this conversation.");
});

test("a failed platform turn delivers the runtime code and message in the notice", async () => {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-rebind-server-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const gjc: GjcPort = {
		ensureSession: async () => ({ sessionId: "mock-session" }),
		forgetRebinds: () => {},
		sendTurn: async () => {
			throw new GjcRuntimeError("gjc turn exited 1", {
				code: "spawn_failed",
				message: "SDK startup did not complete before readiness cutoff",
			});
		},
	};
	server = await startUnixServer({ config, database, gjc, onStop: () => database.close() });
	type ServerFrame = { type?: string; event?: string; payload?: { text?: string } };
	const frames: ServerFrame[] = [];
	let buffered = "";
	const socket = await Bun.connect({
		unix: config.socketPath,
		socket: {
			data(_socket, data) {
				buffered += Buffer.from(data).toString();
				const lines = buffered.split("\n");
				buffered = lines.pop() ?? "";
				for (const line of lines) if (line) frames.push(JSON.parse(line) as ServerFrame);
			},
		},
	});
	const send = (value: unknown) => socket.write(`${JSON.stringify(value)}\n`);
	send({ v: "0.1", type: "hello", payload: { supportedVersions: ["0.1"] } });
	for (let attempt = 0; attempt < 400 && frames.length < 1; attempt++) await Bun.sleep(5);
	send({
		v: "0.1",
		type: "request",
		id: "dm",
		verb: "chat.send",
		params: {
			origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
			text: "hello",
			engagement: { mentioned: false, group: false, authorId: "p1" },
		},
	});
	for (let attempt = 0; attempt < 400 && frames.length < 4; attempt++) await Bun.sleep(5);
	const notice = frames.find((frame) => frame.type === "event" && frame.event === "chat.message");
	expect(notice?.payload?.text).toBe(
		"[turn failed] spawn_failed: SDK startup did not complete before readiness cutoff Send /new to rebind this conversation.",
	);
	socket.end();
});
