import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GjcClient } from "../src/orchestrator/gjc-client";
import { DEFAULT_REBIND_CAP, formatFailureNotice, GjcRuntimeError } from "../src/orchestrator/rebind";
import { GatewayDatabase } from "../src/store/db";

let home = "";
let database: GatewayDatabase | undefined;

function failedChild(stdout: string, stderr = ""): ReturnType<typeof Bun.spawn> {
	return {
		stdout: new Response(stdout).body,
		stderr: new Response(stderr).body,
		exited: Promise.resolve(1),
		kill: () => {},
	} as unknown as ReturnType<typeof Bun.spawn>;
}

async function openDatabase(): Promise<GatewayDatabase> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-bind-redteam-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	return database;
}

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("a condemned bind cannot grow epochs past the durable rebind cap", async () => {
	const db = await openDatabase();
	const logs: string[] = [];
	const client = new GjcClient(db, home, {
		spawn: (() =>
			failedChild(JSON.stringify({ ok: false, error: { code: "resource_gone", message: "session is gone" } }) + "\n")) as unknown as typeof Bun.spawn,
		log: (line) => logs.push(line),
	});

	for (let attempt = 0; attempt < DEFAULT_REBIND_CAP; attempt++)
		await expect(client.ensureSession("discord/dm/capped")).rejects.toMatchObject({ code: "resource_gone" });
	await expect(client.ensureSession("discord/dm/capped")).rejects.toMatchObject({
		name: "RebindCapExceededError",
		code: "rebind_cap_exceeded",
	});
	expect(db.getSessionRecord("discord/dm/capped")?.epoch).toBe(DEFAULT_REBIND_CAP);
	expect(logs).toHaveLength(DEFAULT_REBIND_CAP);
});

test("an explicit rebind-budget clear is the only way to retry a capped bind", async () => {
	const db = await openDatabase();
	let attempts = 0;
	const client = new GjcClient(db, home, {
		spawn: (() => {
			attempts++;
			return failedChild(JSON.stringify({ ok: false, error: { code: "resource_gone", message: "session is gone" } }) + "\n");
		}) as unknown as typeof Bun.spawn,
		log: () => {},
	});

	for (let attempt = 0; attempt < DEFAULT_REBIND_CAP; attempt++) await client.ensureSession("discord/dm/reset").catch(() => {});
	const held = await client.ensureSession("discord/dm/reset").catch((error: unknown) => error);
	expect(formatFailureNotice(held)).toContain("/new");
	client.forgetRebinds("discord/dm/reset");
	await expect(client.ensureSession("discord/dm/reset")).rejects.toMatchObject({ code: "resource_gone" });
	expect(attempts).toBeGreaterThan(DEFAULT_REBIND_CAP);
});

test("durable rebind accounting survives a new bind client after a restart", async () => {
	const db = await openDatabase();
	let attempts = 0;
	const spawn = (() => {
		attempts++;
		return failedChild(JSON.stringify({ ok: false, error: { code: "resource_gone", message: "session is gone" } }) + "\n");
	}) as unknown as typeof Bun.spawn;
	const first = new GjcClient(db, home, { spawn, log: () => {} });
	await first.ensureSession("discord/dm/durable").catch(() => {});
	await first.ensureSession("discord/dm/durable").catch(() => {});
	const afterRestart = new GjcClient(db, home, { spawn, log: () => {} });
	await expect(afterRestart.ensureSession("discord/dm/durable")).rejects.toMatchObject({ code: "resource_gone" });
	await expect(afterRestart.ensureSession("discord/dm/durable")).rejects.toMatchObject({ code: "rebind_cap_exceeded" });
	expect(db.getSessionRecord("discord/dm/durable")?.epoch).toBe(DEFAULT_REBIND_CAP);
	// The capped caller observes the existing epoch through one failed bind before
	// durable accounting refuses to mint another epoch.
	expect(attempts).toBe(DEFAULT_REBIND_CAP * 2 + 1);
});

test("concurrent non-rebindable bind failures share one request and later retry cleanly", async () => {
	const db = await openDatabase();
	let failing = true;
	let calls = 0;
	const client = new GjcClient(db, home, {
		spawn: (() => {
			calls++;
			return failing
				? failedChild(JSON.stringify({ ok: false, error: { code: "invalid_params", message: "bad caller input" } }) + "\n")
				: ({
					stdout: new Response(JSON.stringify({ ok: true, result: { sessionId: "recovered" } }) + "\n").body,
					stderr: new Response("").body,
					exited: Promise.resolve(0),
					kill: () => {},
				} as unknown as ReturnType<typeof Bun.spawn>);
		}) as unknown as typeof Bun.spawn,
		log: () => {},
	});
	const results = await Promise.allSettled([client.ensureSession("discord/dm/shared-failure"), client.ensureSession("discord/dm/shared-failure")]);
	expect(results.every((result) => result.status === "rejected")).toBe(true);
	expect(calls).toBe(1);
	failing = false;
	await expect(client.ensureSession("discord/dm/shared-failure")).resolves.toEqual({ sessionId: "recovered" });
	expect(calls).toBe(2);
});

test("a rebindable-looking message with a non-rebindable code cannot advance the epoch", async () => {
	const db = await openDatabase();
	let calls = 0;
	const client = new GjcClient(db, home, {
		spawn: (() => {
			calls++;
			return failedChild(JSON.stringify({ ok: false, error: { code: "invalid_params", message: "session endpoint record is gone" } }) + "\n");
		}) as unknown as typeof Bun.spawn,
		log: () => {},
	});
	await expect(client.ensureSession("discord/dm/prose-lookalike")).rejects.toMatchObject({ code: "invalid_params" });
	expect(calls).toBe(1);
	expect(db.getSessionRecord("discord/dm/prose-lookalike")).toBeUndefined();
});

test("runtime failure text is redacted before a bind failure reaches callers", async () => {
	const db = await openDatabase();
	const secret = "sk-live-never-log-this";
	const client = new GjcClient(db, home, {
		spawn: (() => failedChild(JSON.stringify({ ok: false, error: { code: "resource_gone", message: `provider denied ${secret}` } }) + "\n")) as unknown as typeof Bun.spawn,
		log: () => {},
	});

	const error = await client.ensureSession("discord/dm/redacted").catch((failure: unknown) => failure);
	expect(error).toBeInstanceOf(GjcRuntimeError);
	expect(String((error as Error).message)).not.toContain(secret);
	expect((error as GjcRuntimeError).runtimeMessage).not.toContain(secret);
});
