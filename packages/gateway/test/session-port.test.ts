import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";

let home = "";
let database: GatewayDatabase | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("broker SessionPort preserves caller op-ref, model choice, bootstrap prompt, terminal status, and transcript body", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("session.create")) return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1" } }), stderr: "" };
		if (args.includes("model.set")) return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { changed: true } }), stderr: "" };
		if (args.includes("send")) return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1", commandId: "cmd-1" } }), stderr: "" };
		if (args.includes("status"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { operationRef: "gw-work-1", status: { status: "terminal_ok" }, summary: { completed: true } } }),
				stderr: "",
			};
		if (args.includes("tail"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { items: [], terminal: true } }), stderr: "" };
		if (args.includes("session.last_assistant"))
			return { exitCode: 0, stdout: JSON.stringify({ type: "query_response", ok: true, page: { items: ["finished body"], complete: true } }), stderr: "" };
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const tailRunner = new TailRunner({ run, repo: join(home, "workspace"), stallTimeoutMs: 1_000 });
	const port = new BrokerSessionPort({
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner,
	});
	port.setStallTimeoutMs(5_000);
	expect(tailRunner.stallTimeoutMs).toBe(5_000);
	const binding = await port.bind({ originKey: "work/task/a", epoch: 0, repo: "/tmp/repo", codingRegister: true });
	const result = await port.request({
		sessionId: binding.sessionId,
		repo: "/tmp/repo",
		text: "implement it",
		systemPreamble: "trusted bootstrap",
		model: { preset: "coding" },
		opRef: "gw-work-1",
		pollMs: 0,
	});

	expect(binding.sessionId).toBe("sdk-1");
	expect(result.assistant.text).toBe("finished body");
	expect(calls[0]).toEqual(
		expect.arrayContaining(["sdk", "session", "raw", "global", "--op", "session.create", "--idempotency-key"]),
	);
	const send = calls.find((args) => args.includes("send"))!;
	expect(send).toEqual(expect.arrayContaining(["--op-ref", "gw-work-1", "--text", "trusted bootstrap\n\nimplement it"]));
	expect(calls.find((args) => args.includes("model.set"))).toEqual(
		expect.arrayContaining(["raw", "control", "sdk-1", "--op", "model.set"]),
	);
	expect(calls.find((args) => args.includes("session.last_assistant"))).toEqual(
		expect.arrayContaining(["raw", "query", "sdk-1", "--query", "session.last_assistant"]),
	);
	expect(calls.find((args) => args.includes("tail"))).toEqual(
		expect.arrayContaining(["sdk", "session", "tail", "sdk-1", "--until-idle"]),
	);
});

test("broker SessionPort reuses the durable epoch binding and does not recreate a session", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1" } }), stderr: "" };
	};
	const port = new BrokerSessionPort({
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ run, repo: "/tmp/repo" }),
	});
	await port.bind({ originKey: "discord/channel/c", epoch: 2, repo: "/tmp/repo" });
	await port.bind({ originKey: "discord/channel/c", epoch: 2, repo: "/tmp/repo" });
	expect(calls.filter((args) => args.includes("session.create"))).toHaveLength(1);
});

test("broker SessionPort resumes saved dead authority through the SDK control before returning the same binding", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const calls: string[][] = [];
	let live = false;
	const repo = join(home, "workspace");
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { session: { sessionId: "saved-1", locator: { repo }, live, deleted: false } } }),
				stderr: "",
			};
		if (args.includes("session.resume")) {
			live = true;
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { resumed: true } }), stderr: "" };
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({ database, cli: run, instanceId: "instance-1", tailRunner: new TailRunner({ run, repo }) });
	await expect(port.resume({ sessionId: "saved-1", repo, originKey: "discord/channel/c", epoch: 3 })).resolves.toEqual({
		sessionId: "saved-1",
		repo,
		originKey: "discord/channel/c",
		epoch: 3,
	});
	expect(calls.filter((args) => args.includes("inspect"))).toHaveLength(2);
	expect(calls.find((args) => args.includes("session.resume"))).toEqual(
		expect.arrayContaining(["sdk", "session", "raw", "control", "saved-1", "--op", "session.resume"]),
	);
});

test("broker SessionPort preserves a structured client-ref conflict emitted with a non-zero CLI status", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const run: CliRunner = async (args) => {
		if (args.includes("send"))
			return {
				exitCode: 1,
				stdout: JSON.stringify({ ok: false, error: { code: "client_ref_conflict", message: "already used" } }),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ run, repo: join(home, "workspace") }),
	});
	await expect(port.send({ sessionId: "sdk-1", repo: join(home, "workspace"), text: "duplicate", opRef: "gw-work-1" })).rejects.toMatchObject({
		name: "OpRefRejectedError",
		code: "client_ref_conflict",
	});
});

test("broker SessionPort retries a terminal-uncertain lifecycle create with the same idempotency key", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	let createCalls = 0;
	const sleeps: number[] = [];
	const run: CliRunner = async (args) => {
		if (!args.includes("session.create")) throw new Error(`unexpected command ${args.join(" ")}`);
		if (createCalls++ === 0)
			return {
				exitCode: 1,
				stdout: JSON.stringify({ ok: false, error: { code: "terminal_uncertain", message: "startup pending" } }),
				stderr: "",
			};
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-after-retry" } }), stderr: "" };
	};
	const port = new BrokerSessionPort({
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ run, repo: join(home, "workspace") }),
		sleep: async (milliseconds) => {
			sleeps.push(milliseconds);
		},
	});
	await expect(port.bind({ originKey: "loopback/loopback/retry", epoch: 0, repo: join(home, "workspace") })).resolves.toMatchObject({
		sessionId: "sdk-after-retry",
	});
	expect(createCalls).toBe(2);
	expect(sleeps).toEqual([1_000]);
});
