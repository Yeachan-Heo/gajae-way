import { Database } from "bun:sqlite";
import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type CliRunner, GjcCliError } from "@gajae-gateway/subsession";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { isDefinitiveSteerRejection } from "../src/orchestrator/persona-session";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { BrokerAuthorityError, GatewayDatabase } from "../src/store/db";
import {
	attachTestBrokerOwnership,
	createOwnedSessionFixture,
	initializeTestBrokerAuthority,
	noRelay,
	ScriptedSessionPort,
	scriptedRelay,
} from "./session-port.fake";

let home = "";
let database: GatewayDatabase | undefined;

test("opt-in fake ownership records only successful binds and refuses unknown saved sessions", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-fake-ownership-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const agentDir = join(home, "agent");
	const repo = join(home, "workspace");
	const fake = new ScriptedSessionPort({ onBind: ({ epoch }) => `owned-${epoch}` });
	const port = attachTestBrokerOwnership(database, fake, agentDir);
	expect(port).toBe(fake);
	fake.setSessionState("unknown-saved", { repo, live: false });
	await expect(port.resume({ sessionId: "unknown-saved", repo, originKey: "origin", epoch: 0 })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
	expect(fake.resumes).toHaveLength(0);
	await port.bind({ originKey: "origin", epoch: 0, repo });
	database.rebindEpoch("origin");
	await port.bind({ originKey: "origin", epoch: 1, repo });
	fake.setSessionState("owned-0", { live: false });
	await port.resume({ sessionId: "owned-0", repo, originKey: "origin", epoch: 0 });
	expect(database.getSessionRecord("origin")).toMatchObject({ sessionId: "owned-1", epoch: 1 });
	const authority = initializeTestBrokerAuthority(database, agentDir);
	expect(database.assertOwnedSession("owned-0", repo, authority)).toMatchObject({ originKey: "origin", epoch: 0 });
	const failing = attachTestBrokerOwnership(
		database,
		new ScriptedSessionPort({
			onBind: () => {
				throw new Error("fixture refused");
			},
		}),
		agentDir,
	);
	await expect(failing.bind({ originKey: "failed", epoch: 0, repo })).rejects.toThrow("fixture refused");
	expect(database.getSessionRecord("failed")).toBeUndefined();
});

afterEach(async () => {
	database?.close();
	database = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("failed-turn evidence comes from the owned shared session file without exposing provider text", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-failure-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const repo = join(home, "workspace");
	const agentDir = join(home, "agent");
	const authority = initializeTestBrokerAuthority(database, agentDir);
	const bucket = join(agentDir, "sessions", "bucket");
	await mkdir(repo);
	await mkdir(bucket, { recursive: true });
	const sessionId = "failed-session";
	await createOwnedSessionFixture(database, authority, { sessionId, originKey: "failure-evidence", epoch: 0, repo });
	const startedAtMs = Date.now();
	const rows = [
		{ type: "session", version: 5, id: sessionId, cwd: repo, timestamp: new Date(startedAtMs - 100).toISOString() },
		{
			type: "message",
			id: "user",
			parentId: null,
			timestamp: new Date(startedAtMs).toISOString(),
			message: { role: "user", timestamp: startedAtMs, content: [{ type: "text", text: "hello" }] },
		},
		{
			type: "message",
			id: "error",
			parentId: "user",
			timestamp: new Date(startedAtMs + 20).toISOString(),
			message: {
				role: "assistant",
				timestamp: startedAtMs + 1,
				content: [],
				stopReason: "error",
				errorStatus: 400,
				errorMessage: "400 Unknown parameter: 'input[1].status'.\nraw-http-request=/private/request.json",
			},
		},
	];
	await writeFile(join(bucket, `now_${sessionId}.jsonl`), `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`);
	const run: CliRunner = async () => {
		throw new Error("Failure evidence must not start an SDK operation");
	};
	const options = {
		database,
		authority,
		cli: run,
		instanceId: "evidence",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	};
	const port = new BrokerSessionPort(options);
	const input = { sessionId, repo, startedAtMs, terminalAtMs: startedAtMs + 30 };
	expect(await port.failedTurnEvidence(input)).toEqual({ reason: "unsupported_input_status" });
	await expect(port.failedTurnEvidence({ ...input, sessionId: "foreign-session" })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
	await expect(port.failedTurnEvidence({ ...input, repo: join(home, "other-repo") })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
});

test("broker SessionPort preserves caller op-ref, model choice, bootstrap prompt, terminal status, and transcript body", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("session.create"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1" } }), stderr: "" };
		if (args.includes("model.profile.set"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { changed: false, id: "gpt-heavy" } }),
				stderr: "",
			};
		if (args.includes("model.set"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { changed: true } }), stderr: "" };
		if (args.includes("session.last_assistant"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					type: "query_response",
					ok: true,
					page: { items: ["finished body"], complete: true },
				}),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	// The turn is submitted and observed on the session's own relay: the prompt
	// goes down as turn.prompt, the terminal status comes back as turn.result.
	const relay = scriptedRelay((request) => {
		if (request.operation === "turn.prompt")
			return {
				ok: true,
				result: { commandId: "cmd-1", turnId: "turn-1", accepted: true, clientRef: request.input.clientRef },
			};
		if (request.operation === "turn.result")
			return { ok: true, result: { kind: "prompt", status: "terminal_ok", clientRef: request.input.clientRef } };
		return { ok: false, error: { code: "unsupported_operation" } };
	});
	const tailRunner = new TailRunner({ stream: relay.spawn, repo: join(home, "workspace"), stallTimeoutMs: 1_000 });
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner,
	});
	port.setStallTimeoutMs(5_000);
	expect(tailRunner.stallTimeoutMs).toBe(5_000);
	const binding = await port.bind({
		originKey: "work/task/a",
		epoch: 0,
		repo: "/tmp/repo",
		codingRegister: true,
		model: { preset: "gpt-heavy" },
	});
	const profileReceipt = await port.setModel({
		sessionId: binding.sessionId,
		repo: "/tmp/repo",
		selection: { preset: "gpt-heavy" },
	});
	expect(profileReceipt).toEqual({ changed: false });
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
	const create = calls.find((args) => args.includes("session.create"))!;
	const createInput = JSON.parse(create[create.indexOf("--json-input") + 1]!) as Record<string, unknown>;
	expect(createInput).toMatchObject({ cwd: "/tmp/repo", modelPreset: "gpt-heavy" });
	expect(binding.startupModelApplied).toBe(true);
	expect(result.receipt).toMatchObject({ operationRef: "gw-work-1", commandId: "cmd-1", turnId: "turn-1" });
	expect(relay.requests[0]).toEqual({
		type: "control_request",
		operation: "turn.prompt",
		input: { text: "trusted bootstrap\n\nimplement it", clientRef: "gw-work-1" },
	});
	expect(relay.requests[1]).toEqual({
		type: "query_request",
		operation: "turn.result",
		input: { kind: "prompt", clientRef: "gw-work-1" },
	});
	expect(calls.some((args) => args.includes("send") || args.includes("tail") || args.includes("status"))).toBe(false);
	expect(calls.find((args) => args.includes("model.profile.set"))).toEqual(
		expect.arrayContaining(["raw", "control", "sdk-1", "--op", "model.profile.set"]),
	);
	expect(calls.find((args) => args.includes("session.last_assistant"))).toEqual(
		expect.arrayContaining(["raw", "query", "sdk-1", "--query", "session.last_assistant"]),
	);
	// The relay is closed once the request settles.
	expect(relay.streams).toHaveLength(1);
	expect(relay.streams[0]!.closed).toBe(true);
});

test("broker SessionPort reuses the durable epoch binding and does not recreate a session", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						session: {
							sessionId: "sdk-1",
							live: true,
							deleted: false,
							locator: { cwd: "/tmp/repo", worktreeRoot: "/tmp/repo" },
						},
					},
				}),
				stderr: "",
			};
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1" } }), stderr: "" };
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: noRelay, repo: "/tmp/repo" }),
	});
	await port.bind({ originKey: "discord/channel/c", epoch: 2, repo: "/tmp/repo" });
	await port.bind({ originKey: "discord/channel/c", epoch: 2, repo: "/tmp/repo" });
	expect(calls.filter((args) => args.includes("session.create"))).toHaveLength(1);
});

test("broker SessionPort resumes saved dead authority through the SDK control before returning the same binding", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const calls: string[][] = [];
	let live = false;
	const repo = join(home, "workspace");
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("inspect"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: { session: { sessionId: "saved-1", locator: { repo }, live, deleted: false } },
				}),
				stderr: "",
			};
		if (args.includes("session.resume")) {
			live = true;
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { resumed: true } }), stderr: "" };
		}
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	await createOwnedSessionFixture(database, authority, {
		sessionId: "saved-1",
		repo,
		originKey: "discord/channel/c",
		epoch: 3,
	});
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
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const run: CliRunner = async (args) => {
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const relay = scriptedRelay(() => ({ ok: false, error: { code: "client_ref_conflict", message: "already used" } }));
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo: join(home, "workspace") }),
	});
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: join(home, "workspace"),
		originKey: "work/conflict",
		epoch: 0,
	});
	await expect(
		port.send({ sessionId: "sdk-1", repo: join(home, "workspace"), text: "duplicate", opRef: "gw-work-1" }),
	).rejects.toMatchObject({
		name: "OpRefRejectedError",
		code: "client_ref_conflict",
	});
});

test("broker SessionPort retries a terminal-uncertain lifecycle create with the same idempotency key", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	let createCalls = 0;
	const createKeys: string[] = [];
	const sleeps: number[] = [];
	const run: CliRunner = async (args) => {
		if (!args.includes("session.create")) throw new Error(`unexpected command ${args.join(" ")}`);
		createKeys.push(args[args.indexOf("--idempotency-key") + 1]!);
		if (createCalls++ === 0)
			return {
				exitCode: 1,
				stdout: JSON.stringify({ ok: false, error: { code: "terminal_uncertain", message: "startup pending" } }),
				stderr: "",
			};
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-after-retry" } }), stderr: "" };
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: noRelay, repo: join(home, "workspace") }),
		sleep: async (milliseconds) => {
			sleeps.push(milliseconds);
		},
	});
	await expect(
		port.bind({ originKey: "loopback/loopback/retry", epoch: 0, repo: join(home, "workspace") }),
	).resolves.toMatchObject({
		sessionId: "sdk-after-retry",
	});
	expect(createCalls).toBe(2);
	expect(createKeys).toHaveLength(2);
	expect(createKeys[0]).toMatch(/^gw-bind-[a-f0-9]{32}$/);
	expect(createKeys[1]).toBe(createKeys[0]);
	expect(sleeps).toEqual([1_000]);
});

test("bind rebinds a persisted live-false session instead of handing a dead monitor endpoint to request", async () => {
	const { mkdtemp, rm } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { GatewayDatabase } = await import("../src/store/db");
	const { BrokerSessionPort } = await import("../src/orchestrator/session-port");
	const { TailRunner } = await import("../src/orchestrator/tail-runner");
	const home = await mkdtemp(join(tmpdir(), "gajaeway-bind-dead-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	try {
		const repo = join(home, "workspace");
		await createOwnedSessionFixture(database, authority, {
			sessionId: "dead-session",
			repo,
			originKey: "monitor/eventtype/x",
			epoch: 1,
		});
		const commands: string[][] = [];
		const cli = async (args: readonly string[]) => {
			commands.push([...args]);
			if (args.includes("inspect") && args.includes("dead-session"))
				return {
					exitCode: 0,
					stdout: JSON.stringify({
						ok: true,
						result: { session: { sessionId: "dead-session", repo, live: false, deleted: false } },
					}),
					stderr: "",
				};
			if (args.includes("session.create"))
				return {
					exitCode: 0,
					stdout: JSON.stringify({ ok: true, result: { sessionId: "fresh-session" } }),
					stderr: "",
				};
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: {} }), stderr: "" };
		};
		const port = new BrokerSessionPort({
			authority,
			database,
			cli,
			instanceId: "i",
			tailRunner: new TailRunner({ stream: noRelay, repo }),
		});
		const binding = await port.bind({ originKey: "monitor/eventtype/x", epoch: 1, repo });
		expect(binding.sessionId).toBe("fresh-session");
		expect(binding.epoch).toBe(2);
		expect(database.getSessionRecord("monitor/eventtype/x")).toMatchObject({ epoch: 2, sessionId: "fresh-session" });
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("a recovered answer is the full body, never the 500-character summary", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-body-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const body = `${"가".repeat(700)} 끝.`;
	const run: CliRunner = async (args) => {
		if (args.includes("transcript.list"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					page: {
						items: [
							// The host ships both: textSummary is body.slice(0, 500).
							{ role: "assistant", ts: new Date().toISOString(), textSummary: body.slice(0, 500), body },
						],
						complete: true,
					},
				}),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const tailRunner = new TailRunner({ stream: noRelay, repo: join(home, "workspace"), stallTimeoutMs: 1_000 });
	const port = new BrokerSessionPort({ database, authority, cli: run, instanceId: "instance-body", tailRunner });
	try {
		await createOwnedSessionFixture(database, authority, {
			sessionId: "11111111-2222-3333-4444-555555555555",
			repo: join(home, "workspace"),
			originKey: "work/body",
			epoch: 0,
		});
		const recovered = await port.fetchAssistantSince({
			sessionId: "11111111-2222-3333-4444-555555555555",
			repo: join(home, "workspace"),
			notBeforeMs: Date.now() - 60_000,
		});
		// Preferring the summary cut every recovered reply mid-sentence at 500.
		expect(recovered?.text).toBe(body);
		expect(recovered?.text.length).toBeGreaterThan(500);
		expect(recovered?.text.endsWith("끝.")).toBe(true);
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("fetchAssistantSince follows transcript continuation pages and returns the newest turn-scoped assistant", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-pages-"));
	const database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const floor = Date.now();
	const cursors: Array<string | undefined> = [];
	const run: CliRunner = async (args) => {
		if (!args.includes("transcript.list")) throw new Error(`unexpected command ${args.join(" ")}`);
		const cursorIndex = args.indexOf("--cursor");
		const cursor = cursorIndex < 0 ? undefined : args[cursorIndex + 1];
		cursors.push(cursor);
		return {
			exitCode: 0,
			stdout: JSON.stringify(
				cursor === undefined
					? {
							page: {
								items: [{ role: "assistant", ts: new Date(floor - 60_000).toISOString(), body: "old answer" }],
								complete: false,
								continuationCursor: "page-2",
							},
						}
					: {
							page: {
								items: [{ role: "assistant", ts: new Date(floor + 1_000).toISOString(), body: "current answer" }],
								complete: true,
							},
						},
			),
			stderr: "",
		};
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-pages",
		tailRunner: new TailRunner({ stream: noRelay, repo: join(home, "workspace"), stallTimeoutMs: 1_000 }),
	});
	try {
		await createOwnedSessionFixture(database, authority, {
			sessionId: "11111111-2222-3333-4444-555555555555",
			repo: join(home, "workspace"),
			originKey: "work/pages",
			epoch: 0,
		});
		const recovered = await port.fetchAssistantSince({
			sessionId: "11111111-2222-3333-4444-555555555555",
			repo: join(home, "workspace"),
			notBeforeMs: floor,
		});
		expect(cursors).toEqual([undefined, "page-2"]);
		expect(recovered).toEqual({ text: "current answer", pages: 2, complete: true });
	} finally {
		database.close();
		await rm(home, { recursive: true, force: true });
	}
});

test("close uses the global lifecycle route: the per-session control route prohibits session.close for the daemon CLI", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		if (args.includes("session.close") && args.includes("control"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: false,
					error: {
						code: "adapter_operation_prohibited",
						message: "session.close is unavailable through the SDK session CLI.",
					},
				}),
				stderr: "",
			};
		if (args.includes("session.close"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1" } }), stderr: "" };
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const port = new BrokerSessionPort({
		authority,
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: noRelay, repo: join(home, "workspace"), stallTimeoutMs: 1_000 }),
	});
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: "/tmp/repo",
		originKey: "work/close",
		epoch: 0,
	});
	await port.close({ sessionId: "sdk-1", repo: "/tmp/repo" });
	expect(calls).toHaveLength(1);
	const args = calls[0]!;
	expect(args.slice(0, 4)).toEqual(["sdk", "session", "raw", "global"]);
	expect(args).toContain("session.close");
	expect(args[args.indexOf("--idempotency-key") + 1]).toMatch(/^gw-close-instance-1-sdk-1-\d+$/);
	expect(JSON.parse(args[args.indexOf("--json-input") + 1]!)).toEqual({ sessionId: "sdk-1" });
});

test("foreign live and saved sessions and wrong-repo owned UUIDs never reach any SDK surface", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-ownership-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	const ownedId = "11111111-2222-3333-4444-555555555555";
	await createOwnedSessionFixture(database, authority, { sessionId: ownedId, originKey: "owned", epoch: 0, repo });
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: {} }), stderr: "" };
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "ownership",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	for (const target of [
		{ sessionId: "foreign-live", repo },
		{ sessionId: "foreign-saved", repo },
		{ sessionId: ownedId, repo: join(home, "other-repo") },
	]) {
		const operations: Array<() => Promise<unknown>> = [
			() => port.inspect(target),
			() => port.liveness(target),
			() => port.queueEmpty(target),
			() => port.resume({ ...target, originKey: "foreign", epoch: 0 }),
			() => port.send({ ...target, text: "must not send", opRef: "gw-foreign" }),
			() => port.steer({ ...target, text: "must not steer", clientRef: "gw-steer" }),
			() => port.setModel({ ...target, selection: "model" }),
			() => port.setModel({ ...target, selection: { preset: "profile" } }),
			() => port.setServiceTier({ ...target, tier: "default" }),
			() => port.status({ ...target, opRef: "gw-foreign" }),
			() => port.failedTurnEvidence({ ...target, startedAtMs: 0, terminalAtMs: 1 }),
			() => port.fetchWorkerOutput({ ...target, opRef: "gw-foreign", notBeforeMs: 0 }),
			() => port.fetchLastAssistant(target),
			() => port.fetchAssistantSince({ ...target, notBeforeMs: 0 }),
			() => port.attachTail({ ...target, brokerGeneration: 0 }),
			() => port.runCompaction({ ...target, originKey: "foreign" }),
			() => port.close(target),
			() => port.request({ ...target, text: "must not replay", opRef: "gw-foreign" }),
		];
		for (const operation of operations) await expect(operation()).rejects.toBeInstanceOf(BrokerAuthorityError);
	}
	expect(calls).toEqual([]);
});

test("legacy private bindings cannot initialize a global port or be resumed and rebound", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-legacy-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	database.putSessionAtEpoch("legacy", "private-session", 0);
	const authority = { canonicalAgentDir: join(home, "global"), identity: `gjc:${join(home, "global")}` };
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		throw new Error("private ID must not be interpreted globally");
	};
	expect(
		() =>
			new BrokerSessionPort({
				database: database!,
				authority,
				cli: run,
				instanceId: "legacy",
				tailRunner: new TailRunner({ stream: noRelay, repo: home }),
			}),
	).toThrow(BrokerAuthorityError);
	expect(() => database!.assertBrokerAuthority(authority, { initializeEmpty: true })).toThrow(BrokerAuthorityError);
	expect(database.getSessionRecord("legacy")).toMatchObject({ sessionId: "private-session", epoch: 0 });
	expect(calls).toEqual([]);
});

test("bind rejects an unowned persisted ID before inspect and without epoch rotation", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-unowned-binding-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "owned", originKey: "origin", epoch: 0, repo });
	const record = database.getSessionRecord("origin")!;
	// Corrupt the read boundary deliberately: no production adoption API is used.
	const getSessionRecord = database.getSessionRecord.bind(database);
	database.getSessionRecord = () => ({ ...record, sessionId: "private-session" });
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		throw new Error("unowned binding reached SDK");
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "binding",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	try {
		await expect(port.bind({ originKey: "origin", epoch: 0, repo })).rejects.toBeInstanceOf(BrokerAuthorityError);
		expect(calls).toEqual([]);
	} finally {
		database.getSessionRecord = getSessionRecord;
	}
	expect(database.getSessionRecord("origin")).toEqual(record);
});

test("historical owned bindings remain readable after epoch retirement but fail after authority cutover", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-history-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, {
		sessionId: "retired-owned",
		originKey: "origin",
		epoch: 0,
		repo,
	});
	database.close();
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	database.assertBrokerAuthority(authority);
	database.rebindEpoch("origin");
	await createOwnedSessionFixture(database, authority, {
		sessionId: "current-owned",
		originKey: "origin",
		epoch: 1,
		repo,
	});
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		return {
			exitCode: 0,
			stdout: JSON.stringify({ ok: true, page: { items: ["historical answer"], complete: true } }),
			stderr: "",
		};
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "history",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	expect((await port.fetchLastAssistant({ sessionId: "retired-owned", repo })).text).toBe("historical answer");
	expect(database.assertOwnedSession("retired-owned", repo, authority)).toMatchObject({
		originKey: "origin",
		epoch: 0,
	});
	const targetAuthority = {
		canonicalAgentDir: join(home, "other-agent"),
		identity: `gjc:${join(home, "other-agent")}`,
	};
	database.cutoverBrokerAuthority({ expectedAuthority: authority, targetAuthority, evidence: "test fixture cutover" });
	await expect(port.bind({ originKey: "origin", epoch: 2, repo })).rejects.toBeInstanceOf(BrokerAuthorityError);
	await expect(port.fetchLastAssistant({ sessionId: "retired-owned", repo })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
	expect(calls).toHaveLength(1);
});

test("authority failures propagate through recovery catches without rebind or replay", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-authority-error-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "owned", originKey: "origin", epoch: 0, repo });
	const refusal = new BrokerAuthorityError("authority_mismatch");
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		throw refusal;
	};
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "failure",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	});
	const target = { sessionId: "owned", repo };
	for (const operation of [
		() => port.bind({ originKey: "origin", epoch: 0, repo }),
		() => port.bind({ originKey: "new-origin", epoch: 0, repo }),
		() => port.inspect(target),
		() => port.liveness(target),
		() => port.runCompaction({ ...target, originKey: "origin" }),
		() => port.fetchWorkerOutput({ ...target, opRef: "gw-owned", notBeforeMs: 0 }),
	])
		await expect(operation()).rejects.toBe(refusal);
	expect(database.getSessionRecord("origin")).toMatchObject({ sessionId: "owned", epoch: 0 });
	expect(database.getSessionRecord("new-origin")).toBeUndefined();
	expect(calls.filter((args) => args.includes("session.create"))).toHaveLength(1);
	expect(calls.some((args) => args.includes("session.resume") || args.includes("send"))).toBe(false);
});

test("owned SDK reads and controls ignore unrelated corrupt lane history without hiding census failures", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-isolated-history-"));
	const path = join(home, "gateway.db");
	database = await GatewayDatabase.open(path);
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "owned", originKey: "origin", epoch: 0, repo });
	const raw = new Database(path);
	try {
		raw
			.query(
				"INSERT INTO lane_jobs(job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				"lanejob-unrelated",
				"work-unrelated",
				"main",
				"/unrelated",
				"awaiting_operator",
				"{",
				"2026-09-08T00:00:00.000Z",
				"2026-09-08T00:00:00.000Z",
			);
	} finally {
		raw.close();
	}
	const calls: string[][] = [];
	const run: CliRunner = async (args) => {
		calls.push([...args]);
		return {
			exitCode: 0,
			stdout: JSON.stringify({
				ok: true,
				page: { items: ["owned answer"], complete: true },
				result: { changed: true },
			}),
			stderr: "",
		};
	};
	const options = {
		database,
		authority,
		cli: run,
		instanceId: "isolated-history",
		tailRunner: new TailRunner({ stream: noRelay, repo }),
	};
	const port = new BrokerSessionPort(options);
	expect((await port.fetchLastAssistant({ sessionId: "owned", repo })).text).toBe("owned answer");
	expect(await port.setModel({ sessionId: "owned", repo, selection: "model" })).toEqual({ changed: true });
	expect(calls).toHaveLength(2);
	expect(() => database!.inspectBrokerAuthority()).toThrow();
	expect(database.laneJobJson("lanejob-unrelated")).toBe("{");
	expect(() =>
		database!.cutoverBrokerAuthority({
			expectedAuthority: authority,
			targetAuthority: { ...authority, identity: "different" },
			evidence: "must validate history",
			disposition: "quarantine",
		}),
	).toThrow();
	await expect(port.fetchLastAssistant({ sessionId: "foreign", repo })).rejects.toBeInstanceOf(BrokerAuthorityError);
	await expect(port.setModel({ sessionId: "foreign", repo, selection: "model" })).rejects.toBeInstanceOf(
		BrokerAuthorityError,
	);
	expect(() => new BrokerSessionPort({ ...options, authority: { ...authority, identity: "wrong" } })).toThrow(
		"authority_mismatch",
	);
	expect(calls).toHaveLength(2);
});
for (const fixture of [
	{ reply: { ok: false, error: { code: "busy" } }, refused: true },
	{ reply: { ok: false, error: { code: "session_unavailable" } }, refused: false },
	{ reply: { ok: true, result: { accepted: false, status: "rejected", error: { code: "busy" } } }, refused: false },
	{
		reply: {
			ok: true,
			result: { accepted: false, status: "rejected", clientRef: "expected-ref", error: { code: "busy" } },
		},
		refused: true,
	},
	{
		reply: {
			ok: true,
			result: { accepted: false, status: "rejected", clientRef: "wrong-ref", error: { code: "busy" } },
		},
		refused: false,
	},
	{ reply: { ok: true, result: { accepted: true, clientRef: "wrong-ref" } }, refused: false },
	{ reply: { ok: true, result: { accepted: true } }, refused: false },
	{ reply: { ok: true, result: { accepted: true, status: "rejected", clientRef: "expected-ref" } }, refused: false },
	{
		reply: { ok: true, result: { accepted: true, status: "accepted", clientRef: "expected-ref", ok: false } },
		refused: false,
	},
	{ reply: { ok: true, result: {} }, refused: false },
] as const)
	test(`steer preserves authoritative rejection versus ambiguity: ${JSON.stringify(fixture)}`, async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
		database = await GatewayDatabase.open(join(home, "gateway.db"));
		const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
		await createOwnedSessionFixture(database, authority, {
			sessionId: "sdk-1",
			repo: "/tmp/repo",
			originKey: "steer-receipt",
			epoch: 0,
		});
		const run: CliRunner = async () => {
			throw new Error("steer must not spawn a CLI");
		};
		const relay = scriptedRelay((request) => {
			expect(request).toEqual({
				type: "control_request",
				operation: "turn.steer",
				input: { text: "input", clientRef: "expected-ref" },
			});
			return fixture.reply as ReturnType<Parameters<typeof scriptedRelay>[0]>;
		});
		const port = new BrokerSessionPort({
			database,
			authority,
			cli: run,
			instanceId: "instance-1",
			tailRunner: new TailRunner({ stream: relay.spawn, repo: join(home, "workspace"), stallTimeoutMs: 1_000 }),
		});
		let failure: unknown;
		try {
			await port.steer({ sessionId: "sdk-1", repo: "/tmp/repo", text: "input", clientRef: "expected-ref" });
		} catch (error) {
			failure = error;
		}
		expect(failure).toBeInstanceOf(GjcCliError);
		expect(((failure as GjcCliError).details as { refused?: boolean } | undefined)?.refused === true).toBe(
			fixture.refused,
		);
	});

test("a relay that dies before the steer reply is ambiguity, never a definitive rejection", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: "/tmp/repo",
		originKey: "steer-torn",
		epoch: 0,
	});
	const relay = scriptedRelay(() => new Promise(() => {}));
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: async () => {
			throw new Error("steer must not spawn a CLI");
		},
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo: join(home, "workspace"), requestTimeoutMs: 50 }),
	});
	const attempt = port.steer({ sessionId: "sdk-1", repo: "/tmp/repo", text: "input", clientRef: "expected-ref" });
	await Bun.sleep(5);
	relay.streams[0]!.close();
	const failure = await attempt.catch((error: unknown) => error);
	expect(failure).toBeInstanceOf(Error);
	expect((failure as { details?: { refused?: boolean } }).details?.refused).toBeUndefined();
	expect(isDefinitiveSteerRejection(failure)).toBe(false);
});

test("send waits out a `busy` refusal and resends under the same op-ref once the turn is free", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));

	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: join(home, "workspace"),
		originKey: "busy-wait",
		epoch: 0,
	});
	const repo = join(home, "workspace");
	const sends: string[] = [];
	const sleeps: number[] = [];
	let clock = 0;
	const run: CliRunner = async (args) => {
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	const relay = scriptedRelay((request) => {
		if (request.operation !== "turn.prompt") throw new Error(`unexpected request ${request.operation}`);
		sends.push(String(request.input.clientRef));
		if (sends.length < 3)
			return { ok: false, error: { code: "busy", message: "turn.prompt is unavailable while the agent is busy" } };
		return { ok: true, result: { commandId: "c", turnId: "t", accepted: true } };
	});
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo }),
		now: () => clock,
		sleep: async (ms) => {
			sleeps.push(ms);
			clock += ms;
		},
	});
	const receipt = await port.send({ sessionId: "sdk-1", repo, text: "hi", opRef: "gw-p-busy1" });
	expect(receipt.operationRef).toBe("gw-p-busy1");
	expect(sends).toEqual(["gw-p-busy1", "gw-p-busy1", "gw-p-busy1"]);
	expect(sleeps).toEqual([2_000, 2_000]);
});

test("send surfaces `busy` once the bounded wait is exhausted, never having sent", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));

	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	await createOwnedSessionFixture(database, authority, {
		sessionId: "sdk-1",
		repo: join(home, "workspace"),
		originKey: "busy-exhaust",
		epoch: 0,
	});
	const repo = join(home, "workspace");
	let clock = 0;
	let sends = 0;
	const run: CliRunner = async () => {
		throw new Error("unexpected command");
	};
	const relay = scriptedRelay(() => {
		sends++;
		return { ok: false, error: { code: "busy", message: "busy" } };
	});
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo }),
		now: () => clock,
		sleep: async (ms) => {
			clock += ms;
		},
	});
	await expect(
		port.send({ sessionId: "sdk-1", repo, text: "hi", opRef: "gw-p-busy2", busyWaitMs: 5_000 }),
	).rejects.toThrow(/busy/);
	// Attempts at 0s, 2s, 4s, 6s; the 6s refusal lands past the 5s deadline and surfaces.
	expect(sends).toBe(4);
});

test("request keeps observing an accepted op on the CLI when its relay tears mid-turn, never abandoning it", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-session-port-"));
	database = await GatewayDatabase.open(join(home, "gateway.db"));
	const authority = initializeTestBrokerAuthority(database, join(home, "agent"));
	const repo = join(home, "workspace");
	await createOwnedSessionFixture(database, authority, { sessionId: "sdk-1", repo, originKey: "torn", epoch: 0 });
	const cliStatus: string[] = [];
	let cliReports = 0;
	const run: CliRunner = async (args) => {
		if (args.includes("status")) {
			cliStatus.push(args[args.indexOf("status") + 2] ?? "");
			cliReports += 1;
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: {
						operationRef: "gw-torn-1",
						status: { status: cliReports < 2 ? "in_flight" : "terminal_ok", clientRef: "gw-torn-1" },
						summary: { completed: cliReports >= 2 },
					},
				}),
				stderr: "",
			};
		}
		if (args.includes("session.last_assistant"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ type: "query_response", ok: true, page: { items: ["survived"], complete: true } }),
				stderr: "",
			};
		throw new Error(`unexpected command ${args.join(" ")}`);
	};
	let relayQueries = 0;
	const relay = scriptedRelay((request) => {
		if (request.operation === "turn.prompt")
			return { ok: true, result: { commandId: "c", turnId: "t", accepted: true, clientRef: request.input.clientRef } };
		relayQueries += 1;
		// The relay tears on the first status read: the answer never comes.
		relay.streams[0]!.close();
		return new Promise(() => {});
	});
	const port = new BrokerSessionPort({
		database,
		authority,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ stream: relay.spawn, repo, requestTimeoutMs: 200 }),
	});
	const result = await port.request({ sessionId: "sdk-1", repo, text: "go", opRef: "gw-torn-1", pollMs: 0 });
	expect(result.status.status.status).toBe("terminal_ok");
	expect(result.assistant.text).toBe("survived");
	expect(relayQueries).toBe(1);
	// The SAME clientRef was observed on the CLI after the tear; nothing was re-sent.
	expect(cliStatus.every((ref) => ref === "gw-torn-1")).toBe(true);
	expect(cliReports).toBe(2);
	expect(relay.requests.filter((request) => request.operation === "turn.prompt")).toHaveLength(1);
});
