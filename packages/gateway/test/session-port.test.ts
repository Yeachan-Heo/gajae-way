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
		if (args.includes("send"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({ ok: true, result: { sessionId: "sdk-1", commandId: "cmd-1" } }),
				stderr: "",
			};
		if (args.includes("status"))
			return {
				exitCode: 0,
				stdout: JSON.stringify({
					ok: true,
					result: { operationRef: "gw-work-1", status: { status: "terminal_ok" }, summary: { completed: true } },
				}),
				stderr: "",
			};
		if (args.includes("tail"))
			return { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { items: [], terminal: true } }), stderr: "" };
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
	const tailRunner = new TailRunner({ run, repo: join(home, "workspace"), stallTimeoutMs: 1_000 });
	const port = new BrokerSessionPort({
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
	const send = calls.find((args) => args.includes("send"))!;
	expect(send).toEqual(
		expect.arrayContaining(["--op-ref", "gw-work-1", "--text", "trusted bootstrap\n\nimplement it"]),
	);
	expect(calls.find((args) => args.includes("model.profile.set"))).toEqual(
		expect.arrayContaining(["raw", "control", "sdk-1", "--op", "model.profile.set"]),
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
		database,
		cli: run,
		instanceId: "instance-1",
		tailRunner: new TailRunner({ run, repo }),
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
	await expect(
		port.bind({ originKey: "loopback/loopback/retry", epoch: 0, repo: join(home, "workspace") }),
	).resolves.toMatchObject({
		sessionId: "sdk-after-retry",
	});
	expect(createCalls).toBe(2);
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
	try {
		const repo = join(home, "workspace");
		database.putSessionAtEpoch("monitor/eventtype/x", "dead-session", 1);
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
			database,
			cli,
			instanceId: "i",
			tailRunner: new TailRunner({ run: cli, repo }),
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
	const tailRunner = new TailRunner({ run, repo: join(home, "workspace"), stallTimeoutMs: 1_000 });
	const port = new BrokerSessionPort({ database, cli: run, instanceId: "instance-body", tailRunner });
	try {
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
		database,
		cli: run,
		instanceId: "instance-pages",
		tailRunner: new TailRunner({ run, repo: join(home, "workspace"), stallTimeoutMs: 1_000 }),
	});
	try {
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
