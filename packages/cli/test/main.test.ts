import { describe, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";

import { tmpdir } from "node:os";
import { join } from "node:path";
import type { OpsCycleResult, OpsSloResult } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import pkg from "../package.json";

import {
	CLI_USAGE,
	COMMANDS,
	cycleExitCode,
	main,
	parseArgs,
	parseServicesArgs,
	renderCycle,
	renderSlo,
	sloExitCode,
	socketPath,
	USAGE_EXIT_CODE,
	usageFor,
} from "../src/main";
import { installServices, resolvePath, serviceSpecs } from "../src/services";

async function runOpsSloCli(result: OpsSloResult, args: readonly string[]) {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-slo-"));
	const path = join(home, "gateway.sock");
	const requests: Array<{ verb: string; params: unknown }> = [];
	const output: string[] = [];
	const errors: string[] = [];
	const originalLog = console.log;
	const originalError = console.error;
	const previousExit = process.exitCode;
	console.log = (line: unknown) => output.push(String(line));
	console.error = (line: unknown) => errors.push(String(line));
	process.exitCode = 0;
	const listener = Bun.listen<{ buffer: string }>({
		unix: path,
		socket: {
			open(socket) {
				socket.data = { buffer: "" };
			},
			data(socket, data) {
				socket.data.buffer += Buffer.from(data).toString("utf8");
				let newline = socket.data.buffer.indexOf("\n");
				while (newline >= 0) {
					const line = socket.data.buffer.slice(0, newline);
					socket.data.buffer = socket.data.buffer.slice(newline + 1);
					const frame = JSON.parse(line) as { type: string; id?: string; verb?: string; params?: unknown };
					if (frame.type === "hello")
						socket.write(`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`);
					else if (frame.type === "request") {
						requests.push({ verb: frame.verb!, params: frame.params });
						socket.write(`${JSON.stringify({ v: "0.1", type: "response", id: frame.id, result })}\n`);
					}
					newline = socket.data.buffer.indexOf("\n");
				}
			},
		},
	});
	try {
		await main(["--socket", path, "ops", "slo", ...args]);
		return { requests, output, errors, exitCode: process.exitCode ?? 0 };
	} finally {
		console.log = originalLog;
		console.error = originalError;
		process.exitCode = previousExit;
		listener.stop(true);
		await rm(home, { recursive: true, force: true });
	}
}

function sampleSloResult(status: "ok" | "failed" | "insufficient" | "pending"): OpsSloResult {
	return {
		days: [
			{
				date: "2026-10-09",
				status,
				complete: status !== "insufficient",
				aliveMs: 86_400_000,
				maxUncoveredGapMs: 0,
				broker_unavailable: status === "failed" ? 1 : 0,
				persona_bind_failed: 0,
				excluded: 0,
			},
		],
		windows: [
			{
				id: "update-1",
				trigger: "update",
				state: "ok",
				requestedAt: "2026-10-09T10:00:00.000Z",
				finishedAt: "2026-10-09T10:31:00.000Z",
				verified: true,
				eligibleStart: "2026-10-09T10:00:00.000Z",
				eligibleEnd: "2026-10-09T10:30:00.000Z",
				overCapMs: 60_000,
			},
		],
		ok: status === "ok",
	};
}

describe("ops slo exit contract", () => {
	test("sends --days, renders provenance, and exits zero only for an ok result", async () => {
		const result = sampleSloResult("ok");
		const run = await runOpsSloCli(result, ["--days", "1", "--json"]);
		expect(run.requests).toEqual([{ verb: "ops.slo", params: { days: 1 } }]);
		expect(run.exitCode).toBe(0);
		expect(JSON.parse(run.output[0]!)).toEqual(result);
		expect(sloExitCode(result)).toBe(0);
		expect(renderSlo(result).join("\\n")).toContain("verified=true");
		expect(renderSlo(result).join("\\n")).toContain("over_cap_ms=60000");
	});

	test("pending has a rerun-later message and a nonzero exit", async () => {
		const run = await runOpsSloCli(sampleSloResult("pending"), ["--days", "1"]);
		expect(run.exitCode).toBe(1);
		expect(run.output[0]).toContain("SLO pending");
		expect(run.output[0]).toContain("rerun later");
	});

	test("failed and insufficient are distinct nonzero outcomes", async () => {
		const failed = await runOpsSloCli(sampleSloResult("failed"), []);
		const insufficient = await runOpsSloCli(sampleSloResult("insufficient"), []);
		expect(failed.exitCode).toBe(1);
		expect(failed.output[0]).toContain("SLO failed");
		expect(insufficient.exitCode).toBe(1);
		expect(insufficient.output[0]).toContain("SLO insufficient");
	});
});
describe("cli arguments", () => {
	test("resolves home and socket override", () => {
		expect(socketPath("/tmp/gajae")).toBe("/tmp/gajae/gateway.sock");
		expect(parseArgs(["--socket", "/tmp/x", "status"])).toEqual({ command: "status", rest: [], socket: "/tmp/x" });
	});

	test("--version prints the package version without requiring a gateway", async () => {
		const output: string[] = [];
		const originalLog = console.log;
		console.log = (line: unknown) => output.push(String(line));
		try {
			await main(["--version"]);
			expect(output).toEqual([pkg.version]);
		} finally {
			console.log = originalLog;
		}
	});

	test("an unknown memory-audit argument is refused instead of degrading to a plain audit", async () => {
		// `memory audit --fix` used to connect, run the read-only audit and exit 1,
		// which reads as a repair attempt that reproduced the failure.
		const errors: string[] = [];
		const console_error = console.error;
		console.error = (message: unknown) => errors.push(String(message));
		const previousExit = process.exitCode;
		try {
			await main(["--socket", join(tmpdir(), "gajaeway-absent.sock"), "memory", "audit", "--fix"]);
			await main(["--socket", join(tmpdir(), "gajaeway-absent.sock"), "memory", "autolink", "--dry-run"]);
			expect(errors.join("\n")).toContain("unknown argument: --fix");
			expect(errors.join("\n")).toContain("unknown argument: --dry-run");
			expect(process.exitCode).toBe(1);
		} finally {
			console.error = console_error;
			process.exitCode = previousExit ?? 0;
		}
	});

	test("ops redeliver sends either the delivery id or an ISO since value", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-redeliver-"));
		const path = join(home, "fake.sock");
		const received: Array<{ verb: string; params: unknown }> = [];
		const output: string[] = [];
		const originalLog = console.log;
		console.log = (line: unknown) => output.push(String(line));
		const listener = Bun.listen<{ buffer: string }>({
			unix: path,
			socket: {
				open(socket) {
					socket.data = { buffer: "" };
				},
				data(socket, data) {
					socket.data.buffer += Buffer.from(data).toString("utf8");
					let newline = socket.data.buffer.indexOf("\n");
					while (newline >= 0) {
						const line = socket.data.buffer.slice(0, newline);
						socket.data.buffer = socket.data.buffer.slice(newline + 1);
						const frame = JSON.parse(line) as { type: string; id?: string; verb?: string; params?: unknown };
						if (frame.type === "hello")
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`,
							);
						else if (frame.type === "request") {
							received.push({ verb: frame.verb as string, params: frame.params });
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "response", id: frame.id, result: { requeued: [] } })}\n`,
							);
						}
						newline = socket.data.buffer.indexOf("\n");
					}
				},
			},
		});
		try {
			await main(["--socket", path, "ops", "redeliver", "delivery-42"]);
			await main(["--socket", path, "ops", "redeliver", "--since", "2026-09-23T10:00:00.000Z"]);
			expect(received).toEqual([
				{ verb: "ops.redeliver", params: { deliveryId: "delivery-42" } },
				{ verb: "ops.redeliver", params: { since: "2026-09-23T10:00:00.000Z" } },
			]);
			expect(output).toEqual(['{"requeued":[]}', '{"requeued":[]}']);
			expect(CLI_USAGE).toContain("ops redeliver <deliveryId>");
			expect(CLI_USAGE).toContain("ops redeliver --since <iso>");
		} finally {
			console.log = originalLog;
			listener.stop(true);
			await rm(home, { recursive: true, force: true });
		}
	});
	test("ops restore is unknown and ops deliveries filters and renders the SDK projection", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-deliveries-"));
		const path = join(home, "fake.sock");
		const received: Array<{ verb: string; params: unknown }> = [];
		const errors: string[] = [];
		const output: string[] = [];
		const originalLog = console.log;
		const originalError = console.error;
		const previousExit = process.exitCode;
		const row = {
			deliveryId: "delivery-42",
			turnId: "turn-42",
			originKey: "discord:channel:channel-42",
			conversationId: "channel-42",
			state: "confirmed",
			createdAt: "2026-10-01T00:00:00.000Z",
			textPrefix: "delivery text",
		};
		console.log = (line: unknown) => output.push(String(line));
		console.error = (line: unknown) => errors.push(String(line));
		const listener = Bun.listen<{ buffer: string }>({
			unix: path,
			socket: {
				open(socket) {
					socket.data = { buffer: "" };
				},
				data(socket, data) {
					socket.data.buffer += Buffer.from(data).toString("utf8");
					let newline = socket.data.buffer.indexOf("\n");
					while (newline >= 0) {
						const line = socket.data.buffer.slice(0, newline);
						socket.data.buffer = socket.data.buffer.slice(newline + 1);
						const frame = JSON.parse(line) as { type: string; id?: string; verb?: string; params?: unknown };
						if (frame.type === "hello")
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`,
							);
						else if (frame.type === "request") {
							received.push({ verb: frame.verb as string, params: frame.params });
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "response", id: frame.id, result: { rows: [row] } })}\n`,
							);
						}
						newline = socket.data.buffer.indexOf("\n");
					}
				},
			},
		});
		try {
			await main(["--socket", path, "ops", "restore", "/tmp/backup.db"]);
			expect(errors.join("\n")).toContain("unknown ops command: restore");
			expect(received).toEqual([]);
			process.exitCode = previousExit ?? 0;

			await main([
				"--socket",
				path,
				"ops",
				"deliveries",
				"--since",
				"2026-10-01T00:00:00.000Z",
				"--state",
				"confirmed",
				"--limit",
				"25",
				"--json",
			]);
			await main(["--socket", path, "ops", "deliveries"]);
			expect(received).toEqual([
				{
					verb: "ops.deliveries",
					params: { since: "2026-10-01T00:00:00.000Z", state: "confirmed", limit: 25 },
				},
				{ verb: "ops.deliveries", params: {} },
			]);
			expect(output).toEqual([
				JSON.stringify({ rows: [row] }),
				"deliveries: 1",
				'2026-10-01T00:00:00.000Z confirmed delivery=delivery-42 turn=turn-42 origin=discord:channel:channel-42 conversation=channel-42 text="delivery text"',
			]);
			expect(CLI_USAGE).toContain("ops deliveries [--since ISO]");
			expect(CLI_USAGE).not.toContain("ops restore");
		} finally {
			console.log = originalLog;
			console.error = originalError;
			process.exitCode = previousExit ?? 0;
			listener.stop(true);
			await rm(home, { recursive: true, force: true });
		}
	});

	test("monitors test --wait returns streamed terminal and timeout stages without polling", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-monitor-wait-"));
		const path = join(home, "fake.sock");
		const received: string[] = [];
		const output: string[] = [];
		const originalLog = console.log;
		console.log = (line: unknown) => output.push(String(line));
		const listener = Bun.listen<{ buffer: string }>({
			unix: path,
			socket: {
				open(socket) {
					socket.data = { buffer: "" };
				},
				data(socket, data) {
					socket.data.buffer += Buffer.from(data).toString("utf8");
					let newline = socket.data.buffer.indexOf("\n");
					while (newline >= 0) {
						const line = socket.data.buffer.slice(0, newline);
						socket.data.buffer = socket.data.buffer.slice(newline + 1);
						const frame = JSON.parse(line) as { type: string; id?: string; verb?: string };
						if (frame.type === "hello")
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`,
							);
						else if (frame.type === "request") {
							received.push(frame.verb as string);
							const eventId = `event-${41 + received.length}`;
							const stage = received.length === 1 ? "delivered" : "authored";
							const event = {
								v: "0.1",
								type: "event",
								event: "monitor.event",
								payload: {
									eventId,
									monitorId: "monitor-1",
									eventType: "test.event",
									firedAt: "2026-09-25T10:00:00.000Z",
									stage,
								},
							};
							const response = {
								v: "0.1",
								type: "response",
								id: frame.id,
								result: { eventId },
							};
							// Exercise the event-before-response race: delivery may settle before
							// the caller has learned the event id from monitor.test.
							socket.write(`${JSON.stringify(event)}\n${JSON.stringify(response)}\n`);
						}
						newline = socket.data.buffer.indexOf("\n");
					}
				},
			},
		});
		try {
			await main(["--socket", path, "monitors", "test", "monitor-1", "--wait=1"]);
			await main(["--socket", path, "monitors", "test", "monitor-1", "--wait=0"]);
			expect(received).toEqual(["monitor.test", "monitor.test"]);
			expect(output).toEqual([
				'{"eventId":"event-42","stage":"delivered"}',
				'{"eventId":"event-43","stage":"authored"}',
			]);
		} finally {
			console.log = originalLog;
			listener.stop(true);
			await rm(home, { recursive: true, force: true });
		}
	});

	test("monitors test rejects invalid wait options before connecting", async () => {
		const errors: string[] = [];
		const originalError = console.error;
		const previousExitCode = process.exitCode ?? 0;
		console.error = (line: unknown) => errors.push(String(line));
		try {
			const cases = [
				["--wait=-1"],
				["--wait=1.5"],
				["--wait=86401"],
				["--wait=invalid"],
				["--wait="],
				["--wait=1", "--wait=2"],
			];
			for (const options of cases) {
				await main([
					"--socket",
					"/nonexistent/gajaeway-monitor-wait.sock",
					"monitors",
					"test",
					"monitor-1",
					...options,
				]);
			}
			expect(errors).toHaveLength(6);
			expect(errors.slice(0, 5).every((error) => error.includes("--wait expects a finite non-negative integer"))).toBe(
				true,
			);
			expect(errors[5]).toContain("duplicate --wait");
		} finally {
			console.error = originalError;
			process.exitCode = previousExitCode;
		}
	});
});

describe("service installation", () => {
	test("install and repair select the same idempotent writer contract", () => {
		expect(parseServicesArgs(["install", "--bin-dir", "/opt/gajaeway/bin"]).action).toBe("install");
		expect(parseServicesArgs(["repair", "--bin-dir", "/opt/gajaeway/bin"]).action).toBe("repair");
	});
	test("managed, login, and fallback PATH entries are ordered and deduplicated", async () => {
		const commands: string[][] = [];
		const path = await resolvePath({
			binDir: "/opt/gajaeway/bin",
			home: "/Users/operator",
			env: { SHELL: "/bin/fish" },
			runtime: {},
			loginPathRunner: async (command) => {
				commands.push([...command]);
				return "~/bin:/opt/gajaeway/bin::/usr/bin:/usr/local/bin";
			},
		});
		expect(commands).toEqual([["/bin/fish", "-lc", 'printf %s "$PATH"']]);
		expect(path.split(":")).toEqual([
			"/opt/gajaeway/bin",
			"/Users/operator/bin",
			"/Users/operator/.local/bin",
			"/Users/operator/.bun/bin",
			"/usr/bin",
			"/usr/local/bin",
			"/bin",
			"/usr/sbin",
			"/sbin",
		]);
	});

	test("explicit PATH and disabled inheritance never invoke login discovery", async () => {
		let calls = 0;
		const runner = async (): Promise<string> => {
			calls++;
			throw new Error("login discovery must not run");
		};
		const explicit = await resolvePath({
			binDir: "/opt/bin",
			home: "/Users/operator",
			runtime: { path: ["~/custom", "/usr/bin"] },
			loginPathRunner: runner,
		});
		const disabled = await resolvePath({
			binDir: "/opt/bin",
			home: "/Users/operator",
			runtime: { inheritLoginPath: false },
			loginPathRunner: runner,
		});
		expect(calls).toBe(0);
		expect(explicit.split(":")).toEqual([
			"/opt/bin",
			"/Users/operator/bin",
			"/Users/operator/.local/bin",
			"/Users/operator/.bun/bin",
			"/Users/operator/custom",
			"/usr/bin",
			"/usr/local/bin",
			"/bin",
			"/usr/sbin",
			"/sbin",
		]);
		expect(disabled.split(":")).toEqual([
			"/opt/bin",
			"/Users/operator/bin",
			"/Users/operator/.local/bin",
			"/Users/operator/.bun/bin",
			"/usr/local/bin",
			"/usr/bin",
			"/bin",
			"/usr/sbin",
			"/sbin",
		]);
	});

	test("install and repair each rewrite exactly five safe LaunchAgent plists", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-services-"));
		try {
			await writeFile(
				join(home, "config.json"),
				JSON.stringify({
					schemaVersion: 1,
					credentials: { provider: { credentialFile: "TOP_SECRET" } },
					runtime: { path: ["~/custom", "/usr/bin"] },
				}),
			);
			const env = { HOME: "/Users/operator", GAJAEWAY_HOME: home };
			const writes: Array<{ path: string; contents: string }> = [];
			const writer = async (path: string, contents: string): Promise<void> => {
				writes.push({ path, contents });
			};
			const options = {
				binDir: "~/gaja&<bin>",
				// This case is about the launchd definitions, so it pins the platform
				// rather than depending on the host the suite happens to run on.
				platform: "darwin" as const,
				launchAgentsDir: join(home, "LaunchAgents"),
				env,
				loginPathRunner: async (): Promise<string> => "/should-not-be-read",
				writeFile: writer,
			};
			const installed = await installServices(options);
			expect(installed).toHaveLength(serviceSpecs().length);
			expect(writes).toHaveLength(serviceSpecs().length);

			expect(writes.map(({ path }) => path)).toEqual([...installed]);
			for (const { contents } of writes) {
				expect(contents).not.toContain("TOP_SECRET");
				expect(contents).toContain("&amp;");
				expect(contents).toContain("&lt;");
				expect(contents).toContain("&gt;");
				expect(contents).toContain("<key>GAJAEWAY_HOME</key>");
				expect(contents).toContain("<key>PATH</key>");
				expect(contents).toContain("StandardOutPath");
				expect(contents).toContain("StandardErrorPath");
			}
			const gateway = writes.find(({ path }) => path.endsWith("dev.gajaeway.gateway.plist"))?.contents ?? "";
			expect(gateway).toContain("<string>daemon</string>");
			expect(gateway).toContain("gateway.stdout.log");
			expect(gateway).toContain("gateway.stderr.log");
			const slack = writes.find(({ path }) => path.endsWith("dev.gajaeway.adapter-slack.plist"))?.contents ?? "";
			expect(slack).toContain("gajaeway-slack</string>");
			expect(slack).toContain("adapter-slack.stdout.log");
			const telegram = writes.find(({ path }) => path.endsWith("dev.gajaeway.adapter-telegram.plist"))?.contents ?? "";
			expect(telegram).toContain("gajaeway-telegram</string>");
			expect(telegram).toContain("adapter-telegram.stdout.log");
			writes.length = 0;
			await installServices(options);
			expect(writes).toHaveLength(serviceSpecs().length);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	});

	test("invalid service selection fails before any plist write", async () => {
		const writes: string[] = [];
		const errors: string[] = [];
		const originalError = console.error;
		const previousExitCode = process.exitCode;
		console.error = (line: unknown) => errors.push(String(line));
		try {
			await main(["services", "unknown", "--bin-dir", "/tmp/bin"], {
				services: {
					writeFile: async (path) => {
						writes.push(path);
					},
				},
			});
		} finally {
			console.error = originalError;
			process.exitCode = previousExitCode;
		}
		expect(writes).toHaveLength(0);
		expect(errors[0]).toContain("usage: gajaeway services install|repair");
	});
});

describe("list flag validation", () => {
	// A bad column name must not require a reachable gateway to be reported.
	const cases = [
		["monitors", "list", "--fields", "bogus"],
		["sessions", "list", "--fields", "bogus"],
		["monitors", "list", "--limit", "nope"],
	];
	for (const args of cases)
		test(`${args.join(" ")} fails before connecting`, async () => {
			const errors: string[] = [];
			const original = console.error;
			console.error = (line: string) => errors.push(line);
			const previousExit = process.exitCode ?? 0;
			try {
				await main(["--socket", "/nonexistent/gajaeway-list-test.sock", ...args]);
			} finally {
				console.error = original;
				process.exitCode = previousExit;
			}
			expect(errors).toHaveLength(1);
			expect(errors[0]).toMatch(/^(unknown field\(s\): bogus \(valid: |--limit expects a non-negative integer)/);
		});
});

describe("monitor update", () => {
	async function run(commands: string[][]) {
		const requests: Array<{ verb: string; params: unknown }> = [];
		const lines: string[] = [];
		const errors: string[] = [];
		const client: GajaewayClient = Object.create(GajaewayClient.prototype);
		client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
			requests.push({ verb, params });
			return { monitorId: "monitor-42" } as T;
		};
		client.close = async () => {};
		const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);

		const log = spyOn(console, "log").mockImplementation((line) => {
			lines.push(String(line));
		});
		const error = spyOn(console, "error").mockImplementation((line) => {
			errors.push(String(line));
		});
		const previousExit = process.exitCode;
		try {
			for (const args of commands) await main(["--socket", "/test/monitor-update.sock", "monitors", "update", ...args]);
			return { requests, lines, errors, connections: connect.mock.calls.length, exitCode: process.exitCode };
		} finally {
			connect.mockRestore();
			log.mockRestore();
			error.mockRestore();
			process.exitCode = previousExit;
		}
	}

	test("sends schedule, enabled, combined shorthand, and partial JSON updates flat", async () => {
		const result = await run([
			["monitor-42", "--schedule", "30 8 * * 1-5"],
			["monitor-42", "--enabled", "false"],
			["monitor-42", "--enabled", "true", "--schedule", "0 9 * * *"],
			["monitor-42", "--json", '{"enabled":true,"instruction":"Review the queue."}'],
		]);
		expect(result.requests).toEqual([
			{ verb: "monitor.update", params: { monitorId: "monitor-42", schedule: "30 8 * * 1-5" } },
			{ verb: "monitor.update", params: { monitorId: "monitor-42", enabled: false } },
			{
				verb: "monitor.update",
				params: { monitorId: "monitor-42", schedule: "0 9 * * *", enabled: true },
			},
			{
				verb: "monitor.update",
				params: { monitorId: "monitor-42", enabled: true, instruction: "Review the queue." },
			},
		]);
		expect(result.lines).toEqual(Array(4).fill('{"monitorId":"monitor-42"}'));
		expect(result.errors).toEqual([]);
		expect(result.connections).toBe(4);
	});

	const invalidCases: Array<[string, string[]]> = [
		["missing id", ["--enabled", "true"]],
		["missing patch", ["monitor-42"]],
		["unknown option", ["monitor-42", "--verbose"]],
		["missing schedule value", ["monitor-42", "--schedule"]],
		["missing enabled value", ["monitor-42", "--enabled"]],
		["invalid enabled value", ["monitor-42", "--enabled", "yes"]],
		["conflicting JSON and shorthand", ["monitor-42", "--json", "{}", "--schedule", "0 9 * * *"]],
		["repeated shorthand", ["monitor-42", "--enabled", "true", "--enabled", "false"]],
		["unexpected positional argument", ["monitor-42", "--enabled", "true", "extra"]],
	];
	for (const [name, args] of invalidCases)
		test(`rejects ${name} with usage before connecting`, async () => {
			const result = await run([args]);
			expect(result.requests).toEqual([]);
			expect(result.errors).toHaveLength(1);
			expect(result.errors[0]).toContain("usage: gajaeway monitors update <id>");
			expect(result.connections).toBe(0);
		});
});

function cycleResult(overrides: Partial<OpsCycleResult> = {}): OpsCycleResult {
	return {
		phase: "idle",
		gates: [],
		generatedAt: "2026-08-26T00:00:00.000Z",
		instanceId: "inst-1",
		memoryClosing: false,
		sessions: [],
		memoryIntents: { queued: 0, written: 0, committed: 0, receipted: 0, quarantined: 0 },
		memoryGitIndexLock: null,
		monitorEvents: [],
		monitorAuthoringLost: [],
		deliveries: { pending: 0, inflight: 0, confirmed: 0, failedAmbiguous: 0, expired: 0 },
		inFlightInbound: 0,
		pendingInbound: 0,
		contextDiff: {
			unread: 0,
			expired: 0,
			truncated: 0,
			omittedOldestAt: null,
			omittedNewestAt: null,
			floorAt: null,
		},
		lanes: { active: 0, max: 8 },
		agentDisk: null,
		brokerAgent: null,
		slo: { days: [], windows: [], ok: false },
		...overrides,
	};
}

describe("cycle rendering", () => {
	test("healthy cycle renders phase and explicit none-gate, no sessions block", () => {
		const lines = renderCycle(cycleResult());
		expect(lines[0]).toBe("phase: idle");
		expect(lines).toContain("gates: none");
		expect(lines.join("\n")).not.toContain("sessions:");
	});
	test("broker-agent drift renders only entries that are not in sync", () => {
		const lines = renderCycle(
			cycleResult({
				brokerAgent: {
					status: "provisioned",
					provisionedAt: "2026-10-09T00:00:00.000Z",
					genId: "gen-1",
					source: "/home/operator/.gjc/agent",
					entries: [
						{ name: "agent.db", kind: "credentials", state: "in_sync" },
						{ name: "models.yml", kind: "model-config", state: "source_changed" },
						{ name: "config.yml", kind: "model-config", state: "missing_copy" },
					],
				},
			}),
		);
		expect(lines).toContain("broker_agent: provisioned");
		expect(lines).toContain("broker_agent models.yml: kind=model-config state=source_changed");
		expect(lines).toContain("broker_agent config.yml: kind=model-config state=missing_copy");
		expect(lines.join("\n")).not.toContain("agent.db: kind=credentials");
	});

	test("gates render visibly with every reason named", () => {
		const lines = renderCycle(
			cycleResult({ phase: "degraded", gates: ["stale_session_identity", "memory_closure_blocked"] }),
		).join("\n");
		expect(lines).toContain("gates: stale_session_identity, memory_closure_blocked");
	});
	test("memory git index lock renders its path and age", () => {
		const path = "/memory/.git/index.lock";
		const lines = renderCycle(
			cycleResult({
				phase: "degraded",
				gates: ["memory_git_index_locked"],
				memoryGitIndexLock: { path, ageMs: 1234 },
			}),
		).join("\n");
		expect(lines).toContain("gates: memory_git_index_locked");
		expect(lines).toContain(`memory_git_index_lock: ${path} age=1234ms`);
	});

	test("mid-rebind session renders the rebinding marker instead of a session id", () => {
		const lines = renderCycle(
			cycleResult({
				gates: ["stale_session_identity"],
				sessions: [
					{
						originKey: "discord/dm/c1/peer=p1",
						origin: { platform: "discord", kind: "dm", conversationId: "c1", peerId: "p1" },
						epoch: 4,
						sessionId: "",
						createdAt: "2026-08-01T00:00:00.000Z",
						lastActivityAt: null,
						pendingInbound: 2,
						unsettledDeliveries: 1,
						oldestUnsettledAgeMs: 42_000,
						contextDiff: {
							unread: 0,
							expired: 0,
							truncated: 0,
							omittedOldestAt: null,
							omittedNewestAt: null,
							floorAt: "2026-08-01T00:00:00.000Z",
						},
						bootstrap: {
							epoch: 4,
							pending: true,
							appliedAt: null,
							includedSections: [],
							byteCount: 0,
							truncated: false,
							diagnostics: [],
						},
					},
				],
			}),
		).join("\n");
		expect(lines).toContain("(rebinding)");
		expect(lines).toContain("discord/dm/c1/peer=p1");
		expect(lines).toContain("42s");
	});

	test("census lines always render so an empty subsystem is distinguishable from a missing one", () => {
		const lines = renderCycle(cycleResult()).join("\n");
		expect(lines).toContain("inbound: pending=0 inflight=0");
		expect(lines).toContain("context: unread=0 expired=0 truncated=0 omitted_oldest=- omitted_newest=-");
		expect(lines).toContain("deliveries: pending=0 inflight=0 confirmed=0 failed_ambiguous=0 expired=0");
		expect(lines).toContain("memory: queued=0 written=0 committed=0 receipted=0 quarantined=0");
		expect(lines).toContain("monitors: none");
		expect(lines).not.toContain("monitor authoring lost");
	});

	test("lost monitor authoring renders each event type with its streak", () => {
		const lines = renderCycle(
			cycleResult({
				phase: "degraded",
				gates: ["monitor_authoring_lost"],
				monitorAuthoringLost: [
					{ eventType: "backlog.watch", consecutive: 3, lastFiredAt: "2026-09-04T07:00:00.000Z" },
					{ eventType: "memory.audit", consecutive: 1, lastFiredAt: "2026-09-04T06:00:00.000Z" },
				],
			}),
		).join("\n");
		expect(lines).toContain("gates: monitor_authoring_lost");
		expect(lines).toContain(
			"monitor authoring lost: backlog.watch=3 (last 2026-09-04T07:00:00.000Z) memory.audit=1 (last 2026-09-04T06:00:00.000Z)",
		);
	});

	test("agent-directory headroom renders free/total, or unobservable when the probe failed", () => {
		const path = "/home/operator/.gjc/agent";
		const low = renderCycle(
			cycleResult({
				phase: "degraded",
				gates: ["agent_disk_headroom"],
				agentDisk: { path, freeBytes: 3 * 1024 ** 3, totalBytes: 456 * 1024 ** 3 },
			}),
		);
		expect(low).toContain("gates: agent_disk_headroom");
		expect(low).toContain(`agent_disk: ${path} free=3.0GiB total=456.0GiB`);
		expect(renderCycle(cycleResult({ agentDisk: { path, freeBytes: null, totalBytes: null } }))).toContain(
			`agent_disk: ${path} unobservable`,
		);
		expect(renderCycle(cycleResult()).join("\n")).not.toContain("agent_disk:");
		expect(cycleExitCode(cycleResult({ gates: ["agent_disk_headroom"] }))).toBe(1);
	});

	test("exit-code contract: gates force exit 1, healthy is exit 0", () => {
		expect(cycleExitCode(cycleResult())).toBe(0);
		for (const gate of [
			"stale_session_identity",
			"delivery_settlement_unknown",
			"memory_closure_blocked",
			"monitor_settlement_failed",
			"monitor_authoring_lost",
		])
			expect(cycleExitCode(cycleResult({ gates: [gate as OpsCycleResult["gates"][number]] }))).toBe(1);
	});
});
describe("usage guard", () => {
	test("an empty argv is a usage error, not a dispatchable command", () => {
		expect(usageFor(undefined)).toBe(CLI_USAGE);
		expect(parseArgs([]).command).toBeUndefined();
		expect(usageFor(parseArgs([]).command)).toBe(CLI_USAGE);
	});

	test("an unknown subcommand is a usage error", () => {
		for (const command of ["bogus", "--help", "-h", "status-ish", ""]) expect(usageFor(command)).toBe(CLI_USAGE);
	});

	test("every dispatchable command passes the guard", () => {
		for (const command of COMMANDS) expect(usageFor(command)).toBeUndefined();
	});

	test("a socket override alone still resolves to no command", () => {
		expect(usageFor(parseArgs(["--socket", "/tmp/x"]).command)).toBe(CLI_USAGE);
	});
});

describe("usage exits the process instead of blocking", () => {
	async function run(args: string[]): Promise<{ code: number; stderr: string; stdout: string }> {
		const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), ...args], {
			stdout: "pipe",
			stderr: "pipe",
			env: { ...process.env, GAJAEWAY_HOME: join(tmpdir(), "gajaeway-cli-usage-nonexistent") },
		});
		const [stdout, stderr, code] = await Promise.all([
			new Response(child.stdout).text(),
			new Response(child.stderr).text(),
			child.exited,
		]);
		return { code, stderr, stdout };
	}

	test("no arguments prints usage on stderr and exits non-zero", async () => {
		const result = await run([]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(CLI_USAGE);
		expect(result.stdout).toBe("");
	}, 30_000);

	test("an unknown subcommand prints usage on stderr and exits non-zero", async () => {
		const result = await run(["bogus"]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(CLI_USAGE);
	}, 30_000);
});

describe("work operator commands", () => {
	async function run(args: string[], result: unknown, sessionId?: string) {
		const previousSessionId = process.env.GJC_SESSION_ID;
		if (sessionId === undefined) delete process.env.GJC_SESSION_ID;
		else process.env.GJC_SESSION_ID = sessionId;
		const requests: Array<{ verb: string; params: unknown }> = [];
		const lines: string[] = [];
		const errors: string[] = [];
		let closed = false;
		const client: GajaewayClient = Object.create(GajaewayClient.prototype);
		client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
			requests.push({ verb, params });
			if (result instanceof Error) throw result;
			return result as T;
		};
		client.close = async () => {
			closed = true;
		};
		const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);
		const log = spyOn(console, "log").mockImplementation((line) => {
			lines.push(String(line));
		});
		const error = spyOn(console, "error").mockImplementation((line) => {
			errors.push(String(line));
		});
		const previousExit = process.exitCode;
		try {
			await main(["--socket", "/test/work.sock", "work", ...args]);
			return {
				requests,
				lines,
				errors,
				closed,
				connections: connect.mock.calls.length,
				exitCode: process.exitCode,
				connectOptions: connect.mock.calls[0]?.[1],
			};
		} finally {
			connect.mockRestore();
			log.mockRestore();
			error.mockRestore();
			process.exitCode = previousExit ?? 0;
			if (previousSessionId === undefined) delete process.env.GJC_SESSION_ID;
			else process.env.GJC_SESSION_ID = previousSessionId;
		}
	}

	for (const [flag, value, model] of [
		["--preset", "reliable", { preset: "reliable" }],
		["--model", "openai/gpt-5.2", "openai/gpt-5.2"],
	] as const) {
		test(`work run forwards ${flag} with cwd and resume`, async () => {
			const output = await run(["run", "fix", "--cwd", "/repo", "--resume", flag, value, "fix", "tests"], {
				held: false,
				text: "done",
			});
			expect(output.requests).toEqual([
				{
					verb: "work.run",
					params: { name: "fix", cwd: "/repo", resume: true, model, text: "fix tests" },
				},
			]);
			expect(output.lines).toEqual(["done"]);
			expect(output.errors).toEqual([]);
			expect(output.closed).toBe(true);
		});
	}

	for (const [flag, value, model] of [
		["--preset", "reliable", { preset: "reliable" }],
		["--model", "openai/gpt-5.2", "openai/gpt-5.2"],
	] as const) {
		test(`work start forwards ${flag} with cwd and resume`, async () => {
			const output = await run(["start", "fix", "--cwd", "/repo", "--resume", flag, value, "fix", "tests"], {
				started: true,
				jobId: "job-1",
				opRef: "op-1",
				sessionKey: "work/task/fix",
				sessionId: "session-1",
			});
			expect(output.requests).toEqual([
				{
					verb: "work.start",
					params: { name: "fix", cwd: "/repo", resume: true, model, text: "fix tests" },
				},
			]);
			expect(output.lines).toEqual(["started: work/task/fix session=session-1 job=job-1 op=op-1"]);
			expect(output.connectOptions).toEqual({ requestTimeoutMs: 120_000, deadlineMs: 30_000 });

			expect(output.closed).toBe(true);
		});
	}

	for (const command of ["run", "start"] as const) {
		test(`work ${command} forwards GJC_SESSION_ID as callerSessionId`, async () => {
			const result =
				command === "start"
					? { started: true, jobId: "job-1", opRef: "op-1", sessionKey: "work/task/fix", sessionId: "session-1" }
					: { held: false, text: "answer" };
			const output = await run([command, "fix", "task"], result, "caller-session-1");
			expect(output.requests).toEqual([
				{
					verb: `work.${command}`,
					params: { name: "fix", text: "task", callerSessionId: "caller-session-1" },
				},
			]);
		});

		test(`work ${command} omits blank GJC_SESSION_ID`, async () => {
			const result =
				command === "start"
					? { started: true, jobId: "job-1", opRef: "op-1", sessionKey: "work/task/fix", sessionId: "session-1" }
					: { held: false, text: "answer" };
			const output = await run([command, "fix", "task"], result, " \t ");
			expect(output.requests).toEqual([{ verb: `work.${command}`, params: { name: "fix", text: "task" } }]);
		});
	}

	test("work start leaves parent selection to the gateway", async () => {
		const output = await run(["start", "fix", "task"], {
			started: true,
			jobId: "j",
			opRef: "o",
			sessionKey: "k",
			sessionId: "s",
		});
		expect(output.requests).toEqual([{ verb: "work.start", params: { name: "fix", text: "task" } }]);
	});

	for (const command of ["run", "start"]) {
		test(`work ${command} prints held without claiming acceptance`, async () => {
			const output = await run([command, "fix", "task"], {
				...(command === "start" ? { started: false } : {}),
				held: true,
				jobId: "job-1",
				state: "awaiting_operator",
				reason: "terminal_uncertain",
			});
			expect(output.lines).toEqual(["HELD: terminal_uncertain\njob: job-1 state: awaiting_operator"]);
			expect(output.exitCode).toBe(1);
			expect(output.closed).toBe(true);
		});
	}

	test("work run remains response-only with a caller wait budget", async () => {
		const output = await run(["run", "fix", "task"], { held: false, text: "answer" });
		expect(output.requests).toEqual([{ verb: "work.run", params: { name: "fix", text: "task" } }]);
		expect(output.connectOptions).toEqual({ requestTimeoutMs: 3_600_000, deadlineMs: 30_000 });
		expect(output.lines).toEqual(["answer"]);
	});

	for (const op of [null, { status: "unknown" }, { status: "terminal_ok", outcome: { reason: "cancelled" } }]) {
		test(`work status prints exact snapshot for ${JSON.stringify(op)}`, async () => {
			const result = {
				jobId: "job-1",
				state: "awaiting_operator",
				sessionId: "",
				lastActivityAt: null,
				attempt: null,
				op,
			};
			const output = await run(["status", "fix"], result);
			expect(output.requests).toEqual([{ verb: "work.status", params: { name: "fix" } }]);
			expect(output.lines).toEqual([JSON.stringify(result)]);
			expect(output.closed).toBe(true);
		});
	}

	test("work steer returns the accepted correlation ref", async () => {
		const output = await run(["steer", "fix", "focus", "tests"], { steered: true, clientRef: "client-exact" });
		expect(output.requests).toEqual([{ verb: "work.steer", params: { name: "fix", text: "focus tests" } }]);
		expect(output.lines).toEqual(["steered: client-exact"]);
		expect(output.closed).toBe(true);
	});

	test("work steer refusal is not acceptance", async () => {
		const output = await run(["steer", "fix", "focus"], { steered: false, reason: "steer_refused:sdk_refused" });
		expect(output.lines).toEqual(["not steered: steer_refused:sdk_refused"]);
		expect(output.exitCode).toBe(1);
	});

	for (const command of ["run", "start", "status", "steer"]) {
		test(`work ${command} reports gateway errors and closes without retry`, async () => {
			const output = await run(
				[command, "fix", ...(command === "status" ? [] : ["task"])],
				new Error("work status unavailable"),
			);
			expect(output.lines).toEqual([]);
			expect(output.errors).toEqual(["work status unavailable"]);
			expect(output.requests).toHaveLength(1);
			expect(output.exitCode).toBe(1);
			expect(output.closed).toBe(true);
		});
	}

	for (const closed of [true, false]) {
		test(`work retire prints a successful retirement with closed=${closed}`, async () => {
			const output = await run(["retire", "fix"], {
				retired: true,
				sessionKey: "work/task/fix",
				sessionId: "session-1",
				closed,
			});
			expect(output.requests).toEqual([{ verb: "work.retire", params: { name: "fix" } }]);
			expect(output.lines).toEqual([`retired: work/task/fix session=session-1 closed=${closed}`]);
			expect(output.closed).toBe(true);
		});
	}

	test("work retire prints refusal without inventing a session", async () => {
		const output = await run(["retire", "fix"], {
			retired: false,
			sessionKey: "work/task/fix",
			reason: "active attempt",
		});
		expect(output.lines).toEqual(["not retired: active attempt"]);
		expect(output.closed).toBe(true);
	});

	test("work jobs renders bound and unbound lanes", async () => {
		const output = await run(["jobs"], {
			jobs: [
				{
					lane_key: "fix",
					state: "running",
					session_id: "session-1",
					last_activity_at: "2026-09-07T00:00:00Z",
					worktree_path: "/repo/fix",
					reports: { pending: 2, claimed: 1, held: 3, undeliverable: 4 },
				},
				{
					lane_key: "done",
					state: "done",
					session_id: "",
					last_activity_at: null,
					worktree_path: "/repo/done",
					reports: { pending: 0, claimed: 0, held: 0, undeliverable: 0 },
				},
			],
		});
		expect(output.requests).toEqual([{ verb: "work.jobs", params: undefined }]);
		expect(output.lines).toEqual([
			"fix running session=session-1 accepted=- op=- last=2026-09-07T00:00:00Z head=- /repo/fix reports=p:2 c:1 h:3 u:4",
			"done done session=- accepted=- op=- last=- head=- /repo/done",
		]);
		expect(output.closed).toBe(true);
	});

	test("work jobs shows the job first, its current attempt, and the lane's last commit (issue #67)", async () => {
		const output = await run(["jobs"], {
			jobs: [
				{
					lane_key: "work-fix",
					state: "attempt_ended",
					session_id: "session-1",
					last_activity_at: "2026-09-07T00:10:00Z",
					worktree_path: "/repo/fix",
					accepted_at: "2026-09-07T00:00:00.000Z",
					attempt: { op_ref: "op-2", started_at: "2026-09-07T00:05:00.000Z", ended_at: "2026-09-07T00:10:00.000Z" },
					last_commit: {
						sha: "0123456789abcdef0123456789abcdef01234567",
						subject: "fix: land it",
						committed_at: "2026-09-07T00:09:20.000Z",
					},
				},
			],
		});
		expect(output.lines).toEqual([
			'fix attempt_ended session=session-1 accepted=2026-09-07T00:00:00.000Z op=op-2 last=2026-09-07T00:10:00Z head=0123456@2026-09-07T00:09:20.000Z "fix: land it" /repo/fix',
		]);
		expect(output.closed).toBe(true);
	});

	test("work jobs distinguishes quarantined history from current running lanes", async () => {
		const output = await run(["jobs"], {
			jobs: [
				{
					lane_key: "work-current",
					state: "running",
					session_id: "global-session",
					last_activity_at: "2026-09-09T00:00:00Z",
					worktree_path: "/repo/current",
				},
				{
					lane_key: "work-legacy",
					state: "running",
					session_id: "private-session",
					last_activity_at: "2026-09-07T00:00:00Z",
					worktree_path: "/repo/legacy",
					quarantined: true,
					reason: "broker_authority_quarantined",
				},
			],
		});
		expect(output.requests).toEqual([{ verb: "work.jobs", params: undefined }]);
		expect(output.lines).toEqual([
			"current running session=global-session accepted=- op=- last=2026-09-09T00:00:00Z head=- /repo/current",
			"legacy HELD: quarantined reason=broker_authority_quarantined historical_state=running session=private-session accepted=- op=- last=2026-09-07T00:00:00Z head=- /repo/legacy",
		]);
		expect(output.errors).toEqual([]);
		expect(output.closed).toBe(true);
	});
	test("work jobs pages with --limit/--offset and reports where the page sits", async () => {
		const job = {
			lane_key: "work-a",
			state: "attempt_ended",
			session_id: "",
			last_activity_at: null,
			worktree_path: "/repo/a",
		};
		const middle = await run(["jobs", "--limit", "1", "--offset", "1"], { total: 3, offset: 1, jobs: [job] });
		expect(middle.requests).toEqual([{ verb: "work.jobs", params: { limit: 1, offset: 1 } }]);
		expect(middle.lines).toEqual(["a attempt_ended session=- accepted=- op=- last=- head=- /repo/a"]);
		expect(middle.errors).toEqual(["jobs 2-2 of 3 (next: --offset 2)"]);
		const last = await run(["jobs", "--offset", "2"], { total: 3, offset: 2, jobs: [job] });
		expect(last.errors).toEqual(["jobs 3-3 of 3"]);
		const whole = await run(["jobs"], { total: 1, offset: 0, jobs: [job] });
		expect(whole.errors).toEqual([]);
	});
	test("work jobs rejects malformed paging flags before connecting", async () => {
		for (const args of [
			["--limit"],
			["--limit", "-1"],
			["--offset", "x"],
			["--limit", "1", "--limit", "2"],
			["--all"],
		]) {
			const output = await run(["jobs", ...args], {});
			expect(output.connections).toBe(0);
			expect(output.requests).toEqual([]);
		}
	});
	test("--notify is a usage error on start and run", async () => {
		for (const command of ["start", "run"] as const) {
			const output = await run([command, "fix", "--notify", "discord/channel/c", "task"], {});
			expect(output.connections).toBe(0);
			expect(output.requests).toEqual([]);
			expect(output.errors[0]).toContain("usage: gajaeway work");
		}
	});

	for (const args of [
		["start", "fix", "--model", "model", "--preset", "preset", "task"],
		["start", "fix", "--preset", "preset", "--model", "model", "task"],
		["start", "fix", "--model"],
		["start", "fix", "--preset", "--resume", "task"],
		["start", "fix", "--cwd", "relative", "task"],
		["run", "fix", "--cwd", "relative", "task"],
		["start", "bad/name", "task"],
		["start", "fix", "   "],
		["status"],
		["status", "fix", "extra"],
		["steer", "fix"],
		["steer", "fix", "--resume", "task"],
		["run", "fix", "--model", "model", "--preset", "preset", "task"],
		["run", "fix", "--preset", "preset", "--model", "model", "task"],
		["run", "fix", "task", "--model"],
		["run", "fix", "task", "--preset"],
		["run", "fix", "task", "--preset", "--resume"],
		["retire"],
		["retire", "fix", "extra"],
		["jobs", "extra"],
	]) {
		test(`invalid work arguments fail before connecting: ${args.join(" ")}`, async () => {
			const output = await run(args, {});
			expect(output.connections).toBe(0);
			expect(output.requests).toEqual([]);
			expect(output.errors[0]).toContain("usage: gajaeway work");
		});
	}
});
