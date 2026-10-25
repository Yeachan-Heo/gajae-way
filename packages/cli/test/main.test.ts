import { describe, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";

import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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
	pollMemoryOp,
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
						if (frame.verb === undefined) throw new Error("gateway request omitted verb");
						requests.push({ verb: frame.verb, params: frame.params });
						socket.write(`${JSON.stringify({ v: "0.1", type: "response", id: frame.id, result })}\n`);
					}
					newline = socket.data.buffer.indexOf("\n");
				}
			},
		},
	});
	try {
		const child = Bun.spawn(
			[process.execPath, join(import.meta.dir, "../src/main.ts"), "--socket", path, "ops", "slo", ...args],
			{
				cwd: join(import.meta.dir, "../../.."),
				stdin: "ignore",
				stdout: "pipe",
				stderr: "pipe",
				env: { ...process.env, GAJAEWAY_HOME: home },
			},
		);
		const [stdout, stderr, exitCode] = await Promise.all([
			new Response(child.stdout).text(),
			new Response(child.stderr).text(),
			child.exited,
		]);
		return { requests, stdout, stderr, exitCode };
	} finally {
		listener.stop(true);
		await rm(home, { recursive: true, force: true });
	}
}

async function runOpsCycleCli(result: OpsCycleResult, args: readonly string[]) {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const lines: string[] = [];
	const errors: string[] = [];
	const client: GajaewayClient = Object.create(GajaewayClient.prototype);
	client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
		requests.push({ verb, params });
		return result as T;
	};
	client.close = async () => {};
	const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(client);
	const log = spyOn(console, "log").mockImplementation((line) => lines.push(String(line)));
	const error = spyOn(console, "error").mockImplementation((line) => errors.push(String(line)));
	const previousExit = process.exitCode;
	try {
		await main(["--socket", "/test/ops-cycle.sock", "ops", "cycle", ...args]);
		return { requests, lines, errors, exitCode: process.exitCode, connections: connect.mock.calls.length };
	} finally {
		connect.mockRestore();
		log.mockRestore();
		error.mockRestore();
		process.exitCode = previousExit ?? 0;
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
		expect(run.stderr).toBe("");
		expect(run.stdout).toBe(`${JSON.stringify(result)}\n`);
		expect(JSON.parse(run.stdout)).toEqual(result);
		expect(sloExitCode(result)).toBe(0);
		expect(renderSlo(result).join("\\n")).toContain("verified=true");
		expect(renderSlo(result).join("\\n")).toContain("over_cap_ms=60000");
	});

	test("pending has a rerun-later message and a nonzero exit", async () => {
		const result = sampleSloResult("pending");
		const run = await runOpsSloCli(result, ["--days", "1"]);
		expect(run.exitCode).toBe(1);
		expect(run.stderr).toBe("");
		expect(run.stdout).toBe(`${renderSlo(result).join("\n")}\n`);
		expect(run.stdout).toContain("SLO pending");
		expect(run.stdout).toContain("rerun later");
	});

	test("failed and insufficient are distinct nonzero outcomes", async () => {
		const failedResult = sampleSloResult("failed");
		const insufficientResult = sampleSloResult("insufficient");
		const failed = await runOpsSloCli(failedResult, []);
		const insufficient = await runOpsSloCli(insufficientResult, []);
		expect(failed.exitCode).toBe(1);
		expect(failed.stderr).toBe("");
		expect(failed.stdout).toBe(`${renderSlo(failedResult).join("\n")}\n`);
		expect(failed.stdout).toContain("SLO failed");
		expect(insufficient.exitCode).toBe(1);
		expect(insufficient.stderr).toBe("");
		expect(insufficient.stdout).toBe(`${renderSlo(insufficientResult).join("\n")}\n`);
		expect(insufficient.stdout).toContain("SLO insufficient");
	});
});

describe("ops cycle paging CLI", () => {
	test("sends bounded page parameters and preserves the full-home gate in JSON", async () => {
		const result = cycleResult({
			phase: "degraded",
			gates: ["stale_session_identity"],
			sessionsTotal: 1_200,
			sessionsOffset: 100,
		});
		const output = await runOpsCycleCli(result, ["--limit", "25", "--offset", "100", "--json"]);
		expect(output.requests).toEqual([{ verb: "ops.cycle", params: { limit: 25, offset: 100 } }]);
		expect(output.lines).toEqual([JSON.stringify(result)]);
		expect(output.exitCode).toBe(1);
		expect(output.errors).toEqual([]);
		const defaultPage = await runOpsCycleCli(result, []);
		expect(defaultPage.requests).toEqual([{ verb: "ops.cycle", params: {} }]);
	});

	test("rejects responses from a pre-pagination gateway instead of rendering invalid page metadata", async () => {
		const session = {
			originKey: "discord/channel/c1",
			epoch: 0,
			sessionId: "session-1",
			pendingInbound: 0,
			unsettledDeliveries: 0,
			oldestUnsettledAgeMs: null,
		} as OpsCycleResult["sessions"][number];
		const paged = cycleResult({ sessions: [session], sessionsTotal: 1 });
		const legacy = { ...paged, sessionsTotal: undefined, sessionsOffset: undefined } as unknown as OpsCycleResult;
		for (const args of [[], ["--limit", "25"]]) {
			const output = await runOpsCycleCli(legacy, args);
			expect(output.lines).toEqual([]);
			expect(output.errors[0]).toContain("paging is unsupported by the running gateway");
			expect(output.exitCode).toBe(1);
		}
		expect(() => renderCycle(legacy)).toThrow("paging is unsupported by the running gateway");
	});

	test("rejects malformed paging options before connecting", async () => {
		for (const args of [
			["--limit"],
			["--limit", "0"],
			["--limit", "501"],
			["--offset", "-1"],
			["--offset", "1.5"],
			["--offset", "1", "--offset", "2"],
			["--json", "--json"],
		]) {
			const output = await runOpsCycleCli(cycleResult(), args);
			expect(output.connections).toBe(0);
			expect(output.requests).toEqual([]);
			expect(output.errors[0]).toContain("usage: gajaeway ops cycle");
		}
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
		console.log = (line: unknown) => {
			const text = String(line);
			if (text === pkg.version) output.push(text);
		};
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
		console.log = (line: unknown) => {
			const text = String(line);
			if (text === JSON.stringify({ requeued: [] })) output.push(text);
		};
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
			state: "expired",
			createdAt: "2026-10-01T00:00:00.000Z",
			lastError: "not_found",
			textPrefix: "delivery text",
		};
		console.log = (line: unknown) => {
			const text = String(line);
			if (
				text === JSON.stringify({ rows: [row] }) ||
				text.startsWith("deliveries: ") ||
				text.startsWith(`${row.createdAt} ${row.state} `)
			)
				output.push(text);
		};
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
				"expired",
				"--limit",
				"25",
				"--json",
			]);
			await main(["--socket", path, "ops", "deliveries"]);
			expect(received).toEqual([
				{
					verb: "ops.deliveries",
					params: { since: "2026-10-01T00:00:00.000Z", state: "expired", limit: 25 },
				},
				{ verb: "ops.deliveries", params: {} },
			]);
			expect(output).toEqual([
				JSON.stringify({ rows: [row] }),
				"deliveries: 1",
				'2026-10-01T00:00:00.000Z expired delivery=delivery-42 turn=turn-42 origin=discord:channel:channel-42 conversation=channel-42 lastError=not_found text="delivery text"',
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
		console.log = (line: unknown) => {
			const text = String(line);
			try {
				const value = JSON.parse(text) as { eventId?: unknown; stage?: unknown };
				if (
					typeof value.eventId === "string" &&
					value.eventId.startsWith("event-") &&
					typeof value.stage === "string"
				) {
					output.push(text);
				}
			} catch {
				// Other tests share the process console.
			}
		};
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
							const stage = received.length === 1 ? "delivered" : received.length === 2 ? "authored" : "skipped";
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
			const skippedStartedAt = Date.now();
			await main(["--socket", path, "monitors", "test", "monitor-1", "--wait=1"]);
			expect(Date.now() - skippedStartedAt).toBeLessThan(900);
			expect(received).toEqual(["monitor.test", "monitor.test", "monitor.test"]);
			expect(output).toEqual([
				'{"eventId":"event-42","stage":"delivered"}',
				'{"eventId":"event-43","stage":"authored"}',
				'{"eventId":"event-44","stage":"skipped"}',
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

	test("monitor settle and retry reject malformed arguments before connecting", async () => {
		const errors: string[] = [];
		const originalError = console.error;
		const previousExitCode = process.exitCode ?? 0;
		console.error = (line: unknown) => errors.push(String(line));
		try {
			for (const args of [
				["monitors", "settle"],
				["monitors", "retry", "event-1", "extra"],
			]) {
				await main(["--socket", "/nonexistent/gajaeway-monitor-operator.sock", ...args]);
			}
			expect(errors).toHaveLength(2);
			expect(errors.every((error) => error.startsWith("usage: gajaeway monitors settle"))).toBe(true);
			expect(errors.every((error) => !error.includes("could not connect"))).toBe(true);
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
			const env = { HOME: home, GAJAEWAY_HOME: home };
			const binDir = join(home, "gaja&<bin>");
			await mkdir(binDir);
			for (const spec of serviceSpecs()) await writeFile(join(binDir, spec.binary), "stub");
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
		sessionsTotal: 0,
		sessionsOffset: 0,
		sessions: [],
		memoryIntents: { queued: 0, written: 0, committed: 0, receipted: 0, quarantined: 0 },
		memoryGitIndexLock: null,
		monitorEvents: [],
		monitorFailures: [],
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
				sessionsTotal: 120,
				sessionsOffset: 100,
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
		expect(lines).toContain("sessions: 101-101 of 120 (next: --offset 101)");
		expect(lines).toContain("100    discord/dm/c1/peer=p1");
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

	test("recent monitor failures render their bounded sanitized detail", () => {
		const lines = renderCycle(
			cycleResult({
				monitorFailures: [
					{
						eventId: "event-1",
						monitorId: "monitor-1",
						eventType: "memory.audit",
						stage: "failed",
						failedAt: "2026-10-15T12:30:00.000Z",
						code: "authoring_turn_failed",
						detail: '{"causeCode":"sdk_prompt_terminal_failed","message":"The model is temporarily unavailable"}',
					},
				],
			}),
		);
		expect(lines).toContain("monitor failures (newest 1):");
		expect(lines.join("\n")).toContain(
			'2026-10-15T12:30:00.000Z authoring_turn_failed event=event-1 monitor=monitor-1 type=memory.audit stage=failed detail={"causeCode":"sdk_prompt_terminal_failed","message":"The model is temporarily unavailable"}',
		);
		expect(renderCycle(cycleResult())).toContain("monitor failures: none");
	});

	test("cycle renderer explicitly rejects gateways without monitor failure diagnostics", () => {
		const olderGatewayResult = { ...cycleResult(), monitorFailures: undefined } as unknown as OpsCycleResult;
		expect(() => renderCycle(olderGatewayResult)).toThrow(
			"ops.cycle monitor failure diagnostics are unsupported by the running gateway",
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

	test("work start resolves relative repo paths in the CLI process", async () => {
		const output = await run(["start", "fix", "--repo", ".", "task"], {
			started: true,
			jobId: "j",
			opRef: "o",
			sessionKey: "k",
			sessionId: "s",
		});
		expect(output.requests).toEqual([
			{ verb: "work.start", params: { name: "fix", repo: resolve("."), text: "task" } },
		]);
	});

	test("work start rejects --base without --repo before connecting", async () => {
		const output = await run(["start", "fix", "--cwd", "/repo", "--base", "dev", "task"], {});
		expect(output.requests).toEqual([]);
		expect(output.connections).toBe(0);
		expect(output.errors.join(" ")).toContain("--repo");
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

	test("work retire prints historical disposition without claiming session closure", async () => {
		const output = await run(["retire", "fix"], {
			retired: true,
			sessionKey: "work/task/fix",
			sessionId: "",
			closed: false,
			disposition: "aborted",
		});
		expect(output.requests).toEqual([{ verb: "work.retire", params: { name: "fix" } }]);
		expect(output.lines).toEqual(["disposed: work/task/fix state=aborted (history and worktree preserved)"]);
		expect(output.closed).toBe(true);
	});

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
		const beyond = await run(["jobs", "--offset", "9"], { total: 3, offset: 9, jobs: [] });
		expect(beyond.errors).toEqual(["jobs: none at offset 9 of 3"]);
	});
	test("work jobs rejects malformed paging flags before connecting", async () => {
		for (const args of [
			["--limit"],
			["--limit", "-1"],
			["--offset", "x"],
			["--limit", "1", "--limit", "2"],
			["--all"],
			["--limit", "0"],
			["--limit", "501"],
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

describe("memory async operations (#473)", () => {
	test("memory.autolink returns opRef and CLI polls for pending→completed result", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-memory-autolink-"));
		const path = join(home, "test.sock");
		const output: string[] = [];
		const originalLog = console.log;
		console.log = (line: unknown) => output.push(String(line));

		const opRef = "gw-ml-test-op-123";
		let pollCount = 0;

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
						if (frame.type === "hello") {
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`,
							);
						} else if (frame.type === "request") {
							if (frame.verb === "memory.autolink") {
								socket.write(`${JSON.stringify({ v: "0.1", type: "response", id: frame.id, result: { opRef } })}\n`);
							} else if (frame.verb === "memory.status") {
								pollCount++;
								const isComplete = pollCount > 2;
								socket.write(
									`${JSON.stringify({
										v: "0.1",
										type: "response",
										id: frame.id,
										result: isComplete
											? {
													status: "completed",
													elapsedMs: 2_500,
													result: {
														filesChanged: 5,
														linksAdded: 10,
														aliases: 3,
														commitSha: "a".repeat(40),
														elapsedMs: 2_500,
													},
												}
											: { status: "pending", elapsedMs: 1_000 },
									})}\n`,
								);
							}
						}
						newline = socket.data.buffer.indexOf("\n");
					}
				},
			},
		});

		try {
			await main(["--socket", path, "memory", "autolink"]);
			expect(output).toContainEqual(
				JSON.stringify({
					filesChanged: 5,
					linksAdded: 10,
					aliases: 3,
					commitSha: "a".repeat(40),
					elapsedMs: 2_500,
				}),
			);
			expect(pollCount).toBeGreaterThan(2);
		} finally {
			console.log = originalLog;
			listener.stop(true);
			await rm(home, { recursive: true, force: true });
		}
	});

	test("memory.audit with failed status exits 1 with diagnostic", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-memory-audit-fail-"));
		const path = join(home, "test.sock");
		const output: string[] = [];
		const errors: string[] = [];
		const originalLog = console.log;
		const originalErr = console.error;
		console.log = (line: unknown) => output.push(String(line));
		console.error = (line: unknown) => errors.push(String(line));
		const previousExit = process.exitCode;

		const opRef = "gw-ma-test-op-fail";

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
						if (frame.type === "hello") {
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`,
							);
						} else if (frame.type === "request") {
							if (frame.verb === "memory.audit") {
								socket.write(`${JSON.stringify({ v: "0.1", type: "response", id: frame.id, result: { opRef } })}\n`);
							} else if (frame.verb === "memory.status") {
								socket.write(
									`${JSON.stringify({
										v: "0.1",
										type: "response",
										id: frame.id,
										result: { status: "failed", elapsedMs: 125, error: "memory corruption detected" },
									})}\n`,
								);
							}
						}
						newline = socket.data.buffer.indexOf("\n");
					}
				},
			},
		});

		try {
			await main(["--socket", path, "memory", "audit"]);
			expect(errors.join("\n")).toContain("audit failed");
			expect(errors.join("\n")).toContain("125ms");
			expect(errors.join("\n")).toContain("memory corruption detected");
			expect(process.exitCode).toBe(1);
		} finally {
			console.log = originalLog;
			console.error = originalErr;
			process.exitCode = previousExit ?? 0;
			listener.stop(true);
			await rm(home, { recursive: true, force: true });
		}
	});

	test("memory operation polling returns still-running when its wait expires", async () => {
		const errors: string[] = [];
		const originalErr = console.error;
		console.error = (line: unknown) => errors.push(String(line));
		let polls = 0;
		const client: Pick<GajaewayClient, "request"> = {
			request: async <T>() => {
				polls++;
				return { status: "pending", elapsedMs: polls * 15 } as T;
			},
		};
		try {
			const result = await pollMemoryOp(client, "gw-ml-still-running", 20, 10);
			expect(result.status).toBe("still_running");
			expect(result.elapsedMs).toBeGreaterThanOrEqual(20);
			expect(polls).toBeGreaterThan(0);
			expect(errors.join("\n")).toContain("gajaeway memory status gw-ml-still-running");
		} finally {
			console.error = originalErr;
		}
	});

	test("memory status subcommand checks operation status", async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-cli-memory-status-"));
		const path = join(home, "test.sock");
		const output: string[] = [];
		const originalLog = console.log;
		console.log = (line: unknown) => output.push(String(line));

		const opRef = "gw-ml-check-status";

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
						if (frame.type === "hello") {
							socket.write(
								`${JSON.stringify({ v: "0.1", type: "negotiated", payload: { profileVersion: "v0.1" } })}\n`,
							);
						} else if (frame.type === "request" && frame.verb === "memory.status") {
							socket.write(
								`${JSON.stringify({
									v: "0.1",
									type: "response",
									id: frame.id,
									result: {
										status: "completed",
										elapsedMs: 100,
										result: {
											filesChanged: 3,
											linksAdded: 7,
											aliases: 2,
											commitSha: "b".repeat(40),
											elapsedMs: 100,
										},
									},
								})}\n`,
							);
						}
						newline = socket.data.buffer.indexOf("\n");
					}
				},
			},
		});

		try {
			await main(["--socket", path, "memory", "status", opRef]);
			const statusOutput = output.find((line) => line.includes("status"));
			expect(statusOutput).toBeDefined();
			expect(statusOutput).toContain("completed");
		} finally {
			console.log = originalLog;
			listener.stop(true);
			await rm(home, { recursive: true, force: true });
		}
	});
});
