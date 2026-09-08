import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import { BrokerSupervisor, reapAgentDir } from "../src/orchestrator/broker";
import { LaneGovernor } from "../src/orchestrator/lane-governor";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { WorkLaneManager } from "../src/orchestrator/work-lane";
import { GatewayDatabase } from "../src/store/db";

const liveTest = process.env.GAJAEWAY_E2E_WORK === "1" ? test : test.skip;
const diagnosticCode = /^[a-z][a-z_]{1,63}$/;

// Select only protocol metadata; never serialize errors, argv, stderr, provider
// prose, textSummary, content, environment, discovery tokens or model config.
function diagnostic(value: unknown, depth = 0): Record<string, unknown> {
	if (!value || typeof value !== "object" || Array.isArray(value) || depth > 5) return {};
	const record = value as Record<string, unknown>;
	const result: Record<string, unknown> = {};
	if (typeof record.ok === "boolean") result.ok = record.ok;
	if (typeof record.code === "string")
		result.code =
			diagnosticCode.test(record.code) || /^provider_http_[0-9]{3}$/.test(record.code)
				? record.code
				: "other_sdk_error";
	if (["unknown", "accepted", "in_flight", "terminal_ok", "failed", "rejected"].includes(String(record.status)))
		result.status = record.status;
	if (["absent", "present", "missing", "unknown"].includes(String(record.receiptState)))
		result.receiptState = record.receiptState;
	for (const key of ["error", "result", "status", "outcome"] as const) {
		const nested = diagnostic(record[key], depth + 1);
		if (Object.keys(nested).length) result[key] = nested;
	}
	return result;
}

liveTest(
	"real WorkLaneManager starts, submits, settles and proves invocation-owned output",
	async () => {
		// No baked-in provider/model assumption: callers select an already usable
		// model and a models.yml containing its provider definition. Only this file
		// is copied; no operator sessions, broker discovery or authentication stores.
		const model = process.env.GAJAEWAY_E2E_MODEL;
		const modelsFile = process.env.GAJAEWAY_E2E_MODELS_FILE;
		if (!model || !modelsFile || Bun.which("gjc") === null)
			throw new Error(
				"live work test requires gjc, GAJAEWAY_E2E_MODEL and GAJAEWAY_E2E_MODELS_FILE; provider credentials must be inherited",
			);
		const home = await mkdtemp(join(tmpdir(), "gajaeway-work-live-"));
		const repo = join(home, "workspace");
		await mkdir(repo, { mode: 0o700 });
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const broker = new BrokerSupervisor({
			home,
			instanceId: database.instanceId,
			cwd: repo,
			ssotAgentDir: null,
			healthIntervalMs: 60_000,
			log: () => {},
		});
		const evidence: Record<string, unknown>[] = [];
		let stage = "configuration";
		let manager: WorkLaneManager | undefined;
		let port: BrokerSessionPort | undefined;
		const cli: CliRunner = async (args, options) => {
			const result = await broker.cli(args, options);
			try {
				const summary = diagnostic(JSON.parse(result.stdout));
				// A fixed-size ring survives generic WorkLaneManager error mapping.
				evidence.push({ exitCode: result.exitCode, ...summary });
				if (evidence.length > 16) evidence.shift();
			} catch {
				evidence.push({ exitCode: result.exitCode, malformedEnvelope: true });
				if (evidence.length > 16) evidence.shift();
			}
			return result;
		};
		try {
			await mkdir(broker.agentDir, { recursive: true, mode: 0o700 });
			await writeFile(join(broker.agentDir, "models.yml"), await readFile(modelsFile, "utf8"), { mode: 0o600 });
			stage = "broker-start";
			await broker.preflight();
			await broker.start();
			port = new BrokerSessionPort({
				database,
				cli,
				instanceId: database.instanceId,
				agentDir: broker.agentDir,
				tailRunner: new TailRunner({ run: cli, stream: (id) => broker.openStream(id), repo, pollIntervalMs: 250 }),
			});
			manager = new WorkLaneManager({
				database,
				port,
				lanes: new LaneGovernor({ database, sessionPort: port, maxLanes: 1 }),
				brokerGeneration: () => broker.generation,
				pollMs: 500,
				waitTimeoutMs: 120_000,
			});
			stage = "manager-start";
			const started = await manager.start({
				name: "live-smoke",
				cwd: repo,
				model,
				text: "Do not use tools. Reply with exactly WORK_LANE_LIVE_OK.",
			});
			if (!started.started) throw new Error("fresh work lane unexpectedly held");
			expect(database.workAttemptGet(started.opRef)?.sendPhase).toBe("accepted");
			stage = "manager-status-and-settlement";
			const deadline = Date.now() + 120_000;
			while (Date.now() < deadline) {
				const status = await manager.status({ name: "live-smoke" });
				if (database.workAttemptGet(started.opRef)?.settledAt) {
					expect(status.op?.status).toBe("terminal_ok");
					expect(status.attempt?.endState).toBe("completed");
					break;
				}
				await Bun.sleep(500);
			}
			const runtime = database.workAttemptGet(started.opRef)!;
			expect(runtime.settledAt).not.toBeNull();
			expect(runtime.terminal?.reasonCode).toBe("end_turn");
			stage = "invocation-owned-output";
			expect(runtime.output.disposition).toBe("available");
			expect(runtime.output.proof?.source).toBe("turn.result");
			expect(runtime.output.proof?.opRef).toBe(started.opRef);
			expect(runtime.output.excerpt?.includes("WORK_LANE_LIVE_OK")).toBe(true);
			expect(runtime.decision).toBe("no_target");
			console.error(
				"work_live_evidence " +
					JSON.stringify({
						started: true,
						accepted: true,
						terminal: "end_turn",
						output: "proven",
						source: "turn.result",
					}),
			);
		} catch {
			// Replace, rather than attach as cause: assertion diagnostics can contain
			// provider-controlled output and SDK exceptions can carry raw envelopes.
			throw new Error("work live failure " + JSON.stringify({ stage, evidence }));
		} finally {
			try {
				await manager?.stop();
				for (const row of database.workLaneRows()) {
					try {
						await port?.close({ sessionId: row.gjc_session_id, repo });
					} catch {
						/* Reap only this private home below. */
					}
				}
			} finally {
				try {
					await broker.stop();
					await reapAgentDir(
						broker.agentDir,
						() => {},
						(pid) => {
							try {
								process.kill(pid, 0);
								return true;
							} catch {
								return false;
							}
						},
					);
				} finally {
					database.close();
					await rm(home, { recursive: true, force: true });
				}
			}
		}
	},
	240_000,
);
