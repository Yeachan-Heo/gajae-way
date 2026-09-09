import { expect, test } from "bun:test";
import { AsyncLocalStorage } from "node:async_hooks";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { type CliRunner, GjcCliError } from "@gajaeway/subsession";
import { GjcCliUnavailableError, GlobalGjcClient, readBrokerDiscovery } from "../src/orchestrator/broker";
import { LaneGovernor } from "../src/orchestrator/lane-governor";
import { BrokerSessionPort, isSteerAccepted } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { WorkLaneManager } from "../src/orchestrator/work-lane";
import { GatewayDatabase } from "../src/store/db";

const liveTest = process.env.GAJAEWAY_E2E_WORK === "1" ? test : test.skip;
const diagnosticCode = /^[a-z][a-z_]{1,63}$/;

export async function observeStatus<T>(input: {
	query: () => Promise<T>;
	deadline: number;
	method: string;
	onRetry: (diagnostic: { method: string; retryCount: number; lastCode: string }) => void;
}): Promise<T> {
	let retryCount = 0;
	let lastCode = "none";
	const expired = () =>
		new Error(`status observation deadline: ${input.method}; retries=${retryCount}; lastCode=${lastCode}`);
	while (Date.now() < input.deadline) {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			const result = await Promise.race([
				input.query(),
				new Promise<never>((_, reject) => {
					timer = setTimeout(() => reject(expired()), Math.max(1, input.deadline - Date.now()));
				}),
			]);
			if (Date.now() >= input.deadline) throw expired();
			return result;
		} catch (error) {
			const code =
				error instanceof GjcCliError && error.details && typeof error.details === "object"
					? (error.details as { code?: unknown }).code
					: undefined;
			if (code === "session_unavailable" || code === "endpoint_stale") lastCode = code;
			else if (
				error instanceof GjcCliUnavailableError &&
				/^gjc sdk request failed: broker_unavailable \((?:command queue timed out|request timed out after [0-9]+ms)\)$/.test(
					error.message,
				)
			)
				lastCode = "transport_timeout";
			else throw error;
			input.onRetry({ method: input.method, retryCount: ++retryCount, lastCode });
		} finally {
			clearTimeout(timer);
		}
		await Bun.sleep(Math.min(250, Math.max(0, input.deadline - Date.now())));
	}
	throw expired();
}

// Scope retries to canary reads, before manager.status erases typed SDK errors.
// The manager's concurrent worker observer keeps its original polling behavior.
const statusObservation = new AsyncLocalStorage<{ deadline: number; method: string }>();
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

// Values never leave memory. Arrays and sensitive subtrees stop at their parent key.
function preservationDelta(name: string, before: Buffer | null, after: Buffer | null): Record<string, unknown> {
	const hash = (bytes: Buffer | null) => (bytes === null ? null : createHash("sha256").update(bytes).digest("hex"));
	const result: Record<string, unknown> = { file: name, beforeSha256: hash(before), afterSha256: hash(after) };
	if (name !== "config.yml") return result;
	const paths: string[] = [];
	let visited = 0;
	let truncated = false;
	const walk = (left: unknown, right: unknown, path: string, depth: number): void => {
		if (++visited > 512 || paths.length >= 32) {
			truncated = true;
			return;
		}
		if (Object.is(left, right)) return;
		const object = (value: unknown): value is Record<string, unknown> =>
			value !== null && typeof value === "object" && !Array.isArray(value);
		if (depth < 5 && object(left) && object(right) && !/auth|token|secret|credential|password|api.?key/i.test(path)) {
			for (const key of new Set([...Object.keys(left), ...Object.keys(right)])) {
				if (visited >= 512 || paths.length >= 32) {
					truncated = true;
					break;
				}
				const safeKey = /^[A-Za-z_][A-Za-z0-9_-]{0,63}$/.test(key) ? key : "[redacted-key]";
				walk(left[key], right[key], path ? `${path}.${safeKey}` : safeKey, depth + 1);
			}
		} else if (JSON.stringify(left) !== JSON.stringify(right)) paths.push(path || "[root]");
	};
	try {
		walk(
			before === null ? undefined : Bun.YAML.parse(before.toString("utf8")),
			after === null ? undefined : Bun.YAML.parse(after.toString("utf8")),
			"",
			0,
		);
		result.changedKeyPaths = paths;
		result.truncated = truncated;
	} catch {
		result.keyDiffUnavailable = true;
	}
	return result;
}

export function provesSharedSteer(input: {
	output: import("../src/orchestrator/session-port").WorkerOutputResult;
	sessionId: string;
	repo: string;
	opRef: string;
	marker: string;
	clientRef: string;
	receipt: { accepted?: unknown; clientRef?: unknown; status?: unknown; ok?: unknown };
	terminal: { operationRef: string; status: { status: string } };
}): boolean {
	return (
		isSteerAccepted(input.receipt) &&
		input.receipt.clientRef === input.clientRef &&
		input.terminal.operationRef === input.opRef &&
		input.terminal.status.status === "terminal_ok" &&
		input.output.status === "proven" &&
		input.output.provenance.source === "turn.result" &&
		input.output.provenance.fullness === "original" &&
		input.output.provenance.sessionId === input.sessionId &&
		input.output.provenance.repo === input.repo &&
		input.output.provenance.opRef === input.opRef &&
		input.output.text.includes(input.marker)
	);
}

liveTest(
	"real WorkLaneManager starts, submits, settles and proves invocation-owned output",
	async () => {
		// Select an existing user model; never copy or modify global configuration.
		const model = process.env.GAJAEWAY_E2E_MODEL;
		const executable = process.env.GJC_EXECUTABLE;
		const agentDir = process.env.GJC_CODING_AGENT_DIR;
		if (!model || !executable || !agentDir || !isAbsolute(executable) || !isAbsolute(agentDir))
			throw new Error(
				"live work test requires explicit absolute GJC_EXECUTABLE/GJC_CODING_AGENT_DIR and GAJAEWAY_E2E_MODEL from existing user configuration",
			);
		if (process.env.GAJAEWAY_E2E_SHARED_STEER !== "1")
			throw new Error(
				"full shared work contract requires GAJAEWAY_E2E_SHARED_STEER=1 for independent SDK steering of this test's own active session",
			);
		const home = await mkdtemp(join(tmpdir(), "gajaeway-work-live-"));
		const repo = join(home, "workspace");
		await mkdir(repo, { mode: 0o700 });
		const initialized = Bun.spawn(["git", "init", "--quiet", repo], { stdout: "ignore", stderr: "pipe" });
		expect(await initialized.exited).toBe(0);
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const broker = new GlobalGjcClient({ executable, agentDir, cwd: repo, healthIntervalMs: 60_000, log: () => {} });
		const canonicalAgentDir = await realpath(agentDir);
		const brokerAuthority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
		database.assertBrokerAuthority(brokerAuthority, { initializeEmpty: true });
		const configSnapshot = async () => {
			const snapshot = new Map<string, Buffer | null>();
			for (const name of ["models.yml", "settings.json", "auth.json", "config.yml", "config.json"]) {
				try {
					snapshot.set(name, await readFile(join(agentDir, name)));
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
					snapshot.set(name, null);
				}
			}
			return snapshot;
		};
		const beforeConfig = await configSnapshot();
		const independent = async (args: string[]) => {
			const child = Bun.spawn([executable, "sdk", "session", "--agent-dir", agentDir, ...args], {
				cwd: repo,
				env: {
					...process.env,
					GJC_EXECUTABLE: executable,
					GJC_CODING_AGENT_DIR: agentDir,
					PI_CODING_AGENT_DIR: agentDir,
					GJC_AGENT_DIR: agentDir,
				},
				stdin: "ignore",
				stdout: "pipe",
				stderr: "ignore",
			});
			const timer = setTimeout(() => child.kill(), 30_000);
			try {
				const [text, code] = await Promise.all([new Response(child.stdout).text(), child.exited]);
				const envelope = JSON.parse(text);
				evidence.push({
					independentCommand: args.slice(0, 2),
					exitCode: code,
					...diagnostic(envelope),
					resultKeys: envelope.result && typeof envelope.result === "object" ? Object.keys(envelope.result) : [],
				});
				expect(code).toBe(0);
				expect(envelope.ok).toBe(true);
				return envelope.result;
			} finally {
				clearTimeout(timer);
			}
		};
		const alive = (pid: number) => {
			try {
				process.kill(pid, 0);
				return true;
			} catch {
				return false;
			}
		};
		let unrelated: string[] = [];
		const unrelatedSnapshot = new Map<string, string>();
		const sessionSnapshot = (row: Record<string, unknown>) =>
			JSON.stringify({
				sessionId: row.sessionId,
				locator: row.locator,
				pid: row.pid,
				live: row.live,
				deleted: row.deleted,
			});
		const evidence: Record<string, unknown>[] = [];
		const failures: Record<string, unknown>[] = [];
		const preservation: Record<string, unknown>[] = [];
		let primaryEvidence: readonly Record<string, unknown>[] = [];
		let stage = "configuration";
		let manager: WorkLaneManager | undefined;
		let port: BrokerSessionPort | undefined;
		const cli: CliRunner = async (args, options) => {
			const observation = statusObservation.getStore();
			const result = await broker.cli(
				args,
				observation
					? {
							...options,
							timeoutMs: Math.max(1, Math.min(options?.timeoutMs ?? 30_000, observation.deadline - Date.now())),
						}
					: options,
			);
			try {
				const summary = diagnostic(JSON.parse(result.stdout));
				// A fixed-size ring survives generic WorkLaneManager error mapping.
				evidence.push({ method: observation?.method ?? "sdk.session", exitCode: result.exitCode, ...summary });
				if (evidence.length > 16) evidence.shift();
			} catch {
				evidence.push({
					method: observation?.method ?? "sdk.session",
					exitCode: result.exitCode,
					malformedEnvelope: true,
				});
				if (evidence.length > 16) evidence.shift();
			}
			return result;
		};
		try {
			stage = "broker-start";
			await broker.preflight();
			await broker.start();
			const discovery = await readBrokerDiscovery(broker.discoveryPath, alive);
			expect(Boolean(discovery)).toBe(true);
			const listed = await independent(["list", "--scope", "all"]);
			expect(Array.isArray(listed.sessions)).toBe(true);
			unrelated = listed.sessions.map((row: { sessionId: string }) => row.sessionId).sort();
			expect(unrelated.every((id) => typeof id === "string")).toBe(true);
			for (const row of listed.sessions) unrelatedSnapshot.set(row.sessionId, sessionSnapshot(row));
			port = new BrokerSessionPort({
				database,
				cli,
				instanceId: database.instanceId,
				authority: brokerAuthority,
				tailRunner: new TailRunner({ run: cli, stream: (id) => broker.openStream(id), repo, pollIntervalMs: 250 }),
			});
			// Retry the same read-only tuple; never bind, resume or submit another turn.
			const rawStatus = port.status.bind(port);
			port.status = async (input) => {
				const observation = statusObservation.getStore();
				if (!observation) return rawStatus(input);
				const report = await observeStatus({
					query: () => rawStatus(input),
					...observation,
					onRetry: (retry) => {
						evidence.push(retry);
						if (evidence.length > 16) evidence.shift();
					},
				});
				expect(report.operationRef).toBe(input.opRef);
				if (report.status.clientRef !== undefined) expect(report.status.clientRef).toBe(input.opRef);
				return report;
			};
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
				text: "Do not use tools. Write a detailed 1,800-word explanation of persistent-session recovery, with multiple sections. Do not finish early. Include WORK_LANE_LIVE_OK at the beginning.",
			});
			if (!started.started) throw new Error("fresh work lane unexpectedly held");
			expect(database.workAttemptGet(started.opRef)?.sendPhase).toBe("accepted");
			stage = "independent-active-turn-steer";
			const activeSessionId = started.sessionId;
			expect(unrelated.includes(activeSessionId)).toBe(false);
			const activeDeadline = Date.now() + 15_000;
			let active = false;
			while (Date.now() < activeDeadline) {
				const status = await statusObservation.run({ deadline: activeDeadline, method: "port.status.active" }, () =>
					port!.status({ sessionId: activeSessionId, repo, opRef: started.opRef }),
				);
				if (status.status.status === "in_flight") {
					active = true;
					break;
				}
				if (["terminal_ok", "failed", "rejected"].includes(status.status.status)) break;
				await Bun.sleep(Math.min(250, Math.max(0, activeDeadline - Date.now())));
			}
			expect(active).toBe(true);
			const steerMarker = `WORK_SHARED_STEER_${crypto.randomUUID().replaceAll("-", "")}`;
			const clientRef = `gw-e2e-independent-steer-${crypto.randomUUID()}`;
			const steer = await independent([
				"raw",
				"control",
				activeSessionId,
				"--op",
				"turn.steer",
				"--json-input",
				JSON.stringify({
					text: `Replace the remaining answer with exactly WORK_LANE_LIVE_OK ${steerMarker}.`,
					clientRef,
				}),
			]);
			expect(isSteerAccepted(steer)).toBe(true);
			expect(steer.clientRef === clientRef).toBe(true);
			evidence.push({ independentSteerAccepted: true });
			stage = "manager-status-and-settlement";
			const deadline = Date.now() + 120_000;
			while (Date.now() < deadline) {
				const status = await statusObservation.run({ deadline, method: "manager.status -> port.status" }, () =>
					manager!.status({ name: "live-smoke" }),
				);
				expect(status.sessionId).toBe(activeSessionId);
				expect(status.attempt?.opRef).toBe(started.opRef);
				if (status.op?.status === "failed") throw new Error("original operation failed");
				if (database.workAttemptGet(started.opRef)?.settledAt) {
					expect(status.op?.status).toBe("terminal_ok");
					expect(status.attempt?.endState).toBe("completed");
					break;
				}
				await Bun.sleep(Math.min(500, Math.max(0, deadline - Date.now())));
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
			stage = "same-operation-steer-output";
			expect(runtime.sessionId).toBe(activeSessionId);
			const terminal = await statusObservation.run({ deadline, method: "port.status.terminal" }, () =>
				port!.status({ sessionId: activeSessionId, repo, opRef: started.opRef }),
			);
			const output = await port.fetchWorkerOutput({
				sessionId: activeSessionId,
				repo,
				opRef: started.opRef,
				notBeforeMs: Math.max(Date.parse(runtime.startedAt), runtime.terminal?.status?.startedAt ?? 0),
				terminalIdentity: runtime.terminal?.status,
			});
			expect(
				provesSharedSteer({
					output,
					sessionId: activeSessionId,
					repo,
					opRef: started.opRef,
					marker: steerMarker,
					clientRef,
					receipt: steer,
					terminal,
				}),
			).toBe(true);
			stage = "independent-global-authority";
			const sessionId = activeSessionId;
			expect(unrelated.includes(sessionId)).toBe(false);
			expect(
				(await independent(["list", "--scope", "all"])).sessions.some(
					(row: { sessionId: string }) => row.sessionId === sessionId,
				),
			).toBe(true);
			const inspected = await independent(["inspect", sessionId, "--repo", repo]);
			expect(inspected.session.sessionId).toBe(sessionId);
			expect(inspected.session.live).toBe(true);
			await manager.stop();
			const generation = broker.generation;
			await broker.stop();
			const stopped = await readBrokerDiscovery(broker.discoveryPath, alive);
			expect(
				stopped?.pid === discovery?.pid && stopped?.url === discovery?.url && stopped?.token === discovery?.token,
			).toBe(true);
			await broker.start();
			expect(broker.generation).toBe(generation);
			expect((await independent(["inspect", sessionId, "--repo", repo])).session.live).toBe(true);
		} catch {
			// Freeze primary evidence before cleanup can issue more SDK requests.
			failures.push({ stage, evidence: [...evidence] });
		} finally {
			primaryEvidence = [...evidence];
			try {
				await manager?.stop();
			} catch {
				failures.push({ stage: "manager-stop" });
			}
			try {
				await broker.start();
				// This fresh database contains only sessions created by this test's port.
				for (const row of database.workLaneRows()) {
					expect(unrelated.includes(row.gjc_session_id)).toBe(false);
					await port?.close({ sessionId: row.gjc_session_id, repo });
				}
			} catch {
				failures.push({ stage: "own-session-cleanup" });
			}
			try {
				await broker.stop();
			} catch {
				failures.push({ stage: "client-stop" });
			}
			try {
				if (unrelated.length) {
					const remaining = (await independent(["list", "--scope", "all"])).sessions as Array<Record<string, unknown>>;
					for (const [id, snapshot] of unrelatedSnapshot) {
						const row = remaining.find((item) => item.sessionId === id);
						expect(
							row !== undefined && sessionSnapshot(row) === snapshot,
							"unrelated session identity/liveness changed",
						).toBe(true);
					}
				}
			} catch {
				failures.push({ stage: "unrelated-session-preservation" });
			}
			try {
				database.close();
			} catch {
				failures.push({ stage: "database-close" });
			}
			try {
				await rm(home, { recursive: true, force: true });
			} catch {
				failures.push({ stage: "temporary-workspace-cleanup" });
			}
			// Last observation: no SDK command may run after this preservation snapshot.
			try {
				const afterConfig = await configSnapshot();
				for (const [name, bytes] of beforeConfig) {
					const current = afterConfig.get(name) ?? null;
					if (!(bytes === null ? current === null : current !== null && bytes.equals(current)))
						preservation.push(preservationDelta(name, bytes, current));
				}
			} catch {
				failures.push({ stage: "config-preservation-check" });
			}
		}
		if (failures.length || preservation.length)
			throw new Error(
				"work live failure " + JSON.stringify({ stage, evidence: primaryEvidence, failures, preservation }),
			);
		console.error(
			"work_live_evidence " +
				JSON.stringify({
					started: true,
					accepted: true,
					terminal: "end_turn",
					output: "proven",
					source: "turn.result",
					independentSteerAccepted: true,
					sameOperationTerminal: true,
					steerMarkerObserved: true,
					preservationPassed: true,
				}),
		);
	},
	300_000,
);
