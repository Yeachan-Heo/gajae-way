import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { isTerminalStatus } from "@gajae-gateway/subsession";
import { GlobalGjcClient, readBrokerDiscovery } from "../src/orchestrator/broker";
import { BrokerSessionPort, SessionTerminalError } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { GatewayDatabase } from "../src/store/db";

const liveTest = process.env.GAJAEWAY_E2E_GJC === "1" ? test : test.skip;

async function waitForTerminal(port: BrokerSessionPort, sessionId: string, repo: string, opRef: string): Promise<void> {
	const deadline = Date.now() + 120_000;
	while (Date.now() < deadline) {
		const status = await port.status({ sessionId, repo, opRef });
		if (isTerminalStatus(status.status.status)) {
			if (status.status.status !== "terminal_ok") throw new SessionTerminalError(status);
			return;
		}
		await Bun.sleep(250);
	}
	throw new Error("live SDK operation exceeded its bounded E2E window");
}

// Retain bytes in memory only; assertion failures must not print user configuration.
async function configSnapshot(agentDir: string): Promise<Map<string, Buffer | null>> {
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

liveTest(
	"real global session preserves user authority across sends, tail and client reconnect",
	async () => {
		const executable = process.env.GJC_EXECUTABLE;
		const agentDir = process.env.GJC_CODING_AGENT_DIR;
		const model = process.env.GAJAEWAY_E2E_MODEL;
		if (!executable || !agentDir || !model || !isAbsolute(executable) || !isAbsolute(agentDir)) {
			throw new Error(
				"live test requires explicit absolute GJC_EXECUTABLE/GJC_CODING_AGENT_DIR and GAJAEWAY_E2E_MODEL from existing user configuration",
			);
		}
		const home = await mkdtemp(join(tmpdir(), "gajaeway-persistent-e2e-"));
		const repo = join(home, "workspace");
		await mkdir(repo, { mode: 0o700 });
		const initialized = Bun.spawn(["git", "init", "--quiet", repo], { stdout: "ignore", stderr: "pipe" });
		expect(await initialized.exited).toBe(0);
		const database = await GatewayDatabase.open(join(home, "gateway.db"));
		const broker = new GlobalGjcClient({ executable, agentDir, cwd: repo, healthIntervalMs: 60_000 });
		const canonicalAgentDir = await realpath(agentDir);
		const brokerAuthority = { canonicalAgentDir, identity: `gjc:${canonicalAgentDir}` };
		database.assertBrokerAuthority(brokerAuthority, { initializeEmpty: true });
		const beforeConfig = await configSnapshot(agentDir);
		const failures: Record<string, unknown>[] = [];
		const preservation: Record<string, unknown>[] = [];
		let stage = "preflight";
		let sessionId: string | undefined;
		let port: BrokerSessionPort | undefined;
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
		const alive = (pid: number) => {
			try {
				process.kill(pid, 0);
				return true;
			} catch {
				return false;
			}
		};
		// Independent launcher invocation: never route this corroboration through the gateway client.
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
				expect(code).toBe(0);
				const envelope = JSON.parse(text);
				expect(envelope.ok).toBe(true);
				return envelope.result;
			} finally {
				clearTimeout(timer);
			}
		};
		try {
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
				cli: broker.cli,
				instanceId: database.instanceId,
				authority: brokerAuthority,
				tailRunner: new TailRunner({
					stream: (id) => broker.openStream(id),
					repo,
				}),
			});
			stage = "own-session-bind-and-model";
			const binding = await port.bind({ originKey: "loopback/loopback/persistent-e2e", epoch: 0, repo });
			sessionId = binding.sessionId;
			expect(unrelated.includes(sessionId)).toBe(false);
			await port.setModel({ sessionId, repo, selection: model });
			const inspect = await independent(["inspect", sessionId, "--repo", repo]);
			expect(inspect.session.sessionId).toBe(sessionId);
			expect(inspect.session.live).toBe(true);
			expect(
				(await independent(["list", "--scope", "all"])).sessions.some(
					(row: { sessionId: string }) => row.sessionId === sessionId,
				),
			).toBe(true);
			stage = "own-session-send-and-duplicate";
			const firstRef = `gw-e2e-first-${crypto.randomUUID()}`;
			const first = await port.request({
				sessionId,
				repo,
				originKey: "loopback/loopback/persistent-e2e",
				text: "Do not use tools. Reply with exactly PERSISTENT_E2E_FIRST.",
				opRef: firstRef,
				waitTimeoutMs: 120_000,
				pollMs: 250,
			});
			expect(first.assistant.text.includes("PERSISTENT_E2E_FIRST")).toBe(true);
			await expect(
				port.send({ sessionId, repo, text: "This duplicate must be rejected.", opRef: firstRef }),
			).rejects.toMatchObject({ name: "OpRefRejectedError", code: "client_ref_conflict" });
			let marker = "PERSISTENT_E2E_FIRST";
			if (process.env.GAJAEWAY_E2E_STEER === "1") {
				stage = "own-session-steer";
				const opRef = `gw-e2e-steer-${crypto.randomUUID()}`;
				marker = `PERSISTENT_E2E_STEER_${crypto.randomUUID().replaceAll("-", "")}`;
				const notBeforeMs = Date.now();
				const receipt = await port.send({
					sessionId,
					repo,
					text: "Without using tools, write a detailed 1,800-word explanation of persistent-session recovery. Use multiple sections and do not finish early.",
					opRef,
				});
				expect(receipt.operationRef).toBe(opRef);
				const deadline = Date.now() + 15_000;
				let inFlight = false;
				while (Date.now() < deadline) {
					const status = await port.status({ sessionId, repo, opRef });
					if (status.status.status === "in_flight") {
						inFlight = true;
						break;
					}
					if (isTerminalStatus(status.status.status)) break;
					await Bun.sleep(250);
				}
				expect(inFlight).toBe(true);
				await port.steer({
					sessionId,
					repo,
					text: `End the answer with exactly ${marker}.`,
					clientRef: `gw-e2e-control-${crypto.randomUUID()}`,
				});
				await waitForTerminal(port, sessionId, repo, opRef);
				const terminal = await port.status({ sessionId, repo, opRef });
				expect(terminal.operationRef).toBe(opRef);
				const output = await port.fetchWorkerOutput({
					sessionId,
					repo,
					opRef,
					notBeforeMs,
					terminalIdentity: terminal.status,
				});
				expect(output.status).toBe("proven");
				if (output.status !== "proven") throw new Error("original steer output unavailable");
				expect(
					output.provenance.sessionId === sessionId &&
						output.provenance.opRef === opRef &&
						output.provenance.repo === repo,
				).toBe(true);
				expect(output.text.includes(marker)).toBe(true);
			}
			stage = "tail-evidence";
			const tail = await independent(["tail", sessionId, "--until-idle", "--all-events", "--timeout-ms", "20000"]);
			expect(Array.isArray(tail.items)).toBe(true);
			expect(
				tail.items.some(
					(item: { kind?: string; payload?: { role?: string; content?: unknown } }) =>
						item.kind === "transcript" &&
						item.payload?.role === "assistant" &&
						Array.isArray(item.payload.content) &&
						JSON.stringify(item.payload.content).includes(marker),
				),
			).toBe(true);
			stage = "client-stop-and-reconnect";
			const generation = broker.generation;
			await broker.stop();
			const stopped = await readBrokerDiscovery(broker.discoveryPath, alive);
			expect(
				stopped?.pid === discovery?.pid && stopped?.url === discovery?.url && stopped?.token === discovery?.token,
			).toBe(true);
			await broker.start();
			expect(broker.generation).toBe(generation);
			expect((await independent(["inspect", sessionId, "--repo", repo])).session.live).toBe(true);
		} catch (error) {
			const code = error instanceof SessionTerminalError ? error.status.status.error?.code : undefined;
			failures.push({
				stage,
				code: typeof code === "string" && /^[a-z][a-z_0-9]{1,63}$/.test(code) ? code : "other_sdk_error",
			});
		} finally {
			try {
				if (sessionId && port) {
					await broker.start();
					await port.close({ sessionId, repo });
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
				const after = await configSnapshot(agentDir);
				for (const [name, bytes] of beforeConfig) {
					const current = after.get(name) ?? null;
					if (!(bytes === null ? current === null : current !== null && bytes.equals(current)))
						preservation.push(preservationDelta(name, bytes, current));
				}
			} catch {
				failures.push({ stage: "config-preservation-check" });
			}
		}
		if (failures.length || preservation.length)
			throw new Error("persistent live failure " + JSON.stringify({ stage, failures, preservation }));
	},
	300_000,
);
