import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, realpath, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { inspectSession, listSessions, parseEnvelope } from "@gajae-gateway/subsession";
import { GlobalGjcClient, type SpawnFn } from "../src/orchestrator/broker";
import { locateSavedTranscript } from "../src/orchestrator/failed-turn-evidence";
import { isSessionGoneCode, isVerifiedGjcVersion, VERIFIED_GJC_THROUGH } from "../src/orchestrator/gjc-contract";
import { parseRunningJobs } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { type LiveBroker, superviseLiveBroker } from "./live-broker";

/**
 * Contract check against the INSTALLED gjc. Run it before raising
 * VERIFIED_GJC_THROUGH and on every host after a gjc upgrade:
 *   GAJAEWAY_E2E_GJC=1 bun test packages/gateway/test/gjc-contract.e2e.test.ts
 * The first probe is read-only; the lifecycle probe creates and closes only a
 * scratch session under its own temporary agent directory, without an LLM turn.
 */
const liveTest = process.env.GAJAEWAY_E2E_GJC === "1" ? test : test.skip;

async function failureCode(client: GlobalGjcClient, args: string[]): Promise<unknown> {
	const result = await client.cli(args, { timeoutMs: 30_000 });
	const envelope = JSON.parse(result.stdout) as { ok?: unknown; error?: { code?: unknown } };
	return envelope.ok === false ? envelope.error?.code : undefined;
}

liveTest(
	"the installed gjc is inside the verified contract and reports a dropped session with a session-gone code",
	async () => {
		const executable = process.env.GJC_EXECUTABLE;
		const agentDir = process.env.GJC_CODING_AGENT_DIR;
		if (!executable || !agentDir || !isAbsolute(executable) || !isAbsolute(agentDir)) {
			throw new Error("live GJC contract test requires explicit absolute GJC_EXECUTABLE and GJC_CODING_AGENT_DIR");
		}
		const supervisor = await superviseLiveBroker(executable, agentDir);
		try {
			const client = new GlobalGjcClient({ executable, agentDir });
			await client.preflight();
			const version = client.gjcVersion ?? "";
			expect({ version, verified: isVerifiedGjcVersion(version) }).toEqual({ version, verified: true });
			const missing = randomUUID();
			for (const args of [
				["sdk", "session", "inspect", missing],
				["sdk", "session", "status", missing, "gw-p-contract-probe"],
			]) {
				const code = await failureCode(client, args);
				expect({ args: args.slice(2, 3), code, gone: isSessionGoneCode(code) }).toEqual({
					args: args.slice(2, 3),
					code,
					gone: true,
				});
			}
		} finally {
			await supervisor.stop();
		}
	},
	120_000,
);

test("the verified ceiling is a major.minor pair", () => {
	expect(VERIFIED_GJC_THROUGH).toMatch(/^\d+\.\d+$/);
});

interface ProcessRecord {
	readonly pid: number;
	readonly ppid: number;
	readonly command: string;
}

// The canonical inspect DTO is IndexedSession, not the normalized BrokerSession
// returned by inspectSession (which intentionally drops hostIncarnation).
type InspectedSession = {
	readonly sessionId: string;
	readonly live: boolean;
	readonly hostIncarnation?: string;
};

async function processSnapshot(env: Record<string, string>): Promise<ProcessRecord[]> {
	const args = process.platform === "darwin" ? ["-axo", "pid=,ppid=,command="] : ["-eo", "pid=,ppid=,args="];
	const child = Bun.spawn(["ps", ...args], { env, stdin: "ignore", stdout: "pipe", stderr: "ignore" });
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		const result = await Promise.race([
			Promise.all([child.exited, new Response(child.stdout).text()]),
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error("process ownership snapshot timed out")), 5_000);
			}),
		]);
		if (result[0] !== 0) throw new Error("could not establish process ownership with ps");
		const records = result[1].split("\n").flatMap((line) => {
			const match = line.match(/^\s*(\d+)\s+(\d+)\s+(.*)$/);
			if (!match) {
				if (line.trim()) throw new Error("unparseable process ownership snapshot");
				return [];
			}
			const command = match[3]!.trim();
			return [{ pid: Number(match[1]), ppid: Number(match[2]), command }];
		});
		if (records.length === 0) throw new Error("empty process ownership snapshot");
		return records;
	} finally {
		clearTimeout(timer);
		if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
	}
}

async function reapOwnedProcesses(agentDir: string, env: Record<string, string>): Promise<void> {
	const exactAgentDir = new RegExp(`(?:^|[\\s"'=])${agentDir.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}(?=$|[\\s"'])`);
	const isOwned = ({ command }: ProcessRecord) =>
		exactAgentDir.test(command) && /\bsdk\s+(?:session-host-internal|broker-internal|broker\s+run)\b/.test(command);
	for (const signal of ["SIGTERM", "SIGKILL"] as const) {
		const deadline = Date.now() + 3_000;
		do {
			const owned = (await processSnapshot(env)).filter(isOwned);
			if (owned.length === 0) return;
			for (const record of owned) {
				// Re-read ownership immediately before each signal: never kill by name
				// or by a stale PID from a previous snapshot.
				const current = (await processSnapshot(env)).find(({ pid }) => pid === record.pid);
				if (!current) continue;
				if (current.command !== record.command)
					throw new Error(`process ${record.pid} changed identity; retaining scratch state`);
				try {
					process.kill(record.pid, signal);
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
				}
			}
			await Bun.sleep(100);
		} while (Date.now() < deadline);
	}
	const remaining = (await processSnapshot(env)).filter(isOwned);
	if (remaining.length !== 0)
		throw new Error(`owned processes remain: ${JSON.stringify(remaining)}; retaining scratch state`);
}

liveTest(
	"the pinned package preserves lifecycle idempotency, relay queries, and close/resume identity",
	async () => {
		const executable = process.env.GJC_EXECUTABLE;
		if (!executable || !isAbsolute(executable)) throw new Error("requires an absolute GJC_EXECUTABLE");
		const root = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-gjc-contract-")));
		const agentDir = join(root, "agent");
		const repo = join(root, "repo");
		const home = join(root, "home");
		const env = {
			PATH: process.env.PATH ?? "/usr/bin:/bin",
			HOME: home,
			USERPROFILE: home,
			LANG: "C",
			TMPDIR: home,
			GJC_CODING_AGENT_DIR: agentDir,
			GJC_SDK_BROKER_AUTOSTART: "0",
		};
		let supervisor: LiveBroker | undefined;
		let client: GlobalGjcClient | undefined;
		let sessionId: string | undefined;
		let close: (() => Promise<unknown>) | undefined;
		const errors: unknown[] = [];
		try {
			await mkdir(home);
			await mkdir(repo);
			await mkdir(agentDir);
			supervisor = await superviseLiveBroker(executable, agentDir, { env, cwd: repo });
			// The client builds an inherited environment internally; replace it at
			// the spawn boundary for every CLI/relay without mutating process.env.
			const spawn = ((options: { readonly cmd: readonly string[]; readonly [key: string]: unknown }) =>
				Bun.spawn({ ...options, cmd: [...options.cmd], env: { ...env }, cwd: repo } as never)) as unknown as SpawnFn;
			const activeClient = new GlobalGjcClient({ executable, agentDir, cwd: repo, spawn });
			client = activeClient;
			const lifecycle = async (operation: string, key: string, input: Record<string, unknown>) =>
				parseEnvelope<{ sessionId: string }>(
					await activeClient.cli(
						[
							"sdk",
							"session",
							"raw",
							"global",
							"--op",
							operation,
							"--idempotency-key",
							key,
							"--json-input",
							JSON.stringify(input),
						],
						{ timeoutMs: 90_000 },
					),
					operation,
				);
			close = async () => lifecycle("session.close", `close-${randomUUID()}`, { sessionId, cwd: repo });
			const inspect = async () =>
				parseEnvelope<{ session: InspectedSession }>(
					await activeClient.cli(["sdk", "session", "inspect", sessionId!]),
					"session inspect",
				).session;
			await activeClient.preflight();
			await activeClient.start();
			const key = `create-${randomUUID()}`;
			const createInput = { cwd: repo, readinessTimeoutMs: 60_000 };
			const created = await lifecycle("session.create", key, createInput);
			sessionId = created.sessionId;
			expect(typeof sessionId).toBe("string");
			expect((await lifecycle("session.create", key, createInput)).sessionId).toBe(sessionId);
			const controller = { run: activeClient.cli, repo };
			expect(await inspectSession(controller, sessionId)).toMatchObject({ sessionId, repo, live: true });
			const originalHost = await inspect();
			expect(originalHost).toMatchObject({ sessionId, live: true });
			expect(typeof originalHost.hostIncarnation).toBe("string");
			expect(originalHost.hostIncarnation?.length).toBeGreaterThan(0);
			expect(await listSessions(controller)).toContainEqual(expect.objectContaining({ sessionId, repo, live: true }));
			const runner = new TailRunner({ stream: (id) => activeClient.openStream(id), repo });
			const tail = await runner.attach({ sessionId, brokerGeneration: 1, repo });
			try {
				const jobs = await tail.query("runtime.jobs.list", {});
				expect(jobs.ok).toBe(true);
				expect(parseRunningJobs(JSON.stringify(jobs))).toEqual([]);
			} finally {
				await tail.close();
			}
			const status = parseEnvelope<{ status: { status: string; receiptState: string } }>(
				await activeClient.cli(["sdk", "session", "status", sessionId, `missing-${randomUUID()}`]),
				"status",
			);
			expect(status.status).toEqual({ status: "unknown", receiptState: "unknown" });
			await close();
			expect(await inspect()).toMatchObject({ sessionId, live: false });
			const sessionPath = await locateSavedTranscript(agentDir, sessionId);
			expect(typeof sessionPath).toBe("string");
			expect(
				(
					await lifecycle("session.resume", `resume-${randomUUID()}`, {
						sessionId,
						sessionPath,
						cwd: repo,
						readinessTimeoutMs: 60_000,
					})
				).sessionId,
			).toBe(sessionId);
			const resumedHost = await inspect();
			expect(resumedHost).toMatchObject({ sessionId, live: true });
			expect(typeof resumedHost.hostIncarnation).toBe("string");
			expect(resumedHost.hostIncarnation?.length).toBeGreaterThan(0);
			expect(resumedHost.hostIncarnation).not.toBe(originalHost.hostIncarnation);
		} catch (error) {
			errors.push(error);
		} finally {
			// Each teardown stage runs even when the preceding one fails. The
			// process sweep also covers a create whose result never reached us.
			for (const cleanup of [
				async () => {
					if (sessionId) await close?.();
				},
				async () => {
					await client?.stop();
				},
				async () => {
					await supervisor?.stop();
				},
			]) {
				try {
					await cleanup();
				} catch (error) {
					errors.push(error);
				}
			}
			try {
				await reapOwnedProcesses(agentDir, env);
				// Never delete broker/session authority until ps proves no owned
				// broker or detached host remains. Snapshot failure retains root.
				await rm(root, { recursive: true, force: true });
			} catch (error) {
				errors.push(new Error(`scratch cleanup failed; retained ${root}`, { cause: error }));
			}
		}
		if (errors.length) throw new AggregateError(errors, `lifecycle contract failed (${root})`);
	},
	180_000,
);
