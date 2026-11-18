import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, realpath, rm } from "node:fs/promises";
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

async function psOutput(args: string[], env: Record<string, string>): Promise<string> {
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
		return result[1];
	} finally {
		clearTimeout(timer);
		if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
	}
}

async function processSnapshot(env: Record<string, string>, pid?: number): Promise<ProcessRecord[]> {
	const columns = process.platform === "darwin" ? "pid=,ppid=,command=" : "pid=,ppid=,args=";
	// A global snapshot is also used for revalidation: ps commonly exits 1 for
	// a vanished single PID, which must not be confused with an unreadable host.
	const output = await psOutput(["-axo", columns], env);
	const records = output.split("\n").flatMap((line) => {
		const match = line.match(/^\s*(\d+)\s+(\d+)\s+(.*)$/);
		if (!match) {
			if (line.trim()) throw new Error("unparseable process ownership snapshot");
			return [];
		}
		const command = match[3]!.trim();
		return [{ pid: Number(match[1]), ppid: Number(match[2]), command }];
	});
	if (records.length === 0) throw new Error("empty process ownership snapshot");
	return pid === undefined ? records : records.filter((record) => record.pid === pid);
}

async function reapOwnedProcesses(agentDir: string, env: Record<string, string>, executable: string): Promise<void> {
	if (process.platform !== "linux" && process.platform !== "darwin")
		throw new Error("process environment ownership unsupported; retaining scratch state");
	const canonicalAgentDir = await realpath(agentDir);
	const runtime = await realpath(executable);
	// npm's wrapper launches a sibling coding-agent entrypoint. Restrict probes
	// to that installation, or to the exact standalone executable, before reading
	// process environment evidence. Unrelated runtime installations are untouched.
	const modules = runtime.indexOf("/node_modules/");
	const runtimeFamily = modules < 0 ? runtime : runtime.slice(0, modules + "/node_modules/".length);
	const owner = env.GAJAEWAY_TEST_OWNER;
	if (!owner) throw new Error("missing fixture ownership marker; retaining scratch state");
	const isCandidate = ({ pid, command }: ProcessRecord) =>
		pid !== process.pid &&
		command.includes(runtimeFamily) &&
		/(?:^|\s)sdk\s+(?:session-host-internal|broker-internal|broker\s+run)(?:\s|$)/.test(command);
	const isOwned = async ({ pid }: ProcessRecord): Promise<boolean> => {
		try {
			// Detached GJC hosts have no agentDir in argv. Read only per-PID
			// environment evidence; never put it (or a read error) in diagnostics.
			const entries =
				process.platform === "linux"
					? (await readFile(`/proc/${pid}/environ`, { encoding: "utf8", signal: AbortSignal.timeout(1_000) })).split(
							"\0",
						)
					: (await psOutput(["eww", "-p", String(pid), "-o", "command="], env)).trim().split(/\s+/);
			if (process.platform === "darwin" && /\s/.test(canonicalAgentDir))
				throw new Error("ambiguous ps environment directory");
			if (process.platform === "darwin" && !entries.some((entry) => /^[A-Za-z_][A-Za-z0-9_]*=/.test(entry)))
				throw new Error("ps environment unavailable");
			return (
				entries.includes(`GAJAEWAY_TEST_OWNER=${owner}`) &&
				["GJC_AGENT_DIR", "GJC_CODING_AGENT_DIR"].some((key) => entries.includes(`${key}=${canonicalAgentDir}`))
			);
		} catch {
			if ((await processSnapshot(env, pid)).length === 0) return false;
			throw new Error(`cannot establish environment ownership for process ${pid}; retaining scratch state`);
		}
	};
	const ownedProcesses = async () => {
		const owned: ProcessRecord[] = [];
		const deadline = Date.now() + 10_000;
		for (const record of (await processSnapshot(env)).filter(isCandidate)) {
			if (Date.now() >= deadline) throw new Error("process ownership scan timed out; retaining scratch state");
			if (await isOwned(record)) owned.push(record);
		}
		return owned;
	};
	for (const signal of ["SIGTERM", "SIGKILL"] as const) {
		const deadline = Date.now() + 3_000;
		do {
			const owned = await ownedProcesses();
			if (owned.length === 0) return;
			for (const record of owned) {
				if (Date.now() >= deadline) break;
				// Re-read ownership immediately before each signal: never kill by name
				// or by a stale PID from a previous snapshot.
				const current = (await processSnapshot(env, record.pid))[0];
				if (!current) continue;
				if (current.command !== record.command || !isCandidate(current) || !(await isOwned(current)))
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
	const remaining = await ownedProcesses();
	if (remaining.length !== 0)
		throw new Error(`owned processes remain: ${remaining.map(({ pid }) => pid).join(", ")}; retaining scratch state`);
}

test("cleanup finds an env-only SDK host without signaling a differently owned sibling", async () => {
	const root = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-gjc-reaper-")));
	const agentDir = join(root, "agent");
	await mkdir(agentDir);
	const env = {
		PATH: process.env.PATH ?? "/usr/bin:/bin",
		HOME: root,
		LANG: "C",
		GAJAEWAY_TEST_OWNER: randomUUID(),
		GJC_AGENT_DIR: agentDir,
	};
	const launch = (owner: string) =>
		Bun.spawn(
			[
				process.execPath,
				"--eval",
				'console.log("ready"); setInterval(() => {}, 1000)',
				"--",
				"sdk",
				"session-host-internal",
			],
			{ env: { ...env, GAJAEWAY_TEST_OWNER: owner }, stdin: "ignore", stdout: "pipe", stderr: "ignore" },
		);
	const children: ReturnType<typeof launch>[] = [];
	const errors: unknown[] = [];
	try {
		const owned = launch(env.GAJAEWAY_TEST_OWNER);
		children.push(owned);
		const sibling = launch(randomUUID());
		children.push(sibling);
		for (const child of children) {
			const reader = child.stdout.getReader();
			let timer: ReturnType<typeof setTimeout> | undefined;
			try {
				const ready = await Promise.race([
					reader.read(),
					new Promise<never>((_, reject) => {
						timer = setTimeout(() => reject(new Error("SDK host-shaped child startup timed out")), 2_000);
					}),
				]);
				expect(new TextDecoder().decode(ready.value).trim()).toBe("ready");
			} finally {
				clearTimeout(timer);
				reader.releaseLock();
			}
		}
		const before = await processSnapshot(env);
		for (const child of children) {
			const record = before.find(({ pid }) => pid === child.pid);
			expect(record?.command).toContain("sdk session-host-internal");
			expect(record?.command).not.toContain(agentDir);
		}
		// No session.create result or session.close reply is available here.
		await reapOwnedProcesses(agentDir, env, process.execPath);
		expect((await processSnapshot(env)).some(({ pid }) => pid === owned.pid)).toBe(false);
		expect((await processSnapshot(env)).some(({ pid }) => pid === sibling.pid)).toBe(true);
		expect(sibling.exitCode).toBeNull();
		expect(sibling.signalCode).toBeNull();
		process.kill(sibling.pid, 0);
	} catch (error) {
		errors.push(error);
	} finally {
		let allStopped = true;
		// Stop the deliberately unowned sibling by its direct child handle, not
		// by the ownership sweep. Keep these waits independent and bounded.
		for (const child of children) {
			let timer: ReturnType<typeof setTimeout> | undefined;
			try {
				if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
				await Promise.race([
					child.exited,
					new Promise<never>((_, reject) => {
						timer = setTimeout(() => reject(new Error("SDK host-shaped child exit unconfirmed")), 2_000);
					}),
				]);
			} catch (error) {
				allStopped = false;
				errors.push(error);
			} finally {
				clearTimeout(timer);
			}
		}
		if (allStopped) {
			try {
				await rm(root, { recursive: true, force: true });
			} catch (error) {
				errors.push(error);
			}
		}
	}
	if (errors.length) throw new AggregateError(errors, `env-only host cleanup regression failed (${root})`);
}, 30_000);

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
			GAJAEWAY_TEST_OWNER: randomUUID(),
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
				await reapOwnedProcesses(agentDir, env, executable);
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
