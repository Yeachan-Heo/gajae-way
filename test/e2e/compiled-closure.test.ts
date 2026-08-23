import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { BrokerCli } from "../../src/broker/cli";
import { RpcClient } from "../../src/rpc-client";
import { canonicalJson } from "../../src/main-session/gates";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore } from "../../src/native-loader";
import { ManagedProcessRegistry, type ManagedBunProcess } from "../helpers/managed-process";
import { FakeBrokerFixture } from "../helpers/main-session";

const managedProcesses = new ManagedProcessRegistry();
const repositoryRoot = path.resolve(import.meta.dir, "..", "..");

// Real daemon health/status RPCs can be delayed by cold boot and CPU contention; 5s is the per-call budget while outer polling remains bounded.
const DAEMON_RPC_TIMEOUT_MS = 5_000;
afterEach(async () => {
	await managedProcesses.reapAll();
});

function compiledWay(): string {
	const executable = path.join(repositoryRoot, "dist", "gajaeway");
	if (!fs.existsSync(executable)) throw new Error("The compiled closure drill requires dist/gajaeway. Run bun scripts/compile.ts before bun test.");
	return executable;
}

async function run(command: readonly string[], cwd = repositoryRoot, env?: NodeJS.ProcessEnv): Promise<string> {
	const child = Bun.spawn({ cmd: [...command], cwd, env, stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
	if (exitCode !== 0) throw new Error(`${command.join(" ")} failed (${exitCode}): ${stderr || stdout}`);
	return stdout;
}

function profile(corpus: string, workspace: string, sessionId: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[main_session]
session_id = "${sessionId}"

[surfaces.owner]
id = "closure-owner"
platform = "test"
kind = "dm"
`;
}

async function connectHealthy(socketPath: string): Promise<RpcClient> {
	let lastError: unknown;
	for (let attempt = 0; attempt < 200; attempt += 1) {
		let client: RpcClient | undefined;
		try {
			client = await RpcClient.connect(socketPath);
			const health = await client.request("way.health", {}, { timeoutMs: DAEMON_RPC_TIMEOUT_MS });
			if ((health.result as { status?: string } | undefined)?.status === "healthy") return client;
			client.close();
		} catch (error) {
			client?.close();
			lastError = error;
		}
		await Bun.sleep(25);
	}
	throw new Error(`compiled daemon did not become healthy${lastError instanceof Error ? `: ${lastError.message}` : ""}`);
}

async function connectAvailable(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 200; attempt += 1) {
		try {
			return await RpcClient.connect(socketPath);
		} catch {
			await Bun.sleep(25);
		}
	}
	throw new Error("compiled daemon did not expose its RPC listener");
}

async function waitForPendingTranscriptVerification(
	client: RpcClient,
	transcriptProof: "pending" | "proven" = "pending",
): Promise<Record<string, unknown>> {
	for (let attempt = 0; attempt < 200; attempt += 1) {
		const response = await client.request("way.status", {}, { timeoutMs: DAEMON_RPC_TIMEOUT_MS });
		const status = response.result as Record<string, unknown> | undefined;
		if (
			status?.state === "verifying" &&
			status.transcript_proof === transcriptProof &&
			status.transcript_verification === "pending"
		) {
			return status;
		}
		await Bun.sleep(25);
	}
	throw new Error("compiled daemon did not publish pending transcript verification");
}

async function waitForFailedClosed(client: RpcClient): Promise<Record<string, unknown>> {
	for (let attempt = 0; attempt < 200; attempt += 1) {
		const health = await client.request("way.health", {}, { timeoutMs: DAEMON_RPC_TIMEOUT_MS });
		if ((health.result as { state?: unknown } | undefined)?.state === "failed_closed") return health.result as Record<string, unknown>;
		await Bun.sleep(25);
	}
	throw new Error("compiled daemon did not become failed_closed");
}

async function eventually<T>(read: () => Promise<T | undefined>, description: string, timeoutMs = 15_000): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	let lastError: unknown;
	while (Date.now() < deadline) {
		try {
			const value = await read();
			if (value !== undefined) return value;
		} catch (error) {
			lastError = error;
		}
		await Bun.sleep(25);
	}
	throw new Error(`${description}${lastError instanceof Error ? `: ${lastError.message}` : ""}`);
}

function spawnCompiledDaemon(command: readonly string[], environment: NodeJS.ProcessEnv): ManagedBunProcess {
	return managedProcesses.spawnDaemon({ cmd: command, cwd: repositoryRoot, env: environment, stderr: "pipe" });
}

test("compiled daemon adopts an external broker session and makes corpus closure results durable and replayable", async () => {
	const executable = compiledWay();
	const fixture = new FakeBrokerFixture();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-compiled-closure-"));
	const corpus = path.join(root, "corpus");
	const remote = path.join(root, "remote.git");
	const state = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	const socketPath = path.join(state, "rpc.sock");
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	};
	let daemon: ManagedBunProcess | undefined;
	let client: RpcClient | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "Compiled Closure Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "compiled-closure@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);
		fs.writeFileSync(profilePath, profile(corpus, fixture.workspace, fixture.sessionId));

		await run([executable, "bootstrap", "--confirm", "--state-dir", state, "--profile", profilePath], repositoryRoot, environment);
		expect(fixture.commands()).toEqual([]);
		daemon = spawnCompiledDaemon([executable, "serve", "--state-dir", state, "--profile", profilePath], environment);
		client = await connectHealthy(socketPath);
		expect((await client.request("way.status", {})).result).toMatchObject({ status: "healthy", main: { session_id: fixture.sessionId } });

		fs.writeFileSync(path.join(corpus, "compiled-close.txt"), "closed by compiled daemon\n");
		const params = { paths: ["compiled-close.txt"], commit_message: "compiled external closure", idempotency_key: "compiled-closure" };
		const first = await client.request("main.corpus.close", params, { timeoutMs: 10_000 });
		expect(first.result).toMatchObject({ committed: true });
		const replay = await client.request("main.corpus.close", params);
		expect(replay.result).toEqual(first.result);
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:compiled-close.txt"])).toBe("closed by compiled daemon\n");
		expect(fixture.commands()).toEqual([]);
	} finally {
		client?.close();
		if (daemon) await managedProcesses.stopDaemon(daemon);
		fixture.dispose();
		fs.rmSync(root, { force: true, recursive: true });
	}
}, 30_000);

test("compiled daemon fences corpus closure before durable claims or Git effects while transcript proof is pending", async () => {
	const executable = compiledWay();
	const fixture = new FakeBrokerFixture();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "g-pc-"));
	const corpus = path.join(root, "corpus");
	const remote = path.join(root, "remote.git");
	const state = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	const socketPath = path.join(state, "rpc.sock");
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	};
	let daemon: ManagedBunProcess | undefined;
	let client: RpcClient | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "Compiled Pending Closure Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "compiled-pending-closure@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);
		fs.writeFileSync(profilePath, profile(corpus, fixture.workspace, fixture.sessionId));

		fixture.setNoEnvelopeWhileBusy();
		fixture.holdNextTurn();
		await new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }).sendPrompt(
			fixture.sessionId,
			"external busy adoption",
			"pending-closure-busy",
		);
		await run([executable, "bootstrap", "--confirm", "--state-dir", state, "--profile", profilePath], repositoryRoot, environment);
		daemon = spawnCompiledDaemon([executable, "serve", "--state-dir", state, "--profile", profilePath], environment);
		client = await connectAvailable(socketPath);
		expect(await waitForPendingTranscriptVerification(client)).toMatchObject({
			status: "booting",
			state: "verifying",
			transcript_proof: "pending",
			transcript_verification: "pending",
		});

		fs.writeFileSync(path.join(corpus, "blocked-close.txt"), "must remain uncommitted\n");
		const localHead = await run(["git", "-C", corpus, "rev-parse", "HEAD"]);
		const remoteHead = await run(["git", `--git-dir=${remote}`, "rev-parse", "main"]);
		const brokerCommands = fixture.commands();
		const blocked = await client.request("main.corpus.close", {
			paths: ["blocked-close.txt"],
			commit_message: "must never commit while proof is pending",
			idempotency_key: "pending-proof-close",
		});
		expect(blocked.error).toMatchObject({ code: 1003, message: "transcript_proof_pending" });
		expect(await run(["git", "-C", corpus, "rev-parse", "HEAD"])).toBe(localHead);
		expect(await run(["git", `--git-dir=${remote}`, "rev-parse", "main"])).toBe(remoteHead);
		expect(await run(["git", "-C", corpus, "status", "--porcelain"])).toBe("?? blocked-close.txt\n");
		expect(fixture.commands()).toEqual(brokerCommands);
	} finally {
		client?.close();
		if (daemon) await managedProcesses.stopDaemon(daemon);
		fixture.dispose();
		fs.rmSync(root, { force: true, recursive: true });
	}
}, 30_000);

test("compiled restart defers a durable closure intent until per-boot transcript verification promotes", async () => {
	const executable = compiledWay();
	const fixture = new FakeBrokerFixture();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "g-dc-"));
	const corpus = path.join(root, "corpus");
	const remote = path.join(root, "remote.git");
	const state = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	const socketPath = path.join(state, "rpc.sock");
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	};
	const closeParams = {
		paths: ["deferred-close.txt"],
		commit_message: "defer closure until transcript verification",
		idempotency_key: "deferred-closure-verification",
	};
	const busyOpRef = "deferred-closure-busy";
	let daemon: ManagedBunProcess | undefined;
	let client: RpcClient | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "Compiled Deferred Closure Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "compiled-deferred-closure@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		fs.writeFileSync(path.join(corpus, "deferred-close.txt"), "must not commit before verification\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);
		fs.writeFileSync(profilePath, profile(corpus, fixture.workspace, fixture.sessionId));
		await run([executable, "bootstrap", "--confirm", "--state-dir", state, "--profile", profilePath], repositoryRoot, environment);

		const core = loadWayCore().WayCore.open(state);
		const gatewayState = new GatewayStateStore(core);
		const identity = gatewayState.read().mainIdentity;
		if (!identity) throw new Error("bootstrap did not persist the adopted identity");
		gatewayState.writeGrowthIntent(identity, Date.now());
		const requestJson = canonicalJson({
			commit_message: closeParams.commit_message,
			idempotency_key: closeParams.idempotency_key,
			paths: closeParams.paths,
		});
		const intent = {
			version: 1,
			operationId: "deferred-verification-closure",
			idempotencyKey: closeParams.idempotency_key,
			requestHash: createHash("sha256").update(requestJson).digest("hex"),
			requestJson,
			corpusPath: fs.realpathSync.native(corpus),
			sessionId: fixture.sessionId,
			paths: closeParams.paths,
			commitMessage: closeParams.commit_message,
		};
		const intentJson = canonicalJson(intent);
		const operationJson = canonicalJson({ version: 1, intentJson, state: "intent", evidence: {} });
		expect(
			core.closureOperationClaim({
				scope: "main.corpus.close",
				key: closeParams.idempotency_key,
				requestJson,
				intentJson,
				operationJson,
			}),
		).toMatchObject({ claimed: true });

		fixture.setNoEnvelopeWhileBusy();
		fixture.holdNextTurn();
		await new BrokerCli({ executable: fixture.executable, environment: fixture.environment() }).sendPrompt(
			fixture.sessionId,
			"hold verification while the closure intent waits",
			busyOpRef,
		);
		const localHead = await run(["git", "-C", corpus, "rev-parse", "HEAD"]);
		const remoteHead = await run(["git", `--git-dir=${remote}`, "rev-parse", "main"]);
		daemon = spawnCompiledDaemon([executable, "serve", "--state-dir", state, "--profile", profilePath], environment);
		client = await connectAvailable(socketPath);
		expect(await waitForPendingTranscriptVerification(client, "proven")).toMatchObject({
			status: "booting",
			state: "verifying",
			transcript_proof: "proven",
			transcript_verification: "pending",
		});
		const fenced = await client.request("main.corpus.close", closeParams);
		expect(fenced.error).toMatchObject({ code: 1003, message: "transcript_verification_pending" });
		await Bun.sleep(250);
		expect(await run(["git", "-C", corpus, "rev-parse", "HEAD"])).toBe(localHead);
		expect(await run(["git", `--git-dir=${remote}`, "rev-parse", "main"])).toBe(remoteHead);
		expect(core.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value).toBe(operationJson);

		fixture.complete(busyOpRef, { text: "verification tail is now complete" });
		await eventually(
			async () => {
				const health = (await client!.request("way.health", {}, { timeoutMs: DAEMON_RPC_TIMEOUT_MS })).result as Record<string, unknown> | undefined;
				if (health?.state === "failed_closed") throw new Error(`compiled daemon failed closed during deferred closure recovery: ${JSON.stringify(health)}`);
				return health?.status === "healthy" && health.state === "running" ? health : undefined;
			},
			"compiled daemon did not promote after verification",
		);
		await eventually(
			async () => {
				const content = await run(["git", `--git-dir=${remote}`, "show", "main:deferred-close.txt"]);
				return content === "must not commit before verification\n" ? content : undefined;
			},
			"deferred closure did not execute after transcript verification promotion",
		);
		const replay = await client.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		expect(replay.result).toMatchObject({ committed: true });
		expect(await run(["git", `--git-dir=${remote}`, "rev-list", "--count", "main"])).toBe("2\n");
		expect(await run(["git", "-C", corpus, "rev-list", "--count", "HEAD"])).toBe("2\n");
	} finally {
		client?.close();
		if (daemon) await managedProcesses.stopDaemon(daemon);
		fixture.dispose();
		fs.rmSync(root, { force: true, recursive: true });
	}
}, 45_000);

test("compiled daemon failed-closed startup never disposes or rebirths the adopted external session", async () => {
	const executable = compiledWay();
	const fixture = new FakeBrokerFixture();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-compiled-failclosed-"));
	const corpus = path.join(root, "corpus");
	const state = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	};
	let daemon: ManagedBunProcess | undefined;
	let client: RpcClient | undefined;
	try {
		fs.mkdirSync(corpus, { recursive: true });
		fs.writeFileSync(profilePath, profile(corpus, fixture.workspace, fixture.sessionId));
		await run([executable, "bootstrap", "--confirm", "--state-dir", state, "--profile", profilePath], repositoryRoot, environment);
		daemon = spawnCompiledDaemon(
			[executable, "serve", "--state-dir", state, "--profile", profilePath, "--fail-closed-linger-ms", "300"],
			{ ...environment, GAJAEWAY_E2E_FAIL_BEFORE_MAIN_HOST: "1" },
		);
		client = await connectAvailable(path.join(state, "rpc.sock"));
		expect(await waitForFailedClosed(client)).toMatchObject({ status: "unhealthy", state: "failed_closed", reason: "startup_failed" });
		client.close();
		client = undefined;
		expect(await daemon.exited).toBe(78);
		expect(fixture.read().sessions[fixture.sessionId]?.row.live).toBe(true);
		expect(fixture.commands()).toEqual([]);
	} finally {
		client?.close();
		if (daemon) await managedProcesses.stopDaemon(daemon);
		fixture.dispose();
		fs.rmSync(root, { force: true, recursive: true });
	}
}, 15_000);
