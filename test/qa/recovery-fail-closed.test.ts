import { createHash } from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { canonicalJson } from "../../src/main-session/gates";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";
import { RpcClient } from "../../src/rpc-client";
import { ManagedProcessRegistry } from "../helpers/managed-process";
import { FakeBrokerFixture } from "../helpers/main-session";

const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
});

function profileContents(corpus: string, workspace: string, sessionId: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[main_session]
session_id = "${sessionId}"

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
`;
}

async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// Socket startup races are expected.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

async function waitForHealth(client: RpcClient, expected: "running" | "degraded" | "failed_closed"): Promise<Record<string, unknown>> {
	for (let attempt = 0; attempt < 500; attempt += 1) {
		const response = await client.request("way.health", {});
		if ((response.result as { state?: unknown } | undefined)?.state === expected) return response.result as Record<string, unknown>;
		await Bun.sleep(10);
	}
	throw new Error(`Daemon did not reach ${expected}.`);
}

async function waitForFile(filePath: string, description: string): Promise<void> {
	for (let attempt = 0; attempt < 500; attempt += 1) {
		if (fs.existsSync(filePath)) return;
		await Bun.sleep(10);
	}
	throw new Error(`${description}: ${filePath}`);
}

interface RunningDaemon {
	readonly child: ReturnType<typeof Bun.spawn>;
	readonly client: RpcClient;
}

async function startDaemon(stateDirectory: string, profilePath: string, environment: NodeJS.ProcessEnv, lingerMs?: number): Promise<RunningDaemon> {
	const child = managedProcesses.spawnDaemon({
		cmd: ["bun", "src/main.ts", "serve", "--state-dir", stateDirectory, "--profile", profilePath, ...(lingerMs ? ["--fail-closed-linger-ms", String(lingerMs)] : [])],
		cwd: process.cwd(),
		env: environment,
		stderr: "pipe",
	});
	try {
		return { child, client: await connectEventually(path.join(stateDirectory, "rpc.sock")) };
	} catch (error) {
		await managedProcesses.stopDaemon(child);
		throw error;
	}
}

async function stopDaemon(daemon: RunningDaemon | undefined): Promise<void> {
	if (!daemon) return;
	daemon.client.close();
	await managedProcesses.stopDaemon(daemon.child);
}

async function readDaemonStderr(daemon: RunningDaemon): Promise<string> {
	const stderr = daemon.child.stderr;
	if (typeof stderr === "number") return "";
	return await new Response(stderr).text();
}

async function run(command: readonly string[], cwd = process.cwd()): Promise<string> {
	const child = Bun.spawn({ cmd: [...command], cwd, stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
	if (exitCode !== 0) throw new Error(`${command.join(" ")} failed (${exitCode}): ${stderr || stdout}`);
	return stdout;
}

function bootstrap(stateDirectory: string, profilePath: string, environment: NodeJS.ProcessEnv): void {
	const result = Bun.spawnSync({
		cmd: ["bun", "src/main.ts", "bootstrap", "--confirm", "--state-dir", stateDirectory, "--profile", profilePath],
		cwd: process.cwd(),
		env: environment,
		stdout: "pipe",
		stderr: "pipe",
	});
	if (result.exitCode !== 0) throw new Error(`bootstrap failed (${result.exitCode}): ${new TextDecoder().decode(result.stderr)}`);
}

function stagePendingMainAdmission(core: WayCoreHandle, key: string, mode: "valid" | "invalid" = "valid"): { readonly opRef: string } {
	const requestJson = JSON.stringify({ idempotency_key: key, surface_id: "owner", text: `recover ${key}` });
	const opRef = `recovery-${key}`;
	const intentJson =
		mode === "invalid"
			? "not-json"
			: JSON.stringify({
					version: 1,
					state: "claimed",
					op_ref: opRef,
					delivered_as: "prompt",
					request_hash: createHash("sha256").update(requestJson).digest("hex"),
				});
	expect(
		core.mainAdmissionOperationClaim({
			scope: "main.submit",
			key,
			requestJson,
			intentJson,
		}),
	).toMatchObject({ claimed: true });
	return { opRef };
}

function stagePendingClosureOperation(
	core: WayCoreHandle,
	corpusPath: string,
	sessionId: string,
	params: { readonly paths: readonly string[]; readonly commit_message: string; readonly idempotency_key: string },
): { readonly intentJson: string; readonly operationJson: string } {
	const requestJson = canonicalJson({
		commit_message: params.commit_message,
		idempotency_key: params.idempotency_key,
		paths: params.paths,
	});
	const intentJson = canonicalJson({
		version: 1,
		operationId: "startup-recovery-fence",
		idempotencyKey: params.idempotency_key,
		requestHash: createHash("sha256").update(requestJson).digest("hex"),
		requestJson,
		corpusPath: fs.realpathSync.native(corpusPath),
		sessionId,
		paths: params.paths,
		commitMessage: params.commit_message,
	});
	const operationJson = canonicalJson({ version: 1, intentJson, state: "intent", evidence: {} });
	expect(
		core.closureOperationClaim({
			scope: "main.corpus.close",
			key: params.idempotency_key,
			requestJson,
			intentJson,
			operationJson,
		}),
	).toMatchObject({ claimed: true });
	return { intentJson, operationJson };
}
test("missing exact adopted broker identity fails closed over UDS, lingers unhealthy, exits 78, and never rebirths", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	fs.mkdirSync(corpus, { recursive: true });
	fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	};
	let healthy: RunningDaemon | undefined;
	let failed: RunningDaemon | undefined;
	try {
		const bootstrap = Bun.spawnSync({
			cmd: ["bun", "src/main.ts", "bootstrap", "--confirm", "--state-dir", stateDirectory, "--profile", profilePath],
			cwd: process.cwd(),
			env: environment,
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(bootstrap.exitCode, new TextDecoder().decode(bootstrap.stderr)).toBe(0);
		expect(fixture.commands()).toEqual([]);
		healthy = await startDaemon(stateDirectory, profilePath, environment);
		expect(await waitForHealth(healthy.client, "running")).toMatchObject({ status: "healthy", state: "running", main: { resumed: true } });
		await stopDaemon(healthy);
		healthy = undefined;

		fixture.setLive(false);
		failed = await startDaemon(stateDirectory, profilePath, environment, 750);
		expect(await waitForHealth(failed.client, "failed_closed")).toMatchObject({
			status: "unhealthy", state: "failed_closed", reason: "session_unavailable", main: { resumed: false, session_id: null },
		});
		failed.client.close();
		expect(await failed.child.exited).toBe(78);
	} finally {
		await stopDaemon(failed);
		await stopDaemon(healthy);
		fixture.dispose();
	}
}, 15_000);

test("a daemon killed after main broker acceptance recovers the pre-effect claim without re-sending", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	fs.mkdirSync(corpus, { recursive: true });
	fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
		GAJAEWAY_E2E_FAIL_AFTER_MAIN_ADMISSION_BROKER_ACCEPTED: "1",
	};
	let killed: RunningDaemon | undefined;
	let recovered: RunningDaemon | undefined;
	try {
		const bootstrap = Bun.spawnSync({
			cmd: ["bun", "src/main.ts", "bootstrap", "--confirm", "--state-dir", stateDirectory, "--profile", profilePath],
			cwd: process.cwd(),
			env: environment,
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(bootstrap.exitCode, new TextDecoder().decode(bootstrap.stderr)).toBe(0);
		killed = await startDaemon(stateDirectory, profilePath, environment);
		expect(await waitForHealth(killed.client, "running")).toMatchObject({ state: "running" });
		fixture.holdNextTurn();
		const request = { text: "claim survives daemon death", surface_id: "owner", idempotency_key: "daemon-death-claim" };
		try {
			await killed.client.request("main.submit", request, { timeoutMs: 1_000 });
		} catch {
			// The test hook exits the daemon immediately after broker acceptance.
		}
		killed.client.close();
		expect(await killed.child.exited).toBe(137);
		killed = undefined;
		// Under full-suite load the fixture's command-log write can lag the kill
		// hook by a beat; wait for the observed send instead of racing it.
		{
			const deadline = Date.now() + 10_000;
			let sends = fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text);
			while (sends.length === 0 && Date.now() < deadline) {
				await Bun.sleep(50);
				sends = fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text);
			}
			expect(sends).toHaveLength(1);
		}

		recovered = await startDaemon(stateDirectory, profilePath, {
			...environment,
			GAJAEWAY_E2E_FAIL_AFTER_MAIN_ADMISSION_BROKER_ACCEPTED: "0",
		});
		expect(await waitForHealth(recovered.client, "running")).toMatchObject({ state: "running", main: { resumed: true } });
		const replay = await recovered.client.request("main.submit", request);
		expect(replay.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		expect(fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text)).toHaveLength(1);
	} finally {
		await stopDaemon(recovered);
		await stopDaemon(killed);
		fixture.dispose();
	}
}, 20_000);

test("a transcript projection failure degrades the host and fences corpus closure before any claim, commit, or push", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const remote = path.join(fixture.root, "remote.git");
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
		GAJAEWAY_E2E_FAIL_TRANSCRIPT_PROJECTION: "1",
	};
	let daemon: RunningDaemon | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "Recovery Fence Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "recovery-fence@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);
		fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
		bootstrap(stateDirectory, profilePath, environment);
		const observer = loadWayCore().WayCore.open(stateDirectory);
		const state = new GatewayStateStore(observer);
		const deliveryBefore = state.read().transcriptDeliveryProgress;
		daemon = await startDaemon(stateDirectory, profilePath, environment);
		expect(await waitForHealth(daemon.client, "running")).toMatchObject({ state: "running" });

		fixture.holdNextTurn();
		const admission = await daemon.client.request("main.submit", {
			text: "force an atomic transcript projection failure",
			surface_id: "owner",
			idempotency_key: "projection-failure-fence",
		});
		expect(admission.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		const opRef = (admission.result as { op_ref: string }).op_ref;
		fixture.complete(opRef, { text: "this reply must not advance delivery" });
		expect(await waitForHealth(daemon.client, "degraded")).toMatchObject({
			status: "unhealthy",
			state: "degraded",
			reason: "transcript_delivery_progress_write_failed",
		});
		const deliveryAfterFailure = state.read().transcriptDeliveryProgress;
		expect(deliveryAfterFailure?.fingerprint.entryCount).toBe((deliveryBefore?.fingerprint.entryCount ?? 0) + 1);
		expect(deliveryAfterFailure?.lastEntryId).toBe(`${fixture.sessionId}:transcript:2`);
		expect(deliveryAfterFailure?.lastEntryId).not.toBe(`${fixture.sessionId}:transcript:3`);
		expect(fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === "force an atomic transcript projection failure")).toHaveLength(1);
		expect(fixture.admissionAttempts().filter(attempt => attempt.text === "force an atomic transcript projection failure")).toHaveLength(1);

		fs.writeFileSync(path.join(corpus, "must-not-close.txt"), "must remain uncommitted\n");
		const localHead = await run(["git", "-C", corpus, "rev-parse", "HEAD"]);
		const remoteHead = await run(["git", `--git-dir=${remote}`, "rev-parse", "main"]);
		const refused = await daemon.client.request("main.corpus.close", {
			paths: ["must-not-close.txt"],
			commit_message: "must never execute after host degradation",
			idempotency_key: "degraded-host-close",
		});
		expect(refused.error).toMatchObject({ code: 1003, message: "transcript_delivery_progress_write_failed" });
		expect(observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value).toBeUndefined();
		expect(await run(["git", "-C", corpus, "rev-parse", "HEAD"])).toBe(localHead);
		expect(await run(["git", `--git-dir=${remote}`, "rev-parse", "main"])).toBe(remoteHead);
		expect(await run(["git", "-C", corpus, "status", "--porcelain"])).toBe("?? must-not-close.txt\n");
	} finally {
		await stopDaemon(daemon);
		fixture.dispose();
	}
}, 30_000);

test("startup closure recovery fences after lease acquisition before Git dispatch and later finalizes exactly once", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const remote = path.join(fixture.root, "remote.git");
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	const acquireMarker = path.join(fixture.root, "closure-after-acquire.marker");
	const acquireRelease = path.join(fixture.root, "closure-after-acquire.release");
	const closeParams = {
		paths: ["recovered-close.txt"],
		commit_message: "recover only after a healthy boot",
		idempotency_key: "startup-recovery-fence",
	};
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
		GAJAEWAY_E2E_FAIL_TRANSCRIPT_PROJECTION: "1",
		GAJAEWAY_E2E_CLOSURE_AFTER_ACQUIRE_MARKER: acquireMarker,
		GAJAEWAY_E2E_CLOSURE_AFTER_ACQUIRE_RELEASE: acquireRelease,
	};
	const healthyEnvironment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
		GAJAEWAY_E2E_FAIL_TRANSCRIPT_PROJECTION: "0",
	};
	let degradedDaemon: RunningDaemon | undefined;
	let healthyDaemon: RunningDaemon | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "Startup Recovery Fence Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "startup-recovery-fence@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		fs.writeFileSync(path.join(corpus, "recovered-close.txt"), "must remain pending until a healthy boot\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);
		fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
		bootstrap(stateDirectory, profilePath, environment);

		const observer = loadWayCore().WayCore.open(stateDirectory);
		const { intentJson } = stagePendingClosureOperation(observer, corpus, fixture.sessionId, closeParams);
		const localHead = await run(["git", "-C", corpus, "rev-parse", "HEAD"]);
		const remoteHead = await run(["git", `--git-dir=${remote}`, "rev-parse", "main"]);
		degradedDaemon = await startDaemon(stateDirectory, profilePath, environment);
		expect(await waitForHealth(degradedDaemon.client, "running")).toMatchObject({ state: "running" });
		await waitForFile(acquireMarker, "startup recovery did not acquire its closure lease");
		expect(JSON.parse(fs.readFileSync(acquireMarker, "utf8"))).toEqual({ operation_id: "startup-recovery-fence", step: "after_acquire" });
		const acquiredRaw = observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value;
		expect(JSON.parse(acquiredRaw ?? "{}")).toMatchObject({ intentJson, state: "acquired" });

		fixture.holdNextTurn();
		const admission = await degradedDaemon.client.request("main.submit", {
			text: "degrade the host while startup recovery holds its acquired closure",
			surface_id: "owner",
			idempotency_key: "startup-recovery-acquire-fence",
		});
		expect(admission.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		fixture.complete((admission.result as { op_ref: string }).op_ref, { text: "projection failure fences the acquired closure" });
		expect(await waitForHealth(degradedDaemon.client, "degraded")).toMatchObject({
			status: "unhealthy",
			state: "degraded",
			reason: "transcript_delivery_progress_write_failed",
		});
		fs.writeFileSync(acquireRelease, "release\n", { encoding: "utf8", mode: 0o600 });
		await Bun.sleep(250);
		const refused = await degradedDaemon.client.request("main.corpus.close", closeParams);
		expect(refused.error).toMatchObject({ code: 1003, message: "transcript_delivery_progress_write_failed" });
		const retainedRaw = observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value;
		expect(JSON.parse(retainedRaw ?? "{}")).toMatchObject({ intentJson, state: "acquired" });
		expect(await run(["git", "-C", corpus, "rev-parse", "HEAD"])).toBe(localHead);
		expect(await run(["git", `--git-dir=${remote}`, "rev-parse", "main"])).toBe(remoteHead);
		expect(await run(["git", `--git-dir=${remote}`, "rev-list", "--count", "main"])).toBe("1\n");

		const stoppedDaemon = degradedDaemon;
		await stopDaemon(stoppedDaemon);
		const stderr = await readDaemonStderr(stoppedDaemon);
		expect(stderr).toContain("Pending corpus closure recovery refused: transcript_delivery_progress_write_failed");
		degradedDaemon = undefined;
		healthyDaemon = await startDaemon(stateDirectory, profilePath, healthyEnvironment);
		expect(await waitForHealth(healthyDaemon.client, "running")).toMatchObject({ state: "running" });
		const finalized = await healthyDaemon.client.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		expect(finalized.result).toMatchObject({ committed: true });
		const replay = await healthyDaemon.client.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		expect(replay.result).toEqual(finalized.result);
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:recovered-close.txt"])).toBe(
			"must remain pending until a healthy boot\n",
		);
		expect(await run(["git", `--git-dir=${remote}`, "rev-list", "--count", "main"])).toBe("2\n");
		expect(await run(["git", "-C", corpus, "rev-list", "--count", "HEAD"])).toBe("2\n");
		expect(observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value).toBeUndefined();
	} finally {
		if (!fs.existsSync(acquireRelease)) fs.writeFileSync(acquireRelease, "release\n", { encoding: "utf8", mode: 0o600 });
		await stopDaemon(healthyDaemon);
		await stopDaemon(degradedDaemon);
		fixture.dispose();
	}
}, 45_000);

test("startup recovery retains a committed-not-pushed closure for exactly-once healthy push finalization", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const remote = path.join(fixture.root, "remote.git");
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	const commitMarker = path.join(fixture.root, "closure-after-commit.marker");
	const commitRelease = path.join(fixture.root, "closure-after-commit.release");
	const closeParams = {
		paths: ["committed-close.txt"],
		commit_message: "push only after a healthy boot",
		idempotency_key: "startup-recovery-commit-fence",
	};
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
		GAJAEWAY_E2E_FAIL_TRANSCRIPT_PROJECTION: "1",
		GAJAEWAY_E2E_CLOSURE_AFTER_COMMIT_MARKER: commitMarker,
		GAJAEWAY_E2E_CLOSURE_AFTER_COMMIT_RELEASE: commitRelease,
	};
	const healthyEnvironment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
		GAJAEWAY_E2E_FAIL_TRANSCRIPT_PROJECTION: "0",
	};
	let degradedDaemon: RunningDaemon | undefined;
	let healthyDaemon: RunningDaemon | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "Committed Recovery Fence Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "committed-recovery-fence@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		fs.writeFileSync(path.join(corpus, "committed-close.txt"), "commit before the host degrades\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);
		fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
		bootstrap(stateDirectory, profilePath, environment);

		const observer = loadWayCore().WayCore.open(stateDirectory);
		const { intentJson } = stagePendingClosureOperation(observer, corpus, fixture.sessionId, closeParams);
		const localHead = (await run(["git", "-C", corpus, "rev-parse", "HEAD"])).trim();
		const remoteHead = (await run(["git", `--git-dir=${remote}`, "rev-parse", "main"])).trim();
		degradedDaemon = await startDaemon(stateDirectory, profilePath, environment);
		expect(await waitForHealth(degradedDaemon.client, "running")).toMatchObject({ state: "running" });
		await waitForFile(commitMarker, "startup recovery did not persist its committed closure evidence");
		expect(JSON.parse(fs.readFileSync(commitMarker, "utf8"))).toEqual({ operation_id: "startup-recovery-fence", step: "after_commit" });
		const committedHead = (await run(["git", "-C", corpus, "rev-parse", "HEAD"])).trim();
		expect(committedHead).not.toBe(localHead);
		const committedRaw = observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value;
		expect(JSON.parse(committedRaw ?? "{}")).toMatchObject({
			intentJson,
			state: "committed",
			evidence: { commitHead: committedHead, committed: true },
		});

		fixture.holdNextTurn();
		const admission = await degradedDaemon.client.request("main.submit", {
			text: "degrade the host after startup recovery commits but before it pushes",
			surface_id: "owner",
			idempotency_key: "startup-recovery-commit-fence",
		});
		expect(admission.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		fixture.complete((admission.result as { op_ref: string }).op_ref, { text: "projection failure fences the pending push" });
		expect(await waitForHealth(degradedDaemon.client, "degraded")).toMatchObject({
			status: "unhealthy",
			state: "degraded",
			reason: "transcript_delivery_progress_write_failed",
		});
		fs.writeFileSync(commitRelease, "release\n", { encoding: "utf8", mode: 0o600 });
		await Bun.sleep(250);
		const refused = await degradedDaemon.client.request("main.corpus.close", closeParams);
		expect(refused.error).toMatchObject({ code: 1003, message: "transcript_delivery_progress_write_failed" });
		const retainedRaw = observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value;
		expect(JSON.parse(retainedRaw ?? "{}")).toMatchObject({
			intentJson,
			state: "committed",
			evidence: { commitHead: committedHead, committed: true },
		});
		expect((await run(["git", "-C", corpus, "rev-parse", "HEAD"])).trim()).toBe(committedHead);
		expect((await run(["git", `--git-dir=${remote}`, "rev-parse", "main"])).trim()).toBe(remoteHead);
		expect(await run(["git", "-C", corpus, "rev-list", "--count", "HEAD"])).toBe("2\n");
		expect(await run(["git", `--git-dir=${remote}`, "rev-list", "--count", "main"])).toBe("1\n");

		const stoppedDaemon = degradedDaemon;
		await stopDaemon(stoppedDaemon);
		const stderr = await readDaemonStderr(stoppedDaemon);
		expect(stderr).toContain("Pending corpus closure recovery refused: transcript_delivery_progress_write_failed");
		degradedDaemon = undefined;
		healthyDaemon = await startDaemon(stateDirectory, profilePath, healthyEnvironment);
		expect(await waitForHealth(healthyDaemon.client, "running")).toMatchObject({ state: "running" });
		const finalized = await healthyDaemon.client.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		expect(finalized.result).toMatchObject({ committed: true });
		const replay = await healthyDaemon.client.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		expect(replay.result).toEqual(finalized.result);
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:committed-close.txt"])).toBe("commit before the host degrades\n");
		expect(await run(["git", `--git-dir=${remote}`, "rev-list", "--count", "main"])).toBe("2\n");
		expect(await run(["git", "-C", corpus, "rev-list", "--count", "HEAD"])).toBe("2\n");
		expect(observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value).toBeUndefined();
	} finally {
		if (!fs.existsSync(commitRelease)) fs.writeFileSync(commitRelease, "release\n", { encoding: "utf8", mode: 0o600 });
		await stopDaemon(healthyDaemon);
		await stopDaemon(degradedDaemon);
		fixture.dispose();
	}
}, 45_000);

async function assertPendingAdmissionRecoveryFailsClosed(
	reason: "main_admission_recovery_unavailable" | "main_admission_recovery_unprovable" | "main_admission_intent_invalid",
	mode: "valid" | "invalid",
	configureFixture: (fixture: FakeBrokerFixture, opRef: string) => void = () => undefined,
): Promise<void> {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	};
	let daemon: RunningDaemon | undefined;
	try {
		fs.mkdirSync(corpus, { recursive: true });
		fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
		bootstrap(stateDirectory, profilePath, environment);
		const core = loadWayCore().WayCore.open(stateDirectory);
		const state = new GatewayStateStore(core);
		const deliveryBefore = state.read().transcriptDeliveryProgress;
		const { opRef } = stagePendingMainAdmission(core, `pending-${reason}`, mode);
		configureFixture(fixture, opRef);
		daemon = await startDaemon(stateDirectory, profilePath, environment, 2_000);
		expect(await waitForHealth(daemon.client, "failed_closed")).toMatchObject({
			status: "unhealthy",
			state: "failed_closed",
			reason,
		});
		expect(state.read()).toMatchObject({ bootstrapState: "FAILED_CLOSED", failedClosedReason: reason });
		expect(state.read().transcriptDeliveryProgress).toEqual(deliveryBefore);
		expect(fixture.commands()).toEqual([]);
		expect(fixture.admissionAttempts()).toEqual([]);
	} finally {
		await stopDaemon(daemon);
		fixture.dispose();
	}
}

test("pending main admission status transport failure fails closed without a broker resend", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_recovery_unavailable", "valid", fixture => fixture.setOperationStatusUnavailable());
}, 15_000);

test("pending main admission with an unprovable broker status fails closed without a broker resend", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_recovery_unprovable", "valid");
}, 15_000);

test("invalid pending main admission intent fails closed before any broker resend", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_intent_invalid", "invalid");
}, 15_000);
