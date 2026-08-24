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
// Real daemon admission RPCs cross a spawned broker process; 10s covers the request under cold-start CPU contention.
const DAEMON_ADMISSION_RPC_TIMEOUT_MS = 10_000;

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
session_kind = "main"
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

interface StagedMainAdmission {
	readonly key: string;
	readonly opRef: string;
	readonly requestJson: string;
	readonly intentJson: string;
}

function stagePendingMainAdmission(core: WayCoreHandle, key: string, mode: "valid" | "invalid" = "valid"): StagedMainAdmission {
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
	return { key, opRef, requestJson, intentJson };
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

function executionLeaseFromOperation(raw: string | undefined): { readonly leaseId: string; readonly fencingToken: string } {
	const parsed = JSON.parse(raw ?? "{}") as { evidence?: { leaseId?: unknown; fencingToken?: unknown } };
	if (typeof parsed.evidence?.leaseId !== "string" || typeof parsed.evidence.fencingToken !== "string") {
		throw new Error("closure operation does not retain an execution lease");
	}
	return { leaseId: parsed.evidence.leaseId, fencingToken: parsed.evidence.fencingToken };
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
			await killed.client.request("main.submit", request, { timeoutMs: DAEMON_ADMISSION_RPC_TIMEOUT_MS });
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

test("a post-acceptance bridge failure with durable terminal evidence reconciles an unknown broker outcome without resend", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
		GAJAEWAY_E2E_THROW_AFTER_MAIN_ADMISSION_BROKER_ACCEPTED: "1",
		GAJAEWAY_E2E_FAIL_AFTER_MAIN_ADMISSION_TERMINAL_EVIDENCE: "1",
	};
	let crashed: RunningDaemon | undefined;
	let recovered: RunningDaemon | undefined;
	try {
		fs.mkdirSync(corpus, { recursive: true });
		fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
		bootstrap(stateDirectory, profilePath, environment);
		const core = loadWayCore().WayCore.open(stateDirectory);
		crashed = await startDaemon(stateDirectory, profilePath, environment);
		expect(await waitForHealth(crashed.client, "running")).toMatchObject({ state: "running" });
		fixture.useBrokerTurnIdForTail();
		fixture.holdNextTurn();
		const request = {
			text: "recover a reply whose bridge response failed",
			surface_id: "owner",
			idempotency_key: "post-acceptance-terminal-recovery",
		};
		const failedResponse = await crashed.client.request("main.submit", request);
		expect(failedResponse.error).toMatchObject({ code: -32603, message: "bridge_exception" });
		const [pending] = core.mainAdmissionOperationsPending();
		const intent = JSON.parse(pending?.intentJson ?? "{}") as { op_ref?: unknown; journal_head_cursor?: unknown };
		if (typeof intent.op_ref !== "string" || typeof intent.journal_head_cursor !== "string") {
			throw new Error("post-acceptance bridge failure did not retain a recoverable durable intent");
		}
		const opRef = intent.op_ref;
		fixture.complete(opRef, { text: "reply persisted before terminal-finalizer interruption" });
		crashed.client.close();
		expect(await crashed.child.exited).toBe(137);
		crashed = undefined;

		const evidence = core.journalRead(intent.journal_head_cursor, 100).events;
		expect(evidence).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					kind: "assistant_message",
					payloadJson: expect.stringContaining("reply persisted before terminal-finalizer interruption"),
				}),
				expect.objectContaining({
					kind: "turn_end",
					payloadJson: expect.stringContaining(`broker-turn-${opRef}`),
				}),
			]),
		);
		const [surviving] = core.mainAdmissionOperationsPending();
		expect(JSON.parse(surviving?.attemptIdsJson ?? "null")).toEqual(expect.arrayContaining([`broker-turn-${opRef}`]));
		fixture.forgetOperation(opRef);

		recovered = await startDaemon(stateDirectory, profilePath, {
			...environment,
			GAJAEWAY_E2E_THROW_AFTER_MAIN_ADMISSION_BROKER_ACCEPTED: "0",
			GAJAEWAY_E2E_FAIL_AFTER_MAIN_ADMISSION_TERMINAL_EVIDENCE: "0",
		});
		expect(await waitForHealth(recovered.client, "running")).toMatchObject({ state: "running", main: { resumed: true } });
		expect(core.mainAdmissionOperationsPending()).toEqual([]);
		const replay = await recovered.client.request("main.submit", request);
		expect(replay.result).toMatchObject({
			accepted: true,
			op_ref: opRef,
			delivered_as: "prompt",
			journal_head_cursor: intent.journal_head_cursor,
		});
		const conflict = await recovered.client.request("main.submit", {
			...request,
			text: "different content with the recovered idempotency key",
		});
		expect(conflict.error).toMatchObject({ code: 1500, message: "idempotency_conflict" });
		expect(fixture.admissionAttempts().filter(attempt => attempt.opRef === opRef)).toHaveLength(1);
	} finally {
		await stopDaemon(recovered);
		await stopDaemon(crashed);
		fixture.dispose();
	}
}, 30_000);

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
		const refusedAttempt = executionLeaseFromOperation(acquiredRaw);

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
		const finalLeaseId = (finalized.result as { lease_id?: unknown }).lease_id;
		const finalFencingToken = (finalized.result as { fencing_token?: unknown }).fencing_token;
		expect(finalLeaseId).toEqual(expect.any(String));
		expect(finalFencingToken).toEqual(expect.any(String));
		expect(finalLeaseId).not.toBe(refusedAttempt.leaseId);
		expect(finalFencingToken).not.toBe(refusedAttempt.fencingToken);
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
		const refusedAttempt = executionLeaseFromOperation(committedRaw);

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
		const finalLeaseId = (finalized.result as { lease_id?: unknown }).lease_id;
		const finalFencingToken = (finalized.result as { fencing_token?: unknown }).fencing_token;
		expect(finalLeaseId).toEqual(expect.any(String));
		expect(finalFencingToken).toEqual(expect.any(String));
		expect(finalLeaseId).not.toBe(refusedAttempt.leaseId);
		expect(finalFencingToken).not.toBe(refusedAttempt.fencingToken);
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

test("RPC closure fences the original and in-flight duplicate, then preserves unrelated closure errors and recovers", async () => {
	const fixture = new FakeBrokerFixture();
	const corpus = path.join(fixture.root, "corpus");
	const remote = path.join(fixture.root, "remote.git");
	const stateDirectory = path.join(fixture.root, "state");
	const profilePath = path.join(fixture.root, "profile.toml");
	const acquireMarker = path.join(fixture.root, "rpc-closure-after-acquire.marker");
	const acquireRelease = path.join(fixture.root, "rpc-closure-after-acquire.release");
	const waiterMarker = path.join(fixture.root, "rpc-closure-inflight-waiter.marker");
	const closeParams = {
		paths: ["rpc-recovered-close.txt"],
		commit_message: "recover RPC closure after a healthy boot",
		idempotency_key: "rpc-closure-readiness-fence",
	};
	const environment = {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
		GAJAEWAY_E2E_FAIL_TRANSCRIPT_PROJECTION: "1",
		GAJAEWAY_E2E_CLOSURE_AFTER_ACQUIRE_MARKER: acquireMarker,
		GAJAEWAY_E2E_CLOSURE_AFTER_ACQUIRE_RELEASE: acquireRelease,
		GAJAEWAY_E2E_CLOSURE_INFLIGHT_WAITER_MARKER: waiterMarker,
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
	let waiterClient: RpcClient | undefined;
	try {
		await run(["git", "init", "--bare", remote]);
		await run(["git", "init", corpus]);
		await run(["git", "-C", corpus, "config", "user.name", "RPC Closure Fence Drill"]);
		await run(["git", "-C", corpus, "config", "user.email", "rpc-closure-fence@example.test"]);
		fs.writeFileSync(path.join(corpus, "base.txt"), "base\n");
		fs.writeFileSync(path.join(corpus, "rpc-recovered-close.txt"), "must not commit while the RPC close is fenced\n");
		await run(["git", "-C", corpus, "add", "--", "base.txt"]);
		await run(["git", "-C", corpus, "commit", "-m", "base"]);
		await run(["git", "-C", corpus, "branch", "-M", "main"]);
		await run(["git", "-C", corpus, "remote", "add", "origin", remote]);
		await run(["git", "-C", corpus, "push", "-u", "origin", "main"]);
		await run(["git", `--git-dir=${remote}`, "symbolic-ref", "HEAD", "refs/heads/main"]);
		fs.writeFileSync(profilePath, profileContents(corpus, fixture.workspace, fixture.sessionId));
		bootstrap(stateDirectory, profilePath, environment);

		const observer = loadWayCore().WayCore.open(stateDirectory);
		const localHead = await run(["git", "-C", corpus, "rev-parse", "HEAD"]);
		const remoteHead = await run(["git", `--git-dir=${remote}`, "rev-parse", "main"]);
		degradedDaemon = await startDaemon(stateDirectory, profilePath, environment);
		expect(await waitForHealth(degradedDaemon.client, "running")).toMatchObject({ state: "running" });
		const original = degradedDaemon.client.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		await waitForFile(acquireMarker, "RPC closure did not acquire its lease");
		const acquiredRaw = observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value;
		const refusedAttempt = executionLeaseFromOperation(acquiredRaw);
		waiterClient = await connectEventually(path.join(stateDirectory, "rpc.sock"));
		const waiter = waiterClient.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		await waitForFile(waiterMarker, "same-key RPC waiter did not enter the in-flight path");
		expect(JSON.parse(fs.readFileSync(waiterMarker, "utf8"))).toEqual({ idempotency_key: closeParams.idempotency_key });

		fixture.holdNextTurn();
		const admission = await degradedDaemon.client.request("main.submit", {
			text: "degrade the host while RPC closure callers wait on its acquired lease",
			surface_id: "owner",
			idempotency_key: "rpc-closure-readiness-fence-admission",
		});
		expect(admission.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		fixture.complete((admission.result as { op_ref: string }).op_ref, { text: "projection failure fences both RPC closure callers" });
		expect(await waitForHealth(degradedDaemon.client, "degraded")).toMatchObject({
			status: "unhealthy",
			state: "degraded",
			reason: "transcript_delivery_progress_write_failed",
		});
		fs.writeFileSync(acquireRelease, "release\n", { encoding: "utf8", mode: 0o600 });
		const [originalResponse, waiterResponse] = await Promise.all([original, waiter]);
		expect(originalResponse.error).toMatchObject({ code: 1003, message: "transcript_delivery_progress_write_failed" });
		expect(waiterResponse.error).toMatchObject({ code: 1003, message: "transcript_delivery_progress_write_failed" });
		expect(JSON.parse(observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value ?? "{}")).toMatchObject({
			state: "acquired",
			evidence: { leaseId: refusedAttempt.leaseId, fencingToken: refusedAttempt.fencingToken },
		});
		expect(await run(["git", "-C", corpus, "rev-parse", "HEAD"])).toBe(localHead);
		expect(await run(["git", `--git-dir=${remote}`, "rev-parse", "main"])).toBe(remoteHead);

		waiterClient.close();
		waiterClient = undefined;
		await stopDaemon(degradedDaemon);
		degradedDaemon = undefined;
		healthyDaemon = await startDaemon(stateDirectory, profilePath, healthyEnvironment);
		expect(await waitForHealth(healthyDaemon.client, "running")).toMatchObject({ state: "running" });
		const finalized = await healthyDaemon.client.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		expect(finalized.result).toMatchObject({ committed: true });
		const finalLeaseId = (finalized.result as { lease_id?: unknown }).lease_id;
		const finalFencingToken = (finalized.result as { fencing_token?: unknown }).fencing_token;
		expect(finalLeaseId).toEqual(expect.any(String));
		expect(finalFencingToken).toEqual(expect.any(String));
		expect(finalLeaseId).not.toBe(refusedAttempt.leaseId);
		expect(finalFencingToken).not.toBe(refusedAttempt.fencingToken);
		const replay = await healthyDaemon.client.request("main.corpus.close", closeParams, { timeoutMs: 10_000 });
		expect(replay.result).toEqual(finalized.result);
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:rpc-recovered-close.txt"])).toBe(
			"must not commit while the RPC close is fenced\n",
		);
		expect(await run(["git", `--git-dir=${remote}`, "rev-list", "--count", "main"])).toBe("2\n");

		fs.writeFileSync(path.join(corpus, "dirty-index.txt"), "unrelated closure error\n");
		await run(["git", "-C", corpus, "add", "--", "dirty-index.txt"]);
		const dirtyParams = {
			paths: ["never-close.txt"],
			commit_message: "must report dirty index",
			idempotency_key: "rpc-closure-non-readiness-error",
		};
		const firstClosureError = await healthyDaemon.client.request("main.corpus.close", dirtyParams, { timeoutMs: 10_000 });
		expect(firstClosureError.error).toMatchObject({ code: 1206, message: "corpus git index is dirty" });
		const retryClosureError = await healthyDaemon.client.request("main.corpus.close", dirtyParams, { timeoutMs: 10_000 });
		expect(retryClosureError.error).toMatchObject({ code: 1206, message: "corpus git index is dirty" });
		expect(retryClosureError.error?.code).not.toBe(1003);
		expect(JSON.parse(observer.gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value ?? "{}")).toMatchObject({ state: "acquired" });
	} finally {
		if (!fs.existsSync(acquireRelease)) fs.writeFileSync(acquireRelease, "release\n", { encoding: "utf8", mode: 0o600 });
		waiterClient?.close();
		await stopDaemon(healthyDaemon);
		await stopDaemon(degradedDaemon);
		fixture.dispose();
	}
}, 45_000);

async function assertPendingAdmissionRecoveryFailsClosed(
	reason: "main_admission_recovery_unavailable" | "main_admission_recovery_unprovable" | "main_admission_intent_invalid",
	mode: "valid" | "invalid",
	configurePending: (fixture: FakeBrokerFixture, pending: StagedMainAdmission, core: WayCoreHandle) => void = () => undefined,
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
		const pending = stagePendingMainAdmission(core, `pending-${reason}`, mode);
		configurePending(fixture, pending, core);
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
		expect(core.mainAdmissionOperationsPending()).toHaveLength(1);
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

test("a suffix-sharing unrelated terminal attempt cannot settle a pending admission", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_recovery_unprovable", "valid", (_fixture, pending, core) => {
		const opaqueAttemptId = `opaque-turn-${pending.opRef}`;
		core.mainAdmissionOperationRecordAttemptIds({
			scope: "main.submit",
			key: pending.key,
			requestJson: pending.requestJson,
			intentJson: pending.intentJson,
			attemptIdsJson: canonicalJson([opaqueAttemptId]),
		});
		core.journalAppend(
			"turn_end",
			canonicalJson({ attempt_id: `other:${opaqueAttemptId}`, generation: 1, lineage: "main" }),
		);
	});
}, 15_000);

test("an unbound pending admission does not infer a legacy terminal alias", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_recovery_unprovable", "valid", (_fixture, pending, core) => {
		core.journalAppend(
			"turn_end",
			canonicalJson({ attempt_id: pending.opRef, generation: 1, lineage: "main" }),
		);
	});
}, 15_000);

test("a pending admission with object attempt_ids_json fails closed without a broker resend", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_intent_invalid", "valid", (_fixture, pending, core) => {
		core.mainAdmissionOperationRecordAttemptIds({
			scope: "main.submit",
			key: pending.key,
			requestJson: pending.requestJson,
			intentJson: pending.intentJson,
			attemptIdsJson: "{}",
		});
	});
}, 15_000);

test("a pending admission with non-JSON attempt_ids_json fails closed without a broker resend", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_intent_invalid", "valid", (_fixture, pending, core) => {
		core.mainAdmissionOperationRecordAttemptIds({
			scope: "main.submit",
			key: pending.key,
			requestJson: pending.requestJson,
			intentJson: pending.intentJson,
			attemptIdsJson: "not-json",
		});
	});
}, 15_000);

test("a pending admission with empty attempt_ids_json fails closed without a broker resend", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_intent_invalid", "valid", (_fixture, pending, core) => {
		core.mainAdmissionOperationRecordAttemptIds({
			scope: "main.submit",
			key: pending.key,
			requestJson: pending.requestJson,
			intentJson: pending.intentJson,
			attemptIdsJson: "[]",
		});
	});
}, 15_000);

test("invalid pending main admission intent fails closed before any broker resend", async () => {
	await assertPendingAdmissionRecoveryFailsClosed("main_admission_intent_invalid", "invalid");
}, 15_000);
