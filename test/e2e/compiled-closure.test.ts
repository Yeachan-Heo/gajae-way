import * as fs from "node:fs";
import * as os from "node:os";
import { createConnection } from "node:net";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { RpcClient } from "../../src/rpc-client";
import { loadWayCore } from "../../src/native-loader";

const repositoryRoot = path.resolve(import.meta.dir, "..", "..");

async function run(command: readonly string[], cwd = repositoryRoot, env?: NodeJS.ProcessEnv): Promise<string> {
	const child = Bun.spawn({ cmd: [...command], cwd, env, stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout, stderr] = await Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]);
	if (exitCode !== 0) throw new Error(`${command.join(" ")} failed (${exitCode}): ${stderr || stdout}`);
	return stdout;
}

function compiledWay(): string {
	const executable = path.join(repositoryRoot, "dist", "way");
	if (!fs.existsSync(executable))
		throw new Error("The compiled closure drill requires dist/way. Run bun scripts/compile.ts before bun test.");
	return executable;
}

function profile(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

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
			const health = await client.request("way.health", {}, { timeoutMs: 1_000 });
			if ((health.result as { status?: string } | undefined)?.status === "healthy") return client;
			client.close();
		} catch (error) {
			client?.close();
			lastError = error;
		}
		await Bun.sleep(25);
	}
	throw new Error(
		`compiled daemon did not become healthy${lastError instanceof Error ? `: ${lastError.message}` : ""}`,
	);
}

async function connectAvailable(socketPath: string): Promise<RpcClient> {
	let lastError: unknown;
	for (let attempt = 0; attempt < 200; attempt += 1) {
		try {
			return await RpcClient.connect(socketPath);
		} catch (error) {
			lastError = error;
		}
		await Bun.sleep(25);
	}
	throw new Error(
		`compiled daemon did not expose its RPC listener${lastError instanceof Error ? `: ${lastError.message}` : ""}`,
	);
}

async function endpointReachable(socketPath: string): Promise<boolean> {
	return await new Promise<boolean>((resolve) => {
		const socket = createConnection(socketPath);
		let settled = false;
		const finish = (reachable: boolean) => {
			if (settled) return;
			settled = true;
			clearTimeout(timeout);
			socket.destroy();
			resolve(reachable);
		};
		const timeout = setTimeout(() => finish(false), 1_000);
		socket.once("connect", () => finish(true));
		socket.once("error", () => finish(false));
	});
}

async function failedClosedHealth(client: RpcClient): Promise<Record<string, unknown>> {
	return await eventually(async () => {
		const response = await client.request("way.health", {}, { timeoutMs: 1_000 });
		const health = response.result;
		return health && typeof health === "object" && (health as { state?: unknown }).state === "failed_closed"
			? (health as Record<string, unknown>)
			: undefined;
	}, "compiled daemon did not become failed_closed");
}

async function eventually<T>(
	read: () => T | undefined | Promise<T | undefined>,
	description: string,
	timeoutMs = 5_000,
): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	let lastError: unknown;
	while (Date.now() < deadline) {
		try {
			const value = await read();
			if (value !== undefined) return value;
		} catch (error) {
			lastError = error;
		}
		await Bun.sleep(20);
	}
	throw new Error(`${description}${lastError instanceof Error ? `: ${lastError.message}` : ""}`);
}

function closureParams(pathname: string, commitMessage: string, idempotencyKey: string): Record<string, unknown> {
	return {
		paths: [pathname],
		commit_message: commitMessage,
		idempotency_key: idempotencyKey,
	};
}

function pausePush(corpus: string, marker: string, release: string): string {
	const hook = path.join(corpus, ".git", "hooks", "pre-push");
	fs.writeFileSync(
		hook,
		`#!/bin/sh\nprintf reached > "${marker}"\nwhile [ ! -e "${release}" ]; do sleep 0.05; done\n`,
		{ mode: 0o755 },
	);
	return hook;
}

async function requestAndDiscardResponse(
	socketPath: string,
	method: string,
	params: Record<string, unknown>,
): Promise<void> {
	await new Promise<void>((resolve, reject) => {
		const socket = createConnection(socketPath);
		let settled = false;
		const finish = (callback: () => void) => {
			if (settled) return;
			settled = true;
			callback();
		};
		socket.once("error", (error) => finish(() => reject(error)));
		socket.once("close", () => finish(() => reject(new Error("RPC socket closed before its response was discarded"))));
		socket.once("data", () =>
			finish(() => {
				socket.destroy();
				resolve();
			}),
		);
		socket.once("connect", () => {
			socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: "discarded-response", method, params })}\n`, (error) => {
				if (error) finish(() => reject(error));
			});
		});
	});
}

test("compiled daemon makes corpus closures durable, replayable, and single-flight", async () => {
	const executable = compiledWay();
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-compiled-closure-"));
	const corpus = path.join(root, "corpus");
	const remote = path.join(root, "remote.git");
	const workspace = path.join(root, "workspace");
	const state = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	const socketPath = path.join(state, "rpc.sock");
	const endpointPath = path.join(os.tmpdir(), `way-e2-${process.pid}-${Date.now()}.sock`);
	fs.mkdirSync(workspace);
	fs.writeFileSync(profilePath, profile(corpus, workspace));
	const environment = {
		...process.env,
		NODE_ENV: "test",
		WAY_E2E_FILE_SDK: "1",
		WAY_BROKER_CLI: "/usr/bin/false",
		WAY_RECONCILE_POLL_MS: "600000",
	};
	let daemon: ReturnType<typeof Bun.spawn> | undefined;
	let client: RpcClient | undefined;
	const extraClients: RpcClient[] = [];
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
		const unavailableProbe = Bun.spawn({
			cmd: [executable, "--health", "--state-dir", state],
			cwd: repositoryRoot,
			env: environment,
			stdout: "pipe",
			stderr: "pipe",
		});
		const [unavailableExitCode, unavailableOutput] = await Promise.all([
			unavailableProbe.exited,
			new Response(unavailableProbe.stdout).text(),
		]);
		expect(unavailableExitCode).toBe(1);
		expect(JSON.parse(unavailableOutput)).toMatchObject({
			status: "unhealthy",
			state: "unavailable",
			reason: "daemon_unreachable",
		});

		await run(
			[executable, "bootstrap", "--confirm", "--state-dir", state, "--profile", profilePath],
			repositoryRoot,
			environment,
		);
		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: environment,
			stdout: "ignore",
			stderr: "pipe",
		});
		client = await connectHealthy(socketPath);
		const cliHealth = JSON.parse(
			await run([executable, "--health", "--state-dir", state], repositoryRoot, environment),
		);
		expect(cliHealth).toMatchObject({ status: "healthy", state: "running", main: { resumed: true } });
		const missingKey = await client.request("main.corpus.close", {
			paths: ["compiled-close.txt"],
			commit_message: "missing idempotency key",
		});
		expect(missingKey.error).toMatchObject({
			code: -32602,
			message: "main.corpus.close idempotency_key must be a non-empty string.",
		});
		const invalidPath = await client.request(
			"main.corpus.close",
			closureParams("../outside.txt", "invalid closure path", "invalid-path-key"),
		);
		expect(invalidPath.error).toMatchObject({
			code: -32602,
			message: "main.corpus.close paths must remain inside the corpus and cannot name the corpus root.",
		});

		// JSON accepts NUL-bearing strings, but no such request may reserve a
		// durable operation because it cannot be passed to Git through execve.
		const nulPath = await client.request(
			"main.corpus.close",
			closureParams("nul\u0000path.txt", "NUL path closure", "nul-path-key"),
		);
		expect(nulPath.error).toMatchObject({
			code: -32602,
			message: "main.corpus.close paths[] must be an execve-safe string.",
		});
		const nulCommitMessage = await client.request(
			"main.corpus.close",
			closureParams("compiled-close.txt", "NUL\u0000commit message", "nul-commit-key"),
		);
		expect(nulCommitMessage.error).toMatchObject({
			code: -32602,
			message: "main.corpus.close commit_message must be an execve-safe string.",
		});
		const nulIdempotencyKey = await client.request(
			"main.corpus.close",
			closureParams("compiled-close.txt", "NUL idempotency key", "nul\u0000idempotency-key"),
		);
		expect(nulIdempotencyKey.error).toMatchObject({
			code: -32602,
			message: "main.corpus.close idempotency_key must be an execve-safe string.",
		});
		expect(
			loadWayCore().WayCore.open(state).gatewayMetaRead(["gitlock_closure_operation"]).entries[0]?.value,
		).toBeUndefined();

		// Restart before the next, unrelated closure. A stale intent would make
		// strict startup reconciliation fail closed instead of becoming healthy.
		client.close();
		client = undefined;
		if (daemon?.exitCode === null) daemon.kill("SIGTERM");
		if (daemon) await daemon.exited;
		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: environment,
			stdout: "ignore",
			stderr: "pipe",
		});
		client = await connectHealthy(socketPath);

		fs.writeFileSync(path.join(corpus, "compiled-close.txt"), "closed by compiled daemon\n");
		const firstParams = closureParams("compiled-close.txt", "compiled daemon closure", "compiled-daemon-closure");
		const first = await client.request("main.corpus.close", firstParams, { timeoutMs: 10_000 });
		expect(first.result).toMatchObject({ committed: true });
		const replay = await client.request("main.corpus.close", firstParams);
		expect(replay.result).toEqual(first.result);
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:compiled-close.txt"])).toBe(
			"closed by compiled daemon\n",
		);
		let subjects = await run(["git", `--git-dir=${remote}`, "log", "--format=%s", "main"]);
		expect(subjects.split("\n").filter((subject) => subject === "compiled daemon closure")).toHaveLength(1);

		// Discard the server response only after the paused push completes. The
		// retry must use the durable first result rather than stage another closure.
		fs.writeFileSync(path.join(corpus, "lost-response.txt"), "persist despite response loss\n");
		const lostMarker = path.join(root, "lost-response-marker");
		const lostRelease = path.join(root, "lost-response-release");
		const pushHook = pausePush(corpus, lostMarker, lostRelease);
		const lostParams = closureParams("lost-response.txt", "response-loss closure", "response-loss-key");
		const discardedResponse = requestAndDiscardResponse(socketPath, "main.corpus.close", lostParams);
		await eventually(
			() => (fs.existsSync(lostMarker) ? true : undefined),
			"response-loss closure did not reach pre-push",
		);
		fs.writeFileSync(lostRelease, "release\n");
		await discardedResponse;
		const recovered = await client.request("main.corpus.close", lostParams);
		expect(recovered.result).toMatchObject({ committed: true });
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:lost-response.txt"])).toBe(
			"persist despite response loss\n",
		);
		subjects = await run(["git", `--git-dir=${remote}`, "log", "--format=%s", "main"]);
		expect(subjects.split("\n").filter((subject) => subject === "response-loss closure")).toHaveLength(1);
		fs.rmSync(pushHook, { force: true });

		// The first request remains paused inside Git while a second same-key call
		// arrives. Both must share the same in-flight closure result and one commit.
		fs.writeFileSync(path.join(corpus, "single-flight.txt"), "single flight\n");
		const concurrentMarker = path.join(root, "concurrent-marker");
		const concurrentRelease = path.join(root, "concurrent-release");
		const concurrentPushHook = pausePush(corpus, concurrentMarker, concurrentRelease);
		const concurrentParams = closureParams("single-flight.txt", "single-flight closure", "single-flight-key");
		const firstConcurrent = client.request("main.corpus.close", concurrentParams);
		await eventually(
			() => (fs.existsSync(concurrentMarker) ? true : undefined),
			"first concurrent closure did not reach pre-push",
		);
		const secondClient = await RpcClient.connect(socketPath);
		extraClients.push(secondClient);
		const conflictingParams = closureParams(
			"single-flight.txt",
			"conflicting single-flight closure",
			"single-flight-key",
		);
		const conflict = await secondClient.request("main.corpus.close", conflictingParams, { timeoutMs: 2_000 });
		expect(conflict.error).toMatchObject({ code: 1500, message: "idempotency_conflict" });
		const secondConcurrent = secondClient.request("main.corpus.close", concurrentParams);
		fs.writeFileSync(concurrentRelease, "release\n");
		const [firstConcurrentResponse, secondConcurrentResponse] = await Promise.all([firstConcurrent, secondConcurrent]);
		expect(firstConcurrentResponse.result).toEqual(secondConcurrentResponse.result);
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:single-flight.txt"])).toBe("single flight\n");
		subjects = await run(["git", `--git-dir=${remote}`, "log", "--format=%s", "main"]);
		expect(subjects.split("\n").filter((subject) => subject === "single-flight closure")).toHaveLength(1);
		fs.rmSync(concurrentPushHook, { force: true });

		// Kill the compiled daemon after `git push` succeeds but before the bridge
		// can persist its public idempotency response. Restart reconciliation must
		// derive the original response from the bound operation/recovery evidence.
		client.close();
		client = undefined;
		if (daemon?.exitCode === null) daemon.kill("SIGTERM");
		if (daemon) await daemon.exited;
		const crashMarker = path.join(root, "after-push-marker");
		const crashRelease = path.join(root, "after-push-release");
		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: {
				...environment,
				WAY_E2E_CLOSURE_AFTER_PUSH_MARKER: crashMarker,
				WAY_E2E_CLOSURE_AFTER_PUSH_RELEASE: crashRelease,
			},
			stdout: "ignore",
			stderr: "pipe",
		});
		client = await connectHealthy(socketPath);
		fs.writeFileSync(path.join(corpus, "crash-reconcile.txt"), "recover after daemon death\n");
		const crashParams = closureParams("crash-reconcile.txt", "push-crash closure", "push-crash-key");
		const interrupted = client.request("main.corpus.close", crashParams, { timeoutMs: 10_000 }).catch(() => undefined);
		await eventually(
			() => (fs.existsSync(crashMarker) ? true : undefined),
			"closure did not reach the post-push crash boundary",
		);
		const originalBoundary = JSON.parse(fs.readFileSync(crashMarker, "utf8")) as { readonly response: unknown };
		daemon.kill("SIGKILL");
		await daemon.exited;
		await interrupted;
		client.close();
		client = undefined;
		expect(await run(["git", `--git-dir=${remote}`, "show", "main:crash-reconcile.txt"])).toBe(
			"recover after daemon death\n",
		);
		subjects = await run(["git", `--git-dir=${remote}`, "log", "--format=%s", "main"]);
		expect(subjects.split("\n").filter((subject) => subject === "push-crash closure")).toHaveLength(1);

		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: environment,
			stdout: "ignore",
			stderr: "pipe",
		});
		client = await connectHealthy(socketPath);
		const recoveredAfterRestart = await client.request("main.corpus.close", crashParams, { timeoutMs: 10_000 });
		expect(recoveredAfterRestart.result).toEqual(originalBoundary.response);
		const replayAfterRestart = await client.request("main.corpus.close", crashParams);
		expect(replayAfterRestart.result).toEqual(recoveredAfterRestart.result);
		const crashConflict = await client.request(
			"main.corpus.close",
			closureParams("crash-reconcile.txt", "conflicting push-crash closure", "push-crash-key"),
		);
		expect(crashConflict.error).toMatchObject({ code: 1500, message: "idempotency_conflict" });
		subjects = await run(["git", `--git-dir=${remote}`, "log", "--format=%s", "main"]);
		expect(subjects.split("\n").filter((subject) => subject === "push-crash closure")).toHaveLength(1);

		// A mismatched remote after post-push death makes reconciliation fail before
		// host ownership. The temporary E2E SDK endpoint is live on a healthy
		// resumed session, so the failed-closed probe proves that pre-host disposal
		// closed the same session-owned endpoint.
		client.close();
		client = undefined;
		if (daemon?.exitCode === null) daemon.kill("SIGTERM");
		if (daemon) await daemon.exited;
		fs.rmSync(endpointPath, { force: true });
		const mismatchMarker = path.join(root, "recovery-mismatch-after-push-marker");
		const mismatchRelease = path.join(root, "recovery-mismatch-after-push-release");
		const endpointEnvironment = { ...environment, WAY_E2E_SDK_ENDPOINT_PATH: endpointPath };
		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath],
			cwd: repositoryRoot,
			env: {
				...endpointEnvironment,
				WAY_E2E_CLOSURE_AFTER_PUSH_MARKER: mismatchMarker,
				WAY_E2E_CLOSURE_AFTER_PUSH_RELEASE: mismatchRelease,
			},
			stdout: "ignore",
			stderr: "pipe",
		});
		client = await connectHealthy(socketPath);
		expect(await endpointReachable(endpointPath)).toBe(true);
		fs.writeFileSync(path.join(corpus, "recovery-mismatch.txt"), "mismatched remote state\n");
		const mismatchParams = closureParams("recovery-mismatch.txt", "recovery-mismatch closure", "recovery-mismatch-key");
		const mismatchRequest = client
			.request("main.corpus.close", mismatchParams, { timeoutMs: 10_000 })
			.catch(() => undefined);
		await eventually(
			() => (fs.existsSync(mismatchMarker) ? true : undefined),
			"recovery-mismatch closure did not reach the post-push crash boundary",
		);
		daemon.kill("SIGKILL");
		await daemon.exited;
		await mismatchRequest;
		client.close();
		client = undefined;
		// SIGKILL leaves a dead Unix-socket inode. Remove only that test-owned
		// dead endpoint so strict resume must create and then dispose a fresh one.
		fs.rmSync(endpointPath, { force: true });

		const remoteAdvance = path.join(root, "recovery-mismatch-remote-advance");
		await run(["git", "clone", remote, remoteAdvance]);
		await run(["git", "-C", remoteAdvance, "config", "user.name", "Compiled Closure Drill"]);
		await run(["git", "-C", remoteAdvance, "config", "user.email", "compiled-closure@example.test"]);
		fs.writeFileSync(path.join(remoteAdvance, "remote-advance.txt"), "advance remote beyond recovery evidence\n");
		await run(["git", "-C", remoteAdvance, "add", "--", "remote-advance.txt"]);
		await run(["git", "-C", remoteAdvance, "commit", "-m", "advance remote beyond recovery evidence"]);
		await run(["git", "-C", remoteAdvance, "push"]);

		daemon = Bun.spawn({
			cmd: [executable, "serve", "--state-dir", state, "--profile", profilePath, "--fail-closed-linger-ms", "3000"],
			cwd: repositoryRoot,
			env: endpointEnvironment,
			stdout: "ignore",
			stderr: "pipe",
		});
		client = await connectAvailable(socketPath);
		const mismatchHealth = await failedClosedHealth(client);
		expect(mismatchHealth).toMatchObject({
			status: "unhealthy",
			state: "failed_closed",
			reason: "startup_failed",
			main: { resumed: false, session_id: null },
		});
		// This is a real UDS connect probe, not merely an endpoint-file check.
		expect(await endpointReachable(endpointPath)).toBe(false);
		client.close();
		client = undefined;
		expect(await daemon.exited).toBe(78);
	} finally {
		for (const extraClient of extraClients) extraClient.close();
		client?.close();
		if (daemon?.exitCode === null) daemon.kill("SIGTERM");
		if (daemon) {
			await Promise.race([daemon.exited, Bun.sleep(2_000)]);
			if (daemon.exitCode === null) daemon.kill("SIGKILL");
			await daemon.exited;
		}
		fs.rmSync(endpointPath, { force: true });
		fs.rmSync(root, { force: true, recursive: true });
	}
}, 60_000);
