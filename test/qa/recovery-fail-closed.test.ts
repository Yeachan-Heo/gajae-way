import * as fs from "node:fs";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
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

async function waitForHealth(client: RpcClient, expected: "running" | "failed_closed"): Promise<Record<string, unknown>> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		const response = await client.request("way.health", {});
		if ((response.result as { state?: unknown } | undefined)?.state === expected) return response.result as Record<string, unknown>;
		await Bun.sleep(10);
	}
	throw new Error(`Daemon did not reach ${expected}.`);
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
		expect(fixture.commands().filter(command => command.operation === "turn.prompt" && command.text === request.text)).toHaveLength(1);

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
