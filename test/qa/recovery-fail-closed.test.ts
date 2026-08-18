import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { expect, test } from "bun:test";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayCore } from "../../src/native-loader";
import { RpcClient } from "../../src/rpc-client";

function temporaryDirectory(name: string): string {
	return fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-qa-${name}-`));
}

function daemonEnvironment(): NodeJS.ProcessEnv {
	return {
		...process.env,
		NODE_ENV: "test",
		WAY_E2E_FILE_SDK: "1",
		WAY_BROKER_CLI: "/usr/bin/false",
		WAY_RECONCILE_POLL_MS: "600000",
	};
}

function profileContents(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

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
				// The listener may not be ready yet.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error(`RPC socket did not become available: ${socketPath}`);
}

async function waitForHealth(client: RpcClient, expectedState: "running" | "failed_closed"): Promise<Record<string, unknown>> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		const response = await client.request("way.health", {});
		if (response.result && (response.result as { state?: unknown }).state === expectedState)
			return response.result as Record<string, unknown>;
		await Bun.sleep(10);
	}
	throw new Error(`Daemon did not reach ${expectedState}.`);
}

interface RunningDaemon {
	readonly child: ReturnType<typeof Bun.spawn>;
	readonly client: RpcClient;
}

async function startDaemon(stateDirectory: string, profilePath: string, lingerMs?: number): Promise<RunningDaemon> {
	const child = Bun.spawn({
		cmd: [
			"bun",
			"src/main.ts",
			"serve",
			"--state-dir",
			stateDirectory,
			"--profile",
			profilePath,
			...(lingerMs === undefined ? [] : ["--fail-closed-linger-ms", String(lingerMs)]),
		],
		cwd: process.cwd(),
		env: daemonEnvironment(),
		stdout: "ignore",
		stderr: "pipe",
	});
	try {
		return { child, client: await connectEventually(path.join(stateDirectory, "rpc.sock")) };
	} catch (error) {
		if (child.exitCode === null) child.kill("SIGKILL");
		await child.exited;
		throw error;
	}
}

async function stopDaemon(daemon: RunningDaemon | undefined): Promise<void> {
	if (!daemon) return;
	daemon.client.close();
	if (daemon.child.exitCode === null) daemon.child.kill("SIGTERM");
	await Promise.race([daemon.child.exited, Bun.sleep(3_000)]);
	if (daemon.child.exitCode === null) daemon.child.kill("SIGKILL");
	await daemon.child.exited;
}

test("a tampered transcript prefix fails closed over the real UDS, lingers unhealthy, exits 78, and never advertises a resumed main", async () => {
	const root = temporaryDirectory("prefix-recovery");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const stateDirectory = path.join(root, "state");
	const profilePath = path.join(root, "profile.toml");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	fs.writeFileSync(profilePath, profileContents(corpus, workspace));
	let healthy: RunningDaemon | undefined;
	let failed: RunningDaemon | undefined;
	try {
		const bootstrap = Bun.spawnSync({
			cmd: ["bun", "src/main.ts", "bootstrap", "--confirm", "--state-dir", stateDirectory, "--profile", profilePath],
			cwd: process.cwd(),
			env: daemonEnvironment(),
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(bootstrap.exitCode, new TextDecoder().decode(bootstrap.stderr)).toBe(0);

		healthy = await startDaemon(stateDirectory, profilePath);
		expect(await waitForHealth(healthy.client, "running")).toMatchObject({
			status: "healthy",
			state: "running",
			main: { resumed: true },
		});
		await stopDaemon(healthy);
		healthy = undefined;

		const state = new GatewayStateStore(loadWayCore().WayCore.open(stateDirectory));
		const identity = state.read().mainIdentity;
		if (!identity) throw new Error("bootstrap did not persist a main identity");
		state.writeGrowthIntent(identity, Date.now());
		const transcript = Buffer.from(fs.readFileSync(identity.canonicalPath));
		const tamperOffset = transcript.indexOf(Buffer.from("e2e bootstrap persisted"));
		if (tamperOffset === -1) throw new Error("bootstrap transcript lacked the expected durable assistant content");
		transcript[tamperOffset] = 0x78;
		fs.writeFileSync(identity.canonicalPath, transcript);

		failed = await startDaemon(stateDirectory, profilePath, 750);
		expect(await waitForHealth(failed.client, "failed_closed")).toMatchObject({
			status: "unhealthy",
			state: "failed_closed",
			reason: "growth_intent_mismatch",
			main: { resumed: false, session_id: null },
		});
		failed.client.close();
		expect(await failed.child.exited).toBe(78);
	} finally {
		await stopDaemon(failed);
		await stopDaemon(healthy);
		fs.rmSync(root, { force: true, recursive: true });
	}
}, 15_000);
