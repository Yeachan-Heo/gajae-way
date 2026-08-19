import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { bootstrapMainSession } from "../../src/main-session/bootstrap";
import { GatewayStateStore } from "../../src/main-session/state";
import { loadWayProfile } from "../../src/profile";
import { loadWayCore } from "../../src/native-loader";
import { FileSdkDouble } from "../helpers/main-session";
import { RpcClient } from "../helpers/rpc-client";
import { ManagedProcessRegistry } from "../helpers/managed-process";

const managedProcesses = new ManagedProcessRegistry();

afterEach(async () => {
	await managedProcesses.reapAll();
});


async function connectEventually(socketPath: string): Promise<RpcClient> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			try {
				return await RpcClient.connect(socketPath);
			} catch {
				// The listener is still becoming ready.
			}
		}
		await Bun.sleep(10);
	}
	throw new Error("RPC socket did not become available.");
}

interface RunningDaemon {
	readonly child: ReturnType<typeof Bun.spawn>;
	readonly client: RpcClient;
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

async function waitForHealth(client: RpcClient, expected: "running" | "failed_closed"): Promise<Record<string, unknown>> {
	for (let attempt = 0; attempt < 100; attempt += 1) {
		const response = await client.request("way.health");
		if (response.result && (response.result as { state?: unknown }).state === expected) return response.result as Record<string, unknown>;
		await Bun.sleep(10);
	}
	throw new Error(`Daemon did not reach ${expected}.`);
}

async function startDaemon(stateDirectory: string, profilePath: string, failClosedLingerMs?: number): Promise<RunningDaemon> {
	const child = managedProcesses.spawnDaemon({
		cmd: [
			"bun",
			"src/main.ts",
			"serve",
			"--state-dir",
			stateDirectory,
			"--profile",
			profilePath,
			...(failClosedLingerMs === undefined ? [] : ["--fail-closed-linger-ms", String(failClosedLingerMs)]),
		],
		cwd: process.cwd(),
		env: daemonEnvironment(),
	});
	try {
		return { child, client: await connectEventually(path.join(stateDirectory, "rpc.sock")) };
	} catch (error) {
		await managedProcesses.stopDaemon(child);
		throw error;
	}
}

async function stopDaemon(server: RunningDaemon | undefined): Promise<void> {
	if (!server) return;
	server.client.close();
	await managedProcesses.stopDaemon(server.child);
}

function profileContents(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = ["SOUL.md", "USER.md"]

[surfaces.owner]
id = "owner"
platform = "test"
kind = "dm"
`;
}

async function bootstrapNativeState(root: string): Promise<{
	stateDirectory: string;
	profilePath: string;
	state: GatewayStateStore;
	sdk: FileSdkDouble;
}> {
	const stateDirectory = path.join(root, "state");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	const profilePath = path.join(root, "profile.toml");
	fs.writeFileSync(profilePath, profileContents(corpus, workspace));
	const state = new GatewayStateStore(loadWayCore().WayCore.open(stateDirectory));
	const sdk = new FileSdkDouble();
	await bootstrapMainSession({ confirm: true, profile: loadWayProfile(profilePath), state, sdk });
	return { stateDirectory, profilePath, state, sdk };
}

test("way profile approve --confirm updates a stopped daemon's bound projection", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-profile-direct-"));
	try {
		const setup = await bootstrapNativeState(root);
		fs.writeFileSync(setup.profilePath, fs.readFileSync(setup.profilePath, "utf8").replace('"SOUL.md", "USER.md"', '"USER.md", "SOUL.md"'));
		const command = Bun.spawnSync({
			cmd: ["bun", "src/main.ts", "profile", "approve", "--confirm", "--state-dir", setup.stateDirectory, "--profile", setup.profilePath],
			cwd: process.cwd(),
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(command.exitCode).toBe(0);
		expect(new TextDecoder().decode(command.stdout)).toContain("receipt_id");
		expect(setup.state.read().bootstrapState).toBe("COMMITTED");
		expect(setup.state.read().profileDigest).toBe(loadWayProfile(setup.profilePath).digest.sha256);
		expect(loadWayCore().WayCore.open(setup.stateDirectory).journalRead(undefined, 10).events.map(event => event.kind)).toContain("profile_approved");
	} finally {
		fs.rmSync(root, { recursive: true, force: true });
	}
});

test("digest-bound profile drift fails closed until approval, then the next daemon restart is healthy", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-profile-restart-"));
	let initial: RunningDaemon | undefined;
	let failed: RunningDaemon | undefined;
	let resumed: RunningDaemon | undefined;
	try {
		const setup = await bootstrapNativeState(root);
		initial = await startDaemon(setup.stateDirectory, setup.profilePath);
		expect(await waitForHealth(initial.client, "running")).toMatchObject({ status: "healthy", state: "running" });
		await stopDaemon(initial);
		initial = undefined;
		fs.writeFileSync(setup.profilePath, fs.readFileSync(setup.profilePath, "utf8").replace('"SOUL.md", "USER.md"', '"USER.md", "SOUL.md"'));
		failed = await startDaemon(setup.stateDirectory, setup.profilePath, 2_000);
		const unhealthy = await waitForHealth(failed.client, "failed_closed");
		expect(unhealthy).toMatchObject({ status: "unhealthy", state: "failed_closed", reason: "profile_drift" });

		const approval = Bun.spawnSync({
			cmd: ["bun", "src/main.ts", "profile", "approve", "--confirm", "--state-dir", setup.stateDirectory, "--profile", setup.profilePath],
			cwd: process.cwd(),
			stdout: "pipe",
			stderr: "pipe",
		});
		expect(approval.exitCode).toBe(0);
		expect(new TextDecoder().decode(approval.stdout)).toContain("receipt_id");
		expect(await failed.child.exited).toBe(78);
		expect(setup.state.read()).toMatchObject({ bootstrapState: "COMMITTED", failedClosedReason: undefined });

		resumed = await startDaemon(setup.stateDirectory, setup.profilePath);
		const healthy = await waitForHealth(resumed.client, "running");
		expect(healthy).toMatchObject({ status: "healthy", state: "running" });
	} finally {
		await stopDaemon(resumed);
		await stopDaemon(failed);
		await stopDaemon(initial);
		fs.rmSync(root, { recursive: true, force: true });
	}
}, 15_000);

test("a tunable-only profile edit restarts healthy without profile approval", async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "gajae-way-profile-tunable-"));
	let initial: RunningDaemon | undefined;
	let restarted: RunningDaemon | undefined;
	try {
		const setup = await bootstrapNativeState(root);
		const priorDigest = setup.state.read().profileDigest;
		initial = await startDaemon(setup.stateDirectory, setup.profilePath);
		expect(await waitForHealth(initial.client, "running")).toMatchObject({ status: "healthy", state: "running" });
		await stopDaemon(initial);
		initial = undefined;
		fs.appendFileSync(setup.profilePath, "\n[poll]\ninterval_ms = 20000\n");
		restarted = await startDaemon(setup.stateDirectory, setup.profilePath);
		const healthy = await waitForHealth(restarted.client, "running");
		expect(healthy).toMatchObject({ status: "healthy", state: "running" });
		expect(setup.state.read().profileDigest).toBe(priorDigest);
		expect(setup.state.read().bootstrapState).toBe("COMMITTED");
	} finally {
		await stopDaemon(restarted);
		await stopDaemon(initial);
		fs.rmSync(root, { recursive: true, force: true });
	}
}, 10_000);
