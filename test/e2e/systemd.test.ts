import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { loadDiscordAdapterConfig } from "../../src/adapter/discord/config";
import { startDiscordAdapter, type RunningDiscordAdapter } from "../../src/adapter/discord/main";
import { loadWayProfile } from "../../src/profile";
import { RpcClient } from "../../src/rpc-client";
import { DiscordFixture } from "../fixtures/discord-fixture";
import { ManagedProcessRegistry } from "../helpers/managed-process";
import { FakeBrokerFixture } from "../helpers/main-session";

const managedProcesses = new ManagedProcessRegistry();

const repositoryRoot = path.resolve(import.meta.dir, "..", "..");
const temporaryDirectories: string[] = [];

type ParsedUnit = {
	readonly source: string;
	readonly sections: ReadonlyMap<string, ReadonlyMap<string, readonly string[]>>;
};

interface RunningDaemon {
	readonly child: ReturnType<typeof Bun.spawn>;
	readonly client: RpcClient;
}

interface SupervisedServiceOptions {
	readonly executable: string;
	readonly stateDirectory: string;
	readonly profilePath: string;
	readonly environment: NodeJS.ProcessEnv;
	readonly fixture: DiscordFixture;
}

afterEach(async () => {
	await managedProcesses.reapAll();
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { force: true, recursive: true });
});

function parseUnit(relativePath: string): ParsedUnit {
	const source = fs.readFileSync(path.join(repositoryRoot, relativePath), "utf8");
	const sections = new Map<string, Map<string, string[]>>();
	let section = "";
	for (const rawLine of source.split(/\r?\n/)) {
		const line = rawLine.trim();
		if (!line || line.startsWith("#") || line.startsWith(";")) continue;
		if (line.startsWith("[") && line.endsWith("]")) {
			section = line.slice(1, -1);
			continue;
		}
		const equals = line.indexOf("=");
		if (equals < 1 || !section) throw new Error(`Invalid unit line in ${relativePath}: ${rawLine}`);
		const key = line.slice(0, equals).trim();
		const value = line.slice(equals + 1).trim();
		const entries = sections.get(section) ?? new Map<string, string[]>();
		const values = entries.get(key) ?? [];
		values.push(value);
		entries.set(key, values);
		sections.set(section, entries);
	}
	return { source, sections };
}

function values(unit: ParsedUnit, section: string, directive: string): readonly string[] {
	return unit.sections.get(section)?.get(directive) ?? [];
}

function expectValue(unit: ParsedUnit, section: string, directive: string, expected: string): void {
	expect(values(unit, section, directive)).toContain(expected);
}

function expectWords(unit: ParsedUnit, section: string, directive: string, expected: readonly string[]): void {
	const actual = new Set(values(unit, section, directive).flatMap(value => value.split(/\s+/).filter(Boolean)));
	expect([...actual].sort()).toEqual([...expected].sort());
}

function temporaryDirectory(name: string): string {
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), `gajaeway-${name}-`));
	temporaryDirectories.push(directory);
	return directory;
}

function testProfile(corpus: string, workspace: string, sessionId: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[surfaces.owner]
id = "discord:owner-dm"
platform = "discord"
kind = "dm"

[main_session]
session_id = "${sessionId}"
`;
}

function e2eEnvironment(fixture: FakeBrokerFixture): NodeJS.ProcessEnv {
	return {
		...fixture.environment(),
		NODE_ENV: "test",
		GAJAEWAY_BROKER_CLI: fixture.executable,
		GAJAEWAY_RECONCILE_POLL_MS: "600000",
	};
}

function compiledWay(): string {
	const executable = path.join(repositoryRoot, "dist", "gajaeway");
	if (!fs.existsSync(executable)) {
		throw new Error("The supervised restart drill requires dist/gajaeway. Run bun scripts/compile.ts before bun test.");
	}
	return executable;
}

async function runCommand(command: readonly string[], environment: NodeJS.ProcessEnv): Promise<{ readonly exitCode: number; readonly stdout: string; readonly stderr: string }> {
	const child = Bun.spawn({ cmd: [...command], cwd: repositoryRoot, env: environment, stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
	return { exitCode, stdout, stderr };
}

async function healthyClient(socketPath: string, description: string): Promise<RpcClient> {
	let lastError: unknown;
	for (let attempt = 0; attempt < 200; attempt += 1) {
		if (fs.existsSync(socketPath)) {
			let client: RpcClient | undefined;
			try {
				client = await RpcClient.connect(socketPath);
				const response = await client.request("way.health", {}, { timeoutMs: 1_000 });
				if (response.result && (response.result as { status?: unknown }).status === "healthy") return client;
				client.close();
			} catch (error) {
				client?.close();
				lastError = error;
			}
		}
		await Bun.sleep(25);
	}
	throw new Error(`${description}${lastError instanceof Error ? `: ${lastError.message}` : ""}`);
}

async function startDaemon(executable: string, stateDirectory: string, profilePath: string, environment: NodeJS.ProcessEnv): Promise<RunningDaemon> {
	const child = managedProcesses.spawnDaemon({
		cmd: [executable, "serve", "--state-dir", stateDirectory, "--profile", profilePath],
		cwd: repositoryRoot,
		env: environment,
	});
	try {
		const client = await healthyClient(path.join(stateDirectory, "rpc.sock"), "daemon did not become healthy");
		return { child, client };
	} catch (error) {
		await managedProcesses.stopDaemon(child);
		throw error;
	}
}


async function stopAdapter(adapter: RunningDiscordAdapter | undefined): Promise<void> {
	if (adapter) await adapter.stop();
}

async function stopDaemon(daemon: RunningDaemon | undefined): Promise<void> {
	if (!daemon) return;
	daemon.client.close();
	await managedProcesses.stopDaemon(daemon.child);
}

/**
 * Portable model of the two systemd units. The test invokes only a daemon
 * crash; BindsTo stops the adapter and PartOf restarts it with the daemon.
 */
class SupervisedService {
	readonly #options: SupervisedServiceOptions;
	#daemon: RunningDaemon | undefined;
	#adapter: RunningDiscordAdapter | undefined;
	#generation = 0;
	#stopping = false;
	#failure: unknown;

	constructor(options: SupervisedServiceOptions) {
		this.#options = options;
	}

	get daemon(): RunningDaemon {
		if (!this.#daemon) throw new Error("supervised daemon is not running");
		return this.#daemon;
	}

	async start(): Promise<void> {
		await this.startGeneration();
	}

	async crashDaemon(): Promise<void> {
		const generation = this.#generation;
		await managedProcesses.crashDaemon(this.daemon.child);
		await eventually(
			() => {
				if (this.#failure) throw this.#failure;
				return this.#generation > generation ? this.#daemon : undefined;
			},
			"systemd did not restore the daemon and adapter after daemon failure",
			12_000,
		);
	}

	async stop(): Promise<void> {
		this.#stopping = true;
		await stopAdapter(this.#adapter);
		this.#adapter = undefined;
		await stopDaemon(this.#daemon);
		this.#daemon = undefined;
	}

	private async startGeneration(): Promise<void> {
		const daemon = await startDaemon(
			this.#options.executable,
			this.#options.stateDirectory,
			this.#options.profilePath,
			this.#options.environment,
		);
		this.#daemon = daemon;
		try {
			this.#adapter = await startDiscordAdapter(
				{
					rpcSocketPath: path.join(this.#options.stateDirectory, "rpc.sock"),
					token: "fixture-token",
					route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
					ackBudgetMs: 2_000,
					claimTtlMs: 5_000,
					readWaitMs: 0,
				},
				{ platformFactory: () => this.#options.fixture, onError: () => undefined },
			);
			this.#generation += 1;
			void this.watchDaemon(daemon).catch(error => {
				this.#failure = error;
			});
		} catch (error) {
			await stopDaemon(daemon);
			this.#daemon = undefined;
			throw error;
		}
	}

	private async watchDaemon(daemon: RunningDaemon): Promise<void> {
		const exitCode = await daemon.child.exited;
		daemon.client.close();
		if (this.#daemon !== daemon) return;
		await stopAdapter(this.#adapter);
		this.#adapter = undefined;
		if (this.#stopping || exitCode === 0 || restartPrevented(parseUnit("ops/systemd/gajaeway.service"), exitCode)) return;
		try {
			await this.startGeneration();
		} catch (error) {
			this.#failure = error;
		}
	}
}

async function eventually<T>(read: () => T | undefined | Promise<T | undefined>, description: string, timeoutMs = 10_000): Promise<T> {
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

function restartPrevented(unit: ParsedUnit, exitCode: number): boolean {
	return values(unit, "Service", "RestartPreventExitStatus").flatMap(value => value.split(/\s+/)).includes(String(exitCode));
}

test("systemd units declare the required hardened daemon and bound adapter contract", () => {
	const daemon = parseUnit("ops/systemd/gajaeway.service");
	const adapter = parseUnit("ops/systemd/gajaeway-discord.service");

	expectValue(daemon, "Unit", "StartLimitIntervalSec", "60s");
	expectValue(daemon, "Unit", "StartLimitBurst", "3");
	expectValue(daemon, "Service", "Type", "notify");
	expectValue(daemon, "Service", "NotifyAccess", "main");
	expectValue(daemon, "Service", "TimeoutStartSec", "6min");
	expectValue(daemon, "Service", "User", "gajaeway");
	expectValue(daemon, "Service", "Group", "gajaeway");
	expectValue(daemon, "Service", "WorkingDirectory", "/var/lib/gajaeway");
	expectValue(daemon, "Service", "Environment", "GAJAEWAY_STATE_DIR=/var/lib/gajaeway");
	expectValue(daemon, "Service", "Environment", "GAJAEWAY_PROFILE=/etc/gajaeway/profile.toml");

	expectValue(daemon, "Service", "UMask", "0077");
	expectValue(daemon, "Service", "RuntimeDirectory", "gajaeway");
	expectValue(daemon, "Service", "RuntimeDirectoryMode", "0700");
	expectValue(daemon, "Service", "StateDirectory", "gajaeway");
	expectValue(daemon, "Service", "StateDirectoryMode", "0700");
	expectValue(daemon, "Service", "NoNewPrivileges", "yes");
	expectValue(daemon, "Service", "ProtectSystem", "strict");
	expectValue(daemon, "Service", "ProtectHome", "true");
	expect(daemon.source).toContain("set ProtectHome=read-only");
	expectValue(daemon, "Service", "ProtectKernelTunables", "yes");
	expectValue(daemon, "Service", "ProtectKernelModules", "yes");
	expectValue(daemon, "Service", "ProtectKernelLogs", "yes");
	expectWords(daemon, "Service", "RestrictAddressFamilies", ["AF_UNIX", "AF_INET", "AF_INET6"]);
	expectValue(daemon, "Service", "MemoryMax", "1G");
	expectValue(daemon, "Service", "TasksMax", "256");
	expectValue(daemon, "Service", "TimeoutStopSec", "30s");
	expectValue(daemon, "Service", "KillMode", "mixed");
	expectValue(daemon, "Service", "Restart", "on-failure");
	expectValue(daemon, "Service", "RestartSec", "5s");
	expectWords(daemon, "Unit", "Wants", ["network-online.target", "gajaeway-discord.service"]);
	expectValue(daemon, "Unit", "Before", "gajaeway-discord.service");

	expectValue(daemon, "Service", "RestartPreventExitStatus", "78");
	expectValue(daemon, "Service", "ExecStart", "/usr/local/bin/gajaeway serve --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml");
	expectWords(daemon, "Service", "ReadWritePaths", ["/var/lib/gajaeway", "/srv/gajaeway/corpus", "/srv/gajaeway/workspace"]);

	expectValue(adapter, "Unit", "Requires", "gajaeway.service");
	expectValue(adapter, "Unit", "BindsTo", "gajaeway.service");
	expectValue(adapter, "Unit", "PartOf", "gajaeway.service");
	expectWords(adapter, "Unit", "After", ["network-online.target", "gajaeway.service"]);
	expectValue(adapter, "Unit", "StartLimitIntervalSec", "60s");
	expectValue(adapter, "Unit", "StartLimitBurst", "3");
	expectValue(adapter, "Service", "Type", "simple");
	expectValue(adapter, "Service", "User", "gajaeway");
	expectValue(adapter, "Service", "Group", "gajaeway");
	expectValue(adapter, "Service", "UMask", "0077");
	expectValue(adapter, "Service", "Environment", "GAJAEWAY_STATE_DIR=/var/lib/gajaeway");
	expectValue(adapter, "Service", "Environment", "GAJAEWAY_PROFILE=/etc/gajaeway/profile.toml");
	expectValue(adapter, "Service", "NoNewPrivileges", "yes");
	expectValue(adapter, "Service", "PrivateTmp", "yes");
	expectValue(adapter, "Service", "ProtectSystem", "strict");
	expectValue(adapter, "Service", "ProtectHome", "true");
	expectValue(adapter, "Service", "ProtectKernelTunables", "yes");
	expectValue(adapter, "Service", "ProtectKernelModules", "yes");
	expectValue(adapter, "Service", "ProtectKernelLogs", "yes");
	expectValue(adapter, "Service", "Restart", "on-failure");
	expectValue(adapter, "Service", "RestartSec", "5s");
	expectValue(adapter, "Service", "TimeoutStopSec", "30s");
	expectValue(adapter, "Service", "KillMode", "mixed");
	expectValue(adapter, "Service", "MemoryMax", "512M");
	expectValue(adapter, "Service", "TasksMax", "128");
	expectValue(adapter, "Service", "LoadCredential", "discord-token:/etc/gajaeway/credentials/discord-token");
	expectValue(adapter, "Service", "Environment", "GAJAEWAY_DISCORD_TOKEN_FILE=%d/discord-token");
	expect(adapter.source).toContain("mode 0600");
	expectValue(adapter, "Service", "ExecStart", "/usr/local/bin/gajaeway-discord --state-dir /var/lib/gajaeway --profile /etc/gajaeway/profile.toml");
	expectWords(adapter, "Service", "RestrictAddressFamilies", ["AF_UNIX", "AF_INET", "AF_INET6"]);
	expectValue(adapter, "Service", "ReadOnlyPaths", "/var/lib/gajaeway");

	const mainSource = fs.readFileSync(path.join(repositoryRoot, "src/main.ts"), "utf8");
	const notifySource = fs.readFileSync(path.join(repositoryRoot, "crates/way-core/src/systemd.rs"), "utf8");
	expect(mainSource).toContain('core.sdNotifyReady("gajaeway running")');
	expect(notifySource).toContain("READY=1\\nSTATUS=");
});

test("example profile covers the identity projection, mutable tunables, and Discord adapter section", () => {
	const profilePath = path.join(repositoryRoot, "ops/profiles/gaebal-gajae.example.toml");
	const profile = loadWayProfile(profilePath);
	expect(profile.injection.files).toEqual(["SOUL.md", "USER.md", "daily/{date}.md", "MEMORY.md"]);
	expect(profile.restrictedFiles.conversation).toEqual(["MEMORY.md"]);
	expect(profile.ownerSurfaces).toEqual([{ id: "discord:owner-dm", platform: "discord", kind: "dm" }]);
	expect(profile.operator).toMatchObject({ id: "gaebal-gajae-operator" });
	expect(profile.tunables).toMatchObject({ poll: { interval_ms: 15000 }, ack: { budget_ms: 2000 } });
	const adapter = loadDiscordAdapterConfig({
		profile,
		profilePath,
		stateDir: "/var/lib/gajaeway",
		environment: { GAJAEWAY_DISCORD_BOT_TOKEN: "fixture-token" },
	});
	expect(adapter).toMatchObject({ route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" }, ackBudgetMs: 2000, claimTtlMs: 5000, readWaitMs: 1000 });
});

test("supervised daemon restart restores the PartOf-bound fixture adapter and compiled RPC delivery", async () => {
	const executable = compiledWay();
	const root = temporaryDirectory("sd");
	const fixtureSession = new FakeBrokerFixture();
	const stateDirectory = path.join(root, "state");
	const corpus = path.join(root, "corpus");
	const workspace = fixtureSession.workspace;
	const profilePath = path.join(root, "profile.toml");
	fs.mkdirSync(corpus);
	fs.writeFileSync(profilePath, testProfile(corpus, workspace, fixtureSession.sessionId));
	const environment = e2eEnvironment(fixtureSession);
	const bootstrap = await runCommand([executable, "bootstrap", "--confirm", "--state-dir", stateDirectory, "--profile", profilePath], environment);
	expect(bootstrap.exitCode).toBe(0);
	expect(bootstrap.stdout).toContain('"state":"committed"');

	const fixture = new DiscordFixture();
	let service: SupervisedService | undefined;
	try {
		service = new SupervisedService({ executable, stateDirectory, profilePath, environment, fixture });
		await service.start();
		expect((await service.daemon.client.request("way.health")).result).toMatchObject({ status: "healthy", state: "running" });

		// The test only crashes the daemon. The unit topology causes BindsTo to stop
		// the adapter and PartOf to bring it back with the daemon's restart.
		await service.crashDaemon();
		expect((await service.daemon.client.request("way.health")).result).toMatchObject({ status: "healthy", state: "running" });

		const submitted = await service.daemon.client.request("main.submit", {
			text: "restart fixture delivery",
			surface_id: "discord:owner-dm",
			idempotency_key: "systemd-restart-delivery",
		});
		expect(submitted.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		const delivery = await eventually(
			() => fixture.sends.find(send => send.text === "ack"),
			"restarted adapter did not deliver the fixture reply",
			12_000,
		);
		expect(delivery.channelId).toBe("123456789012345678");
		expect(fixture.connectCount).toBeGreaterThanOrEqual(2);
		expect(fixture.disconnectCount).toBeGreaterThanOrEqual(1);

		const failClosed = await runCommand(
			[executable, "serve", "--state-dir", path.join(root, "x"), "--profile", profilePath, "--fail-closed-linger-ms", "25"],
			environment,
		);
		expect(failClosed.exitCode, failClosed.stderr).toBe(78);
		expect(restartPrevented(parseUnit("ops/systemd/gajaeway.service"), failClosed.exitCode)).toBe(true);
	} finally {
		await service?.stop();
		fixtureSession.dispose();
	}
}, 30_000);
