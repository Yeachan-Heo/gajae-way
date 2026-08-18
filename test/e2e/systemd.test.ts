import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { afterEach, expect, test } from "bun:test";
import { loadDiscordAdapterConfig } from "../../src/adapter/discord/config";
import { startDiscordAdapter, type RunningDiscordAdapter } from "../../src/adapter/discord/main";
import { loadWayProfile } from "../../src/profile";
import { RpcClient } from "../../src/rpc-client";
import { DiscordFixture } from "../fixtures/discord-fixture";

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

interface RunningService {
	readonly daemon: RunningDaemon;
	readonly adapter: RunningDiscordAdapter;
}

afterEach(async () => {
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
	const directory = fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-${name}-`));
	temporaryDirectories.push(directory);
	return directory;
}

function testProfile(corpus: string, workspace: string): string {
	return `[corpus]
path = "${corpus}"
workspace = "${workspace}"

[injection]
files = []

[surfaces.owner]
id = "discord:owner-dm"
platform = "discord"
kind = "dm"
`;
}

function e2eEnvironment(): NodeJS.ProcessEnv {
	return {
		...process.env,
		NODE_ENV: "test",
		WAY_E2E_FILE_SDK: "1",
		WAY_BROKER_CLI: "/usr/bin/false",
		WAY_RECONCILE_POLL_MS: "600000",
	};
}

function compiledWay(): string {
	const executable = path.join(repositoryRoot, "dist", "way");
	if (!fs.existsSync(executable)) {
		throw new Error("The supervised restart drill requires dist/way. Run bun scripts/compile.ts before bun test.");
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
	const child = Bun.spawn({
		cmd: [executable, "serve", "--state-dir", stateDirectory, "--profile", profilePath],
		cwd: repositoryRoot,
		env: environment,
		stdout: "ignore",
		stderr: "ignore",
	});
	try {
		const client = await healthyClient(path.join(stateDirectory, "rpc.sock"), "daemon did not become healthy");
		return { child, client };
	} catch (error) {
		if (child.exitCode === null) child.kill("SIGKILL");
		await child.exited;
		throw error;
	}
}

async function awaitChildExit(child: ReturnType<typeof Bun.spawn>, timeoutMs = 3_000): Promise<void> {
	await Promise.race([child.exited, Bun.sleep(timeoutMs)]);
	if (child.exitCode === null) child.kill("SIGKILL");
	await child.exited;
}

async function stopAdapter(adapter: RunningDiscordAdapter | undefined): Promise<void> {
	if (adapter) await adapter.stop();
}

async function stopDaemon(daemon: RunningDaemon | undefined): Promise<void> {
	if (!daemon) return;
	daemon.client.close();
	if (daemon.child.exitCode === null) daemon.child.kill("SIGTERM");
	await awaitChildExit(daemon.child);
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
	const daemon = parseUnit("ops/systemd/gajae-way.service");
	const adapter = parseUnit("ops/systemd/gajae-way-discord.service");

	expectValue(daemon, "Unit", "StartLimitIntervalSec", "60s");
	expectValue(daemon, "Unit", "StartLimitBurst", "3");
	expectValue(daemon, "Service", "Type", "notify");
	expectValue(daemon, "Service", "NotifyAccess", "main");
	expectValue(daemon, "Service", "TimeoutStartSec", "6min");
	expectValue(daemon, "Service", "User", "gajae-way");
	expectValue(daemon, "Service", "Group", "gajae-way");
	expectValue(daemon, "Service", "WorkingDirectory", "/var/lib/gajae-way");
	expectValue(daemon, "Service", "Environment", "WAY_STATE_DIR=/var/lib/gajae-way");
	expectValue(daemon, "Service", "Environment", "WAY_PROFILE=/etc/gajae-way/profile.toml");

	expectValue(daemon, "Service", "UMask", "0077");
	expectValue(daemon, "Service", "RuntimeDirectory", "gajae-way");
	expectValue(daemon, "Service", "RuntimeDirectoryMode", "0700");
	expectValue(daemon, "Service", "StateDirectory", "gajae-way");
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

	expectValue(daemon, "Service", "RestartPreventExitStatus", "78");
	expectValue(daemon, "Service", "ExecStart", "/usr/local/bin/way serve --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml");
	expectWords(daemon, "Service", "ReadWritePaths", ["/var/lib/gajae-way", "/srv/gajae-way/corpus", "/srv/gajae-way/workspace"]);

	expectValue(adapter, "Unit", "Requires", "gajae-way.service");
	expectValue(adapter, "Unit", "BindsTo", "gajae-way.service");
	expectWords(adapter, "Unit", "After", ["network-online.target", "gajae-way.service"]);
	expectValue(adapter, "Unit", "StartLimitIntervalSec", "60s");
	expectValue(adapter, "Unit", "StartLimitBurst", "3");
	expectValue(adapter, "Service", "Type", "simple");
	expectValue(adapter, "Service", "User", "gajae-way");
	expectValue(adapter, "Service", "Group", "gajae-way");
	expectValue(adapter, "Service", "UMask", "0077");
	expectValue(adapter, "Service", "Environment", "WAY_STATE_DIR=/var/lib/gajae-way");
	expectValue(adapter, "Service", "Environment", "WAY_PROFILE=/etc/gajae-way/profile.toml");
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
	expectValue(adapter, "Service", "LoadCredential", "discord-token:/etc/gajae-way/credentials/discord-token");
	expectValue(adapter, "Service", "Environment", "WAY_DISCORD_TOKEN_FILE=%d/discord-token");
	expect(adapter.source).toContain("mode 0600");
	expectValue(adapter, "Service", "ExecStart", "/usr/local/bin/way-discord --state-dir /var/lib/gajae-way --profile /etc/gajae-way/profile.toml");
	expectWords(adapter, "Service", "RestrictAddressFamilies", ["AF_UNIX", "AF_INET", "AF_INET6"]);
	expectValue(adapter, "Service", "ReadOnlyPaths", "/var/lib/gajae-way");

	const mainSource = fs.readFileSync(path.join(repositoryRoot, "src/main.ts"), "utf8");
	const notifySource = fs.readFileSync(path.join(repositoryRoot, "crates/way-core/src/systemd.rs"), "utf8");
	expect(mainSource).toContain('core.sdNotifyReady("gajae-way running")');
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
		stateDir: "/var/lib/gajae-way",
		environment: { WAY_DISCORD_BOT_TOKEN: "fixture-token" },
	});
	expect(adapter).toMatchObject({ route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" }, ackBudgetMs: 2000, claimTtlMs: 5000, readWaitMs: 1000 });
});

test("supervised restart recovers compiled way RPC and fixture adapter delivery; exit 78 is restart-prevented", async () => {
	const executable = compiledWay();
	const root = temporaryDirectory("sd");
	const stateDirectory = path.join(root, "state");
	const corpus = path.join(root, "corpus");
	const workspace = path.join(root, "workspace");
	const profilePath = path.join(root, "profile.toml");
	fs.mkdirSync(corpus);
	fs.mkdirSync(workspace);
	fs.writeFileSync(profilePath, testProfile(corpus, workspace));
	const environment = e2eEnvironment();
	const bootstrap = await runCommand([executable, "bootstrap", "--confirm", "--state-dir", stateDirectory, "--profile", profilePath], environment);
	expect(bootstrap.exitCode).toBe(0);
	expect(bootstrap.stdout).toContain('"state":"committed"');

	const fixture = new DiscordFixture();
	let service: RunningService | undefined;
	let restarted: RunningService | undefined;
	try {
		const daemon = await startDaemon(executable, stateDirectory, profilePath, environment);
		const adapter = await startDiscordAdapter(
			{
				rpcSocketPath: path.join(stateDirectory, "rpc.sock"),
				token: "fixture-token",
				route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
				ackBudgetMs: 2_000,
				claimTtlMs: 5_000,
				readWaitMs: 0,
			},
			{ platformFactory: () => fixture, onError: () => undefined },
		);
		service = { daemon, adapter };
		expect((await daemon.client.request("way.health")).result).toMatchObject({ status: "healthy", state: "running" });

		// This is the portable equivalent of systemd Restart=on-failure plus the
		// adapter's BindsTo= relationship: the daemon dies without cleanup, then
		// the supervisor recreates the daemon and its adapter.
		service.daemon.child.kill("SIGKILL");
		await service.daemon.child.exited;
		service.daemon.client.close();
		await stopAdapter(service.adapter);
		service = undefined;

		const restartedDaemon = await startDaemon(executable, stateDirectory, profilePath, environment);
		const restartedAdapter = await startDiscordAdapter(
			{
				rpcSocketPath: path.join(stateDirectory, "rpc.sock"),
				token: "fixture-token",
				route: { channelId: "123456789012345678", surfaceId: "discord:owner-dm" },
				ackBudgetMs: 2_000,
				claimTtlMs: 5_000,
				readWaitMs: 0,
			},
			{ platformFactory: () => fixture, onError: () => undefined },
		);
		restarted = { daemon: restartedDaemon, adapter: restartedAdapter };
		expect((await restartedDaemon.client.request("way.health")).result).toMatchObject({ status: "healthy", state: "running" });

		const submitted = await restartedDaemon.client.request("main.submit", {
			text: "restart fixture delivery",
			surface_id: "discord:owner-dm",
			idempotency_key: "systemd-restart-delivery",
		});
		expect(submitted.result).toMatchObject({ accepted: true, delivered_as: "prompt" });
		const delivery = await eventually(
			() => fixture.sends.find(send => send.text === "fixture reply: restart fixture delivery"),
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
		expect(restartPrevented(parseUnit("ops/systemd/gajae-way.service"), failClosed.exitCode)).toBe(true);
	} finally {
		await stopAdapter(restarted?.adapter);
		await stopDaemon(restarted?.daemon);
		await stopAdapter(service?.adapter);
		await stopDaemon(service?.daemon);
	}
}, 30_000);
