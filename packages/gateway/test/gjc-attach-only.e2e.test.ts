import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { access, mkdir, mkdtemp, readFile, realpath, rm, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import { gatewayHome } from "../src/config";
import {
	brokerHealthArgs,
	defaultPidAlive,
	GJC_ATTACH_ONLY_ENV,
	GjcCliUnavailableError,
	GlobalGjcClient,
	isHealthySessionList,
	readBrokerDiscovery,
	readPinnedGjcVersion,
	type SpawnFn,
	streamRelayArgs,
} from "../src/orchestrator/broker";
import { BrokerSessionPort } from "../src/orchestrator/session-port";
import { TailRunner } from "../src/orchestrator/tail-runner";
import { Store } from "../src/store";
import { initializeTestBrokerAuthority } from "./session-port.fake";

const LIVE = process.env.GAJAEWAY_E2E_GJC === "1";
const PINNED_VERSION = readPinnedGjcVersion();
// GJC 0.18.9 is the first release with attach-only clients (gajae-code #6557)
// and `gjc sdk broker run` (#6560); 0.18.8 ignores the environment flag and
// remains the negative-control version.
const U1_ATTACH_ONLY_VERSION = "0.18.9";
const REPO_ROOT = resolve(import.meta.dir, "../../..");
const GATEWAY_MAIN = join(REPO_ROOT, "packages/gateway/src/main.ts");
const pinTest = LIVE ? test : test.skip;

interface GjcExecutable {
	readonly path: string;
	readonly version: string;
}

const GJC = LIVE ? await resolveGjcExecutable() : undefined;
const HAS_ATTACH_ONLY =
	GJC !== undefined &&
	versionAtLeast(GJC.version, U1_ATTACH_ONLY_VERSION) &&
	versionAtLeast(PINNED_VERSION, U1_ATTACH_ONLY_VERSION);
const attachOnlySkipReason = LIVE
	? `requires U1 release (GJC >= ${U1_ATTACH_ONLY_VERSION}; executable=${GJC?.version ?? "unresolved"}, package pin=${PINNED_VERSION})`
	: `requires GAJAEWAY_E2E_GJC=1 and U1 release (GJC >= ${U1_ATTACH_ONLY_VERSION})`;
const attachOnlyTest = HAS_ATTACH_ONLY ? test : test.skip;
const controlEnabled = LIVE && (GJC?.version === "0.18.8" || process.env.GAJAEWAY_E2E_GJC_CONTROL === "1");
const controlTest = controlEnabled ? test : test.skip;
const controlSkipReason = LIVE
	? "manual negative control; set GAJAEWAY_E2E_GJC_CONTROL=1"
	: "requires GAJAEWAY_E2E_GJC=1";
const sessionSource = process.env.GJC_CODING_AGENT_DIR;
const sessionModel = process.env.GAJAEWAY_E2E_MODEL;
const sessionFlowEnabled =
	LIVE &&
	sessionSource !== undefined &&
	isAbsolute(sessionSource) &&
	sessionModel !== undefined &&
	sessionModel.trim().length > 0;
const sessionFlowSkipReason = !LIVE
	? "requires GAJAEWAY_E2E_GJC=1, an absolute GJC_CODING_AGENT_DIR, and GAJAEWAY_E2E_MODEL"
	: "requires an absolute GJC_CODING_AGENT_DIR with provider credentials and GAJAEWAY_E2E_MODEL";
const sessionFlowTest = sessionFlowEnabled ? test : test.skip;

interface CaptureResult {
	readonly exitCode: number;
	readonly stdout: string;
	readonly stderr: string;
}

interface ProcessRecord {
	readonly pid: number;
	readonly ppid: number;
	readonly command: string;
}

interface DiscoveryRecord {
	readonly pid: number;
}

interface HostProcess {
	readonly pid: number;
	readonly child: ReturnType<typeof Bun.spawn>;
	readonly stdout: Promise<string>;
	readonly stderr: Promise<string>;
}

interface Fixture {
	readonly root: string;
	readonly home: string;
	readonly source: string;
	readonly sourceIsIsolated: boolean;
	readonly workspace: string;
	readonly agentDir: string;
	readonly clients: GlobalGjcClient[];
	readonly hosts: HostProcess[];
	readonly brokerPids: Set<number>;
}

interface ClientHarness {
	readonly client: GlobalGjcClient;
	readonly commands: string[][];
	armBarrier(barrier: SpawnBarrier | undefined): void;
}

interface SpawnRequest {
	readonly cmd: readonly string[];
	readonly cwd?: string;
	readonly env?: Record<string, string>;
	readonly stdin?: unknown;
	readonly stdout?: unknown;
	readonly stderr?: unknown;
	readonly [key: string]: unknown;
}

interface SpawnBarrier {
	readonly entered: Promise<{ readonly cmd: readonly string[]; readonly env: Readonly<Record<string, string>> }>;
	readonly released: Promise<void>;
	isReleased(): boolean;
	release(): void;
	capture(request: SpawnRequest): void;
}

async function resolveGjcExecutable(): Promise<GjcExecutable> {
	const explicit = process.env.GJC_EXECUTABLE;
	const layout = homeLayout(process.env.GAJAEWAY_HOME ?? gatewayHome());
	const candidates = explicit
		? [explicit]
		: [...new Set([layout.gjcExecutable(), layout.gjcExecutable(PINNED_VERSION)])];
	for (const candidate of candidates) {
		if (!isAbsolute(candidate)) throw new Error("GJC_EXECUTABLE must be an absolute path");
		try {
			await access(candidate, constants.X_OK);
			const path = await realpath(candidate);
			const result = await captureCommand([path, "--version"], { env: isolatedEnvironment(tmpdir()) }, 10_000);
			const version = (result.stdout || result.stderr).match(/(?:gjc\/)?(\d+\.\d+\.\d+)/)?.[1];
			if (result.exitCode !== 0 || !version) throw new Error(`cannot resolve gjc version from ${candidate}`);
			return { path, version };
		} catch (error) {
			if (explicit) throw new Error(`GJC_EXECUTABLE is unavailable: ${candidate}`);
			if ((error as NodeJS.ErrnoException).code !== "ENOENT" && (error as NodeJS.ErrnoException).code !== "EACCES")
				throw error;
		}
	}
	throw new Error(
		`GAJAEWAY_E2E_GJC=1 requires GJC_EXECUTABLE or the private pinned gjc under ${layout.gjcPrefix}; tried ${candidates.join(", ")}`,
	);
}

function versionAtLeast(actual: string, minimum: string): boolean {
	const parts = (value: string) => value.split(".").map(Number);
	const actualParts = parts(actual);
	const minimumParts = parts(minimum);
	for (let index = 0; index < 3; index++) {
		if (actualParts[index]! > minimumParts[index]!) return true;
		if (actualParts[index]! < minimumParts[index]!) return false;
	}
	return true;
}

function isolatedEnvironment(home: string, extra: Record<string, string> = {}): Record<string, string> {
	return {
		PATH: process.env.PATH ?? "/usr/bin:/bin",
		HOME: home,
		USERPROFILE: home,
		LANG: "C",
		TMPDIR: home,
		...extra,
	};
}

async function captureCommand(
	command: string[],
	options: { readonly cwd?: string; readonly env?: Record<string, string> } = {},
	timeoutMs = 60_000,
): Promise<CaptureResult> {
	const child = Bun.spawn(command, {
		cwd: options.cwd ?? REPO_ROOT,
		env: options.env,
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
	});
	const output = Promise.all([
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		child.exited,
	] as const);
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		const result = await Promise.race([
			output,
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error(`command timed out after ${timeoutMs}ms`)), timeoutMs);
			}),
		]);
		const [stdout, stderr, exitCode] = result;
		return { exitCode, stdout, stderr };
	} catch (error) {
		try {
			child.kill("SIGKILL");
		} catch {
			// The command may have exited at the timeout boundary.
		}
		await child.exited;
		throw error;
	} finally {
		if (timer) clearTimeout(timer);
	}
}

async function createFixture(sourcePath?: string): Promise<Fixture> {
	const root = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-gjc-attach-only-")));
	const home = join(root, "home");
	const sourceIsIsolated = sourcePath === undefined;
	const source = sourcePath === undefined ? join(root, "source-agent") : resolve(sourcePath);
	const fixture: Fixture = {
		root,
		home,
		source,
		sourceIsIsolated,
		workspace: join(root, "workspace"),
		agentDir: homeLayout(home).brokerAgentDir,
		clients: [],
		hosts: [],
		brokerPids: new Set(),
	};
	try {
		if (!GJC) throw new Error("live GJC executable was not resolved");
		if (GJC.version !== PINNED_VERSION)
			throw new Error(`live e2e requires the gateway pin: executable=${GJC.version}, package pin=${PINNED_VERSION}`);
		await mkdir(home, { recursive: true, mode: 0o700 });
		await mkdir(fixture.workspace, { recursive: true, mode: 0o700 });
		await installPrivatePinnedGjc(fixture);
		if (sourcePath === undefined) {
			await mkdir(source, { recursive: true, mode: 0o700 });
			await initializeAgentWithGjc(fixture);
		} else {
			await realpath(source);
		}
		await runProvisionVerb(fixture);
		return fixture;
	} catch (error) {
		await cleanupFixture(fixture);
		throw error;
	}
}

async function installPrivatePinnedGjc(fixture: Fixture): Promise<void> {
	if (!GJC) throw new Error("live GJC executable was not resolved");
	const layout = homeLayout(fixture.home);
	const versioned = layout.gjcExecutable(PINNED_VERSION);
	await mkdir(dirname(versioned), { recursive: true, mode: 0o700 });
	await symlink(GJC.path, versioned);
	await symlink(join(layout.gjcPrefix, PINNED_VERSION), layout.gjcCurrent);
}

async function initializeAgentWithGjc(fixture: Fixture): Promise<void> {
	if (!GJC) throw new Error("live GJC executable was not resolved");
	const sourceHome = join(fixture.root, "source-home");
	await mkdir(sourceHome, { recursive: true, mode: 0o700 });
	const result = await captureCommand(
		[GJC.path, "sdk", "session", "list", "--scope", "all", "--json", "--agent-dir", fixture.source],
		{
			cwd: fixture.root,
			env: isolatedEnvironment(sourceHome, { GJC_CODING_AGENT_DIR: fixture.source }),
		},
	);
	if (result.exitCode !== 0) throw new Error(`gjc could not initialize isolated agent.db (exit=${result.exitCode})`);
	const initializedBroker = await readDiscoveryRecord(fixture.source);
	if (initializedBroker) {
		fixture.brokerPids.add(initializedBroker.pid);
		await killPid(initializedBroker.pid);
	}
}

async function runProvisionVerb(fixture: Fixture): Promise<void> {
	const result = await captureCommand(
		[process.execPath, GATEWAY_MAIN, "broker-agent", "provision", "--mode", "force", "--from", fixture.source],
		{
			cwd: REPO_ROOT,
			env: isolatedEnvironment(fixture.home, { GAJAEWAY_HOME: fixture.home }),
		},
	);
	if (result.exitCode !== 0)
		throw new Error(
			`real broker-agent provision verb failed (exit=${result.exitCode}): ${`${result.stdout}\n${result.stderr}`.trim().slice(-800)}`,
		);
	const manifest = JSON.parse(await readFile(homeLayout(fixture.home).brokerProvisionManifest, "utf8")) as {
		version?: unknown;
		agentDir?: unknown;
		genId?: unknown;
	};
	expect(manifest.version).toBe(2);
	expect(manifest.agentDir).toBe(await realpath(fixture.agentDir));
	expect(typeof manifest.genId).toBe("string");
	expect(await Bun.file(join(fixture.agentDir, "agent.db")).exists()).toBe(true);
}

async function readDiscoveryRecord(agentDir: string): Promise<DiscoveryRecord | undefined> {
	try {
		const record = JSON.parse(await readFile(join(agentDir, "sdk", "broker.json"), "utf8")) as { pid?: unknown };
		return typeof record.pid === "number" && Number.isSafeInteger(record.pid) && record.pid > 1
			? { pid: record.pid }
			: undefined;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		return undefined;
	}
}

function createSpawnBarrier(): SpawnBarrier {
	let resolveEntered!: (request: {
		readonly cmd: readonly string[];
		readonly env: Readonly<Record<string, string>>;
	}) => void;
	let resolveRelease!: () => void;
	let released = false;
	let captured = false;
	const entered = new Promise<{ readonly cmd: readonly string[]; readonly env: Readonly<Record<string, string>> }>(
		(resolvePromise) => {
			resolveEntered = resolvePromise;
		},
	);
	const releasedPromise = new Promise<void>((resolvePromise) => {
		resolveRelease = resolvePromise;
	});
	return {
		entered,
		released: releasedPromise,
		isReleased: () => released,
		release() {
			released = true;
			resolveRelease();
		},
		capture(request) {
			if (captured) return;
			captured = true;
			resolveEntered({ cmd: [...request.cmd], env: { ...(request.env ?? {}) } });
		},
	};
}

function makeClient(
	fixture: Fixture,
	options: { readonly stripAttachOnly?: boolean; readonly pinnedVersion?: string } = {},
): ClientHarness {
	if (!GJC) throw new Error("live GJC executable was not resolved");
	let barrier: SpawnBarrier | undefined;
	const commands: string[][] = [];
	const spawn = ((rawOptions: SpawnRequest) => {
		const request = { ...rawOptions, cmd: [...rawOptions.cmd], env: { ...(rawOptions.env ?? {}) } };
		commands.push([...request.cmd]);
		const realSpawn = () => spawnReal(request, options.stripAttachOnly === true);
		if (barrier && !barrier.isReleased() && request.cmd.includes("session") && request.cmd.includes("list")) {
			barrier.capture({ ...request, env: environmentForSpawn(request.env, options.stripAttachOnly === true) });
			return spawnBehindBarrier(request, barrier, () => realSpawn());
		}
		return realSpawn();
	}) as unknown as SpawnFn;

	const restore = isolateClientConstructorEnvironment(fixture.home);
	let client: GlobalGjcClient;
	try {
		client = new GlobalGjcClient({
			executable: GJC.path,
			agentDir: fixture.agentDir,
			cwd: fixture.home,
			pinnedVersion: options.pinnedVersion ?? PINNED_VERSION,
			spawn,
			healthIntervalMs: 60_000,
			readinessAttempts: 30,
			readinessDelayMs: 100,
		});
	} finally {
		restore();
	}
	fixture.clients.push(client!);
	return {
		client: client!,
		commands,
		armBarrier(next) {
			barrier = next;
		},
	};
}

function isolateClientConstructorEnvironment(home: string): () => void {
	const original = { ...process.env };
	for (const key of Object.keys(process.env)) delete process.env[key];
	process.env.PATH = original.PATH ?? "/usr/bin:/bin";
	process.env.HOME = home;
	process.env.USERPROFILE = home;
	process.env.TMPDIR = home;
	process.env.LANG = "C";
	return () => {
		for (const key of Object.keys(process.env)) delete process.env[key];
		for (const [key, value] of Object.entries(original)) if (value !== undefined) process.env[key] = value;
	};
}

function environmentForSpawn(
	environment: Record<string, string> | undefined,
	stripAttachOnly: boolean,
): Record<string, string> {
	const env = { ...(environment ?? {}) };
	if (stripAttachOnly) delete env[GJC_ATTACH_ONLY_ENV];
	return env;
}

function spawnReal(request: SpawnRequest, stripAttachOnly: boolean): ReturnType<SpawnFn> {
	return Bun.spawn({
		...request,
		cmd: [...request.cmd],
		env: environmentForSpawn(request.env, stripAttachOnly),
	} as never) as ReturnType<SpawnFn>;
}

function spawnBehindBarrier(
	request: SpawnRequest,
	barrier: SpawnBarrier,
	spawn: () => ReturnType<SpawnFn>,
): ReturnType<SpawnFn> {
	let stdoutController!: ReadableStreamDefaultController<Uint8Array>;
	let stderrController!: ReadableStreamDefaultController<Uint8Array>;
	const stdout = new ReadableStream<Uint8Array>({
		start(controller) {
			stdoutController = controller;
		},
	});
	const stderr = new ReadableStream<Uint8Array>({
		start(controller) {
			stderrController = controller;
		},
	});
	let realChild: ReturnType<SpawnFn> | undefined;
	let killed = false;
	let resolveExited!: (code: number) => void;
	let rejectExited!: (error: unknown) => void;
	const exited = new Promise<number>((resolvePromise, rejectPromise) => {
		resolveExited = resolvePromise;
		rejectExited = rejectPromise;
	});
	void barrier.released.then(() => {
		if (killed) {
			stdoutController.close();
			stderrController.close();
			resolveExited(128);
			return;
		}
		try {
			realChild = spawn();
			const streams = realChild as unknown as { stdout: unknown; stderr: unknown };
			void copyReadable(streams.stdout, stdoutController);
			void copyReadable(streams.stderr, stderrController);
			realChild.exited.then(resolveExited, rejectExited);
		} catch (error) {
			stdoutController.error(error);
			stderrController.error(error);
			rejectExited(error);
		}
	});
	const proxy = {
		pid: -1,
		stdin: request.stdin === "pipe" ? null : 0,
		stdout,
		stderr,
		exited,
		kill() {
			killed = true;
			try {
				realChild?.kill("SIGKILL");
			} catch {
				// A simultaneous exit is confirmed by the exited promise.
			}
		},
	};
	return proxy as unknown as ReturnType<SpawnFn>;
}

async function copyReadable(stream: unknown, controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> {
	if (!(stream instanceof ReadableStream)) {
		controller.close();
		return;
	}
	const reader = stream.getReader();
	try {
		for (;;) {
			const { value, done } = await reader.read();
			if (done) break;
			controller.enqueue(value);
		}
	} catch (error) {
		controller.error(error);
	} finally {
		reader.releaseLock();
		try {
			controller.close();
		} catch {
			// The consumer may already have cancelled the stream.
		}
	}
}

async function startBrokerHost(fixture: Fixture): Promise<HostProcess> {
	const child = Bun.spawn([process.execPath, GATEWAY_MAIN, "broker"], {
		cwd: REPO_ROOT,
		env: isolatedEnvironment(fixture.home, { GAJAEWAY_HOME: fixture.home }),
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
	});
	const host: HostProcess = {
		pid: child.pid,
		child,
		stdout: new Response(child.stdout).text(),
		stderr: new Response(child.stderr).text(),
	};
	fixture.hosts.push(host);
	return host;
}

async function waitForLiveDiscovery(fixture: Fixture, host: HostProcess): Promise<{ readonly pid: number }> {
	const deadline = Date.now() + 30_000;
	while (Date.now() < deadline) {
		const discovery = await readBrokerDiscovery(join(fixture.agentDir, "sdk", "broker.json"), defaultPidAlive);
		if (discovery) {
			fixture.brokerPids.add(discovery.pid);
			return { pid: discovery.pid };
		}
		const exited = await Promise.race([
			host.child.exited.then((exitCode) => ({ exitCode })),
			Bun.sleep(100).then(() => undefined),
		]);
		if (exited) {
			const output = `${await host.stdout}\n${await host.stderr}`.slice(0, 1_000);
			throw new Error(`broker host exited before discovery became live (exit=${exited.exitCode}): ${output}`);
		}
	}
	throw new Error("timed out waiting for a live broker discovery record");
}

async function processSnapshot(): Promise<ProcessRecord[]> {
	const args = process.platform === "darwin" ? ["-axo", "pid=,ppid=,command="] : ["-eo", "pid=,ppid=,args="];
	const result = await captureCommand(["ps", ...args], { env: isolatedEnvironment(tmpdir()) }, 5_000);
	if (result.exitCode !== 0) throw new Error("could not read process ancestry with ps");
	return result.stdout
		.split("\n")
		.map((line) => {
			const match = line.match(/^\s*(\d+)\s+(\d+)\s+(.*)$/);
			return match ? { pid: Number(match[1]), ppid: Number(match[2]), command: match[3]!.trim() } : undefined;
		})
		.filter((record): record is ProcessRecord => record !== undefined);
}

function isBrokerCommand(command: string): boolean {
	return /\bsdk\s+broker-internal\b/.test(command) || /\bsdk\s+broker\s+run\b/.test(command);
}

function isSessionHostCommand(command: string): boolean {
	return /\bsdk\s+session-host-internal\b/.test(command);
}

function ancestors(snapshot: readonly ProcessRecord[], pid: number): number[] {
	const byPid = new Map(snapshot.map((record) => [record.pid, record]));
	const result: number[] = [];
	const seen = new Set<number>();
	let current = byPid.get(pid);
	while (current && current.ppid > 1 && !seen.has(current.ppid)) {
		result.push(current.ppid);
		seen.add(current.ppid);
		current = byPid.get(current.ppid);
	}
	return result;
}

function brokerProcesses(snapshot: readonly ProcessRecord[], agentDir: string): ProcessRecord[] {
	return snapshot.filter((record) => isBrokerCommand(record.command) && record.command.includes(agentDir));
}

async function assertBrokerOwnedByHost(fixture: Fixture, host: HostProcess, discoveryPid: number): Promise<void> {
	const snapshot = await processSnapshot();
	const brokers = brokerProcesses(snapshot, fixture.agentDir);
	expect(brokers.map(({ pid }) => pid)).toEqual([discoveryPid]);
	const owner = snapshot.find(({ pid }) => pid === discoveryPid);
	expect(owner?.ppid).toBe(host.pid);
	expect(ancestors(snapshot, discoveryPid)).toContain(host.pid);
}

async function assertNoUnownedBrokerForTenSeconds(fixture: Fixture, hostPid: number): Promise<void> {
	const deadline = Date.now() + 10_000;
	while (Date.now() < deadline) {
		const snapshot = await processSnapshot();
		const offenders = brokerProcesses(snapshot, fixture.agentDir).filter(
			(record) => !ancestors(snapshot, record.pid).includes(hostPid),
		);
		expect(offenders).toEqual([]);
		await Bun.sleep(100);
	}
}

async function killPid(pid: number): Promise<void> {
	try {
		process.kill(pid, "SIGKILL");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
	}
	await waitForPidDeath(pid, 10_000);
}

async function waitForPidDeath(pid: number, timeoutMs: number): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (!defaultPidAlive(pid)) return;
		await Bun.sleep(50);
	}
	throw new Error(`pid ${pid} did not exit after SIGKILL`);
}

async function killHostAndBroker(fixture: Fixture, host: HostProcess, brokerPid: number): Promise<void> {
	fixture.brokerPids.add(brokerPid);
	await killPid(brokerPid);
	try {
		host.child.kill("SIGKILL");
	} catch {
		// The host may already have exited; its exit promise is authoritative.
	}
	await host.child.exited;
	await waitForPidDeath(host.pid, 10_000);
	const stale = await readDiscoveryRecord(fixture.agentDir);
	expect(stale?.pid).toBe(brokerPid);
}

async function assertRestartRecovery(
	fixture: Fixture,
	client: GlobalGjcClient,
	oldPid: number,
	probes: readonly { readonly name: string; readonly args: string[] }[] = [],
): Promise<void> {
	const host = await startBrokerHost(fixture);
	const discovery = await waitForLiveDiscovery(fixture, host);
	expect(discovery.pid).not.toBe(oldPid);
	await assertBrokerOwnedByHost(fixture, host, discovery.pid);
	await client.preflight();
	if (probes.length === 0) {
		const result = await client.cli(brokerHealthArgs());
		expect(isHealthySessionList(result)).toBe(true);
		return;
	}
	for (const probe of probes) {
		const result = await client.cli(probe.args);
		if (probe.name === "broker health/session list") expect(isHealthySessionList(result)).toBe(true);
		if (probe.name !== "session.create") continue;
		const envelope = JSON.parse(result.stdout) as { ok?: unknown; result?: { sessionId?: unknown } };
		expect(envelope).toMatchObject({ ok: true });
		expect(typeof envelope.result?.sessionId).toBe("string");
		const closed = await client.cli([
			"sdk",
			"session",
			"raw",
			"global",
			"--op",
			"session.close",
			"--idempotency-key",
			`gajaeway-e2e-cleanup-${randomUUID()}`,
			"--json-input",
			JSON.stringify({ sessionId: envelope.result!.sessionId }),
		]);
		expect(JSON.parse(closed.stdout).ok).toBe(true);
	}
}

function attachOnlyProbes(fixture: Fixture): Array<{ readonly name: string; readonly args: string[] }> {
	const missingSession = randomUUID();
	const createRef = `gajaeway-e2e-create-${randomUUID()}`;
	const closeRef = `gajaeway-e2e-close-${randomUUID()}`;
	return [
		{ name: "broker health/session list", args: [...brokerHealthArgs()] },
		{ name: "session inspect", args: ["sdk", "session", "inspect", missingSession] },
		{
			name: "session.create",
			args: [
				"sdk",
				"session",
				"raw",
				"global",
				"--op",
				"session.create",
				"--idempotency-key",
				createRef,
				"--json-input",
				JSON.stringify({ cwd: fixture.workspace }),
			],
		},
		{
			name: "turn.prompt",
			args: [
				"sdk",
				"session",
				"raw",
				"control",
				missingSession,
				"--op",
				"turn.prompt",
				"--json-input",
				JSON.stringify({ text: "attach-only e2e probe", clientRef: `gajaeway-e2e-${randomUUID()}` }),
			],
		},
		{
			name: "turn.result",
			args: [
				"sdk",
				"session",
				"raw",
				"query",
				missingSession,
				"--query",
				"turn.result",
				"--json-input",
				'{"kind":"prompt","clientRef":"gajaeway-e2e-missing"}',
			],
		},
		{
			name: "session.close",
			args: [
				"sdk",
				"session",
				"raw",
				"global",
				"--op",
				"session.close",
				"--idempotency-key",
				closeRef,
				"--json-input",
				JSON.stringify({ sessionId: missingSession }),
			],
		},
		{
			name: "runtime.jobs.list",
			args: ["sdk", "session", "raw", "query", missingSession, "--query", "runtime.jobs.list", "--json-input", "{}"],
		},
		{
			name: "compaction.run",
			args: ["sdk", "session", "raw", "control", missingSession, "--op", "compaction.run", "--json-input", "{}"],
		},
		{ name: "serve relay", args: [...streamRelayArgs(missingSession)] },
	];
}

async function expectUnavailable(client: GlobalGjcClient, args: string[], name: string): Promise<void> {
	try {
		await client.cli(args);
	} catch (error) {
		expect(error).toBeInstanceOf(GjcCliUnavailableError);
		return;
	}
	throw new Error(`${name} did not return broker_unavailable`);
}

async function cleanupFixture(fixture: Fixture): Promise<void> {
	for (const client of fixture.clients.splice(0).reverse()) {
		try {
			await client.stop();
		} catch {
			// The process sweep below is the final ownership boundary for this temp home.
		}
	}
	for (const host of fixture.hosts.splice(0).reverse()) {
		try {
			host.child.kill("SIGTERM");
		} catch {
			// The host may already have exited.
		}
		await Promise.race([host.child.exited, Bun.sleep(2_000)]);
		try {
			host.child.kill("SIGKILL");
		} catch {
			// The graceful stop already reaped it.
		}
		await host.child.exited;
	}
	const ownedAgentDirs = [fixture.agentDir, ...(fixture.sourceIsIsolated ? [fixture.source] : [])];
	for (let pass = 0; pass < 3; pass++) {
		const snapshot = await processSnapshot().catch(() => []);
		const ownedProcesses = snapshot.filter(
			(record) =>
				ownedAgentDirs.some((agentDir) => record.command.includes(agentDir)) &&
				(isBrokerCommand(record.command) || isSessionHostCommand(record.command)),
		);
		for (const record of ownedProcesses) {
			try {
				process.kill(record.pid, "SIGKILL");
			} catch {
				// The process may exit between ps and signal delivery.
			}
		}
		for (const pid of fixture.brokerPids) {
			try {
				process.kill(pid, "SIGKILL");
			} catch {
				// The process may already be gone.
			}
		}
		await Bun.sleep(100);
	}
	await rm(fixture.root, { recursive: true, force: true });
}

async function withFixture(sourcePath: string | undefined, run: (fixture: Fixture) => Promise<void>): Promise<void> {
	const fixture = await createFixture(sourcePath);
	try {
		await run(fixture);
	} finally {
		await cleanupFixture(fixture);
	}
}

pinTest(
	"the real broker-agent provision verb provisions an isolated home and GJC version mismatch is refused",
	async () => {
		await withFixture(undefined, async (fixture) => {
			const harness = makeClient(fixture, { pinnedVersion: "0.0.0" });
			const mismatch = await harness.client.preflight().catch((error: unknown) => error);
			expect(mismatch).toBeInstanceOf(Error);
			expect((mismatch as Error).message).toContain("version mismatch");
			expect(harness.commands.map((command) => command.slice(1))).toEqual([["--version"]]);
		});
	},
	120_000,
);

const attachOnlyTestTitleA = HAS_ATTACH_ONLY
	? "Case A refuses all gateway GJC command paths after host stop"
	: `Case A refuses all gateway GJC command paths after host stop [skipped: ${attachOnlySkipReason}]`;

attachOnlyTest(
	attachOnlyTestTitleA,
	async () => {
		await withFixture(undefined, async (fixture) => {
			const harness = makeClient(fixture);
			const host = await startBrokerHost(fixture);
			const original = await waitForLiveDiscovery(fixture, host);
			await harness.client.preflight();
			await harness.client.start();
			await killHostAndBroker(fixture, host, original.pid);
			const probes = attachOnlyProbes(fixture);
			for (const probe of probes) await expectUnavailable(harness.client, probe.args, probe.name);
			await expect(harness.client.preflight()).rejects.toBeInstanceOf(GjcCliUnavailableError);
			await assertNoUnownedBrokerForTenSeconds(fixture, host.pid);
			await assertRestartRecovery(fixture, harness.client, original.pid, probes);
		});
	},
	180_000,
);

const attachOnlyTestTitleB = HAS_ATTACH_ONLY
	? "Case B refuses a held command after live-check/dead-execution"
	: `Case B refuses a held command after live-check/dead-execution [skipped: ${attachOnlySkipReason}]`;

attachOnlyTest(
	attachOnlyTestTitleB,
	async () => {
		await withFixture(undefined, async (fixture) => {
			const harness = makeClient(fixture);
			const host = await startBrokerHost(fixture);
			const original = await waitForLiveDiscovery(fixture, host);
			await harness.client.preflight();
			await harness.client.start();
			expect(await harness.client.judgeLiveness()).toMatchObject({ state: "live", pid: original.pid });

			const probes = attachOnlyProbes(fixture);
			const barrier = createSpawnBarrier();
			harness.armBarrier(barrier);
			const command = harness.client.cli(probes[0]!.args);
			try {
				const held = await barrier.entered;
				expect(held.cmd.slice(1, 4)).toEqual(["sdk", "session", "list"]);
				expect(held.env[GJC_ATTACH_ONLY_ENV]).toBe("0");
				await killHostAndBroker(fixture, host, original.pid);
				barrier.release();
				await expect(command).rejects.toBeInstanceOf(GjcCliUnavailableError);
			} finally {
				barrier.release();
				harness.armBarrier(undefined);
			}
			for (const probe of probes.slice(1)) await expectUnavailable(harness.client, probe.args, probe.name);
			await expect(harness.client.preflight()).rejects.toBeInstanceOf(GjcCliUnavailableError);
			await assertNoUnownedBrokerForTenSeconds(fixture, host.pid);
			await assertRestartRecovery(fixture, harness.client, original.pid, probes);
		});
	},
	180_000,
);

const controlTitle = controlEnabled
	? "negative control reproduces an autostarted broker with the attach-only env removed"
	: `negative control reproduces an autostarted broker [skipped: ${controlSkipReason}]`;
controlTest(
	controlTitle,

	async () => {
		await withFixture(undefined, async (fixture) => {
			const harness = makeClient(fixture, { stripAttachOnly: true });
			const host = await startBrokerHost(fixture);
			const original = await waitForLiveDiscovery(fixture, host);
			await harness.client.preflight();
			await harness.client.start();
			expect(await harness.client.judgeLiveness()).toMatchObject({ state: "live", pid: original.pid });

			const barrier = createSpawnBarrier();
			harness.armBarrier(barrier);
			const command = harness.client.cli(brokerHealthArgs());
			try {
				const held = await barrier.entered;
				expect(held.env[GJC_ATTACH_ONLY_ENV]).toBeUndefined();
				await killHostAndBroker(fixture, host, original.pid);
				barrier.release();
				const result = await command;
				expect(result.exitCode).toBe(0);
				const orphan = await waitForUnownedBroker(fixture, host.pid, 30_000);
				const discovery = await readDiscoveryRecord(fixture.agentDir);
				expect(discovery?.pid).toBe(orphan.pid);
				const snapshot = await processSnapshot();
				const orphanAncestors = ancestors(snapshot, orphan.pid);
				expect(orphanAncestors).not.toContain(host.pid);
				console.info(
					`negative_control_spawn_detected pid=${orphan.pid} ppid=${orphan.ppid} command=${orphan.command} ancestry=${orphanAncestors.join(",")}`,
				);
				await killPid(orphan.pid);
			} finally {
				barrier.release();
				harness.armBarrier(undefined);
			}
			await assertRestartRecovery(fixture, harness.client, original.pid);
		});
	},
	180_000,
);

sessionFlowTest(
	`real broker session create/prompt/result flow (${sessionFlowSkipReason})`,
	async () => {
		if (!sessionSource || !sessionModel) throw new Error("GJC_CODING_AGENT_DIR and GAJAEWAY_E2E_MODEL are required");

		await withFixture(sessionSource, async (fixture) => {
			const host = await startBrokerHost(fixture);
			await waitForLiveDiscovery(fixture, host);
			const client = makeClient(fixture).client;
			await client.preflight();
			await client.start();
			const database = await Store.open(join(fixture.home, "gateway.db"));
			try {
				const authority = initializeTestBrokerAuthority(database, fixture.agentDir);
				const tailRunner = new TailRunner({
					stream: (sessionId) => client.openStream(sessionId),
					repo: fixture.workspace,
					stallTimeoutMs: 180_000,
				});
				const port = new BrokerSessionPort({
					authority,
					database,
					cli: client.cli,
					tailRunner,
				});
				const originKey = `work/task/gjc-e2e-${randomUUID()}`;
				const binding = await port.bind({ originKey, epoch: 0, repo: fixture.workspace, model: sessionModel });
				const result = await port.request({
					sessionId: binding.sessionId,
					repo: fixture.workspace,
					originKey,
					opRef: `gjc-e2e-${randomUUID()}`,
					text: "Reply with exactly: gajaeway-gjc-e2e-ok",
					waitTimeoutMs: 180_000,
					pollMs: 500,
				});
				expect(result.status.status.status).toBe("terminal_ok");
				expect(result.assistant.text.toLowerCase()).toContain("gajaeway-gjc-e2e-ok");
				await port.close({ sessionId: binding.sessionId, repo: fixture.workspace });
			} finally {
				database.close();
			}
		});
	},
	300_000,
);

async function waitForUnownedBroker(fixture: Fixture, hostPid: number, timeoutMs: number): Promise<ProcessRecord> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const snapshot = await processSnapshot();
		const orphan = brokerProcesses(snapshot, fixture.agentDir).find(
			(record) => !ancestors(snapshot, record.pid).includes(hostPid),
		);
		if (orphan) return orphan;
		await Bun.sleep(100);
	}
	throw new Error("negative control did not produce a broker outside the host process ancestry");
}
