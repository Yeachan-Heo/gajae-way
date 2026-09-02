import { afterEach, expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import type { GatewayConfig } from "../src/config";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { bootGateway } from "../src/boot";
import { BrokerSupervisor, MIN_GJC_VERSION } from "../src/orchestrator/broker";
import { sessionPortFromResponder } from "./session-port.fake";

const directories: string[] = [];

afterEach(async () => {
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

async function temporaryHome(prefix: string): Promise<string> {
	const home = await mkdtemp(join(tmpdir(), prefix));
	directories.push(home);
	return home;
}

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 100; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

const HEALTHY = { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" };
const GENERIC_HELP = { exitCode: 0, stdout: "USAGE\n  $ gjc sdk [ACTION] [FLAGS]\n", stderr: "" };

test("observes the gjc-owned agent-dir daemon, never spawns a host, and fences recovery with generation", async () => {
	const home = await temporaryHome("gajaeway-broker-supervisor-");
	const generations: number[] = [];
	const cliCalls: string[][] = [];
	let spawned = 0;
	let probeResult = true;
	let healthChecks = 0;
	const logs: string[] = [];
	const command: CliRunner = async (args) => {
		cliCalls.push([...args]);
		return HEALTHY;
	};
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-1",
		spawn: (() => {
			spawned++;
			throw new Error("the gateway must not spawn a session host");
		}) as unknown as typeof Bun.spawn,
		command,
		healthProbe: async () => {
			healthChecks++;
			return probeResult;
		},
		healthIntervalMs: 5,
		restartBackoff: { initialMs: 1, maxMs: 2 },
		log: (line) => logs.push(line),
	});
	broker.onGeneration((generation) => generations.push(generation));

	await broker.start();
	expect(spawned).toBe(0);
	expect(broker.generation).toBe(1);
	await broker.cli(["sdk", "session", "list"]);
	expect(cliCalls).toEqual([["sdk", "session", "--agent-dir", broker.agentDir, "list"]]);

	// The daemon stops answering: this generation is fenced and a new one is
	// published only once the daemon is observed healthy again.
	probeResult = false;
	await eventually(() => logs.some((line) => line.includes("failed health")), "unhealthy daemon was not fenced");
	probeResult = true;
	await eventually(() => broker.generation === 2, "daemon recovery did not publish a new generation");
	expect(generations).toEqual([1, 2]);
	expect(healthChecks).toBeGreaterThanOrEqual(3);
	expect(spawned).toBe(0);
	expect(logs.some((line) => line.startsWith("broker_restart generation=2"))).toBe(true);

	await broker.stop();
	expect(await Bun.file(broker.lockPath).exists()).toBe(false);
});

test("red-team: an unhealthy health flip-flop never consumes or double-publishes a generation", async () => {
	const home = await temporaryHome("gajaeway-broker-health-flip-flop-");
	const generations: number[] = [];
	const logs: string[] = [];
	let probeCalls = 0;
	let firstFailedRecoveryObserved!: () => void;
	const firstFailedRecovery = new Promise<void>((resolve) => {
		firstFailedRecoveryObserved = resolve;
	});
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-health-flip-flop",
		healthProbe: async () => {
			probeCalls++;
			if (probeCalls === 1) return true;
			if (probeCalls === 2) return false;
			if (probeCalls === 3) {
				firstFailedRecoveryObserved();
				return false;
			}
			return true;
		},
		healthIntervalMs: 5,
		readinessAttempts: 1,
		restartBackoff: { initialMs: 10, maxMs: 10 },
		log: (line) => logs.push(line),
	});
	broker.onGeneration((generation) => generations.push(generation));

	try {
		await broker.start();
		await eventually(() => logs.some((line) => line.includes("failed health")), "unhealthy daemon was not fenced");
		// The first retry also reports unhealthy. This removes timing from the
		// flip-flop: the daemon only returns after a failed recovery observation.
		await firstFailedRecovery;
		const generationDuringOutage = broker.generation;
		const publishedDuringOutage = [...generations];

		await eventually(() => generations.length === 2, "recovered daemon did not publish a replacement generation");

		// Generation is an observed-recovery fence, not a retry counter. A health
		// flip-flop must not consume generations while the daemon remains down or
		// publish more than the single replacement after it returns.
		expect({
			generationDuringOutage,
			publishedDuringOutage,
			recoveredGeneration: broker.generation,
			generations,
		}).toEqual({
			generationDuringOutage: 1,
			publishedDuringOutage: [1],
			recoveredGeneration: 2,
			generations: [1, 2],
		});
	} finally {
		await broker.stop();
	}
});

test("default readiness probes cwd scope so the non-Git persona workspace never fails the daemon probe", async () => {
	const home = await temporaryHome("gajaeway-broker-readiness-scope-");
	const commands: string[][] = [];
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-readiness-scope",
		command: async (args) => {
			commands.push([...args]);
			return HEALTHY;
		},
		healthIntervalMs: 60_000,
	});
	try {
		await broker.start();
		expect(commands).toEqual([["sdk", "session", "--agent-dir", broker.agentDir, "list", "--scope", "cwd"]]);
	} finally {
		await broker.stop();
	}
});

test("readiness rejects a zero-exit generic help reply: only a structural session-list envelope is healthy", async () => {
	const home = await temporaryHome("gajaeway-broker-generic-help-");
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-generic",
		command: async () => GENERIC_HELP,
		readinessAttempts: 2,
		readinessDelayMs: 0,
		log: () => {},
	});
	await expect(broker.start()).rejects.toThrow("did not become healthy");
});

test("reclaims only stale process remnants while preserving persistent session authority and transcript evidence", async () => {
	const home = await temporaryHome("gajaeway-broker-stale-");
	const instanceId = "instance-stale";
	const stateDir = join(home, "broker", instanceId);
	const staleSocket = join(stateDir, "broker.sock");
	const authorityPath = join(stateDir, "agent", "sessions", "saved-session-1", "transcript.json");
	await mkdir(join(stateDir, "agent", "sessions", "saved-session-1"), { recursive: true });
	await writeFile(authorityPath, JSON.stringify({ sessionId: "saved-session-1", transcript: "durable transcript body" }));
	await writeFile(staleSocket, "stale endpoint");
	await writeFile(join(stateDir, "broker.lock"), `${JSON.stringify({ pid: 424_242, generation: 9 })}\n`);
	const broker = new BrokerSupervisor({
		home,
		instanceId,
		healthProbe: async () => true,
		healthIntervalMs: 60_000,
		isPidAlive: (pid) => {
			expect(pid).toBe(424_242);
			return false;
		},
	});

	await broker.start();
	expect(await Bun.file(staleSocket).exists()).toBe(false);
	expect(JSON.parse(await readFile(authorityPath, "utf8"))).toEqual({
		sessionId: "saved-session-1",
		transcript: "durable transcript body",
	});
	const lock = JSON.parse(await readFile(broker.lockPath, "utf8"));
	expect(lock).toEqual({ pid: process.pid, generation: 1 });
	expect((await lstat(broker.stateDir)).mode & 0o777).toBe(0o700);
	expect((await lstat(broker.lockPath)).mode & 0o777).toBe(0o600);
	await broker.stop();
});

test("refuses to clean a private broker directory owned by a live process", async () => {
	const home = await temporaryHome("gajaeway-broker-live-lock-");
	const stateDir = join(home, "broker", "instance-live");
	await mkdir(stateDir, { recursive: true });
	await writeFile(join(stateDir, "broker.lock"), `${JSON.stringify({ pid: 424_243, generation: 1 })}\n`);
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-live",
		healthProbe: async () => true,
		isPidAlive: () => true,
	});

	await expect(broker.start()).rejects.toThrow("held by live pid 424243");
	expect(await Bun.file(join(stateDir, "broker.lock")).exists()).toBe(true);
});

test("boot aborts before accepting connections when the Stage 0 version floor is unavailable", async () => {
	const home = await temporaryHome("gajaeway-broker-preflight-");
	const commands: string[][] = [];
	const command: CliRunner = async (args) => {
		commands.push([...args]);
		return { exitCode: 0, stdout: "gjc/0.15.5\n", stderr: "" };
	};

	await expect(bootGateway({ home, broker: { command } })).rejects.toThrow(
		`requires gjc >= ${MIN_GJC_VERSION}; found 0.15.5`,
	);
	expect(commands).toEqual([["--version"]]);
	expect(await Bun.file(join(home, "gateway.sock")).exists()).toBe(false);
});

test("boot aborts when the sdk surface answers generic help instead of a session-list envelope", async () => {
	const home = await temporaryHome("gajaeway-broker-preflight-marker-");
	const command: CliRunner = async (args) =>
		args[0] === "--version" ? { exitCode: 0, stdout: `gjc/${MIN_GJC_VERSION}\n`, stderr: "" } : GENERIC_HELP;
	await expect(bootGateway({ home, broker: { command } })).rejects.toThrow("did not answer a valid session-list envelope");
	expect(await Bun.file(join(home, "gateway.sock")).exists()).toBe(false);
});

test("boot starts the broker before the Unix server and routes ordered shutdown through it", async () => {
	const home = await temporaryHome("gajaeway-broker-boot-");
	const commands: string[][] = [];
	const command: CliRunner = async (args) => {
		commands.push([...args]);
		return args[0] === "--version" ? { exitCode: 0, stdout: `gjc/${MIN_GJC_VERSION}\n`, stderr: "" } : HEALTHY;
	};
	let spawned = 0;
	const server = await bootGateway({
		home,
		broker: {
			command,
			spawn: (() => {
				spawned++;
				throw new Error("boot must not spawn a session host");
			}) as unknown as typeof Bun.spawn,
			healthProbe: async () => true,
			healthIntervalMs: 60_000,
			log: () => {},
		},
	});

	try {
		expect(commands[0]).toEqual(["--version"]);
		expect(commands[1]?.slice(0, 2)).toEqual(["sdk", "session"]);
		expect(commands[1]).toContain("--agent-dir");
		expect(spawned).toBe(0);
		expect((await lstat(join(home, "gateway.sock"))).isSocket()).toBe(true);
	} finally {
		await server.stop("test shutdown");
	}
	// Ownership is released on ordered shutdown; the private agent dir (durable authority) survives.
	expect((await lstat(join(home, "broker"))).isDirectory()).toBe(true);
	const [instanceDir] = (await readdir(join(home, "broker")));
	expect(instanceDir).toBeDefined();
	expect(await Bun.file(join(home, "broker", instanceDir!, "broker.lock")).exists()).toBe(false);
});

test("Unix shutdown drains runtime work before stopping the broker and closing the database", async () => {
	const home = await temporaryHome("gajaeway-broker-server-");
	const config: GatewayConfig = {
		schemaVersion: 1,
		home,
		configPath: join(home, "config.json"),
		socketPath: join(home, "gateway.sock"),
		dbPath: join(home, "gateway.db"),
		logVerbosity: "info",
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const events: string[] = [];
	const sessionPort = sessionPortFromResponder({ respond: async () => "unused" });
	const broker = {
		stop: async () => {
			events.push("broker");
		},
	} as unknown as BrokerSupervisor;
	const server = await startUnixServer({
		config,
		database,
		sessionPort,
		broker,
		onStop: () => {
			events.push("database");
			database.close();
		},
	});
	await server.stop("test shutdown");

	expect(events).toEqual(["broker", "database"]);
	expect(await Bun.file(config.socketPath).exists()).toBe(false);
});

test("an explicit agent dir is supervised in place so pre-cutover sessions are adopted, not abandoned", async () => {
	const home = await temporaryHome("gajaeway-broker-adopt-");
	const inherited = join(home, "operator-agent");
	await mkdir(inherited, { recursive: true });
	const commands: string[][] = [];
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-adopt",
		agentDir: inherited,
		command: async (args) => {
			commands.push([...args]);
			return HEALTHY;
		},
		healthIntervalMs: 60_000,
	});
	try {
		await broker.start();
		expect(broker.agentDir).toBe(inherited);
		expect(commands[0]).toContain(inherited);
		// Ownership/lock state stays instance-private even when the agent dir is shared.
		expect(broker.lockPath).toBe(join(home, "broker", "instance-adopt", "broker.lock"));
	} finally {
		await broker.stop();
	}
});



test("start pins steeringMode=all and interruptMode=wait in the private agent dir without touching other keys", async () => {
	const home = await temporaryHome("gajaeway-broker-steering-");
	const broker = new BrokerSupervisor({ home, instanceId: "instance-steer", command: async () => HEALTHY, healthIntervalMs: 60_000 });
	await mkdir(broker.agentDir, { recursive: true });
	await writeFile(join(broker.agentDir, "config.yml"), "modelRoles:\n  default: x/y\nsteeringMode: one-at-a-time\nfollowUpMode: one-at-a-time\n");
	try {
		await broker.start();
		const text = await readFile(join(broker.agentDir, "config.yml"), "utf8");
		expect(text).toContain("steeringMode: all");
		expect(text).toContain("interruptMode: wait");
		expect(text).toContain("followUpMode: one-at-a-time");
		expect(text).toContain("default: x/y");
		expect(text.match(/steeringMode:/g)).toHaveLength(1);
	} finally {
		await broker.stop();
	}
});
