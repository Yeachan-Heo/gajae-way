import { afterEach, expect, test } from "bun:test";
import { lstat, mkdir, mkdtemp, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajaeway/subsession";
import { bootGateway } from "../src/boot";
import type { GatewayConfig } from "../src/config";
import {
	BrokerSupervisor,
	MIN_GJC_VERSION,
	probeBrokerDiscovery,
	probeBrokerEndpoint,
	readBrokerDiscovery,
} from "../src/orchestrator/broker";
import { startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
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
		ssotAgentDir: null,
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
		ssotAgentDir: null,
		home,
		instanceId: "instance-health-flip-flop",
		healthProbe: async () => {
			probeCalls++;
			if (probeCalls === 1) return true;
			// Three consecutive periodic strikes fence the generation (hysteresis:
			// one slow probe under load never retires live turns).
			if (probeCalls <= 4) return false;
			if (probeCalls === 5) {
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

/** A stand-in for gjc's broker transport: token-gated upgrade, unsolicited broker_hello on open. */
function fakeBrokerTransport(token: string, options: { hello?: boolean } = {}) {
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch(request, srv) {
			const url = new URL(request.url);
			if (request.headers.get("upgrade")?.toLowerCase() !== "websocket")
				return new Response("Upgrade Required", { status: 426 });
			if (url.searchParams.get("token") !== token) return new Response("Unauthorized", { status: 401 });
			return srv.upgrade(request) ? undefined : new Response("upgrade failed", { status: 400 });
		},
		websocket: {
			open(socket) {
				if (options.hello !== false) socket.send(JSON.stringify({ type: "broker_hello", protocolVersion: 3 }));
			},
			message() {},
		},
	});
	return { url: `ws://127.0.0.1:${server.port}`, stop: () => server.stop(true) };
}

async function writeDiscovery(agentDir: string, body: Record<string, unknown>): Promise<string> {
	await mkdir(join(agentDir, "sdk"), { recursive: true });
	const path = join(agentDir, "sdk", "broker.json");
	await writeFile(path, JSON.stringify(body));
	return path;
}

function discoveryBody(url: string, token: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
	return {
		version: 1,
		protocolVersion: 3,
		pid: process.pid,
		incarnation: "darwin:1:1",
		host: "127.0.0.1",
		port: Number(new URL(url).port),
		url,
		token,
		startedAt: Date.now() - 1_000,
		heartbeatAt: Date.now(),
		...extra,
	};
}

test("default readiness spawns gjc once to launch the daemon, then judges health only by the published endpoint", async () => {
	const home = await temporaryHome("gajaeway-broker-readiness-endpoint-");
	const commands: string[][] = [];
	const transport = fakeBrokerTransport("secret-token");
	let agentDir = "";
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-readiness-endpoint",
		command: async (args) => {
			commands.push([...args]);
			// gjc's first agent-dir command auto-starts its daemon, which then publishes discovery.
			await writeDiscovery(agentDir, discoveryBody(transport.url, "secret-token"));
			return HEALTHY;
		},
		healthIntervalMs: 60_000,
		readinessDelayMs: 0,
	});
	agentDir = broker.agentDir;
	try {
		await broker.start();
		expect(commands).toEqual([["sdk", "session", "--agent-dir", broker.agentDir, "list", "--scope", "cwd"]]);
	} finally {
		await broker.stop();
		transport.stop();
	}
});

test("readiness fails closed when the daemon never publishes a live endpoint, even if the CLI answers", async () => {
	const home = await temporaryHome("gajaeway-broker-no-endpoint-");
	let commands = 0;
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-no-endpoint",
		command: async () => {
			commands++;
			return HEALTHY;
		},
		readinessAttempts: 2,
		readinessDelayMs: 0,
		log: () => {},
	});
	await expect(broker.start()).rejects.toThrow("did not become healthy");
	expect(commands).toBe(2);
});

test("endpoint probe: discovery is rejected for a stale heartbeat, a dead pid, a bad token, or a silent daemon", async () => {
	const home = await temporaryHome("gajaeway-broker-endpoint-probe-");
	const transport = fakeBrokerTransport("secret-token");
	const silent = fakeBrokerTransport("secret-token", { hello: false });
	const alive = async () => true;
	const dead = async () => false;
	try {
		const fresh = await writeDiscovery(join(home, "fresh"), discoveryBody(transport.url, "secret-token"));
		expect(await readBrokerDiscovery(fresh, alive)).toMatchObject({ pid: process.pid, url: transport.url });
		expect(await readBrokerDiscovery(fresh, dead)).toBeUndefined();

		const stale = await writeDiscovery(
			join(home, "stale"),
			discoveryBody(transport.url, "secret-token", { heartbeatAt: Date.now() - 16_000 }),
		);
		expect(await readBrokerDiscovery(stale, alive)).toBeUndefined();

		const context = (discoveryPath: string) => ({
			agentDir: home,
			cli: async () => HEALTHY,
			discoveryPath,
			isPidAlive: alive,
			timeoutMs: 500,
		});
		expect(await probeBrokerDiscovery(context(fresh))).toBe(true);
		expect(await probeBrokerDiscovery(context(join(home, "missing", "broker.json")))).toBe(false);
		expect(
			await probeBrokerEndpoint({ pid: process.pid, url: transport.url, token: "wrong", heartbeatAt: Date.now() }, 500),
		).toBe(false);
		expect(
			await probeBrokerEndpoint(
				{ pid: process.pid, url: silent.url, token: "secret-token", heartbeatAt: Date.now() },
				100,
			),
		).toBe(false);
		const closed = fakeBrokerTransport("secret-token");
		closed.stop();
		expect(
			await probeBrokerEndpoint(
				{ pid: process.pid, url: closed.url, token: "secret-token", heartbeatAt: Date.now() },
				500,
			),
		).toBe(false);
	} finally {
		transport.stop();
		silent.stop();
	}
});

test("endpoint probe costs milliseconds: 20 sequential probes finish well under one CLI cold start", async () => {
	const home = await temporaryHome("gajaeway-broker-endpoint-cost-");
	const transport = fakeBrokerTransport("secret-token");
	try {
		const path = await writeDiscovery(home, discoveryBody(transport.url, "secret-token"));
		const context = {
			agentDir: home,
			cli: async () => HEALTHY,
			discoveryPath: path,
			isPidAlive: async () => true,
			timeoutMs: 500,
		};
		const started = performance.now();
		for (let index = 0; index < 20; index++) expect(await probeBrokerDiscovery(context)).toBe(true);
		expect(performance.now() - started).toBeLessThan(500);
	} finally {
		transport.stop();
	}
});

test("reclaims only stale process remnants while preserving persistent session authority and transcript evidence", async () => {
	const home = await temporaryHome("gajaeway-broker-stale-");
	const instanceId = "instance-stale";
	const stateDir = join(home, "broker", instanceId);
	const staleSocket = join(stateDir, "broker.sock");
	const authorityPath = join(stateDir, "agent", "sessions", "saved-session-1", "transcript.json");
	await mkdir(join(stateDir, "agent", "sessions", "saved-session-1"), { recursive: true });
	await writeFile(
		authorityPath,
		JSON.stringify({ sessionId: "saved-session-1", transcript: "durable transcript body" }),
	);
	await writeFile(staleSocket, "stale endpoint");
	await writeFile(join(stateDir, "broker.lock"), `${JSON.stringify({ pid: 424_242, generation: 9 })}\n`);
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
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
		ssotAgentDir: null,
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

	await expect(bootGateway({ home, broker: { ssotAgentDir: null, command } })).rejects.toThrow(
		`requires gjc >= ${MIN_GJC_VERSION}; found 0.15.5`,
	);
	expect(commands).toEqual([["--version"]]);
	expect(await Bun.file(join(home, "gateway.sock")).exists()).toBe(false);
});

test("boot aborts when the sdk surface answers generic help instead of a session-list envelope", async () => {
	const home = await temporaryHome("gajaeway-broker-preflight-marker-");
	const command: CliRunner = async (args) =>
		args[0] === "--version" ? { exitCode: 0, stdout: `gjc/${MIN_GJC_VERSION}\n`, stderr: "" } : GENERIC_HELP;
	await expect(bootGateway({ home, broker: { ssotAgentDir: null, command } })).rejects.toThrow(
		"did not answer a valid session-list envelope",
	);
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
			ssotAgentDir: null,
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
	const [instanceDir] = await readdir(join(home, "broker"));
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
		ssotAgentDir: null,
		home,
		instanceId: "instance-adopt",
		agentDir: inherited,
		command: async (args) => {
			commands.push([...args]);
			return HEALTHY;
		},
		// The launch command must bind the adopted dir; health itself is stubbed.
		healthProbe: async ({ cli }) => {
			await cli(["sdk", "session", "list", "--scope", "cwd"]);
			return true;
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
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-steer",
		ssotAgentDir: null,
		command: async () => HEALTHY,
		healthProbe: async () => true,
		healthIntervalMs: 60_000,
	});
	await mkdir(broker.agentDir, { recursive: true });
	await writeFile(
		join(broker.agentDir, "config.yml"),
		"modelRoles:\n  default: x/y\nsteeringMode: one-at-a-time\nfollowUpMode: one-at-a-time\n",
	);
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

test("start seeds the private agent dir from the operator SSOT and fails loudly when models.yml is missing", async () => {
	const home = await temporaryHome("gajaeway-broker-ssot-");
	const ssot = join(home, "ssot");
	await mkdir(join(ssot, "model-presets"), { recursive: true });
	await writeFile(join(ssot, "models.yml"), "providers:\n  p:\n    baseUrl: https://x\n");
	await writeFile(join(ssot, "model-presets", "state.json"), '{"presets":{}}');
	await writeFile(join(ssot, "config.yml"), "modelRoles:\n  default: p/m\nsteeringMode: one-at-a-time\n");
	const logs: string[] = [];
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-ssot",
		ssotAgentDir: ssot,
		command: async () => HEALTHY,
		healthProbe: async () => true,
		healthIntervalMs: 60_000,
		log: (line) => logs.push(line),
	});
	try {
		await broker.start();
		expect(await readFile(join(broker.agentDir, "models.yml"), "utf8")).toContain("baseUrl: https://x");
		expect(await readFile(join(broker.agentDir, "model-presets", "state.json"), "utf8")).toContain("presets");
		const config = await readFile(join(broker.agentDir, "config.yml"), "utf8");
		expect(config).toContain("default: p/m");
		expect(config).toContain("steeringMode: all");
		expect(config).toContain("interruptMode: wait");
		expect(config.match(/steeringMode:/g)).toHaveLength(1);
		expect(logs.some((line) => line.startsWith("broker_agent_dir_seeded"))).toBe(true);
	} finally {
		await broker.stop();
	}
	const empty = join(home, "empty-ssot");
	await mkdir(empty, { recursive: true });
	const missing = new BrokerSupervisor({
		home,
		instanceId: "instance-ssot-missing",
		ssotAgentDir: empty,
		command: async () => HEALTHY,
		healthProbe: async () => true,
	});
	await expect(missing.start()).rejects.toThrow("operator SSOT");
});

test("boot reap removes lock tombstones and spawn residue from the private agent dir only", async () => {
	const home = await temporaryHome("gajaeway-broker-reap-");
	const broker = new BrokerSupervisor({
		home,
		instanceId: "instance-reap",
		ssotAgentDir: null,
		command: async () => HEALTHY,
		healthProbe: async () => true,
		healthIntervalMs: 60_000,
		isPidAlive: () => false,
	});
	const sdk = join(broker.agentDir, "sdk");
	await mkdir(join(sdk, "sessions"), { recursive: true });
	await mkdir(join(sdk, ".broker.lock.stale-deadbeef"), { recursive: true });
	await mkdir(join(sdk, "sessions", "index.jsonl.lock.pending.1.x"), { recursive: true });
	await writeFile(join(sdk, "broker.startup-failure.json"), "{}");
	await writeFile(join(sdk, "sessions", "index.jsonl"), "");
	await writeFile(join(sdk, "broker.json"), "{}");
	try {
		await broker.start();
		const names = await readdir(sdk);
		expect(names).not.toContain(".broker.lock.stale-deadbeef");
		expect(names).not.toContain("broker.startup-failure.json");
		expect(names).toContain("broker.json");
		expect(await readdir(join(sdk, "sessions"))).toEqual(["index.jsonl"]);
	} finally {
		await broker.stop();
	}
});
