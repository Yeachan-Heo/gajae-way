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

/**
 * A stand-in for gjc's broker transport: token-gated upgrade, unsolicited
 * broker_hello on open, and a `broker_response` to `session.list`. `router`
 * models the daemon's session Router: "ok" answers, "error" answers ok:false,
 * "stall" never answers (accept loop alive, routing wedged).
 */
function fakeBrokerTransport(
	token: string,
	options: { hello?: boolean; protocolVersion?: number; router?: "ok" | "error" | "stall" } = {},
) {
	const requests: unknown[] = [];
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
				if (options.hello !== false)
					socket.send(JSON.stringify({ type: "broker_hello", protocolVersion: options.protocolVersion ?? 3 }));
			},
			message(socket, raw) {
				const frame = JSON.parse(String(raw)) as { id?: string; operation?: string };
				requests.push(frame);
				if ((options.router ?? "ok") === "stall") return;
				if (options.router === "error")
					socket.send(
						JSON.stringify({ type: "broker_response", id: frame.id, ok: false, error: { code: "unavailable" } }),
					);
				else if (frame.operation === "session.list")
					socket.send(JSON.stringify({ type: "broker_response", id: frame.id, ok: true, result: { sessions: [] } }));
				else
					socket.send(
						JSON.stringify({ type: "broker_response", id: frame.id, ok: false, error: { code: "unknown_operation" } }),
					);
			},
		},
	});
	return { url: `ws://127.0.0.1:${server.port}`, requests, stop: () => server.stop(true) };
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
		expect(commands).toEqual([
			[
				"sdk",
				"session",
				"--agent-dir",
				broker.agentDir,
				"raw",
				"global",
				"--op",
				"session.list",
				"--json-input",
				'{"resolveSessionId":"00000000-0000-4000-8000-000000000000"}',
			],
		]);
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

test("red-team B1: a daemon that never answers is unhealthy; one that answers - even with an error - is alive", async () => {
	const stalled = fakeBrokerTransport("secret-token", { router: "stall" });
	const erroring = fakeBrokerTransport("secret-token", { router: "error" });
	const healthy = fakeBrokerTransport("secret-token");
	const at = (url: string) => ({ pid: process.pid, url, token: "secret-token", heartbeatAt: Date.now() });
	try {
		const started = performance.now();
		expect(await probeBrokerEndpoint(at(stalled.url), 300)).toBe(false);
		// A stalled router is detected at the timeout, never earlier and never by hanging.
		expect(performance.now() - started).toBeGreaterThanOrEqual(290);
		expect(performance.now() - started).toBeLessThan(1_000);
		// An error response is a LIVE daemon rejecting our input, not a dead one:
		// treating it as death let a cursor leak escalate into killing a healthy
		// broker mid-turn (live, 2026-09-03). The code is reported, not fatal.
		const codes: string[] = [];
		expect(await probeBrokerEndpoint(at(erroring.url), 500, (code) => void codes.push(code))).toBe(true);
		expect(codes).toEqual(["unavailable"]);
		expect(await probeBrokerEndpoint(at(healthy.url), 500)).toBe(true);
		// The probe is an exact-empty lookup: no session count can make it
		// paginate or allocate a cursor, so a process can run indefinitely.
		expect(healthy.requests).toEqual([
			expect.objectContaining({
				type: "broker_request",
				operation: "session.list",
				input: { resolveSessionId: "00000000-0000-4000-8000-000000000000" },
			}),
		]);
		// A hello with the wrong protocol version is not a broker we know how to talk to.
		const wrongVersion = fakeBrokerTransport("secret-token", { protocolVersion: 2 });
		try {
			expect(await probeBrokerEndpoint(at(wrongVersion.url), 500)).toBe(false);
		} finally {
			wrongVersion.stop();
		}
	} finally {
		stalled.stop();
		erroring.stop();
		healthy.stop();
	}
});

test("red-team F4: discovery URL is validated structurally, never by string prefix", async () => {
	const home = await temporaryHome("gajaeway-broker-url-");
	const alive = async () => true;
	const cases: Array<[string, boolean]> = [
		["ws://127.0.0.1:60232", true],
		["ws://127.0.0.1:60232/", true],
		["ws://127.0.0.1:80@evil.example", false],
		["ws://127.0.0.1:60232/path", false],
		["ws://127.0.0.1:60232?x=1", false],
		["ws://127.0.0.1:60232#frag", false],
		["ws://localhost:60232", false],
		["wss://127.0.0.1:60232", false],
		["ws://127.0.0.1", false],
		["not a url", false],
	];
	for (const [url, ok] of cases) {
		const path = await writeDiscovery(
			join(home, Bun.hash(url).toString(16)),
			discoveryBody("ws://127.0.0.1:1", "t", { url }),
		);
		expect(await readBrokerDiscovery(path, alive), url).toEqual(ok ? expect.objectContaining({ url }) : undefined);
	}
});

test("red-team P2/P5: heartbeat TTL boundary and malformed discovery never throw", async () => {
	const home = await temporaryHome("gajaeway-broker-ttl-");
	const alive = async () => true;
	const now = Date.now();
	const fresh = await writeDiscovery(
		join(home, "a"),
		discoveryBody("ws://127.0.0.1:1", "t", { heartbeatAt: now - 14_999 }),
	);
	const stale = await writeDiscovery(
		join(home, "b"),
		discoveryBody("ws://127.0.0.1:1", "t", { heartbeatAt: now - 15_001 }),
	);
	expect(await readBrokerDiscovery(fresh, alive, now)).toBeDefined();
	expect(await readBrokerDiscovery(stale, alive, now)).toBeUndefined();
	await mkdir(join(home, "c", "sdk"), { recursive: true });
	await writeFile(join(home, "c", "sdk", "broker.json"), "{not json");
	expect(await readBrokerDiscovery(join(home, "c", "sdk", "broker.json"), alive)).toBeUndefined();
	await writeFile(join(home, "c", "sdk", "broker.json"), "null");
	expect(await readBrokerDiscovery(join(home, "c", "sdk", "broker.json"), alive)).toBeUndefined();
	await writeFile(join(home, "c", "sdk", "broker.json"), JSON.stringify([1, 2]));
	expect(await readBrokerDiscovery(join(home, "c", "sdk", "broker.json"), alive)).toBeUndefined();
});

test("red-team P3/P7: a non-hello first frame is rejected, and a live endpoint means zero gjc spawns at readiness", async () => {
	const home = await temporaryHome("gajaeway-broker-nohello-");
	// A daemon that speaks first but not with a hello.
	const rogue = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch: (request, srv) => (srv.upgrade(request) ? undefined : new Response("no", { status: 400 })),
		websocket: {
			open(socket) {
				socket.send(JSON.stringify({ type: "broker_response", id: "x", ok: true }));
				socket.close();
			},
			message() {},
		},
	});
	const transport = fakeBrokerTransport("secret-token");
	let spawned = 0;
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-no-spawn",
		command: async () => {
			spawned++;
			return HEALTHY;
		},
		healthIntervalMs: 60_000,
		readinessDelayMs: 0,
	});
	try {
		expect(
			await probeBrokerEndpoint(
				{ pid: process.pid, url: `ws://127.0.0.1:${rogue.port}`, token: "t", heartbeatAt: Date.now() },
				500,
			),
		).toBe(false);
		await writeDiscovery(broker.agentDir, discoveryBody(transport.url, "secret-token"));
		await broker.start();
		expect(spawned).toBe(0);
	} finally {
		await broker.stop();
		transport.stop();
		rogue.stop(true);
	}
});

test("red-team G2-B1: a daemon that keeps a fresh heartbeat but cannot route session.list is retired and relaunched", async () => {
	const home = await temporaryHome("gajaeway-broker-wedged-");
	// Generation A: greets, keeps its heartbeat fresh, never answers session.list.
	const wedged = fakeBrokerTransport("secret-token", { router: "stall" });
	// Generation B: the replacement gjc launches after A is retired.
	const replacement = fakeBrokerTransport("secret-token");
	let launches = 0;
	const killed: number[] = [];
	const wedgedPid = 424242;
	let agentDir = "";
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-wedged",
		command: async () => {
			launches++;
			await writeDiscovery(agentDir, discoveryBody(replacement.url, "secret-token", { pid: process.pid }));
			return HEALTHY;
		},
		isPidAlive: (pid) => pid === process.pid || (pid === wedgedPid && !killed.includes(pid)),
		healthIntervalMs: 60_000,
		healthProbeTimeoutMs: 100,
		readinessDelayMs: 0,
		readinessAttempts: 20,
		log: () => {},
	});
	agentDir = broker.agentDir;
	const originalKill = process.kill;
	(process as { kill: typeof process.kill }).kill = ((pid: number, signal?: string | number) => {
		if (pid === wedgedPid) {
			killed.push(pid);
			return true;
		}
		return originalKill(pid, signal as NodeJS.Signals);
	}) as typeof process.kill;
	try {
		// The wedged daemon refreshes its heartbeat the whole time.
		await writeDiscovery(agentDir, discoveryBody(wedged.url, "secret-token", { pid: wedgedPid }));
		const refresher = setInterval(() => {
			if (killed.length === 0)
				void writeDiscovery(agentDir, discoveryBody(wedged.url, "secret-token", { pid: wedgedPid }));
		}, 20);
		try {
			await broker.start();
		} finally {
			clearInterval(refresher);
		}
		expect(killed).toEqual([wedgedPid]);
		expect(launches).toBe(1);
		expect(replacement.requests.length).toBeGreaterThanOrEqual(1);
	} finally {
		(process as { kill: typeof process.kill }).kill = originalKill;
		await broker.stop();
		wedged.stop();
		replacement.stop();
	}
});

test("red-team G3-F3: wedged-daemon strikes are scoped to one daemon; a self-replaced daemon starts from zero", async () => {
	const home = await temporaryHome("gajaeway-broker-strike-scope-");
	// Daemon A (pid 1001) is wedged. After two failed probes it self-replaces
	// with daemon B (pid 1002) on a transport that fails ONE probe and then
	// routes. B must never be retired: it only ever accrued one strike of its own.
	const wedged = fakeBrokerTransport("secret-token", { router: "stall" });
	let bAnswers = false;
	const flaky = fakeBrokerTransport("secret-token", { router: "stall" });
	const healthy = fakeBrokerTransport("secret-token");
	const killed: number[] = [];
	let agentDir = "";
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-strike-scope",
		command: async () => HEALTHY,
		isPidAlive: (pid) => pid === process.pid || ((pid === 1001 || pid === 1002) && !killed.includes(pid)),
		healthIntervalMs: 60_000,
		healthProbeTimeoutMs: 60,
		readinessDelayMs: 0,
		readinessAttempts: 20,
		log: () => {},
	});
	agentDir = broker.agentDir;
	const originalKill = process.kill;
	(process as { kill: typeof process.kill }).kill = ((pid: number, signal?: string | number) => {
		if (pid === 1001 || pid === 1002) {
			killed.push(pid);
			return true;
		}
		return originalKill(pid, signal as NodeJS.Signals);
	}) as typeof process.kill;
	// Discovery script: A twice, then B (flaky) once, then B (healthy).
	let phase = 0;
	const refresher = setInterval(() => {
		phase++;
		const body =
			phase <= 4
				? discoveryBody(wedged.url, "secret-token", { pid: 1001 })
				: !bAnswers
					? discoveryBody(flaky.url, "secret-token", { pid: 1002 })
					: discoveryBody(healthy.url, "secret-token", { pid: 1002 });
		if (phase >= 7) bAnswers = true;
		void writeDiscovery(agentDir, body);
	}, 30);
	try {
		await writeDiscovery(agentDir, discoveryBody(wedged.url, "secret-token", { pid: 1001 }));
		await broker.start();
		expect(killed).not.toContain(1002);
	} finally {
		clearInterval(refresher);
		(process as { kill: typeof process.kill }).kill = originalKill;
		await broker.stop();
		wedged.stop();
		flaky.stop();
		healthy.stop();
	}
});

test("red-team G4-F1: a strike is charged to the discovery record that was probed, so a mid-probe flip to B never counts against B", async () => {
	const home = await temporaryHome("gajaeway-broker-strike-race-");
	// A stalls every probe. B is wedged on its first two probes, then healthy.
	// Sequence: A fails once, then discovery flips to B WHILE A's 2nd probe is
	// in flight. Post-probe attribution would read B and charge A's 2nd failure
	// to B (B: 1), then B's own two failures make 3 and B is killed. Probing
	// the record that was read charges it to A (A: 2), B starts at 0, survives
	// its two real strikes and comes healthy. Nothing is retired.
	const stalled = fakeBrokerTransport("secret-token", { router: "stall" });
	let bFailures = 0;
	const bServer = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch(request, srv) {
			if (new URL(request.url).searchParams.get("token") !== "secret-token")
				return new Response("Unauthorized", { status: 401 });
			return srv.upgrade(request) ? undefined : new Response("no", { status: 400 });
		},
		websocket: {
			open(socket) {
				socket.send(JSON.stringify({ type: "broker_hello", protocolVersion: 3 }));
			},
			message(socket, raw) {
				const frame = JSON.parse(String(raw)) as { id?: string };
				if (bFailures < 2) {
					bFailures++;
					return; // stall
				}
				socket.send(JSON.stringify({ type: "broker_response", id: frame.id, ok: true, result: { sessions: [] } }));
			},
		},
	});
	const bUrl = `ws://127.0.0.1:${bServer.port}`;
	const killed: number[] = [];
	let agentDir = "";
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-strike-race",
		command: async () => HEALTHY,
		isPidAlive: (pid) => pid === process.pid || ((pid === 2001 || pid === 2002) && !killed.includes(pid)),
		healthIntervalMs: 60_000,
		healthProbeTimeoutMs: 120,
		readinessDelayMs: 0,
		readinessAttempts: 20,
		log: () => {},
	});
	agentDir = broker.agentDir;
	const originalKill = process.kill;
	(process as { kill: typeof process.kill }).kill = ((pid: number, signal?: string | number) => {
		if (pid === 2001 || pid === 2002) {
			killed.push(pid);
			return true;
		}
		return originalKill(pid, signal as NodeJS.Signals);
	}) as typeof process.kill;
	try {
		await writeDiscovery(agentDir, discoveryBody(stalled.url, "secret-token", { pid: 2001 }));
		const flipper = setInterval(() => {
			// A's 2nd request has arrived and is stalling: flip discovery now.
			if (stalled.requests.length >= 2) {
				clearInterval(flipper);
				void writeDiscovery(agentDir, discoveryBody(bUrl, "secret-token", { pid: 2002 }));
			}
		}, 2);
		try {
			await broker.start();
		} finally {
			clearInterval(flipper);
		}
		expect(stalled.requests.length).toBe(2);
		expect(bFailures).toBe(2);
		expect(killed).toEqual([]);
	} finally {
		(process as { kill: typeof process.kill }).kill = originalKill;
		await broker.stop();
		stalled.stop();
		bServer.stop(true);
	}
});

test("live regression: a daemon whose session.list keeps failing is never killed - it is answering", async () => {
	const home = await temporaryHome("gajaeway-broker-app-error-");
	// Exactly the 2026-09-03 outage: the probe's own paginated session.list
	// pinned a 15-minute continuation cursor every 5s until the daemon's pool of
	// 32 was exhausted, after which every call answered `invalid_input`. The
	// supervisor read that as death and killed a healthy broker mid-turn.
	const erroring = fakeBrokerTransport("secret-token", { router: "error" });
	const killed: number[] = [];
	const logs: string[] = [];
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-app-error",
		command: async () => HEALTHY,
		isPidAlive: (pid) => pid === process.pid || (pid === 7007 && !killed.includes(pid)),
		healthIntervalMs: 20,
		healthProbeTimeoutMs: 200,
		readinessDelayMs: 0,
		log: (line) => void logs.push(line),
	});
	const originalKill = process.kill;
	(process as { kill: typeof process.kill }).kill = ((pid: number, signal?: string | number) => {
		if (pid === 7007) {
			killed.push(pid);
			return true;
		}
		return originalKill(pid, signal as NodeJS.Signals);
	}) as typeof process.kill;
	try {
		await writeDiscovery(broker.agentDir, discoveryBody(erroring.url, "secret-token", { pid: 7007 }));
		await broker.start();
		// Let the periodic health timer run well past the three-strike threshold.
		await Bun.sleep(300);
		expect(killed).toEqual([]);
		expect(logs.filter((line) => line.includes("strike"))).toEqual([]);
		expect(logs.some((line) => line.includes("broker_probe_application_error code=unavailable"))).toBe(true);
	} finally {
		(process as { kill: typeof process.kill }).kill = originalKill;
		await broker.stop();
		erroring.stop();
	}
});

test("red-team F6: a timed-out CLI child holds its slot until it has actually exited", async () => {
	const home = await temporaryHome("gajaeway-broker-timeout-child-");
	const transport = fakeBrokerTransport("secret-token");
	let exitChild: (() => void) | undefined;
	let live = 0;
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-timeout-child",
		spawn: (() => {
			live++;
			const exited = new Promise<number>((resolve) => {
				exitChild = () => {
					live--;
					resolve(143);
				};
			});
			const never = () => new ReadableStream<Uint8Array>({ start() {} });
			return { stdout: never(), stderr: never(), exited, kill: () => exitChild?.() } as unknown as ReturnType<
				typeof Bun.spawn
			>;
		}) as unknown as typeof Bun.spawn,
		healthIntervalMs: 60_000,
		readinessDelayMs: 0,
	});
	try {
		await writeDiscovery(broker.agentDir, discoveryBody(transport.url, "secret-token"));
		await broker.start();
		const started = performance.now();
		await expect(broker.cli(["sdk", "session", "status", "s", "--repo", home], { timeoutMs: 50 })).rejects.toThrow(
			"timed out",
		);
		// kill() resolved `exited`, so the slot was released only once the child was gone.
		expect(live).toBe(0);
		expect(performance.now() - started).toBeGreaterThanOrEqual(45);
	} finally {
		await broker.stop();
		transport.stop();
	}
});

test("the CLI health probe has its own lane: saturating the shared cap with slow observation calls never delays it", async () => {
	const home = await temporaryHome("gajaeway-broker-probe-lane-");
	const transport = fakeBrokerTransport("secret-token");
	const releases: Array<() => void> = [];
	const spawnedArgs: string[][] = [];
	const child = (args: string[]) => {
		const isProbe = args.includes("list") && args.includes("--scope");
		spawnedArgs.push(args);
		const stdout = isProbe
			? Promise.resolve(HEALTHY.stdout)
			: new Promise<string>((resolve) => releases.push(() => resolve(JSON.stringify({ ok: true, result: {} }))));
		const body = (text: Promise<string>) =>
			new ReadableStream<Uint8Array>({
				async start(controller) {
					controller.enqueue(new TextEncoder().encode(await text));
					controller.close();
				},
			});
		return {
			stdout: body(stdout),
			stderr: body(Promise.resolve("")),
			exited: stdout.then(() => 0),
			kill: () => {},
		} as unknown as ReturnType<typeof Bun.spawn>;
	};
	const broker = new BrokerSupervisor({
		ssotAgentDir: null,
		home,
		instanceId: "instance-probe-lane",
		spawn: ((options: { cmd: string[] }) => child(options.cmd.slice(1))) as unknown as typeof Bun.spawn,
		healthIntervalMs: 60_000,
		readinessDelayMs: 0,
		healthProbeTimeoutMs: 1_000,
	});
	try {
		// Readiness: the launch command runs once, then the endpoint probe judges health.
		await writeDiscovery(broker.agentDir, discoveryBody(transport.url, "secret-token"));
		await broker.start();
		// Saturate every shared slot (MAX_CONCURRENT_CLI = 4) plus a queued fifth with slow status calls.
		const observation = Array.from({ length: 5 }, (_, index) =>
			broker.cli(["sdk", "session", "status", `s-${index}`, "--repo", home], { timeoutMs: 30_000 }),
		);
		await eventually(
			() => spawnedArgs.filter((args) => args.includes("status")).length === 4,
			"shared slots did not fill",
		);
		const started = performance.now();
		const probe = await broker.cli(["sdk", "session", "list", "--scope", "cwd"], { timeoutMs: 1_000 });
		expect(performance.now() - started).toBeLessThan(500);
		expect(JSON.parse(probe.stdout)).toMatchObject({ ok: true });
		expect(spawnedArgs.filter((args) => args.includes("status")).length).toBe(4);
		// The queued fifth call only spawns after a slot frees, so drain until all five settled.
		let settled = 0;
		for (const task of observation) void task.then(() => settled++);
		while (settled < observation.length) {
			for (const release of releases.splice(0)) release();
			await Bun.sleep(5);
		}
	} finally {
		for (const release of releases.splice(0)) release();
		await broker.stop();
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

test("boot aborts when the broker endpoint never provides application health", async () => {
	const home = await temporaryHome("gajaeway-broker-preflight-marker-");
	const command: CliRunner = async (args) =>
		args[0] === "--version" ? { exitCode: 0, stdout: `gjc/${MIN_GJC_VERSION}\n`, stderr: "" } : GENERIC_HELP;
	await expect(bootGateway({ home, broker: { ssotAgentDir: null, command } })).rejects.toThrow(
		"broker daemon did not become healthy",
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
		expect(commands).toEqual([["--version"]]);
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
