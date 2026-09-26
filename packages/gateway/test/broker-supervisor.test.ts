import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type BrokerDiscovery,
	brokerHealthArgs,
	GlobalGjcClient,
	HEALTH_PROBE_SESSION_ID,
	isHealthySessionList,
	isLoopbackWebSocketUrl,
	isUsageRejection,
	preflightGjcRuntime,
	probeBrokerEndpoint,
	readBrokerDiscovery,
	type SpawnFn,
} from "../src/orchestrator/broker";

const directories: string[] = [];
const clients: GlobalGjcClient[] = [];
afterEach(async () => {
	await Promise.all(clients.splice(0).map((client) => client.stop()));
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

test("failed relay signals never turn an unconfirmed exit into clean shutdown", async () => {
	let finish = (_code: number) => {};
	const exited = new Promise<number>((resolve) => {
		finish = resolve;
	});
	const signals: string[] = [];
	const logs: string[] = [];
	const spawn = (() => ({
		exited,
		stdout: new ReadableStream(),
		kill(signal: string) {
			signals.push(signal);
			throw new Error("secret-token-must-not-leak");
		},
	})) as unknown as SpawnFn;
	const value = client({ spawn, log: (line) => logs.push(line) });
	await value.start();
	const relay = value.openStream("owned-session");
	relay.close();
	try {
		await expect(value.stop()).rejects.toThrow("shutdown incomplete");
		expect(signals).toEqual(["SIGTERM", "SIGKILL"]);
		expect(logs.join(" ")).not.toContain("secret-token-must-not-leak");
		await expect(value.start()).rejects.toThrow("exit remains unconfirmed");
	} finally {
		finish(0);
		await exited;
		await value.stop();
	}
}, 10_000);

test("the stdio relay binds its agent directory through the environment, never a --agent-dir flag", async () => {
	// gjc 0.16.7: `sdk serve` rejects --agent-dir ("unknown argument", exit 2
	// right after the hello frame). A relay that dies in under a second is
	// counted as a reopen failure; six of those declare the stream dead and hold
	// the turn. The flag therefore silences every long turn (live, 2026-09-17).
	const spawned: Array<{ cmd: readonly string[]; env: Record<string, string> }> = [];
	let finish = (_code: number) => {};
	const spawn = ((options: { cmd: readonly string[]; env: Record<string, string> }) => {
		spawned.push(options);
		const exited = new Promise<number>((resolve) => {
			finish = resolve;
		});
		return {
			exited,
			stdout: new ReadableStream(),
			kill() {
				finish(0);
			},
		};
	}) as unknown as SpawnFn;
	const value = client({ spawn });
	await value.start();
	const relay = value.openStream("owned-session");
	try {
		expect(spawned).toHaveLength(1);
		const { cmd, env } = spawned[0]!;
		expect(cmd.slice(1)).toEqual(["sdk", "serve", "--stdio", "--session", "owned-session"]);
		expect(cmd.some((arg) => arg.startsWith("--agent-dir"))).toBe(false);
		// The binding still happens - through the trusted environment.
		expect(env.GJC_CODING_AGENT_DIR).toBe(value.agentDir);
		expect(env.PI_CODING_AGENT_DIR).toBe(value.agentDir);
	} finally {
		relay.close();
	}
});

test("CLI timeout with unconfirmed termination fences client generation until observed exit", async () => {
	let finish = (_code: number) => {};
	const exited = new Promise<number>((resolve) => {
		finish = resolve;
	});
	const spawn = (() => ({
		exited,
		stdout: new Blob([]).stream(),
		stderr: new Blob([]).stream(),
		kill() {},
	})) as unknown as SpawnFn;
	const value = client({ spawn, command: undefined });
	await value.start();
	try {
		await expect(value.cli(["sdk", "session", "list"], { timeoutMs: 5 })).rejects.toThrow("termination failed");
		await expect(value.cli(["sdk", "session", "list"])).rejects.toThrow("stopped");
		await expect(value.start()).rejects.toThrow("exit remains unconfirmed");
		expect(value.generation).toBe(1);
	} finally {
		finish(0);
		await exited;
		await value.stop();
	}
}, 10_000);

test("normal owned relay exit is observed and allows clean idempotent stop", async () => {
	let finish = (_code: number) => {};
	const exited = new Promise<number>((resolve) => {
		finish = resolve;
	});
	let signals = 0;
	const spawn = (() => ({
		exited,
		stdout: new ReadableStream(),
		kill() {
			signals++;
			finish(0);
		},
	})) as unknown as SpawnFn;
	const value = client({ spawn });
	await value.start();
	value.openStream("owned-session");
	await value.stop();
	await value.stop();
	expect(signals).toBe(1);
});
async function directory(): Promise<string> {
	const path = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-global-client-")));
	directories.push(path);
	return path;
}
const healthy = { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" };
const discovery = (): BrokerDiscovery => ({
	pid: 12345,
	url: "ws://127.0.0.1:12345",
	token: "fake-incarnation",
	heartbeatAt: Date.now(),
});
function client(options: ConstructorParameters<typeof GlobalGjcClient>[0] = {}): GlobalGjcClient {
	const value = new GlobalGjcClient({
		executable: "/fake/nondefault/bin/gjc",
		agentDir: "/fake/global/agent",
		command: async () => healthy,
		discovery: async () => discovery(),
		healthProbe: async () => true,
		log: () => {},
		...options,
	});
	clients.push(value);
	return value;
}
async function eventually(predicate: () => boolean): Promise<void> {
	for (let i = 0; i < 100; i++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate()).toBe(true);
}

test("symlink profile aliases share canonical authority, CLI and relay environment without config writes", async () => {
	const root = await directory();
	const profile = join(root, "user-agent");
	const alias = join(root, "profile-alias");
	await mkdir(profile);
	await writeFile(join(profile, "config.yml"), "steeringMode: one-at-a-time\n");
	await symlink(profile, alias);
	const invocations: Array<{ cmd: string[]; env: Record<string, string> }> = [];
	const spawn = ((options: { cmd: string[]; env: Record<string, string> }) => {
		invocations.push(options);
		return {
			exited: Promise.resolve(0),
			stdout: new Blob([healthy.stdout]).stream(),
			stderr: new Blob([]).stream(),
			kill() {},
		};
	}) as unknown as SpawnFn;
	const direct = client({ agentDir: profile });
	const linked = client({ agentDir: alias, spawn, command: undefined });
	expect(linked.agentDir).toBe(await realpath(profile));
	expect(linked.agentDir).toBe(direct.agentDir);
	await linked.start();
	await linked.cli(["sdk", "session", "list", "--scope", "all"]);
	const stream = linked.openStream("owned-session");
	for await (const _line of stream.lines) {
		/* drain fake relay */
	}
	await linked.stop();
	expect(invocations).toHaveLength(2);
	const [cliInvocation, relayInvocation] = invocations as [(typeof invocations)[0], (typeof invocations)[0]];
	// The CLI binds by flag; the relay (`sdk serve`) accepts no --agent-dir and
	// binds by environment only. Both must resolve the alias to the same profile.
	expect(cliInvocation.cmd[cliInvocation.cmd.indexOf("--agent-dir") + 1]).toBe(direct.agentDir);
	expect(relayInvocation.cmd).toEqual([
		relayInvocation.cmd[0],
		"sdk",
		"serve",
		"--stdio",
		"--session",
		"owned-session",
	]);
	for (const invocation of invocations) {
		expect(invocation.env.GJC_CODING_AGENT_DIR).toBe(direct.agentDir);
		expect(invocation.env.PI_CODING_AGENT_DIR).toBe(direct.agentDir);
		expect(invocation.env.GJC_AGENT_DIR).toBe(direct.agentDir);
	}
	expect(await readFile(join(profile, "config.yml"), "utf8")).toBe("steeringMode: one-at-a-time\n");
	expect(await readdir(profile)).toEqual(["config.yml"]);
});

test("missing profiles retain GJC autostart but later alias changes cannot silently retarget authority", async () => {
	const root = await directory();
	const profile = join(root, "new-profile");
	const value = client({ agentDir: profile });
	expect(value.agentDir).toBe(profile);
	await mkdir(profile);
	await value.start();
	await value.stop();
	const other = join(root, "other-profile");
	await mkdir(other);
	await rm(profile, { recursive: true });
	await symlink(other, profile);
	await expect(value.start()).rejects.toThrow("canonical identity changed");
	await expect(value.cli(["sdk", "session", "list"])).rejects.toThrow("canonical identity changed");
	expect(() => value.openStream("owned-session")).toThrow("canonical identity changed");
	expect(await readdir(other)).toEqual([]);
});

test("adopts a nondefault global agent directory without writing, seeding, deleting locks, or signalling its daemon", async () => {
	const agentDir = await directory();
	await mkdir(join(agentDir, "sdk"));
	const files = {
		"config.yml": "steeringMode: one-at-a-time\ninterruptMode: immediate\n",
		"models.yml": "operator: unchanged\n",
		"sdk/broker.lock": "operator-owned-lock",
		"sdk/.broker.lock.stale-1": "leave-me",
	};
	for (const [name, content] of Object.entries(files)) await writeFile(join(agentDir, name), content);
	const record = { ...discovery(), protocolVersion: 3, host: "127.0.0.1" };
	await writeFile(join(agentDir, "sdk/broker.json"), JSON.stringify(record));
	const kill = spyOn(process, "kill");
	try {
		const value = client({ agentDir, discovery: undefined, isPidAlive: () => true });
		await value.start();
		expect(value.generation).toBe(1);
		await value.stop();
		expect(kill).not.toHaveBeenCalled();
		for (const [name, content] of Object.entries(files))
			expect(await readFile(join(agentDir, name), "utf8")).toBe(content);
		expect(JSON.parse(await readFile(join(agentDir, "sdk/broker.json"), "utf8"))).toEqual(record);
		expect((await readdir(agentDir)).sort()).toEqual(["config.yml", "models.yml", "sdk"]);
	} finally {
		kill.mockRestore();
	}
});

test("no discovery launches only a read-only explicit all-scope readiness request", async () => {
	let observed = false;
	const calls: string[][] = [];
	const value = client({
		discovery: async () => (observed ? discovery() : undefined),
		command: async (args) => {
			calls.push([...args]);
			observed = true;
			return healthy;
		},
	});
	await value.start();
	expect(calls).toEqual([["sdk", "session", "list", "--scope", "all", "--json", "--agent-dir", value.agentDir]]);
	expect(value.generation).toBe(1);
});

test("wedged global daemon stays untouched and readiness fails bounded", async () => {
	let commands = 0;
	const value = client({
		healthProbe: () => new Promise(() => {}),
		command: async () => {
			commands++;
			return healthy;
		},
		healthProbeTimeoutMs: 5,
		readinessAttempts: 2,
		readinessDelayMs: 0,
	});
	await expect(value.start()).rejects.toThrow("no repair attempted");
	expect(commands).toBe(0);
	expect(value.generation).toBe(0);
});

test("reconnection is read-only and only a changed incarnation advances generation", async () => {
	let current = discovery();
	let available = true;
	let probes = 0;
	let commands = 0;
	const generations: number[] = [];
	const value = client({
		discovery: async () => current,
		healthProbe: async () => {
			probes++;
			return available;
		},
		command: async () => {
			commands++;
			return healthy;
		},
		healthIntervalMs: 5,
		reconnectBackoff: { initialMs: 2, maxMs: 5 },
	});
	value.onGeneration((generation) => generations.push(generation));
	await value.start();
	available = false;
	const before = probes;
	await eventually(() => probes > before + 1);
	await expect(value.cli(["sdk", "session", "list"])).rejects.toThrow("unavailable");
	available = true;
	const recovering = probes;
	await eventually(() => probes > recovering + 1);
	expect(value.generation).toBe(1);
	current = { ...current, token: "replacement-token" };
	await eventually(() => value.generation === 2);
	expect(generations).toEqual([1, 2]);
	expect(commands).toBe(0);
});

test("issue #189: a broker killed in a loop raises one churn alert and a state change, not N healthy verdicts", async () => {
	let current = discovery();
	let available = true;
	const logs: string[] = [];
	const value = client({
		discovery: async () => current,
		healthProbe: async () => available,
		healthIntervalMs: 2,
		reconnectBackoff: { initialMs: 2, maxMs: 2 },
		log: (line) => logs.push(line),
	});
	await value.start();
	expect(value.respawnChurn()).toBe(false);
	for (let cycle = 1; cycle <= 6; cycle++) {
		available = false;
		await eventually(() => logs.some((line) => line.includes("observing without repair")));
		logs.splice(0, logs.length, ...logs.filter((line) => !line.includes("observing without repair")));
		current = { ...current, pid: 20_000 + cycle };
		available = true;
		await eventually(() => value.generation === cycle + 1);
		if (cycle < 3) expect(value.respawnChurn()).toBe(false);
	}
	expect(value.recentRespawns()).toBe(6);
	expect(value.respawnChurn()).toBe(true);
	const alerts = logs.filter((line) => line.startsWith("broker_respawn_churn "));
	expect(alerts).toEqual([
		expect.stringContaining("broker_respawn_churn respawns=3 windowMs=1800000 pid=20003 generation=4"),
	]);
	// The window drains: churn is an episode, not a permanent verdict.
	expect(value.respawnChurn(Date.now() + 31 * 60_000)).toBe(false);
});

test("each unavailable observation names why the broker was rejected", async () => {
	// 2026-09-23: 484 identical "observing without repair" lines over 81 minutes
	// could not tell a dead discovery pid from a live broker that refused the
	// probe, so nobody could see the daemon was being killed under the gateway.
	const agentDir = await directory();
	await mkdir(join(agentDir, "sdk"));
	const record = { ...discovery(), protocolVersion: 3, host: "127.0.0.1" };
	await writeFile(join(agentDir, "sdk", "broker.json"), JSON.stringify(record));
	let alive = true;
	let probeOk = true;
	const logs: string[] = [];
	const value = client({
		agentDir,
		discovery: undefined,
		isPidAlive: () => alive,
		healthProbe: async () => probeOk,
		healthIntervalMs: 2,
		reconnectBackoff: { initialMs: 2, maxMs: 2 },
		log: (line) => logs.push(line),
	});
	await value.start();
	probeOk = false;
	await eventually(() => logs.some((line) => line.includes("endpoint probe failed for live discovery pid 12345")));
	alive = false;
	await eventually(() => logs.some((line) => line.endsWith("observing without repair: discovery pid 12345 is dead)")));
	await rm(join(agentDir, "sdk", "broker.json"));
	await eventually(() => logs.some((line) => line.endsWith("observing without repair: discovery absent)")));
});

test("a live broker the gateway cannot reach past the bound asks the owner to exit (#246)", async () => {
	// 2026-09-21: a current, healthy broker, yet every request failed
	// broker_unavailable for 37 minutes while service_alive kept ticking; only a
	// gateway restart recovered it.
	let probeOk = true;
	const exceeded: string[] = [];
	const value = client({
		healthProbe: async () => probeOk,
		healthIntervalMs: 2,
		reconnectBackoff: { initialMs: 2, maxMs: 2 },
		liveOutageLimitMs: 40,
		onLiveOutageExceeded: (detail) => exceeded.push(detail),
	});
	await value.start();
	expect(value.outage()).toBeUndefined();
	probeOk = false;
	await eventually(() => exceeded.length > 0);
	expect(exceeded[0]).toMatch(/^live broker unreachable for \d+s: endpoint probe failed for live discovery pid 12345$/);
	expect(value.outage()).toMatch(
		/^broker_unavailable_for=\d+s reason=endpoint probe failed for live discovery pid 12345$/,
	);
	probeOk = true;
	await eventually(() => value.outage() === undefined);
});

test("a dead or absent broker never asks the gateway to exit (#246)", async () => {
	// 2026-09-23: the broker itself was being killed; restarting the gateway on
	// top of it would only stack restarts.
	const agentDir = await directory();
	await mkdir(join(agentDir, "sdk"));
	await writeFile(
		join(agentDir, "sdk", "broker.json"),
		JSON.stringify({ ...discovery(), protocolVersion: 3, host: "127.0.0.1" }),
	);
	let alive = true;
	const exceeded: string[] = [];
	const value = client({
		agentDir,
		discovery: undefined,
		isPidAlive: () => alive,
		healthProbe: async () => alive,
		healthIntervalMs: 2,
		reconnectBackoff: { initialMs: 2, maxMs: 2 },
		liveOutageLimitMs: 20,
		onLiveOutageExceeded: (detail) => exceeded.push(detail),
	});
	await value.start();
	alive = false;
	await eventually(() => value.outage()?.endsWith("reason=discovery pid 12345 is dead") === true);
	await Bun.sleep(80);
	expect(value.outage()).toContain("reason=discovery pid 12345 is dead");
	expect(exceeded).toEqual([]);
});

test("rejects retarget arguments before executing commands", async () => {
	let calls = 0;
	const value = client({
		command: async () => {
			calls++;
			return healthy;
		},
	});
	for (const args of [
		["sdk", "session", "list", "--agent-dir", "/other"],
		["sdk", "serve", "--agent-dir=/other"],
		["sdk", "session", "list", "--agent-dir", value.agentDir],
		["sdk", "serve", "--cwd=/other"],
		["--version"],
	]) {
		expect(() => value.cli(args)).toThrow();
	}
	expect(calls).toBe(0);
});

test("CLI, version preflight and stdio relay use exactly one executable, cwd and environment", async () => {
	const cwd = await directory();
	const agentDir = join(cwd, "operator-agent");
	const invocations: Array<{ cmd: string[]; cwd: string; env: Record<string, string> }> = [];
	let relayKills = 0;
	const spawn = ((options: { cmd: string[]; cwd: string; env: Record<string, string> }) => {
		invocations.push(options);
		// The preflight relay probe targets the health session id and must exit
		// on its own; only a real owned-session relay stays open until killed.
		const relay = options.cmd.includes("serve") && !options.cmd.includes(HEALTH_PROBE_SESSION_ID);
		let finish = (_code: number) => {};
		const exited = relay
			? new Promise<number>((resolve) => {
					finish = resolve;
				})
			: Promise.resolve(0);
		return {
			exited,
			stdout: new Blob([
				options.cmd.includes("--version") ? "gjc/0.16.3\n" : relay ? "event\n" : healthy.stdout,
			]).stream(),
			stderr: new Blob([]).stream(),
			kill: () => {
				if (relay) relayKills++;
				finish(0);
			},
		};
	}) as unknown as SpawnFn;
	const value = client({ cwd, agentDir, executable: "/opt/user-selected/gjc", spawn, command: undefined });
	await value.preflight();
	await value.start();
	const stream = value.openStream("owned-session");
	const lines: string[] = [];
	for await (const line of stream.lines) lines.push(line);
	await value.stop();
	expect(lines).toEqual(["event"]);
	expect(value.gjcVersion).toBe("0.16.3");
	expect(invocations.length).toBe(4);
	const probe = invocations.find((invocation) => invocation.cmd.includes(HEALTH_PROBE_SESSION_ID));
	expect(probe?.cmd.slice(1)).toEqual(["sdk", "serve", "--stdio", "--session", HEALTH_PROBE_SESSION_ID]);
	for (const invocation of invocations) {
		expect(invocation.cmd[0]).toBe("/opt/user-selected/gjc");
		expect(invocation.cwd).toBe(cwd);
		expect(invocation.env).toEqual(invocations[0]!.env);
		expect(invocation.env.GJC_CODING_AGENT_DIR).toBe(agentDir);
		expect(invocation.env.PI_CODING_AGENT_DIR).toBe(agentDir);
	}
	expect(relayKills).toBeGreaterThan(0);
});

test("bounded CLI semaphore never exceeds four children and queued commands time out", async () => {
	let active = 0;
	let maximum = 0;
	const pending: Array<() => void> = [];
	const value = client({
		command: async () => {
			active++;
			maximum = Math.max(maximum, active);
			await new Promise<void>((resolve) => pending.push(resolve));
			active--;
			return healthy;
		},
	});
	const commands = Array.from({ length: 4 }, () => value.cli(["sdk", "session", "list"], { timeoutMs: 500 }));
	await eventually(() => active === 4);
	await expect(value.cli(["sdk", "session", "list"], { timeoutMs: 5 })).rejects.toThrow("timed out");
	expect(maximum).toBe(4);
	for (const resolve of pending) resolve();
	await Promise.all(commands);
});

test("stop rejects queued/future requests and closes only gateway spawned relays", async () => {
	let kills = 0;
	const spawn = (() => {
		let finish = (_code: number) => {};
		const exited = new Promise<number>((resolve) => {
			finish = resolve;
		});
		return {
			exited,
			stdout: new ReadableStream(),
			kill: () => {
				kills++;
				finish(0);
			},
		};
	}) as unknown as SpawnFn;
	const value = client({ spawn });
	await value.start();
	const relay = value.openStream("owned-session");
	await value.stop();
	relay.close();
	expect(kills).toBeGreaterThan(0);
	await expect(value.cli(["sdk", "session", "list"])).rejects.toThrow("stopped");
	expect(() => value.openStream("owned-session")).toThrow("unavailable");
});

test("global client preserves user broker running beyond gateway stop", async () => {
	let requests = 0;
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch(request, server) {
			return server.upgrade(request) ? undefined : new Response("upgrade required", { status: 400 });
		},
		websocket: {
			open(socket) {
				socket.send(JSON.stringify({ type: "broker_hello", protocolVersion: 3 }));
			},
			message(socket, data) {
				const request = JSON.parse(String(data));
				requests++;
				expect(request.operation).toBe("session.get_endpoint");
				expect(request.input).toEqual({ sessionId: "00000000-0000-4000-8000-000000000000" });
				socket.send(JSON.stringify({ type: "broker_response", id: request.id, ok: true, result: { sessions: [] } }));
			},
		},
	});
	try {
		const record = { ...discovery(), url: `ws://127.0.0.1:${server.port}` };
		const value = client({ discovery: async () => record, healthProbe: undefined });
		await value.start();
		await value.stop();
		expect(await probeBrokerEndpoint(record, 500)).toBe(true);
		expect(requests).toBe(2);
	} finally {
		await server.stop(true);
	}
});

test("probe bounds a live hello-only fake daemon without signalling it", async () => {
	const server = Bun.serve({
		hostname: "127.0.0.1",
		port: 0,
		fetch(request, server) {
			return server.upgrade(request) ? undefined : new Response(null, { status: 400 });
		},
		websocket: {
			open(socket) {
				socket.send(JSON.stringify({ type: "broker_hello", protocolVersion: 3 }));
			},
			message() {},
		},
	});
	try {
		expect(await probeBrokerEndpoint({ ...discovery(), url: `ws://127.0.0.1:${server.port}` }, 10)).toBe(false);
	} finally {
		await server.stop(true);
	}
});

test("discovery refuses stale, future, foreign endpoints and dead PIDs without deleting anything", async () => {
	const path = join(await directory(), "broker.json");
	const now = Date.now();
	const valid = { ...discovery(), heartbeatAt: now, protocolVersion: 3, host: "127.0.0.1" };
	for (const invalid of [
		{ ...valid, heartbeatAt: now - 20_000 },
		{ ...valid, heartbeatAt: now + 20_000 },
		{ ...valid, protocolVersion: 2 },
		{ ...valid, url: "ws://127.0.0.1:80@evil.example" },
		{ ...valid, pid: -1 },
	]) {
		const text = JSON.stringify(invalid);
		await writeFile(path, text);
		expect(await readBrokerDiscovery(path, () => true, now)).toBeUndefined();
		expect(await readFile(path, "utf8")).toBe(text);
	}
	await writeFile(path, JSON.stringify(valid));
	expect(await readBrokerDiscovery(path, () => false, now)).toBeUndefined();
	expect(await readBrokerDiscovery(path, () => true, now)).toEqual({
		pid: valid.pid,
		url: valid.url,
		token: valid.token,
		heartbeatAt: now,
	});
});

test("runtime capability requires a real session envelope and explicit all scope", async () => {
	expect(brokerHealthArgs()).toEqual(["sdk", "session", "list", "--scope", "all"]);
	expect(isHealthySessionList({ ...healthy, stdout: "USAGE gjc sdk" })).toBe(false);
	await expect(preflightGjcRuntime(async () => ({ ...healthy, stdout: "gjc/0.15.6" }))).rejects.toThrow(
		"requires gjc >= 0.16.0",
	);
	await expect(preflightGjcRuntime(async () => ({ ...healthy, stdout: "gjc/0.16.0" }))).resolves.toEqual({
		version: "0.16.0",
	});
	await expect(
		preflightGjcRuntime(
			async () => ({ ...healthy, stdout: "gjc/0.16.3" }),
			undefined,
			async () => ({ ...healthy, stdout: "{}" }),
		),
	).rejects.toThrow("session-list");
	for (const url of [
		"ws://localhost:123",
		"ws://127.0.0.1:123/path",
		"wss://127.0.0.1:123",
		"ws://127.0.0.1:123?token=bad",
	]) {
		expect(isLoopbackWebSocketUrl(url)).toBe(false);
	}
	expect(isLoopbackWebSocketUrl("ws://127.0.0.1:123")).toBe(true);
});

test("rejects relative executable and untrusted project dotenv path selection", async () => {
	expect(() => client({ executable: "gjc" })).toThrow("absolute");
	const cwd = await directory();
	await writeFile(
		join(cwd, ".env"),
		`GJC_CODING_AGENT_DIR=${process.env.GJC_CODING_AGENT_DIR ?? "/untrusted"}\nHOME=${process.env.HOME}\n`,
	);
	expect(() => client({ cwd })).toThrow("project-declared");
});

test("resolves trusted user environment and home-relative config names without a private fallback", async () => {
	const cwd = await directory();
	const names = [
		"HOME",
		"GJC_EXECUTABLE",
		"GJC_CODING_AGENT_DIR",
		"PI_CODING_AGENT_DIR",
		"GJC_CONFIG_DIR",
		"PI_CONFIG_DIR",
	] as const;
	const before = Object.fromEntries(names.map((name) => [name, process.env[name]]));
	try {
		process.env.HOME = cwd;
		process.env.GJC_EXECUTABLE = "/mnt/offloading/.bun/bin/gjc";
		process.env.GJC_CODING_AGENT_DIR = join(cwd, "user-global-agent");
		process.env.PI_CODING_AGENT_DIR = join(cwd, "legacy-agent");
		process.env.GJC_CONFIG_DIR = "/custom-config";
		delete process.env.PI_CONFIG_DIR;
		const direct = client({ cwd, executable: undefined, agentDir: undefined });
		expect(direct.executable).toBe("/mnt/offloading/.bun/bin/gjc");
		expect(direct.agentDir).toBe(join(cwd, "user-global-agent"));
		delete process.env.GJC_CODING_AGENT_DIR;
		expect(client({ cwd, agentDir: undefined }).agentDir).toBe(join(cwd, "legacy-agent"));
		delete process.env.PI_CODING_AGENT_DIR;
		expect(client({ cwd, agentDir: undefined }).agentDir).toBe(join(cwd, "custom-config", "agent"));
		process.env.GJC_CONFIG_DIR = "../escape";
		expect(client({ cwd, agentDir: undefined }).agentDir).toBe(join(cwd, ".gjc", "agent"));
	} finally {
		for (const name of names) {
			if (before[name] === undefined) delete process.env[name];
			else process.env[name] = before[name];
		}
	}
});

test("preflight fails closed when the stream relay rejects its argv (usage exit 2)", async () => {
	const run = async (args: readonly string[]) =>
		args[0] === "--version" ? { exitCode: 0, stdout: "gjc/0.16.6\n", stderr: "" } : healthy;
	const sdk = async (args: readonly string[]) =>
		args[1] === "serve"
			? {
					exitCode: 2,
					stdout: "USAGE\n  $ gjc sdk [ACTION] [FLAGS]\n",
					stderr: "gjc sdk serve: unknown argument: --agent-dir\n",
				}
			: healthy;
	await expect(preflightGjcRuntime(run, "0.15.6", sdk)).rejects.toThrow("stream relay rejected its argv");
	// A relay that exits for a runtime reason (unindexed probe id) is not an argv rejection.
	const runtime = async (args: readonly string[]) =>
		args[1] === "serve" ? { exitCode: 1, stdout: "", stderr: "not_found: session is not indexed\n" } : healthy;
	await expect(preflightGjcRuntime(run, "0.15.6", runtime)).resolves.toEqual({ version: "0.16.6" });
});

test("the gjc 0.17.4 structured usage envelope is a usage rejection; runtime envelopes are not", () => {
	// Verbatim shape of the official 0.17.4 binary rejecting `sdk session inspect <id> --repo <dir>`.
	const rejected = {
		exitCode: 2,
		stdout: "",
		stderr:
			'COMMAND ["sdk","session","inspect"]\nERROR {"code":"usage","category":"usage","message":"The command arguments are invalid.","retryability":"no","outcomeCertainty":"not-applied","references":[]}\n',
	};
	expect(isUsageRejection(rejected)).toBe(true);
	// Same binary, parsed argv, broker absent: exit 1 with a runtime code.
	expect(
		isUsageRejection({
			exitCode: 1,
			stdout: "",
			stderr: 'ERROR {"code":"endpoint_stale","category":"unavailable"}\n',
		}),
	).toBe(false);
	// Exit 2 alone is not proof of an argv rejection.
	expect(isUsageRejection({ exitCode: 2, stdout: "", stderr: 'ERROR {"code":"broker_unavailable"}\n' })).toBe(false);
});

test("every sdk session call adds JSON output and binds --agent-dir at the leaf", async () => {
	const calls: string[][] = [];
	const value = client({
		command: async (args) => {
			calls.push([...args]);
			return healthy;
		},
	});
	await value.start();
	for (const leaf of [
		["list", "--scope", "all"],
		["inspect", "s-1"],
		["status", "s-1", "op-1"],
		["send", "s-1", "--text", "hello", "--op-ref", "op-1", "--wait", "--json"],
		["tail", "s-1", "--strict", "--all-events", "--json"],
		["tail", "s-1", "--cursor", "cursor-1"],
		["close", "s-1"],
		["retire", "s-1"],
		["raw", "control", "s-1", "--op", "model.set", "--json-input", '{"id":"test-model"}'],
		["raw", "query", "s-1", "--query", "transcript.list"],
		["raw", "global", "--op", "session.close"],
		["raw", "global", "--op", "session.create", "--idempotency-key", "create-1", "--json-input", '{"cwd":"/work"}'],
	]) {
		await value.cli(["sdk", "session", ...leaf]);
	}
	for (const args of calls) {
		expect(args[2]).not.toBe("--agent-dir");
		expect(args.filter((arg) => arg === "--json")).toHaveLength(1);
		expect(args.slice(-2)).toEqual(["--agent-dir", value.agentDir]);
	}
	expect(calls).toHaveLength(12);
	expect(calls[11]).toContain('{"cwd":"/work"}');
});

test("session payload values equal to --json are preserved while output mode is added", async () => {
	let invocation: readonly string[] | undefined;
	const value = client({
		command: async (args) => {
			invocation = [...args];
			return healthy;
		},
	});
	const args = ["sdk", "session", "send", "s-1", "--text", "--json"];
	await value.cli(args);
	expect(invocation).toEqual([...args, "--json", "--agent-dir", value.agentDir]);
	expect(args).toEqual(["sdk", "session", "send", "s-1", "--text", "--json"]);
	await value.cli([...args, "--json"]);
	expect(invocation).toEqual([...args, "--json", "--agent-dir", value.agentDir]);
	const cursorArgs = ["sdk", "session", "tail", "s-1", "--cursor", "--json"];
	await value.cli(cursorArgs);
	expect(invocation).toEqual([...cursorArgs, "--json", "--agent-dir", value.agentDir]);
	expect(() => value.cli(["sdk", "session", "list", "--json", "--json"])).toThrow("duplicate --json flags");
	expect(() => value.cli(["sdk", "session", "list", "--", "--json"])).toThrow("option delimiters");
});

test("relay stdout and stderr are decoded independently: a multibyte character split across stdout chunks survives interleaved stderr and stderr EOF", async () => {
	// A JSON frame carrying Korean text, cut in the middle of "가" (3 bytes) at a
	// chunk boundary, with a stderr line delivered between the two halves and
	// stderr closing before stdout finishes. One shared streaming decoder would
	// take the continuation bytes from stderr (or flush U+FFFD on stderr EOF).
	const frame = `{"type":"event","kind":"message_end","text":"안녕 가재 🦞"}\n`;
	const bytes = new TextEncoder().encode(frame);
	const prefixBytes = new TextEncoder().encode(frame.slice(0, frame.indexOf("가"))).length + 1;
	const first = bytes.subarray(0, prefixBytes);
	const second = bytes.subarray(prefixBytes);
	const stdoutQueue: Array<Uint8Array | null> = [];
	let stdoutWake: (() => void) | undefined;
	const stdout = new ReadableStream<Uint8Array>({
		async pull(controller) {
			while (stdoutQueue.length === 0) await new Promise<void>((resolve) => (stdoutWake = resolve));
			const chunk = stdoutQueue.shift();
			if (chunk === null || chunk === undefined) controller.close();
			else controller.enqueue(chunk);
		},
	});
	const pushStdout = (chunk: Uint8Array | null) => {
		stdoutQueue.push(chunk);
		stdoutWake?.();
	};
	let finish = (_code: number) => {};
	const exited = new Promise<number>((resolve) => {
		finish = resolve;
	});
	const spawn = (() => ({
		exited,
		stdout,
		stderr: new Blob(['{"ok":false,"error":{"code":"warning"}}\n']).stream(),
		kill: () => finish(0),
	})) as unknown as SpawnFn;
	const value = client({ spawn });
	await value.start();
	const relay = value.openStream("owned-session");
	const lines: string[] = [];
	const consumed = (async () => {
		for await (const line of relay.lines) lines.push(line);
	})();
	pushStdout(first);
	await Bun.sleep(20); // stderr line and stderr EOF land while stdout's character is half-read
	pushStdout(second);
	pushStdout(null);
	await consumed;
	await value.stop();
	expect(lines).toContain(frame.trimEnd());
	expect(lines).toContain('{"ok":false,"error":{"code":"warning"}}');
	expect(lines.join("\n")).not.toContain("\uFFFD");
});
