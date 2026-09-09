import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, readdir, readFile, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type BrokerDiscovery,
	brokerHealthArgs,
	GlobalGjcClient,
	isHealthySessionList,
	isLoopbackWebSocketUrl,
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
	for (const invocation of invocations) {
		expect(invocation.cmd[invocation.cmd.indexOf("--agent-dir") + 1]).toBe(direct.agentDir);
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
	expect(calls).toEqual([["sdk", "session", "--agent-dir", value.agentDir, "list", "--scope", "all"]]);
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
		const relay = options.cmd.includes("serve");
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
	expect(invocations.length).toBe(3);
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
				expect(request.operation).toBe("session.list");
				expect(request.input.resolveSessionId).toBe("00000000-0000-4000-8000-000000000000");
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
	await expect(preflightGjcRuntime(async () => ({ ...healthy, stdout: "gjc/0.14.0" }))).rejects.toThrow("requires");
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
