import { afterEach, expect, test } from "bun:test";
import { realpathSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BROKER_HEARTBEAT_TTL_MS, GlobalGjcClient } from "../src/orchestrator/broker";

const directories: string[] = [];
const clients: GlobalGjcClient[] = [];
afterEach(async () => {
	await Promise.all(clients.splice(0).map((client) => client.stop()));
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

const healthy = { stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "", exitCode: 0 };

async function fixture(state: "pid_dead" | "absent" | "heartbeat_stale", failure?: "exit" | "throw") {
	const home = realpathSync(await mkdtemp(join(tmpdir(), "gajaeway-broker-nudge-")));
	directories.push(home);
	const agentDir = join(home, "agent");
	await mkdir(join(agentDir, "sdk"), { recursive: true });
	const record = {
		protocolVersion: 3,
		host: "127.0.0.1",
		url: "ws://127.0.0.1:43123",
		token: "test-token",
		pid: 8123,
		heartbeatAt: Date.now() - (state === "heartbeat_stale" ? BROKER_HEARTBEAT_TTL_MS + 1 : 0),
	};
	if (state !== "absent") await writeFile(join(agentDir, "sdk", "broker.json"), JSON.stringify(record));
	let available = true;
	let now = 0;
	const calls: Array<{ args: readonly string[]; priority?: string; timeoutMs?: number }> = [];
	const logs: string[] = [];
	let completed: (() => void) | undefined;
	const broker = new GlobalGjcClient({
		executable: "/fake/gjc",
		agentDir,
		cwd: home,
		discovery: async () => (available ? record : undefined),
		healthProbe: () => true,
		isPidAlive: () => state === "heartbeat_stale",
		releaseBrokerScope: null,
		healthIntervalMs: 2_000_000_000,
		nudgeClock: () => now,
		command: async (args, options) => {
			calls.push({ args, ...options });
			if (failure === "throw") throw new Error("launcher failed");
			return failure === "exit" ? { ...healthy, exitCode: 1 } : healthy;
		},
		log: (line) => {
			logs.push(line);
			if (line.startsWith("broker_nudge_")) completed?.();
		},
	});
	clients.push(broker);
	await broker.start();
	available = false;
	return {
		broker,
		calls,
		logs,
		agentDir,
		setNow(value: number) {
			now = value;
		},
		async tick(expectNudge = false) {
			const done = expectNudge
				? new Promise<void>((resolve) => {
						completed = resolve;
					})
				: undefined;
			expect(await broker.observe()).toBe(false);
			if (done) await done;
			// Drain the nudge's finally handler, not a wall-clock sleep.
			await Promise.resolve();
			completed = undefined;
		},
	};
}

test("pid_dead gets exactly one nudge inside the window and a second at its boundary", async () => {
	const f = await fixture("pid_dead");
	await f.tick(true);
	for (const now of [1, 1_000, 44_999]) {
		f.setNow(now);
		await f.tick();
	}
	expect(f.calls).toHaveLength(1);
	expect(f.logs).toEqual(["broker_nudge_success triggered autostart"]);
	expect(f.calls[0].args.slice(0, 3)).toEqual(["sdk", "session", "list"]);
	expect(f.calls[0].args).toContain("--agent-dir");
	expect(f.calls[0].args).toContain(f.agentDir);
	expect(f.calls[0].priority).toBe("background");
	f.setNow(45_000);
	await f.tick(true);
	expect(f.calls).toHaveLength(2);
});

for (const failure of ["exit", "throw"] as const) {
	test(`a failed nudge (${failure}) retains its backoff`, async () => {
		const f = await fixture("pid_dead", failure);
		await f.tick(true);
		for (const now of [1, 20_000, 44_999]) {
			f.setNow(now);
			await f.tick();
		}
		expect(f.calls).toHaveLength(1);
		expect(f.logs).toEqual([
			failure === "exit" ? "broker_nudge_failed exitCode=1" : "broker_nudge_error: launcher failed",
		]);
		f.setNow(45_000);
		await f.tick(true);
		expect(f.calls).toHaveLength(2);
	});
}

test("absent discovery gets nudged but ordinary commands stay fenced after start", async () => {
	const f = await fixture("absent");
	await f.tick(true);
	await expect(f.broker.cli(["sdk", "session", "list"])).rejects.toThrow("client stopped or broker unavailable");
	await expect(f.broker.cli(["sdk", "session", "list"], { priority: "background" })).rejects.toThrow(
		"client stopped or broker unavailable",
	);
	expect(f.calls).toHaveLength(1);
	await f.tick();
	expect(f.calls).toHaveLength(1);
});

test("heartbeat_stale never gets nudged even beyond the backoff window", async () => {
	const f = await fixture("heartbeat_stale");
	for (const now of [0, 1_000, 45_000, 90_000]) {
		f.setNow(now);
		await f.tick();
	}
	expect(f.calls).toHaveLength(0);
	expect(f.logs).toEqual([]);
	await expect(f.broker.cli(["sdk", "session", "list"])).rejects.toThrow("broker unavailable");
});

test("ordinary commands forward their priority and requested timeout to the command runner", async () => {
	const f = await fixture("absent");
	await f.broker.cli(["sdk", "session", "list"], { timeoutMs: 1000 });
	await f.broker.cli(["sdk", "session", "list"], { priority: "background", timeoutMs: 2000 });
	expect(f.calls.map((call) => call.priority)).toEqual(["interactive", "background"]);
	expect(f.calls[0].timeoutMs).toBeGreaterThan(0);
	expect(f.calls[0].timeoutMs).toBeLessThanOrEqual(1000);
	expect(f.calls[1].timeoutMs).toBeGreaterThan(0);
	expect(f.calls[1].timeoutMs).toBeLessThanOrEqual(2000);
});

test("background commands retain their two-slot cap and interactive waiters dequeue first", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-priority-"));
	directories.push(home);
	const calls: Array<{ name: string; priority?: string; finish: () => void }> = [];
	const interactiveStarted = Promise.withResolvers<void>();
	const backgroundStarted = Promise.withResolvers<void>();
	const broker = new GlobalGjcClient({
		executable: "/fake/gjc",
		agentDir: join(home, "agent"),
		cwd: home,
		command: (args, options) =>
			new Promise((resolve) => {
				calls.push({ name: args[3], priority: options?.priority, finish: () => resolve(healthy) });
				if (args[3] === "i3") interactiveStarted.resolve();
				if (args[3] === "b3") backgroundStarted.resolve();
			}),
	});
	clients.push(broker);
	const pending = [
		broker.cli(["sdk", "session", "get", "b1"], { priority: "background" }),
		broker.cli(["sdk", "session", "get", "b2"], { priority: "background" }),
		broker.cli(["sdk", "session", "get", "b3"], { priority: "background" }),
		broker.cli(["sdk", "session", "get", "i1"]),
		broker.cli(["sdk", "session", "get", "i2"]),
		broker.cli(["sdk", "session", "get", "i3"]),
	];
	expect(calls.map((call) => call.name)).toEqual(["b1", "b2", "i1", "i2"]);
	calls[0].finish();
	await pending[0];
	await interactiveStarted.promise;
	expect(calls.map((call) => call.name)).toEqual(["b1", "b2", "i1", "i2", "i3"]);
	expect(calls[4].priority).toBe("interactive");
	calls[4].finish();
	await pending[5];
	await backgroundStarted.promise;
	expect(calls[5].name).toBe("b3");
	expect(calls[5].priority).toBe("background");
	for (const call of calls) call.finish();
	await Promise.all(pending);
});
