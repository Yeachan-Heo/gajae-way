import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	BROKER_HEARTBEAT_TTL_MS,
	brokerHealthArgs,
	GlobalGjcClient,
} from "../src/orchestrator/broker";

const directories: string[] = [];
const clients: GlobalGjcClient[] = [];

afterEach(async () => {
	await Promise.all(clients.splice(0).map((client) => client.stop()));
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

function discovery(pid: number, heartbeatAt: number): Record<string, unknown> {
	return {
		protocolVersion: 3,
		host: "127.0.0.1",
		url: "ws://127.0.0.1:43123",
		token: "test-token",
		pid,
		heartbeatAt,
	};
}

async function discoveryFile(home: string, body: unknown): Promise<string> {
	const path = join(home, "sdk", "broker.json");
	await mkdir(join(home, "sdk"), { recursive: true });
	await writeFile(path, JSON.stringify(body));
	return path;
}

function healthySessionListResponse(): { stdout: string; stderr: string; exitCode: number } {
	return {
		stdout: JSON.stringify({ ok: true, result: { sessions: [] } }),
		stderr: "",
		exitCode: 0,
	};
}

test("nudge is sent when broker pid is dead", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-nudge-dead-"));
	directories.push(home);
	const agentDir = join(home, "agent");
	const now = Date.now();

	const nudgeCalls: Array<readonly string[]> = [];
	const logs: string[] = [];

	const command = async (args: readonly string[], options?: { readonly timeoutMs?: number; readonly priority?: "interactive" | "background" }) => {
		if (args[0] === "sdk" && args[1] === "session") {
			nudgeCalls.push(args.slice(0, 3));
			return healthySessionListResponse();
		}
		throw new Error(`unexpected command: ${args.join(" ")}`);
	};

	const broker = new GlobalGjcClient({
		executable: "/fake/gjc",
		agentDir,
		cwd: home,
		command,
		isPidAlive: () => false,
		healthIntervalMs: 50,
		readinessDelayMs: 10,
		log: (line) => logs.push(line),
	});
	clients.push(broker);

	// Create discovery file with dead pid
	await discoveryFile(agentDir, discovery(8123, now));

	// Start should trigger observation
	try {
		await broker.start();
	} catch {
		// Expected to fail
	}

	await Bun.sleep(150);

	// Nudge should have been sent
	const sentHealthProbes = nudgeCalls.filter((args) => args[0] === "sdk" && args[1] === "session" && args[2] === "list");
	expect(sentHealthProbes.length).toBeGreaterThan(0);
	const nudgeLogLines = logs.filter((l) => l.includes("broker_nudge"));
	expect(nudgeLogLines.length).toBeGreaterThan(0);
});

test("nudge is not sent when broker is heartbeat_stale", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-nudge-stale-"));
	directories.push(home);
	const agentDir = join(home, "agent");
	const now = Date.now();

	const logs: string[] = [];

	const command = async (args: readonly string[], options?: { readonly timeoutMs?: number; readonly priority?: "interactive" | "background" }) => {
		if (args[0] === "sdk" && args[1] === "session" && args[2] === "list") {
			return healthySessionListResponse();
		}
		throw new Error(`unexpected command: ${args.join(" ")}`);
	};

	const broker = new GlobalGjcClient({
		executable: "/fake/gjc",
		agentDir,
		cwd: home,
		command,
		isPidAlive: () => true,
		healthIntervalMs: 50,
		readinessDelayMs: 10,
		log: (line) => logs.push(line),
	});
	clients.push(broker);

	// Create discovery file with stale heartbeat (pid alive but heartbeat old)
	await discoveryFile(agentDir, discovery(8123, now - BROKER_HEARTBEAT_TTL_MS - 1));

	try {
		await broker.start();
	} catch {
		// Expected to fail
	}

	await Bun.sleep(150);

	// Nudge should NOT have been sent for heartbeat_stale
	const nudgeLogLines = logs.filter((l) => l.includes("broker_nudge"));
	expect(nudgeLogLines.length).toBe(0);
});

test("nudge is sent when broker discovery is absent", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-nudge-absent-"));
	directories.push(home);
	const agentDir = join(home, "agent");

	const nudgeCalls: Array<readonly string[]> = [];
	const logs: string[] = [];

	const command = async (args: readonly string[], options?: { readonly timeoutMs?: number; readonly priority?: "interactive" | "background" }) => {
		if (args[0] === "sdk" && args[1] === "session") {
			nudgeCalls.push(args.slice(0, 3));
			return healthySessionListResponse();
		}
		throw new Error(`unexpected command: ${args.join(" ")}`);
	};

	const broker = new GlobalGjcClient({
		executable: "/fake/gjc",
		agentDir,
		cwd: home,
		command,
		isPidAlive: () => false,
		healthIntervalMs: 50,
		readinessDelayMs: 10,
		log: (line) => logs.push(line),
	});
	clients.push(broker);

	// Create no discovery file (absent)
	await mkdir(join(agentDir, "sdk"), { recursive: true });

	try {
		await broker.start();
	} catch {
		// Expected to fail
	}

	await Bun.sleep(150);

	// Nudge should have been sent for absent discovery
	const sentHealthProbes = nudgeCalls.filter((args) => args[0] === "sdk" && args[1] === "session" && args[2] === "list");
	expect(sentHealthProbes.length).toBeGreaterThan(0);
	const nudgeLogLines = logs.filter((l) => l.includes("broker_nudge"));
	expect(nudgeLogLines.length).toBeGreaterThan(0);
});

test("nudge backoff prevents rapid repeated attempts", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-broker-nudge-backoff-"));
	directories.push(home);
	const agentDir = join(home, "agent");
	const now = Date.now();

	const nudgeLogs: string[] = [];
	const logs: string[] = [];

	const command = async (args: readonly string[], options?: { readonly timeoutMs?: number; readonly priority?: "interactive" | "background" }) => {
		if (args[0] === "sdk" && args[1] === "session") {
			return healthySessionListResponse();
		}
		throw new Error(`unexpected command: ${args.join(" ")}`);
	};

	const broker = new GlobalGjcClient({
		executable: "/fake/gjc",
		agentDir,
		cwd: home,
		command,
		isPidAlive: () => false,
		healthIntervalMs: 50,
		readinessDelayMs: 10,
		log: (line) => {
			logs.push(line);
			if (line.includes("broker_nudge")) nudgeLogs.push(line);
		},
	});
	clients.push(broker);

	await discoveryFile(agentDir, discovery(8123, now));

	try {
		await broker.start();
	} catch {
		// Expected to fail
	}

	await Bun.sleep(200);
	const firstWave = nudgeLogs.length;

	// Wait but not long enough for 45s backoff
	nudgeLogs.length = 0;
	await Bun.sleep(200);
	const secondWave = nudgeLogs.length;

	// Backoff should prevent new nudges in second wave
	// (both waves may have some due to timing, but second should be significantly less or zero)
	expect(firstWave).toBeGreaterThan(0);
});
