import { afterEach, expect, test } from "bun:test";
import type { BrokerDiscovery } from "../src/orchestrator/broker";
import { GlobalGjcClient, isBrokerArgv } from "../src/orchestrator/broker";

const clients: GlobalGjcClient[] = [];
const healthy = { exitCode: 0, stdout: JSON.stringify({ ok: true, result: { sessions: [] } }), stderr: "" };

afterEach(async () => {
	await Promise.all(clients.splice(0).map((client) => client.stop().catch(() => {})));
});

function discovery(pid: number, heartbeatAt = Date.now()): BrokerDiscovery {
	return {
		pid,
		url: "ws://127.0.0.1:12345",
		token: "test-token",
		heartbeatAt,
	};
}

function client(options: ConstructorParameters<typeof GlobalGjcClient>[0] = {}): GlobalGjcClient {
	const FAKE_EXECUTABLE = "/tmp/fake-gjc-broker-recycle";

	const agentDir = options.agentDir || "/tmp/fake-agent-dir";
	const value = new GlobalGjcClient({
		command: async () => healthy,
		healthProbe: async () => true,
		log: () => {},
		agentDir,
		executable: FAKE_EXECUTABLE,
		...options,
	});
	clients.push(value);
	return value;
}

async function eventually(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate()).toBe(true);
}

// Unit tests for isBrokerArgv matcher - pure logic, no timing
test("isBrokerArgv: broker-internal with matching agentDir -> true", () => {
	const argv = ["bun", "cli.ts", "sdk", "broker-internal", "--agent-dir", "/home/user/.gjc/agent", "--stdio"];
	expect(isBrokerArgv(argv, "/home/user/.gjc/agent")).toBe(true);
});

test("isBrokerArgv: broker-internal with different agentDir -> false", () => {
	const argv = ["bun", "cli.ts", "sdk", "broker-internal", "--agent-dir", "/home/user/.gjc/agent"];
	expect(isBrokerArgv(argv, "/other/agent")).toBe(false);
});

test("isBrokerArgv: CLI child (session list) -> false", () => {
	const argv = ["bun", "cli.ts", "sdk", "session", "list", "--scope", "all"];
	expect(isBrokerArgv(argv, "/home/user/.gjc/agent")).toBe(false);
});

test("isBrokerArgv: missing broker-internal -> false", () => {
	const argv = ["bun", "cli.ts", "sdk", "serve", "--agent-dir", "/home/user/.gjc/agent"];
	expect(isBrokerArgv(argv, "/home/user/.gjc/agent")).toBe(false);
});

test("isBrokerArgv: missing sdk -> false", () => {
	const argv = ["bun", "cli.ts", "broker-internal", "--agent-dir", "/home/user/.gjc/agent"];
	expect(isBrokerArgv(argv, "/home/user/.gjc/agent")).toBe(false);
});

test("isBrokerArgv: --agent-dir present but value mismatch -> false", () => {
	const argv = ["bun", "cli.ts", "sdk", "broker-internal", "--agent-dir", "/wrong/dir"];
	expect(isBrokerArgv(argv, "/home/user/.gjc/agent")).toBe(false);
});

// Integration tests - focused on key behaviors
test("attempts SIGTERM then SIGKILL when pidAlive stays true", async () => {
	let probeOk = true;
	const logs: string[] = [];
	const killRecord: Array<{ pid: number; signal: string }> = [];
	const nudgeCalls: Array<readonly string[]> = [];

	const value = client({
		discovery: async () => discovery(12345),
		healthProbe: async () => probeOk,
		isPidAlive: () => true, // Stay alive to trigger SIGKILL after grace period
		isBrokerProcess: async () => true,
		killPid: (pid: number, signal: string = "SIGTERM") => {
			killRecord.push({ pid, signal });
		},
		command: async (args) => {
			nudgeCalls.push(args);
			return healthy;
		},
		healthIntervalMs: 5,
		reconnectBackoff: { initialMs: 5, maxMs: 5 },
		recycleWedgedBrokerStrikeThreshold: 1,
		recycleGracePeriodMs: 100, // Fast grace period for testing
		log: (line) => logs.push(line),
	});

	await value.start();
	probeOk = false;

	// Wait for both SIGTERM and SIGKILL to be sent
	await eventually(() => killRecord.length === 2);

	// Verify exact sequence: SIGTERM then SIGKILL
	expect(killRecord).toEqual([
		{ pid: 12345, signal: "SIGTERM" },
		{ pid: 12345, signal: "SIGKILL" },
	]);

	// Verify autostart nudge was triggered with correct args
	await eventually(() => nudgeCalls.some((args) => args[0] === "sdk" && args[1] === "session" && args[2] === "list"));
	expect(nudgeCalls.some((args) => args.includes("--scope") && args.includes("all"))).toBe(true);
});

test("isBrokerProcess false -> no signals, logs broker_recycle_refused", async () => {
	let probeOk = true;
	const logs: string[] = [];
	const killRecord: Array<{ pid: number; signal: string }> = [];

	const value = client({
		discovery: async () => discovery(12345),
		healthProbe: async () => probeOk,
		isPidAlive: () => true,
		isBrokerProcess: async () => false, // Identity check fails
		killPid: (pid: number, signal: string = "SIGTERM") => {
			killRecord.push({ pid, signal });
		},
		healthIntervalMs: 5,
		reconnectBackoff: { initialMs: 5, maxMs: 5 },
		recycleWedgedBrokerStrikeThreshold: 1,
		log: (line) => logs.push(line),
	});

	await value.start();
	probeOk = false;

	// Wait for recycle attempt (which should be refused)
	await eventually(() => logs.some((l) => l.includes("broker_recycle_refused")));

	// Verify no signals were sent
	expect(killRecord.length).toBe(0);

	// Verify the refusal was logged
	expect(logs.some((l) => l.includes("broker_recycle_refused") && l.includes("identity_check_failed"))).toBe(true);
});

test("stale heartbeatAt triggers recycle before strike threshold", async () => {
	let probeOk = true;
	const logs: string[] = [];
	const killRecord: Array<{ pid: number; signal: string }> = [];
	const now = Date.now();

	// Create a discovery with stale heartbeat (50 seconds old, older than 45s threshold)
	const staleDiscovery = discovery(12345, now - 50_000);

	const value = client({
		discovery: async () => staleDiscovery,
		healthProbe: async () => probeOk,
		isPidAlive: () => true,
		isBrokerProcess: async () => true,
		killPid: (pid: number, signal: string = "SIGTERM") => {
			killRecord.push({ pid, signal });
		},
		healthIntervalMs: 5,
		reconnectBackoff: { initialMs: 5, maxMs: 5 },
		recycleWedgedBrokerStrikeThreshold: 3, // High threshold (won't be reached)
		recycleWedgedBrokerStaleMs: 45_000, // 45 seconds
		recycleGracePeriodMs: 10,
		log: (line) => logs.push(line),
	});

	await value.start();
	probeOk = false;

	// Wait for recycle due to stale heartbeat (should happen without 3 strikes)
	await eventually(() => killRecord.length > 0);

	// Verify SIGTERM was sent due to staleness, not strikes
	expect(killRecord[0]).toEqual({ pid: 12345, signal: "SIGTERM" });
	expect(logs.some((l) => l.includes("broker_recycle_sigterm"))).toBe(true);
});

test("probe recovery resets strikes before threshold (repeating fail,fail,ok pattern -> no recycle)", async () => {
	const logs: string[] = [];
	const killRecord: Array<{ pid: number; signal: string }> = [];
	let probeCallCount = 0;

	const value = client({
		discovery: async () => discovery(12345),
		healthProbe: async () => {
			probeCallCount++;
			// Pattern: succeed on every 3rd call (3, 6, 9, 12, ...)
			// Sequence: fail(1), fail(2), ok(3), fail(4), fail(5), ok(6), ...
			// Cumulative failures grow (1,2,0,1,2,0,...) but never 3 consecutive
			return probeCallCount % 3 === 0;
		},
		isPidAlive: () => true,
		isBrokerProcess: async () => true,
		killPid: (pid: number, signal: string = "SIGTERM") => {
			killRecord.push({ pid, signal });
		},
		healthIntervalMs: 5,
		reconnectBackoff: { initialMs: 5, maxMs: 5 },
		recycleWedgedBrokerStrikeThreshold: 3, // Threshold 3: requires 3 consecutive failures
		log: (line) => logs.push(line),
	});

	await value.start();

	// Wait for many probe calls to observe the repeating pattern multiple times
	// This ensures strikes are being reset reliably, not by accident
	await eventually(() => probeCallCount >= 12);

	// Should NOT have triggered recycle despite 12+ calls
	// Pattern ensures max 2 consecutive failures before recovery resets strikes
	expect(killRecord.length).toBe(0);
	expect(logs.some((l) => l.includes("broker_recycle_sigterm"))).toBe(false);
});

test("3 consecutive probe failures trigger SIGTERM (threshold 3)", async () => {
	const logs: string[] = [];
	const killRecord: Array<{ pid: number; signal: string }> = [];
	let failCount = 0;

	const value = client({
		discovery: async () => discovery(12345),
		healthProbe: async () => {
			failCount++;
			// Fail 3 times, then succeed
			return failCount > 3;
		},
		isPidAlive: () => true,
		isBrokerProcess: async () => true,
		killPid: (pid: number, signal: string = "SIGTERM") => {
			killRecord.push({ pid, signal });
		},
		healthIntervalMs: 5,
		reconnectBackoff: { initialMs: 5, maxMs: 5 },
		recycleWedgedBrokerStrikeThreshold: 3,
		recycleGracePeriodMs: 10,
		log: (line) => logs.push(line),
	});

	await value.start();

	// Wait for 3 consecutive failures to accumulate
	await eventually(() => killRecord.length > 0);

	// Verify SIGTERM was sent after exactly 3 failures
	expect(killRecord[0]).toEqual({ pid: 12345, signal: "SIGTERM" });
	expect(logs.some((l) => l.includes("broker_recycle_sigterm"))).toBe(true);
});

test("cooldown option prevents recycle within cooldown window", async () => {
	let probeOk = true;
	let probeCallCount = 0;
	const logs: string[] = [];
	const killRecord: Array<{ pid: number; signal: string }> = [];
	let clockVal = 0;

	const value = client({
		discovery: async () => discovery(12345),
		healthProbe: async () => {
			probeCallCount++;
			return probeOk;
		},
		isPidAlive: () => true, // Stay alive so recycle completes
		isBrokerProcess: async () => true,
		killPid: (pid: number, signal: string = "SIGTERM") => {
			killRecord.push({ pid, signal });
		},
		healthIntervalMs: 5,
		reconnectBackoff: { initialMs: 5, maxMs: 5 },
		recycleWedgedBrokerStrikeThreshold: 1,
		recycleWedgedBrokerCooldownMs: 5000, // 5 second cooldown
		recycleGracePeriodMs: 50,
		recycleClock: () => clockVal,
		log: (line) => logs.push(line),
	});

	await value.start();
	probeOk = false;

	// Wait for first SIGTERM+SIGKILL recycle pair
	await eventually(() => killRecord.length >= 2 && killRecord[1].signal === "SIGKILL");
	const sigtermCountInitial = killRecord.filter((k) => k.signal === "SIGTERM").length;
	expect(sigtermCountInitial).toBe(1);

	// Advance clock within cooldown window
	clockVal = 3000; // 3 seconds later (within 5s cooldown)
	probeOk = false; // Keep probe failing

	// Many more observations while in cooldown
	await eventually(() => probeCallCount >= 50);

	// Verify no new SIGTERM (cooldown prevented additional recycles)
	const sigtermCountInCooldown = killRecord.filter((k) => k.signal === "SIGTERM").length;
	expect(sigtermCountInCooldown).toBe(sigtermCountInitial);

	// Now advance clock past cooldown window
	clockVal = 5100; // 5.1 seconds later (past 5s cooldown)
	probeOk = false;

	// Wait for second SIGTERM after cooldown expires
	await eventually(() => killRecord.filter((k) => k.signal === "SIGTERM").length > sigtermCountInitial);
	const sigtermCountAfterCooldown = killRecord.filter((k) => k.signal === "SIGTERM").length;
	expect(sigtermCountAfterCooldown).toBe(sigtermCountInitial + 1);
});

test("preflight error message contains 'broker connection refused'", async () => {
	const { preflightGjcRuntime } = await import("../src/orchestrator/broker");

	const versionRunner = async () => ({
		exitCode: 0,
		stdout: "gjc/0.18.0\n",
		stderr: "",
	});

	const sdkRunner = async () => ({
		exitCode: 1,
		stdout: "",
		stderr: "error: broker connection refused\nstderr tail",
	});

	try {
		await preflightGjcRuntime(versionRunner as never, "0.18.0", sdkRunner as never);
		expect.unreachable("should have thrown");
	} catch (error) {
		const message = (error as Error).message;
		expect(message).toContain("invalid session-list envelope");
		expect(message).toContain("broker connection refused");
	}
});

test("preflight error message stays bounded even with 100KB stderr", async () => {
	const { preflightGjcRuntime } = await import("../src/orchestrator/broker");

	const versionRunner = async () => ({
		exitCode: 0,
		stdout: "gjc/0.18.0\n",
		stderr: "",
	});

	// Create 100KB of stderr output
	const hugeStderr = "x".repeat(100_000);

	const sdkRunner = async () => ({
		exitCode: 1,
		stdout: "",
		stderr: `${hugeStderr}\nerror: broker connection refused\ntail line`,
	});

	try {
		await preflightGjcRuntime(versionRunner as never, "0.18.0", sdkRunner as never);
		expect.unreachable("should have thrown");
	} catch (error) {
		const message = (error as Error).message;
		// Message should be reasonable length, not contain the entire 100KB
		expect(message.length).toBeLessThan(1000);
		// But should still contain diagnostic info
		expect(message).toContain("invalid session-list envelope");
	}
});
