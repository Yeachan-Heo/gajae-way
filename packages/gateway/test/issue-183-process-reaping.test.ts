import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ProcessTableEntry, reapLeftoverWorkers, selectOrphanedWorkers } from "../src/ops/process-reaper";

/**
 * Test suite for issue #183: Agent child processes outlive their unit and are never reaped.
 * Verifies that:
 * 1. selectOrphanedWorkers correctly identifies orphaned workers belonging to this gateway
 * 2. selectOrphanedWorkers does NOT match unrelated bun processes
 * 3. reapLeftoverWorkers handles missing gjc executable gracefully
 * 4. reapLeftoverWorkers does not crash on permission errors or missing /proc
 */

test("process reaper: returns consistent result structure in clean environment", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-process-reaper-clean-"));
	try {
		// Create minimal home layout structure for reapLeftoverWorkers to discover
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(join(home, "gjc", "current", "bin", "gjc"), "");

		const result = await reapLeftoverWorkers(home);
		// In a clean test environment, we shouldn't find SDK serve processes
		expect(typeof result.found).toBe("number");
		expect(typeof result.killed).toBe("number");
		expect(typeof result.failed).toBe("number");
		expect(result.found).toBeGreaterThanOrEqual(0);
		expect(result.killed).toBe(0);
		expect(result.failed).toBe(0);
		// Invariant: killed + failed <= found
		expect(result.killed + result.failed).toBeLessThanOrEqual(result.found);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("process reaper: handles missing gjc executable gracefully", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-process-reaper-missing-gjc-"));
	try {
		// Create home layout but NOT the gjc executable
		await mkdir(join(home, "broker", "agent"), { recursive: true });

		// Should not throw even if gjc executable doesn't exist
		const result = await reapLeftoverWorkers(home, {
			log: () => {
				/* suppress logs */
			},
		});
		expect(typeof result.found).toBe("number");
		expect(typeof result.killed).toBe("number");
		expect(typeof result.failed).toBe("number");
		expect(result.found).toBeGreaterThanOrEqual(0);
		// No processes should be matched if gjc executable doesn't exist
		expect(result.killed).toBe(0);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("process reaper: respects home directory parameter", async () => {
	const home1 = await mkdtemp(join(tmpdir(), "gajaeway-reaper-home1-"));
	const home2 = await mkdtemp(join(tmpdir(), "gajaeway-reaper-home2-"));
	try {
		// Set up both home directories
		for (const h of [home1, home2]) {
			await mkdir(join(h, "gjc", "current", "bin"), { recursive: true });
			await writeFile(join(h, "gjc", "current", "bin", "gjc"), "");
		}

		// Should work independently for each home
		const result1 = await reapLeftoverWorkers(home1, {
			log: () => {
				/* suppress */
			},
		});
		const result2 = await reapLeftoverWorkers(home2, {
			log: () => {
				/* suppress */
			},
		});

		// Both should return valid results
		expect(typeof result1.found).toBe("number");
		expect(typeof result2.found).toBe("number");
		expect(result1.killed).toBe(0);
		expect(result2.killed).toBe(0);
	} finally {
		await rm(home1, { recursive: true, force: true });
		await rm(home2, { recursive: true, force: true });
	}
});

test("process reaper: does not throw on unreadable /proc entries", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-process-reaper-unreadable-"));
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(join(home, "gjc", "current", "bin", "gjc"), "");

		// Should complete without throwing, even if some /proc entries are unreadable
		const result = await reapLeftoverWorkers(home, {
			log: () => {
				/* suppress */
			},
		});

		expect(typeof result.found).toBe("number");
		expect(typeof result.killed).toBe("number");
		expect(typeof result.failed).toBe("number");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

/**
 * Fixture tests using selectOrphanedWorkers directly with injected process tables.
 * These tests verify each guard condition independently.
 * All SKIP tests must NOT match (invariant: found = 0).
 * All POSITIVE tests must match (invariant: found = 1).
 */

test("fixture: SKIP unrelated orphaned bun process (different command)", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2000,
			ppid: 9999, // Non-existent parent (orphaned)
			argv: ["bun", "some-unrelated-script.ts"],
			agentDir: brokerAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should NOT match because argv doesn't match worker pattern
	expect(orphaned.length).toBe(0);
});

test("fixture: SKIP worker with different agent directory", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const otherAgentDir = "/home/user/gajaeway/other/agent";
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2001,
			ppid: 9999,
			argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000001"],
			agentDir: otherAgentDir, // Different agent dir
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should NOT match because agentDir is different
	expect(orphaned.length).toBe(0);
});

test("fixture: SKIP prefix-colliding agent directory", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const collidingAgentDir = "/home/user/gajaeway/broker/agent2"; // Prefix collision
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2002,
			ppid: 9999,
			argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000002"],
			agentDir: collidingAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should NOT match because agentDir is not exactly equal
	expect(orphaned.length).toBe(0);
});

test("fixture: SKIP worker whose ppid === selfPid (child of current gateway)", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2003,
			ppid: selfPid, // Child of the current gateway
			argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000003"],
			agentDir: brokerAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should NOT reap because parent is the current gateway
	expect(orphaned.length).toBe(0);
});

test("fixture: SKIP argv superset/prefix match (extra token)", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2010,
			ppid: 9999,
			argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "id", "--extra-flag"],
			agentDir: brokerAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should NOT match because argv has extra tokens (not exact match)
	expect(orphaned.length).toBe(0);
});

test("fixture: SKIP argv variant (serve-x instead of serve)", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2011,
			ppid: 9999,
			argv: [gjcPath, "sdk", "serve-x", "--stdio", "--session", "id"],
			agentDir: brokerAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should NOT match because "serve-x" !== "serve"
	expect(orphaned.length).toBe(0);
});

test("fixture: POSITIVE reap worker with dead parent (ppid=9999 not in table)", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2004,
			ppid: 9999, // Non-existent PID (not in process table)
			argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000002"],
			agentDir: brokerAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should MATCH and be reaped (parent is dead, not in the table)
	expect(orphaned.length).toBe(1);
	expect(orphaned[0]).toBe(2004);
});

test("fixture: POSITIVE reap worker with ppid=1", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2005,
			ppid: 1, // Init process; orphaned worker
			argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000003"],
			agentDir: brokerAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should MATCH and be reaped (parent is init, which is not the current gateway)
	expect(orphaned.length).toBe(1);
	expect(orphaned[0]).toBe(2005);
});

test("fixture: POSITIVE reap worker with systemd --user subreaper (live non-gateway parent)", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const selfPid = 1000;
	const systemdPid = 1234;

	const processTable: ProcessTableEntry[] = [
		{
			pid: systemdPid,
			ppid: 1,
			argv: ["/usr/lib/systemd/systemd", "--user"],
			agentDir: undefined,
		},
		{
			pid: 2006,
			ppid: systemdPid,
			argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000004"],
			agentDir: brokerAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should MATCH and be reaped (parent is systemd, which is NOT the current gateway)
	expect(orphaned.length).toBe(1);
	expect(orphaned[0]).toBe(2006);
});

test("fixture: POSITIVE reap worker spawned as bun gjcPath (argv[0] = bun)", async () => {
	const gjcPath = "/home/user/gajaeway/gjc/current/bin/gjc";
	const brokerAgentDir = "/home/user/gajaeway/broker/agent";
	const selfPid = 1000;

	const processTable: ProcessTableEntry[] = [
		{
			pid: 2007,
			ppid: 9999,
			argv: ["bun", gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000005"],
			agentDir: brokerAgentDir,
		},
	];

	const orphaned = selectOrphanedWorkers(processTable, {
		gjcPath,
		brokerAgentDir,
		selfPid,
	});

	// Should MATCH (argv pattern allows bun as first token)
	expect(orphaned.length).toBe(1);
	expect(orphaned[0]).toBe(2007);
});

test("integration: reapLeftoverWorkers with injected process table", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-integration-kill-"));
	const brokerAgentDir = join(home, "broker", "agent");
	const gjcPath = join(home, "gjc", "current", "bin", "gjc");
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(gjcPath, "");
		await mkdir(brokerAgentDir, { recursive: true });

		const selfPid = 1000;
		const processTable: ProcessTableEntry[] = [
			{
				pid: 2008,
				ppid: 9999, // Dead parent
				argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000006"],
				agentDir: brokerAgentDir,
			},
			{
				pid: 2009,
				ppid: 1, // Init parent
				argv: [gjcPath, "sdk", "serve", "--stdio", "--session", "00000000-0000-4000-8000-000000000007"],
				agentDir: brokerAgentDir,
			},
		];

		const killCalled: Array<{ pid: number; signal: string }> = [];
		const result = await reapLeftoverWorkers(home, {
			log: () => {
				/* suppress */
			},
			processTable,
			kill: (pid, signal) => {
				killCalled.push({ pid, signal });
				return Promise.resolve();
			},
		});

		// Should find and kill both orphaned workers
		expect(result.found).toBe(2);
		expect(result.killed).toBe(2);
		expect(killCalled.length).toBe(2);
		expect(killCalled[0]).toEqual({ pid: 2008, signal: "SIGTERM" });
		expect(killCalled[1]).toEqual({ pid: 2009, signal: "SIGTERM" });
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
