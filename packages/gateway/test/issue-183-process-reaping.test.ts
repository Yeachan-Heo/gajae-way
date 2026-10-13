import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ProcessTableEntry, reapLeftoverWorkers } from "../src/ops/process-reaper";

/**
 * Test suite for issue #183: Agent child processes outlive their unit and are never reaped.
 * Verifies that:
 * 1. Process reaper correctly identifies orphaned workers belonging to this gateway
 * 2. Process reaper does NOT match unrelated bun processes
 * 3. Process reaper handles missing gjc executable gracefully
 * 4. Process reaper does not crash on permission errors or missing /proc
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
 * Fixture tests with injected process tables.
 * These tests verify each guard condition independently.
 * All SKIP tests must NOT match and be reaped (invariant: found = 0).
 * All POSITIVE tests must match and be reaped (invariant: found = 1, killed = 1).
 */

test("fixture: SKIP unrelated orphaned bun process (different command)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-fixture-unrelated-"));
	const brokerAgentDir = join(home, "broker", "agent");
	const gjcPath = join(home, "gjc", "current", "bin", "gjc");
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(gjcPath, "");
		await mkdir(brokerAgentDir, { recursive: true });

		const deadParent = 9999; // Non-existent PID
		const processTable: ProcessTableEntry[] = [
			{
				pid: 2000,
				ppid: deadParent,
				command: "bun some-unrelated-script.ts",
				environ: {
					GJC_CODING_AGENT_DIR: brokerAgentDir,
				},
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

		// Should NOT match because command doesn't match exact tokens
		expect(result.found).toBe(0);
		expect(result.killed).toBe(0);
		expect(killCalled.length).toBe(0);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fixture: SKIP worker with different agent directory", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-fixture-diff-agent-"));
	const brokerAgentDir = join(home, "broker", "agent");
	const otherAgentDir = join(home, "other", "agent");
	const gjcPath = join(home, "gjc", "current", "bin", "gjc");
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(gjcPath, "");
		await mkdir(brokerAgentDir, { recursive: true });
		await mkdir(otherAgentDir, { recursive: true });

		const deadParent = 9999; // Non-existent PID
		const processTable: ProcessTableEntry[] = [
			{
				pid: 2001,
				ppid: deadParent,
				command: `bun ${gjcPath} sdk serve session`,
				environ: {
					GJC_CODING_AGENT_DIR: otherAgentDir,
				},
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

		// Should NOT match because agent dir doesn't match exactly
		expect(result.found).toBe(0);
		expect(result.killed).toBe(0);
		expect(killCalled.length).toBe(0);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fixture: SKIP prefix-colliding agent directory", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-fixture-prefix-"));
	const brokerAgentDir = join(home, "broker", "agent");
	const collideAgentDir = join(home, "broker", "agent2"); // Prefix collision
	const gjcPath = join(home, "gjc", "current", "bin", "gjc");
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(gjcPath, "");
		await mkdir(brokerAgentDir, { recursive: true });
		await mkdir(collideAgentDir, { recursive: true });

		const deadParent = 9999; // Non-existent PID
		const processTable: ProcessTableEntry[] = [
			{
				pid: 2002,
				ppid: deadParent,
				command: `bun ${gjcPath} sdk serve session`,
				environ: {
					GJC_CODING_AGENT_DIR: collideAgentDir,
				},
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

		// Should NOT match because agent dir is different (exact equality check)
		expect(result.found).toBe(0);
		expect(result.killed).toBe(0);
		expect(killCalled.length).toBe(0);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fixture: SKIP worker whose parent is a live gateway process", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-fixture-parent-alive-"));
	const brokerAgentDir = join(home, "broker", "agent");
	const gjcPath = join(home, "gjc", "current", "bin", "gjc");
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(gjcPath, "");
		await mkdir(brokerAgentDir, { recursive: true });

		const aliveParent = 1234; // Parent process that we'll mark as alive
		const processTable: ProcessTableEntry[] = [
			{
				pid: aliveParent,
				ppid: 1,
				command: `bun ${gjcPath} sdk broker run --agent-dir ${brokerAgentDir}`,
			},
			{
				pid: 2003,
				ppid: aliveParent,
				command: `bun ${gjcPath} sdk serve --stdio --session 00000000-0000-4000-8000-000000000001`,
				environ: {
					GJC_CODING_AGENT_DIR: brokerAgentDir,
				},
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

		// Should NOT reap because parent is alive and is a gateway process (has sdk broker tokens)
		expect(result.found).toBe(0);
		expect(result.killed).toBe(0);
		expect(killCalled.length).toBe(0);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fixture: POSITIVE reap worker with dead parent (ppid=9999 not in table)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-fixture-positive-dead-"));
	const brokerAgentDir = join(home, "broker", "agent");
	const gjcPath = join(home, "gjc", "current", "bin", "gjc");
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(gjcPath, "");
		await mkdir(brokerAgentDir, { recursive: true });

		const deadParent = 9999; // Non-existent PID (not in process table)
		const processTable: ProcessTableEntry[] = [
			{
				pid: 2004,
				ppid: deadParent,
				command: `bun ${gjcPath} sdk serve --stdio --session 00000000-0000-4000-8000-000000000002`,
				environ: {
					GJC_CODING_AGENT_DIR: brokerAgentDir,
				},
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

		// Should MATCH and be reaped (parent is dead, not in the table)
		expect(result.found).toBe(1);
		expect(result.killed).toBe(1);
		expect(killCalled.length).toBe(1);
		expect(killCalled[0]).toEqual({ pid: 2004, signal: "SIGTERM" });
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fixture: POSITIVE reap worker with ppid=1", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-fixture-positive-ppid1-"));
	const brokerAgentDir = join(home, "broker", "agent");
	const gjcPath = join(home, "gjc", "current", "bin", "gjc");
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(gjcPath, "");
		await mkdir(brokerAgentDir, { recursive: true });

		const processTable: ProcessTableEntry[] = [
			{
				pid: 2005,
				ppid: 1, // Init process; orphaned worker
				command: `bun ${gjcPath} sdk serve --stdio --session 00000000-0000-4000-8000-000000000003`,
				environ: {
					GJC_CODING_AGENT_DIR: brokerAgentDir,
				},
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

		// Should MATCH and be reaped (parent is init, which is not a gateway)
		expect(result.found).toBe(1);
		expect(result.killed).toBe(1);
		expect(killCalled.length).toBe(1);
		expect(killCalled[0]).toEqual({ pid: 2005, signal: "SIGTERM" });
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fixture: POSITIVE reap worker with systemd --user subreaper (live non-gateway parent)", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-fixture-positive-systemd-"));
	const brokerAgentDir = join(home, "broker", "agent");
	const gjcPath = join(home, "gjc", "current", "bin", "gjc");
	try {
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		await writeFile(gjcPath, "");
		await mkdir(brokerAgentDir, { recursive: true });

		const systemdPid = 1234; // systemd --user process
		const processTable: ProcessTableEntry[] = [
			{
				pid: systemdPid,
				ppid: 1,
				command: "/usr/lib/systemd/systemd --user",
			},
			{
				pid: 2006,
				ppid: systemdPid,
				command: `bun ${gjcPath} sdk serve --stdio --session 00000000-0000-4000-8000-000000000004`,
				environ: {
					GJC_CODING_AGENT_DIR: brokerAgentDir,
				},
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

		// Should MATCH and be reaped (parent is systemd, which is NOT a gateway process)
		expect(result.found).toBe(1);
		expect(result.killed).toBe(1);
		expect(killCalled.length).toBe(1);
		expect(killCalled[0]).toEqual({ pid: 2006, signal: "SIGTERM" });
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
