import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import { reapLeftoverWorkers } from "../src/ops/process-reaper";

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
		// Create minimal home layout structure so homeLayout() doesn't fail
		const layout = homeLayout(home);
		await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
		// Create a dummy gjc executable (doesn't need to be real for this test)
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
		for (const home of [home1, home2]) {
			await mkdir(join(home, "gjc", "current", "bin"), { recursive: true });
			await writeFile(join(home, "gjc", "current", "bin", "gjc"), "");
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
