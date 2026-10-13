import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, leaseHeld } from "@gajae-gateway/home";
import { runTelegramAdapterWithLease } from "../src/main";

async function withHome(body: (home: string) => Promise<void>): Promise<void> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-telegram-lease-"));
	try {
		await body(home);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}

async function runAdapter(home: string): Promise<{ code: number; stderr: string }> {
	const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts")], {
		stdout: "pipe",
		stderr: "pipe",
		env: { ...process.env, GAJAEWAY_HOME: home },
	});
	const [stderr, code] = await Promise.all([new Response(child.stderr).text(), child.exited]);
	return { code, stderr };
}

test("a second Telegram adapter process exits 1 with lease_held", async () => {
	await withHome(async (home) => {
		const lease = await acquireLease(home, "adapter-telegram");
		try {
			const result = await runAdapter(home);
			expect(result.code).toBe(1);
			expect(result.stderr).toContain("lease_held name=adapter-telegram");
			expect(result.stderr).toContain(`owner_pid=${process.pid}`);
		} finally {
			await lease.release();
		}
	});
}, 30_000);

test("the Telegram adapter lease is released when its startup lifetime ends", async () => {
	await withHome(async (home) => {
		let heldDuringRun = false;
		await runTelegramAdapterWithLease(home, async () => {
			heldDuringRun = await leaseHeld(home, "adapter-telegram");
		});
		expect(heldDuringRun).toBe(true);
		expect(await leaseHeld(home, "adapter-telegram")).toBe(false);
		const next = await acquireLease(home, "adapter-telegram");
		await next.release();
	});
});
