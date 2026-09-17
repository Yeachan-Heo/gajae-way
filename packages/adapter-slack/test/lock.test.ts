import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AdapterAlreadyRunningError, AdapterLock, processIsAlive } from "../src/lock";

async function withHome(body: (home: string) => Promise<void>) {
	const home = await mkdtemp(join(tmpdir(), "slack-lock-"));
	try {
		await body(home);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}

test("Slack first instance records its pid and refuses a live second instance", async () => {
	await withHome(async (home) => {
		const lock = await AdapterLock.acquire(home, { pid: 4242, alive: () => true });
		expect(lock.path).toBe(join(home, "adapter-slack.pid"));
		expect(await readFile(lock.path, "utf8")).toBe("4242\n");
		const attempt = AdapterLock.acquire(home, { pid: 5353, alive: () => true });
		await expect(attempt).rejects.toBeInstanceOf(AdapterAlreadyRunningError);
		await expect(attempt).rejects.toThrow("Another Slack adapter is already running (pid 4242");
		expect(await readFile(lock.path, "utf8")).toBe("4242\n");
	});
});

test("Slack reclaims dead and unparseable pidfiles", async () => {
	await withHome(async (home) => {
		await writeFile(join(home, "adapter-slack.pid"), "4242\n");
		const lock = await AdapterLock.acquire(home, { pid: 5353, alive: () => false });
		expect(await readFile(lock.path, "utf8")).toBe("5353\n");
		for (const stale of ["", "not a pid"]) {
			await writeFile(lock.path, stale);
			await AdapterLock.acquire(home, {
				pid: 5353,
				alive: () => {
					throw new Error("Slack stale pid must not be checked");
				},
			});
			expect(await readFile(lock.path, "utf8")).toBe("5353\n");
		}
	});
});

test("Slack release only deletes the holder's pidfile", async () => {
	await withHome(async (home) => {
		const lock = await AdapterLock.acquire(home, { pid: 4242, alive: () => true });
		await lock.release();
		expect(await Bun.file(lock.path).exists()).toBe(false);
		const next = await AdapterLock.acquire(home, { pid: 5353, alive: () => true });
		await lock.release();
		expect(await readFile(next.path, "utf8")).toBe("5353\n");
	});
});

test("Slack liveness recognizes this process and an absent process", () => {
	expect(processIsAlive(process.pid)).toBe(true);
	expect(processIsAlive(0x7ff_ffff)).toBe(false);
});
