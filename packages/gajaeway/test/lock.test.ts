import { afterEach, expect, test } from "bun:test";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DaemonLock, DaemonLockRefusalError, type PidLiveness } from "../src/lock";

const homes: string[] = [];
afterEach(async () => {
	await Promise.all(
		homes.splice(0).map(async (home) => {
			await chmod(join(home, "gajaeway.pid"), 0o600).catch(() => {});
			await rm(home, { recursive: true, force: true });
		}),
	);
});

async function home(): Promise<string> {
	const path = await mkdtemp(join(tmpdir(), "gajaeway-daemon-lock-"));
	homes.push(path);
	return path;
}

function ports(pid: number, liveness: (holder: number) => PidLiveness, logs: string[] = []) {
	return { pid, liveness, log: (line: string) => logs.push(line) };
}

test("a fresh daemon lock claims gajaeway.pid and releases it", async () => {
	const dir = await home();
	const lock = await DaemonLock.acquire(
		dir,
		ports(1001, () => "dead"),
	);
	expect(await Bun.file(lock.path).text()).toBe("1001\n");
	await lock.release();
	expect(await Bun.file(lock.path).exists()).toBe(false);
});

test("a live holder (including an EPERM-equivalent liveness result) that never exits is refused after the wait", async () => {
	const dir = await home();
	const path = join(dir, "gajaeway.pid");
	await writeFile(path, "2002\n");
	await expect(
		DaemonLock.acquire(
			dir,
			ports(1001, () => "alive"),
			{ waitMs: 200, sleep: async () => {} },
		),
	).rejects.toMatchObject({
		name: "DaemonLockRefusalError",
		exitCode: 2,
	});
	expect((await readFile(path, "utf8")).trim()).toBe("2002");
});

test("a lock naming this process refuses rather than unlinking itself", async () => {
	const dir = await home();
	await writeFile(join(dir, "gajaeway.pid"), `${process.pid}\n`);
	await expect(
		DaemonLock.acquire(
			dir,
			ports(process.pid, () => "dead"),
		),
	).rejects.toThrow("lock names this process; a previous run of this pid left it behind");
});

test("malformed and unreadable lock files fail closed", async () => {
	const malformedHome = await home();
	await writeFile(join(malformedHome, "gajaeway.pid"), "not-a-pid\n");
	await expect(
		DaemonLock.acquire(
			malformedHome,
			ports(1001, () => "dead"),
		),
	).rejects.toThrow("is unreadable or malformed");

	const unreadableHome = await home();
	const unreadable = join(unreadableHome, "gajaeway.pid");
	await writeFile(unreadable, "2002\n");
	await chmod(unreadable, 0o000);
	await expect(
		DaemonLock.acquire(
			unreadableHome,
			ports(1001, () => "dead"),
		),
	).rejects.toThrow("is unreadable or malformed");
});

test("only an ESRCH-equivalent dead PID is reclaimed and logged", async () => {
	const dir = await home();
	const path = join(dir, "gajaeway.pid");
	const logs: string[] = [];
	await writeFile(path, "2002\n");
	const lock = await DaemonLock.acquire(
		dir,
		ports(1001, () => "dead", logs),
	);
	expect(await readFile(path, "utf8")).toBe("1001\n");
	expect(logs).toEqual(["daemon_lock_reclaimed stale_pid=2002"]);
	await lock.release();
});

test("an unknown liveness probe never reclaims a valid lock", async () => {
	const dir = await home();
	await writeFile(join(dir, "gajaeway.pid"), "2002\n");
	await expect(
		DaemonLock.acquire(
			dir,
			ports(1001, () => "unknown"),
		),
	).rejects.toThrow("cannot prove whether pid 2002 is running");
});

test("a reclaim race lets one owner win and refuses the loser", async () => {
	const dir = await home();
	await writeFile(join(dir, "gajaeway.pid"), "9999\n");
	const liveness = (pid: number): PidLiveness => (pid === 9999 ? "dead" : "alive");
	const results = await Promise.allSettled([
		DaemonLock.acquire(dir, ports(1001, liveness)),
		DaemonLock.acquire(dir, ports(1002, liveness)),
	]);
	const winners = results.filter(
		(result): result is PromiseFulfilledResult<DaemonLock> => result.status === "fulfilled",
	);
	const losers = results.filter((result): result is PromiseRejectedResult => result.status === "rejected");
	expect(winners).toHaveLength(1);
	expect(losers).toHaveLength(1);
	expect(losers[0]?.reason).toBeInstanceOf(DaemonLockRefusalError);
	await winners[0]?.value.release();
});

test("release leaves a replaced lock untouched", async () => {
	const dir = await home();
	const lock = await DaemonLock.acquire(
		dir,
		ports(1001, () => "dead"),
	);
	await writeFile(lock.path, "2002\n");
	await lock.release();
	expect(await Bun.file(lock.path).text()).toBe("2002\n");
});

test("a live holder is waited out (never signalled); the lock is claimed once it exits", async () => {
	const dir = await home();
	await writeFile(join(dir, "gajaeway.pid"), "4242\n");
	const logs: string[] = [];
	let polls = 0;
	const lock = await DaemonLock.acquire(
		dir,
		{ pid: 9001, liveness: () => (++polls >= 4 ? "dead" : "alive"), log: (line) => void logs.push(line) },
		{ waitMs: 5_000, sleep: async () => {} },
	);
	try {
		expect(polls).toBeGreaterThanOrEqual(4);
		expect(logs.some((line) => line.startsWith("daemon_predecessor_live pid=4242"))).toBe(true);
		expect(logs).toContain("daemon_predecessor_exited pid=4242");
		expect((await readFile(join(dir, "gajaeway.pid"), "utf8")).trim()).toBe("9001");
	} finally {
		await lock.release();
	}
});

test("a holder that outlives the wait refuses (exit for the service manager to retry) and keeps its lock", async () => {
	const dir = await home();
	await writeFile(join(dir, "gajaeway.pid"), "4243\n");
	await expect(
		DaemonLock.acquire(dir, { pid: 9002, liveness: () => "alive" }, { waitMs: 300, sleep: async () => {} }),
	).rejects.toBeInstanceOf(DaemonLockRefusalError);
	expect((await readFile(join(dir, "gajaeway.pid"), "utf8")).trim()).toBe("4243");
});

test("--only-new refuses a live holder immediately without polling", async () => {
	const dir = await home();
	await writeFile(join(dir, "gajaeway.pid"), "4244\n");
	let polls = 0;
	await expect(
		DaemonLock.acquire(dir, { pid: 9003, liveness: () => (polls++, "alive") }, { onlyNew: true }),
	).rejects.toThrow("--only-new");
	expect(polls).toBe(1);
});

test("a stale lock replaced by a live owner between liveness proof and unlink is never clobbered", async () => {
	const dir = await home();
	const path = join(dir, "gajaeway.pid");
	await writeFile(path, "9999\n");
	// The reclaimer proves 9999 dead; while it is inside the reclaim window, a
	// different daemon (2002) already re-claimed the file. Liveness for 2002 is
	// "alive" so the reclaimer must observe the replacement and refuse.
	let calls = 0;
	const liveness = (pid: number): PidLiveness => {
		if (pid === 9999) {
			calls++;
			// Simulate the replacement racing in right after the liveness proof.
			void writeFile(path, "2002\n");
			return "dead";
		}
		return "alive";
	};
	await expect(DaemonLock.acquire(dir, ports(1001, liveness))).rejects.toBeInstanceOf(DaemonLockRefusalError);
	expect(calls).toBe(1);
	expect((await readFile(path, "utf8")).trim()).toBe("2002");
	expect(await Bun.file(`${path}.reclaim`).exists()).toBe(false);
});

test("a live reclaim token blocks a second reclaimer instead of letting two unlink", async () => {
	const dir = await home();
	await writeFile(join(dir, "gajaeway.pid"), "9999\n");
	await writeFile(join(dir, "gajaeway.pid.reclaim"), "1001\n");
	// The token's owner (1001) is still alive mid-reclaim; only the lock holder is dead.
	const liveness = (pid: number): PidLiveness => (pid === 1001 ? "alive" : "dead");
	await expect(DaemonLock.acquire(dir, ports(1002, liveness))).rejects.toThrow("being reclaimed by another process");
	expect((await readFile(join(dir, "gajaeway.pid"), "utf8")).trim()).toBe("9999");
});

test("a reclaim token left by a crashed reclaimer is itself reclaimed when its owner is provably dead", async () => {
	const dir = await home();
	const logs: string[] = [];
	await writeFile(join(dir, "gajaeway.pid"), "9999\n");
	await writeFile(join(dir, "gajaeway.pid.reclaim"), "8888\n");
	const lock = await DaemonLock.acquire(
		dir,
		ports(1002, () => "dead", logs),
	);
	expect(logs).toContain("daemon_lock_reclaim_token_reclaimed stale_pid=8888");
	expect(logs).toContain("daemon_lock_reclaimed stale_pid=9999");
	expect(await Bun.file(join(dir, "gajaeway.pid.reclaim")).exists()).toBe(false);
	await lock.release();
});
