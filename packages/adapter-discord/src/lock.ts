import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

/**
 * Single-instance guard for the Discord adapter.
 *
 * Two adapters on one bot token is not a nuisance, it is corruption: both
 * receive every message and both reply, and the resident launchd-managed
 * process fights a hand-started one over the same gateway session. The claim is
 * a pidfile in GAJAEWAY_HOME — the same directory and the same 0o600 file state
 * the adapters already persist into — taken with O_EXCL so two simultaneous
 * starts cannot both believe they won.
 *
 * A pidfile left behind by a killed process is not a lock: it is reclaimed when
 * its recorded pid is gone, otherwise a crash would need manual cleanup before
 * launchd could restart anything.
 */

export class AdapterAlreadyRunningError extends Error {
	constructor(
		readonly holderPid: number,
		readonly path: string,
	) {
		super(
			`Another Discord adapter is already running (pid ${holderPid}, lock ${path}). Stop it before starting a second one.`,
		);
		this.name = "AdapterAlreadyRunningError";
	}
}

export interface AdapterLockPorts {
	/** The pid recorded in the lock. */
	readonly pid: number;
	/** Whether a recorded holder is still a live process. */
	readonly alive: (pid: number) => boolean;
}

export function defaultLockPorts(): AdapterLockPorts {
	return { pid: process.pid, alive: processIsAlive };
}

/** `kill(pid, 0)`: EPERM means alive under another user, ESRCH means gone. */
export function processIsAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		return (error as NodeJS.ErrnoException).code === "EPERM";
	}
}

export class AdapterLock {
	private constructor(
		readonly path: string,
		readonly pid: number,
	) {}

	static async acquire(home: string, ports: AdapterLockPorts = defaultLockPorts()): Promise<AdapterLock> {
		const path = join(home, "adapter-discord.pid");
		await mkdir(home, { recursive: true });
		if (await claim(path, ports.pid)) return new AdapterLock(path, ports.pid);
		const holder = await readHolder(path);
		// An unreadable or unparseable pidfile is treated as stale rather than as
		// a live holder: refusing forever on a truncated file would be worse than
		// reclaiming a lock nobody holds.
		if (holder !== undefined && holder !== ports.pid && ports.alive(holder)) {
			throw new AdapterAlreadyRunningError(holder, path);
		}
		await rm(path, { force: true });
		if (await claim(path, ports.pid)) return new AdapterLock(path, ports.pid);
		// Lost the reclaim race: whoever took it between the rm and the claim owns it.
		throw new AdapterAlreadyRunningError((await readHolder(path)) ?? 0, path);
	}

	/** Drops the lock, but never one this process does not still hold. */
	async release(): Promise<void> {
		if ((await readHolder(this.path)) !== this.pid) return;
		await rm(this.path, { force: true });
	}
}

async function claim(path: string, pid: number): Promise<boolean> {
	try {
		await writeFile(path, `${pid}\n`, { flag: "wx", mode: 0o600 });
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "EEXIST") return false;
		throw error;
	}
}

async function readHolder(path: string): Promise<number | undefined> {
	try {
		const pid = Number.parseInt((await readFile(path, "utf8")).trim(), 10);
		return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
	} catch {
		return undefined;
	}
}
