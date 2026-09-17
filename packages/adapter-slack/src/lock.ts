import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

/** A single Slack bot token must not have two adapters replying to every event. */
export class AdapterAlreadyRunningError extends Error {
	constructor(
		readonly holderPid: number,
		readonly path: string,
	) {
		super(
			`Another Slack adapter is already running (pid ${holderPid}, lock ${path}). Stop it before starting a second one.`,
		);
		this.name = "AdapterAlreadyRunningError";
	}
}

export interface AdapterLockPorts {
	readonly pid: number;
	readonly alive: (pid: number) => boolean;
}

export function defaultLockPorts(): AdapterLockPorts {
	return { pid: process.pid, alive: processIsAlive };
}

/** EPERM still proves a holder exists under another user. */
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
		const path = join(home, "adapter-slack.pid");
		await mkdir(home, { recursive: true });
		// Fast path: an exclusive create wins outright when no pidfile exists.
		if (await claim(path, ports.pid)) return new AdapterLock(path, ports.pid);
		const holder = await readHolder(path);
		// A crash or truncated pidfile must not require manual cleanup to restart.
		if (holder !== undefined && holder !== ports.pid && ports.alive(holder)) {
			throw new AdapterAlreadyRunningError(holder, path);
		}
		// Reclaim atomically. `rm` + create is a window in which a second starter can
		// remove OUR fresh claim after reading the same stale holder; renaming a
		// fully written private file over the stale path leaves no such window, and
		// reading the path back afterwards tells each contender whether it won.
		const temporary = `${path}.${ports.pid}.${randomUUID()}.tmp`;
		await writeFile(temporary, `${ports.pid}\n`, { flag: "wx", mode: 0o600 });
		try {
			// Somebody else may have reclaimed (or a live holder may have appeared)
			// between the stale read and now: never replace a pid that is not the
			// stale one we judged dead.
			const current = await readHolder(path);
			if (current !== holder && current !== undefined && current !== ports.pid && ports.alive(current)) {
				throw new AdapterAlreadyRunningError(current, path);
			}
			await rename(temporary, path);
		} finally {
			await rm(temporary, { force: true });
		}
		const winner = await readHolder(path);
		if (winner === ports.pid) return new AdapterLock(path, ports.pid);
		throw new AdapterAlreadyRunningError(winner ?? 0, path);
	}

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
