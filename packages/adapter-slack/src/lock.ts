import { randomUUID } from "node:crypto";
import { link, mkdir, readFile, rename, rm, stat, unlink, writeFile } from "node:fs/promises";
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

/** How long a contender waits on a live reclaimer before failing closed (200 x 5ms = 1s). */
const RECLAIM_WAIT_ATTEMPTS = 200;
const RECLAIM_WAIT_STEP_MS = 5;

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
		// Reclaiming a stale pidfile must elect exactly ONE contender, and a
		// liveness probe cannot arbitrate that: the winner's fresh claim belongs to
		// a process still booting, which a probe may not see yet. So contenders
		// serialise on an exclusive marker (`link` is atomic-exclusive, `rename` is
		// not). The marker records its owner's pid: a waiter only ever removes a
		// marker whose recorded owner is provably dead, never one it merely finds
		// slow, and a winner only removes the marker it created (same inode) - so
		// a slow live owner can never be displaced and two contenders can never
		// both pass the election.
		const election = `${path}.reclaim`;
		const temporary = `${path}.${ports.pid}.${randomUUID()}.tmp`;
		await writeFile(temporary, `${ports.pid}\n`, { flag: "wx", mode: 0o600 });
		let electedInode: number | bigint | undefined;
		try {
			for (let attempt = 0; electedInode === undefined; attempt++) {
				try {
					await link(temporary, election);
					electedInode = (await stat(temporary)).ino;
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
					if (attempt < RECLAIM_WAIT_ATTEMPTS) {
						await new Promise((resolve) => setTimeout(resolve, RECLAIM_WAIT_STEP_MS));
						continue;
					}
					// Waited the whole window. A marker is an orphan only when it is OLDER
					// than that window (so its owner had every chance to finish) AND its
					// recorded owner is gone; the liveness probe alone is not enough,
					// because a winner still booting may not be visible to it yet. Anything
					// else fails closed: a second adapter is worse than a delayed one.
					const orphan = await orphanedMarker(election, ports);
					if (orphan !== undefined) {
						await unlinkIfHolder(election, orphan);
						attempt = 0;
						continue;
					}
					// Name the process that actually holds the adapter: a live pidfile holder
					// if one appeared meanwhile, else whoever is still reclaiming.
					const live = await readHolder(path);
					throw new AdapterAlreadyRunningError(
						live !== undefined && live !== holder && live !== ports.pid ? live : ((await readHolder(election)) ?? 0),
						path,
					);
				}
			}
			// Elected. The pidfile must still name the dead holder read above; any
			// other pid is a claim an earlier winner made moments ago.
			const current = await readHolder(path);
			if (current !== holder && current !== ports.pid) throw new AdapterAlreadyRunningError(current ?? 0, path);
			await rename(temporary, path);
			return new AdapterLock(path, ports.pid);
		} finally {
			if (electedInode !== undefined) await unlinkIfInode(election, electedInode);
			await rm(temporary, { force: true });
		}
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

/**
 * The pid recorded in an election marker that is provably abandoned: older than
 * the full wait window and naming a process that is gone. Undefined otherwise.
 */
async function orphanedMarker(path: string, ports: AdapterLockPorts): Promise<number | undefined> {
	try {
		const info = await stat(path);
		if (Date.now() - info.mtimeMs < RECLAIM_WAIT_ATTEMPTS * RECLAIM_WAIT_STEP_MS) return undefined;
	} catch {
		return undefined;
	}
	const owner = await readHolder(path);
	if (owner === undefined) return undefined;
	return owner !== ports.pid && !ports.alive(owner) ? owner : undefined;
}

/** Removes the election marker only while it still records `owner`; a newer marker is someone else's. */
async function unlinkIfHolder(path: string, owner: number): Promise<void> {
	if ((await readHolder(path)) !== owner) return;
	await unlink(path).catch((error: NodeJS.ErrnoException) => {
		if (error.code !== "ENOENT") throw error;
	});
}

/** Removes the election marker only while it is still the very file this contender linked. */
async function unlinkIfInode(path: string, inode: number | bigint): Promise<void> {
	try {
		if ((await stat(path)).ino !== inode) return;
	} catch {
		return;
	}
	await unlink(path).catch((error: NodeJS.ErrnoException) => {
		if (error.code !== "ENOENT") throw error;
	});
}
