import { randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

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
		// a process still booting, which a probe may not see yet.
		//
		// The election is a DIRECTORY. `mkdir` is the one primitive here that is
		// both atomic and exclusive on every POSIX filesystem: exactly one caller
		// creates it, everyone else gets EEXIST, and there is no read-then-act
		// window to race (unlike link/rename of a marker file, which a delayed
		// contender could move after another had inspected it). The directory
		// records its owner's pid inside; a crashed owner's directory is reclaimed
		// by renaming the whole directory away - one rename wins, the loser gets
		// ENOENT - and re-electing through mkdir again.
		const election = `${path}.reclaim.d`;
		let elected = false;
		try {
			for (let attempt = 0; !elected; attempt++) {
				try {
					await mkdir(election);
					try {
						await writeFile(join(election, "owner"), `${ports.pid}\n`, { mode: 0o600 });
					} catch {
						// The directory moved between mkdir and the owner write: a stale
						// reclaimer renamed it (and puts it back on seeing our inode). Not
						// elected; go around again.
						continue;
					}
					elected = true;
				} catch (error) {
					// EEXIST is the normal "someone else is electing"; anything else while
					// contenders shuffle directories around (ENOENT/EINVAL from a rename
					// landing mid-call) is transient and simply retried.
					if ((error as NodeJS.ErrnoException).code === "EACCES") throw error;
					if (attempt < RECLAIM_WAIT_ATTEMPTS) {
						await new Promise((resolve) => setTimeout(resolve, RECLAIM_WAIT_STEP_MS));
						continue;
					}
					// Waited the whole window. An election is abandoned only when it is
					// OLDER than that window and its recorded owner is gone; the liveness
					// probe alone is not enough, because a winner still booting may not be
					// visible to it yet. Anything else fails closed.
					const orphan = await orphanedElection(election, ports);
					if (orphan !== undefined) {
						await reclaimElection(election, orphan.inode, ports.pid).catch(() => {});
						attempt = 0;
						continue;
					}
					const live = await readHolder(path);
					throw new AdapterAlreadyRunningError(
						live !== undefined && live !== holder && live !== ports.pid
							? live
							: ((await readHolder(join(election, "owner"))) ?? 0),
						path,
					);
				}
			}
			// Elected. The pidfile must still name the dead holder read above; any
			// other pid is a claim an earlier winner made moments ago.
			const current = await readHolder(path);
			if (current !== holder && current !== ports.pid) throw new AdapterAlreadyRunningError(current ?? 0, path);
			const temporary = `${path}.${ports.pid}.${randomUUID()}.tmp`;
			await writeFile(temporary, `${ports.pid}\n`, { flag: "wx", mode: 0o600 });
			await rename(temporary, path);
			return new AdapterLock(path, ports.pid);
		} finally {
			// Only the elected contender owns the directory; a loser never touches it.
			if (elected) await rm(election, { recursive: true, force: true }).catch(() => {});
			// Tombstones of reclaimed elections are best-effort garbage.
			await sweepTombstones(path);
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
 * The pid recorded in an election directory that is provably abandoned: older
 * than the full wait window and naming a process that is gone. Undefined otherwise.
 */
async function orphanedElection(
	dir: string,
	ports: AdapterLockPorts,
): Promise<{ readonly owner: number; readonly inode: number | bigint } | undefined> {
	let inode: number | bigint;
	try {
		const info = await stat(dir);
		if (Date.now() - info.mtimeMs < RECLAIM_WAIT_ATTEMPTS * RECLAIM_WAIT_STEP_MS) return undefined;
		inode = info.ino;
	} catch {
		return undefined;
	}
	const owner = await readHolder(join(dir, "owner"));
	// A directory with no owner file yet is mid-creation; leave it alone.
	if (owner === undefined) return undefined;
	return owner !== ports.pid && !ports.alive(owner) ? { owner, inode } : undefined;
}

/**
 * Moves an abandoned election out of the way - but only the exact directory
 * that was judged abandoned. `rename` moves whatever inode currently sits at
 * the path, so after moving it the inode is checked: if a fresh election from
 * another contender was moved instead, it is put straight back (the two
 * renames are the only writers of that pathname, so this is the losing
 * reclaimer undoing its own mistake, not a new race).
 */
async function reclaimElection(dir: string, expectedInode: number | bigint, pid: number): Promise<void> {
	const tombstone = `${dir}.${pid}.${randomUUID()}.dead`;
	try {
		await rename(dir, tombstone);
	} catch (cause) {
		// ENOENT: another contender already reclaimed it.
		if ((cause as NodeJS.ErrnoException).code !== "ENOENT") throw cause;
		return;
	}
	let moved: number | bigint | undefined;
	try {
		moved = (await stat(tombstone)).ino;
	} catch {
		return;
	}
	if (moved === expectedInode) {
		await rm(tombstone, { recursive: true, force: true });
		return;
	}
	// Wrong directory: a live election was in progress. Restore it if the slot
	// is still free; if someone else already elected there, leave the fresh
	// directory as a tombstone for the sweep (its owner's writeFile will ENOENT
	// and re-elect).
	await rename(tombstone, dir).catch(async (cause: NodeJS.ErrnoException) => {
		if (cause.code !== "EEXIST" && cause.code !== "ENOTEMPTY") throw cause;
		await rm(tombstone, { recursive: true, force: true });
	});
}

/** Removes `.dead` tombstones of reclaimed elections; failures are ignored. */
async function sweepTombstones(path: string): Promise<void> {
	const dir = dirname(path);
	const prefix = `${basename(path)}.reclaim.d.`;
	let entries: string[];
	try {
		entries = await readdir(dir);
	} catch {
		return;
	}
	for (const entry of entries) {
		if (!entry.startsWith(prefix) || !entry.endsWith(".dead")) continue;
		// Another contender may be renaming the very same tombstone right now; a
		// failed sweep is not this acquisition's problem.
		await rm(join(dir, entry), { recursive: true, force: true }).catch(() => {});
	}
}
