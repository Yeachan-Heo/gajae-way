import { randomUUID } from "node:crypto";
import { link, mkdir, readdir, readFile, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";

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
	/** Test-only seam for a slow critical section after election and before the pidfile write. */
	readonly beforePidfileWrite?: () => void | Promise<void>;
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

/** How long a contender waits on a live reclaimer before failing closed (200 x 5ms = 1s). */
const RECLAIM_WAIT_ATTEMPTS = 200;
const RECLAIM_WAIT_STEP_MS = 5;
const RECLAIM_WAIT_WINDOW_MS = RECLAIM_WAIT_ATTEMPTS * RECLAIM_WAIT_STEP_MS;
const RECLAIM_HEARTBEAT_MS = Math.max(1, Math.floor(RECLAIM_WAIT_WINDOW_MS / 4));

export class AdapterLock {
	private constructor(
		readonly path: string,
		readonly pid: number,
	) {}

	static async acquire(home: string, ports: AdapterLockPorts = defaultLockPorts()): Promise<AdapterLock> {
		const path = join(home, "adapter-discord.pid");
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
		// The election is a DIRECTORY that is fully prepared in private - owner
		// file already written - and then `rename`d into place. Renaming a
		// directory onto a path that is already a non-empty directory fails
		// atomically (ENOTEMPTY/EEXIST), so exactly one contender's rename lands,
		// and the winning election is never observable without its owner: there
		// is no mkdir-then-write window in which a crash leaves an ownerless
		// election behind. The elected directory's inode is remembered and
		// re-verified right before the pidfile is replaced, so an election that
		// was moved out from under us (by a stale reclaimer) is detected and the
		// acquisition fails closed rather than admitting a second winner.
		const election = `${path}.reclaim.d`;
		const candidate = `${path}.${ports.pid}.${randomUUID()}.candidate`;
		await mkdir(candidate, { mode: 0o700 });
		await writeFile(join(candidate, "owner"), `${ports.pid}\n`, { mode: 0o600 });
		const candidateInode = (await stat(candidate)).ino;
		let elected = false;
		let heartbeat: ReturnType<typeof setInterval> | undefined;
		try {
			for (let attempt = 0; !elected; attempt++) {
				try {
					await utimes(candidate, new Date(), new Date());
					await rename(candidate, election);
					try {
						await utimes(election, new Date(), new Date());
					} catch (error) {
						if ((error as NodeJS.ErrnoException).code === "ENOENT")
							throw new AdapterAlreadyRunningError((await readHolder(path)) ?? 0, path);
						throw error;
					}
					elected = true;
					heartbeat = setInterval(() => {
						void utimes(election, new Date(), new Date()).catch(() => {});
					}, RECLAIM_HEARTBEAT_MS);
					heartbeat.unref?.();
					break;
				} catch (error) {
					const code = (error as NodeJS.ErrnoException).code;
					// EEXIST/ENOTEMPTY: someone else holds the election. Anything else
					// is a real filesystem problem and is surfaced, not masked.
					if (code !== "EEXIST" && code !== "ENOTEMPTY") throw error;
				}
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
					await reclaimElection(election, orphan.inode, ports.pid);
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
			// Elected. The election must still be OURS (a stale reclaimer may have
			// moved it) and the pidfile must still name the dead holder read above;
			// any other pid is a claim an earlier winner made moments ago.
			await assertElectionOwner(election, candidateInode, path);
			const current = await readHolder(path);
			if (current !== holder) throw new AdapterAlreadyRunningError(current ?? 0, path);
			await ports.beforePidfileWrite?.();
			await assertElectionOwner(election, candidateInode, path);
			if ((await readHolder(path)) !== holder)
				throw new AdapterAlreadyRunningError((await readHolder(path)) ?? 0, path);
			const temporary = `${path}.${ports.pid}.${randomUUID()}.tmp`;
			await writeFile(temporary, `${ports.pid}\n`, { flag: "wx", mode: 0o600 });
			const previous = `${path}.${ports.pid}.${randomUUID()}.previous`;
			let previousMoved = false;
			let installedInode: number | bigint | undefined;
			try {
				const expectedInode = await stat(path)
					.then((info) => info.ino)
					.catch(() => undefined);
				if (expectedInode !== undefined) {
					await rename(path, previous);
					previousMoved = true;
					const movedInode = (await stat(previous)).ino;
					if (movedInode !== expectedInode || (await readHolder(previous)) !== holder) {
						await restoreNoReplace(previous, path);
						previousMoved = false;
						throw new AdapterAlreadyRunningError((await readHolder(path)) ?? 0, path);
					}
				}
				await assertElectionOwner(election, candidateInode, path);
				await linkNoReplace(temporary, path);
				installedInode = (await stat(path)).ino;
				await assertElectionOwner(election, candidateInode, path);
				if (previousMoved) {
					await rm(previous, { force: true });
					previousMoved = false;
				}
			} catch (error) {
				if (installedInode !== undefined) await removeFileIfInode(path, installedInode);
				if (previousMoved) await restoreNoReplace(previous, path);
				throw error;
			} finally {
				await rm(temporary, { force: true });
				await rm(previous, { force: true });
			}
			return new AdapterLock(path, ports.pid);
		} finally {
			if (heartbeat !== undefined) clearInterval(heartbeat);
			// Only the elected contender removes the election, and only while it is
			// still the directory it installed (same inode).
			if (elected) await removeIfInode(election, candidateInode);
			await rm(candidate, { recursive: true, force: true });
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

async function assertElectionOwner(dir: string, inode: number | bigint, path: string): Promise<void> {
	if ((await stat(dir).catch(() => undefined))?.ino === inode) return;
	throw new AdapterAlreadyRunningError((await readHolder(path)) ?? 0, path);
}

async function linkNoReplace(source: string, destination: string): Promise<void> {
	try {
		await link(source, destination);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "EEXIST") {
			throw new AdapterAlreadyRunningError((await readHolder(destination)) ?? 0, destination);
		}
		throw error;
	}
}

/** Restores a moved pidfile only while the destination is still free. */
async function restoreNoReplace(source: string, destination: string): Promise<void> {
	try {
		await link(source, destination);
		await rm(source, { force: true });
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "EEXIST" && (error as NodeJS.ErrnoException).code !== "ENOENT")
			throw error;
		await rm(source, { force: true });
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
	// Elections are prepared with their owner file BEFORE being renamed into
	// place, so a live election always has one. An ownerless directory older
	// than the wait window is debris (a hand-made or partially copied one) and
	// is reclaimable; leaving it would lock every future start out.
	if (owner === undefined) return { owner: 0, inode };
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
		// Best-effort: a concurrent sweep may be removing the same tombstone.
		await rm(tombstone, { recursive: true, force: true }).catch(() => {});
		return;
	}
	// Wrong directory: a live election was in progress. Restore it if the slot
	// is still free; if someone else already elected there, leave the fresh
	// directory as a tombstone for the sweep (its owner's writeFile will ENOENT
	// and re-elect).
	await rename(tombstone, dir).catch(async (cause: NodeJS.ErrnoException) => {
		// EEXIST/ENOTEMPTY: the slot was re-taken meanwhile; ENOENT: another
		// contender's sweep already removed this tombstone. Either way the
		// displaced election is gone, and its owner fails closed on its inode
		// check before it could touch the pidfile.
		if (cause.code !== "EEXIST" && cause.code !== "ENOTEMPTY" && cause.code !== "ENOENT") throw cause;
		await rm(tombstone, { recursive: true, force: true }).catch(() => {});
	});
}

/** Removes the election directory only while it is still the inode we installed. */
async function removeIfInode(dir: string, inode: number | bigint): Promise<void> {
	try {
		if ((await stat(dir)).ino !== inode) return;
	} catch {
		return;
	}
	await rm(dir, { recursive: true, force: true }).catch(() => {});
}

/** Removes a pidfile only while it is still the inode this contender installed. */
async function removeFileIfInode(path: string, inode: number | bigint): Promise<void> {
	const tombstone = `${path}.${randomUUID()}.lost`;
	try {
		await rename(path, tombstone);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		return;
	}
	try {
		if ((await stat(tombstone)).ino === inode) {
			await rm(tombstone, { force: true });
			return;
		}
		await restoreNoReplace(tombstone, path);
	} catch {
		await rm(tombstone, { force: true }).catch(() => {});
	}
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
