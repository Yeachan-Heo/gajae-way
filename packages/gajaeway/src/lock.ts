import { mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";

export type PidLiveness = "alive" | "dead" | "unknown";

/** Process facts are injected so lock ownership can be tested without signalling real processes. */
export interface DaemonLockPorts {
	readonly pid: number;
	readonly liveness: (pid: number) => PidLiveness;
	readonly log?: (line: string) => void;
}

export class DaemonLockRefusalError extends Error {
	readonly exitCode = 2;

	constructor(message: string) {
		super(message);
		this.name = "DaemonLockRefusalError";
	}
}

/** `kill(pid, 0)`: EPERM proves a process exists; only ESRCH proves it is gone. */
export function processLiveness(pid: number): PidLiveness {
	try {
		process.kill(pid, 0);
		return "alive";
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "EPERM") return "alive";
		if (code === "ESRCH") return "dead";
		return "unknown";
	}
}

export function defaultDaemonLockPorts(): DaemonLockPorts {
	return { pid: process.pid, liveness: processLiveness, log: (line) => console.error(line) };
}

/** How long a newcomer waits for a live predecessor to finish its ordered shutdown before failing closed. */
export const PREDECESSOR_EXIT_WAIT_MS = 20_000;

export interface DaemonLockAcquireOptions {
	/**
	 * `--only-new`: a live holder is an immediate refusal. Without it the newcomer
	 * waits (bounded) for the holder to exit - the service manager already sent
	 * it the stop signal, or is about to - and only then claims the lock.
	 */
	readonly onlyNew?: boolean;
	readonly waitMs?: number;
	readonly sleep?: (ms: number) => Promise<void>;
}

/**
 * Per-home daemon ownership guard, and the daemon's lifecycle belongs to the
 * service manager (launchd / systemd): this never signals a peer. A state that
 * cannot be proven stale is never reclaimed - the gateway's socket boot path
 * unlinks blindly, so a second owner would otherwise take down the resident
 * daemon.
 *
 * Live finding (2026-09-03): `launchctl kickstart -k` restarts the job before
 * the previous process has finished its ordered shutdown; two daemons then
 * supervised one private gjc broker and took turns retiring it. A live holder
 * is therefore WAITED OUT rather than refused outright, and a holder that
 * outlives the wait makes the newcomer exit for the service manager to retry
 * under its own throttle.
 */
export class DaemonLock {
	private constructor(
		readonly path: string,
		readonly pid: number,
	) {}

	static async acquire(
		home: string,
		ports: DaemonLockPorts = defaultDaemonLockPorts(),
		options: DaemonLockAcquireOptions = {},
	): Promise<DaemonLock> {
		const path = join(home, "gajaeway.pid");
		await mkdir(home, { recursive: true, mode: 0o700 });
		if (await claim(path, ports.pid)) return new DaemonLock(path, ports.pid);

		const holder = await readHolder(path);
		if (holder === ports.pid) {
			throw new DaemonLockRefusalError("lock names this process; a previous run of this pid left it behind");
		}

		switch (safeLiveness(ports, holder, path)) {
			case "alive": {
				if (options.onlyNew)
					throw new DaemonLockRefusalError(
						`Another gajaeway daemon is already running (pid ${holder}); --only-new refuses to wait for it`,
					);
				const waitMs = options.waitMs ?? PREDECESSOR_EXIT_WAIT_MS;
				ports.log?.(`daemon_predecessor_live pid=${holder} action=wait maxMs=${waitMs}`);
				if (!(await waitForDead(ports, holder, path, waitMs, options.sleep ?? ((ms) => Bun.sleep(ms)))))
					throw new DaemonLockRefusalError(
						`Another gajaeway daemon is already running (pid ${holder}) and did not exit within ${waitMs}ms; the service manager owns its lifecycle, retry after it exits`,
					);
				ports.log?.(`daemon_predecessor_exited pid=${holder}`);
				break;
			}
			case "unknown":
				throw new DaemonLockRefusalError(
					`daemon lock ${path} cannot prove whether pid ${holder} is running; remove it by hand only after confirming no gajaeway daemon is running`,
				);
			case "dead":
				break;
		}

		// Replacement-safe reclaim. Two starters can both prove the same holder
		// dead; without serialisation the loser would unlink the WINNER's fresh
		// lock. The reclaim token is itself O_EXCL, so exactly one starter may
		// reclaim a given stale file; it then re-verifies the file still holds the
		// dead pid it observed (same inode, same content) before unlinking.
		const token = `${path}.reclaim`;
		if (!(await claim(token, ports.pid))) {
			throw new DaemonLockRefusalError(`daemon lock ${path} is being reclaimed by another process; retry shortly`);
		}
		try {
			const before = await identity(path);
			if (before === undefined || before.holder !== holder) {
				throw new DaemonLockRefusalError(
					`daemon lock ${path} changed while reclaiming; another process may be starting`,
				);
			}
			const again = await identity(path);
			if (again === undefined || again.ino !== before.ino || again.holder !== holder) {
				throw new DaemonLockRefusalError(
					`daemon lock ${path} changed while reclaiming; another process may be starting`,
				);
			}
			try {
				await rm(path);
			} catch {
				throw new DaemonLockRefusalError(
					`daemon lock ${path} could not be reclaimed; remove it by hand only after confirming no gajaeway daemon is running`,
				);
			}
			if (!(await claim(path, ports.pid))) {
				throw new DaemonLockRefusalError(
					`daemon lock ${path} could not be re-claimed; another process may be starting`,
				);
			}
			ports.log?.(`daemon_lock_reclaimed stale_pid=${holder}`);
			return new DaemonLock(path, ports.pid);
		} finally {
			await rm(token, { force: true });
		}
	}

	/** Drops the file only while it still names this lock's owner PID. */
	async release(): Promise<void> {
		try {
			const mine = await identity(this.path);
			if (mine === undefined || mine.holder !== this.pid) return;
			await rm(this.path);
		} catch {
			// An unreadable or replaced lock is not ours to remove.
		}
	}
}

async function waitForDead(
	ports: DaemonLockPorts,
	pid: number,
	path: string,
	timeoutMs: number,
	sleep: (ms: number) => Promise<void>,
): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (safeLiveness(ports, pid, path) === "dead") return true;
		await sleep(100);
	}
	return safeLiveness(ports, pid, path) === "dead";
}

function safeLiveness(ports: DaemonLockPorts, pid: number, path: string): PidLiveness {
	try {
		return ports.liveness(pid);
	} catch {
		throw new DaemonLockRefusalError(
			`daemon lock ${path} cannot prove whether pid ${pid} is running; remove it by hand only after confirming no gajaeway daemon is running`,
		);
	}
}

/** Inode + parsed holder, or undefined when the file is gone. */
async function identity(path: string): Promise<{ ino: number; holder: number } | undefined> {
	try {
		const [info, holder] = await Promise.all([stat(path), readHolder(path)]);
		return { ino: Number(info.ino), holder };
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
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

async function readHolder(path: string): Promise<number> {
	let raw: string;
	try {
		raw = await readFile(path, "utf8");
	} catch {
		throw malformedLock(path);
	}
	const text = raw.trim();
	const pid = Number(text);
	if (!/^[0-9]+$/.test(text) || !Number.isSafeInteger(pid) || pid <= 0) throw malformedLock(path);
	return pid;
}

function malformedLock(path: string): DaemonLockRefusalError {
	return new DaemonLockRefusalError(
		`daemon lock ${path} is unreadable or malformed; remove it by hand only after confirming no gajaeway daemon is running`,
	);
}
