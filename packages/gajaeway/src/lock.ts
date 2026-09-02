import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
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

/**
 * Per-home daemon ownership guard. A state that cannot be proven stale is never
 * reclaimed: the gateway's socket boot path unlinks blindly, so a second owner
 * would otherwise take down the resident daemon.
 */
export class DaemonLock {
	private constructor(
		readonly path: string,
		readonly pid: number,
	) {}

	static async acquire(home: string, ports: DaemonLockPorts = defaultDaemonLockPorts()): Promise<DaemonLock> {
		const path = join(home, "gajaeway.pid");
		await mkdir(home, { recursive: true, mode: 0o700 });
		if (await claim(path, ports.pid)) return new DaemonLock(path, ports.pid);

		const holder = await readHolder(path);
		if (holder === ports.pid) {
			throw new DaemonLockRefusalError("lock names this process; a previous run of this pid left it behind");
		}

		switch (safeLiveness(ports, holder, path)) {
			case "alive":
				throw new DaemonLockRefusalError(`Another gajaeway daemon is already running (pid ${holder})`);
			case "unknown":
				throw new DaemonLockRefusalError(
					`daemon lock ${path} cannot prove whether pid ${holder} is running; remove it by hand only after confirming no gajaeway daemon is running`,
				);
			case "dead":
				break;
		}

		try {
			await rm(path);
		} catch {
			throw new DaemonLockRefusalError(
				`daemon lock ${path} could not be reclaimed; remove it by hand only after confirming no gajaeway daemon is running`,
			);
		}
		if (!(await claim(path, ports.pid))) {
			throw new DaemonLockRefusalError(`daemon lock ${path} could not be re-claimed; another process may be starting`);
		}
		ports.log?.(`daemon_lock_reclaimed stale_pid=${holder}`);
		return new DaemonLock(path, ports.pid);
	}

	/** Drops the file only while it still names this lock's owner PID. */
	async release(): Promise<void> {
		try {
			if ((await readHolder(this.path)) !== this.pid) return;
			await rm(this.path);
		} catch {
			// An unreadable or replaced lock is not ours to remove.
		}
	}
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
