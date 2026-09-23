import { dlopen, FFIType } from "bun:ffi";
import { constants } from "node:fs";
import { lstat, open, readFile, rename, unlink } from "node:fs/promises";
import { join } from "node:path";

export const HOME_LOCK_FILE = "gateway-home.lock";

// The inode is permanent. Only the kernel may release this lock (close/exit),
// never a PID check followed by unlink, which admits two simultaneous holders.
let advisoryLock: { readonly symbols: { readonly flock: (fd: number, operation: number) => number } } | undefined;
function flock(fd: number, operation: number): number {
	if (!advisoryLock) {
		const library =
			process.platform === "darwin"
				? "/usr/lib/libSystem.B.dylib"
				: process.platform === "linux"
					? "libc.so.6"
					: undefined;
		if (!library) throw new Error("gateway_home_lock_platform_unsupported");
		advisoryLock = dlopen(library, { flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 } });
	}
	return advisoryLock.symbols.flock(fd, operation);
}

export interface GatewayHomeLease {
	readonly token: string;
	release(): Promise<void>;
}

/** Shared by boot and offline administration; does not touch daemon.pid or SDK files. */
export async function acquireGatewayHome(home: string): Promise<GatewayHomeLease> {
	const path = join(home, HOME_LOCK_FILE);
	const handle = await open(
		path,
		constants.O_RDWR | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
		0o600,
	).catch((error: NodeJS.ErrnoException) => {
		if (error.code !== "EEXIST") throw error;
		return open(path, constants.O_RDWR | constants.O_NOFOLLOW);
	});
	try {
		const info = await handle.stat();
		if (!info.isFile() || info.nlink !== 1 || info.size > 4096) throw new Error("gateway_home_owner_indeterminate");
		if (flock(handle.fd, 2 | 4) !== 0) throw new Error("gateway_home_owned"); // LOCK_EX | LOCK_NB
		const raw = await handle.readFile("utf8");
		if (raw.length > 4096) throw new Error("gateway_home_owner_indeterminate");
		// An empty lock file carries no owner record: it is either a file this
		// call just created, one another racer created between our O_EXCL failure
		// and our open, or one abandoned by a process that died before persisting.
		// We hold the kernel lock, so no live owner exists; refusing here would
		// let two simultaneous acquisitions both lose and brick the home forever.
		if (raw.length) {
			let owner: Record<string, unknown>;
			try {
				owner = JSON.parse(raw);
			} catch {
				throw new Error("gateway_home_owner_indeterminate");
			}
			if (
				!owner ||
				!Number.isSafeInteger(owner.pid) ||
				(owner.pid as number) <= 0 ||
				typeof owner.token !== "string" ||
				!owner.token ||
				typeof owner.startedAt !== "string" ||
				!Number.isFinite(Date.parse(owner.startedAt)) ||
				!["held", "released"].includes(owner.state as string)
			)
				throw new Error("gateway_home_owner_indeterminate");
			// A valid abandoned record is recoverable only after kernel ownership is
			// acquired AND its old process is proven gone. PID reuse fails closed.
			if (owner.state === "held" && defaultTakeoverPorts().isPidAlive(owner.pid as number))
				throw new Error("gateway_home_owner_live");
		}
		const token = crypto.randomUUID();
		const owner = { pid: process.pid, token, startedAt: new Date().toISOString(), state: "held" };
		const persist = async () => {
			const bytes = Buffer.from(`${JSON.stringify(owner)}\n`);
			let offset = 0;
			while (offset < bytes.length) {
				const result = await handle.write(bytes, offset, bytes.length - offset, offset);
				if (!result.bytesWritten) throw new Error("gateway_home_owner_write_failed");
				offset += result.bytesWritten;
			}
			await handle.truncate(bytes.length);
			await handle.sync();
		};
		await persist();
		let released = false;
		return {
			token,
			release: async () => {
				if (released) return;
				released = true;
				try {
					owner.state = "released";
					await persist();
				} finally {
					await handle.close();
				}
			},
		};
	} catch (error) {
		await handle.close();
		throw error;
	}
}

/**
 * One gateway per GAJAEWAY_HOME. The daemon's lifecycle belongs to the service
 * manager (launchd / systemd); the gateway never starts, signals, or kills a
 * peer. What it DOES own is the decision to proceed.
 *
 * Live finding (2026-09-03): `launchctl kickstart -k` restarts the job before
 * the previous process has finished its ordered shutdown. During that window
 * the replacement unlinked the socket and contested `broker.lock`, and once
 * through, two supervisors observed one private gjc daemon and took turns
 * retiring it - seventy generations in an hour. The socket and the broker
 * lock are per-home resources; ownership must be settled before either is
 * touched, and it must be settled without the gateway becoming a process
 * manager.
 *
 * The kernel lease above is the ownership authority for both boot and admin;
 * a contending lease refuses immediately so the service manager can retry.
 * The PID record below remains service presentation and a conservative guard
 * against pre-lock daemons. Proven same-home legacy daemons are waited out
 * (bounded), unless --only-new is requested. Only dead PID records are stale;
 * malformed records and live unrelated processes are never silently replaced.
 */

export const PID_FILE = "daemon.pid";
const PREDECESSOR_EXIT_WAIT_MS = 20_000;

export interface PidRecord {
	readonly pid: number;
	readonly home: string;
	readonly startedAt: string;
}

export interface TakeoverPorts {
	readonly isPidAlive: (pid: number) => boolean;
	/** The command line of `pid`, or undefined when it cannot be read (gone or unreadable). */
	readonly commandOf: (pid: number) => string | undefined;
	readonly sleep: (ms: number) => Promise<void>;
	readonly log: (line: string) => void;
}

export class GatewayAlreadyRunningError extends Error {
	readonly code = "gateway_already_running";
	constructor(
		readonly pid: number,
		home: string,
		detail: string,
	) {
		super(`a gateway for ${home} is already running as pid ${pid}: ${detail}`);
	}
}

export function pidFilePath(home: string): string {
	return join(home, PID_FILE);
}

export async function readPidRecord(home: string): Promise<PidRecord | undefined> {
	let raw: string;
	try {
		raw = await readFile(pidFilePath(home), "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
	try {
		const parsed: unknown = JSON.parse(raw);
		if (typeof parsed !== "object" || parsed === null) return undefined;
		const record = parsed as Record<string, unknown>;
		if (
			typeof record.pid !== "number" ||
			!Number.isSafeInteger(record.pid) ||
			record.pid <= 0 ||
			typeof record.home !== "string" ||
			typeof record.startedAt !== "string"
		)
			return undefined;
		return { pid: record.pid, home: record.home, startedAt: record.startedAt };
	} catch {
		return undefined;
	}
}

/** True only for a live process that is a gateway daemon for exactly this home. */
export function isSameHomeGateway(record: PidRecord, home: string, ports: TakeoverPorts): boolean {
	if (record.home !== home) return false;
	if (record.pid === process.pid) return false;
	if (!ports.isPidAlive(record.pid)) return false;
	const command = ports.commandOf(record.pid);
	return command !== undefined && /gajaeway-gateway(?:\s|$)/.test(command) && /\bdaemon\b/.test(command);
}

/**
 * Publishes the service PID after the caller acquires an exclusive home lease.
 * Returns the pid of a legacy predecessor waited out, or undefined. This PID
 * presentation alone is not an exclusive lock. Never signals another process.
 */
export async function claimGatewayHome(
	home: string,
	options: { readonly onlyNew: boolean; readonly waitMs?: number },
	ports: TakeoverPorts,
): Promise<number | undefined> {
	const existing = await readPidRecord(home);
	if (!existing) {
		try {
			await lstat(pidFilePath(home));
			throw new Error("gateway_pid_indeterminate");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
	}
	let predecessor: number | undefined;
	if (existing && isSameHomeGateway(existing, home, ports)) {
		if (options.onlyNew) throw new GatewayAlreadyRunningError(existing.pid, home, "--only-new refuses to wait for it");
		const waitMs = options.waitMs ?? PREDECESSOR_EXIT_WAIT_MS;
		ports.log(
			`gateway_predecessor_live pid=${existing.pid} startedAt=${existing.startedAt} action=wait maxMs=${waitMs}`,
		);
		if (!(await waitForExit(existing.pid, waitMs, ports)))
			throw new GatewayAlreadyRunningError(
				existing.pid,
				home,
				`still alive after ${waitMs}ms; the service manager owns its lifecycle, retry after it exits`,
			);
		ports.log(`gateway_predecessor_exited pid=${existing.pid}`);
		predecessor = existing.pid;
	} else if (existing) {
		if (ports.isPidAlive(existing.pid))
			throw new GatewayAlreadyRunningError(
				existing.pid,
				home,
				"live process identity is not a proven same-home gateway",
			);
		ports.log(`gateway_pid_stale pid=${existing.pid} home=${existing.home} reason=not_a_live_gateway_for_this_home`);
	}
	await writePidRecord(home, { pid: process.pid, home, startedAt: new Date().toISOString() });
	return predecessor;
}

/** Removes the record only if it still names this process; a successor's record is never touched. */
export async function releaseGatewayHome(home: string): Promise<void> {
	const current = await readPidRecord(home);
	if (current?.pid !== process.pid) return;
	try {
		await unlink(pidFilePath(home));
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}

async function writePidRecord(home: string, record: PidRecord): Promise<void> {
	const path = pidFilePath(home);
	const temporary = `${path}.${process.pid}.tmp`;
	const handle = await open(temporary, "w", 0o600);
	try {
		await handle.writeFile(`${JSON.stringify(record)}\n`);
	} finally {
		await handle.close();
	}
	await rename(temporary, path);
}

async function waitForExit(pid: number, timeoutMs: number, ports: TakeoverPorts): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (!ports.isPidAlive(pid)) return true;
		await ports.sleep(100);
	}
	return !ports.isPidAlive(pid);
}

export function defaultTakeoverPorts(log: (line: string) => void = (line) => console.error(line)): TakeoverPorts {
	return {
		isPidAlive: (pid) => {
			try {
				process.kill(pid, 0);
				return true;
			} catch (error) {
				return (error as NodeJS.ErrnoException).code !== "ESRCH";
			}
		},
		commandOf: (pid) => {
			const result = Bun.spawnSync(["ps", "-o", "command=", "-p", String(pid)]);
			const command = result.stdout.toString().trim();
			return result.exitCode === 0 && command.length > 0 ? command : undefined;
		},
		sleep: (ms) => Bun.sleep(ms),
		log,
	};
}
