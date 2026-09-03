import { open, readFile, rename, unlink } from "node:fs/promises";
import { join } from "node:path";

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
 * Policy: when a live same-home gateway holds the pid record, the newcomer
 * WAITS (bounded) for it to exit - the service manager already sent it the
 * stop signal, or is about to. If it is still alive at the deadline the
 * newcomer exits non-zero and lets the service manager retry under its own
 * throttle. With `--only-new` there is no wait: any live predecessor is an
 * immediate refusal. A pid record whose process is dead, or is not a gateway
 * for THIS home, is stale and is replaced.
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
 * Settles ownership of `home` for this process and records it. Returns the
 * pid of the predecessor this process waited out, or undefined when there was
 * none. Never signals another process.
 */
export async function claimGatewayHome(
	home: string,
	options: { readonly onlyNew: boolean; readonly waitMs?: number },
	ports: TakeoverPorts,
): Promise<number | undefined> {
	const existing = await readPidRecord(home);
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
				return (error as NodeJS.ErrnoException).code === "EPERM";
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
