import { dlopen, FFIType, read } from "bun:ffi";
import { constants } from "node:fs";
import { type FileHandle, mkdir, open, readFile } from "node:fs/promises";
import { homeLayout, type LeaseName } from "./layout";

/**
 * The one ownership primitive in gajae-way. A lease is an exclusive,
 * non-blocking kernel flock on a permanent file under $GAJAEWAY_HOME/leases.
 * Only the kernel releases it (close or process exit); the file is never
 * unlinked and no PID liveness check ever decides ownership. The owner record
 * inside the file is diagnostic only.
 */

const LOCK_EX = 2;
const LOCK_NB = 4;
const F_GETFD = 1;
const FD_CLOEXEC = 1;
const MAX_OWNER_BYTES = 4096;

interface Platform {
	readonly library: string;
	readonly errnoSymbol: "__error" | "__errno_location";
	readonly oCloexec: number;
	readonly ewouldblock: number;
}

const PLATFORMS: Partial<Record<NodeJS.Platform, Platform>> = {
	darwin: { library: "/usr/lib/libSystem.B.dylib", errnoSymbol: "__error", oCloexec: 0x1000000, ewouldblock: 35 },
	linux: { library: "libc.so.6", errnoSymbol: "__errno_location", oCloexec: 0o2000000, ewouldblock: 11 },
};

interface Libc {
	readonly platform: Platform;
	flock(fd: number, operation: number): number;
	getfd(fd: number): number;
	errno(): number;
}

let libc: Libc | undefined;

// A held lease must never be closed by garbage collection: that would drop the
// kernel lock while the owner still believes it holds it. Held handles stay
// reachable here until release().
const heldHandles = new Set<FileHandle>();

function loadLibc(name: LeaseName): Libc {
	if (libc) return libc;
	const platform = PLATFORMS[process.platform];
	if (!platform) throw new LeaseError("lease_unsupported", name, "dlopen", process.platform);
	const symbols = {
		flock: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
		// F_GETFD takes no third argument, so calling the variadic fcntl with
		// two fixed arguments is ABI-safe on every supported platform.
		fcntl: { args: [FFIType.i32, FFIType.i32], returns: FFIType.i32 },
		[platform.errnoSymbol]: { args: [], returns: FFIType.ptr },
	} as const;
	const library = dlopen(platform.library, symbols);
	const errnoLocation = library.symbols[platform.errnoSymbol] as unknown as () => number;
	libc = {
		platform,
		flock: (fd, operation) => library.symbols.flock(fd, operation),
		getfd: (fd) => library.symbols.fcntl(fd, F_GETFD),
		errno: () => read.i32(errnoLocation() as never, 0),
	};
	return libc;
}

export interface LeaseOwner {
	readonly pid: number;
	readonly token: string;
	readonly startedAt: string;
}

export class LeaseHeldError extends Error {
	readonly code = "lease_held";
	constructor(
		readonly name: LeaseName,
		readonly owner: LeaseOwner | undefined,
	) {
		super(
			`lease_held name=${name}${owner ? ` owner_pid=${owner.pid} since=${owner.startedAt}` : ""}`,
		);
	}
}

export class LeaseError extends Error {
	constructor(
		readonly code: "lease_failed" | "lease_unsupported",
		readonly name: LeaseName,
		readonly syscall: string,
		readonly errno: number | string,
	) {
		super(`${code} name=${name} syscall=${syscall} errno=${errno}`);
	}
}

export interface Lease extends AsyncDisposable {
	readonly name: LeaseName;
	readonly token: string;
	readonly path: string;
	/** Closes the descriptor; the kernel drops the lock. Idempotent. Never unlinks. */
	release(): Promise<void>;
}

async function openLeaseFile(path: string, name: LeaseName, lib: Libc, create: boolean): Promise<FileHandle> {
	const flags = constants.O_RDWR | constants.O_NOFOLLOW | lib.platform.oCloexec | (create ? constants.O_CREAT : 0);
	let handle: FileHandle;
	try {
		handle = await open(path, flags, 0o600);
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code ?? String(error);
		throw new LeaseError("lease_failed", name, "open", code);
	}
	try {
		const info = await handle.stat();
		if (!info.isFile() || info.nlink !== 1) throw new LeaseError("lease_failed", name, "fstat", "not_regular_file");
		const fdFlags = lib.getfd(handle.fd);
		if (fdFlags < 0 || (fdFlags & FD_CLOEXEC) === 0)
			throw new LeaseError("lease_failed", name, "fcntl", fdFlags < 0 ? lib.errno() : "cloexec_missing");
		return handle;
	} catch (error) {
		await handle.close();
		throw error;
	}
}

function tryLock(handle: FileHandle, name: LeaseName, lib: Libc): boolean {
	if (lib.flock(handle.fd, LOCK_EX | LOCK_NB) === 0) return true;
	const errno = lib.errno();
	if (errno === lib.platform.ewouldblock) return false;
	throw new LeaseError("lease_failed", name, "flock", errno);
}

function parseOwner(raw: string): LeaseOwner | undefined {
	if (!raw.trim() || raw.length > MAX_OWNER_BYTES) return undefined;
	try {
		const value = JSON.parse(raw) as Partial<LeaseOwner>;
		if (
			Number.isSafeInteger(value.pid) &&
			typeof value.token === "string" &&
			typeof value.startedAt === "string"
		)
			return { pid: value.pid as number, token: value.token, startedAt: value.startedAt };
	} catch {}
	return undefined;
}

/** Diagnostic read of the owner record. Takes no lock and decides nothing. */
export async function readLeaseOwner(home: string, name: LeaseName): Promise<LeaseOwner | undefined> {
	try {
		return parseOwner(await readFile(homeLayout(home).lease(name), "utf8"));
	} catch {
		return undefined;
	}
}

/**
 * Probe: true when some open file description holds the lease. Uses its own
 * descriptor and closes it; never touches a holder's descriptor.
 */
export async function leaseHeld(home: string, name: LeaseName): Promise<boolean> {
	const lib = loadLibc(name);
	let handle: FileHandle;
	try {
		handle = await openLeaseFile(homeLayout(home).lease(name), name, lib, false);
	} catch (error) {
		if (error instanceof LeaseError && error.syscall === "open" && error.errno === "ENOENT") return false;
		throw error;
	}
	try {
		return !tryLock(handle, name, lib);
	} finally {
		await handle.close();
	}
}

/** Non-blocking. Throws LeaseHeldError only on contention; every other failure is a LeaseError. */
export async function acquireLease(home: string, name: LeaseName): Promise<Lease> {
	const lib = loadLibc(name);
	const layout = homeLayout(home);
	try {
		await mkdir(layout.leasesDir, { recursive: true, mode: 0o700 });
	} catch (error) {
		throw new LeaseError("lease_failed", name, "mkdir", (error as NodeJS.ErrnoException).code ?? String(error));
	}
	const path = layout.lease(name);
	const handle = await openLeaseFile(path, name, lib, true);
	try {
		if (!tryLock(handle, name, lib)) throw new LeaseHeldError(name, await readLeaseOwner(home, name));
		const owner: LeaseOwner = { pid: process.pid, token: crypto.randomUUID(), startedAt: new Date().toISOString() };
		const bytes = Buffer.from(`${JSON.stringify(owner)}\n`);
		try {
			await handle.truncate(0);
			let offset = 0;
			while (offset < bytes.length) {
				const { bytesWritten } = await handle.write(bytes, offset, bytes.length - offset, offset);
				if (bytesWritten === 0) throw new Error("short write");
				offset += bytesWritten;
			}
			await handle.sync();
		} catch (error) {
			throw new LeaseError("lease_failed", name, "write", (error as NodeJS.ErrnoException).code ?? String(error));
		}
		heldHandles.add(handle);
		let released = false;
		const release = async () => {
			if (released) return;
			released = true;
			heldHandles.delete(handle);
			await handle.close();
		};
		return { name, token: owner.token, path, release, [Symbol.asyncDispose]: release };
	} catch (error) {
		await handle.close();
		throw error;
	}
}

/** For processes whose only correct response to contention is to exit and let the service manager retry. */
export function exitOnLeaseHeld(error: LeaseHeldError, log: (line: string) => void = (line) => console.error(line)): never {
	log(error.message);
	process.exit(1);
}
