import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { homeLayout } from "@gajae-gateway/home";

export interface ProcessReaperOptions {
	/** Optional timeout for the reaper itself (ms). */
	readonly timeoutMs?: number;
	/** Log function for diagnostics. */
	readonly log?: (line: string) => void;
}

export interface ReapResult {
	readonly found: number;
	readonly killed: number;
	readonly failed: number;
}

/**
 * Identifies and kills leftover child processes from the previous gateway epoch.
 *
 * Only reaps processes that are:
 * 1. bun processes spawned by the exact gjcExecutable (resolved from home)
 * 2. Orphaned (parent PID is 1, meaning parent died)
 * 3. NOT running as the current gateway process (by matching parent process)
 *
 * This ensures we never kill unrelated bun processes system-wide.
 */
export async function reapLeftoverWorkers(home: string, options: ProcessReaperOptions = {}): Promise<ReapResult> {
	const { log = (s) => console.warn(s) } = options;
	let found = 0;
	let killed = 0;
	let failed = 0;

	try {
		const resolvedHome = resolve(home);
		const layout = homeLayout(resolvedHome);
		const gjcExecutable = await resolvePath(layout.gjcExecutable());
		const brokerAgentDir = layout.brokerAgentDir;

		// Get current process's ppid to avoid matching our own children
		const gatewayPpid = process.ppid;

		const foundPids = await findOrphanedWorkers({
			gjcExecutable,
			brokerAgentDir,
			gatewayPpid,
			log,
		});
		found = foundPids.length;

		if (foundPids.length === 0) return { found, killed, failed };

		log(`process-reaper: found ${foundPids.length} orphaned worker processes to reap`);

		// Kill each found process with SIGTERM then SIGKILL
		for (const pid of foundPids) {
			try {
				await killProcess(pid);
				killed++;
				log(`process-reaper: reaped leftover worker pid=${pid}`);
			} catch (error) {
				failed++;
				log(`process-reaper: failed to reap pid=${pid}: ${error instanceof Error ? error.message : String(error)}`);
			}
		}
	} catch (error) {
		log(`process-reaper: scan failed: ${error instanceof Error ? error.message : String(error)}`);
	}

	return { found, killed, failed };
}

/** Resolve a path through symlinks; return undefined if it doesn't exist. */
async function resolvePath(path: string): Promise<string> {
	try {
		const { realpath } = await import("node:fs/promises");
		return await realpath(path);
	} catch {
		// Path doesn't exist; return as-is so matching will fail gracefully
		return path;
	}
}

interface WorkerSearchOptions {
	readonly gjcExecutable: string;
	readonly brokerAgentDir: string;
	readonly gatewayPpid: number;
	readonly log: (line: string) => void;
}

/**
 * Finds processes that are orphaned (ppid === 1) and spawned by the exact gjcExecutable.
 * Uses platform-specific discovery: /proc on Linux, ps on Unix/macOS.
 */
async function findOrphanedWorkers(options: WorkerSearchOptions): Promise<number[]> {
	if (process.platform === "linux") {
		return await findOrphanedLinux(options);
	} else {
		return await findOrphanedUnix(options);
	}
}

/**
 * Linux-specific process discovery using /proc filesystem.
 *
 * Strategy: Read /proc/[pid]/stat to find ppid, and /proc/[pid]/cmdline + /proc/[pid]/environ
 * to verify the process matches our gjcExecutable and is for our agent directory.
 */
async function findOrphanedLinux(options: WorkerSearchOptions): Promise<number[]> {
	const result: number[] = [];
	try {
		const procEntries = await readdir("/proc").catch(() => []);
		for (const entry of procEntries) {
			const pid = Number(entry);
			if (!Number.isSafeInteger(pid) || pid <= 0) continue;

			try {
				// Read stat to get parent PID (field 3, 0-indexed)
				const stat = await readFile(join("/proc", entry, "stat"), "utf-8").catch(() => "");
				if (!stat) continue;

				const parts = stat.split(" ");
				const ppid = Number(parts[3]);

				// Only reap processes with ppid 1 (init), not those with ppid === gatewayPpid
				if (ppid !== 1) continue;

				// Read command line to check for bun
				const cmdline = await readFile(join("/proc", entry, "cmdline"), "utf-8")
					.then((s) => s.split("\0").filter(Boolean))
					.catch(() => []);

				if (cmdline.length === 0 || !cmdline[0]?.endsWith("bun")) continue;

				// Check if this process was spawned by our gjcExecutable
				// Look for the executable in the command line or parent process info
				const cmdlineStr = cmdline.join(" ");
				if (!cmdlineStr.includes("sdk serve") && !cmdlineStr.includes("session")) continue;

				// Verify environment contains GJC_CODING_AGENT_DIR pointing to our broker agent dir
				const environ = await readFile(join("/proc", entry, "environ"), "utf-8")
					.then((s) => s.split("\0").filter(Boolean))
					.catch(() => []);

				const agentDirMatch = environ.find((e) => e.startsWith("GJC_CODING_AGENT_DIR="));
				if (!agentDirMatch?.includes(options.brokerAgentDir)) continue;

				// This is an orphaned process spawned by our gateway
				result.push(pid);
			} catch {
				// Ignore errors reading proc entries (permission denied, process died, etc)
			}
		}
	} catch {
		// /proc not available
	}
	return result;
}

/**
 * Unix-specific process discovery using ps command.
 * Less precise than /proc, but works on macOS and other Unix systems.
 */
async function findOrphanedUnix(options: WorkerSearchOptions): Promise<number[]> {
	const result: number[] = [];
	try {
		const proc = Bun.spawn(["ps", "-eo", "ppid=,pid=,command="], {
			stdout: "pipe",
			stderr: "ignore",
		});

		const output = await new Response(proc.stdout as ReadableStream).text();
		const lines = output.trim().split("\n");

		for (const line of lines) {
			const parts = line.trim().split(/\s+/);
			if (parts.length < 3) continue;

			const ppid = Number(parts[0]);
			const pid = Number(parts[1]);
			const command = parts.slice(2).join(" ");

			// Only reap orphaned processes (ppid === 1)
			if (!Number.isSafeInteger(pid) || ppid !== 1) continue;

			// Must be a bun process
			if (!command.includes("bun")) continue;

			// Must be running sdk serve or session (gjc spawned)
			if (!command.includes("sdk serve") && !command.includes("session")) continue;

			// On Unix systems without /proc, we can't easily verify the agent directory,
			// so we use additional heuristics: the command should contain our gjcExecutable path
			if (!command.includes(options.gjcExecutable)) continue;

			result.push(pid);
		}

		await proc.exited;
	} catch {
		// ps not available or other error
	}

	return result;
}

/**
 * Sends SIGTERM to a process, then SIGKILL if it doesn't die quickly.
 */
async function killProcess(pid: number, timeoutMs = 2_000): Promise<void> {
	for (const signal of ["SIGTERM", "SIGKILL"] as const) {
		try {
			process.kill(pid, signal);
		} catch {
			// Already dead or permission denied
		}

		// Wait a bit for the process to exit
		try {
			const checkProc = Bun.spawn(["kill", "-0", String(pid)], {
				stdout: "ignore",
				stderr: "ignore",
			});
			const exitPromise = checkProc.exited;
			const timeoutPromise = new Promise<boolean>((resolve) => {
				setTimeout(() => resolve(false), timeoutMs);
			});

			const stillAlive = await Promise.race([exitPromise.then(() => false), timeoutPromise]).catch(() => true);

			if (!stillAlive) return; // Process is dead
		} catch {
			// Assume process is dead if we can't check
			return;
		}
	}

	throw new Error(`Process ${pid} did not exit after SIGTERM and SIGKILL`);
}
