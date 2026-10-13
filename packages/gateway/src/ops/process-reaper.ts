import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { homeLayout } from "@gajae-gateway/home";

export interface ProcessReaperOptions {
	/** Optional timeout for the reaper itself (ms). */
	readonly timeoutMs?: number;
	/** Log function for diagnostics. */
	readonly log?: (line: string) => void;
	/** Optional injected process table for testing. */
	readonly processTable?: ProcessTableEntry[];
	/** Optional kill function for testing. If not provided, uses process.kill. */
	readonly kill?: (pid: number, signal: string) => Promise<void>;
	/** Optional procRoot for testing (/proc filesystem root). */
	readonly procRoot?: string;
}

export interface ProcessTableEntry {
	readonly pid: number;
	readonly ppid: number;
	readonly argv: string[];
	readonly agentDir: string | undefined;
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
 * 1. Spawned with exactly argv = [gjcPath (or bun gjcPath), "sdk", "serve", "--stdio", "--session", <id>]
 * 2. With agentDir exactly matching our broker agent directory
 * 3. With ppid !== current process.pid (orphaned from this gateway instance)
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

		// Get current process's PID; orphaned workers will have ppid !== this
		const selfPid = process.pid;

		// Read process table (from injection or /proc or ps)
		const processTable = options.processTable ?? (await readProcessTable(options.procRoot));

		// Classify which processes are orphaned workers we should reap
		const foundPids = selectOrphanedWorkers(processTable, {
			gjcPath: gjcExecutable,
			brokerAgentDir,
			selfPid,
		});

		found = foundPids.length;
		if (foundPids.length === 0) return { found, killed, failed };

		log(`process-reaper: found ${foundPids.length} orphaned worker processes to reap`);

		// Kill each found process with SIGTERM then SIGKILL
		for (const pid of foundPids) {
			try {
				await killProcess(pid, options.kill);
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

/**
 * Pure classifier: identifies orphaned workers from a process table.
 *
 * A worker is selected for reaping if:
 * 1. Its argv matches [gjcPath or "bun" + gjcPath, "sdk", "serve", "--stdio", "--session", <id>]
 *    (argv is an exact token match, not substring or prefix)
 * 2. Its agentDir === brokerAgentDir (exact equality)
 * 3. Its ppid !== selfPid (not a child of the current gateway instance)
 */
export function selectOrphanedWorkers(
	table: ProcessTableEntry[],
	options: { readonly gjcPath: string; readonly brokerAgentDir: string; readonly selfPid: number },
): number[] {
	const result: number[] = [];

	for (const entry of table) {
		// Check argv token match: [gjcPath or bun+gjcPath, "sdk", "serve", "--stdio", "--session", id]
		if (!isWorkerArgv(entry.argv, options.gjcPath)) continue;

		// Check agentDir exact equality
		if (entry.agentDir !== options.brokerAgentDir) continue;

		// Check if orphaned: ppid !== selfPid
		if (entry.ppid === options.selfPid) continue;

		result.push(entry.pid);
	}

	return result;
}

/**
 * Checks if argv matches the exact pattern for a spawned worker.
 * Pattern: [gjcPath or "bun" + gjcPath, "sdk", "serve", "--stdio", "--session", <id>]
 */
function isWorkerArgv(argv: string[], gjcPath: string): boolean {
	if (argv.length < 6) return false;

	let idx = 0;

	// Check first token: either gjcPath directly or "bun" followed by gjcPath
	if (argv[0] === "bun") {
		if (argv[1] !== gjcPath) return false;
		idx = 2;
	} else {
		if (argv[0] !== gjcPath) return false;
		idx = 1;
	}

	// Remaining argv must be exactly: "sdk", "serve", "--stdio", "--session", <id>
	if (argv.length !== idx + 5) return false;
	if (argv[idx] !== "sdk") return false;
	if (argv[idx + 1] !== "serve") return false;
	if (argv[idx + 2] !== "--stdio") return false;
	if (argv[idx + 3] !== "--session") return false;
	// argv[idx + 4] is the session ID, which can be any string

	return true;
}

/**
 * Reads the process table from /proc (Linux) or ps (macOS/Unix).
 * On Linux, reads /proc/[pid]/stat, /proc/[pid]/cmdline, /proc/[pid]/environ.
 * On macOS, uses `ps eww` (environment may be incomplete; skip agentDir if not present).
 * Returns empty array if the system doesn't provide process information.
 */
async function readProcessTable(procRoot?: string): Promise<ProcessTableEntry[]> {
	if (process.platform === "linux") {
		return await readProcessTableLinux(procRoot);
	} else {
		return await readProcessTableUnix();
	}
}

/**
 * Linux-specific process discovery from /proc filesystem.
 * Reads /proc/[pid]/stat for ppid, /proc/[pid]/cmdline for argv, /proc/[pid]/environ for GJC_CODING_AGENT_DIR.
 */
async function readProcessTableLinux(procRoot: string = "/proc"): Promise<ProcessTableEntry[]> {
	const result: ProcessTableEntry[] = [];

	try {
		const entries = await readdir(procRoot);

		for (const entry of entries) {
			const pid = Number(entry);
			if (!Number.isSafeInteger(pid) || pid <= 0) continue;

			try {
				// Read /proc/[pid]/stat to extract ppid (field 3)
				const stat = await readFile(join(procRoot, entry, "stat"), "utf-8").catch(() => "");
				if (!stat) continue;

				const statParts = stat.split(" ");
				const ppid = Number(statParts[3]);
				if (!Number.isSafeInteger(ppid)) continue;

				// Read /proc/[pid]/cmdline (NUL-separated argv)
				const cmdlineStr = await readFile(join(procRoot, entry, "cmdline"), "utf-8").catch(() => "");
				const argv = cmdlineStr.split("\0").filter(Boolean);
				if (argv.length === 0) continue;

				// Read /proc/[pid]/environ to extract GJC_CODING_AGENT_DIR
				const environStr = await readFile(join(procRoot, entry, "environ"), "utf-8").catch(() => "");
				const environ = environStr.split("\0");
				let agentDir: string | undefined;
				for (const envVar of environ) {
					if (envVar.startsWith("GJC_CODING_AGENT_DIR=")) {
						agentDir = envVar.substring("GJC_CODING_AGENT_DIR=".length);
						break;
					}
				}

				result.push({ pid, ppid, argv, agentDir });
			} catch {
				// Ignore errors reading proc entries (permission denied, process died, etc)
			}
		}
	} catch {
		// /proc not available or other error; return empty table
	}

	return result;
}

/**
 * Unix-specific process discovery using `ps eww` command.
 * Returns ppid, pid, argv, and environment variables.
 * Note: On macOS/Unix, environment may be incomplete or unavailable; this is best-effort.
 * If GJC_CODING_AGENT_DIR is not present in ps output, agentDir will be undefined.
 */
async function readProcessTableUnix(): Promise<ProcessTableEntry[]> {
	const result: ProcessTableEntry[] = [];

	try {
		// ps eww output: all processes with all environment variables
		// Format: PPID PID COMMAND [with embedded environ]
		const proc = Bun.spawn(["ps", "eww"], {
			stdout: "pipe",
			stderr: "ignore",
		});

		const output = await new Response(proc.stdout as ReadableStream).text();
		const lines = output.trim().split("\n");

		// Skip header line
		for (let i = 1; i < lines.length; i++) {
			const line = lines[i];
			if (!line) continue;

			// ps eww format: leading columns are PPID PID, followed by COMMAND
			// COMMAND may contain spaces and environment variables (space-separated)
			// We need to parse: PPID PID command args...
			const parts = line.trim().split(/\s+/);
			if (parts.length < 3) continue;

			const ppid = Number(parts[0]);
			const pid = Number(parts[1]);
			if (!Number.isSafeInteger(ppid) || !Number.isSafeInteger(pid)) continue;

			// The rest is the command and environment, space-separated
			// For simplicity, treat the remaining parts as argv + environ
			// argv[0] is the command name, argv[1:] are the arguments
			const argv = [parts[2]];
			let agentDir: string | undefined;

			// Scan remaining parts for the session ID and environment
			for (let j = 3; j < parts.length; j++) {
				const part = parts[j];
				if (part.startsWith("GJC_CODING_AGENT_DIR=")) {
					agentDir = part.substring("GJC_CODING_AGENT_DIR=".length);
				} else if (!part.includes("=")) {
					// Argument (not an environment variable)
					argv.push(part);
				}
			}

			result.push({ pid, ppid, argv, agentDir });
		}

		await proc.exited;
	} catch {
		// ps not available or other error; return empty table
	}

	return result;
}

/**
 * Sends SIGTERM to a process, then SIGKILL if it doesn't die quickly.
 */
async function killProcess(
	pid: number,
	killFn?: (pid: number, signal: string) => Promise<void>,
	timeoutMs = 2_000,
): Promise<void> {
	for (const signal of ["SIGTERM", "SIGKILL"] as const) {
		try {
			if (killFn) {
				await killFn(pid, signal);
			} else {
				process.kill(pid, signal);
			}
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
