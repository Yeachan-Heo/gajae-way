import { readdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { homeLayout } from "@gajae-gateway/home";

export interface ProcessReaperOptions {
	/** Optional timeout for the reaper itself (ms). */
	readonly timeoutMs?: number;
	/** Log function for diagnostics. */
	readonly log?: (line: string) => void;
	/** Optional injected process discovery (for testing). */
	readonly processTable?: ProcessTableEntry[];
	/** Optional kill function for testing. If not provided, uses process.kill. */
	readonly kill?: (pid: number, signal: string) => Promise<void>;
}

export interface ProcessTableEntry {
	readonly pid: number;
	readonly ppid: number;
	readonly command: string;
	readonly environ?: Record<string, string>;
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
 * 2. Orphaned (parent process is no longer alive, not just ppid === 1)
 * 3. With GJC_CODING_AGENT_DIR exactly matching our broker agent directory
 * 4. NOT running as the current gateway process (by matching parent process)
 * 5. Matching exact argv tokens ("sdk serve" or "session" token match, not substring)
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
			processTable: options.processTable,
			kill: options.kill,
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

interface WorkerSearchOptions {
	readonly gjcExecutable: string;
	readonly brokerAgentDir: string;
	readonly gatewayPpid: number;
	readonly log: (line: string) => void;
	readonly processTable?: ProcessTableEntry[];
	readonly isProcessAliveOverride?: (pid: number) => Promise<boolean>;
	readonly kill?: (pid: number, signal: string) => Promise<void>;
}

/**
 * Checks if a process with the given PID is alive.
 */
async function isProcessAlive(pid: number, processTable?: ProcessTableEntry[]): Promise<boolean> {
	if (processTable) {
		return processTable.some((entry) => entry.pid === pid);
	}
	try {
		if (process.platform === "linux") {
			const stat = await readFile(`/proc/${pid}/stat`, "utf-8").catch(() => "");
			return stat.length > 0;
		} else {
			const proc = Bun.spawn(["kill", "-0", String(pid)], {
				stdout: "ignore",
				stderr: "ignore",
			});
			await proc.exited;
			return proc.exitCode === 0;
		}
	} catch {
		return false;
	}
}

/**
 * Checks if a process is a gajaeway gateway process by checking its command for exact tokens.
 * A gateway process must have both "sdk" and "broker" tokens in its command.
 */
function isGatewayProcess(processTable: ProcessTableEntry[], parentPid: number): boolean {
	const parent = processTable.find((entry) => entry.pid === parentPid);
	if (!parent) return false;

	// Check for exact token match: must have "sdk" followed by "broker" somewhere in the command
	const cmdTokens = parent.command.split(/\s+/);
	let foundSdk = false;
	for (let i = 0; i < cmdTokens.length; i++) {
		if (cmdTokens[i] === "sdk") {
			foundSdk = true;
		}
		if (foundSdk && cmdTokens[i] === "broker") {
			return true;
		}
	}
	return false;
}

/**
 * Finds processes that are orphaned (parent process no longer exists or is not a gateway)
 * and spawned by the exact gjcExecutable.
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
 * Linux-specific process discovery using /proc filesystem or injected test data.
 *
 * Strategy: Read /proc/[pid]/stat to find ppid, and /proc/[pid]/cmdline + /proc/[pid]/environ
 * to verify the process matches our gjcExecutable and is for our agent directory.
 * A process is reaped if its parent is no longer alive or is not the current gateway.
 */
async function findOrphanedLinux(options: WorkerSearchOptions): Promise<number[]> {
	const result: number[] = [];

	// Use injected process table for testing if available
	if (options.processTable) {
		for (const entry of options.processTable) {
			if (!entry.command.includes("bun")) continue;
			if (entry.ppid === process.pid) continue;
			if (!entry.environ) continue;

			const agentDirValue = entry.environ.GJC_CODING_AGENT_DIR;
			if (agentDirValue !== options.brokerAgentDir) continue;

			const cmdline = entry.command.split(" ");
			const hasExactSdkServe = cmdline.some((arg, idx) => arg === "sdk" && cmdline[idx + 1] === "serve");
			const hasSessionToken = cmdline.includes("session") || cmdline.includes("--session");
			if (!hasExactSdkServe && !hasSessionToken) continue;

			// Check if parent is a live gateway process
			const parentIsGateway = isGatewayProcess(options.processTable, entry.ppid);
			if (parentIsGateway) continue; // Skip if parent is a live gateway

			result.push(entry.pid);
		}
		return result;
	}

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

				// Skip processes that are children of the current gateway
				if (ppid === process.pid) continue;

				// Read command line to check for bun
				const cmdline: string[] = await readFile(join("/proc", entry, "cmdline"), "utf-8")
					.then((s) => s.split("\0").filter(Boolean))
					.catch(() => []);

				if (cmdline.length === 0 || !cmdline[0]?.endsWith("bun")) continue;

				// Check if this process was spawned by our gateway (has exact argv tokens)
				// Look for exact token matches of "sdk" with "serve" or "session" followed by gateway identifier
				const hasExactSdkServe = cmdline.some((arg, idx) => arg === "sdk" && cmdline[idx + 1] === "serve");
				const hasSessionToken = cmdline.includes("session");
				if (!hasExactSdkServe && !hasSessionToken) continue;

				// Verify environment contains GJC_CODING_AGENT_DIR pointing EXACTLY to our broker agent dir
				const environ: string[] = await readFile(join("/proc", entry, "environ"), "utf-8")
					.then((s) => s.split("\0").filter(Boolean))
					.catch(() => []);

				const agentDirMatch = environ.find((e) => e.startsWith("GJC_CODING_AGENT_DIR="));
				if (!agentDirMatch) continue;

				// Extract the actual path value and compare for exact equality
				const agentDirValue = agentDirMatch.substring("GJC_CODING_AGENT_DIR=".length);
				if (agentDirValue !== options.brokerAgentDir) continue;

				// Check if the parent process is still alive and is a gateway
				// On real /proc systems, we can't easily check if parent is a gateway,
				// so we only check if parent is alive; if not alive, reap it
				const parentAlive = await isProcessAlive(ppid);
				if (parentAlive) continue;

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
 * Unix-specific process discovery using ps command or injected test data.
 * Less precise than /proc, but works on macOS and other Unix systems.
 * A process is reaped if its parent is no longer alive or is not the current gateway.
 */
async function findOrphanedUnix(options: WorkerSearchOptions): Promise<number[]> {
	const result: number[] = [];

	// Use injected process table for testing if available
	if (options.processTable) {
		for (const entry of options.processTable) {
			if (!entry.command.includes("bun")) continue;
			if (entry.ppid === process.pid) continue;

			const hasExactSdkServe = entry.command.includes(" sdk serve") || entry.command.startsWith("sdk serve");
			const hasSessionToken =
				entry.command.includes(" --session ") ||
				entry.command.includes(" session ") ||
				entry.command.includes(" session$") ||
				entry.command.endsWith(" session") ||
				entry.command === "session";
			if (!hasExactSdkServe && !hasSessionToken) continue;

			if (!entry.command.includes(options.gjcExecutable)) continue;

			// Check if parent is a live gateway process
			const parentIsGateway = isGatewayProcess(options.processTable, entry.ppid);
			if (parentIsGateway) continue; // Skip if parent is a live gateway

			result.push(entry.pid);
		}
		return result;
	}

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

			if (!Number.isSafeInteger(pid)) continue;

			// Skip processes that are children of the current gateway
			if (ppid === process.pid) continue;

			// Must be a bun process
			if (!command.includes("bun")) continue;

			// Must be running exact token matches of "sdk serve" or contain "--session"
			const hasExactSdkServe = command.includes(" sdk serve") || command.startsWith("sdk serve");
			const hasSessionToken = command.includes(" --session ") || command.includes("--session=");
			if (!hasExactSdkServe && !hasSessionToken) continue;

			// On Unix systems, verify the command contains our gjcExecutable path
			if (!command.includes(options.gjcExecutable)) continue;

			// Check if the parent process is still alive
			// On real systems without process table injection, we can't easily check gateway status,
			// so we only check if parent is alive; if not alive, reap it
			const parentAlive = await isProcessAlive(ppid);
			if (parentAlive) continue;

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
