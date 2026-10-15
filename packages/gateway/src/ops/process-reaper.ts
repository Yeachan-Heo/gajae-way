import { readdir, readFile, realpath } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { homeLayout } from "@gajae-gateway/home";

export interface ProcessReaperOptions {
	/** Timeout for each graceful/forceful termination attempt (ms). */
	readonly timeoutMs?: number;
	/** Log function for diagnostics. */
	readonly log?: (line: string) => void;
	/** Injected process table; requires an injected kill function to prevent host signals in tests. */
	readonly processTable?: ProcessTableEntry[];
	/** Optional kill function for tests. If not provided, uses process.kill. */
	readonly kill?: (pid: number, signal: string) => Promise<void>;
	/** Optional liveness probe for tests. */
	readonly isProcessAlive?: (pid: number) => Promise<boolean>;
	/** Optional identity revalidation hook for tests. */
	readonly isCurrentWorker?: (candidate: ProcessTableEntry) => Promise<boolean>;
	/** Optional procRoot for testing (/proc filesystem root). */
	readonly procRoot?: string;
	/** Effective executable passed to the GJC client. */
	readonly gjcExecutable?: string;
	/** GJC installation prefix; prior versioned executables are accepted under this directory. */
	readonly gjcPrefix?: string;
	/** Effective configured GJC agent directory. */
	readonly brokerAgentDir?: string;
}

export interface ProcessTableEntry {
	readonly pid: number;
	readonly ppid: number;
	readonly argv: string[];
	readonly agentDir: string | undefined;
	/** Stable process-start identity: /proc start ticks on Linux, lstart from ps elsewhere. */
	readonly identity?: string;
	/** Linux process state, used to treat unreaped zombies as exited. */
	readonly state?: string;
}

export interface ReapResult {
	readonly found: number;
	readonly killed: number;
	readonly failed: number;
}

export interface WorkerMatchOptions {
	readonly gjcPath: string;
	readonly gjcPrefix?: string;
	readonly brokerAgentDir: string;
	readonly selfPid: number;
}

/**
 * Identifies and terminates leftover GJC session relays from a previous gateway epoch.
 *
 * A candidate must have an exact GJC relay argv, the canonical configured agent directory,
 * and no live gateway as its parent. Every candidate's process-start identity is checked
 * again immediately before each signal to avoid signaling a reused PID. A relay directly
 * parented by a live gateway is still owned; after its gateway exits, the OS reparents it to
 * systemd or another subreaper, where this scan can identify it as orphaned.
 */
export async function reapLeftoverWorkers(home: string, options: ProcessReaperOptions = {}): Promise<ReapResult> {
	if (options.processTable !== undefined && (options.kill === undefined || options.isProcessAlive === undefined)) {
		throw new Error("processTable injection requires kill and isProcessAlive functions");
	}

	const { log = (line) => console.warn(line) } = options;
	let found = 0;
	let killed = 0;
	let failed = 0;

	try {
		const layout = homeLayout(resolve(home));
		const gjcPath =
			options.gjcExecutable === undefined ? await canonicalizePath(layout.gjcExecutable()) : options.gjcExecutable;
		const gjcPrefix =
			options.gjcPrefix === undefined
				? options.gjcExecutable === undefined
					? await canonicalizePath(layout.gjcPrefix)
					: undefined
				: await canonicalizePath(options.gjcPrefix);
		const brokerAgentDir = await canonicalizePath(options.brokerAgentDir ?? layout.brokerAgentDir);
		const matchOptions: WorkerMatchOptions = { gjcPath, gjcPrefix, brokerAgentDir, selfPid: process.pid };
		const processTable = options.processTable ?? (await readProcessTable(options.procRoot, matchOptions));
		const foundPids = selectOrphanedWorkers(processTable, matchOptions);
		found = foundPids.length;
		if (foundPids.length === 0) return { found, killed, failed };

		log(`process-reaper: found ${foundPids.length} orphaned worker processes to reap`);

		for (const pid of foundPids) {
			const candidate = processTable.find((entry) => entry.pid === pid);
			if (!candidate) continue;
			try {
				const reaped = await killProcess(candidate, {
					kill: options.kill,
					isAlive: options.isProcessAlive ?? ((targetPid) => isProcessAlive(targetPid, options.procRoot)),
					isCurrentWorker:
						options.isCurrentWorker ??
						(async (original) => {
							const currentTable = options.processTable ?? (await readProcessTable(options.procRoot, matchOptions));
							const current = currentTable.find((entry) => entry.pid === original.pid);
							if (!sameProcessIdentity(original, current, options.processTable !== undefined)) return false;
							return selectOrphanedWorkers(currentTable, matchOptions).includes(original.pid);
						}),
					timeoutMs: options.timeoutMs ?? 2_000,
				});
				if (reaped) {
					killed++;
					log(`process-reaper: reaped leftover worker pid=${pid}`);
				}
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

/** Pure classifier, shared by Linux, Unix, and injected fixtures. */
export function selectOrphanedWorkers(table: ProcessTableEntry[], options: WorkerMatchOptions): number[] {
	const gatewayPids = new Set(table.filter((entry) => isGatewayProcess(entry.argv)).map((entry) => entry.pid));
	const result: number[] = [];

	for (const entry of table) {
		if (!isWorkerArgv(entry.argv, options)) continue;
		if (entry.agentDir !== options.brokerAgentDir) continue;
		if (entry.ppid === options.selfPid || gatewayPids.has(entry.ppid)) continue;
		result.push(entry.pid);
	}

	return result;
}

function isWorkerArgv(argv: string[], options: WorkerMatchOptions): boolean {
	let executableIndex = 0;
	if (basename(argv[0] ?? "") === "bun") executableIndex = 1;
	if (argv.length !== executableIndex + 6) return false;

	const executable = argv[executableIndex];
	if (!executable || !isGjcExecutable(executable, options.gjcPath, options.gjcPrefix)) return false;
	const args = argv.slice(executableIndex + 1);
	return (
		args[0] === "sdk" &&
		args[1] === "serve" &&
		args[2] === "--stdio" &&
		args[3] === "--session" &&
		args[4] !== undefined &&
		args.length === 5
	);
}

function isGjcExecutable(executable: string, gjcPath: string, gjcPrefix?: string): boolean {
	if (executable === gjcPath) return true;
	if (gjcPrefix === undefined) return false;

	const pathFromPrefix = relative(gjcPrefix, executable);
	if (!pathFromPrefix || isAbsolute(pathFromPrefix)) return false;
	const [version, ...remainder] = pathFromPrefix.split(sep);
	if (version === undefined || !/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/.test(version)) {
		return false;
	}

	// The versioned bin shim resolves to the package entrypoint. Accept either argv form,
	// including an older version that an update has already pruned from disk.
	return (
		(remainder.length === 2 && remainder[0] === "bin" && remainder[1] === "gjc") ||
		(remainder.length === 5 &&
			remainder[0] === "node_modules" &&
			remainder[1] === "@gajae-code" &&
			remainder[2] === "coding-agent" &&
			remainder[3] === "bin" &&
			remainder[4] === "gjc.js")
	);
}

function isGatewayProcess(argv: string[]): boolean {
	if (basename(argv[0] ?? "") === "gajaeway-gateway" && argv[1] === "daemon") return true;
	if (basename(argv[0] ?? "") !== "bun" || argv[2] !== "daemon") return false;
	const script = (argv[1] ?? "").replaceAll("\\", "/");
	return script === "packages/gateway/src/main.ts" || script.endsWith("/packages/gateway/src/main.ts");
}

function sameProcessIdentity(
	original: ProcessTableEntry,
	current: ProcessTableEntry | undefined,
	injectedTable: boolean,
): boolean {
	if (!current) return false;
	if (original.identity !== undefined && current.identity !== undefined) return original.identity === current.identity;
	return injectedTable && original === current;
}

async function canonicalizePath(path: string): Promise<string> {
	const absolute = resolve(path);
	try {
		return await realpath(absolute);
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code !== "ENOENT" && code !== "ENOTDIR") throw error;
		const parent = dirname(absolute);
		if (parent === absolute) return absolute;
		return join(await canonicalizePath(parent), basename(absolute));
	}
}

async function readProcessTable(procRoot?: string, matchOptions?: WorkerMatchOptions): Promise<ProcessTableEntry[]> {
	if (procRoot !== undefined || process.platform === "linux") return await readProcessTableLinux(procRoot);
	return await readProcessTableUnix(matchOptions);
}

async function readProcessTableLinux(procRoot = "/proc"): Promise<ProcessTableEntry[]> {
	let pids: string[];
	try {
		pids = await readdir(procRoot);
	} catch {
		return [];
	}

	const result: ProcessTableEntry[] = [];
	for (const name of pids) {
		const pid = Number(name);
		if (!Number.isSafeInteger(pid) || pid <= 0) continue;
		const entry = await readProcessEntryLinux(pid, procRoot);
		if (entry) result.push(entry);
	}
	return result;
}

async function readProcessEntryLinux(pid: number, procRoot: string): Promise<ProcessTableEntry | undefined> {
	const directory = join(procRoot, String(pid));
	try {
		const statText = await readFile(join(directory, "stat"), "utf8");
		const stat = parseLinuxStat(statText);
		if (!stat) return undefined;

		const cmdline = await readFile(join(directory, "cmdline"), "utf8").catch(() => "");
		const argv = cmdline.split("\0").filter(Boolean);
		if (argv.length === 0) return undefined;

		const environment = await readFile(join(directory, "environ"), "utf8").catch(() => "");
		const agentDir = environment
			.split("\0")
			.find((variable) => variable.startsWith("GJC_CODING_AGENT_DIR="))
			?.slice("GJC_CODING_AGENT_DIR=".length);
		return { pid, ppid: stat.ppid, argv, agentDir, identity: stat.startTime, state: stat.state };
	} catch {
		return undefined;
	}
}

interface LinuxStat {
	readonly state: string;
	readonly ppid: number;
	readonly startTime: string;
}

function parseLinuxStat(stat: string): LinuxStat | undefined {
	const closingParen = stat.lastIndexOf(")");
	if (closingParen < 0) return undefined;
	const fields = stat
		.slice(closingParen + 1)
		.trim()
		.split(/\s+/);
	const ppid = Number(fields[1]);
	const startTime = fields[19];
	if (!fields[0] || !Number.isSafeInteger(ppid) || startTime === undefined) return undefined;
	return { state: fields[0], ppid, startTime };
}

/** Parses BSD-style ps rows with explicit PPID, PID, lstart, and command columns. */
export function parseUnixProcessTable(
	output: string,
	expectedAgentDir?: string,
	workerPathOptions?: Pick<WorkerMatchOptions, "gjcPath" | "gjcPrefix">,
): ProcessTableEntry[] {
	const result: ProcessTableEntry[] = [];
	for (const line of output.split(/\r?\n/)) {
		const match = line.trim().match(/^(\d+)\s+(\d+)\s+(\S+\s+\S+\s+\S+\s+\S+\s+\S+)\s+(.*)$/);
		if (!match) continue;

		const ppid = Number(match[1]);
		const pid = Number(match[2]);
		if (!Number.isSafeInteger(ppid) || !Number.isSafeInteger(pid)) continue;

		const commandAndEnvironment = match[4];
		const envMatch = findUnixAgentDir(commandAndEnvironment, expectedAgentDir);
		const environmentStart = envMatch ? findUnixEnvironmentStart(commandAndEnvironment, envMatch.index) : undefined;
		const command =
			environmentStart === undefined
				? commandAndEnvironment
				: commandAndEnvironment.slice(0, environmentStart).trimEnd();
		const argv = parseUnixCommandArgv(command, workerPathOptions);
		if (argv.length === 0) continue;
		result.push({
			pid,
			ppid,
			argv,
			agentDir: envMatch?.value,
			identity: match[3].trim(),
		});
	}
	return result;
}

function parseUnixCommandArgv(
	command: string,
	workerPathOptions?: Pick<WorkerMatchOptions, "gjcPath" | "gjcPrefix">,
): string[] {
	const marker = " sdk serve --stdio --session ";
	const markerIndex = command.lastIndexOf(marker);
	if (markerIndex < 0) return parseUnixGatewayArgv(command) ?? command.split(/\s+/).filter(Boolean);

	const executable = command.slice(0, markerIndex);
	const bunArgv = parseUnixBunLauncher(executable, workerPathOptions);
	const launcher = bunArgv ?? [executable];
	const args = command
		.slice(markerIndex + 1)
		.split(/\s+/)
		.filter(Boolean);
	return [...launcher, ...args];
}

function parseUnixBunLauncher(
	command: string,
	workerPathOptions?: Pick<WorkerMatchOptions, "gjcPath" | "gjcPrefix">,
): string[] | undefined {
	for (let separator = command.indexOf(" "); separator >= 0; separator = command.indexOf(" ", separator + 1)) {
		const launcher = command.slice(0, separator);
		if (basename(launcher) !== "bun") continue;

		const executable = command.slice(separator + 1);
		const matchesGjc = workerPathOptions
			? isGjcExecutable(executable, workerPathOptions.gjcPath, workerPathOptions.gjcPrefix)
			: basename(executable) === "gjc" || basename(executable) === "gjc.js";
		if (matchesGjc) return [launcher, executable];
	}
	return undefined;
}

function parseUnixGatewayArgv(command: string): string[] | undefined {
	const directExecutable = "gajaeway-gateway";
	for (
		let index = command.indexOf(directExecutable);
		index >= 0;
		index = command.indexOf(directExecutable, index + directExecutable.length)
	) {
		const executableEnd = index + directExecutable.length;
		const executable = command.slice(0, executableEnd);
		if (basename(executable) === directExecutable && /^\s+daemon(?:\s|$)/.test(command.slice(executableEnd))) {
			return [executable, "daemon"];
		}
	}

	const sourceScript = "packages/gateway/src/main.ts";
	for (
		let index = command.indexOf(sourceScript);
		index >= 0;
		index = command.indexOf(sourceScript, index + sourceScript.length)
	) {
		const scriptEnd = index + sourceScript.length;
		if (!/^\s+daemon(?:\s|$)/.test(command.slice(scriptEnd))) continue;

		for (
			let separator = command.indexOf(" ");
			separator >= 0 && separator < index;
			separator = command.indexOf(" ", separator + 1)
		) {
			const executable = command.slice(0, separator);
			if (basename(executable) !== "bun") continue;

			const script = command.slice(separator + 1, scriptEnd);
			if (script === sourceScript || script.endsWith(`/${sourceScript}`)) return [executable, script, "daemon"];
		}
	}
	return undefined;
}

function findUnixAgentDir(
	commandAndEnvironment: string,
	expectedAgentDir?: string,
): { readonly index: number; readonly value: string | undefined } | undefined {
	if (expectedAgentDir === undefined) {
		const match = /(?:^|\s)GJC_CODING_AGENT_DIR=([^\s]*)/.exec(commandAndEnvironment);
		return match ? { index: match.index, value: match[1] || undefined } : undefined;
	}

	const key = "GJC_CODING_AGENT_DIR=";
	for (
		let index = commandAndEnvironment.indexOf(key);
		index >= 0;
		index = commandAndEnvironment.indexOf(key, index + key.length)
	) {
		const before = index === 0 || /\s/.test(commandAndEnvironment[index - 1] ?? "");
		if (!before) continue;
		const value = commandAndEnvironment.slice(index + key.length);
		// ps flattens environment values into the command field, so no whitespace or marker
		// suffix can prove where a path containing spaces ends. broker.ts orders the agent dir
		// last; accept only an exact end-of-field match and skip ambiguous older layouts.
		return value === expectedAgentDir ? { index, value: expectedAgentDir } : undefined;
	}
	return undefined;
}

function findUnixEnvironmentStart(commandAndEnvironment: string, agentDirIndex: number): number {
	const marker = " sdk serve --stdio --session ";
	const markerIndex = commandAndEnvironment.lastIndexOf(marker);
	if (markerIndex < 0) return agentDirIndex;

	const sessionStart = markerIndex + marker.length;
	const sessionEnd = commandAndEnvironment.indexOf(" ", sessionStart);
	if (sessionEnd < 0 || sessionEnd >= agentDirIndex) return agentDirIndex;

	// BSD ps appends the whole environment after argv, not only the configured GJC variable.
	const precedingEnvironment = commandAndEnvironment.slice(sessionEnd, agentDirIndex);
	const firstVariable = /\s[A-Za-z_][A-Za-z0-9_]*=/.exec(precedingEnvironment);
	return firstVariable ? sessionEnd + firstVariable.index : agentDirIndex;
}

/**
 * The BSD `ps` scan for hosts without `/proc`. `-E` appends each process's environment
 * after its argv, which the parser needs to find the configured agent directory. The
 * procps-style `eww` word is not an option on macOS (`ps: illegal argument: eww`), so a
 * scan spelled that way exits 1 and the boot-time reaper never ran there.
 */
export const UNIX_PROCESS_TABLE_COMMAND = ["ps", "-A", "-ww", "-E", "-o", "ppid=,pid=,lstart=,command="] as const;

async function readProcessTableUnix(matchOptions?: WorkerMatchOptions): Promise<ProcessTableEntry[]> {
	const proc = Bun.spawn([...UNIX_PROCESS_TABLE_COMMAND], {
		env: { ...process.env, LC_ALL: "C" },
		stdout: "pipe",
		stderr: "ignore",
	});
	const output = await new Response(proc.stdout as ReadableStream).text();
	const exitCode = await proc.exited;
	if (exitCode !== 0) throw new Error(`ps process-table scan exited with status ${exitCode}`);
	return parseUnixProcessTable(output, matchOptions?.brokerAgentDir, matchOptions);
}

async function isProcessAlive(pid: number, procRoot?: string): Promise<boolean> {
	if (process.platform === "linux" || procRoot !== undefined) {
		const statRoot = procRoot ?? "/proc";
		let statText: string;
		try {
			statText = await readFile(join(statRoot, String(pid), "stat"), "utf8");
		} catch (error) {
			const code = (error as NodeJS.ErrnoException).code;
			if (code === "ENOENT" || code === "ENOTDIR") return false;
			throw error;
		}
		const stat = parseLinuxStat(statText);
		if (!stat) throw new Error(`invalid /proc stat for pid=${pid}`);
		return stat.state !== "Z" && stat.state !== "X";
	}

	return await isProcessAliveUnix(pid);
}

async function isProcessAliveUnix(pid: number): Promise<boolean> {
	const proc = Bun.spawn(["ps", "-p", String(pid), "-o", "pid=,stat="], {
		stdout: "pipe",
		stderr: "ignore",
	});
	const output = (await new Response(proc.stdout as ReadableStream).text()).trim();
	const exitCode = await proc.exited;
	if (output === "" && (exitCode === 0 || exitCode === 1)) return false;
	if (exitCode !== 0) throw new Error(`ps liveness check exited with status ${exitCode} for pid=${pid}`);

	const match = output.match(/^(\d+)\s+(\S+)/);
	if (!match || Number(match[1]) !== pid) throw new Error(`ps returned an invalid liveness row for pid=${pid}`);
	return match[2]?.[0] !== "Z" && match[2]?.[0] !== "X";
}

interface KillProcessOptions {
	readonly kill?: (pid: number, signal: string) => Promise<void>;
	readonly isAlive: (pid: number) => Promise<boolean>;
	readonly isCurrentWorker: (candidate: ProcessTableEntry) => Promise<boolean>;
	readonly timeoutMs: number;
}

async function killProcess(candidate: ProcessTableEntry, options: KillProcessOptions): Promise<boolean> {
	for (const signal of ["SIGTERM", "SIGKILL"] as const) {
		if (!(await options.isCurrentWorker(candidate))) {
			if (!(await options.isAlive(candidate.pid))) return false;
			throw new Error(`worker identity changed before ${signal} pid=${candidate.pid}`);
		}

		try {
			if (options.kill) await options.kill(candidate.pid, signal);
			else process.kill(candidate.pid, signal);
		} catch {
			// Check liveness after ESRCH, EPERM, or another signaling error before deciding what follows.
		}

		if (await waitForExit(candidate.pid, options.isAlive, options.timeoutMs)) return true;
	}

	throw new Error(`process ${candidate.pid} remained alive after SIGTERM and SIGKILL`);
}

async function waitForExit(
	pid: number,
	isAlive: (pid: number) => Promise<boolean>,
	timeoutMs: number,
): Promise<boolean> {
	const deadline = Date.now() + Math.max(0, timeoutMs);
	while (true) {
		if (!(await isAlive(pid))) return true;
		const remaining = deadline - Date.now();
		if (remaining <= 0) return false;
		await new Promise<void>((resolveDelay) => setTimeout(resolveDelay, Math.min(25, remaining)));
	}
}
