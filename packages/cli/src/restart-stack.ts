import { randomUUID } from "node:crypto";
import { access, mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import {
	currentPlatform,
	launchAgentPlistPath,
	restartOrder,
	restartStackCommands,
	type ServicePlatform,
	serviceSpecs,
	systemdUnitDir,
	systemdUnitName,
	systemdUnitPath,
} from "./services";

/**
 * `gajaeway ops restart-stack` (issue #54).
 *
 * A persona turn that restarts the gateway is killed with it, so running the
 * ordered restart inline stopped between the gateway and its adapters and left
 * the adapters on the old binary while the caller reported success. The
 * sequence therefore runs in a detached supervisor (its own session, so the
 * caller's process group and the service manager's job kill do not reach it),
 * verifies every service against its deployed binary, and records a receipt
 * under `$GAJAEWAY_HOME` that the next turn reads with `--status`.
 */

export type RestartStepResult = "pending" | "ok" | "stale" | "failed" | "skipped";
export type RestartState = "queued" | "running" | "ok" | "stale" | "failed";

export interface RestartStep {
	readonly label: string;
	/** Absent on systemd, where the gateway unit's restart carries its dependents. */
	command?: readonly string[];
	exitStatus?: number;
	result: RestartStepResult;
	pid?: number;
	processStartedAt?: string;
	binary?: string;
	binaryModifiedAt?: string;
	detail?: string;
}

export interface RestartReceipt {
	readonly id: string;
	state: RestartState;
	readonly platform: ServicePlatform;
	readonly requestedAt: string;
	supervisorPid?: number;
	finishedAt?: string;
	readonly steps: RestartStep[];
}

/** A running service process as the service manager and `ps` report it. */
export interface ServiceProcess {
	readonly pid: number;
	/** Epoch ms; `ps` reports whole seconds. */
	readonly startedAt: number;
	readonly binary: string;
}

export type CommandRunner = (command: readonly string[]) => number | PromiseLike<number>;
export type ServiceProbe = (label: string) => Promise<ServiceProcess | undefined>;
export type IsServiceInstalled = (label: string) => Promise<boolean>;

export interface RunRestartOptions {
	readonly home: string;
	readonly id: string;
	readonly platform?: ServicePlatform;
	readonly uid?: number;
	readonly runner?: CommandRunner;
	readonly probe?: ServiceProbe;
	/** Checks if a service is installed; if not provided, defaults to checking the service manager's files. */
	readonly isServiceInstalled?: IsServiceInstalled;
	/** Epoch ms modification time of a deployed binary. */
	readonly binaryModifiedAt?: (path: string) => Promise<number>;
	readonly now?: () => number;
	readonly sleep?: (ms: number) => Promise<void>;
	/** How long one service may take to come back verified; covers the gateway's ordered shutdown. */
	readonly verifyTimeoutMs?: number;
	readonly pollMs?: number;
	/** Environment variables; used to determine systemd unit dir on linux. Defaults to process.env. */
	readonly env?: NodeJS.ProcessEnv;
	/** LaunchAgents directory on darwin. Defaults to ~/Library/LaunchAgents. */
	readonly launchAgentsDir?: string;
	/** systemd unit directory on linux. Defaults to $XDG_CONFIG_HOME/systemd/user. */
	readonly unitDir?: string;
}

const RECEIPT_FILE = "restart-stack.json";
const VERIFY_TIMEOUT_MS = 90_000;
const POLL_MS = 1_000;

export function restartReceiptPath(home: string): string {
	return join(home, RECEIPT_FILE);
}

export async function readRestartReceipt(home: string): Promise<RestartReceipt | undefined> {
	let raw: string;
	try {
		raw = await readFile(restartReceiptPath(home), "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
	return JSON.parse(raw) as RestartReceipt;
}

async function writeReceipt(home: string, receipt: RestartReceipt): Promise<void> {
	await mkdir(home, { recursive: true });
	const path = restartReceiptPath(home);
	const temporary = `${path}.${process.pid}.tmp`;
	await writeFile(temporary, `${JSON.stringify(receipt, null, 2)}\n`, { mode: 0o600 });
	await rename(temporary, path);
}

function newReceipt(id: string, platform: ServicePlatform, uid: number | undefined, now: number): RestartReceipt {
	const commands = restartStackCommands(platform, uid);
	return {
		id,
		state: "queued",
		platform,
		requestedAt: new Date(now).toISOString(),
		steps: restartOrder().map((spec, index) => ({
			label: spec.label,
			...(platform === "darwin" ? { command: commands[index] } : index === 0 ? { command: commands[0] } : {}),
			result: "pending",
		})),
	};
}

async function defaultCommandRunner(command: readonly string[]): Promise<number> {
	const child = Bun.spawn([...command], { stdin: "ignore", stdout: "ignore", stderr: "ignore" });
	return await child.exited;
}

async function capture(command: readonly string[]): Promise<string | undefined> {
	const child = Bun.spawn([...command], {
		stdin: "ignore",
		stdout: "pipe",
		stderr: "ignore",
		env: { ...process.env, LC_ALL: "C" },
	});
	const output = await new Response(child.stdout).text();
	return (await child.exited) === 0 ? output : undefined;
}

/** Reads the live pid from the service manager, then its start time and executable from `ps`. */
export function defaultServiceProbe(platform: ServicePlatform): ServiceProbe {
	return async (label) => {
		const spec = serviceSpecs().find((candidate) => candidate.label === label);
		if (spec === undefined) throw new Error(`unknown service label: ${label}`);
		let pid: number | undefined;
		if (platform === "darwin") {
			const listed = await capture(["launchctl", "list", label]);
			const match = listed?.match(/"PID"\s*=\s*(\d+);/);
			pid = match ? Number(match[1]) : undefined;
		} else {
			const shown = await capture(["systemctl", "--user", "show", "-p", "MainPID", "--value", systemdUnitName(spec)]);
			pid = shown === undefined ? undefined : Number(shown.trim());
		}
		if (pid === undefined || !Number.isInteger(pid) || pid <= 0) return undefined;
		const lstart = await capture(["ps", "-o", "lstart=", "-p", String(pid)]);
		const args = await capture(["ps", "-ww", "-o", "args=", "-p", String(pid)]);
		if (lstart === undefined || args === undefined) return undefined;
		const startedAt = Date.parse(lstart.trim());
		if (!Number.isFinite(startedAt)) return undefined;
		// The service definitions exec `<bin-dir>/<binary> <fixed args>`; stripping
		// the known arguments keeps a bin dir containing spaces intact.
		const suffix = spec.args.length > 0 ? ` ${spec.args.join(" ")}` : "";
		const line = args.trim();
		const binary = suffix && line.endsWith(suffix) ? line.slice(0, -suffix.length) : line;
		return { pid, startedAt, binary };
	};
}

async function defaultBinaryModifiedAt(path: string): Promise<number> {
	return (await stat(path)).mtimeMs;
}

/** Checks if a service is installed by verifying the service manager's definition file exists. */
function defaultIsServiceInstalled(options: {
	readonly platform: ServicePlatform;
	readonly env: NodeJS.ProcessEnv;
	readonly launchAgentsDir?: string;
	readonly unitDir?: string;
}): IsServiceInstalled {
	return async (label: string) => {
		const spec = serviceSpecs().find((s) => s.label === label);
		if (spec === undefined) return false;

		if (options.platform === "darwin") {
			const userHome = options.env.HOME || homedir();
			const agentsDir = options.launchAgentsDir || join(userHome, "Library", "LaunchAgents");
			const plistPath = launchAgentPlistPath(spec, agentsDir);
			try {
				await access(plistPath);
				return true;
			} catch {
				return false;
			}
		}

		// linux
		const userHome = options.env.HOME || homedir();
		const unitDirPath = options.unitDir || systemdUnitDir(options.env, userHome);
		const unitPath = systemdUnitPath(spec, unitDirPath);
		try {
			await access(unitPath);
			return true;
		} catch {
			return false;
		}
	};
}

function floorSecond(ms: number): number {
	return Math.floor(ms / 1_000) * 1_000;
}

/**
 * Runs the ordered restart, verifying each service before the next command,
 * and persists the receipt after every transition. The first service that is
 * stale or missing aborts the remainder.
 */
export async function runRestartStack(options: RunRestartOptions): Promise<RestartReceipt> {
	const platform = options.platform ?? currentPlatform();
	const runner = options.runner ?? defaultCommandRunner;
	const probe = options.probe ?? defaultServiceProbe(platform);
	const modifiedAt = options.binaryModifiedAt ?? defaultBinaryModifiedAt;
	const isInstalled =
		options.isServiceInstalled ??
		defaultIsServiceInstalled({
			platform,
			env: options.env ?? process.env,
			launchAgentsDir: options.launchAgentsDir,
			unitDir: options.unitDir,
		});
	const now = options.now ?? Date.now;
	const sleep = options.sleep ?? ((ms: number) => Bun.sleep(ms));
	const timeoutMs = options.verifyTimeoutMs ?? VERIFY_TIMEOUT_MS;
	const pollMs = options.pollMs ?? POLL_MS;

	const existing = await readRestartReceipt(options.home);
	const receipt =
		existing?.id === options.id && existing.state === "queued"
			? existing
			: newReceipt(options.id, platform, options.uid, now());
	receipt.state = "running";
	receipt.supervisorPid = process.pid;
	await writeReceipt(options.home, receipt);

	let issuedAt = now();
	for (const step of receipt.steps) {
		if (receipt.state !== "running") {
			step.result = "skipped";
			continue;
		}
		try {
			// Check if the service is installed before trying to restart it
			const installed = await isInstalled(step.label);
			if (!installed) {
				step.result = "skipped";
				step.detail = "service is not installed";
				await writeReceipt(options.home, receipt);
				continue;
			}

			if (step.command !== undefined) {
				issuedAt = now();
				const status = await runner(step.command);
				step.exitStatus = status;
				if (status !== 0) {
					step.result = "failed";
					step.detail = `${step.command.join(" ")} exited with status ${status}`;
				}
			}
			if (step.result === "pending") await verifyStep(step, issuedAt);
		} catch (error) {
			step.result = "failed";
			step.detail = error instanceof Error ? error.message : String(error);
		}
		if (step.result !== "ok" && step.result !== "skipped") {
			receipt.state = step.result === "stale" ? "stale" : "failed";
		}
		await writeReceipt(options.home, receipt);
	}
	if (receipt.state === "running") receipt.state = "ok";
	receipt.finishedAt = new Date(now()).toISOString();
	await writeReceipt(options.home, receipt);
	return receipt;

	async function verifyStep(step: RestartStep, since: number): Promise<void> {
		const deadline = now() + timeoutMs;
		for (;;) {
			const running = await probe(step.label);
			let binaryModifiedAt = 0;
			if (running !== undefined) {
				binaryModifiedAt = await modifiedAt(running.binary);
				step.pid = running.pid;
				step.processStartedAt = new Date(running.startedAt).toISOString();
				step.binary = running.binary;
				step.binaryModifiedAt = new Date(binaryModifiedAt).toISOString();
				// `ps` has one-second resolution, so the bound is floored to match.
				if (running.startedAt >= floorSecond(Math.max(binaryModifiedAt, since))) {
					step.result = "ok";
					delete step.detail;
					return;
				}
			}
			if (now() >= deadline) {
				if (running === undefined) {
					step.result = "failed";
					step.detail = "no running process after restart";
				} else {
					step.result = "stale";
					step.detail =
						running.startedAt < binaryModifiedAt
							? "process started before the deployed binary was modified"
							: "process was not restarted by this sequence";
				}
				return;
			}
			await sleep(pollMs);
		}
	}
}

export interface LaunchRestartOptions {
	readonly home: string;
	readonly platform?: ServicePlatform;
	readonly uid?: number;
	/** Argv that re-enters the CLI; `--run <id>` is appended. */
	readonly worker?: readonly string[];
	readonly env?: NodeJS.ProcessEnv;
}

/** The argv that runs this CLI again, whether compiled or run from source. */
export function cliCommand(): string[] {
	return Bun.main.startsWith("/$bunfs/") ? [process.execPath] : [process.execPath, Bun.main];
}

function alive(pid: number | undefined): boolean {
	if (pid === undefined) return false;
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

/**
 * Queues a restart and hands it to a detached supervisor, returning as soon as
 * the supervisor is spawned so the caller may be killed by the restart itself.
 */
export async function launchRestartStack(
	options: LaunchRestartOptions,
): Promise<{ readonly receipt: RestartReceipt; readonly supervisorPid: number }> {
	const platform = options.platform ?? currentPlatform();
	const existing = await readRestartReceipt(options.home);
	if (existing?.state === "running" && alive(existing.supervisorPid))
		throw new Error(`restart-stack ${existing.id} is still running (supervisor pid ${existing.supervisorPid})`);
	const receipt = newReceipt(randomUUID(), platform, options.uid, Date.now());
	await writeReceipt(options.home, receipt);
	const worker = [...(options.worker ?? [...cliCommand(), "ops", "restart-stack"]), "--run", receipt.id];
	const child = Bun.spawn(supervisorArgv(platform, worker, options.home, receipt.id), {
		detached: true,
		stdin: "ignore",
		stdout: "ignore",
		stderr: "ignore",
		env: { ...(options.env ?? process.env), GAJAEWAY_HOME: options.home },
	});
	child.unref();
	return { receipt, supervisorPid: child.pid };
}

/**
 * A new session escapes the caller's process group, which is enough under
 * launchd. Under systemd the caller usually lives in a service cgroup that the
 * restart it requests may stop, so the supervisor is started as its own
 * transient user unit instead.
 */
export function supervisorArgv(
	platform: ServicePlatform,
	worker: readonly string[],
	home: string,
	id: string,
): string[] {
	if (platform === "darwin") return [...worker];
	return [
		"systemd-run",
		"--user",
		"--collect",
		"--quiet",
		`--unit=gajaeway-restart-stack-${id}`,
		`--setenv=GAJAEWAY_HOME=${home}`,
		"--",
		...worker,
	];
}

/**
 * The receipt's state, except that an unfinished sequence whose supervisor is
 * gone is reported as interrupted rather than left looking in progress.
 */
export function effectiveRestartState(receipt: RestartReceipt): RestartState | "interrupted" {
	if (receipt.state === "running" && !alive(receipt.supervisorPid)) return "interrupted";
	return receipt.state;
}

export function renderRestartReceipt(receipt: RestartReceipt): string[] {
	const lines = [
		`restart-stack ${receipt.id}: ${effectiveRestartState(receipt)}`,
		`requestedAt: ${receipt.requestedAt}`,
		`finishedAt: ${receipt.finishedAt ?? "-"}`,
	];
	for (const step of receipt.steps) {
		const facts = [
			step.pid === undefined ? undefined : `pid=${step.pid}`,
			step.processStartedAt === undefined ? undefined : `started=${step.processStartedAt}`,
			step.binaryModifiedAt === undefined ? undefined : `binary=${step.binaryModifiedAt}`,
			step.detail,
		].filter((fact) => fact !== undefined);
		lines.push(`${step.label}: ${step.result}${facts.length > 0 ? ` ${facts.join(" ")}` : ""}`);
	}
	return lines;
}
