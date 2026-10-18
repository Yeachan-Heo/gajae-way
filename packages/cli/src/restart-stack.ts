import { randomUUID } from "node:crypto";
import { access, mkdir, readFile, realpath, rename, stat, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { basename, dirname, join } from "node:path";
import {
	acquireLease,
	homeLayout,
	type Lease,
	LeaseHeldError,
	type LeaseName,
	leaseHeld,
	readLeaseOwner,
} from "@gajae-gateway/home";
import { ProtocolError } from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";

import {
	adapterConfigPath,
	BROKER_UNIT,
	currentPlatform,
	launchAgentPlistPath,
	removeStaleServices,
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
 * The sequence runs in a detached supervisor (its own session, so the caller's
 * process group and the service manager's job kill do not reach it). Before
 * restarting any service, the supervisor asks the live gateway to hold new
 * persona dispatches and waits for accepted turns to finish delivery. It then
 * verifies every service against its deployed binary and records a receipt
 * under `$GAJAEWAY_HOME` that the next turn reads with `--status`.
 */

export type RestartStepResult = "pending" | "ok" | "stale" | "failed" | "skipped";
export type RestartState = "queued" | "running" | "ok" | "stale" | "failed" | "indeterminate";

export interface RestartDrainLease {
	readonly superseded: AbortSignal;
	assertActive(): Promise<void>;
	cancel(): Promise<void>;
	close(): Promise<void>;
}

export type RestartTrigger = "update" | "manual";

export interface RestartStep {
	readonly label: string;
	/** The final service-manager command; update broker receipts separately record agent sync. */
	command?: readonly string[];
	exitStatus?: number;
	result: RestartStepResult;
	pid?: number;
	processStartedAt?: string;
	binary?: string;
	binaryModifiedAt?: string;
	agentSync?: { readonly command: readonly string[]; readonly exitStatus: number; readonly error?: string };
	detail?: string;
}

export interface RestartReceipt {
	readonly id: string;
	readonly trigger: RestartTrigger;
	state: RestartState;
	/** A live Gateway accepted this drain and must remain fenced across its own restart. */
	drainPrepared?: boolean;
	readonly platform: ServicePlatform;
	readonly requestedAt: string;
	supervisorPid?: number;
	finishedAt?: string;
	/** Why the sequence never ran, e.g. the supervisor could not be started. */
	detail?: string;
	/** Warning for a safe, narrowly-scoped transition from a pre-drain gateway release. */
	warning?: string;
	readonly steps: RestartStep[];
}

/** The main PID reported by the service manager. */
export interface ServiceProcess {
	readonly pid: number;
}

export interface CommandRunnerOptions {
	readonly allowFailure?: boolean;
	readonly signal?: AbortSignal;
}
export type CommandRunner = (
	command: readonly string[],
	options?: CommandRunnerOptions,
) => number | PromiseLike<number>;
export type ServiceProbe = (label: string) => Promise<ServiceProcess | undefined>;
export type IsServiceInstalled = (label: string) => Promise<boolean>;

/** The trigger is explicit so only update receipts synchronize provisioned agent state. */
export interface RunRestartOptions {
	readonly home: string;
	readonly id: string;
	/** Gateway socket override passed through the detached supervisor. */
	readonly gatewaySocketPath?: string;
	/** Test seam for the gateway's durable persona-turn drain. */
	readonly prepareRestartDrain?: (
		socketPath: string,
		restartId: string,
		trigger: RestartTrigger,
		options?: {
			readonly mode?: "prepare" | "reattach";
			onIndeterminate?: () => Promise<void>;
		},
	) => Promise<RestartDrainLease | undefined>;
	readonly trigger?: RestartTrigger;
	readonly platform?: ServicePlatform;
	readonly uid?: number;
	readonly runner?: CommandRunner;
	/** Maximum time a service command may keep the restart drain before it is cancelled. */
	readonly commandTimeoutMs?: number;
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
	readonly binDir?: string;
	readonly unitDir?: string;
}

const RECEIPT_FILE = "restart-stack.json";
const VERIFY_TIMEOUT_MS = 90_000;
const POLL_MS = 1_000;
const RESTART_DRAIN_WAIT_MS = 120_000;
const RESTART_DRAIN_REQUEST_TIMEOUT_MS = RESTART_DRAIN_WAIT_MS + 15_000;
const SERVICE_COMMAND_TIMEOUT_MS = 180_000;
const SERVICE_COMMAND_KILL_GRACE_MS = 2_000;
const SYSTEMD_JOB_PROBE_TIMEOUT_MS = 5_000;
const SYSTEMD_JOB_SETTLE_TIMEOUT_MS = 60_000;
const SYSTEMD_JOB_POLL_MS = 250;

class RestartOperationIndeterminateError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "RestartOperationIndeterminateError";
	}
}

class CommandTimedOutError extends Error {
	constructor(command: readonly string[]) {
		super(`command timed out: ${command.join(" ")}`);
		this.name = "CommandTimedOutError";
	}
}

class CommandUnsettledError extends Error {
	constructor(command: readonly string[]) {
		super(`command did not exit after SIGKILL: ${command.join(" ")}`);
		this.name = "CommandUnsettledError";
	}
}

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

function newReceipt(
	id: string,
	platform: ServicePlatform,
	uid: number | undefined,
	now: number,
	trigger: "update" | "manual",
): RestartReceipt {
	const commands = restartStackCommands(platform, uid);
	return {
		id,
		trigger,
		state: "queued",
		drainPrepared: false,
		platform,
		requestedAt: new Date(now).toISOString(),
		steps: restartOrder(platform).map((spec, index) => ({
			label: spec.label,
			...(platform === "darwin"
				? { command: commands[index] }
				: spec.id === "broker"
					? { command: commands[0] }
					: spec.id === "gateway"
						? { command: commands[1] }
						: {}),
			result: "pending",
		})),
	};
}

async function waitForChildExit(
	child: Bun.Subprocess,
	command: readonly string[],
	signal?: AbortSignal,
): Promise<number> {
	if (signal === undefined) return await child.exited;
	let abortResolve!: () => void;
	const aborted = new Promise<void>((resolve) => (abortResolve = resolve));
	const onAbort = () => abortResolve();
	if (signal.aborted) onAbort();
	else signal.addEventListener("abort", onAbort, { once: true });
	try {
		const first = await Promise.race([
			child.exited.then((status) => ({ status })),
			aborted.then(() => ({ aborted: true as const })),
		]);
		if ("status" in first) return first.status;
		child.kill("SIGTERM");
		let status = await Promise.race([child.exited, Bun.sleep(SERVICE_COMMAND_KILL_GRACE_MS).then(() => undefined)]);
		if (status === undefined) {
			child.kill("SIGKILL");
			status = await Promise.race([child.exited, Bun.sleep(SERVICE_COMMAND_KILL_GRACE_MS).then(() => undefined)]);
		}
		if (status === undefined) throw new CommandUnsettledError(command);
		throw new CommandTimedOutError(command);
	} finally {
		signal.removeEventListener("abort", onAbort);
	}
}

async function defaultCommandRunner(command: readonly string[], options?: CommandRunnerOptions): Promise<number> {
	const child = Bun.spawn([...command], {
		stdin: "ignore",
		stdout: "ignore",
		stderr: "ignore",
		env: { ...process.env },
	});
	return await waitForChildExit(child, command, options?.signal);
}

async function capture(
	command: readonly string[],
	timeoutMs = SYSTEMD_JOB_PROBE_TIMEOUT_MS,
): Promise<string | undefined> {
	const child = Bun.spawn([...command], {
		stdin: "ignore",
		stdout: "pipe",
		stderr: "ignore",
		env: { ...process.env, LC_ALL: "C" },
	});
	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), timeoutMs);
	const output = new Response(child.stdout).text().catch(() => undefined);
	try {
		const status = await waitForChildExit(child, command, controller.signal);
		const text = await output;
		return status === 0 ? text : undefined;
	} catch {
		return undefined;
	} finally {
		clearTimeout(timeout);
	}
}

interface SystemdJob {
	readonly id: number;
	readonly unit: string;
}

export function parseSystemdJobs(output: string): SystemdJob[] {
	const jobs: SystemdJob[] = [];
	for (const line of output.split(/\r?\n/)) {
		const match = line.match(/^\s*(\d+)\s+(\S+)(?:\s+\S+\s+\S+)?\s*$/);
		if (!match) continue;
		const id = Number(match[1]);
		if (Number.isSafeInteger(id)) jobs.push({ id, unit: match[2] as string });
	}
	return jobs;
}

function isSystemdServiceOperation(command: readonly string[]): boolean {
	if (command[0] !== "systemctl" || !command.includes("--user")) return false;
	const verb = command.find((part) => ["start", "stop", "restart", "try-restart", "reload", "disable"].includes(part));
	if (verb === undefined) return false;
	return command.some((part) => part.endsWith(".service")) && (verb !== "disable" || command.includes("--now"));
}

async function timedSystemdCommand(command: readonly string[], timeoutMs: number): Promise<number | undefined> {
	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), timeoutMs);
	try {
		return await defaultCommandRunner(command, { signal: controller.signal });
	} catch {
		return undefined;
	} finally {
		clearTimeout(timeout);
	}
}

async function systemdJobs(): Promise<SystemdJob[] | undefined> {
	const output = await capture(["systemctl", "--user", "list-jobs", "--no-legend", "--no-pager"]);
	return output === undefined ? undefined : parseSystemdJobs(output);
}

async function waitForSystemdServiceJobs(command: readonly string[]): Promise<boolean> {
	const targets = new Set(command.filter((part) => part.endsWith(".service")));
	const relevant = (job: SystemdJob) => targets.has(job.unit) || job.unit.startsWith("gajaeway-");
	const deadline = Date.now() + SYSTEMD_JOB_SETTLE_TIMEOUT_MS;
	let cancelRequested = false;
	for (;;) {
		const jobs = await systemdJobs();
		if (jobs === undefined) return false;
		const pending = jobs.filter(relevant);
		if (pending.length === 0) return true;
		if (!cancelRequested) {
			cancelRequested = true;
			await timedSystemdCommand(
				["systemctl", "--user", "cancel", ...pending.map((job) => String(job.id))],
				SYSTEMD_JOB_PROBE_TIMEOUT_MS,
			);
		}
		if (Date.now() >= deadline) return false;
		await Bun.sleep(SYSTEMD_JOB_POLL_MS);
	}
}

/** Reads the current main PID directly from launchd or systemd. */
export function defaultServiceProbe(platform: ServicePlatform, uid = process.getuid?.() ?? 0): ServiceProbe {
	return async (label) => {
		const spec = serviceSpecs().find((candidate) => candidate.label === label);
		if (spec === undefined) throw new Error(`unknown service label: ${label}`);
		const shown =
			platform === "darwin"
				? await capture(["launchctl", "print", `gui/${uid}/${label}`])
				: await capture(["systemctl", "--user", "show", "-p", "MainPID", "--value", systemdUnitName(spec)]);
		const rawPid = platform === "darwin" ? shown?.match(/^\s*pid = (\d+);?\s*$/m)?.[1] : shown?.trim();
		const pid = rawPid === undefined ? undefined : Number(rawPid);
		if (pid === undefined || !Number.isSafeInteger(pid) || pid <= 0) return undefined;
		return { pid };
	};
}

async function defaultBinaryModifiedAt(path: string): Promise<number> {
	return (await stat(path)).mtimeMs;
}

async function prepareGatewayRestartDrain(
	socketPath: string,
	restartId: string,
	_trigger: RestartTrigger,
	options?: { readonly mode?: "prepare" | "reattach"; onIndeterminate?: () => Promise<void> },
): Promise<RestartDrainLease | undefined> {
	let client: GajaewayClient;
	try {
		client = await GajaewayClient.connectSocket(socketPath, {
			clientName: "gajaeway-restart-stack",
			requestTimeoutMs: RESTART_DRAIN_REQUEST_TIMEOUT_MS,
		});
	} catch (error) {
		const code = error instanceof Error ? (error as NodeJS.ErrnoException).code : undefined;
		if (code === "ENOENT" || code === "ECONNREFUSED" || code === "ENOTSOCK") return undefined;
		throw new Error(
			`gateway restart drain connection failed: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	const superseded = new AbortController();
	const offSuperseded = client.on("gateway.restart.superseded", (payload) => {
		const event = payload as { restartId?: unknown } | undefined;
		if (event?.restartId === restartId)
			superseded.abort(new Error("gateway restart drain was superseded by an owner restart"));
	});
	try {
		const result = await client.request<{ ready?: unknown; drained?: unknown }>("gateway.restart.prepare", {
			restartId,
			waitMs: RESTART_DRAIN_WAIT_MS,
		});
		if (result.ready !== true || result.drained !== true)
			throw new Error("gateway did not confirm restart-drain delivery readiness");
	} catch (error) {
		if (options?.mode === "reattach") {
			try {
				await options.onIndeterminate?.();
			} catch (persistError) {
				console.error(`restart drain indeterminate receipt failed id=${restartId} detail=${String(persistError)}`);
			}
		} else {
			try {
				await client.request("gateway.restart.cancel", { restartId });
			} catch {
				// The gateway may have stopped or failed after it accepted the drain;
				// closing the socket also releases an uncommitted hold.
			}
		}
		offSuperseded();
		await client.close();
		if (error instanceof ProtocolError && error.code === "unknown_verb") throw error;
		throw new Error(`gateway restart drain failed: ${error instanceof Error ? error.message : String(error)}`);
	}
	return {
		superseded: superseded.signal,
		assertActive: async () => {
			if (superseded.signal.aborted)
				throw superseded.signal.reason instanceof Error
					? superseded.signal.reason
					: new Error("gateway restart drain was superseded");
			const result = await client.request<{ ready?: unknown; drained?: unknown }>("gateway.restart.prepare", {
				restartId,
				waitMs: RESTART_DRAIN_WAIT_MS,
			});
			if (superseded.signal.aborted)
				throw superseded.signal.reason instanceof Error
					? superseded.signal.reason
					: new Error("gateway restart drain was superseded");
			if (result.ready !== true || result.drained !== true) throw new Error("gateway restart drain ownership was lost");
		},
		cancel: async () => {
			const result = await client.request<{ cancelled?: unknown; transferred?: unknown }>("gateway.restart.cancel", {
				restartId,
			});
			if (result.cancelled !== true)
				throw new Error(
					result.transferred === true
						? "gateway transferred the restart drain to an owner restart"
						: "gateway did not confirm restart-drain cancellation",
				);
		},
		close: async () => {
			offSuperseded();
			await client.close();
		},
	};
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

const SERVICE_LEASES: Partial<Record<string, LeaseName>> = {
	broker: "broker",
	gateway: "gateway",
	"adapter-discord": "adapter-discord",
	"adapter-slack": "adapter-slack",
	"adapter-telegram": "adapter-telegram",
};

function serviceStopCommand(platform: ServicePlatform, uid: number | undefined): readonly string[] {
	if (platform === "linux") return ["systemctl", "--user", "stop", BROKER_UNIT];
	const target = uid ?? process.getuid?.() ?? 0;
	return ["launchctl", "bootout", `gui/${target}/dev.gajaeway.broker`];
}

function serviceBootstrapCommand(
	options: RunRestartOptions,
	platform: ServicePlatform,
	label: string,
): readonly string[] {
	if (platform === "linux") return ["systemctl", "--user", "start", BROKER_UNIT];
	const target = options.uid ?? process.getuid?.() ?? 0;
	const userHome = options.env?.HOME || homedir();
	const agentsDir = options.launchAgentsDir ?? join(userHome, "Library", "LaunchAgents");
	const spec = serviceSpecs().find((candidate) => candidate.label === label);
	if (spec === undefined) throw new Error(`unknown service label: ${label}`);
	return ["launchctl", "bootstrap", `gui/${target}`, launchAgentPlistPath(spec, agentsDir)];
}
/**
 * Runs the ordered restart, verifying each service before the next command,
 * and persists the receipt after every transition. The first service that is
 * stale or missing aborts the remainder.
 */
export async function runRestartStack(options: RunRestartOptions): Promise<RestartReceipt> {
	let lease: Lease;
	try {
		lease = await acquireLease(options.home, "update");
		console.info(`lease_acquired name=update pid=${process.pid}`);
	} catch (error) {
		if (error instanceof LeaseHeldError) console.error(error.message);
		else console.error(`lease_failed name=update detail=${error instanceof Error ? error.message : String(error)}`);
		throw error;
	}
	let preparedDrain: { readonly lease: RestartDrainLease; readonly receipt: RestartReceipt } | undefined;
	let receipt: RestartReceipt | undefined;
	try {
		receipt = await runRestartStackWithLease(options, (drain, preparedReceipt) => {
			preparedDrain = { lease: drain, receipt: preparedReceipt };
		});
		return receipt;
	} finally {
		try {
			const completedReceipt = receipt ?? preparedDrain?.receipt;
			if (completedReceipt !== undefined) {
				await releaseRestartDrain(preparedDrain?.lease, completedReceipt, options.id, options);
				if (receipt !== undefined && completedReceipt.state === "running") {
					completedReceipt.state = "ok";
					completedReceipt.finishedAt = new Date((options.now ?? Date.now)()).toISOString();
					await writeReceipt(options.home, completedReceipt);
				}
			}
		} finally {
			await lease.release();
		}
	}
}

export interface RecoverRestartStackOptions {
	readonly home: string;
	readonly gatewaySocketPath?: string;
	readonly confirmServiceManagerSettled: boolean;
}

/** Clears a persisted restart fence only after an operator confirms the manager has no work in flight. */
export async function recoverRestartStack(options: RecoverRestartStackOptions): Promise<RestartReceipt> {
	if (!options.confirmServiceManagerSettled)
		throw new Error("restart-stack recovery requires --confirm-service-manager-settled");
	const lease = await acquireLease(options.home, "update");
	try {
		const receipt = await readRestartReceipt(options.home);
		if (receipt === undefined || (receipt.state !== "running" && receipt.state !== "indeterminate"))
			throw new Error("restart-stack has no interrupted or indeterminate drain to recover");
		if (receipt.platform === "linux") {
			const jobs = await systemdJobs();
			if (jobs === undefined) throw new Error("could not verify systemd jobs; restart drain remains held");
			const active = jobs.filter(
				(job) => job.unit.startsWith("gajaeway-") || receipt.steps.some((step) => step.command?.includes(job.unit)),
			);
			if (active.length > 0)
				throw new Error(
					`systemd still has restart-stack jobs: ${active.map((job) => `${job.id}:${job.unit}`).join(", ")}`,
				);
		}

		const socketPath = options.gatewaySocketPath ?? join(options.home, "gateway.sock");
		let client: GajaewayClient | undefined;
		try {
			client = await GajaewayClient.connectSocket(socketPath, { clientName: "gajaeway-restart-recovery" });
		} catch (error) {
			const code = error instanceof Error ? (error as NodeJS.ErrnoException).code : undefined;
			if (code !== "ENOENT" && code !== "ECONNREFUSED" && code !== "ENOTSOCK") throw error;
		}
		if (client !== undefined) {
			try {
				const result = await client.request<{ cancelled: boolean }>("gateway.restart.cancel", {
					restartId: receipt.id,
				});
				if (!result.cancelled) throw new Error("gateway did not confirm restart-drain cancellation");
			} finally {
				await client.close();
			}
		} else if (await leaseHeld(options.home, "gateway")) {
			throw new Error(
				"gateway lease is still held but its socket is unavailable; retry recovery when the gateway is ready or stopped",
			);
		}
		receipt.state = "failed";
		receipt.finishedAt = new Date().toISOString();
		receipt.detail = "operator recovered after confirming the service manager has no operation in flight";
		for (const step of receipt.steps)
			if (step.result === "pending") {
				step.result = "skipped";
				step.detail = "not run; restart sequence was recovered by the operator";
			}
		await writeReceipt(options.home, receipt);
		return receipt;
	} finally {
		await lease.release();
	}
}

async function runRestartStackWithLease(
	options: RunRestartOptions,
	onDrainPrepared: (drain: RestartDrainLease, receipt: RestartReceipt) => void,
): Promise<RestartReceipt> {
	const platform = options.platform ?? currentPlatform();
	const runner = options.runner ?? defaultCommandRunner;
	const probe = options.probe ?? defaultServiceProbe(platform, options.uid);
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
	const commandTimeoutMs = options.commandTimeoutMs ?? SERVICE_COMMAND_TIMEOUT_MS;
	const env = options.env ?? process.env;
	const deployedExecutable = await realpath(process.execPath).catch(() => undefined);
	const installedBinDir =
		options.binDir ??
		env.GAJAEWAY_BIN_DIR ??
		(deployedExecutable !== undefined && basename(deployedExecutable).startsWith("gajaeway")
			? dirname(deployedExecutable)
			: undefined);
	const binDir = installedBinDir ?? dirname(process.execPath);

	const originalPids = new Map<string, number | undefined>();
	for (const spec of restartOrder(platform)) {
		if (SERVICE_LEASES[spec.id] === undefined && (await isInstalled(spec.label)))
			originalPids.set(spec.label, (await probe(spec.label))?.pid);
	}

	const existing = await readRestartReceipt(options.home);
	if (existing?.id === options.id && existing.state !== "queued")
		throw new Error(`restart-stack ${options.id}: receipt is ${existing.state}; refusing to start worker`);
	const receipt =
		existing?.id === options.id
			? existing
			: newReceipt(options.id, platform, options.uid, now(), options.trigger ?? "manual");
	receipt.state = "running";
	receipt.supervisorPid = process.pid;
	await writeReceipt(options.home, receipt);

	let restartDrain: RestartDrainLease | undefined;
	try {
		restartDrain = await (options.prepareRestartDrain ?? prepareGatewayRestartDrain)(
			options.gatewaySocketPath ?? env.GAJAEWAY_SOCKET_PATH ?? join(options.home, "gateway.sock"),
			options.id,
			receipt.trigger,
		);
	} catch (error) {
		if (receipt.trigger === "update" && error instanceof ProtocolError && error.code === "unknown_verb") {
			receipt.warning = "running gateway lacks restart draining; this update relies on durable turn recovery";
			await writeReceipt(options.home, receipt);
		} else {
			receipt.state = "failed";
			receipt.finishedAt = new Date(now()).toISOString();
			receipt.detail = `persona turn drain failed: ${error instanceof Error ? error.message : String(error)}`;
			for (const step of receipt.steps) step.result = "skipped";
			await writeReceipt(options.home, receipt);
			return receipt;
		}
	}
	if (restartDrain !== undefined) {
		receipt.drainPrepared = true;
		onDrainPrepared(restartDrain, receipt);
		await writeReceipt(options.home, receipt);
	}
	let gatewayRestarted = false;
	const assertRestartDrain = async (): Promise<void> => {
		if (restartDrain?.superseded.aborted)
			throw restartDrain.superseded.reason instanceof Error
				? restartDrain.superseded.reason
				: new Error("gateway restart drain was superseded");
		if (restartDrain !== undefined && !gatewayRestarted) await restartDrain.assertActive();
		if (restartDrain?.superseded.aborted)
			throw restartDrain.superseded.reason instanceof Error
				? restartDrain.superseded.reason
				: new Error("gateway restart drain was superseded");
	};
	const runServiceCommand = async (
		command: readonly string[],
		_runOptions?: { readonly allowFailure?: boolean },
	): Promise<number> => {
		await assertRestartDrain();
		const controller = new AbortController();
		let timedOut = false;
		const timeout = setTimeout(() => {
			timedOut = true;
			controller.abort();
		}, commandTimeoutMs);
		let status: number;
		try {
			status = await runner(command, { signal: controller.signal });
		} catch (error) {
			if (!timedOut) throw error;
			if (platform === "linux" && isSystemdServiceOperation(command)) {
				if (!(await waitForSystemdServiceJobs(command)))
					throw new RestartOperationIndeterminateError(
						`systemd operation did not settle after command timeout: ${command.join(" ")}`,
					);
				status = 0;
			} else if (platform === "darwin" && command[0] === "launchctl") {
				throw new RestartOperationIndeterminateError(
					`launchd operation could not be confirmed after command timeout: ${command.join(" ")}`,
				);
			} else if (error instanceof CommandUnsettledError) {
				throw new RestartOperationIndeterminateError(error.message);
			} else throw new Error(`command timed out after ${commandTimeoutMs}ms: ${command.join(" ")}`);
		} finally {
			clearTimeout(timeout);
		}
		// Keep the lease until the manager command settles; a pending owner restart
		// can then take over without racing the operation already in flight.
		if (restartDrain?.superseded.aborted)
			throw restartDrain.superseded.reason instanceof Error
				? restartDrain.superseded.reason
				: new Error("gateway restart drain was superseded");
		return status;
	};
	if (installedBinDir !== undefined) {
		try {
			await removeStaleServices({
				binDir: installedBinDir,
				platform,
				uid: options.uid,
				env,
				...(options.launchAgentsDir === undefined ? {} : { launchAgentsDir: options.launchAgentsDir }),
				...(options.unitDir === undefined ? {} : { unitDir: options.unitDir }),
				run: runServiceCommand,
			});
		} catch (error) {
			receipt.state = error instanceof RestartOperationIndeterminateError ? "indeterminate" : "failed";
			receipt.finishedAt = new Date(now()).toISOString();
			receipt.detail = `stale service cleanup failed: ${error instanceof Error ? error.message : String(error)}`;
			for (const step of receipt.steps) step.result = "skipped";
			await writeReceipt(options.home, receipt);
			return receipt;
		}
	}
	let issuedAt = now();
	let gatewayRestartAttempted = false;
	for (const step of receipt.steps) {
		if (receipt.state !== "running") {
			step.result = "skipped";
			continue;
		}
		try {
			if (
				step.label === "dev.gajaeway.broker" &&
				!(await Bun.file(homeLayout(options.home).brokerProvisionManifest).exists())
			) {
				step.result = "skipped";
				step.detail = "broker agent is not provisioned";
				await writeReceipt(options.home, receipt);
				continue;
			}
			const installed = await isInstalled(step.label);
			if (!installed) {
				step.result = "skipped";
				step.detail = "service is not installed";
				await writeReceipt(options.home, receipt);
				continue;
			}
			// An installed unit can still be unable to start: an adapter whose
			// `adapter-<platform>.json` is absent exits on the credential read, and
			// launchd KeepAlive turns that into a crash loop. Skip it the same way an
			// uninstalled service is skipped — the rule `gajaeway update` already
			// applies when it renders adapter units — so one unconfigured adapter
			// cannot abort the rest of the sequence.
			const adapterSpec = serviceSpecs().find((candidate) => candidate.label === step.label);
			const adapterConfig = adapterSpec === undefined ? undefined : adapterConfigPath(options.home, adapterSpec.id);
			if (adapterConfig !== undefined && !(await Bun.file(adapterConfig).exists())) {
				step.result = "skipped";
				step.detail = "adapter is not configured";
				await writeReceipt(options.home, receipt);
				continue;
			}

			if (step.label === "dev.gajaeway.broker" && receipt.trigger === "update") {
				const stopCommand = serviceStopCommand(platform, options.uid);
				const stopStatus = await runServiceCommand(stopCommand);
				if (stopStatus !== 0 && !(platform === "darwin" && (await probe(step.label)) === undefined))
					throw new Error(`${stopCommand.join(" ")} exited with status ${stopStatus}`);

				const provisionCommand = [
					join(binDir, "gajaeway-gateway"),
					"broker-agent",
					"provision",
					"--mode",
					"update",
					"--lease-wait-ms",
					"35000",
				];
				const provisionStatus = await runServiceCommand(provisionCommand);
				step.agentSync = {
					command: provisionCommand,
					exitStatus: provisionStatus,
					...(provisionStatus === 1 || provisionStatus === 3
						? { error: `broker-agent provision exited with status ${provisionStatus}` }
						: provisionStatus === 4
							? { error: "broker_agent_unrecoverable" }
							: {}),
				};
				if (provisionStatus === 4) {
					delete step.command;
					step.exitStatus = 4;
					step.result = "failed";
					step.detail = "broker left stopped: broker_agent_unrecoverable; run gajaeway setup --broker-agent";
				} else if (provisionStatus === 0 || provisionStatus === 1 || provisionStatus === 3) {
					const startCommand = serviceBootstrapCommand(options, platform, step.label);
					step.command = startCommand;
					issuedAt = now();
					const startStatus = await runServiceCommand(startCommand);
					step.exitStatus = startStatus;
					if (startStatus !== 0) {
						step.result = "failed";
						step.detail = `${startCommand.join(" ")} exited with status ${startStatus}`;
					} else await verifyStep(step, issuedAt);
				} else {
					delete step.command;
					step.exitStatus = provisionStatus;
					step.result = "failed";
					step.detail = `broker-agent provision exited with unexpected status ${provisionStatus}`;
				}
			} else {
				if (step.command !== undefined) {
					if (step.label === "dev.gajaeway.gateway") gatewayRestartAttempted = true;
					issuedAt = now();
					let command = step.command;
					let status = await runServiceCommand(command);
					if (status !== 0 && platform === "darwin" && (await probe(step.label)) === undefined) {
						command = serviceBootstrapCommand(options, platform, step.label);
						step.command = command;
						issuedAt = now();
						status = await runServiceCommand(command);
					}
					step.exitStatus = status;
					if (status !== 0) {
						step.result = "failed";
						step.detail = `${command.join(" ")} exited with status ${status}`;
					}
				}
				if (step.result === "pending") await verifyStep(step, issuedAt);
			}
		} catch (error) {
			step.result = "failed";
			step.detail = error instanceof Error ? error.message : String(error);
			if (error instanceof RestartOperationIndeterminateError) {
				receipt.state = "indeterminate";
				receipt.detail = step.detail;
			}
		}
		if (step.label === "dev.gajaeway.gateway" && step.result === "ok") gatewayRestarted = true;
		if (
			gatewayRestartAttempted &&
			step.label === "dev.gajaeway.gateway" &&
			step.result !== "ok" &&
			receipt.state !== "indeterminate"
		) {
			receipt.state = "indeterminate";
			receipt.detail = `gateway restart did not verify; operator recovery is required: ${step.detail ?? "unknown outcome"}`;
		}
		if (receipt.state !== "indeterminate" && step.result !== "ok" && step.result !== "skipped") {
			receipt.state = step.result === "stale" ? "stale" : "failed";
		}
		await writeReceipt(options.home, receipt);
	}
	if (receipt.state !== "running") receipt.finishedAt = new Date(now()).toISOString();
	await writeReceipt(options.home, receipt);
	return receipt;

	async function verifyStep(step: RestartStep, since: number): Promise<void> {
		const spec = serviceSpecs().find((candidate) => candidate.label === step.label);
		if (spec === undefined) throw new Error(`unknown service label: ${step.label}`);
		const leaseName = SERVICE_LEASES[spec.id];
		const binary = join(binDir, spec.binary);
		const binaryModifiedAt = leaseName === undefined ? undefined : await modifiedAt(binary);
		const deadline = now() + timeoutMs;
		for (;;) {
			const running = await probe(step.label);
			const owner = leaseName === undefined ? undefined : await readLeaseOwner(options.home, leaseName);
			if (running !== undefined) step.pid = running.pid;

			if (leaseName !== undefined) {
				step.binary = binary;
				step.binaryModifiedAt = new Date(binaryModifiedAt as number).toISOString();
				if (owner !== undefined) step.processStartedAt = owner.startedAt;
				const startedAt = owner === undefined ? Number.NaN : Date.parse(owner.startedAt);
				if (
					running !== undefined &&
					owner !== undefined &&
					running.pid === owner.pid &&
					Number.isFinite(startedAt) &&
					startedAt >= Math.max(since, binaryModifiedAt as number)
				) {
					step.result = "ok";
					delete step.detail;
					return;
				}
			} else if (running !== undefined && running.pid !== originalPids.get(step.label)) {
				step.result = "ok";
				delete step.detail;
				return;
			}

			if (now() >= deadline) {
				if (running === undefined) {
					step.result = "failed";
					step.detail = "no running process after restart";
				} else if (leaseName === undefined) {
					step.result = "stale";
					step.detail = "MainPID did not change after restart";
				} else if (owner === undefined) {
					step.result = "stale";
					step.detail = "no lease owner after restart";
				} else if (running.pid !== owner.pid) {
					step.result = "stale";
					step.detail = "lease owner PID does not match service MainPID";
				} else if (Date.parse(owner.startedAt) < (binaryModifiedAt as number)) {
					step.result = "stale";
					step.detail = "process started before the deployed binary was modified";
				} else {
					step.result = "stale";
					step.detail = "lease owner predates this restart sequence";
				}
				return;
			}
			await sleep(pollMs);
		}
	}
}

async function releaseRestartDrain(
	restartDrain: RestartDrainLease | undefined,
	receipt: RestartReceipt,
	restartId: string,
	options: RunRestartOptions,
): Promise<void> {
	if (receipt.state === "indeterminate") {
		await closeRestartDrain(restartDrain, restartId);
		return;
	}
	const gatewayRestarted = receipt.steps.some((step) => step.label === "dev.gajaeway.gateway" && step.result === "ok");
	if (gatewayRestarted && receipt.drainPrepared) {
		const socketPath =
			options.gatewaySocketPath ?? options.env?.GAJAEWAY_SOCKET_PATH ?? join(options.home, "gateway.sock");
		const prepare = options.prepareRestartDrain ?? prepareGatewayRestartDrain;
		const now = options.now ?? Date.now;
		const sleep = options.sleep ?? ((ms: number) => Bun.sleep(ms));
		const retryDelayMs = Math.max(1, Math.floor(options.pollMs ?? POLL_MS));
		const reattachDeadline = now() + Math.max(0, options.verifyTimeoutMs ?? VERIFY_TIMEOUT_MS);
		let reattached: RestartDrainLease | undefined;
		try {
			for (;;) {
				reattached = await prepare(socketPath, restartId, receipt.trigger, {
					mode: "reattach",
					onIndeterminate: async () => {
						receipt.state = "indeterminate";
						receipt.finishedAt = new Date().toISOString();
						receipt.detail = "new gateway did not confirm the restart drain after service restart";
						await writeReceipt(options.home, receipt);
					},
				});
				if (reattached !== undefined) break;
				const remainingMs = reattachDeadline - now();
				if (remainingMs <= 0)
					throw new Error("replacement gateway socket did not become ready before the reattach deadline");
				await sleep(Math.min(retryDelayMs, remainingMs));
			}
			await reattached.cancel();
		} catch (error) {
			receipt.state = "indeterminate";
			receipt.finishedAt = new Date().toISOString();
			receipt.detail = `restart drain reattachment failed: ${error instanceof Error ? error.message : String(error)}`;
			await writeReceipt(options.home, receipt);
			console.error(`restart drain reattachment failed id=${restartId} detail=${receipt.detail}`);
		} finally {
			await closeRestartDrain(reattached, restartId);
			await closeRestartDrain(restartDrain, restartId);
		}
		return;
	}
	if (restartDrain !== undefined) {
		try {
			await restartDrain.cancel();
		} catch (error) {
			receipt.state = "indeterminate";
			receipt.finishedAt = new Date((options.now ?? Date.now)()).toISOString();
			receipt.detail = `restart drain cancellation failed: ${error instanceof Error ? error.message : String(error)}`;
			await writeReceipt(options.home, receipt);
			console.error(`restart drain cancellation failed id=${restartId} detail=${String(error)}`);
		}
	}
	await closeRestartDrain(restartDrain, restartId);
}

async function closeRestartDrain(restartDrain: RestartDrainLease | undefined, restartId: string): Promise<void> {
	if (restartDrain === undefined) return;
	try {
		await restartDrain.close();
	} catch (error) {
		console.error(`restart drain socket close failed id=${restartId} detail=${String(error)}`);
	}
}

export interface LaunchRestartOptions {
	readonly home: string;
	readonly gatewaySocketPath?: string;
	readonly trigger?: "update" | "manual";
	readonly platform?: ServicePlatform;
	readonly uid?: number;
	/** Installed service binary directory used for lease verification. */
	readonly binDir?: string;
	/** Argv that re-enters the CLI; `--run <id>` is appended. */
	readonly worker?: readonly string[];
	readonly env?: NodeJS.ProcessEnv;
	/** Starts systemd-run; injectable for launcher-result handling. */
	readonly systemdRun?: (command: readonly string[]) => Bun.Subprocess;
	/** Maximum time to wait for systemd-run to acknowledge the worker exec. */
	readonly systemdRunTimeoutMs?: number;
}

const SYSTEMD_RUN_TIMEOUT_MS = 30_000;

/** The argv that runs this CLI again, whether compiled or run from source. */
export function cliCommand(): string[] {
	return Bun.main.startsWith("/$bunfs/") ? [process.execPath] : [process.execPath, Bun.main];
}

/**
 * Queues a restart and hands it to a detached supervisor, returning once the
 * platform launcher acknowledges it so the caller may be killed by the restart.
 */
export async function launchRestartStack(
	options: LaunchRestartOptions,
): Promise<{ readonly receipt: RestartReceipt; readonly supervisorPid: number }> {
	const platform = options.platform ?? currentPlatform();
	if (await leaseHeld(options.home, "update"))
		throw new LeaseHeldError("update", await readLeaseOwner(options.home, "update"));
	const receipt = newReceipt(randomUUID(), platform, options.uid, Date.now(), options.trigger ?? "manual");
	await writeReceipt(options.home, receipt);
	const worker = [...(options.worker ?? [...cliCommand(), "ops", "restart-stack"]), "--run", receipt.id];
	const workerEnv = options.env ?? process.env;
	const gatewaySocketPath = options.gatewaySocketPath ?? workerEnv.GAJAEWAY_SOCKET_PATH;
	const argv = supervisorArgv(platform, worker, options.home, receipt.id, options.binDir, gatewaySocketPath);
	const spawnOptions = {
		detached: true,
		stdin: "ignore" as const,
		stdout: "ignore" as const,
		stderr: "ignore" as const,
		env: {
			...workerEnv,
			GAJAEWAY_HOME: options.home,
			...(options.gatewaySocketPath === undefined ? {} : { GAJAEWAY_SOCKET_PATH: options.gatewaySocketPath }),
			...(options.binDir === undefined ? {} : { GAJAEWAY_BIN_DIR: options.binDir }),
		},
	};
	let child: Bun.Subprocess;
	try {
		child =
			platform === "linux"
				? (options.systemdRun ?? ((command) => Bun.spawn([...command], spawnOptions)))(argv)
				: Bun.spawn(argv, spawnOptions);
	} catch (error) {
		const detail =
			platform === "linux"
				? `supervisor systemd-run could not be started (${error instanceof Error ? error.message : String(error)}); without a service manager here, restart through the host's own supervisor`
				: `supervisor ${argv[0]} could not be started (${error instanceof Error ? error.message : String(error)})`;
		await failLaunch(options.home, receipt, detail);
		throw new Error(`restart-stack ${receipt.id}: failed: ${detail}`);
	}
	if (platform === "linux") {
		const outcome = await waitForLauncher(child, options.systemdRunTimeoutMs ?? SYSTEMD_RUN_TIMEOUT_MS);
		if (outcome.kind === "timed_out") {
			const detail = `supervisor systemd-run timed out after ${options.systemdRunTimeoutMs ?? SYSTEMD_RUN_TIMEOUT_MS}ms; handoff outcome is indeterminate`;
			await failLaunch(options.home, receipt, detail);
			throw new Error(`restart-stack ${receipt.id}: failed: ${detail}`);
		}
		if (outcome.exitStatus !== 0) {
			const detail = `supervisor systemd-run exited with status ${outcome.exitStatus}`;
			await failLaunch(options.home, receipt, detail);
			throw new Error(`restart-stack ${receipt.id}: failed: ${detail}`);
		}
	}
	child.unref();
	return { receipt, supervisorPid: child.pid };
}

async function failLaunch(home: string, receipt: RestartReceipt, detail: string): Promise<void> {
	receipt.state = "failed";
	receipt.finishedAt = new Date().toISOString();
	receipt.detail = detail;
	for (const step of receipt.steps) step.result = "skipped";
	await writeReceipt(home, receipt);
}

async function waitForLauncher(
	child: Bun.Subprocess,
	timeoutMs: number,
): Promise<{ readonly kind: "exited"; readonly exitStatus: number } | { readonly kind: "timed_out" }> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const outcome = await Promise.race([
		child.exited.then((exitStatus) => ({ kind: "exited" as const, exitStatus })),
		new Promise<{ readonly kind: "timed_out" }>((resolve) => {
			timer = setTimeout(() => resolve({ kind: "timed_out" }), timeoutMs);
		}),
	]);
	if (timer !== undefined) clearTimeout(timer);
	if (outcome.kind === "timed_out") {
		child.kill("SIGKILL");
		void child.exited.catch(() => undefined);
	}
	return outcome;
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
	binDir?: string,
	gatewaySocketPath?: string,
): string[] {
	if (platform === "darwin") return [...worker];
	return [
		"systemd-run",
		"--user",
		"--collect",
		"--quiet",
		"--service-type=exec",
		`--unit=gajaeway-restart-stack-${id}`,
		`--setenv=GAJAEWAY_HOME=${home}`,
		...(binDir === undefined ? [] : [`--setenv=GAJAEWAY_BIN_DIR=${binDir}`]),
		...(gatewaySocketPath === undefined ? [] : [`--setenv=GAJAEWAY_SOCKET_PATH=${gatewaySocketPath}`]),
		"--",
		...worker,
	];
}

/**
 * The receipt's state, except that an unfinished sequence whose supervisor is
 * gone is reported as interrupted rather than left looking in progress.
 */
export async function effectiveRestartState(
	receipt: RestartReceipt,
	home: string,
): Promise<RestartState | "interrupted"> {
	if (receipt.state === "running" && !(await leaseHeld(home, "update"))) return "interrupted";
	return receipt.state;
}

export function renderRestartReceipt(
	receipt: RestartReceipt,
	state: RestartState | "interrupted" = receipt.state,
): string[] {
	const lines = [
		`restart-stack ${receipt.id}: ${state}`,
		`requestedAt: ${receipt.requestedAt}`,
		`finishedAt: ${receipt.finishedAt ?? "-"}`,
		...(receipt.detail === undefined ? [] : [`detail: ${receipt.detail}`]),
		...(receipt.warning === undefined ? [] : [`warning: ${receipt.warning}`]),
	];
	for (const step of receipt.steps) {
		const facts = [
			step.pid === undefined ? undefined : `pid=${step.pid}`,
			step.processStartedAt === undefined ? undefined : `started=${step.processStartedAt}`,
			step.binaryModifiedAt === undefined ? undefined : `binary=${step.binaryModifiedAt}`,
			step.agentSync?.error === undefined ? undefined : `agentSync.error=${step.agentSync.error}`,
			step.detail,
		].filter((fact) => fact !== undefined);
		lines.push(`${step.label}: ${step.result}${facts.length > 0 ? ` ${facts.join(" ")}` : ""}`);
	}
	return lines;
}
