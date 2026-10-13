import { randomUUID } from "node:crypto";
import { access, mkdir, readFile, rename, stat, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import {
	acquireLease,
	homeLayout,
	type Lease,
	LeaseHeldError,
	type LeaseName,
	leaseHeld,
	readLeaseOwner,
} from "@gajae-gateway/home";

import {
	BROKER_UNIT,
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
	readonly trigger: "update" | "manual";
	state: RestartState;
	readonly platform: ServicePlatform;
	readonly requestedAt: string;
	supervisorPid?: number;
	finishedAt?: string;
	/** Why the sequence never ran, e.g. the supervisor could not be started. */
	detail?: string;
	readonly steps: RestartStep[];
}

/** The main PID reported by the service manager. */
export interface ServiceProcess {
	readonly pid: number;
}

export type CommandRunner = (command: readonly string[]) => number | PromiseLike<number>;
export type ServiceProbe = (label: string) => Promise<ServiceProcess | undefined>;
export type IsServiceInstalled = (label: string) => Promise<boolean>;

/** The trigger is explicit so only update receipts synchronize provisioned agent state. */
export interface RunRestartOptions {
	readonly home: string;
	readonly id: string;
	readonly trigger?: "update" | "manual";
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
	readonly binDir?: string;
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
		platform,
		requestedAt: new Date(now).toISOString(),
		steps: restartOrder().map((spec, index) => ({
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
	try {
		return await runRestartStackWithLease(options);
	} finally {
		await lease.release();
	}
}

async function runRestartStackWithLease(options: RunRestartOptions): Promise<RestartReceipt> {
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
	const binDir = options.binDir ?? process.env.GAJAEWAY_BIN_DIR ?? dirname(process.execPath);

	const originalPids = new Map<string, number | undefined>();
	for (const spec of restartOrder()) {
		if (SERVICE_LEASES[spec.id] === undefined && (await isInstalled(spec.label)))
			originalPids.set(spec.label, (await probe(spec.label))?.pid);
	}

	const existing = await readRestartReceipt(options.home);
	const receipt =
		existing?.id === options.id && existing.state === "queued"
			? existing
			: newReceipt(options.id, platform, options.uid, now(), options.trigger ?? "manual");
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

			if (step.label === "dev.gajaeway.broker" && receipt.trigger === "update") {
				const stopCommand = serviceStopCommand(platform, options.uid);
				const stopStatus = await runner(stopCommand);
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
				const provisionStatus = await runner(provisionCommand);
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
					const startStatus = await runner(startCommand);
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
					issuedAt = now();
					let command = step.command;
					let status = await runner(command);
					if (
						status !== 0 &&
						receipt.trigger === "update" &&
						platform === "darwin" &&
						(await probe(step.label)) === undefined
					) {
						command = serviceBootstrapCommand(options, platform, step.label);
						step.command = command;
						issuedAt = now();
						status = await runner(command);
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

export interface LaunchRestartOptions {
	readonly home: string;
	readonly trigger?: "update" | "manual";
	readonly platform?: ServicePlatform;
	readonly uid?: number;
	/** Installed service binary directory used for lease verification. */
	readonly binDir?: string;
	/** Argv that re-enters the CLI; `--run <id>` is appended. */
	readonly worker?: readonly string[];
	readonly env?: NodeJS.ProcessEnv;
}

/** The argv that runs this CLI again, whether compiled or run from source. */
export function cliCommand(): string[] {
	return Bun.main.startsWith("/$bunfs/") ? [process.execPath] : [process.execPath, Bun.main];
}

/**
 * Queues a restart and hands it to a detached supervisor, returning as soon as
 * the supervisor is spawned so the caller may be killed by the restart itself.
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
	const argv = supervisorArgv(platform, worker, options.home, receipt.id, options.binDir);
	let child: Bun.Subprocess;
	try {
		child = Bun.spawn(argv, {
			detached: true,
			stdin: "ignore",
			stdout: "ignore",
			stderr: "ignore",
			env: {
				...(options.env ?? process.env),
				GAJAEWAY_HOME: options.home,
				...(options.binDir === undefined ? {} : { GAJAEWAY_BIN_DIR: options.binDir }),
			},
		});
	} catch (error) {
		// Nothing will ever pick this receipt up: leaving it `queued` reads as a
		// restart still on its way. A host without a systemd user manager (a
		// container) has no `systemd-run`, and its restart belongs to whatever
		// supervises that host.
		receipt.state = "failed";
		receipt.finishedAt = new Date().toISOString();
		receipt.detail = `supervisor ${argv[0]} could not be started (${error instanceof Error ? error.message : String(error)}); without a service manager here, restart through the host's own supervisor`;
		for (const step of receipt.steps) step.result = "skipped";
		await writeReceipt(options.home, receipt);
		throw new Error(`restart-stack ${receipt.id}: failed: ${receipt.detail}`);
	}
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
	binDir?: string,
): string[] {
	if (platform === "darwin") return [...worker];
	return [
		"systemd-run",
		"--user",
		"--collect",
		"--quiet",
		`--unit=gajaeway-restart-stack-${id}`,
		`--setenv=GAJAEWAY_HOME=${home}`,
		...(binDir === undefined ? [] : [`--setenv=GAJAEWAY_BIN_DIR=${binDir}`]),
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
