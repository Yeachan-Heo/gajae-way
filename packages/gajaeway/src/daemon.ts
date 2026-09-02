import { join } from "node:path";
import { startDiscordAdapter } from "@gajaeway/adapter-discord";
import { startTelegramAdapter } from "@gajaeway/adapter-telegram";
import { type AdminServer, jsonlAuditLog, startAdminServer } from "@gajaeway/admin";
import {
	type BrokerSupervisorDependencies,
	bootGatewayFromConfig,
	type GatewayServer,
	gatewayHome,
	type LocalGatewayPort,
	loadConfig,
	sanitizeDiagnostic,
} from "@gajaeway/gateway";
import { adapterInputs, adminEvents, requestAfterOpen } from "./adapters";
import { DaemonLock, DaemonLockRefusalError, defaultDaemonLockPorts } from "./lock";
import { AdapterSupervisor } from "./supervisor";

type ConfigOverrides = NonNullable<Parameters<typeof loadConfig>[0]>["overrides"];

const FORCE_STOP_TIMEOUT_MS = 30_000;

export interface RunDaemonOptions {
	readonly home?: string;
	readonly overrides?: ConfigOverrides;
	readonly broker?: BrokerSupervisorDependencies;
	/** `0` is deliberately a test-only direct seam; environment parsing never accepts it. */
	readonly adminPort?: number;
	readonly exit?: (code: number) => void;
	readonly log?: (line: string) => void;
}

interface AdminLifecycle {
	readonly server: Pick<AdminServer, "stop">;
	readonly port: Pick<LocalGatewayPort, "close">;
}

interface SupervisorLifecycle {
	stop(): Promise<void>;
	stopAdapters(): Promise<void>;
}

export interface DaemonOptions {
	readonly lock: Pick<DaemonLock, "release">;
	readonly exit?: (code: number) => void;
	readonly log?: (line: string) => void;
	/** Test seam. Production uses the fixed thirty-second composite deadline. */
	readonly forceStopTimeoutMs?: number;
}

/**
 * Partial-owner lifecycle. It is created immediately after lock acquisition, so
 * a signal during config loading still has an owner that can release the lock.
 */
export class Daemon {
	readonly #lock: Pick<DaemonLock, "release">;
	readonly #exit: (code: number) => void;
	readonly #log: (line: string) => void;
	readonly #forceStopTimeoutMs: number;
	readonly #abort = new AbortController();
	readonly stopped: Promise<void>;
	readonly #resolveStopped: () => void;
	#gateway: Pick<GatewayServer, "stop"> | undefined;
	#admin: AdminLifecycle | undefined;
	#supervisor: SupervisorLifecycle | undefined;
	#stopPromise: Promise<void> | undefined;
	#requestedStatus = 0;
	#ready = false;
	#signalHandler: (() => void) | undefined;

	constructor(options: DaemonOptions) {
		this.#lock = options.lock;
		this.#exit = options.exit ?? ((code) => process.exit(code));
		this.#log = options.log ?? ((line) => console.error(line));
		this.#forceStopTimeoutMs = options.forceStopTimeoutMs ?? FORCE_STOP_TIMEOUT_MS;
		let resolve!: () => void;
		this.stopped = new Promise<void>((done) => {
			resolve = done;
		});
		this.#resolveStopped = resolve;
	}

	get signal(): AbortSignal {
		return this.#abort.signal;
	}

	get requestedStatus(): number {
		return this.#requestedStatus;
	}

	setGateway(gateway: Pick<GatewayServer, "stop">): void {
		this.#gateway = gateway;
	}

	setAdmin(server: Pick<AdminServer, "stop">, port: Pick<LocalGatewayPort, "close">): void {
		this.#admin = { server, port };
	}

	setSupervisor(supervisor: SupervisorLifecycle): void {
		this.#supervisor = supervisor;
	}

	markReady(): void {
		this.#ready = true;
	}

	installSignalHandlers(): void {
		if (this.#signalHandler) return;
		const handler = () => {
			// A signal while boot is still assembling resources is a failed boot, not
			// an ordinary clean stop of a running daemon.
			if (!this.#ready) this.requestExit(1);
			void this.stop("signal received");
		};
		this.#signalHandler = handler;
		process.once("SIGINT", handler);
		process.once("SIGTERM", handler);
	}

	assertBoot(phase: string): void {
		if (!this.signal.aborted) return;
		this.#log(`boot_aborted phase=${phase}`);
		throw new BootAbortedError(phase);
	}

	requestExit(code: number): void {
		const normalized = Number.isFinite(code) ? Math.max(0, Math.trunc(code)) : 1;
		this.#requestedStatus = Math.max(this.#requestedStatus, normalized);
		if (this.#requestedStatus > 0) process.exitCode = this.#requestedStatus;
	}

	stop(reason = "shutdown requested"): Promise<void> {
		if (this.#stopPromise) return this.#stopPromise;
		let resolve!: () => void;
		let reject!: (reason?: unknown) => void;
		const stopping = new Promise<void>((done, fail) => {
			resolve = done;
			reject = fail;
		});
		this.#stopPromise = stopping;
		this.#abort.abort();
		void this.#stop(reason).then(resolve, reject);
		return stopping;
	}

	async #stop(reason: string): Promise<void> {
		const forceTimer = setTimeout(() => {
			this.#log("gajaeway daemon stop forced");
			this.#exit(this.#requestedStatus || 1);
		}, this.#forceStopTimeoutMs);
		forceTimer.unref?.();
		this.#log(`gajaeway daemon stopping reason=${reason}`);
		try {
			await this.#safeShutdown("supervisor", async () => await this.#supervisor?.stop());
			await Promise.all([
				this.#safeShutdown("admin", async () => await this.#stopAdmin()),
				this.#safeShutdown("adapters", async () => await this.#supervisor?.stopAdapters()),
			]);
			await this.#safeShutdown("gateway", async () => await this.#gateway?.stop(reason));
			await this.#safeShutdown("lock", async () => await this.#lock.release());
		} finally {
			clearTimeout(forceTimer);
			this.#removeSignalHandlers();
			process.exitCode = this.#requestedStatus;
			this.#log(`gajaeway daemon stopped status=${this.#requestedStatus}`);
			this.#resolveStopped();
		}
	}

	async #stopAdmin(): Promise<void> {
		const admin = this.#admin;
		if (!admin) return;
		try {
			await admin.server.stop();
		} finally {
			admin.port.close();
		}
	}

	async #safeShutdown(phase: string, work: () => Promise<void>): Promise<void> {
		try {
			await work();
		} catch (error) {
			this.requestExit(1);
			this.#log(`gajaeway daemon shutdown_error phase=${phase} error=${diagnostic(error)}`);
		}
	}

	#removeSignalHandlers(): void {
		if (!this.#signalHandler) return;
		process.off("SIGINT", this.#signalHandler);
		process.off("SIGTERM", this.#signalHandler);
		this.#signalHandler = undefined;
	}
}

/** Composes the single in-process gateway, admin console, and enabled adapters. */
export async function runDaemon(options: RunDaemonOptions = {}): Promise<Daemon> {
	const home = options.home ?? gatewayHome();
	const log = options.log ?? ((line: string) => console.error(line));
	const lock = await DaemonLock.acquire(home, { ...defaultDaemonLockPorts(), log });
	const daemon = new Daemon({ lock, exit: options.exit, log });
	daemon.installSignalHandlers();
	log(`gajaeway daemon starting pid=${process.pid} home=${home}`);

	try {
		const config = await loadConfig({ home, overrides: options.overrides });
		daemon.assertBoot("config");
		const inputs = await adapterInputs(config, home);
		daemon.assertBoot("config");

		const gateway = await bootGatewayFromConfig(config, {
			broker: options.broker,
			shutdown: async (reason) => await daemon.stop(reason),
		});
		if (daemon.signal.aborted) {
			await gateway.stop("boot aborted");
			daemon.assertBoot("gateway");
		}
		daemon.setGateway(gateway);
		daemon.assertBoot("gateway");

		const localAdminPort = gateway.attach("admin");
		let resolveOpened!: (value: unknown | PromiseLike<unknown>) => void;
		let rejectOpened!: (reason?: unknown) => void;
		const openedReady = new Promise<unknown>((resolve, reject) => {
			resolveOpened = resolve;
			rejectOpened = reject;
		});
		void openedReady.catch(() => {});
		const admin = startAdminServer({
			request: requestAfterOpen(localAdminPort, openedReady),
			events: adminEvents(localAdminPort),
			auditLog: jsonlAuditLog(join(home, "admin-audit.jsonl")),
			port: options.adminPort ?? adminPortFromEnvironment(),
		});
		daemon.setAdmin(admin, localAdminPort);
		const opened = localAdminPort.open();
		void opened.then(resolveOpened, rejectOpened);
		await opened;
		daemon.assertBoot("admin");

		const supervisor = new AdapterSupervisor({
			server: gateway,
			log,
			escalate: async (name) => {
				daemon.requestExit(1);
				await daemon.stop(`adapter_escalated ${name}`);
			},
		});
		daemon.setSupervisor(supervisor);
		const adapters: string[] = [];
		if (inputs.discord) {
			adapters.push("discord");
			await supervisor.start(
				"discord",
				async (generation) =>
					await startDiscordAdapter(inputs.discord as NonNullable<typeof inputs.discord>, generation.port, generation),
			);
		}
		if (inputs.telegram) {
			adapters.push("telegram");
			await supervisor.start(
				"telegram",
				async (generation) =>
					await startTelegramAdapter(
						inputs.telegram as NonNullable<typeof inputs.telegram>,
						generation.port,
						home,
						generation,
					),
			);
		}
		daemon.assertBoot("adapters");
		daemon.markReady();
		log(`gajaeway daemon ready pid=${process.pid} home=${home} adapters=[${adapters.join(",")}] admin=${admin.url}`);
		return daemon;
	} catch (error) {
		daemon.requestExit(1);
		await daemon.stop(`boot failed: ${diagnostic(error)}`);
		throw error;
	}
}

/** Matches the historical admin binary's environment contract exactly. */
export function adminPortFromEnvironment(env: NodeJS.ProcessEnv = process.env): number {
	const raw = env.GAJAEWAY_ADMIN_PORT;
	if (raw === undefined) return 8788;
	const port = Number(raw);
	if (!Number.isInteger(port) || port < 1 || port > 65_535) {
		throw new Error(`GAJAEWAY_ADMIN_PORT must be a port number, got ${JSON.stringify(raw)}`);
	}
	return port;
}

export function isDaemonLockRefusal(error: unknown): error is DaemonLockRefusalError {
	return error instanceof DaemonLockRefusalError;
}

class BootAbortedError extends Error {
	constructor(phase: string) {
		super(`boot aborted during ${phase}`);
		this.name = "BootAbortedError";
	}
}

function diagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error";
}
