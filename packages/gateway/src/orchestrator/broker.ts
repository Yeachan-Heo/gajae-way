import { chmod, cp, mkdir, open, readdir, readFile, rename, rm, unlink, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import type { CliResult, CliRunner } from "@gajaeway/subsession";
import { sanitizeDiagnostic } from "./rebind";

/** The Stage 0 capability report was run successfully on this runtime floor. */
export const MIN_GJC_VERSION = "0.15.6";
/** Structural marker the health/capability probe requires from `sdk session list`. */
const HEALTH_PROBE_SESSION_ID = "00000000-0000-4000-8000-000000000000";
const SESSION_LIST_MARKER = "sessions";
export const DEFAULT_PERSONA_HOST_ID = "persona";

/** Process seam for gjc command execution; the gateway never spawns a session host itself. */
export type SpawnFn = typeof Bun.spawn;

/** A command seam for preflight and the broker-bound SDK CLI. */
export type GjcCommandRunner = CliRunner;

export interface BrokerRestartBackoff {
	readonly initialMs?: number;
	readonly maxMs?: number;
}

export interface BrokerHealthContext {
	readonly agentDir: string;
	readonly cli: CliRunner;
	/** Endpoint discovery the gjc daemon publishes at `<agentDir>/sdk/broker.json`. */
	readonly discoveryPath: string;
	readonly isPidAlive: PidAliveProbe;
	readonly timeoutMs: number;
	/** Reports a routed-but-rejected probe answer; liveness is unaffected. */
	readonly onApplicationError?: (code: string) => void;
}

export type BrokerHealthProbe = (context: BrokerHealthContext) => boolean | Promise<boolean>;
export type PidAliveProbe = (pid: number) => boolean | Promise<boolean>;
export type BrokerGenerationListener = (generation: number) => void;

export interface BrokerSupervisorOptions {
	/** Gateway-private root; state is nested beneath broker/<instanceId>. */
	readonly home: string;
	/** Durable database meta.instance_id, never an origin or a mutable display name. */
	readonly instanceId: string;
	/** One explicit persona host today; the lookup API keeps this boundary ready for more hosts. */
	readonly personaHostId?: string;
	/** The persona workspace is the working directory for agent-dir-bound gjc commands. */
	readonly cwd?: string;
	/**
	 * Explicit agent directory (tests/tooling). Production always uses the
	 * instance-private `<home>/broker/<instanceId>/agent`: pre-cutover stores
	 * written by older gjc are not readable by the current runtime, so they are
	 * never adopted; every origin binds a fresh session on first use.
	 */
	readonly agentDir?: string;
	/**
	 * Operator SSOT for provider/model configuration (`models.yml`,
	 * `model-presets/`, `config.yml`). Defaults to `~/.gjc/agent`. Seeded into the
	 * private agent dir on every start; set to `null` to disable seeding (tests).
	 */
	readonly ssotAgentDir?: string | null;
	/** Process seam for gjc command execution; production uses Bun.spawn bound to Bun. */
	readonly spawn?: SpawnFn;
	/** Command seam used by preflight and CliRunner; production spawns gjc commands directly. */
	readonly command?: GjcCommandRunner;
	/** Health seam; production probes the daemon's published WebSocket endpoint (no process spawn). */
	readonly healthProbe?: BrokerHealthProbe;
	/** Stale-lock proof seam. */
	readonly isPidAlive?: PidAliveProbe;
	readonly healthIntervalMs?: number;
	readonly healthProbeTimeoutMs?: number;
	readonly readinessAttempts?: number;
	readonly readinessDelayMs?: number;
	readonly restartBackoff?: BrokerRestartBackoff;
	readonly log?: (line: string) => void;
}

/** Dependencies accepted by boot; home, instance identity, and cwd are gateway-owned facts. */
export type BrokerSupervisorDependencies = Omit<BrokerSupervisorOptions, "home" | "instanceId" | "cwd">;

export interface PersonaBroker {
	readonly personaHostId: string;
	readonly agentDir: string;
	readonly generation: number;
	readonly cli: CliRunner;
}

type ActiveBroker = {
	readonly generation: number;
};

type LockRecord = {
	readonly pid: number;
	readonly generation: number;
};

const DEFAULT_HEALTH_INTERVAL_MS = 5_000;
const DEFAULT_HEALTH_PROBE_TIMEOUT_MS = 30_000;
/** Consecutive failed periodic probes before a generation is fenced. A single slow probe under load must not retire every live turn. */
const HEALTH_FAILURE_STRIKES = 3;
/** Consecutive readiness attempts that find a live-looking endpoint failing the application probe before the daemon is retired. */
const WEDGED_DAEMON_STRIKES = 3;
/** Concurrent `gjc sdk` invocations per gateway; more only multiplies daemon spawn races. */
const MAX_CONCURRENT_CLI = 4;
/** How long a non-probe invocation waits for a fenced generation to recover before failing. */
const BROKER_WAIT_MS = 60_000;

export class GjcCliUnavailableError extends Error {
	readonly code = "broker_unavailable";
	constructor(message: string) {
		super(`gjc sdk request failed: broker_unavailable (${message})`);
	}
}
const DEFAULT_COMMAND_TIMEOUT_MS = 30_000;
// The first agent-dir-scoped command auto-starts gjc's own broker daemon; give
// its lifecycle launcher a moment to converge after the first successful probe.
// Injected test probes define readiness themselves and skip this grace.
const DEFAULT_STARTUP_STABILIZATION_MS = 500;
const DEFAULT_READINESS_ATTEMPTS = 20;
const DEFAULT_READINESS_DELAY_MS = 100;
const DEFAULT_RESTART_INITIAL_MS = 250;
const DEFAULT_RESTART_MAX_MS = 10_000;

/**
 * Session hosting is gjc's own concern: the first agent-dir-scoped `gjc sdk`
 * command auto-starts a per-agent-dir broker daemon that owns session host
 * children. The gateway therefore never spawns a host process; it isolates the
 * persona broker by owning a PRIVATE agent dir and supervises that daemon by
 * observing it (health, generation fencing), not by owning its process. Stage 0
 * (artifacts/p2b-issue92-capability-report.md) reached this daemon through its
 * then-public `broker-internal` entrypoint; gjc >= 0.16.0 no longer exposes it
 * on the CLI surface, which is why observation is the only portable contract.
 */
export function brokerHealthArgs(): readonly string[] {
	// Exact-empty lookup: proves the broker can execute session.list while
	// guaranteeing a one-row-or-empty result, so it can NEVER allocate a
	// continuation cursor regardless of how many sessions exist. The old `{}`
	// probe leaked one cursor every 5s after the index exceeded 100 sessions and
	// exhausted the 32-slot pool in under three minutes.
	return [
		"sdk",
		"session",
		"raw",
		"global",
		"--op",
		"session.list",
		"--json-input",
		JSON.stringify({ resolveSessionId: HEALTH_PROBE_SESSION_ID }),
	];
}

/** gjc's own discovery TTL: a heartbeat older than this means the daemon is gone even if the file remains. */
export const BROKER_HEARTBEAT_TTL_MS = 15_000;

export type BrokerDiscovery = {
	readonly pid: number;
	readonly url: string;
	readonly token: string;
	readonly heartbeatAt: number;
};

/**
 * Parses `<agentDir>/sdk/broker.json` with the same structural rules gjc's own
 * client applies (discovery.ts readBrokerDiscovery): loopback host, protocol 3,
 * live pid, fresh heartbeat. Anything else is "no daemon", never an exception.
 */
export async function readBrokerDiscovery(
	discoveryPath: string,
	isPidAlive: PidAliveProbe,
	now = Date.now(),
	ttlMs = BROKER_HEARTBEAT_TTL_MS,
): Promise<BrokerDiscovery | undefined> {
	let raw: unknown;
	try {
		raw = JSON.parse(await readFile(discoveryPath, "utf8"));
	} catch {
		return undefined;
	}
	if (typeof raw !== "object" || raw === null) return undefined;
	const d = raw as Record<string, unknown>;
	if (
		d.protocolVersion !== 3 ||
		d.host !== "127.0.0.1" ||
		typeof d.url !== "string" ||
		!isLoopbackWebSocketUrl(d.url) ||
		typeof d.token !== "string" ||
		d.token.length === 0 ||
		typeof d.pid !== "number" ||
		!Number.isSafeInteger(d.pid) ||
		d.pid <= 0 ||
		typeof d.heartbeatAt !== "number" ||
		!Number.isFinite(d.heartbeatAt)
	)
		return undefined;
	if (now - d.heartbeatAt > ttlMs) return undefined;
	if (!(await isPidAlive(d.pid))) return undefined;
	return { pid: d.pid, url: d.url, token: d.token, heartbeatAt: d.heartbeatAt };
}

/**
 * Structural loopback check. A string prefix test would accept
 * `ws://127.0.0.1:80@evil.example` (hostname evil.example) and ship the token
 * off-host; only a parsed URL with the exact loopback hostname, an explicit
 * port, no credentials, and no path/query/fragment is a broker endpoint.
 */
export function isLoopbackWebSocketUrl(value: string): boolean {
	let url: URL;
	try {
		url = new URL(value);
	} catch {
		return false;
	}
	return (
		url.protocol === "ws:" &&
		url.hostname === "127.0.0.1" &&
		url.port !== "" &&
		url.username === "" &&
		url.password === "" &&
		(url.pathname === "" || url.pathname === "/") &&
		url.search === "" &&
		url.hash === ""
	);
}

/** The request the probe sends over the authenticated socket; a real read-only broker operation, not just a hello. */
const PROBE_REQUEST_ID = "gajaeway-health";

/**
 * Credential-bound liveness AND request-path readiness without a process
 * spawn: connect to the published endpoint, require the daemon's protocol-3
 * `broker_hello`, then issue `session.list` (the same read-only operation the
 * CLI probe used) and require a well-formed `broker_response` for it. A
 * daemon whose accept/auth loop is alive but whose session Router is wedged
 * answers the hello and then fails or stalls the request; that is unhealthy.
 * Costs ~1ms end to end; a `gjc sdk session list` spawn costs ~1s of CPU.
 */
export function probeBrokerEndpoint(
	discovery: BrokerDiscovery,
	timeoutMs: number,
	/** Reports a routed-but-rejected answer. Liveness is unaffected; the code is worth logging. */
	onApplicationError?: (code: string) => void,
): Promise<boolean> {
	return new Promise<boolean>((resolve) => {
		let socket: WebSocket;
		try {
			const url = new URL(discovery.url);
			url.searchParams.set("token", discovery.token);
			socket = new WebSocket(url);
		} catch {
			resolve(false);
			return;
		}
		let settled = false;
		let greeted = false;
		const settle = (value: boolean) => {
			if (settled) return;
			settled = true;
			clearTimeout(timer);
			resolve(value);
			try {
				socket.close();
			} catch {
				// best effort: the daemon drops the socket on its own
			}
		};
		const timer = setTimeout(() => settle(false), timeoutMs);
		socket.addEventListener("message", (event) => {
			let frame: Record<string, unknown>;
			try {
				const parsed: unknown = JSON.parse(String(event.data));
				if (typeof parsed !== "object" || parsed === null) return settle(false);
				frame = parsed as Record<string, unknown>;
			} catch {
				return settle(false);
			}
			if (!greeted) {
				if (frame.type !== "broker_hello" || frame.protocolVersion !== 3) return settle(false);
				greeted = true;
				try {
					socket.send(
						JSON.stringify({
							type: "broker_request",
							id: PROBE_REQUEST_ID,
							operation: "session.list",
							// Exact-empty lookup is structural health evidence without ever
							// allocating a continuation cursor, even with millions of sessions.
							input: { resolveSessionId: HEALTH_PROBE_SESSION_ID },
						}),
					);
				} catch {
					settle(false);
				}
				return;
			}
			if (frame.type !== "broker_response" || frame.id !== PROBE_REQUEST_ID) return settle(false);
			// ANY answer to a routed request proves what this probe exists to prove:
			// the daemon accepted the connection, authenticated it, parsed the frame,
			// reached the session Router, and replied. `ok:false` means it rejected
			// OUR INPUT - that is a live daemon disagreeing, not a dead one. Treating
			// it as death is what let a self-inflicted cursor leak escalate into
			// killing a healthy broker mid-turn (live, 2026-09-03). Only silence
			// (timeout), a transport failure, or a malformed/unmatched frame is a
			// failure; a wedged Router never answers and still trips the timeout.
			if (frame.ok !== true) {
				const detail = frame.error;
				const code = typeof detail === "object" && detail !== null ? (detail as { code?: unknown }).code : undefined;
				onApplicationError?.(typeof code === "string" ? code : "unknown");
			}
			settle(true);
		});
		socket.addEventListener("error", () => settle(false));
		socket.addEventListener("close", () => settle(false));
	});
}

/**
 * Default health probe: discovery file + live pid + fresh heartbeat + hello
 * + a real read-only broker request. No `gjc` process is spawned, so the
 * probe's wall time does not scale with host load the way a Bun cold start does.
 */
export async function probeBrokerDiscovery(context: BrokerHealthContext): Promise<boolean> {
	const discovery = await readBrokerDiscovery(context.discoveryPath, context.isPidAlive);
	if (!discovery) return false;
	return await probeBrokerEndpoint(discovery, context.timeoutMs, context.onApplicationError);
}

/** A session-list envelope is healthy only when it is a structurally valid `ok` reply. */
export function isHealthySessionList(result: CliResult): boolean {
	if (result.exitCode !== 0) return false;
	try {
		const parsed = JSON.parse(result.stdout) as { ok?: unknown; result?: unknown };
		if (parsed.ok !== true) return false;
		const body = parsed.result;
		return (
			typeof body === "object" && body !== null && Array.isArray((body as Record<string, unknown>)[SESSION_LIST_MARKER])
		);
	} catch {
		return false;
	}
}

/**
 * Enforces the Stage 0 runtime floor before the gateway accepts any connection:
 * a semantic version at or above the verified floor, plus proof that the `sdk`
 * surface answers a structurally valid session-list envelope (never trusting a
 * zero exit code alone, which generic help output also produces).
 */
export async function preflightGjcRuntime(
	run: GjcCommandRunner,
	minimumVersion = MIN_GJC_VERSION,
	sdk?: GjcCommandRunner,
): Promise<void> {
	const version = await run(["--version"], { timeoutMs: DEFAULT_HEALTH_PROBE_TIMEOUT_MS });
	if (version.exitCode !== 0) {
		throw new Error(`gjc runtime preflight failed: gjc --version exited ${version.exitCode}`);
	}
	const found = parseGjcVersion(version.stdout || version.stderr);
	const minimum = parseGjcVersion(minimumVersion);
	if (!found) {
		throw new Error("gjc runtime preflight failed: gjc --version did not report a semantic version");
	}
	if (!minimum) throw new Error(`invalid configured gjc minimum version ${minimumVersion}`);
	if (compareVersions(found, minimum) < 0) {
		throw new Error(`gjc runtime preflight failed: requires gjc >= ${minimumVersion}; found ${formatVersion(found)}`);
	}
	if (sdk) {
		const capability = await sdk([...brokerHealthArgs()], { timeoutMs: DEFAULT_COMMAND_TIMEOUT_MS });
		if (!isHealthySessionList(capability)) {
			throw new Error(
				`gjc runtime preflight failed: \`gjc sdk session list\` did not answer a valid session-list envelope (exit ${capability.exitCode})`,
			);
		}
	}
}

/**
 * Supervises the gateway's private persona broker: it owns the agent dir and its
 * lock, observes the gjc-managed daemon's health, and publishes a generation
 * every time the daemon is observed to come back after a failure. It deliberately
 * exposes only generation and a broker-bound CLI: session recovery remains with
 * the per-origin actor, never this supervisor.
 */
export class BrokerSupervisor implements PersonaBroker {
	readonly personaHostId: string;
	readonly stateDir: string;
	readonly agentDir: string;
	/** gjc's own daemon publishes its endpoint discovery here. */
	readonly discoveryPath: string;
	readonly lockPath: string;
	readonly cli: CliRunner;

	readonly #spawn: SpawnFn;
	readonly #command: GjcCommandRunner;
	readonly #healthProbe: BrokerHealthProbe;
	readonly #ssotAgentDir: string | undefined;
	readonly #isPidAlive: PidAliveProbe;
	readonly #cwd: string;
	readonly #healthIntervalMs: number;
	readonly #healthProbeTimeoutMs: number;
	#healthStrikes = 0;
	readonly #readinessAttempts: number;
	readonly #readinessDelayMs: number;
	readonly #restartInitialMs: number;
	readonly #restartMaxMs: number;
	readonly #startupStabilizationMs: number;
	/** Only the default endpoint probe relies on a CLI call to auto-start gjc's daemon. */
	readonly #spawnsDaemon: boolean;
	readonly #log: (line: string) => void;
	readonly #listeners = new Set<BrokerGenerationListener>();

	#lock: Awaited<ReturnType<typeof open>> | undefined;
	#active: ActiveBroker | undefined;
	#generation = 0;
	#restartFailures = 0;
	#healthTimer: ReturnType<typeof setInterval> | undefined;
	#restartTimer: ReturnType<typeof setTimeout> | undefined;
	#healthCheckInFlight = false;
	#launching = false;
	#started = false;
	#stopping = false;
	#startPromise: Promise<void> | undefined;
	#stopPromise: Promise<void> | undefined;

	constructor(options: BrokerSupervisorOptions) {
		if (!/^[A-Za-z0-9._-]+$/.test(options.instanceId)) {
			throw new Error("broker instance id must contain only letters, numbers, dots, underscores, or hyphens");
		}
		this.personaHostId = options.personaHostId ?? DEFAULT_PERSONA_HOST_ID;
		if (!/^[A-Za-z0-9._-]+$/.test(this.personaHostId)) {
			throw new Error("broker persona host id must contain only letters, numbers, dots, underscores, or hyphens");
		}
		this.stateDir = join(options.home, "broker", options.instanceId);
		this.agentDir = options.agentDir ?? join(this.stateDir, "agent");
		this.discoveryPath = join(this.agentDir, "sdk", "broker.json");
		this.lockPath = join(this.stateDir, "broker.lock");
		this.#cwd = options.cwd ?? options.home;
		this.#ssotAgentDir = options.ssotAgentDir === null ? undefined : (options.ssotAgentDir ?? defaultSsotAgentDir());
		this.#spawn = options.spawn ?? Bun.spawn.bind(Bun);
		this.#command = options.command ?? ((args, commandOptions) => this.#runCommand(args, commandOptions));
		this.cli = (args, commandOptions) => this.#command(bindAgentDir(args, this.agentDir), commandOptions);
		this.#healthProbe = options.healthProbe ?? probeBrokerDiscovery;
		this.#startupStabilizationMs = options.healthProbe ? 0 : DEFAULT_STARTUP_STABILIZATION_MS;
		this.#spawnsDaemon = options.healthProbe === undefined;
		this.#isPidAlive = options.isPidAlive ?? defaultPidAlive;
		this.#healthIntervalMs = positiveInteger(options.healthIntervalMs, DEFAULT_HEALTH_INTERVAL_MS, "healthIntervalMs");
		this.#healthProbeTimeoutMs = positiveInteger(
			options.healthProbeTimeoutMs,
			DEFAULT_HEALTH_PROBE_TIMEOUT_MS,
			"healthProbeTimeoutMs",
		);
		this.#readinessAttempts = positiveInteger(
			options.readinessAttempts,
			DEFAULT_READINESS_ATTEMPTS,
			"readinessAttempts",
		);
		this.#readinessDelayMs = nonNegativeInteger(
			options.readinessDelayMs,
			DEFAULT_READINESS_DELAY_MS,
			"readinessDelayMs",
		);
		this.#restartInitialMs = nonNegativeInteger(
			options.restartBackoff?.initialMs,
			DEFAULT_RESTART_INITIAL_MS,
			"restartBackoff.initialMs",
		);
		this.#restartMaxMs = nonNegativeInteger(
			options.restartBackoff?.maxMs,
			DEFAULT_RESTART_MAX_MS,
			"restartBackoff.maxMs",
		);
		if (this.#restartMaxMs < this.#restartInitialMs) {
			throw new Error("restartBackoff.maxMs must be greater than or equal to restartBackoff.initialMs");
		}
		this.#log = options.log ?? ((line) => console.error(line));
	}

	/** Takes ownership of the private agent dir and does not resolve until its daemon answers healthy. */
	async start(): Promise<void> {
		if (this.#started) return;
		if (this.#startPromise) return await this.#startPromise;
		this.#stopping = false;
		const start = this.#start();
		this.#startPromise = start;
		try {
			await start;
			this.#started = true;
		} finally {
			this.#startPromise = undefined;
		}
	}

	/** Runs the boot-time capability gate against the private agent dir. */
	async preflight(): Promise<void> {
		// Version is the only CLI preflight. #start immediately performs the real
		// application-level session.list probe against the private broker endpoint;
		// spawning another SDK CLI here duplicated that check and could time out
		// before an already-healthy daemon was observed.
		await preflightGjcRuntime(this.#command, MIN_GJC_VERSION);
	}

	/** The current daemon generation; it starts at 1 and increases every time the daemon is observed to recover. */
	get generation(): number {
		return this.#generation;
	}

	/** Explicit one-host lookup; callers cannot accidentally select a per-origin broker. */
	brokerFor(personaHostId: string): PersonaBroker {
		if (personaHostId !== this.personaHostId) {
			throw new Error(`no broker is registered for persona host ${personaHostId}`);
		}
		return this;
	}

	/** Consumers may fence work on a host replacement without handing recovery policy to this class. */
	onGeneration(listener: BrokerGenerationListener): () => void {
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}

	/** Stops observation and releases ownership; the gjc daemon's own lifecycle is not the gateway's to end. */
	async stop(): Promise<void> {
		if (this.#stopPromise) return await this.#stopPromise;
		this.#stopping = true;
		const starting = this.#startPromise;
		const stop = (async () => {
			if (starting) await starting.catch(() => {});
			await this.#stop();
		})();
		this.#stopPromise = stop;
		try {
			await stop;
		} finally {
			this.#stopPromise = undefined;
		}
	}

	async #start(): Promise<void> {
		await this.#acquireLock();
		try {
			await mkdir(this.agentDir, { recursive: true, mode: 0o700 });
			await chmod(this.agentDir, 0o700);
			await reapAgentDir(this.agentDir, this.#log, this.#isPidAlive);
			if (this.#ssotAgentDir) await seedAgentDirFromSsot(this.#ssotAgentDir, this.agentDir, this.#log);
			await ensureSteeringDefaults(this.agentDir);
			await this.#launchGeneration();
		} catch (error) {
			this.#stopping = true;
			if (!this.#active) await this.#releaseLock();
			throw error;
		}
	}

	async #stop(): Promise<void> {
		this.#clearHealthTimer();
		if (this.#restartTimer) {
			clearTimeout(this.#restartTimer);
			this.#restartTimer = undefined;
		}
		this.#active = undefined;
		await this.#releaseLock();
		this.#started = false;
	}

	async #acquireLock(): Promise<void> {
		const root = join(this.stateDir, "..");
		await mkdir(root, { recursive: true, mode: 0o700 });
		await chmod(root, 0o700);
		for (let attempt = 0; attempt < 3; attempt++) {
			await mkdir(this.stateDir, { recursive: true, mode: 0o700 });
			await chmod(this.stateDir, 0o700);
			try {
				this.#lock = await open(this.lockPath, "wx", 0o600);
				await this.#writeLock();
				return;
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
			}
			const stale = await this.#staleLockRecord();
			if (!stale) continue;
			const live = await this.#isPidAlive(stale.pid);
			if (live) {
				throw new Error(
					`broker ownership lock is held by live pid ${stale.pid} for gateway instance ${this.stateDir}; refusing to take over`,
				);
			}
			await this.#removeStaleState(stale);
		}
		throw new Error(`could not acquire broker ownership lock at ${this.lockPath}`);
	}

	/** Returns a stale candidate only when an existing lock contains a usable PID. */
	async #staleLockRecord(): Promise<LockRecord | undefined> {
		let raw: string;
		try {
			raw = await readFile(this.lockPath, "utf8");
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
			throw error;
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(raw);
		} catch {
			throw new Error(`broker ownership lock ${this.lockPath} is malformed; refusing stale cleanup`);
		}
		if (
			typeof parsed !== "object" ||
			parsed === null ||
			typeof (parsed as { pid?: unknown }).pid !== "number" ||
			!Number.isSafeInteger((parsed as { pid: number }).pid) ||
			(parsed as { pid: number }).pid <= 0 ||
			typeof (parsed as { generation?: unknown }).generation !== "number" ||
			!Number.isSafeInteger((parsed as { generation: number }).generation) ||
			(parsed as { generation: number }).generation < 0
		) {
			throw new Error(`broker ownership lock ${this.lockPath} is malformed; refusing stale cleanup`);
		}
		return parsed as LockRecord;
	}

	/**
	 * A dead PID proves only the old process and its endpoint/lock remnants are
	 * stale. The private agent directory carries durable broker authority and
	 * must survive takeover; recursive removal would erase sessions that AC4 is
	 * specifically meant to recover.
	 */
	async #removeStaleState(stale: LockRecord): Promise<void> {
		// Two reclaimers may race on the same dead lock. Rename (atomic) rather than
		// unlink: the loser's rename fails with ENOENT because the winner already
		// moved it, and a lock the winner has since re-created is never touched.
		const tombstone = `${this.lockPath}.stale-${stale.pid}-${process.pid}-${Date.now()}`;
		try {
			await rename(this.lockPath, tombstone);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
			throw error;
		}
		try {
			if ((await readFile(tombstone, "utf8")).includes(`"pid":${stale.pid}`)) await unlink(tombstone);
			else await rename(tombstone, this.lockPath);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		try {
			await unlink(join(this.stateDir, "broker.sock"));
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
	}

	async #writeLock(): Promise<void> {
		if (!this.#lock) throw new Error("broker ownership lock is not held");
		await writeFile(this.lockPath, `${JSON.stringify({ pid: process.pid, generation: this.#generation })}\n`, {
			mode: 0o600,
		});
		await chmod(this.lockPath, 0o600);
	}

	async #releaseLock(): Promise<void> {
		const lock = this.#lock;
		this.#lock = undefined;
		if (!lock) return;
		await lock.close();
		try {
			await unlink(this.lockPath);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
	}

	/** A generation begins when the agent-dir daemon is observed healthy; readiness launches it via one CLI call only when no live endpoint is published. */
	async #launchGeneration(): Promise<void> {
		this.#launching = true;
		const active: ActiveBroker = { generation: this.#generation + 1 };
		try {
			this.#active = active;
			await this.#awaitHealthy(active);
			if (this.#active !== active) throw new Error("broker observation was cancelled during readiness");
			// The public generation advances only once the daemon is OBSERVED healthy;
			// a failed recovery attempt never burns a generation number.
			this.#generation = active.generation;
			await this.#writeLock();
			this.#publishGeneration(active.generation);
			this.#startHealthTimer();
		} catch (error) {
			if (this.#active === active) this.#active = undefined;
			throw error;
		} finally {
			this.#launching = false;
		}
	}

	#probeContext(): BrokerHealthContext {
		return {
			agentDir: this.agentDir,
			cli: this.cli,
			discoveryPath: this.discoveryPath,
			isPidAlive: this.#isPidAlive,
			timeoutMs: this.#healthProbeTimeoutMs,
		};
	}

	async #awaitHealthy(active: ActiveBroker): Promise<void> {
		// A daemon that keeps publishing a fresh heartbeat but fails the
		// application probe (session Router wedged) would otherwise be probed
		// forever: readiness refuses to launch while discovery looks live. After
		// this many consecutive live-but-unhealthy attempts AGAINST THE SAME
		// daemon (pid+url) it is retired and the next attempt launches a
		// replacement. A newly published daemon starts from zero strikes.
		let liveButUnhealthy = 0;
		let struckIdentity: string | undefined;
		for (let attempt = 0; attempt < this.#readinessAttempts; attempt++) {
			if (this.#stopping) throw new Error("broker observation stopped during readiness");
			if (this.#active !== active) throw new Error("broker observation was replaced before readiness completed");
			try {
				if (!this.#spawnsDaemon) {
					if (await this.#healthProbe(this.#probeContext())) {
						if (this.#startupStabilizationMs > 0) await sleep(this.#startupStabilizationMs);
						return;
					}
				} else {
					// Read discovery ONCE and probe exactly that record, so the verdict
					// and the identity a strike is charged to can never diverge (a
					// daemon replaced mid-probe must not inherit its predecessor's
					// failures).
					const discovery = await readBrokerDiscovery(this.discoveryPath, this.#isPidAlive);
					if (
						discovery &&
						(await probeBrokerEndpoint(discovery, this.#healthProbeTimeoutMs, (code) =>
							this.#log(`broker_probe_application_error code=${code} (daemon is answering; not a liveness failure)`),
						))
					) {
						if (this.#startupStabilizationMs > 0) await sleep(this.#startupStabilizationMs);
						return;
					}
					const identity = discovery ? `${discovery.pid}|${discovery.url}` : undefined;
					if (identity !== struckIdentity) {
						liveButUnhealthy = 0;
						struckIdentity = identity;
					}
					if (discovery && ++liveButUnhealthy >= WEDGED_DAEMON_STRIKES) {
						this.#log(
							`broker_daemon_retired pid=${discovery.pid} reason=live_endpoint_failed_application_probe strikes=${liveButUnhealthy}`,
						);
						await this.#retireDaemon(discovery.pid);
						liveButUnhealthy = 0;
						struckIdentity = undefined;
					} else if (!discovery) {
						// The endpoint probe never spawns. gjc auto-starts its daemon on
						// the first agent-dir-scoped sdk command, so when no live discovery
						// exists one CLI call is the launch trigger; its envelope is not
						// the health verdict, the next endpoint probe is.
						const launch = await this.cli([...brokerHealthArgs()], { timeoutMs: DEFAULT_COMMAND_TIMEOUT_MS });
						if (!isHealthySessionList(launch))
							this.#log(`broker daemon launch command exited ${launch.exitCode} without a session-list envelope`);
					}
				}
			} catch (error) {
				this.#log(`broker health probe during startup failed: ${diagnostic(error)}`);
			}
			if (attempt + 1 < this.#readinessAttempts) await sleep(this.#readinessDelayMs);
		}
		throw new Error(`broker daemon did not become healthy after ${this.#readinessAttempts} probe(s)`);
	}

	/**
	 * Ends the private daemon process (and its hosts) so gjc can start a fresh
	 * one, and removes its stale discovery so readiness stops trusting it. The
	 * agent directory itself is never touched: sessions are durable and resume.
	 */
	async #retireDaemon(pid: number): Promise<void> {
		await reapAgentDir(this.agentDir, this.#log, this.#isPidAlive);
		if (await this.#isPidAlive(pid)) {
			try {
				process.kill(pid, "SIGKILL");
			} catch {
				// already gone
			}
		}
		try {
			await unlink(this.discoveryPath);
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
	}

	#startHealthTimer(): void {
		this.#clearHealthTimer();
		this.#healthTimer = setInterval(() => void this.#checkHealth(), this.#healthIntervalMs);
	}

	#clearHealthTimer(): void {
		if (!this.#healthTimer) return;
		clearInterval(this.#healthTimer);
		this.#healthTimer = undefined;
	}

	async #checkHealth(): Promise<void> {
		if (this.#healthCheckInFlight || this.#stopping) return;
		const active = this.#active;
		if (!active) return;
		this.#healthCheckInFlight = true;
		let healthy = false;
		try {
			healthy = await this.#healthProbe(this.#probeContext());
		} catch (error) {
			this.#log(`broker health probe failed: ${diagnostic(error)}`);
		} finally {
			this.#healthCheckInFlight = false;
		}
		if (healthy && !this.#stopping && this.#active === active) {
			// A completed periodic probe is the stable-health boundary that resets restart backoff.
			this.#restartFailures = 0;
			this.#healthStrikes = 0;
			return;
		}
		if (this.#stopping || this.#active !== active) return;
		this.#healthStrikes++;
		if (this.#healthStrikes < HEALTH_FAILURE_STRIKES) {
			this.#log(
				`broker health probe strike ${this.#healthStrikes}/${HEALTH_FAILURE_STRIKES}; generation ${active.generation} retained`,
			);
			return;
		}
		// The daemon is gjc-owned; a sustained probe failure fences this generation
		// and waits (with backoff) for the daemon to answer again, then publishes a new one.
		this.#clearHealthTimer();
		this.#active = undefined;
		this.#log(`broker daemon generation ${active.generation} failed health; awaiting recovery`);
		this.#scheduleRestart("broker health probe failed");
	}

	#scheduleRestart(reason: string): void {
		if (this.#stopping || this.#restartTimer) return;
		const multiplier = 2 ** Math.min(this.#restartFailures, 30);
		const delay = Math.min(this.#restartInitialMs * multiplier, this.#restartMaxMs);
		this.#restartFailures++;
		this.#log(`broker_restart generation=${this.#generation + 1} backoffMs=${delay} reason=${reason}`);
		this.#restartTimer = setTimeout(() => {
			this.#restartTimer = undefined;
			void this.#restart();
		}, delay);
	}

	async #restart(): Promise<void> {
		if (this.#stopping) return;
		try {
			await this.#launchGeneration();
		} catch (error) {
			if (this.#stopping) return;
			this.#log(`broker recovery observation failed: ${diagnostic(error)}`);
			if (!this.#active) this.#scheduleRestart("broker restart readiness failed");
		}
	}

	#publishGeneration(generation: number): void {
		for (const listener of this.#listeners) {
			try {
				listener(generation);
			} catch (error) {
				this.#log(`broker generation listener failed: ${diagnostic(error)}`);
			}
		}
	}

	/**
	 * Event-driven observation transport: one resident `gjc sdk serve --stdio
	 * --session <id>` relay per attached session. The relay forwards the host's
	 * live WebSocket frames to stdout the instant they are emitted. The gateway
	 * holds stdin open (the relay exits on stdin EOF); `write` lets the session
	 * channel (I4a) send `query_request` frames on the same connection, so
	 * connection-bound continuation cursors stay valid and no second process is
	 * spawned for a read.
	 */
	openStream(sessionId: string): { readonly lines: AsyncIterable<string>; write(line: string): void; close(): void } {
		const child = this.#spawn({
			cmd: ["gjc", "sdk", "serve", "--stdio", "--session", sessionId],
			cwd: this.#cwd,
			stdin: "pipe",
			stdout: "pipe",
			stderr: "ignore",
			env: brokerEnvironment(this.agentDir),
		});
		// No capability handshake here: gjc 0.16.0 gates tool_activity and
		// reasoning_summary behind a NEGOTIATED capability, and only its
		// websocket transport implements that negotiation - a `sdk serve --stdio`
		// client cannot declare one, so the host filters those frames for us
		// whatever we write to stdin (verified: zero tool_activity frames in a
		// full day of live traffic). The stream therefore carries lifecycle and
		// turn_stream frames only; chat.progress reports elapsed time, and any
		// counters it does have are estimated from finalized assistant text.
		// Real tool/token counters would require streaming over the broker
		// WebSocket instead of the stdio relay.
		let closed = false;
		const close = () => {
			if (closed) return;
			closed = true;
			try {
				child.kill();
			} catch {
				// best effort: the relay also exits on stdin close
			}
		};
		const write = (line: string) => {
			if (closed) throw new Error(`stream for ${sessionId} is closed`);
			const stdin = child.stdin as { write(chunk: string): unknown; flush?(): unknown };
			stdin.write(line);
			stdin.flush?.();
		};
		const lines = (async function* () {
			const reader = (child.stdout as ReadableStream<Uint8Array>).getReader();
			const decoder = new TextDecoder();
			let buffer = "";
			try {
				for (;;) {
					const { value, done } = await reader.read();
					if (done) break;
					buffer += decoder.decode(value, { stream: true });
					let newline = buffer.indexOf("\n");
					while (newline >= 0) {
						yield buffer.slice(0, newline);
						buffer = buffer.slice(newline + 1);
						newline = buffer.indexOf("\n");
					}
				}
				if (buffer.length > 0) yield buffer;
			} finally {
				reader.releaseLock();
			}
		})();
		return { lines, write, close };
	}

	/**
	 * Every `gjc sdk` invocation that cannot reach the daemon auto-spawns a
	 * detached broker. N concurrent invocations during an outage therefore race
	 * to spawn N brokers, wedge the lock tree with quarantine tombstones, and the
	 * daemon never comes back (live: 14 brokers on one agent dir). Two fences:
	 * a hard cap on concurrent invocations, and while no generation is active only
	 * the health probe runs; everything else waits for the observed recovery.
	 */
	#inflight = 0;
	readonly #cliQueue: Array<() => void> = [];
	/** The probe lane is separate: observation traffic saturating the shared cap must never delay a health verdict. */
	#probeInflight = false;
	readonly #probeQueue: Array<() => void> = [];

	async #acquireCliSlot(args: readonly string[]): Promise<boolean> {
		const isProbe = args.includes("list") && args.includes("--scope");
		if (isProbe) {
			if (this.#probeInflight) await new Promise<void>((resolve) => this.#probeQueue.push(resolve));
			this.#probeInflight = true;
			return true;
		}
		const deadline = Date.now() + BROKER_WAIT_MS;
		while (!this.#active && !this.#stopping && this.#generation > 0) {
			if (Date.now() >= deadline)
				throw new GjcCliUnavailableError("broker generation fenced; daemon has not recovered");
			await new Promise<void>((resolve) => setTimeout(resolve, 250));
		}
		if (this.#inflight >= MAX_CONCURRENT_CLI) await new Promise<void>((resolve) => this.#cliQueue.push(resolve));
		this.#inflight++;
		return false;
	}

	#releaseCliSlot(probe: boolean): void {
		if (probe) {
			this.#probeInflight = false;
			this.#probeQueue.shift()?.();
			return;
		}
		this.#inflight--;
		this.#cliQueue.shift()?.();
	}

	async #runCommand(args: readonly string[], options?: { readonly timeoutMs?: number }): Promise<CliResult> {
		const probe = await this.#acquireCliSlot(args);
		try {
			return await this.#runCommandUnfenced(args, options);
		} finally {
			this.#releaseCliSlot(probe);
		}
	}

	async #runCommandUnfenced(args: readonly string[], options?: { readonly timeoutMs?: number }): Promise<CliResult> {
		const agentDir = args.includes("--agent-dir") ? this.agentDir : undefined;
		const child = this.#spawn({
			cmd: ["gjc", ...args],
			cwd: this.#cwd,
			stdin: "ignore",
			stdout: "pipe",
			stderr: "pipe",
			// Broker-bound commands share the same private state while retaining provider variables.
			env: agentDir ? brokerEnvironment(agentDir) : (process.env as Record<string, string>),
		});
		return await collectCommand(child, options?.timeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS);
	}
}

/**
 * The persona turn model is "always steer, never interrupt the conversation":
 * every mid-turn message must reach the running turn. gjc's `steeringMode:
 * one-at-a-time` would queue steers, so it is pinned to `all`. `interruptMode`
 * is pinned to `wait`: gjc's `immediate` re-checks steers after every tool
 * call and can derail a tool chain mid-flight (upstream behavior), so steers
 * are folded in at turn boundaries instead. Other operator settings in
 * config.yml are left untouched.
 */
export const STEERING_DEFAULTS: Readonly<Record<string, string>> = { steeringMode: "all", interruptMode: "wait" };

/**
 * Boot-time reap of everything the previous gateway incarnation left behind in
 * ITS OWN private agent dir: gjc daemon/host/relay processes bound to that dir
 * and the lock tombstones a spawn stampede leaves (gajae-code#5198). Nothing
 * outside the private dir is touched, so the operator's own ~/.gjc/agent
 * daemon and sessions are never affected. Safe on every restart: sessions are
 * durable and resume through recovery.
 */
export async function reapAgentDir(
	agentDir: string,
	log: (line: string) => void,
	isPidAlive: (pid: number) => boolean | Promise<boolean>,
): Promise<void> {
	let killed = 0;
	try {
		const ps = Bun.spawnSync(["ps", "-Ao", "pid=,ppid=,args="]);
		const rows = ps.stdout
			.toString()
			.split("\n")
			.map((line) => line.trim().match(/^(\d+)\s+(\d+)\s+(.*)$/))
			.filter((m): m is RegExpMatchArray => m !== null)
			.map((m) => ({ pid: Number(m[1]), ppid: Number(m[2]), args: m[3] ?? "" }));
		const owned = new Set<number>();
		for (const row of rows)
			if (/gjc sdk broker-internal/.test(row.args) && row.args.includes(`--agent-dir ${agentDir}`)) owned.add(row.pid);
		for (const row of rows)
			if (/gjc sdk (session-host-internal|serve --stdio|session tail)/.test(row.args) && owned.has(row.ppid))
				owned.add(row.pid);
		for (const pid of owned) {
			if (pid === process.pid || !(await isPidAlive(pid))) continue;
			try {
				process.kill(pid, "SIGTERM");
				killed++;
			} catch {
				// already gone
			}
		}
		if (killed > 0) {
			await new Promise((resolve) => setTimeout(resolve, 2_000));
			for (const pid of owned)
				if (await isPidAlive(pid))
					try {
						process.kill(pid, "SIGKILL");
					} catch {
						// already gone
					}
		}
	} catch (error) {
		log(
			`broker_reap_processes_failed detail=${sanitizeDiagnostic(error instanceof Error ? error.message : String(error))}`,
		);
	}
	let tombstones = 0;
	const sdk = join(agentDir, "sdk");
	for (const [dir, pattern] of [
		[sdk, /^\.broker\.lock\.stale-|^broker-spawn\..*\.log$|^broker\.startup-failure\.json$|^broker\.lock$/],
		[join(sdk, "sessions"), /^index\.jsonl\.lock/],
	] as const) {
		let names: string[] = [];
		try {
			names = await readdir(dir);
		} catch {
			continue;
		}
		for (const name of names)
			if (pattern.test(name)) {
				await rm(join(dir, name), { recursive: true, force: true });
				tombstones++;
			}
	}
	if (killed > 0 || tombstones > 0)
		log(`broker_reaped agentDir=${agentDir} processes=${killed} tombstones=${tombstones}`);
}

export function defaultSsotAgentDir(): string {
	const home = process.env.HOME ?? homedir();
	return join(home, ".gjc", "agent");
}

/**
 * The operator's `~/.gjc/agent` is the single source of truth for provider and
 * model configuration. The gateway's private agent dir is isolation, not a
 * second place to edit config: on every start it is re-seeded from the SSOT
 * (`models.yml`, `model-presets/`, and `config.yml` operator keys), then the
 * turn-model steering keys are pinned on top. A missing SSOT `models.yml` is
 * a boot failure: the persona could otherwise bind sessions that cannot reach
 * any model provider.
 */
export async function seedAgentDirFromSsot(ssot: string, agentDir: string, log: (line: string) => void): Promise<void> {
	const modelsPath = join(ssot, "models.yml");
	let models: string;
	try {
		models = await readFile(modelsPath, "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT")
			throw new Error(
				`gjc runtime preflight failed: operator SSOT ${modelsPath} is missing; the persona broker cannot reach a model provider`,
			);
		throw error;
	}
	await writeFile(join(agentDir, "models.yml"), models, { mode: 0o600 });
	const presets = join(ssot, "model-presets");
	try {
		await cp(presets, join(agentDir, "model-presets"), { recursive: true, force: true });
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
	let operatorConfig = "";
	try {
		operatorConfig = await readFile(join(ssot, "config.yml"), "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
	const stripped = operatorConfig
		.split("\n")
		.filter((line) => !Object.keys(STEERING_DEFAULTS).some((key) => line.startsWith(`${key}:`)))
		.join("\n");
	await writeFile(join(agentDir, "config.yml"), `${stripped.replace(/\s+$/, "")}\n`, { mode: 0o600 });
	log(`broker_agent_dir_seeded ssot=${ssot} agentDir=${agentDir}`);
}

export async function ensureSteeringDefaults(agentDir: string): Promise<void> {
	const path = join(agentDir, "config.yml");
	let text = "";
	try {
		text = await readFile(path, "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
	let next = text;
	for (const [key, value] of Object.entries(STEERING_DEFAULTS)) {
		const line = new RegExp(`^${key}:[ \\t]*.*$`, "m");
		next = line.test(next)
			? next.replace(line, `${key}: ${value}`)
			: `${next}${next.length && !next.endsWith("\n") ? "\n" : ""}${key}: ${value}\n`;
	}
	if (next !== text) await writeFile(path, next, { mode: 0o600 });
}

function bindAgentDir(args: readonly string[], agentDir: string): readonly string[] {
	if (args[0] !== "sdk") throw new Error("broker CliRunner accepts only gjc sdk commands");
	const bound = [...args];
	const agentDirCount = bound.filter((arg) => arg === "--agent-dir").length;
	if (agentDirCount > 1 || bound.some((arg) => arg.startsWith("--agent-dir="))) {
		throw new Error("broker CliRunner accepts at most one explicit --agent-dir argument");
	}
	const agentDirIndex = bound.indexOf("--agent-dir");
	if (agentDirIndex >= 0) {
		if (bound[agentDirIndex + 1] !== agentDir) {
			throw new Error("broker CliRunner cannot target an agent directory other than its owned broker state");
		}
		bound.splice(agentDirIndex, 2);
	}
	if (bound[1] === "session") bound.splice(2, 0, "--agent-dir", agentDir);
	else bound.push("--agent-dir", agentDir);
	return bound;
}

function brokerEnvironment(agentDir: string): Record<string, string> {
	return {
		...process.env,
		GJC_AGENT_DIR: agentDir,
		GJC_CODING_AGENT_DIR: agentDir,
	} as Record<string, string>;
}
/** Bounded wait for a killed child so a released CLI slot never overlaps a still-running process. */
const KILL_GRACE_MS = 2_000;

async function collectCommand(child: ReturnType<typeof Bun.spawn>, timeoutMs: number): Promise<CliResult> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		const result = await Promise.race([
			Promise.all([
				new Response(child.stdout as ReadableStream).text(),
				new Response(child.stderr as ReadableStream).text(),
				child.exited,
			]),
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error(`gjc command timed out after ${timeoutMs}ms`)), timeoutMs);
			}),
		]);
		return { stdout: result[0], stderr: result[1], exitCode: result[2] };
	} catch (error) {
		// The slot is released only after the child is gone: TERM, then KILL if it
		// lingers. Otherwise a probe/launch that timed out under load keeps running
		// beside its replacement and the single-flight lane means nothing.
		child.kill();
		const exited = await Promise.race([child.exited.then(() => true), sleep(KILL_GRACE_MS).then(() => false)]);
		if (!exited) {
			child.kill("SIGKILL");
			await Promise.race([child.exited, sleep(KILL_GRACE_MS)]);
		}
		throw error;
	} finally {
		if (timer) clearTimeout(timer);
	}
}

function parseGjcVersion(value: string): readonly [number, number, number] | undefined {
	const match = value.match(/(?:gjc\/)?(\d+)\.(\d+)\.(\d+)(?:[-+][0-9A-Za-z.-]+)?/);
	if (!match) return undefined;
	return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function compareVersions(left: readonly number[], right: readonly number[]): number {
	for (let index = 0; index < 3; index++) {
		if (left[index] !== right[index]) return left[index] - right[index];
	}
	return 0;
}

function formatVersion(version: readonly [number, number, number]): string {
	return version.join(".");
}

function diagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error";
}

function defaultPidAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "ESRCH") return false;
		if (code === "EPERM") return true;
		throw error;
	}
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result <= 0) throw new Error(`${name} must be a positive integer`);
	return result;
}

function nonNegativeInteger(value: number | undefined, fallback: number, name: string): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result < 0) throw new Error(`${name} must be a non-negative integer`);
	return result;
}

function sleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
