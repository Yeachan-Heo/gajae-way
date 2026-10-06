import { readFileSync, realpathSync } from "node:fs";
import { homedir } from "node:os";
import { basename, dirname, isAbsolute, join, normalize, resolve } from "node:path";
import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import {
	type BrokerDiscovery,
	type BrokerLivenessVerdict,
	judgeBrokerLiveness,
	type PidAliveProbe,
	readBrokerDiscovery,
} from "./broker-liveness";
import { type BrokerRelease, type BrokerReleaser, createBrokerReleaser } from "./broker-scope";
import { isVerifiedGjcVersion, MIN_GJC_VERSION, VERIFIED_GJC_THROUGH } from "./gjc-contract";
import { sanitizeDiagnostic } from "./rebind";

export {
	BROKER_HEARTBEAT_TTL_MS,
	type BrokerDiscovery,
	type BrokerLivenessVerdict,
	describeBindHold,
	judgeBrokerLiveness,
	type PidAliveProbe,
	readBrokerDiscovery,
} from "./broker-liveness";

/**
 * Candidate locations for the pinned GJC version, in priority order.
 *
 * A compiled binary keeps the *build-time* source path in `__filename`, so the
 * checkout candidate only resolves on the machine that built it. An installed
 * binary carries the pin beside itself instead; without that second candidate a
 * binary shipped to another host exits at boot naming a path from the build
 * machine, which no operator can act on (found deploying dev to jip-gajae).
 */
export function pinnedGjcCandidatePaths(execPath: string = process.execPath): readonly string[] {
	return [join(dirname(dirname(dirname(__filename))), "package.json"), join(dirname(execPath), "package.json")];
}

/** Read the pinned GJC version this build runs against, or throw naming every candidate tried. */
export function readPinnedGjcVersion(candidates: readonly string[] = pinnedGjcCandidatePaths()): string {
	for (const path of candidates) {
		try {
			const version = (JSON.parse(readFileSync(path, "utf-8")) as { gjc?: { version?: unknown } }).gjc?.version;
			if (typeof version === "string" && version.length > 0) return version;
		} catch {
			// A missing or malformed candidate is not fatal: a later one may carry the pin.
		}
	}
	throw new Error(`Failed to read pinned GJC version from any of ${candidates.join(", ")}`);
}
export const HEALTH_PROBE_SESSION_ID = "00000000-0000-4000-8000-000000000000";
const COMMAND_TIMEOUT_MS = 30_000;
// GJC's authoritative shared session flags are in packages/coding-agent/src/commands/sdk.ts;
// value-taking options here keep literal --json payloads from becoming output flags.
const SESSION_VALUE_OPTIONS = new Set([
	"--cursor",
	"--idempotency-key",
	"--json-input",
	"--op",
	"--op-ref",
	"--prompt",
	"--query",
	"--repo",
	"--scope",
	"--text",
	"--timeout-ms",
]);
/** Window over which observed broker incarnation changes count as churn. */
export const BROKER_RESPAWN_WINDOW_MS = 30 * 60_000;
/** Respawns inside the window that turn silent recovery into one operator alert. */
export const BROKER_RESPAWN_CHURN_THRESHOLD = 3;
export const BROKER_STALL_THRESHOLD = 3;
export type SpawnFn = typeof Bun.spawn;
export type GjcCommandRunner = CliRunner;
export interface BrokerAuthorityChange {
	readonly previous: { readonly pid: number; readonly generation: number };
	readonly current: { readonly pid: number; readonly generation: number };
	readonly reason: "broker_stall" | "authority_changed";
}
export type BrokerGenerationListener = (generation: number, change?: BrokerAuthorityChange) => void;
export interface BrokerHealthContext {
	readonly agentDir: string;
	readonly cli: CliRunner;
	readonly discoveryPath: string;
	readonly isPidAlive: PidAliveProbe;
	readonly timeoutMs: number;
	readonly onApplicationError?: (code: string) => void;
}
export type BrokerHealthProbe = (context: BrokerHealthContext) => boolean | Promise<boolean>;
export interface GlobalGjcClientOptions {
	readonly executable?: string;
	readonly agentDir?: string;
	readonly cwd?: string;
	readonly spawn?: SpawnFn;
	readonly command?: GjcCommandRunner;
	readonly discovery?: () => Promise<BrokerDiscovery | undefined>;
	readonly healthProbe?: BrokerHealthProbe;
	readonly isPidAlive?: PidAliveProbe;
	readonly healthIntervalMs?: number;
	readonly healthProbeTimeoutMs?: number;
	readonly readinessAttempts?: number;
	readonly readinessDelayMs?: number;
	readonly reconnectBackoff?: { readonly initialMs?: number; readonly maxMs?: number };
	readonly log?: (line: string) => void;
	/**
	 * Exact GJC version required; if set, preflight refuses to start the broker
	 * if the running version does not match exactly. Used by the gateway to
	 * enforce version pinning across releases.
	 */
	readonly pinnedVersion?: string;
	/**
	 * How long a started client may keep failing to reach a broker whose own
	 * discovery record says it is live before `onLiveOutageExceeded` fires.
	 */
	readonly liveOutageLimitMs?: number;
	readonly brokerGracePeriodMs?: number;
	/**
	 * The broker is up but this process has not reached it for
	 * `liveOutageLimitMs`: the fault is on the gateway side, so the owner should
	 * exit and let the service manager restart it (#246). A dead or absent broker
	 * never fires this; restarting the gateway would not bring it back.
	 */
	readonly onLiveOutageExceeded?: (detail: string) => void;
	/** Moves a broker found in the gateway's own systemd unit into its own scope; `null` disables. */
	readonly releaseBrokerScope?: BrokerReleaser | null;
}
export type GlobalGjcClientDependencies = Omit<GlobalGjcClientOptions, "cwd">;

/** A resident bidirectional JSONL relay to one SDK session host. */
export interface SessionRelayStream {
	readonly lines: AsyncIterable<string>;
	/** Writes one JSONL frame to the host; throws once the relay is closed. */
	write(line: string): void;
	close(): void;
}
export class GjcCliUnavailableError extends Error {
	readonly code = "broker_unavailable";
	constructor(message: string) {
		super(`gjc sdk request failed: broker_unavailable (${message})`);
	}
}

/** Boot-only SDK capability check. Periodic health uses an exact, cursor-free socket lookup. */
export function brokerHealthArgs(): readonly string[] {
	return ["sdk", "session", "list", "--scope", "all"];
}
export function isLoopbackWebSocketUrl(value: string): boolean {
	try {
		const url = new URL(value);
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
	} catch {
		return false;
	}
}
export function probeBrokerEndpoint(
	discovery: BrokerDiscovery,
	timeoutMs: number,
	onApplicationError?: (code: string) => void,
): Promise<boolean> {
	return new Promise((resolve) => {
		let socket: WebSocket;
		try {
			const url = new URL(discovery.url);
			if (!isLoopbackWebSocketUrl(discovery.url)) return resolve(false);
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
				/* already closed */
			}
		};
		const timer = setTimeout(() => settle(false), timeoutMs);
		socket.addEventListener("message", (event) => {
			let frame: Record<string, unknown>;
			try {
				const value: unknown = JSON.parse(String(event.data));
				if (!value || typeof value !== "object") return settle(false);
				frame = value as Record<string, unknown>;
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
							id: "gajaeway-health",
							operation: "session.get_endpoint",
							input: { sessionId: HEALTH_PROBE_SESSION_ID },
						}),
					);
				} catch {
					settle(false);
				}
				return;
			}
			if (frame.type !== "broker_response" || frame.id !== "gajaeway-health" || typeof frame.ok !== "boolean")
				return settle(false);
			if (!frame.ok) {
				const code = (frame.error as { code?: unknown } | undefined)?.code;
				onApplicationError?.(typeof code === "string" ? code : "unknown");
			}
			// A routed application rejection is not evidence of a dead daemon.
			settle(true);
		});
		socket.addEventListener("error", () => settle(false));
		socket.addEventListener("close", () => settle(false));
	});
}
export async function probeBrokerDiscovery(context: BrokerHealthContext): Promise<boolean> {
	const discovery = await readBrokerDiscovery(context.discoveryPath, context.isPidAlive);
	return discovery ? await probeBrokerEndpoint(discovery, context.timeoutMs, context.onApplicationError) : false;
}
export function isHealthySessionList(result: CliResult): boolean {
	if (result.exitCode !== 0) return false;
	try {
		const parsed = JSON.parse(result.stdout);
		return parsed?.ok === true && Array.isArray(parsed.result?.sessions);
	} catch {
		return false;
	}
}
export async function preflightGjcRuntime(
	run: GjcCommandRunner,
	minimumVersion = MIN_GJC_VERSION,
	sdk?: GjcCommandRunner,
	options?: { readonly pinnedVersion?: string },
): Promise<{ readonly version: string }> {
	const result = await run(["--version"], { timeoutMs: COMMAND_TIMEOUT_MS });
	const version = (result.stdout || result.stderr).match(/(?:gjc\/)?(\d+)\.(\d+)\.(\d+)/);
	if (result.exitCode !== 0 || !version) throw new Error("gjc runtime preflight failed: invalid version response");

	const detectedVersion = version.slice(1, 4).join(".");

	// If a pinned version is required, check for exact match
	if (options?.pinnedVersion) {
		if (detectedVersion !== options.pinnedVersion) {
			throw new Error(
				`gjc runtime preflight failed: version mismatch (running ${detectedVersion}, gateway requires ${options.pinnedVersion}). Run 'gajaeway ops upgrade' to update the gateway's gjc.`,
			);
		}
	} else {
		// Otherwise check minimum version
		const minimum = minimumVersion.match(/^(\d+)\.(\d+)\.(\d+)$/);
		if (!minimum) throw new Error("gjc runtime preflight failed: invalid minimum version spec");
		for (let i = 1; i <= 3; i++) {
			if (Number(version[i]) < Number(minimum[i]))
				throw new Error(`gjc runtime preflight failed: requires gjc >= ${minimumVersion}`);
			if (Number(version[i]) > Number(minimum[i])) break;
		}
	}
	// A failed session list is the broker not answering yet (host reboot, stale
	// lock being cleared), not a wrong runtime: it is classed as unavailable so
	// boot waits it out instead of exiting into a restart loop (#182).
	if (sdk && !isHealthySessionList(await sdk(brokerHealthArgs(), { timeoutMs: COMMAND_TIMEOUT_MS }))) {
		throw new GjcCliUnavailableError("gjc runtime preflight failed: invalid session-list envelope");
	}
	// The relay argv contract is boot-gated: a usage rejection (exit 2) here
	// means EVERY tail stream would die at spawn and the gateway would silently
	// fall back to slow polling. Measured 2026-09-15: `sdk serve --agent-dir`
	// exit 2 for days with no boot-time signal. Fail closed instead.
	if (sdk) {
		const relay = await sdk(streamRelayArgs(HEALTH_PROBE_SESSION_ID), { timeoutMs: COMMAND_TIMEOUT_MS });
		if (isUsageRejection(relay)) throw new Error("gjc runtime preflight failed: stream relay rejected its argv");
	}
	return { version: version.slice(1, 4).join(".") };
}
/** The exact argv `openStream` spawns; `serve` is env-bound and takes no --agent-dir (gjc 0.16.6 exits 2). */
export function streamRelayArgs(sessionId: string): readonly string[] {
	return ["sdk", "serve", "--stdio", "--session", sessionId];
}
/**
 * gjc exits 2 on an argv it cannot parse; every runtime failure exits 1 or
 * prints a JSON envelope. Up to 0.17.2 the text says `unknown argument`/USAGE;
 * 0.17.4 reports a structured `"code":"usage"` error instead.
 */
export function isUsageRejection(result: CliResult): boolean {
	return (
		result.exitCode === 2 && /unknown argument|USAGE|"code"\s*:\s*"usage"/i.test(`${result.stdout}\n${result.stderr}`)
	);
}

/** A client of the user's global runtime. No directory, daemon, lock, or session ownership. */
export class GlobalGjcClient {
	readonly executable: string;
	readonly agentDir: string;
	readonly discoveryPath: string;
	readonly cli: CliRunner;
	readonly #options: GlobalGjcClientOptions;
	readonly #spawn: SpawnFn;
	readonly #releaseBrokerScope: BrokerReleaser | undefined;
	readonly #cwd: string;
	readonly #env: Record<string, string>;
	readonly #timeout: number;
	readonly #interval: number;
	readonly #initialBackoff: number;
	readonly #maxBackoff: number;
	readonly #brokerGracePeriodOverride: number | undefined;
	readonly #listeners = new Set<BrokerGenerationListener>();
	readonly #children = new Set<ReturnType<SpawnFn>>();
	readonly #terminations = new Map<ReturnType<SpawnFn>, Promise<void>>();
	readonly #relays = new Set<() => void>();
	readonly #queue: Array<{ resolve: () => void; priority: "interactive" | "background" }> = [];
	#backgroundInFlight = 0;
	#inflight = 0;
	#generation = 0;
	#identity: string | undefined;
	#authorityPid: number | undefined;
	#probeFailures = 0;
	#stallDetected = false;
	#ownedPids = new Map<number, string>(); // pid -> discovery identity, for real ownership tracking
	#brokerGracePeriodMs = 2_000; // Configurable for tests, must be >= 2s
	#available = false;
	#stopped = false;
	#stoppedAtEpoch: number | undefined; // When stopped due to involuntary termination failure
	#stoppedAtTime: number | undefined; // Time when involuntary stop occurred
	#started = false;
	#epoch = 0;
	#failures = 0;
	/** Why the most recent observation failed; logged so an outage names its cause. */
	#unavailableReason = "not observed";
	/** Whether the most recent failed observation saw a live broker in discovery. */
	#observedLiveBroker = false;
	/** Start of the current post-start outage, whatever its cause. */
	#outageSince: number | undefined;
	/** Start of the current run of failures against a broker discovery calls live. */
	#liveOutageSince: number | undefined;
	readonly #liveOutageLimit: number;
	#timer: ReturnType<typeof setTimeout> | undefined;
	#starting: Promise<void> | undefined;
	#gjcVersion: string | undefined;
	/** Observation times of incarnation changes after the first; bounded by the churn window. */
	#respawns: number[] = [];
	#churnAlerted = false;

	constructor(options: GlobalGjcClientOptions = {}) {
		this.#options = options;
		this.#cwd = resolve(options.cwd ?? process.cwd());
		const trusted = trustedEnvironment(this.#cwd);
		const executable = options.executable ?? trusted.GJC_EXECUTABLE ?? Bun.which("gjc");
		if (!executable || !isAbsolute(executable)) throw new Error("GJC executable must resolve to an absolute path");
		this.executable = executable;
		const configName = validConfigName(trusted.GJC_CONFIG_DIR) ?? validConfigName(trusted.PI_CONFIG_DIR) ?? ".gjc";
		if (!options.agentDir && !trusted.GJC_CODING_AGENT_DIR && !trusted.PI_CODING_AGENT_DIR && !trusted.HOME) {
			throw new Error("Cannot establish trusted GJC user home; provide an explicit agentDir");
		}
		this.agentDir = canonicalAgentDir(
			resolve(
				this.#cwd,
				options.agentDir ??
					trusted.GJC_CODING_AGENT_DIR ??
					trusted.PI_CODING_AGENT_DIR ??
					join(trusted.HOME ?? homedir(), configName, "agent"),
			),
		);
		this.discoveryPath = join(this.agentDir, "sdk", "broker.json");
		this.#env = {
			...trusted,
			GJC_EXECUTABLE: this.executable,
			GJC_CODING_AGENT_DIR: this.agentDir,
			PI_CODING_AGENT_DIR: this.agentDir,
			GJC_AGENT_DIR: this.agentDir,
		};
		this.#spawn = options.spawn ?? Bun.spawn.bind(Bun);
		this.#releaseBrokerScope =
			options.releaseBrokerScope === null
				? undefined
				: (options.releaseBrokerScope ?? (process.platform === "linux" ? createBrokerReleaser() : undefined));
		this.#timeout = integer(options.healthProbeTimeoutMs, COMMAND_TIMEOUT_MS, 1);
		this.#interval = integer(options.healthIntervalMs, 5_000, 1);
		this.#initialBackoff = integer(options.reconnectBackoff?.initialMs, 250, 1);
		this.#maxBackoff = integer(options.reconnectBackoff?.maxMs, 10_000, this.#initialBackoff);
		this.#brokerGracePeriodOverride = options.brokerGracePeriodMs;
		if (this.#brokerGracePeriodOverride !== undefined) {
			this.#brokerGracePeriodMs = this.#brokerGracePeriodOverride;
		}
		this.#liveOutageLimit = integer(options.liveOutageLimitMs, 180_000, 1);
		integer(options.readinessAttempts, 20, 1);
		integer(options.readinessDelayMs, 100, 0);
		this.cli = (args, commandOptions) =>
			this.#run(
				bindAgentDir(args, this.agentDir),
				commandOptions?.timeoutMs,
				commandOptions?.priority ?? "interactive",
			);
	}
	get generation(): number {
		return this.#generation;
	}
	get gjcVersion(): string | undefined {
		return this.#gjcVersion;
	}
	/** The current broker outage as a log fragment, or undefined while requests can be served. */
	outage(now = Date.now()): string | undefined {
		if (this.#outageSince === undefined) return undefined;
		return `broker_unavailable_for=${Math.max(0, Math.floor((now - this.#outageSince) / 1000))}s reason=${this.#unavailableReason}`;
	}
	/**
	 * Broker incarnation changes observed inside the churn window. A daemon that
	 * keeps dying and being respawned by its own lifecycle passes every
	 * liveness probe between deaths; only the repetition names the outage.
	 */
	recentRespawns(now = Date.now()): number {
		const floor = now - BROKER_RESPAWN_WINDOW_MS;
		const live = this.#respawns.findIndex((at) => at > floor);
		this.#respawns.splice(0, live < 0 ? this.#respawns.length : live);
		return this.#respawns.length;
	}
	/** True while respawns inside the window reach the churn threshold. */
	respawnChurn(now = Date.now()): boolean {
		return this.recentRespawns(now) >= BROKER_RESPAWN_CHURN_THRESHOLD;
	}
	/** Judges the daemon from its own discovery file, bypassing SDK transport. */
	judgeLiveness(): Promise<BrokerLivenessVerdict> {
		return judgeBrokerLiveness(this.discoveryPath, this.#options.isPidAlive ?? defaultPidAlive);
	}
	onGeneration(listener: BrokerGenerationListener): () => void {
		this.#listeners.add(listener);
		return () => this.#listeners.delete(listener);
	}
	async preflight(): Promise<void> {
		const result = await preflightGjcRuntime(
			(args, options) => this.#run(args, options?.timeoutMs),
			MIN_GJC_VERSION,
			this.cli,
			{ pinnedVersion: this.#options.pinnedVersion },
		);
		this.#noteVersion(result.version);
	}
	/**
	 * Re-reads `gjc --version`. gjc is upgraded underneath a running gateway,
	 * and the first observable sign is a new broker generation, so the gateway
	 * re-checks there instead of trusting the version it booted with.
	 */
	async refreshVersion(): Promise<string | undefined> {
		const result = await this.#run(["--version"], COMMAND_TIMEOUT_MS);
		const version = (result.stdout || result.stderr).match(/(\d+\.\d+\.\d+)/)?.[1];
		if (result.exitCode === 0 && version) this.#noteVersion(version);
		return this.#gjcVersion;
	}
	#noteVersion(version: string): void {
		const previous = this.#gjcVersion;
		this.#gjcVersion = version;
		if (previous !== undefined && previous !== version)
			console.error(`gjc_version_changed from=${previous} to=${version}`);
		if (previous !== version && !isVerifiedGjcVersion(version))
			console.error(
				`gjc_unverified_version version=${version} verifiedThrough=${VERIFIED_GJC_THROUGH}.x reason=error_contract_unverified`,
			);
	}
	async start(): Promise<void> {
		if (this.#starting) return this.#starting;
		// Allow recovery from involuntary stop even if already started
		if (this.#started && !this.#stopped) return;
		if (this.#children.size > 0) throw new GjcCliUnavailableError("owned child exit remains unconfirmed");
		this.#stopped = false;
		const epoch = ++this.#epoch;
		this.#starting = this.#start(epoch);
		try {
			await this.#starting;
		} finally {
			this.#starting = undefined;
		}
	}
	async #start(epoch: number): Promise<void> {
		const attempts = this.#options.readinessAttempts ?? 20;
		// This one read-only SDK request may auto-start GJC through GJC's own normal lifecycle.
		// Recovery observation below never retries launcher commands or repairs a shared daemon.
		const discovery = await this.#discovery();
		if (!discovery) {
			const result = await this.cli(brokerHealthArgs(), { timeoutMs: this.#timeout });
			if (!isHealthySessionList(result)) throw new GjcCliUnavailableError("invalid SDK readiness response");
		}
		for (let i = 0; i < attempts && epoch === this.#epoch && !this.#stopped; i++) {
			if (await this.#observe(epoch)) {
				this.#started = true;
				this.#schedule(epoch);
				return;
			}
			if (i + 1 < attempts) await delay(this.#options.readinessDelayMs ?? 100);
		}
		throw new GjcCliUnavailableError("global broker is not ready; no repair attempted");
	}
	async stop(): Promise<void> {
		this.#stopped = true;
		this.#stoppedAtEpoch = undefined; // Clear involuntary stop markers
		this.#stoppedAtTime = undefined;
		this.#started = false;
		this.#available = false;
		this.#outageSince = undefined;
		this.#liveOutageSince = undefined;
		this.#probeFailures = 0;
		this.#stallDetected = false;
		++this.#epoch;
		if (this.#timer) clearTimeout(this.#timer);
		this.#timer = undefined;
		for (const close of this.#relays) close();
		for (const entry of this.#queue.splice(0)) entry.resolve();
		const results = await Promise.allSettled([...this.#children].map((child) => this.#terminate(child)));
		if (results.some((result) => result.status === "rejected") || this.#children.size > 0) {
			throw new GjcCliUnavailableError("shutdown incomplete: owned child exit remains unconfirmed");
		}
	}
	#track(child: ReturnType<SpawnFn>): void {
		this.#children.add(child);
		void child.exited.then(
			() => {
				this.#children.delete(child);
				// If this was the last unconfirmed child from an involuntary stop,
				// clear the stop markers and attempt recovery via start().
				if (this.#stoppedAtEpoch !== undefined && this.#children.size === 0) {
					this.#stoppedAtEpoch = undefined;
					this.#stoppedAtTime = undefined;
					// Attempt to restart; if it fails, the client stays stopped for diagnostics.
					void this.start().catch(() => {});
				}
			},
			() => {},
		);
	}
	#terminate(child: ReturnType<SpawnFn>): Promise<void> {
		const pending = this.#terminations.get(child);
		if (pending) return pending;
		const termination = terminateChild(child)
			.catch((error: unknown) => {
				// Unconfirmed children must not overlap a replacement client generation.
				// Mark involuntary stop and keep observation running so the #246 guard
				// (onLiveOutageExceeded) can trigger if the child stays unconfirmed too long.
				this.#stopped = true;
				this.#stoppedAtEpoch = this.#epoch; // Mark involuntary stop
				this.#stoppedAtTime = Date.now();
				this.#started = false;
				this.#available = false;
				++this.#epoch;
				if (this.#timer) clearTimeout(this.#timer);
				this.#timer = undefined;
				this.#schedule(this.#epoch);
				throw error;
			})
			.finally(() => {
				this.#terminations.delete(child);
			});
		this.#terminations.set(child, termination);
		return termination;
	}
	async #discovery(): Promise<BrokerDiscovery | undefined> {
		this.#assertAgentDirIdentity();
		return await bounded(
			this.#options.discovery
				? this.#options.discovery()
				: readBrokerDiscovery(this.discoveryPath, this.#options.isPidAlive ?? defaultPidAlive),
			this.#timeout,
		);
	}
	async #observe(epoch: number, reread = true): Promise<boolean> {
		this.#observedLiveBroker = false;
		try {
			const discovery = await this.#discovery();
			if (epoch !== this.#epoch || this.#stopped) return false;
			if (!discovery) {
				this.#available = false;
				this.#probeFailures = 0;
				const verdict = await this.judgeLiveness();
				this.#observedLiveBroker = verdict.state === "live";
				this.#unavailableReason = describeDiscoveryFailure(verdict);
				return false;
			}
			this.#observedLiveBroker = true;
			const healthProbe = this.#options.healthProbe;
			const healthy = await bounded(
				healthProbe
					? Promise.resolve().then(() =>
							healthProbe({
								agentDir: this.agentDir,
								cli: this.cli,
								discoveryPath: this.discoveryPath,
								isPidAlive: this.#options.isPidAlive ?? defaultPidAlive,
								timeoutMs: this.#timeout,
							}),
						)
					: probeBrokerEndpoint(discovery, this.#timeout),
				this.#timeout,
			).catch(() => false);
			if (epoch !== this.#epoch || this.#stopped) return false;
			this.#assertAgentDirIdentity();
			this.#available = healthy;
			if (!healthy) {
				this.#unavailableReason = `endpoint probe failed for live discovery pid ${discovery.pid}`;
				if (reread && ++this.#probeFailures >= BROKER_STALL_THRESHOLD) {
					if (!this.#stallDetected) {
						this.#stallDetected = true;
						const isOwned = this.#ownedPids.has(discovery.pid);
						if (isOwned) {
							// Owned broker: terminate to recover from wedge, respawn via normal lifecycle
							this.#log(
								`broker_stall_detected pid=${discovery.pid} generation=${this.#generation} failures=${this.#probeFailures} action=terminate_owned`,
							);
							this.#terminateWedgedOwnedBroker(discovery.pid).catch((error) => {
								try {
									this.#log(`broker_termination_failed pid=${discovery.pid} error=${error instanceof Error ? error.message : String(error)}`);
								} catch {}
							});
						} else {
							// Shared broker: mark unavailable but keep probing, do not kill
							this.#log(
								`broker_stall_detected pid=${discovery.pid} generation=${this.#generation} failures=${this.#probeFailures} action=keep_probing_shared reason=broker_stall_shared_broker_not_owned_by_gateway_not_terminating`,
							);
						}
					}
					// Re-read and authenticate the replacement before publishing authority.
					// Discovery alone cannot authorize a rebind, and shared daemons are never killed.
					return await this.#observe(epoch, false);
				}
				return false;
			}
			const identity = `${discovery.pid}|${discovery.url}|${discovery.token}`;
			if (identity !== this.#identity) {
				const previous =
					this.#authorityPid === undefined ? undefined : { pid: this.#authorityPid, generation: this.#generation };
				await this.#releaseBroker(discovery.pid);
				if (epoch !== this.#epoch || this.#stopped) return false;
				if (this.#identity !== undefined) this.#noteRespawn(discovery.pid);
				this.#identity = identity;
				this.#authorityPid = discovery.pid;
				// Clear owned pids on identity change to prevent stale ownership tracking
				this.#ownedPids.clear();
				this.#generation++;
				const change: BrokerAuthorityChange | undefined = previous
					? {
							previous,
							current: { pid: discovery.pid, generation: this.#generation },
							reason: this.#stallDetected ? "broker_stall" : "authority_changed",
						}
					: undefined;
				for (const listener of this.#listeners) {
					try {
						listener(this.#generation, change);
					} catch (error) {
						this.#log(error);
					}
				}
			}
			this.#probeFailures = 0;
			this.#stallDetected = false;
			return true;
		} catch (error) {
			if (epoch === this.#epoch && !this.#stopped) {
				this.#available = false;
				this.#unavailableReason = `observation failed: ${error instanceof Error ? error.message : String(error)}`;
				this.#log(error);
			}
			return false;
		}
	}
	/** A broker our own SDK command autostarted must not share the gateway unit's stop (#183). */
	async #releaseBroker(pid: number): Promise<void> {
		if (!this.#releaseBrokerScope) return;
		let result: BrokerRelease;
		try {
			result = await this.#releaseBrokerScope(pid);
		} catch (error) {
			result = {
				outcome: "skipped",
				reason: `scope_failed: ${error instanceof Error ? error.message : String(error)}`,
			};
		}
		if (result.outcome === "released") {
			// Record this pid as owned: the releaser actually moved it out of the gateway's systemd unit
			const identity = `${pid}|${this.#identity ?? "unknown"}`;
			this.#ownedPids.set(pid, identity);
			this.#log(`broker_scope_released pid=${pid} scope=${result.scope} processes=${result.pids.length}`);
		} else if (result.reason.startsWith("scope_failed")) {
			this.#log(`broker_scope_release_failed pid=${pid} reason=${result.reason}`);
		} else {
			// outcome: "skipped" - this broker was not released, so it's shared
			this.#log(`broker_scope_skipped pid=${pid} reason=${result.reason}`);
		}
	}
	#noteRespawn(pid: number): void {
		const now = Date.now();
		this.#respawns.push(now);
		if (!this.respawnChurn(now)) {
			this.#churnAlerted = false;
			return;
		}
		// One loud line per churn episode, not one per respawn: the episode ends
		// only when the window drains below the threshold.
		if (this.#churnAlerted) return;
		this.#churnAlerted = true;
		this.#log(
			`broker_respawn_churn respawns=${this.recentRespawns(now)} windowMs=${BROKER_RESPAWN_WINDOW_MS} pid=${pid} generation=${this.#generation + 1}`,
		);
	}
	/** Terminate a wedged owned broker with SIGTERM/SIGKILL escalation to allow respawn via normal lifecycle. */
	async #terminateWedgedOwnedBroker(pid: number): Promise<void> {
		const gracePeriod = this.#brokerGracePeriodMs;
		for (const signal of ["SIGTERM", "SIGKILL"] as const) {
			try {
				process.kill(pid, signal);
				// Allow sane grace period (>=2s by default) for process to respond to signal
				await delay(gracePeriod);
				// Check if process is still alive
				try {
					process.kill(pid, 0);
					// Process still alive, continue to next signal escalation
					continue;
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code === "ESRCH") {
						// Process is dead
						return;
					}
					// EPERM or other error means we can't verify, but continue
					continue;
				}
			} catch {
				// Signal send failed, try next escalation
				continue;
			}
		}
	}
	#schedule(epoch: number): void {
		// Continue observation after involuntary stops so the #246 guard can trigger.
		// Only skip if voluntarily stopped (user called stop(), not termination failure).
		const voluntarilyStop = this.#stopped && this.#stoppedAtEpoch === undefined;
		if (voluntarilyStop || epoch !== this.#epoch) return;
		const wait = this.#available
			? this.#interval
			: Math.min(this.#initialBackoff * 2 ** Math.min(this.#failures++, 20), this.#maxBackoff);
		this.#timer = setTimeout(() => {
			this.#timer = undefined;
			void this.#observe(epoch).then((healthy) => {
				const now = Date.now();
				if (healthy) {
					this.#failures = 0;
					this.#outageSince = undefined;
					this.#liveOutageSince = undefined;
				} else if (epoch === this.#epoch && !this.#stopped) {
					this.#log(
						new GjcCliUnavailableError(
							`global broker unavailable; observing without repair: ${this.#unavailableReason}`,
						),
					);
					this.#noteOutage(now);
				}
				// Check for unconfirmed child timeout regardless of broker health.
				// If child is still unconfirmed past the deadline, exit so systemd restarts.
				if (epoch === this.#epoch && this.#stoppedAtEpoch !== undefined && this.#children.size > 0) {
					const elapsed = now - (this.#stoppedAtTime ?? 0);
					if (elapsed >= this.#liveOutageLimit) {
						this.#options.onLiveOutageExceeded?.(
							sanitizeDiagnostic(
								`owned child unconfirmed for ${Math.floor(elapsed / 1000)}s; exiting for systemd restart`,
							),
						);
					}
				}
				this.#schedule(epoch);
			});
		}, wait);
	}
	#noteOutage(now: number): void {
		this.#outageSince ??= now;
		if (!this.#observedLiveBroker) {
			this.#liveOutageSince = undefined;
			return;
		}
		this.#liveOutageSince ??= now;
		const elapsed = now - this.#liveOutageSince;
		if (elapsed < this.#liveOutageLimit) return;
		// Restart the window so an owner that does not exit is told again later,
		// not on every probe.
		this.#liveOutageSince = now;
		this.#options.onLiveOutageExceeded?.(
			sanitizeDiagnostic(`live broker unreachable for ${Math.floor(elapsed / 1000)}s: ${this.#unavailableReason}`),
		);
	}
	#log(error: unknown): void {
		(this.#options.log ?? console.error)(sanitizeDiagnostic(error instanceof Error ? error.message : String(error)));
	}
	async #run(
		args: readonly string[],
		requestedTimeout?: number,
		priority: "interactive" | "background" = "interactive",
	): Promise<CliResult> {
		this.#assertAgentDirIdentity();
		const timeout = integer(requestedTimeout, COMMAND_TIMEOUT_MS, 1);
		const deadline = Date.now() + timeout;
		// Interactive waiters always have headroom. Background can max 2 in flight, interactive can max 2.
		const canProceed = (): boolean => {
			if (priority === "interactive") {
				return this.#inflight < 4;
			}
			// Background: must be under 4 total AND under 2 background concurrent
			return this.#inflight < 4 && this.#backgroundInFlight < 2;
		};
		while (!canProceed()) {
			let resolve: (() => void) | undefined;
			const entry = { resolve: () => {}, priority };
			try {
				await bounded(
					new Promise<void>((r) => {
						resolve = r;
						entry.resolve = r;
						this.#queue.push(entry);
					}),
					Math.max(1, deadline - Date.now()),
				);
			} finally {
				if (resolve) {
					const i = this.#queue.findIndex((e) => e.resolve === resolve);
					if (i >= 0) this.#queue.splice(i, 1);
				}
			}
			if (Date.now() >= deadline) throw new GjcCliUnavailableError("command queue timed out");
		}
		this.#assertAgentDirIdentity();
		if (this.#stopped || (this.#started && !this.#available))
			throw new GjcCliUnavailableError("client stopped or broker unavailable");
		this.#inflight++;
		if (priority === "background") this.#backgroundInFlight++;
		try {
			const remaining = Math.max(1, deadline - Date.now());
			if (this.#options.command)
				return await bounded(this.#options.command(args, { timeoutMs: remaining, priority }), remaining);
			const child = this.#spawn({
				cmd: [this.executable, ...args],
				cwd: this.#cwd,
				env: this.#env,
				stdin: "ignore",
				stdout: "pipe",
				stderr: "pipe",
			});
			this.#track(child);
			try {
				const [stdout, stderr, exitCode] = await bounded(
					Promise.all([
						new Response(child.stdout as ReadableStream).text(),
						new Response(child.stderr as ReadableStream).text(),
						child.exited,
					]),
					remaining,
				);
				return { stdout, stderr, exitCode };
			} catch (error) {
				await this.#terminate(child);
				throw error;
			}
		} finally {
			this.#inflight--;
			if (priority === "background") this.#backgroundInFlight--;
			// Dequeue interactive first, or a background waiter if none interactive are waiting
			let nextIndex = this.#queue.findIndex((e) => e.priority === "interactive");
			if (nextIndex < 0) {
				nextIndex = this.#queue.findIndex((e) => e.priority === "background");
			}
			if (nextIndex >= 0) {
				const [next] = this.#queue.splice(nextIndex, 1);
				next.resolve();
			}
		}
	}
	#assertAgentDirIdentity(): void {
		if (canonicalAgentDir(this.agentDir) !== this.agentDir) {
			throw new GjcCliUnavailableError(
				"agent directory canonical identity changed; recreate the client before binding authority",
			);
		}
	}
	/**
	 * One resident `gjc sdk serve --stdio` relay for a session: JSONL frames go
	 * down its stdin to the host (hello, control/query requests) and the host's
	 * frames come back up stdout. Only gateway-created relays are terminated,
	 * never their GJC daemon or session host.
	 */
	openStream(sessionId: string): SessionRelayStream {
		this.#assertAgentDirIdentity();
		if (!sessionId || sessionId.startsWith("-") || /[\r\n\0]/.test(sessionId)) throw new Error("invalid session ID");
		if (this.#stopped || !this.#available) throw new GjcCliUnavailableError("broker unavailable");
		// `gjc sdk serve` takes no `--agent-dir` (gjc 0.16.7: "unknown argument",
		// exit 2 after the hello frame). It binds through GJC_CODING_AGENT_DIR,
		// which #env already pins to this.agentDir. Passing the flag made every
		// relay die in <1s, the runner counted six sub-5s reopens as a dead
		// stream, declared a retention gap, and held the turn - the persona went
		// mute on long turns and presence never advanced (live, 2026-09-17).
		const child = this.#spawn({
			cmd: [this.executable, ...streamRelayArgs(sessionId)],
			cwd: this.#cwd,
			env: this.#env,
			stdin: "pipe",
			stdout: "pipe",
			// The serve CLI prints its own refusal envelope (`endpoint_stale`,
			// `not_found`, `broker_unavailable`) on STDERR and exits. Ignoring it
			// left the runner waiting the full hello timeout on a retired session
			// (live, 2026-09-20). Both streams feed one line iterator.
			stderr: "pipe",
		});
		this.#track(child);
		let closed = false;
		const close = () => {
			if (closed) return;
			closed = true;
			this.#relays.delete(close);
			void this.#terminate(child).catch((error: unknown) => {
				try {
					this.#log(error);
				} catch {
					/* retain failed child for stop() even if logging fails */
				}
			});
		};
		this.#relays.add(close);
		void child.exited.then(close, close);
		const lines = (async function* () {
			const queue: Array<string | null> = [];
			let wake: (() => void) | undefined;
			const push = (line: string | null) => {
				queue.push(line);
				wake?.();
				wake = undefined;
			};
			const streams: ReadableStream<Uint8Array>[] = [];
			for (const stream of [child.stdout, child.stderr]) if (stream instanceof ReadableStream) streams.push(stream);
			let open = streams.length;
			const pump = async (stream: ReadableStream<Uint8Array>) => {
				const reader = stream.getReader();
				// One streaming decoder PER stream: a multibyte character split at a
				// stdout chunk boundary must take its continuation bytes from stdout,
				// never from an interleaved stderr chunk or stderr's EOF flush.
				const decoder = new TextDecoder();
				let buffer = "";
				try {
					for (;;) {
						const { value, done } = await reader.read();
						if (done) break;
						buffer += decoder.decode(value, { stream: true });
						let newline = buffer.indexOf("\n");
						while (newline >= 0) {
							push(buffer.slice(0, newline));
							buffer = buffer.slice(newline + 1);
							newline = buffer.indexOf("\n");
						}
					}
					buffer += decoder.decode();
					if (buffer) push(buffer);
				} finally {
					reader.releaseLock();
					if (--open === 0) push(null);
				}
			};
			if (open === 0) push(null);
			for (const stream of streams) void pump(stream).catch(() => push(null));
			try {
				for (;;) {
					if (queue.length === 0) await new Promise<void>((resolve) => (wake = resolve));
					const line = queue.shift();
					if (line === null || line === undefined) {
						if (line === null) break;
						continue;
					}
					yield line;
				}
			} finally {
				close();
			}
		})();
		const write = (line: string): void => {
			if (closed) throw new Error("relay closed");
			const stdin = child.stdin;
			if (!stdin || typeof stdin === "number") throw new Error("relay stdin unavailable");
			stdin.write(`${line}\n`);
			// FileSink.flush is synchronous unless the pipe is backpressured, in
			// which case it returns a promise that rejects on EPIPE once the child
			// is gone. An escaped rejection would be unhandled; the relay ending is
			// already reported through `lines`, so a late flush failure only closes.
			const flushed = stdin.flush();
			if (flushed instanceof Promise) flushed.catch(() => close());
		};
		return { lines, write, close };
	}
}
function bindAgentDir(args: readonly string[], agentDir: string): readonly string[] {
	if (args[0] !== "sdk") throw new Error("Global GJC client accepts only sdk commands");
	if (args.some((arg) => /^(--agent-dir|--cwd|--config-dir|--executable)(=|$)/.test(arg))) {
		throw new Error("Global GJC client rejects caller retarget arguments");
	}
	const bound = [...args];
	// `gjc sdk serve` has no --agent-dir flag (exit 2 + usage on gjc 0.16.6):
	// the relay takes its agent dir from the exported GJC_*_AGENT_DIR env.
	if (bound[1] === "serve") return bound;
	if (bound[1] === "session") {
		// gjc 0.17.6 requires --json for machine-readable session errors.
		// Skip option payloads so a literal --json value is not an output flag.
		let jsonFlags = 0;
		for (let i = 2; i < bound.length; i++) {
			const arg = bound[i]!;
			if (arg === "--") throw new Error("Global GJC client rejects session option delimiters");
			if (SESSION_VALUE_OPTIONS.has(arg)) {
				if (bound[i + 1] === undefined) throw new Error(`Global GJC client requires a value after ${arg}`);
				i++;
			} else if (arg === "--json") {
				if (++jsonFlags > 1) throw new Error("Global GJC client rejects duplicate --json flags");
			} else if (arg.startsWith("--json=")) {
				throw new Error("Global GJC client rejects --json values");
			}
		}
		if (jsonFlags === 0) bound.push("--json");
		// Every `sdk session` leaf owns its own --agent-dir option; gjc 0.17.4
		// rejects it at the family level (`sdk session --agent-dir <dir> list`,
		// exit 2 usage). Keep this leaf-level binding last.
		bound.push("--agent-dir", agentDir);
		return bound;
	}
	bound.push("--agent-dir", agentDir);
	return bound;
}
/** Fail closed on project dotenv path declarations; do not promote Bun-loaded project values to user authority. */
function trustedEnvironment(cwd: string): Record<string, string> {
	const env = Object.fromEntries(
		Object.entries(process.env).filter((entry): entry is [string, string] => entry[1] !== undefined),
	);
	const sensitive = new Set([
		"HOME",
		"USERPROFILE",
		"GJC_EXECUTABLE",
		"GJC_CONFIG_DIR",
		"PI_CONFIG_DIR",
		"GJC_CODING_AGENT_DIR",
		"PI_CODING_AGENT_DIR",
	]);
	const files = new Set([
		".env",
		".env.local",
		".env.development",
		".env.production",
		".env.test",
		".env.development.local",
		".env.production.local",
		".env.test.local",
	]);
	for (const root of new Set([cwd, process.cwd()]))
		for (const file of files) {
			let text: string;
			try {
				text = readFileSync(join(root, file), "utf8");
			} catch (error) {
				if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
				throw error;
			}
			for (const match of text.matchAll(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=/gm)) {
				const key = process.platform === "win32" ? match[1]!.toUpperCase() : match[1]!;
				if (sensitive.has(key) && env[key])
					throw new Error(`Cannot trust project-declared ${key}; launch from a clean service directory`);
			}
		}
	return env;
}
/** Preserve GJC autostart for a missing profile, resolving existing ancestor aliases first.
 * A later alias replacement is rejected rather than silently changing database authority.
 */
function canonicalAgentDir(path: string): string {
	try {
		return realpathSync(path);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		const parent = dirname(path);
		if (parent === path) throw error;
		return join(canonicalAgentDir(parent), basename(path));
	}
}
/** Names why no fresh discovery record was usable, from the daemon's own file. */
function describeDiscoveryFailure(verdict: BrokerLivenessVerdict): string {
	if (verdict.state === "absent") return "discovery absent";
	if (verdict.state === "live") return `discovery pid ${verdict.pid} rejected`;
	return verdict.reason === "pid_dead"
		? `discovery pid ${verdict.pid} is dead`
		: `discovery pid ${verdict.pid} stopped heartbeating at ${new Date(verdict.heartbeatAt).toISOString()}`;
}
function validConfigName(value: string | undefined): string | undefined {
	const name = value?.trim();
	return name && !normalize(name).split(/[\\/]/).includes("..") ? name : undefined;
}
function defaultPidAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
		if ((error as NodeJS.ErrnoException).code === "EPERM") return true;
		throw error;
	}
}
function integer(value: number | undefined, fallback: number, minimum: number): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result < minimum) throw new Error(`expected integer >= ${minimum}`);
	return result;
}
function delay(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}
async function bounded<T>(promise: Promise<T>, timeout: number): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new GjcCliUnavailableError(`request timed out after ${timeout}ms`)), timeout);
			}),
		]);
	} finally {
		if (timer) clearTimeout(timer);
	}
}
async function terminateChild(child: ReturnType<SpawnFn>): Promise<void> {
	// Signal errors alone do not establish failure: a concurrently exiting child
	// can reject a signal while its exit promise still supplies definitive proof.
	for (const signal of ["SIGTERM", "SIGKILL"] as const) {
		try {
			child.kill(signal);
		} catch {
			/* exit observation below is authoritative */
		}
		try {
			await bounded(child.exited, 2_000);
			return;
		} catch {
			/* escalate or report unconfirmed exit */
		}
	}
	throw new GjcCliUnavailableError("owned child termination failed: exit unconfirmed after bounded TERM/KILL waits");
}
