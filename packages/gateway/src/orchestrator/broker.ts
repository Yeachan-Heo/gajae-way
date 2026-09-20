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

export const MIN_GJC_VERSION = "0.15.6";
export const HEALTH_PROBE_SESSION_ID = "00000000-0000-4000-8000-000000000000";
const COMMAND_TIMEOUT_MS = 30_000;
export type SpawnFn = typeof Bun.spawn;
export type GjcCommandRunner = CliRunner;
export type BrokerGenerationListener = (generation: number) => void;
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
): Promise<{ readonly version: string }> {
	const result = await run(["--version"], { timeoutMs: COMMAND_TIMEOUT_MS });
	const version = (result.stdout || result.stderr).match(/(?:gjc\/)?(\d+)\.(\d+)\.(\d+)/);
	const minimum = minimumVersion.match(/^(\d+)\.(\d+)\.(\d+)$/);
	if (result.exitCode !== 0 || !version || !minimum)
		throw new Error("gjc runtime preflight failed: invalid version response");
	for (let i = 1; i <= 3; i++) {
		if (Number(version[i]) < Number(minimum[i]))
			throw new Error(`gjc runtime preflight failed: requires gjc >= ${minimumVersion}`);
		if (Number(version[i]) > Number(minimum[i])) break;
	}
	if (sdk && !isHealthySessionList(await sdk(brokerHealthArgs(), { timeoutMs: COMMAND_TIMEOUT_MS }))) {
		throw new Error("gjc runtime preflight failed: invalid session-list envelope");
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
/** gjc prints usage and exits 2 on an unknown flag; every runtime failure exits 1 or prints a JSON envelope. */
export function isUsageRejection(result: CliResult): boolean {
	return result.exitCode === 2 && /unknown argument|USAGE/i.test(`${result.stdout}\n${result.stderr}`);
}

/** A client of the user's global runtime. No directory, daemon, lock, or session ownership. */
export class GlobalGjcClient {
	readonly executable: string;
	readonly agentDir: string;
	readonly discoveryPath: string;
	readonly cli: CliRunner;
	readonly #options: GlobalGjcClientOptions;
	readonly #spawn: SpawnFn;
	readonly #cwd: string;
	readonly #env: Record<string, string>;
	readonly #timeout: number;
	readonly #interval: number;
	readonly #initialBackoff: number;
	readonly #maxBackoff: number;
	readonly #listeners = new Set<BrokerGenerationListener>();
	readonly #children = new Set<ReturnType<SpawnFn>>();
	readonly #terminations = new Map<ReturnType<SpawnFn>, Promise<void>>();
	readonly #relays = new Set<() => void>();
	readonly #queue: Array<() => void> = [];
	#inflight = 0;
	#generation = 0;
	#identity: string | undefined;
	#available = false;
	#stopped = false;
	#started = false;
	#epoch = 0;
	#failures = 0;
	#timer: ReturnType<typeof setTimeout> | undefined;
	#starting: Promise<void> | undefined;
	#gjcVersion: string | undefined;

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
		this.#timeout = integer(options.healthProbeTimeoutMs, COMMAND_TIMEOUT_MS, 1);
		this.#interval = integer(options.healthIntervalMs, 5_000, 1);
		this.#initialBackoff = integer(options.reconnectBackoff?.initialMs, 250, 1);
		this.#maxBackoff = integer(options.reconnectBackoff?.maxMs, 10_000, this.#initialBackoff);
		integer(options.readinessAttempts, 20, 1);
		integer(options.readinessDelayMs, 100, 0);
		this.cli = (args, commandOptions) => this.#run(bindAgentDir(args, this.agentDir), commandOptions?.timeoutMs);
	}
	get generation(): number {
		return this.#generation;
	}
	get gjcVersion(): string | undefined {
		return this.#gjcVersion;
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
		);
		this.#gjcVersion = result.version;
	}
	async start(): Promise<void> {
		if (this.#starting) return this.#starting;
		if (this.#started) return;
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
		this.#started = false;
		this.#available = false;
		++this.#epoch;
		if (this.#timer) clearTimeout(this.#timer);
		this.#timer = undefined;
		for (const close of this.#relays) close();
		for (const wake of this.#queue.splice(0)) wake();
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
				this.#stopped = true;
				this.#started = false;
				this.#available = false;
				++this.#epoch;
				if (this.#timer) clearTimeout(this.#timer);
				this.#timer = undefined;
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
	async #observe(epoch: number): Promise<boolean> {
		try {
			const discovery = await this.#discovery();
			if (epoch !== this.#epoch || this.#stopped) return false;
			if (!discovery) {
				this.#available = false;
				return false;
			}
			const healthy = await bounded(
				this.#options.healthProbe
					? Promise.resolve(
							this.#options.healthProbe({
								agentDir: this.agentDir,
								cli: this.cli,
								discoveryPath: this.discoveryPath,
								isPidAlive: this.#options.isPidAlive ?? defaultPidAlive,
								timeoutMs: this.#timeout,
							}),
						)
					: probeBrokerEndpoint(discovery, this.#timeout),
				this.#timeout,
			);
			if (epoch !== this.#epoch || this.#stopped) return false;
			this.#assertAgentDirIdentity();
			this.#available = healthy;
			if (!healthy) return false;
			const identity = `${discovery.pid}|${discovery.url}|${discovery.token}`;
			if (identity !== this.#identity) {
				this.#identity = identity;
				this.#generation++;
				for (const listener of this.#listeners) {
					try {
						listener(this.#generation);
					} catch (error) {
						this.#log(error);
					}
				}
			}
			return true;
		} catch (error) {
			if (epoch === this.#epoch && !this.#stopped) {
				this.#available = false;
				this.#log(error);
			}
			return false;
		}
	}
	#schedule(epoch: number): void {
		if (this.#stopped || epoch !== this.#epoch) return;
		const wait = this.#available
			? this.#interval
			: Math.min(this.#initialBackoff * 2 ** Math.min(this.#failures++, 20), this.#maxBackoff);
		this.#timer = setTimeout(() => {
			this.#timer = undefined;
			void this.#observe(epoch).then((healthy) => {
				if (healthy) this.#failures = 0;
				else this.#log(new GjcCliUnavailableError("global broker unavailable; observing without repair"));
				this.#schedule(epoch);
			});
		}, wait);
	}
	#log(error: unknown): void {
		(this.#options.log ?? console.error)(sanitizeDiagnostic(error instanceof Error ? error.message : String(error)));
	}
	async #run(args: readonly string[], requestedTimeout?: number): Promise<CliResult> {
		this.#assertAgentDirIdentity();
		const timeout = integer(requestedTimeout, COMMAND_TIMEOUT_MS, 1);
		const deadline = Date.now() + timeout;
		while (this.#inflight >= 4) {
			let wake: (() => void) | undefined;
			try {
				await bounded(
					new Promise<void>((resolve) => {
						wake = resolve;
						this.#queue.push(resolve);
					}),
					Math.max(1, deadline - Date.now()),
				);
			} finally {
				if (wake) {
					const i = this.#queue.indexOf(wake);
					if (i >= 0) this.#queue.splice(i, 1);
				}
			}
			if (Date.now() >= deadline) throw new GjcCliUnavailableError("command queue timed out");
		}
		this.#assertAgentDirIdentity();
		if (this.#stopped || (this.#started && !this.#available))
			throw new GjcCliUnavailableError("client stopped or broker unavailable");
		this.#inflight++;
		try {
			const remaining = Math.max(1, deadline - Date.now());
			if (this.#options.command) return await bounded(this.#options.command(args, { timeoutMs: remaining }), remaining);
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
			this.#queue.shift()?.();
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
			stderr: "ignore",
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
				buffer += decoder.decode();
				if (buffer) yield buffer;
			} finally {
				reader.releaseLock();
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
	if (bound[1] === "session") bound.splice(2, 0, "--agent-dir", agentDir);
	else bound.push("--agent-dir", agentDir);
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
