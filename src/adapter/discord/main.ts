import * as path from "node:path";
import { defaultConfig, parseWayConfig } from "../../config";
import { RpcClient, rpcResult, type JsonRpcClient } from "../../rpc-client";
import { loadWayCore } from "../../native-loader";
import { loadDiscordAdapterConfig, type DiscordAdapterConfig } from "./config";
import { DiscordOutbox } from "./outbox";
import { DiscordGatewayPlatform, validateDiscordToken, type DiscordFetch, type DiscordPlatform } from "./platform";
import { DiscordRouteHandler } from "./route";

const usage = `Usage:
  gajaeway-discord [--state-dir PATH] [--profile PATH] [--rpc-socket PATH]
  gajaeway-discord --check [--state-dir PATH] [--profile PATH] [--rpc-socket PATH]
  gajaeway-discord --help | --version`;

const GATEWAY_READY_POLL_MS = 100;
const GATEWAY_HEALTH_TIMEOUT_MS = 1_000;
const GATEWAY_READINESS_DIAGNOSTIC_INTERVAL_MS = 30_000;
const DISCORD_TYPING_KEEPALIVE_MS = 8_000;
const DISCORD_TYPING_KEEPALIVE_CAP_MS = 10 * 60_000;

export interface DiscordTypingKeepaliveClock {
	now(): number;
	setTimeout(callback: () => void | Promise<void>, milliseconds: number): unknown;
	clearTimeout(timer: unknown): void;
}

export interface DiscordAdapterDependencies {
	readonly environment?: NodeJS.ProcessEnv;
	readonly fetch?: DiscordFetch;
	rpcConnect?(socketPath: string): Promise<JsonRpcClient>;
	platformFactory?(config: DiscordAdapterConfig): DiscordPlatform;
	onError?(error: Error): void;
	/** Receives rate-limited gateway-readiness diagnostics; stderr is the default sink. */
	onDiagnostic?(message: string): void;
	/** Cancels startup while the adapter is intentionally waiting for a fenced gateway. */
	startupSignal?: AbortSignal;
	/** Test-only clock injection for deterministic typing keepalive scheduling. */
	typingKeepaliveClock?: DiscordTypingKeepaliveClock;
	waitForShutdown?(): Promise<void>;

}

export interface DiscordAdapterCheck {
	readonly discordUserId: string;
	readonly gatewayHealth: Record<string, unknown>;
}

export interface RunningDiscordAdapter {
	stop(): Promise<void>;
}

/** Starts the adapter only after a running gateway can durably accept ingress. */
export async function startDiscordAdapter(
	config: DiscordAdapterConfig,
	dependencies: DiscordAdapterDependencies = {},
): Promise<RunningDiscordAdapter> {
	const onError = dependencies.onError ?? (error => console.error(`gajaeway-discord failed: ${error.message}`));
	const onDiagnostic = dependencies.onDiagnostic ?? (message => console.error(`gajaeway-discord: ${message}`));
	const rpc = await waitForGatewayRunning(
		config.rpcSocketPath,
		dependencies.rpcConnect ?? RpcClient.connect,
		dependencies.startupSignal,
		onDiagnostic,
	);
	let platform: DiscordPlatform | undefined;
	let unsubscribe: (() => void) | undefined;
	let controller: AbortController | undefined;
	let typingKeepalive: DiscordTypingKeepalive | undefined;
	try {
		platform =
			dependencies.platformFactory?.(config) ??
			new DiscordGatewayPlatform({
				token: config.token,
				...(dependencies.fetch ? { fetch: dependencies.fetch } : {}),
				...(config.apiBaseUrl ? { apiBaseUrl: config.apiBaseUrl } : {}),
				...(config.gatewayUrl ? { gatewayUrl: config.gatewayUrl } : {}),
			});
		const activePlatform = platform;
		const activeController = new AbortController();
		controller = activeController;
		const activeTypingKeepalive = new DiscordTypingKeepalive({
			platform: activePlatform,
			signal: activeController.signal,
			onError,
			clock: dependencies.typingKeepaliveClock,
		});
		typingKeepalive = activeTypingKeepalive;
		const router = new DiscordRouteHandler({
			route: config.route,
			rpc,
			platform: activePlatform,
			acknowledgement: { budgetMs: config.ackBudgetMs },
			onAccepted: message => activeTypingKeepalive.begin(message.channelId, message.id),
			onAcknowledged: acknowledgement => activeTypingKeepalive.start(config.route.channelId, acknowledgement.messageId),
			onAcknowledgementFailed: message => activeTypingKeepalive.cancel(message.channelId, message.id),
		});
		const outbox = new DiscordOutbox({
			rpc,
			platform: activePlatform,
			route: config.route,
			claimTtlMs: config.claimTtlMs,
			readWaitMs: config.readWaitMs,
			hooks: { afterSendBeforeCommit: () => activeTypingKeepalive.stop(config.route.channelId) },
			onError,
		});
		unsubscribe = activePlatform.onMessage(async message => {
			try {
				await router.handle(message);
			} catch (error) {
				onError(asError(error));
			}
		});
		await activePlatform.connect();
		const outboxRun = outbox.run(activeController.signal);
		let stopped = false;
		return {
			async stop(): Promise<void> {
				if (stopped) return;
				stopped = true;
				activeController.abort();
				activeTypingKeepalive.dispose();
				unsubscribe?.();
				try {
					await activePlatform.disconnect();
				} finally {
					rpc.close();
					await outboxRun;
				}
			},
		};
	} catch (error) {
		typingKeepalive?.dispose();
		controller?.abort();
		unsubscribe?.();
		try {
			await platform?.disconnect();
		} finally {
			rpc.close();
		}
		throw error;
	}
}

class DiscordTypingKeepalive {
	readonly #platform: DiscordPlatform;
	readonly #signal: AbortSignal;
	readonly #onError: (error: Error) => void;
	readonly #clock: DiscordTypingKeepaliveClock;
	readonly #channels = new Map<string, TypingKeepaliveChannel>();
	#stopped = false;

	constructor(options: {
		readonly platform: DiscordPlatform;
		readonly signal: AbortSignal;
		readonly onError: (error: Error) => void;
		readonly clock?: DiscordTypingKeepaliveClock;
	}) {
		this.#platform = options.platform;
		this.#signal = options.signal;
		this.#onError = options.onError;
		this.#clock = options.clock ?? systemTypingKeepaliveClock;
		if (this.#signal.aborted) {
			this.#stopped = true;
		} else {
			this.#signal.addEventListener("abort", this.dispose, { once: true });
		}
	}

	begin(channelId: string, messageId: string): void {
		if (this.#stopped || this.#signal.aborted) return;
		const channel = this.#channels.get(channelId) ?? { admissions: new Set<string>() };
		channel.admissions.add(messageId);
		this.#channels.set(channelId, channel);
	}

	start(channelId: string, messageId: string): void {
		if (this.#stopped || this.#signal.aborted) return;
		const channel = this.#channels.get(channelId);
		if (!channel || !channel.admissions.delete(messageId)) return;
		channel.deadlineAt = this.#clock.now() + DISCORD_TYPING_KEEPALIVE_CAP_MS;
		if (channel.timer === undefined) this.schedule(channelId, channel);
	}

	cancel(channelId: string, messageId: string): void {
		const channel = this.#channels.get(channelId);
		if (!channel || !channel.admissions.delete(messageId)) return;
		if (channel.deadlineAt === undefined && channel.admissions.size === 0) this.#channels.delete(channelId);
	}

	stop(channelId: string): void {
		const channel = this.#channels.get(channelId);
		if (!channel) return;
		if (channel.timer !== undefined) this.#clock.clearTimeout(channel.timer);
		this.#channels.delete(channelId);
	}

	dispose = (): void => {
		if (this.#stopped) return;
		this.#stopped = true;
		this.#signal.removeEventListener("abort", this.dispose);
		for (const channelId of [...this.#channels.keys()]) this.stop(channelId);
	};

	private schedule(channelId: string, channel: TypingKeepaliveChannel): void {
		if (!this.isCurrent(channelId, channel) || channel.deadlineAt === undefined) return;
		const remainingMs = channel.deadlineAt - this.#clock.now();
		if (remainingMs <= 0) {
			this.stop(channelId);
			return;
		}
		channel.timer = this.#clock.setTimeout(async () => {
			await this.tick(channelId, channel);
		}, Math.min(DISCORD_TYPING_KEEPALIVE_MS, remainingMs));
	}

	private async tick(channelId: string, channel: TypingKeepaliveChannel): Promise<void> {
		if (!this.isCurrent(channelId, channel) || channel.deadlineAt === undefined) return;
		channel.timer = undefined;
		if (this.#clock.now() >= channel.deadlineAt) {
			this.stop(channelId);
			return;
		}
		this.schedule(channelId, channel);
		try {
			await this.#platform.ackTyping(channelId);
		} catch (error) {
			this.reportFailure(channelId, error);
		}
	}

	private isCurrent(channelId: string, channel: TypingKeepaliveChannel): boolean {
		return !this.#stopped && !this.#signal.aborted && this.#channels.get(channelId) === channel;
	}

	private reportFailure(channelId: string, error: unknown): void {
		const cause = asError(error);
		try {
			this.#onError(new Error(`Discord typing keepalive failed for channel ${channelId}: ${cause.message}`, { cause }));
		} catch {
			// Reporting must not terminate the adapter's ingress or egress loops.
		}
	}
}

interface TypingKeepaliveChannel {
	readonly admissions: Set<string>;
	deadlineAt?: number;
	timer?: unknown;
}

const systemTypingKeepaliveClock: DiscordTypingKeepaliveClock = {
	now: Date.now,
	setTimeout: (callback, milliseconds) =>
		setTimeout(() => {
			void callback();
		}, milliseconds),
	clearTimeout: timer => clearTimeout(timer as ReturnType<typeof setTimeout>),
};

async function waitForGatewayRunning(
	socketPath: string,
	rpcConnect: (socketPath: string) => Promise<JsonRpcClient>,
	signal: AbortSignal | undefined,
	onDiagnostic: (message: string) => void,
): Promise<JsonRpcClient> {
	const diagnostics = createGatewayReadinessDiagnostics(onDiagnostic);
	let rpc: JsonRpcClient | undefined;
	try {
		for (;;) {
			if (signal?.aborted) throw adapterStartupAborted();
			if (!rpc) {
				try {
					rpc = await rpcConnect(socketPath);
				} catch (error) {
					diagnostics.transportOrProtocol(`could not connect to the local gateway: ${asError(error).message}`);
					await sleepForGatewayReadiness(signal);
					continue;
				}
			}
			if (signal?.aborted) throw adapterStartupAborted();
			try {
				const response = await rpc.request("way.health", {}, { timeoutMs: GATEWAY_HEALTH_TIMEOUT_MS });
				if (gatewayIsRunning(response.result)) return rpc;
				if (gatewayIsVerifying(response.result)) {
					diagnostics.verifying();
				} else if (response.error) {
					diagnostics.transportOrProtocol(`way.health returned ${response.error.code} ${response.error.message}`);
					rpc.close();
					rpc = undefined;
				} else {
					diagnostics.notRunning(response.result);
				}
			} catch (error) {
				diagnostics.transportOrProtocol(`way.health request failed: ${asError(error).message}`);
				rpc?.close();
				rpc = undefined;
			}
			await sleepForGatewayReadiness(signal);
		}
	} catch (error) {
		rpc?.close();
		throw error;
	}
}

function createGatewayReadinessDiagnostics(onDiagnostic: (message: string) => void): {
	verifying(): void;
	transportOrProtocol(detail: string): void;
	notRunning(health: unknown): void;
} {
	const lastAt = new Map<string, number>();
	const emit = (kind: string, message: string): void => {
		const now = Date.now();
		const previous = lastAt.get(kind);
		if (previous !== undefined && now - previous < GATEWAY_READINESS_DIAGNOSTIC_INTERVAL_MS) return;
		lastAt.set(kind, now);
		onDiagnostic(message);
	};
	return {
		verifying: () => emit("verifying", "gateway verifying (expected wait); delaying Discord connection until healthy/running."),
		transportOrProtocol: detail => emit("transport_or_protocol", `gateway transport/protocol error while waiting: ${detail}`),
		notRunning: health => emit("not_running", `gateway is not healthy/running while waiting: ${gatewayHealthSummary(health)}`),
	};
}

async function sleepForGatewayReadiness(signal: AbortSignal | undefined): Promise<void> {
	await Bun.sleep(GATEWAY_READY_POLL_MS);
	if (signal?.aborted) throw adapterStartupAborted();
}

function adapterStartupAborted(): Error {
	const error = new Error("Discord adapter startup was aborted while waiting for gateway readiness.");
	error.name = "AbortError";
	return error;
}

function gatewayIsRunning(value: unknown): value is Record<string, unknown> {
	return isRecord(value) && value.status === "healthy" && value.state === "running";
}

function gatewayIsVerifying(value: unknown): value is Record<string, unknown> {
	return isRecord(value) && value.state === "verifying";
}

function gatewayHealthSummary(value: unknown): string {
	if (!isRecord(value)) return "invalid way.health result";
	const status = typeof value.status === "string" ? value.status : "unknown";
	const state = typeof value.state === "string" ? value.state : "unknown";
	return `status=${status}, state=${state}`;
}

/** `--check`: verify the credential with Discord and confirm the UDS gateway is healthy and running. */
export async function checkDiscordAdapter(
	config: DiscordAdapterConfig,
	dependencies: Pick<DiscordAdapterDependencies, "fetch" | "rpcConnect"> = {},
): Promise<DiscordAdapterCheck> {
	const rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(config.rpcSocketPath);
	try {
		const [user, healthResponse] = await Promise.all([
			validateDiscordToken(config.token, dependencies.fetch, config.apiBaseUrl),
			rpc.request("way.health", {}, { timeoutMs: 5_000 }),
		]);
		const health = rpcResult<unknown>(healthResponse, "way.health");
		if (!gatewayIsRunning(health)) {
			throw new Error("Gateway way.health did not report healthy running status.");
		}
		return { discordUserId: user.id, gatewayHealth: health };
	} finally {
		rpc.close();
	}
}

export async function runDiscordAdapter(
	arguments_ = process.argv.slice(2),
	dependencies: DiscordAdapterDependencies = {},
): Promise<void> {
	if (arguments_.includes("--help") || arguments_.includes("-h")) {
		console.log(usage);
		return;
	}
	if (arguments_.includes("--version") || arguments_.includes("-V")) {
		console.log(`gajaeway-discord ${loadWayCore().healthInfo().version}`);
		return;
	}
	const environment = dependencies.environment ?? process.env;
	const parsed = parseWayConfig(arguments_, defaultConfig(environment));
	const command = parseAdapterArguments(parsed.remaining);
	let config = loadDiscordAdapterConfig({
		environment,
		stateDir: parsed.config.stateDir,
		profilePath: parsed.config.profilePath,
	});
	if (command.rpcSocketPath) config = { ...config, rpcSocketPath: path.resolve(command.rpcSocketPath) };
	if (command.check) {
		const checked = await checkDiscordAdapter(config, dependencies);
		console.log(JSON.stringify({ status: "ok", discord_user_id: checked.discordUserId, gateway: checked.gatewayHealth }));
		return;
	}
	const adapter = await startDiscordAdapter(config, dependencies);
	try {
		await (dependencies.waitForShutdown ?? waitForShutdown)();
	} finally {
		await adapter.stop();
	}
}

function parseAdapterArguments(arguments_: readonly string[]): { check: boolean; rpcSocketPath?: string } {
	let check = false;
	let rpcSocketPath: string | undefined;
	for (let index = 0; index < arguments_.length; index += 1) {
		const argument = arguments_[index];
		if (argument === "--check") {
			if (check) throw new Error(`--check may only be supplied once.\n${usage}`);
			check = true;
			continue;
		}
		if (argument === "--rpc-socket") {
			const value = arguments_[index + 1];
			if (!value) throw new Error(`--rpc-socket requires a value.\n${usage}`);
			rpcSocketPath = value;
			index += 1;
			continue;
		}
		throw new Error(`Unknown argument: ${argument}\n${usage}`);
	}
	return { check, ...(rpcSocketPath ? { rpcSocketPath } : {}) };
}

async function waitForShutdown(): Promise<void> {
	await new Promise<void>(resolve => {
		const stop = () => resolve();
		process.once("SIGINT", stop);
		process.once("SIGTERM", stop);
	});
}

function asError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

if (import.meta.main) {
	try {
		await runDiscordAdapter();
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}
