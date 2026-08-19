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

export interface DiscordAdapterDependencies {
	readonly environment?: NodeJS.ProcessEnv;
	readonly fetch?: DiscordFetch;
	rpcConnect?(socketPath: string): Promise<JsonRpcClient>;
	platformFactory?(config: DiscordAdapterConfig): DiscordPlatform;
	onError?(error: Error): void;
	waitForShutdown?(): Promise<void>;
}

export interface DiscordAdapterCheck {
	readonly discordUserId: string;
	readonly gatewayHealth: Record<string, unknown>;
}

export interface RunningDiscordAdapter {
	stop(): Promise<void>;
}

/** Starts the adapter as a pure UDS RPC client with no durable local state. */
export async function startDiscordAdapter(
	config: DiscordAdapterConfig,
	dependencies: DiscordAdapterDependencies = {},
): Promise<RunningDiscordAdapter> {
	const rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(config.rpcSocketPath);
	const platform =
		dependencies.platformFactory?.(config) ??
		new DiscordGatewayPlatform({
			token: config.token,
			...(dependencies.fetch ? { fetch: dependencies.fetch } : {}),
			...(config.apiBaseUrl ? { apiBaseUrl: config.apiBaseUrl } : {}),
			...(config.gatewayUrl ? { gatewayUrl: config.gatewayUrl } : {}),
		});
	const onError = dependencies.onError ?? (error => console.error(`gajaeway-discord failed: ${error.message}`));
	const router = new DiscordRouteHandler({
		route: config.route,
		rpc,
		platform,
		acknowledgement: { budgetMs: config.ackBudgetMs },
	});
	const outbox = new DiscordOutbox({
		rpc,
		platform,
		route: config.route,
		claimTtlMs: config.claimTtlMs,
		readWaitMs: config.readWaitMs,
		onError,
	});
	const controller = new AbortController();
	const unsubscribe = platform.onMessage(async message => {
		try {
			await router.handle(message);
		} catch (error) {
			onError(asError(error));
		}
	});
	try {
		await platform.connect();
	} catch (error) {
		unsubscribe();
		await platform.disconnect();
		rpc.close();
		throw error;
	}
	const outboxRun = outbox.run(controller.signal);
	let stopped = false;
	return {
		async stop(): Promise<void> {
			if (stopped) return;
			stopped = true;
			controller.abort();
			unsubscribe();
			await platform.disconnect();
			rpc.close();
			await outboxRun;
		},
	};
}

/** `--check`: verify the credential with Discord and confirm the UDS gateway is healthy. */
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
		if (!isRecord(health) || health.status !== "healthy") {
			throw new Error("Gateway way.health did not report healthy status.");
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
