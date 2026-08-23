import * as path from "node:path";
import { defaultConfig, parseWayConfig } from "../config";
import { RpcClient, type JsonRpcClient } from "../rpc-client";
import { loadDiscordAdapterConfig } from "./discord/config";
import { DiscordOutbox } from "./discord/outbox";
import { DiscordGatewayPlatform, type DiscordPlatform } from "./discord/platform";
import { DiscordRouteHandler } from "./discord/route";
import { loadTelegramAdapterConfig, type TelegramAdapterConfig } from "./telegram/config";
import { TelegramPlatform, validateTelegramToken } from "./telegram/platform";

export interface UnifiedAdapterDependencies {
	readonly environment?: NodeJS.ProcessEnv;
	rpcConnect?(socketPath: string): Promise<JsonRpcClient>;
	platformFactory?(config: unknown): DiscordPlatform;
	fetch?: typeof fetch;
	onError?(error: Error): void;
	waitForShutdown?(): Promise<void>;
}

export interface RunningUnifiedAdapter {
	stop(): Promise<void>;
}

/** Runs all enabled channel peers in one adapter process. */
export async function startUnifiedAdapter(options: { discord?: ReturnType<typeof loadDiscordAdapterConfig>; telegram?: TelegramAdapterConfig; dependencies?: UnifiedAdapterDependencies } = {}): Promise<RunningUnifiedAdapter> {
	const dependencies = options.dependencies ?? {};
	const running: Array<RunningUnifiedAdapter> = [];
	if (options.discord) running.push(await startDiscordPeer(options.discord, dependencies));
	if (options.telegram?.enabled) running.push(await startTelegramPeer(options.telegram, dependencies));
	return { async stop() { for (const peer of running.reverse()) await peer.stop(); } };
}

async function startDiscordPeer(config: ReturnType<typeof loadDiscordAdapterConfig>, dependencies: UnifiedAdapterDependencies): Promise<RunningUnifiedAdapter> {
	const rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(config.rpcSocketPath);
	const platform = new DiscordGatewayPlatform({ token: config.token, fetch: dependencies.fetch });
	const bot = await platform.getCurrentUser();
	const route = new DiscordRouteHandler({ routes: config.routes, rpc, platform, botUserId: bot.id, allowBots: config.allowBots, blockedAuthorIds: config.blockedAuthorIds });
	const outbox = new DiscordOutbox({ rpc, platform, routes: config.routes, unattributedDelivery: config.unattributedDelivery, unattributedRoute: config.unattributedRoute, claimTtlMs: config.claimTtlMs, readWaitMs: config.readWaitMs });
	const unsubscribe = platform.onMessage(message => { void route.handle(message).catch(error => dependencies.onError?.(error instanceof Error ? error : new Error(String(error)))); });
	await platform.connect();
	const controller = new AbortController();
	const draining = outbox.run(controller.signal);
	return { async stop() { controller.abort(); unsubscribe(); await platform.disconnect(); rpc.close(); await draining; } };
}

async function startTelegramPeer(config: TelegramAdapterConfig, dependencies: UnifiedAdapterDependencies): Promise<RunningUnifiedAdapter> {
	const rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(config.rpcSocketPath);
	const platform = new TelegramPlatform({ token: config.token, fetch: dependencies.fetch, stateDir: config.stateDir });
	const bot = await platform.getCurrentUser();
	const route = new DiscordRouteHandler({ routes: config.routes, rpc, platform, botUserId: bot.id, allowBots: config.allowBots, blockedAuthorIds: config.blockedAuthorIds });
	const outbox = new DiscordOutbox({ rpc, platform, routes: config.routes, unattributedDelivery: "suppress", claimTtlMs: 5_000, readWaitMs: 1_000 });
	const unsubscribe = platform.onMessage(message => { void route.handle(message).catch(error => dependencies.onError?.(error instanceof Error ? error : new Error(String(error)))); });
	await platform.connect();
	const controller = new AbortController();
	const draining = outbox.run(controller.signal);
	return { async stop() { controller.abort(); unsubscribe(); await platform.disconnect(); rpc.close(); await draining; } };
}

export async function runUnifiedAdapter(arguments_: readonly string[] = process.argv.slice(2), dependencies: UnifiedAdapterDependencies = {}): Promise<void> {
	const environment = dependencies.environment ?? process.env;
	// The documented invocation (and the shipped systemd units) pass --state-dir,
	// --profile, and optionally --rpc-socket. Parse them and thread them into both
	// channel config loaders: resolving config from the environment alone silently
	// ignores the operator's flags and loads the wrong profile.
	const parsed = parseWayConfig(arguments_, defaultConfig(environment));
	let rpcSocketOverride: string | undefined;
	for (let index = 0; index < parsed.remaining.length; index += 1) {
		const argument = parsed.remaining[index];
		if (argument === "--check") continue;
		if (argument === "--rpc-socket") {
			const value = parsed.remaining[index + 1];
			if (!value) throw new Error("--rpc-socket requires a value.");
			rpcSocketOverride = path.resolve(value);
			index += 1;
			continue;
		}
		throw new Error(`Unknown argument: ${argument}`);
	}
	const loaderOptions = { environment, stateDir: parsed.config.stateDir, profilePath: parsed.config.profilePath };
	let discord = loadDiscordAdapterConfig(loaderOptions);
	let telegram = loadTelegramAdapterConfig(loaderOptions);
	if (rpcSocketOverride) {
		discord = { ...discord, rpcSocketPath: rpcSocketOverride };
		telegram = { ...telegram, rpcSocketPath: rpcSocketOverride };
	}
	if (parsed.remaining.includes("--check")) {
		const checks: Record<string, unknown> = { status: "ok" };
		if (discord.token) checks.discord = await (await import("./discord/main")).checkDiscordAdapter(discord, dependencies as any);
		if (telegram.enabled) checks.telegram = await validateTelegramToken(telegram.token, dependencies.fetch);
		console.log(JSON.stringify(checks));
		return;
	}
	const adapter = await startUnifiedAdapter({ discord, telegram, dependencies });
	try {
		await (dependencies.waitForShutdown ?? (() => new Promise<void>(resolve => process.once("SIGTERM", resolve))))();
	} finally {
		await adapter.stop();
	}
}

if (import.meta.main) await runUnifiedAdapter();
