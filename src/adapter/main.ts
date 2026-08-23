import { RpcClient, type JsonRpcClient } from "../rpc-client";
import { loadDiscordAdapterConfig } from "./discord/config";
import { DiscordOutbox } from "./discord/outbox";
import { DiscordGatewayPlatform, type DiscordPlatform } from "./discord/platform";
import { DiscordRouteHandler } from "./discord/route";
import { loadTelegramAdapterConfig, type TelegramAdapterConfig } from "./telegram/config";
import { TelegramPlatform, validateTelegramToken } from "./telegram/platform";

export interface UnifiedAdapterDependencies {
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
	const environment = process.env;
	const discord = loadDiscordAdapterConfig({ environment });
	const telegram = loadTelegramAdapterConfig({ environment });
	if (arguments_.includes("--check")) {
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
