import { RpcClient, type JsonRpcClient } from "../../rpc-client";
import { loadDiscordAdapterConfig } from "../discord/config";
import { DiscordOutbox } from "../discord/outbox";
import { DiscordGatewayPlatform, type DiscordPlatform } from "../discord/platform";
import { DiscordRouteHandler } from "../discord/route";
import { loadTelegramAdapterConfig, type TelegramAdapterConfig } from "./config";
import { TelegramPlatform, validateTelegramToken } from "./platform";

export interface TelegramAdapterDependencies {
	rpcConnect?(socketPath: string): Promise<JsonRpcClient>;
	platformFactory?(config: TelegramAdapterConfig): DiscordPlatform;
	fetch?: typeof fetch;
	onError?(error: Error): void;
}

export async function startTelegramAdapter(config: TelegramAdapterConfig, dependencies: TelegramAdapterDependencies = {}): Promise<{ stop(): Promise<void> }> {
	if (!config.enabled) return { async stop() {} };
	const rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(config.rpcSocketPath);
	const platform = dependencies.platformFactory?.(config) ?? new TelegramPlatform({ token: config.token, fetch: dependencies.fetch, stateDir: config.stateDir });
	const botUser = await platform.getCurrentUser();
	const router = new DiscordRouteHandler({ routes: config.routes, rpc, platform, botUserId: botUser.id, allowBots: config.allowBots, blockedAuthorIds: config.blockedAuthorIds });
	const outbox = new DiscordOutbox({ rpc, platform, routes: config.routes, unattributedDelivery: "suppress", claimTtlMs: 5_000, readWaitMs: 1_000 });
	const unsubscribe = platform.onMessage(message => {
		void router.handle(message).catch(error => dependencies.onError?.(error instanceof Error ? error : new Error(String(error))));
	});
	await platform.connect();
	const controller = new AbortController();
	const running = outbox.run(controller.signal);
	return {
		async stop(): Promise<void> {
			controller.abort();
			unsubscribe();
			await platform.disconnect();
			rpc.close();
			await running;
		},
	};
}

export async function checkTelegramAdapter(config: TelegramAdapterConfig, dependencies: Pick<TelegramAdapterDependencies, "fetch" | "rpcConnect"> = {}): Promise<{ telegramUserId: string }> {
	const user = await validateTelegramToken(config.token, dependencies.fetch);
	const rpc = await (dependencies.rpcConnect ?? RpcClient.connect)(config.rpcSocketPath);
	rpc.close();
	return { telegramUserId: user.id };
}

export async function runTelegramAdapter(arguments_: readonly string[] = process.argv.slice(2), dependencies: TelegramAdapterDependencies = {}): Promise<void> {
	const environment = process.env;
	const config = loadTelegramAdapterConfig({ environment, stateDir: environment.GAJAEWAY_STATE_DIR });
	if (arguments_.includes("--check")) {
		console.log(JSON.stringify({ status: "ok", ...(await checkTelegramAdapter(config, dependencies)) }));
		return;
	}
	const adapter = await startTelegramAdapter(config, dependencies);
	await new Promise<void>(resolve => process.once("SIGTERM", resolve));
	await adapter.stop();
}

if (import.meta.main) await runTelegramAdapter();
