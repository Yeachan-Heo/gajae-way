import { type JsonRpcClient, RpcClient } from "../../rpc-client";
import { AdapterEgress } from "../runtime/egress";
import { AdapterIngress } from "../runtime/ingress";
import type { AdapterPlatform } from "../runtime/protocol";
import { AdapterProtocolUnsupportedError } from "../runtime/protocol";
import { assertGatewayReady, assertSurfaceUsable } from "../runtime/session";
import { loadTelegramAdapterConfig, TELEGRAM_CONSUMER_ID, type TelegramAdapterConfig } from "./config";
import { TelegramPlatform } from "./platform";

export interface TelegramAdapterDependencies {
	readonly config?: TelegramAdapterConfig;
	readonly platformFactory?: (config: TelegramAdapterConfig) => AdapterPlatform;
	readonly rpcConnect?: (socketPath: string) => Promise<JsonRpcClient>;
	readonly onError?: (error: Error) => void;
	readonly startupSignal?: AbortSignal;
}

/**
 * Telegram adapter entrypoint.
 *
 * Built entirely on the shared adapter runtime: this file wires configuration
 * to the port and owns no delivery logic of its own. That is the point of the
 * extraction - a third adapter should be a driver plus this much wiring, not a
 * re-implementation of the protocol.
 */
export async function runTelegramAdapter(dependencies: TelegramAdapterDependencies = {}): Promise<void> {
	const config = dependencies.config ?? loadTelegramAdapterConfig();
	const onError =
		dependencies.onError ?? ((error: Error) => process.stderr.write(`gajaeway-telegram: ${error.message}\n`));
	const connect = dependencies.rpcConnect ?? RpcClient.connect;
	const rpc = await connect(config.rpcSocketPath);
	const controller = new AbortController();
	let platform: AdapterPlatform | undefined;
	try {
		// Readiness and protocol negotiation happen before anything is claimed,
		// so a fenced gateway or mismatched build never settles a delivery.
		await assertGatewayReady(rpc);
		await assertSurfaceUsable(rpc, config.surfaceId);

		const activePlatform =
			dependencies.platformFactory?.(config) ??
			new TelegramPlatform({
				token: config.token,
				...(config.apiBaseUrl === undefined ? {} : { apiBaseUrl: config.apiBaseUrl }),
			});
		platform = activePlatform;

		const ingress = new AdapterIngress({
			rpc,
			platform: activePlatform,
			surfaceId: config.surfaceId,
			chatId: config.chatId,
			ackBudgetMs: config.ackBudgetMs,
			onAcknowledgementDiagnostic: (message, reason) =>
				onError(new Error(`Telegram typing unavailable for message ${message.platformMsgId}: ${reason}`)),
		});
		const egress = new AdapterEgress({
			rpc,
			platform: activePlatform,
			consumerId: TELEGRAM_CONSUMER_ID,
			surfaceId: config.surfaceId,
			chatId: config.chatId,
			claimTtlMs: config.claimTtlMs,
			readWaitMs: config.readWaitMs,
			onError,
		});

		activePlatform.onDisconnect((reason) => onError(new Error(`Telegram transport disconnected: ${reason}`)));
		await activePlatform.start(async (message) => {
			try {
				await ingress.handle(message);
			} catch (error) {
				onError(error instanceof Error ? error : new Error(String(error)));
			}
		});

		if (dependencies.startupSignal?.aborted) return;
		await egress.run(controller.signal);
	} catch (error) {
		if (error instanceof AdapterProtocolUnsupportedError) {
			onError(error);
			throw error;
		}
		throw error;
	} finally {
		controller.abort();
		await platform?.stop().catch(() => undefined);
		rpc.close();
	}
}

if (import.meta.main) {
	runTelegramAdapter().catch((error: unknown) => {
		process.stderr.write(`gajaeway-telegram failed: ${error instanceof Error ? error.message : String(error)}\n`);
		process.exit(1);
	});
}
