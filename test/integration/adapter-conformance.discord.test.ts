import type { AdapterPlatform, SendResult } from "../../src/adapter/runtime/protocol";
import { type ConformancePlatform, describeAdapterConformance } from "../helpers/adapter-conformance";

/**
 * Discord declares `platform_nonce`: it accepts a caller-supplied nonce and
 * deduplicates server-side via `enforce_nonce`, so retries of the same journal
 * event are idempotent.
 */
function createDiscordConformancePlatform(): ConformancePlatform {
	const sends: string[] = [];
	const nonces: Array<string | undefined> = [];
	const typings: string[] = [];
	const platform: AdapterPlatform = {
		dedupe: "platform_nonce",
		async start() {},
		async stop() {},
		async send(_chatId, text, options): Promise<SendResult> {
			sends.push(text);
			nonces.push(options.nonce);
			return { platformMsgId: `discord-${sends.length}` };
		},
		async ack() {},
		async typing(chatId) {
			typings.push(chatId);
		},
		onDisconnect() {},
	};
	return { platform, sends, nonces, typings };
}

describeAdapterConformance({
	name: "discord",
	consumerId: "gajaeway-discord",
	chatId: "123456789012345678",
	surfaceId: "discord:owner-dm",
	create: createDiscordConformancePlatform,
});
