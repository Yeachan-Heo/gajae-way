import type { AdapterPlatform, InboundMessage, SendResult } from "../runtime/protocol";
import type { DiscordPlatform } from "./platform";

/**
 * Binds the Discord driver to the shared adapter port.
 *
 * Discord declares `platform_nonce`: it accepts a caller-supplied nonce and
 * deduplicates server-side via `enforce_nonce`, so a retry of the same journal
 * event is idempotent. `ack` maps to a reaction; `typing` maps to the REST
 * typing call.
 *
 * This is the driver's port binding, not a compatibility layer: every adapter
 * needs one, and keeping it separate lets the Discord gateway client retain its
 * platform-shaped API while all delivery logic lives in the shared runtime.
 */
export function discordAdapterPlatform(platform: DiscordPlatform, channelId: string): AdapterPlatform {
	let unsubscribe: (() => void) | undefined;
	return {
		dedupe: "platform_nonce",
		async start(onMessage: (message: InboundMessage) => Promise<void>): Promise<void> {
			unsubscribe = platform.onMessage(async (message) => {
				await onMessage({
					platformMsgId: message.id,
					chatId: message.channelId,
					text: message.text,
					senderId: "",
					...(message.authorBot ? { authorBot: true } : {}),
				});
			});
			await platform.connect();
		},
		async stop(): Promise<void> {
			unsubscribe?.();
			unsubscribe = undefined;
			await platform.disconnect();
		},
		async send(chat: string, text: string, options: { readonly nonce?: string }): Promise<SendResult> {
			if (options.nonce === undefined) {
				// A nonce-capable platform must always receive one, or
				// `enforce_nonce` silently stops suppressing duplicates.
				throw new Error("Discord egress requires a deterministic wire nonce.");
			}
			const platformMsgId = await platform.send(chat, text, options.nonce);
			return { platformMsgId };
		},
		async ack(chat: string, platformMsgId: string): Promise<void> {
			await platform.react(chat, platformMsgId, "\u2705");
		},
		async typing(chat: string): Promise<void> {
			await platform.ackTyping(chat);
		},
		onDisconnect(): void {
			// The Discord client reconnects internally; there is no separate
			// disconnect signal to forward.
		},
		// Retained so callers can address the configured channel explicitly.
		...({ channelId } as Record<string, unknown>),
	};
}
