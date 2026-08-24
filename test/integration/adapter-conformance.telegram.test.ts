import { TelegramPlatform } from "../../src/adapter/telegram/platform";
import { type ConformancePlatform, describeAdapterConformance } from "../helpers/adapter-conformance";

/**
 * Runs the shared conformance suite against the REAL Telegram driver, with only
 * the network stubbed. Conformance over a hand-written fake would prove the fake
 * satisfies the port; this proves the shipped driver does.
 *
 * Telegram declares `at_least_once`: the Bot API offers no send-idempotency key,
 * so the runtime guarantees the send-before-commit ordering invariant and
 * suppresses duplicates within one process lifetime, but not across a crash. It
 * maps `ack` to a no-op and `typing` to sendChatAction.
 */
function createTelegramConformancePlatform(): ConformancePlatform {
	const sends: string[] = [];
	const nonces: Array<string | undefined> = [];
	const typings: string[] = [];
	let messageId = 0;

	const platform = new TelegramPlatform({
		token: "conformance-token",
		fetch: async (url, init) => {
			const body = JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>;
			if (url.endsWith("/sendMessage")) {
				sends.push(String(body.text ?? ""));
				// The driver must not forward a nonce; record what the port saw.
				nonces.push(body.nonce as string | undefined);
				messageId += 1;
				return new Response(JSON.stringify({ ok: true, result: { message_id: messageId } }), {
					headers: { "content-type": "application/json" },
				});
			}
			if (url.endsWith("/sendChatAction")) {
				typings.push(String(body.chat_id ?? ""));
				return new Response(JSON.stringify({ ok: true, result: true }), {
					headers: { "content-type": "application/json" },
				});
			}
			return new Response(JSON.stringify({ ok: true, result: [] }), {
				headers: { "content-type": "application/json" },
			});
		},
	});

	return { platform, sends, nonces, typings };
}

describeAdapterConformance({
	name: "telegram",
	consumerId: "gajaeway-telegram",
	chatId: "-1001234567890",
	surfaceId: "telegram:owner-dm",
	create: createTelegramConformancePlatform,
});
