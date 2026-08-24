import { expect, test } from "bun:test";
import { TelegramPlatform, TelegramPlatformError, toInboundMessage } from "../../src/adapter/telegram/platform";

function jsonResponse(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
	return new Response(JSON.stringify(body), {
		status,
		headers: { "content-type": "application/json", ...headers },
	});
}

test("Telegram declares at_least_once and ignores a nonce it cannot honour", async () => {
	const bodies: string[] = [];
	const platform = new TelegramPlatform({
		token: "test-token",
		fetch: async (_url, init) => {
			bodies.push(String(init?.body ?? ""));
			return jsonResponse({ ok: true, result: { message_id: 77 } });
		},
	});

	expect(platform.dedupe).toBe("at_least_once");
	const sent = await platform.send("-100123", "hello", { nonce: "deadbeef" });

	expect(sent.platformMsgId).toBe("77");
	// The nonce must not be forwarded: implying server-side dedupe Telegram
	// cannot provide would overstate the delivery guarantee.
	expect(bodies[0]).not.toContain("deadbeef");
});

test("a poll pass routes text messages and always advances the offset", async () => {
	const calls: string[] = [];
	const platform = new TelegramPlatform({
		token: "test-token",
		fetch: async (_url, init) => {
			const body = String(init?.body ?? "");
			calls.push(body);
			if (calls.length === 1) {
				return jsonResponse({
					ok: true,
					result: [
						{ update_id: 10, message: { message_id: 1, chat: { id: -100 }, from: { id: 5 }, text: "route me" } },
						// A non-text update must still advance the offset, or the
						// same batch is returned forever.
						{ update_id: 11, edited_message: { message_id: 2 } },
					],
				});
			}
			return jsonResponse({ ok: true, result: [] });
		},
	});

	const seen: string[] = [];
	expect(await platform.pollOnce(async (message) => void seen.push(message.text))).toBe(1);
	expect(seen).toEqual(["route me"]);

	await platform.pollOnce(async () => {});
	// Offset advanced past BOTH updates, including the unrouted one.
	expect(calls[1]).toContain('"offset":12');
});

test("a rate-limited call honours retry_after from the parameters block", async () => {
	const waits: number[] = [];
	let calls = 0;
	const platform = new TelegramPlatform({
		token: "test-token",
		sleep: async (ms) => void waits.push(ms),
		fetch: async () => {
			calls += 1;
			if (calls === 1) {
				return jsonResponse({ ok: false, description: "Too Many Requests", parameters: { retry_after: 0.5 } }, 429);
			}
			return jsonResponse({ ok: true, result: { message_id: 3 } });
		},
	});

	await platform.send("-100123", "retried", {});

	expect(calls).toBe(2);
	expect(waits).toEqual([500]);
});

test("an API-level failure surfaces the Telegram description", async () => {
	const platform = new TelegramPlatform({
		token: "test-token",
		fetch: async () => jsonResponse({ ok: false, description: "chat not found" }, 400),
	});

	await expect(platform.send("-100123", "nope", {})).rejects.toBeInstanceOf(TelegramPlatformError);
});

test("inbound parsing rejects shapes that cannot be routed", () => {
	expect(toInboundMessage({ message_id: 1, chat: { id: -1 }, text: "ok" })?.text).toBe("ok");
	expect(toInboundMessage({ message_id: 1, chat: { id: -1 }, text: "   " })).toBeUndefined();
	expect(toInboundMessage({ message_id: 1, text: "no chat" })).toBeUndefined();
	expect(toInboundMessage({ chat: { id: -1 }, text: "no id" })).toBeUndefined();
	expect(
		toInboundMessage({ message_id: 1, chat: { id: -1 }, from: { id: 2, is_bot: true }, text: "echo" })?.authorBot,
	).toBe(true);
});
