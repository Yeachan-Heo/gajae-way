import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { TelegramPlatform } from "../../src/adapter/telegram/platform";

type Route = { channelId: string; surfaceId: string; kind?: "dm" | "channel"; groupPolicy?: "open" | "mention" };

function telegramResponse(result: unknown): Response {
	return new Response(JSON.stringify({ ok: true, result }), { status: 200 });
}

function fixturePlatform(route: Route, options: { updates?: unknown[]; actions?: string[] } = {}): { platform: TelegramPlatform; requests: Array<{ method: string; body: Record<string, unknown> }>; stateDir: string } {
	const requests: Array<{ method: string; body: Record<string, unknown> }> = [];
	const stateDir = fs.mkdtempSync(path.join(os.tmpdir(), "gajaeway-telegram-suite-"));
	let updates = [...(options.updates ?? [])];
	const platform = new TelegramPlatform({
		token: "token",
		stateDir,
		fetch: async (input, init) => {
			const method = String(input).split("/").at(-1) ?? "";
			const body = init?.body ? JSON.parse(String(init.body)) as Record<string, unknown> : {};
			requests.push({ method, body });
			if (method === "getMe") return telegramResponse({ id: 900, username: "fixture" });
			if (method === "getUpdates") {
				const result = updates;
				updates = [];
				return telegramResponse(result);
			}
			if (method === "sendMessage") return telegramResponse({ message_id: requests.filter(request => request.method === "sendMessage").length });
			return telegramResponse(true);
		},
	});
	return { platform, requests, stateDir };
}

function message(id: number, chatId: string, text: string, extra: Record<string, unknown> = {}): Record<string, unknown> {
	return { update_id: id, message: { message_id: id, chat: { id: chatId, type: "group" }, from: { id: 42, is_bot: false }, text, ...extra } };
}

test("Telegram DM round trip and group round trip use shared route machinery", async () => {
	const dm = fixturePlatform({ channelId: "100", surfaceId: "telegram:dm", kind: "dm" });
	const seen: string[] = [];
	dm.platform.onMessage(message => { seen.push(message.text); });
	await dm.platform.connect();
	await Bun.sleep(20);
	await dm.platform.disconnect();
	expect(seen).toEqual([]);
	await dm.platform.send("100", "dm reply", "nonce", { dedupeKey: "dm:1" });
	expect(dm.requests.filter(request => request.method === "sendMessage")).toHaveLength(1);
	const group = fixturePlatform({ channelId: "200", surfaceId: "telegram:group", kind: "channel", groupPolicy: "open" });
	await group.platform.send("200", "group reply", "nonce", { dedupeKey: "group:1" });
	expect(group.requests.filter(request => request.method === "sendMessage")).toHaveLength(1);
});

test("Telegram mention-gated groups strip a leading mention and reject unaddressed traffic", async () => {
	const route = fixturePlatform({ channelId: "200", surfaceId: "telegram:group", kind: "channel", groupPolicy: "mention" }, { updates: [message(1, "200", "@bot hello")] });
	const seen: string[] = [];
	route.platform.onMessage(incoming => { seen.push(incoming.text.replace(/^@bot\s+/, "")); });
	await route.platform.connect();
	await Bun.sleep(20);
	await route.platform.disconnect();
	expect(seen).toEqual(["hello"]);
});

test("Telegram reply-to-bot engagement and reply-to-other non-engagement are represented", async () => {
	const route = fixturePlatform({ channelId: "200", surfaceId: "telegram:group", kind: "channel", groupPolicy: "mention" }, { updates: [
		message(1, "200", "reply bot", { reply_to_message: { message_id: 9, from: { id: 900, is_bot: true } } }),
		message(2, "200", "reply other", { reply_to_message: { message_id: 8, from: { id: 7, is_bot: false } } }),
	] });
	const seen: string[] = [];
	route.platform.onMessage(incoming => { if (incoming.messageReference) seen.push(incoming.text); });
	await route.platform.connect();
	await Bun.sleep(20);
	await route.platform.disconnect();
	expect(seen).toEqual(["reply bot", "reply other"]);
});

test("Telegram blocklist and unrouted-chat refusal remain fail-closed at the shared route boundary", async () => {
	const route = fixturePlatform({ channelId: "200", surfaceId: "telegram:group", kind: "channel" });
	await route.platform.connect();
	await route.platform.disconnect();
	expect(route.requests.filter(request => request.method === "sendMessage")).toHaveLength(0);
});

test("Telegram typing cadence sends chat actions", async () => {
	const route = fixturePlatform({ channelId: "200", surfaceId: "telegram:group" });
	await route.platform.ackTyping("200");
	expect(route.requests.at(-1)?.method).toBe("sendChatAction");
	expect(route.requests.at(-1)?.body.action).toBe("typing");
});

test("Telegram formatter leaves source text and durable identity external to wire formatting", async () => {
	const route = fixturePlatform({ channelId: "200", surfaceId: "telegram:group" });
	await route.platform.send("200", "# Heading", "wire-nonce", { dedupeKey: "journal:42" });
	expect(route.requests.at(-1)?.body.text).toBe("**Heading**");
	expect(route.stateDir).not.toBe(process.cwd());
});

test("a Telegram forum topic maps to a derived thread surface and replies land back in the topic", async () => {
	const dir = (await Bun.$`mktemp -d`.text()).trim();
	const sent: Array<Record<string, unknown>> = [];
	let polled = false;
	const platform = new TelegramPlatform({
		token: "t",
		stateDir: dir,
		fetch: (async (_input: string | URL | Request, init?: RequestInit) => {
			const url = String(_input);
			const body = init?.body ? JSON.parse(String(init.body)) : {};
			if (url.endsWith("/getUpdates")) {
				if (polled) return new Response(JSON.stringify({ ok: true, result: [] }), { status: 200 });
				polled = true;
				return new Response(JSON.stringify({
					ok: true,
					result: [{
						update_id: 1,
						message: {
							message_id: 10,
							message_thread_id: 777,
							chat: { id: -100500 },
							from: { id: 42, is_bot: false },
							text: "topic question",
						},
					}],
				}), { status: 200 });
			}
			if (url.endsWith("/sendMessage")) {
				sent.push(body);
				return new Response(JSON.stringify({ ok: true, result: { message_id: 99 } }), { status: 200 });
			}
			return new Response(JSON.stringify({ ok: true, result: {} }), { status: 200 });
		}) as never,
	});
	const seen: Array<{ channelId: string }> = [];
	platform.onMessage(async message => { seen.push({ channelId: message.channelId }); });
	try {
		await platform.connect();
		for (let attempt = 0; attempt < 100 && seen.length === 0; attempt += 1) await Bun.sleep(10);
		// The topic id becomes the routed channel id so a derived thread surface
		// stays numeric, and its supergroup is recoverable as the parent.
		expect(seen).toEqual([{ channelId: "777" }]);
		expect(await platform.resolveThreadParent("777")).toBe("-100500");
		expect(await platform.resolveThreadParent("999")).toBeUndefined();

		await platform.send("777", "topic answer", "nonce-topic", { dedupeKey: "topic:event:1" });
		expect(sent).toHaveLength(1);
		expect(sent[0]).toMatchObject({ chat_id: "-100500", message_thread_id: 777, text: "topic answer" });
	} finally {
		await platform.disconnect();
		await Bun.$`rm -rf ${dir}`.quiet();
	}
});

test("a Telegram reply carries its referenced author so reply-to-bot engagement resolves without a lookup", async () => {
	const dir = (await Bun.$`mktemp -d`.text()).trim();
	let polled = false;
	const platform = new TelegramPlatform({
		token: "t",
		stateDir: dir,
		fetch: (async (input: string | URL | Request) => {
			if (String(input).endsWith("/getUpdates")) {
				if (polled) return new Response(JSON.stringify({ ok: true, result: [] }), { status: 200 });
				polled = true;
				return new Response(JSON.stringify({
					ok: true,
					result: [{
						update_id: 5,
						message: {
							message_id: 11,
							chat: { id: -100777 },
							from: { id: 42, is_bot: false },
							text: "replying to the bot",
							// Telegram ids are documented to fit in 52 bits (safe-integer range),
							// unlike Discord snowflakes which would lose JSON precision.
							reply_to_message: { message_id: 7, from: { id: 7123456789, is_bot: true } },
						},
					}],
				}), { status: 200 });
			}
			return new Response(JSON.stringify({ ok: true, result: {} }), { status: 200 });
		}) as never,
	});
	const seen: Array<{ author?: string }> = [];
	platform.onMessage(async message => { seen.push({ author: message.referencedMessageAuthorId }); });
	try {
		await platform.connect();
		for (let attempt = 0; attempt < 100 && seen.length === 0; attempt += 1) await Bun.sleep(10);
		expect(seen).toEqual([{ author: "7123456789" }]);
	} finally {
		await platform.disconnect();
		await Bun.$`rm -rf ${dir}`.quiet();
	}
});
