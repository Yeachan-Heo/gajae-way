import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload } from "@gajaeway/protocol";
import { DiscordAdapterStartupError, loadDiscordAdapterConfig } from "../src/config";
import {
	chunkDiscordMessage,
	type DiscordClientLike,
	engagementForMessage,
	type GatewayClientLike,
	LruSet,
	settleDiscordDelivery,
	subscribeDiscordDeliveries,
	TypingIndicator,
} from "../src/main";
import { discordMessageOrigin } from "../src/origin";

const author = { id: "author-1" };

test("loads and trims the token credential file without exposing its value", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-adapter-"));
	try {
		await writeFile(join(home, "token"), " secret-token \n");
		await writeFile(
			join(home, "adapter-discord.json"),
			JSON.stringify({ tokenFile: "token", channels: { c: { engagement: "open" } } }),
		);
		const config = await loadDiscordAdapterConfig({ GAJAEWAY_HOME: home });
		expect(config.token).toBe("secret-token");
		expect(config.tokenFile).toBe(join(home, "token"));
		await writeFile(join(home, "adapter-discord.json"), "{}");
		await expect(loadDiscordAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toBeInstanceOf(DiscordAdapterStartupError);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("maps guild channels, threads, and DMs to canonical Discord origins", () => {
	expect(discordMessageOrigin({ author, channel: { id: "channel-1" } })).toEqual({
		platform: "discord",
		kind: "channel",
		conversationId: "channel-1",
	});
	expect(
		discordMessageOrigin({ author, channel: { id: "thread-1", parentId: "channel-1", isThread: () => true } }),
	).toEqual({
		platform: "discord",
		kind: "thread",
		conversationId: "thread-1",
		parentId: "channel-1",
	});
	expect(discordMessageOrigin({ author, channel: { id: "dm-1", isDMBased: () => true } })).toEqual({
		platform: "discord",
		kind: "dm",
		conversationId: "dm-1",
		peerId: "author-1",
	});
});

test("derives engagement from Discord mentions and recognizes DMs as non-group", () => {
	const bot = { id: "bot.1" };
	const mentioned = engagementForMessage(
		{ id: "1", content: "hello <@!bot.1>", author, channel: { id: "channel" }, mentions: { has: () => false } },
		bot,
	);
	expect(mentioned).toEqual({ mentioned: true, group: true, authorId: "author-1" });
	const dm = engagementForMessage(
		{ id: "2", content: "hello", author, channel: { id: "dm", isDMBased: () => true }, mentions: { has: () => false } },
		bot,
	);
	expect(dm).toEqual({ mentioned: false, group: false, authorId: "author-1" });
});

test("LRU idempotency accepts each id once and evicts least recent ids", () => {
	const ids = new LruSet(2);
	expect(ids.addIfAbsent("a")).toBe(true);
	expect(ids.addIfAbsent("a")).toBe(false);
	expect(ids.addIfAbsent("b")).toBe(true);
	expect(ids.addIfAbsent("c")).toBe(true);
	expect(ids.addIfAbsent("a")).toBe(true);
});

test("chunks Discord messages at the 2000 character limit", () => {
	const chunks = chunkDiscordMessage("x".repeat(4_001));
	expect(chunks.map((chunk) => chunk.length)).toEqual([2_000, 2_000, 1]);
});

test("settles a delivery after sending all chunks", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: string[] = [];
	const gateway = mockGateway(requests);
	const discord: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async (text: string) => void sent.push(text) }) },
	};
	await settleDiscordDelivery(gateway, discord, delivery("x".repeat(2_001)));
	expect(sent.map((text) => text.length)).toEqual([2_000, 1]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("prefixes ambiguous redelivery and records failed settlement", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: string[] = [];
	const gateway = mockGateway(requests);
	const discord: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async (text: string) => void sent.push(text) }) },
	};
	await settleDiscordDelivery(gateway, discord, { ...delivery("reply"), duplicateWarning: true });
	expect(sent).toEqual(["[recovered - may be a duplicate] reply"]);
	requests.length = 0;
	const failing: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async () => Promise.reject(new Error("timeout after dispatch")) }) },
	};
	await settleDiscordDelivery(gateway, failing, delivery("reply"));
	expect(requests).toEqual([
		{ verb: "delivery.fail", params: { deliveryId: "delivery-1", reason: "timeout after dispatch", ambiguous: true } },
	]);
});

test("delivery subscription filters non-Discord and missing delivery ids", async () => {
	let handler: ((message: ChatMessagePayload) => void) | undefined;
	const requests: Array<{ verb: string; params: unknown }> = [];
	const gateway: GatewayClientLike = {
		request: async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return {} as T;
		},
		onChatMessage: (listener) => {
			handler = listener;
			return () => {};
		},
	};
	const off = subscribeDiscordDeliveries(gateway, { channels: { fetch: async () => ({ send: async () => {} }) } });
	handler?.({ ...delivery("ignored"), origin: { platform: "telegram", kind: "channel", conversationId: "t" } });
	await Bun.sleep(0);
	expect(requests).toEqual([]);
	off();
});

test("typing indicator pulses while a turn runs and stops when the delivery settles", async () => {
	let typingCount = 0;
	const discord: DiscordClientLike = {
		channels: {
			fetch: async () => ({
				send: async () => {},
				sendTyping: async () => void typingCount++,
			}),
		},
	};
	const typing = new TypingIndicator(discord, 5, 10_000, { error: () => {} });
	typing.begin("channel-1");
	await Bun.sleep(20);
	expect(typingCount).toBeGreaterThanOrEqual(2);
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleDiscordDelivery(mockGateway(requests), discord, delivery("reply"), typing);
	const settled = typingCount;
	await Bun.sleep(25);
	expect(typingCount).toBe(settled);
});

test("typing indicator stops at its deadline and on channels without sendTyping", async () => {
	let typingCount = 0;
	const typingCapable: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async () => {}, sendTyping: async () => void typingCount++ }) },
	};
	const deadlined = new TypingIndicator(typingCapable, 5, 12, { error: () => {} });
	deadlined.begin("channel-1");
	await Bun.sleep(40);
	const atDeadline = typingCount;
	await Bun.sleep(20);
	expect(typingCount).toBe(atDeadline);

	let sent = 0;
	const sendOnly: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async () => void sent++ }) },
	};
	const incapable = new TypingIndicator(sendOnly, 5, 10_000, { error: () => {} });
	incapable.begin("channel-2");
	await Bun.sleep(20);
	expect(sent).toBe(0);
});

function delivery(text: string): ChatMessagePayload {
	return {
		turnId: "turn-1",
		origin: { platform: "discord", kind: "channel", conversationId: "channel-1" },
		role: "assistant",
		text,
		final: true,
		deliveryId: "delivery-1",
	};
}

function mockGateway(requests: Array<{ verb: string; params: unknown }>): GatewayClientLike {
	return {
		request: async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return {} as T;
		},
		onChatMessage: () => () => {},
	};
}
