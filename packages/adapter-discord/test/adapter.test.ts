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
	handleSlashCommand,
	LruSet,
	settleDiscordDelivery,
	subscribeDiscordDeliveries,
	TypingIndicator,
	WorkingStatus,
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

test("working status posts one amended message per conversation and clears on delivery", async () => {
	const sent: string[] = [];
	const edits: string[] = [];
	let deleted = 0;
	const statusMessage = {
		edit: async (text: string) => void edits.push(text),
		delete: async () => void deleted++,
	};
	const discord: DiscordClientLike = {
		channels: {
			fetch: async () => ({
				send: async (text: string) => {
					sent.push(text);
					return statusMessage;
				},
			}),
		},
	};
	const status = new WorkingStatus(discord, { error: () => {} });
	const origin = { platform: "discord", kind: "channel", conversationId: "channel-1" } as const;
	await status.update({ turnId: "t", origin, elapsedMs: 16_000, toolCalls: 1, outputTokens: 210 });
	await status.update({ turnId: "t", origin, elapsedMs: 125_000, toolCalls: 3, outputTokens: 1250 });
	expect(sent).toEqual(["⏳ working… (16s, 1 tool, 210 tok)"]);
	expect(edits).toEqual(["⏳ working… (2m 05s, 3 tools, 1.3k tok)"]);
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleDiscordDelivery(mockGateway(requests), discord, delivery("real reply"), undefined, status);
	expect(deleted).toBe(1);
	// A later delivery without a live status message is a no-op.
	await settleDiscordDelivery(mockGateway(requests), discord, delivery("again"), undefined, status);
	expect(deleted).toBe(1);
});

test("working status ignores non-discord progress and survives channel failures", async () => {
	const failing: DiscordClientLike = {
		channels: {
			fetch: async () => {
				throw new Error("network down");
			},
		},
	};
	const status = new WorkingStatus(failing, { error: () => {} });
	await status.update({
		turnId: "t",
		origin: { platform: "telegram", kind: "channel", conversationId: "tg" },
		elapsedMs: 20_000,
		toolCalls: 1,
		outputTokens: 0,
	});
	await status.update({
		turnId: "t",
		origin: { platform: "discord", kind: "channel", conversationId: "c" },
		elapsedMs: 20_000,
		toolCalls: 1,
		outputTokens: 0,
	});
	await status.clear("c"); // nothing posted; must not throw
});

test("slash commands /new and /reset map to gateway session resets with the invoker attributed", async () => {
	const sent: Array<{ messageId: string; text: string; engagement: unknown }> = [];
	const gateway = {
		requestInbound: async (messageId: string, _origin: unknown, text: string, engagement: unknown) => {
			sent.push({ messageId, text, engagement });
			return { engaged: true };
		},
	};
	let replied = "";
	const interaction = {
		isChatInputCommand: () => true,
		commandName: "new",
		id: "itx-1",
		user: { id: "owner-1", username: "bellman" },
		channel: { id: "channel-9", type: 0 },
		reply: async (options: { content: string }) => {
			replied = options.content;
		},
	};
	await handleSlashCommand(interaction as never, gateway as never, { error: () => {} });
	expect(sent).toHaveLength(1);
	expect(sent[0]).toMatchObject({
		messageId: "slash-itx-1",
		text: "/new",
		engagement: { mentioned: true, group: true, authorId: "owner-1", authorName: "bellman" },
	});
	expect(replied).toContain("session reset");
	// Non-command interactions and unknown commands are ignored outright.
	await handleSlashCommand({ ...interaction, commandName: "dance" } as never, gateway as never, { error: () => {} });
	await handleSlashCommand({ ...interaction, isChatInputCommand: () => false } as never, gateway as never, {
		error: () => {},
	});
	expect(sent).toHaveLength(1);
});

test("a declined slash command answers not-authorized instead of claiming a reset", async () => {
	const gateway = { requestInbound: async () => ({ engaged: false }) };
	let replied = "";
	await handleSlashCommand(
		{
			isChatInputCommand: () => true,
			commandName: "reset",
			id: "itx-2",
			user: { id: "intruder", username: "mallory" },
			channel: { id: "channel-9", type: 0 },
			reply: async (options: { content: string }) => {
				replied = options.content;
			},
		} as never,
		gateway as never,
		{ error: () => {} },
	);
	expect(replied).toContain("not authorized");
});
