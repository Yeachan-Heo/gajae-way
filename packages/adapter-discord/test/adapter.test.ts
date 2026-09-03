import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, ChatProgressPayload } from "@gajaeway/protocol";
import { DiscordAdapterStartupError, loadDiscordAdapterConfig } from "../src/config";
import {
	addressedTurn,
	chunkDiscordMessage,
	type DiscordClientLike,
	engagementForMessage,
	type GatewayClientLike,
	handleSlashCommand,
	LruSet,
	settleDiscordDelivery,
	subscribeDiscordDeliveries,
	subscribeDiscordProgress,
	TypingIndicator,
	WorkingStatus,
	workingStatusText,
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

test("reports the author's server tag, which every human in the room can already read", () => {
	const bot = { id: "bot.1" };
	const tagged = engagementForMessage(
		{
			id: "1",
			content: "hello <@!bot.1>",
			author: { id: "author-2", username: "leesayah", primaryGuild: { tag: "GJC", identityEnabled: true } },
			channel: { id: "channel" },
			mentions: { has: () => false },
		},
		bot,
	);
	expect(tagged.authorServerTag).toBe("GJC");
	// A tag the account is not displaying is invisible to the room; reporting it
	// would let the persona claim to see a badge nobody else can.
	const hidden = engagementForMessage(
		{
			id: "2",
			content: "hello <@!bot.1>",
			author: { id: "author-3", username: "nobadge", primaryGuild: { tag: "GJC", identityEnabled: false } },
			channel: { id: "channel" },
			mentions: { has: () => false },
		},
		bot,
	);
	expect(hidden.authorServerTag).toBeUndefined();
	expect("authorServerTag" in hidden).toBe(false);
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

// The merge seam between this lane's reaction wiring and the reply-metadata lane
// lives inside settleDiscordDelivery's dispatch. Both branches are exercised here
// because deleting either one leaves every other test in the repo green: a lost
// reaction branch would POST the bare emoji as a message, and a lost reply branch
// would silently stop threading.
test("a reaction delivery reacts to its target and never posts a message", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: string[] = [];
	const reacted: string[] = [];
	const discord: DiscordClientLike = {
		channels: {
			fetch: async () => ({
				send: async (text: string) => void sent.push(text),
				messages: {
					fetch: async () => ({ react: async (emoji: string) => void reacted.push(emoji) }),
				},
			}),
		},
	};
	await settleDiscordDelivery(mockGateway(requests), discord, {
		...delivery("👍"),
		reaction: { targetMessageId: "target-1", emoji: "👍", emojiName: "thumbsup" },
	});
	expect(reacted).toEqual(["👍"]);
	expect(sent).toEqual([]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("a reply-threaded delivery threads its first chunk and only its first chunk", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const payloads: unknown[] = [];
	const discord: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async (payload: unknown) => void payloads.push(payload) }) },
	};
	await settleDiscordDelivery(mockGateway(requests), discord, {
		...delivery("x".repeat(2_001)),
		replyToMessageId: "msg-42",
	});
	expect(payloads).toHaveLength(2);
	expect(payloads[0]).toEqual({
		content: "x".repeat(2_000),
		reply: { messageReference: "msg-42", failIfNotExists: false },
	});
	// The continuation is a plain string: threading every chunk would spam the reference.
	expect(payloads[1]).toBe("x");
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
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

test("a final progress event ends the typing hint even when the turn delivered nothing", async () => {
	let typingCount = 0;
	const discord: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async () => {}, sendTyping: async () => void typingCount++ }) },
	};
	const typing = new TypingIndicator(discord, 5, 10_000, { error: () => {} });
	let emit: ((progress: ChatProgressPayload) => void) | undefined;
	const gateway: GatewayClientLike = {
		request: async <T>() => ({}) as T,
		onChatMessage: () => () => {},
		onChatProgress: (handler) => {
			emit = handler;
			return () => {};
		},
	};
	const cleared: string[] = [];
	subscribeDiscordProgress(
		gateway,
		{ update: async () => {}, clear: async (conversationId) => void cleared.push(conversationId) },
		{ error: () => {} },
		typing,
	);
	typing.begin("channel-1");
	await Bun.sleep(20);
	expect(typingCount).toBeGreaterThanOrEqual(2);
	// Silent turn: no delivery ever arrives, only the final progress frame.
	emit?.({
		turnId: "turn-1",
		origin: { platform: "discord", kind: "channel", conversationId: "channel-1" },
		final: true,
		elapsedMs: 1,
		toolCalls: 0,
		outputTokens: 0,
	});
	const settled = typingCount;
	await Bun.sleep(25);
	expect(typingCount).toBe(settled);
	expect(cleared).toEqual(["channel-1"]);
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
	status.arm("channel-1");
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
	status.arm("tg");
	status.arm("c");
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

test("presence is shown only where the persona was addressed: DM, mention, or open-channel promotion", () => {
	expect(addressedTurn({ group: false, mentioned: false })).toBe(true); // DM
	expect(addressedTurn({ group: true, mentioned: true })).toBe(true); // mention, or open promotion
	expect(addressedTurn({ group: true, mentioned: false })).toBe(false); // overheard public channel
});

test("an unaddressed public-channel turn posts no working status until it is armed; clear disarms", async () => {
	const sent: string[] = [];
	let deleted = 0;
	const discord: DiscordClientLike = {
		channels: {
			fetch: async () => ({
				send: async (text: string) => {
					sent.push(text);
					return { edit: async () => {}, delete: async () => void deleted++ };
				},
			}),
		},
	};
	const status = new WorkingStatus(discord, { error: () => {} });
	const origin = { platform: "discord", kind: "channel", conversationId: "public-1" } as const;
	const tick = { turnId: "t", origin, elapsedMs: 16_000, toolCalls: 1, outputTokens: 210 };
	await status.update(tick);
	await status.update(tick);
	expect(sent).toEqual([]);
	status.arm("public-1");
	await status.update(tick);
	expect(sent).toHaveLength(1);
	await status.clear("public-1");
	expect(deleted).toBe(1);
	// Disarmed: the next turn's ticks are silent again until re-armed.
	await status.update(tick);
	expect(sent).toHaveLength(1);
});

test("the working status never claims counters the runtime did not report", () => {
	// gjc 0.16.0's stdio relay cannot negotiate the tool_activity capability, so
	// both counters stay zero for the whole turn: showing them read as "this turn
	// did nothing" for minutes (live, 2026-09-03).
	expect(workingStatusText({ elapsedMs: 280_000, toolCalls: 0, outputTokens: 0 })).toBe("⏳ working… (4m 40s)");
	expect(workingStatusText({ elapsedMs: 16_000, toolCalls: 1, outputTokens: 0 })).toBe("⏳ working… (16s, 1 tool)");
	expect(workingStatusText({ elapsedMs: 16_000, toolCalls: 0, outputTokens: 210 })).toBe("⏳ working… (16s, 210 tok)");
	expect(workingStatusText({ elapsedMs: 125_000, toolCalls: 3, outputTokens: 1250 })).toBe(
		"⏳ working… (2m 05s, 3 tools, 1.3k tok)",
	);
});
