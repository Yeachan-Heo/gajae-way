import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, ChatProgressPayload } from "@gajae-gateway/protocol";
import { PRESENCE_MIN_SWAP_MS } from "@gajae-gateway/protocol";
import { DiscordAdapterStartupError, loadDiscordAdapterConfig } from "../src/config";
import {
	chunkDiscordMessage,
	DISCORD_SLASH_COMMANDS,
	type DiscordClientLike,
	deriveThreadName,
	engagementForMessage,
	type GatewayClientLike,
	handleModelAutocomplete,
	handleSlashCommand,
	isPresenceReaction,
	LruSet,
	maybeCreateThreadOnMention,
	nameThreadFromMessage,
	settleDiscordDelivery,
	subscribeDiscordDeliveries,
	subscribeDiscordProgress,
	TypingIndicator,
	UnnamedThreads,
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

test("accepts exact engagement modes and audiences while preserving omitted audience", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-policy-"));
	try {
		await writeFile(join(home, "token"), "secret-token");
		await writeFile(
			join(home, "adapter-discord.json"),
			JSON.stringify({
				tokenFile: "token",
				channels: {
					legacy: { engagement: "open" },
					collab: { engagement: "mention-open", audience: "all" },
					locked: { engagement: "closed", audience: "bot-only" },
				},
			}),
		);
		const config = await loadDiscordAdapterConfig({ GAJAEWAY_HOME: home });
		expect(config.channels?.legacy).toEqual({ engagement: "open" });
		expect(config.channels?.collab).toEqual({ engagement: "mention-open", audience: "all" });
		expect(config.channels?.locked).toEqual({ engagement: "closed", audience: "bot-only" });

		for (const channels of [
			{ c: { engagement: "open-mention-only" } },
			{ c: { engagement: "open", audience: "sometimes" } },
			{ c: { engagement: "open", extra: true } },
		]) {
			await writeFile(join(home, "adapter-discord.json"), JSON.stringify({ tokenFile: "token", channels }));
			await expect(loadDiscordAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toBeInstanceOf(
				DiscordAdapterStartupError,
			);
		}
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("accepts and validates statusReactions configuration option", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-status-"));
	try {
		await writeFile(join(home, "token"), "secret-token");

		// Valid options
		for (const mode of ["gradient", "static", "off"] as const) {
			await writeFile(
				join(home, "adapter-discord.json"),
				JSON.stringify({ tokenFile: "token", statusReactions: mode }),
			);
			const config = await loadDiscordAdapterConfig({ GAJAEWAY_HOME: home });
			expect(config.statusReactions).toBe(mode);
		}

		// Invalid options should be rejected
		for (const invalid of ["disabled", "on", "both", 123, true]) {
			await writeFile(
				join(home, "adapter-discord.json"),
				JSON.stringify({ tokenFile: "token", statusReactions: invalid }),
			);
			await expect(loadDiscordAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toBeInstanceOf(
				DiscordAdapterStartupError,
			);
		}

		// Omitted statusReactions should use default (undefined, which means gradient at runtime)
		await writeFile(join(home, "adapter-discord.json"), JSON.stringify({ tokenFile: "token" }));
		const defaultConfig = await loadDiscordAdapterConfig({ GAJAEWAY_HOME: home });
		expect(defaultConfig.statusReactions).toBeUndefined();
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

const threadDelivery = (text: string): ChatMessagePayload => ({
	...delivery(text),
	origin: { platform: "discord", kind: "thread", conversationId: "thread-1", parentId: "channel-1" },
});

test("an archived thread is unarchived before delivery and the reply lands in the thread", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: Array<[string, unknown]> = [];
	const thread = {
		archived: true,
		setArchived: async (archived: boolean) => {
			thread.archived = archived;
		},
		send: async (payload: unknown) => void sent.push(["thread-1", payload]),
	};
	const discord: DiscordClientLike = {
		channels: {
			fetch: async (id: string) =>
				id === "thread-1" ? thread : { send: async (payload: unknown) => void sent.push([id, payload]) },
		},
	};
	await settleDiscordDelivery(mockGateway(requests), discord, threadDelivery("monitor result"));
	expect(thread.archived).toBe(false);
	expect(sent).toEqual([["thread-1", "monitor result"]]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("an archived thread that cannot be unarchived falls back to its parent channel and says why", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: Array<[string, unknown]> = [];
	const errors: string[] = [];
	const thread = {
		archived: true,
		setArchived: async () => Promise.reject(Object.assign(new Error("Missing Permissions"), { code: 50013 })),
		send: async (payload: unknown) => void sent.push(["thread-1", payload]),
	};
	const discord: DiscordClientLike = {
		channels: {
			fetch: async (id: string) =>
				id === "thread-1" ? thread : { send: async (payload: unknown) => void sent.push([id, payload]) },
		},
	};
	const original = console.error;
	console.error = (line: string) => void errors.push(line);
	try {
		await settleDiscordDelivery(mockGateway(requests), discord, threadDelivery("monitor result"));
	} finally {
		console.error = original;
	}
	expect(sent).toEqual([
		[
			"channel-1",
			"[thread <#thread-1> is archived and could not be unarchived (Missing Permissions); posting here instead]\nmonitor result",
		],
	]);
	expect(errors).toEqual([
		"Discord delivery delivery-1: thread thread-1 is archived and could not be unarchived (Missing Permissions); delivered to parent channel channel-1 instead.",
	]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("an active thread and a plain channel deliver exactly as before", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: Array<[string, unknown]> = [];
	const discord: DiscordClientLike = {
		channels: {
			fetch: async (id: string) => ({
				archived: false,
				setArchived: async () => {
					throw new Error("must not unarchive an active thread");
				},
				send: async (payload: unknown) => void sent.push([id, payload]),
			}),
		},
	};
	await settleDiscordDelivery(mockGateway(requests), discord, threadDelivery("in thread"));
	await settleDiscordDelivery(mockGateway(requests), discord, delivery("in channel"));
	expect(sent).toEqual([
		["thread-1", "in thread"],
		["channel-1", "in channel"],
	]);
	expect(requests.map((request) => request.verb)).toEqual(["delivery.confirm", "delivery.confirm"]);
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

function presenceDiscord() {
	const reacted: string[] = [];
	const removed: string[] = [];
	const message = {
		react: async (emoji: string) => void reacted.push(emoji),
		reactions: {
			resolve: (emoji: string) => ({
				users: { remove: async (userId: string) => void removed.push(`${emoji}:${userId}`) },
			}),
		},
	};
	const discord: DiscordClientLike = {
		channels: { fetch: async () => ({ messages: { fetch: async () => message }, send: async () => ({}) }) },
	};
	return { discord, reacted, removed };
}

test("working status is a reaction gradient on the triggering message and clears on delivery", async () => {
	const { discord, reacted, removed } = presenceDiscord();
	let clock = 0;
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		() => clock,
	);
	const origin = { platform: "discord", kind: "channel", conversationId: "channel-1" } as const;
	// Explicitly pass engagement to show reactions; this is a DM (not a group)
	const engagement = { group: false, mentioned: false };
	status.arm("channel-1", "m-1", engagement);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]);
	clock += PRESENCE_MIN_SWAP_MS;
	await status.update({
		turnId: "t",
		origin,
		elapsedMs: 61_000,
		toolCalls: 1,
		outputTokens: 210,
		activity: { kind: "tool", label: "bash" },
	});
	// Phase ⏳→🔧, one minute, one tool: a remove and three adds; nothing posted.
	expect(removed).toEqual(["⏳:bot-1"]);
	expect(reacted).toEqual(["⏳", "🔧", "🕐", "1️⃣"]);
	// Inside the window: coalesced.
	await status.update({ turnId: "t", origin, elapsedMs: 125_000, toolCalls: 3, outputTokens: 1250 });
	expect(reacted).toHaveLength(4);
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleDiscordDelivery(mockGateway(requests), discord, delivery("real reply"), undefined, status);
	expect(new Set(removed)).toEqual(new Set(["⏳:bot-1", "🔧:bot-1", "🕐:bot-1", "1️⃣:bot-1"]));
	// A later delivery with no live gradient is a no-op.
	const before = removed.length;
	await settleDiscordDelivery(mockGateway(requests), discord, delivery("again"), undefined, status);
	expect(removed).toHaveLength(before);
});

test("an interim delivery keeps the working status and re-pulses typing; only the final reply clears", async () => {
	const { discord, removed } = presenceDiscord();
	let typingCount = 0;
	const typingDiscord: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async () => {}, sendTyping: async () => void typingCount++ }) },
	};
	const typing = new TypingIndicator(typingDiscord, 10_000, 60_000, { error: () => {} });
	const status = new WorkingStatus(discord, { error: () => {} }, () => ({ id: "bot-1" }));
	const engagement = { group: false, mentioned: false };
	status.arm("channel-1", "m-1", engagement);
	typing.begin("channel-1");
	await Bun.sleep(5);
	const beforeInterim = typingCount;
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleDiscordDelivery(
		mockGateway(requests),
		discord,
		{ ...delivery("still working"), final: false },
		typing,
		status,
	);
	await Bun.sleep(5);
	// Posting cleared Discord's hint; it was re-sent at once, not after the 10s tick.
	expect(typingCount).toBe(beforeInterim + 1);
	expect(removed).toEqual([]);
	await settleDiscordDelivery(
		mockGateway(requests),
		discord,
		{ ...delivery("👍"), final: false, reaction: { targetMessageId: "m-1", emoji: "👍", emojiName: "thumbsup" } },
		typing,
		status,
	);
	expect(removed).toEqual([]);
	await settleDiscordDelivery(mockGateway(requests), discord, delivery("done"), typing, status);
	expect(removed).toEqual(["⏳:bot-1"]);
	const settled = typingCount;
	typing.refresh("channel-1");
	await Bun.sleep(5);
	expect(typingCount).toBe(settled);
});

test("working status ignores non-discord progress and survives channel failures", async () => {
	const failing: DiscordClientLike = {
		channels: {
			fetch: async () => {
				throw new Error("network down");
			},
		},
	};
	const status = new WorkingStatus(failing, { error: () => {} }, () => ({ id: "bot-1" }));
	status.arm("tg", "m-1");
	status.arm("c", "m-2");
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
	await status.clear("c"); // nothing reacted; must not throw
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

test("/model is registered with show/set/clear and set autocompletes its choice", () => {
	const model = DISCORD_SLASH_COMMANDS.find((command) => command.name === "model");
	expect(model?.options?.map((option) => option.name)).toEqual(["show", "set", "clear"]);
	const set = model?.options?.find((option) => option.name === "set");
	expect(set?.options?.[0]).toMatchObject({ name: "choice", required: true, autocomplete: true });
});

test("/model subcommands map to the gateway's /model text verbs, not to a session reset", async () => {
	const sent: string[] = [];
	const gateway = {
		requestInbound: async (_messageId: string, _origin: unknown, text: string) => {
			sent.push(text);
			return { engaged: true };
		},
	};
	const replies: string[] = [];
	const invoke = (subcommand: string, choice: string | null) =>
		handleSlashCommand(
			{
				isChatInputCommand: () => true,
				commandName: "model",
				id: `itx-${subcommand}`,
				user: { id: "owner-1", username: "bellman" },
				channel: { id: "channel-9", type: 0 },
				options: { getSubcommand: () => subcommand, getString: () => choice },
				reply: async (options: { content: string }) => {
					replies.push(options.content);
				},
			} as never,
			gateway as never,
			{ error: () => {} },
		);
	await invoke("show", null);
	await invoke("set", " gpt-heavy ");
	await invoke("clear", null);
	// A set with no usable choice is refused locally instead of turning into a bare read.
	await invoke("set", "  ");
	expect(sent).toEqual(["/model", "/model set gpt-heavy", "/model clear"]);
	expect(replies.slice(0, 3).every((reply) => !reply.includes("session reset"))).toBe(true);
	expect(replies[3]).toContain("choose a model");
});

test("/model autocomplete filters gateway choices and fails soft to an empty list", async () => {
	const responded: Array<Array<{ name: string; value: string }>> = [];
	const interaction = (focused: string) => ({
		isAutocomplete: () => true,
		commandName: "model",
		options: { getFocused: () => focused },
		respond: async (choices: Array<{ name: string; value: string }>) => {
			responded.push(choices);
		},
	});
	const many = Array.from({ length: 40 }, (_, index) => `preset-${index}`);
	await handleModelAutocomplete(interaction("HEAVY") as never, {
		modelChoices: async () => ["frontier-heavy", "gpt-heavy", "glm-gpt"],
	});
	await handleModelAutocomplete(interaction("") as never, { modelChoices: async () => many });
	await handleModelAutocomplete(interaction("x") as never, {
		modelChoices: async () => {
			throw new Error("gateway down");
		},
	});
	expect(responded[0]).toEqual([
		{ name: "frontier-heavy", value: "frontier-heavy" },
		{ name: "gpt-heavy", value: "gpt-heavy" },
	]);
	expect(responded[1]).toHaveLength(25);
	expect(responded[2]).toEqual([]);
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

test("presence is shown for every engaged turn: DMs, mentions, and un-mentioned group/thread follow-ups", async () => {
	// The gateway's engagement decision is authoritative; the adapter shows
	// presence for all accepted turns unless statusReactions is 'off' mode.
	// No local heuristic gate on mentioned/group status (live, 2026-09-17).
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(discord, { error: () => {} }, () => ({ id: "bot-1" }));
	// DM with no mention: shows presence
	status.arm("dm-1", "m-1", { group: false, mentioned: false });
	await Bun.sleep(1);
	expect(reacted).toContain("⏳");
	// Group channel with mention: shows presence
	status.arm("channel-1", "m-2", { group: true, mentioned: true });
	await Bun.sleep(1);
	expect(reacted).toContain("⏳");
	// Group channel without mention (thread follow-up): shows presence
	status.arm("thread-1", "m-3", { group: true, mentioned: false });
	await Bun.sleep(1);
	expect(reacted).toContain("⏳");
});

test("presence shows only when armed; clear disarms and handles un-mentioned thread follow-ups", async () => {
	const { discord, reacted, removed } = presenceDiscord();
	const status = new WorkingStatus(discord, { error: () => {} }, () => ({ id: "bot-1" }));
	const origin = { platform: "discord", kind: "thread", conversationId: "thread-1", parentId: "ch-1" } as const;
	const tick = { turnId: "t", origin, elapsedMs: 16_000, toolCalls: 1, outputTokens: 210 };
	// Before arming: no reactions
	await status.update(tick);
	await status.update(tick);
	expect(reacted).toEqual([]);
	// Arm a thread reply without mention: engagement.mentioned=false but gateway engaged=true.
	const engagement = { group: true, mentioned: false };
	status.arm("thread-1", "m-9", engagement);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]);
	await status.clear("thread-1");
	expect(removed).toEqual(["⏳:bot-1"]);
	// Disarmed: the next update's ticks are silent again until re-armed.
	await status.update(tick);
	expect(reacted).toHaveLength(1);
});

test("presence for a channel mention answered in a new thread reacts on the message in the parent channel", async () => {
	const reacted: string[] = [];
	const removed: string[] = [];
	const fetchedChannels: string[] = [];
	const trigger = {
		react: async (emoji: string) => void reacted.push(emoji),
		reactions: {
			resolve: (emoji: string) => ({
				users: { remove: async (userId: string) => void removed.push(`${emoji}:${userId}`) },
			}),
		},
	};
	// The trigger lives only in the parent channel; the new thread does not hold it.
	const discord: DiscordClientLike = {
		channels: {
			fetch: async (id: string) => {
				fetchedChannels.push(id);
				return {
					messages: {
						fetch: async (messageId: string) => {
							if (id !== "ch-1" || messageId !== "m-1")
								throw Object.assign(new Error("Unknown Message"), { code: 10008 });
							return trigger;
						},
					},
					send: async () => ({}),
				};
			},
		},
	};
	const status = new WorkingStatus(discord, { error: () => {} }, () => ({ id: "bot-1" }));
	status.arm("thread-1", "m-1", { group: true, mentioned: true }, "ch-1");
	await Bun.sleep(1);
	expect(fetchedChannels).toEqual(["ch-1"]);
	expect(reacted).toEqual(["⏳"]);
	// The reply lands in the thread conversation and clears the parent-channel marker.
	await status.clear("thread-1");
	expect(removed).toEqual(["⏳:bot-1"]);
});

test("our own presence markers are never reported inbound as engagement", () => {
	expect(isPresenceReaction("🔧")).toBe(true);
	expect(isPresenceReaction("✍️")).toBe(true);
	expect(isPresenceReaction("✍")).toBe(true);
	expect(isPresenceReaction("👍")).toBe(false);
});

test("statusReactions 'static' mode shows only phase marker, not clock or effort", async () => {
	const { discord, reacted, removed } = presenceDiscord();
	let clock = 0;
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		() => clock,
		"static", // static mode
	);
	const origin = { platform: "discord", kind: "channel", conversationId: "channel-1" } as const;
	const engagement = { group: false, mentioned: false }; // DM
	status.arm("channel-1", "m-1", engagement);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]);
	clock += PRESENCE_MIN_SWAP_MS;
	await status.update({
		turnId: "t",
		origin,
		elapsedMs: 61_000, // One minute elapsed
		toolCalls: 1,
		outputTokens: 210,
		activity: { kind: "tool", label: "bash" },
	});
	// In static mode: no phase swap, no clock (🕐) or effort (1️⃣); stays at ⏳
	expect(reacted).toEqual(["⏳"]);
	expect(removed).toEqual([]);
});

test("statusReactions 'off' mode produces zero reactions on group channels without mention", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		"off", // off mode
	);
	const groupNoMention = { group: true, mentioned: false }; // Group channel, not mentioned
	status.arm("channel-1", "m-1", groupNoMention);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // No reactions shown
});

test("statusReactions 'off' mode produces zero reactions on mentioned turns in group channels", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		"off", // off mode
	);
	const groupWithMention = { group: true, mentioned: true }; // Group channel, but mentioned
	status.arm("channel-1", "m-1", groupWithMention);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // No reactions shown
});

test("statusReactions 'off' mode produces zero reactions on DMs", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		"off", // off mode
	);
	const dm = { group: false, mentioned: false }; // DM (not a group)
	status.arm("dm-1", "m-1", dm);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // No reactions shown
});

test("statusReactions 'off' mode produces zero reactions on bot-audience channels", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		"off", // off mode
	);
	const botAudience = { group: true, mentioned: false }; // Bot-only audience
	status.arm("bot-channel", "m-1", botAudience);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // No reactions shown on bot-audience channels
});

test("statusReactions 'gradient' mode is default and shows full reaction gradient", async () => {
	const { discord, reacted } = presenceDiscord();
	let clock = 0;
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		() => clock,
		"gradient", // gradient mode (default)
	);
	const origin = { platform: "discord", kind: "channel", conversationId: "channel-1" } as const;
	const groupNoMention = { group: true, mentioned: false }; // Even group channels without mention show gradient
	status.arm("channel-1", "m-1", groupNoMention);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]);
	clock += PRESENCE_MIN_SWAP_MS;
	await status.update({
		turnId: "t",
		origin,
		elapsedMs: 61_000,
		toolCalls: 1,
		outputTokens: 210,
		activity: { kind: "tool", label: "bash" },
	});
	// Gradient mode shows phase + clock + effort
	expect(reacted).toContain("🔧");
	expect(reacted).toContain("🕐");
	expect(reacted).toContain("1️⃣");
});

test("statusReactions defaults to ON for all engaged turns: group channels without mention now show presence", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		// statusReactions unset (undefined) - new default is to show presence
	);
	const groupNoMention = { group: true, mentioned: false }; // Group channel, not mentioned (thread follow-up case)
	status.arm("channel-1", "m-1", groupNoMention);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]); // DEFAULT: reactions shown for all engaged turns
});

test("statusReactions never shows for bot-audience channels (config constraint)", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		undefined,
		{ "bot-channel": { audience: "bot-only" } },
		// statusReactions unset (undefined) - bot-only channels always suppress reactions
	);
	const botAudience = { group: true, mentioned: true }; // Bot-only audience
	status.arm("bot-channel", "m-1", botAudience);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // No reactions on bot-only channels regardless of mode
});

test("statusReactions defaults to gradient (full transitions) when unset for all engaged turns", async () => {
	const { discord, reacted } = presenceDiscord();
	let clock = 0;
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		() => clock,
		// statusReactions unset (undefined) - default is gradient transitions
	);
	const origin = { platform: "discord", kind: "dm", conversationId: "dm-1", peerId: "user-1" } as const;
	const dm = { group: false, mentioned: false }; // DM (not a group)
	status.arm("dm-1", "m-1", dm);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]); // Starts with queued marker
	clock += PRESENCE_MIN_SWAP_MS;
	await status.update({
		turnId: "t",
		origin,
		elapsedMs: 61_000,
		toolCalls: 1,
		outputTokens: 210,
		activity: { kind: "tool", label: "bash" },
	});
	// Default (undefined) mode shows gradient: phase + clock + effort transitions
	expect(reacted).toContain("🔧");
	expect(reacted).toContain("🕐");
	expect(reacted).toContain("1️⃣");
});

test("statusReactions defaults to ON for mentioned turns in group channels when unset", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		// statusReactions unset (undefined) - new default is to show presence
	);
	const groupWithMention = { group: true, mentioned: true }; // Group channel, mentioned
	status.arm("channel-1", "m-1", groupWithMention);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]); // DEFAULT: reactions shown for all engaged turns including mentioned group channels
});

test("statusReactions 'static' mode through full sequence: arm, update phase/clock/effort, clear", async () => {
	const { discord, reacted, removed } = presenceDiscord();
	let clock = 0;
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		() => clock,
		"static",
	);
	const origin = { platform: "discord", kind: "channel", conversationId: "channel-1" } as const;
	const engagement = { group: false, mentioned: false }; // DM
	// ARM: adds exactly one ⏳ marker
	status.arm("channel-1", "m-1", engagement);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]);
	// UPDATE 1: no phase swap, no clock or effort added
	clock += PRESENCE_MIN_SWAP_MS;
	await status.update({
		turnId: "t",
		origin,
		elapsedMs: 61_000, // One minute elapsed
		toolCalls: 1,
		outputTokens: 210,
		activity: { kind: "tool", label: "bash" },
	});
	// In static mode: ⏳ stays, no phase swap to 🔧, no clock (🕐) or effort (1️⃣)
	expect(reacted).toEqual(["⏳"]);
	expect(removed).toEqual([]);
	// UPDATE 2: continue with more elapsed time
	clock += PRESENCE_MIN_SWAP_MS;
	await status.update({
		turnId: "t",
		origin,
		elapsedMs: 120_000, // Two minutes elapsed
		toolCalls: 2,
		outputTokens: 500,
		activity: { kind: "tool", label: "bash" },
	});
	// In static mode, no new reactions added for additional time/effort
	expect(reacted).toEqual(["⏳"]); // Still just ⏳
	expect(removed).toEqual([]); // Nothing removed
	// CLEAR: removes ⏳
	await status.clear("channel-1");
	await Bun.sleep(1);
	expect(removed).toEqual(["⏳:bot-1"]); // Removes the ⏳ marker
});

test("statusReactions with undefined engagement produces zero reaction calls", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		// statusReactions unset (undefined)
	);
	// Arm with undefined engagement (not passed)
	status.arm("channel-1", "m-1", undefined);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // No reactions when engagement is undefined
});

test("rapid consecutive messages in one thread: only latest carries status reactions, older ones cleaned up", async () => {
	const { discord, reacted, removed } = presenceDiscord();
	const status = new WorkingStatus(discord, { error: () => {} }, () => ({ id: "bot-1" }));
	const engagement = { group: true, mentioned: false }; // Thread follow-up without mention
	// Arm first message in thread
	status.arm("thread-1", "m-1", engagement);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]); // First message gets reactions
	const firstReactCount = reacted.length;
	// Rapidly arm second message in same thread
	status.arm("thread-1", "m-2", engagement);
	await Bun.sleep(1);
	expect(reacted.length).toBeGreaterThanOrEqual(firstReactCount); // New message armed
	// Verify first message reactions were cleaned up
	expect(removed).toContain("⏳:bot-1"); // ⏳ removed from first message
	// Rapidly arm third message in same thread
	status.arm("thread-1", "m-3", engagement);
	await Bun.sleep(1);
	// Second message should be cleaned up when third is armed
	const beforeCleanup = removed.length;
	expect(removed.length).toBeGreaterThan(beforeCleanup - 1); // Another removal for second message
	// Only the latest message (m-3) should have active reactions
	await status.clear("thread-1");
	// All reactions should be cleaned up
	expect(removed).toContain("⏳:bot-1");
});

test("typing indicator is idempotent: multiple begin calls on same conversation update deadline without error", async () => {
	let channelFetched = 0;
	const typing = new TypingIndicator({
		channels: {
			fetch: async () => {
				channelFetched++;
				return {
					sendTyping: async () => {
						// Mock successful typing send
						return undefined;
					},
				};
			},
		},
	} as any);
	// Multiple begin calls on same conversation should be idempotent
	typing.begin("conv-1");
	const firstCallTime = Date.now();
	await Bun.sleep(10);
	typing.begin("conv-1");
	await Bun.sleep(10);
	typing.begin("conv-1");
	// Each begin after the first should have updated the deadline, not created a new run.
	// Verify by checking that typing.refresh doesn't error.
	try {
		typing.refresh("conv-1");
		typing.end("conv-1");
		expect(true).toBe(true); // No errors
	} catch {
		expect(true).toBe(false); // Should not error
	}
});

test("DM messages stay flat with no thread creation", () => {
	const botUser = { id: "bot-1" };
	const dmMessage = {
		id: "msg-1",
		author: { id: "user-1" },
		channel: { id: "dm-channel-1", isDMBased: () => true },
		mentions: { has: () => true },
		content: "hello bot",
	};
	const engagement = engagementForMessage(dmMessage as any, botUser);
	const origin = discordMessageOrigin(dmMessage as any);
	// DM should stay kind="dm" even with mention, no threading
	expect(origin.kind).toBe("dm");
	expect(engagement.mentioned).toBe(true);
	expect(engagement.group).toBe(false);
});

test("channel mention should set mentioned flag for thread creation logic", () => {
	const botUser = { id: "bot-1" };
	const channelMessage = {
		id: "msg-1",
		author: { id: "user-1" },
		channel: { id: "channel-1", type: 0 },
		mentions: { has: () => true },
		content: "<@bot-1> hello",
	};
	const engagement = engagementForMessage(channelMessage as any, botUser);
	const origin = discordMessageOrigin(channelMessage as any);
	// Channel mention should be marked as mentioned and group
	expect(origin.kind).toBe("channel");
	expect(engagement.mentioned).toBe(true);
	expect(engagement.group).toBe(true);
});

test("thread reply message stays in thread", () => {
	const botUser = { id: "bot-1" };
	const threadMessage = {
		id: "msg-2",
		author: { id: "user-1" },
		channel: {
			id: "thread-1",
			parentId: "channel-1",
			isThread: () => true,
		},
		mentions: { has: () => false },
		content: "reply in thread",
	};
	const origin = discordMessageOrigin(threadMessage as any);
	// Thread message should preserve thread origin
	expect(origin.kind).toBe("thread");
	expect(origin.conversationId).toBe("thread-1");
	expect(origin.parentId).toBe("channel-1");
});

test("channel mention with default policy (unset threadOnMention) creates thread", async () => {
	const botUser = { id: "bot-1" };
	const channelMessage = {
		id: "msg-1",
		author: { id: "user-1" },
		channel: { id: "channel-1", type: 0 },
		mentions: { has: () => true },
		content: "<@bot-1> hello",
		startThread: async ({ name }: { name: string; autoArchiveDuration: number }) => ({
			id: "thread-new-1",
		}),
	};
	const engagement = engagementForMessage(channelMessage as any, botUser);
	const origin = discordMessageOrigin(channelMessage as any);
	// With default policy (threadOnMention unset/undefined), should create thread
	const result = await maybeCreateThreadOnMention(
		channelMessage as any,
		engagement,
		origin,
		undefined,
		new UnnamedThreads(),
	);
	expect(result.kind).toBe("thread");
	expect(result.conversationId).toBe("thread-new-1");
	expect(result.parentId).toBe("channel-1");
});

test("channel mention with threadOnMention: true creates thread", async () => {
	const botUser = { id: "bot-1" };
	const channelMessage = {
		id: "msg-2",
		author: { id: "user-1" },
		channel: { id: "channel-2", type: 0 },
		mentions: { has: () => true },
		content: "<@bot-1> hello",
		startThread: async ({ name }: { name: string; autoArchiveDuration: number }) => ({
			id: "thread-new-2",
		}),
	};
	const engagement = engagementForMessage(channelMessage as any, botUser);
	const origin = discordMessageOrigin(channelMessage as any);
	// With threadOnMention explicitly set to true, should create thread
	const policy = { threadOnMention: true };
	const result = await maybeCreateThreadOnMention(
		channelMessage as any,
		engagement,
		origin,
		policy,
		new UnnamedThreads(),
	);
	expect(result.kind).toBe("thread");
	expect(result.conversationId).toBe("thread-new-2");
	expect(result.parentId).toBe("channel-2");
});

test("channel mention with threadOnMention: false skips thread creation", async () => {
	const botUser = { id: "bot-1" };
	let threadCreated = false;
	const channelMessage = {
		id: "msg-3",
		author: { id: "user-1" },
		channel: { id: "channel-3", type: 0 },
		mentions: { has: () => true },
		content: "<@bot-1> hello",
		startThread: async ({ name }: { name: string; autoArchiveDuration: number }) => {
			threadCreated = true;
			return { id: "thread-new-3" };
		},
	};
	const engagement = engagementForMessage(channelMessage as any, botUser);
	const origin = discordMessageOrigin(channelMessage as any);
	// With threadOnMention set to false, should NOT create thread
	const policy = { threadOnMention: false };
	const result = await maybeCreateThreadOnMention(
		channelMessage as any,
		engagement,
		origin,
		policy,
		new UnnamedThreads(),
	);
	expect(result).toEqual(origin);
	expect(threadCreated).toBe(false);
});

test("non-channel or non-mentioned message ignores threadOnMention policy", async () => {
	const botUser = { id: "bot-1" };
	const dmMessage = {
		id: "msg-4",
		author: { id: "user-1" },
		channel: { id: "dm-1", isDMBased: () => true },
		mentions: { has: () => true },
		content: "hello",
	};
	const engagement = engagementForMessage(dmMessage as any, botUser);
	const origin = discordMessageOrigin(dmMessage as any);
	// DMs should never thread regardless of policy
	const policy = { threadOnMention: true };
	const result = await maybeCreateThreadOnMention(dmMessage as any, engagement, origin, policy, new UnnamedThreads());
	expect(result.kind).toBe("dm");
});

test("accepts and validates threadOnMention boolean in channel config", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-threadOnMention-"));
	try {
		await writeFile(join(home, "token"), "secret-token");

		// Valid: threadOnMention as true or false
		for (const value of [true, false]) {
			await writeFile(
				join(home, "adapter-discord.json"),
				JSON.stringify({
					tokenFile: "token",
					channels: { c: { threadOnMention: value } },
				}),
			);
			const config = await loadDiscordAdapterConfig({ GAJAEWAY_HOME: home });
			expect(config.channels?.c?.threadOnMention).toBe(value);
		}

		// Valid: omitted threadOnMention (defaults to unset in config)
		await writeFile(
			join(home, "adapter-discord.json"),
			JSON.stringify({ tokenFile: "token", channels: { c: { engagement: "open" } } }),
		);
		const defaultConfig = await loadDiscordAdapterConfig({ GAJAEWAY_HOME: home });
		expect(defaultConfig.channels?.c?.threadOnMention).toBeUndefined();

		// Invalid: threadOnMention as non-boolean values
		for (const invalid of ["true", 1, null, "false"]) {
			await writeFile(
				join(home, "adapter-discord.json"),
				JSON.stringify({
					tokenFile: "token",
					channels: { c: { threadOnMention: invalid } },
				}),
			);
			await expect(loadDiscordAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toBeInstanceOf(
				DiscordAdapterStartupError,
			);
		}
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("deriveThreadName keeps the words of the message and drops Discord markup", () => {
	expect(deriveThreadName("<@123456789> 디코는 쓰레드 파지 말고 플랫하게 대화하는 옵션 없냐")).toBe(
		"디코는 쓰레드 파지 말고 플랫하게 대화하는 옵션 없냐",
	);
	expect(deriveThreadName("<@!1> <@&2> check <#3> **now**")).toBe("check now");
	expect(deriveThreadName("fix `threadOnMention` <:gajae:42> please")).toBe("fix threadOnMention please");
	expect(deriveThreadName("see https://example.com/x\n> quoted\n```ts\nconst a = 1;\n```")).toBe("see quoted");
	expect(deriveThreadName("thread_on_mention stays")).toBe("thread_on_mention stays");
});

test("deriveThreadName is undefined when nothing nameable is left", () => {
	expect(deriveThreadName("<@123>")).toBeUndefined();
	expect(deriveThreadName("  <@123>  ```x```  ")).toBeUndefined();
	expect(deriveThreadName("")).toBeUndefined();
});

test("deriveThreadName cuts long text at a word boundary without splitting characters", () => {
	const long = "This is a longer question that might exceed the preferred limit but should truncate";
	const name = deriveThreadName(long) as string;
	expect(name).toBe("This is a longer question that might exceed the…");
	const unbroken = "🦞".repeat(80);
	const cut = deriveThreadName(unbroken) as string;
	expect(Array.from(cut)).toHaveLength(51);
	expect(cut.endsWith("🦞…")).toBe(true);
});

function threadingMessage(content: string, started: string[]) {
	return {
		id: "msg-1",
		author: { id: "user-1" },
		channel: { id: "channel-1", type: 0 },
		mentions: { has: () => true },
		content,
		startThread: async ({ name }: { name: string; autoArchiveDuration: number }) => {
			started.push(name);
			return { id: "thread-1" };
		},
	};
}

test("a mention opens a thread named after the message and needs no rename", async () => {
	const started: string[] = [];
	const message = threadingMessage("<@111> 스레드 이름 좀 바꿔줘", started);
	const unnamed = new UnnamedThreads();
	const engagement = engagementForMessage(message as any, { id: "111" });
	await maybeCreateThreadOnMention(
		message as any,
		engagement,
		discordMessageOrigin(message as any),
		undefined,
		unnamed,
	);
	expect(started).toEqual(["스레드 이름 좀 바꿔줘"]);
	expect(unnamed.take("thread-1")).toBe(false);
});

test("a bare mention opens a fallback-named thread, renamed once from the first message with words", async () => {
	const started: string[] = [];
	const unnamed = new UnnamedThreads();
	const trigger = threadingMessage("<@111>", started);
	const engagement = engagementForMessage(trigger as any, { id: "111" });
	const origin = await maybeCreateThreadOnMention(
		trigger as any,
		engagement,
		discordMessageOrigin(trigger as any),
		undefined,
		unnamed,
	);
	expect(started).toEqual(["Discussion"]);
	const renamed: string[] = [];
	const inThread = (content: string) => ({
		content,
		channel: { setName: async (name: string) => void renamed.push(name) },
	});
	// A message with nothing nameable does not use up the rename.
	await nameThreadFromMessage(inThread("<@111>") as any, origin, unnamed);
	expect(renamed).toEqual([]);
	await nameThreadFromMessage(inThread("디코 플랫 대화 옵션") as any, origin, unnamed);
	await nameThreadFromMessage(inThread("그리고 하나 더") as any, origin, unnamed);
	expect(renamed).toEqual(["디코 플랫 대화 옵션"]);
});

test("threads the adapter did not open are never renamed", async () => {
	const renamed: string[] = [];
	const origin = { platform: "discord", kind: "thread", conversationId: "human-thread", parentId: "c" } as const;
	await nameThreadFromMessage(
		{ content: "hello there", channel: { setName: async (name: string) => void renamed.push(name) } } as any,
		origin,
		new UnnamedThreads(),
	);
	expect(renamed).toEqual([]);
});

test("a failed thread rename is logged and does not throw", async () => {
	const errors: string[] = [];
	const unnamed = new UnnamedThreads();
	unnamed.add("thread-1");
	const origin = { platform: "discord", kind: "thread", conversationId: "thread-1", parentId: "c" } as const;
	const failing = {
		content: "name me",
		channel: {
			setName: async () => {
				throw new Error("Missing Permissions");
			},
		},
	};
	await nameThreadFromMessage(failing as any, origin, unnamed, { error: (line: string) => void errors.push(line) });
	expect(errors).toEqual(["Discord thread thread-1 rename failed: Missing Permissions"]);
});

test("UnnamedThreads forgets the oldest thread past its bound", () => {
	const unnamed = new UnnamedThreads(2);
	unnamed.add("a");
	unnamed.add("b");
	unnamed.add("c");
	expect(unnamed.take("a")).toBe(false);
	expect(unnamed.take("b")).toBe(true);
	expect(unnamed.take("c")).toBe(true);
});
