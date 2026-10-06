import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, ChatProgressPayload } from "@gajae-gateway/protocol";
import { PRESENCE_MIN_SWAP_MS } from "@gajae-gateway/protocol";
import { DiscordAdapterStartupError, loadDiscordAdapterConfig } from "../src/config";
import {
	addressedTurn,
	chunkDiscordMessage,
	DISCORD_SLASH_COMMANDS,
	type DiscordClientLike,
	engagementForMessage,
	type GatewayClientLike,
	handleModelAutocomplete,
	handleSlashCommand,
	isPresenceReaction,
	LruSet,
	settleDiscordDelivery,
	subscribeDiscordDeliveries,
	subscribeDiscordProgress,
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

// Purpose: prove an explicit MEDIA directive becomes a real Discord upload and is not exposed as text.
// Expected: one captioned native file upload is sent without exposing the path; completion requires delivery confirmation.
test("uploads an explicit MEDIA path as a Discord attachment", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-media-"));
	try {
		const filePath = join(home, "chatting_client.cpp");
		const bytes = Buffer.from("int main() {}\n");
		await writeFile(filePath, bytes);
		const requests: Array<{ verb: string; params: unknown }> = [];
		const sent: unknown[] = [];
		const discord: DiscordClientLike = {
			channels: { fetch: async () => ({ send: async (payload: unknown) => void sent.push(payload) }) },
		};
		await settleDiscordDelivery(mockGateway(requests), discord, delivery(`Source file:\nMEDIA:${filePath}`));
		expect(sent).toHaveLength(1);
		const upload = sent[0] as { content: string; files: Array<{ name: string; attachment: Buffer }> };
		expect(upload.content).toBe("Source file:");
		expect(upload.files[0]?.name).toBe("chatting_client.cpp");
		expect(upload.files[0]?.attachment).toEqual(bytes);
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

// Purpose: verify a file-only reply still reaches Discord when removing MEDIA leaves no caption.
// Expected: one empty-content upload is sent and the delivery confirms; both are required for completion.
test("sends a MEDIA attachment without an empty text-only message", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-media-only-"));
	try {
		const filePath = join(home, "report.pdf");
		await writeFile(filePath, Buffer.from("pdf"));
		const requests: Array<{ verb: string; params: unknown }> = [];
		const sent: unknown[] = [];
		const discord: DiscordClientLike = {
			channels: { fetch: async () => ({ send: async (payload: unknown) => void sent.push(payload) }) },
		};
		await settleDiscordDelivery(mockGateway(requests), discord, delivery(`MEDIA:${filePath}`));
		expect(sent).toHaveLength(1);
		expect(sent[0]).toMatchObject({ content: "", files: [{ name: "report.pdf" }] });
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

// Purpose: ensure redelivery metadata never hides a leading MEDIA directive or exposes its local path.
// Expected: the recovered warning and attachment are delivered together and the ledger confirms the send.
test("preserves MEDIA uploads on ambiguous redelivery", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-media-retry-"));
	try {
		const filePath = join(home, "retry.cpp");
		await writeFile(filePath, Buffer.from("source"));
		const requests: Array<{ verb: string; params: unknown }> = [];
		const sent: unknown[] = [];
		let failFirstSend = true;
		const discord: DiscordClientLike = {
			channels: {
				fetch: async () => ({
					send: async (payload: unknown) => {
						if (failFirstSend) {
							failFirstSend = false;
							throw Object.assign(new Error("connection reset after dispatch"), { code: "ECONNRESET" });
						}
						void sent.push(payload);
					},
				}),
			},
		};
		await settleDiscordDelivery(mockGateway(requests), discord, delivery(`MEDIA:${filePath}`));
		await settleDiscordDelivery(mockGateway(requests), discord, {
			...delivery(`MEDIA:${filePath}`),
			duplicateWarning: true,
		});
		expect(sent).toHaveLength(1);
		const upload = sent[0] as { content: string; files: Array<{ name: string }> };
		expect(upload.content).toContain("[recovered - may be a duplicate]");
		expect(upload.content).not.toContain(filePath);
		expect(upload.files[0]?.name).toBe("retry.cpp");
		expect(requests).toEqual([
			{
				verb: "delivery.fail",
				params: {
					deliveryId: "delivery-1",
					reason: "connection reset after dispatch",
					ambiguous: true,
				},
			},
			{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } },
		]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

// Purpose: ensure long captions keep their upload attached to the first Discord chunk.
// Expected: the first chunk carries both reply metadata and files; remaining chunks contain text only.
test("keeps files and reply metadata with the first chunk of a long caption", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-media-long-"));
	try {
		const filePath = join(home, "long.txt");
		await writeFile(filePath, Buffer.from("long"));
		const requests: Array<{ verb: string; params: unknown }> = [];
		const sent: unknown[] = [];
		const discord: DiscordClientLike = {
			channels: { fetch: async () => ({ send: async (payload: unknown) => void sent.push(payload) }) },
		};
		await settleDiscordDelivery(mockGateway(requests), discord, {
			...delivery(`${"x".repeat(2_001)}\nMEDIA:${filePath}`),
			replyToMessageId: "message-1",
		});
		expect(sent).toHaveLength(2);
		expect(sent[0]).toMatchObject({
			content: "x".repeat(2_000),
			reply: { messageReference: "message-1", failIfNotExists: false },
			files: [{ name: "long.txt" }],
		});
		expect(sent[1]).toBe("x");
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

// Purpose: ensure missing requested files fail visibly instead of confirming a caption-only send.
// Expected: no Discord message is sent and the delivery ledger receives a failure; that is the completion condition.
test("fails delivery before posting when a MEDIA file is missing", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: unknown[] = [];
	const discord: DiscordClientLike = {
		channels: { fetch: async () => ({ send: async (payload: unknown) => void sent.push(payload) }) },
	};
	await settleDiscordDelivery(mockGateway(requests), discord, delivery("Caption\nMEDIA:/not/a/real/gajaeway-file.bin"));
	expect(sent).toEqual([]);
	expect(requests).toHaveLength(1);
	expect(requests[0]?.verb).toBe("delivery.fail");
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

test("presence is shown only where the persona was addressed: DM, mention, or open-channel promotion", () => {
	expect(addressedTurn({ group: false, mentioned: false })).toBe(true); // DM
	expect(addressedTurn({ group: true, mentioned: true })).toBe(true); // mention, or open promotion
	expect(addressedTurn({ group: true, mentioned: false })).toBe(false); // overheard public channel
});

test("an unaddressed public-channel turn shows no presence until it is armed; clear disarms", async () => {
	const { discord, reacted, removed } = presenceDiscord();
	const status = new WorkingStatus(discord, { error: () => {} }, () => ({ id: "bot-1" }));
	const origin = { platform: "discord", kind: "channel", conversationId: "public-1" } as const;
	const tick = { turnId: "t", origin, elapsedMs: 16_000, toolCalls: 1, outputTokens: 210 };
	await status.update(tick);
	await status.update(tick);
	expect(reacted).toEqual([]);
	// Explicitly pass engagement to show reactions when arming; this is a DM (not a group)
	const engagement = { group: false, mentioned: false };
	status.arm("public-1", "m-9", engagement);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]);
	await status.clear("public-1");
	expect(removed).toEqual(["⏳:bot-1"]);
	// Disarmed: the next turn's ticks are silent again until re-armed.
	await status.update(tick);
	expect(reacted).toHaveLength(1);
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

test("statusReactions defaults to 'off' for group channels when unset", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		// statusReactions unset (undefined)
	);
	const groupNoMention = { group: true, mentioned: false }; // Group channel, not mentioned
	status.arm("channel-1", "m-1", groupNoMention);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // No reactions shown with default off behavior
});

test("statusReactions defaults to 'off' for bot-audience channels when unset", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		undefined,
		{ "bot-channel": { audience: "bot-only" } },
		// statusReactions unset (undefined)
	);
	const botAudience = { group: true, mentioned: true }; // Bot-only audience
	status.arm("bot-channel", "m-1", botAudience);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // No reactions shown with default off behavior
});

test("statusReactions defaults to 'gradient' for DMs when unset", async () => {
	const { discord, reacted } = presenceDiscord();
	let clock = 0;
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		() => clock,
		// statusReactions unset (undefined)
	);
	const origin = { platform: "discord", kind: "dm", conversationId: "dm-1", peerId: "user-1" } as const;
	const dm = { group: false, mentioned: false }; // DM (not a group)
	status.arm("dm-1", "m-1", dm);
	await Bun.sleep(1);
	expect(reacted).toEqual(["⏳"]); // Default gradient behavior for DMs
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

test("statusReactions defaults to 'off' for mentioned turns in group channels when unset", async () => {
	const { discord, reacted } = presenceDiscord();
	const status = new WorkingStatus(
		discord,
		{ error: () => {} },
		() => ({ id: "bot-1" }),
		Date.now,
		// statusReactions unset (undefined)
	);
	const groupWithMention = { group: true, mentioned: true }; // Group channel, mentioned
	status.arm("channel-1", "m-1", groupWithMention);
	await Bun.sleep(1);
	expect(reacted).toEqual([]); // Default is 'off' for ANY group channel, even when mentioned
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
