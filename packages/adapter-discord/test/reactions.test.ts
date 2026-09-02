import { expect, test } from "bun:test";
import type { ChatMessagePayload } from "@gajaeway/protocol";
import type { DiscordClientLike, GatewayClientLike } from "../src/main";
import {
	type DiscordInboundReaction,
	describeInboundReaction,
	GuildEmojiResolver,
	type InboundReactionDescription,
	ReactionRateLimiter,
	reactionRetryAfterMs,
	settleDiscordReaction,
} from "../src/reactions";

const bot = { id: "bot-1" };
const silent = { error: () => {} };

test("reacts with the allowlist unicode and never posts a message", async () => {
	const requests: Request[] = [];
	const harness = channelHarness();
	await settleDiscordReaction(
		mockGateway(requests),
		harness.discord,
		reactionDelivery(),
		new GuildEmojiResolver(),
		instantLimiter(),
	);
	expect(harness.reacted).toEqual(["👍"]);
	expect(harness.sent).toEqual([]);
	expect(harness.fetchedMessages).toEqual(["target-1"]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("prefers a same-named custom guild emoji and caches the lookup, hit and miss alike", async () => {
	const resolver = new GuildEmojiResolver();
	const present = guild("guild-1", [{ name: "thumbsup", id: "555" }]);
	expect(resolver.resolve(present.guild, "thumbsup", "👍")).toBe("thumbsup:555");
	expect(resolver.resolve(present.guild, "thumbsup", "👍")).toBe("thumbsup:555");
	expect(present.scans).toBe(1);

	const lacking = guild("guild-2", [{ name: "something-else", id: "777" }]);
	expect(lacking.resolve("thumbsup", "👍", resolver)).toBe("👍");
	expect(lacking.resolve("thumbsup", "👍", resolver)).toBe("👍");
	expect(lacking.scans).toBe(1);

	// A DM has no guild at all, so it must not even try.
	expect(resolver.resolve(undefined, "thumbsup", "👍")).toBe("👍");
});

test("a custom guild emoji is reacted with name:id and still posts nothing", async () => {
	const requests: Request[] = [];
	const owner = guild("guild-1", [{ name: "lobster", id: "999" }]);
	const harness = channelHarness(owner.guild);
	await settleDiscordReaction(
		mockGateway(requests),
		harness.discord,
		reactionDelivery({ targetMessageId: "target-1", emoji: "🦞", emojiName: "lobster" }),
		new GuildEmojiResolver(),
		instantLimiter(),
	);
	expect(harness.reacted).toEqual(["lobster:999"]);
	expect(harness.sent).toEqual([]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("a guild lacking the named emoji degrades to unicode and never leaks emoji markup to send()", async () => {
	const requests: Request[] = [];
	const other = guild("guild-1", [{ name: "not-lobster", id: "999" }]);
	const harness = channelHarness(other.guild);
	await settleDiscordReaction(
		mockGateway(requests),
		harness.discord,
		reactionDelivery({ targetMessageId: "target-1", emoji: "🦞", emojiName: "lobster" }),
		new GuildEmojiResolver(),
		instantLimiter(),
	);
	expect(harness.reacted).toEqual(["🦞"]);
	expect(harness.sent).toEqual([]);
	// The custom-emoji spellings must never reach a channel; only react() may see them.
	expect(harness.reacted[0]).not.toContain("<:");
	expect(harness.reacted[0]).not.toContain(":lobster:");
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("a deleted target message fails definitively and never confirms", async () => {
	const requests: Request[] = [];
	const discord: DiscordClientLike = {
		channels: {
			fetch: async () => ({
				send: async () => {},
				messages: { fetch: async () => Promise.reject(Object.assign(new Error("Unknown Message"), { code: 10008 })) },
			}),
		},
	};
	await settleDiscordReaction(
		mockGateway(requests),
		discord,
		reactionDelivery(),
		new GuildEmojiResolver(),
		instantLimiter(),
		silent,
	);
	expect(requests).toEqual([
		{
			verb: "delivery.fail",
			params: { deliveryId: "delivery-1", reason: "Unknown Message", ambiguous: false },
		},
	]);
});

test("a non-snowflake target Discord rejects with 50035 fails definitively (live: UUID target replayed 19 times)", async () => {
	const requests: Request[] = [];
	const discord: DiscordClientLike = {
		channels: {
			fetch: async () => ({
				send: async () => {},
				messages: {
					fetch: async (id: string) =>
						Promise.reject(
							Object.assign(
								new Error(`Invalid Form Body\nmessage_id[NUMBER_TYPE_COERCE]: Value "${id}" is not snowflake.`),
								{
									code: 50035,
								},
							),
						),
				},
			}),
		},
	};
	await settleDiscordReaction(
		mockGateway(requests),
		discord,
		reactionDelivery({ targetMessageId: "406f6a86-8033-4655-a91b-a8cc36019bdb", emoji: "👍", emojiName: "thumbsup" }),
		new GuildEmojiResolver(),
		instantLimiter(),
		silent,
	);
	expect(requests).toHaveLength(1);
	expect(requests[0]?.verb).toBe("delivery.fail");
	expect(requests[0]?.params).toMatchObject({ deliveryId: "delivery-1", ambiguous: false });
});

test("Discord 50035 Invalid Form Body is permanent: the same request would fail identically on every redelivery", async () => {
	const requests: Request[] = [];
	const harness = channelHarness(undefined, async () => {
		throw Object.assign(new Error("Invalid Form Body"), { code: 50035 });
	});
	await settleDiscordReaction(
		mockGateway(requests),
		harness.discord,
		reactionDelivery({ targetMessageId: "154471973767270411", emoji: "👍", emojiName: "thumbsup" }),
		new GuildEmojiResolver(),
		instantLimiter(),
		silent,
	);
	expect(requests).toEqual([
		{ verb: "delivery.fail", params: { deliveryId: "delivery-1", reason: "Invalid Form Body", ambiguous: false } },
	]);
});

test("a channel that cannot hold reactions fails definitively", async () => {
	const requests: Request[] = [];
	const discord: DiscordClientLike = { channels: { fetch: async () => ({ send: async () => {} }) } };
	await settleDiscordReaction(
		mockGateway(requests),
		discord,
		reactionDelivery(),
		new GuildEmojiResolver(),
		instantLimiter(),
		silent,
	);
	expect(requests).toHaveLength(1);
	const failure = requests[0] as { verb: string; params: { ambiguous: boolean; reason: string } };
	expect(failure.verb).toBe("delivery.fail");
	expect(failure.params.ambiguous).toBe(false);
	expect(failure.params.reason).toContain("cannot receive reactions");
});

test("a 429 carrying retry_after is retried through the injected sleep and then confirms", async () => {
	const requests: Request[] = [];
	const slept: number[] = [];
	let attempts = 0;
	const harness = channelHarness(undefined, () => {
		attempts++;
		if (attempts === 1) {
			return Promise.reject(
				Object.assign(new Error("You are being rate limited."), { status: 429, retry_after: 0.75 }),
			);
		}
		return Promise.resolve();
	});
	await settleDiscordReaction(
		mockGateway(requests),
		harness.discord,
		reactionDelivery(),
		new GuildEmojiResolver(),
		instantLimiter(slept),
	);
	expect(attempts).toBe(2);
	expect(slept).toContain(750);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("a 429 that outlives the attempt bound reports a failed delivery", async () => {
	const requests: Request[] = [];
	const slept: number[] = [];
	let attempts = 0;
	const harness = channelHarness(undefined, () => {
		attempts++;
		return Promise.reject(Object.assign(new Error("rate limited"), { status: 429, rawError: { retry_after: 1 } }));
	});
	await settleDiscordReaction(
		mockGateway(requests),
		harness.discord,
		reactionDelivery(),
		new GuildEmojiResolver(),
		instantLimiter(slept),
		silent,
	);
	expect(attempts).toBe(3);
	expect(slept.filter((ms) => ms === 1_000)).toHaveLength(2);
	expect(requests).toEqual([
		{ verb: "delivery.fail", params: { deliveryId: "delivery-1", reason: "rate limited", ambiguous: true } },
	]);
});

test("a rejection without retry instructions is not retried", async () => {
	const slept: number[] = [];
	let attempts = 0;
	const limiter = instantLimiter(slept);
	await expect(
		limiter.run("channel-1", () => {
			attempts++;
			return Promise.reject(new Error("Missing Permissions"));
		}),
	).rejects.toThrow("Missing Permissions");
	expect(attempts).toBe(1);
	expect(slept).toEqual([]);
});

test("reads the retry hint in the units each source actually uses", () => {
	// Discord's 429 body and the HTTP header are SECONDS.
	expect(reactionRetryAfterMs({ retry_after: 0.25 })).toBe(250);
	expect(reactionRetryAfterMs({ rawError: { retry_after: 64.57 } })).toBe(64_570);
	expect(reactionRetryAfterMs({ headers: { "retry-after": "2" } })).toBe(2_000);
	expect(reactionRetryAfterMs({ response: { headers: new Headers({ "retry-after": "3" }) } })).toBe(3_000);
	// discord.js RateLimitError reports MILLISECONDS: reading it as seconds would
	// turn a 1.5s wait into a 25 minute one.
	expect(reactionRetryAfterMs({ retryAfter: 1_500 })).toBe(1_500);
	expect(reactionRetryAfterMs({ timeToReset: 900 })).toBe(900);
	// A body value wins over the discord.js field when both are present.
	expect(reactionRetryAfterMs({ retry_after: 1, retryAfter: 1_000 })).toBe(1_000);
	expect(reactionRetryAfterMs(new Error("nope"))).toBeUndefined();
	expect(reactionRetryAfterMs({ retry_after: -5 })).toBeUndefined();
});

test("spaces successive reactions on one channel by the self-imposed floor", async () => {
	const slept: number[] = [];
	let clock = 1_000;
	const limiter = new ReactionRateLimiter(
		250,
		async (ms) => {
			slept.push(ms);
			clock += ms;
		},
		() => clock,
	);
	await limiter.run("channel-1", async () => {});
	await limiter.run("channel-1", async () => {});
	// Other channels have their own budget, so they are not made to wait.
	await limiter.run("channel-2", async () => {});
	expect(slept).toEqual([250]);
	// Once a channel's deadline has elapsed its entry is evicted, so the map cannot
	// grow one permanent entry per channel that ever saw a reaction. Eviction must
	// not cost spacing: after the jump the next call reserves a fresh slot without
	// waiting, and the one after it waits the full floor again. (The assertion
	// above is what catches an inverted predicate: deleting a still-live deadline
	// would drop the 250ms wait entirely.)
	clock += 1_000;
	await limiter.run("channel-1", async () => {});
	expect(slept).toEqual([250]);
	await limiter.run("channel-1", async () => {});
	expect(slept).toEqual([250, 250]);
});

test("inbound reactions ignore our own bot and describe human adds and removes identically", () => {
	const human = { id: "user-1", username: "eunji", globalName: "Eunji" };
	const expected: InboundReactionDescription = {
		action: "add",
		origin: { platform: "discord", kind: "channel", conversationId: "channel-1" },
		targetMessageId: "target-1",
		emoji: "👍",
		engagement: {
			mentioned: false,
			group: true,
			authorId: "user-1",
			authorName: "Eunji",
			authorHandle: "eunji",
			channelLabel: "#general",
			serverLabel: "Lobster HQ",
		},
	};
	expect(describeInboundReaction(inboundReaction("👍"), human, bot, "add")).toEqual(expected);
	// A removal is the same metadata with the retracting action; both are reported.
	expect(describeInboundReaction(inboundReaction("👍"), human, bot, "remove")).toEqual({
		...expected,
		action: "remove",
	});
	expect(describeInboundReaction(inboundReaction("👍"), { id: "bot-1" }, bot, "add")).toBeUndefined();
	expect(describeInboundReaction(inboundReaction("👍"), { id: "bot-1" }, bot, "remove")).toBeUndefined();
});

test("inbound reactions report a custom emoji as custom:name and drop unplaceable ones", () => {
	const human = { id: "user-1", username: "eunji" };
	const custom = describeInboundReaction(inboundReaction("lobster", "999"), human, bot, "add");
	// Never `:lobster:`: the persona reads this metadata and could echo it into a
	// channel, where the colon form renders as literal text.
	expect(custom?.emoji).toBe("custom:lobster");
	expect(custom?.emoji).not.toContain(":lobster:");
	const noChannel = { emoji: { name: "👍" }, message: { id: "target-1" } };
	expect(describeInboundReaction(noChannel, human, bot, "add")).toBeUndefined();
	const noMessageId = { emoji: { name: "👍" }, message: { channel: { id: "channel-1" } } };
	expect(describeInboundReaction(noMessageId, human, bot, "add")).toBeUndefined();
	const noEmojiName = { emoji: { name: null }, message: { id: "target-1", channel: { id: "channel-1" } } };
	expect(describeInboundReaction(noEmojiName, human, bot, "add")).toBeUndefined();
});

test("a DM reaction is not a group engagement and keeps the reactor as peer", () => {
	const described = describeInboundReaction(
		{ emoji: { name: "👀" }, message: { id: "target-1", channel: { id: "dm-1", isDMBased: () => true } } },
		{ id: "user-1" },
		bot,
		"add",
	);
	expect(described?.origin).toEqual({ platform: "discord", kind: "dm", conversationId: "dm-1", peerId: "user-1" });
	expect(described?.engagement).toEqual({ mentioned: false, group: false, authorId: "user-1" });
});

type Request = { verb: string; params: unknown };

function reactionDelivery(
	reaction: { targetMessageId: string; emoji: string; emojiName: string } = {
		targetMessageId: "target-1",
		emoji: "👍",
		emojiName: "thumbsup",
	},
): ChatMessagePayload {
	return {
		turnId: "turn-1",
		origin: { platform: "discord", kind: "channel", conversationId: "channel-1" },
		role: "assistant",
		text: reaction.emoji,
		final: true,
		deliveryId: "delivery-1",
		reaction,
	};
}

function mockGateway(requests: Request[]): Pick<GatewayClientLike, "request"> {
	return {
		request: async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return {} as T;
		},
	};
}

/** A limiter whose spacing and 429 waits are recorded instead of actually waited out. */
function instantLimiter(slept: number[] = []): ReactionRateLimiter {
	return new ReactionRateLimiter(
		250,
		async (ms) => {
			slept.push(ms);
		},
		() => 0,
	);
}

function guild(
	id: string,
	emojis: readonly { name: string | null; id: string }[],
): {
	guild: { id: string; emojis: { readonly cache: Iterable<{ name: string | null; id: string }> } };
	readonly scans: number;
	resolve(name: string, unicode: string, resolver: GuildEmojiResolver): string;
} {
	let scans = 0;
	const value = {
		guild: {
			id,
			emojis: {
				get cache() {
					scans++;
					return emojis;
				},
			},
		},
		get scans() {
			return scans;
		},
		resolve(name: string, unicode: string, resolver: GuildEmojiResolver) {
			return resolver.resolve(value.guild, name, unicode);
		},
	};
	return value;
}

/** A reactable channel that records every react() and every send() it is asked for. */
function channelHarness(
	owner?: { id: string; emojis: { readonly cache: Iterable<{ name: string | null; id: string }> } },
	react?: (emoji: string) => Promise<unknown>,
): {
	discord: DiscordClientLike;
	readonly reacted: string[];
	readonly sent: string[];
	readonly fetchedMessages: string[];
} {
	const reacted: string[] = [];
	const sent: string[] = [];
	const fetchedMessages: string[] = [];
	const discord: DiscordClientLike = {
		channels: {
			fetch: async () => ({
				...(owner ? { guild: owner } : {}),
				send: async (text: string) => void sent.push(text),
				messages: {
					fetch: async (id: string) => {
						fetchedMessages.push(id);
						return {
							react: async (emoji: string) => {
								reacted.push(emoji);
								if (react) await react(emoji);
							},
						};
					},
				},
			}),
		},
	};
	return { discord, reacted, sent, fetchedMessages };
}

function inboundReaction(name: string, id?: string): DiscordInboundReaction {
	return {
		emoji: { name, ...(id ? { id } : {}) },
		message: {
			id: "target-1",
			channel: { id: "channel-1", name: "general" },
			guild: { name: "Lobster HQ" },
		},
	};
}
