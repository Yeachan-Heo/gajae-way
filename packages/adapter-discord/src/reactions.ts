import type { ChatMessagePayload, EngagementContext, OriginRef, ReactionAction } from "@gajae-gateway/protocol";
import { resolveDisplayName, resolveServerTag } from "./author";
import { type DiscordClientLike, deliveryFailureIsAmbiguous, type GatewayClientLike } from "./main";
import { type DiscordMessageOriginShape, discordMessageOrigin } from "./origin";

/** Our own self-throttle between reaction requests on one channel; see ReactionRateLimiter. */
const REACTION_MIN_SPACING_MS = 250;
/** Total tries for one reaction, including the first: two 429 retries then give up. */
const REACTION_MAX_ATTEMPTS = 3;

/** One custom guild emoji, as reachable from `guild.emojis.cache`. */
export interface DiscordGuildEmojiLike {
	readonly name: string | null;
	readonly id: string;
}

/** The guild slice a reaction needs: its id (cache key) and its custom emoji list. */
export interface DiscordGuildLike {
	readonly id: string;
	readonly emojis: { readonly cache: Iterable<DiscordGuildEmojiLike> };
}

/** A message we can put a reaction on; duck-typed from `channel.messages.fetch()`. */
export interface DiscordReactableMessageLike {
	react(emoji: string): Promise<unknown>;
}

/** The channel slice a reaction needs: the target message and the owning guild, if any. */
export interface DiscordReactionChannelLike {
	readonly guild?: DiscordGuildLike | null;
	readonly messages: { fetch(id: string): Promise<unknown> };
}

/**
 * Resolves an allowlist short name to a *custom guild* emoji identifier of the
 * form `name:id`, which is the spelling discord.js `message.react()` accepts.
 *
 * The `name:id` / `<:name:id>` / `:name:` spellings are ONLY ever handed to
 * `react()`. They must NEVER reach `channel.send()`: a channel whose guild does
 * not own the emoji renders the markup as literal text, so a fallback that
 * "degrades to a message" would post `<:lobster:123>` into the room. The plain
 * unicode from the allowlist is the only spelling safe to send as text.
 *
 * Resolution is cached per `guildId|name` INCLUDING misses. A guild that simply
 * has no such emoji is the common case, and re-scanning its whole emoji cache on
 * every single reaction is pure waste.
 */
export class GuildEmojiResolver {
	/** `guildId|name` -> `name:id`, or undefined for a cached miss. */
	readonly #resolved = new Map<string, string | undefined>();

	/**
	 * Returns the `name:id` identifier when this guild owns a custom emoji with
	 * that name, otherwise the allowlist unicode. A channel with no guild (a DM)
	 * can never have one, so it goes straight to unicode.
	 */
	resolve(guild: DiscordGuildLike | null | undefined, name: string, unicode: string): string {
		if (!guild?.id) return unicode;
		const key = `${guild.id}|${name}`;
		if (this.#resolved.has(key)) return this.#resolved.get(key) ?? unicode;
		const identifier = findGuildEmoji(guild, name);
		this.#resolved.set(key, identifier);
		return identifier ?? unicode;
	}
}

function findGuildEmoji(guild: DiscordGuildLike, name: string): string | undefined {
	const cache = guild.emojis?.cache;
	if (!cache) return undefined;
	for (const emoji of cache) {
		if (emoji?.name === name && emoji.id) return `${emoji.name}:${emoji.id}`;
	}
	return undefined;
}

/**
 * Serializes reaction requests per channel and honours Discord's 429 response.
 *
 * VERIFIED against https://docs.discord.com/developers/topics/rate-limits
 * (fetched 2026-08-27):
 *  - rate limits "should not be hard coded into your app"; an app is expected to
 *    parse the response headers instead;
 *  - the global limit is 50 requests/second per bot, independent of any route;
 *  - an exceeded limit returns HTTP 429 with a `retry_after` float field (in
 *    seconds) and a `Retry-After` header, and the app should retry from that
 *    value;
 *  - a 429 counts toward the invalid-request threshold of 10,000 per 10 minutes
 *    that earns a temporary Cloudflare ban, EXCEPT when the response carries
 *    `X-RateLimit-Scope: shared`.
 *
 * UNVERIFIED: Discord documents no fixed numeric per-route limit for the
 * reaction endpoint (the docs deliberately publish none, and warn that even the
 * quota headers can be inaccurate for emoji routes). The 250ms per-channel
 * spacing below is therefore OUR OWN conservative floor, not a documented
 * Discord number: it exists because a 429 we provoke is an invalid request that
 * counts against the ban threshold, and reactions are cosmetic enough that
 * spacing them out costs nothing.
 */
export class ReactionRateLimiter {
	/** channelId -> earliest timestamp at which the next request may go out. */
	readonly #nextAt = new Map<string, number>();

	constructor(
		readonly minSpacingMs = REACTION_MIN_SPACING_MS,
		readonly sleep: (ms: number) => Promise<void> = defaultSleep,
		readonly now: () => number = Date.now,
		readonly maxAttempts = REACTION_MAX_ATTEMPTS,
	) {}

	/**
	 * Runs one reaction request, spaced against the previous one on the same
	 * channel and retried on a 429 that tells us how long to wait. Anything else,
	 * and a 429 that outlives the attempt bound, is rethrown so the caller can
	 * report a failed delivery instead of retrying forever.
	 */
	async run<T>(channelId: string, task: () => Promise<T>): Promise<T> {
		let lastError: unknown;
		for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
			await this.#space(channelId);
			try {
				return await task();
			} catch (error) {
				lastError = error;
				const retryAfterMs = reactionRetryAfterMs(error);
				if (retryAfterMs === undefined || attempt === this.maxAttempts) throw error;
				await this.sleep(retryAfterMs);
			}
		}
		throw lastError;
	}

	async #space(channelId: string): Promise<void> {
		const now = this.now();
		// Drop channels whose spacing has already elapsed: without this the map keeps one
		// entry per channel that ever received a reaction, for the adapter's lifetime.
		for (const [channel, deadline] of this.#nextAt) if (deadline <= now) this.#nextAt.delete(channel);
		const earliest = this.#nextAt.get(channelId) ?? 0;
		const wait = earliest - now;
		// Reserve this channel's slot before awaiting, so concurrent reactions on the
		// same channel queue behind each other instead of all reading the same slot.
		this.#nextAt.set(channelId, Math.max(now, earliest) + this.minSpacingMs);
		if (wait > 0) await this.sleep(wait);
	}
}

function defaultSleep(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Extracts the wait a rejected request instructs us to take, in milliseconds.
 *
 * The UNITS DIFFER by source and getting them wrong is a 1000x error, so each
 * source is read explicitly rather than through one generic lookup:
 *  - `retry_after` in a 429 JSON body is SECONDS (float), per
 *    https://docs.discord.com/developers/topics/rate-limits (verified 2026-08-27);
 *    discord.js exposes the parsed body as `rawError`.
 *  - the HTTP `Retry-After` header is SECONDS (RFC 9110).
 *  - discord.js's own `RateLimitError.retryAfter` / `.timeToReset` are
 *    MILLISECONDS (verified in @discordjs/rest RateLimitData typings: "The time,
 *    in milliseconds, that will need to pass before this specific request can be
 *    retried"). Note that by default discord.js waits rate limits out internally
 *    and only throws this when configured to reject, so it is the rarer path.
 *
 * Returns undefined when the rejection carries no such instruction, which means
 * "do not retry".
 */
export function reactionRetryAfterMs(error: unknown): number | undefined {
	if (typeof error !== "object" || error === null) return undefined;
	const record = error as Record<string, unknown>;
	const raw = record.rawError as Record<string, unknown> | undefined;
	const headers = record.headers as Record<string, unknown> | undefined;
	const seconds = firstFiniteNumber([
		record.retry_after,
		raw?.retry_after,
		headers?.["retry-after"],
		headers?.["Retry-After"],
		readHeader(record.response, "retry-after"),
	]);
	if (seconds !== undefined) return seconds < 0 ? undefined : Math.ceil(seconds * 1000);
	const milliseconds = firstFiniteNumber([record.retryAfter, record.timeToReset]);
	if (milliseconds === undefined || milliseconds < 0) return undefined;
	return Math.ceil(milliseconds);
}

function readHeader(response: unknown, name: string): unknown {
	if (typeof response !== "object" || response === null) return undefined;
	const headers = (response as { headers?: unknown }).headers;
	if (typeof headers !== "object" || headers === null) return undefined;
	const get = (headers as { get?: unknown }).get;
	if (typeof get !== "function") return (headers as Record<string, unknown>)[name];
	return (get as (key: string) => unknown).call(headers, name);
}

function firstFiniteNumber(candidates: readonly unknown[]): number | undefined {
	for (const candidate of candidates) {
		const value = typeof candidate === "string" ? Number(candidate) : candidate;
		if (typeof value === "number" && Number.isFinite(value)) return value;
	}
	return undefined;
}

/**
 * Settles a reaction delivery: react to the target message and confirm, or fail
 * with a readable reason. Posts NO message — the emoji is the whole payload.
 *
 * Ambiguity is decided by the same `deliveryFailureIsAmbiguous` the text path
 * uses, and reaction-specific failures are marked with the same permanent codes:
 * a missing channel (10003) or a deleted target message (10008) is definitive,
 * so the gateway must not redeliver. Anything else is ambiguous and safe to
 * retry, because a reaction is a PUT on Discord and therefore idempotent —
 * reacting twice with the same emoji leaves exactly one reaction.
 */
export async function settleDiscordReaction(
	gateway: Pick<GatewayClientLike, "request">,
	discord: DiscordClientLike,
	message: ChatMessagePayload,
	resolver: GuildEmojiResolver,
	limiter: ReactionRateLimiter,
	log: Pick<Console, "error"> = console,
): Promise<void> {
	if (message.origin.platform !== "discord" || !message.deliveryId || !message.reaction) return;
	const deliveryId = message.deliveryId;
	const { targetMessageId, emoji, emojiName } = message.reaction;
	const conversationId = message.origin.conversationId;
	try {
		const channel = await discord.channels.fetch(conversationId);
		if (!isDiscordReactionChannel(channel)) {
			throw Object.assign(new Error(`Discord channel ${conversationId} cannot receive reactions`), { code: 10003 });
		}
		const target = await channel.messages.fetch(targetMessageId);
		if (!isReactableMessage(target)) {
			throw Object.assign(new Error(`Discord message ${targetMessageId} cannot be reacted to`), { code: 10008 });
		}
		const spelling = resolver.resolve(channel.guild, emojiName, emoji);
		await limiter.run(conversationId, () => target.react(spelling));
		await gateway.request("delivery.confirm", { deliveryId });
	} catch (error) {
		const reason = error instanceof Error ? error.message : String(error);
		// A silently dropped reaction is the failure mode to avoid: every path lands here.
		log.error(`Discord reaction ${emojiName} on ${targetMessageId} failed: ${reason}`);
		await gateway.request("delivery.fail", { deliveryId, reason, ambiguous: deliveryFailureIsAmbiguous(error) });
	}
}

/** The emoji slice of a discord.js reaction; `id` is set only for custom guild emoji. */
export interface DiscordReactionEmojiLike {
	readonly name?: string | null;
	readonly id?: string | null;
}

/** Duck-typed `MessageReaction` / `PartialMessageReaction`. */
export interface DiscordInboundReaction {
	readonly emoji: DiscordReactionEmojiLike;
	readonly message: {
		readonly id?: string | null;
		readonly channel?: DiscordMessageOriginShape["channel"] | null;
		readonly guild?: { readonly name?: string } | null;
	};
}

/** Duck-typed `User` / `PartialUser` of whoever reacted. */
export interface DiscordReactingUser {
	readonly id: string;
	readonly bot?: boolean;
	/** Nullable because an uncached partial user carries no resolved handle. */
	readonly username?: string | null;
	readonly globalName?: string | null;
	/** Server tag badge (`primary_guild`); absent on an uncached partial user. */
	readonly primaryGuild?: {
		readonly tag?: string | null;
		readonly identityEnabled?: boolean | null;
		readonly identityGuildId?: string | null;
	} | null;
}

/** What an inbound reaction contributes to conversation context. Never a turn. */
export interface InboundReactionDescription {
	readonly origin: OriginRef;
	readonly targetMessageId: string;
	readonly emoji: string;
	/** add, or remove when the reactor took the signal back. */
	readonly action: ReactionAction;
	readonly engagement: EngagementContext;
}

/**
 * Maps an inbound Discord reaction to `engagement.reaction` metadata, or
 * undefined when it must be ignored: our own bot's reaction (we would be
 * reporting our own outbound acknowledgements back as engagement), a reaction on
 * a message with no id, or one whose channel is unknown (an uncached partial we
 * cannot place in a conversation).
 *
 * `mentioned` is always false, because a reaction never addresses the persona.
 * A custom guild emoji is reported as `custom:<name>` rather than `:name:`: the
 * persona reads this text in its context block and could echo it into a channel,
 * where `:name:` renders as literal colons for anyone whose guild lacks it.
 */
export function describeInboundReaction(
	reaction: DiscordInboundReaction,
	user: DiscordReactingUser,
	botUser: unknown,
	action: ReactionAction,
): InboundReactionDescription | undefined {
	const botId = typeof botUser === "object" && botUser !== null && "id" in botUser ? String(botUser.id) : "";
	if (botId !== "" && user?.id === botId) return undefined;
	const targetMessageId = reaction.message?.id;
	const channel = reaction.message?.channel;
	if (!targetMessageId || !channel || !user?.id) return undefined;
	const emoji = describeReactionEmoji(reaction.emoji);
	if (!emoji) return undefined;
	const origin = discordMessageOrigin({ author: { id: user.id }, channel });
	const authorLike = {
		id: user.id,
		...(user.username ? { username: user.username } : {}),
		...(user.globalName ? { globalName: user.globalName } : {}),
		...(user.primaryGuild ? { primaryGuild: user.primaryGuild } : {}),
	};
	const displayName = resolveDisplayName(authorLike);
	const serverTag = resolveServerTag(authorLike);
	return {
		origin,
		action,
		targetMessageId,
		emoji,
		engagement: {
			mentioned: false,
			group: origin.kind !== "dm",
			authorId: user.id,
			...(displayName ? { authorName: displayName } : {}),
			...(user.username ? { authorHandle: user.username } : {}),
			...(serverTag ? { authorServerTag: serverTag } : {}),
			...(channel.name ? { channelLabel: `#${channel.name}` } : {}),
			...(reaction.message?.guild?.name ? { serverLabel: reaction.message.guild.name } : {}),
		},
	};
}

function describeReactionEmoji(emoji: DiscordReactionEmojiLike | undefined): string | undefined {
	const name = emoji?.name ?? undefined;
	if (!name) return undefined;
	return emoji?.id ? `custom:${name}` : name;
}

function isDiscordReactionChannel(value: unknown): value is DiscordReactionChannelLike {
	if (typeof value !== "object" || value === null || !("messages" in value)) return false;
	const messages = (value as { messages?: { fetch?: unknown } }).messages;
	return typeof messages === "object" && messages !== null && typeof messages.fetch === "function";
}

function isReactableMessage(value: unknown): value is DiscordReactableMessageLike {
	return typeof value === "object" && value !== null && typeof (value as { react?: unknown }).react === "function";
}
