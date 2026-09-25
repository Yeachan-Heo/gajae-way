import { join } from "node:path";
import { installStructuredLogging } from "@gajae-gateway/log";
import {
	type ChannelEngagementPolicy,
	type ChatMessagePayload,
	type ChatProgressPayload,
	type EngagementContext,
	type OriginRef,
	PRESENCE_ALL_MARKERS,
	type PresenceMarker,
	type PresenceState,
	presenceInitial,
	presenceMarkersFor,
	presenceTransition,
	type ReactionAction,
	type SessionModelChoicesResult,
} from "@gajae-gateway/protocol";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { AttachmentBuilder, Client, GatewayIntentBits, MessageFlags, Partials } from "discord.js";
import pkg from "../package.json";
import { type AttachmentCarrier, describeInboundBody, firstVoiceMessage } from "./attachments";
import { type AuthorLike, resolveDisplayName, resolveServerTag } from "./author";
import {
	adapterHome,
	type LoadedDiscordAdapterConfig,
	type LoadedDiscordVoiceConfig,
	loadDiscordAdapterConfig,
} from "./config";
import { AdapterAlreadyRunningError, AdapterLock } from "./lock";
import { type DiscordMessageOriginShape, discordMessageOrigin } from "./origin";
import {
	type DiscordInboundReaction,
	type DiscordReactingUser,
	describeInboundReaction,
	GuildEmojiResolver,
	ReactionRateLimiter,
	settleDiscordReaction,
} from "./reactions";
import {
	classifyRecoveryFailure,
	clearAttempt,
	recoveryCursorPath as defaultRecoveryCursorPath,
	loadRecoveryCursors,
	pruneKnownDms,
	RECOVERY_ATTEMPT_BACKOFF_MS,
	RECOVERY_MAX_ATTEMPTS,
	RECOVERY_MAX_PAGES,
	RECOVERY_RETRY_BASE_MS,
	RECOVERY_RETRY_MAX_MS,
	type RecoverableChannel,
	type RecoveryCursorState,
	type RecoveryDeadLetter,
	type RecoveryDeadLetterDigest,
	type RecoveryFailureClass,
	RecoveryGate,
	recordAttempt,
	recordDeadLetter,
	recoverConversation,
	rememberKnownDm,
	retainRecoveryCursors,
	saveRecoveryCursors,
	snowflakeIsAfter,
	summarizeRecoveryFailure,
} from "./recovery";
import { type ReplyMessageLike, resolveReplyContext } from "./reply";
import { type SpeechConfig, type SpeechPorts, synthesizeVoice } from "./speech";
import {
	type TranscriptionPorts,
	type TranscriptResult,
	transcribeVoiceMessage,
	type VoiceTranscriptionConfig,
	withTranscript,
} from "./voice";

/**
 * Reaction state that must outlive a single delivery: the guild custom-emoji
 * lookup cache and the per-channel request self-throttle. One pair per delivery
 * subscription, so a long-running adapter shares both across every reaction
 * while a direct caller stays independent.
 */
export interface DiscordReactionPorts {
	readonly resolver: GuildEmojiResolver;
	readonly limiter: ReactionRateLimiter;
}

export function createReactionPorts(): DiscordReactionPorts {
	return { resolver: new GuildEmojiResolver(), limiter: new ReactionRateLimiter() };
}

/** Gateway liveness probe cadence; three consecutive failures reconnect. */
const MONITOR_INTERVAL_MS = 30_000;
const MONITOR_RETRY_MS = 5_000;
const MONITOR_STRIKES = 3;
/** Working-status without a progress tick for this long is stale (gateway ticks every 15s). */
const WORKING_STATUS_STALE_MS = 90_000;
/** Consecutive unreadable fetches before a target stops poisoning global recovery completion. */
const RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS = 3;
/** Active plus recent archived threads recovered per configured parent in one pass. */
const RECOVERY_THREAD_TARGET_CAP = 200;
/** Archived threads older than the DM retention window are outside restart recovery scope. */
const RECOVERY_THREAD_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;

/**
 * A single slow/failed status probe (gateway busy under load) must not tear
 * down a healthy link and its delivery subscription: only a sustained failure
 * (MONITOR_STRIKES consecutive) reconnects. A closed socket is still detected
 * immediately through the client's own close path.
 */
export function monitorFailureDecision(
	strikes: number,
): { action: "retry"; strikes: number } | { action: "reconnect" } {
	return strikes + 1 >= MONITOR_STRIKES ? { action: "reconnect" } : { action: "retry", strikes: strikes + 1 };
}
const DISCORD_MESSAGE_LIMIT = 2_000;
// Discord clears the typing hint after ~10s, so refresh inside that window while a turn is running.
const TYPING_REFRESH_MS = 7_000;
// Hard ceiling above the gateway's 300s gjc turn timeout: a lost turn must not type forever.
const TYPING_MAX_MS = 330_000;
const REQUIRED_INTENTS = [
	GatewayIntentBits.Guilds,
	GatewayIntentBits.GuildMessages,
	GatewayIntentBits.MessageContent,
	GatewayIntentBits.DirectMessages,
	// Without the reaction intents Discord never dispatches messageReactionAdd /
	// messageReactionRemove at all, so inbound reactions would silently not exist.
	// GUILD_MESSAGE_REACTIONS (1 << 10) carries MESSAGE_REACTION_ADD/REMOVE and
	// DIRECT_MESSAGE_REACTIONS (1 << 13) does the same for DMs; neither is a
	// privileged intent, so no portal approval is needed
	// (https://docs.discord.com/developers/events/gateway, verified 2026-08-27).
	GatewayIntentBits.GuildMessageReactions,
	GatewayIntentBits.DirectMessageReactions,
];
/**
 * Reactions on messages this process never cached (anything from before the last
 * restart) arrive as PARTIAL structures, and discord.js drops those events
 * entirely unless the partials are enabled. Our own outbound messages are exactly
 * the ones people react to, and they are the first thing to fall out of cache.
 */
const REQUIRED_PARTIALS = [Partials.Message, Partials.Channel, Partials.Reaction, Partials.User];

export interface GatewayClientLike {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void;
	onChatProgress?(handler: (progress: ChatProgressPayload) => void): () => void;
	close?(): Promise<void>;
}

export interface DiscordTextChannelLike {
	send(
		payload:
			| string
			| { content: string; reply?: { messageReference: string; failIfNotExists?: boolean } }
			// A voice message carries no content: only the attachment and the flag.
			| { files: readonly unknown[]; flags: number },
	): Promise<unknown>;
}

/**
 * Everything the delivery path needs to speak a reply.
 *
 * Absent when no voice key is configured, which is what makes the whole feature
 * opt-in: with no `speech` the adapter behaves exactly as it did before, and a
 * `voiceText` on a delivery is simply ignored.
 */
export interface DiscordSpeechPorts {
	readonly config: SpeechConfig;
	readonly ports: SpeechPorts;
}

export interface DiscordTypingChannelLike {
	sendTyping(): Promise<unknown>;
}

export interface TypingPort {
	begin(conversationId: string): void;
	end(conversationId: string): void;
}

export interface DiscordClientLike {
	channels: { fetch(id: string): Promise<unknown> };
}

export interface DiscordInboundMessage extends DiscordMessageOriginShape, ReplyMessageLike, AttachmentCarrier {
	readonly id: string;
	readonly content: string;
	readonly createdTimestamp?: number;
	readonly author: {
		readonly id: string;
		readonly bot?: boolean;
		readonly username?: string;
		/** Account-wide display name, shown when a guild has no nickname. */
		readonly globalName?: string | null;
		/** Server tag badge (`primary_guild`), rendered next to the name. */
		readonly primaryGuild?: {
			readonly tag?: string | null;
			readonly identityEnabled?: boolean | null;
			readonly identityGuildId?: string | null;
		} | null;
	};
	readonly mentions?: { has(user: unknown): boolean; readonly repliedUser?: AuthorLike | null };
	readonly guild?: { readonly name?: string } | null;
	/**
	 * Guild membership for this message, present only for guild messages.
	 * `displayName` is what the server actually shows in the member list.
	 */
	readonly member?: {
		readonly nick?: string | null;
		readonly displayName?: string | null;
		readonly nickname?: string | null;
	} | null;
}

/**
 * Resolves the name a reader would see next to the message in this server.
 *
 * Discord shows a per-guild nickname when one is set, then the account's global
 * display name, and only falls back to the raw handle when neither exists.
 * Reporting the handle instead makes the persona address people by a string
 * nobody in the room sees, and it differs per server for the same account.
 */
export function resolveAuthorDisplayName(message: DiscordInboundMessage): string | undefined {
	return resolveDisplayName(message.author, message.member);
}

/** Bounded inbound message-id memory prevents gateway replay/reconnect duplicate turns. */
export class LruSet {
	readonly #values = new Map<string, undefined>();
	constructor(readonly limit = 10_000) {
		if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("LRU limit must be a positive integer");
	}

	addIfAbsent(value: string): boolean {
		if (this.#values.has(value)) {
			this.#values.delete(value);
			this.#values.set(value, undefined);
			return false;
		}
		this.#values.set(value, undefined);
		if (this.#values.size > this.limit) this.#values.delete(this.#values.keys().next().value as string);
		return true;
	}
}

export function engagementForMessage(message: DiscordInboundMessage, botUser: unknown): EngagementContext {
	const origin = discordMessageOrigin(message);
	const botId = typeof botUser === "object" && botUser !== null && "id" in botUser ? String(botUser.id) : "";
	const contentMention = botId !== "" && new RegExp(`<@!?${escapeRegExp(botId)}>`).test(message.content);
	const displayName = resolveAuthorDisplayName(message);
	const serverTag = resolveServerTag(message.author);
	const replyTo = resolveReplyContext(message, botId);
	return {
		mentioned: Boolean(message.mentions?.has(botUser) || contentMention || replyTo?.fromSelf),
		group: origin.kind !== "dm",
		authorId: message.author.id,
		...(message.author.bot ? { authorIsBot: true } : {}),
		...(displayName ? { authorName: displayName } : {}),
		...(message.author.username ? { authorHandle: message.author.username } : {}),
		...(serverTag ? { authorServerTag: serverTag } : {}),
		...(message.channel.name ? { channelLabel: `#${message.channel.name}` } : {}),
		...(message.guild?.name ? { serverLabel: message.guild.name } : {}),
		...(replyTo ? { replyTo } : {}),
	};
}

/**
 * Decides whether an incoming Discord message becomes a turn, and with what engagement.
 * Returns undefined when the message must be ignored outright.
 *
 * Only transport-level rejection happens here: malformed events and our own
 * messages are dropped. The gateway is the single authority for channel mode,
 * audience, allowlist, and bounded bot-collaboration decisions.
 */
export function decideInbound(
	message: DiscordInboundMessage,
	botUser: unknown,
	_channels: Readonly<Record<string, ChannelEngagementPolicy>> | undefined,
): EngagementContext | undefined {
	if (message.id === undefined) return undefined;
	const botId = typeof botUser === "object" && botUser !== null && "id" in botUser ? String(botUser.id) : "";
	if (botId !== "" && message.author.id === botId) return undefined;
	return engagementForMessage(message, botUser);
}

/** Queued edits survive a gateway-link outage up to this many messages (oldest dropped with a log line). */
const EDIT_OUTBOX_LIMIT = 256;

export interface PendingEdit {
	readonly messageId: string;
	readonly origin: OriginRef;
	readonly text: string;
	readonly engagement: EngagementContext;
	readonly receivedAt?: string;
}

/** The chat.edit request an edited Discord message becomes, or undefined when it is not ours to forward. */
export interface DescribedMessageEdit {
	readonly messageId: string;
	readonly origin: OriginRef;
	readonly text: string;
	readonly engagement: EngagementContext;
	readonly receivedAt: string;
}

/**
 * Same admission as a new message (our own messages and empty bodies are
 * dropped; open channels promote humans to a mention), on the message's NEW
 * content. Whether the original was ever ingested is the gateway's call: it
 * ignores edits of messages it never saw.
 *
 * discord.js fires `messageUpdate` for far more than user edits: a link
 * preview resolving, an embed being attached, a pin, a flag change. None of
 * those changed what the user said. Only a change in the RENDERED body is an
 * edit; when the previous body is known (cached `before`) and equal, or the
 * message carries no `editedTimestamp` at all, nothing is forwarded.
 */
export function describeMessageEdit(
	message: DiscordInboundMessage & AttachmentCarrier & { readonly editedTimestamp?: number | null },
	botUser: unknown,
	channels: Readonly<Record<string, ChannelEngagementPolicy>> | undefined,
	before?: (AttachmentCarrier & { readonly content?: string | null; readonly partial?: boolean }) | null,
): DescribedMessageEdit | undefined {
	const editedAt = message.editedTimestamp;
	if (typeof editedAt !== "number") return undefined;
	const engagement = decideInbound(message, botUser, channels);
	if (!engagement) return undefined;
	const text = describeInboundBody(message);
	if (text === "") return undefined;
	if (before && !before.partial && describeInboundBody({ ...before, content: before.content ?? "" }) === text)
		return undefined;
	return {
		messageId: message.id as string,
		origin: discordMessageOrigin(message),
		text,
		engagement,
		receivedAt: new Date(editedAt).toISOString(),
	};
}

/**
 * True when the persona was addressed: a DM, or a group message that mentions
 * it (an `open` channel promotes every human message to a mention, so it is
 * covered here too). Only addressed turns show presence - typing, the
 * "working…" post - before the reply lands; an overheard public-channel turn
 * stays invisible until it actually says something.
 */
export function addressedTurn(engagement: Pick<EngagementContext, "group" | "mentioned">): boolean {
	return !engagement.group || engagement.mentioned;
}

/**
 * Preserves arrival order per conversation across asynchronous ingress work.
 *
 * Transcribing a voice message takes a network round-trip, so a short text
 * message arriving right after a long voice message would otherwise reach the
 * gateway first and the persona would read the conversation backwards. Each
 * conversation gets its own chain; separate conversations stay parallel, because
 * one slow transcription must not stall an unrelated room.
 *
 * The chain is dropped as soon as it drains so a long-lived adapter does not
 * accumulate an entry per conversation it has ever seen.
 */
export class OrderedIngress {
	private readonly chains = new Map<string, Promise<void>>();

	run(key: string, task: () => Promise<void>): void {
		const previous = this.chains.get(key) ?? Promise.resolve();
		// A rejected task must not poison the chain for later messages.
		const next = previous
			.then(task)
			.catch((error: unknown) =>
				console.error(`Discord ingress failed: ${error instanceof Error ? error.message : String(error)}`),
			);
		this.chains.set(key, next);
		void next.then(() => {
			if (this.chains.get(key) === next) this.chains.delete(key);
		});
	}

	/** Test seam: settles once nothing is in flight. */
	async drain(): Promise<void> {
		while (this.chains.size > 0) await Promise.all([...this.chains.values()]);
	}
}

/**
 * Transcribes an inbound voice message, or resolves undefined when there is
 * nothing to transcribe or transcription is not configured.
 *
 * Every failure resolves undefined rather than throwing: the message must still
 * be delivered with its url when speech-to-text is unavailable.
 */
export async function transcribeIfVoice(
	message: AttachmentCarrier,
	voice: VoiceTranscriptionConfig | undefined,
	ports: TranscriptionPorts = { fetch, log: (line) => console.error(`Discord ${line}`) },
): Promise<TranscriptResult | undefined> {
	if (!voice) return undefined;
	const url = firstVoiceMessage(message)?.url;
	if (typeof url !== "string" || url === "") return undefined;
	return await transcribeVoiceMessage(url, voice, ports);
}

export function chunkDiscordMessage(text: string): string[] {
	if (text.length === 0) return [""];
	const chunks: string[] = [];
	for (let offset = 0; offset < text.length; offset += DISCORD_MESSAGE_LIMIT) {
		chunks.push(text.slice(offset, offset + DISCORD_MESSAGE_LIMIT));
	}
	return chunks;
}

export function deliveryFailureIsAmbiguous(error: unknown): boolean {
	const code = typeof error === "object" && error !== null && "code" in error ? String(error.code) : "";
	// Discord's known permanent errors have not dispatched anything: unknown channel
	// (10003), unknown message (10008), missing access/permissions (50001, 50013),
	// and a request Discord will reject identically forever (50035 Invalid Form Body).
	return !new Set(["10003", "10008", "50001", "50013", "50035"]).has(code);
}

/**
 * Keeps Discord's "is typing…" hint alive from the moment an engaged turn is accepted until its
 * reply is delivered. One run per conversation; a failed pulse stops that run rather than retrying,
 * because the typing hint is cosmetic and must never compete with delivery.
 */
export class TypingIndicator implements TypingPort {
	readonly #runs = new Map<string, { deadline: number; timer: ReturnType<typeof setTimeout> | undefined }>();

	constructor(
		readonly discord: DiscordClientLike,
		readonly refreshMs = TYPING_REFRESH_MS,
		readonly maxMs = TYPING_MAX_MS,
		readonly log: Pick<Console, "error"> = console,
	) {}

	begin(conversationId: string): void {
		const existing = this.#runs.get(conversationId);
		if (existing) {
			existing.deadline = Date.now() + this.maxMs;
			return;
		}
		const run = { deadline: Date.now() + this.maxMs, timer: undefined };
		this.#runs.set(conversationId, run);
		void this.#pulse(conversationId, run);
	}

	end(conversationId: string): void {
		const run = this.#runs.get(conversationId);
		if (!run) return;
		if (run.timer) clearTimeout(run.timer);
		this.#runs.delete(conversationId);
	}

	async #pulse(
		conversationId: string,
		run: { deadline: number; timer: ReturnType<typeof setTimeout> | undefined },
	): Promise<void> {
		if (this.#runs.get(conversationId) !== run) return;
		try {
			const channel = await this.discord.channels.fetch(conversationId);
			if (!isDiscordTypingChannel(channel)) {
				this.#runs.delete(conversationId);
				return;
			}
			await channel.sendTyping();
		} catch (error) {
			this.log.error(
				`Discord typing indicator stopped for ${conversationId}: ${error instanceof Error ? error.message : String(error)}`,
			);
			this.#runs.delete(conversationId);
			return;
		}
		if (this.#runs.get(conversationId) !== run) return;
		if (Date.now() >= run.deadline) {
			this.#runs.delete(conversationId);
			return;
		}
		run.timer = setTimeout(() => void this.#pulse(conversationId, run), this.refreshMs);
	}
}

/** The slice of a fetched Discord message presence needs: react, and remove our own reaction. */
export interface PresenceMessageLike {
	react(emoji: string): Promise<unknown>;
	readonly reactions?: {
		resolve(emoji: string): { users: { remove(userId: string): Promise<unknown> } } | null | undefined;
	};
}

function isPresenceMessage(value: unknown): value is PresenceMessageLike {
	return typeof value === "object" && value !== null && "react" in value && typeof value.react === "function";
}

type PresenceEntry = {
	readonly conversationId: string;
	readonly messageId: string;
	message?: PresenceMessageLike;
	/** Coalescing state: what the gradient should show. */
	state: PresenceState;
	/** Unicode markers the API confirmed are on the message. */
	readonly shown: Set<string>;
	wanted: boolean;
	reconciling: boolean;
	/** A change arrived while a pass was running; the loop re-diffs before it exits. */
	pending: boolean;
};

/** Bound on re-diff passes in one reconcile run; retirement cleanup runs regardless. */
const RECONCILE_MAX_PASSES = 8;

/**
 * Presence as a reaction gradient on the triggering message.
 *
 * Instead of posting and editing a "working…" message, the adapter reacts to
 * the message it is answering: a phase marker (⏳ queued, 🔧 tool, 💭 thinking,
 * ✍️ writing), a clock face that advances every minute, and an effort digit
 * for tool calls / tokens. Markers are swapped only when their bucket changes
 * and at most once per coalescing window, and every marker is removed when
 * the reply lands (or the turn goes stale). Nothing is posted or edited. The
 * typing indicator still runs alongside: it is free and Discord-native.
 *
 * Desired state (`state`/`wanted`) and applied state (`shown`) are kept apart
 * and reconciled by one loop per message, so a slow Discord call can delay a
 * swap but never lose it, and a failed call leaves the marker un-shown to be
 * retried rather than believed.
 */
export class WorkingStatus {
	readonly #discord: DiscordClientLike;
	readonly #log: Pick<Console, "error">;
	readonly #getBotUser: () => unknown;
	readonly #now: () => number;
	readonly #entries = new Map<string, PresenceEntry>();
	readonly #staleTimers = new Map<string, ReturnType<typeof setTimeout>>();

	constructor(
		discord: DiscordClientLike,
		log: Pick<Console, "error"> = console,
		getBotUser: () => unknown = () => undefined,
		now: () => number = Date.now,
	) {
		this.#discord = discord;
		this.#log = log;
		this.#getBotUser = getBotUser;
		this.#now = now;
	}

	/**
	 * An addressed turn was accepted for `messageId` in `conversationId`. The
	 * queued marker goes on immediately; it is the room's only sign the message
	 * was seen until the first progress tick. Best-effort, never awaited.
	 */
	arm(conversationId: string, messageId: string): void {
		const prior = this.#entries.get(conversationId);
		if (prior && prior.messageId === messageId) {
			// Same message re-armed (an accepted edit): the markers on it are still
			// ours; restart the gradient from queued without losing ownership.
			prior.wanted = true;
			prior.state = presenceInitial(this.#now());
			this.#armStale(conversationId);
			void this.#reconcile(prior);
			return;
		}
		if (prior) void this.#retire(prior);
		const entry: PresenceEntry = {
			conversationId,
			messageId,
			state: presenceInitial(this.#now()),
			shown: new Set(),
			wanted: true,
			reconciling: false,
			pending: false,
		};
		this.#entries.set(conversationId, entry);
		this.#armStale(conversationId);
		void this.#reconcile(entry);
	}

	#armStale(conversationId: string): void {
		const prior = this.#staleTimers.get(conversationId);
		if (prior) clearTimeout(prior);
		const timer = setTimeout(() => {
			this.#staleTimers.delete(conversationId);
			void this.clear(conversationId);
		}, WORKING_STATUS_STALE_MS);
		timer.unref?.();
		this.#staleTimers.set(conversationId, timer);
	}

	async update(progress: ChatProgressPayload): Promise<void> {
		if (progress.origin.platform !== "discord") return;
		const entry = this.#entries.get(progress.origin.conversationId);
		if (!entry || !entry.wanted) return;
		this.#armStale(progress.origin.conversationId);
		const swap = presenceTransition(entry.state, progress, this.#now());
		if (!swap) return;
		entry.state = swap.state;
		await this.#reconcile(entry);
	}

	async clear(conversationId: string): Promise<void> {
		const timer = this.#staleTimers.get(conversationId);
		if (timer) clearTimeout(timer);
		this.#staleTimers.delete(conversationId);
		const entry = this.#entries.get(conversationId);
		if (!entry) return;
		this.#entries.delete(conversationId);
		await this.#retire(entry);
	}

	async #retire(entry: PresenceEntry): Promise<void> {
		entry.wanted = false;
		await this.#reconcile(entry);
	}

	async #resolve(entry: PresenceEntry): Promise<PresenceMessageLike | undefined> {
		if (entry.message) return entry.message;
		const channel = await this.#discord.channels.fetch(entry.conversationId);
		const fetched = await (channel as { messages?: { fetch(id: string): Promise<unknown> } }).messages?.fetch(
			entry.messageId,
		);
		if (!isPresenceMessage(fetched)) return undefined;
		entry.message = fetched;
		return fetched;
	}

	/**
	 * Drives `shown` towards the desired set; one loop per entry, re-diffing
	 * after every pass and once more for any change that arrived mid-pass.
	 * Retirement is always driven to completion, even after a failed add.
	 */
	async #reconcile(entry: PresenceEntry): Promise<void> {
		if (entry.reconciling) {
			entry.pending = true;
			return;
		}
		entry.reconciling = true;
		try {
			const botUser = this.#getBotUser() as { id?: unknown } | undefined;
			const botId = typeof botUser?.id === "string" ? botUser.id : undefined;
			for (let pass = 0; pass < RECONCILE_MAX_PASSES; pass++) {
				entry.pending = false;
				const desired = new Set(entry.wanted ? presenceMarkersFor(entry.state.snapshot).map((m) => m.unicode) : []);
				const remove = [...entry.shown].filter((unicode) => !desired.has(unicode));
				const add = [...desired].filter((unicode) => !entry.shown.has(unicode));
				if (remove.length === 0 && add.length === 0) {
					if (!entry.pending) return;
					continue;
				}
				let message: PresenceMessageLike | undefined;
				try {
					message = await this.#resolve(entry);
				} catch (error) {
					this.#log.error(`Discord presence could not resolve ${entry.conversationId}: ${errorMessage(error)}`);
					return;
				}
				if (!message) return;
				for (const unicode of remove) {
					try {
						if (message.reactions && botId) await message.reactions.resolve(unicode)?.users.remove(botId);
					} catch (error) {
						this.#log.error(
							`Discord presence could not remove ${unicode} on ${entry.conversationId}: ${errorMessage(error)}`,
						);
					}
					entry.shown.delete(unicode);
				}
				let addFailed = false;
				for (const unicode of add) {
					if (!entry.wanted) break;
					try {
						await message.react(unicode);
						entry.shown.add(unicode);
					} catch (error) {
						this.#log.error(
							`Discord presence could not add ${unicode} on ${entry.conversationId}: ${errorMessage(error)}`,
						);
						addFailed = true;
						break;
					}
				}
				if (addFailed && entry.wanted && !entry.pending) return;
			}
			if (!entry.wanted && entry.shown.size > 0 && entry.message?.reactions && botId) {
				const reactions = entry.message.reactions;
				for (const unicode of [...entry.shown]) {
					await reactions
						.resolve(unicode)
						?.users.remove(botId)
						.catch((error: unknown) =>
							this.#log.error(
								`Discord presence could not remove ${unicode} on ${entry.conversationId}: ${errorMessage(error)}`,
							),
						);
					entry.shown.delete(unicode);
				}
			}
		} finally {
			entry.reconciling = false;
		}
	}
}

function errorMessage(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

/** True for a unicode reaction the adapter itself puts on messages as presence. */
export function isPresenceReaction(unicode: string): boolean {
	const bare = unicode.replace(/\uFE0F/g, "");
	return PRESENCE_ALL_MARKERS.some((marker) => marker.unicode.replace(/\uFE0F/g, "") === bare);
}

export async function settleDiscordDelivery(
	gateway: Pick<GatewayClientLike, "request">,
	discord: DiscordClientLike,
	message: ChatMessagePayload,
	typing?: TypingPort,
	status?: WorkingStatus,
	reactions: DiscordReactionPorts = createReactionPorts(),
	speech?: DiscordSpeechPorts,
): Promise<void> {
	if (message.origin.platform !== "discord" || !message.deliveryId) return;
	const deliveryId = message.deliveryId;
	// A reaction delivery reacts and posts nothing; it settles on the same ledger.
	if (message.reaction) {
		try {
			await settleDiscordReaction(gateway, discord, message, reactions.resolver, reactions.limiter);
		} finally {
			await status?.clear(message.origin.conversationId);
			typing?.end(message.origin.conversationId);
		}
		return;
	}
	try {
		const { channel, notice } = await resolveDeliveryChannel(discord, message.origin, deliveryId);
		const body = message.duplicateWarning ? `[recovered - may be a duplicate] ${message.text}` : message.text;
		const text = notice ? `${notice}\n${body}` : body;
		const chunks = chunkDiscordMessage(text);
		for (let index = 0; index < chunks.length; index++) {
			// Reply-threading applies to the first chunk only; failIfNotExists keeps a
			// deleted target from failing the whole delivery.
			const chunk = chunks[index] as string;
			// A reply reference points into the thread, so it is dropped on parent fallback.
			if (index === 0 && message.replyToMessageId && !notice)
				await channel.send({
					content: chunk,
					reply: { messageReference: message.replyToMessageId, failIfNotExists: false },
				});
			else await channel.send(chunk);
		}
		// Voice rides AFTER the text, and only after the text actually landed:
		// pairing them is for a readable history, so the readable half must be
		// the one that is guaranteed. A synthesis failure is logged and the
		// delivery still confirms — the words arrived, which is the deliverable.
		if (message.voiceText && speech) await sendVoiceMessage(channel, message.voiceText, speech);
		await gateway.request("delivery.confirm", { deliveryId });
	} catch (error) {
		await gateway.request("delivery.fail", {
			deliveryId,
			reason: error instanceof Error ? error.message : String(error),
			ambiguous: deliveryFailureIsAmbiguous(error),
		});
	} finally {
		await status?.clear(message.origin.conversationId);
		typing?.end(message.origin.conversationId);
	}
}

interface ArchivableThreadLike {
	readonly archived: boolean | null;
	setArchived(archived: boolean): Promise<unknown>;
}

function isArchivedThread(value: unknown): value is ArchivableThreadLike {
	return (
		typeof value === "object" &&
		value !== null &&
		"archived" in value &&
		value.archived === true &&
		"setArchived" in value &&
		typeof value.setArchived === "function"
	);
}

async function fetchTextChannel(discord: DiscordClientLike, id: string): Promise<DiscordTextChannelLike> {
	const channel = await discord.channels.fetch(id);
	if (!isDiscordTextChannel(channel))
		throw Object.assign(new Error(`Discord channel ${id} cannot receive messages`), { code: 10003 });
	return channel;
}

/**
 * Resolves where a delivery is posted. A thread archives after inactivity, and
 * posting into it only auto-unarchives when the bot holds the right permission,
 * so an archived thread is explicitly unarchived first. When that fails the
 * reply goes to the parent channel with a visible notice and a log line: a
 * monitor result that silently vanishes behind an archived thread is the worst
 * outcome, because its schedule still looks healthy.
 */
async function resolveDeliveryChannel(
	discord: DiscordClientLike,
	origin: ChatMessagePayload["origin"],
	deliveryId: string,
): Promise<{ readonly channel: DiscordTextChannelLike; readonly notice?: string }> {
	const channel = await fetchTextChannel(discord, origin.conversationId);
	if (origin.kind !== "thread" || !origin.parentId || !isArchivedThread(channel)) return { channel };
	try {
		await channel.setArchived(false);
		return { channel };
	} catch (error) {
		const reason = `thread ${origin.conversationId} is archived and could not be unarchived (${errorMessage(error)})`;
		const parent = await fetchTextChannel(discord, origin.parentId);
		console.error(`Discord delivery ${deliveryId}: ${reason}; delivered to parent channel ${origin.parentId} instead.`);
		return {
			channel: parent,
			notice: `[thread <#${origin.conversationId}> is archived and could not be unarchived (${errorMessage(error)}); posting here instead]`,
		};
	}
}

/**
 * Posts a spoken copy of a reply as a real Discord voice message.
 *
 * Never throws: the text half of this delivery has already been sent and
 * confirmed-in-progress, so failing here would turn a missing courtesy into a
 * failed delivery and a duplicate on retry.
 */
async function sendVoiceMessage(
	channel: DiscordTextChannelLike,
	text: string,
	speech: DiscordSpeechPorts,
): Promise<void> {
	try {
		const voice = await synthesizeVoice(text, speech.config, speech.ports);
		if (!voice) return;
		const attachment = new AttachmentBuilder(Buffer.from(voice.ogg), { name: "voice-message.ogg" })
			.setDuration(voice.seconds)
			.setWaveform(voice.waveform);
		// IsVoiceMessage is what makes Discord render a waveform and a play button
		// instead of a file card, and it requires empty content.
		await channel.send({ files: [attachment], flags: MessageFlags.IsVoiceMessage });
	} catch (error) {
		console.error(`Discord voice reply failed: ${error instanceof Error ? error.message : String(error)}`);
	}
}

export function subscribeDiscordDeliveries(
	gateway: GatewayClientLike,
	discord: DiscordClientLike,
	typing?: TypingPort,
	status?: WorkingStatus,
	log: Pick<Console, "error"> = console,
	speech?: DiscordSpeechPorts,
): () => void {
	// One reaction port pair per subscription: the emoji cache and the throttle are
	// only useful across deliveries, and a live adapter has exactly one subscription.
	const reactions = createReactionPorts();
	return gateway.onChatMessage((message) => {
		void settleDiscordDelivery(gateway, discord, message, typing, status, reactions, speech).catch((error) =>
			log.error(
				`Discord delivery settlement request failed: ${error instanceof Error ? error.message : String(error)}`,
			),
		);
	});
}

export function subscribeDiscordProgress(
	gateway: GatewayClientLike,
	// Structural, not the concrete class: the progress path is the one piece worth
	// testing without a Discord client, and the private message cache is irrelevant here.
	status: Pick<WorkingStatus, "update" | "clear">,
	log: Pick<Console, "error"> = console,
	typing?: TypingPort,
): () => void {
	if (!gateway.onChatProgress) return () => {};
	return gateway.onChatProgress((progress) => {
		// `final` means the turn stopped working. It arrives even when the turn
		// delivered nothing - a silence token in an open channel - which is the only
		// signal that the temporary status must go. Clearing on delivery alone left
		// one orphaned "working" message per suppressed turn, and the typing hint
		// (begun on engagement, ended only by a delivery) "typing…" for its full
		// 330s cap after every silent turn (집가재, 2026-09-02).
		if (progress.final) typing?.end(progress.origin.conversationId);
		const action = progress.final ? status.clear(progress.origin.conversationId) : status.update(progress);
		void action.catch((error) =>
			log.error(
				`Discord working status ${progress.final ? "clear" : "update"} failed: ${error instanceof Error ? error.message : String(error)}`,
			),
		);
	});
}

/**
 * Builds the speech ports from config, or undefined when voice is not set up.
 *
 * The same `voice` section powers inbound transcription and outbound speech: one
 * provider, one key, one place to turn it off.
 *
 * The waveform decoder shells out to ffmpeg and is wired only as an optional
 * port — Discord needs `duration_secs`, which is read from the Ogg stream
 * itself, so a host without ffmpeg still sends a real voice message and only
 * loses the shape of the bar.
 */
export function discordSpeechPorts(voice: LoadedDiscordVoiceConfig | undefined): DiscordSpeechPorts | undefined {
	if (!voice) return undefined;
	// Mapped field by field on purpose. `LoadedDiscordVoiceConfig` structurally
	// satisfies `SpeechConfig`, so passing it straight through type-checks while
	// silently feeding the speech-to-text `endpoint`/`model`/`timeoutMs` into the
	// text-to-speech call. The two services share a key, not a URL.
	return {
		config: {
			apiKey: voice.apiKey,
			...(voice.voiceId ? { voiceId: voice.voiceId } : {}),
			...(voice.speechModel ? { model: voice.speechModel } : {}),
			...(voice.speechEndpoint ? { endpoint: voice.speechEndpoint } : {}),
			...(voice.outputFormat ? { outputFormat: voice.outputFormat } : {}),
			...(voice.maxSpokenChars ? { maxSpokenChars: voice.maxSpokenChars } : {}),
			...(voice.speechTimeoutMs ? { timeoutMs: voice.speechTimeoutMs } : {}),
			...(voice.speechSpeed !== undefined ? { speed: voice.speechSpeed } : {}),
		},
		ports: {
			fetch,
			decodePcm: decodePcmWithFfmpeg,
			log: (line) => console.error(`Discord ${line}`),
		},
	};
}

/** Decodes to mono 8 kHz PCM for waveform peaks only; resolves undefined if ffmpeg is absent. */
async function decodePcmWithFfmpeg(ogg: Uint8Array): Promise<Int16Array | undefined> {
	const child = Bun.spawn(["ffmpeg", "-v", "error", "-i", "pipe:0", "-ac", "1", "-ar", "8000", "-f", "s16le", "-"], {
		stdin: "pipe",
		stdout: "pipe",
		stderr: "pipe",
	});
	child.stdin.write(ogg);
	await child.stdin.end();
	const raw = new Uint8Array(await new Response(child.stdout).arrayBuffer());
	if ((await child.exited) !== 0 || raw.byteLength < 2) return undefined;
	// The byte length can be odd if ffmpeg was cut off; drop the trailing half sample.
	const usable = raw.byteLength - (raw.byteLength % 2);
	return new Int16Array(raw.buffer.slice(raw.byteOffset, raw.byteOffset + usable));
}

export async function startDiscordAdapter(config: LoadedDiscordAdapterConfig): Promise<void> {
	const discord = new Client({
		intents: [...new Set([...REQUIRED_INTENTS, ...(config.intents ?? [])])],
		// DM channels are not cached on a cold start; without the Channel partial
		// discord.js drops messageCreate for uncached DMs, silently losing owner DMs.
		// REQUIRED_PARTIALS carries that Channel partial plus the reaction partials,
		// which uncached reaction events need for the same reason.
		partials: REQUIRED_PARTIALS,
	});
	const typing = new TypingIndicator(discord);
	const status = new WorkingStatus(discord, console, () => discord.user);
	const gateway = new ReconnectingGateway(
		config.gatewaySocket ?? defaultGatewaySocket(),
		discord,
		config,
		typing,
		status,
		join(adapterHome(), "adapters", "discord", "recovery-cursor.json"),
		() => discord.user,
		undefined,
		undefined,
		discordSpeechPorts(config.voice),
	);
	// Transcription makes ingress asynchronous, and two messages in one
	// conversation must not overtake each other while one waits on the network.
	// The gateway serializes turns per origin, but it serializes them in arrival
	// order, so the ordering has to be preserved here, before it hands them over.
	const ingress = new OrderedIngress();
	discord.on("messageCreate", (message) => {
		const engagement = decideInbound(message, discord.user, config.channels);
		if (!engagement) return;
		// Attachments are rendered into the body: a voice message or an uncaptioned
		// image has no content at all, and the gateway rejects empty text, so
		// forwarding content alone dropped the message without a trace.
		const rendered = describeInboundBody(message);
		if (rendered === "") return;
		const origin = discordMessageOrigin(message);
		const receivedAt =
			typeof message.createdTimestamp === "number" ? new Date(message.createdTimestamp).toISOString() : undefined;
		ingress.run(origin.conversationId, async () => {
			// A voice message carries no text at all, so without a transcript the
			// history shows a url and nothing about what was said. Doing this in the
			// runtime rather than the persona is a standing owner instruction.
			const body = withTranscript(rendered, await transcribeIfVoice(message, config.voice));
			// The modality of the question decides the modality of the answer: a
			// spoken message is answered in voice and text both, without the
			// persona having to ask for it.
			const spoken = firstVoiceMessage(message) !== undefined;
			gateway.sendInbound(message.id as string, origin, body, engagement, receivedAt, spoken);
		});
	});
	// An edit is an update of a message the persona may already have read, not a
	// new message: it goes out as chat.edit and reaches the session as a
	// [MESSAGE POINTER] update. Partial (uncached) messages carry no author or
	// content until fetched; fetching is what makes the edit describable.
	discord.on("messageUpdate", (before, after) => {
		void (async () => {
			const message = after.partial ? await after.fetch().catch(() => undefined) : after;
			if (!message) return;
			const edit = describeMessageEdit(message, discord.user, config.channels, before);
			if (!edit) return;
			ingress.run(edit.origin.conversationId, async () => {
				gateway.sendEdit(edit.messageId, edit.origin, edit.text, edit.engagement, edit.receivedAt);
			});
		})();
	});
	// A reaction is engagement metadata, never a turn: it goes out on its own verb.
	discord.on("messageReactionAdd", (reaction, user) => {
		gateway.sendReaction(reaction, user, "add", discord.user);
	});
	// A REMOVAL means the reactor retracted the signal. It is recorded as its own
	// metadata event rather than erasing the add, because the persona may already
	// have read the add — rewriting history behind it would make its memory of the
	// conversation disagree with what it was told.
	discord.on("messageReactionRemove", (reaction, user) => {
		gateway.sendReaction(reaction, user, "remove", discord.user);
	});
	discord.on("interactionCreate", (interaction) => {
		if (interaction.isAutocomplete()) void handleModelAutocomplete(interaction, gateway);
		else if (interaction.isChatInputCommand()) void handleSlashCommand(interaction, gateway);
	});
	discord.once("ready", () => {
		console.log("Discord adapter connected.");
		// Slash-command mapping: /new, /reset and /model are first-class Discord
		// commands that route into the gateway's chat-command verbs for the invoking
		// conversation (typing "/new" as chat text never reaches messageCreate).
		void discord.application?.commands
			.set([...DISCORD_SLASH_COMMANDS])
			.catch((error: unknown) =>
				console.error(
					`Discord slash-command registration failed: ${error instanceof Error ? error.message : String(error)}`,
				),
			);
		void gateway.recoverMissedMessages();
	});
	console.log("Discord adapter starting.");
	await gateway.connect();
	await discord.login(config.token);
}

/** Discord application-command option types used by the registration below. */
const SUBCOMMAND_OPTION = 1;
const STRING_OPTION = 3;
/** Discord caps an autocomplete response at 25 choices. */
const AUTOCOMPLETE_LIMIT = 25;

/**
 * Every slash command the adapter registers. `/model` mirrors the gateway's
 * `/model`, `/model set <choice>` and `/model clear` text commands; the
 * choice autocompletes from the gateway's model catalog so nobody has to
 * remember preset names, but free text is still accepted.
 */
export const DISCORD_SLASH_COMMANDS = [
	{ name: "new", description: "Start a fresh persona session in this conversation" },
	{ name: "reset", description: "Reset this conversation's persona session" },
	{ name: "restart", description: "Restart the gateway process (owner only)" },
	{
		name: "model",
		description: "Show or change the model for this conversation",
		options: [
			{ type: SUBCOMMAND_OPTION, name: "show", description: "Show the effective model and where it comes from" },
			{
				type: SUBCOMMAND_OPTION,
				name: "set",
				description: "Use a gjc preset or model selector for this conversation",
				options: [
					{
						type: STRING_OPTION,
						name: "choice",
						description: "Preset name, provider/model selector, or preset:/model: prefixed name",
						required: true,
						autocomplete: true,
					},
				],
			},
			{ type: SUBCOMMAND_OPTION, name: "clear", description: "Drop this conversation's override" },
		],
	},
] as const satisfies ReadonlyArray<{
	readonly name: string;
	readonly description: string;
	readonly options?: ReadonlyArray<{
		readonly type: number;
		readonly name: string;
		readonly description: string;
		readonly options?: ReadonlyArray<{
			readonly type: number;
			readonly name: string;
			readonly description: string;
			readonly required?: boolean;
			readonly autocomplete?: boolean;
		}>;
	}>;
}>;

/** Duck-typed slice of a Discord autocomplete interaction. */
export interface AutocompleteInteractionLike {
	isAutocomplete?(): boolean;
	readonly commandName?: string;
	readonly options: { getFocused(): string };
	respond(choices: Array<{ name: string; value: string }>): Promise<unknown>;
}

/**
 * Answers `/model set` autocomplete from the gateway's catalog. Fails soft: an
 * unreachable gateway or unreadable catalog yields an empty list, which Discord
 * shows as "no options" while still accepting a typed choice.
 */
export async function handleModelAutocomplete(
	interaction: AutocompleteInteractionLike,
	gateway: Pick<ReconnectingGateway, "modelChoices">,
	log: Pick<Console, "error"> = console,
): Promise<void> {
	if (!interaction.isAutocomplete?.() || interaction.commandName !== "model") return;
	let choices: readonly string[] = [];
	try {
		choices = await gateway.modelChoices();
	} catch (error) {
		log.error(`Discord /model autocomplete failed: ${error instanceof Error ? error.message : String(error)}`);
	}
	const needle = interaction.options.getFocused().trim().toLowerCase();
	const matches = choices
		.filter((choice) => choice.toLowerCase().includes(needle))
		.slice(0, AUTOCOMPLETE_LIMIT)
		.map((choice) => ({ name: choice, value: choice }));
	try {
		await interaction.respond(matches);
	} catch (error) {
		log.error(`Discord /model autocomplete reply failed: ${error instanceof Error ? error.message : String(error)}`);
	}
}

/** Duck-typed slice of a Discord chat-input command interaction. */
export interface SlashInteractionLike {
	isChatInputCommand?(): boolean;
	readonly commandName?: string;
	/** Present for commands with subcommands/options (`/model`). */
	readonly options?: {
		getSubcommand(required?: boolean): string | null;
		getString(name: string, required?: boolean): string | null;
	};
	readonly id: string;
	readonly user?: {
		readonly id: string;
		readonly username?: string;
		readonly globalName?: string | null;
		readonly primaryGuild?: {
			readonly tag?: string | null;
			readonly identityEnabled?: boolean | null;
			readonly identityGuildId?: string | null;
		} | null;
	};
	/** Guild member for the invoking user, when the command ran in a guild. */
	readonly member?: {
		readonly nick?: string | null;
		readonly displayName?: string | null;
		readonly nickname?: string | null;
	} | null;
	readonly channel?: DiscordMessageOriginShape["channel"] | null;
	reply(options: { content: string; ephemeral?: boolean }): Promise<unknown>;
}

/** Same precedence as messages, for a slash command's invoking user. */
export function resolveInteractionDisplayName(
	interaction: Pick<SlashInteractionLike, "user" | "member">,
): string | undefined {
	return resolveDisplayName(interaction.user, interaction.member);
}

export async function handleSlashCommand(
	interaction: SlashInteractionLike,
	gateway: Pick<ReconnectingGateway, "requestInbound">,
	log: Pick<Console, "error"> = console,
): Promise<void> {
	if (!interaction.isChatInputCommand?.()) return;
	const command = interaction.commandName;
	if (command !== "new" && command !== "reset" && command !== "model") return;
	if (!interaction.channel || !interaction.user) return;
	try {
		let text = `/${command}`;
		if (command === "model") {
			const subcommand = interaction.options?.getSubcommand(false) ?? "show";
			if (subcommand === "set") {
				const choice = interaction.options?.getString("choice")?.trim() ?? "";
				// An empty choice would reach the gateway as a bare `/model` read and
				// look like an accepted change that did nothing.
				if (!choice) {
					await interaction.reply({ content: "choose a model: `/model set <choice>`", ephemeral: true });
					return;
				}
				text = `/model set ${choice}`;
			} else if (subcommand === "clear") text = "/model clear";
		}
		const origin = discordMessageOrigin({ author: { id: interaction.user.id }, channel: interaction.channel });
		const interactionServerTag = resolveServerTag(interaction.user);
		const result = await gateway.requestInbound(`slash-${interaction.id}`, origin, text, {
			mentioned: true,
			group: origin.kind !== "dm",
			authorId: interaction.user.id,
			...(resolveInteractionDisplayName(interaction)
				? { authorName: resolveInteractionDisplayName(interaction) as string }
				: {}),
			...(interaction.user.username ? { authorHandle: interaction.user.username } : {}),
			...(interactionServerTag ? { authorServerTag: interactionServerTag } : {}),
		});
		// Honest ack: the gateway allowlist may decline the command (non-owner in a
		// group surface) — never claim a reset that did not happen. `/model` never
		// resets the session; the gateway posts the resulting selection itself.
		await interaction.reply(
			result?.engaged
				? { content: command === "model" ? "🦞 model command accepted" : "🦞 session reset", ephemeral: true }
				: { content: "not authorized for session commands here", ephemeral: true },
		);
	} catch (error) {
		log.error(`Discord slash command failed: ${error instanceof Error ? error.message : String(error)}`);
	}
}

/** Outcome of one recovery send; every failure carries its classification. */
export type RecoveredSendResult =
	| { readonly verdict: "acked" | "duplicate" }
	| { readonly verdict: "unavailable"; readonly failure: RecoveryFailureClass; readonly summary: string };

export class ReconnectingGateway {
	#client: GajaewayClient | undefined;
	#reconnecting = false;
	#attempt = 0;
	#deliveryOff: (() => void) | undefined;
	#progressOff: (() => void) | undefined;
	readonly #inbound = new RecoveryGate();
	#cursors: RecoveryCursorState | undefined;
	#cursorLoads: Promise<void> | undefined;
	#cursorFault: string | undefined;
	#cursorSaves: Promise<void> = Promise.resolve();
	#recovering = false;
	#retryTimer: ReturnType<typeof setTimeout> | undefined;
	#retryAttempt = 0;
	readonly #unreadableRecoveryTargets = new Map<string, number>();
	#recoverableIds = new Set<string>();

	constructor(
		readonly socketPath: string,
		readonly discord: DiscordClientLike,
		readonly config: LoadedDiscordAdapterConfig,
		readonly typing?: TypingPort,
		readonly status?: WorkingStatus,
		readonly recoveryCursorPath: string = defaultRecoveryCursorPath(),
		readonly getBotUser: () => unknown = () => undefined,
		initialClient?: GajaewayClient,
		/** Injectable only so tests do not pay real recovery backoff. */
		readonly sleep: (ms: number) => Promise<void> = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
		readonly speech?: DiscordSpeechPorts,
	) {
		this.#client = initialClient;
		void this.ensureCursors();
	}

	/** Test seam: the link came back with this client (what connect() does after a successful socket open). */
	adoptClient(client: GajaewayClient): void {
		this.#client = client;
		this.#attempt = 0;
		void this.#flushEdits();
	}

	async connect(): Promise<void> {
		try {
			const client = await GajaewayClient.connectSocket(this.socketPath, { clientName: "adapter-discord" });
			this.#client = client;
			this.#attempt = 0;
			this.#deliveryOff?.();
			this.#deliveryOff = subscribeDiscordDeliveries(
				client,
				this.discord,
				this.typing,
				this.status,
				console,
				this.speech,
			);
			this.#progressOff?.();
			this.#progressOff = this.status ? subscribeDiscordProgress(client, this.status, console, this.typing) : undefined;
			console.log("Discord adapter connected to gateway.");
			this.monitor(client);
			// Queued edits first: a backfilled message must not overtake an edit
			// the user made before the link came back.
			await this.#flushEdits();
			void this.recoverMissedMessages();
		} catch {
			this.scheduleReconnect();
		}
	}

	/**
	 * Bounded catch-up for messages missed while the adapter or its gateway link was
	 * offline (issue #33). Runs after Discord ready and after every gateway reconnect;
	 * safe alongside live traffic because the gateway durably dedupes on message id.
	 */
	async recoverMissedMessages(): Promise<void> {
		if (this.#recovering) {
			// A pass is already running and owns the follow-up decision for its own outcome;
			// this caller still gets a retry so its trigger is not silently dropped.
			this.scheduleRecoveryRetry();
			return;
		}
		const botUser = this.getBotUser();
		if (!this.#client || !botUser) {
			// A fired retry that cannot proceed (no gateway link yet, Discord not ready) must
			// reschedule itself, otherwise the gap waits for a reconnect that may never come.
			this.scheduleRecoveryRetry();
			return;
		}
		this.#recovering = true;
		// A pass counts as complete only when every non-quarantined target finished cleanly.
		// Permission-dead targets are still probed on reconnect, but stop driving a 60s error loop.
		let completed = false;
		try {
			await this.ensureCursors();
			if (!this.#cursors) {
				console.error(
					`Discord recovery refused: cursor store ${this.recoveryCursorPath} is unusable (${this.#cursorFault ?? "unknown error"}); retrying with backoff.`,
				);
				return;
			}
			const pruned = pruneKnownDms(this.#cursors, Date.now());
			if (pruned !== this.#cursors) this.persist(pruned);
			const configuredIds = Object.keys(this.config.channels ?? {});
			const queue = [...configuredIds, ...Object.keys(this.#cursors.knownDms)];
			if (queue.length === 0) {
				completed = true;
				return;
			}
			const seen = new Set<string>();
			this.#recoverableIds = new Set(queue);
			let incomplete = false;
			for (let index = 0; index < queue.length; index++) {
				const conversationId = queue[index] as string;
				if (seen.has(conversationId)) continue;
				seen.add(conversationId);
				try {
					const result = await this.recoverChannel(conversationId, botUser, configuredIds.includes(conversationId));
					if (result.incomplete) incomplete = true;
					for (const threadId of result.threadIds) {
						if (seen.has(threadId)) continue;
						this.#recoverableIds.add(threadId);
						queue.push(threadId);
					}
				} catch (error) {
					console.error(
						`Discord recovery aborted for conversation ${conversationId}: ${error instanceof Error ? error.message : String(error)}`,
					);
					incomplete = true;
				}
			}
			this.persist(retainRecoveryCursors(this.#cursors, this.#recoverableIds));
			completed = !incomplete;
		} catch (error) {
			console.error(
				`Discord recovery pass failed: ${error instanceof Error ? error.message : String(error)}; retrying with backoff.`,
			);
		} finally {
			this.#recovering = false;
			if (completed) {
				this.#retryAttempt = 0;
				if (this.#retryTimer) clearTimeout(this.#retryTimer);
				this.#retryTimer = undefined;
			} else this.scheduleRecoveryRetry();
			// A resolved pass means its progress is on disk. Persists stay fire-and-forget
			// during the pass, then the pass joins the chain before reporting completion.
			await this.cursorsFlushed;
		}
	}

	/** Resolves once every cursor write queued so far has hit the cursor store. */
	get cursorsFlushed(): Promise<void> {
		return this.#cursorSaves;
	}

	/** Last recovery cursor load error, or undefined while persistence is healthy. */
	get cursorFault(): string | undefined {
		return this.#cursorFault;
	}

	/** Durable discard log, newest last; bounded by RECOVERY_DEAD_LETTER_CAP. */
	get deadLetters(): readonly RecoveryDeadLetter[] {
		return this.#cursors?.deadLetters ?? [];
	}

	/** Per-conversation discard aggregates; survive dead-letter eviction. */
	get deadLetterDigest(): Readonly<Record<string, RecoveryDeadLetterDigest>> {
		return this.#cursors?.deadLetterDigest ?? {};
	}

	/** True while a backoff retry of the recovery pass is armed. */
	get recoveryRetryPending(): boolean {
		return this.#retryTimer !== undefined;
	}

	/** Recovers one conversation and discovers child threads when this is a configured parent. */
	private async recoverChannel(
		channelId: string,
		botUser: unknown,
		discoverThreads: boolean,
	): Promise<{ readonly incomplete: boolean; readonly threadIds: readonly string[] }> {
		let fetched: unknown;
		try {
			fetched = await this.discord.channels.fetch(channelId);
		} catch (error) {
			if (!isPermanentDiscordUnreadable(error)) {
				console.error(
					`Discord recovery could not fetch conversation ${channelId}: ${error instanceof Error ? error.message : String(error)}; cursor unchanged, retrying with backoff.`,
				);
				return { incomplete: true, threadIds: [] };
			}
			return {
				incomplete: this.noteUnreadableRecoveryTarget(
					channelId,
					error instanceof Error ? error.message : String(error),
				),
				threadIds: [],
			};
		}
		if (!isRecoverableChannel(fetched)) {
			return {
				incomplete: this.noteUnreadableRecoveryTarget(channelId, "deleted, no access, or not a text channel"),
				threadIds: [],
			};
		}
		this.clearUnreadableRecoveryTarget(channelId);
		const threadIds = discoverThreads ? await discoverRecoveryThreadIds(fetched, channelId, Date.now()) : [];

		const cursors = this.#cursors;
		// A quarantined watermark (channel previously dropped from config) counts: resuming
		// from it beats replaying the whole bootstrap window on re-add.
		const before = cursors?.recoveredThrough[channelId] ?? cursors?.quarantined[channelId]?.watermark;
		const outcome = await recoverConversation(fetched, {
			cursor: before,
			nowMs: Date.now(),
			deliver: async (message) => {
				// Same normalization as live messageCreate: one decision, one origin shape,
				// one gateway verb — recovery never forks engagement semantics.
				const engagement = decideInbound(message, botUser, this.config.channels);
				if (!engagement) return "skip";
				const origin = discordMessageOrigin(message);
				let failure: RecoveryFailureClass = "write-path-unknown";
				let summary = "unclassified chat.send failure";
				for (let attempt = 1; attempt <= RECOVERY_MAX_ATTEMPTS; attempt++) {
					const result = await this.requestRecovered(message.id, origin, message.content, engagement);
					if (result.verdict !== "unavailable") {
						this.forgetAttempts(message.id);
						return result.verdict;
					}
					failure = result.failure;
					summary = result.summary;
					// Cross-pass accounting: a message alternating terminal and transient
					// failures still converges on its budget instead of blocking its channel.
					this.noteAttempt(message.id, channelId, failure, summary);
					if (attempt < RECOVERY_MAX_ATTEMPTS) await this.sleep(RECOVERY_ATTEMPT_BACKOFF_MS * 2 ** (attempt - 1));
				}
				this.flushCursors();
				const terminalAttempts = this.#cursors?.attempts[message.id]?.terminalAttempts ?? 0;
				// Out of per-pass budget. A discard is only *proposed* here, and only with
				// per-payload evidence (terminal classification) sustained across passes.
				// recoverConversation still refuses to commit it until a later message lands,
				// so a contract regression that fails every message discards nothing.
				if (failure === "terminal-message" && terminalAttempts >= RECOVERY_MAX_ATTEMPTS) {
					console.error(
						`Discord recovery proposes discarding message ${message.id} in channel ${channelId} after ${terminalAttempts} terminal chat.send rejections (${summary}); held until a later message proves the write path works.`,
					);
					return "discard-candidate";
				}
				console.error(
					`Discord recovery holding message ${message.id} in channel ${channelId} after ${RECOVERY_MAX_ATTEMPTS} failed chat.send attempts (${failure}: ${summary}); cursor stays behind it and the pass retries with backoff.`,
				);
				return "unavailable";
			},
			onDiscard: (message) => {
				const ledger = this.#cursors?.attempts[message.id];
				console.error(
					`Discord recovery is DISCARDING message ${message.id} in channel ${channelId} after ${ledger?.terminalAttempts ?? RECOVERY_MAX_ATTEMPTS} terminal chat.send rejections (${ledger?.summary ?? "terminal rejection"}). Dead-lettered; the rest of the backfill continues.`,
				);
				this.deadLetter({
					messageId: message.id,
					conversationId: channelId,
					classification: "terminal-message",
					attempts: ledger?.terminalAttempts ?? RECOVERY_MAX_ATTEMPTS,
					at: new Date().toISOString(),
					summary: ledger?.summary ?? "terminal rejection",
				});
				this.forgetAttempts(message.id);
			},
		});
		// Durable progress is everything the run walked past — acked sends, known duplicates
		// and committed discards alike. Anything narrower strands the gap behind a page bound
		// full of skipped messages.
		//
		// NIT (accepted): this runs once per pass, so a crash mid-pass loses the in-pass
		// window and the next pass re-sends it. Exactly-once there rests on the gateway's
		// durable message-id dedupe (inbound_messages/conversation_context), not on this
		// watermark; per-message persistence would cost one fsync per replayed message. The
		// dead-letter record is written before the cursor moves and is idempotent per message
		// id, so the replay cannot double-count a discard.
		this.advanceRecovered(channelId, outcome.advancedTo);
		if (outcome.fetchError) {
			console.error(
				`Discord recovery could not read history for channel ${channelId}: ${outcome.fetchError}; other channels continue, retrying with backoff.`,
			);
			return { incomplete: true, threadIds };
		}
		if (outcome.failed) {
			const reason =
				outcome.held > 0
					? `${outcome.held} message(s) failed the same way with nothing succeeding in between — treating it as a gateway write-path problem, discarding nothing`
					: "gateway unavailable";
			console.error(
				`Discord recovery paused for channel ${channelId} at message ${outcome.advancedTo}; ${reason}, retrying with backoff.`,
			);
			return { incomplete: true, threadIds };
		}
		if (outcome.truncated) {
			const progress =
				before !== undefined && outcome.advancedTo === before
					? `cursor unchanged at ${before}`
					: `cursor advanced to ${outcome.advancedTo}`;
			console.error(
				`Discord recovery hit the ${RECOVERY_MAX_PAGES}-page bound for channel ${channelId} after ${outcome.delivered} fresh message(s), ${outcome.duplicates} duplicate(s), and ${outcome.skipped} skip(s); ${progress}.`,
			);
			return { incomplete: true, threadIds };
		}
		if (outcome.delivered > 0 || outcome.duplicates > 0 || outcome.skipped > 0 || outcome.discarded > 0) {
			console.log(
				`Discord recovery backfilled ${outcome.delivered} fresh message(s), found ${outcome.duplicates} duplicate(s), skipped ${outcome.skipped}, and discarded ${outcome.discarded} for channel ${channelId}.`,
			);
		}
		return { incomplete: false, threadIds };
	}

	private noteUnreadableRecoveryTarget(conversationId: string, reason: string): boolean {
		const attempts = (this.#unreadableRecoveryTargets.get(conversationId) ?? 0) + 1;
		this.#unreadableRecoveryTargets.set(conversationId, attempts);
		if (attempts < RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS) {
			console.error(
				`Discord recovery could not read conversation ${conversationId}: ${reason}; cursor unchanged, retrying with backoff (${attempts}/${RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS}).`,
			);
			return true;
		}
		if (attempts === RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS) {
			console.warn(
				`Discord recovery quarantined unreadable conversation ${conversationId} after ${attempts} attempts (${reason}); its watermark is retained and other conversations may complete. It will be probed again on reconnect.`,
			);
		}
		return false;
	}

	private clearUnreadableRecoveryTarget(conversationId: string): void {
		const attempts = this.#unreadableRecoveryTargets.get(conversationId);
		if (attempts === undefined) return;
		this.#unreadableRecoveryTargets.delete(conversationId);
		if (attempts >= RECOVERY_UNREADABLE_QUARANTINE_ATTEMPTS)
			console.log(`Discord recovery re-admitted conversation ${conversationId} after history became readable.`);
	}

	/** Re-runs recovery after a backoff so a paused or truncated gap keeps draining. */
	private scheduleRecoveryRetry(): void {
		if (this.#retryTimer) return;
		const delay = Math.min(RECOVERY_RETRY_MAX_MS, RECOVERY_RETRY_BASE_MS * 2 ** Math.min(this.#retryAttempt++, 6));
		console.log(`Discord recovery retrying in ${delay}ms.`);
		const timer = setTimeout(() => {
			this.#retryTimer = undefined;
			void this.recoverMissedMessages();
		}, delay);
		timer.unref?.();
		this.#retryTimer = timer;
	}

	/**
	 * Durably records a discarded message before the cursor moves past it. Console lines are
	 * lost on restart; this record is what makes a discard auditable and replayable by hand.
	 */
	private deadLetter(entry: RecoveryDeadLetter): void {
		const current = this.#cursors;
		if (!current) return;
		this.persist(recordDeadLetter(current, entry));
	}

	/**
	 * Stages one failed attempt in the cross-pass ledger. Staged, not persisted: the retry
	 * loop calls this up to RECOVERY_MAX_ATTEMPTS times per message and `flushCursors` writes
	 * the accumulated result once, so a blocked message costs one save instead of three.
	 */
	private noteAttempt(
		messageId: string,
		conversationId: string,
		classification: RecoveryFailureClass,
		summary: string,
	): void {
		const current = this.#cursors;
		if (!current) return;
		this.#cursors = recordAttempt(current, messageId, conversationId, classification, summary);
	}

	/** Drops the ledger entry for a message that landed or was discarded. */
	private forgetAttempts(messageId: string): void {
		const current = this.#cursors;
		if (!current) return;
		const next = clearAttempt(current, messageId);
		if (next !== current) this.persist(next);
	}

	/** Persists whatever is currently staged in memory. */
	private flushCursors(): void {
		const current = this.#cursors;
		if (current) this.persist(current);
	}

	private ensureCursors(): Promise<void> {
		this.#cursorLoads ??= loadRecoveryCursors(this.recoveryCursorPath)
			.then((state) => {
				this.#cursors = state;
				this.#cursorFault = undefined;
			})
			.catch((error: unknown) => {
				// Observable and retryable: drop the memoized load so the next recovery pass
				// tries again instead of silently running without persistence forever.
				this.#cursorFault = error instanceof Error ? error.message : String(error);
				this.#cursorLoads = undefined;
				console.error(`Discord recovery cursor load failed: ${this.#cursorFault}`);
			});
		return this.#cursorLoads;
	}

	/**
	 * Records recovery progress for a conversation and persists it (serialized,
	 * fire-and-forget). Only completed recovery progress lands here — live sends must never
	 * push this watermark past a gap they did not backfill (issue #33). Watermarks for
	 * conversations recovery does not iterate (threads/DMs, channels dropped from config)
	 * are moved to the bounded `quarantined` section rather than deleted.
	 */
	private advanceRecovered(conversationId: string, messageId: string): void {
		const current = this.#cursors;
		if (!current) return;
		const existing = current.recoveredThrough[conversationId] ?? current.quarantined[conversationId]?.watermark;
		if (existing !== undefined && !snowflakeIsAfter(messageId, existing)) return;
		this.persist(
			retainRecoveryCursors(
				{ ...current, recoveredThrough: { ...current.recoveredThrough, [conversationId]: messageId } },
				this.#recoverableIds,
			),
		);
	}

	/** Serialized, fire-and-forget cursor-store write; a failed persist only widens the gap. */
	private persist(next: RecoveryCursorState): void {
		this.#cursors = next;
		this.#cursorSaves = this.#cursorSaves
			.then(() => saveRecoveryCursors(this.recoveryCursorPath, next))
			.catch((error: unknown) =>
				console.error(
					`Discord recovery cursor persist failed: ${error instanceof Error ? error.message : String(error)}`,
				),
			);
	}

	sendInbound(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
		receivedAt?: string,
		voice?: boolean,
	): void {
		void this.requestInbound(messageId, origin, text, engagement, receivedAt, voice);
	}

	/**
	 * Edits that could not reach the gateway (link down, request failed). Unlike
	 * a missed message, history recovery cannot reconstruct an edit - the
	 * original id is already known - so the edit itself is kept and replayed on
	 * the next connect. Bounded and keyed by message: a later edit of the same
	 * message supersedes an older queued one, and the gateway dedupes a replay
	 * by content.
	 */
	readonly #editOutbox = new Map<string, PendingEdit>();
	#editFlush: Promise<void> | undefined;

	/**
	 * Edited message -> chat.edit. Not routed through requestInbound: its dedupe
	 * key is the platform message id, which the ORIGINAL already consumed, and
	 * the gateway owns edit idempotency (one row per message + content).
	 */
	sendEdit(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
		receivedAt?: string,
	): void {
		this.#editOutbox.set(messageId, { messageId, origin, text, engagement, ...(receivedAt ? { receivedAt } : {}) });
		if (this.#editOutbox.size > EDIT_OUTBOX_LIMIT) {
			const oldest = this.#editOutbox.keys().next().value as string;
			this.#editOutbox.delete(oldest);
			console.error(`Discord edit outbox full; dropped the oldest queued edit (message ${oldest}).`);
		}
		void this.#flushEdits();
	}

	/** Test seam: queued edits not yet acknowledged by the gateway. */
	get pendingEdits(): readonly PendingEdit[] {
		return [...this.#editOutbox.values()];
	}

	async #flushEdits(): Promise<void> {
		if (this.#editFlush) return await this.#editFlush;
		this.#editFlush = (async () => {
			// Drain the LIVE map, oldest first, until it is empty: an edit queued
			// while an earlier request is in flight (a superseding edit of the
			// same message, or another message in the same tick) must go out in
			// this pass, not wait for the next unrelated trigger.
			for (;;) {
				const edit = this.#editOutbox.values().next().value as PendingEdit | undefined;
				if (!edit) return;
				const client = this.#client;
				if (!client) {
					this.scheduleReconnect();
					return;
				}
				try {
					const result = await client.request<{ engaged?: boolean }>("chat.edit", {
						origin: edit.origin,
						messageId: edit.messageId,
						text: edit.text,
						engagement: edit.engagement,
						...(edit.receivedAt ? { receivedAt: edit.receivedAt } : {}),
					});
					// Acknowledged: drop it unless a newer edit of the same message
					// was queued behind this one meanwhile.
					if (this.#editOutbox.get(edit.messageId) === edit) this.#editOutbox.delete(edit.messageId);
					if (result?.engaged && addressedTurn(edit.engagement)) {
						this.status?.arm(edit.origin.conversationId, edit.messageId);
						this.typing?.begin(edit.origin.conversationId);
					}
				} catch (error) {
					console.error(
						`Discord chat.edit failed; edit of ${edit.messageId} kept for replay: ${error instanceof Error ? error.message : String(error)}`,
					);
					this.scheduleReconnect();
					return;
				}
			}
		})().finally(() => {
			this.#editFlush = undefined;
		});
		return await this.#editFlush;
	}

	/**
	 * Inbound reaction -> engagement.reaction, fire and forget.
	 *
	 * Deliberately NOT routed through requestInbound: that path dedupes on the
	 * platform message id (a reaction carries its *target's* id, so the second
	 * reaction on a message would be swallowed as a duplicate) and it starts the
	 * typing indicator, which promises a reply that metadata never produces.
	 */
	sendReaction(
		reaction: DiscordInboundReaction,
		user: DiscordReactingUser,
		action: ReactionAction,
		botUser: unknown,
	): void {
		// Our own presence markers are never engagement, even when the identity
		// check that filters our reactions is not yet available on a cold start.
		if (isPresenceReaction(String(reaction.emoji?.name ?? ""))) {
			const botId = typeof botUser === "object" && botUser !== null && "id" in botUser ? String(botUser.id) : "";
			if (!botId || String(user.id) === botId) return;
		}
		const described = describeInboundReaction(reaction, user, botUser, action);
		if (!described) return;
		const client = this.#client;
		if (!client) return;
		// A rejected engagement note is metadata, not a link failure: reconnecting
		// here replays every undelivered ledger row and re-arms typing/status for
		// nothing. The monitor owns reconnect decisions.
		void client.request("engagement.reaction", described).catch((error) => {
			console.error(`Discord engagement.reaction failed: ${error instanceof Error ? error.message : String(error)}`);
		});
	}

	/** `/model set` autocomplete source; throws when the gateway link is down. */
	async modelChoices(): Promise<readonly string[]> {
		const client = this.#client;
		if (!client) throw new Error("gateway link not connected");
		const result = await client.request<SessionModelChoicesResult>("session.modelChoices");
		return result.choices;
	}

	/** Like sendInbound but reports the gateway's engagement decision to the caller. */
	async requestInbound(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
		receivedAt?: string,
		/** The message was spoken, so the reply is owed in both modalities. */
		voice?: boolean,
	): Promise<{ engaged?: boolean } | undefined> {
		if (origin.platform === "discord" && origin.kind === "dm") {
			await this.ensureCursors();
			if (this.#cursors) this.persist(rememberKnownDm(this.#cursors, origin.conversationId, Date.now()));
		}
		let result: { engaged?: boolean } | undefined;
		const verdict = await this.#inbound.join(messageId, async () => {
			const client = this.#client;
			if (!client) {
				this.scheduleReconnect();
				return "unavailable";
			}
			try {
				// The gateway acknowledges engagement before running the turn, so typing starts only for
				// turns that will actually produce a reply and never outlives the delivery that clears it.
				result = await client.request<{ engaged?: boolean }>("chat.send", {
					origin,
					text,
					engagement,
					messageId,
					...(receivedAt ? { receivedAt } : {}),
					...(voice ? { voice: true } : {}),
				});
				// No recovery-watermark write here on purpose: a live message is no evidence that
				// the older messages behind it were ever backfilled (issue #33).
				// Presence hints are shown only where the persona was ADDRESSED: a DM,
				// an explicit mention, or an `open` channel's promotion (all three are
				// `mentioned` by the time engagement is built). A public channel the
				// persona merely overhears shows nothing until the reply itself lands.
				if (result?.engaged && addressedTurn(engagement)) {
					this.status?.arm(origin.conversationId, messageId);
					this.typing?.begin(origin.conversationId);
				}
				return "acked";
			} catch (error) {
				// Never silent (#176): a live message the gateway did not accept is only
				// recovered by the next backfill pass, so the drop must be traceable to
				// the message and conversation it belongs to.
				console.error(
					`Discord chat.send failed message=${messageId} channel=${origin.conversationId}: ${summarizeRecoveryFailure(error)}; left for recovery.`,
				);
				this.scheduleReconnect();
				return "unavailable";
			}
		});
		return verdict === "acked" ? result : undefined;
	}

	/**
	 * Recovery send for messages missed while offline (issue #33): same LRU dedupe, same
	 * chat.send verb, same durable gateway exactly-once as live sends. Returns whether the
	 * gateway acknowledged ("acked"), the message was already known ("duplicate"), or the
	 * send must be retried later ("unavailable" — the recovery watermark does not advance).
	 *
	 * Every failure carries a classification, because that is the only thing allowed to
	 * decide whether a message may ever be discarded. An attempt that merely joined another
	 * in-flight send reports `write-path-unknown`, the safe default.
	 */
	async requestRecovered(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
	): Promise<RecoveredSendResult> {
		let failure: RecoveryFailureClass | undefined;
		let summary: string | undefined;
		const verdict = await this.#inbound.join(messageId, async () => {
			const client = this.#client;
			if (!client) {
				failure = "retryable";
				summary = "gateway link not connected";
				return "unavailable";
			}
			try {
				await client.request("chat.send", { origin, text, engagement, messageId });
				return "acked";
			} catch (error) {
				failure = classifyRecoveryFailure(error);
				summary = summarizeRecoveryFailure(error);
				return "unavailable";
			}
		});
		if (verdict !== "unavailable") return { verdict };
		return {
			verdict,
			failure: failure ?? "write-path-unknown",
			summary: summary ?? "unclassified chat.send failure",
		};
	}

	private monitor(client: GajaewayClient, strikes = 0): void {
		setTimeout(
			() => {
				if (this.#client !== client) return;
				void client.request("gateway.status").then(
					() => this.monitor(client, 0),
					() => {
						const next = monitorFailureDecision(strikes);
						if (next.action === "reconnect") this.scheduleReconnect();
						else this.monitor(client, next.strikes);
					},
				);
			},
			strikes === 0 ? MONITOR_INTERVAL_MS : MONITOR_RETRY_MS,
		);
	}

	private scheduleReconnect(): void {
		if (this.#reconnecting) return;
		this.#reconnecting = true;
		this.#client = undefined;
		this.#deliveryOff?.();
		const delay = Math.min(30_000, 500 * 2 ** Math.min(this.#attempt++, 6));
		const jitter = Math.floor(Math.random() * Math.max(1, delay / 4));
		console.log(`Discord adapter gateway reconnecting in ${delay + jitter}ms.`);
		setTimeout(() => {
			this.#reconnecting = false;
			void this.connect();
		}, delay + jitter);
	}
}

function defaultGatewaySocket(): string {
	return `${process.env.GAJAEWAY_HOME ?? `${process.env.HOME ?? "~"}/.gajaeway`}/gateway.sock`;
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isDiscordTextChannel(value: unknown): value is DiscordTextChannelLike {
	return typeof value === "object" && value !== null && "send" in value && typeof value.send === "function";
}

function isDiscordTypingChannel(value: unknown): value is DiscordTypingChannelLike {
	return typeof value === "object" && value !== null && "sendTyping" in value && typeof value.sendTyping === "function";
}

function isRecoverableChannel(value: unknown): value is RecoverableChannel {
	return (
		typeof value === "object" &&
		value !== null &&
		"messages" in value &&
		typeof (value as { messages?: { fetch?: unknown } }).messages?.fetch === "function"
	);
}

function isPermanentDiscordUnreadable(error: unknown): boolean {
	const candidate = error as { readonly code?: unknown; readonly status?: unknown; readonly httpStatus?: unknown };
	return (
		candidate?.code === 10_003 ||
		candidate?.code === 50_001 ||
		candidate?.code === 50_013 ||
		candidate?.status === 403 ||
		candidate?.status === 404 ||
		candidate?.httpStatus === 403 ||
		candidate?.httpStatus === 404
	);
}

interface RecoveryThreadLike extends RecoverableChannel {
	readonly id: string;
	readonly parentId?: string | null;
	readonly lastMessageId?: string | null;
	readonly archiveTimestamp?: number | null;
}

interface RecoveryThreadManagerLike {
	fetchActive(cache?: boolean): Promise<{ readonly threads: unknown }>;
	fetchArchived(options: {
		readonly type: "public" | "private";
		readonly limit: number;
	}): Promise<{ readonly threads: unknown; readonly hasMore?: boolean }>;
}

function collectionValues(value: unknown): unknown[] {
	if (typeof value !== "object" || value === null || !(Symbol.iterator in value)) return [];
	return [...(value as Iterable<unknown>)].map((entry) => (Array.isArray(entry) ? entry[1] : entry));
}

async function discoverRecoveryThreadIds(
	parent: RecoverableChannel,
	parentId: string,
	nowMs: number,
): Promise<readonly string[]> {
	const manager = (parent as { readonly threads?: Partial<RecoveryThreadManagerLike> }).threads;
	if (!manager || typeof manager.fetchActive !== "function" || typeof manager.fetchArchived !== "function") return [];
	const candidates: Array<{ readonly thread: RecoveryThreadLike; readonly archived: boolean }> = [];
	let failed = false;
	try {
		const active = await manager.fetchActive(false);
		for (const value of collectionValues(active.threads))
			if (isRecoveryThread(value, parentId)) candidates.push({ thread: value, archived: false });
	} catch (error) {
		failed = true;
		console.error(
			`Discord recovery could not enumerate active threads for channel ${parentId}: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	for (const type of ["public", "private"] as const) {
		try {
			const archived = await manager.fetchArchived({ type, limit: 100 });
			for (const value of collectionValues(archived.threads))
				if (isRecoveryThread(value, parentId)) candidates.push({ thread: value, archived: true });
		} catch (error) {
			failed = true;
			console.error(
				`Discord recovery could not enumerate archived ${type} threads for channel ${parentId}: ${error instanceof Error ? error.message : String(error)}`,
			);
		}
	}
	const cutoff = nowMs - RECOVERY_THREAD_RETENTION_MS;
	const ids = new Set<string>();
	for (const { thread, archived } of candidates) {
		if (archived && threadActivityTimestamp(thread) < cutoff) continue;
		ids.add(thread.id);
		if (ids.size >= RECOVERY_THREAD_TARGET_CAP) break;
	}
	if (failed)
		console.warn(
			`Discord recovery thread discovery for channel ${parentId} was partial; ${ids.size} readable thread(s) remain eligible and other conversations continue.`,
		);
	return [...ids];
}

function isRecoveryThread(value: unknown, parentId: string): value is RecoveryThreadLike {
	return (
		isRecoverableChannel(value) &&
		"id" in value &&
		typeof value.id === "string" &&
		"parentId" in value &&
		value.parentId === parentId
	);
}

function threadActivityTimestamp(thread: RecoveryThreadLike): number {
	if (typeof thread.archiveTimestamp === "number") return thread.archiveTimestamp;
	if (thread.lastMessageId && /^\d+$/.test(thread.lastMessageId))
		return Number((BigInt(thread.lastMessageId) >> 22n) + 1_420_070_400_000n);
	return 0;
}
export const DISCORD_USAGE = [
	"usage: gajaeway-discord [--help] [--version]",
	"",
	"Runs the Discord adapter in the foreground. Configuration is read from",
	"$GAJAEWAY_HOME/adapter-discord.json; one instance at a time per home.",
].join("\n");

/** Usage errors exit 2, as `gajaeway-gateway` does; 1 stays a runtime failure. */
export const USAGE_EXIT_CODE = 2;

export type DiscordArgv =
	| { readonly kind: "run" }
	| { readonly kind: "help" }
	| { readonly kind: "version" }
	| { readonly kind: "usage"; readonly message: string };

/**
 * Resolved before any connection work. `--help` used to boot a real adapter,
 * which meant that merely asking what the binary does opened a second Discord
 * session alongside the resident one.
 */
export function parseDiscordArgs(args: readonly string[]): DiscordArgv {
	if (args.length === 0) return { kind: "run" };
	if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) return { kind: "help" };
	if (args.length === 1 && (args[0] === "--version" || args[0] === "-v")) return { kind: "version" };
	return { kind: "usage", message: `gajaeway-discord: unexpected argument ${args[0]}\n${DISCORD_USAGE}` };
}

if (import.meta.main) {
	const argv = parseDiscordArgs(process.argv.slice(2));
	if (argv.kind === "help") {
		console.log(DISCORD_USAGE);
	} else if (argv.kind === "version") {
		console.log(pkg.version);
	} else if (argv.kind === "usage") {
		console.error(argv.message);
		process.exit(USAGE_EXIT_CODE);
	} else {
		const disposeLogging = installStructuredLogging({
			path: join(adapterHome(), "adapter-discord.log"),
		});
		// The lock is taken before the config is even read: refusing early keeps a
		// stray start from touching the resident instance's gateway session.
		AdapterLock.acquire(adapterHome())
			.then(async (lock) => {
				// Registering a signal handler suppresses the default terminate, so
				// the lock is dropped and the exit is then performed by hand.
				const release = (): void =>
					void lock.release().finally(() => {
						disposeLogging();
						process.exit(0);
					});
				process.once("SIGINT", release);
				process.once("SIGTERM", release);
				await startDiscordAdapter(await loadDiscordAdapterConfig());
			})
			.catch((error) => {
				console.error(error instanceof Error ? error.message : String(error));
				disposeLogging();
				process.exitCode = error instanceof AdapterAlreadyRunningError ? USAGE_EXIT_CODE : 1;
			});
	}
}
