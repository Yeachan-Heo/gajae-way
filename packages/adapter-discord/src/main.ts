import { join } from "node:path";
import type {
	ChatMessagePayload,
	ChatProgressPayload,
	EngagementContext,
	OriginRef,
	ReactionAction,
} from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";
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
		mentioned: Boolean(message.mentions?.has(botUser) || contentMention),
		group: origin.kind !== "dm",
		authorId: message.author.id,
		...(message.author.bot ? { authorIsBot: true } : {}),
		...(displayName ? { authorName: displayName } : {}),
		...(message.author.username ? { authorHandle: message.author.username } : {}),
		...(serverTag ? { authorServerTag: serverTag } : {}),
		...(message.channel.name ? { channelLabel: `#${message.channel.name}` } : {}),
		...(message.guild?.name ? { serverLabel: message.guild.name } : {}),
		// Metadata only: a reply — even a reply to us — never promotes engagement.
		...(replyTo ? { replyTo } : {}),
	};
}

/**
 * Decides whether an incoming Discord message becomes a turn, and with what engagement.
 * Returns undefined when the message must be ignored outright.
 *
 * Collaboration means hearing other bots, so only our own messages are dropped. An `open`
 * channel promotes a human message to a mention so the persona joins the room without being
 * called; bot authors never get that promotion, because two open-channel bots would answer each
 * other forever. A bot has to address us explicitly to get a turn.
 */
export function decideInbound(
	message: DiscordInboundMessage,
	botUser: unknown,
	channels: Readonly<Record<string, { readonly engagement?: "open" }>> | undefined,
): EngagementContext | undefined {
	if (message.id === undefined) return undefined;
	const botId = typeof botUser === "object" && botUser !== null && "id" in botUser ? String(botUser.id) : "";
	if (botId !== "" && message.author.id === botId) return undefined;
	const origin = discordMessageOrigin(message);
	const base = engagementForMessage(message, botUser);
	const open = origin.kind !== "dm" && channels?.[origin.conversationId]?.engagement === "open";
	return open && !message.author.bot ? { ...base, mentioned: true } : base;
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
	channels: Readonly<Record<string, { readonly engagement?: "open" }>> | undefined,
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

/** A Discord message the adapter posted and can amend or remove; duck-typed from channel.send(). */
interface EditableDiscordMessage {
	edit(text: string): Promise<unknown>;
	delete(): Promise<unknown>;
}

function isEditableMessage(value: unknown): value is EditableDiscordMessage {
	return (
		typeof value === "object" &&
		value !== null &&
		"edit" in value &&
		typeof (value as EditableDiscordMessage).edit === "function" &&
		"delete" in value &&
		typeof (value as EditableDiscordMessage).delete === "function"
	);
}

function formatElapsed(elapsedMs: number): string {
	const total = Math.floor(elapsedMs / 1000);
	const minutes = Math.floor(total / 60);
	const seconds = total % 60;
	return minutes > 0 ? `${minutes}m ${String(seconds).padStart(2, "0")}s` : `${seconds}s`;
}

function formatTokens(outputTokens: number): string {
	return outputTokens >= 1000 ? `${(outputTokens / 1000).toFixed(1)}k tok` : `${outputTokens} tok`;
}

/**
 * The status shows only what the runtime actually reported. Counters come from
 * the live tail, and gjc 0.16.0 filters `tool_activity` for stdio clients (its
 * stdio transport cannot negotiate the capability), so on that path they stay
 * zero for the whole turn - "working… (4m 40s, 0 tools, 0 tok)" was two fields
 * of noise claiming the turn did nothing (live, 2026-09-03). Zero counters are
 * therefore omitted rather than displayed as facts.
 */
export function workingStatusText(
	progress: Pick<ChatProgressPayload, "elapsedMs" | "toolCalls" | "outputTokens">,
): string {
	const parts = [formatElapsed(progress.elapsedMs)];
	if (progress.toolCalls > 0) parts.push(`${progress.toolCalls} tool${progress.toolCalls === 1 ? "" : "s"}`);
	if (progress.outputTokens > 0) parts.push(formatTokens(progress.outputTokens));
	return `⏳ working… (${parts.join(", ")})`;
}

/**
 * Renders a long-running turn as one temporary, amended status message per
 * conversation ("working… (2m 05s, 3 tools)") driven by gateway chat.progress
 * events, and removes it when the real reply is delivered. Best-effort only:
 * status failures never compete with delivery.
 */
export class WorkingStatus {
	readonly #discord: DiscordClientLike;
	readonly #log: Pick<Console, "error">;
	readonly #messages = new Map<string, EditableDiscordMessage | "pending">();
	readonly #staleTimers = new Map<string, ReturnType<typeof setTimeout>>();
	/**
	 * Conversations where the persona was ADDRESSED (a DM, an explicit mention,
	 * or an `open` channel's promotion). Only these get the "working…" post.
	 * In a public channel that merely *might* draw a reply, nothing is shown
	 * until the reply itself lands: a spinner there is noise for everyone
	 * present, most of the time for a turn that ends in silence.
	 */
	readonly #addressed = new Set<string>();

	constructor(discord: DiscordClientLike, log: Pick<Console, "error"> = console) {
		this.#discord = discord;
		this.#log = log;
	}

	/** Marks a conversation as one the persona was addressed in for the turn that just started. */
	arm(conversationId: string): void {
		this.#addressed.add(conversationId);
	}

	/**
	 * A status that stops receiving progress (the turn wedged into a recovery
	 * hold, or the gateway died) must not outlive the turn: after
	 * WORKING_STATUS_STALE_MS without a tick it is removed.
	 */
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
		const conversationId = progress.origin.conversationId;
		if (!this.#addressed.has(conversationId)) return;
		this.#armStale(conversationId);
		const text = workingStatusText(progress);
		const existing = this.#messages.get(conversationId);
		if (existing === "pending") return; // a send is already in flight; next tick edits
		try {
			if (existing) {
				await existing.edit(text);
				return;
			}
			this.#messages.set(conversationId, "pending");
			const channel = await this.#discord.channels.fetch(conversationId);
			if (!isDiscordTextChannel(channel)) {
				this.#messages.delete(conversationId);
				return;
			}
			const posted = await channel.send(text);
			if (isEditableMessage(posted) && this.#messages.get(conversationId) === "pending") {
				this.#messages.set(conversationId, posted);
				return;
			}
			this.#messages.delete(conversationId);
			// The reply was delivered (clear ran) while this send was in flight: the
			// freshly posted status is already stale — remove it or it lingers forever.
			if (isEditableMessage(posted)) await posted.delete().catch(() => {});
		} catch (error) {
			this.#messages.delete(conversationId);
			this.#log.error(
				`Discord working status failed for ${conversationId}: ${error instanceof Error ? error.message : String(error)}`,
			);
		}
	}

	async clear(conversationId: string): Promise<void> {
		this.#addressed.delete(conversationId);
		const timer = this.#staleTimers.get(conversationId);
		if (timer) clearTimeout(timer);
		this.#staleTimers.delete(conversationId);
		const existing = this.#messages.get(conversationId);
		this.#messages.delete(conversationId);
		if (!existing || existing === "pending") return;
		try {
			await existing.delete();
		} catch {
			// the status message may already be gone; cosmetic either way
		}
	}
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
		const channel = await discord.channels.fetch(message.origin.conversationId);
		if (!isDiscordTextChannel(channel)) {
			throw Object.assign(new Error(`Discord channel ${message.origin.conversationId} cannot receive messages`), {
				code: 10003,
			});
		}
		const text = message.duplicateWarning ? `[recovered - may be a duplicate] ${message.text}` : message.text;
		const chunks = chunkDiscordMessage(text);
		for (let index = 0; index < chunks.length; index++) {
			// Reply-threading applies to the first chunk only; failIfNotExists keeps a
			// deleted target from failing the whole delivery.
			const chunk = chunks[index] as string;
			if (index === 0 && message.replyToMessageId)
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
	const status = new WorkingStatus(discord);
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
		if (interaction.isChatInputCommand()) void handleSlashCommand(interaction, gateway);
	});
	discord.once("ready", () => {
		console.log("Discord adapter connected.");
		// Slash-command mapping: /new and /reset are first-class Discord commands
		// that route into the gateway's session-reset verbs for the invoking
		// conversation (typing "/new" as chat text never reaches messageCreate).
		void discord.application?.commands
			.set([
				{ name: "new", description: "Start a fresh persona session in this conversation" },
				{ name: "reset", description: "Reset this conversation's persona session" },
				{ name: "restart", description: "Restart the gateway process (owner only)" },
			])
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

/** Duck-typed slice of a Discord chat-input command interaction. */
export interface SlashInteractionLike {
	isChatInputCommand?(): boolean;
	readonly commandName?: string;
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
	if (interaction.commandName !== "new" && interaction.commandName !== "reset") return;
	if (!interaction.channel || !interaction.user) return;
	try {
		const origin = discordMessageOrigin({ author: { id: interaction.user.id }, channel: interaction.channel });
		const interactionServerTag = resolveServerTag(interaction.user);
		const result = await gateway.requestInbound(`slash-${interaction.id}`, origin, `/${interaction.commandName}`, {
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
		// group surface) — never claim a reset that did not happen.
		await interaction.reply(
			result?.engaged
				? { content: "🦞 session reset", ephemeral: true }
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
			const client = await GajaewayClient.connectSocket(this.socketPath);
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
		const channelIds = Object.keys(this.config.channels ?? {});
		// Nothing configured means nothing to recover, ever: that is the one early return
		// that must not reschedule.
		if (channelIds.length === 0) return;
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
		// A pass counts as complete only when every channel finished cleanly. Anything else —
		// an unusable cursor store, a channel that reported an open gap, an unexpected throw —
		// arms the backoff retry. No path may reset the backoff without one of the two.
		let completed = false;
		try {
			await this.ensureCursors();
			if (!this.#cursors) {
				// Fail closed: without a readable watermark a backfill cannot record progress,
				// so it would replay the same bootstrap window forever and never close the gap.
				console.error(
					`Discord recovery refused: cursor store ${this.recoveryCursorPath} is unusable (${this.#cursorFault ?? "unknown error"}); retrying with backoff.`,
				);
				return;
			}
			let incomplete = false;
			for (const channelId of channelIds) {
				try {
					if (await this.recoverChannel(channelId, botUser)) incomplete = true;
				} catch (error) {
					// One channel's failure must never abort the pass or the channels behind it.
					console.error(
						`Discord recovery aborted for channel ${channelId}: ${error instanceof Error ? error.message : String(error)}`,
					);
					incomplete = true;
				}
			}
			completed = !incomplete;
		} catch (error) {
			console.error(
				`Discord recovery pass failed: ${error instanceof Error ? error.message : String(error)}; retrying with backoff.`,
			);
		} finally {
			this.#recovering = false;
			if (completed) this.#retryAttempt = 0;
			else this.scheduleRecoveryRetry();
			// A resolved pass means its progress is on disk. Persists stay
			// fire-and-forget during the pass so one slow write cannot stall the
			// backfill, but a caller that awaits the pass and then crashes must not
			// lose the watermark it was told about — and a reader that awaits it must
			// not observe the previous cursor. In the finally, so an early return and a
			// failed pass flush what they already queued.
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

	/** True when the gap for this channel is still open and a retry is scheduled. */
	private async recoverChannel(channelId: string, botUser: unknown): Promise<boolean> {
		let fetched: unknown;
		try {
			fetched = await this.discord.channels.fetch(channelId);
		} catch (error) {
			console.error(
				`Discord recovery could not fetch channel ${channelId}: ${error instanceof Error ? error.message : String(error)}; cursor unchanged, retrying with backoff.`,
			);
			return true;
		}
		if (!isRecoverableChannel(fetched)) {
			// Deleted channel, revoked permission, or a non-text channel id in config: the gap
			// for this channel is unknown, not empty. Never treat that as a clean pass — leave
			// the cursor where it is and let the retry (and the operator) see it.
			console.error(
				`Discord recovery has no readable history for channel ${channelId} (deleted, no access, or not a text channel); cursor unchanged, retrying with backoff.`,
			);
			return true;
		}
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
			return true;
		}
		if (outcome.failed) {
			const reason =
				outcome.held > 0
					? `${outcome.held} message(s) failed the same way with nothing succeeding in between — treating it as a gateway write-path problem, discarding nothing`
					: "gateway unavailable";
			console.error(
				`Discord recovery paused for channel ${channelId} at message ${outcome.advancedTo}; ${reason}, retrying with backoff.`,
			);
			return true;
		}
		if (outcome.truncated) {
			const progress =
				before !== undefined && outcome.advancedTo === before
					? `cursor unchanged at ${before}`
					: `cursor advanced to ${outcome.advancedTo}`;
			console.error(
				`Discord recovery hit the ${RECOVERY_MAX_PAGES}-page bound for channel ${channelId} after ${outcome.delivered} fresh message(s), ${outcome.duplicates} duplicate(s), and ${outcome.skipped} skip(s); ${progress}.`,
			);
			return true;
		}
		if (outcome.delivered > 0 || outcome.duplicates > 0 || outcome.skipped > 0 || outcome.discarded > 0) {
			console.log(
				`Discord recovery backfilled ${outcome.delivered} fresh message(s), found ${outcome.duplicates} duplicate(s), skipped ${outcome.skipped}, and discarded ${outcome.discarded} for channel ${channelId}.`,
			);
		}
		return false;
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
				Object.keys(this.config.channels ?? {}),
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
						this.status?.arm(edit.origin.conversationId);
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
					this.status?.arm(origin.conversationId);
					this.typing?.begin(origin.conversationId);
				}
				return "acked";
			} catch {
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
		// The lock is taken before the config is even read: refusing early keeps a
		// stray start from touching the resident instance's gateway session.
		AdapterLock.acquire(adapterHome())
			.then(async (lock) => {
				// Registering a signal handler suppresses the default terminate, so
				// the lock is dropped and the exit is then performed by hand.
				const release = (): void => void lock.release().finally(() => process.exit(0));
				process.once("SIGINT", release);
				process.once("SIGTERM", release);
				await startDiscordAdapter(await loadDiscordAdapterConfig());
			})
			.catch((error) => {
				console.error(error instanceof Error ? error.message : String(error));
				process.exitCode = error instanceof AdapterAlreadyRunningError ? USAGE_EXIT_CODE : 1;
			});
	}
}
