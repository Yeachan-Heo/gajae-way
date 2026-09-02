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
import { adapterHome, type LoadedDiscordVoiceConfig, loadDiscordAdapterConfig } from "./config";
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

const DISCORD_MESSAGE_LIMIT = 2_000;
// Discord clears the typing hint after ~10s, so refresh inside that window while a turn is running.
const TYPING_REFRESH_MS = 7_000;
// Hard ceiling above the gateway's 300s gjc turn timeout: a lost turn must not type forever.
const TYPING_MAX_MS = 330_000;
/** Working-status without a progress tick for this long is stale (gateway ticks every 15s). */
const WORKING_STATUS_STALE_MS = 90_000;
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
	close?(): void | Promise<void>;
}

export interface AdapterHandle {
	readonly stop: () => Promise<void>;
	readonly settled: Promise<void>;
}

/** Structural copy of the composition-owned generation contract. */
export interface Generation {
	readonly id: number;
	readonly signal: AbortSignal;
	readonly port: GatewayClientLike & { open(): Promise<unknown> };
	track<T>(task: Promise<T>): Promise<T>;
	sleep(ms: number): Promise<void>;
}

export interface DiscordAdapterInput {
	readonly token: string;
	readonly intents?: readonly number[];
	readonly voice?: LoadedDiscordVoiceConfig;
	/** Snapshot used solely to enumerate Discord history during recovery. */
	readonly recoveryChannels: readonly string[];
	readonly recoveryCursorPath: string;
}

export type OpenGatewayClient = GatewayClientLike & { open(): Promise<unknown> };

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

export interface DiscordAdapterClient extends DiscordClientLike {
	readonly user?: unknown;
	readonly application?: {
		readonly commands?: {
			set(commands: readonly { readonly name: string; readonly description: string }[]): Promise<unknown>;
		};
	};
	on(event: string, listener: (...args: unknown[]) => void): unknown;
	once(event: string, listener: (...args: unknown[]) => void): unknown;
	login(token: string): Promise<unknown>;
	destroy(): void;
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
 * Returns undefined when the message must be ignored outright. Engagement gates belong
 * to the gateway, so this adapter reports only facts observed from Discord.
 */
export function decideInbound(message: DiscordInboundMessage, botUser: unknown): EngagementContext | undefined {
	if (message.id === undefined) return undefined;
	const botId = typeof botUser === "object" && botUser !== null && "id" in botUser ? String(botUser.id) : "";
	if (botId !== "" && message.author.id === botId) return undefined;
	return engagementForMessage(message, botUser);
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

	dispose(): void {
		for (const run of this.#runs.values()) if (run.timer) clearTimeout(run.timer);
		this.#runs.clear();
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

	constructor(discord: DiscordClientLike, log: Pick<Console, "error"> = console) {
		this.#discord = discord;
		this.#log = log;
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
		this.#armStale(conversationId);
		const text = `⏳ working… (${formatElapsed(progress.elapsedMs)}, ${progress.toolCalls} tool${progress.toolCalls === 1 ? "" : "s"}, ${formatTokens(progress.outputTokens)})`;
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

	async dispose(): Promise<void> {
		await Promise.all([...this.#messages.keys()].map((conversationId) => this.clear(conversationId)));
	}

	async clear(conversationId: string): Promise<void> {
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

export type DiscordTaskTracker = <T>(task: Promise<T>) => Promise<T>;

const identityDiscordTrack: DiscordTaskTracker = (task) => task;

export function subscribeDiscordDeliveries(
	gateway: GatewayClientLike,
	discord: DiscordClientLike,
	typing?: TypingPort,
	status?: WorkingStatus,
	log: Pick<Console, "error"> = console,
	speech?: DiscordSpeechPorts,
	track: DiscordTaskTracker = identityDiscordTrack,
	shouldHandle: () => boolean = () => true,
): () => void {
	// One reaction port pair per subscription: the emoji cache and the throttle are
	// only useful across deliveries, and a live adapter has exactly one subscription.
	const reactions = createReactionPorts();
	return gateway.onChatMessage((message) => {
		if (!shouldHandle()) return;
		void track(settleDiscordDelivery(gateway, discord, message, typing, status, reactions, speech)).catch((error) =>
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
	track: DiscordTaskTracker = identityDiscordTrack,
	shouldHandle: () => boolean = () => true,
): () => void {
	if (!gateway.onChatProgress) return () => {};
	return gateway.onChatProgress((progress) => {
		if (!shouldHandle()) return;
		// `final` means the turn stopped working. It arrives even when the turn
		// delivered nothing - a silence token in an open channel - which is the only
		// signal that the temporary status must go. Clearing on delivery alone left
		// one orphaned "working" message per suppressed turn, and the typing hint
		// (begun on engagement, ended only by a delivery) "typing…" for its full
		// 330s cap after every silent turn (집가재, 2026-09-02).
		if (progress.final) typing?.end(progress.origin.conversationId);
		const action = progress.final ? status.clear(progress.origin.conversationId) : status.update(progress);
		void track(action).catch((error) =>
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

const DISCORD_UNRECOVERABLE_CLOSE_CODES = new Set([4004, 4010, 4011, 4012, 4013, 4014]);

/** Starts a production Discord client for one composition-owned generation. */
export async function startDiscordAdapter(
	input: DiscordAdapterInput,
	gateway: OpenGatewayClient,
	gen: Generation,
): Promise<AdapterHandle> {
	const discord = new Client({
		intents: [...new Set([...REQUIRED_INTENTS, ...(input.intents ?? [])])],
		// DM channels are not cached on a cold start; without the Channel partial
		// discord.js drops messageCreate for uncached DMs, silently losing owner DMs.
		// REQUIRED_PARTIALS carries that Channel partial plus the reaction partials,
		// which uncached reaction events need for the same reason.
		partials: REQUIRED_PARTIALS,
	}) as unknown as DiscordAdapterClient;
	return startDiscordAdapterWithClient(input, gateway, gen, discord);
}

/** Test seam for lifecycle faults without opening a Discord websocket. */
export async function startDiscordAdapterWithClient(
	input: DiscordAdapterInput,
	gateway: OpenGatewayClient,
	gen: Generation,
	discord: DiscordAdapterClient,
	log: Pick<Console, "error" | "log"> = console,
): Promise<AdapterHandle> {
	const typing = new TypingIndicator(discord, TYPING_REFRESH_MS, TYPING_MAX_MS, log);
	const status = new WorkingStatus(discord, log);
	const tasks = new Set<Promise<unknown>>();
	let stopped = false;
	let destroyed = false;
	let stopPromise: Promise<void> | undefined;
	let settle: (() => void) | undefined;
	let rejectSettled: ((error: unknown) => void) | undefined;
	let settledDone = false;
	const settled = new Promise<void>((resolve, reject) => {
		settle = resolve;
		rejectSettled = reject;
	});
	// `start()` can throw after rejecting `settled`; retain the rejection for the
	// supervisor without producing an unhandled-rejection warning first.
	void settled.catch(() => {});
	const track: DiscordTaskTracker = <T>(task: Promise<T>): Promise<T> => {
		const tracked = gen.track(task);
		tasks.add(tracked);
		void tracked.finally(() => tasks.delete(tracked)).catch(() => {});
		return tracked;
	};
	const active = (): boolean => !stopped && !gen.signal.aborted;
	const fail = (error: unknown): void => {
		if (!active() || settledDone) return;
		settledDone = true;
		rejectSettled?.(error);
	};
	const link = new DiscordGatewayLink(
		gateway,
		discord,
		input,
		typing,
		status,
		input.recoveryCursorPath,
		() => discord.user,
		undefined,
		discordSpeechPorts(input.voice),
		gen,
	);

	// Transcription makes ingress asynchronous, and two messages in one
	// conversation must not overtake each other while one waits on the network.
	// The gateway serializes turns per origin, but it serializes them in arrival
	// order, so the ordering has to be preserved here, before it hands them over.
	const ingress = new OrderedIngress();
	discord.on("messageCreate", (...args) => {
		if (!active()) return;
		const message = args[0] as DiscordInboundMessage;
		const engagement = decideInbound(message, discord.user);
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
			if (!active()) return;
			// A voice message carries no text at all, so without a transcript the
			// history shows a url and nothing about what was said. Doing this in the
			// runtime rather than the persona is a standing owner instruction.
			const body = withTranscript(rendered, await transcribeIfVoice(message, input.voice));
			if (!active()) return;
			// The modality of the question decides the modality of the answer: a
			// spoken message is answered in voice and text both, without the
			// persona having to ask for it.
			const spoken = firstVoiceMessage(message) !== undefined;
			link.sendInbound(message.id, origin, body, engagement, receivedAt, spoken);
		});
	});
	// A reaction is engagement metadata, never a turn: it goes out on its own verb.
	discord.on("messageReactionAdd", (...args) => {
		if (active())
			link.sendReaction(args[0] as DiscordInboundReaction, args[1] as DiscordReactingUser, "add", discord.user);
	});
	// A REMOVAL means the reactor retracted the signal. It is recorded as its own
	// metadata event rather than erasing the add, because the persona may already
	// have read the add — rewriting history behind it would make its memory of the
	// conversation disagree with what it was told.
	discord.on("messageReactionRemove", (...args) => {
		if (active())
			link.sendReaction(args[0] as DiscordInboundReaction, args[1] as DiscordReactingUser, "remove", discord.user);
	});
	discord.on("interactionCreate", (...args) => {
		if (!active()) return;
		const interaction = args[0] as SlashInteractionLike;
		if (interaction.isChatInputCommand?.()) void handleSlashCommand(interaction, link, log);
	});
	discord.once("ready", () => {
		if (!active()) return;
		log.log("Discord adapter connected.");
		// Slash-command mapping: /new and /reset are first-class Discord commands
		// that route into the gateway's session-reset verbs for the invoking
		// conversation (typing "/new" as chat text never reaches messageCreate).
		void discord.application?.commands
			?.set([
				{ name: "new", description: "Start a fresh persona session in this conversation" },
				{ name: "reset", description: "Reset this conversation's persona session" },
				{ name: "restart", description: "Restart the gateway process (owner only)" },
			])
			.catch((error: unknown) =>
				log.error(
					`Discord slash-command registration failed: ${error instanceof Error ? error.message : String(error)}`,
				),
			);
		void link.recoverMissedMessages();
	});
	discord.on("shardDisconnect", (...args) => {
		if (!active()) return;
		const close = args[0];
		const code =
			typeof close === "object" && close !== null && "code" in close && typeof close.code === "number"
				? close.code
				: undefined;
		if (code !== undefined && DISCORD_UNRECOVERABLE_CLOSE_CODES.has(code)) {
			const error = new Error(`discord_unrecoverable_close code=${code}`);
			log.error(error.message);
			fail(error);
		}
	});
	discord.on("shardReconnecting", () => {
		if (active()) log.error("Discord shard reconnecting.");
	});
	discord.on("shardError", (...args) => {
		if (active()) log.error(`Discord shard error: ${describeError(args[0])}`);
	});
	discord.on("error", (...args) => {
		if (active()) log.error(`Discord client error: ${describeError(args[0])}`);
	});

	const deliveryOff = subscribeDiscordDeliveries(
		gateway,
		discord,
		typing,
		status,
		log,
		discordSpeechPorts(input.voice),
		track,
		active,
	);
	const progressOff = subscribeDiscordProgress(gateway, status, log, typing, track, active);
	const stop = (): Promise<void> => {
		stopPromise ??= (async () => {
			stopped = true;
			deliveryOff();
			progressOff();
			await link.stop();
			typing.dispose();
			await status.dispose();
			await Promise.allSettled([...tasks]);
			if (!destroyed) {
				destroyed = true;
				discord.destroy();
			}
			await gateway.close?.();
			if (!settledDone) {
				settledDone = true;
				settle?.();
			}
		})();
		return stopPromise;
	};

	try {
		log.log("Discord adapter starting.");
		await gateway.open();
	} catch (error) {
		fail(error);
		await stop();
		throw error;
	}
	void discord.login(input.token).then(
		() => {},
		(error: unknown) => {
			if (!active()) return;
			fail(error);
			void stop();
		},
	);
	return { stop, settled };
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
	gateway: Pick<DiscordGatewayLink, "requestInbound">,
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

export class DiscordGatewayLink {
	readonly #inbound = new RecoveryGate();
	#cursors: RecoveryCursorState | undefined;
	#cursorLoads: Promise<void> | undefined;
	#cursorFault: string | undefined;
	#cursorSaves: Promise<void> = Promise.resolve();
	#recovering = false;
	#retryTimer: ReturnType<typeof setTimeout> | undefined;
	#retryAttempt = 0;
	#stopped = false;
	readonly #stopController = new AbortController();
	#activeRecovery: Promise<void> | undefined;

	readonly sleep: (ms: number) => Promise<void>;

	constructor(
		readonly gateway: GatewayClientLike,
		readonly discord: DiscordClientLike,
		readonly input: DiscordAdapterInput,
		readonly typing: TypingPort | undefined,
		readonly status: WorkingStatus | undefined,
		readonly recoveryCursorPath: string,
		readonly getBotUser: () => unknown,
		sleep: ((ms: number) => Promise<void>) | undefined,
		readonly speech: DiscordSpeechPorts | undefined,
		readonly gen: Generation,
	) {
		this.sleep = sleep ?? ((ms) => this.gen.sleep(ms));
		void this.ensureCursors();
	}

	/**
	 * Bounded catch-up for messages missed while the adapter was offline. Recovery
	 * uses the restart-bound channel snapshot only; engagement remains gateway-owned.
	 */
	async recoverMissedMessages(): Promise<void> {
		if (!this.active()) return;
		if (this.#activeRecovery) {
			this.scheduleRecoveryRetry();
			return;
		}
		const pass = this.#recoverMissedMessages();
		this.#activeRecovery = pass;
		try {
			await pass;
		} finally {
			if (this.#activeRecovery === pass) this.#activeRecovery = undefined;
		}
	}

	async #recoverMissedMessages(): Promise<void> {
		const channelIds = this.input.recoveryChannels;
		// Nothing configured means nothing to recover, ever: that is the one early return
		// that must not reschedule.
		if (channelIds.length === 0 || !this.active()) return;
		const botUser = this.getBotUser();
		if (!botUser) {
			// A fired retry that cannot proceed (Discord is not ready) must reschedule itself,
			// otherwise the gap waits for an event that may never arrive.
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
			if (!this.active()) return;
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
				if (!this.active()) return;
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
			if (this.active())
				console.error(
					`Discord recovery pass failed: ${error instanceof Error ? error.message : String(error)}; retrying with backoff.`,
				);
		} finally {
			this.#recovering = false;
			if (this.active()) {
				if (completed) this.#retryAttempt = 0;
				else this.scheduleRecoveryRetry();
			}
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
		if (!this.active()) return false;
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
				if (!this.active()) return "unavailable";
				// Same normalization as live messageCreate: one decision, one origin shape,
				// one gateway verb — recovery never forks engagement semantics.
				const engagement = decideInbound(message, botUser);
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
					if (attempt < RECOVERY_MAX_ATTEMPTS)
						await this.waitForRecovery(RECOVERY_ATTEMPT_BACKOFF_MS * 2 ** (attempt - 1));
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
				`Discord recovery hit the ${RECOVERY_MAX_PAGES}-page bound for channel ${channelId} after ${outcome.delivered} message(s) and ${outcome.skipped} skip(s); ${progress}.`,
			);
			return true;
		}
		if (outcome.delivered > 0 || outcome.skipped > 0 || outcome.discarded > 0) {
			console.log(
				`Discord recovery backfilled ${outcome.delivered} message(s), skipped ${outcome.skipped} and discarded ${outcome.discarded} for channel ${channelId}.`,
			);
		}
		return false;
	}

	/** Re-runs recovery after a backoff so a paused or truncated gap keeps draining. */
	private scheduleRecoveryRetry(): void {
		if (!this.active() || this.#retryTimer) return;
		const delay = Math.min(RECOVERY_RETRY_MAX_MS, RECOVERY_RETRY_BASE_MS * 2 ** Math.min(this.#retryAttempt++, 6));
		console.log(`Discord recovery retrying in ${delay}ms.`);
		const timer = setTimeout(() => {
			this.#retryTimer = undefined;
			if (this.active()) void this.recoverMissedMessages();
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
				this.input.recoveryChannels,
			),
		);
	}

	/** Serialized, fire-and-forget cursor-store write; a failed persist only widens the gap. */
	private persist(next: RecoveryCursorState): void {
		if (!this.active()) return;
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
		if (!this.active()) return;
		const described = describeInboundReaction(reaction, user, botUser, action);
		if (!described) return;
		// A rejected engagement note is metadata, not a link failure: reconnecting
		// here replays every undelivered ledger row and re-arms typing/status for
		// nothing. The generation owns reconnect decisions.
		void this.gateway.request("engagement.reaction", described).catch((error) => {
			if (this.active()) console.error(`Discord engagement.reaction failed: ${describeError(error)}`);
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
		if (!this.active()) return undefined;
		let result: { engaged?: boolean } | undefined;
		const verdict = await this.#inbound.join(messageId, async () => {
			if (!this.active()) return "unavailable";
			try {
				// The gateway acknowledges engagement before running the turn, so typing starts only for
				// turns that will actually produce a reply and never outlives the delivery that clears it.
				result = await this.gateway.request<{ engaged?: boolean }>("chat.send", {
					origin,
					text,
					engagement,
					messageId,
					...(receivedAt ? { receivedAt } : {}),
					...(voice ? { voice: true } : {}),
				});
				// No recovery-watermark write here on purpose: a live message is no evidence that
				// the older messages behind it were ever backfilled (issue #33).
				if (result?.engaged && this.active()) this.typing?.begin(origin.conversationId);
				return "acked";
			} catch (error) {
				if (this.active()) console.error(`Discord gateway request failed: ${describeError(error)}`);
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
			if (!this.active()) {
				failure = "retryable";
				summary = "adapter stopped";
				return "unavailable";
			}
			try {
				await this.gateway.request("chat.send", { origin, text, engagement, messageId });
				return "acked";
			} catch (error) {
				failure = classifyRecoveryFailure(error);
				summary = summarizeRecoveryFailure(error);
				if (this.active()) console.error(`Discord gateway request failed: ${summary}`);
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

	async stop(): Promise<void> {
		this.#stopped = true;
		this.#stopController.abort();
		if (this.#retryTimer) clearTimeout(this.#retryTimer);
		this.#retryTimer = undefined;
		await this.#activeRecovery?.catch(() => {});
		await this.cursorsFlushed;
	}

	private active(): boolean {
		return !this.#stopped && !this.gen.signal.aborted;
	}

	private async waitForRecovery(ms: number): Promise<void> {
		if (!this.active()) return;
		await awaitWithAbort(this.sleep(ms), this.#stopController.signal);
	}
}

function escapeRegExp(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function describeError(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

function awaitWithAbort<T>(task: Promise<T>, signal: AbortSignal): Promise<T> {
	return new Promise((resolve, reject) => {
		const onAbort = () => {
			signal.removeEventListener("abort", onAbort);
			reject(Object.assign(new Error("The operation was aborted"), { name: "AbortError" }));
		};
		if (signal.aborted) {
			void task.catch(() => {});
			onAbort();
			return;
		}
		signal.addEventListener("abort", onAbort, { once: true });
		void task.then(
			(value) => {
				signal.removeEventListener("abort", onAbort);
				resolve(value);
			},
			(error: unknown) => {
				signal.removeEventListener("abort", onAbort);
				reject(error);
			},
		);
	});
}

function abortableGenerationSleep(ms: number, signal: AbortSignal): Promise<void> {
	return new Promise((resolve, reject) => {
		if (signal.aborted) {
			reject(Object.assign(new Error("The operation was aborted"), { name: "AbortError" }));
			return;
		}
		const timer = setTimeout(() => {
			signal.removeEventListener("abort", onAbort);
			resolve();
		}, ms);
		const onAbort = () => {
			clearTimeout(timer);
			signal.removeEventListener("abort", onAbort);
			reject(Object.assign(new Error("The operation was aborted"), { name: "AbortError" }));
		};
		signal.addEventListener("abort", onAbort, { once: true });
	});
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
		void AdapterLock.acquire(adapterHome())
			.then(async (lock) => {
				const home = adapterHome();
				const controller = new AbortController();
				let handle: AdapterHandle | undefined;
				let stopping: Promise<void> | undefined;
				const stop = (): Promise<void> => {
					stopping ??= (async () => {
						controller.abort();
						await handle?.stop();
						await lock.release();
					})();
					return stopping;
				};
				const release = (): void => void stop().finally(() => process.exit(0));
				process.once("SIGINT", release);
				process.once("SIGTERM", release);
				try {
					const config = await loadDiscordAdapterConfig();
					const client = await GajaewayClient.connectSocket(config.gatewaySocket ?? join(home, "gateway.sock"));
					// The temporary socket bridge negotiates before the adapter registers event
					// handlers. S5 deletes it once in-process ports provide replay ordering.
					const gateway: OpenGatewayClient = {
						request: (verb, params) => client.request(verb, params),
						onChatMessage: (handler) => client.on("chat.message", (payload) => handler(payload as ChatMessagePayload)),
						onChatProgress: (handler) =>
							client.on("chat.progress", (payload) => handler(payload as ChatProgressPayload)),
						open: async () => ({ replayed: 0 }),
						close: () => client.close(),
					};
					const gen: Generation = {
						id: 1,
						signal: controller.signal,
						port: gateway,
						track: (task) => task,
						sleep: (ms) => abortableGenerationSleep(ms, controller.signal),
					};
					handle = await startDiscordAdapter(
						{
							token: config.token,
							intents: config.intents,
							voice: config.voice,
							recoveryChannels: Object.keys(config.channels ?? {}),
							recoveryCursorPath: join(home, "adapters", "discord", "recovery-cursor.json"),
						},
						gateway,
						gen,
					);
					await handle.settled;
				} finally {
					await stop();
				}
			})
			.catch((error) => {
				console.error(error instanceof Error ? error.message : String(error));
				process.exitCode = error instanceof AdapterAlreadyRunningError ? USAGE_EXIT_CODE : 1;
			});
	}
}
