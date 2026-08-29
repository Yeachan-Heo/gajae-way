import type {
	ChatMessagePayload,
	ChatProgressPayload,
	EngagementContext,
	OriginRef,
	ReactionAction,
} from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";
import { AttachmentBuilder, Client, GatewayIntentBits, MessageFlags, Partials } from "discord.js";
import { type AttachmentCarrier, describeInboundBody, firstVoiceMessage } from "./attachments";
import { type AuthorLike, resolveDisplayName } from "./author";
import { type LoadedDiscordAdapterConfig, type LoadedDiscordVoiceConfig, loadDiscordAdapterConfig } from "./config";
import { type DiscordMessageOriginShape, discordMessageOrigin } from "./origin";
import {
	type DiscordInboundReaction,
	type DiscordReactingUser,
	describeInboundReaction,
	GuildEmojiResolver,
	ReactionRateLimiter,
	settleDiscordReaction,
} from "./reactions";
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
	const replyTo = resolveReplyContext(message, botId);
	return {
		mentioned: Boolean(message.mentions?.has(botUser) || contentMention),
		group: origin.kind !== "dm",
		authorId: message.author.id,
		...(message.author.bot ? { authorIsBot: true } : {}),
		...(displayName ? { authorName: displayName } : {}),
		...(message.author.username ? { authorHandle: message.author.username } : {}),
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
	// Discord's known permanent target/permission errors have not dispatched a message.
	return !new Set(["10003", "10008", "50001", "50013"]).has(code);
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
 * Renders a long-running turn as one temporary, amended status message per
 * conversation ("working… (2m 05s, 3 tools)") driven by gateway chat.progress
 * events, and removes it when the real reply is delivered. Best-effort only:
 * status failures never compete with delivery.
 */
export class WorkingStatus {
	readonly #discord: DiscordClientLike;
	readonly #log: Pick<Console, "error">;
	readonly #messages = new Map<string, EditableDiscordMessage | "pending">();

	constructor(discord: DiscordClientLike, log: Pick<Console, "error"> = console) {
		this.#discord = discord;
		this.#log = log;
	}

	async update(progress: ChatProgressPayload): Promise<void> {
		if (progress.origin.platform !== "discord") return;
		const conversationId = progress.origin.conversationId;
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

	async clear(conversationId: string): Promise<void> {
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
): () => void {
	if (!gateway.onChatProgress) return () => {};
	return gateway.onChatProgress((progress) => {
		// `final` means the turn stopped working. It arrives even when the turn
		// delivered nothing - a silence token in an open channel - which is the only
		// signal that the temporary status must go. Clearing on delivery alone left
		// one orphaned "working" message per suppressed turn.
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
			])
			.catch((error: unknown) =>
				console.error(
					`Discord slash-command registration failed: ${error instanceof Error ? error.message : String(error)}`,
				),
			);
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
		const result = await gateway.requestInbound(`slash-${interaction.id}`, origin, `/${interaction.commandName}`, {
			mentioned: true,
			group: origin.kind !== "dm",
			authorId: interaction.user.id,
			...(resolveInteractionDisplayName(interaction)
				? { authorName: resolveInteractionDisplayName(interaction) as string }
				: {}),
			...(interaction.user.username ? { authorHandle: interaction.user.username } : {}),
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

class ReconnectingGateway {
	#client: GajaewayClient | undefined;
	#reconnecting = false;
	#attempt = 0;
	#deliveryOff: (() => void) | undefined;
	#progressOff: (() => void) | undefined;
	readonly #inbound = new LruSet();

	constructor(
		readonly socketPath: string,
		readonly discord: DiscordClientLike,
		readonly config: LoadedDiscordAdapterConfig,
		readonly typing?: TypingPort,
		readonly status?: WorkingStatus,
		readonly speech?: DiscordSpeechPorts,
	) {}

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
			this.#progressOff = this.status ? subscribeDiscordProgress(client, this.status) : undefined;
			console.log("Discord adapter connected to gateway.");
			this.monitor(client);
		} catch {
			this.scheduleReconnect();
		}
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
		const described = describeInboundReaction(reaction, user, botUser, action);
		if (!described) return;
		const client = this.#client;
		if (!client) return;
		void client.request("engagement.reaction", described).catch(() => this.scheduleReconnect());
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
		if (!this.#inbound.addIfAbsent(messageId)) return;
		const client = this.#client;
		if (!client) return;
		// The gateway acknowledges engagement before running the turn, so typing starts only for
		// turns that will actually produce a reply and never outlives the delivery that clears it.
		// The platform message id travels with the turn: the gateway keys its durable inbound
		// queue on it, so a replayed or backfilled message is deduped there and not just in the
		// adapter's in-memory set, which does not survive a restart.
		try {
			const result = await client.request<{ engaged?: boolean }>("chat.send", {
				origin,
				text,
				engagement,
				messageId,
				...(receivedAt ? { receivedAt } : {}),
				...(voice ? { voice: true } : {}),
			});
			if (result?.engaged) this.typing?.begin(origin.conversationId);
			return result;
		} catch {
			this.scheduleReconnect();
			return undefined;
		}
	}

	private monitor(client: GajaewayClient): void {
		setTimeout(() => {
			if (this.#client !== client) return;
			void client.request("gateway.status").then(
				() => this.monitor(client),
				() => this.scheduleReconnect(),
			);
		}, 30_000);
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

if (import.meta.main) {
	loadDiscordAdapterConfig()
		.then(startDiscordAdapter)
		.catch((error) => {
			console.error(error instanceof Error ? error.message : String(error));
			process.exitCode = 1;
		});
}
