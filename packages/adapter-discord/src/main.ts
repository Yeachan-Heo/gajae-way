import type { ChatMessagePayload, ChatProgressPayload, EngagementContext, OriginRef } from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";
import { shouldAdmitTurn } from "@gajaeway/voice-core";
import { Client, GatewayIntentBits } from "discord.js";
import { resolveDisplayName } from "./author";
import { type LoadedDiscordAdapterConfig, loadDiscordAdapterConfig } from "./config";
import { LruSet } from "./lru-set";
import { type DiscordMessageOriginShape, discordMessageOrigin } from "./origin";
import { handleVoiceCommand, voiceOriginKey } from "./voice/commands";
import { createVoiceDeliveryRouter, type VoiceDeliveryRouter } from "./voice/router";
import { createVoiceRuntime, type VoiceRuntime } from "./voice/runtime";

// Discord clears the typing hint after ~10s, so refresh inside that window while a turn is running.
const TYPING_REFRESH_MS = 7_000;
// Hard ceiling above the gateway's 300s gjc turn timeout: a lost turn must not type forever.
const TYPING_MAX_MS = 330_000;
const REQUIRED_INTENTS = [
	GatewayIntentBits.Guilds,
	GatewayIntentBits.GuildMessages,
	GatewayIntentBits.MessageContent,
	GatewayIntentBits.DirectMessages,
	// Voice rooms: receiving speech and noticing the bot being moved or kicked both
	// depend on voice-state events, so the intent is required for the voice interface.
	GatewayIntentBits.GuildVoiceStates,
];

export interface GatewayClientLike {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void;
	onChatProgress?(handler: (progress: ChatProgressPayload) => void): () => void;
	close?(): Promise<void>;
}

export interface DiscordTextChannelLike {
	send(
		payload: string | { content: string; reply?: { messageReference: string; failIfNotExists?: boolean } },
	): Promise<unknown>;
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

export interface DiscordInboundMessage extends DiscordMessageOriginShape {
	readonly id: string;
	readonly content: string;
	readonly author: {
		readonly id: string;
		readonly bot?: boolean;
		readonly username?: string;
		/** Account-wide display name, shown when a guild has no nickname. */
		readonly globalName?: string | null;
	};
	readonly mentions?: { has(user: unknown): boolean };
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

export function engagementForMessage(message: DiscordInboundMessage, botUser: unknown): EngagementContext {
	const origin = discordMessageOrigin(message);
	const botId = typeof botUser === "object" && botUser !== null && "id" in botUser ? String(botUser.id) : "";
	const contentMention = botId !== "" && new RegExp(`<@!?${escapeRegExp(botId)}>`).test(message.content);
	const displayName = resolveAuthorDisplayName(message);
	return {
		mentioned: Boolean(message.mentions?.has(botUser) || contentMention),
		group: origin.kind !== "dm",
		authorId: message.author.id,
		...(displayName ? { authorName: displayName } : {}),
		...(message.author.username ? { authorHandle: message.author.username } : {}),
		...(message.channel.name ? { channelLabel: `#${message.channel.name}` } : {}),
		...(message.guild?.name ? { serverLabel: message.guild.name } : {}),
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

export { LruSet } from "./lru-set";
export { chunkDiscordMessage } from "./voice/router";

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

/**
 * The one delivery settlement path. Text delivery is the default and audio is only ever
 * chosen after a known voice modality resolves and redelivery has been excluded, so this
 * function is never forked: voice support is added by passing the voice-aware router.
 */
export async function settleDiscordDelivery(
	gateway: Pick<GatewayClientLike, "request">,
	discord: DiscordClientLike,
	message: ChatMessagePayload,
	typing?: TypingPort,
	status?: WorkingStatus,
	router?: VoiceDeliveryRouter,
): Promise<void> {
	await (router ?? createTextOnlyDeliveryRouter())(gateway, discord, message, typing, status);
}

/**
 * Settlement for a process with no voice runtime: no modality ever resolves, so every
 * delivery takes the text path. Text-only deployments keep exactly their old behavior.
 */
function createTextOnlyDeliveryRouter(): VoiceDeliveryRouter {
	return createVoiceDeliveryRouter({ submitContext: async () => undefined });
}

export function subscribeDiscordDeliveries(
	gateway: GatewayClientLike,
	discord: DiscordClientLike,
	typing?: TypingPort,
	status?: WorkingStatus,
	log: Pick<Console, "error"> = console,
	router?: VoiceDeliveryRouter,
): () => void {
	// One router per subscription: its in-flight and settled-id memory is scoped to the
	// gateway connection, which is what makes double-confirm and re-speaking impossible.
	const settle = router ?? createTextOnlyDeliveryRouter();
	return gateway.onChatMessage((message) => {
		void settleDiscordDelivery(gateway, discord, message, typing, status, settle).catch((error) =>
			log.error(
				`Discord delivery settlement request failed: ${error instanceof Error ? error.message : String(error)}`,
			),
		);
	});
}

export function subscribeDiscordProgress(
	gateway: GatewayClientLike,
	status: WorkingStatus,
	log: Pick<Console, "error"> = console,
): () => void {
	if (!gateway.onChatProgress) return () => {};
	return gateway.onChatProgress((progress) => {
		void status
			.update(progress)
			.catch((error) =>
				log.error(`Discord working status update failed: ${error instanceof Error ? error.message : String(error)}`),
			);
	});
}

export async function startDiscordAdapter(config: LoadedDiscordAdapterConfig): Promise<void> {
	const discord = new Client({ intents: [...new Set([...REQUIRED_INTENTS, ...(config.intents ?? [])])] });
	const typing = new TypingIndicator(discord);
	const status = new WorkingStatus(discord);
	// Voice is opt-in. A configured-but-unusable credential fails startup here rather
	// than degrading to a silently text-only bot.
	const voice = createVoiceRuntime(config, discord);
	const gateway = new ReconnectingGateway(
		config.gatewaySocket ?? defaultGatewaySocket(),
		discord,
		config,
		typing,
		status,
		voice,
	);
	discord.on("messageCreate", (message) => {
		const engagement = decideInbound(message, discord.user, config.channels);
		if (!engagement) return;
		gateway.sendInbound(message.id as string, discordMessageOrigin(message), message.content, engagement);
	});
	discord.on("interactionCreate", (interaction) => {
		if (!interaction.isChatInputCommand()) return;
		if (voice && interaction.commandName === "voice") {
			void handleVoiceCommand(interaction as never, voice.sessions);
			return;
		}
		void handleSlashCommand(interaction, gateway);
	});
	if (voice) {
		// The bot being moved or removed, and the room emptying out, are both voice-state
		// events: the lifecycle owner decides which of them ends the session.
		discord.on("voiceStateUpdate", (previous, next) => {
			const channelId = previous.channelId ?? next.channelId;
			if (!channelId) return;
			const session = voice.sessions.get(voiceOriginKey(channelId));
			if (!session) return;
			void session.handleVoiceStateChange({
				userId: next.id ?? previous.id,
				oldChannelId: previous.channelId,
				newChannelId: next.channelId,
				members: [...(next.channel?.members?.values() ?? [])].map((member) => ({
					id: member.id,
					bot: member.user?.bot === true,
				})),
			});
		});
		for (const signal of ["SIGINT", "SIGTERM"] as const) {
			process.once(signal, () => {
				void voice.closeAll("shutdown");
			});
		}
	}
	discord.once("ready", () => {
		console.log("Discord adapter connected.");
		// Slash-command mapping: /new and /reset are first-class Discord commands
		// that route into the gateway's session-reset verbs for the invoking
		// conversation (typing "/new" as chat text never reaches messageCreate).
		void discord.application?.commands
			.set([
				{ name: "new", description: "Start a fresh persona session in this conversation" },
				{ name: "reset", description: "Reset this conversation's persona session" },
				...(voice
					? [
							{
								name: "voice",
								description: "Join or leave this server's voice conversation",
								options: [
									{ type: 1, name: "join", description: "Join your current voice channel" },
									{ type: 1, name: "leave", description: "Leave the voice channel" },
								],
							},
						]
					: []),
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
		const result = await gateway.requestInbound(
			`slash-${interaction.id}`,
			origin,
			`/${interaction.commandName}`,
			{
				mentioned: true,
				group: origin.kind !== "dm",
				authorId: interaction.user.id,
				...(resolveInteractionDisplayName(interaction)
					? { authorName: resolveInteractionDisplayName(interaction) as string }
					: {}),
				...(interaction.user.username ? { authorHandle: interaction.user.username } : {}),
			},
			// The session verbs reply without running a turn, so they never terminate and
			// must not mark the origin busy.
			{ admit: false },
		);
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

/**
 * Exported for tests: reconnect behavior is a real contract, including the voice
 * teardown that must happen when the gateway socket is lost.
 */
export class ReconnectingGateway {
	#client: GajaewayClient | undefined;
	#reconnecting = false;
	#attempt = 0;
	#deliveryOff: (() => void) | undefined;
	#progressOff: (() => void) | undefined;
	#turnEndOff: (() => void) | undefined;
	readonly #inbound = new LruSet();

	constructor(
		readonly socketPath: string,
		readonly discord: DiscordClientLike,
		readonly config: LoadedDiscordAdapterConfig,
		readonly typing?: TypingPort,
		readonly status?: WorkingStatus,
		readonly voice?: VoiceRuntime,
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
				this.#voiceRouter(client),
			);
			this.#turnEndOff?.();
			this.#turnEndOff = this.voice ? this.#subscribeTurnEnd(client) : undefined;
			this.#progressOff?.();
			this.#progressOff = this.status ? subscribeDiscordProgress(client, this.status) : undefined;
			console.log("Discord adapter connected to gateway.");
			this.monitor(client);
		} catch {
			this.scheduleReconnect();
		}
	}

	sendInbound(messageId: string, origin: OriginRef, text: string, engagement: EngagementContext): void {
		void this.requestInbound(messageId, origin, text, engagement);
	}

	/**
	 * Like sendInbound but reports the gateway's decision to the caller, including the
	 * assigned turn id.
	 *
	 * The turn id is what makes an origin's busy state knowable: it is recorded in the
	 * voice turn book for BOTH modalities, so speech arriving during an in-flight text
	 * turn becomes unread context instead of a second turn. Slash-command paths pass
	 * `admit: false` because they answer without running a turn and so never terminate.
	 */
	async requestInbound(
		messageId: string,
		origin: OriginRef,
		text: string,
		engagement: EngagementContext,
		options: { readonly admit?: boolean; readonly modality?: "voice" | "text" } = {},
	): Promise<{ engaged?: boolean; turnId?: string | null } | undefined> {
		if (!this.#inbound.addIfAbsent(messageId)) return;
		const client = this.#client;
		if (!client) return;
		// The gateway acknowledges engagement before running the turn, so typing starts only for
		// turns that will actually produce a reply and never outlives the delivery that clears it.
		// The platform message id travels with the turn: the gateway keys its durable inbound
		// queue on it, so a replayed or backfilled message is deduped there and not just in the
		// adapter's in-memory set, which does not survive a restart.
		try {
			const result = await client.request<{ engaged?: boolean; turnId?: string | null }>("chat.send", {
				origin,
				text,
				engagement,
				messageId,
			});
			if (result?.engaged) this.typing?.begin(origin.conversationId);
			this.#recordTurn(messageId, origin, result, options);
			return result;
		} catch {
			this.scheduleReconnect();
			return undefined;
		}
	}

	/**
	 * A voice-aware router when a room can exist, and the plain text router otherwise.
	 * Modality is resolved per turn id, so a text turn is never spoken.
	 */
	#voiceRouter(client: GajaewayClient): VoiceDeliveryRouter {
		const voice = this.voice;
		if (!voice) return createTextOnlyDeliveryRouter();
		return createVoiceDeliveryRouter({
			modality: {
				resolve: (turnId) => voice.resolveModality(turnId),
				recordPart: (turnId, atMs, final) => voice.recordDeliveryPart(turnId, atMs, final),
			},
			// Playback belongs to the room that owns the delivery's conversation. When that
			// room is gone the router falls back to text rather than speaking into nothing.
			playback: {
				play: async (message) => {
					const playback = voice.playbackFor(message.origin.conversationId);
					if (playback === undefined) throw new Error("no live voice room owns this delivery");
					return playback.play(message);
				},
			},
			submitContext: (params) => client.contextBatch(params),
		});
	}

	/** The turn's terminal event is what releases the origin so speech can start a new turn. */
	#subscribeTurnEnd(client: GajaewayClient): () => void {
		return client.onTurnEnd((payload) => {
			this.voice?.sessions.get(voiceOriginKey(payload.origin.conversationId))?.settleTurn(payload.turnId);
		});
	}

	/** Only a turn the gateway actually accepted, and that will terminate, is tracked. */
	#recordTurn(
		messageId: string,
		origin: OriginRef,
		result: { engaged?: boolean; turnId?: string | null } | undefined,
		options: { readonly admit?: boolean; readonly modality?: "voice" | "text" },
	): void {
		if (options.admit === false) return;
		const session = this.voice?.sessions.get(voiceOriginKey(origin.conversationId));
		if (!session || !shouldAdmitTurn({ engaged: result?.engaged, turnId: result?.turnId })) return;
		session.admitTurn({
			messageId,
			turnId: result?.turnId as string,
			modality: options.modality ?? "text",
			acceptedAtMs: Date.now(),
		});
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
		// A voice room outlives the socket it speaks through, so losing the gateway must
		// close it: keeping the audio path open would transcribe (and bill) speech that
		// can no longer become a turn.
		void this.voice?.closeAll("gateway_lost");
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
