import type { ChatMessagePayload, ChatProgressPayload, EngagementContext, OriginRef } from "@gajaeway/protocol";
import { GajaewayClient } from "@gajaeway/sdk";
import { Client, GatewayIntentBits } from "discord.js";
import { type LoadedDiscordAdapterConfig, loadDiscordAdapterConfig } from "./config";
import { type DiscordMessageOriginShape, discordMessageOrigin } from "./origin";

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
];

export interface GatewayClientLike {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
	onChatMessage(handler: (message: ChatMessagePayload) => void): () => void;
	onChatProgress?(handler: (progress: ChatProgressPayload) => void): () => void;
	close?(): Promise<void>;
}

export interface DiscordTextChannelLike {
	send(text: string): Promise<unknown>;
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
	readonly author: { readonly id: string; readonly bot?: boolean };
	readonly mentions?: { has(user: unknown): boolean };
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
	return {
		mentioned: Boolean(message.mentions?.has(botUser) || contentMention),
		group: origin.kind !== "dm",
		authorId: message.author.id,
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
): Promise<void> {
	if (message.origin.platform !== "discord" || !message.deliveryId) return;
	const deliveryId = message.deliveryId;
	try {
		const channel = await discord.channels.fetch(message.origin.conversationId);
		if (!isDiscordTextChannel(channel)) {
			throw Object.assign(new Error(`Discord channel ${message.origin.conversationId} cannot receive messages`), {
				code: 10003,
			});
		}
		const text = message.duplicateWarning ? `[recovered - may be a duplicate] ${message.text}` : message.text;
		for (const chunk of chunkDiscordMessage(text)) await channel.send(chunk);
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

export function subscribeDiscordDeliveries(
	gateway: GatewayClientLike,
	discord: DiscordClientLike,
	typing?: TypingPort,
	status?: WorkingStatus,
	log: Pick<Console, "error"> = console,
): () => void {
	return gateway.onChatMessage((message) => {
		void settleDiscordDelivery(gateway, discord, message, typing, status).catch((error) =>
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
	const gateway = new ReconnectingGateway(
		config.gatewaySocket ?? defaultGatewaySocket(),
		discord,
		config,
		typing,
		status,
	);
	discord.on("messageCreate", (message) => {
		const engagement = decideInbound(message, discord.user, config.channels);
		if (!engagement) return;
		gateway.sendInbound(message.id as string, discordMessageOrigin(message), message.content, engagement);
	});
	discord.once("ready", () => console.log("Discord adapter connected."));
	console.log("Discord adapter starting.");
	await gateway.connect();
	await discord.login(config.token);
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
	) {}

	async connect(): Promise<void> {
		try {
			const client = await GajaewayClient.connectSocket(this.socketPath);
			this.#client = client;
			this.#attempt = 0;
			this.#deliveryOff?.();
			this.#deliveryOff = subscribeDiscordDeliveries(client, this.discord, this.typing, this.status);
			this.#progressOff?.();
			this.#progressOff = this.status ? subscribeDiscordProgress(client, this.status) : undefined;
			console.log("Discord adapter connected to gateway.");
			this.monitor(client);
		} catch {
			this.scheduleReconnect();
		}
	}

	sendInbound(messageId: string, origin: OriginRef, text: string, engagement: EngagementContext): void {
		if (!this.#inbound.addIfAbsent(messageId)) return;
		const client = this.#client;
		if (!client) return;
		// The gateway acknowledges engagement before running the turn, so typing starts only for
		// turns that will actually produce a reply and never outlives the delivery that clears it.
		// The platform message id travels with the turn: the gateway keys its durable inbound
		// queue on it, so a replayed or backfilled message is deduped there and not just in the
		// adapter's in-memory set, which does not survive a restart.
		void client.request<{ engaged?: boolean }>("chat.send", { origin, text, engagement, messageId }).then(
			(result) => {
				if (result?.engaged) this.typing?.begin(origin.conversationId);
			},
			() => this.scheduleReconnect(),
		);
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
