import type {
	ChatContextEntry,
	ChatContextParams,
	ChatContextResult,
	ChatMessagePayload,
	EngagementContext,
} from "@gajaeway/protocol";
import { originKey } from "@gajaeway/protocol";
import { LruSet } from "../lru-set";

/** Settled delivery ids remembered per router, bounding re-entrancy memory. */
const SETTLED_DELIVERY_MEMORY = 10_000;

/** Discord's platform limit for one text message. */
export const DISCORD_TEXT_MESSAGE_LIMIT = 2_000;

/** Default cap used only when a caller does not provide the voice configuration. */
export const DEFAULT_VOICE_CONTEXT_CAP = { maxItems: 20, maxCharsPerItem: 200 } as const;

export interface DiscordDeliveryGateway {
	request<T = unknown>(verb: string, params?: unknown): Promise<T>;
}

export interface DiscordDeliveryChannel {
	send(
		payload:
			| string
			| {
					readonly content: string;
					readonly reply?: { readonly messageReference: string; readonly failIfNotExists?: boolean };
			  },
	): Promise<unknown>;
}

export interface DiscordDeliveryClient {
	readonly channels: { fetch(id: string): Promise<unknown> };
}

export interface DeliveryTypingPort {
	end(conversationId: string): void;
}

export interface DeliveryStatusPort {
	clear(conversationId: string): Promise<void> | void;
}

export interface VoiceDeliveryModality {
	resolve(turnId: string): "voice" | undefined;
	recordPart?(turnId: string, atMs: number, final?: boolean): boolean;
}

export interface VoicePlaybackCompleted {
	readonly outcome: "completed";
	readonly audioPlayed?: boolean;
}

export interface VoicePlaybackBargeIn {
	readonly outcome: "barge_in";
	readonly audioPlayed?: boolean;
	/** Unspoken response text to carry into the next turn. */
	readonly remainder?: string;
	/** Absolute character cut when the playback port reports one instead of remainder. */
	readonly charIndex?: number;
	readonly truncationMs?: number;
	readonly atMs?: number;
	readonly at?: string;
	readonly engagement?: EngagementContext;
	/** A playback implementation may provide a fully formed context row. */
	readonly contextEntry?: ChatContextEntry;
}

export interface VoicePlaybackFailed {
	readonly outcome: "failed";
	readonly error?: unknown;
	readonly audioPlayed?: boolean;
}

export type VoicePlaybackOutcome = VoicePlaybackCompleted | VoicePlaybackBargeIn | VoicePlaybackFailed;

/** Audio is deliberately behind this narrow port so router tests never need a provider or audio. */
export interface VoicePlaybackPort {
	play(message: ChatMessagePayload): Promise<VoicePlaybackOutcome>;
}

export interface VoiceDeliveryRouterOptions {
	readonly modality?: VoiceDeliveryModality;
	readonly playback?: VoicePlaybackPort;
	readonly submitContext: (params: ChatContextParams) => Promise<ChatContextResult | undefined>;
	readonly contextCap?: Readonly<{ maxItems: number; maxCharsPerItem: number }>;
	readonly clock?: { now(): number };
}

export type VoiceDeliveryRouter = (
	gateway: DiscordDeliveryGateway,
	discord: DiscordDeliveryClient,
	message: ChatMessagePayload,
	typing?: DeliveryTypingPort,
	status?: DeliveryStatusPort,
) => Promise<void>;

const PERMANENT_DISCORD_FAILURE_CODES = new Set(["10003", "10008", "50001", "50013"]);

class VoicePlaybackError extends Error {
	readonly audioPlayed: boolean;

	constructor(error: unknown, audioPlayed: boolean) {
		super(error instanceof Error ? error.message : String(error));
		this.name = "VoicePlaybackError";
		this.audioPlayed = audioPlayed;
		if (error instanceof Error && error.stack !== undefined) this.stack = error.stack;
	}
}

function isDiscordDeliveryChannel(value: unknown): value is DiscordDeliveryChannel {
	return typeof value === "object" && value !== null && "send" in value && typeof value.send === "function";
}

function failureCode(error: unknown): string {
	if (typeof error !== "object" || error === null || !("code" in error)) return "";
	const code = error.code;
	return typeof code === "string" || typeof code === "number" ? String(code) : "";
}

function deliveryFailureIsAmbiguous(error: unknown): boolean {
	return !PERMANENT_DISCORD_FAILURE_CODES.has(failureCode(error));
}

function errorAudioPlayed(error: unknown): boolean {
	if (typeof error !== "object" || error === null || !("audioPlayed" in error)) return false;
	return error.audioPlayed === true;
}

export function chunkDiscordMessage(text: string): readonly string[] {
	if (text.length === 0) return [""];
	const chunks: string[] = [];
	for (let offset = 0; offset < text.length; offset += DISCORD_TEXT_MESSAGE_LIMIT) {
		chunks.push(text.slice(offset, offset + DISCORD_TEXT_MESSAGE_LIMIT));
	}
	return chunks;
}

function duplicateWarningText(message: ChatMessagePayload): string {
	return message.duplicateWarning ? `[recovered - may be a duplicate] ${message.text}` : message.text;
}

async function fetchTextChannel(
	discord: DiscordDeliveryClient,
	conversationId: string,
): Promise<DiscordDeliveryChannel> {
	const channel = await discord.channels.fetch(conversationId);
	if (!isDiscordDeliveryChannel(channel)) {
		throw Object.assign(new Error(`Discord channel ${conversationId} cannot receive messages`), { code: "10003" });
	}
	return channel;
}

async function sendTextChunks(
	discord: DiscordDeliveryClient,
	message: ChatMessagePayload,
	text: string,
): Promise<void> {
	const channel = await fetchTextChannel(discord, message.origin.conversationId);
	const chunks = chunkDiscordMessage(text);
	for (let index = 0; index < chunks.length; index += 1) {
		const chunk = chunks[index];
		if (chunk === undefined) continue;
		if (index === 0 && message.replyToMessageId !== undefined) {
			await channel.send({
				content: chunk,
				reply: { messageReference: message.replyToMessageId, failIfNotExists: false },
			});
		} else {
			await channel.send(chunk);
		}
	}
}

function assistantEngagement(): EngagementContext {
	return { mentioned: false, group: true, authorId: "assistant" };
}

function interruptionEntry(
	message: ChatMessagePayload,
	interruption: VoicePlaybackBargeIn,
	nowMs: number,
): ChatContextEntry {
	if (interruption.contextEntry !== undefined) return interruption.contextEntry;
	const atMs = interruption.atMs ?? nowMs;
	const text =
		interruption.remainder ??
		(interruption.charIndex === undefined ? message.text : message.text.slice(Math.max(0, interruption.charIndex)));
	const at = interruption.at ?? new Date(atMs).toISOString();
	return {
		messageId: `voice:${message.origin.conversationId}:assistant:${atMs}`,
		text,
		engagement: interruption.engagement ?? assistantEngagement(),
		at,
	};
}

function playbackQueueTask(
	queues: Map<string, { tail: Promise<void> }>,
	key: string,
	task: () => Promise<void>,
): Promise<void> {
	const queue = queues.get(key) ?? { tail: Promise.resolve() };
	queues.set(key, queue);
	const run = queue.tail.then(task, task);
	queue.tail = run.then(
		() => undefined,
		() => undefined,
	);
	return run;
}

/**
 * Creates the single Discord delivery settlement choke point. Text delivery remains the default;
 * audio is selected only after a known voice modality has been resolved and redelivery has been
 * excluded. Each origin owns one serial playback queue, while delivery ids are idempotent within
 * the router lifetime.
 */
export function createVoiceDeliveryRouter(options: VoiceDeliveryRouterOptions): VoiceDeliveryRouter {
	const inflight = new Set<string>();
	// A long-lived router settles ids for the life of the gateway connection, so this
	// memory is bounded rather than an ever-growing Set.
	const settled = new LruSet(SETTLED_DELIVERY_MEMORY);
	const queues = new Map<string, { tail: Promise<void> }>();
	const clock = options.clock ?? { now: () => Date.now() };
	const contextCap = options.contextCap ?? DEFAULT_VOICE_CONTEXT_CAP;

	const settleText = async (
		gateway: DiscordDeliveryGateway,
		discord: DiscordDeliveryClient,
		message: ChatMessagePayload,
	): Promise<void> => {
		await sendTextChunks(discord, message, duplicateWarningText(message));
		await gateway.request("delivery.confirm", { deliveryId: message.deliveryId });
	};

	const settleVoice = async (
		gateway: DiscordDeliveryGateway,
		discord: DiscordDeliveryClient,
		message: ChatMessagePayload,
	): Promise<void> => {
		if (options.playback === undefined) throw new Error("voice playback is unavailable");
		let played = false;
		try {
			const result = await options.playback.play(message);
			played = result.audioPlayed ?? true;
			if (result.outcome === "failed") {
				throw new VoicePlaybackError(result.error ?? new Error("voice playback failed"), played);
			}
			if (result.outcome === "barge_in") {
				await sendTextChunks(discord, message, message.text);
				const entry = interruptionEntry(message, result, clock.now());
				const recorded = await options.submitContext({ origin: message.origin, entries: [entry], cap: contextCap });
				if (recorded === undefined) throw new Error("barge-in context was not recorded");
			}
			await gateway.request("delivery.confirm", { deliveryId: message.deliveryId });
		} catch (error) {
			if (played && !errorAudioPlayed(error)) throw new VoicePlaybackError(error, true);
			throw error;
		}
	};

	return async (gateway, discord, message, typing, status): Promise<void> => {
		if (message.origin.platform !== "discord" || !message.deliveryId) return;
		const deliveryId = message.deliveryId;
		if (inflight.has(deliveryId) || !settled.addIfAbsent(deliveryId)) return;
		inflight.add(deliveryId);
		let voicePath = false;
		let audioPlayed = false;
		try {
			const modality = message.turnId === "" ? undefined : options.modality?.resolve(message.turnId);
			voicePath = message.redelivered !== true && modality === "voice";
			if (!voicePath) {
				await settleText(gateway, discord, message);
			} else {
				options.modality?.recordPart?.(message.turnId, clock.now(), message.final);
				const key = originKey(message.origin);
				await playbackQueueTask(queues, key, async () => {
					try {
						await settleVoice(gateway, discord, message);
					} catch (error) {
						audioPlayed = audioPlayed || errorAudioPlayed(error);
						throw error;
					}
				});
			}
		} catch (error) {
			audioPlayed = audioPlayed || errorAudioPlayed(error);
			await gateway
				.request("delivery.fail", {
					deliveryId,
					reason: error instanceof Error ? error.message : String(error),
					ambiguous: voicePath ? audioPlayed : deliveryFailureIsAmbiguous(error),
				})
				.catch(() => undefined);
		} finally {
			inflight.delete(deliveryId);
			try {
				await status?.clear(message.origin.conversationId);
			} finally {
				typing?.end(message.origin.conversationId);
			}
		}
	};
}
