import {
	RpcJournalConsumer,
	type JournalDeliveryProof,
	type RpcJournalEvent,
} from "../../journal-consumer";
import type { JsonRpcClient } from "../../rpc-client";
import type { DiscordPlatform } from "./platform";
import type { DiscordRoute } from "./route";

export const DISCORD_CONSUMER_ID = "gajaeway-discord";
export const DEFAULT_DISCORD_CLAIM_TTL_MS = 5_000;
export const DEFAULT_DISCORD_READ_WAIT_MS = 1_000;

/** One egress effect, settled durably only by consumer.commit. */
export interface DiscordOutboxItem {
	readonly cursor: string;
	readonly seq: string;
	readonly text: string;
	readonly dedupeKey: string;
	readonly nonce: string;
}

export interface DiscordOutboxHooks {
	beforeSend?(item: DiscordOutboxItem): void | Promise<void>;
	afterSendBeforeCommit?(item: DiscordOutboxItem, platformMessageId: string): void | Promise<void>;
}

export interface DiscordOutboxOptions {
	readonly rpc: JsonRpcClient;
	readonly platform: DiscordPlatform;
	readonly route: DiscordRoute;
	readonly consumerId?: string;
	readonly claimTtlMs?: number;
	readonly readWaitMs?: number;
	readonly idleDelayMs?: number;
	readonly retryDelayMs?: number;
	readonly now?: () => number;
	readonly hooks?: DiscordOutboxHooks;
	onError?(error: Error): void;
}

export type DiscordOutboxRunResult = "sent" | "idle" | "claim_held";

export class DiscordOutboxError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "DiscordOutboxError";
	}
}

/** The only assistant-message payload accepted for Discord egress. */
export interface FinalizedAssistantMessagePayload {
	readonly finalized: true;
	readonly text: string;
	readonly message_id?: string;
	readonly timestamp?: number;
}

/**
 * Pure RPC consumer loop. It holds no cursor, nonce map, or journal state
 * across process boundaries: the gateway checkpoint is the sole recovery
 * authority and Discord receives the deterministic nonce on every retry.
 */
export class DiscordOutbox {
	readonly #platform: DiscordPlatform;
	readonly #route: DiscordRoute;
	readonly #hooks: DiscordOutboxHooks;
	readonly #consumer: RpcJournalConsumer;
	readonly #idleDelayMs: number;
	readonly #retryDelayMs: number;
	readonly #onError: (error: Error) => void;

	constructor(options: DiscordOutboxOptions) {
		if (!options.route.surfaceId.trim() || !/^\d+$/.test(options.route.channelId)) {
			throw new DiscordOutboxError("Discord outbox requires a valid configured route.");
		}
		const consumerId = options.consumerId ?? DISCORD_CONSUMER_ID;
		const claimTtlMs = boundedInteger(
			options.claimTtlMs ?? DEFAULT_DISCORD_CLAIM_TTL_MS,
			"claimTtlMs",
			5_000,
			600_000,
		);
		const readWaitMs = boundedInteger(options.readWaitMs ?? DEFAULT_DISCORD_READ_WAIT_MS, "readWaitMs", 0, 60_000);
		if (readWaitMs >= claimTtlMs) throw new DiscordOutboxError("readWaitMs must be shorter than claimTtlMs.");
		this.#platform = options.platform;
		this.#route = options.route;
		this.#hooks = options.hooks ?? {};
		this.#idleDelayMs = boundedInteger(options.idleDelayMs ?? 50, "idleDelayMs", 0, 60_000);
		this.#retryDelayMs = boundedInteger(options.retryDelayMs ?? 250, "retryDelayMs", 0, 60_000);
		this.#onError = options.onError ?? (error => console.error(`gajaeway-discord outbox failed: ${error.message}`));
		this.#consumer = new RpcJournalConsumer({
			rpc: options.rpc,
			consumerId,
			claimTtlMs,
			readWaitMs,
			kinds: ["assistant_message"],
			now: options.now,
			errorFactory: message => new DiscordOutboxError(message),
			gapError: gap =>
				new DiscordOutboxError(
					`Discord consumer checkpoint ${gap.checkpoint} is behind journal retention; operator resync is required at ${gap.resyncCursor}.`,
				),
			publish: async (event, context) => await this.publish(event, context.cursor, context.signal),
		});
	}

	async run(signal: AbortSignal): Promise<void> {
		while (!signal.aborted) {
			try {
				const result = await this.runOnce(signal);
				if (result !== "sent") await sleep(this.#idleDelayMs, signal);
			} catch (error) {
				if (signal.aborted) return;
				this.#onError(asError(error));
				await sleep(this.#retryDelayMs, signal);
			}
		}
	}

	async runOnce(signal?: AbortSignal): Promise<DiscordOutboxRunResult> {
		const result = await this.#consumer.runOnce(signal);
		return result === "published" ? "sent" : result;
	}

	private async publish(event: RpcJournalEvent, cursor: string, signal?: AbortSignal): Promise<JournalDeliveryProof> {
		const item = eventToOutboxItem(event, this.#route, cursor);
		await this.#hooks.beforeSend?.(item);
		throwIfAborted(signal);
		const platformMessageId = await this.#platform.send(this.#route.channelId, item.text, item.nonce);
		if (!platformMessageId) throw new DiscordOutboxError(`Discord returned no message id for journal event ${item.seq}.`);
		await this.#hooks.afterSendBeforeCommit?.(item, platformMessageId);
		throwIfAborted(signal);
		return { seq: item.seq, platform_msg_id: platformMessageId, dedupe_key: item.dedupeKey };
	}
}

export function discordDedupeKey(surfaceId: string, seq: string): string {
	return `gajaeway-discord:${surfaceId}:${seq}`;
}

function eventToOutboxItem(event: RpcJournalEvent, route: DiscordRoute, cursor: string): DiscordOutboxItem {
	if (event.kind !== "assistant_message") throw new DiscordOutboxError(`Unexpected event kind in Discord outbox: ${event.kind}.`);
	const seq = sequenceString(event.seq);
	const text = assistantMessageText(event.payload);
	if (!text.trim()) throw new DiscordOutboxError(`assistant_message event ${seq} has no deliverable text.`);
	const dedupeKey = discordDedupeKey(route.surfaceId, seq);
	return { cursor, seq, text, dedupeKey, nonce: dedupeKey };
}

/** Rejects streaming/legacy shapes: egress must consume a terminal persisted message only. */
export function assistantMessageText(payload: unknown): string {
	if (!isRecord(payload) || payload.finalized !== true || typeof payload.text !== "string") return "";
	return payload.text;
}

function sequenceString(seq: string | number): string {
	if (typeof seq === "string" && /^\d+$/.test(seq)) return seq;
	if (typeof seq === "number" && Number.isSafeInteger(seq) && seq >= 0) return String(seq);
	throw new DiscordOutboxError("Journal event sequence must be an unsigned safe integer.");
}

function boundedInteger(value: number, name: string, minimum: number, maximum: number): number {
	if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new DiscordOutboxError(`${name} must be an integer in ${minimum}..=${maximum}.`);
	}
	return value;
}

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (!signal?.aborted) return;
	const error = new Error("Discord outbox stopped.");
	error.name = "AbortError";
	throw error;
}

function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
	if (milliseconds === 0 || signal.aborted) return Promise.resolve();
	return new Promise(resolve => {
		const timeout = setTimeout(finish, milliseconds);
		const onAbort = () => finish();
		function finish(): void {
			clearTimeout(timeout);
			signal.removeEventListener("abort", onAbort);
			resolve();
		}
		signal.addEventListener("abort", onAbort, { once: true });
	});
}

function asError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
