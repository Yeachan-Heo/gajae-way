import { createHash } from "node:crypto";
import { MemoryChunkLedger, type ChunkLedger } from "../chunk-ledger";
import {
	RpcJournalConsumer,
	type JournalDeliveryProof,
	type RpcJournalEvent,
} from "../../journal-consumer";
import type { JsonRpcClient } from "../../rpc-client";
import type { DiscordUnattributedDelivery } from "./config";
import { formatDiscordOutboundText, splitDiscordMessage, type DiscordMessageReference, type DiscordPlatform } from "./platform";
import { resolveDiscordEgressRoute, validateDiscordRoutes, type DiscordRoute } from "./route";

export const DISCORD_CONSUMER_ID = "gajaeway-discord";
export const DEFAULT_DISCORD_CLAIM_TTL_MS = 5_000;
export const DEFAULT_DISCORD_READ_WAIT_MS = 1_000;

/** One egress effect, settled durably only by consumer.commit. */
export interface DiscordOutboxItem {
	readonly cursor: string;
	readonly seq: string;
	readonly channelId: string;
	readonly surfaceId: string;
	readonly text: string;
	readonly dedupeKey: string;
	readonly nonce: string;
	readonly replyTo?: DiscordMessageReference;
}

export interface DiscordOutboxHooks {
	beforeSend?(item: DiscordOutboxItem): void | Promise<void>;
	afterSendBeforeCommit?(item: DiscordOutboxItem, platformMessageId: string): void | Promise<void>;
}

export interface DiscordOutboxOptions {
	readonly rpc: JsonRpcClient;
	readonly platform: DiscordPlatform;
	readonly routes: readonly DiscordRoute[];
	readonly unattributedDelivery: DiscordUnattributedDelivery;
	readonly unattributedRoute?: DiscordRoute;
	readonly consumerId?: string;
	readonly claimTtlMs?: number;
	readonly readWaitMs?: number;
	readonly idleDelayMs?: number;
	readonly retryDelayMs?: number;
	readonly now?: () => number;
	readonly hooks?: DiscordOutboxHooks;
	/** Durable record of confirmed chunks so a replayed multi-chunk set cannot double-post. */
	readonly chunkLedger?: ChunkLedger;
	/** Resolves the most recent inbound trigger for reply threading on a surface. */
	replyReferenceForSurface?(surfaceId: string): DiscordMessageReference | undefined;
	onError?(error: Error): void;
	onDiagnostic?(message: string): void;
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
	readonly surface_id?: string;
}

interface ParsedAssistantMessagePayload {
	readonly text: string;
	readonly surfaceId?: string;
	readonly surfaceIdMalformed: boolean;
}

interface SuppressedDelivery {
	readonly kind: "suppressed";
	readonly seq: string;
	readonly dedupeKey: string;
	readonly reason: string;
}

interface SendDelivery {
	readonly kind: "send";
	readonly item: DiscordOutboxItem;
}

type ResolvedDelivery = SendDelivery | SuppressedDelivery;

/**
 * Pure RPC consumer loop. It holds no cursor, nonce map, or journal state
 * across process boundaries: the gateway checkpoint is the sole recovery
 * authority and Discord receives the deterministic nonce on every retry.
 */
export class DiscordOutbox {
	readonly #platform: DiscordPlatform;
	readonly #routes: readonly DiscordRoute[];
	readonly #unattributedDelivery: DiscordUnattributedDelivery;
	readonly #unattributedRoute: DiscordRoute | undefined;
	readonly #replyReferenceForSurface: ((surfaceId: string) => DiscordMessageReference | undefined) | undefined;
	readonly #hooks: DiscordOutboxHooks;
	readonly #chunkLedger: ChunkLedger;
	readonly #consumer: RpcJournalConsumer;
	readonly #idleDelayMs: number;
	readonly #retryDelayMs: number;
	readonly #onError: (error: Error) => void;
	readonly #onDiagnostic: (message: string) => void;

	constructor(options: DiscordOutboxOptions) {
		try {
			validateDiscordRoutes(options.routes);
		} catch (error) {
			throw new DiscordOutboxError(error instanceof Error ? error.message : String(error));
		}
		if (options.unattributedDelivery !== "owner-dm" && options.unattributedDelivery !== "suppress") {
			throw new DiscordOutboxError("Discord unattributed delivery policy must be owner-dm or suppress.");
		}
		if (options.unattributedDelivery === "owner-dm") {
			if (!options.unattributedRoute || options.unattributedRoute.kind !== "dm") {
				throw new DiscordOutboxError("Discord owner-dm unattributed delivery requires a configured dm route.");
			}
			if (!options.routes.some(route => route.channelId === options.unattributedRoute?.channelId && route.surfaceId === options.unattributedRoute?.surfaceId)) {
				throw new DiscordOutboxError("Discord owner-dm unattributed route must appear in the configured route table.");
			}
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
		this.#routes = [...options.routes];
		this.#unattributedDelivery = options.unattributedDelivery;
		this.#unattributedRoute = options.unattributedRoute;
		this.#replyReferenceForSurface = options.replyReferenceForSurface;
		this.#hooks = options.hooks ?? {};
		this.#chunkLedger = options.chunkLedger ?? new MemoryChunkLedger();
		this.#idleDelayMs = boundedInteger(options.idleDelayMs ?? 50, "idleDelayMs", 0, 60_000);
		this.#retryDelayMs = boundedInteger(options.retryDelayMs ?? 250, "retryDelayMs", 0, 60_000);
		this.#onError = options.onError ?? (error => console.error(`gajaeway-discord outbox failed: ${error.message}`));
		this.#onDiagnostic = options.onDiagnostic ?? (message => console.error(`gajaeway-discord outbox: ${message}`));
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
		const delivery = this.resolveDelivery(event, cursor);
		if (delivery.kind === "suppressed") {
			safeDiscordDiagnostic(this.#onDiagnostic, `suppressed assistant_message ${delivery.seq}: ${delivery.reason}`);
			return { seq: delivery.seq, dedupe_key: delivery.dedupeKey };
		}
		const item = delivery.item;
		await this.#hooks.beforeSend?.(item);
		const chunks = splitDiscordMessage(item.text);
		if (chunks.chunks.length === 0) throw new DiscordOutboxError(`assistant_message event ${item.seq} has no deliverable text.`);
		if (chunks.truncated) {
			safeDiscordDiagnostic(this.#onDiagnostic, `assistant_message ${item.seq} exceeded the Discord chunk bound; sent a truncated presentation.`);
		}
		let firstMessageId: string | undefined;
		for (const [index, chunk] of chunks.chunks.entries()) {
			throwIfAborted(signal);
			const nonce = discordChunkWireNonce(item.dedupeKey, index);
			// A replayed chunk set must not re-post chunks that already landed.
			// Deterministic nonces alone cannot guarantee that: Discord's
			// enforce_nonce deduplication is time-bounded, so an outage longer than
			// its window would duplicate. The durable ledger makes the skip decision
			// independent of elapsed time.
			const ledgerKey = `${item.dedupeKey}:chunk:${index}`;
			const alreadySent = this.#chunkLedger.recorded(ledgerKey);
			if (alreadySent) {
				firstMessageId ??= alreadySent;
				continue;
			}
			const platformMessageId = await this.#platform.send(item.channelId, chunk, nonce, { formatted: true, dedupeKey: item.dedupeKey, ...(index === 0 && item.replyTo ? { replyTo: item.replyTo } : {}) });
			if (!platformMessageId) throw new DiscordOutboxError(`Discord returned no message id for journal event ${item.seq} chunk ${index}.`);
			this.#chunkLedger.record(ledgerKey, platformMessageId);
			firstMessageId ??= platformMessageId;
		}
		if (!firstMessageId) throw new DiscordOutboxError(`Discord returned no message id for journal event ${item.seq}.`);
		await this.#hooks.afterSendBeforeCommit?.(item, firstMessageId);
		throwIfAborted(signal);
		return { seq: item.seq, platform_msg_id: firstMessageId, dedupe_key: item.dedupeKey };
	}

	private resolveDelivery(event: RpcJournalEvent, cursor: string): ResolvedDelivery {
		if (event.kind !== "assistant_message") throw new DiscordOutboxError(`Unexpected event kind in Discord outbox: ${event.kind}.`);
		const seq = sequenceString(event.seq);
		const payload = finalizedAssistantMessagePayload(event.payload);
		if (!payload || !payload.text.trim()) throw new DiscordOutboxError(`assistant_message event ${seq} has no deliverable text.`);

		if (payload.surfaceId) {
			const route = resolveDiscordEgressRoute(this.#routes, payload.surfaceId);
			if (route) return { kind: "send", item: eventToOutboxItem(seq, payload.text, cursor, route, payload.surfaceId, this.#replyReferenceForSurface?.(payload.surfaceId)) };
		}

		const reason = payload.surfaceIdMalformed
			? "payload surface_id is malformed"
			: payload.surfaceId
				? `no configured route matches surface_id ${JSON.stringify(payload.surfaceId)}`
				: "payload has no surface_id";
		if (this.#unattributedDelivery === "owner-dm") {
			const route = this.#unattributedRoute;
			if (!route) throw new DiscordOutboxError("Discord owner-dm unattributed route is unavailable.");
			return { kind: "send", item: eventToOutboxItem(seq, payload.text, cursor, route, route.surfaceId, this.#replyReferenceForSurface?.(route.surfaceId)) };
		}
		const dedupeSurfaceId = payload.surfaceId ?? "unattributed";
		return { kind: "suppressed", seq, dedupeKey: discordDedupeKey(dedupeSurfaceId, seq), reason };
	}
}

export function discordDedupeKey(surfaceId: string, seq: string): string {
	return `gajaeway-discord:${surfaceId}:${seq}`;
}

/**
 * Discord rejects nonces longer than 25 characters (50035 NONCE_TYPE_TOO_LONG),
 * so the wire nonce is a deterministic 24-hex-character (96-bit) digest of the
 * full dedupe key. At one billion events, the birthday-bound collision chance is
 * about 6.3e-12; the full dedupe key remains the durable consumer.commit proof.
 * Retries and post-restart replays of the same journal event keep producing the
 * identical nonce, which lets `enforce_nonce` suppress a prompt duplicate. That
 * deduplication is TIME-BOUNDED on Discord's side, so the durable chunk ledger -
 * not the nonce - is what makes a delayed replay safe.
 */
export function discordWireNonce(dedupeKey: string): string {
	return createHash("sha256").update(dedupeKey).digest("hex").slice(0, 24);
}

/** Stable per-chunk nonce derived from the durable event key and chunk index. */
export function discordChunkWireNonce(dedupeKey: string, index: number): string {
	if (!Number.isSafeInteger(index) || index < 0) throw new DiscordOutboxError("Discord chunk index must be a non-negative safe integer.");
	return index === 0 ? discordWireNonce(dedupeKey) : discordWireNonce(`${dedupeKey}:chunk:${index}`);
}

function eventToOutboxItem(
	seq: string,
	text: string,
	cursor: string,
	route: DiscordRoute,
	surfaceId: string,
	replyTo?: DiscordMessageReference,
): DiscordOutboxItem {
	const dedupeKey = discordDedupeKey(surfaceId, seq);
	return {
		cursor,
		seq,
		channelId: route.channelId,
		surfaceId,
		text: formatDiscordOutboundText(text),
		dedupeKey,
		nonce: discordWireNonce(dedupeKey),
		...(replyTo === undefined ? {} : { replyTo }),
	};
}

/** Rejects streaming/legacy shapes: egress must consume a terminal persisted message only. */
export function assistantMessageText(payload: unknown): string {
	return finalizedAssistantMessagePayload(payload)?.text ?? "";
}

function finalizedAssistantMessagePayload(payload: unknown): ParsedAssistantMessagePayload | undefined {
	if (!isRecord(payload) || payload.finalized !== true || typeof payload.text !== "string") return undefined;
	if (payload.surface_id === undefined) return { text: payload.text, surfaceIdMalformed: false };
	if (typeof payload.surface_id !== "string" || !payload.surface_id.trim()) return { text: payload.text, surfaceIdMalformed: true };
	return { text: payload.text, surfaceId: payload.surface_id.trim(), surfaceIdMalformed: false };
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

function safeDiscordDiagnostic(onDiagnostic: (message: string) => void, message: string): void {
	try {
		onDiagnostic(message);
	} catch {
		// Diagnostics are best-effort and must not poison ordered delivery.
	}
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
