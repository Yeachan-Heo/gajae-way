import { createHash } from "node:crypto";
import { type JournalDeliveryProof, RpcJournalConsumer, type RpcJournalEvent } from "../../journal-consumer";
import type { JsonRpcClient } from "../../rpc-client";
import type { AdapterPlatform } from "./protocol";

export const DEFAULT_CLAIM_TTL_MS = 5_000;
export const DEFAULT_READ_WAIT_MS = 1_000;

/**
 * Bound on the in-process sent-seq map.
 *
 * Egress claims at most 100 events per read, so this covers an order of
 * magnitude more than any single in-flight claim window plus its retries.
 * Reaching it at all means `consumer.commit` has stalled, which the gateway
 * already surfaces as an adapter-disconnected alert.
 */
export const SENT_SEQ_LIMIT = 1_024;

/**
 * Journal kinds an adapter settles by default.
 *
 * Only kinds the gateway actually emits may be requested: `main.events.read`
 * rejects an unknown kind filter outright, so listing a not-yet-shipped kind
 * here would break every adapter. Alert kinds are opted in via `eventKinds`
 * once the observability slice adds them to the daemon.
 */
export const EGRESS_EVENT_KINDS = ["assistant_message"] as const;

export interface EgressItem {
	readonly cursor: string;
	readonly seq: string;
	readonly kind: string;
	readonly text: string;
	readonly dedupeKey: string;
	readonly nonce: string;
}

export interface EgressHooks {
	beforeSend?(item: EgressItem): void | Promise<void>;
	afterSendBeforeCommit?(item: EgressItem, platformMsgId: string | undefined): void | Promise<void>;
}

export interface EgressOptions {
	readonly rpc: JsonRpcClient;
	readonly platform: AdapterPlatform;
	readonly consumerId: string;
	readonly surfaceId: string;
	readonly chatId: string;
	readonly claimTtlMs?: number;
	readonly readWaitMs?: number;
	readonly idleDelayMs?: number;
	readonly retryDelayMs?: number;
	readonly now?: () => number;
	readonly hooks?: EgressHooks;
	/** Renders a non-transcript event, or returns undefined to settle silently. */
	readonly renderEvent?: (event: RpcJournalEvent) => string | undefined;
	/** Overrides the settled journal kinds; must only name kinds the gateway emits. */
	readonly eventKinds?: readonly string[];
	onError?(error: Error): void;
}

export type EgressRunResult = "sent" | "idle" | "claim_held";

export class AdapterEgressError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AdapterEgressError";
	}
}

/**
 * Shared egress loop for every adapter.
 *
 * Holds no DURABLE delivery state - the gateway checkpoint remains the sole
 * recovery authority. It does keep a bounded in-process map of sequences whose
 * platform send has already returned but whose commit has not yet been
 * observed, so a reconnect, a re-claim, or a claim-lease renewal cannot re-send
 * within one process lifetime. Across a process crash, or with two overlapping
 * adapter processes, an `at_least_once` platform may still duplicate; that
 * residual is real and is not papered over.
 */
export class AdapterEgress {
	readonly #platform: AdapterPlatform;
	readonly #chatId: string;
	readonly #surfaceId: string;
	readonly #consumerId: string;
	readonly #hooks: EgressHooks;
	readonly #consumer: RpcJournalConsumer;
	readonly #idleDelayMs: number;
	readonly #retryDelayMs: number;
	readonly #onError: (error: Error) => void;
	readonly #renderEvent: (event: RpcJournalEvent) => string | undefined;
	/** seq -> proof for sends that returned but are not yet committed. */
	readonly #sentSeqs = new Map<string, JournalDeliveryProof>();

	constructor(options: EgressOptions) {
		if (!options.surfaceId.trim()) throw new AdapterEgressError("Adapter egress requires a surface id.");
		if (!options.chatId.trim()) throw new AdapterEgressError("Adapter egress requires a chat id.");
		const claimTtlMs = boundedInteger(options.claimTtlMs ?? DEFAULT_CLAIM_TTL_MS, "claimTtlMs", 5_000, 600_000);
		const readWaitMs = boundedInteger(options.readWaitMs ?? DEFAULT_READ_WAIT_MS, "readWaitMs", 0, 60_000);
		if (readWaitMs >= claimTtlMs) throw new AdapterEgressError("readWaitMs must be shorter than claimTtlMs.");
		this.#platform = options.platform;
		this.#chatId = options.chatId;
		this.#surfaceId = options.surfaceId;
		this.#consumerId = options.consumerId;
		this.#hooks = options.hooks ?? {};
		this.#idleDelayMs = boundedInteger(options.idleDelayMs ?? 50, "idleDelayMs", 0, 60_000);
		this.#retryDelayMs = boundedInteger(options.retryDelayMs ?? 250, "retryDelayMs", 0, 60_000);
		this.#onError = options.onError ?? ((error) => console.error(`adapter egress failed: ${error.message}`));
		this.#renderEvent = options.renderEvent ?? defaultRenderEvent;
		this.#consumer = new RpcJournalConsumer({
			rpc: options.rpc,
			consumerId: options.consumerId,
			claimTtlMs,
			readWaitMs,
			kinds: [...(options.eventKinds ?? EGRESS_EVENT_KINDS)],
			now: options.now,
			errorFactory: (message) => new AdapterEgressError(message),
			gapError: (gap) =>
				new AdapterEgressError(
					`Adapter consumer checkpoint ${gap.checkpoint} is behind journal retention; operator resync is required at ${gap.resyncCursor}.`,
				),
			publish: async (event, context) => await this.publish(event, context.cursor, context.signal),
			afterCommit: (seqs) => {
				// Committed durably: the in-process guard is no longer needed.
				for (const seq of seqs) this.#sentSeqs.delete(seq);
			},
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

	async runOnce(signal?: AbortSignal): Promise<EgressRunResult> {
		const result = await this.#consumer.runOnce(signal);
		return result === "published" ? "sent" : result;
	}

	/** Test/observability accessor for the in-process guard size. */
	get pendingSentSeqCount(): number {
		return this.#sentSeqs.size;
	}

	private async publish(event: RpcJournalEvent, cursor: string, signal?: AbortSignal): Promise<JournalDeliveryProof> {
		const seq = sequenceString(event.seq);
		const already = this.#sentSeqs.get(seq);
		// A reconnect or re-claim must not re-send what this process already sent.
		if (already) return already;

		const text = this.#renderEvent(event);
		const dedupeKey = adapterDedupeKey(this.#consumerId, this.#surfaceId, seq);
		const item: EgressItem = {
			cursor,
			seq,
			kind: event.kind,
			text: text ?? "",
			dedupeKey,
			nonce: wireNonce(dedupeKey),
		};

		// Nothing renderable (for example an alert this adapter must not show):
		// still settle it, or this consumer's checkpoint stalls forever.
		if (text === undefined || !text.trim()) {
			const proof: JournalDeliveryProof = { seq, dedupe_key: dedupeKey };
			this.remember(seq, proof);
			return proof;
		}

		await this.#hooks.beforeSend?.(item);
		throwIfAborted(signal);
		const sent = await this.#platform.send(
			this.#chatId,
			item.text,
			this.#platform.dedupe === "platform_nonce" ? { nonce: item.nonce } : {},
		);
		if (this.#platform.dedupe === "platform_nonce" && !sent.platformMsgId) {
			throw new AdapterEgressError(`Platform returned no message id for journal event ${seq}.`);
		}
		await this.#hooks.afterSendBeforeCommit?.(item, sent.platformMsgId);
		throwIfAborted(signal);
		const proof: JournalDeliveryProof = {
			seq,
			dedupe_key: dedupeKey,
			...(sent.platformMsgId === undefined ? {} : { platform_msg_id: sent.platformMsgId }),
		};
		this.remember(seq, proof);
		return proof;
	}

	private remember(seq: string, proof: JournalDeliveryProof): void {
		if (this.#sentSeqs.size >= SENT_SEQ_LIMIT) {
			// Oldest-first eviction. An evicted seq degrades to the platform's
			// declared dedupe mode, which is a graceful fallback rather than a
			// correctness break.
			const oldest = this.#sentSeqs.keys().next();
			if (!oldest.done) this.#sentSeqs.delete(oldest.value);
		}
		this.#sentSeqs.set(seq, proof);
	}
}

export function adapterDedupeKey(consumerId: string, surfaceId: string, seq: string): string {
	return `${consumerId}:${surfaceId}:${seq}`;
}

/**
 * Some platforms cap nonce length (Discord rejects >25 characters with 50035
 * NONCE_TYPE_TOO_LONG), so the wire nonce is a deterministic 24-hex-character
 * (96-bit) digest of the full dedupe key. Retries and post-restart replays of
 * the same journal event keep producing the identical nonce, preserving
 * server-side deduplication. The full dedupe key remains the commit proof.
 */
export function wireNonce(dedupeKey: string): string {
	return createHash("sha256").update(dedupeKey).digest("hex").slice(0, 24);
}

/** Rejects streaming/legacy shapes: egress consumes a terminal persisted message only. */
export function assistantMessageText(payload: unknown): string {
	if (!isRecord(payload) || payload.finalized !== true || typeof payload.text !== "string") return "";
	return payload.text;
}

function defaultRenderEvent(event: RpcJournalEvent): string | undefined {
	if (event.kind === "assistant_message") {
		const text = assistantMessageText(event.payload);
		if (!text.trim())
			throw new AdapterEgressError(`assistant_message event ${String(event.seq)} has no deliverable text.`);
		return text;
	}
	// Alerts are settled but not rendered unless an adapter opts in.
	return undefined;
}

function sequenceString(seq: string | number): string {
	if (typeof seq === "string" && /^\d+$/.test(seq)) return seq;
	if (typeof seq === "number" && Number.isSafeInteger(seq) && seq >= 0) return String(seq);
	throw new AdapterEgressError("Journal event sequence must be an unsigned safe integer.");
}

function boundedInteger(value: number, name: string, minimum: number, maximum: number): number {
	if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new AdapterEgressError(`${name} must be an integer in ${minimum}..=${maximum}.`);
	}
	return value;
}

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (!signal?.aborted) return;
	const error = new Error("Adapter egress stopped.");
	error.name = "AbortError";
	throw error;
}

function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
	if (milliseconds === 0 || signal.aborted) return Promise.resolve();
	return new Promise((resolve) => {
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
