import { RpcResponseError, rpcResult, type JsonRpcClient } from "../../rpc-client";
import type { DiscordPlatform } from "./platform";
import type { DiscordRoute } from "./route";

export const DISCORD_CONSUMER_ID = "way-discord";
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

interface ConsumerClaim {
	readonly claim_id: string;
	readonly cursor: string;
	readonly expires_at: number;
}

interface EventRead {
	readonly events: readonly EventFrame[];
	readonly next_cursor: string;
	readonly gap?: { readonly missing_from: string; readonly missing_to: string; readonly resync_cursor: string };
}

interface EventFrame {
	readonly seq: string | number;
	readonly kind: string;
	readonly payload: unknown;
}

interface DeliveryProof {
	readonly seq: string;
	readonly platform_msg_id: string;
	readonly dedupe_key: string;
}

/**
 * Pure RPC consumer loop. It holds no cursor, nonce map, or journal state
 * across process boundaries: the gateway checkpoint is the sole recovery
 * authority and Discord receives the deterministic nonce on every retry.
 */
export class DiscordOutbox {
	readonly #rpc: JsonRpcClient;
	readonly #platform: DiscordPlatform;
	readonly #route: DiscordRoute;
	readonly #consumerId: string;
	readonly #claimTtlMs: number;
	readonly #readWaitMs: number;
	readonly #idleDelayMs: number;
	readonly #retryDelayMs: number;
	readonly #now: () => number;
	readonly #hooks: DiscordOutboxHooks;
	readonly #onError: (error: Error) => void;

	constructor(options: DiscordOutboxOptions) {
		if (!options.route.surfaceId.trim() || !/^\d+$/.test(options.route.channelId)) {
			throw new DiscordOutboxError("Discord outbox requires a valid configured route.");
		}
		this.#rpc = options.rpc;
		this.#platform = options.platform;
		this.#route = options.route;
		this.#consumerId = options.consumerId ?? DISCORD_CONSUMER_ID;
		this.#claimTtlMs = boundedInteger(
			options.claimTtlMs ?? DEFAULT_DISCORD_CLAIM_TTL_MS,
			"claimTtlMs",
			5_000,
			600_000,
		);
		this.#readWaitMs = boundedInteger(options.readWaitMs ?? DEFAULT_DISCORD_READ_WAIT_MS, "readWaitMs", 0, 60_000);
		if (this.#readWaitMs >= this.#claimTtlMs) {
			throw new DiscordOutboxError("readWaitMs must be shorter than claimTtlMs.");
		}
		this.#idleDelayMs = boundedInteger(options.idleDelayMs ?? 50, "idleDelayMs", 0, 60_000);
		this.#retryDelayMs = boundedInteger(options.retryDelayMs ?? 250, "retryDelayMs", 0, 60_000);
		this.#now = options.now ?? Date.now;
		this.#hooks = options.hooks ?? {};
		this.#onError = options.onError ?? (error => console.error(`way-discord outbox failed: ${error.message}`));
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
		throwIfAborted(signal);
		let claim: ConsumerClaim;
		try {
			claim = parseClaim(
				rpcResult<unknown>(
					await this.#rpc.request("consumer.claim", { consumer_id: this.#consumerId, claim_ttl_ms: this.#claimTtlMs }, { signal }),
					"consumer.claim",
				),
			);
		} catch (error) {
			if (error instanceof RpcResponseError && error.code === 1601) return "claim_held";
			throw error;
		}

		try {
			let readCursor = claim.cursor;
			let readFromCheckpoint = true;
			for (;;) {
				throwIfAborted(signal);
				const readParams = {
					...(readFromCheckpoint ? { consumer_id: this.#consumerId } : { cursor: readCursor }),
					limit: 100,
					wait_ms: this.#readWaitMs,
					kinds: ["assistant_message"],
				};
				const read = parseEventRead(
					rpcResult<unknown>(
						await this.#rpc.request(
							"main.events.read",
							readParams,
							{ signal, timeoutMs: this.#readWaitMs + 2_000 },
						),
						"main.events.read",
					),
				);
				readFromCheckpoint = false;
				if (read.gap) {
					throw new DiscordOutboxError(
						`Discord consumer checkpoint ${claim.cursor} is behind journal retention; operator resync is required at ${read.gap.resync_cursor}.`,
					);
				}
				if (read.events.length > 0) {
					const proofs: DeliveryProof[] = [];
					for (const event of read.events) {
						const item = eventToOutboxItem(event, this.#route, read.next_cursor);
						await this.#hooks.beforeSend?.(item);
						throwIfAborted(signal);
						const platformMessageId = await this.#platform.send(this.#route.channelId, item.text, item.nonce);
						if (!platformMessageId) throw new DiscordOutboxError(`Discord returned no message id for journal event ${item.seq}.`);
						await this.#hooks.afterSendBeforeCommit?.(item, platformMessageId);
						throwIfAborted(signal);
						proofs.push({ seq: item.seq, platform_msg_id: platformMessageId, dedupe_key: item.dedupeKey });
					}
					await this.commit(claim, read.next_cursor, proofs, signal);
					return "sent";
				}

				if (read.next_cursor === readCursor || this.nearClaimExpiry(claim)) {
					// Same-cursor commits are valid and release a claim without advancing
					// it. This never turns an unconfirmed effect into a checkpoint.
					await this.commit(claim, claim.cursor, [], signal);
					return "idle";
				}
				// Filtered reads can safely skip unrelated events only in this volatile
				// process loop. A later confirmed assistant send commits the resulting
				// cursor atomically; a crash restarts from the server checkpoint.
				readCursor = read.next_cursor;
			}
		} catch (error) {
			if (!signal?.aborted) await this.releaseUnadvancedClaim(claim);
			throw error;
		}
	}

	private nearClaimExpiry(claim: ConsumerClaim): boolean {
		return this.#now() + this.#readWaitMs + 250 >= claim.expires_at;
	}

	private async commit(claim: ConsumerClaim, cursor: string, proofs: readonly DeliveryProof[], signal?: AbortSignal): Promise<void> {
		rpcResult<unknown>(
			await this.#rpc.request(
				"consumer.commit",
				{
					consumer_id: this.#consumerId,
					claim_id: claim.claim_id,
					cursor,
					proofs,
				},
				{ signal },
			),
			"consumer.commit",
		);
	}

	private async releaseUnadvancedClaim(claim: ConsumerClaim): Promise<void> {
		try {
			await this.commit(claim, claim.cursor, []);
		} catch {
			// A real process crash has the same result: the short server-side claim
			// expiry fences the next adapter instance. Never advance after failure.
		}
	}
}

export function discordDedupeKey(surfaceId: string, seq: string): string {
	return `way-discord:${surfaceId}:${seq}`;
}

function eventToOutboxItem(event: EventFrame, route: DiscordRoute, cursor: string): DiscordOutboxItem {
	if (event.kind !== "assistant_message") throw new DiscordOutboxError(`Unexpected event kind in Discord outbox: ${event.kind}.`);
	const seq = sequenceString(event.seq);
	const text = assistantMessageText(event.payload);
	if (!text.trim()) throw new DiscordOutboxError(`assistant_message event ${seq} has no deliverable text.`);
	const dedupeKey = discordDedupeKey(route.surfaceId, seq);
	return { cursor, seq, text, dedupeKey, nonce: dedupeKey };
}

/** Extracts the finalized text shapes emitted by the v1 main-session host. */
export function assistantMessageText(payload: unknown): string {
	if (typeof payload === "string") return payload;
	if (!isRecord(payload)) return "";
	for (const key of ["text", "content", "message"]) {
		const value = payload[key];
		if (typeof value === "string") return value;
		if (isRecord(value)) {
			for (const nestedKey of ["text", "content", "delta"]) {
				if (typeof value[nestedKey] === "string") return value[nestedKey] as string;
			}
		}
	}
	const assistantEvent = payload.assistantMessageEvent;
	if (isRecord(assistantEvent)) {
		for (const key of ["text", "content", "delta"]) {
			if (typeof assistantEvent[key] === "string") return assistantEvent[key] as string;
		}
	}
	return "";
}

function parseClaim(value: unknown): ConsumerClaim {
	if (!isRecord(value) || typeof value.claim_id !== "string" || typeof value.cursor !== "string" || typeof value.expires_at !== "number") {
		throw new DiscordOutboxError("consumer.claim returned an invalid response.");
	}
	return { claim_id: value.claim_id, cursor: value.cursor, expires_at: value.expires_at };
}

function parseEventRead(value: unknown): EventRead {
	if (!isRecord(value) || !Array.isArray(value.events) || typeof value.next_cursor !== "string") {
		throw new DiscordOutboxError("main.events.read returned an invalid response.");
	}
	const events: EventFrame[] = [];
	for (const event of value.events) {
		if (!isRecord(event) || (typeof event.seq !== "number" && typeof event.seq !== "string") || typeof event.kind !== "string") {
			throw new DiscordOutboxError("main.events.read returned an invalid event.");
		}
		events.push({ seq: event.seq, kind: event.kind, payload: event.payload });
	}
	let gap: EventRead["gap"];
	if (value.gap !== undefined) {
		if (!isRecord(value.gap) || typeof value.gap.missing_from !== "string" || typeof value.gap.missing_to !== "string" || typeof value.gap.resync_cursor !== "string") {
			throw new DiscordOutboxError("main.events.read returned an invalid retention gap.");
		}
		gap = {
			missing_from: value.gap.missing_from,
			missing_to: value.gap.missing_to,
			resync_cursor: value.gap.resync_cursor,
		};
	}
	return { events, next_cursor: value.next_cursor, ...(gap ? { gap } : {}) };
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
