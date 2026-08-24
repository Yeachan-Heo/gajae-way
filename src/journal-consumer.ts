import { type JsonRpcClient, RpcResponseError, rpcResult } from "./rpc-client";

export interface RpcJournalEvent {
	readonly seq: string | number;
	readonly ts?: number;
	readonly kind: string;
	readonly payload: unknown;
}

export interface JournalDeliveryProof {
	readonly seq: string;
	readonly platform_msg_id?: string;
	readonly dedupe_key: string;
}

export interface JournalRetentionGap {
	readonly missingFrom: string;
	readonly missingTo: string;
	readonly resyncCursor: string;
}

export interface JournalPublicationContext {
	readonly cursor: string;
	readonly signal?: AbortSignal;
}

export interface RpcJournalConsumerOptions {
	readonly rpc: JsonRpcClient;
	readonly consumerId: string;
	readonly claimTtlMs: number;
	readonly readWaitMs: number;
	readonly kinds: readonly string[];
	readonly publish: (event: RpcJournalEvent, context: JournalPublicationContext) => Promise<JournalDeliveryProof>;
	/** Invoked only after `consumer.commit` durably settles the listed sequences. */
	readonly afterCommit?: (seqs: readonly string[]) => void;
	readonly now?: () => number;
	/** Release an unadvanced claim when a caller intentionally aborts. */
	readonly releaseOnAbort?: boolean;
	readonly errorFactory?: (message: string) => Error;
	readonly gapError?: (
		input: JournalRetentionGap & { readonly consumerId: string; readonly checkpoint: string },
	) => Error;
}

export type RpcJournalConsumerRunResult = "published" | "idle" | "claim_held";

interface ConsumerClaim {
	readonly claimId: string;
	readonly cursor: string;
	readonly expiresAt: number;
}

interface EventRead {
	readonly events: readonly RpcJournalEvent[];
	readonly nextCursor: string;
	readonly gap?: JournalRetentionGap;
}

export class RpcJournalConsumerError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "RpcJournalConsumerError";
	}
}

export class RpcJournalRetentionGapError extends RpcJournalConsumerError {
	readonly consumerId: string;
	readonly checkpoint: string;
	readonly gap: JournalRetentionGap;

	constructor(consumerId: string, checkpoint: string, gap: JournalRetentionGap) {
		super(
			`Consumer ${consumerId} checkpoint ${checkpoint} is behind journal retention; operator resync is required at ${gap.resyncCursor}.`,
		);
		this.name = "RpcJournalRetentionGapError";
		this.consumerId = consumerId;
		this.checkpoint = checkpoint;
		this.gap = gap;
	}
}

/**
 * Shared pure-RPC journal delivery loop. It owns no cursor or durable delivery
 * state: publication must resolve to a proof before the gateway checkpoint can
 * advance through consumer.commit.
 */
export class RpcJournalConsumer {
	readonly #rpc: JsonRpcClient;
	readonly #consumerId: string;
	readonly #claimTtlMs: number;
	readonly #readWaitMs: number;
	readonly #kinds: readonly string[];
	readonly #publish: RpcJournalConsumerOptions["publish"];
	readonly #afterCommit: RpcJournalConsumerOptions["afterCommit"];
	readonly #now: () => number;
	readonly #releaseOnAbort: boolean;
	readonly #errorFactory: (message: string) => Error;
	readonly #gapError: NonNullable<RpcJournalConsumerOptions["gapError"]>;

	constructor(options: RpcJournalConsumerOptions) {
		if (!options.consumerId.trim()) throw new RpcJournalConsumerError("Journal consumer_id must not be empty.");
		if (!Array.isArray(options.kinds) || options.kinds.length === 0 || options.kinds.some((kind) => !kind.trim())) {
			throw new RpcJournalConsumerError("Journal consumer kinds must contain at least one non-empty kind.");
		}
		if (new Set(options.kinds).size !== options.kinds.length) {
			throw new RpcJournalConsumerError("Journal consumer kinds must not contain duplicates.");
		}
		this.#rpc = options.rpc;
		this.#consumerId = options.consumerId;
		this.#claimTtlMs = boundedInteger(options.claimTtlMs, "claimTtlMs", 5_000, 600_000);
		this.#readWaitMs = boundedInteger(options.readWaitMs, "readWaitMs", 0, 60_000);
		if (this.#readWaitMs >= this.#claimTtlMs)
			throw new RpcJournalConsumerError("readWaitMs must be shorter than claimTtlMs.");
		this.#kinds = [...options.kinds];
		this.#publish = options.publish;
		this.#afterCommit = options.afterCommit;
		this.#now = options.now ?? Date.now;
		this.#releaseOnAbort = options.releaseOnAbort ?? false;
		this.#errorFactory = options.errorFactory ?? ((message) => new RpcJournalConsumerError(message));
		this.#gapError =
			options.gapError ?? ((input) => new RpcJournalRetentionGapError(input.consumerId, input.checkpoint, input));
	}

	async runOnce(signal?: AbortSignal): Promise<RpcJournalConsumerRunResult> {
		throwIfAborted(signal);
		let claim: ConsumerClaim;
		try {
			claim = this.parseClaim(
				rpcResult<unknown>(
					await this.#rpc.request(
						"consumer.claim",
						{ consumer_id: this.#consumerId, claim_ttl_ms: this.#claimTtlMs },
						{ signal },
					),
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
				const read = this.parseEventRead(
					rpcResult<unknown>(
						await this.#rpc.request(
							"main.events.read",
							{
								...(readFromCheckpoint ? { consumer_id: this.#consumerId } : { cursor: readCursor }),
								limit: 100,
								wait_ms: this.#readWaitMs,
								kinds: this.#kinds,
							},
							{ signal, timeoutMs: this.#readWaitMs + 2_000 },
						),
						"main.events.read",
					),
				);
				readFromCheckpoint = false;
				if (read.gap) {
					throw this.#gapError({ ...read.gap, consumerId: this.#consumerId, checkpoint: claim.cursor });
				}
				if (read.events.length > 0) {
					const proofs: JournalDeliveryProof[] = [];
					for (const event of read.events) {
						throwIfAborted(signal);
						const proof = await this.#publish(event, { cursor: read.nextCursor, signal });
						this.validateProof(proof, event);
						proofs.push(proof);
					}
					await this.commit(claim, read.nextCursor, proofs, signal);
					return "published";
				}
				if (read.nextCursor === readCursor || this.nearClaimExpiry(claim)) {
					// Releasing an unchanged cursor never acknowledges unconfirmed output.
					await this.commit(claim, claim.cursor, [], signal);
					return "idle";
				}
				// A filtered read can inspect unrelated events without persisting a skip.
				// Only a later publication can advance the durable consumer checkpoint.
				readCursor = read.nextCursor;
			}
		} catch (error) {
			if (!signal?.aborted || this.#releaseOnAbort) await this.releaseUnadvancedClaim(claim);
			throw error;
		}
	}

	private nearClaimExpiry(claim: ConsumerClaim): boolean {
		return this.#now() + this.#readWaitMs + 250 >= claim.expiresAt;
	}

	private async commit(
		claim: ConsumerClaim,
		cursor: string,
		proofs: readonly JournalDeliveryProof[],
		signal?: AbortSignal,
	): Promise<void> {
		rpcResult<unknown>(
			await this.#rpc.request(
				"consumer.commit",
				{
					consumer_id: this.#consumerId,
					claim_id: claim.claimId,
					cursor,
					proofs,
				},
				{ signal },
			),
			"consumer.commit",
		);
		// Durably settled: only now may a caller drop any in-process guard it
		// keeps for these sequences.
		if (proofs.length > 0) this.#afterCommit?.(proofs.map((proof) => proof.seq));
	}

	private async releaseUnadvancedClaim(claim: ConsumerClaim): Promise<void> {
		try {
			await this.commit(claim, claim.cursor, []);
		} catch {
			// Process death has the same safe outcome: the server-side claim expires.
		}
	}

	private parseClaim(value: unknown): ConsumerClaim {
		if (
			!isRecord(value) ||
			typeof value.claim_id !== "string" ||
			typeof value.cursor !== "string" ||
			typeof value.expires_at !== "number"
		) {
			throw this.#errorFactory("consumer.claim returned an invalid response.");
		}
		return { claimId: value.claim_id, cursor: value.cursor, expiresAt: value.expires_at };
	}

	private parseEventRead(value: unknown): EventRead {
		if (!isRecord(value) || !Array.isArray(value.events) || typeof value.next_cursor !== "string") {
			throw this.#errorFactory("main.events.read returned an invalid response.");
		}
		const events: RpcJournalEvent[] = [];
		for (const event of value.events) {
			if (
				!isRecord(event) ||
				(typeof event.seq !== "number" && typeof event.seq !== "string") ||
				typeof event.kind !== "string"
			) {
				throw this.#errorFactory("main.events.read returned an invalid event.");
			}
			sequenceString(event.seq, this.#errorFactory);
			if (!this.#kinds.includes(event.kind)) {
				throw this.#errorFactory(`main.events.read returned an unsupported event kind: ${event.kind}`);
			}
			events.push({
				seq: event.seq,
				...(typeof event.ts === "number" && Number.isFinite(event.ts) ? { ts: event.ts } : {}),
				kind: event.kind,
				payload: event.payload,
			});
		}
		if (value.gap === undefined) return { events, nextCursor: value.next_cursor };
		if (
			!isRecord(value.gap) ||
			typeof value.gap.missing_from !== "string" ||
			typeof value.gap.missing_to !== "string" ||
			typeof value.gap.resync_cursor !== "string"
		) {
			throw this.#errorFactory("main.events.read returned an invalid retention gap.");
		}
		return {
			events,
			nextCursor: value.next_cursor,
			gap: {
				missingFrom: value.gap.missing_from,
				missingTo: value.gap.missing_to,
				resyncCursor: value.gap.resync_cursor,
			},
		};
	}

	private validateProof(proof: JournalDeliveryProof, event: RpcJournalEvent): void {
		if (
			!isRecord(proof) ||
			typeof proof.seq !== "string" ||
			typeof proof.dedupe_key !== "string" ||
			!proof.dedupe_key
		) {
			throw this.#errorFactory("Journal publication returned an invalid delivery proof.");
		}
		if (proof.platform_msg_id !== undefined && (typeof proof.platform_msg_id !== "string" || !proof.platform_msg_id)) {
			throw this.#errorFactory("Journal publication returned an invalid platform message id.");
		}
		if (proof.seq !== sequenceString(event.seq, this.#errorFactory)) {
			throw this.#errorFactory("Journal publication proof sequence does not match its event.");
		}
	}
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function boundedInteger(value: number, name: string, minimum: number, maximum: number): number {
	if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new RpcJournalConsumerError(`${name} must be an integer in ${minimum}..=${maximum}.`);
	}
	return value;
}

function sequenceString(value: string | number, errorFactory: (message: string) => Error): string {
	if (typeof value === "string" && /^\d+$/.test(value)) return value;
	if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return String(value);
	throw errorFactory("Journal event sequence must be an unsigned safe integer.");
}

function throwIfAborted(signal: AbortSignal | undefined): void {
	if (!signal?.aborted) return;
	const error = new Error("Journal consumer stopped.");
	error.name = "AbortError";
	throw error;
}
