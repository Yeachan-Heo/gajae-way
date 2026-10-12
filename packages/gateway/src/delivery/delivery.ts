import {
	type ChatMessagePayload,
	isSilentOutput,
	type OriginRef,
	originKey,
	type ReactionRef,
} from "@gajae-gateway/protocol";
import type { ExpiredDeliveryRow, LedgerOutcome } from "../store/groups/outbox";
import type { WriteTx } from "../store/tx";
import type { Store } from "../store/store";
export { ACK_TIMEOUT_MS, classifyDeliveryError } from "../store/groups/outbox";

const DELIVERY_FRESHNESS_MS = 24 * 60 * 60 * 1_000;

export interface DeliverySweep {
	readonly payloads: readonly ChatMessagePayload[];
	readonly expired: readonly ExpiredDeliveryRow[];
}

export interface DeliveryRequeue {
	readonly requeued: readonly string[];
	readonly payloads: readonly ChatMessagePayload[];
}

/**
 * Pure construction shared by ordinary dispatch and atomic work settlement.
 * `final` is false for mid-turn speech: adapters keep the turn's working
 * status alive through it and tear it down only on the terminal reply (or the
 * final progress tick).
 */
export function buildDeliveryPayload(
	turnId: string,
	origin: OriginRef,
	text: string,
	deliveryId: string,
	replyToMessageId?: string,
	final = true,
): ChatMessagePayload | undefined {
	if (isSilentOutput(text)) return undefined;
	originKey(origin);
	return {
		turnId,
		origin,
		role: "assistant",
		text,
		final,
		deliveryId,
		...(replyToMessageId ? { replyToMessageId } : {}),
	};
}

export class DeliveryService {
	readonly #store: Store;
	constructor(store: Store) {
		this.#store = store;
	}
	prepare(
		turnId: string,
		origin: OriginRef,
		text: string,
		replyToMessageId?: string,
		deliveryId: string = crypto.randomUUID(),
		final = true,
	): ChatMessagePayload | undefined {
		const payload = buildDeliveryPayload(turnId, origin, text, deliveryId, replyToMessageId, final);
		if (!payload) return undefined;
		if (!this.#store.transaction((tx) => this.#store.outbox.create(tx, {
			deliveryId,
			turnId,
			originKey: originKey(origin),
			payloadJson: JSON.stringify(payload),
		}))) return undefined;
		return payload;
	}

	persistInTransaction(tx: WriteTx, payload: ChatMessagePayload): boolean {
		if (isSilentOutput(payload.text)) throw new Error("silent payload cannot be persisted");
		if (!payload.deliveryId) throw new Error("delivery id is required");
		return this.#store.outbox.enqueue(tx, {
			deliveryId: payload.deliveryId,
			turnId: payload.turnId,
			originKey: originKey(payload.origin),
			payloadJson: JSON.stringify(payload),
		});
	}

	get(deliveryId: string) {
		return this.#store.outbox.get(deliveryId);
	}
	list(filters: { readonly since?: string; readonly state?: string; readonly limit?: number } = {}) {
		return this.#store.outbox.list(filters);
	}
	getMany(deliveryIds: readonly string[]) {
		return this.#store.outbox.getMany(deliveryIds);
	}
	listUndelivered(freshnessMs: number, now = Date.now(), ignoreBackoff = false) {
		return this.#store.outbox.listUndelivered(freshnessMs, now, ignoreBackoff);
	}
	createPending(row: { deliveryId: string; turnId: string; originKey: string; payloadJson: string }): boolean {
		return this.#store.transaction((tx) => this.#store.outbox.create(tx, row));
	}
	createPendingInTransaction(tx: WriteTx, row: {
		deliveryId: string;
		turnId: string;
		originKey: string;
		payloadJson: string;
	}): boolean {
		return this.#store.outbox.enqueue(tx, row);
	}

	/** A reaction is a durable delivery; reconnects replay it and adapter settlement is idempotent. */
	prepareReaction(turnId: string, origin: OriginRef, reaction: ReactionRef): ChatMessagePayload {
		const deliveryId = crypto.randomUUID();
		const payload: ChatMessagePayload = {
			turnId,
			origin,
			role: "assistant",
			text: reaction.emoji,
			final: false,
			deliveryId,
			reaction,
		};
		this.createPending({ deliveryId, turnId, originKey: originKey(origin), payloadJson: JSON.stringify(payload) });
		return payload;
	}

	markInflight(deliveryId: string): void {
		this.#store.transaction((tx) => this.#store.outbox.markInflight(tx, deliveryId));
	}
	confirm(deliveryId: string): LedgerOutcome {
		return this.#store.transaction((tx) => this.#store.outbox.confirm(tx, deliveryId));
	}
	fail(deliveryId: string, ambiguous = false, reason?: string): LedgerOutcome {
		return this.#store.transaction((tx) => this.#store.outbox.fail(tx, deliveryId, ambiguous, reason));
	}
	expireStale(freshnessMs: number, now = Date.now()): ExpiredDeliveryRow[] {
		return this.#store.transaction((tx) => this.#store.outbox.expireStale(tx, freshnessMs, now));
	}
	requeueById(deliveryId: string): string[] {
		return this.#store.transaction((tx) => this.#store.outbox.requeue(tx, deliveryId));
	}
	requeueSince(since: string): string[] {
		const timestamp = Date.parse(since);
		if (!Number.isFinite(timestamp)) throw new TypeError("since must be an ISO timestamp");
		return this.#store.transaction((tx) => this.#store.outbox.requeueSince(tx, new Date(timestamp).toISOString()));
	}
	pruneOlderThan(deliveredOlderThanMs: number, now = Date.now()): number {
		return this.#store.transaction((tx) => this.#store.outbox.prune(tx, deliveredOlderThanMs, now));
	}
	counts(now = Date.now()) {
		return this.#store.outbox.counts(now);
	}

	/** Applies the age TTL, then returns replay candidates under the ledger retry and ACK policy. */
	sweep(now = Date.now(), onConnect = false, redeliver = true): DeliverySweep {
		const expired = this.expireStale(DELIVERY_FRESHNESS_MS, now);
		return { payloads: redeliver ? this.#redeliveries(now, onConnect) : [], expired };
	}
	requeue(deliveryId?: string, since?: string): DeliveryRequeue {
		if ((deliveryId === undefined) === (since === undefined))
			throw new TypeError("exactly one of deliveryId or since is required");
		const requeued = deliveryId === undefined ? this.requeueSince(since as string) : this.requeueById(deliveryId);
		const rows = new Map(this.getMany(requeued).map((row) => [row.deliveryId, row]));
		return {
			requeued,
			payloads: requeued.flatMap((id) => {
				const row = rows.get(id);
				return row ? [{ ...(JSON.parse(row.payloadJson) as ChatMessagePayload), redelivered: true, duplicateWarning: true }] : [];
			}),
		};
	}
	redeliveries(): ChatMessagePayload[] {
		return this.#redeliveries();
	}
	prune(deliveredOlderThanMs = 7 * 24 * 60 * 60 * 1000, now = Date.now()): number {
		return this.pruneOlderThan(deliveredOlderThanMs, now);
	}
	status() {
		return this.counts();
	}
	#redeliveries(now = Date.now(), ignoreBackoff = false): ChatMessagePayload[] {
		return this.listUndelivered(DELIVERY_FRESHNESS_MS, now, ignoreBackoff).map((row) => ({
			...(JSON.parse(row.payloadJson) as ChatMessagePayload),
			redelivered: true,
			...(row.state === "inflight" || row.state === "failed_ambiguous" ? { duplicateWarning: true } : {}),
		}));
	}
}
