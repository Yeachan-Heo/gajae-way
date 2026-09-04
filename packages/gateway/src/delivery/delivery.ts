import {
	type ChatMessagePayload,
	isSilenceToken,
	type OriginRef,
	originKey,
	type ReactionRef,
	stripControlTokens,
} from "@gajaeway/protocol";
import type { DeliveryLedger, LedgerOutcome } from "../store/ledger";

export class DeliveryService {
	readonly #ledger: DeliveryLedger;
	constructor(ledger: DeliveryLedger) {
		this.#ledger = ledger;
	}
	/**
	 * The ledger write is the last boundary before text becomes a platform message,
	 * so it sanitizes rather than trusting its caller. Callers already strip, but a
	 * path that forgets — a runtime failure notice quoting a model reply, a future
	 * verb — would otherwise persist raw control syntax into the ledger, where a
	 * reconnect replays it verbatim.
	 *
	 * Silence is matched EXACTLY here, unlike on the turn path. Containment belongs
	 * where parts are judged; applying it this far down suppressed a runtime failure
	 * notice whose diagnostic text merely quoted `[SILENT]`, and a failed turn must
	 * always leave the owner something visible.
	 *
	 * The guarantee is PROSPECTIVE. Rows written before this sanitation existed still
	 * hold whatever text they were given, and `redeliveries()` replays a stored
	 * payload verbatim on reconnect — deliberately, because rewriting a recorded
	 * delivery would make the ledger lie about what was sent. Any leak surviving an
	 * upgrade therefore drains with the in-flight rows rather than being edited away.
	 */
	prepare(turnId: string, origin: OriginRef, text: string, replyToMessageId?: string): ChatMessagePayload | undefined {
		if (isSilenceToken(text)) return undefined;
		const sanitized = stripControlTokens(text);
		if (!sanitized || isSilenceToken(sanitized)) return undefined;
		const deliveryId = crypto.randomUUID();
		const payload: ChatMessagePayload = {
			turnId,
			origin,
			role: "assistant",
			text: sanitized,
			final: true,
			deliveryId,
			...(replyToMessageId ? { replyToMessageId } : {}),
		};
		this.#ledger.createPending({
			deliveryId,
			turnId,
			originKey: originKey(origin),
			payloadJson: JSON.stringify(payload),
		});
		return payload;
	}
	/**
	 * A reaction is a LEDGER DELIVERY, not a separate class of work.
	 *
	 * Justification from this repo's actual delivery path: `delivery.confirm` /
	 * `delivery.fail` are the only mechanism an adapter has to report an outcome,
	 * and `DeliveryLedger.listUndelivered` is the only mechanism that survives a
	 * restart. A reaction outside the ledger could only fail silently — the exact
	 * failure mode we must avoid — and would vanish on crash. The ledger's
	 * at-least-once redelivery is safe here precisely because reacting is
	 * idempotent on both platforms: re-applying the same emoji to the same message
	 * is a no-op, unlike re-posting text. A recovered reaction still carries the
	 * ledger's duplicateWarning flag, but adapters ignore it on the reaction path
	 * instead of prefixing the "[recovered - may be a duplicate]" label a message
	 * needs — there is no duplicate to warn about.
	 *
	 * `text` is the bare unicode emoji: an adapter that ignores `reaction` degrades
	 * to a visible acknowledgement instead of dropping the delivery.
	 */
	prepareReaction(turnId: string, origin: OriginRef, reaction: ReactionRef): ChatMessagePayload {
		const deliveryId = crypto.randomUUID();
		const payload: ChatMessagePayload = {
			turnId,
			origin,
			role: "assistant",
			text: reaction.emoji,
			final: true,
			deliveryId,
			reaction,
		};
		this.#ledger.createPending({
			deliveryId,
			turnId,
			originKey: originKey(origin),
			payloadJson: JSON.stringify(payload),
		});
		return payload;
	}
	markInflight(deliveryId: string): void {
		this.#ledger.markInflight(deliveryId);
	}
	confirm(deliveryId: string): LedgerOutcome {
		return this.#ledger.confirm(deliveryId);
	}
	fail(deliveryId: string, ambiguous?: boolean): LedgerOutcome {
		return this.#ledger.fail(deliveryId, ambiguous);
	}
	redeliveries(): ChatMessagePayload[] {
		return this.#ledger.listUndelivered(24 * 60 * 60 * 1000).map((row) => ({
			...(JSON.parse(row.payloadJson) as ChatMessagePayload),
			redelivered: true,
			...(row.state === "inflight" || row.state === "failed_ambiguous" ? { duplicateWarning: true } : {}),
		}));
	}
	prune(): number {
		return this.#ledger.prune(7 * 24 * 60 * 60 * 1000);
	}
	status() {
		return this.#ledger.counts();
	}
}
