import { type ChatMessagePayload, isSilenceToken, type OriginRef, originKey } from "@gajaeway/protocol";
import type { DeliveryLedger } from "../store/ledger";

export class DeliveryService {
	readonly #ledger: DeliveryLedger;
	constructor(ledger: DeliveryLedger) {
		this.#ledger = ledger;
	}
	prepare(turnId: string, origin: OriginRef, text: string, replyToMessageId?: string): ChatMessagePayload | undefined {
		if (isSilenceToken(text)) return undefined;
		const deliveryId = crypto.randomUUID();
		const payload: ChatMessagePayload = {
			turnId,
			origin,
			role: "assistant",
			text,
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
	markInflight(deliveryId: string): void {
		this.#ledger.markInflight(deliveryId);
	}
	confirm(deliveryId: string): boolean {
		return this.#ledger.confirm(deliveryId);
	}
	fail(deliveryId: string, ambiguous?: boolean): boolean {
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
