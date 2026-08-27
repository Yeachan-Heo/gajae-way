/**
 * The "does anything need me?" projection.
 *
 * This is severity policy, and it lives on the server exactly once. The browser
 * must never assemble this queue: two copies of a threshold is two thresholds.
 *
 * The thresholds are deliberately hysteretic. A monitor event that was admitted
 * four seconds ago is not stuck, it is in flight; the gateway's own
 * reconciliation replays unfinished `admitted` / `dispatched` / `failed` events
 * (docs/monitors.md step 7), so the console only escalates once that repair path
 * has visibly failed to run.
 */

import type { GatewayStatusResult, MonitorEventRecord, MonitorRecord } from "@gajaeway/protocol";
import { formatDuration, pluralise, shortId } from "./format";

/** A monitor event in a non-terminal stage is only an attention item past this age. */
export const STUCK_EVENT_MS = 600_000;
/** A pending delivery is only an attention item past this age. */
export const PENDING_DELIVERY_MS = 300_000;

export type AttentionTone = "danger" | "warn";

export type AttentionItem = {
	readonly key: string;
	/** Sort rank; lower is more urgent. */
	readonly severity: number;
	readonly tone: AttentionTone;
	/** The symptom, in the owner's language. */
	readonly title: string;
	/** The cause, one tap down in the same card. */
	readonly detail: string;
	readonly meta: string;
	/** ISO timestamp the client ticks an age against, when there is one. */
	readonly at: string | null;
};

/** Data the attention projection cannot see, and the gap that would supply it. */
export type CoverageGap = {
	readonly gap: string;
	readonly missing: string;
};

/**
 * Named so the panel can say what it is blind to instead of implying an empty
 * queue means nothing is wrong. Every entry maps to a section 8 gap.
 */
export const ATTENTION_GAPS: readonly CoverageGap[] = [
	{ gap: "G8", missing: "operator holds — subsession state never crosses the gateway protocol" },
	{ gap: "G3", missing: "ambiguous and expired deliveries — gateway.status returns counts only" },
	{ gap: "G5", missing: "quarantined memory intents — no verb exposes memory_intents" },
];

export type MonitorSnapshot = {
	readonly monitor: MonitorRecord;
	readonly recentEvents: readonly MonitorEventRecord[];
};

const STUCK_STAGES = new Set(["admitted", "dispatched", "failed"]);

function stageSentence(stage: string): string {
	switch (stage) {
		case "admitted":
			return "admitted but never dispatched to a session";
		case "dispatched":
			return "dispatched but no output was authored";
		case "failed":
			return "failed and has not been replayed";
		default:
			return `stuck at stage ${stage}`;
	}
}

export function buildAttention(
	status: GatewayStatusResult | null,
	monitors: readonly MonitorSnapshot[],
	now: Date,
): readonly AttentionItem[] {
	const items: AttentionItem[] = [];

	const delivery = status?.delivery;
	if (delivery && delivery.pending > 0 && (delivery.oldestPendingAgeMs ?? 0) >= PENDING_DELIVERY_MS) {
		const age = formatDuration(delivery.oldestPendingAgeMs ?? 0);
		items.push({
			key: "delivery:pending",
			severity: delivery.pending > 1 ? 1 : 2,
			tone: "danger",
			title: `${pluralise(delivery.pending, "reply", "replies")} never reached their platform`,
			detail: `The oldest has been waiting ${age}. Either the adapter is down or it is not settling the ledger.`,
			meta: "gateway.status · delivery",
			at: null,
		});
	}

	for (const { monitor, recentEvents } of monitors) {
		for (const event of recentEvents) {
			if (!STUCK_STAGES.has(event.stage)) continue;
			const firedAt = new Date(event.firedAt);
			const age = Number.isNaN(firedAt.getTime()) ? 0 : now.getTime() - firedAt.getTime();
			if (event.stage !== "failed" && age < STUCK_EVENT_MS) continue;
			items.push({
				key: `monitor-event:${event.eventId}`,
				severity: event.stage === "failed" ? 2 : 3,
				tone: event.stage === "failed" ? "danger" : "warn",
				title: `${monitor.name} · ${event.eventType} produced nothing`,
				detail: `The event was ${stageSentence(event.stage)}.`,
				meta: `event ${shortId(event.eventId)} · stage ${event.stage}`,
				at: event.firedAt,
			});
		}
	}

	return items.sort((a, b) => a.severity - b.severity || a.key.localeCompare(b.key));
}
