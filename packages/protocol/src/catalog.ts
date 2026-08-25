import type { OriginRef } from "./origin";

/**
 * Typed verb + event catalogs for profile v0.1. Catalogs grow additively per
 * phase inside the fixed generic envelope (ARCH-006). The gateway action
 * registry must cover every entry here — enforced by sdk-coverage-inventory.
 *
 * P1 additions: non-loopback chat origins, engagement metadata, ledger-backed
 * delivery settlement verbs, and redelivery labeling on chat.message.
 */

export interface GatewayStatusResult {
	readonly profileVersion: string;
	readonly capabilities: readonly string[];
	readonly pid: number;
	readonly startedAt: string;
	readonly schemaVersion: number;
	/** Session census (grows in later phases). */
	readonly sessions: { readonly active: number };
	/** Delivery ledger health (P1+). */
	readonly delivery?: { readonly pending: number; readonly oldestPendingAgeMs: number | null };
}

/** Inbound engagement metadata supplied by adapters for group-capable origins. */
export interface EngagementContext {
	/** True when the agent account was explicitly mentioned/addressed. */
	readonly mentioned: boolean;
	/** True when the origin is a group surface (channel/thread/topic), false for DMs. */
	readonly group: boolean;
	/** Platform-scoped author id of the inbound message. */
	readonly authorId: string;
}

export interface ChatSendParams {
	readonly origin: OriginRef;
	readonly text: string;
	/** Required for non-loopback origins; the gateway applies engagement policy. */
	readonly engagement?: EngagementContext;
}

export interface ChatSendResult {
	/**
	 * Gateway-assigned turn id, or null when engagement policy declined the
	 * message (not mentioned in a mention-gated group). Declined messages are
	 * still context, never commands.
	 */
	readonly turnId: string | null;
	readonly engaged: boolean;
}

export interface ChatMessagePayload {
	readonly turnId: string;
	readonly origin: OriginRef;
	readonly role: "assistant";
	readonly text: string;
	/** True when this is the final message of the turn. */
	readonly final: boolean;
	/**
	 * Ledger delivery id when this message requires platform delivery
	 * settlement (non-loopback origins). Adapters MUST settle it via
	 * delivery.confirm / delivery.fail.
	 */
	readonly deliveryId?: string;
	/** True when re-emitted from the ledger after a restart. */
	readonly redelivered?: boolean;
	/**
	 * True when the original send was mid-flight at crash time: the platform
	 * may already have the message, so adapters must deliver with a visible
	 * duplicate label (honest at-least-once, spec fact 14).
	 */
	readonly duplicateWarning?: boolean;
}

export interface DeliveryConfirmParams {
	readonly deliveryId: string;
}

export interface DeliveryFailParams {
	readonly deliveryId: string;
	readonly reason: string;
	/** True when the send may have reached the platform (ambiguous outcome). */
	readonly ambiguous?: boolean;
}

/** Verb catalog: verb name -> { params, result } (documentation-level typing). */
export interface VerbCatalogV01 {
	"gateway.status": { params: undefined; result: GatewayStatusResult };
	"gateway.shutdown": { params: undefined; result: { readonly stopping: true } };
	"chat.send": { params: ChatSendParams; result: ChatSendResult };
	"delivery.confirm": { params: DeliveryConfirmParams; result: { readonly settled: true } };
	"delivery.fail": { params: DeliveryFailParams; result: { readonly recorded: true } };
}

/** Event catalog: event name -> payload. */
export interface EventCatalogV01 {
	"chat.message": ChatMessagePayload;
	"gateway.stopping": { readonly reason: string };
}

export const VERBS_V01 = [
	"gateway.status",
	"gateway.shutdown",
	"chat.send",
	"delivery.confirm",
	"delivery.fail",
] as const;
export const EVENTS_V01 = ["chat.message", "gateway.stopping"] as const;

export type VerbName = keyof VerbCatalogV01;
export type EventName = keyof EventCatalogV01;

/**
 * Silence tokens (spec fact 22, Hermes pattern): when a turn's final reply is
 * exactly one of these (after trim), the gateway suppresses outbound delivery
 * while keeping the turn in the session transcript.
 */
export const SILENCE_TOKENS = ["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY"] as const;

export function isSilenceToken(text: string): boolean {
	const normalized = text.trim().toUpperCase();
	return (SILENCE_TOKENS as readonly string[]).some((t) => t.toUpperCase() === normalized);
}
