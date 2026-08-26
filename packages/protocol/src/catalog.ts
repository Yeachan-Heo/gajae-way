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
	/** Human-readable author name when the platform provides one. */
	readonly authorName?: string;
	/** Human-readable conversation label (channel/group name) when available. */
	readonly channelLabel?: string;
	/** Human-readable server/guild/workspace label when the platform has one above the channel. */
	readonly serverLabel?: string;
}

export interface ChatSendParams {
	readonly origin: OriginRef;
	readonly text: string;
	/** Required for non-loopback origins; the gateway applies engagement policy. */
	readonly engagement?: EngagementContext;
}

/** Periodic liveness for a long-running turn: the persona is working, not gone. */
export interface ChatProgressPayload {
	readonly turnId: string;
	readonly origin: OriginRef;
	/** Wall-clock milliseconds since the turn was accepted. */
	readonly elapsedMs: number;
	/** Tool executions the turn has started so far. */
	readonly toolCalls: number;
	/** Output tokens produced so far (exact per completed message, estimated between). */
	readonly outputTokens: number;
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
	/** Platform message id this message replies to (reply-threading), when the persona chose one. */
	readonly replyToMessageId?: string;
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

/**
 * Cross-session recall (P2, spec fact 10): on-demand, bounded, source-cited.
 * Never returns raw transcripts; snippets are working-memory digests and every
 * snippet names its source origin.
 */
export interface SessionRecallParams {
	/** Free-text relevance query; empty returns most-recent snippets. */
	readonly query?: string;
	/** Max snippets returned; server clamps to its own ceiling. */
	readonly limit?: number;
	/** Origin the request is made on behalf of; excluded from results. */
	readonly requestingOrigin?: OriginRef;
}

export interface RecallSnippet {
	/** Source origin citation — always present (spec fact 10). */
	readonly origin: OriginRef;
	/** Bounded digest text, never raw transcript. */
	readonly text: string;
	readonly at: string;
}

export interface SessionRecallResult {
	readonly snippets: readonly RecallSnippet[];
}

export interface SessionListResult {
	readonly sessions: readonly {
		readonly origin: OriginRef;
		readonly createdAt: string;
		readonly lastActivityAt: string | null;
		readonly epoch: number;
	}[];
}

/**
 * Memory system surface (P3, spec fact 8): filesystem-first Markdown memory.
 * memory.audit runs the structural validator; memory.search is map-then-BM25
 * retrieval over the canonical tree. Both are read-only verbs.
 */
export interface MemoryAuditResult {
	readonly ok: boolean;
	readonly issues: readonly {
		readonly code: string;
		readonly path: string;
		readonly message: string;
	}[];
}

export interface MemorySearchParams {
	readonly query: string;
	readonly limit?: number;
}

export interface MemorySearchResult {
	readonly hits: readonly {
		readonly path: string;
		readonly score: number;
		readonly excerpt: string;
	}[];
}

/**
 * Monitor surface (P4, spec facts 7/11/12/19): unified Monitor abstraction.
 * A cron is a Monitor with a periodic static trigger. Event types are declared
 * at creation, never inferred; unknown types route to the catch-all session.
 */
export type TriggerSpec =
	| { readonly kind: "cron"; readonly schedule: string }
	| { readonly kind: "webhook"; readonly route: string }
	| { readonly kind: "watcher"; readonly root: string; readonly debounceMs?: number }
	| { readonly kind: "script"; readonly command: readonly string[]; readonly intervalMs: number };

export type BurstPolicyKind = "coalesce" | "dedupe" | "serialize" | "drop";

export interface MonitorSpec {
	readonly name: string;
	readonly trigger: TriggerSpec;
	/** Declared event types this monitor may emit (spec fact 19). */
	readonly eventTypes: readonly string[];
	/** Burst policy; coalesce when unspecified (spec fact 12). */
	readonly burstPolicy?: BurstPolicyKind;
	/** Channel target for authored output: at most one (spec fact 7). */
	readonly channelTarget?: { readonly origin: OriginRef } | null;
	readonly enabled?: boolean;
}

export interface MonitorRecord extends MonitorSpec {
	readonly monitorId: string;
	readonly createdAt: string;
	readonly burstPolicy: BurstPolicyKind;
	readonly enabled: boolean;
}

export interface MonitorTestParams {
	readonly monitorId: string;
	readonly eventType?: string;
	readonly payload?: unknown;
}

export interface MonitorEventRecord {
	readonly eventId: string;
	readonly monitorId: string;
	readonly eventType: string;
	readonly firedAt: string;
	readonly stage: string;
}

/** Verb catalog: verb name -> { params, result } (documentation-level typing). */
export interface VerbCatalogV01 {
	"gateway.status": { params: undefined; result: GatewayStatusResult };
	"gateway.shutdown": { params: undefined; result: { readonly stopping: true } };
	"chat.send": { params: ChatSendParams; result: ChatSendResult };
	"delivery.confirm": { params: DeliveryConfirmParams; result: { readonly settled: true } };
	"delivery.fail": { params: DeliveryFailParams; result: { readonly recorded: true } };
	"session.recall": { params: SessionRecallParams; result: SessionRecallResult };
	"session.list": { params: undefined; result: SessionListResult };
	"memory.audit": { params: undefined; result: MemoryAuditResult };
	"memory.search": { params: MemorySearchParams; result: MemorySearchResult };
	"monitor.add": { params: MonitorSpec; result: { readonly monitorId: string } };
	"monitor.list": { params: undefined; result: { readonly monitors: readonly MonitorRecord[] } };
	"monitor.inspect": {
		params: { readonly monitorId: string };
		result: { readonly monitor: MonitorRecord; readonly recentEvents: readonly MonitorEventRecord[] };
	};
	"monitor.test": { params: MonitorTestParams; result: { readonly eventId: string } };
	"monitor.remove": { params: { readonly monitorId: string }; result: { readonly removed: true } };
	"ops.backup": {
		params: { readonly path: string };
		result: { readonly path: string; readonly bytes: number };
	};
	"ops.integrity": { params: undefined; result: { readonly ok: boolean; readonly detail: string } };
}

/** Event catalog: event name -> payload. */
export interface EventCatalogV01 {
	"chat.message": ChatMessagePayload;
	"chat.progress": ChatProgressPayload;
	"gateway.stopping": { readonly reason: string };
	"monitor.event": MonitorEventRecord;
}

export const VERBS_V01 = [
	"gateway.status",
	"gateway.shutdown",
	"chat.send",
	"delivery.confirm",
	"delivery.fail",
	"session.recall",
	"session.list",
	"memory.audit",
	"memory.search",
	"monitor.add",
	"monitor.list",
	"monitor.inspect",
	"monitor.test",
	"monitor.remove",
	"ops.backup",
	"ops.integrity",
] as const;
export const EVENTS_V01 = ["chat.message", "chat.progress", "gateway.stopping", "monitor.event"] as const;

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
