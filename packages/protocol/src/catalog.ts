import type { OriginRef } from "./origin";
import type { ReactionAction, ReactionRef } from "./reactions";

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

/**
 * The message an inbound message replies to, when the platform reports one.
 *
 * A reply is the only signal that says *which* of the many messages in a busy
 * room is being answered, so it is carried as metadata rather than folded into
 * engagement decisions. Every field except `messageId` is optional on purpose:
 * platforms hand out the reference id eagerly but the referenced author and
 * text only when they are already resolved, and an adapter must never delay
 * inbound handling on an extra API fetch to fill this in. Absent beats guessed.
 */
export interface ReplyContext {
	/** Platform-scoped id of the referenced message. Always known when a reply exists. */
	readonly messageId: string;
	/** Platform-scoped author id of the referenced message, when it is already resolved. */
	readonly authorId?: string;
	/** Display name of the referenced author, resolved with the same precedence as `authorName`. */
	readonly authorName?: string;
	/**
	 * True when the referenced message was authored by our own agent account,
	 * false when it was authored by somebody else. Absent means the referenced
	 * author is unknown, so ownership could not be decided — never assume false.
	 */
	readonly fromSelf?: boolean;
	/** Short single-line excerpt of the referenced text, when the platform included it. */
	readonly excerpt?: string;
}

/** Inbound engagement metadata supplied by adapters for group-capable origins. */
export interface EngagementContext {
	/** True when the agent account was explicitly mentioned/addressed. */
	readonly mentioned: boolean;
	/** True when the origin is a group surface (channel/thread/topic), false for DMs. */
	readonly group: boolean;
	/** Platform-scoped author id of the inbound message. */
	readonly authorId: string;
	/** True when the platform marks the author as a bot/automation account. */
	readonly authorIsBot?: boolean;
	/**
	 * Name to address the author by: the per-surface display name a reader in the
	 * room actually sees, not the account handle. On Discord that is the guild
	 * nickname, then the global display name, then the handle.
	 */
	readonly authorName?: string;
	/**
	 * Raw platform handle, kept separately for identification and logs. Prefer
	 * `authorName` when speaking to or about the author.
	 */
	readonly authorHandle?: string;
	/** Human-readable conversation label (channel/group name) when available. */
	readonly channelLabel?: string;
	/** Human-readable server/guild/workspace label when the platform has one above the channel. */
	readonly serverLabel?: string;
	/**
	 * The message this one replies to, when the platform reports a reply. Absent
	 * for every message that is not a reply, so existing payloads are unchanged.
	 * Metadata only: a reply never decides engagement by itself.
	 */
	readonly replyTo?: ReplyContext;
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
	/**
	 * True on the last progress event of a turn, including a turn that ends with a
	 * silence token and therefore delivers nothing.
	 *
	 * Adapters render progress as a temporary message and clear it when the reply
	 * lands. A suppressed turn has no delivery, so without this flag the "working"
	 * message is orphaned in the channel forever - which is exactly what happened
	 * in every `open` channel where the persona chose to stay silent.
	 */
	readonly final?: boolean;
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
	/**
	 * When present this delivery is a REACTION, not a message: the adapter must
	 * react to `reaction.targetMessageId` and post nothing. `text` still carries
	 * the bare unicode emoji so an adapter without reaction support degrades to a
	 * visible acknowledgement instead of a lost delivery.
	 */
	readonly reaction?: ReactionRef;
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

/** A worker gjc session run: an isolated coding-register session doing delegated work. */
export interface WorkRunParams {
	/** Stable worker name; the same name resumes the same gjc session. */
	readonly name: string;
	readonly text: string;
	/** Working directory for the worker session (e.g. a repo checkout). */
	readonly cwd?: string;
}
export interface WorkRunResult {
	readonly text: string;
	readonly sessionKey: string;
}

/**
 * Outbound reaction (chat.react): react to ONE specific message in ONE specific
 * origin. The target message id is required — "react to the last message" is not
 * expressible, because "last" changes under you. `emoji` accepts any allowlisted
 * spelling (`👍`, `thumbsup`, `:thumbsup:`) and is canonicalized by the gateway.
 *
 * Allowlisted is not the same as deliverable: a platform may accept only part of
 * the allowlist (Telegram publishes a fixed reaction set), and the gateway
 * refuses an emoji that origin cannot express rather than queueing a delivery
 * that can only fail. `reactionAllowlistFor(platform)` is what a caller should
 * offer.
 */
export interface ChatReactParams {
	readonly origin: OriginRef;
	readonly targetMessageId: string;
	readonly emoji: string;
}

export interface ChatReactResult {
	/** Ledger delivery id: adapters settle a reaction exactly like a message. */
	readonly deliveryId: string;
	/** Canonical unicode the gateway resolved the requested emoji to. */
	readonly emoji: string;
}

/**
 * Inbound reaction (engagement.reaction): someone reacted to a message, or took
 * their reaction back. This is engagement metadata and NEVER a turn: it is
 * recorded as conversation context for the next engaged turn to read, and it does
 * not wake the persona. `engaged` is therefore always false.
 */
export interface EngagementReactionParams {
	readonly origin: OriginRef;
	/** Platform id of the message that was reacted to. */
	readonly targetMessageId: string;
	/** Raw platform emoji as the reactor sent it; not restricted to the allowlist. */
	readonly emoji: string;
	readonly action: ReactionAction;
	readonly engagement: EngagementContext;
}

export interface EngagementReactionResult {
	readonly recorded: boolean;
	/** Always false: a reaction is metadata, never a turn. */
	readonly engaged: false;
}
/**
 * Operator runtime-cycle projection (ops.cycle): a read-only, snapshot view of
 * where every runtime cycle currently stands — durable inbound dispatch,
 * delivery settlement, memory closure, monitor settlement — plus per-session
 * identity with epoch/provenance. Derived, never authoritative: the durable
 * SQLite rows and the delivery ledger remain the source of truth, and this
 * projection adds no writer of its own.
 */
export type CyclePhase = "idle" | "dispatching" | "delivering" | "draining" | "degraded";

/**
 * Fail-closed reason a phase cannot be reported as healthy. The projection
 * must never guess an optimistic phase over missing evidence.
 */
export type CycleGateReason =
	| "stale_session_identity"
	| "delivery_settlement_unknown"
	| "memory_closure_blocked"
	| "monitor_settlement_failed";

export interface CycleSessionView {
	/** Canonical, opaque origin key (protocol originKey; never reparsed). */
	readonly originKey: string;
	/** Validated origin ref for display provenance. */
	readonly origin: OriginRef;
	readonly epoch: number;
	/**
	 * Bound gjc session id. Empty string means the origin is mid-rebind:
	 * epoch was bumped (or the session was never created), so identity is
	 * stale by construction and turns rebind on dispatch.
	 */
	readonly sessionId: string;
	readonly createdAt: string;
	readonly lastActivityAt: string | null;
	/** Durable inbound messages still awaiting their turn for this origin. */
	readonly pendingInbound: number;
	/** Ledger deliveries not yet confirmed/expired for this origin. */
	readonly unsettledDeliveries: number;
	/** Oldest unsettled delivery age in ms, null when none are unsettled. */
	readonly oldestUnsettledAgeMs: number | null;
}

export interface OpsCycleResult {
	/** Aggregate runtime phase; "degraded" is emitted whenever gates is non-empty. */
	readonly phase: CyclePhase;
	/**
	 * Fail-closed gate reasons. Empty iff the cycle is healthy. Unknown
	 * settlement states surface as gates, never as healthy silence.
	 */
	readonly gates: readonly CycleGateReason[];
	readonly generatedAt: string;
	/** Gateway instance id that produced this snapshot (provenance). */
	readonly instanceId: string;
	/** True when a memory-closure drain is in flight at snapshot time. */
	readonly memoryClosing: boolean;
	readonly sessions: readonly CycleSessionView[];
	/** Settlement census of durable memory intents. */
	readonly memoryIntents: {
		readonly queued: number;
		readonly written: number;
		readonly committed: number;
		readonly receipted: number;
		readonly quarantined: number;
	};
	/** Monitor events not yet terminally settled, by stage. */
	readonly monitorEvents: { readonly stage: string; readonly count: number }[];
	/** Delivery ledger census across all states. */
	readonly deliveries: {
		readonly pending: number;
		readonly inflight: number;
		readonly confirmed: number;
		readonly failedAmbiguous: number;
		readonly expired: number;
	};
	/** Durable inbound messages claimed but not completed right now. */
	readonly inFlightInbound: number;
	/** Durable inbound messages still awaiting their turn, across ALL origins. */
	readonly pendingInbound: number;
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
	"work.run": { params: WorkRunParams; result: WorkRunResult };
	"chat.react": { params: ChatReactParams; result: ChatReactResult };
	"engagement.reaction": { params: EngagementReactionParams; result: EngagementReactionResult };
	"ops.cycle": { params: undefined; result: OpsCycleResult };
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
	"work.run",
	"chat.react",
	"engagement.reaction",
	"ops.cycle",
] as const;
export const EVENTS_V01 = ["chat.message", "chat.progress", "gateway.stopping", "monitor.event"] as const;

export type VerbName = keyof VerbCatalogV01;
export type EventName = keyof EventCatalogV01;

/**
 * Silence tokens (spec fact 22, Hermes pattern): when a turn's final reply is
 * exactly one of these (after trim), the gateway suppresses outbound delivery
 * while keeping the turn in the session transcript.
 *
 * Matching is bracket-insensitive. `[SILENT]` was the only bracketed spelling
 * in the original list, so an owner or persona writing the equally natural
 * `[NO_REPLY]` produced a literal message in the room instead of silence.
 * Brackets are decoration, not meaning: strip one optional surrounding pair
 * before comparing.
 */
export const SILENCE_TOKENS = ["[SILENT]", "SILENT", "NO_REPLY", "NO REPLY"] as const;

export function isSilenceToken(text: string): boolean {
	const normalized = unbracket(text.trim()).toUpperCase();
	return (SILENCE_TOKENS as readonly string[]).some((t) => unbracket(t).toUpperCase() === normalized);
}

function unbracket(text: string): string {
	return text.startsWith("[") && text.endsWith("]") && text.length > 2 ? text.slice(1, -1).trim() : text;
}
