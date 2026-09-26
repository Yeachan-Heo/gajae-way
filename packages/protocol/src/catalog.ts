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
	readonly delivery?: {
		readonly pending: number;
		readonly oldestPendingAgeMs: number | null;
		readonly expired: number;
		readonly recentExpired: readonly {
			readonly deliveryId: string;
			readonly originKey: string;
			readonly attempts: number;
			readonly expiredAt: string;
		}[];
	};
	/** Aggregate-only conversation diff health; never includes message bodies. */
	readonly contextDiff?: ConversationContextDiagnostics;
	/**
	 * Operator counters for the bot-audience guard: addressed messages declined by
	 * the consecutive-turn budget, and admissions stopped by the runaway rate limit.
	 */
	readonly engagement?: { readonly botAudienceDeclines: number; readonly botAudienceRateLimited: number };
	/**
	 * Connected clients with their process generation. `staleGeneration` marks a
	 * client whose process predates this gateway process: the adapter survived a
	 * gateway-only restart and is serving the previous generation.
	 */
	readonly clients?: readonly {
		readonly name: string;
		readonly startedAt?: string;
		readonly connectedAt: string;
		readonly staleGeneration: boolean;
	}[];
}

export interface ConversationContextDiagnostics {
	readonly unread: number;
	readonly expired: number;
	readonly truncated: number;
	readonly omittedOldestAt: string | null;
	readonly omittedNewestAt: string | null;
	/** Durable reset floor for a specific origin; null on aggregate projections. */
	readonly floorAt: string | null;
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
	/**
	 * The message must be recorded but must never open a turn. Set by an adapter
	 * that backfills a message it knows the room has already moved past - e.g. a
	 * catch-up after an outage where the message is old and the persona has since
	 * answered in that conversation. Without it, a recovery pass answers hours-old
	 * messages as if they were new (live, 2026-09-17: a 16:21 mention re-answered
	 * at 18:29 after a gateway restart). Overrides every other gate.
	 */
	readonly contextOnly?: boolean;
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
	/**
	 * Server/clan tag the author wears next to their name, when the platform has
	 * such a badge and the author enabled it. On Discord this is the primary
	 * guild tag (`User#primaryGuild.tag`), which is exactly what a reader in the
	 * room sees and what tells them which server that account belongs to — the
	 * persona was blind to it while every human could read it.
	 */
	readonly authorServerTag?: string;
	/** Human-readable conversation label (channel/group name) when available. */
	readonly channelLabel?: string;
	/** Human-readable server/guild/workspace label when the platform has one above the channel. */
	readonly serverLabel?: string;
	/**
	 * The message this one replies to, when the platform reports a reply. Absent
	 * for every message that is not a reply, so existing payloads are unchanged.
	 * A platform adapter may treat `fromSelf` as an addressed signal alongside a
	 * real mention; the gateway still applies the configured mode and audience.
	 */
	readonly replyTo?: ReplyContext;
}

export interface ChatSendParams {
	readonly origin: OriginRef;
	readonly text: string;
	/** Stable platform message id for idempotent live/backfill ingestion. */
	readonly messageId?: string;
	/** Platform event time, when available; ordering and age policy use this value. */
	readonly receivedAt?: string;
	/** Required for non-loopback origins; the gateway applies engagement policy. */
	readonly engagement?: EngagementContext;
}

/**
 * A platform message the gateway already ingested was edited. The edit is not
 * a new message: the gateway streams it into the same session as an update of
 * a `[MESSAGE POINTER: <messageId>]`, steered into the running turn or sent
 * as the next one. An edit of a message the gateway never saw is ignored.
 */
export interface ChatEditParams {
	readonly origin: OriginRef;
	/** Platform id of the message that was edited (the original `chat.send` messageId). */
	readonly messageId: string;
	/** The full new body. */
	readonly text: string;
	/** Platform edit time, when available. */
	readonly receivedAt?: string;
	/** Required for non-loopback origins; the gateway applies engagement policy. */
	readonly engagement?: EngagementContext;
}

export interface ChatEditResult {
	/** Gateway-assigned turn id for the update, or null when it was declined or the message is unknown. */
	readonly turnId: string | null;
	readonly engaged: boolean;
}

/**
 * What a working turn is doing right now, for the presence hint. `tool` names
 * the tool being run (with the model's stated intent or a short argument
 * summary as `detail`); `thinking` is the model reading a tool result;
 * `writing` is assistant text being produced. Labels are bounded, single-line
 * and control-character free: they are rendered verbatim into chat.
 */
export interface ChatProgressActivity {
	readonly kind: "tool" | "thinking" | "writing";
	readonly label: string;
	readonly detail?: string;
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
	/** The current activity, when the tail has reported one. Absent before the first tool/text frame. */
	readonly activity?: ChatProgressActivity;
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
	/**
	 * When present the adapter must ALSO post this text as a spoken voice
	 * message, in addition to delivering `text` normally.
	 *
	 * Set when the turn was triggered by a voice message: the owner asked for a
	 * voice reply to be paired with its text automatically, decided by the
	 * inbound modality rather than by anything the persona has to remember.
	 * The two cannot share one platform message — Discord requires empty content
	 * on a voice message — so the adapter sends text first, then the audio.
	 *
	 * Absent on redelivery after a restart: the modality lives with the in-flight
	 * turn, not in the delivery ledger, so a recovered delivery degrades to
	 * text-only. Text is the deliverable and voice is the courtesy, so that is
	 * the safe direction to lose.
	 */
	readonly voiceText?: string;
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

export interface OpsRedeliverParams {
	readonly deliveryId?: string;
	readonly since?: string;
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
		readonly bootstrap: SessionBootstrapProjection;
	}[];
}

/**
 * `/model set` choices (session.modelChoices): preset names from the gjc
 * profile's `models.yml` `profiles:` map plus the configured gateway selector.
 * Fails soft: an unreadable catalog yields an empty list, never an error.
 */
export interface SessionModelChoicesResult {
	readonly choices: readonly string[];
}

export interface SessionBootstrapProjection {
	readonly epoch: number;
	readonly pending: boolean;
	readonly appliedAt: string | null;
	readonly includedSections: readonly string[];
	readonly byteCount: number;
	readonly truncated: boolean;
	readonly diagnostics: readonly string[];
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
export type MonitorModelSelection = string | { readonly preset: string };
export type MonitorServiceTier =
	| "none"
	| "auto"
	| "default"
	| "flex"
	| "scale"
	| "priority"
	| "openai-only"
	| "claude-only";

/**
 * Where a monitor's authored output goes. Destination and mentions are typed
 * fields so they are never encoded into an event type or recovered from
 * instruction prose (issue #180).
 */
export interface MonitorChannelTarget {
	readonly origin: OriginRef;
	/**
	 * Platform user ids pinged at the start of every delivered note, as `<@id>`.
	 * Discord and Slack targets only.
	 */
	readonly mentionUserIds?: readonly string[];
}

export interface MonitorSpec {
	readonly name: string;
	readonly trigger: TriggerSpec;
	/** Declared event types this monitor may emit (spec fact 19). */
	readonly eventTypes: readonly string[];
	/** Burst policy; coalesce when unspecified (spec fact 12). */
	readonly burstPolicy?: BurstPolicyKind;
	/** Channel target for authored output: at most one (spec fact 7). */
	readonly channelTarget?: MonitorChannelTarget | null;
	/**
	 * Per-monitor execution instruction handed to the authoring turn. Without it
	 * a monitor's session only learns that an event fired, so it can do nothing
	 * but write a receipt note. Free text, bounded length; the JSON-array
	 * response contract is unaffected.
	 */
	readonly instruction?: string;
	/** Absent means inherit the gateway default; present overrides this monitor's authoring session. */
	readonly model?: MonitorModelSelection;
	/** Absent means inherit the gateway default; present overrides this monitor's request tier. */
	readonly serviceTier?: MonitorServiceTier;
	readonly enabled?: boolean;
}

export interface MonitorRecord extends MonitorSpec {
	readonly monitorId: string;
	readonly createdAt: string;
	readonly burstPolicy: BurstPolicyKind;
	readonly enabled: boolean;
}

export interface MonitorUpdateParams extends Partial<Omit<MonitorSpec, "trigger">> {
	readonly monitorId: string;
	/** Replace the trigger spec; use schedule to change only a cron schedule. */
	readonly trigger?: TriggerSpec;
	/** Change the schedule while retaining the monitor's existing cron trigger. */
	readonly schedule?: string;
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
	/** Historical authority hold; stage remains the recorded historical stage. */
	readonly quarantined?: boolean;
	readonly reason?: string;
}

/** A worker gjc session run: an isolated coding-register session doing delegated work. */
export interface WorkRunParams {
	/** Stable worker name; the same name resumes the same gjc session. */
	readonly name: string;
	readonly text: string;
	/** Working directory for the worker session (e.g. a repo checkout). */
	readonly cwd?: string;
	/**
	 * Explicit operator acknowledgement that lets a new attempt start while
	 * the durable job is awaiting_operator (issue #10 hold semantics).
	 */
	readonly resume?: boolean;
	/** Startup model: an explicit model id or a model profile preset; applied at session create and on every send. */
	readonly model?: string | { readonly preset: string };
	/** Untrusted routing hint linking this request to the calling GJC session. */
	readonly callerSessionId?: string;
}

/** Asynchronous starts and synchronous runs share the same caller metadata. */
export type WorkStartParams = WorkRunParams;

export type WorkStartResult =
	| {
			readonly started: true;
			readonly jobId: string;
			readonly opRef: string;
			readonly sessionKey: string;
			readonly sessionId: string;
	  }
	| {
			readonly started: false;
			readonly held: true;
			readonly jobId: string;
			readonly state: string;
			readonly reason: string;
	  };

/** Public structural projection; protocol must not depend on subsession. */
export interface PromptStatusBody {
	readonly status: "accepted" | "in_flight" | "terminal_ok" | "failed" | "unknown";
	readonly commandId?: string;
	readonly turnId?: string;
	readonly clientRef?: string;
	readonly acceptedAt?: number;
	readonly startedAt?: number;
	readonly terminalAt?: number;
	readonly receiptState?: "absent" | "present" | "missing" | "unknown";
	readonly outcome?: { readonly kind?: string; readonly reason?: string; readonly provenance?: string };
	/** Gateway-filtered safe failure codes/messages, never raw SDK exceptions. */
	readonly error?: { readonly code?: string; readonly message?: string };
}

export interface WorkStatusParams {
	readonly name: string;
}

/** Read-only durable snapshot plus a matching-binding live operation query. */
export interface WorkStatusResult {
	readonly jobId: string;
	readonly state: string;
	readonly sessionId: string;
	readonly lastActivityAt: string | null;
	readonly attempt: {
		readonly opRef: string;
		readonly startedAt: string;
		readonly endedAt?: string;
		readonly endState?: string;
	} | null;
	readonly op: PromptStatusBody | null;
}

export interface WorkSteerParams {
	readonly name: string;
	readonly text: string;
}

export type WorkSteerResult =
	| { readonly steered: true; readonly clientRef: string }
	| { readonly steered: false; readonly reason: string };

export interface WorkRetireParams {
	readonly name: string;
}

/**
 * Retirement closes the worker's gjc session and clears the gateway binding,
 * so the next `work.run` for that name creates a fresh session. A lane with an
 * open attempt is never retired from under its turn.
 */
export type WorkRetireResult =
	| { readonly retired: true; readonly sessionKey: string; readonly sessionId: string; readonly closed: boolean }
	| { readonly retired: false; readonly sessionKey: string; readonly reason: string };

/** Structured detail carried by a `lane_capacity` error. */
export interface LaneCapacityDetail {
	readonly maxLanes: number;
	readonly active: number;
	/**
	 * Retirement candidates, idlest first, so the caller can free a slot
	 * deliberately. `idleMs` is -1 when the lane has no recorded activity.
	 * A candidate may still refuse retirement (open attempt, unproven end).
	 */
	readonly candidates: ReadonlyArray<{ readonly name: string; readonly idleMs: number; readonly state: string }>;
}

/**
 * Either a held outcome (the durable job is awaiting_operator after a crash /
 * restart and nothing ran) or a completed attempt carrying its durable job and
 * op identities.
 */
export type WorkRunResult =
	| { readonly held: true; readonly jobId: string; readonly state: string; readonly reason: string }
	| {
			readonly held: false;
			readonly text: string;
			readonly sessionKey: string;
			readonly jobId: string;
			readonly opRef: string;
	  };

/** Operator projection over durable lane jobs (issue #10). */
export interface WorkJobsResult {
	readonly jobs: Array<{
		readonly job_id: string;
		readonly lane_key: string;
		readonly state: string;
		/** Historical authority hold; state remains the recorded historical state. */
		readonly quarantined?: boolean;
		readonly reason?: string;
		readonly branch: string;
		readonly worktree_path: string;
		readonly session_id: string;
		readonly last_activity_at: string | null;
		readonly updated_at: string;
		/** Worktree HEAD (issue #67): progress evidence that survives an op dying; null when unreadable. */
		readonly last_commit: { readonly sha: string; readonly subject: string; readonly committed_at: string } | null;
		/** When the job was accepted; absent on a corrupt record. */
		readonly accepted_at?: string;
		/** The current attempt, a detail of the job; absent on a corrupt record. */
		readonly attempt?: { readonly op_ref: string; readonly started_at: string; readonly ended_at?: string } | null;
		readonly reports?: {
			readonly pending: number;
			readonly claimed: number;
			readonly held: number;
			readonly undeliverable: number;
		};
	}>;
}

/**
 * Live config reload. `changed` are the reloadable fields actually applied,
 * `restartRequired` names edited fields only a restart can apply, and `ignored`
 * names edited fields no code reads at all — so the caller is never told a field
 * took effect when it did not.
 */
export type ConfigReloadResult =
	| {
			readonly ok: true;
			readonly changed: readonly string[];
			readonly restartRequired: readonly string[];
			readonly ignored: readonly string[];
	  }
	| {
			readonly ok: false;
			readonly diagnostics: readonly { readonly code: string; readonly message: string }[];
	  };

/**
 * Outbound reaction (chat.react): react to ONE specific message in ONE specific
 * origin. The target message id is required — "react to the last message" is not
 * expressible, because "last" changes under you. `emoji` accepts any allowlisted
 * Outbound reaction (chat.react): react to ONE specific message in ONE specific
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
	| "monitor_settlement_failed"
	| "monitor_settlement_stuck"
	| "monitor_authoring_lost"
	| "lane_capacity_exhausted"
	| "inbound_starved"
	| "agent_disk_headroom";

/**
 * Free space on the filesystem holding the broker-bound GJC agent directory.
 * GJC owns and never reaps that directory (sessions, recovery snapshots), so
 * the gateway only observes headroom; null bytes mean the probe failed.
 */
export interface AgentDiskView {
	readonly path: string;
	readonly freeBytes: number | null;
	readonly totalBytes: number | null;
}

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
	/** Per-origin unread/omission diagnostics; never includes message bodies. */
	readonly contextDiff: ConversationContextDiagnostics;
	/** Durable metadata-only bootstrap projection; source bodies are never exposed. */
	readonly bootstrap: SessionBootstrapProjection;
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
	/**
	 * Event types whose most recent terminal events (last 24h) exhausted retries
	 * with no authored output: `consecutive` lost slots, newest at `lastFiredAt`.
	 */
	readonly monitorAuthoringLost: readonly {
		readonly eventType: string;
		readonly consecutive: number;
		readonly lastFiredAt: string;
	}[];
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
	/** Aggregate unread/omission diagnostics across origins. */
	readonly contextDiff: ConversationContextDiagnostics;
	/** Worker-lane census against the configured admission cap. */
	readonly lanes: { readonly active: number; readonly max: number };
	/** Agent-directory disk headroom; null when no broker agent directory is bound. */
	readonly agentDisk: AgentDiskView | null;
}

/** Verb catalog: verb name -> { params, result } (documentation-level typing). */
export interface VerbCatalogV01 {
	"gateway.status": { params: undefined; result: GatewayStatusResult };
	"gateway.shutdown": { params: undefined; result: { readonly stopping: true } };
	"gateway.reloadConfig": { params: undefined; result: ConfigReloadResult };
	"chat.send": { params: ChatSendParams; result: ChatSendResult };
	"chat.edit": { params: ChatEditParams; result: ChatEditResult };
	"delivery.confirm": { params: DeliveryConfirmParams; result: { readonly settled: true } };
	"delivery.fail": { params: DeliveryFailParams; result: { readonly recorded: true } };
	"session.recall": { params: SessionRecallParams; result: SessionRecallResult };
	"session.list": { params: undefined; result: SessionListResult };
	"session.modelChoices": { params: undefined; result: SessionModelChoicesResult };
	"memory.audit": { params: undefined; result: MemoryAuditResult };
	"memory.autolink": {
		params: undefined;
		result: { readonly filesChanged: number; readonly linksAdded: number; readonly aliases: number };
	};
	"memory.search": { params: MemorySearchParams; result: MemorySearchResult };
	"monitor.add": { params: MonitorSpec; result: { readonly monitorId: string } };
	"monitor.update": { params: MonitorUpdateParams; result: { readonly monitorId: string } };
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
	"ops.redeliver": { params: OpsRedeliverParams; result: { readonly requeued: readonly string[] } };
	"ops.integrity": { params: undefined; result: { readonly ok: boolean; readonly detail: string } };
	"work.run": { params: WorkRunParams; result: WorkRunResult };
	"work.start": { params: WorkStartParams; result: WorkStartResult };
	"work.status": { params: WorkStatusParams; result: WorkStatusResult };
	"work.steer": { params: WorkSteerParams; result: WorkSteerResult };
	"work.jobs": { result: WorkJobsResult };
	"work.retire": { params: WorkRetireParams; result: WorkRetireResult };
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
	"chat.edit",
	"delivery.confirm",
	"delivery.fail",
	"session.recall",
	"session.list",
	"session.modelChoices",
	"memory.audit",
	"memory.autolink",
	"memory.search",
	"monitor.add",
	"monitor.update",
	"monitor.list",
	"monitor.inspect",
	"monitor.test",
	"monitor.remove",
	"ops.backup",
	"ops.redeliver",
	"ops.integrity",
	"work.run",
	"work.start",
	"work.status",
	"work.steer",
	"work.jobs",
	"work.retire",
	"chat.react",
	"engagement.reaction",
	"gateway.reloadConfig",
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

/** Existing embedded marker grammar; inspect original content before clipping. */
export function containsSilenceToken(text: string): boolean {
	return /\[(SILENT|silent)\]/.test(text);
}

function unbracket(text: string): string {
	return text.startsWith("[") && text.endsWith("]") && text.length > 2 ? text.slice(1, -1).trim() : text;
}
