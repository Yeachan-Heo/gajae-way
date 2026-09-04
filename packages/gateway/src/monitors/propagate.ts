import {
	CATCH_ALL_EVENT_ORIGIN,
	type ChatMessagePayload,
	eventTypeOrigin,
	isSilenceToken,
	type MonitorEventRecord,
	type MonitorRecord,
	type OriginRef,
	originKey,
} from "@gajaeway/protocol";
import type { GjcModelSelection, GjcServiceTier } from "../config";
import type { DeliveryService } from "../delivery/delivery";
import type { MemoryClosureQueue } from "../memory/closure";
import type { SessionPort } from "../orchestrator/session-port";
import type { GatewayDatabase } from "../store/db";
import { MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS, RECONCILABLE_STAGES, TERMINAL_STAGES } from "../store/db";
import {
	type AuthoringFailureClass,
	buildMonitorCompactionDigest,
	type CompactionPort,
	classifyAuthoringFailure,
	classifyExecutorFailure,
	classifyProtocolFailure,
	decideSessionRoll,
	type ExecutorFailureReason,
	isAsideTimeoutFailure,
	isOrphanedExecutorFailure,
	MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD,
	MONITOR_DIGEST_MAX_NOTES,
	MONITOR_PROTOCOL_FAILURE_ROLL_THRESHOLD,
	type MonitorDigestNote,
	type NativeCompactionStatus,
	type ProtocolFailureReason,
	type SessionRollReason,
	unavailableCompactionPort,
} from "./compaction";
import type { MonitorRegistry } from "./registry";

/** Product-level semantics for the seeded maintenance events (generic, persona-independent). */
const MAINTENANCE_GUIDANCE: Record<string, string | undefined> = {
	"memory.canonicalize":
		"For memory.canonicalize events: read the memory tree's daily/ captures, append the durable facts into their canonical axis files (people/ projects/ decisions/ events/ tasks/ channels/ ops/ reflections/ plus any axis registered in the corpus's axes.json), keep every original byte (never delete or summarize-replace, move stray root files into an axis directory with their raw content preserved), and make new files reachable from MEMORY.md. ops/ holds repeatable operating rules, runtime/session/tool procedure, principles distilled from failure, and state the next executor picks up; it is routable, so write into ops/rules/, ops/distillations/ or ops/handoffs/ and never one growing file, and never put raw transcript, secrets or dated small talk there. reflections/ holds what you learned about your own behaviour as dated append-only entries named reflections/YYYY-MM-DD.md (several entries per day are fine, per-subject files are not); each entry records source/time, the observed failure or drift, the invariant learned, why it matters, the concrete next action, and its promotion target - an operating invariant promotes to ops/rules, a cross-incident learning to ops/distillations, and a project or person correction to that axis. ops constrains what to do before acting; decisions/ records what was chosen at a point in time and why. Metadata for the graph: give each canonical file YAML frontmatter with `aliases:` (Korean AND romanized name pairs - a matcher that only knows one script misses the other) and `tags:` (a few stable topic labels; tags power retrieval boosts and Obsidian tag groups). The mechanical crosslinking itself is deterministic: run `gajaeway memory autolink` at the end of your pass - it wraps the first mention of every aliased canonical file in a relative link without touching any other byte. You may still hand-link where judgment is needed, and you may append `근거:`/`관련:` evidence lines at the END of a file when you personally know the causal source (never fabricate causality from date coincidence; existing wording stays untouched). The gateway commits; you only write files.",
	"memory.audit":
		"For memory.audit events: run the memory validator (gajaeway memory audit) and put a one-line pass report or the failure diagnostics plus your repair attempt into the note.",
};

/**
 * Stable, public-safe dispatch failure codes. The raw error message is NEVER
 * persisted or logged — it can carry secrets (tokens, URLs, file paths); the
 * structured code plus the dispatch phase is what the operator gets.
 */
type DispatchFailureCode =
	| "session_bind_failed"
	| "authoring_turn_failed"
	// Context-class authoring failure: empty response, context-length rejection
	// or a zero-token completion. Distinct from `authoring_response_invalid`
	// because only this class is evidence that compaction did not happen.
	| "authoring_context_exhausted"
	| "authoring_response_invalid"
	// Executor-class: the aside collection worker hit its 300s cap. Its own code
	// so it can never be read as, or aggregated with, context exhaustion.
	| "aside_timeout"
	// Executor-class: the child process died on the wrapper's timeout while the
	// external daemon's work kept running, so the next tick stacks a duplicate.
	// Its own code because the operator action is "reclaim the external
	// executor", not anything to do with this session.
	| "orphaned_executor"
	// Executor-class: a worker timeout, external tool failure or lock skip that
	// is neither the aside worker nor an orphaned external run.
	| "executor_failed"
	| "delivery_prepare_failed"
	| "internal_error";

export interface MonitorDispatchFailure {
	readonly eventId: string;
	readonly code: DispatchFailureCode;
}

/**
 * Observable safety-net state for one monitor session origin (issue #68).
 *
 * `turns` is recorded but never a roll trigger — it exists so an operator can
 * see how long a session has been alive next to the failure evidence.
 */
export interface MonitorSessionSafetyState {
	/** Authoring turns taken in the current epoch. OBSERVATIONAL ONLY. */
	turns: number;
	/**
	 * Consecutive context-class authoring failures attributable to the CURRENT
	 * session, with no answered turn since. Reset by any non-empty answer and by
	 * a roll. This is the only counter the roll decision reads.
	 */
	contextFailures: number;
	/**
	 * Context-class failures that were NOT counted because they came from a
	 * stale or replayed event, or from a dead epoch (diagnostic). A dead
	 * session's failures must never roll its successor.
	 */
	staleContextFailures: number;
	/** Executor-class failures in this epoch — timeouts, tools, locks (diagnostic). */
	executorFailures: number;
	/**
	 * Executor-class failures that left external work running behind a dead
	 * child (diagnostic). Never part of the context streak: the session is fine,
	 * the external executor is not.
	 */
	orphanedExecutorFailures: number;
	/**
	 * Coded reason of the last executor-class failure. Survives a roll on
	 * purpose: an unreclaimed external executor is not fixed by rolling a
	 * session, so the signal must not be wiped by one.
	 */
	lastExecutorReason: ExecutorFailureReason | undefined;
	/** Protocol-class failures in this epoch — malformed or off-contract answers (diagnostic). */
	protocolFailures: number;
	/**
	 * Coded reason of the last protocol-class failure. The offending answer text
	 * is never logged (it can carry secrets), so this coded rule name is the only
	 * diagnosis an operator gets — without it, an off-contract session is
	 * indistinguishable from any other invalid answer (measured: jip-gajae
	 * `sns-threads`, 19 identical failures in one day).
	 */
	lastProtocolReason: ProtocolFailureReason | undefined;
	/** Result of the last native-compaction attempt, or undefined if never attempted. */
	nativeCompaction: NativeCompactionStatus | undefined;
	/** Armed roll: consumed at the next dispatch boundary. */
	pendingRoll: SessionRollReason | undefined;
	/** Reason of the last roll actually performed. */
	lastRoll: SessionRollReason | undefined;
}

export class MonitorPropagator {
	readonly #database: GatewayDatabase;
	readonly #registry: MonitorRegistry;
	readonly #sessionPort: SessionPort;
	readonly #memory: MemoryClosureQueue;
	readonly #delivery: DeliveryService;
	readonly #emit: (event: MonitorEventRecord) => void;
	readonly #ownerTarget: { readonly origin: OriginRef } | undefined;
	readonly #deliver: ((payload: ChatMessagePayload) => void) | undefined;
	readonly #now: () => number;
	readonly #acquireLease: (eventId: string, owner: string, leaseId: string, ttlMs: number, now: number) => boolean;
	readonly #fencedUpdate: (eventId: string, leaseId: string, batchId: string, now: number) => boolean;
	#batches = new Map<string, { eventIds: string[]; timer: ReturnType<typeof setTimeout> }>();
	/** Event ids this process is dispatching right now, so reconcile never double-runs them. */
	#inFlight = new Set<string>();
	/** Re-entrancy guard: only one reconcile sweep may run at a time in this process. */
	#reconciling = false;
	/** In-flight dispatch promises per event, for awaitable submission. */
	#inFlightPromises = new Map<string, Promise<void>>();
	#closing = false;
	/** Per-origin serialization lives in SessionPort, shared with all SDK callers. */
	readonly #repo: string;
	readonly #model: GjcModelSelection | undefined;
	readonly #serviceTier: GjcServiceTier | undefined;
	readonly #appliedModels = new Map<string, string>();
	readonly #appliedServiceTiers = new Map<string, GjcServiceTier>();
	/**
	 * Per-session-origin safety-net state (issue #68). Process-local on purpose:
	 * it is evidence about the CURRENT live session, and a restart mints a fresh
	 * bind anyway, so persisting it would only carry a stale verdict forward.
	 */
	readonly #safety = new Map<string, MonitorSessionSafetyState>();
	/** THE ONE seam that asks for native compaction. */
	readonly #compaction: CompactionPort;
	/** Consecutive context-class authoring failures required before a roll. */
	readonly #contextFailureRollThreshold: number;
	/** Consecutive protocol-class (off-contract) failures required before a roll. */
	readonly #protocolFailureRollThreshold: number;
	constructor(options: {
		database: GatewayDatabase;
		registry: MonitorRegistry;
		sessionPort: SessionPort;
		memory: MemoryClosureQueue;
		delivery: DeliveryService;
		emit: (event: MonitorEventRecord) => void;
		/** Default recipient for monitors without their own channel target. */
		ownerTarget?: { readonly origin: OriginRef };
		/** Broadcasts a prepared chat.message to live adapter connections. */
		deliver?: (payload: ChatMessagePayload) => void;
		/** Workspace used for broker-session creation and all SDK queries. */
		repo?: string;
		/** Model/preset and request tier applied to dedicated monitor authoring sessions. */
		model?: GjcModelSelection;
		serviceTier?: GjcServiceTier;
		/** Injectable clock for deterministic lease expiry/renewal in tests. */
		now?: () => number;
		/**
		 * Injectable lease-acquire (test/ops seam): defaults to the durable DB
		 * claim. Tests may wrap it to force deterministic claim outcomes while
		 * keeping the production dispatch path identical.
		 */
		acquireLease?: (eventId: string, owner: string, leaseId: string, ttlMs: number, now: number) => boolean;
		/** Injectable fenced-batching seam (tests): defaults to the durable fenced update. */
		fencedUpdate?: (eventId: string, leaseId: string, batchId: string, now: number) => boolean;
		/** Native-compaction seam. Defaults to the honest `unavailable` port. */
		compaction?: CompactionPort;
		/**
		 * Consecutive context-class authoring failures required before the
		 * safety net rolls a session. Config-supplied
		 * (`monitorContextFailureRollThreshold`); defaults to
		 * MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD.
		 */
		contextFailureRollThreshold?: number;
		/**
		 * Consecutive protocol-class failures required before the safety net rolls
		 * a session that keeps answering off contract. Defaults to
		 * MONITOR_PROTOCOL_FAILURE_ROLL_THRESHOLD.
		 */
		protocolFailureRollThreshold?: number;
	}) {
		this.#database = options.database;
		this.#model = options.model;
		this.#serviceTier = options.serviceTier;
		this.#registry = options.registry;
		this.#sessionPort = options.sessionPort;
		this.#memory = options.memory;
		this.#delivery = options.delivery;
		this.#emit = options.emit;
		this.#ownerTarget = options.ownerTarget;
		this.#deliver = options.deliver;
		this.#now = options.now ?? (() => Date.now());
		this.#acquireLease =
			options.acquireLease ??
			((eventId, owner, leaseId, ttlMs, at) =>
				this.#database.monitorEventAcquireLease(eventId, owner, leaseId, ttlMs, at));
		this.#fencedUpdate =
			options.fencedUpdate ??
			((eventId, lease, batch, at) => this.#database.monitorEventFencedUpdate(eventId, lease, "batched", batch, at));
		this.#compaction = options.compaction ?? unavailableCompactionPort;
		this.#contextFailureRollThreshold = options.contextFailureRollThreshold ?? MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD;
		this.#protocolFailureRollThreshold =
			options.protocolFailureRollThreshold ?? MONITOR_PROTOCOL_FAILURE_ROLL_THRESHOLD;
		this.#repo = options.repo ?? process.cwd();
	}
	/** Cancels pending burst timers so no new dispatch starts during shutdown. */
	dispose(): void {
		this.#closing = true;
		for (const batch of this.#batches.values()) clearTimeout(batch.timer);
		this.#batches.clear();
	}
	/** Waits for every live dispatch and reconcile writer before the database closes. */
	async drain(): Promise<void> {
		this.dispose();
		while (this.#reconciling || this.#inFlightPromises.size > 0) {
			const active = [...new Set(this.#inFlightPromises.values())];
			if (active.length > 0) await Promise.all(active);
			else await Bun.sleep(1);
		}
	}
	submit(monitorId: string, eventType: string, payload: unknown): string {
		if (this.#closing) throw new Error("monitor propagator is closing");
		const monitor = this.#registry.get(monitorId);
		if (!monitor?.enabled) throw new Error("unknown or disabled monitor");
		if (typeof eventType !== "string" || !eventType) throw new Error("event type is required");
		const eventId = crypto.randomUUID();
		const firedAt = new Date().toISOString();
		this.#database.withTransaction(() =>
			this.#database.monitorEventCreate({
				eventId,
				monitorId,
				eventType,
				payloadJson: JSON.stringify(payload ?? null),
				firedAt,
			}),
		);
		this.#emit({ eventId, monitorId, eventType, firedAt, stage: "admitted" });
		const key = `${monitorId}\u0000${eventType}`;
		if (monitor.burstPolicy === "serialize") {
			void this.#dispatch([eventId]);
			return eventId;
		}
		const previous = this.#batches.get(key);
		if (previous) {
			if (monitor.burstPolicy === "drop") previous.eventIds.splice(0, previous.eventIds.length, eventId);
			else if (monitor.burstPolicy === "dedupe") {
				const seen = previous.eventIds.some(
					(id) =>
						this.#database.monitorEventRows().find((row) => row.event_id === id)?.payload_json ===
						JSON.stringify(payload ?? null),
				);
				if (!seen) previous.eventIds.push(eventId);
				else this.#database.monitorEventUpdate(eventId, "batched", "deduped");
			} else previous.eventIds.push(eventId);
			return eventId;
		}
		const batch = {
			eventIds: [eventId],
			timer: setTimeout(() => {
				this.#batches.delete(key);
				void this.#dispatch(batch.eventIds);
			}, 250),
		};
		this.#batches.set(key, batch);
		return eventId;
	}
	/**
	 * Awaitable submission (bounded test/ops seam): performs the SAME admission
	 * and burst/dispatch semantics as submit(), but resolves when THIS event's
	 * dispatch chain has fully settled — the exact original promise, never a
	 * second dispatch.
	 */
	async submitAwaitable(monitorId: string, eventType: string, payload: unknown): Promise<string> {
		const monitor = this.#registry.get(monitorId);
		if (!monitor?.enabled) throw new Error("unknown or disabled monitor");
		// Honest contract: this seam is only exact for `serialize` monitors, whose
		// dispatch starts immediately — the returned promise is the event's real
		// in-flight chain. For burst policies (coalesce/dedupe/drop) the dispatch
		// is deferred by the burst window, so no in-flight promise exists yet;
		// fail loudly instead of pretending to be awaitable.
		if (monitor.burstPolicy !== "serialize") {
			throw new Error("submitAwaitable only supports burstPolicy 'serialize'");
		}
		const eventId = this.submit(monitorId, eventType, payload);
		const inflight = this.#inFlightPromises.get(eventId);
		if (!inflight) throw new Error("dispatch promise missing for serialize event");
		await inflight;
		return eventId;
	}
	/**
	 * Cron slot admission (red-team blockers 2+3): the slot claim and the event
	 * row are created in ONE transaction, and the event's `fired_at` IS the
	 * exact scheduled slot timestamp — the durable record carries the scheduled
	 * identity, not just the payload. Returns the eventId, or null when the
	 * slot was already claimed (duplicate tick / restart catch-up overlap).
	 */
	submitSlot(monitorId: string, eventType: string, payload: unknown, slotAt: Date): string | null {
		const monitor = this.#registry.get(monitorId);
		if (!monitor?.enabled) throw new Error("unknown or disabled monitor");
		// Round-4 blocker 4: a monitor must never backfill slots scheduled before
		// it existed — catch-up admission is clamped to monitor.createdAt.
		if (slotAt.getTime() < Date.parse(monitor.createdAt)) return null;
		const eventId = crypto.randomUUID();
		const admitted = this.#database.monitorSlotClaimWithEvent({
			monitorId,
			slotAt: slotAt.toISOString(),
			eventId,
			eventType,
			payloadJson: JSON.stringify(payload ?? null),
		});
		if (!admitted) return null;
		this.#emit({ eventId, monitorId, eventType, firedAt: slotAt.toISOString(), stage: "admitted" });
		const key = `${monitorId}\u0000${eventType}`;
		if (monitor.burstPolicy === "serialize") {
			void this.#dispatch([eventId]);
			return eventId;
		}
		const previous = this.#batches.get(key);
		if (previous) {
			if (monitor.burstPolicy === "drop") previous.eventIds.splice(0, previous.eventIds.length, eventId);
			else previous.eventIds.push(eventId);
			return eventId;
		}
		const batch = {
			eventIds: [eventId],
			timer: setTimeout(() => {
				this.#batches.delete(key);
				void this.#dispatch(batch.eventIds);
			}, 250),
		};
		this.#batches.set(key, batch);
		return eventId;
	}
	/**
	 * Recovery sweep. Oldest-first, at-most-one concurrent sweep per process:
	 * - `batched` rows not in this process's in-flight set are orphans of a dead
	 *   dispatch and are reclaimed exactly like `admitted`/`dispatched`/`failed`
	 *   (the fix for issue #29's stranded canonicalize events).
	 * - events with an authored output but no memory intent get their memory
	 *   closure re-enqueued (a crash between authoring and enqueue).
	 * - terminal rows are skipped.
	 * The reclaim budget (MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS) bounds retries:
	 * an event that keeps failing lands on `failed_no_retry` — operator-visible,
	 * never an infinite dispatch loop.
	 */
	async reconcile(): Promise<void> {
		if (this.#reconciling) {
			// Yield once so a caller racing the first sweep observes its claimed work
			// rather than an artificial pre-bind microtask gap in the shared port.
			await Bun.sleep(0);
			return;
		}
		this.#reconciling = true;
		try {
			// Legacy split-state repair: a crash between ledger confirm and batch
			// settlement (pre-atomic path) can leave confirmed deliveries with
			// authored events. Repair them deterministically on startup.
			for (const delivery of this.#database.deliveryRows()) {
				if (delivery.state !== "confirmed") continue;
				const batch = this.#database.monitorEventRows().filter((row) => row.batch_id === delivery.turn_id);
				const needsRepair = batch.some((row) => row.stage === "authored");
				if (!needsRepair) continue;
				this.#database.withTransaction(() => {
					for (const row of batch)
						if (row.stage === "authored") this.#database.monitorEventUpdate(row.event_id, "delivered");
				});
			}
			// Replay oldest-first: recovery must re-author events in the order they fired.
			for (const row of this.#database.monitorEventRows(undefined, "oldest")) {
				if ((TERMINAL_STAGES as readonly string[]).includes(row.stage)) continue;
				const output = this.#database.authoredOutput(row.event_id);
				const hasMemory = this.#database
					.memoryIntentRows()
					.some((intent) => intent.kind === "monitor-event" && intent.payload_json.includes(row.event_id));
				if (output && !hasMemory) this.#author(row.event_id, output, row.stage === "authored_no_delivery", row);
				else if (!output && this.#recoverable(row)) {
					// Red-team blocker 1: a live dispatch lease owned by ANOTHER attempt
					// means the authoring turn may still complete elsewhere; a new
					// process must not re-author concurrently. Only unclaimed events
					// are reclaimed here (the dispatcher acquires its own lease).
					if (this.#database.monitorEventLiveLeaseOwner(row.event_id, this.#now())) continue;
					if (row.dispatch_attempts >= MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS) {
						this.#database.withTransaction(() =>
							this.#database.monitorEventUpdate(row.event_id, "failed_no_retry", null),
						);
						continue;
					}
					this.#database.monitorEventIncrementAttempts(row.event_id);
					await this.#dispatch([row.event_id]);
				}
			}
		} finally {
			this.#reconciling = false;
		}
	}

	/**
	 * `batched` is a live stage: a dispatch that is still awaiting its authoring turn sits there
	 * for minutes. It is also where an event is stranded forever if the process dies mid-dispatch,
	 * and reconcile used to skip it, so a restart during canonicalization silently killed that run
	 * (observed: two `memory.canonicalize` events stuck at `batched`, one for six hours).
	 * The in-flight set tells the two apart exactly — anything batched that this process is not
	 * currently dispatching is an orphan, including every batched row after a restart.
	 */
	#recoverable(row: { stage: string; event_id: string }): boolean {
		if ((TERMINAL_STAGES as readonly string[]).includes(row.stage)) return false;
		if (!RECONCILABLE_STAGES.includes(row.stage as never) && row.stage !== "authored_no_delivery") return false;
		if (row.stage === "authored_no_delivery") return false;
		if (row.stage === "batched") return !this.#inFlight.has(row.event_id);
		return ["admitted", "dispatched", "failed"].includes(row.stage);
	}
	async #dispatch(eventIds: string[]): Promise<void> {
		for (const id of eventIds) this.#inFlight.add(id);
		// Track the exact in-flight promise per event so awaitable submission can
		// await the ORIGINAL chain (no second dispatch, no double authoring).
		const promise = this.#dispatchBatch(eventIds).finally(() => {
			for (const id of eventIds) {
				this.#inFlight.delete(id);
				if (this.#inFlightPromises.get(id) === promise) this.#inFlightPromises.delete(id);
			}
		});
		for (const id of eventIds) this.#inFlightPromises.set(id, promise);
		await promise;
	}
	async #dispatchBatch(eventIds: string[]): Promise<void> {
		const rows = this.#database.monitorEventRows().filter((row) => eventIds.includes(row.event_id));
		if (!rows.length) return;
		const monitor = this.#registry.get(rows[0]?.monitor_id);
		if (!monitor) return;
		const batchId = crypto.randomUUID();
		// Red-team blocker 1: acquire a durable lease per event BEFORE claiming the
		// batch. The lease survives this process (owner token + expiry), so a new
		// gateway process whose reconcile sees the stranded `batched` rows will NOT
		// re-author while this attempt is live. Events whose lease is held by a
		// live claim from another attempt are skipped here.
		const now = this.#now;
		const owner = `gateway:${process.pid}`;
		const leaseId = crypto.randomUUID();
		const leaseTtlMs = 10 * 60_000;
		const leased: typeof rows = [];
		for (const row of rows) {
			// The atomic claim is the ONLY gate: acquireLease fails when a live
			// lease exists (check-to-claim race impossible inside one statement).
			if (this.#acquireLease(row.event_id, owner, leaseId, leaseTtlMs, now())) {
				leased.push(row);
			}
		}
		if (!leased.length) return;
		// Fence the initial stage claim: only rows we still own transition.
		const claimed: typeof leased = [];
		for (const row of leased) {
			if (this.#fencedUpdate(row.event_id, leaseId, batchId, now())) claimed.push(row);
		}
		if (!claimed.length) {
			// Ownership stolen between claim and batching: release every acquired
			// lease and bail — no heartbeat, no dispatch, no writes.
			for (const row of leased) this.#database.monitorEventReleaseLease(row.event_id, leaseId);
			return;
		}
		// Heartbeat: renew the lease while the authoring turn is in flight so long
		// turns (observed 14m+ canonicalizations) never expire mid-flight, while a
		// dead owner's lease still times out (bounded expiry = TTL after the last
		// heartbeat).
		const stopHeartbeat = this.#startLeaseHeartbeat(
			claimed.map((row) => row.event_id),
			leaseId,
			leaseTtlMs,
		);
		const declared = new Set(monitor.eventTypes);
		const sessionOrigin = declared.has(claimed[0]?.event_type)
			? eventTypeOrigin(claimed[0]!.event_type)
			: CATCH_ALL_EVENT_ORIGIN;
		const sessionOriginKey = originKey(sessionOrigin);
		// One generic port owns all same-origin authoring serialization; monitor
		// leases remain active while a prior call is awaiting a terminal receipt.
		await this.#sessionPort.runExclusive(sessionOriginKey, async () => {
			// Bound outside the try so the failure handler can ask the compaction port
			// to act on the very session that failed, and can tell whether that
			// session is still the live one.
			let boundSessionId: string | undefined;
			let boundSessionEpoch: number | undefined;
			// A replayed batch: reconcile bumps dispatch_attempts before re-dispatching
			// a stranded or failed event, so a non-zero count means these events are
			// not this session's own fresh work. Their context failure says nothing
			// about the CURRENT session and must not feed its streak — the payload is
			// old, and the failure may well have been produced against a session that
			// no longer exists.
			const replayedBatch = claimed.some((row) => row.dispatch_attempts > 0);
			try {
				// Safety-net roll boundary (issue #68). It sits HERE, after the
				// per-origin turn chain has been acquired and before this batch's
				// session is bound: the previous batch's authoring turn has already
				// settled, and this batch's events are claimed under live leases but
				// not yet authored. A roll can therefore neither strand an in-flight
				// batch nor let one be authored twice — the events simply land in the
				// new epoch's session. Leases and fencing are untouched.
				const digest = this.#rollSessionIfArmed(sessionOriginKey, JSON.stringify(sessionOrigin), monitor);
				const boundEpoch = this.#database.getSessionRecord(sessionOriginKey)?.epoch ?? 0;
				const effectiveModel = (monitor.model as GjcModelSelection | undefined) ?? this.#model;
				const effectiveServiceTier = (monitor.serviceTier as GjcServiceTier | undefined) ?? this.#serviceTier;
				const binding = await this.#sessionPort.bind({
					originKey: sessionOriginKey,
					epoch: boundEpoch,
					repo: this.#repo,
					...(effectiveModel ? { model: effectiveModel } : {}),
				});
				const { sessionId } = binding;
				boundSessionId = sessionId;
				boundSessionEpoch = binding.epoch;
				const modelKey = effectiveModel
					? typeof effectiveModel === "string"
						? effectiveModel
						: `preset:${effectiveModel.preset}`
					: undefined;
				if (effectiveModel && !binding.startupModelApplied && this.#appliedModels.get(sessionId) !== modelKey) {
					await this.#sessionPort.setModel({ sessionId, repo: this.#repo, selection: effectiveModel });
					this.#appliedModels.set(sessionId, modelKey!);
				} else if (effectiveModel && binding.startupModelApplied) {
					this.#appliedModels.set(sessionId, modelKey!);
				}
				if (effectiveServiceTier && this.#appliedServiceTiers.get(sessionId) !== effectiveServiceTier) {
					await this.#sessionPort.setServiceTier({ sessionId, repo: this.#repo, tier: effectiveServiceTier });
					this.#appliedServiceTiers.set(sessionId, effectiveServiceTier);
				}
				// Guidance order: the monitor's own instruction first (it is what the
				// owner actually asked this monitor to do), then any built-in
				// maintenance semantics for the claimed event types. Without either,
				// the authoring turn only gets the receipt-note contract.
				const maintenance = claimed
					.map((row) => MAINTENANCE_GUIDANCE[row.event_type])
					.filter((entry, index, all) => entry && all.indexOf(entry) === index);
				const guidance = [monitor.instruction?.trim() || undefined, ...maintenance].filter(Boolean).join(" ");
				const prompt = `Author monitor events.${guidance ? ` ${guidance}` : ""}${digest ? `\n${digest}\n` : ""} Respond ONLY with a JSON array containing exactly one {"eventId","note"} entry per event: ${JSON.stringify(claimed.map((row) => ({ eventId: row.event_id, eventType: row.event_type, payload: JSON.parse(row.payload_json) })))}`;
				const opRef = `gw-m-${batchId.replaceAll("-", "")}`;
				const response = (
					await this.#sessionPort.request({
						sessionId,
						repo: this.#repo,
						originKey: sessionOriginKey,
						text: prompt,
						opRef,
					})
				).assistant.text;
				// The authoring turn is now part of the session transcript whatever its
				// content, so it is counted here rather than after the response is
				// validated. The count is OBSERVATIONAL: it is reported, and it never
				// rolls a session on its own — native gjc compaction owns keeping the
				// context bounded while the session is answering.
				const turns = this.#database.incrementTurnCount(sessionOriginKey, JSON.stringify(sessionOrigin));
				this.#safetyState(sessionOriginKey).turns = turns;
				// A non-empty answer is proof the session still has usable context, so
				// it clears the context-failure streak. A malformed answer is still an
				// answer: it must not arm the safety net.
				if (response.trim()) this.#safetyState(sessionOriginKey).contextFailures = 0;
				else throw new Error("authoring response is empty");
				// Lease fencing: after an await, this attempt may no longer own the
				// claim (expired + stolen). Every write below is conditional on the
				// live lease; a stale attempt's completion becomes a no-op.
				// Atomic fencing: each stage write carries its live-lease check in the
				// same UPDATE, so a lease stolen between check and write cannot be
				// exploited (TOCTOU-free). `fenced` = rows whose write landed.
				const fenced = claimed.filter((row) =>
					this.#database.monitorEventFencedUpdate(row.event_id, leaseId, "dispatched", batchId),
				);
				if (!fenced.length) return;
				const authored = parseAuthoredArray(response) as Array<{ eventId?: unknown; note?: unknown }>;
				if (!Array.isArray(authored)) throw new Error("authoring response is not an array");
				// Strict response contract: exactly one valid entry per claimed event —
				// a partial/missing/duplicate/extra response is a structured failure.
				// Omitted events must stay recoverable (dispatched/failed), never
				// silently delivered alongside their batch.
				const claimedIds = new Set(claimed.map((row) => row.event_id));
				const seenIds = new Set<string>();
				for (const entry of authored) {
					if (typeof entry.eventId !== "string" || typeof entry.note !== "string") {
						throw new Error("authoring response entry missing eventId or note");
					}
					if (!claimedIds.has(entry.eventId)) {
						throw new Error(`authoring response contains unknown event ${entry.eventId}`);
					}
					if (seenIds.has(entry.eventId)) {
						throw new Error(`authoring response duplicates event ${entry.eventId}`);
					}
					seenIds.add(entry.eventId);
				}
				for (const id of claimedIds) {
					if (!seenIds.has(id)) throw new Error(`authoring response omits event ${id}`);
				}
				for (const entry of authored)
					if (
						typeof entry.eventId === "string" &&
						fenced.some((row) => row.event_id === entry.eventId) &&
						typeof entry.note === "string"
					) {
						const row = this.#database.monitorEventRows().find((candidate) => candidate.event_id === entry.eventId);
						if (!row) continue;
						const intentId = `monitor-event-intent:${entry.eventId}`;
						const authoredOk = this.#database.monitorEventFencedAuthorWithIntent(
							entry.eventId,
							leaseId,
							entry.note,
							false,
							row.event_type,
							intentId,
							JSON.stringify(eventTypeOrigin(row.event_type)),
						);
						if (!authoredOk) continue;
						// Wake the closure queue so the atomically admitted intent is
						// processed in the same run (no restart needed).
						this.#memory.enqueueExistingId(intentId);
						this.#emit({
							eventId: entry.eventId,
							monitorId: monitor.monitorId,
							eventType: row.event_type,
							firedAt: row.fired_at,
							stage: "authored",
						});
					}
				// A monitor without its own channel target reports to the configured owner
				// target when one exists: a personal agent's maintenance and event notes go
				// to the owner by default rather than vanishing into the logs. With no
				// target at all the event settles terminally as `authored_no_delivery`
				// instead of pretending a delivery is still pending (issue #29 defect 3).
				const target = monitor.channelTarget ?? this.#ownerTarget;
				if (!target) {
					for (const row of fenced) {
						if (this.#database.authoredOutput(row.event_id) === undefined) continue;
						this.#database.monitorEventFencedUpdate(row.event_id, leaseId, "authored_no_delivery", batchId);
					}
					return;
				}
				// Fenced delivery admission: the ledger insert is conditional on every
				// fenced event still holding its lease (same transaction). A stale
				// attempt therefore cannot emit a second delivery.
				const deliveryId = crypto.randomUUID();
				const deliveryText = authored
					.filter(
						(entry): entry is { eventId: string; note: string } =>
							typeof entry.eventId === "string" &&
							typeof entry.note === "string" &&
							fenced.some((row) => row.event_id === entry.eventId),
					)
					.map((entry) => entry.note)
					.join("\n");
				if (!isSilenceToken(deliveryText)) {
					const origin = target.origin;
					const payload: ChatMessagePayload = {
						turnId: batchId,
						origin,
						role: "assistant",
						text: deliveryText,
						final: true,
						deliveryId,
					};
					const admitted = this.#database.monitorDeliveryPrepareFenced(
						deliveryId,
						batchId,
						originKey(origin),
						JSON.stringify(payload),
						fenced.map((row) => row.event_id),
						leaseId,
					);
					if (!admitted) return;
					this.#delivery.markInflight(deliveryId);
					// Push to live adapters NOW: without this the note sat in the ledger
					// until the next adapter reconnect flushed redeliveries (live finding:
					// owner-DM canonicalize note stuck inflight for minutes).
					this.#deliver?.(payload);
				}
			} catch (error) {
				// Public-safe structured evidence only: a stable phase code and event ids.
				// The raw error body can carry secrets and is never persisted or logged.
				const failureClass = classifyAuthoringFailure(error);
				const code: DispatchFailureCode = failureCode(error, failureClass);
				for (const row of leased) {
					this.#database.monitorEventFencedFail(
						row.event_id,
						leaseId,
						batchId,
						code,
						// #64: the detail must carry the actual cause (sanitized), not echo the code.
						`dispatch phase failed (${code}): ${failureDetail(error)}`,
					);
				}
				console.error(`monitor dispatch failed (${code}): events ${claimed.map((row) => row.event_id).join(",")}`);
				await this.#recordAuthoringFailure({
					sessionOriginKey,
					failureClass,
					// Passed for PURE classification only (executor sub-kind). The message
					// is never logged or persisted from here.
					error,
					sessionId: boundSessionId,
					boundEpoch: boundSessionEpoch,
					replayed: replayedBatch,
					monitor,
				});
			} finally {
				stopHeartbeat();
				// Release the leases this attempt holds. Lease-guarded: if this attempt
				// expired and another process stole the claim, this release is a no-op,
				// and a stale attempt's completion can never overwrite the newer claim.
				for (const row of leased) this.#database.monitorEventReleaseLease(row.event_id, leaseId);
			}
		});
	}
	/** Read-only safety-net evidence for one session origin (ops/tests). */
	sessionSafetyState(sessionOriginKey: string): MonitorSessionSafetyState {
		return { ...this.#safetyState(sessionOriginKey) };
	}
	#safetyState(sessionOriginKey: string): MonitorSessionSafetyState {
		const existing = this.#safety.get(sessionOriginKey);
		if (existing) return existing;
		const fresh: MonitorSessionSafetyState = {
			turns: 0,
			contextFailures: 0,
			staleContextFailures: 0,
			orphanedExecutorFailures: 0,
			lastExecutorReason: undefined,
			executorFailures: 0,
			protocolFailures: 0,
			lastProtocolReason: undefined,
			nativeCompaction: undefined,
			pendingRoll: undefined,
			lastRoll: undefined,
		};
		this.#safety.set(sessionOriginKey, fresh);
		return fresh;
	}
	/**
	 * THE ONE monitor-session failure classifier and native-compaction request
	 * point (issue #68).
	 *
	 * Only a context-class failure of the CURRENT session's own fresh work can
	 * feed the streak. Everything else is recorded and dropped:
	 * - executor-class (aside worker timeout, orphaned external run, tool
	 *   failure, lock skip) and protocol-class (malformed or off-contract
	 *   answer) are not evidence about context size at all. The executor
	 *   sub-kind is named for the operator (`lastExecutorReason`), and the
	 *   orphaned case is counted separately because its remedy is reclaiming an
	 *   external daemon job, not touching this session.
	 * - a replayed batch (reconcile revived it) carries an old payload and may
	 *   have failed against a session that is already gone.
	 * - a bound epoch that is no longer current means the failure belongs to a
	 *   dead session; its record must never roll the successor.
	 *
	 * When it does count, native compaction is requested first and the safety net
	 * arms only if that did not succeed AND the streak has reached
	 * `contextFailureRollThreshold` consecutive failures.
	 */
	async #recordAuthoringFailure(input: {
		sessionOriginKey: string;
		failureClass: AuthoringFailureClass;
		/** Inspected by pure classifiers only; never logged, never persisted. */
		error: unknown;
		sessionId: string | undefined;
		boundEpoch: number | undefined;
		replayed: boolean;
		monitor: MonitorRecord;
	}): Promise<void> {
		const { sessionOriginKey, failureClass, error, sessionId, boundEpoch, replayed, monitor } = input;
		const state = this.#safetyState(sessionOriginKey);
		if (failureClass === "executor") {
			state.executorFailures += 1;
			const reason = classifyExecutorFailure(error);
			state.lastExecutorReason = reason;
			if (reason === "executor_orphaned_external_work") {
				state.orphanedExecutorFailures += 1;
				// Name it and stop. The session answered nothing wrong; an external
				// daemon job outlived its child process and the next tick will stack
				// another one. Rolling the session would hide that, not fix it.
				console.error(
					`monitor executor orphaned for ${sessionOriginKey} (monitor ${monitor.monitorId}): reason=${reason} orphaned_failures=${state.orphanedExecutorFailures} context_failures=${state.contextFailures} (unchanged) — external executor work was not reclaimed; the session is left untouched.`,
				);
			}
			return;
		}
		if (failureClass === "protocol") {
			state.protocolFailures += 1;
			const protocolReason = classifyProtocolFailure(error);
			state.lastProtocolReason = protocolReason;
			// The answer itself may never be logged, so name the violated rule and
			// the streak instead: that is the whole diagnosis an operator gets.
			console.error(
				`monitor answer off contract for ${sessionOriginKey} (monitor ${monitor.monitorId}): reason=${protocolReason} protocol_failures=${state.protocolFailures}/${this.#protocolFailureRollThreshold} context_failures=${state.contextFailures} (unchanged)`,
			);
			// A protocol failure says nothing about context size, so it never feeds
			// the context streak and never requests compaction. It does get its own
			// remedy: an epoch's worth of identical off-contract answers is a session
			// that cannot follow its own contract any more, and before this the
			// gateway retried it forever with no remediation at all (measured:
			// jip-gajae `sns-threads`, 19/19 ticks in one day).
			const staleBinding =
				replayed ||
				boundEpoch === undefined ||
				boundEpoch !== (this.#database.getSessionRecord(sessionOriginKey)?.epoch ?? 0);
			if (!staleBinding && state.protocolFailures >= this.#protocolFailureRollThreshold) {
				state.pendingRoll = "protocol_failures_off_contract";
				console.error(
					`monitor session safety net armed for ${sessionOriginKey} (monitor ${monitor.monitorId}): reason=protocol_failures_off_contract protocol_failures=${state.protocolFailures}/${this.#protocolFailureRollThreshold} last_reason=${protocolReason} turns=${state.turns}`,
				);
			}
			return;
		}
		const liveEpoch = this.#database.getSessionRecord(sessionOriginKey)?.epoch ?? 0;
		if (replayed || boundEpoch === undefined || boundEpoch !== liveEpoch) {
			state.staleContextFailures += 1;
			console.error(
				`monitor context failure not counted for ${sessionOriginKey} (monitor ${monitor.monitorId}): replayed=${replayed} bound_epoch=${boundEpoch ?? "none"} live_epoch=${liveEpoch}`,
			);
			return;
		}
		state.contextFailures += 1;
		const status = sessionId ? await this.#requestNativeCompaction(sessionId) : "unavailable";
		state.nativeCompaction = status;
		const reason = decideSessionRoll({
			consecutiveContextFailures: state.contextFailures,
			threshold: this.#contextFailureRollThreshold,
			nativeCompaction: status,
		});
		if (!reason) return;
		// Arm, do not roll here: the roll must happen at the dispatch boundary
		// where no batch is in flight.
		state.pendingRoll = reason;
		console.error(
			`monitor session safety net armed for ${sessionOriginKey} (monitor ${monitor.monitorId}): reason=${reason} context_failures=${state.contextFailures}/${this.#contextFailureRollThreshold} native_compaction=${status} turns=${state.turns}`,
		);
	}
	/**
	 * Asks the runtime to compact this session natively — the first line of
	 * defence, and the ONLY place the gateway requests it. A port that throws did
	 * not compact, so it reports `failed`; the thrown message never escapes.
	 */
	async #requestNativeCompaction(sessionId: string): Promise<NativeCompactionStatus> {
		try {
			return (await this.#compaction.run(sessionId)).status;
		} catch {
			return "failed";
		}
	}
	/**
	 * THE ONE monitor-session roll choke point (issue #68). LAST RESORT: it fires
	 * only for a roll armed by `#recordAuthoringFailure`, never on turn count.
	 *
	 * Returns the digest to inject into this turn's prompt, or undefined when no
	 * roll was armed. The digest is pure text assembly — a roll costs no model
	 * turn.
	 */
	#rollSessionIfArmed(sessionOriginKey: string, originRefJson: string, monitor: MonitorRecord): string | undefined {
		const state = this.#safetyState(sessionOriginKey);
		const reason = state.pendingRoll;
		if (!reason) return undefined;
		// Build the digest BEFORE the roll: it reads durable authored notes, which
		// the roll does not touch, but reading first keeps the ordering obvious.
		const digest = buildMonitorCompactionDigest({
			monitorName: monitor.name,
			instruction: monitor.instruction,
			notes: this.#recentAuthoredNotes(monitor.monitorId),
		});
		// bumpEpoch is the existing rotation primitive: epoch + 1 and turn_count 0.
		// The next broker-backed SessionPort bind mints a fresh session and
		// idempotency key for this monitor origin.
		this.#database.withTransaction(() => this.#database.bumpEpoch(sessionOriginKey, originRefJson));
		state.pendingRoll = undefined;
		state.lastRoll = reason;
		// The new epoch starts with a clean slate: the previous session's failure
		// evidence is about a session that no longer exists.
		state.contextFailures = 0;
		state.executorFailures = 0;
		state.orphanedExecutorFailures = 0;
		state.protocolFailures = 0;
		state.turns = 0;
		console.error(
			`monitor session rolled for ${sessionOriginKey} (monitor ${monitor.monitorId}): reason=${reason} native_compaction=${state.nativeCompaction ?? "not_attempted"} digest=${digest.length}B.`,
		);
		return digest;
	}
	/** Newest authored notes for one monitor, newest first, bounded by the digest budget. */
	#recentAuthoredNotes(monitorId: string): MonitorDigestNote[] {
		const notes: MonitorDigestNote[] = [];
		for (const row of this.#database.monitorEventRows(monitorId)) {
			if (notes.length >= MONITOR_DIGEST_MAX_NOTES) break;
			const note = this.#database.authoredOutput(row.event_id);
			if (note === undefined) continue;
			notes.push({ eventType: row.event_type, firedAt: row.fired_at, note });
		}
		return notes;
	}
	/**
	 * Renews every held lease on an interval (TTL/3) so the claim stays live for
	 * the duration of a long authoring turn without extending unboundedly after
	 * owner death. Returns a stop function.
	 */
	#startLeaseHeartbeat(eventIds: string[], leaseId: string, ttlMs: number): () => void {
		const timer = setInterval(
			() => {
				for (const eventId of eventIds) this.#database.monitorEventRenewLease(eventId, leaseId, ttlMs, this.#now());
			},
			Math.max(1000, Math.floor(ttlMs / 3)),
		);
		timer.unref?.();
		return () => clearInterval(timer);
	}
	#author(
		eventId: string,
		note: string,
		noDelivery = false,
		row?: { monitor_id: string; event_type: string; fired_at: string },
	): void {
		const eventRow = row ?? this.#database.monitorEventRows().find((candidate) => candidate.event_id === eventId);
		if (!eventRow) return;
		// Atomic output+intent with a DETERMINISTIC intent id: reconcile and a
		// concurrent dispatch can never create two intents for one event.
		const intentId = `monitor-event-intent:${eventId}`;
		this.#database.monitorEventFencedAuthorWithIntent(
			eventId,
			"",
			note,
			noDelivery,
			eventRow.event_type,
			intentId,
			JSON.stringify(eventTypeOrigin(eventRow.event_type)),
		);
		// Wake the closure queue for the intent admitted above.
		this.#memory.enqueueExistingId(intentId);
		this.#emit({
			eventId,
			monitorId: eventRow.monitor_id,
			eventType: eventRow.event_type,
			firedAt: eventRow.fired_at,
			stage: noDelivery ? "authored_no_delivery" : "authored",
		});
	}
}

function failureCode(error: unknown, failureClass: AuthoringFailureClass): DispatchFailureCode {
	// Context exhaustion outranks the phase codes: it is the one class the safety
	// net acts on, and an operator must be able to see it in the event row.
	if (failureClass === "context") return "authoring_context_exhausted";
	// A malformed or off-contract body is a response-contract failure, not a
	// mystery internal error.
	if (failureClass === "protocol") return "authoring_response_invalid";
	// Executor class from here down, most specific first. An orphaned external
	// run outranks the aside timeout: the same message usually looks like a
	// timeout, but "external work is still running" is the actionable part. Both
	// get their own code so neither can ever be aggregated with context
	// exhaustion.
	if (isOrphanedExecutorFailure(error)) return "orphaned_executor";
	if (isAsideTimeoutFailure(error)) return "aside_timeout";
	const message = error instanceof Error ? error.message : String(error);
	if (message.includes("session send") || message.includes("SessionPort")) return "authoring_turn_failed";
	if (message.includes("session bind")) return "session_bind_failed";
	if (/timeout|timed out|tool failed|external tool|lock/i.test(message)) return "executor_failed";
	return "internal_error";
}

/**
 * The authoring turn frequently does real work (posts, files, tool calls) and
 * then answers in prose or a fenced ```json block around the array. A strict
 * JSON.parse of the whole reply marked those events failed even though the
 * side effect had happened (live: sns-x-v7 / sns-threads-v7 "failed_no_retry"
 * while the posts were up). Accept the whole reply as JSON, a fenced block, or
 * the last top-level [...] array in the text; anything else is still invalid.
 */
export function parseAuthoredArray(response: string): unknown {
	const text = response.trim();
	const attempts: string[] = [text];
	const fenced = [...text.matchAll(/```(?:json)?\s*([\s\S]*?)```/g)].map((m) => m[1]?.trim() ?? "");
	attempts.push(...fenced.reverse());
	// Every balanced top-level [...] span in the text, last first (the array
	// usually closes the reply); string contents are skipped so notes with
	// brackets do not split the span.
	const spans: string[] = [];
	for (let start = text.indexOf("["); start >= 0; start = text.indexOf("[", start + 1)) {
		let depth = 0;
		let inString = false;
		for (let i = start; i < text.length; i++) {
			const ch = text[i];
			if (inString) {
				if (ch === "\\") i++;
				else if (ch === '"') inString = false;
				continue;
			}
			if (ch === '"') inString = true;
			else if (ch === "[") depth++;
			else if (ch === "]" && --depth === 0) {
				spans.push(text.slice(start, i + 1));
				break;
			}
		}
	}
	attempts.push(...spans.reverse());
	let lastError: unknown;
	for (const candidate of attempts) {
		if (!candidate) continue;
		try {
			const parsed = JSON.parse(candidate) as unknown;
			if (Array.isArray(parsed)) return parsed;
		} catch (error) {
			lastError = error;
		}
	}
	throw new Error(
		`authoring response is not a JSON array (${lastError instanceof Error ? lastError.message : "no array found"})`,
	);
}

/**
 * Actionable but public-safe failure detail (#64 vs. the raw-body ban): the
 * error class, a stable error code when the error carries one, and the first
 * in-repo stack frame (file:line). The raw message is deliberately NOT
 * persisted — SDK error bodies can carry prompt content.
 */
function failureDetail(error: unknown): string {
	const knownNames = new Set(["Error", "TypeError", "RangeError", "AggregateError", "TimeoutError", "GjcRuntimeError"]);
	const rawName = error instanceof Error ? error.constructor.name : typeof error;
	const name = knownNames.has(rawName) ? rawName : error instanceof Error ? "Error" : "unknown";
	const message = error instanceof Error ? error.message : "";
	const cause = /Database has closed|closed database/i.test(message)
		? "database_closed"
		: /SQLITE_BUSY|database is locked/i.test(message)
			? "database_busy"
			: /timed out|timeout/i.test(message)
				? "timeout"
				: undefined;
	const frameNames = ["withTransaction", "#dispatchBatch", "monitorEventFencedFail", "request", "bind"];
	const frame =
		error instanceof Error && error.stack
			? frameNames.find((candidate) => error.stack?.split("\n").some((line) => line.includes(candidate)))
			: undefined;
	return [`class=${name}`, cause ? `cause=${cause}` : "", frame ? `frame=${frame}` : ""].filter(Boolean).join(" ");
}
