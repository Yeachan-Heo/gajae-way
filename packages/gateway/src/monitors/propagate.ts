import {
	CATCH_ALL_EVENT_ORIGIN,
	type ChatMessagePayload,
	eventTypeOrigin,
	isSilenceToken,
	type MonitorEventRecord,
	type OriginRef,
	originKey,
} from "@gajaeway/protocol";
import type { DeliveryService } from "../delivery/delivery";
import type { MemoryClosureQueue } from "../memory/closure";
import type { GjcPort } from "../orchestrator/gjc-client";
import type { GatewayDatabase } from "../store/db";
import { MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS, RECONCILABLE_STAGES, TERMINAL_STAGES } from "../store/db";
import type { MonitorRegistry } from "./registry";

/** Product-level semantics for the seeded maintenance events (generic, persona-independent). */
const MAINTENANCE_GUIDANCE: Record<string, string | undefined> = {
	"memory.canonicalize":
		"For memory.canonicalize events: read the memory tree's daily/ captures, append the durable facts into their canonical axis files (people/ projects/ decisions/ events/ tasks/ channels/ ops/ reflections/ plus any axis registered in the corpus's axes.json), keep every original byte (never delete or summarize-replace, move stray root files into an axis directory with their raw content preserved), and make new files reachable from MEMORY.md. ops/ holds repeatable operating rules, runtime/session/tool procedure, principles distilled from failure, and state the next executor picks up; it is routable, so write into ops/rules/, ops/distillations/ or ops/handoffs/ and never one growing file, and never put raw transcript, secrets or dated small talk there. reflections/ holds what you learned about your own behaviour as dated append-only entries named reflections/YYYY-MM-DD.md (several entries per day are fine, per-subject files are not); each entry records source/time, the observed failure or drift, the invariant learned, why it matters, the concrete next action, and its promotion target - an operating invariant promotes to ops/rules, a cross-incident learning to ops/distillations, and a project or person correction to that axis. ops constrains what to do before acting; decisions/ records what was chosen at a point in time and why. The gateway commits; you only write files.",
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
	| "authoring_response_invalid"
	| "delivery_prepare_failed"
	| "internal_error";

export interface MonitorDispatchFailure {
	readonly eventId: string;
	readonly code: DispatchFailureCode;
}

export class MonitorPropagator {
	readonly #database: GatewayDatabase;
	readonly #registry: MonitorRegistry;
	readonly #gjc: GjcPort;
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
	constructor(options: {
		database: GatewayDatabase;
		registry: MonitorRegistry;
		gjc: GjcPort;
		memory: MemoryClosureQueue;
		delivery: DeliveryService;
		emit: (event: MonitorEventRecord) => void;
		/** Default recipient for monitors without their own channel target. */
		ownerTarget?: { readonly origin: OriginRef };
		/** Broadcasts a prepared chat.message to live adapter connections. */
		deliver?: (payload: ChatMessagePayload) => void;
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
	}) {
		this.#database = options.database;
		this.#registry = options.registry;
		this.#gjc = options.gjc;
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
	}
	/** Cancels pending burst timers so a closed database is never touched after shutdown. */
	dispose(): void {
		for (const batch of this.#batches.values()) clearTimeout(batch.timer);
		this.#batches.clear();
	}
	submit(monitorId: string, eventType: string, payload: unknown): string {
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
		if (this.#reconciling) return;
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
			? eventTypeOrigin(rows[0]?.event_type)
			: CATCH_ALL_EVENT_ORIGIN;
		try {
			const { sessionId } = await this.#gjc.ensureSession(
				originKey(sessionOrigin),
				this.#database.getSessionRecord(originKey(sessionOrigin))?.epoch ?? 0,
			);
			const guidance = claimed
				.map((row) => MAINTENANCE_GUIDANCE[row.event_type])
				.filter((entry, index, all) => entry && all.indexOf(entry) === index)
				.join(" ");
			const prompt = `Author monitor events.${guidance ? ` ${guidance}` : ""} Respond ONLY with a JSON array containing exactly one {"eventId","note"} entry per event: ${JSON.stringify(claimed.map((row) => ({ eventId: row.event_id, eventType: row.event_type, payload: JSON.parse(row.payload_json) })))}`;
			const response = await this.#gjc.sendTurn(sessionId, prompt);
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
			const authored = JSON.parse(response) as Array<{ eventId?: unknown; note?: unknown }>;
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
			const code: DispatchFailureCode = failureCode(error);
			for (const row of leased) {
				this.#database.monitorEventFencedFail(row.event_id, leaseId, batchId, code, `dispatch phase failed (${code})`);
			}
			console.error(`monitor dispatch failed (${code}): events ${claimed.map((row) => row.event_id).join(",")}`);
		} finally {
			stopHeartbeat();
			// Release the leases this attempt holds. Lease-guarded: if this attempt
			// expired and another process stole the claim, this release is a no-op,
			// and a stale attempt's completion can never overwrite the newer claim.
			for (const row of leased) this.#database.monitorEventReleaseLease(row.event_id, leaseId);
		}
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

function failureCode(error: unknown): DispatchFailureCode {
	const message = error instanceof Error ? error.message : String(error);
	if (message.includes("authoring response is not an array")) return "authoring_response_invalid";
	if (message.includes("sendTurn")) return "authoring_turn_failed";
	if (message.includes("ensureSession")) return "session_bind_failed";
	return "internal_error";
}
