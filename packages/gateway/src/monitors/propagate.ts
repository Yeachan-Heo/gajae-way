import {
	CATCH_ALL_EVENT_ORIGIN,
	type ChatMessagePayload,
	eventTypeOrigin,
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
	#batches = new Map<string, { eventIds: string[]; timer: ReturnType<typeof setTimeout> }>();
	/** Event ids this process is dispatching right now, so reconcile never double-runs them. */
	#inFlight = new Set<string>();
	/** Re-entrancy guard: only one reconcile sweep may run at a time in this process. */
	#reconciling = false;
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
	}) {
		this.#database = options.database;
		this.#registry = options.registry;
		this.#gjc = options.gjc;
		this.#memory = options.memory;
		this.#delivery = options.delivery;
		this.#emit = options.emit;
		this.#ownerTarget = options.ownerTarget;
		this.#deliver = options.deliver;
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
	 * Cron slot admission (red-team blockers 2+3): the slot claim and the event
	 * row are created in ONE transaction, and the event's `fired_at` IS the
	 * exact scheduled slot timestamp — the durable record carries the scheduled
	 * identity, not just the payload. Returns the eventId, or null when the
	 * slot was already claimed (duplicate tick / restart catch-up overlap).
	 */
	submitSlot(monitorId: string, eventType: string, payload: unknown, slotAt: Date): string | null {
		const monitor = this.#registry.get(monitorId);
		if (!monitor?.enabled) throw new Error("unknown or disabled monitor");
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
			// First: admit any cron slots whose claim committed but whose event row
			// never did (crash between the two writes in a non-atomic path). The
			// slot row keeps the scheduled identity, so recovery restores the event
			// with its exact scheduled fired_at.
			for (const slot of this.#database.monitorSlotUnadmitted()) {
				const monitor = this.#registry.get(slot.monitor_id);
				if (!monitor?.enabled) continue;
				const eventId = crypto.randomUUID();
				const eventType = monitor.eventTypes[0] ?? "monitor.event";
				this.#database.withTransaction(() => {
					this.#database.monitorEventCreate({
						eventId,
						monitorId: slot.monitor_id,
						eventType,
						payloadJson: JSON.stringify({ at: slot.slot_at }),
						firedAt: slot.slot_at,
					});
					this.#database.monitorSlotLinkEvent(slot.monitor_id, slot.slot_at, eventId);
				});
			}
			// Replay oldest-first: recovery must re-author events in the order they fired.
			for (const row of this.#database.monitorEventRows(undefined, "oldest")) {
				if ((TERMINAL_STAGES as readonly string[]).includes(row.stage)) continue;
				const output = this.#database.authoredOutput(row.event_id);
				const hasMemory = this.#database
					.memoryIntentRows()
					.some((intent) => intent.kind === "monitor-event" && intent.payload_json.includes(row.event_id));
				if (output && !hasMemory) this.#author(row.event_id, output, row.stage === "authored_no_delivery");
				else if (!output && this.#recoverable(row)) {
					// Red-team blocker 1: a live dispatch lease owned by ANOTHER attempt
					// means the authoring turn may still complete elsewhere; a new
					// process must not re-author concurrently. Only unclaimed events
					// are reclaimed here (the dispatcher acquires its own lease).
					if (this.#database.monitorEventLiveLeaseOwner(row.event_id)) continue;
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
		try {
			await this.#dispatchBatch(eventIds);
		} finally {
			for (const id of eventIds) this.#inFlight.delete(id);
		}
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
		const owner = `gateway:${process.pid}`;
		const leaseId = crypto.randomUUID();
		const leaseTtlMs = 10 * 60_000;
		const leased: typeof rows = [];
		for (const row of rows) {
			if (this.#database.monitorEventLiveLeaseOwner(row.event_id)) continue;
			this.#database.monitorEventAcquireLease(row.event_id, owner, leaseId, leaseTtlMs);
			leased.push(row);
		}
		if (!leased.length) return;
		this.#database.withTransaction(() => {
			for (const row of leased) this.#database.monitorEventUpdate(row.event_id, "batched", batchId);
		});
		const declared = new Set(monitor.eventTypes);
		const sessionOrigin = declared.has(leased[0]?.event_type)
			? eventTypeOrigin(rows[0]?.event_type)
			: CATCH_ALL_EVENT_ORIGIN;
		try {
			const { sessionId } = await this.#gjc.ensureSession(
				originKey(sessionOrigin),
				this.#database.getSessionRecord(originKey(sessionOrigin))?.epoch ?? 0,
			);
			const guidance = leased
				.map((row) => MAINTENANCE_GUIDANCE[row.event_type])
				.filter((entry, index, all) => entry && all.indexOf(entry) === index)
				.join(" ");
			const prompt = `Author monitor events.${guidance ? ` ${guidance}` : ""} Respond ONLY with a JSON array containing exactly one {"eventId","note"} entry per event: ${JSON.stringify(leased.map((row) => ({ eventId: row.event_id, eventType: row.event_type, payload: JSON.parse(row.payload_json) })))}`;
			const response = await this.#gjc.sendTurn(sessionId, prompt);
			this.#database.withTransaction(() => {
				for (const row of leased) this.#database.monitorEventUpdate(row.event_id, "dispatched", batchId);
			});
			const authored = JSON.parse(response) as Array<{ eventId?: unknown; note?: unknown }>;
			if (!Array.isArray(authored)) throw new Error("authoring response is not an array");
			for (const entry of authored)
				if (typeof entry.eventId === "string" && eventIds.includes(entry.eventId) && typeof entry.note === "string")
					this.#author(entry.eventId, entry.note);
			// A monitor without its own channel target reports to the configured owner
			// target when one exists: a personal agent's maintenance and event notes go
			// to the owner by default rather than vanishing into the logs. With no
			// target at all the event settles terminally as `authored_no_delivery`
			// instead of pretending a delivery is still pending (issue #29 defect 3).
			const target = monitor.channelTarget ?? this.#ownerTarget;
			if (!target) {
				this.#database.withTransaction(() => {
					for (const row of leased)
						if (this.#database.authoredOutput(row.event_id) !== undefined)
							this.#database.monitorEventUpdate(row.event_id, "authored_no_delivery", batchId);
				});
				return;
			}
			const delivery = this.#delivery.prepare(
				batchId,
				target.origin,
				authored
					.filter(
						(entry): entry is { eventId: string; note: string } =>
							typeof entry.eventId === "string" && typeof entry.note === "string",
					)
					.map((entry) => entry.note)
					.join("\n"),
			);
			if (delivery) {
				this.#delivery.markInflight(delivery.deliveryId as string);
				// Push to live adapters NOW: without this the note sat in the ledger
				// until the next adapter reconnect flushed redeliveries (live finding:
				// owner-DM canonicalize note stuck inflight for minutes).
				this.#deliver?.(delivery);
			}
		} catch (error) {
			// Public-safe structured evidence only: a stable phase code and event ids.
			// The raw error body can carry secrets and is never persisted or logged.
			const code: DispatchFailureCode = failureCode(error);
			this.#database.withTransaction(() => {
				for (const row of leased) {
					this.#database.monitorEventUpdate(row.event_id, "failed", batchId);
					this.#database.monitorFailureRecord(row.event_id, code, `dispatch phase failed (${code})`);
				}
			});
			console.error(`monitor dispatch failed (${code}): events ${leased.map((row) => row.event_id).join(",")}`);
		} finally {
			// Release the leases this attempt holds. Lease-guarded: if this attempt
			// expired and another process stole the claim, this release is a no-op,
			// and a stale attempt's completion can never overwrite the newer claim.
			for (const row of leased) this.#database.monitorEventReleaseLease(row.event_id, leaseId);
		}
	}
	#author(eventId: string, note: string, noDelivery = false): void {
		const row = this.#database.monitorEventRows().find((candidate) => candidate.event_id === eventId);
		if (!row) return;
		this.#database.withTransaction(() => {
			this.#database.authoredOutputCreate(eventId, note);
			this.#database.monitorEventUpdate(eventId, noDelivery ? "authored_no_delivery" : "authored");
		});
		this.#memory.enqueue({
			kind: "monitor-event",
			originRefJson: JSON.stringify(eventTypeOrigin(row.event_type)),
			userText: `Monitor event ${eventId}: ${row.event_type}`,
			replyText: note,
		});
		this.#emit({
			eventId,
			monitorId: row.monitor_id,
			eventType: row.event_type,
			firedAt: row.fired_at,
			stage: "authored",
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
