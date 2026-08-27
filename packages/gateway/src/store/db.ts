import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";

export interface InboundMessageRow {
	readonly message_id: string;
	readonly origin_key: string;
	readonly origin_ref_json: string;
	readonly body: string;
	readonly engagement_json: string | null;
	readonly received_at: string;
}

const LATEST_SCHEMA_VERSION = 11;
/** A scheduled cron slot the gateway claimed or fired for one monitor. */
export interface MonitorSlotRow {
	readonly monitor_id: string;
	readonly slot_at: string;
	readonly created_at: string;
}

/** Public-safe dispatch failure evidence (never a raw error body). */
export interface MonitorFailureRow {
	readonly event_id: string;
	readonly code: string;
	readonly detail: string;
	readonly failed_at: string;
}

export class DatabaseStartupError extends Error {
	readonly code: "newer_schema" | "integrity_check_failed";
	constructor(code: DatabaseStartupError["code"], message: string) {
		super(message);
		this.name = "DatabaseStartupError";
		this.code = code;
	}
}

/**
 * Durable monitor-event stage contract (fail-closed). `monitorEventUpdate`
 * rejects any stage outside this set, so a typo or a corrupt write cannot
 * invent a state the recovery logic does not know how to reclaim.
 *
 * - admitted: durable row exists, not yet claimed by a dispatch.
 * - batched: claimed by a dispatch awaiting its authoring turn (a live stage,
 *   minutes at most) or stranded there by a crash mid-dispatch.
 * - dispatched: authoring turn sent, output not yet parsed.
 * - authored: note authored; delivery to the target is pending or settled.
 * - delivered: the batch's ledger delivery was confirmed by the adapter.
 * - authored_no_delivery: terminal — the monitor has no channel target and no
 *   owner target is configured, so nothing will ever be delivered. Explicit
 *   instead of `authored`-forever so the operator sees a finished state.
 * - failed: the last dispatch attempt failed; reconcile redispatches.
 * - failed_no_retry: dispatch failed and reconcile will not retry it again
 *   (reclaim budget exhausted); operator-visible terminal state.
 */
export const MONITOR_EVENT_STAGES = [
	"admitted",
	"batched",
	"dispatched",
	"authored",
	"delivered",
	"authored_no_delivery",
	"failed",
	"failed_no_retry",
] as const;
export type MonitorEventStage = (typeof MONITOR_EVENT_STAGES)[number];
/** Stages whose rows reconcile() may claim and redispatch. */
export const RECONCILABLE_STAGES: readonly MonitorEventStage[] = ["admitted", "batched", "dispatched", "failed"];
/** Terminal stages: no further transition will ever happen without operator action. */
export const TERMINAL_STAGES: readonly MonitorEventStage[] = ["delivered", "authored_no_delivery", "failed_no_retry"];

/** Upper bound on how often a single event may be reclaimed by reconcile. */
export const MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS = 5;

export class GatewayDatabase {
	readonly #database: Database;
	#inTransaction = false;

	private constructor(database: Database) {
		this.#database = database;
	}

	static async open(path: string): Promise<GatewayDatabase> {
		await mkdir(dirname(path), { recursive: true, mode: 0o700 });
		const database = new Database(path);
		database.exec("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000; PRAGMA synchronous = NORMAL;");
		const instance = new GatewayDatabase(database);
		instance.migrate();
		instance.integrityCheck();
		return instance;
	}

	get schemaVersion(): number {
		const row = this.#database
			.query<{ version: number }, []>("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations")
			.get();
		return row?.version ?? 0;
	}

	get activeSessionCount(): number {
		return this.#database.query<{ count: number }, []>("SELECT COUNT(*) AS count FROM sessions").get()?.count ?? 0;
	}
	/**
	 * Returns the session row even when no gjc session is bound yet (empty
	 * sessionId): the epoch must stay visible after /new bumps it, or dispatch
	 * silently falls back to epoch 0 and the old transcript.
	 */
	getSessionRecord(originKey: string): { sessionId: string; epoch: number } | undefined {
		const row = this.#database
			.query<{ gjc_session_id: string; epoch: number }, [string]>(
				"SELECT gjc_session_id, epoch FROM sessions WHERE origin_key = ?",
			)
			.get(originKey);
		return row ? { sessionId: row.gjc_session_id, epoch: row.epoch } : undefined;
	}

	bumpEpoch(originKey: string, originRefJson: string): number {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO sessions (origin_key, origin_ref_json, gjc_session_id, epoch, created_at, last_activity_at) VALUES (?, ?, '', 1, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET epoch = epoch + 1, gjc_session_id = '', turn_count = 0, origin_ref_json = excluded.origin_ref_json, last_activity_at = excluded.last_activity_at",
			)
			.run(originKey, originRefJson, now, now);
		return this.#database
			.query<{ epoch: number }, [string]>("SELECT epoch FROM sessions WHERE origin_key = ?")
			.get(originKey)!.epoch;
	}

	updateActivity(originKey: string, originRefJson: string): void {
		this.#database
			.query("UPDATE sessions SET last_activity_at = ?, origin_ref_json = ? WHERE origin_key = ?")
			.run(new Date().toISOString(), originRefJson, originKey);
	}

	/**
	 * Counts completed turns within the current epoch. The gjc session transcript
	 * grows with every `--resume`, so per-turn prefill latency grows without bound;
	 * the server auto-rotates the epoch once this count reaches its ceiling.
	 */
	incrementTurnCount(originKey: string): number {
		this.#database.query("UPDATE sessions SET turn_count = turn_count + 1 WHERE origin_key = ?").run(originKey);
		return (
			this.#database
				.query<{ turn_count: number }, [string]>("SELECT turn_count FROM sessions WHERE origin_key = ?")
				.get(originKey)?.turn_count ?? 0
		);
	}

	sessionRows(): Array<{
		origin_ref_json: string | null;
		created_at: string;
		last_activity_at: string | null;
		epoch: number;
	}> {
		return this.#database
			.query("SELECT origin_ref_json, created_at, last_activity_at, epoch FROM sessions ORDER BY created_at")
			.all() as Array<{
			origin_ref_json: string | null;
			created_at: string;
			last_activity_at: string | null;
			epoch: number;
		}>;
	}
	/**
	 * Cycle projection source (ops.cycle): full session identity including the bound
	 * gjc session id. An empty gjc_session_id with a positive epoch is the mid-rebind
	 * state after /new; the projection must report it as stale identity, never healthy.
	 */
	sessionIdentityRows(): Array<{
		origin_key: string;
		origin_ref_json: string | null;
		gjc_session_id: string;
		epoch: number;
		created_at: string;
		last_activity_at: string | null;
	}> {
		return this.#database
			.query(
				"SELECT origin_key, origin_ref_json, gjc_session_id, epoch, created_at, last_activity_at FROM sessions ORDER BY created_at",
			)
			.all() as Array<{
			origin_key: string;
			origin_ref_json: string | null;
			gjc_session_id: string;
			epoch: number;
			created_at: string;
			last_activity_at: string | null;
		}>;
	}

	/** Cycle projection source: durable inbound queue state census across all origins. */
	inboundStateCounts(): Array<{ state: string; n: number }> {
		return this.#database.query("SELECT state, COUNT(*) AS n FROM inbound_messages GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	/** Cycle projection source: pending inbound count per origin key. */
	inboundPendingByOrigin(): Array<{ origin_key: string; n: number }> {
		return this.#database
			.query("SELECT origin_key, COUNT(*) AS n FROM inbound_messages WHERE state = 'pending' GROUP BY origin_key")
			.all() as Array<{ origin_key: string; n: number }>;
	}

	/** Cycle projection source: delivery state census across all origins. */
	deliveryStateCounts(): Array<{ state: string; n: number }> {
		return this.#database.query("SELECT state, COUNT(*) AS n FROM deliveries GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	/** Cycle projection source: unsettled delivery age census per origin key. */
	deliveryUnsettledByOrigin(now = Date.now()): Array<{ origin_key: string; n: number; oldest_ms: number }> {
		return this.#database
			.query(
				// Age in ms from a seconds binding: parenthesize so the seconds delta is
				// multiplied, not the timestamp alone (SQL precedence binds * before -).
				"SELECT origin_key, COUNT(*) AS n, MAX((? - CAST(strftime('%s', created_at) AS INTEGER)) * 1000) AS oldest_ms FROM deliveries WHERE state IN ('pending','inflight','failed_ambiguous') GROUP BY origin_key",
			)
			.all(Math.floor(now / 1000)) as Array<{ origin_key: string; n: number; oldest_ms: number }>;
	}

	/** Cycle projection source: memory-intent settlement census. */
	memoryIntentCounts(): Array<{ state: string; n: number }> {
		return this.#database.query("SELECT state, COUNT(*) AS n FROM memory_intents GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	/** Cycle projection source: monitor-event stage census. */
	monitorEventStageCounts(): Array<{ stage: string; n: number }> {
		return this.#database.query("SELECT stage, COUNT(*) AS n FROM monitor_events GROUP BY stage").all() as Array<{
			stage: string;
			n: number;
		}>;
	}

	addRecall(originKey: string, originRefJson: string, text: string): void {
		this.#database
			.query("INSERT INTO recall_snippets (origin_key, origin_ref_json, text, at) VALUES (?, ?, ?, ?)")
			.run(originKey, originRefJson, text.slice(0, 1000), new Date().toISOString());
		this.#database
			.query(
				"DELETE FROM recall_snippets WHERE origin_key = ? AND id NOT IN (SELECT id FROM recall_snippets WHERE origin_key = ? ORDER BY id DESC LIMIT 20)",
			)
			.run(originKey, originKey);
	}

	recallRows(): Array<{ origin_key: string; origin_ref_json: string; text: string; at: string }> {
		return this.#database
			.query("SELECT origin_key, origin_ref_json, text, at FROM recall_snippets ORDER BY id DESC")
			.all() as Array<{ origin_key: string; origin_ref_json: string; text: string; at: string }>;
	}

	memoryIntentCreate(row: { id: string; kind: string; payloadJson: string }): void {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO memory_intents (id, kind, payload_json, state, created_at, updated_at) VALUES (?, ?, ?, 'queued', ?, ?)",
			)
			.run(row.id, row.kind, row.payloadJson, now, now);
	}

	/** Insertion-order view (rowid) — preserves admission order within the same millisecond. */
	memoryIntentRowsByRowid(): Array<{ id: string; kind: string; payload_json: string; state: string }> {
		return this.#database
			.query(
				"SELECT id, kind, payload_json, state FROM memory_intents ORDER BY rowid",
			)
			.all() as Array<{ id: string; kind: string; payload_json: string; state: string }>;
	}

	memoryIntentUpdate(id: string, state: "queued" | "written" | "committed" | "receipted" | "quarantined"): void {
		this.#database
			.query("UPDATE memory_intents SET state = ?, updated_at = ? WHERE id = ?")
			.run(state, new Date().toISOString(), id);
	}

	memoryIntentRows(): Array<{
		id: string;
		kind: string;
		payload_json: string;
		state: "queued" | "written" | "committed" | "receipted" | "quarantined";
	}> {
		return this.#database
			.query("SELECT id, kind, payload_json, state FROM memory_intents ORDER BY created_at, id")
			.all() as Array<{
			id: string;
			kind: string;
			payload_json: string;
			state: "queued" | "written" | "committed" | "receipted" | "quarantined";
		}>;
	}

	/** The insert is the acceptance boundary: dispatch may only start once the row is durable. */
	inboundEnqueue(row: {
		messageId: string;
		originKey: string;
		originRefJson: string;
		body: string;
		engagementJson?: string;
	}): boolean {
		const changes = this.#database
			.query(
				"INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at) VALUES (?, ?, ?, ?, ?, 'pending', ?) ON CONFLICT(message_id) DO NOTHING",
			)
			.run(
				row.messageId,
				row.originKey,
				row.originRefJson,
				row.body,
				row.engagementJson ?? null,
				new Date().toISOString(),
			);
		return changes.changes > 0;
	}

	/** Claims the oldest pending message for an origin, so replay and live dispatch cannot double-run it. */
	inboundClaimNext(originKey: string): InboundMessageRow | undefined {
		return this.withTransaction(() => {
			const row = this.#database
				.query<InboundMessageRow, [string]>(
					"SELECT message_id, origin_key, origin_ref_json, body, engagement_json, received_at FROM inbound_messages WHERE origin_key = ? AND state = 'pending' ORDER BY received_at, message_id LIMIT 1",
				)
				.get(originKey);
			if (!row) return undefined;
			this.#database.query("UPDATE inbound_messages SET state = 'processing' WHERE message_id = ?").run(row.message_id);
			return row;
		});
	}

	inboundComplete(messageId: string): void {
		this.#database.query("UPDATE inbound_messages SET state = 'done' WHERE message_id = ?").run(messageId);
	}

	/**
	 * Conversation context ledger: every inbound platform message (engaged or not)
	 * is recorded here; a turn consumes the unread diff since the last reply.
	 */
	contextRecord(row: {
		messageId: string;
		originKey: string;
		authorId?: string;
		authorName?: string;
		body: string;
	}): void {
		this.#database
			.query(
				"INSERT INTO conversation_context (message_id, origin_key, author_id, author_name, body, received_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(message_id) DO NOTHING",
			)
			.run(
				row.messageId,
				row.originKey,
				row.authorId ?? null,
				row.authorName ?? null,
				row.body,
				new Date().toISOString(),
			);
	}

	contextUnread(
		originKey: string,
		limit = 100,
	): Array<{
		message_id: string;
		author_id: string | null;
		author_name: string | null;
		body: string;
		received_at: string;
	}> {
		return this.#database
			.query<
				{ message_id: string; author_id: string | null; author_name: string | null; body: string; received_at: string },
				[string, number]
			>(
				"SELECT message_id, author_id, author_name, body, received_at FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL ORDER BY received_at, message_id LIMIT ?",
			)
			.all(originKey, limit);
	}

	contextConsume(messageIds: readonly string[]): void {
		if (messageIds.length === 0) return;
		const now = new Date().toISOString();
		this.withTransaction(() => {
			for (const id of messageIds)
				this.#database.query("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ?").run(now, id);
		});
	}

	contextPrune(maxAgeMs: number): number {
		const cutoff = new Date(Date.now() - maxAgeMs).toISOString();
		return this.#database.query("DELETE FROM conversation_context WHERE received_at < ?").run(cutoff).changes;
	}

	/** Startup recovery: a turn killed mid-flight must not strand its claimed message. */
	inboundRecoverProcessing(): number {
		return this.#database.query("UPDATE inbound_messages SET state = 'pending' WHERE state = 'processing'").run()
			.changes;
	}

	inboundPendingCount(originKey: string): number {
		return (
			this.#database
				.query<{ n: number }, [string]>(
					"SELECT COUNT(*) AS n FROM inbound_messages WHERE origin_key = ? AND state = 'pending'",
				)
				.get(originKey)?.n ?? 0
		);
	}

	getSession(originKey: string): string | undefined {
		return this.#database
			.query<{ gjc_session_id: string }, [string]>("SELECT gjc_session_id FROM sessions WHERE origin_key = ?")
			.get(originKey)?.gjc_session_id;
	}

	putSession(originKey: string, sessionId: string): void {
		this.#database
			.query(
				"INSERT INTO sessions (origin_key, gjc_session_id, created_at) VALUES (?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id",
			)
			.run(originKey, sessionId, new Date().toISOString());
	}

	deliveryCreate(row: { id: string; turnId: string; originKey: string; payloadJson: string }): void {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
			)
			.run(row.id, row.turnId, row.originKey, row.payloadJson, now, now);
	}

	deliveryUpdate(id: string, state: string, attempts?: number): void {
		this.#database
			.query("UPDATE deliveries SET state = ?, attempts = COALESCE(?, attempts), updated_at = ? WHERE delivery_id = ?")
			.run(state, attempts ?? null, new Date().toISOString(), id);
	}

	deliveryRows(): Array<{
		delivery_id: string;
		turn_id: string;
		origin_key: string;
		payload_json: string;
		state: string;
		attempts: number;
		created_at: string;
		updated_at: string;
	}> {
		return this.#database
			.query(
				"SELECT delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at FROM deliveries ORDER BY created_at",
			)
			.all() as Array<{
			delivery_id: string;
			turn_id: string;
			origin_key: string;
			payload_json: string;
			state: string;
			attempts: number;
			created_at: string;
			updated_at: string;
		}>;
	}

	monitorCreate(row: {
		id: string;
		name: string;
		triggerJson: string;
		eventTypesJson: string;
		burstPolicy: string;
		channelTargetJson: string | null;
		enabled: boolean;
	}): void {
		this.#database
			.query(
				"INSERT INTO monitors (monitor_id, name, trigger_json, event_types_json, burst_policy, channel_target_json, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.run(
				row.id,
				row.name,
				row.triggerJson,
				row.eventTypesJson,
				row.burstPolicy,
				row.channelTargetJson,
				row.enabled ? 1 : 0,
				new Date().toISOString(),
			);
	}
	monitorRows(): Array<{
		monitor_id: string;
		name: string;
		trigger_json: string;
		event_types_json: string;
		burst_policy: string;
		channel_target_json: string | null;
		enabled: number;
		created_at: string;
	}> {
		return this.#database
			.query(
				"SELECT monitor_id, name, trigger_json, event_types_json, burst_policy, channel_target_json, enabled, created_at FROM monitors ORDER BY created_at",
			)
			.all() as Array<{
			monitor_id: string;
			name: string;
			trigger_json: string;
			event_types_json: string;
			burst_policy: string;
			channel_target_json: string | null;
			enabled: number;
			created_at: string;
		}>;
	}
	monitorDelete(id: string): boolean {
		return this.#database.query("DELETE FROM monitors WHERE monitor_id = ?").run(id).changes > 0;
	}
	monitorEventCreate(row: {
		eventId: string;
		monitorId: string;
		eventType: string;
		payloadJson: string;
		firedAt: string;
	}): void {
		this.#database
			.query(
				"INSERT INTO monitor_events (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, updated_at) VALUES (?, ?, ?, ?, ?, 'admitted', NULL, ?)",
			)
			.run(row.eventId, row.monitorId, row.eventType, row.payloadJson, row.firedAt, new Date().toISOString());
	}
	/**
	 * Durable dispatch lease (red-team blocker 1): a process claims an event
	 * before sending its authoring turn. The claim stores an owner token and an
	 * expiry; a claim is valid only while `expires_at` is in the future. This
	 * closes the restart race where the external gjc authoring turn outlives a
	 * dead gateway process — a new process must not re-author the same event
	 * concurrently, and only an expired claim may be stolen.
	 *
	 * Semantics:
	 * - claim succeeds iff no live (unexpired) lease exists for the event;
	 * - stealing replaces the expired lease with a fresh lease_id + owner;
	 * - `monitorEventReleaseLease` is lease-guarded: a stale attempt whose lease
	 *   expired and was stolen can never release the newer owner's claim.
	 */
	monitorEventAcquireLease(
		eventId: string,
		owner: string,
		leaseId: string,
		ttlMs: number,
		now = Date.now(),
	): boolean {
		const nowIso = new Date(now).toISOString();
		const expiresIso = new Date(now + ttlMs).toISOString();
		const claim = this.#database
			.query(
				`INSERT INTO dispatch_leases (event_id, owner, lease_id, acquired_at, expires_at) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(event_id) DO UPDATE SET owner = excluded.owner, lease_id = excluded.lease_id, acquired_at = excluded.acquired_at, expires_at = excluded.expires_at
WHERE excluded.acquired_at IS NOT NULL AND (SELECT expires_at FROM dispatch_leases WHERE event_id = excluded.event_id) <= excluded.acquired_at`,
			)
			.run(eventId, owner, leaseId, nowIso, expiresIso).changes;
		return claim > 0;
	}
	/** Releases a lease only when the caller still owns it (stale attempts are no-ops). */
	monitorEventReleaseLease(eventId: string, leaseId: string): void {
		this.#database
			.query("DELETE FROM dispatch_leases WHERE event_id = ? AND lease_id = ?")
			.run(eventId, leaseId);
	}
	/** Returns the lease id of the live (unexpired) claim, if any. */
	monitorEventLiveLeaseOwner(eventId: string, now = Date.now()): string | undefined {
		const row = this.#database
			.query<{ lease_id: string; expires_at: string }, [string]>(
				"SELECT lease_id, expires_at FROM dispatch_leases WHERE event_id = ?",
			)
			.get(eventId);
		if (row && Date.parse(row.expires_at) > now) return row.lease_id;
		return undefined;
	}
	/** Extends a live lease only when the caller still owns it (stale attempts are no-ops). */
	monitorEventRenewLease(eventId: string, leaseId: string, ttlMs: number, now = Date.now()): boolean {
		const row = this.#database
			.query<{ lease_id: string }, [string, string, string]>(
				"SELECT lease_id FROM dispatch_leases WHERE event_id = ? AND lease_id = ? AND expires_at > ?",
			)
			.get(eventId, leaseId, new Date(now).toISOString());
		if (!row) return false;
		this.#database
			.query("UPDATE dispatch_leases SET expires_at = ? WHERE event_id = ? AND lease_id = ?")
			.run(new Date(now + ttlMs).toISOString(), eventId, leaseId);
		return true;
	}
	/**
	 * Fenced stage transition (round-3 blocker 1): the lease ownership check and
	 * the stage write happen atomically in ONE UPDATE — the WHERE clause includes
	 * a live-lease subquery, so a lease stolen between the caller's check and the
	 * write cannot be exploited (no TOCTOU). Returns true when this attempt's
	 * write actually landed.
	 */
	monitorEventFencedUpdate(
		eventId: string,
		leaseId: string,
		stage: MonitorEventStage,
		batchId: string | null = null,
		now = Date.now(),
	): boolean {
		if (!MONITOR_EVENT_STAGES.includes(stage)) throw new Error(`unknown monitor event stage: ${stage}`);
		const changes = this.#database
			.query(
				`UPDATE monitor_events SET stage = ?, batch_id = COALESCE(?, batch_id), updated_at = ?
WHERE event_id = ? AND EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`,
			)
			.run(stage, batchId, new Date(now).toISOString(), eventId, leaseId, new Date(now).toISOString()).changes;
		return changes > 0;
	}
	/**
	 * Fenced output write: authored_outputs upsert + authored stage transition in
	 * one transaction, both gated on the live lease. A stale attempt cannot write
	 * its note over the newer attempt's.
	 */
	monitorEventFencedAuthor(eventId: string, leaseId: string, note: string, noDelivery = false, now = Date.now()): boolean {
		return this.withTransaction(() => {
			const changes = this.#database
				.query(
					`UPDATE monitor_events SET stage = ?, updated_at = ?
WHERE event_id = ? AND EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`,
				)
				.run(noDelivery ? "authored_no_delivery" : "authored", new Date(now).toISOString(), eventId, leaseId, new Date(now).toISOString()).changes;
			if (changes === 0) return false;
			this.authoredOutputCreate(eventId, note);
			return true;
		});
	}
	/**
	 * Fenced delivery admission (round-3 blocker 1): the pending delivery row is
	 * inserted only if EVERY given event still holds leaseId live, checked in the
	 * same transaction as the insert. Returns false (nothing written) when any
	 * event's lease was lost — a stale attempt can never emit.
	 */
	monitorDeliveryPrepareFenced(
		deliveryId: string,
		batchId: string,
		originKey: string,
		payloadJson: string,
		eventIds: readonly string[],
		leaseId: string,
		now = Date.now(),
	): boolean {
		return this.withTransaction(() => {
			for (const eventId of eventIds) {
				const lease = this.#database
					.query<{ lease_id: string; expires_at: string }, [string]>(
						"SELECT lease_id, expires_at FROM dispatch_leases WHERE event_id = ?",
					)
					.get(eventId);
				if (!lease || lease.lease_id !== leaseId || Date.parse(lease.expires_at) <= now) return false;
			}
			const nowIso = new Date(now).toISOString();
			this.#database
				.query(
					"INSERT INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
				)
				.run(deliveryId, batchId, originKey, payloadJson, nowIso, nowIso);
			return true;
		});
	}
	/**
	 * Atomic LEASE-FENCED authored output + memory-intent admission (round-3/4
	 * blockers): the authored_outputs upsert and the queued monitor-event intent
	 * commit in ONE transaction, and the stage UPDATE carries the live-lease
	 * predicate — a crash or interleaving between writes can never produce zero
	 * or duplicate intents for the same event, and a stale attempt cannot write.
	 */
	monitorEventFencedAuthorWithIntent(
		eventId: string,
		leaseId: string,
		note: string,
		noDelivery: boolean,
		monitorId: string,
		eventType: string,
		firedAt: string,
		intentId: string,
		originRefJson: string,
		now = Date.now(),
	): boolean {
		return this.withTransaction(() => {
			// leaseId === "" means UNFENCED (reconcile re-author path: single-flight,
			// event already proven non-terminal). Dispatch attempts always pass a
			// real lease id and get the EXISTS predicate.
			const leasePredicate =
				leaseId === ""
					? "1=1"
					: `EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`;
			const leaseArgs = leaseId === "" ? [] : [leaseId, new Date(now).toISOString()];
			const changes = this.#database
				.query(
					`UPDATE monitor_events SET stage = ?, updated_at = ?
WHERE event_id = ? AND ${leasePredicate}`,
				)
				.run(noDelivery ? "authored_no_delivery" : "authored", new Date(now).toISOString(), eventId, ...leaseArgs)
				.changes;
			if (changes === 0) return false;
			this.authoredOutputCreate(eventId, note);
			// Idempotent intent: the INSERT is keyed on the deterministic intent id;
			// a second admission for the same event is a no-op.
			const existing = this.#database
				.query<{ id: string }, [string]>("SELECT id FROM memory_intents WHERE id = ?")
				.get(intentId);
			if (existing) return true;
			this.memoryIntentCreate({
				id: intentId,
				kind: "monitor-event",
				payloadJson: JSON.stringify({
					kind: "monitor-event",
					identity: `monitor-event:${eventId}`,
					originRefJson,
					userText: `Monitor event ${eventId}: ${eventType}`,
					replyText: note,
				}),
			});
			void monitorId;
			void firedAt;
			return true;
		});
	}
	/**
	 * Fenced failure write: failed stage + public-safe evidence row in one
	 * transaction, both gated on the live lease (round-3 blocker 1).
	 */
	monitorEventFencedFail(eventId: string, leaseId: string, batchId: string, code: string, detail: string, now = Date.now()): boolean {
		return this.withTransaction(() => {
			const changes = this.#database
				.query(
					`UPDATE monitor_events SET stage = 'failed', batch_id = ?, updated_at = ?
WHERE event_id = ? AND EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`,
				)
				.run(batchId, new Date(now).toISOString(), eventId, leaseId, new Date(now).toISOString()).changes;
			if (changes === 0) return false;
			this.monitorFailureRecord(eventId, code, detail);
			return true;
		});
	}
	/** True while the given lease is the live claim for the event. */
	monitorEventLeaseHeld(eventId: string, leaseId: string, now = Date.now()): boolean {
		const row = this.#database
			.query<{ lease_id: string; expires_at: string }, [string]>(
				"SELECT lease_id, expires_at FROM dispatch_leases WHERE event_id = ?",
			)
			.get(eventId);
		return row?.lease_id === leaseId && Date.parse(row.expires_at) > now;
	}
	/**
	 * Fail-closed stage transition: an unknown stage name throws instead of
	 * writing a state no recovery path understands.
	 */
	monitorEventUpdate(eventId: string, stage: MonitorEventStage, batchId: string | null = null): void {
		if (!MONITOR_EVENT_STAGES.includes(stage)) throw new Error(`unknown monitor event stage: ${stage}`);
		this.#database
			.query("UPDATE monitor_events SET stage = ?, batch_id = COALESCE(?, batch_id), updated_at = ? WHERE event_id = ?")
			.run(stage, batchId, new Date().toISOString(), eventId);
	}
	/**
	 * Red-team blocker 4: settlement must be MONOTONIC. Once an event is
	 * terminally settled (`delivered`), a late/out-of-order delivery.fail can
	 * never regress it; a duplicate confirm is a no-op. Non-terminal stages
	 * (`authored`) still receive fail-path demotions so failed deliveries stay
	 * operator-visible.
	 */
	monitorEventSettle(eventId: string, stage: "delivered" | "authored"): boolean {
		const row = this.#database
			.query<{ stage: string }, [string]>("SELECT stage FROM monitor_events WHERE event_id = ?")
			.get(eventId);
		if (!row) return false;
		if (stage === "delivered" && row.stage !== "delivered") {
			this.monitorEventUpdate(eventId, "delivered");
			return true;
		}
		// Fail path: only demote events still in `authored`; never touch delivered
		// (or any terminal stage).
		if (stage === "authored" && row.stage === "authored") return false;
		if (stage === "authored" && row.stage !== "delivered" && row.stage !== "authored_no_delivery" && row.stage !== "failed_no_retry") {
			this.monitorEventUpdate(eventId, "authored");
			return true;
		}
		return false;
	}
	/**
	 * Bumps the reclaim counter; returns the new count. Reconcile stops
	 * reclaiming an event once it exceeds MONITOR_EVENT_MAX_DISPATCH_ATTEMPTS.
	 */
	monitorEventIncrementAttempts(eventId: string): number {
		this.#database
			.query("UPDATE monitor_events SET dispatch_attempts = dispatch_attempts + 1, updated_at = ? WHERE event_id = ?")
			.run(new Date().toISOString(), eventId);
		return (
			this.#database
				.query<{ n: number }, [string]>("SELECT dispatch_attempts AS n FROM monitor_events WHERE event_id = ?")
				.get(eventId)?.n ?? 0
		);
	}
	monitorEventDispatchAttempts(eventId: string): number {
		return (
			this.#database
				.query<{ dispatch_attempts: number }, [string]>(
					"SELECT dispatch_attempts FROM monitor_events WHERE event_id = ?",
				)
				.get(eventId)?.dispatch_attempts ?? 0
		);
	}
	/**
	 * `order` is explicit because listings want newest-first while recovery must replay in the
	 * order events fired; `rowid` breaks same-millisecond ties so both orders are deterministic.
	 */
	monitorEventRows(
		monitorId?: string,
		order: "newest" | "oldest" = "newest",
	): Array<{
		event_id: string;
		monitor_id: string;
		event_type: string;
		payload_json: string;
		fired_at: string;
		stage: string;
		batch_id: string | null;
		dispatch_attempts: number;
		updated_at: string;
	}> {
		const direction = order === "oldest" ? "ASC" : "DESC";
		return this.#database
			.query(
				monitorId
					? `SELECT * FROM monitor_events WHERE monitor_id = ? ORDER BY fired_at ${direction}, rowid ${direction}`
					: `SELECT * FROM monitor_events ORDER BY fired_at ${direction}, rowid ${direction}`,
			)
			.all(...(monitorId ? [monitorId] : [])) as Array<{
			event_id: string;
			monitor_id: string;
			event_type: string;
			payload_json: string;
			fired_at: string;
			stage: string;
			batch_id: string | null;
			dispatch_attempts: number;
			updated_at: string;
		}>;
	}
	authoredOutputCreate(eventId: string, outputText: string): void {
		this.#database
			.query(
				"INSERT INTO authored_outputs (event_id, output_text, authored_at) VALUES (?, ?, ?) ON CONFLICT(event_id) DO UPDATE SET output_text = excluded.output_text, authored_at = excluded.authored_at",
			)
			.run(eventId, outputText, new Date().toISOString());
	}
	authoredOutput(eventId: string): string | undefined {
		return this.#database
			.query<{ output_text: string }, [string]>("SELECT output_text FROM authored_outputs WHERE event_id = ?")
			.get(eventId)?.output_text;
	}
	/** Latest public-safe failure evidence for one event, if any. */
	monitorFailure(eventId: string): MonitorFailureRow | undefined {
		return (
			this.#database
				.query<MonitorFailureRow, [string]>(
					"SELECT event_id, code, detail, failed_at FROM monitor_failures WHERE event_id = ? ORDER BY rowid DESC LIMIT 1",
				)
				.get(eventId) ?? undefined
		);
	}
	/**
	 * Persists bounded, public-safe dispatch evidence: a stable machine code plus
	 * a one-line detail that must never contain secrets or raw error bodies.
	 * Keeps the newest 20 rows per event and deletes stale rows so the table
	 * cannot grow without bound across retries.
	 */
	monitorFailureRecord(eventId: string, code: string, detail: string): void {
		// Runs inside the caller's transaction when one is open (dispatch failure
		// bookkeeping must be atomic with the stage transition); standalone otherwise.
		this.#database
			.query("INSERT INTO monitor_failures (event_id, code, detail, failed_at) VALUES (?, ?, ?, ?)")
			.run(eventId, code, detail.slice(0, 500), new Date().toISOString());
		this.#database
			.query(
				"DELETE FROM monitor_failures WHERE event_id = ? AND rowid NOT IN (SELECT rowid FROM monitor_failures WHERE event_id = ? ORDER BY rowid DESC LIMIT 20)",
			)
			.run(eventId, eventId);
	}
	monitorFailures(eventIds: readonly string[]): Map<string, MonitorFailureRow> {
		const rows = new Map<string, MonitorFailureRow>();
		for (const eventId of eventIds) {
			const row = this.monitorFailure(eventId);
			if (row) rows.set(eventId, row);
		}
		return rows;
	}
	/**
	 * Red-team blocker 2: the slot claim and the event admission commit in ONE
	 * transaction. A claim row whose event_id is NULL means the gateway fired
	 * the slot but died before submitting the event — reconcile admits it. A
	 * crash can therefore strand neither a claimed-but-unadmitted slot (this
	 * row shape makes it visible) nor a duplicate event (unique slot key).
	 */
	monitorSlotClaimWithEvent(row: {
		monitorId: string;
		slotAt: string;
		eventId: string;
		eventType: string;
		payloadJson: string;
	}): boolean {
		const now = new Date().toISOString();
		return (
			this.withTransaction(() => {
				const claim = this.#database
					.query(
						"INSERT INTO monitor_slots (monitor_id, slot_at, event_id, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(monitor_id, slot_at) DO NOTHING",
					)
					.run(row.monitorId, row.slotAt, row.eventId, now).changes;
				if (claim === 0) return false;
				this.monitorEventCreate({
					eventId: row.eventId,
					monitorId: row.monitorId,
					eventType: row.eventType,
					payloadJson: row.payloadJson,
					firedAt: row.slotAt,
				});
				return true;
			}) as boolean
		);
	}
	/** Test/recovery seam: move a monitor's creation instant (clamps catch-up). */
	monitorSetCreatedAt(monitorId: string, createdAt: string): void {
		this.#database
			.query("UPDATE monitors SET created_at = ? WHERE monitor_id = ?")
			.run(createdAt, monitorId);
	}
	monitorSlotExists(monitorId: string, slotAt: string): boolean {
		const row = this.#database
			.query<{ n: number }, [string, string]>(
				"SELECT COUNT(*) AS n FROM monitor_slots WHERE monitor_id = ? AND slot_at = ?",
			)
			.get(monitorId, slotAt);
		return (row?.n ?? 0) > 0;
	}
	/** Drops slot ledger entries older than the retention window (bounded table). */
	monitorSlotPrune(olderThanMs: number, now = Date.now()): number {
		const cutoff = new Date(now - olderThanMs).toISOString();
		return this.#database.query("DELETE FROM monitor_slots WHERE slot_at < ?").run(cutoff).changes;
	}

	deliveryPrune(before: string): number {
		return this.#database.query("DELETE FROM deliveries WHERE state = 'confirmed' AND updated_at < ?").run(before)
			.changes;
	}

	/**
	 * All writes pass through this single-writer boundary. Callbacks must be
	 * synchronous and must never perform external I/O; nested transactions fail.
	 */
	withTransaction<T>(work: () => T): T {
		if (this.#inTransaction) throw new Error("nested database transactions are forbidden");
		this.#inTransaction = true;
		try {
			this.#database.exec("BEGIN IMMEDIATE");
			const result = work();
			this.#database.exec("COMMIT");
			return result;
		} catch (error) {
			this.#database.exec("ROLLBACK");
			throw error;
		} finally {
			this.#inTransaction = false;
		}
	}

	backupInto(path: string): void {
		this.#database.query("VACUUM INTO ?").run(path);
	}

	integrityCheckDetail(): string {
		return (
			this.#database.query<{ integrity_check: string }, []>("PRAGMA integrity_check").get()?.integrity_check ??
			"unknown"
		);
	}

	close(): void {
		this.#database.close();
	}

	private migrate(): void {
		this.#database.exec(
			"CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
		);
		const current = this.schemaVersion;
		if (current > LATEST_SCHEMA_VERSION) {
			throw new DatabaseStartupError(
				"newer_schema",
				`database schema ${current} is newer than supported schema ${LATEST_SCHEMA_VERSION}`,
			);
		}
		if (current < 1) {
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE TABLE sessions (origin_key TEXT PRIMARY KEY, gjc_session_id TEXT NOT NULL, created_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(1, new Date().toISOString());
			});
		}
		if (current < 2) {
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, turn_id TEXT NOT NULL, origin_key TEXT NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','inflight','confirmed','failed_ambiguous','expired')), attempts INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(2, new Date().toISOString());
			});
		}

		if (current < 3) {
			this.withTransaction(() => {
				this.#database.exec(
					"ALTER TABLE sessions ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0; ALTER TABLE sessions ADD COLUMN last_activity_at TEXT; ALTER TABLE sessions ADD COLUMN origin_ref_json TEXT; CREATE TABLE recall_snippets (id INTEGER PRIMARY KEY AUTOINCREMENT, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, text TEXT NOT NULL, at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(3, new Date().toISOString());
			});
		}

		if (current < 4) {
			this.withTransaction(() => {
				this.#database.exec("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)");
				this.#database.query("INSERT INTO meta (key, value) VALUES ('instance_id', ?)").run(crypto.randomUUID());
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(4, new Date().toISOString());
			});
		}
		if (current < 5) {
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE TABLE memory_intents (id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload_json TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('queued','written','committed','receipted','quarantined')), created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(5, new Date().toISOString());
			});
		}
		if (current < 6) {
			this.withTransaction(() => {
				this.#database.exec(
					"CREATE TABLE monitors (monitor_id TEXT PRIMARY KEY, name TEXT NOT NULL, trigger_json TEXT NOT NULL, event_types_json TEXT NOT NULL, burst_policy TEXT NOT NULL, channel_target_json TEXT, enabled INTEGER NOT NULL, created_at TEXT NOT NULL); CREATE TABLE monitor_events (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','failed')), batch_id TEXT, updated_at TEXT NOT NULL); CREATE TABLE authored_outputs (event_id TEXT PRIMARY KEY, output_text TEXT NOT NULL, authored_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(6, new Date().toISOString());
			});
		}
		if (current < 7) {
			this.withTransaction(() => {
				// Inbound messages must be durable before dispatch: a message arriving while a turn is
				// in flight was previously processed transiently and lost outright.
				this.#database.exec(
					"CREATE TABLE inbound_messages (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL); CREATE INDEX inbound_messages_claim ON inbound_messages (origin_key, state, received_at)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(7, new Date().toISOString());
			});
		}
		if (current < 8) {
			this.withTransaction(() => {
				// Declined messages are still context, never commands: every inbound platform
				// message lands here so an engaged turn can read the unread diff since the
				// persona's last reply in that conversation.
				this.#database.exec(
					"CREATE TABLE conversation_context (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, author_id TEXT, author_name TEXT, body TEXT NOT NULL, received_at TEXT NOT NULL, consumed_at TEXT); CREATE INDEX conversation_context_unread ON conversation_context (origin_key, consumed_at, received_at)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(8, new Date().toISOString());
			});
		}
		if (current < 9) {
			this.withTransaction(() => {
				// Session growth bound: the gjc transcript grows with every --resume, so
				// turns get slower forever without rotation. turn_count tracks completed
				// turns in the current epoch; the server auto-bumps the epoch at a ceiling.
				this.#database.exec("ALTER TABLE sessions ADD COLUMN turn_count INTEGER NOT NULL DEFAULT 0");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(9, new Date().toISOString());
			});
		}
		if (current < 10) {
			this.withTransaction(() => {
				// Monitor recovery (issue #29): SQLite cannot ALTER a CHECK constraint, so
				// monitor_events is rebuilt with the extended stage contract (authored_
				// no_delivery, failed_no_retry) plus a per-event reclaim counter, and the
				// new bounded evidence tables are created. Recovery semantics:
				// - every legacy row keeps its stage (reconcile now reclaims `batched`),
				// - the two live stranded `batched` rows stay `batched` and are reclaimed
				//   automatically by the next reconcile — no manual DB mutation.
				// Fail-closed: any legacy stage outside the contract is surfaced by the
				// runtime cycle projection as `monitor_settlement_stuck`, never silently
				// accepted.
				this.#database.exec(
					`CREATE TABLE monitor_events_new (event_id TEXT PRIMARY KEY, monitor_id TEXT NOT NULL, event_type TEXT NOT NULL, payload_json TEXT NOT NULL, fired_at TEXT NOT NULL, stage TEXT NOT NULL CHECK(stage IN ('admitted','batched','dispatched','authored','delivered','authored_no_delivery','failed','failed_no_retry')), batch_id TEXT, dispatch_attempts INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
INSERT INTO monitor_events_new (event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, dispatch_attempts, updated_at) SELECT event_id, monitor_id, event_type, payload_json, fired_at, stage, batch_id, 0, updated_at FROM monitor_events;
DROP TABLE monitor_events;
ALTER TABLE monitor_events_new RENAME TO monitor_events;
CREATE TABLE monitor_failures (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, code TEXT NOT NULL, detail TEXT NOT NULL, failed_at TEXT NOT NULL);
CREATE INDEX monitor_failures_event ON monitor_failures (event_id, failed_at);
CREATE TABLE monitor_slots (monitor_id TEXT NOT NULL, slot_at TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (monitor_id, slot_at));`,
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(10, new Date().toISOString());
			});
		}
		if (current < 11) {
			this.withTransaction(() => {
				// Red-team hardening of the #29 recovery design:
				// - dispatch_leases: durable per-event dispatch ownership (owner token +
				//   expiry) so a NEW gateway process cannot re-author an event whose
				//   authoring turn may still be alive in an external gjc session after
				//   the old process died; only expired claims are recoverable, and a
				//   stale attempt's completion can never overwrite a newer lease.
				// - monitor_slots.event_id: slot claim and event admission commit in ONE
				//   transaction, so a crash between "slot claimed" and "event created"
				//   cannot strand a fired-but-unadmitted slot.
				this.#database.exec(
					`CREATE TABLE dispatch_leases (event_id TEXT PRIMARY KEY, owner TEXT NOT NULL, lease_id TEXT NOT NULL, acquired_at TEXT NOT NULL, expires_at TEXT NOT NULL);
CREATE INDEX dispatch_leases_expiry ON dispatch_leases (expires_at);
ALTER TABLE monitor_slots ADD COLUMN event_id TEXT;`,
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(11, new Date().toISOString());
			});
		}
	}

	get instanceId(): string {
		const row = this.#database
			.query<{ value: string }, [string]>("SELECT value FROM meta WHERE key = ?")
			.get("instance_id");
		if (!row) throw new DatabaseStartupError("integrity_check_failed", "meta.instance_id missing after migration");
		return row.value;
	}

	metaGet(key: string): string | undefined {
		return this.#database.query<{ value: string }, [string]>("SELECT value FROM meta WHERE key = ?").get(key)?.value;
	}

	metaSet(key: string, value: string): void {
		this.#database
			.query("INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
			.run(key, value);
	}

	private integrityCheck(): void {
		const result = this.integrityCheckDetail();
		if (result !== "ok")
			throw new DatabaseStartupError("integrity_check_failed", `SQLite integrity_check failed: ${result}`);
	}
}
