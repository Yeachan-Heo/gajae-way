import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";

/**
 * A gjc model selection: either an explicit selector string or a model profile
 * preset. Structurally identical to the config type; duplicated as a local
 * shape so the store layer does not import the config module.
 */
export type GjcModelSelection = string | { readonly preset: string };

export interface ConversationModelRecord {
	readonly selection: GjcModelSelection;
	readonly setBy?: string;
	readonly updatedAt: string;
}

/**
 * Validates a stored selection. Fails closed on anything unexpected: a bad row
 * must not become argv for a gjc spawn.
 */
export function parseModelSelection(value: unknown): GjcModelSelection | undefined {
	if (typeof value === "string") return value.trim() === "" ? undefined : value;
	if (typeof value !== "object" || value === null) return undefined;
	const keys = Object.keys(value);
	if (keys.length !== 1 || keys[0] !== "preset") return undefined;
	const preset = (value as { preset?: unknown }).preset;
	if (typeof preset !== "string" || preset.trim() === "") return undefined;
	return { preset };
}

export const INBOUND_TURN_STATES = ["bound", "accepted", "done"] as const;
export type InboundTurnState = (typeof INBOUND_TURN_STATES)[number];
export const INBOUND_TURN_ROLES = ["trigger", "steer"] as const;
export type InboundTurnRole = (typeof INBOUND_TURN_ROLES)[number];

/**
 * One inbound platform message. Canonical flow: a message is either steered
 * into the origin's running turn (role `steer`, done on receipt) or becomes
 * the trigger of the next turn (role `trigger`); there is no coalescing, no
 * settle window and no expiry - an unanswered row stays `pending` until a
 * turn takes it.
 */
export interface InboundMessageRow {
	readonly message_id: string;
	readonly origin_key: string;
	readonly origin_ref_json: string;
	readonly body: string;
	readonly engagement_json: string | null;
	readonly state: "pending" | "processing" | "done";
	readonly received_at: string;
	readonly turn_role: InboundTurnRole | null;
	readonly turn_epoch: number | null;
	/**
	 * trigger: bound (session chosen, send not acknowledged) -> accepted (the
	 * runtime holds the op) -> done. steer: bound (the steer was ISSUED but the
	 * transport tore before an answer; the message may be inside the turn) ->
	 * done (recorded acceptance), or back to NULL (definitive refusal). A
	 * `bound` steer is never dispatched as a turn of its own.
	 */
	readonly turn_state: InboundTurnState | null;
	readonly turn_op_ref: string | null;
	/** Bound before send so an accepted retired turn can reattach after restart. */
	readonly bound_session_id: string | null;
	/** Stamped at bind, before the send; the earliest wall clock this turn's answer can carry. */
	readonly dispatched_at: string | null;
	/** JSON map part -> ledger delivery id that satisfied that terminal reply slot. */
	readonly terminal_delivery_id: string | null;
}

/** A trigger row's turn, as the actor tracks it. */
export interface InboundTurn {
	readonly originKey: string;
	readonly epoch: number;
	readonly state: Extract<InboundTurnState, "bound" | "accepted">;
	readonly opRef: string;
	readonly sessionId: string | null;
	readonly triggerMessageId: string;
}

/** A second nonterminal trigger in one epoch would violate at-most-one running turn. */
export class InboundTurnConflictError extends Error {
	constructor(originKey: string, epoch: number) {
		super(`origin ${originKey} already has a nonterminal turn in epoch ${epoch}`);
		this.name = "InboundTurnConflictError";
	}
}

const LATEST_SCHEMA_VERSION = 19;
/** Maximum number of prior messages supplied to one engaged conversation turn. */
export const CONVERSATION_DIFF_MAX_ROWS = 60;
/** Maximum age of prior messages supplied to one engaged conversation turn. */
export const CONVERSATION_DIFF_MAX_AGE_MS = 6 * 60 * 60 * 1000;
/** Consumed context bodies older than this are deleted after aggregate evidence is retained. */
export const CONVERSATION_CONTEXT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

function freshTurnMetaKey(originKey: string, epoch: number, triggerMessageId: string): string {
	return `fresh_turn_attempt:${originKey}:${epoch}:${triggerMessageId}`;
}

export interface ConversationContextRow {
	readonly message_id: string;
	readonly author_id: string | null;
	readonly author_name: string | null;
	readonly body: string;
	readonly received_at: string;
}

export interface ConversationContextDiagnostics {
	readonly unread: number;
	readonly expired: number;
	readonly truncated: number;
	readonly omittedOldestAt: string | null;
	readonly omittedNewestAt: string | null;
	readonly floorAt: string | null;
}

export interface ConversationContextWindow {
	readonly rows: readonly ConversationContextRow[];
	readonly selectedMessageIds: readonly string[];
	readonly effectiveFloor: string;
	readonly expiredCount: number;
	readonly truncatedCount: number;
	readonly omittedOldestAt: string | null;
	readonly omittedNewestAt: string | null;
	readonly omissionRevision: number;
	readonly diagnostics: ConversationContextDiagnostics;
}

function parseStringList(value: string): readonly string[] {
	try {
		const parsed = JSON.parse(value);
		return Array.isArray(parsed) && parsed.every((item) => typeof item === "string") ? parsed : ["projection_corrupt"];
	} catch {
		return ["projection_corrupt"];
	}
}
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

	getSessionBootstrap(originKey: string):
		| {
				epoch: number;
				lastBootstrappedEpoch: number;
				appliedAt: string | null;
				includedSections: readonly string[];
				byteCount: number;
				truncated: boolean;
				diagnostics: readonly string[];
		  }
		| undefined {
		const row = this.#database
			.query<
				{
					epoch: number;
					last_bootstrapped_epoch: number;
					bootstrap_applied_at: string | null;
					bootstrap_sections_json: string;
					bootstrap_byte_count: number;
					bootstrap_truncated: number;
					bootstrap_diagnostics_json: string;
				},
				[string]
			>(
				"SELECT epoch, last_bootstrapped_epoch, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions WHERE origin_key = ?",
			)
			.get(originKey);
		if (!row) return undefined;
		return {
			epoch: row.epoch,
			lastBootstrappedEpoch: row.last_bootstrapped_epoch,
			appliedAt: row.bootstrap_applied_at,
			includedSections: parseStringList(row.bootstrap_sections_json),
			byteCount: row.bootstrap_byte_count,
			truncated: row.bootstrap_truncated === 1,
			diagnostics: parseStringList(row.bootstrap_diagnostics_json),
		};
	}

	markSessionBootstrapped(
		originKey: string,
		epoch: number,
		projection: {
			readonly includedSections: readonly string[];
			readonly byteCount: number;
			readonly truncated: boolean;
			readonly diagnostics: readonly string[];
		},
	): boolean {
		const now = new Date().toISOString();
		return (
			this.#database
				.query(
					"UPDATE sessions SET last_bootstrapped_epoch = ?, bootstrap_applied_at = ?, bootstrap_sections_json = ?, bootstrap_byte_count = ?, bootstrap_truncated = ?, bootstrap_diagnostics_json = ? WHERE origin_key = ? AND epoch = ? AND last_bootstrapped_epoch < ?",
				)
				.run(
					epoch,
					now,
					JSON.stringify(projection.includedSections),
					projection.byteCount,
					projection.truncated ? 1 : 0,
					JSON.stringify(projection.diagnostics),
					originKey,
					epoch,
					epoch,
				).changes === 1
		);
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

	/**
	 * Rebind reset for a poisoned gjc session key (#13): the same semantics as
	 * the `/new` reset path — epoch + 1, turn_count back to 0, and the gjc
	 * binding cleared, so the next session.create op derives a fresh idempotency
	 * key and a recovery near the rotation boundary cannot instantly discard its
	 * fresh binding on an inherited count — but the stored origin ref is kept,
	 * because a rebind is a runtime recovery rather than a user command and
	 * carries no origin payload of its own.
	 */
	rebindEpoch(originKey: string): number {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO sessions (origin_key, gjc_session_id, epoch, turn_count, created_at, last_activity_at) VALUES (?, '', 1, 0, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET epoch = epoch + 1, gjc_session_id = '', turn_count = 0, last_activity_at = excluded.last_activity_at",
			)
			.run(originKey, now, now);
		const row = this.#database
			.query<{ epoch: number }, [string]>("SELECT epoch FROM sessions WHERE origin_key = ?")
			.get(originKey);
		if (!row) throw new Error(`session row for ${originKey} disappeared during rebind`);
		return row.epoch;
	}

	updateActivity(originKey: string, originRefJson: string): void {
		this.#database
			.query("UPDATE sessions SET last_activity_at = ?, origin_ref_json = ? WHERE origin_key = ?")
			.run(new Date().toISOString(), originRefJson, originKey);
	}

	/**
	 * Counts completed monitor-authoring turns within the current epoch for
	 * observability and safety diagnostics. Persistent SDK sessions rely on native
	 * compaction; this count never rotates a persona or worker epoch.
	 *
	 * The write is an upsert because monitor authoring origins (issue #68) reach
	 * this path before any chat turn ever bound their session row. When
	 * `originRefJson` is given the row is created with it, so the seeded row is
	 * projectable (admin/cycle) instead of an origin-less stub.
	 */
	incrementTurnCount(originKey: string, originRefJson?: string): number {
		const now = new Date().toISOString();
		this.#database
			.query(
				"INSERT INTO sessions (origin_key, origin_ref_json, gjc_session_id, epoch, turn_count, created_at, last_activity_at) VALUES (?, ?, '', 0, 1, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET turn_count = turn_count + 1",
			)
			.run(originKey, originRefJson ?? null, now, now);
		return (
			this.#database
				.query<{ turn_count: number }, [string]>("SELECT turn_count FROM sessions WHERE origin_key = ?")
				.get(originKey)?.turn_count ?? 0
		);
	}

	/** Completed turns in the current epoch, without mutating the counter. */
	sessionTurnCount(originKey: string): number {
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
		last_bootstrapped_epoch: number;
		bootstrap_applied_at: string | null;
		bootstrap_sections_json: string;
		bootstrap_byte_count: number;
		bootstrap_truncated: number;
		bootstrap_diagnostics_json: string;
	}> {
		return this.#database
			.query(
				"SELECT origin_ref_json, created_at, last_activity_at, epoch, last_bootstrapped_epoch, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions ORDER BY created_at",
			)
			.all() as Array<{
			origin_ref_json: string | null;
			created_at: string;
			last_activity_at: string | null;
			epoch: number;
			last_bootstrapped_epoch: number;
			bootstrap_applied_at: string | null;
			bootstrap_sections_json: string;
			bootstrap_byte_count: number;
			bootstrap_truncated: number;
			bootstrap_diagnostics_json: string;
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
		last_bootstrapped_epoch: number;
		bootstrap_applied_at: string | null;
		bootstrap_sections_json: string;
		bootstrap_byte_count: number;
		bootstrap_truncated: number;
		bootstrap_diagnostics_json: string;
	}> {
		return this.#database
			.query(
				"SELECT origin_key, origin_ref_json, gjc_session_id, epoch, created_at, last_activity_at, last_bootstrapped_epoch, bootstrap_applied_at, bootstrap_sections_json, bootstrap_byte_count, bootstrap_truncated, bootstrap_diagnostics_json FROM sessions ORDER BY created_at",
			)
			.all() as Array<{
			origin_key: string;
			origin_ref_json: string | null;
			gjc_session_id: string;
			epoch: number;
			created_at: string;
			last_activity_at: string | null;
			last_bootstrapped_epoch: number;
			bootstrap_applied_at: string | null;
			bootstrap_sections_json: string;
			bootstrap_byte_count: number;
			bootstrap_truncated: number;
			bootstrap_diagnostics_json: string;
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
			.query("SELECT id, kind, payload_json, state FROM memory_intents ORDER BY rowid")
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
		receivedAt?: string;
	}): boolean {
		const receivedAt = row.receivedAt ?? new Date().toISOString();
		if (!Number.isFinite(Date.parse(receivedAt))) throw new Error("inbound receivedAt must be an ISO timestamp");
		const changes = this.#database
			.query(
				"INSERT INTO inbound_messages (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at) VALUES (?, ?, ?, ?, ?, 'pending', ?) ON CONFLICT(message_id) DO NOTHING",
			)
			.run(row.messageId, row.originKey, row.originRefJson, row.body, row.engagementJson ?? null, receivedAt);
		return changes.changes > 0;
	}

	inboundColumns(): string {
		return "message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at, turn_role, turn_epoch, turn_state, turn_op_ref, bound_session_id, dispatched_at, terminal_delivery_id";
	}

	/**
	 * Oldest pending row not yet part of any turn: the next trigger or steer.
	 * Ties on `received_at` (a burst inside one millisecond) break on insertion
	 * order, never on the platform message id, so fragments are steered in the
	 * order they arrived.
	 */
	inboundPendingOldest(originKey: string): InboundMessageRow | undefined {
		return (
			this.#database
				.query<InboundMessageRow, [string]>(
					`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE origin_key = ? AND state = 'pending' AND turn_state IS NULL ORDER BY received_at, rowid LIMIT 1`,
				)
				.get(originKey) ?? undefined
		);
	}

	/**
	 * Binds the oldest pending row as the trigger of a new turn: session chosen,
	 * op-ref fixed, `dispatched_at` stamped - all BEFORE the send, so the stamp
	 * is provably ahead of any answer. The partial unique index rejects a second
	 * nonterminal trigger in the same epoch.
	 */
	inboundBindTurn(input: {
		messageId: string;
		originKey: string;
		epoch: number;
		opRef: string;
		sessionId: string;
		dispatchedAt?: string;
	}): InboundMessageRow {
		if (!Number.isSafeInteger(input.epoch) || input.epoch < 0)
			throw new Error("turn epoch must be a non-negative integer");
		if (!input.sessionId) throw new Error("turn session id must not be empty");
		return this.withTransaction(() => {
			const existing = this.#database
				.query<{ n: number }, [string, number]>(
					"SELECT COUNT(*) AS n FROM inbound_messages WHERE origin_key = ? AND turn_epoch = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted')",
				)
				.get(input.originKey, input.epoch)?.n;
			if ((existing ?? 0) > 0) throw new InboundTurnConflictError(input.originKey, input.epoch);
			const changes = this.#database
				.query(
					"UPDATE inbound_messages SET turn_role = 'trigger', turn_epoch = ?, turn_state = 'bound', turn_op_ref = ?, bound_session_id = ?, dispatched_at = ?, terminal_delivery_id = NULL WHERE message_id = ? AND origin_key = ? AND state = 'pending' AND turn_state IS NULL",
				)
				.run(
					input.epoch,
					input.opRef,
					input.sessionId,
					input.dispatchedAt ?? new Date().toISOString(),
					input.messageId,
					input.originKey,
				).changes;
			if (changes !== 1) throw new Error(`inbound ${input.messageId} is not a pending unbound row`);
			return this.inboundTurnRow(input.opRef) as InboundMessageRow;
		});
	}

	/**
	 * Records the accepted receipt boundary: the runtime now holds the
	 * operation. A member still riding with the trigger (v18 settled+bound
	 * batch) was in that prompt, so it is done input from here on.
	 */
	inboundTurnAccept(opRef: string): boolean {
		return this.withTransaction(() => {
			const accepted =
				this.#database
					.query(
						"UPDATE inbound_messages SET turn_state = 'accepted' WHERE turn_op_ref = ? AND turn_role = 'trigger' AND state = 'pending' AND turn_state = 'bound'",
					)
					.run(opRef).changes > 0;
			if (accepted)
				this.#database
					.query(
						"UPDATE inbound_messages SET state = 'done', turn_state = 'done' WHERE turn_op_ref = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound'",
					)
					.run(opRef);
			return accepted;
		});
	}

	/** The turn's pre-send dispatch stamp. */
	inboundTurnDispatchedAt(opRef: string): string | undefined {
		return (
			this.#database
				.query<{ dispatched_at: string | null }, [string]>(
					"SELECT dispatched_at FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'",
				)
				.get(opRef)?.dispatched_at ?? undefined
		);
	}

	/**
	 * Claims one terminal reply slot (`part`) of a turn for `deliveryId`. The
	 * trigger row stores the claims as a JSON map of part -> delivery id.
	 * Returns the id that owns the slot after the call: the claimant's when the
	 * slot was free, otherwise the earlier winner's. Durable, so a rebuilt
	 * lifecycle after a restart sees the claim.
	 */
	inboundTurnClaimTerminal(opRef: string, part: number, deliveryId: string): string {
		return this.withTransaction(() => {
			const row = this.#database
				.query<{ terminal_delivery_id: string | null }, [string]>(
					"SELECT terminal_delivery_id FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'",
				)
				.get(opRef);
			let claims: Record<string, string> = {};
			if (row?.terminal_delivery_id) {
				try {
					const parsed: unknown = JSON.parse(row.terminal_delivery_id);
					if (typeof parsed === "object" && parsed !== null) claims = parsed as Record<string, string>;
				} catch {
					claims = {};
				}
			}
			const key = String(part);
			const owner = claims[key];
			if (owner) return owner;
			claims[key] = deliveryId;
			this.#database
				.query("UPDATE inbound_messages SET terminal_delivery_id = ? WHERE turn_op_ref = ? AND turn_role = 'trigger'")
				.run(JSON.stringify(claims), opRef);
			return deliveryId;
		});
	}

	/**
	 * Terminal reconciliation: legacy state and turn state become done in one
	 * statement for the trigger. A steer still `bound` at this point was issued
	 * into the turn but never answered (torn transport): whether the model saw
	 * it is unknowable now that the turn is over, so it is NOT closed and NOT
	 * dispatched - it stays attributed to this op-ref as an operator-visible
	 * hold (`inboundSteersHeld`). Returns how many TRIGGER rows closed, so a
	 * repeat call is a no-op.
	 */
	inboundTurnComplete(opRef: string): number {
		return this.#database
			.query(
				"UPDATE inbound_messages SET state = 'done', turn_state = 'done' WHERE turn_op_ref = ? AND turn_role = 'trigger' AND state = 'pending' AND turn_state IN ('bound', 'accepted')",
			)
			.run(opRef).changes;
	}

	/**
	 * Releases a turn whose send provably never landed, or whose op failed, for
	 * one fresh turn: the trigger goes back to plain pending. The retry ordinal
	 * is durable so a crash cannot recreate the same op-ref.
	 */
	inboundTurnRequeue(opRef: string): number {
		return this.withTransaction(() => {
			const trigger = this.inboundTurnRow(opRef);
			if (!trigger || trigger.turn_epoch === null) throw new Error(`turn ${opRef} has no retryable trigger`);
			if (trigger.state !== "pending" || !["bound", "accepted"].includes(trigger.turn_state ?? ""))
				throw new Error(`turn ${opRef} cannot be requeued from its current lifecycle state`);
			const key = freshTurnMetaKey(trigger.origin_key, trigger.turn_epoch, trigger.message_id);
			const prior = Number.parseInt(this.metaGet(key) ?? "0", 10);
			const attempt = Number.isSafeInteger(prior) && prior >= 0 ? prior + 1 : 1;
			this.metaSet(key, String(attempt));
			// The trigger and any member still riding with it (a v18 settled+bound
			// batch whose send was proven absent) go back to plain pending together.
			this.#database
				.query(
					"UPDATE inbound_messages SET turn_role = NULL, turn_epoch = NULL, turn_state = NULL, turn_op_ref = NULL, bound_session_id = NULL, dispatched_at = NULL, terminal_delivery_id = NULL WHERE turn_op_ref = ? AND state = 'pending' AND (turn_role = 'trigger' OR (turn_role = 'steer' AND turn_state = 'bound'))",
				)
				.run(opRef);
			return attempt;
		});
	}

	freshTurnAttempt(originKey: string, epoch: number, triggerMessageId: string): number {
		const value = Number.parseInt(this.metaGet(freshTurnMetaKey(originKey, epoch, triggerMessageId)) ?? "0", 10);
		return Number.isSafeInteger(value) && value >= 0 ? value : 0;
	}

	/**
	 * Marks a pending row as a steer ISSUED into the running turn, before the
	 * transport answers. It is attributed to the turn from this moment: no
	 * dispatch path can take it as a trigger, and a restart finds it here.
	 */
	inboundSteerIssued(input: { messageId: string; epoch: number; opRef: string }): boolean {
		return (
			this.#database
				.query(
					"UPDATE inbound_messages SET turn_role = 'steer', turn_epoch = ?, turn_state = 'bound', turn_op_ref = ? WHERE message_id = ? AND state = 'pending' AND (turn_state IS NULL OR (turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?))",
				)
				.run(input.epoch, input.opRef, input.messageId, input.opRef).changes === 1
		);
	}

	/** The runtime recorded the steer: the row is done input of the turn. */
	inboundSteerAccepted(input: { messageId: string; epoch: number; opRef: string }): boolean {
		const result = this.#database
			.query(
				"UPDATE inbound_messages SET state = 'done', turn_role = 'steer', turn_epoch = ?, turn_state = 'done', turn_op_ref = ? WHERE message_id = ? AND state = 'pending' AND (turn_state IS NULL OR (turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?))",
			)
			.run(input.epoch, input.opRef, input.messageId, input.opRef);
		return result.changes === 1;
	}

	/** The runtime definitively refused the steer: the row is an ordinary pending message again. */
	inboundSteerRefused(messageId: string, opRef: string): boolean {
		return (
			this.#database
				.query(
					"UPDATE inbound_messages SET turn_role = NULL, turn_epoch = NULL, turn_state = NULL, turn_op_ref = NULL WHERE message_id = ? AND state = 'pending' AND turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?",
				)
				.run(messageId, opRef).changes === 1
		);
	}

	/** Held steers of this origin whose turn is already terminal: unresolvable by the turn, only by a clientRef replay. */
	inboundSteersHeldAfterTerminal(originKey: string): readonly InboundMessageRow[] {
		return this.#database
			.query<InboundMessageRow, [string]>(
				`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE origin_key = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound' AND turn_op_ref IN (SELECT turn_op_ref FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state = 'done') ORDER BY received_at, rowid`,
			)
			.all(originKey);
	}

	/** Steers issued into `opRef` whose outcome is still unknown, oldest first. */
	inboundSteersHeld(opRef: string): readonly InboundMessageRow[] {
		return this.#database
			.query<InboundMessageRow, [string]>(
				`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound' ORDER BY received_at, rowid`,
			)
			.all(opRef);
	}

	/** The trigger row of a turn. */
	inboundTurnRow(opRef: string): InboundMessageRow | undefined {
		return (
			this.#database
				.query<InboundMessageRow, [string]>(
					`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'`,
				)
				.get(opRef) ?? undefined
		);
	}

	/** Every row attributed to a turn: its trigger and the steers it absorbed. */
	inboundTurnRows(opRef: string): readonly InboundMessageRow[] {
		return this.#database
			.query<InboundMessageRow, [string]>(
				`SELECT ${this.inboundColumns()} FROM inbound_messages WHERE turn_op_ref = ? ORDER BY received_at, rowid`,
			)
			.all(opRef);
	}

	/** Current and retired nonterminal trigger rows are the recovery subjects. */
	inboundNonterminalTurns(originKey: string, epoch?: number): readonly InboundTurn[] {
		type Row = {
			origin_key: string;
			turn_epoch: number;
			turn_state: Extract<InboundTurnState, "bound" | "accepted">;
			turn_op_ref: string;
			bound_session_id: string | null;
			message_id: string;
		};
		const select =
			"SELECT origin_key, turn_epoch, turn_state, turn_op_ref, bound_session_id, message_id FROM inbound_messages";
		const rows =
			epoch === undefined
				? this.#database
						.query<Row, [string]>(
							`${select} WHERE origin_key = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') ORDER BY turn_epoch, received_at, message_id`,
						)
						.all(originKey)
				: this.#database
						.query<Row, [string, number]>(
							`${select} WHERE origin_key = ? AND turn_epoch = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') ORDER BY received_at, message_id`,
						)
						.all(originKey, epoch);
		return rows.map((row) => ({
			originKey: row.origin_key,
			epoch: row.turn_epoch,
			state: row.turn_state,
			opRef: row.turn_op_ref,
			sessionId: row.bound_session_id,
			triggerMessageId: row.message_id,
		}));
	}

	/** Origins with pending rows not yet in a turn: after a restart these have no actor, so recovery must admit them. */
	inboundPendingOrigins(): readonly string[] {
		return this.#database
			.query<{ origin_key: string }, []>(
				"SELECT DISTINCT origin_key FROM inbound_messages WHERE state = 'pending' AND turn_state IS NULL",
			)
			.all()
			.map((row) => row.origin_key);
	}

	/** Origins with bound/accepted turns that need actor reconstruction after boot. */
	inboundNonterminalOrigins(): readonly string[] {
		return this.#database
			.query<{ origin_key: string }, []>(
				"SELECT DISTINCT origin_key FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') ORDER BY origin_key",
			)
			.all()
			.map((row) => row.origin_key);
	}

	inboundNonterminalTurnCount(): number {
		return (
			this.#database
				.query<{ n: number }, []>(
					"SELECT COUNT(*) AS n FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted')",
				)
				.get()?.n ?? 0
		);
	}

	/**
	 * `/new` discards the pending, not-yet-in-a-turn rows received at or before
	 * the floor. This is the ONLY path that completes an inbound row without a
	 * turn, and it is user-initiated; nothing expires a queued message on age.
	 */
	inboundDiscardBefore(originKey: string, floorAt: string): string[] {
		const discard = () => {
			const ids = this.#database
				.query<{ message_id: string }, [string, string]>(
					"SELECT message_id FROM inbound_messages WHERE origin_key = ? AND state = 'pending' AND turn_state IS NULL AND received_at <= ?",
				)
				.all(originKey, floorAt)
				.map((row) => row.message_id);
			this.#database
				.query(
					"UPDATE inbound_messages SET state = 'done' WHERE origin_key = ? AND state = 'pending' AND turn_state IS NULL AND received_at <= ?",
				)
				.run(originKey, floorAt);
			return ids;
		};
		// resetConversationSession already owns a single atomic transaction; opening
		// another one would break its epoch/floor invariant. Standalone callers get
		// the same atomic select-and-discard boundary here.
		return this.#inTransaction ? discard() : this.withTransaction(discard);
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
		receivedAt?: string;
	}): void {
		const inserted = this.#database
			.query(
				"INSERT INTO conversation_context (message_id, origin_key, author_id, author_name, body, received_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(message_id) DO NOTHING",
			)
			.run(
				row.messageId,
				row.originKey,
				row.authorId ?? null,
				row.authorName ?? null,
				row.body,
				row.receivedAt ?? new Date().toISOString(),
			);
		if (inserted.changes > 0) this.contextMaintain();
	}

	/**
	 * Active retention boundary. Unread rows older than the six-hour relevance
	 * window are expired with aggregate-only evidence; consumed bodies are kept
	 * for a short operational interval, then deleted. Recent unread rows remain
	 * untouched so a failed turn can retry them.
	 */
	contextMaintain(
		now = new Date(),
		retentionMs = CONVERSATION_CONTEXT_RETENTION_MS,
	): { expired: number; deleted: number } {
		if (!Number.isSafeInteger(retentionMs) || retentionMs < CONVERSATION_DIFF_MAX_AGE_MS)
			throw new Error("context retention must cover the unread relevance window");
		return this.withTransaction(() => {
			const at = now.toISOString();
			const relevanceCutoff = new Date(now.getTime() - CONVERSATION_DIFF_MAX_AGE_MS).toISOString();
			const groups = this.#database
				.query<{ origin_key: string; count: number; oldest: string | null; newest: string | null }, [string]>(
					"SELECT origin_key, COUNT(*) AS count, MIN(received_at) AS oldest, MAX(received_at) AS newest FROM conversation_context WHERE consumed_at IS NULL AND received_at < ? GROUP BY origin_key",
				)
				.all(relevanceCutoff);
			let expired = 0;
			for (const group of groups) {
				this.#database
					.query(
						"UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND consumed_at IS NULL AND received_at < ?",
					)
					.run(at, group.origin_key, relevanceCutoff);
				this.#recordContextOmissions(
					group.origin_key,
					{ count: group.count, oldest: group.oldest, newest: group.newest },
					{ count: 0, oldest: null, newest: null },
					at,
				);
				expired += group.count;
			}
			const deleteCutoff = new Date(now.getTime() - retentionMs).toISOString();
			const deleted = this.#database
				.query("DELETE FROM conversation_context WHERE consumed_at IS NOT NULL AND received_at < ?")
				.run(deleteCutoff).changes;
			return { expired, deleted };
		});
	}

	contextUnread(originKey: string, limit = 100): ConversationContextRow[] {
		return this.#database
			.query<
				{ message_id: string; author_id: string | null; author_name: string | null; body: string; received_at: string },
				[string, number]
			>(
				"SELECT message_id, author_id, author_name, body, received_at FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL ORDER BY received_at, message_id, rowid LIMIT ?",
			)
			.all(originKey, limit);
	}

	/** When the current session row for this origin was created, if any. */
	contextSessionCreatedAt(originKey: string): string | undefined {
		return this.#database
			.query<{ created_at: string }, [string]>("SELECT created_at FROM sessions WHERE origin_key = ?")
			.get(originKey)?.created_at;
	}

	contextFloorAt(originKey: string): string | undefined {
		return (
			this.#database
				.query<{ floor_at: string | null }, [string]>(
					"SELECT floor_at FROM conversation_context_state WHERE origin_key = ?",
				)
				.get(originKey)?.floor_at ?? undefined
		);
	}

	/**
	 * Atomically prepares the newest bounded unread diff and expires everything
	 * outside it. Selected rows remain unread until the caller proves a terminal
	 * turn outcome; omitted rows are consumed immediately so they cannot replay in
	 * later chunks. Bodies never enter the aggregate diagnostics table.
	 */
	/**
	 * Recent conversation for a fresh session: the last `limit` platform messages
	 * (consumed or not) plus the persona's own confirmed replies, oldest first.
	 * Gives a new epoch the thread it is joining instead of only the unread diff.
	 */
	recentConversation(
		originKey: string,
		conversationId: string,
		limit: number,
		sinceIso: string,
	): Array<{ id?: string; at: string; author: string; body: string }> {
		// /new sets a floor: nothing from before the reset is ever shown again.
		const state = this.#database
			.query<{ floor_at: string | null; floor_row_id: number | null }, [string]>(
				"SELECT floor_at, floor_row_id FROM conversation_context_state WHERE origin_key = ?",
			)
			.get(originKey);
		// Everyone in the window: humans, other bots, and the persona itself.
		const floorAt = [state?.floor_at ?? "", sinceIso].sort().at(-1) ?? sinceIso;
		const floorRowId = state?.floor_row_id ?? 0;
		const inbound = this.#database
			.query<
				{ message_id: string; received_at: string; author_name: string | null; author_id: string | null; body: string },
				[string, string, number, number]
			>(
				"SELECT message_id, received_at, author_name, author_id, body FROM conversation_context WHERE origin_key = ? AND body NOT LIKE '[reaction]%' AND received_at >= ? AND rowid > ? ORDER BY received_at DESC LIMIT ?",
			)
			.all(originKey, floorAt, floorRowId, limit)
			.map((row) => ({
				id: row.message_id,
				at: row.received_at,
				author: row.author_name ?? row.author_id ?? "unknown",
				body: row.body,
			}));
		const replies = this.#database
			.query<{ created_at: string; payload_json: string }, [string, string, number]>(
				"SELECT created_at, payload_json FROM deliveries WHERE state = 'confirmed' AND json_extract(payload_json, '$.origin.conversationId') = ? AND json_extract(payload_json, '$.reaction') IS NULL AND created_at >= ? ORDER BY created_at DESC LIMIT ?",
			)
			.all(conversationId, floorAt, limit)
			.map((row) => {
				const text = (JSON.parse(row.payload_json) as { text?: unknown }).text;
				return { at: row.created_at, author: "you", body: typeof text === "string" ? text : "" };
			})
			.filter((row) => row.body.length > 0);
		return [...inbound, ...replies].sort((a, b) => a.at.localeCompare(b.at)).slice(-limit);
	}

	contextWindow(
		originKey: string,
		triggerMessageId: string,
		now = new Date(),
		limit = CONVERSATION_DIFF_MAX_ROWS,
		maxAgeMs = CONVERSATION_DIFF_MAX_AGE_MS,
	): ConversationContextWindow {
		if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("context window limit must be positive");
		if (!Number.isSafeInteger(maxAgeMs) || maxAgeMs < 0) throw new Error("context max age must be non-negative");
		return this.withTransaction(() => {
			const state = this.#database
				.query<{ floor_at: string | null; floor_row_id: number }, [string]>(
					"SELECT floor_at, floor_row_id FROM conversation_context_state WHERE origin_key = ?",
				)
				.get(originKey);
			const floorRowId = state?.floor_row_id ?? 0;
			const ageFloor = new Date(now.getTime() - maxAgeMs).toISOString();
			// `/new` owns the durable context floor. A session row is written only after
			// inbound acceptance, so treating its creation timestamp as a floor would
			// erase the very first settled batch before its persistent session can read it.
			const effectiveFloor = [ageFloor, state?.floor_at]
				.filter((value): value is string => value !== undefined && value !== null)
				.sort()
				.at(-1) as string;
			const expired = this.#contextAggregate(
				"origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND (received_at < ? OR rowid <= ?)",
				[originKey, triggerMessageId, effectiveFloor, floorRowId],
			);
			if (expired.count > 0) {
				this.#database
					.query(
						"UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND (received_at < ? OR rowid <= ?)",
					)
					.run(now.toISOString(), originKey, triggerMessageId, effectiveFloor, floorRowId);
			}

			const newest = this.#database
				.query<ConversationContextRow & { row_id: number }, [string, string, string, number, number]>(
					"SELECT rowid AS row_id, message_id, author_id, author_name, body, received_at FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND received_at >= ? AND rowid > ? ORDER BY received_at DESC, message_id DESC, rowid DESC LIMIT ?",
				)
				.all(originKey, triggerMessageId, effectiveFloor, floorRowId, limit);
			const boundary = newest[newest.length - 1];
			const truncated = boundary
				? this.#contextAggregate(
						"origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND received_at >= ? AND (received_at < ? OR (received_at = ? AND message_id < ?) OR (received_at = ? AND message_id = ? AND rowid < ?))",
						[
							originKey,
							triggerMessageId,
							effectiveFloor,
							boundary.received_at,
							boundary.received_at,
							boundary.message_id,
							boundary.received_at,
							boundary.message_id,
							boundary.row_id,
						],
					)
				: { count: 0, oldest: null, newest: null };
			if (boundary && truncated.count > 0) {
				this.#database
					.query(
						"UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND received_at >= ? AND (received_at < ? OR (received_at = ? AND message_id < ?) OR (received_at = ? AND message_id = ? AND rowid < ?))",
					)
					.run(
						now.toISOString(),
						originKey,
						triggerMessageId,
						effectiveFloor,
						boundary.received_at,
						boundary.received_at,
						boundary.message_id,
						boundary.received_at,
						boundary.message_id,
						boundary.row_id,
					);
			}
			this.#recordContextOmissions(originKey, expired, truncated, now.toISOString());
			const pending = this.#pendingContextEvidence(originKey);
			const rows = newest
				.slice()
				.reverse()
				.map(({ row_id: _rowId, ...row }) => row);
			return {
				rows,
				selectedMessageIds: rows.map((row) => row.message_id),
				effectiveFloor,
				expiredCount: pending.expired,
				truncatedCount: pending.truncated,
				omittedOldestAt: pending.oldest,
				omittedNewestAt: pending.newest,
				omissionRevision: pending.revision,
				diagnostics: this.contextDiagnostics(originKey),
			};
		});
	}

	/** Establishes a durable reset floor and expires every pre-floor unread row. */
	contextSetFloor(originKey: string, floorAt = new Date().toISOString()): void {
		const floorRowId =
			this.#database
				.query<{ row_id: number }, [string]>(
					"SELECT COALESCE(MAX(rowid), 0) AS row_id FROM conversation_context WHERE origin_key = ?",
				)
				.get(originKey)?.row_id ?? 0;
		const expired = this.#contextAggregate(
			"origin_key = ? AND consumed_at IS NULL AND (received_at < ? OR rowid <= ?)",
			[originKey, floorAt, floorRowId],
		);
		if (expired.count > 0)
			this.#database
				.query(
					"UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND consumed_at IS NULL AND (received_at < ? OR rowid <= ?)",
				)
				.run(floorAt, originKey, floorAt, floorRowId);
		this.#database
			.query(
				"INSERT INTO conversation_context_state (origin_key, floor_at, floor_row_id) VALUES (?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET floor_at = excluded.floor_at, floor_row_id = excluded.floor_row_id",
			)
			.run(originKey, floorAt, floorRowId);
		this.#recordContextOmissions(originKey, expired, { count: 0, oldest: null, newest: null }, floorAt);
	}

	/** Commits a successful/duplicate-safe turn cursor and acknowledges its omission notice atomically. */
	contextCommitWindow(originKey: string, messageIds: readonly string[], omissionRevision: number): void {
		const now = new Date().toISOString();
		this.withTransaction(() => {
			for (const id of messageIds)
				this.#database.query("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ?").run(now, id);
			this.#database
				.query(
					"UPDATE conversation_context_state SET pending_expired_count = 0, pending_truncated_count = 0, pending_omitted_oldest_at = NULL, pending_omitted_newest_at = NULL WHERE origin_key = ? AND omission_revision = ?",
				)
				.run(originKey, omissionRevision);
		});
	}

	contextDiagnostics(originKey?: string): ConversationContextDiagnostics {
		const unread = originKey
			? (this.#database
					.query<{ n: number }, [string]>(
						"SELECT COUNT(*) AS n FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL",
					)
					.get(originKey)?.n ?? 0)
			: (this.#database
					.query<{ n: number }, []>("SELECT COUNT(*) AS n FROM conversation_context WHERE consumed_at IS NULL")
					.get()?.n ?? 0);
		const state = originKey
			? this.#database
					.query<
						{
							expired: number;
							truncated: number;
							oldest: string | null;
							newest: string | null;
							floor: string | null;
						},
						[string]
					>(
						"SELECT expired_count AS expired, truncated_count AS truncated, omitted_oldest_at AS oldest, omitted_newest_at AS newest, floor_at AS floor FROM conversation_context_state WHERE origin_key = ?",
					)
					.get(originKey)
			: this.#database
					.query<{ expired: number; truncated: number; oldest: string | null; newest: string | null; floor: null }, []>(
						"SELECT COALESCE(SUM(expired_count), 0) AS expired, COALESCE(SUM(truncated_count), 0) AS truncated, MIN(omitted_oldest_at) AS oldest, MAX(omitted_newest_at) AS newest, NULL AS floor FROM conversation_context_state",
					)
					.get();
		return {
			unread,
			expired: state?.expired ?? 0,
			truncated: state?.truncated ?? 0,
			omittedOldestAt: state?.oldest ?? null,
			omittedNewestAt: state?.newest ?? null,
			floorAt: state?.floor ?? null,
		};
	}

	contextDiagnosticsByOrigin(): Map<string, ConversationContextDiagnostics> {
		const origins = this.#database
			.query<{ origin_key: string }, []>(
				"SELECT origin_key FROM conversation_context UNION SELECT origin_key FROM conversation_context_state",
			)
			.all();
		return new Map(origins.map((row) => [row.origin_key, this.contextDiagnostics(row.origin_key)]));
	}

	#contextAggregate(
		where: string,
		params: readonly (string | number)[],
	): {
		count: number;
		oldest: string | null;
		newest: string | null;
	} {
		const row = this.#database
			.query<{ count: number; oldest: string | null; newest: string | null }, (string | number)[]>(
				`SELECT COUNT(*) AS count, MIN(received_at) AS oldest, MAX(received_at) AS newest FROM conversation_context WHERE ${where}`,
			)
			.get(...params);
		return row ?? { count: 0, oldest: null, newest: null };
	}

	#recordContextOmissions(
		originKey: string,
		expired: { count: number; oldest: string | null; newest: string | null },
		truncated: { count: number; oldest: string | null; newest: string | null },
		at: string,
	): void {
		if (expired.count === 0 && truncated.count === 0) return;
		const oldest =
			[expired.oldest, truncated.oldest].filter((value): value is string => value !== null).sort()[0] ?? null;
		const newest =
			[expired.newest, truncated.newest]
				.filter((value): value is string => value !== null)
				.sort()
				.at(-1) ?? null;
		this.#database.query("INSERT OR IGNORE INTO conversation_context_state (origin_key) VALUES (?)").run(originKey);
		this.#database
			.query(
				"UPDATE conversation_context_state SET expired_count = expired_count + ?, truncated_count = truncated_count + ?, pending_expired_count = pending_expired_count + ?, pending_truncated_count = pending_truncated_count + ?, omission_revision = omission_revision + 1, omitted_oldest_at = CASE WHEN omitted_oldest_at IS NULL OR ? < omitted_oldest_at THEN ? ELSE omitted_oldest_at END, omitted_newest_at = CASE WHEN omitted_newest_at IS NULL OR ? > omitted_newest_at THEN ? ELSE omitted_newest_at END, pending_omitted_oldest_at = CASE WHEN pending_omitted_oldest_at IS NULL OR ? < pending_omitted_oldest_at THEN ? ELSE pending_omitted_oldest_at END, pending_omitted_newest_at = CASE WHEN pending_omitted_newest_at IS NULL OR ? > pending_omitted_newest_at THEN ? ELSE pending_omitted_newest_at END, last_omitted_at = ? WHERE origin_key = ?",
			)
			.run(
				expired.count,
				truncated.count,
				expired.count,
				truncated.count,
				oldest,
				oldest,
				newest,
				newest,
				oldest,
				oldest,
				newest,
				newest,
				at,
				originKey,
			);
	}

	#pendingContextEvidence(originKey: string): {
		expired: number;
		truncated: number;
		oldest: string | null;
		newest: string | null;
		revision: number;
	} {
		return (
			this.#database
				.query<
					{ expired: number; truncated: number; oldest: string | null; newest: string | null; revision: number },
					[string]
				>(
					"SELECT pending_expired_count AS expired, pending_truncated_count AS truncated, pending_omitted_oldest_at AS oldest, pending_omitted_newest_at AS newest, omission_revision AS revision FROM conversation_context_state WHERE origin_key = ?",
				)
				.get(originKey) ?? { expired: 0, truncated: 0, oldest: null, newest: null, revision: 0 }
		);
	}

	contextConsume(messageIds: readonly string[]): void {
		if (messageIds.length === 0) return;
		const now = new Date().toISOString();
		this.withTransaction(() => {
			for (const id of messageIds)
				this.#database.query("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ?").run(now, id);
		});
	}

	/** Whether this origin ever ingested `messageId` (as a turn trigger/steer, or as conversation context). */
	inboundKnownMessage(originKey: string, messageId: string): boolean {
		return (
			(this.#database
				.query<{ n: number }, [string, string, string, string]>(
					"SELECT (SELECT COUNT(*) FROM inbound_messages WHERE origin_key = ? AND message_id = ?) + (SELECT COUNT(*) FROM conversation_context WHERE origin_key = ? AND message_id = ?) AS n",
				)
				.get(originKey, messageId, originKey, messageId)?.n ?? 0) > 0
		);
	}

	/**
	 * Rewrites the recorded body of an already-ingested platform message after
	 * the user edited it, so a later unread diff shows what the message says
	 * now. The edit itself is streamed as its own inbound row.
	 */
	contextUpdateBody(originKey: string, messageId: string, body: string): boolean {
		return (
			this.#database
				.query("UPDATE conversation_context SET body = ? WHERE origin_key = ? AND message_id = ?")
				.run(body, originKey, messageId).changes > 0
		);
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

	/**
	 * Fenced persistent-session binding. A create response may race `/new`; the
	 * binding must never overwrite a newer durable epoch with an old session id.
	 */
	putSessionAtEpoch(originKey: string, sessionId: string, epoch: number): boolean {
		if (!Number.isSafeInteger(epoch) || epoch < 0) throw new Error("session epoch must be a non-negative integer");
		return (
			this.#database
				.query(
					"INSERT INTO sessions (origin_key, gjc_session_id, epoch, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET gjc_session_id = excluded.gjc_session_id WHERE sessions.epoch = excluded.epoch",
				)
				.run(originKey, sessionId, epoch, new Date().toISOString()).changes === 1
		);
	}

	/** Opaque runtime-issued tail checkpoint for a bound SDK session. */
	tailCursorGet(sessionId: string): string | undefined {
		return this.#database
			.query<{ cursor: string }, [string]>("SELECT cursor FROM session_tail_cursors WHERE session_id = ?")
			.get(sessionId)?.cursor;
	}

	/** Commits a cursor only after the caller has applied all preceding tail effects. */
	tailCursorCommit(sessionId: string, cursor: string): void {
		if (cursor.length === 0) throw new Error("tail cursor must not be empty");
		this.#database
			.query(
				"INSERT INTO session_tail_cursors (session_id, cursor, updated_at) VALUES (?, ?, ?) ON CONFLICT(session_id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
			)
			.run(sessionId, cursor, new Date().toISOString());
	}

	tailCursorClear(sessionId: string): void {
		this.#database.query("DELETE FROM session_tail_cursors WHERE session_id = ?").run(sessionId);
	}

	deliveryCreate(row: { id: string; turnId: string; originKey: string; payloadJson: string }): boolean {
		const now = new Date().toISOString();
		return (
			this.#database
				.query(
					"INSERT OR IGNORE INTO deliveries (delivery_id, turn_id, origin_key, payload_json, state, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)",
				)
				.run(row.id, row.turnId, row.originKey, row.payloadJson, now, now).changes === 1
		);
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
		instruction: string | null;
	}): void {
		this.#database
			.query(
				"INSERT INTO monitors (monitor_id, name, trigger_json, event_types_json, burst_policy, channel_target_json, enabled, created_at, instruction) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
				row.instruction,
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
		instruction: string | null;
	}> {
		return this.#database
			.query(
				"SELECT monitor_id, name, trigger_json, event_types_json, burst_policy, channel_target_json, enabled, created_at, instruction FROM monitors ORDER BY created_at",
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
			instruction: string | null;
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
	monitorEventAcquireLease(eventId: string, owner: string, leaseId: string, ttlMs: number, now = Date.now()): boolean {
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
		this.#database.query("DELETE FROM dispatch_leases WHERE event_id = ? AND lease_id = ?").run(eventId, leaseId);
	}
	/** Total live (unexpired) leases — test/ops hygiene seam for leak detection. */
	monitorLeaseLiveCount(now = Date.now()): number {
		return (
			this.#database
				.query<{ n: number }, [string]>("SELECT COUNT(*) AS n FROM dispatch_leases WHERE expires_at > ?")
				.get(new Date(now).toISOString())?.n ?? 0
		);
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
	monitorEventFencedAuthor(
		eventId: string,
		leaseId: string,
		note: string,
		noDelivery = false,
		now = Date.now(),
	): boolean {
		return this.withTransaction(() => {
			const changes = this.#database
				.query(
					`UPDATE monitor_events SET stage = ?, updated_at = ?
WHERE event_id = ? AND EXISTS (
SELECT 1 FROM dispatch_leases l WHERE l.event_id = monitor_events.event_id AND l.lease_id = ? AND l.expires_at > ?
)`,
				)
				.run(
					noDelivery ? "authored_no_delivery" : "authored",
					new Date(now).toISOString(),
					eventId,
					leaseId,
					new Date(now).toISOString(),
				).changes;
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
		eventType: string,
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
				.run(
					noDelivery ? "authored_no_delivery" : "authored",
					new Date(now).toISOString(),
					eventId,
					...leaseArgs,
				).changes;
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
			return true;
		});
	}
	/**
	 * Fenced failure write: failed stage + public-safe evidence row in one
	 * transaction, both gated on the live lease (round-3 blocker 1).
	 */
	monitorEventFencedFail(
		eventId: string,
		leaseId: string,
		batchId: string,
		code: string,
		detail: string,
		now = Date.now(),
	): boolean {
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
	/**
	 * Atomic confirm + settlement (terminal-critic blocker 2): the ledger row
	 * transition to `confirmed` and the batch monitor-event settlement commit in
	 * ONE transaction. `outcome` distinguishes unknown / transitioned /
	 * already_terminal so the server can ack idempotently. A crash can no longer
	 * leave confirmed-ledger/authored-event pairs.
	 */
	deliveryConfirmWithSettle(
		deliveryId: string,
		monitorEventStage: "delivered" | "authored",
	): "unknown" | "transitioned" | "already_terminal" {
		return this.withTransaction(() => {
			const delivery = this.#database
				.query<{ delivery_id: string; turn_id: string; state: string }, [string]>(
					"SELECT delivery_id, turn_id, state FROM deliveries WHERE delivery_id = ?",
				)
				.get(deliveryId);
			if (!delivery) return "unknown";
			if (delivery.state === "confirmed" || delivery.state === "expired") {
				// Idempotent no-op: already terminal. Repairs a legacy split state
				// (confirmed ledger, authored events) if one exists.
				if (delivery.state === "confirmed") {
					this.settleMonitorEventsForTurn(delivery.turn_id, monitorEventStage);
				}
				return "already_terminal";
			}
			this.#database
				.query("UPDATE deliveries SET state = 'confirmed', updated_at = ? WHERE delivery_id = ?")
				.run(new Date().toISOString(), deliveryId);
			this.settleMonitorEventsForTurn(delivery.turn_id, monitorEventStage);
			return "transitioned";
		});
	}
	/** Settles the monitor batch of a turn id (used by the atomic confirm path). */
	settleMonitorEventsForTurn(turnId: string, stage: "delivered" | "authored"): void {
		const events = this.#database
			.query<{ event_id: string }, [string]>("SELECT event_id FROM monitor_events WHERE batch_id = ?")
			.all(turnId);
		for (const event of events) this.monitorEventSettle(event.event_id, stage);
	}
	monitorEventSettle(eventId: string, stage: "delivered" | "authored"): boolean {
		const row = this.#database
			.query<{ stage: string }, [string]>("SELECT stage FROM monitor_events WHERE event_id = ?")
			.get(eventId);
		if (!row) return false;
		// delivered may ONLY be reached from authored: an omitted event still in
		// dispatched/batched can never be promoted by a batch-wide settlement
		// (terminal-critic blocker 3).
		if (stage === "delivered" && row.stage === "authored") {
			this.monitorEventUpdate(eventId, "delivered");
			return true;
		}
		// Fail path: only demote events still in `authored`; never touch delivered
		// (or any terminal stage).
		if (stage === "authored" && row.stage === "authored") return false;
		if (
			stage === "authored" &&
			row.stage !== "delivered" &&
			row.stage !== "authored_no_delivery" &&
			row.stage !== "failed_no_retry"
		) {
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
		return this.withTransaction(() => {
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
		}) as boolean;
	}
	/** Test/recovery seam: move a monitor's creation instant (clamps catch-up). */
	monitorSetCreatedAt(monitorId: string, createdAt: string): void {
		this.#database.query("UPDATE monitors SET created_at = ? WHERE monitor_id = ?").run(createdAt, monitorId);
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
				// v9 retained an observational monitor turn count. Persistent SDK sessions
				// now rely on native compaction; no gateway turn ceiling consumes this column.
				this.#database.exec("ALTER TABLE sessions ADD COLUMN turn_count INTEGER NOT NULL DEFAULT 0");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(9, new Date().toISOString());
			});
		}
		if (current < 10) {
			this.withTransaction(() => {
				// Issue #10: durable lane jobs. One row per job; the record JSON is the
				// fail-closed authority (schema-validated on read by @gajaeway/subsession),
				// while the status column stays a plain indexed projection for operators.
				this.#database.exec(
					"CREATE TABLE lane_jobs (job_id TEXT PRIMARY KEY, lane_key TEXT NOT NULL UNIQUE, branch TEXT NOT NULL, worktree_path TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('running','attempt_ended','awaiting_operator','stalled','done','aborted')), record_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(10, new Date().toISOString());
			});
		}
		if (current < 11) {
			this.withTransaction(() => {
				// Monitor recovery (issue #29): extend the monitor event state contract,
				// preserve every legacy row, and add bounded operator evidence/slot tables.
				// Existing schema-10 lane_jobs deployments reach this step without loss.
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
					.run(11, new Date().toISOString());
			});
		}
		if (current < 12) {
			this.withTransaction(() => {
				// Durable dispatch ownership and atomic scheduled-slot admission.
				this.#database.exec(
					`CREATE TABLE dispatch_leases (event_id TEXT PRIMARY KEY, owner TEXT NOT NULL, lease_id TEXT NOT NULL, acquired_at TEXT NOT NULL, expires_at TEXT NOT NULL);
CREATE INDEX dispatch_leases_expiry ON dispatch_leases (expires_at);
ALTER TABLE monitor_slots ADD COLUMN event_id TEXT;`,
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(12, new Date().toISOString());
			});
		}
		if (current < 13) {
			this.withTransaction(() => {
				// Issue #35: durable reset floors plus aggregate-only omission evidence.
				// Existing context rows remain intact and unread until the bounded policy
				// classifies them; lane jobs, monitor tables, and meta counters are untouched.
				this.#database.exec(
					"CREATE TABLE IF NOT EXISTS conversation_context_state (origin_key TEXT PRIMARY KEY, floor_at TEXT, floor_row_id INTEGER NOT NULL DEFAULT 0, expired_count INTEGER NOT NULL DEFAULT 0, truncated_count INTEGER NOT NULL DEFAULT 0, omitted_oldest_at TEXT, omitted_newest_at TEXT, last_omitted_at TEXT, pending_expired_count INTEGER NOT NULL DEFAULT 0, pending_truncated_count INTEGER NOT NULL DEFAULT 0, pending_omitted_oldest_at TEXT, pending_omitted_newest_at TEXT, omission_revision INTEGER NOT NULL DEFAULT 0)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(13, new Date().toISOString());
			});
		}
		if (current < 14) {
			this.withTransaction(() => {
				// Issue #37: durable once-per-origin-epoch bootstrap projection. Pending
				// state is the monotonic inequality epoch > last_bootstrapped_epoch, so
				// every existing and newly bumped epoch starts pending without a second
				// state machine that can drift from sessions. Bodies are never persisted.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(sessions)")
						.all()
						.map((row) => row.name),
				);
				const additions = [
					["last_bootstrapped_epoch", "INTEGER NOT NULL DEFAULT -1"],
					["bootstrap_applied_at", "TEXT"],
					["bootstrap_sections_json", "TEXT NOT NULL DEFAULT '[]'"],
					["bootstrap_byte_count", "INTEGER NOT NULL DEFAULT 0"],
					["bootstrap_truncated", "INTEGER NOT NULL DEFAULT 0 CHECK(bootstrap_truncated IN (0,1))"],
					["bootstrap_diagnostics_json", "TEXT NOT NULL DEFAULT '[]'"],
				] as const;
				for (const [name, declaration] of additions)
					if (!columns.has(name)) this.#database.exec(`ALTER TABLE sessions ADD COLUMN ${name} ${declaration}`);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(14, new Date().toISOString());
			});
		}
		if (current < 15) {
			this.withTransaction(() => {
				// Per-monitor authoring instruction. Nullable additive column: every
				// existing monitor keeps firing with no instruction and the authoring
				// prompt falls back to the built-in maintenance guidance.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(monitors)")
						.all()
						.map((row) => row.name),
				);
				if (!columns.has("instruction")) this.#database.exec("ALTER TABLE monitors ADD COLUMN instruction TEXT");
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(15, new Date().toISOString());
			});
		}
		if (current < 16) {
			this.withTransaction(() => {
				// Issue #20: per-conversation model override. This lives beside session
				// state rather than in config.json, because config is the owner's file
				// (a slash command must not race their editor) and `model` is not a
				// reloadable field, so a config write would not be read until restart.
				this.#database.exec(
					"CREATE TABLE IF NOT EXISTS conversation_model (origin_key TEXT PRIMARY KEY, selection_json TEXT NOT NULL, set_by TEXT, updated_at TEXT NOT NULL)",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(16, new Date().toISOString());
			});
		}
		if (current < 17) {
			this.withTransaction(() => {
				// Issue #92: lifecycle state and its durable session/cursor binding are
				// additive. Do not rewrite the legacy state column or its v16 CHECK.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
						.all()
						.map((row) => row.name),
				);
				const additions = [
					["batch_key", "TEXT"],
					["batch_role", "TEXT CHECK(batch_role IS NULL OR batch_role IN ('trigger', 'member', 'steer'))"],
					["batch_epoch", "INTEGER"],
					["batch_state", "TEXT CHECK(batch_state IS NULL OR batch_state IN ('settled', 'accepted', 'done'))"],
					["attributed_op_ref", "TEXT"],
					["accepted_at", "TEXT"],
					["bound_session_id", "TEXT"],
				] as const;
				for (const [name, declaration] of additions)
					if (!columns.has(name)) this.#database.exec(`ALTER TABLE inbound_messages ADD COLUMN ${name} ${declaration}`);
				this.#database.exec(
					"CREATE UNIQUE INDEX IF NOT EXISTS inbound_messages_nonterminal_trigger ON inbound_messages (origin_key, batch_epoch) WHERE batch_role = 'trigger' AND batch_state IN ('settled', 'accepted')",
				);
				this.#database.exec(
					"CREATE TABLE IF NOT EXISTS session_tail_cursors (session_id TEXT PRIMARY KEY, cursor TEXT NOT NULL, updated_at TEXT NOT NULL)",
				);
				this.#database.exec(
					"UPDATE inbound_messages SET bound_session_id = (SELECT gjc_session_id FROM sessions WHERE sessions.origin_key = inbound_messages.origin_key AND sessions.epoch = inbound_messages.batch_epoch AND sessions.gjc_session_id <> '') WHERE bound_session_id IS NULL AND batch_state IN ('settled', 'accepted')",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(17, new Date().toISOString());
			});
		}
		if (current < 18) {
			this.withTransaction(() => {
				// dispatched_at: wall-clock stamped at bind time, BEFORE the send, so a
				// tail-less reconcile has a transcript floor that can never postdate
				// the answer (accepted_at is stamped after the CLI returns and can).
				// terminal_delivery_id: which ledger delivery satisfied this batch's
				// one terminal reply slot; a later terminal for the same batch is a
				// no-op regardless of text.
				const columns = new Set(
					this.#database
						.query<{ name: string }, []>("PRAGMA table_info(inbound_messages)")
						.all()
						.map((row) => row.name),
				);
				for (const [name, declaration] of [
					["dispatched_at", "TEXT"],
					["terminal_delivery_id", "TEXT"],
				] as const)
					if (!columns.has(name)) this.#database.exec(`ALTER TABLE inbound_messages ADD COLUMN ${name} ${declaration}`);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(18, new Date().toISOString());
			});
		}
		if (current < 19) {
			this.withTransaction(() => {
				// The batch model is gone: no settle window, no coalesced members, no
				// expiry. One trigger row = one turn; steers attach to the running
				// turn's op-ref. Rebuilt table because SQLite cannot drop CHECKed
				// columns in place. Preserved per row: bound_session_id (reattach
				// after restart), dispatched_at (the attribution floor) and
				// terminal_delivery_id (restart-safe per-part terminal claim).
				// A v18 nonterminal batch maps to a nonterminal turn on its trigger.
				// Its `member` rows were part of that batch's ONE prompt: members of
				// an accepted batch (the runtime holds the op, they were sent) become
				// done input attributed to the turn, like steers. Members of a
				// settled+bound batch sit in the send/ack crash window - the prompt
				// may or may not have reached the runtime - so they ride with the
				// trigger as pending `steer`/`bound` rows and are decided WITH it by
				// recovery (accepted -> done input; send proven absent -> released
				// to plain pending together). Members of a settled+unbound batch
				// were never sent and go back to plain pending. Nothing is deleted.
				// Legacy `processing` rows (the pre-actor claim path, whose startup
				// normalisation is gone) return to pending.
				this.#database.exec(
					"CREATE TABLE inbound_messages_v19 (message_id TEXT PRIMARY KEY, origin_key TEXT NOT NULL, origin_ref_json TEXT NOT NULL, body TEXT NOT NULL, engagement_json TEXT, state TEXT NOT NULL CHECK(state IN ('pending','processing','done')), received_at TEXT NOT NULL, turn_role TEXT CHECK(turn_role IS NULL OR turn_role IN ('trigger', 'steer')), turn_epoch INTEGER, turn_state TEXT CHECK(turn_state IS NULL OR turn_state IN ('bound', 'accepted', 'done')), turn_op_ref TEXT, bound_session_id TEXT, dispatched_at TEXT, terminal_delivery_id TEXT)",
				);
				this.#database.exec(
					"INSERT INTO inbound_messages_v19 (message_id, origin_key, origin_ref_json, body, engagement_json, state, received_at) SELECT message_id, origin_key, origin_ref_json, body, engagement_json, CASE state WHEN 'processing' THEN 'pending' ELSE state END, received_at FROM inbound_messages",
				);
				// Triggers that were bound (session chosen) keep their turn; an unbound
				// settled trigger never had an operation and returns to plain pending.
				this.#database.exec(
					"UPDATE inbound_messages_v19 SET turn_role = 'trigger', turn_epoch = src.batch_epoch, turn_state = CASE src.batch_state WHEN 'settled' THEN 'bound' ELSE src.batch_state END, turn_op_ref = src.attributed_op_ref, bound_session_id = src.bound_session_id, dispatched_at = src.dispatched_at, terminal_delivery_id = src.terminal_delivery_id FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'trigger' AND src.attributed_op_ref IS NOT NULL AND (src.batch_state <> 'settled' OR src.bound_session_id IS NOT NULL)",
				);
				this.#database.exec(
					"UPDATE inbound_messages_v19 SET turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'done', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'steer'",
				);
				this.#database.exec(
					"UPDATE inbound_messages_v19 SET state = 'done', turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'done', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'member' AND src.batch_state IN ('accepted', 'done')",
				);
				this.#database.exec(
					"UPDATE inbound_messages_v19 SET turn_role = 'steer', turn_epoch = src.batch_epoch, turn_state = 'bound', turn_op_ref = src.attributed_op_ref FROM inbound_messages AS src WHERE src.message_id = inbound_messages_v19.message_id AND src.batch_role = 'member' AND src.batch_state = 'settled' AND src.bound_session_id IS NOT NULL AND src.state = 'pending'",
				);
				this.#database.exec("DROP TABLE inbound_messages");
				this.#database.exec("ALTER TABLE inbound_messages_v19 RENAME TO inbound_messages");
				this.#database.exec("CREATE INDEX inbound_messages_claim ON inbound_messages (origin_key, state, received_at)");
				this.#database.exec(
					"CREATE UNIQUE INDEX inbound_messages_nonterminal_trigger ON inbound_messages (origin_key, turn_epoch) WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted')",
				);
				this.#database.exec(
					"CREATE UNIQUE INDEX inbound_messages_turn_trigger ON inbound_messages (turn_op_ref) WHERE turn_role = 'trigger'",
				);
				this.#database
					.query("INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)")
					.run(19, new Date().toISOString());
			});
		}
	}

	// --- Issue #20: per-conversation model override -------------------------

	/**
	 * Reads a conversation's model override. Returns undefined when none is set
	 * and, deliberately, also when the stored row fails to parse: a corrupt
	 * override must degrade to the configured default rather than propagate a
	 * malformed selector into a gjc spawn.
	 */
	conversationModelGet(originKey: string): ConversationModelRecord | undefined {
		const row = this.#database
			.query<{ selection_json: string; set_by: string | null; updated_at: string }, [string]>(
				"SELECT selection_json, set_by, updated_at FROM conversation_model WHERE origin_key = ?",
			)
			.get(originKey);
		if (!row) return undefined;
		let selection: unknown;
		try {
			selection = JSON.parse(row.selection_json);
		} catch {
			return undefined;
		}
		const parsed = parseModelSelection(selection);
		if (!parsed) return undefined;
		return {
			selection: parsed,
			...(row.set_by === null ? {} : { setBy: row.set_by }),
			updatedAt: row.updated_at,
		};
	}

	/** Writes (or replaces) a conversation's model override. */
	conversationModelSet(originKey: string, selection: GjcModelSelection, setBy?: string): void {
		this.#database
			.query(
				"INSERT OR REPLACE INTO conversation_model (origin_key, selection_json, set_by, updated_at) VALUES (?, ?, ?, ?)",
			)
			.run(originKey, JSON.stringify(selection), setBy ?? null, new Date().toISOString());
	}

	/** Drops a conversation's override. Reports whether a row was actually removed. */
	conversationModelClear(originKey: string): boolean {
		const before = this.#database
			.query<{ n: number }, [string]>("SELECT COUNT(*) AS n FROM conversation_model WHERE origin_key = ?")
			.get(originKey);
		this.#database.query("DELETE FROM conversation_model WHERE origin_key = ?").run(originKey);
		return (before?.n ?? 0) > 0;
	}
	// --- Issue #10: durable lane jobs ---------------------------------------

	/**
	 * Upserts a validated lane job record. Callers MUST pass an already
	 * schema-validated record (parseLaneJobRecord output); the database stores
	 * the JSON verbatim so reads can re-validate fail-closed.
	 */
	putLaneJob(job: {
		readonly jobId: string;
		readonly laneKey: string;
		readonly state: string;
		readonly createdAt: string;
		readonly updatedAt: string;
		readonly lane: { readonly branch: string; readonly worktreePath: string };
		readonly json: string;
	}): void {
		this.#database
			.query(
				"INSERT INTO lane_jobs (job_id, lane_key, branch, worktree_path, state, record_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET lane_key = excluded.lane_key, branch = excluded.branch, worktree_path = excluded.worktree_path, state = excluded.state, record_json = excluded.record_json, updated_at = excluded.updated_at",
			)
			.run(
				job.jobId,
				job.laneKey,
				job.lane.branch,
				job.lane.worktreePath,
				job.state,
				job.json,
				job.createdAt,
				job.updatedAt,
			);
	}

	laneJobJson(jobId: string): string | undefined {
		return this.#database
			.query<{ record_json: string }, [string]>("SELECT record_json FROM lane_jobs WHERE job_id = ?")
			.get(jobId)?.record_json;
	}

	laneJobJsonByLaneKey(laneKey: string): string | undefined {
		return this.#database
			.query<{ record_json: string }, [string]>("SELECT record_json FROM lane_jobs WHERE lane_key = ?")
			.get(laneKey)?.record_json;
	}

	laneJobRows(): Array<{
		job_id: string;
		lane_key: string;
		state: string;
		branch: string;
		worktree_path: string;
		updated_at: string;
	}> {
		return this.#database
			.query(
				"SELECT job_id, lane_key, state, branch, worktree_path, updated_at FROM lane_jobs ORDER BY updated_at DESC",
			)
			.all() as Array<{
			job_id: string;
			lane_key: string;
			state: string;
			branch: string;
			worktree_path: string;
			updated_at: string;
		}>;
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
