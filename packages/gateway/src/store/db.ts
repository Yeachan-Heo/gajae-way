import { Database } from "bun:sqlite";
import { mkdir } from "node:fs/promises";
import { dirname } from "node:path";

const LATEST_SCHEMA_VERSION = 10;

export interface InboundMessageRow {
	readonly message_id: string;
	readonly origin_key: string;
	readonly origin_ref_json: string;
	readonly body: string;
	readonly engagement_json: string | null;
	readonly received_at: string;
}

export class DatabaseStartupError extends Error {
	readonly code: "newer_schema" | "integrity_check_failed";
	constructor(code: DatabaseStartupError["code"], message: string) {
		super(message);
		this.name = "DatabaseStartupError";
		this.code = code;
	}
}

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
	monitorEventUpdate(eventId: string, stage: string, batchId: string | null = null): void {
		this.#database
			.query("UPDATE monitor_events SET stage = ?, batch_id = COALESCE(?, batch_id), updated_at = ? WHERE event_id = ?")
			.run(stage, batchId, new Date().toISOString(), eventId);
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
