import type { GroupDb, StoreCore } from "../core";
import { BrokerAuthorityError } from "../errors";
import type { WriteTx } from "../tx";

export const INBOUND_TURN_STATES = ["bound", "accepted", "done"] as const;
export type InboundTurnState = (typeof INBOUND_TURN_STATES)[number];
export const INBOUND_TURN_ROLES = ["trigger", "steer"] as const;
export type InboundTurnRole = (typeof INBOUND_TURN_ROLES)[number];

/** One durable platform or lane-report message and its turn lifecycle. */
export interface InboundMessageRow {
	readonly message_id: string;
	readonly source: "platform" | "lane_report";
	readonly origin_key: string;
	readonly origin_ref_json: string;
	readonly body: string;
	readonly engagement_json: string | null;
	readonly state: "pending" | "processing" | "done";
	readonly received_at: string;
	readonly turn_role: InboundTurnRole | null;
	readonly turn_epoch: number | null;
	readonly turn_state: InboundTurnState | null;
	readonly turn_op_ref: string | null;
	readonly bound_session_id: string | null;
	readonly dispatched_at: string | null;
	readonly terminal_delivery_id: string | null;
}

export interface CrossSessionWatchRow {
	readonly watch_id: string;
	readonly watcher_origin_key: string;
	readonly watcher_origin_ref_json: string;
	readonly watched_origin_key: string;
	readonly watched_origin_ref_json: string;
	readonly handoff_chain_json: string;
	readonly reason: string;
	readonly expires_at: string;
	readonly state: "pending" | "fired" | "expired";
	readonly registered_at: string;
	readonly triggered_message_id: string | null;
	readonly triggered_at: string | null;
}

export type WatchRegistration =
	| { readonly kind: "registered" | "replayed"; readonly watch: CrossSessionWatchRow }
	| { readonly kind: "already_watching"; readonly watch: CrossSessionWatchRow };

export type TerminalUnlinkedReason = "silent" | "loopback" | "turn_failed" | "retired" | "no_delivery";

export function terminalDeliveryIds(json: string | null): string[] {
	if (!json) return [];
	try {
		const parsed: unknown = JSON.parse(json);
		if (typeof parsed !== "object" || parsed === null) return [];
		return Object.entries(parsed)
			.filter(([part, id]) => /^\d+$/.test(part) && typeof id === "string" && id.length > 0)
			.map(([, id]) => id as string);
	} catch {
		return [];
	}
}

export interface InboundTurn {
	readonly originKey: string;
	readonly epoch: number;
	readonly state: Extract<InboundTurnState, "bound" | "accepted">;
	readonly opRef: string;
	readonly sessionId: string | null;
	readonly triggerMessageId: string;
}

export class InboundTurnConflictError extends Error {
	constructor(originKey: string, epoch: number) {
		super(`origin ${originKey} already has a nonterminal turn in epoch ${epoch}`);
		this.name = "InboundTurnConflictError";
	}
}

export const CONVERSATION_DIFF_MAX_ROWS = 60;
export const CONVERSATION_DIFF_MAX_AGE_MS = 6 * 60 * 60 * 1000;
export const CONVERSATION_CONTEXT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
export const RETENTION_BATCH_ROWS = 500;

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

const REPLAYABLE_INBOUND =
	"NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'inbound' AND q.subject_id = inbound_messages.message_id)";
const INBOUND_COLUMNS =
	"message_id, source, origin_key, origin_ref_json, body, engagement_json, state, received_at, turn_role, turn_epoch, turn_state, turn_op_ref, bound_session_id, dispatched_at, terminal_delivery_id";

/** Persistence for inbound queueing, turn lifecycle, conversation context, and recall. */
export class InboxGroup {
	readonly #db: GroupDb;

	constructor(core: StoreCore) {
		this.#db = core.groupDb("inbox");
	}

	originTriggeredTurn(originKey: string): boolean {
		return (
			this.#db
				.prepare("SELECT 1 AS n FROM inbound_messages WHERE origin_key = ? AND turn_role = 'trigger' LIMIT 1")
				.get(originKey) !== null
		);
	}

	messageJoinedTurn(originKey: string, messageId: string): boolean {
		return (
			this.#db
				.prepare(
					"SELECT 1 AS n FROM inbound_messages WHERE origin_key = ? AND message_id = ? AND turn_role IN ('trigger', 'steer') LIMIT 1",
				)
				.get(originKey, messageId) !== null
		);
	}

	stateCounts(): Array<{ state: string; n: number }> {
		return this.#db.prepare("SELECT state, COUNT(*) AS n FROM inbound_messages GROUP BY state").all() as Array<{
			state: string;
			n: number;
		}>;
	}

	unquarantinedOrigins(): string[] {
		return (
			this.#db
				.prepare(
					"SELECT DISTINCT origin_key FROM inbound_messages i WHERE NOT EXISTS (SELECT 1 FROM broker_quarantine q WHERE q.kind = 'inbound' AND q.subject_id = i.message_id)",
				)
				.all() as Array<{ origin_key: string }>
		).map((row) => row.origin_key);
	}

	openCount(): number {
		return (
			(
				this.#db
					.prepare(
						`SELECT COUNT(*) AS n FROM inbound_messages WHERE (state <> 'done' OR turn_state IN ('bound','accepted')) AND ${REPLAYABLE_INBOUND}`,
					)
					.get() as { n: number } | null
			)?.n ?? 0
		);
	}

	pendingByOrigin(): Array<{
		origin_key: string;
		n: number;
		oldest_unbound_received_at: string | null;
		active: number;
	}> {
		return this.#db
			.prepare(
				`SELECT origin_key, COUNT(*) AS n,
					MIN(CASE WHEN turn_state IS NULL THEN received_at END) AS oldest_unbound_received_at,
					SUM(CASE WHEN turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') THEN 1 ELSE 0 END) AS active
				FROM inbound_messages WHERE state = 'pending' AND ${REPLAYABLE_INBOUND} GROUP BY origin_key`,
			)
			.all() as Array<{ origin_key: string; n: number; oldest_unbound_received_at: string | null; active: number }>;
	}

	enqueue(
		tx: WriteTx,
		row: {
			readonly messageId: string;
			readonly originKey: string;
			readonly originRefJson: string;
			readonly body: string;
			readonly engagementJson?: string | null;
			readonly receivedAt?: string;
			readonly source?: "platform" | "lane_report";
		},
	): boolean {
		const receivedAt = row.receivedAt ?? new Date().toISOString();
		if (!Number.isFinite(Date.parse(receivedAt))) throw new Error("inbound receivedAt must be an ISO timestamp");
		const source = row.source ?? "platform";
		if (source === "lane_report" && row.engagementJson !== undefined && row.engagementJson !== null)
			throw new Error("lane report inbound rows cannot carry engagement metadata");
		return (
			this.#db
				.prepare(
					"INSERT INTO inbound_messages (message_id, source, origin_key, origin_ref_json, body, engagement_json, state, received_at) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?) ON CONFLICT(message_id) DO NOTHING",
				)
				.run(
					tx,
					row.messageId,
					source,
					row.originKey,
					row.originRefJson,
					row.body,
					row.engagementJson ?? null,
					receivedAt,
				).changes > 0
		);
	}

	registerWatch(
		tx: WriteTx,
		input: {
			readonly watchId: string;
			readonly watcherOriginKey: string;
			readonly watcherOriginRefJson: string;
			readonly watchedOriginKey: string;
			readonly watchedOriginRefJson: string;
			readonly handoffChainJson: string;
			readonly reason: string;
			readonly registeredAt: string;
			readonly expiresAt: string;
		},
	): WatchRegistration {
		if (!input.watchId.trim()) throw new Error("watch id must not be empty");
		if (!Number.isFinite(Date.parse(input.registeredAt)) || !Number.isFinite(Date.parse(input.expiresAt)))
			throw new Error("watch timestamps must be valid ISO timestamps");
		if (Date.parse(input.expiresAt) <= Date.parse(input.registeredAt))
			throw new Error("watch expiry must be after registration");
		this.#db
			.prepare(
				"UPDATE cross_session_watches SET state = 'expired' WHERE watcher_origin_key = ? AND watched_origin_key = ? AND state = 'pending' AND expires_at <= ?",
			)
			.run(tx, input.watcherOriginKey, input.watchedOriginKey, input.registeredAt);

		const byId = this.#watch(input.watchId);
		if (byId) return { kind: "replayed", watch: byId };
		const existing = this.#db
			.prepare(
				"SELECT watch_id, watcher_origin_key, watcher_origin_ref_json, watched_origin_key, watched_origin_ref_json, handoff_chain_json, reason, expires_at, state, registered_at, triggered_message_id, triggered_at FROM cross_session_watches WHERE watcher_origin_key = ? AND watched_origin_key = ? AND state = 'pending'",
			)
			.get(input.watcherOriginKey, input.watchedOriginKey) as CrossSessionWatchRow | null;
		if (existing) return { kind: "already_watching", watch: existing };

		this.#db
			.prepare(
				"INSERT INTO cross_session_watches (watch_id, watcher_origin_key, watcher_origin_ref_json, watched_origin_key, watched_origin_ref_json, handoff_chain_json, reason, expires_at, state, registered_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)",
			)
			.run(
				tx,
				input.watchId,
				input.watcherOriginKey,
				input.watcherOriginRefJson,
				input.watchedOriginKey,
				input.watchedOriginRefJson,
				input.handoffChainJson,
				input.reason,
				input.expiresAt,
				input.registeredAt,
			);
		return { kind: "registered", watch: this.#watch(input.watchId) as CrossSessionWatchRow };
	}

	claimWatchesForOrigin(
		tx: WriteTx,
		watchedOriginKey: string,
		sourceMessageId: string,
		at: string,
	): CrossSessionWatchRow[] {
		if (!Number.isFinite(Date.parse(at))) throw new Error("watch match time must be a valid ISO timestamp");
		this.#db
			.prepare(
				"UPDATE cross_session_watches SET state = 'expired' WHERE watched_origin_key = ? AND state = 'pending' AND expires_at <= ?",
			)
			.run(tx, watchedOriginKey, at);
		const pending = this.#db
			.prepare(
				"SELECT watch_id, watcher_origin_key, watcher_origin_ref_json, watched_origin_key, watched_origin_ref_json, handoff_chain_json, reason, expires_at, state, registered_at, triggered_message_id, triggered_at FROM cross_session_watches WHERE watched_origin_key = ? AND state = 'pending' AND expires_at > ? ORDER BY registered_at, watch_id",
			)
			.all(watchedOriginKey, at) as CrossSessionWatchRow[];
		const claimed: CrossSessionWatchRow[] = [];
		for (const watch of pending) {
			const changes = this.#db
				.prepare(
					"UPDATE cross_session_watches SET state = 'fired', triggered_message_id = ?, triggered_at = ? WHERE watch_id = ? AND state = 'pending' AND expires_at > ?",
				)
				.run(tx, sourceMessageId, at, watch.watch_id, at).changes;
			if (changes === 1)
				claimed.push({ ...watch, state: "fired", triggered_message_id: sourceMessageId, triggered_at: at });
		}
		return claimed;
	}

	sweepWatches(
		tx: WriteTx,
		cutoffs: { readonly now: string; readonly historyCutoff: string; readonly batch: number },
	): { readonly deleted: number; readonly more: boolean } {
		const expired = this.#db
			.prepare(
				"UPDATE cross_session_watches SET state = 'expired' WHERE watch_id IN (SELECT watch_id FROM cross_session_watches WHERE state = 'pending' AND expires_at <= ? LIMIT ?)",
			)
			.run(tx, cutoffs.now, cutoffs.batch).changes;
		const moreExpired =
			this.#db
				.prepare("SELECT 1 AS n FROM cross_session_watches WHERE state = 'pending' AND expires_at <= ? LIMIT 1")
				.get(cutoffs.now) !== null;
		const deleted = this.#db
			.prepare(
				"DELETE FROM cross_session_watches WHERE watch_id IN (SELECT watch_id FROM cross_session_watches WHERE state <> 'pending' AND COALESCE(triggered_at, expires_at) < ? LIMIT ?)",
			)
			.run(tx, cutoffs.historyCutoff, cutoffs.batch).changes;
		const moreDeleted =
			this.#db
				.prepare(
					"SELECT 1 AS n FROM cross_session_watches WHERE state <> 'pending' AND COALESCE(triggered_at, expires_at) < ? LIMIT 1",
				)
				.get(cutoffs.historyCutoff) !== null;
		return { deleted, more: expired >= cutoffs.batch || moreExpired || deleted >= cutoffs.batch || moreDeleted };
	}

	watchRows(): CrossSessionWatchRow[] {
		return this.#db
			.prepare(
				"SELECT watch_id, watcher_origin_key, watcher_origin_ref_json, watched_origin_key, watched_origin_ref_json, handoff_chain_json, reason, expires_at, state, registered_at, triggered_message_id, triggered_at FROM cross_session_watches ORDER BY registered_at, watch_id",
			)
			.all() as CrossSessionWatchRow[];
	}

	#watch(watchId: string): CrossSessionWatchRow | undefined {
		return (
			(this.#db
				.prepare(
					"SELECT watch_id, watcher_origin_key, watcher_origin_ref_json, watched_origin_key, watched_origin_ref_json, handoff_chain_json, reason, expires_at, state, registered_at, triggered_message_id, triggered_at FROM cross_session_watches WHERE watch_id = ?",
				)
				.get(watchId) as CrossSessionWatchRow | null) ?? undefined
		);
	}

	pendingOldest(originKey: string): InboundMessageRow | undefined {
		const row = this.#db
			.prepare(
				`SELECT ${INBOUND_COLUMNS} FROM inbound_messages WHERE origin_key = ? AND state = 'pending' AND turn_state IS NULL AND ${REPLAYABLE_INBOUND} ORDER BY received_at, rowid LIMIT 1`,
			)
			.get(originKey) as InboundMessageRow | null;
		return row ?? undefined;
	}

	bindTurn(
		tx: WriteTx,
		input: {
			readonly messageId: string;
			readonly originKey: string;
			readonly epoch: number;
			readonly opRef: string;
			readonly sessionId: string;
			readonly dispatchedAt?: string;
		},
	): InboundMessageRow {
		if (!Number.isSafeInteger(input.epoch) || input.epoch < 0)
			throw new Error("turn epoch must be a non-negative integer");
		if (!input.sessionId) throw new Error("turn session id must not be empty");
		this.#assertNotQuarantined(input.messageId);
		this.#assertReplayableTurn(input.opRef);
		const existing = this.#db
			.prepare(
				"SELECT COUNT(*) AS n FROM inbound_messages WHERE origin_key = ? AND turn_epoch = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted')",
			)
			.get(input.originKey, input.epoch) as { n: number } | null;
		if ((existing?.n ?? 0) > 0) throw new InboundTurnConflictError(input.originKey, input.epoch);
		const changes = this.#db
			.prepare(
				"UPDATE inbound_messages SET turn_role = 'trigger', turn_epoch = ?, turn_state = 'bound', turn_op_ref = ?, bound_session_id = ?, dispatched_at = ?, terminal_delivery_id = NULL WHERE message_id = ? AND origin_key = ? AND state = 'pending' AND turn_state IS NULL",
			)
			.run(
				tx,
				input.epoch,
				input.opRef,
				input.sessionId,
				input.dispatchedAt ?? new Date().toISOString(),
				input.messageId,
				input.originKey,
			).changes;
		if (changes !== 1) throw new Error(`inbound ${input.messageId} is not a pending unbound row`);
		return this.turnRow(input.opRef) as InboundMessageRow;
	}

	turnAccept(tx: WriteTx, opRef: string): boolean {
		const accepted =
			this.#db
				.prepare(
					"UPDATE inbound_messages SET turn_state = 'accepted' WHERE turn_op_ref = ? AND turn_role = 'trigger' AND state = 'pending' AND turn_state = 'bound'",
				)
				.run(tx, opRef).changes > 0;
		if (accepted)
			this.#db
				.prepare(
					"UPDATE inbound_messages SET state = 'done', turn_state = 'done' WHERE turn_op_ref = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound'",
				)
				.run(tx, opRef);
		return accepted;
	}

	turnDispatchedAt(opRef: string): string | undefined {
		return (
			(
				this.#db
					.prepare("SELECT dispatched_at FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'")
					.get(opRef) as { dispatched_at: string | null } | null
			)?.dispatched_at ?? undefined
		);
	}

	claimTerminal(tx: WriteTx, opRef: string, part: number, deliveryId: string): string {
		if (!Number.isSafeInteger(part) || part < 0)
			throw new Error("terminal delivery part must be a non-negative integer");
		this.#assertNotQuarantinedByOpRef(opRef);
		const row = this.#db
			.prepare("SELECT terminal_delivery_id FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'")
			.get(opRef) as { terminal_delivery_id: string | null } | null;
		if (!row) throw new Error(`turn ${opRef} has no trigger`);
		let claims: Record<string, string> = {};
		if (row.terminal_delivery_id) {
			try {
				const parsed: unknown = JSON.parse(row.terminal_delivery_id);
				if (typeof parsed === "object" && parsed !== null && !Array.isArray(parsed))
					claims = parsed as Record<string, string>;
			} catch {
				claims = {};
			}
		}
		const key = String(part);
		const owner = claims[key];
		if (owner) return owner;
		delete claims.none;
		claims[key] = deliveryId;
		this.#db
			.prepare("UPDATE inbound_messages SET terminal_delivery_id = ? WHERE turn_op_ref = ? AND turn_role = 'trigger'")
			.run(tx, JSON.stringify(claims), opRef);
		return deliveryId;
	}

	markUnlinked(tx: WriteTx, opRef: string, reason: TerminalUnlinkedReason): void {
		this.#db
			.prepare(
				"UPDATE inbound_messages SET terminal_delivery_id = ? WHERE turn_op_ref = ? AND turn_role = 'trigger' AND terminal_delivery_id IS NULL",
			)
			.run(tx, JSON.stringify({ none: reason }), opRef);
	}

	terminalLinkAudit(sinceIso: string): { done: number; unlinked: number } {
		const row = this.#db
			.prepare(
				"SELECT COUNT(*) AS done, SUM(terminal_delivery_id IS NULL) AS unlinked FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state = 'done' AND dispatched_at >= ?",
			)
			.get(sinceIso) as { done: number; unlinked: number | null } | null;
		return { done: row?.done ?? 0, unlinked: row?.unlinked ?? 0 };
	}

	turnComplete(tx: WriteTx, opRef: string, unlinkedReason: TerminalUnlinkedReason = "no_delivery"): number {
		const unlinked = JSON.stringify({ none: unlinkedReason });
		const completed = this.#db
			.prepare(
				"UPDATE inbound_messages SET state = 'done', turn_state = 'done', terminal_delivery_id = COALESCE(terminal_delivery_id, ?) WHERE turn_op_ref = ? AND turn_role = 'trigger' AND state = 'pending' AND turn_state IN ('bound', 'accepted')",
			)
			.run(tx, unlinked, opRef).changes;
		if (completed === 0) {
			this.#db
				.prepare(
					"UPDATE inbound_messages SET terminal_delivery_id = ? WHERE turn_op_ref = ? AND turn_role = 'trigger' AND turn_state = 'done' AND terminal_delivery_id IS NULL",
				)
				.run(tx, unlinked, opRef);
		}
		return completed;
	}

	turnRequeue(tx: WriteTx, opRef: string): number {
		return this.#db
			.prepare(
				"UPDATE inbound_messages SET turn_role = NULL, turn_epoch = NULL, turn_state = NULL, turn_op_ref = NULL, bound_session_id = NULL, dispatched_at = NULL, terminal_delivery_id = NULL WHERE turn_op_ref = ? AND state = 'pending' AND (turn_role = 'trigger' OR (turn_role = 'steer' AND turn_state = 'bound'))",
			)
			.run(tx, opRef).changes;
	}

	steerIssued(
		tx: WriteTx,
		input: { readonly messageId: string; readonly epoch: number; readonly opRef: string },
	): boolean {
		this.#assertNotQuarantined(input.messageId);
		this.#assertReplayableTurn(input.opRef);
		return (
			this.#db
				.prepare(
					"UPDATE inbound_messages SET turn_role = 'steer', turn_epoch = ?, turn_state = 'bound', turn_op_ref = ? WHERE message_id = ? AND state = 'pending' AND (turn_state IS NULL OR (turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?))",
				)
				.run(tx, input.epoch, input.opRef, input.messageId, input.opRef).changes === 1
		);
	}

	steerAccepted(
		tx: WriteTx,
		input: {
			readonly messageId: string;
			readonly epoch: number;
			readonly opRef: string;
			readonly contextMessageId?: string;
		},
	): boolean {
		const accepted =
			this.#db
				.prepare(
					"UPDATE inbound_messages SET state = 'done', turn_role = 'steer', turn_epoch = ?, turn_state = 'done', turn_op_ref = ? WHERE message_id = ? AND state = 'pending' AND (turn_state IS NULL OR (turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?))",
				)
				.run(tx, input.epoch, input.opRef, input.messageId, input.opRef).changes === 1;
		if (accepted && input.contextMessageId)
			this.#db
				.prepare("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ? AND consumed_at IS NULL")
				.run(tx, new Date().toISOString(), input.contextMessageId);
		return accepted;
	}

	steerRefused(tx: WriteTx, messageId: string, opRef: string): boolean {
		return (
			this.#db
				.prepare(
					"UPDATE inbound_messages SET turn_role = NULL, turn_epoch = NULL, turn_state = NULL, turn_op_ref = NULL WHERE message_id = ? AND state = 'pending' AND turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?",
				)
				.run(tx, messageId, opRef).changes === 1
		);
	}

	steerAbandoned(tx: WriteTx, messageId: string, opRef: string, contextMessageId?: string): boolean {
		const closed =
			this.#db
				.prepare(
					"UPDATE inbound_messages SET state = 'done', turn_state = 'done' WHERE message_id = ? AND state = 'pending' AND turn_role = 'steer' AND turn_state = 'bound' AND turn_op_ref = ?",
				)
				.run(tx, messageId, opRef).changes === 1;
		if (closed && contextMessageId)
			this.#db
				.prepare("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ? AND consumed_at IS NULL")
				.run(tx, new Date().toISOString(), contextMessageId);
		return closed;
	}

	steersHeldAfterTerminal(originKey: string): readonly InboundMessageRow[] {
		return this.#db
			.prepare(
				`SELECT ${INBOUND_COLUMNS} FROM inbound_messages WHERE origin_key = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound' AND ${REPLAYABLE_INBOUND} AND turn_op_ref IN (SELECT turn_op_ref FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state = 'done') ORDER BY received_at, rowid`,
			)
			.all(originKey) as InboundMessageRow[];
	}

	steersHeld(opRef: string): readonly InboundMessageRow[] {
		return this.#db
			.prepare(
				`SELECT ${INBOUND_COLUMNS} FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound' AND ${REPLAYABLE_INBOUND} ORDER BY received_at, rowid`,
			)
			.all(opRef) as InboundMessageRow[];
	}

	turnRow(opRef: string): InboundMessageRow | undefined {
		const row = this.#db
			.prepare(`SELECT ${INBOUND_COLUMNS} FROM inbound_messages WHERE turn_op_ref = ? AND turn_role = 'trigger'`)
			.get(opRef) as InboundMessageRow | null;
		return row ?? undefined;
	}

	turnRows(opRef: string): readonly InboundMessageRow[] {
		return this.#db
			.prepare(`SELECT ${INBOUND_COLUMNS} FROM inbound_messages WHERE turn_op_ref = ? ORDER BY received_at, rowid`)
			.all(opRef) as InboundMessageRow[];
	}

	nonterminalTurns(originKey: string, epoch?: number): readonly InboundTurn[] {
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
				? (this.#db
						.prepare(
							`${select} WHERE origin_key = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') AND ${REPLAYABLE_INBOUND} ORDER BY turn_epoch, received_at, message_id`,
						)
						.all(originKey) as Row[])
				: (this.#db
						.prepare(
							`${select} WHERE origin_key = ? AND turn_epoch = ? AND turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') AND ${REPLAYABLE_INBOUND} ORDER BY received_at, message_id`,
						)
						.all(originKey, epoch) as Row[]);
		return rows.map((row) => ({
			originKey: row.origin_key,
			epoch: row.turn_epoch,
			state: row.turn_state,
			opRef: row.turn_op_ref,
			sessionId: row.bound_session_id,
			triggerMessageId: row.message_id,
		}));
	}

	pendingOrigins(): readonly string[] {
		return this.#db
			.prepare(
				`SELECT DISTINCT origin_key FROM inbound_messages WHERE state = 'pending' AND turn_state IS NULL AND ${REPLAYABLE_INBOUND}`,
			)
			.all()
			.map((row) => (row as { origin_key: string }).origin_key);
	}

	hasQuarantinedNonterminalTurn(originKey: string): boolean {
		return (
			this.#db
				.prepare(
					"SELECT 1 AS n FROM inbound_messages i JOIN broker_quarantine q ON q.kind = 'inbound' AND q.subject_id = i.message_id WHERE i.origin_key = ? AND i.turn_role = 'trigger' AND i.turn_state IN ('bound','accepted') LIMIT 1",
				)
				.get(originKey) !== null
		);
	}

	nonterminalOrigins(): readonly string[] {
		return this.#db
			.prepare(
				`SELECT DISTINCT origin_key FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') AND ${REPLAYABLE_INBOUND} ORDER BY origin_key`,
			)
			.all()
			.map((row) => (row as { origin_key: string }).origin_key);
	}

	heldSteerOrigins(): readonly string[] {
		return this.#db
			.prepare(
				"SELECT DISTINCT origin_key FROM inbound_messages WHERE turn_role = 'steer' AND state = 'pending' AND turn_state = 'bound' AND turn_op_ref IN (SELECT turn_op_ref FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state = 'done') ORDER BY origin_key",
			)
			.all()
			.map((row) => (row as { origin_key: string }).origin_key);
	}

	nonterminalTurnCount(): number {
		return (
			(
				this.#db
					.prepare(
						`SELECT COUNT(*) AS n FROM inbound_messages WHERE turn_role = 'trigger' AND turn_state IN ('bound', 'accepted') AND ${REPLAYABLE_INBOUND}`,
					)
					.get() as { n: number } | null
			)?.n ?? 0
		);
	}

	discardBefore(tx: WriteTx, originKey: string, floorAt: string): string[] {
		const ids = this.#db
			.prepare(
				`SELECT message_id FROM inbound_messages WHERE origin_key = ? AND source = 'platform' AND state = 'pending' AND turn_state IS NULL AND received_at <= ? AND ${REPLAYABLE_INBOUND}`,
			)
			.all(originKey, floorAt)
			.map((row) => (row as { message_id: string }).message_id);
		this.#db
			.prepare(
				`UPDATE inbound_messages SET state = 'done' WHERE origin_key = ? AND source = 'platform' AND state = 'pending' AND turn_state IS NULL AND received_at <= ? AND ${REPLAYABLE_INBOUND}`,
			)
			.run(tx, originKey, floorAt);
		return ids;
	}

	contextRecord(
		tx: WriteTx,
		row: {
			readonly messageId: string;
			readonly originKey: string;
			readonly authorId?: string;
			readonly authorName?: string;
			readonly body: string;
			readonly receivedAt?: string;
		},
	): void {
		this.#db
			.prepare(
				"INSERT INTO conversation_context (message_id, origin_key, author_id, author_name, body, received_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(message_id) DO NOTHING",
			)
			.run(
				tx,
				row.messageId,
				row.originKey,
				row.authorId ?? null,
				row.authorName ?? null,
				row.body,
				row.receivedAt ?? new Date().toISOString(),
			);
	}

	contextMaintain(
		tx: WriteTx,
		now = new Date(),
		retentionMs = CONVERSATION_CONTEXT_RETENTION_MS,
	): { expired: number; deleted: number } {
		if (!Number.isSafeInteger(retentionMs) || retentionMs < CONVERSATION_DIFF_MAX_AGE_MS)
			throw new Error("context retention must cover the unread relevance window");
		const at = now.toISOString();
		const relevanceCutoff = new Date(now.getTime() - CONVERSATION_DIFF_MAX_AGE_MS).toISOString();
		const groups = this.#db
			.prepare(
				"SELECT origin_key, COUNT(*) AS count, MIN(received_at) AS oldest, MAX(received_at) AS newest FROM conversation_context WHERE consumed_at IS NULL AND received_at < ? GROUP BY origin_key",
			)
			.all(relevanceCutoff) as Array<{
			origin_key: string;
			count: number;
			oldest: string | null;
			newest: string | null;
		}>;
		let expired = 0;
		for (const group of groups) {
			this.#db
				.prepare(
					"UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND consumed_at IS NULL AND received_at < ?",
				)
				.run(tx, at, group.origin_key, relevanceCutoff);
			this.#recordContextOmissions(
				tx,
				group.origin_key,
				{ count: group.count, oldest: group.oldest, newest: group.newest },
				{ count: 0, oldest: null, newest: null },
				at,
			);
			expired += group.count;
		}
		const deleteCutoff = new Date(now.getTime() - retentionMs).toISOString();
		let deleted = 0;
		while (true) {
			const batch = this.#db
				.prepare(
					"DELETE FROM conversation_context WHERE rowid IN (SELECT rowid FROM conversation_context WHERE consumed_at IS NOT NULL AND received_at < ? LIMIT ?)",
				)
				.run(tx, deleteCutoff, RETENTION_BATCH_ROWS).changes;
			deleted += batch;
			if (batch < RETENTION_BATCH_ROWS) break;
		}
		return { expired, deleted };
	}

	contextUnread(originKey: string, limit = 100): ConversationContextRow[] {
		return this.#db
			.prepare(
				"SELECT message_id, author_id, author_name, body, received_at FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL ORDER BY received_at, message_id, rowid LIMIT ?",
			)
			.all(originKey, limit) as ConversationContextRow[];
	}

	contextFloorAt(originKey: string): string | undefined {
		return (
			(
				this.#db.prepare("SELECT floor_at FROM conversation_context_state WHERE origin_key = ?").get(originKey) as {
					floor_at: string | null;
				} | null
			)?.floor_at ?? undefined
		);
	}

	/**
	 * Recent INBOUND messages only, oldest first, for judging one message against
	 * what people said around it. Deliberately excludes the persona's own replies:
	 * they are long, and including them cost 5 of 14 real owner messages a false
	 * skip (#243). A [POST:] from another conversation (`post/` row) is the
	 * persona's own words too. Read-only - it consumes nothing and sets no floor.
	 */
	recentInbound(
		originKey: string,
		limit: number,
		sinceIso: string,
	): Array<{ id: string; at: string; author: string; body: string }> {
		return this.#recentContextRows(originKey, limit, sinceIso, false);
	}

	/**
	 * Recent conversation rows for a fresh session: the last `limit` context rows
	 * (consumed or not, oldest first) INCLUDING the persona's own `post/` rows, so
	 * a [POST:] from another conversation appears once as its context row and not
	 * also as a delivery. Read-only - it consumes nothing and sets no floor.
	 */
	recentContext(
		originKey: string,
		limit: number,
		sinceIso: string,
	): Array<{ id: string; at: string; author: string; body: string }> {
		return this.#recentContextRows(originKey, limit, sinceIso, true);
	}

	#recentContextRows(
		originKey: string,
		limit: number,
		sinceIso: string,
		includePosts: boolean,
	): Array<{ id: string; at: string; author: string; body: string }> {
		const state = this.#db
			.prepare("SELECT floor_at, floor_row_id FROM conversation_context_state WHERE origin_key = ?")
			.get(originKey) as { floor_at: string | null; floor_row_id: number | null } | null;
		const floorAt = [state?.floor_at ?? "", sinceIso].sort().at(-1) ?? sinceIso;
		const postsFilter = includePosts ? "" : " AND message_id NOT LIKE 'post/%'";
		return this.#db
			.prepare(
				`SELECT message_id, author_name, author_id, body, received_at FROM conversation_context WHERE origin_key = ? AND body NOT LIKE '[reaction]%'${postsFilter} AND received_at >= ? AND rowid > ? ORDER BY received_at DESC LIMIT ?`,
			)
			.all(originKey, floorAt, state?.floor_row_id ?? 0, limit)
			.map((value) => {
				const row = value as {
					message_id: string;
					author_name: string | null;
					author_id: string | null;
					body: string;
					received_at: string;
				};
				return {
					id: row.message_id,
					at: row.received_at,
					author: row.author_name ?? row.author_id ?? "unknown",
					body: row.body,
				};
			})
			.reverse();
	}

	contextWindow(
		tx: WriteTx,
		originKey: string,
		triggerMessageId: string,
		now = new Date(),
		limit = CONVERSATION_DIFF_MAX_ROWS,
		maxAgeMs = CONVERSATION_DIFF_MAX_AGE_MS,
	): ConversationContextWindow {
		if (!Number.isSafeInteger(limit) || limit < 1) throw new Error("context window limit must be positive");
		if (!Number.isSafeInteger(maxAgeMs) || maxAgeMs < 0) throw new Error("context max age must be non-negative");
		const state = this.#db
			.prepare("SELECT floor_at, floor_row_id FROM conversation_context_state WHERE origin_key = ?")
			.get(originKey) as { floor_at: string | null; floor_row_id: number | null } | null;
		const floorRowId = state?.floor_row_id ?? 0;
		const ageFloor = new Date(now.getTime() - maxAgeMs).toISOString();
		const effectiveFloor = [ageFloor, state?.floor_at]
			.filter((value): value is string => value !== undefined && value !== null)
			.sort()
			.at(-1) as string;
		const expired = this.#contextAggregate(
			tx,
			"origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND (received_at < ? OR rowid <= ?)",
			[originKey, triggerMessageId, effectiveFloor, floorRowId],
		);
		if (expired.count > 0)
			this.#db
				.prepare(
					"UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND (received_at < ? OR rowid <= ?)",
				)
				.run(tx, now.toISOString(), originKey, triggerMessageId, effectiveFloor, floorRowId);

		const newest = this.#db
			.prepare(
				"SELECT rowid AS row_id, message_id, author_id, author_name, body, received_at FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND received_at >= ? AND rowid > ? ORDER BY received_at DESC, message_id DESC, rowid DESC LIMIT ?",
			)
			.all(originKey, triggerMessageId, effectiveFloor, floorRowId, limit) as Array<
			ConversationContextRow & { row_id: number }
		>;
		const boundary = newest[newest.length - 1];
		const truncated = boundary
			? this.#contextAggregate(
					tx,
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
		if (boundary && truncated.count > 0)
			this.#db
				.prepare(
					"UPDATE conversation_context SET consumed_at = ? WHERE origin_key = ? AND consumed_at IS NULL AND message_id <> ? AND received_at >= ? AND (received_at < ? OR (received_at = ? AND message_id < ?) OR (received_at = ? AND message_id = ? AND rowid < ?))",
				)
				.run(
					tx,
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
		this.#recordContextOmissions(tx, originKey, expired, truncated, now.toISOString());
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
	}

	contextSetFloor(tx: WriteTx, originKey: string, floorAt: string, failedOps: ReadonlySet<string>): void {
		const maxRowId =
			(
				this.#db
					.prepare("SELECT COALESCE(MAX(rowid), 0) AS row_id FROM conversation_context WHERE origin_key = ?")
					.get(originKey) as { row_id: number } | null
			)?.row_id ?? 0;
		const carriedIds = this.#unansweredTailRowIds(originKey, floorAt, failedOps);
		const keep = carriedIds.length > 0 ? ` AND rowid NOT IN (${carriedIds.map(() => "?").join(",")})` : "";
		const expireWhere = `origin_key = ? AND consumed_at IS NULL AND (received_at < ? OR rowid <= ?)${keep}`;
		const expireParams = [originKey, floorAt, maxRowId, ...carriedIds];
		const expired = this.#contextAggregate(tx, expireWhere, expireParams);
		if (expired.count > 0)
			this.#db
				.prepare(`UPDATE conversation_context SET consumed_at = ? WHERE ${expireWhere}`)
				.run(tx, floorAt, ...expireParams);
		const oldestCarried = carriedIds[0];
		let stateFloorAt = floorAt;
		let stateFloorRowId = maxRowId;
		if (oldestCarried !== undefined) {
			this.#db
				.prepare(
					`UPDATE conversation_context SET consumed_at = NULL WHERE rowid IN (${carriedIds.map(() => "?").join(",")})`,
				)
				.run(tx, ...carriedIds);
			stateFloorAt =
				(
					this.#db.prepare("SELECT received_at FROM conversation_context WHERE rowid = ?").get(oldestCarried) as {
						received_at: string;
					} | null
				)?.received_at ?? floorAt;
			stateFloorRowId = oldestCarried - 1;
		}
		this.#db
			.prepare(
				"INSERT INTO conversation_context_state (origin_key, floor_at, floor_row_id) VALUES (?, ?, ?) ON CONFLICT(origin_key) DO UPDATE SET floor_at = excluded.floor_at, floor_row_id = excluded.floor_row_id",
			)
			.run(tx, originKey, stateFloorAt, stateFloorRowId);
		this.#recordContextOmissions(tx, originKey, expired, { count: 0, oldest: null, newest: null }, floorAt);
	}

	contextCommitWindow(tx: WriteTx, originKey: string, messageIds: readonly string[], omissionRevision: number): void {
		const now = new Date().toISOString();
		for (const id of messageIds)
			this.#db.prepare("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ?").run(tx, now, id);
		this.#db
			.prepare(
				"UPDATE conversation_context_state SET pending_expired_count = 0, pending_truncated_count = 0, pending_omitted_oldest_at = NULL, pending_omitted_newest_at = NULL WHERE origin_key = ? AND omission_revision = ?",
			)
			.run(tx, originKey, omissionRevision);
	}

	contextDiagnostics(originKey?: string): ConversationContextDiagnostics {
		const unread = originKey
			? ((
					this.#db
						.prepare("SELECT COUNT(*) AS n FROM conversation_context WHERE origin_key = ? AND consumed_at IS NULL")
						.get(originKey) as { n: number } | null
				)?.n ?? 0)
			: ((
					this.#db.prepare("SELECT COUNT(*) AS n FROM conversation_context WHERE consumed_at IS NULL").get() as {
						n: number;
					} | null
				)?.n ?? 0);
		const state = originKey
			? (this.#db
					.prepare(
						"SELECT expired_count AS expired, truncated_count AS truncated, omitted_oldest_at AS oldest, omitted_newest_at AS newest, floor_at AS floor FROM conversation_context_state WHERE origin_key = ?",
					)
					.get(originKey) as {
					expired: number;
					truncated: number;
					oldest: string | null;
					newest: string | null;
					floor: string | null;
				} | null)
			: (this.#db
					.prepare(
						"SELECT COALESCE(SUM(expired_count), 0) AS expired, COALESCE(SUM(truncated_count), 0) AS truncated, MIN(omitted_oldest_at) AS oldest, MAX(omitted_newest_at) AS newest, NULL AS floor FROM conversation_context_state",
					)
					.get() as {
					expired: number;
					truncated: number;
					oldest: string | null;
					newest: string | null;
					floor: null;
				} | null);
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
		const origins = this.#db
			.prepare("SELECT origin_key FROM conversation_context UNION SELECT origin_key FROM conversation_context_state")
			.all() as Array<{ origin_key: string }>;
		return new Map(origins.map((row) => [row.origin_key, this.contextDiagnostics(row.origin_key)]));
	}

	contextConsume(tx: WriteTx, messageIds: readonly string[]): void {
		if (messageIds.length === 0) return;
		const now = new Date().toISOString();
		for (const id of messageIds)
			this.#db.prepare("UPDATE conversation_context SET consumed_at = ? WHERE message_id = ?").run(tx, now, id);
	}

	knownMessage(originKey: string, messageId: string): boolean {
		return (
			((
				this.#db
					.prepare(
						"SELECT (SELECT COUNT(*) FROM inbound_messages WHERE origin_key = ? AND message_id = ?) + (SELECT COUNT(*) FROM conversation_context WHERE origin_key = ? AND message_id = ?) AS n",
					)
					.get(originKey, messageId, originKey, messageId) as { n: number } | null
			)?.n ?? 0) > 0
		);
	}

	updateContextBody(tx: WriteTx, originKey: string, messageId: string, body: string): boolean {
		return (
			this.#db
				.prepare("UPDATE conversation_context SET body = ? WHERE origin_key = ? AND message_id = ?")
				.run(tx, body, originKey, messageId).changes > 0
		);
	}

	pendingCount(originKey: string): number {
		return (
			(
				this.#db
					.prepare(
						`SELECT COUNT(*) AS n FROM inbound_messages WHERE origin_key = ? AND state = 'pending' AND ${REPLAYABLE_INBOUND}`,
					)
					.get(originKey) as { n: number } | null
			)?.n ?? 0
		);
	}

	messageExists(messageId: string): boolean {
		return this.#db.prepare("SELECT 1 FROM inbound_messages WHERE message_id = ?").get(messageId) !== null;
	}

	hasQuarantinedTurn(opRef: string): boolean {
		return (
			this.#db
				.prepare(
					"SELECT 1 FROM inbound_messages i JOIN broker_quarantine q ON q.kind = 'inbound' AND q.subject_id = i.message_id WHERE i.turn_op_ref = ? LIMIT 1",
				)
				.get(opRef) !== null
		);
	}

	sweep(tx: WriteTx, cutoffs: { historyCutoff: string; batch: number }): number {
		return this.#db
			.prepare(
				`DELETE FROM inbound_messages WHERE rowid IN (SELECT rowid FROM inbound_messages WHERE state = 'done' AND received_at < ? AND ${REPLAYABLE_INBOUND} AND NOT EXISTS (SELECT 1 FROM inbound_messages s WHERE s.turn_op_ref = inbound_messages.turn_op_ref AND s.state <> 'done') LIMIT ?)`,
			)
			.run(tx, cutoffs.historyCutoff, cutoffs.batch).changes;
	}

	addRecall(tx: WriteTx, originKey: string, originRefJson: string, text: string): void {
		this.#db
			.prepare("INSERT INTO recall_snippets (origin_key, origin_ref_json, text, at) VALUES (?, ?, ?, ?)")
			.run(tx, originKey, originRefJson, text.slice(0, 1000), new Date().toISOString());
		this.#db
			.prepare(
				"DELETE FROM recall_snippets WHERE origin_key = ? AND id NOT IN (SELECT id FROM recall_snippets WHERE origin_key = ? ORDER BY id DESC LIMIT 20)",
			)
			.run(tx, originKey, originKey);
	}

	recallRows(): Array<{ origin_key: string; origin_ref_json: string; text: string; at: string }> {
		return this.#db
			.prepare("SELECT origin_key, origin_ref_json, text, at FROM recall_snippets ORDER BY id DESC")
			.all() as Array<{ origin_key: string; origin_ref_json: string; text: string; at: string }>;
	}

	#assertNotQuarantined(messageId: string): void {
		if (this.#db.prepare("SELECT 1 FROM broker_quarantine WHERE kind = 'inbound' AND subject_id = ?").get(messageId))
			throw new BrokerAuthorityError("quarantined");
	}

	#assertNotQuarantinedByOpRef(opRef: string): void {
		if (this.hasQuarantinedTurn(opRef)) throw new BrokerAuthorityError("quarantined");
	}

	#assertReplayableTurn(opRef: string): void {
		if (this.hasQuarantinedTurn(opRef)) throw new BrokerAuthorityError("quarantined");
	}

	#unansweredTailRowIds(originKey: string, floorAt: string, failedOps: ReadonlySet<string>): number[] {
		if (failedOps.size === 0) return [];
		const relevanceCutoff = new Date(Date.parse(floorAt) - CONVERSATION_DIFF_MAX_AGE_MS).toISOString();
		const triggers = this.#db
			.prepare(
				"SELECT turn_op_ref FROM inbound_messages WHERE origin_key = ? AND turn_role = 'trigger' AND state = 'done' AND turn_op_ref IS NOT NULL AND received_at >= ? AND received_at < ? ORDER BY received_at, rowid",
			)
			.all(originKey, relevanceCutoff, floorAt) as Array<{ turn_op_ref: string }>;
		const carryOps: string[] = [];
		for (const { turn_op_ref: opRef } of triggers) {
			if (failedOps.has(opRef)) carryOps.push(opRef);
			else carryOps.length = 0;
		}
		if (carryOps.length === 0) return [];
		return this.#db
			.prepare(
				`SELECT c.rowid AS row_id FROM conversation_context c WHERE c.origin_key = ? AND c.received_at >= ? AND c.received_at < ? AND c.message_id IN (SELECT message_id FROM inbound_messages WHERE origin_key = ? AND turn_op_ref IN (${carryOps.map(() => "?").join(",")})) ORDER BY c.rowid`,
			)
			.all(originKey, relevanceCutoff, floorAt, originKey, ...carryOps)
			.map((value) => (value as { row_id: number }).row_id);
	}

	#contextAggregate(
		tx: WriteTx,
		where: string,
		params: (string | number)[],
	): { count: number; oldest: string | null; newest: string | null } {
		const row = this.#db
			.prepare(
				`SELECT COUNT(*) AS count, MIN(received_at) AS oldest, MAX(received_at) AS newest FROM conversation_context WHERE ${where}`,
			)
			.getWithWrite(tx, ...params) as { count: number; oldest: string | null; newest: string | null } | null;
		return row ?? { count: 0, oldest: null, newest: null };
	}

	#recordContextOmissions(
		tx: WriteTx,
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
		this.#db.prepare("INSERT OR IGNORE INTO conversation_context_state (origin_key) VALUES (?)").run(tx, originKey);
		this.#db
			.prepare(
				"UPDATE conversation_context_state SET expired_count = expired_count + ?, truncated_count = truncated_count + ?, pending_expired_count = pending_expired_count + ?, pending_truncated_count = pending_truncated_count + ?, omission_revision = omission_revision + 1, omitted_oldest_at = CASE WHEN omitted_oldest_at IS NULL OR ? < omitted_oldest_at THEN ? ELSE omitted_oldest_at END, omitted_newest_at = CASE WHEN omitted_newest_at IS NULL OR ? > omitted_newest_at THEN ? ELSE omitted_newest_at END, pending_omitted_oldest_at = CASE WHEN pending_omitted_oldest_at IS NULL OR ? < pending_omitted_oldest_at THEN ? ELSE pending_omitted_oldest_at END, pending_omitted_newest_at = CASE WHEN pending_omitted_newest_at IS NULL OR ? > pending_omitted_newest_at THEN ? ELSE pending_omitted_newest_at END, last_omitted_at = ? WHERE origin_key = ?",
			)
			.run(
				tx,
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
			(this.#db
				.prepare(
					"SELECT pending_expired_count AS expired, pending_truncated_count AS truncated, pending_omitted_oldest_at AS oldest, pending_omitted_newest_at AS newest, omission_revision AS revision FROM conversation_context_state WHERE origin_key = ?",
				)
				.get(originKey) as {
				expired: number;
				truncated: number;
				oldest: string | null;
				newest: string | null;
				revision: number;
			} | null) ?? { expired: 0, truncated: 0, oldest: null, newest: null, revision: 0 }
		);
	}
}
