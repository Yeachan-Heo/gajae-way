import { statfsSync, statSync } from "node:fs";
import { join } from "node:path";
import {
	type AgentDiskView,
	CATCH_ALL_EVENT_ORIGIN,
	type CycleGateReason,
	type CyclePhase,
	type CycleSessionView,
	LOOPBACK_ORIGIN,
	type MonitorFailureDiagnostic,
	monitorSessionOrigin,
	OPS_CYCLE_DEFAULT_LIMIT,
	type OpsCycleParams,
	type OpsCycleResult,
	type OpsSloResult,
	originKey,
	validateOriginRef,
} from "@gajae-gateway/protocol";
import { type BrokerAgentDrift, readBrokerAgentDrift } from "../broker-agent/drift";
import { DEFAULT_WORK_MAX_LANES } from "../config";
import { isBrokerIndexLockBlocked, readBrokerExitRecordSync } from "../orchestrator/broker-liveness";
import { isVerifiedGjcVersion } from "../orchestrator/gjc-contract";
import { WORK_LANE_PREFIX } from "../orchestrator/lane-governor";
import { sanitizeDiagnosticDetail } from "../orchestrator/rebind";
import type { Store } from "../store";
import type { ConversationContextDiagnostics } from "../store/groups/inbox";
import type { MonitorsGroup } from "../store/groups/monitors";
import { DEFAULT_SLO_DAYS, evaluateSlo } from "./slo";

/**
 * Operator runtime-cycle projection (ops.cycle).
 *
 * A pure, read-only derivation over the gateway's durable single-writer state:
 * sessions/epochs, the durable inbound queue, the delivery ledger, memory
 * intents, and monitor events. It performs no writes and mutates no runtime
 * object; the SQLite rows and the ledger remain the only authority.
 *
 * Fail-closed rules:
 * - A session row whose bound gjc session id is empty (mid-rebind after /new,
 *   or never created) is reported as stale identity, never as healthy.
 * - Any delivery row in a state outside the ledger's known set is an unknown
 *   settlement and gates the projection — it is never counted as healthy.
 * - Quarantined memory intents and failed monitor events are surfaced as gates;
 *   the operator sees them instead of a green light.
 */

/** Ledger states the delivery subsystem itself defines; anything else is unknown. */
const KNOWN_DELIVERY_STATES = new Set(["pending", "inflight", "confirmed", "failed_ambiguous", "expired"]);
/** Inbound queue states defined by the durable queue itself. */
const KNOWN_INBOUND_STATES = new Set(["pending", "processing", "done"]);
/** Lane-job states after which the worker owns no unresolved work; only these may vouch for a retired lane. */
const SETTLED_JOB_STATES = new Set(["attempt_ended", "done", "aborted"]);
/**
 * Oldest replayable pending trigger age, with zero in flight, past which the
 * queue is starved rather than busy. A cold persona bind measures ~10-60s and
 * the dispatch retry backoff caps well under this; ten minutes with nothing
 * moving has only ever meant a stuck actor.
 */
export const INBOUND_STARVATION_MS = 10 * 60_000;
/**
 * Oldest pending platform inbound message from a non-bot author without
 * any delivery activity on the origin since it arrived. A silent origin
 * suggests the message is not being processed or delivered.
 */
export const ORIGIN_SILENT_THRESHOLD_MS = 15 * 60_000;
/**

 * Monitor authoring loss (#160): scheduled events exhausting retries with no
 * authored output while chat delivery stays healthy. Only the latest window
 * counts, so a loss that has since aged out stops gating without operator
 * action. One lost slot of one type is noise; this many distinct types whose
 * latest slot was lost, or this many consecutive lost slots of one type, is an
 * authoring-lane outage (observed: 19h of 100% loss with every other gate green).
 */
export const MONITOR_AUTHORING_LOSS_WINDOW_MS = 24 * 60 * 60_000;
export const MONITOR_AUTHORING_LOSS_TYPES = 2;
export const MONITOR_AUTHORING_LOSS_CONSECUTIVE = 2;
/**
 * Consecutive most-recent monitor events settled `failed_no_retry` that mark
 * monitor dispatch as failing. Issue #189: 26 terminal slots in a row over 19h
 * while every liveness signal stayed green; three in a row is already an outage.
 */
export const MONITOR_TERMINAL_STREAK_THRESHOLD = 3;
export const MONITOR_FAILURE_DIAGNOSTIC_LIMIT = 5;
/**
 * Agent-directory headroom floor (issue #15). GJC keeps sessions, blobs and
 * recovery snapshots under its agent directory with no retention or reaper
 * (measured 8.9 GB of `.gjc-recovery`, later 70+ GB total), and the gateway may
 * not scan or delete GJC-owned state. The gateway's share of the fix is to
 * gate the cycle before that growth reaches the disk-full cliff where session
 * creation fails: under 10% of the volume or 5 GiB available, whichever trips
 * first.
 */
export const AGENT_DISK_MIN_FREE_RATIO = 0.1;
export const AGENT_DISK_MIN_FREE_BYTES = 5 * 1024 ** 3;

/** Filesystem headroom for the agent directory; a failed probe reports null bytes, never a guess. */
export function observeAgentDisk(path: string): AgentDiskView {
	try {
		const stats = statfsSync(path);
		return { path, freeBytes: stats.bavail * stats.bsize, totalBytes: stats.blocks * stats.bsize };
	} catch {
		return { path, freeBytes: null, totalBytes: null };
	}
}

function agentDiskLow(disk: AgentDiskView): boolean {
	if (disk.freeBytes === null || disk.totalBytes === null) return true;
	return disk.freeBytes < AGENT_DISK_MIN_FREE_BYTES || disk.freeBytes < disk.totalBytes * AGENT_DISK_MIN_FREE_RATIO;
}

function observeMemoryGitIndexLock(
	memoryRoot: string | undefined,
	nowMs: number,
): {
	readonly path: string;
	readonly ageMs: number;
} | null {
	if (memoryRoot === undefined) return null;
	const path = join(memoryRoot, ".git", "index.lock");
	try {
		const ageMs = Math.max(0, nowMs - statSync(path).mtimeMs);
		console.error(`memory_git_index_locked path=${path} age_ms=${Math.floor(ageMs)}`);
		return { path, ageMs };
	} catch {
		return null;
	}
}

export interface RuntimeCycleSources {
	readonly sessionRows: Array<{
		readonly origin_key: string;
		readonly origin_ref_json: string | null;
		readonly gjc_session_id: string;
		readonly epoch: number;
		readonly created_at: string;
		readonly last_activity_at: string | null;
		readonly last_bootstrapped_epoch: number;
		readonly bootstrap_applied_at: string | null;
		readonly bootstrap_sections_json: string;
		readonly bootstrap_byte_count: number;
		readonly bootstrap_truncated: number;
		readonly bootstrap_diagnostics_json: string;
	}>;
	readonly inboundPendingByOrigin: ReadonlyMap<string, number>;
	/** Age of the oldest replayable pending trigger with nothing in flight; the bricked-origin signature. */
	readonly oldestStarvedPendingMs: number | null;
	readonly contextByOrigin: ReadonlyMap<string, ConversationContextDiagnostics>;
	readonly contextDiff: ConversationContextDiagnostics;
	readonly inboundCounts: ReadonlyMap<string, number>;
	readonly pendingInbound: number;
	readonly inFlightInbound: number;
	readonly unknownInboundStates: readonly string[];
	readonly deliveryCounts: ReadonlyMap<string, number>;
	readonly unknownDeliveryStates: readonly string[];
	readonly unsettledByOrigin: ReadonlyMap<string, { n: number; oldestMs: number }>;
	readonly memoryIntents: ReadonlyMap<string, number>;
	readonly memoryGitIndexLock: { readonly path: string; readonly ageMs: number } | null;
	readonly monitorStages: ReadonlyMap<string, number>;
	readonly monitorFailures: readonly MonitorFailureDiagnostic[];
	/** Event types whose latest terminal slots in the loss window were lost before authoring. */
	readonly monitorAuthoringLost: ReturnType<MonitorsGroup["authoringLossStreaks"]>;
	readonly memoryClosing: boolean;
	readonly instanceId: string;
	/** Bound `work.run` lanes and the configured admission cap. */
	readonly activeLanes: number;
	readonly maxLanes: number;
	/**
	 * Worker origins whose durable lane job is settled (`attempt_ended`,
	 * `done`, `aborted`). An unbound worker row is a deliberate retirement only
	 * with this positive evidence: no job row at all is a failed first bind
	 * (`rebindEpoch` runs before the job is created), and a `running`,
	 * `awaiting_operator`, or `stalled` job is a crash-left or held worker.
	 */
	readonly settledWorkOrigins: ReadonlySet<string>;
	/**
	 * When the newest broker authority cutover committed; null when none ran.
	 * A cutover unbinds every origin (`gjc_session_id = ''`) and quarantines
	 * every inbound row, lane job and monitor event it finds.
	 */
	readonly latestCutoverAt: string | null;
	/**
	 * Origins with durable work admitted since the newest cutover: inbound rows,
	 * lane jobs and monitor events outside quarantine. A failed first bind leaves
	 * the session row untouched, so this is the evidence that an origin was asked
	 * to rebind. Empty when no cutover ran.
	 */
	readonly addressedSinceCutover: ReadonlySet<string>;
	/** Headroom of the broker-bound GJC agent directory; null when none is bound. */
	readonly agentDisk: AgentDiskView | null;
	/** gjc version the broker client last observed; undefined before preflight or without a broker. */
	readonly gjcVersion: string | undefined;
	/** Newest settled monitor events that ended `failed_no_retry` or `contract_unmet` before any success. */
	readonly monitorTerminalStreak: number;
	/** True while the shared broker's incarnation keeps changing (die/respawn churn). */
	readonly brokerRespawnChurn: boolean;
	/** Untrusted broker exit record from the GJC-owned agent directory, when present. */
	readonly brokerExitRecord: unknown;
	readonly brokerAgent: BrokerAgentDrift | null;
	readonly slo: OpsSloResult;
	/**
	 * Platform inbound messages from non-bot authors pending or bound older
	 * than the threshold with no delivery on the origin since they arrived,
	 * and not in muted threads. Per origin: oldest pending age, message id,
	 * received timestamp, and blocking turn op-ref if part of a turn.
	 */
	readonly originSilence: ReadonlyMap<
		string,
		{
			readonly oldestPendingMs: number;
			readonly messageId: string;
			readonly receivedAt: string;
			readonly turnOpRef: string | null;
		}
	>;
}

export class RuntimeCycleProjector {
	readonly #database: Store;
	readonly #memory: { readonly queueDepth: number };
	readonly #maxLanes: number;
	readonly #agentDir: string | undefined;
	readonly #memoryRoot: string | undefined;
	readonly #gjcVersion: () => string | undefined;
	readonly #brokerRespawnChurn: () => boolean;
	readonly #home: string | undefined;

	constructor(
		database: Store,
		memory: { readonly queueDepth: number },
		options: {
			readonly maxLanes?: number;
			readonly agentDir?: string;
			readonly home?: string;
			readonly memoryRoot?: string;
			readonly gjcVersion?: () => string | undefined;
			readonly brokerRespawnChurn?: () => boolean;
		} = {},
	) {
		this.#database = database;
		this.#memory = memory;
		this.#maxLanes = options.maxLanes ?? DEFAULT_WORK_MAX_LANES;
		this.#agentDir = options.agentDir;
		this.#home = options.home;
		this.#memoryRoot = options.memoryRoot;
		this.#gjcVersion = options.gjcVersion ?? (() => undefined);
		this.#brokerRespawnChurn = options.brokerRespawnChurn ?? (() => false);
	}

	/** Snapshots durable state and projects the runtime cycle. Read-only; no writes. */
	project(now = new Date(), pagination: OpsCycleParams = {}): OpsCycleResult {
		const sources = this.#sources(now.getTime());
		return projectRuntimeCycle(sources, now.toISOString(), pagination);
	}

	#sources(nowMs: number): RuntimeCycleSources {
		const sessions = this.#database.bindings.sessionIdentityRows();
		const inbound = this.#database.inbox.stateCounts();
		const pendingRows = this.#database.inbox.pendingByOrigin();
		const pendingByOrigin = new Map(pendingRows.map((r) => [r.origin_key, r.n]));
		const inFlightInbound = pendingRows.reduce((sum, row) => sum + row.active, 0);
		// Starvation is per origin: an unbound row waiting behind that origin's
		// running turn is queued, not starved; only an origin with nothing in
		// flight and an old unbound row is stuck.
		const oldestStarvedPendingMs = pendingRows.reduce<number | null>((oldest, row) => {
			if (row.active > 0 || row.oldest_unbound_received_at === null) return oldest;
			const age = nowMs - Date.parse(row.oldest_unbound_received_at);
			return Number.isFinite(age) && (oldest === null || age > oldest) ? age : oldest;
		}, null);
		const contextByOrigin = this.#database.inbox.contextDiagnosticsByOrigin();
		const deliveries = this.#database.outbox.stateCounts();
		const unsettled = new Map(
			this.#database.outbox.unsettledByOrigin(nowMs).map((r) => [r.origin_key, { n: r.n, oldestMs: r.oldest_ms }]),
		);
		const memory = this.#database.memory.intentCounts();
		const monitors = this.#database.monitors.eventStageCounts();
		const monitorFailures = this.#database.monitors.recentFailures(MONITOR_FAILURE_DIAGNOSTIC_LIMIT).map((row) => ({
			eventId: row.event_id,
			monitorId: row.monitor_id,
			eventType: row.event_type,
			stage: row.stage,
			failedAt: row.failed_at,
			code: row.code,
			detail: sanitizeDiagnosticDetail(row.detail, 500),
		}));
		const inboundMap = new Map(inbound.map((r) => [r.state, r.n]));
		const unknownInbound = inbound.map((r) => r.state).filter((state) => !KNOWN_INBOUND_STATES.has(state));
		const unknownDeliveries = deliveries.map((r) => r.state).filter((state) => !KNOWN_DELIVERY_STATES.has(state));
		const latestCutoverAt = this.#database.bindings.latestBrokerCutoverAt();
		return {
			sessionRows: sessions,
			inboundCounts: inboundMap,
			inFlightInbound,
			pendingInbound: inboundMap.get("pending") ?? 0,
			unknownInboundStates: unknownInbound,
			deliveryCounts: new Map(deliveries.map((r) => [r.state, r.n])),
			unknownDeliveryStates: unknownDeliveries,
			unsettledByOrigin: unsettled,
			memoryIntents: new Map(memory.map((r) => [r.state, r.n])),
			memoryGitIndexLock: observeMemoryGitIndexLock(this.#memoryRoot, nowMs),
			monitorStages: new Map(monitors.map((r) => [r.stage, r.n])),
			monitorFailures,
			monitorAuthoringLost: this.#database.monitors.authoringLossStreaks(
				new Date(nowMs - MONITOR_AUTHORING_LOSS_WINDOW_MS).toISOString(),
			),
			inboundPendingByOrigin: pendingByOrigin,
			oldestStarvedPendingMs,
			contextByOrigin,
			contextDiff: this.#database.inbox.contextDiagnostics(),
			memoryClosing: this.#memory.queueDepth > 0,
			instanceId: this.#database.instanceId,
			activeLanes: this.#database.bindings.workLaneRows().length,
			maxLanes: this.#maxLanes,
			settledWorkOrigins: new Set(
				this.#database.work
					.laneJobRows()
					.filter((row) => SETTLED_JOB_STATES.has(row.state))
					.map((row) => `${WORK_LANE_PREFIX}${row.lane_key.slice("work-".length)}`),
			),
			latestCutoverAt,
			addressedSinceCutover: this.#addressedSinceCutover(latestCutoverAt),
			agentDisk: this.#agentDir === undefined ? null : observeAgentDisk(this.#agentDir),
			gjcVersion: this.#gjcVersion(),
			monitorTerminalStreak: this.#database.monitors.consecutiveTerminalFailures(),
			brokerRespawnChurn: this.#brokerRespawnChurn(),
			brokerExitRecord: this.#agentDir === undefined ? undefined : readBrokerExitRecordSync(this.#agentDir),
			brokerAgent:
				this.#home === undefined || this.#agentDir === undefined
					? null
					: readBrokerAgentDrift({ home: this.#home, agentDir: this.#agentDir }),
			slo: evaluateSlo(this.#database.core, DEFAULT_SLO_DAYS, new Date(nowMs)),
			originSilence: this.#originSilence(nowMs),
		};
	}

	#addressedSinceCutover(latestCutoverAt: string | null): Set<string> {
		const origins = new Set<string>();
		if (latestCutoverAt === null) return origins;
		for (const key of this.#database.inbox.unquarantinedOrigins()) origins.add(key);
		// laneJobRows() already leaves quarantined jobs out.
		for (const row of this.#database.work.laneJobRows())
			origins.add(`${WORK_LANE_PREFIX}${row.lane_key.slice("work-".length)}`);
		// Same session-origin choice as monitor dispatch: a declared type gets its
		// own session, anything else the monitor's catch-all. A deleted monitor's
		// declarations are unknown, so both candidates count.
		for (const row of this.#database.monitors.unquarantinedEventTypes()) {
			const declared = parseDeclaredEventTypes(row.event_types_json);
			if (declared === undefined || declared.includes(row.event_type))
				origins.add(originKey(monitorSessionOrigin(row.monitor_id, row.event_type)));
			if (declared === undefined || !declared.includes(row.event_type))
				origins.add(originKey(monitorSessionOrigin(row.monitor_id, CATCH_ALL_EVENT_ORIGIN.conversationId)));
		}
		return origins;
	}

	/**
	 * Platform inbound messages from non-bot authors pending or bound older
	 * than the threshold with no delivery on the origin since they arrived, and
	 * not in muted threads. Issue #688.
	 */
	#originSilence(nowMs: number): Map<
		string,
		{
			readonly oldestPendingMs: number;
			readonly messageId: string;
			readonly receivedAt: string;
			readonly turnOpRef: string | null;
		}
	> {
		const silence = new Map<
			string,
			{
				oldestPendingMs: number;
				messageId: string;
				receivedAt: string;
				turnOpRef: string | null;
			}
		>();

		// Query for pending platform inbound messages from non-bot authors, oldest
		// per origin. Exclude muted threads and messages without a recent delivery.
		const db = (this.#database as unknown as { core: { db: any } }).core.db;
		const rows = db
			.prepare(
				`SELECT
					i.origin_key,
					i.message_id,
					i.received_at,
					i.turn_op_ref,
					MIN(CASE WHEN COALESCE(json_extract(i.engagement_json, '$.authorIsBot'), 0) <> 1 THEN i.received_at ELSE NULL END) AS oldest_non_bot_at
				FROM inbound_messages i
				WHERE i.source = 'platform'
				  AND i.state IN ('pending', 'processing')
				  AND (i.turn_state IN ('bound', 'accepted') OR i.turn_state IS NULL)
				  AND COALESCE(json_extract(i.engagement_json, '$.authorIsBot'), 0) <> 1
				  AND NOT EXISTS (
				    SELECT 1 FROM thread_mutes t
				    WHERE i.origin_key LIKE 'slack/thread/%' AND t.thread_origin_key = i.origin_key
				       OR i.origin_key LIKE 'discord/thread/%' AND t.thread_origin_key = i.origin_key
				  )
				  AND NOT EXISTS (
				    SELECT 1 FROM broker_quarantine q
				    WHERE q.kind = 'inbound' AND q.subject_id = i.message_id
				  )
				GROUP BY i.origin_key
				HAVING oldest_non_bot_at IS NOT NULL
			`,
			)
			.all() as Array<{
			origin_key: string;
			message_id: string;
			received_at: string;
			turn_op_ref: string | null;
			oldest_non_bot_at: string | null;
		}>;

		for (const row of rows) {
			if (!row.oldest_non_bot_at) continue;
			const receivedMs = Date.parse(row.oldest_non_bot_at);
			if (!Number.isFinite(receivedMs)) continue;
			const ageMs = nowMs - receivedMs;

			// Check if there's been any delivery on this origin since the message arrived
			const hasDeliverySince = db
				.prepare(
					`SELECT 1 FROM outbound_deliveries
					WHERE origin_key = ? AND state = 'confirmed' AND confirmed_at >= ?
					LIMIT 1`,
				)
				.get(row.origin_key, row.oldest_non_bot_at);

			// Only gate if older than threshold and no delivery since arrival
			if (ageMs >= ORIGIN_SILENT_THRESHOLD_MS && !hasDeliverySince) {
				silence.set(row.origin_key, {
					oldestPendingMs: ageMs,
					messageId: row.message_id,
					receivedAt: row.oldest_non_bot_at,
					turnOpRef: row.turn_op_ref,
				});
			}
		}

		return silence;
	}
}

function parseDeclaredEventTypes(json: string | null): string[] | undefined {
	if (json === null) return undefined;
	try {
		const parsed: unknown = JSON.parse(json);
		return Array.isArray(parsed) && parsed.every((type) => typeof type === "string") ? parsed : undefined;
	} catch {
		return undefined;
	}
}

/** Pure projection over already-snapshotted sources — independently unit-testable. */
export function projectRuntimeCycle(
	sources: RuntimeCycleSources,
	generatedAt: string,
	pagination: OpsCycleParams = {},
): OpsCycleResult {
	const gates = new Set<CycleGateReason>();
	const cutoverMs = sources.latestCutoverAt === null ? Number.NaN : Date.parse(sources.latestCutoverAt);

	const sessionsTotal = sources.sessionRows.length;
	const sessionsOffset = pagination.offset ?? 0;
	const limit = pagination.limit ?? OPS_CYCLE_DEFAULT_LIMIT;
	const pageStart = Math.min(sessionsOffset, sessionsTotal);
	const pageEnd = Math.min(sessionsTotal, pageStart + limit);
	const sessions: CycleSessionView[] = [];
	sources.sessionRows.forEach((row, index) => {
		// A retired worker lane keeps its row (the epoch derives the next create
		// key) with no bound session; that is the designed idle state, not a
		// persona origin stuck mid-rebind. Retirement is proven only by a
		// settled lane job: a failed first bind leaves the same unbound row with
		// no job, and a crash leaves one with an unsettled job. Both stay gated.
		const retiredLane =
			row.origin_key.startsWith(WORK_LANE_PREFIX) &&
			row.gjc_session_id === "" &&
			sources.settledWorkOrigins.has(row.origin_key);
		// A cutover unbinds every origin at once. An origin with no work admitted
		// and no epoch change since then is quarantined history, not a rebind in
		// progress. Admission (inbound, lane job, monitor event) is the evidence:
		// a failed first bind never touches the session row itself.
		const lastActivityMs = Date.parse(row.last_activity_at ?? row.created_at);
		const cutoverRetired =
			row.gjc_session_id === "" && lastActivityMs <= cutoverMs && !sources.addressedSinceCutover.has(row.origin_key);
		if ((row.gjc_session_id === "" && !retiredLane && !cutoverRetired) || row.epoch < 0)
			gates.add("stale_session_identity");
		if (index < pageStart || index >= pageEnd) return;
		const origin = parseOriginRef(row.origin_ref_json);
		sessions.push({
			originKey: row.origin_key,
			origin,
			epoch: row.epoch,
			sessionId: row.gjc_session_id,
			createdAt: row.created_at,
			pendingInbound: sources.inboundPendingByOrigin.get(row.origin_key) ?? 0,
			lastActivityAt: row.last_activity_at,
			unsettledDeliveries: sources.unsettledByOrigin.get(row.origin_key)?.n ?? 0,
			oldestUnsettledAgeMs: sources.unsettledByOrigin.get(row.origin_key)?.oldestMs ?? null,
			contextDiff: sources.contextByOrigin.get(row.origin_key) ?? {
				unread: 0,
				expired: 0,
				truncated: 0,
				omittedOldestAt: null,
				omittedNewestAt: null,
				floorAt: null,
			},
			bootstrap: {
				epoch: row.epoch,
				pending: row.last_bootstrapped_epoch < row.epoch,
				appliedAt: row.bootstrap_applied_at,
				includedSections: parseStringList(row.bootstrap_sections_json),
				byteCount: row.bootstrap_byte_count,
				truncated: row.bootstrap_truncated === 1,
				diagnostics: parseStringList(row.bootstrap_diagnostics_json),
			},
		});
	});

	if (sources.unknownDeliveryStates.length > 0 || sources.unknownInboundStates.length > 0)
		gates.add("delivery_settlement_unknown");

	const quarantined = sources.memoryIntents.get("quarantined") ?? 0;
	const queued = sources.memoryIntents.get("queued") ?? 0;
	const written = sources.memoryIntents.get("written") ?? 0;
	const committed = sources.memoryIntents.get("committed") ?? 0;
	if (quarantined > 0) gates.add("memory_closure_blocked");
	if (sources.memoryGitIndexLock) gates.add("memory_git_index_locked");
	if (
		sources.brokerAgent &&
		(sources.brokerAgent.status === "unprovisioned" ||
			sources.brokerAgent.status === "interrupted" ||
			sources.brokerAgent.entries.some((entry) => entry.name === "agent.db" && entry.state === "missing_copy"))
	)
		gates.add("broker_agent_unprovisioned");

	const monitorFailed = sources.monitorStages.get("failed") ?? 0;
	if (monitorFailed > 0) gates.add("monitor_settlement_failed");
	// Events stuck at a non-terminal stage are visibly unresolved (issue #29:
	// `batched` rows used to strand forever while the projection stayed green).
	if (sources.monitorStages.get("batched") || sources.monitorStages.get("dispatched"))
		gates.add("monitor_settlement_stuck");
	// Terminal pre-author loss is invisible to the stage census gates above
	// (`failed_no_retry` is terminal), so a dead authoring lane read as healthy.
	if (
		sources.monitorAuthoringLost.length >= MONITOR_AUTHORING_LOSS_TYPES ||
		sources.monitorAuthoringLost.some((lost) => lost.consecutive >= MONITOR_AUTHORING_LOSS_CONSECUTIVE)
	)
		gates.add("monitor_authoring_lost");
	// Lane saturation is an operator condition: every further work.run is
	// refused until a lane is retired, so it must not read as a healthy idle.
	if (sources.activeLanes >= sources.maxLanes) gates.add("lane_capacity_exhausted");
	// Pending work with nothing in flight for longer than any healthy dispatch
	// takes is a stuck actor, not a busy one. Measured 2026-09-15: 159 pending /
	// 0 in flight for hours projected as `dispatching` while four origins were
	// bricked on a disowned tail. Automation must read that as degraded.
	if (sources.oldestStarvedPendingMs !== null && sources.oldestStarvedPendingMs >= INBOUND_STARVATION_MS)
		gates.add("inbound_starved");
	if (sources.agentDisk && agentDiskLow(sources.agentDisk)) gates.add("agent_disk_headroom");
	// The gateway classifies gjc envelopes by the contract of the gjc it was verified
	// against; a newer minor may have renamed the codes recovery depends on.
	if (sources.gjcVersion !== undefined && !isVerifiedGjcVersion(sources.gjcVersion))
		gates.add("gjc_unverified_version");
	// Issue #189: pid/lock liveness stayed green through a 19h monitor outage.
	// Dispatch outcome and incarnation churn are the health signals that moved.
	if (sources.monitorTerminalStreak >= MONITOR_TERMINAL_STREAK_THRESHOLD) gates.add("monitor_dispatch_failing");
	if (sources.brokerRespawnChurn) gates.add("broker_respawn_churn");
	if (isBrokerIndexLockBlocked(sources.brokerExitRecord)) gates.add("broker_index_lock_blocked");
	// Issue #688: platform messages from non-bot authors pending or bound older
	// than the threshold with no delivery on the origin since arrival, and not in
	// muted threads. A silent origin suggests the message is not being processed.
	if (sources.originSilence.size > 0) gates.add("origin_silent");

	const pendingInbound = sources.pendingInbound;
	const unsettled = totalUnsettled(sources);
	const memoryClosing = sources.memoryClosing || queued + written + committed > 0;

	const phase = decidePhase({
		gates: gates.size,
		inFlightInbound: sources.inFlightInbound,
		pendingInbound,
		unsettled,
		memoryClosing,
	});

	return {
		phase,
		gates: [...gates],
		generatedAt,
		instanceId: sources.instanceId,
		memoryClosing,
		sessionsTotal,
		sessionsOffset,
		sessions,
		memoryIntents: {
			queued,
			written,
			committed,
			receipted: sources.memoryIntents.get("receipted") ?? 0,
			quarantined,
		},
		memoryGitIndexLock: sources.memoryGitIndexLock,
		monitorEvents: [...sources.monitorStages.entries()]
			.map(([stage, count]) => ({ stage, count }))
			.sort((a, b) => a.stage.localeCompare(b.stage)),
		monitorFailures: sources.monitorFailures,
		monitorAuthoringLost: sources.monitorAuthoringLost,
		deliveries: {
			pending: sources.deliveryCounts.get("pending") ?? 0,
			inflight: sources.deliveryCounts.get("inflight") ?? 0,
			confirmed: sources.deliveryCounts.get("confirmed") ?? 0,
			failedAmbiguous: sources.deliveryCounts.get("failed_ambiguous") ?? 0,
			expired: sources.deliveryCounts.get("expired") ?? 0,
		},
		inFlightInbound: sources.inFlightInbound,
		pendingInbound,
		contextDiff: sources.contextDiff,
		lanes: { active: sources.activeLanes, max: sources.maxLanes },
		agentDisk: sources.agentDisk,
		brokerAgent: sources.brokerAgent,
		slo: sources.slo,
	};
}

function parseStringList(value: string): readonly string[] {
	try {
		const parsed = JSON.parse(value);
		return Array.isArray(parsed) && parsed.every((item) => typeof item === "string") ? parsed : ["projection_corrupt"];
	} catch {
		return ["projection_corrupt"];
	}
}

function decidePhase(state: {
	gates: number;
	inFlightInbound: number;
	pendingInbound: number;
	unsettled: number;
	memoryClosing: boolean;
}): CyclePhase {
	// Degraded dominates: a gated cycle is never reported as merely busy.
	if (state.gates > 0) return "degraded";
	if (state.inFlightInbound > 0) return "dispatching";
	if (state.unsettled > 0) return "delivering";
	if (state.memoryClosing) return "draining";
	if (state.pendingInbound > 0) return "dispatching";
	return "idle";
}

function parseOriginRef(originRefJson: string | null) {
	if (!originRefJson) return LOOPBACK_ORIGIN;
	try {
		return validateOriginRef(JSON.parse(originRefJson) as never);
	} catch {
		// A stored origin ref that no longer validates is itself a projection
		// anomaly; the row is still shown, on the loopback identity placeholder,
		// so the operator sees the anomaly instead of a missing session.
		return LOOPBACK_ORIGIN;
	}
}

function totalUnsettled(sources: RuntimeCycleSources): number {
	let total = 0;
	for (const { n } of sources.unsettledByOrigin.values()) total += n;
	return total;
}
