import { statfsSync } from "node:fs";
import {
	type AgentDiskView,
	type CycleGateReason,
	type CyclePhase,
	type CycleSessionView,
	LOOPBACK_ORIGIN,
	type OpsCycleResult,
	validateOriginRef,
} from "@gajae-gateway/protocol";
import { DEFAULT_WORK_MAX_LANES } from "../config";
import { WORK_LANE_PREFIX } from "../orchestrator/lane-governor";
import type { GatewayDatabase } from "../store/db";

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
	readonly contextByOrigin: ReadonlyMap<string, ReturnType<GatewayDatabase["contextDiagnostics"]>>;
	readonly contextDiff: ReturnType<GatewayDatabase["contextDiagnostics"]>;
	readonly inboundCounts: ReadonlyMap<string, number>;
	readonly pendingInbound: number;
	readonly inFlightInbound: number;
	readonly unknownInboundStates: readonly string[];
	readonly deliveryCounts: ReadonlyMap<string, number>;
	readonly unknownDeliveryStates: readonly string[];
	readonly unsettledByOrigin: ReadonlyMap<string, { n: number; oldestMs: number }>;
	readonly memoryIntents: ReadonlyMap<string, number>;
	readonly monitorStages: ReadonlyMap<string, number>;
	/** Event types whose latest terminal slots in the loss window were lost before authoring. */
	readonly monitorAuthoringLost: ReturnType<GatewayDatabase["monitorAuthoringLossStreaks"]>;
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
	/** Headroom of the broker-bound GJC agent directory; null when none is bound. */
	readonly agentDisk: AgentDiskView | null;
}

export class RuntimeCycleProjector {
	readonly #database: GatewayDatabase;
	readonly #memory: { readonly queueDepth: number };
	readonly #maxLanes: number;
	readonly #agentDir: string | undefined;

	constructor(
		database: GatewayDatabase,
		memory: { readonly queueDepth: number },
		options: { readonly maxLanes?: number; readonly agentDir?: string } = {},
	) {
		this.#database = database;
		this.#memory = memory;
		this.#maxLanes = options.maxLanes ?? DEFAULT_WORK_MAX_LANES;
		this.#agentDir = options.agentDir;
	}

	/** Snapshots durable state and projects the runtime cycle. Read-only; no writes. */
	project(now = new Date()): OpsCycleResult {
		const sources = this.#sources(now.getTime());
		return projectRuntimeCycle(sources, now.toISOString());
	}

	#sources(nowMs: number): RuntimeCycleSources {
		const sessions = this.#database.sessionIdentityRows();
		const inbound = this.#database.inboundStateCounts();
		const pendingRows = this.#database.inboundPendingByOrigin();
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
		const contextByOrigin = this.#database.contextDiagnosticsByOrigin();
		const deliveries = this.#database.deliveryStateCounts();
		const unsettled = new Map(
			this.#database.deliveryUnsettledByOrigin(nowMs).map((r) => [r.origin_key, { n: r.n, oldestMs: r.oldest_ms }]),
		);
		const memory = this.#database.memoryIntentCounts();
		const monitors = this.#database.monitorEventStageCounts();
		const inboundMap = new Map(inbound.map((r) => [r.state, r.n]));
		const unknownInbound = inbound.map((r) => r.state).filter((state) => !KNOWN_INBOUND_STATES.has(state));
		const unknownDeliveries = deliveries.map((r) => r.state).filter((state) => !KNOWN_DELIVERY_STATES.has(state));
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
			monitorStages: new Map(monitors.map((r) => [r.stage, r.n])),
			monitorAuthoringLost: this.#database.monitorAuthoringLossStreaks(
				new Date(nowMs - MONITOR_AUTHORING_LOSS_WINDOW_MS).toISOString(),
			),
			inboundPendingByOrigin: pendingByOrigin,
			oldestStarvedPendingMs,
			contextByOrigin,
			contextDiff: this.#database.contextDiagnostics(),
			memoryClosing: this.#memory.queueDepth > 0,
			instanceId: this.#database.instanceId,
			activeLanes: this.#database.workLaneRows().length,
			maxLanes: this.#maxLanes,
			settledWorkOrigins: new Set(
				this.#database
					.laneJobRows()
					.filter((row) => SETTLED_JOB_STATES.has(row.state))
					.map((row) => `${WORK_LANE_PREFIX}${row.lane_key.slice("work-".length)}`),
			),
			agentDisk: this.#agentDir === undefined ? null : observeAgentDisk(this.#agentDir),
		};
	}
}

/** Pure projection over already-snapshotted sources — independently unit-testable. */
export function projectRuntimeCycle(sources: RuntimeCycleSources, generatedAt: string): OpsCycleResult {
	const gates = new Set<CycleGateReason>();

	const sessions: CycleSessionView[] = sources.sessionRows.map((row) => {
		const origin = parseOriginRef(row.origin_ref_json);
		// A retired worker lane keeps its row (the epoch derives the next create
		// key) with no bound session; that is the designed idle state, not a
		// persona origin stuck mid-rebind. Retirement is proven only by a
		// settled lane job: a failed first bind leaves the same unbound row with
		// no job, and a crash leaves one with an unsettled job. Both stay gated.
		const retiredLane =
			row.origin_key.startsWith(WORK_LANE_PREFIX) &&
			row.gjc_session_id === "" &&
			sources.settledWorkOrigins.has(row.origin_key);
		if ((row.gjc_session_id === "" && !retiredLane) || row.epoch < 0) gates.add("stale_session_identity");
		return {
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
		};
	});

	if (sources.unknownDeliveryStates.length > 0 || sources.unknownInboundStates.length > 0)
		gates.add("delivery_settlement_unknown");

	const quarantined = sources.memoryIntents.get("quarantined") ?? 0;
	const queued = sources.memoryIntents.get("queued") ?? 0;
	const written = sources.memoryIntents.get("written") ?? 0;
	const committed = sources.memoryIntents.get("committed") ?? 0;
	if (quarantined > 0) gates.add("memory_closure_blocked");

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
		sessions,
		memoryIntents: {
			queued,
			written,
			committed,
			receipted: sources.memoryIntents.get("receipted") ?? 0,
			quarantined,
		},
		monitorEvents: [...sources.monitorStages.entries()]
			.map(([stage, count]) => ({ stage, count }))
			.sort((a, b) => a.stage.localeCompare(b.stage)),
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
