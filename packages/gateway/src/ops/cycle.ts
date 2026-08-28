import {
	type CycleGateReason,
	type CyclePhase,
	type CycleSessionView,
	LOOPBACK_ORIGIN,
	type OpsCycleResult,
	validateOriginRef,
} from "@gajaeway/protocol";
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

export interface RuntimeCycleSources {
	readonly sessionRows: Array<{
		readonly origin_key: string;
		readonly origin_ref_json: string | null;
		readonly gjc_session_id: string;
		readonly epoch: number;
		readonly created_at: string;
		readonly last_activity_at: string | null;
	}>;
	readonly inboundPendingByOrigin: ReadonlyMap<string, number>;
	readonly inboundCounts: ReadonlyMap<string, number>;
	readonly pendingInbound: number;
	readonly inFlightInbound: number;
	readonly unknownInboundStates: readonly string[];
	readonly deliveryCounts: ReadonlyMap<string, number>;
	readonly unknownDeliveryStates: readonly string[];
	readonly unsettledByOrigin: ReadonlyMap<string, { n: number; oldestMs: number }>;
	readonly memoryIntents: ReadonlyMap<string, number>;
	readonly monitorStages: ReadonlyMap<string, number>;
	readonly memoryClosing: boolean;
	readonly instanceId: string;
}

export class RuntimeCycleProjector {
	readonly #database: GatewayDatabase;
	readonly #memory: { readonly queueDepth: number };

	constructor(database: GatewayDatabase, memory: { readonly queueDepth: number }) {
		this.#database = database;
		this.#memory = memory;
	}

	/** Snapshots durable state and projects the runtime cycle. Read-only; no writes. */
	project(now = new Date()): OpsCycleResult {
		const sources = this.#sources(now.getTime());
		return projectRuntimeCycle(sources, now.toISOString());
	}

	#sources(nowMs: number): RuntimeCycleSources {
		const sessions = this.#database.sessionIdentityRows();
		const inbound = this.#database.inboundStateCounts();
		const pendingByOrigin = new Map(this.#database.inboundPendingByOrigin().map((r) => [r.origin_key, r.n]));
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
			inFlightInbound: inboundMap.get("processing") ?? 0,
			pendingInbound: inboundMap.get("pending") ?? 0,
			unknownInboundStates: unknownInbound,
			deliveryCounts: new Map(deliveries.map((r) => [r.state, r.n])),
			unknownDeliveryStates: unknownDeliveries,
			unsettledByOrigin: unsettled,
			memoryIntents: new Map(memory.map((r) => [r.state, r.n])),
			monitorStages: new Map(monitors.map((r) => [r.stage, r.n])),
			inboundPendingByOrigin: pendingByOrigin,
			memoryClosing: this.#memory.queueDepth > 0,
			instanceId: this.#database.instanceId,
		};
	}
}

/** Pure projection over already-snapshotted sources — independently unit-testable. */
export function projectRuntimeCycle(sources: RuntimeCycleSources, generatedAt: string): OpsCycleResult {
	const gates = new Set<CycleGateReason>();

	const sessions: CycleSessionView[] = sources.sessionRows.map((row) => {
		const origin = parseOriginRef(row.origin_ref_json);
		if (row.gjc_session_id === "" || row.epoch < 0) gates.add("stale_session_identity");
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
		deliveries: {
			pending: sources.deliveryCounts.get("pending") ?? 0,
			inflight: sources.deliveryCounts.get("inflight") ?? 0,
			confirmed: sources.deliveryCounts.get("confirmed") ?? 0,
			failedAmbiguous: sources.deliveryCounts.get("failed_ambiguous") ?? 0,
			expired: sources.deliveryCounts.get("expired") ?? 0,
		},
		inFlightInbound: sources.inFlightInbound,
		pendingInbound,
	};
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
