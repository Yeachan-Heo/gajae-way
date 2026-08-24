import {
	BrokerCli,
	BrokerCliError,
	type SessionMetadataV1,
	type SdkSessionRowV1,
	type BrokerTransport,
} from "./cli";

export const DEFAULT_RECONCILE_POLL_MS = 15_000;
export const MAX_RECONCILE_CYCLE_MS = 20_000;
export const MAX_METADATA_QUERIES_PER_CYCLE = 10;
export const INITIAL_METADATA_BACKOFF_MS = 30_000;
export const MAX_METADATA_BACKOFF_MS = 15 * 60_000;

export interface RegistryRow {
	readonly sessionId: string;
	readonly kind: string;
	readonly purpose?: string;
	readonly brief?: string;
	readonly status: string;
	readonly surfaceId?: string;
	readonly locator?: string;
	readonly endpointGeneration?: number;
	readonly hostIncarnation?: string;
	readonly identityProvenance?: string;
	readonly indexSeq?: number;
	readonly live: boolean;
	readonly deleted: boolean;
	readonly terminalUncertain: boolean;
	readonly ambiguous: boolean;
	readonly activityState?: "active" | "idle";
	readonly activityAt?: number;
	readonly lastHeartbeatAt?: number;
	readonly metaName?: string;
	readonly metaCwd?: string;
	readonly metaKind?: string;
	readonly metadataState: "pending" | "enriched" | "unavailable";
	readonly metadataAt?: number;
	readonly source: "gateway" | "reconciler";
	readonly createdAt: number;
	readonly lastSeenAt?: number;
	readonly closedAt?: number;
	readonly registryRev: number;
	readonly quarantined: boolean;
}

export interface RegistryCore {
	registryApplyBrokerSnapshot(input: {
		observedAt: number;
		rows: Array<{
			sessionId: string;
			locator: string;
			endpointGeneration: number;
			hostIncarnation?: string;
			identityProvenance?: string;
			indexSeq: number;
			live: boolean;
			deleted: boolean;
			terminalUncertain: boolean;
			ambiguous: boolean;
			activityState?: "active" | "idle";
			activityAt?: number;
			lastHeartbeatAt?: number;
		}>;
	}): {
		newSessionIds: string[];
		changedSessionIds: string[];
		changedIndexSeqSessionIds: string[];
		driftCount: number;
	};
	registryList(input?: { limit?: number; offset?: number }): { rows: RegistryRow[]; total: number };
	registryApplyMetadata(input: { sessionId: string; name: string; cwd: string; kind: string; observedAt: number }): RegistryRow;
	registryMarkMetadataUnavailable(input: { sessionId: string; observedAt: number }): RegistryRow;
	setReconcileStatus(input: { lastOkAt: number; cycleMs: number; driftCount: number }): void;
}

export interface BrokerReconcilerOptions {
	readonly core: RegistryCore;
	readonly broker?: BrokerTransport;
	readonly pollMs?: number;
	readonly cycleSlaMs?: number;
	readonly now?: () => number;
}

export interface ReconcileCycleResult {
	readonly observedAt: number;
	readonly cycleMs: number;
	readonly driftCount: number;
	readonly metadataQueries: number;
}

interface MetadataBackoff {
	readonly delayMs: number;
	readonly nextAttemptAt: number;
}

function asNativeRow(row: SdkSessionRowV1) {
	return {
		sessionId: row.sessionId,
		locator: JSON.stringify(row.locator),
		endpointGeneration: row.endpointGeneration,
		...(row.hostIncarnation === undefined ? {} : { hostIncarnation: row.hostIncarnation }),
		...(row.identityProvenance === undefined ? {} : { identityProvenance: row.identityProvenance }),
		indexSeq: row.indexSeq,
		live: row.live,
		deleted: row.deleted,
		terminalUncertain: row.terminalUncertain ?? false,
		ambiguous: row.ambiguous ?? false,
		...(row.activity === undefined ? {} : { activityState: row.activity.state, activityAt: row.activity.at }),
		...(row.lastHeartbeatAt === undefined ? {} : { lastHeartbeatAt: row.lastHeartbeatAt }),
	};
}

/**
 * Polls the supported broker CLI. A failed command or DTO parse happens before
 * `registryApplyBrokerSnapshot`, so a failed cycle cannot partially mutate the
 * durable registry.
 */
export class BrokerReconciler {
	readonly core: RegistryCore;
	readonly broker: BrokerTransport;
	readonly pollMs: number;
	readonly cycleSlaMs: number;
	readonly #now: () => number;
	readonly #metadataBackoff = new Map<string, MetadataBackoff>();
	#timer: ReturnType<typeof setInterval> | undefined;
	#inFlight: Promise<ReconcileCycleResult> | undefined;

	constructor(options: BrokerReconcilerOptions) {
		this.core = options.core;
		this.broker = options.broker ?? new BrokerCli();
		this.pollMs = options.pollMs ?? DEFAULT_RECONCILE_POLL_MS;
		this.cycleSlaMs = options.cycleSlaMs ?? MAX_RECONCILE_CYCLE_MS;
		this.#now = options.now ?? Date.now;
		if (!Number.isSafeInteger(this.pollMs) || this.pollMs <= 0) throw new Error("pollMs must be a positive safe integer.");
		if (!Number.isSafeInteger(this.cycleSlaMs) || this.cycleSlaMs <= 0 || this.cycleSlaMs > MAX_RECONCILE_CYCLE_MS) {
			throw new Error(`cycleSlaMs must be in 1..=${MAX_RECONCILE_CYCLE_MS}.`);
		}
	}

	start(): void {
		if (this.#timer) return;
		void this.trigger().catch(() => {
			// A subsequent poll retries. `trigger()` remains available to callers who
			// need the typed failure rather than background liveness.
		});
		this.#timer = setInterval(() => {
			void this.trigger().catch(() => {
				// Preserve the last successful status; broker failure is deliberately stale.
			});
		}, this.pollMs);
	}

	stop(): void {
		if (!this.#timer) return;
		clearInterval(this.#timer);
		this.#timer = undefined;
	}

	/** Runs one on-demand cycle, coalescing concurrent requests into one snapshot. */
	trigger(): Promise<ReconcileCycleResult> {
		if (this.#inFlight) return this.#inFlight;
		const running = this.runCycle();
		this.#inFlight = running.finally(() => {
			this.#inFlight = undefined;
		});
		return this.#inFlight;
	}

	private remaining(deadline: number): number {
		return Math.max(0, deadline - this.#now());
	}

	private commandBudget(deadline: number): number {
		return Math.max(1, Math.min(this.remaining(deadline), this.broker.commandTimeoutMs, Math.floor(this.cycleSlaMs / 2)));
	}

	private async runCycle(): Promise<ReconcileCycleResult> {
		const startedAt = this.#now();
		const deadline = startedAt + this.cycleSlaMs;
		const listed = await this.broker.listSessions({ timeoutMs: this.commandBudget(deadline) });
		const observedAt = this.#now();
		// No registry mutation occurs until the entire versioned list is parsed.
		const applied = this.core.registryApplyBrokerSnapshot({
			observedAt,
			rows: listed.sessions.map(asNativeRow),
		});
		const rows = this.allRows();
		const metadataQueries = await this.enrich(rows, applied.newSessionIds, applied.changedIndexSeqSessionIds, new Set(listed.sessions.map(row => row.sessionId)), deadline);
		const cycleMs = this.#now() - startedAt;
		this.core.setReconcileStatus({ lastOkAt: this.#now(), cycleMs, driftCount: applied.driftCount });
		return { observedAt, cycleMs, driftCount: applied.driftCount, metadataQueries };
	}

	private allRows(): RegistryRow[] {
		const rows: RegistryRow[] = [];
		let offset = 0;
		for (;;) {
			const page = this.core.registryList({ limit: 500, offset });
			rows.push(...page.rows);
			offset += page.rows.length;
			if (offset >= page.total || page.rows.length === 0) return rows;
		}
	}

	private eligibleForMetadata(row: RegistryRow, now: number): boolean {
		if (row.metadataState !== "unavailable") return true;
		const backoff = this.#metadataBackoff.get(row.sessionId);
		if (backoff) return now >= backoff.nextAttemptAt;
		return now >= (row.metadataAt ?? 0) + INITIAL_METADATA_BACKOFF_MS;
	}

	private async enrich(
		rows: readonly RegistryRow[],
		newSessionIds: readonly string[],
		changedIndexSeqSessionIds: readonly string[],
		listedSessionIds: ReadonlySet<string>,
		deadline: number,
	): Promise<number> {
		const now = this.#now();
		const newSessions = new Set(newSessionIds);
		const changedIndexSeq = new Set(changedIndexSeqSessionIds);
		const candidates = rows
			.filter(row => listedSessionIds.has(row.sessionId) && !row.deleted && this.eligibleForMetadata(row, now))
			.sort((left, right) => {
				const priority = (row: RegistryRow): readonly [number, number, number, string] => [
					newSessions.has(row.sessionId) ? 0 : 1,
					changedIndexSeq.has(row.sessionId) ? 0 : 1,
					row.surfaceId === undefined ? 1 : 0,
					row.sessionId,
				];
				const leftPriority = priority(left);
				const rightPriority = priority(right);
				for (let index = 0; index < leftPriority.length; index += 1) {
					if (leftPriority[index] === rightPriority[index]) continue;
					return leftPriority[index] < rightPriority[index] ? -1 : 1;
				}
				return 0;
			})
			.slice(0, MAX_METADATA_QUERIES_PER_CYCLE);
		await Promise.all(candidates.map(row => this.enrichOne(row, deadline)));
		return candidates.length;
	}

	private async enrichOne(row: RegistryRow, deadline: number): Promise<void> {
		if (this.remaining(deadline) <= 0) return;
		const timeoutMs = this.commandBudget(deadline);
		try {
			const metadata = await this.broker.sessionMetadata(row.sessionId, { timeoutMs });
			this.applyMetadata(metadata);
			this.#metadataBackoff.delete(row.sessionId);
		} catch (error) {
			if (!(error instanceof BrokerCliError)) throw error;
			const now = this.#now();
			const current = this.#metadataBackoff.get(row.sessionId);
			const priorDelay = current?.delayMs ?? (row.metadataState === "unavailable" ? INITIAL_METADATA_BACKOFF_MS : 0);
			const delayMs = priorDelay === 0 ? INITIAL_METADATA_BACKOFF_MS : Math.min(MAX_METADATA_BACKOFF_MS, priorDelay * 2);
			this.#metadataBackoff.set(row.sessionId, { delayMs, nextAttemptAt: now + delayMs });
			this.core.registryMarkMetadataUnavailable({ sessionId: row.sessionId, observedAt: now });
		}
	}

	private applyMetadata(metadata: SessionMetadataV1): void {
		this.core.registryApplyMetadata({
			sessionId: metadata.sessionId,
			// The real broker omits name for unnamed sessions; the registry column stays non-null.
			name: metadata.name ?? "",
			cwd: metadata.cwd,
			kind: metadata.kind,
			observedAt: this.#now(),
		});
	}
}
