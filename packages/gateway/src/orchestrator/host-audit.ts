/**
 * I6c host audit (owner decision Q4: default `index-dead-only`).
 *
 * The gateway may reap a session host that lives in ITS private agent dir only
 * when every clause of the predicate holds on evidence it can prove:
 *   (a) ownership: the process environment carries GJC_CODING_AGENT_DIR=<private dir>;
 *   (b) index verdict from the broker's own snapshot (never a raw file read): the
 *       row naming that pid is not live with a heartbeat older than 2x the staleness
 *       window, or no row references that pid;
 *   (c) incarnation: the row's incarnation equals the OS-observed value; for the
 *       no-row case the process start time predates the snapshot (PID-reuse guard)
 *       and the pid appears in no non-terminal turn_attempts.host_pid;
 *   (d) admission guard: no possibly-admitted attempt, unexpired fence, nonterminal
 *       trigger, open lane attempt or dispatched monitor event is bound to a session
 *       whose row names that pid;
 *   (e) (b)-(d) are revalidated right before SIGTERM and again before SIGKILL.
 * Unprovable -> skip with a named reason. The first boot after the schema-21
 * upgrade runs observe-only (`unknown => protect`) until the backfill flag is set.
 * `all` is an explicit operator flag that reaps every env-proven host; it is never
 * a runtime path.
 */

export type BrokerReapPolicy = "index-dead-only" | "all";

export interface HostCandidate {
	readonly pid: number;
	readonly ppid: number;
	readonly args: string;
	/** From the process environment (ps -E / /proc/<pid>/environ). */
	readonly agentDirEnv: string | undefined;
	/** OS process start time (epoch ms) when derivable. */
	readonly startedAtMs: number | undefined;
	/** OS-derived incarnation per the I0a derivation; undefined when not derivable. */
	readonly incarnation: string | undefined;
}

export interface IndexRow {
	readonly sessionId: string;
	readonly pid: number | undefined;
	readonly live: boolean;
	readonly lastHeartbeatAt: number | undefined;
	readonly incarnation: string | undefined;
}

export interface IndexSnapshot {
	readonly observedAt: number;
	readonly rows: readonly IndexRow[];
	/** Staleness window the broker itself uses for heartbeats (ms). */
	readonly stalenessMs: number;
}

export interface AuditEvidence {
	/** Sessions with a possibly-admitted attempt, unexpired fence, or nonterminal bound work. */
	readonly protectedSessions: ReadonlySet<string>;
	/** pids referenced by any non-terminal turn_attempts.host_pid. */
	readonly activeHostPids: ReadonlySet<number>;
	readonly backfillDone: boolean;
}

export type AuditVerdict =
	| { readonly kind: "reap"; readonly pid: number; readonly basis: "index-dead" | "unindexed" | "policy-all" }
	| { readonly kind: "skip"; readonly pid: number; readonly reason: string }
	| { readonly kind: "observe"; readonly pid: number; readonly wouldReap: boolean; readonly reason: string };

export function auditHost(input: {
	candidate: HostCandidate;
	privateAgentDir: string;
	daemonPid: number | undefined;
	snapshot: IndexSnapshot | undefined;
	evidence: AuditEvidence;
	policy: BrokerReapPolicy;
	nowMs: number;
}): AuditVerdict {
	const { candidate, snapshot, evidence, policy } = input;
	const pid = candidate.pid;
	if (pid === process.pid || pid === input.daemonPid) return { kind: "skip", pid, reason: "not_a_host" };
	// (a) ownership by environment, exact match (no prefix collisions).
	if (candidate.agentDirEnv !== input.privateAgentDir) return { kind: "skip", pid, reason: "ownership_unproven" };
	if (policy === "all") return { kind: "reap", pid, basis: "policy-all" };
	if (!snapshot) return { kind: "skip", pid, reason: "index_unavailable" };
	const decide = (): AuditVerdict => {
		const rows = snapshot.rows.filter((row) => row.pid === pid);
		if (rows.length > 0) {
			// (b) index verdict: every row naming the pid must be dead-and-stale.
			for (const row of rows) {
				if (row.live) return { kind: "skip", pid, reason: "index_live" };
				if (row.lastHeartbeatAt === undefined) return { kind: "skip", pid, reason: "heartbeat_unknown" };
				if (input.nowMs - row.lastHeartbeatAt < 2 * snapshot.stalenessMs)
					return { kind: "skip", pid, reason: "heartbeat_recent" };
				// (c) incarnation equality.
				if (row.incarnation === undefined || candidate.incarnation === undefined)
					return { kind: "skip", pid, reason: "incarnation_not_derivable" };
				if (row.incarnation !== candidate.incarnation) return { kind: "skip", pid, reason: "incarnation_mismatch" };
				// (d) admission guard.
				if (evidence.protectedSessions.has(row.sessionId)) return { kind: "skip", pid, reason: "admitted_work" };
			}
			return { kind: "reap", pid, basis: "index-dead" };
		}
		// No row references the pid: (c) PID-reuse guard + (d) attempt guard.
		if (candidate.startedAtMs === undefined) return { kind: "skip", pid, reason: "start_time_unknown" };
		if (candidate.startedAtMs >= snapshot.observedAt) return { kind: "skip", pid, reason: "started_after_snapshot" };
		if (evidence.activeHostPids.has(pid)) return { kind: "skip", pid, reason: "active_attempt_host" };
		return { kind: "reap", pid, basis: "unindexed" };
	};
	const verdict = decide();
	// First boot after upgrade: observe only, unknown => protect.
	if (!evidence.backfillDone)
		return {
			kind: "observe",
			pid,
			wouldReap: verdict.kind === "reap",
			reason: verdict.kind === "reap" ? `would_reap:${verdict.basis}` : verdict.reason,
		};
	return verdict;
}

export function parseBrokerReapPolicy(value: unknown): BrokerReapPolicy {
	if (value === undefined || value === null || value === "") return "index-dead-only";
	if (value === "index-dead-only" || value === "all") return value;
	throw new Error(`brokerReap must be "index-dead-only" or "all", got ${JSON.stringify(value)}`);
}
