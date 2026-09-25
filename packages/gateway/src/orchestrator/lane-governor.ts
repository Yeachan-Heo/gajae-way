import { type LaneCapacityDetail, ProtocolError } from "@gajae-gateway/protocol";
import { type LaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import { DEFAULT_WORK_IDLE_RETIRE_MS, DEFAULT_WORK_MAX_LANES } from "../config";
import type { GatewayDatabase } from "../store/db";
import { sanitizeDiagnostic } from "./rebind";
import { isSessionUnavailable, type SessionPort } from "./session-port";

export const WORK_LANE_PREFIX = "work/task/";

/** Lane-job states after which a bound session is dead weight and may be retired by the sweep. */
const TERMINAL_JOB_STATES = new Set(["done", "aborted"]);
/**
 * Attempt end states the gateway recorded WITHOUT broker proof that the
 * operation stopped: a reaped wait (`attempt_ended`), a crash-left attempt
 * (`terminal_uncertain`), and a local `failed` (any exception on the request
 * path, including a status/transport error after an accepted send). Retiring
 * such a lane needs runtime evidence; only `completed` is broker-proven.
 */
const UNPROVEN_END_STATES = new Set(["attempt_ended", "terminal_uncertain", "terminal_missing_receipt", "failed"]);

/**
 * Durable identity of one work lane job. The jobId is INJECTIVE: each
 * UTF-8 byte of the exact name becomes two hex digits, so distinct names
 * (Foo/foo, a.b/a_b) always map to distinct ids while staying within the
 * [a-z0-9-] jobId alphabet. 64-byte names cap at 128 hex chars + the prefix,
 * inside the schema's id length bound.
 */
export function laneJobIdentity(workName: string): { jobId: string; laneKey: string } {
	return { jobId: `lanejob-${Buffer.from(workName, "utf8").toString("hex")}`, laneKey: `work-${workName}` };
}

export function workSessionKey(name: string): string {
	return `${WORK_LANE_PREFIX}${name}`;
}

export interface ActiveLane {
	readonly name: string;
	readonly sessionKey: string;
	readonly sessionId: string;
	readonly lastActivityAt: string | null;
	readonly idleMs: number;
	/** Lane-job state; `unknown` when no job row exists, `corrupt` when the row cannot be parsed. */
	readonly state: string;
	/**
	 * True while the lane must not be retired: the durable job has an attempt
	 * without a recorded end, or the job record is corrupt and cannot prove
	 * there is none. Missing evidence is never "settled".
	 */
	readonly attemptOpen: boolean;
}

export type LaneRetireOutcome =
	| { readonly retired: true; readonly sessionKey: string; readonly sessionId: string; readonly closed: boolean }
	| { readonly retired: false; readonly sessionKey: string; readonly reason: string };

export type SweepReason = "idle" | "job_done" | "job_aborted";

/** What a sweep observed when it nominated a lane; re-proven under the lane lock. */
export interface SweepNomination {
	readonly sessionId: string;
	readonly reason: SweepReason;
	/** The sweep's clock; the locked recheck evaluates fresh state at this time or later. */
	readonly now: number;
}

export interface LaneGovernorOptions {
	readonly database: GatewayDatabase;
	readonly sessionPort: SessionPort;
	readonly maxLanes?: number;
	readonly idleRetireMs?: number;
	readonly now?: () => number;
	readonly log?: (line: string) => void;
}

/**
 * Gateway-owned admission and retirement for asynchronous work lanes.
 *
 * Every lane is a bound `work/task/<name>` origin hosted by the private broker,
 * so the gateway can count, cap, and close them. Retirement is `session.close`
 * plus an epoch bump: the saved session stays in the broker index (deleting it
 * would trip gjc's global cleanup fence) while the next start/run for that
 * name creates a fresh session instead of resuming a stale one.
 */
export class LaneGovernor {
	readonly #database: GatewayDatabase;
	readonly #port: SessionPort;
	readonly maxLanes: number;
	readonly idleRetireMs: number;
	readonly #now: () => number;
	readonly #log: (line: string) => void;
	#recoveryGate?: () => Promise<void>;
	#stopped = false;
	readonly #mutations = new Set<Promise<LaneRetireOutcome>>();

	constructor(options: LaneGovernorOptions) {
		this.#database = options.database;
		this.#port = options.sessionPort;
		this.maxLanes = positiveInteger(options.maxLanes, DEFAULT_WORK_MAX_LANES, "maxLanes");
		this.idleRetireMs = positiveInteger(options.idleRetireMs, DEFAULT_WORK_IDLE_RETIRE_MS, "idleRetireMs");
		this.#now = options.now ?? (() => Date.now());
		this.#log = options.log ?? ((line) => console.error(line));
	}

	/** Installed by the sole attempt owner; recovery registration precedes retirement. */
	setRecoveryGate(gate: () => Promise<void>): void {
		this.#recoveryGate = gate;
		this.#stopped = false;
	}

	async stop(): Promise<void> {
		this.#stopped = true;
		await Promise.allSettled([...this.#mutations]);
	}

	activeLanes(now = this.#now()): ActiveLane[] {
		return this.#database.workLaneRows().map((row) => {
			const name = row.origin_key.slice(WORK_LANE_PREFIX.length);
			const job = this.#job(name);
			const activityMs = row.last_activity_at ? Date.parse(row.last_activity_at) : Number.NaN;
			return {
				name,
				sessionKey: row.origin_key,
				sessionId: row.gjc_session_id,
				lastActivityAt: row.last_activity_at,
				idleMs: Number.isFinite(activityMs) ? Math.max(0, now - activityMs) : Number.POSITIVE_INFINITY,
				state: job === "corrupt" ? "corrupt" : (job?.state ?? "unknown"),
				attemptOpen: job === "corrupt" || (job?.attempts.some((attempt) => attempt.endedAt === undefined) ?? false),
			};
		});
	}

	/**
	 * A name that already owns a bound lane is always admitted (it resumes its
	 * own session). A new name past the cap is refused with the idlest lanes as
	 * retirement candidates, so the caller frees a slot deliberately.
	 */
	assertAdmission(name: string): void {
		const { jobId } = laneJobIdentity(name);
		if (this.#database.isBrokerQuarantined("work", jobId))
			throw new ProtocolError("verb_failed", "work lane belongs to a quarantined broker authority", {
				reasonCode: "broker_authority_quarantined",
				jobId,
				name,
			});
		const lanes = this.activeLanes();
		if (lanes.some((lane) => lane.name === name)) return;
		if (lanes.length < this.maxLanes) return;
		const detail: LaneCapacityDetail = {
			maxLanes: this.maxLanes,
			active: lanes.length,
			candidates: [...lanes]
				.sort((left, right) => right.idleMs - left.idleMs)
				.map((lane) => ({
					name: lane.name,
					idleMs: Number.isFinite(lane.idleMs) ? lane.idleMs : -1,
					state: lane.state,
				})),
		};
		throw new ProtocolError(
			"lane_capacity",
			`work lanes are at capacity (${lanes.length}/${this.maxLanes}); retire one with work.retire before starting ${name}`,
			detail,
		);
	}

	/**
	 * Serialized with the lane's short mutation lock, never its response wait.
	 * The binding identity observed
	 * by the caller (sweep snapshot, operator view) is re-proven under the lock:
	 * a lane that was reused or rebound meanwhile is left alone.
	 *
	 * `expected` carries the nominating snapshot: the session id it selected and
	 * the predicate it selected on. Both are re-proven under the lock; a lane
	 * reused on the same session (no longer idle, job no longer terminal) or
	 * rebound to a new one is left alone.
	 */
	retire(name: string, reason: string, expected?: SweepNomination): Promise<LaneRetireOutcome> {
		if (this.#database.isBrokerQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({
				retired: false,
				sessionKey: workSessionKey(name),
				reason: "broker_authority_quarantined",
			});
		if (this.#stopped)
			return Promise.resolve({ retired: false, sessionKey: workSessionKey(name), reason: "gateway is stopping" });
		const task = (async () => {
			await this.#recoveryGate?.();
			if (this.#stopped)
				return { retired: false as const, sessionKey: workSessionKey(name), reason: "gateway is stopping" };
			return this.#retire(name, reason, expected);
		})();
		this.#mutations.add(task);
		void task.then(
			() => this.#mutations.delete(task),
			() => this.#mutations.delete(task),
		);
		return task;
	}

	#retire(name: string, reason: string, expected?: SweepNomination): Promise<LaneRetireOutcome> {
		const sessionKey = workSessionKey(name);
		if (this.#database.isBrokerQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({ retired: false, sessionKey, reason: "broker_authority_quarantined" });
		// Refuse an unsettled lane now rather than queuing retirement behind its
		// turn; the same check repeats under the lock because a run may start
		// before we acquire it.
		const preflight = this.activeLanes().find((candidate) => candidate.name === name);
		if (preflight?.attemptOpen) return Promise.resolve(unsettled(sessionKey, preflight));
		return this.#port.runExclusive(sessionKey, async () => {
			if (this.#database.isBrokerQuarantined("work", laneJobIdentity(name).jobId))
				return { retired: false, sessionKey, reason: "broker_authority_quarantined" };
			if (this.#stopped) return { retired: false, sessionKey, reason: "gateway is stopping" };
			const lane = this.activeLanes(expected ? Math.max(expected.now, this.#now()) : undefined).find(
				(candidate) => candidate.name === name,
			);
			if (!lane) return { retired: false, sessionKey, reason: "no bound lane for that name" };
			if (expected && lane.sessionId !== expected.sessionId)
				return { retired: false, sessionKey, reason: `lane was rebound to ${lane.sessionId} since it was selected` };
			if (lane.attemptOpen) return unsettled(sessionKey, lane);
			if (expected && this.#sweepReason(lane) !== expected.reason)
				return { retired: false, sessionKey, reason: `lane no longer qualifies for ${expected.reason} retirement` };
			const job = this.#job(name);
			const record = job === "corrupt" ? undefined : job;
			const repo = record?.lane.worktreePath ?? process.cwd();
			// The ledger's last attempt ended, but only the broker knows whether
			// its operation actually reached a terminal state: a timed-out wait
			// or a crash-left attempt may still be running.
			const runtime = await this.#runtimeSettled(record, lane.sessionId, repo);
			if (!runtime.settled) return { retired: false, sessionKey, reason: runtime.reason };
			let closed = false;
			try {
				await this.#port.close({ sessionId: lane.sessionId, repo });
				closed = true;
			} catch (error) {
				// An unconfirmed close must not release the slot: a worker that is
				// still alive would then be uncounted, which is the exact failure
				// this governor exists to prevent. Only broker liveness proving the
				// session gone lets the binding clear.
				const detail = sanitizeDiagnostic(diagnostic(error));
				const liveness = await this.#liveness(lane.sessionId, repo);
				if (liveness.live !== false && !liveness.disowned) {
					this.#log(`lane_close_failed name=${name} session=${lane.sessionId} detail=${detail} action=retained`);
					return {
						retired: false,
						sessionKey,
						reason: `session.close failed and the session is not proven gone: ${detail}`,
					};
				}
				this.#log(`lane_close_failed name=${name} session=${lane.sessionId} detail=${detail} action=session_gone`);
			}
			this.#database.rebindEpoch(sessionKey);
			this.#log(`lane_retired name=${name} session=${lane.sessionId} reason=${reason} closed=${closed}`);
			return { retired: true, sessionKey, sessionId: lane.sessionId, closed };
		});
	}

	/**
	 * Retires lanes whose job is terminal or that have been quiet past the idle
	 * ceiling. The snapshot only nominates; `retire` re-proves the binding
	 * identity and eligibility under the lane lock before closing anything.
	 */
	async sweep(now = this.#now()): Promise<number> {
		if (this.#stopped) return 0;
		let retired = 0;
		for (const lane of this.activeLanes(now)) {
			const reason = this.#sweepReason(lane);
			if (!reason) continue;
			const outcome = await this.retire(lane.name, reason, { sessionId: lane.sessionId, reason, now });
			if (outcome.retired) retired++;
		}
		return retired;
	}

	/** The sweep predicate, evaluated on a fresh lane view; undefined when the lane must stay. */
	#sweepReason(lane: ActiveLane): SweepReason | undefined {
		if (lane.attemptOpen) return undefined;
		if (TERMINAL_JOB_STATES.has(lane.state)) return `job_${lane.state}` as SweepReason;
		return lane.idleMs >= this.idleRetireMs ? "idle" : undefined;
	}

	/**
	 * Runtime terminality for the ledger's last attempt. `endedAt` is the
	 * gateway's observation, not the broker's: `attempt_ended` (reaped wait)
	 * and `terminal_uncertain` (crash-left) attempts may still be running. They
	 * are settled only when `status` reports a terminal op, or reports an
	 * unknown op on a session the broker says is dead (nothing can be running).
	 */
	async #runtimeSettled(
		job: LaneJobRecord | undefined,
		sessionId: string,
		repo: string,
	): Promise<{ readonly settled: true } | { readonly settled: false; readonly reason: string }> {
		const last = job?.attempts.at(-1);
		if (!last || last.sessionId !== sessionId || !UNPROVEN_END_STATES.has(last.endState ?? ""))
			return { settled: true };
		let status: string;
		try {
			status = (await this.#port.status({ sessionId, repo, opRef: last.opRef })).status.status;
		} catch (error) {
			// The router answering session_unavailable is positive evidence the host
			// is gone once inspect agrees; a broker outage is neither and holds.
			if (isSessionUnavailable(error)) {
				const liveness = await this.#liveness(sessionId, repo).catch(() => undefined);
				if (liveness && (liveness.live === false || liveness.disowned)) return { settled: true };
			}
			return {
				settled: false,
				reason: `attempt ${last.opRef} status unavailable: ${sanitizeDiagnostic(diagnostic(error))}`,
			};
		}
		if (status === "terminal_ok" || status === "failed") return { settled: true };
		if (status === "unknown") {
			const liveness = await this.#liveness(sessionId, repo);
			if (liveness.live === false || liveness.disowned) return { settled: true };
		}
		return {
			settled: false,
			reason: `attempt ${last.opRef} ended ${last.endState} in the ledger but the broker reports ${status}`,
		};
	}

	async #liveness(sessionId: string, repo: string): Promise<{ live: boolean | undefined; disowned: boolean }> {
		if (!this.#port.liveness) return { live: undefined, disowned: false };
		return await this.#port.liveness({ sessionId, repo });
	}

	/** `undefined` when no job row exists; `"corrupt"` when one exists but cannot be trusted. */
	#job(name: string): LaneJobRecord | "corrupt" | undefined {
		const json = this.#database.laneJobJson(laneJobIdentity(name).jobId);
		// A row that exists with an empty body is damage, not absence.
		if (json === undefined) return undefined;
		try {
			return parseLaneJobRecord(json);
		} catch {
			return "corrupt";
		}
	}
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
	if (value === undefined) return fallback;
	if (!Number.isInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`);
	return value;
}

function unsettled(sessionKey: string, lane: ActiveLane): LaneRetireOutcome {
	return {
		retired: false,
		sessionKey,
		reason:
			lane.state === "corrupt"
				? `lane job record for ${sessionKey} is corrupt; reconcile it before retiring`
				: `attempt still open on ${sessionKey}; let it end first`,
	};
}

function diagnostic(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
