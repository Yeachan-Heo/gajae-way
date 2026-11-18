import { existsSync, lstatSync, realpathSync, rmSync, statfsSync } from "node:fs";
import { lstat, readdir } from "node:fs/promises";
import { basename, dirname, join, resolve, sep } from "node:path";
import type { LogLevel } from "@gajae-gateway/log";
import { type LaneCapacityDetail, ProtocolError } from "@gajae-gateway/protocol";
import { type LaneJobRecord, parseLaneJobRecord } from "@gajae-gateway/subsession";
import {
	DEFAULT_WORK_ATTEMPT_ENDED_RECLAIM_AFTER_MS,
	DEFAULT_WORK_DONE_RECLAIM_GRACE_MS,
	DEFAULT_WORK_IDLE_RETIRE_MS,
	DEFAULT_WORK_LOW_DISK_FREE_BYTES,
	DEFAULT_WORK_LOW_DISK_FREE_RATIO,
	DEFAULT_WORK_MAX_LANES,
} from "../config";
import type { Store } from "../store";
import { runGit } from "./git-command";
import { sanitizeDiagnostic } from "./rebind";
import type { SessionPort } from "./session-port";

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
/** Deadline for the fresh-remote fetch that proves a lane's work landed on its integration base. */
const RECLAIM_REMOTE_FETCH_TIMEOUT_MS = 10_000;

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

export function managedLaneBranchName(workName: string): string {
	return `work/${Buffer.from(workName, "utf8").toString("hex")}`;
}

export function managedLaneRepositoryPath(laneRoot: string, workName: string): string {
	return join(resolve(laneRoot), "repositories", Buffer.from(workName, "utf8").toString("hex"));
}

export function managedLaneWorktreePath(laneRoot: string, workName: string): string {
	return join(resolve(laneRoot), "lanes", Buffer.from(workName, "utf8").toString("hex"));
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
	| {
			readonly retired: true;
			readonly sessionKey: string;
			readonly sessionId: string;
			readonly closed: boolean;
			readonly forced?: boolean;
	  }
	| { readonly retired: false; readonly sessionKey: string; readonly reason: string };

export type SweepReason = "idle" | "job_done" | "job_aborted";

/** What a sweep observed when it nominated a lane; re-proven under the lane lock. */
export interface SweepNomination {
	readonly sessionId: string;
	readonly reason: SweepReason;
	/** The sweep's clock; the locked recheck evaluates fresh state at this time or later. */
	readonly now: number;
}

export type LaneForceRetireReason = "session_dead" | "session_disowned" | "operator_retired" | "operator_abandoned";
type ForceRetireSettlement = (name: string, opRef: string, reason: LaneForceRetireReason, endedAt: string) => boolean;

export interface LaneGovernorOptions {
	readonly database: Store;
	readonly sessionPort: SessionPort;
	/** Configured lane root used to prove managed repository ownership before deletion. */
	readonly laneRoot?: string;
	readonly maxLanes?: number;
	readonly idleRetireMs?: number;
	readonly doneReclaimGraceMs?: number;
	readonly attemptEndedReclaimAfterMs?: number;
	readonly lowDiskFreeBytes?: number;
	readonly lowDiskFreeRatio?: number;
	readonly now?: () => number;
	readonly log?: (line: string, level?: LogLevel) => void;
}

export interface LaneDiskSnapshot {
	readonly path: string;
	/** Allocated bytes below `path/lanes`; null when the walk failed. */
	readonly totalBytes: number | null;
	/** Bytes still eligible for safe reclamation after the latest pass. */
	readonly reclaimableBytes: number | null;
	readonly filesystemFreeBytes: number | null;
	readonly filesystemTotalBytes: number | null;
	readonly dirtyAttemptEnded: number;
	readonly unpushedAttemptEnded: number;
	readonly lowFreeDisk: boolean;
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
	readonly #database: Store;
	readonly #port: SessionPort;
	readonly #laneRoot: string | undefined;
	readonly maxLanes: number;
	readonly idleRetireMs: number;
	readonly #now: () => number;
	readonly #log: (line: string, level?: LogLevel) => void;
	#recoveryGate?: () => Promise<void>;
	#forceRetireSettlement?: ForceRetireSettlement;
	#stopped = false;
	readonly #mutations = new Set<Promise<unknown>>();
	readonly #doneReclaimGraceMs: number;
	readonly #attemptEndedReclaimAfterMs: number;
	readonly #lowDiskFreeBytes: number;
	readonly #lowDiskFreeRatio: number;
	#laneDiskSnapshot: LaneDiskSnapshot | null = null;
	#reclaimPass: Promise<number> | undefined;

	constructor(options: LaneGovernorOptions) {
		this.#database = options.database;
		this.#port = options.sessionPort;
		this.#laneRoot = options.laneRoot === undefined ? undefined : resolve(options.laneRoot);
		this.maxLanes = positiveInteger(options.maxLanes, DEFAULT_WORK_MAX_LANES, "maxLanes");
		this.idleRetireMs = positiveInteger(options.idleRetireMs, DEFAULT_WORK_IDLE_RETIRE_MS, "idleRetireMs");
		this.#doneReclaimGraceMs = positiveInteger(
			options.doneReclaimGraceMs,
			DEFAULT_WORK_DONE_RECLAIM_GRACE_MS,
			"doneReclaimGraceMs",
		);
		this.#attemptEndedReclaimAfterMs = positiveInteger(
			options.attemptEndedReclaimAfterMs,
			DEFAULT_WORK_ATTEMPT_ENDED_RECLAIM_AFTER_MS,
			"attemptEndedReclaimAfterMs",
		);
		this.#lowDiskFreeBytes = positiveInteger(
			options.lowDiskFreeBytes,
			DEFAULT_WORK_LOW_DISK_FREE_BYTES,
			"lowDiskFreeBytes",
		);
		this.#lowDiskFreeRatio = options.lowDiskFreeRatio ?? DEFAULT_WORK_LOW_DISK_FREE_RATIO;
		if (!Number.isFinite(this.#lowDiskFreeRatio) || this.#lowDiskFreeRatio <= 0 || this.#lowDiskFreeRatio > 1)
			throw new RangeError("lowDiskFreeRatio must be greater than 0 and at most 1");
		this.#now = options.now ?? (() => Date.now());
		this.#log = options.log ?? ((line, level) => console[level ?? "info"](line));
	}

	get laneDiskSnapshot(): LaneDiskSnapshot | null {
		return this.#laneDiskSnapshot;
	}

	/** Installed by the sole attempt owner; recovery registration precedes retirement. */
	setRecoveryGate(gate: () => Promise<void>): void {
		this.#recoveryGate = gate;
		this.#stopped = false;
	}

	/** Installed by the attempt owner so forced retirement settles history and runtime atomically. */
	setForceRetireSettlement(settle: ForceRetireSettlement): void {
		this.#forceRetireSettlement = settle;
	}

	async stop(): Promise<void> {
		this.#stopped = true;
		await Promise.allSettled([...this.#mutations]);
	}

	activeLanes(now = this.#now()): ActiveLane[] {
		return this.#database.bindings.workLaneRows().map((row) => {
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
	 * Quarantined lanes do not count against maxLanes.
	 */
	assertAdmission(name: string): void {
		const { jobId } = laneJobIdentity(name);
		if (this.#database.bindings.isQuarantined("work", jobId))
			throw new ProtocolError("verb_failed", "work lane belongs to a quarantined broker authority", {
				reasonCode: "broker_authority_quarantined",
				jobId,
				name,
			});
		const allLanes = this.activeLanes();
		// Filter out quarantined lanes from capacity count
		const lanes = allLanes.filter(
			(lane) => !this.#database.bindings.isQuarantined("work", laneJobIdentity(lane.name).jobId),
		);
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
		if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
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
		if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({ retired: false, sessionKey, reason: "broker_authority_quarantined" });
		// Refuse an unsettled lane now rather than queuing retirement behind its
		// turn; the same check repeats under the lock because a run may start
		// before we acquire it.
		const preflight = this.activeLanes().find((candidate) => candidate.name === name);
		if (preflight?.attemptOpen) return Promise.resolve(unsettled(sessionKey, preflight));
		return this.#port.runExclusive(sessionKey, async () => {
			if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
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
			// Try job record repo first, then owned binding repo, then process.cwd() fallback.
			// In broker mode: if there's no job record and no owned binding, the lane is an orphan.
			// In singleton mode: process.cwd() is safe (no broker enforcing repo matching).
			let repo = record?.lane.worktreePath;
			if (!repo) {
				repo = this.#database.bindings.workLaneRepoBySessionId(lane.sessionId);
			}
			if (!repo) {
				// No job record and no owned binding repo.
				// Check if we're in broker mode: if so, this is an orphan.
				// If not, fall back to process.cwd() (safe in singleton mode).
				if (this.#database.bindings.isBrokerMode()) {
					// Broker mode: cannot close safely without knowing the repo.
					this.#log(`lane_retire_failed name=${name} session=${lane.sessionId} reason=orphan_no_repo_found`);
					return { retired: false, sessionKey, reason: "orphan lane: no job record and no owned binding repo" };
				}
				// Singleton mode: safe to use process.cwd().
				repo = process.cwd();
			}
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
				const liveness = await this.#safeLiveness(lane.sessionId, repo);
				if (liveness.live !== false && !liveness.disowned) {
					this.#log(
						`lane_close_failed name=${name} session=${lane.sessionId} detail=${detail} action=retained`,
						"error",
					);
					return {
						retired: false,
						sessionKey,
						reason: `session.close failed and the session is not proven gone: ${detail}`,
					};
				}
				this.#log(
					`lane_close_failed name=${name} session=${lane.sessionId} detail=${detail} action=session_gone`,
					"error",
				);
			}
			// A settled attempt can still own an open checks cycle. Left open, the checks
			// poller keeps re-steering red checks into a fresh session after retirement.
			// Settle it only once the session is closed or proven gone: a retained lane
			// must keep its checks monitoring.
			const lastAttempt = record?.attempts.at(-1);
			if (record?.state === "awaiting_checks" && lastAttempt) {
				const endedAt = new Date(this.#now()).toISOString();
				if (!this.#forceRetireSettlement?.(name, lastAttempt.opRef, "operator_retired", endedAt)) {
					const detail = "awaiting_checks cycle could not be settled";
					this.#log(`lane_retire_failed name=${name} opRef=${lastAttempt.opRef} reason=${detail}`);
					return { retired: false, sessionKey, reason: detail };
				}
			}
			// Only a non-forced operator retirement (no sweep nomination) deletes the
			// runtime-created worktree and branch. Unattended sweep/force retirement keeps
			// the durable lane path and its unpushed commits resumable.
			const repositoryPath = record?.lane.repositoryPath ?? record?.lane.repository;
			if (!expected && record?.lane.worktreePath && repositoryPath) {
				try {
					await this.#cleanupWorktree(
						name,
						lane.sessionId,
						record.lane.worktreePath,
						repositoryPath,
						record.lane.repositoryPath !== undefined,
					);
				} catch (error) {
					const detail = sanitizeDiagnostic(error instanceof Error ? error.message : String(error));
					this.#log(`lane_worktree_cleanup_failed name=${name} session=${lane.sessionId} reason=${detail}`, "error");
					return { retired: false, sessionKey, reason: `worktree cleanup failed: ${detail}` };
				}
			}

			this.#database.transaction((tx) => this.#database.bindings.rotateEpoch(tx, sessionKey));
			this.#log(`lane_retired name=${name} session=${lane.sessionId} reason=${reason} closed=${closed}`, "info");
			return { retired: true, sessionKey, sessionId: lane.sessionId, closed };
		});
	}

	/**
	 * Retires lanes whose job is terminal or that have been quiet past the idle
	 * ceiling. The snapshot only nominates; `retire` re-proves the binding
	 * identity and eligibility under the lane lock before closing anything.
	 * Per-lane errors are caught and logged; one lane's failure does not abort the sweep.
	 * Dead/disowned lane reconciliation is handled separately via retireAllDead().
	 */
	async sweep(now = this.#now()): Promise<number> {
		if (this.#stopped) return 0;
		let retired = 0;
		for (const lane of this.activeLanes(now)) {
			const reason = this.#sweepReason(lane);
			if (!reason) continue;
			try {
				const outcome = await this.retire(lane.name, reason, { sessionId: lane.sessionId, reason, now });
				if (outcome.retired) retired++;
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_retire_failed name=${lane.name} session=${lane.sessionId} reason=${detail}`);
			}
		}
		return retired;
	}

	/** Removes only stale, terminal, gateway-managed worktrees under the existing maintenance tick. */
	reclaimTerminalWorktrees(now = this.#now()): Promise<number> {
		if (this.#stopped || this.#laneRoot === undefined) return Promise.resolve(0);
		if (this.#reclaimPass) return this.#reclaimPass;
		const task = this.#reclaimTerminalWorktrees(now);
		this.#reclaimPass = task;
		this.#mutations.add(task);
		void task.then(
			() => {
				this.#mutations.delete(task);
				if (this.#reclaimPass === task) this.#reclaimPass = undefined;
			},
			() => {
				this.#mutations.delete(task);
				if (this.#reclaimPass === task) this.#reclaimPass = undefined;
			},
		);
		return task;
	}

	async #reclaimTerminalWorktrees(now: number): Promise<number> {
		const laneRoot = this.#laneRoot;
		if (!laneRoot || this.#stopped) return 0;
		const lanesPath = join(laneRoot, "lanes");
		const eligiblePaths: string[] = [];
		let dirtyAttemptEnded = 0;
		let unpushedAttemptEnded = 0;
		let reclaimed = 0;
		for (const row of this.#database.work.laneJobRows()) {
			if (!row.lane_key.startsWith("work-")) continue;
			const name = row.lane_key.slice("work-".length);
			if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(name)) continue;
			if (this.#stopped) break;
			try {
				const outcome = await this.#port.runExclusive(workSessionKey(name), async () => {
					if (this.#stopped || this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					const record = this.#job(name);
					if (!record || record === "corrupt" || record.reclaimedAt !== undefined)
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					const graceMs =
						record.state === "done"
							? this.#doneReclaimGraceMs
							: record.state === "attempt_ended"
								? this.#attemptEndedReclaimAfterMs
								: undefined;
					if (graceMs === undefined || now - Date.parse(record.updatedAt) < graceMs)
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					if (record.attempts.some((attempt) => attempt.endedAt === undefined)) {
						this.#log(`lane_reclaim_skipped name=${name} reason=attempt_open`, "warn");
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					}
					if (this.activeLanes().some((lane) => lane.name === name)) {
						this.#log(`lane_reclaim_skipped name=${name} reason=live`, "info");
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					}
					const repositoryPath = record.lane.repositoryPath;
					const worktreePath = record.lane.worktreePath;
					if (!repositoryPath || !this.#managedLanePaths(name, record)) {
						this.#log(`lane_reclaim_skipped name=${name} reason=unmanaged_path`, "warn");
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					}
					try {
						assertManagedRepositoryPath(laneRoot, name, repositoryPath);
						assertManagedWorktreeLocation(laneRoot, name, worktreePath);
					} catch (error) {
						this.#log(
							`lane_reclaim_skipped name=${name} reason=path_validation detail=${sanitizeDiagnostic(diagnostic(error))}`,
							"warn",
						);
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					}
					if (!existsSync(worktreePath)) {
						try {
							let stillRegistered = !(await worktreeIsUnregistered(repositoryPath, worktreePath));
							// If stale registration remains after external deletion (interrupted git worktree remove),
							// prune it and verify it was unregistered.
							if (stillRegistered) {
								const pruneResult = await runGit(["worktree", "prune"], { cwd: repositoryPath });
								if (pruneResult.exitCode !== 0) {
									this.#log(
										`lane_reclaim_failed name=${name} reason=git_worktree_prune_failed detail=${sanitizeDiagnostic(pruneResult.stderr.trim() || "unknown error")}`,
										"error",
									);
									return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
								}
								stillRegistered = !(await worktreeIsUnregistered(repositoryPath, worktreePath));
								if (stillRegistered) {
									this.#log(
										`lane_reclaim_failed name=${name} reason=missing_worktree_still_registered_after_prune`,
										"error",
									);
									return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
								}
							}
						} catch (error) {
							this.#log(
								`lane_reclaim_failed name=${name} reason=git_worktree_verify_failed detail=${sanitizeDiagnostic(diagnostic(error))}`,
								"error",
							);
							return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
						}
						if (!this.#markReclaimed(name, record, now)) {
							this.#log(`lane_reclaim_failed name=${name} reason=job_changed_after_remove`, "error");
							return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
						}
						this.#log(
							`lane_worktree_reclaimed name=${name} state=${record.state} reclaimed_at=${new Date(now).toISOString()}`,
							"info",
						);
						return { reclaimed: true, eligible: true, bytes: 0, dirty: false, unpushed: false };
					}
					if (record.state === "attempt_ended") {
						const head = await runGit(["rev-parse", "--verify", "HEAD^{commit}"], { cwd: worktreePath });
						if (head.exitCode !== 0 || !/^[0-9a-f]{40}\s*$/.test(head.stdout)) {
							this.#log(`lane_reclaim_skipped name=${name} reason=git_head_failed`, "warn");
							return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
						}
						// A lane is reclaimable-as-merged only when its work provably landed on
						// the integration base (issuecomment-6960912321): see #laneMergedIntoBase.
						const merged = await this.#laneMergedIntoBase(name, head.stdout.trim(), repositoryPath);
						if (!merged.ok) {
							this.#log(`lane_reclaim_skipped name=${name} reason=${merged.reason}`, "warn");
							return {
								reclaimed: false,
								eligible: false,
								bytes: 0,
								dirty: false,
								unpushed: merged.reason === "unpushed",
							};
						}
						const status = await runGit(["status", "--porcelain", "--untracked-files=all"], {
							cwd: worktreePath,
						});
						if (status.exitCode !== 0) {
							this.#log(`lane_reclaim_skipped name=${name} reason=git_status_failed`, "warn");
							return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
						}
						if (status.stdout.length > 0) {
							this.#log(`lane_reclaim_skipped name=${name} reason=dirty`, "warn");
							return { reclaimed: false, eligible: false, bytes: 0, dirty: true, unpushed: false };
						}
					}
					if (existsSync(worktreePath)) {
						const removed = await runGit(["worktree", "remove", "--", worktreePath], { cwd: repositoryPath });
						if (removed.exitCode !== 0) {
							this.#log(`lane_reclaim_failed name=${name} reason=git_worktree_remove_failed`, "error");
							return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
						}
					}
					try {
						if (existsSync(worktreePath) || !(await worktreeIsUnregistered(repositoryPath, worktreePath))) {
							this.#log(`lane_reclaim_failed name=${name} reason=worktree_remains`, "error");
							return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
						}
					} catch (error) {
						this.#log(
							`lane_reclaim_failed name=${name} reason=git_worktree_verify_failed detail=${sanitizeDiagnostic(diagnostic(error))}`,
							"error",
						);
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					}
					if (!this.#markReclaimed(name, record, now)) {
						this.#log(`lane_reclaim_failed name=${name} reason=job_changed_after_remove`, "error");
						return { reclaimed: false, eligible: false, bytes: 0, dirty: false, unpushed: false };
					}
					const at = new Date(now).toISOString();
					this.#log(`lane_worktree_reclaimed name=${name} state=${record.state} reclaimed_at=${at}`, "info");
					return { reclaimed: true, eligible: true, bytes: 0, dirty: false, unpushed: false };
				});
				if (outcome.eligible) eligiblePaths.push(managedLaneWorktreePath(laneRoot, name));
				if (outcome.reclaimed) {
					reclaimed++;
				}
				if (outcome.dirty) dirtyAttemptEnded++;
				if (outcome.unpushed) unpushedAttemptEnded++;
			} catch (error) {
				this.#log(`lane_reclaim_failed name=${name} reason=${sanitizeDiagnostic(diagnostic(error))}`, "error");
			}
		}
		let tree: LaneTreeBytes | undefined;
		try {
			tree = await scanLaneTree(lanesPath);
		} catch (error) {
			this.#log(`lane_disk_scan_failed path=${lanesPath} reason=${sanitizeDiagnostic(diagnostic(error))}`, "warn");
		}
		let filesystemFreeBytes: number | null = null;
		let filesystemTotalBytes: number | null = null;
		try {
			const stats = statfsSync(laneRoot);
			filesystemFreeBytes = stats.bavail * stats.bsize;
			filesystemTotalBytes = stats.blocks * stats.bsize;
		} catch {
			// Missing headroom evidence is exposed as low, never guessed healthy.
		}
		const lowFreeDisk =
			filesystemFreeBytes === null ||
			filesystemTotalBytes === null ||
			filesystemFreeBytes < this.#lowDiskFreeBytes ||
			filesystemFreeBytes < filesystemTotalBytes * this.#lowDiskFreeRatio;
		this.#laneDiskSnapshot = {
			path: lanesPath,
			totalBytes: tree?.totalBytes ?? null,
			reclaimableBytes:
				tree === undefined
					? null
					: eligiblePaths.reduce((sum, path) => sum + (tree.worktrees.get(resolve(path)) ?? 0), 0),
			filesystemFreeBytes,
			filesystemTotalBytes,
			dirtyAttemptEnded,
			unpushedAttemptEnded,
			lowFreeDisk,
		};
		return reclaimed;
	}

	#managedLanePaths(name: string, record: LaneJobRecord): boolean {
		return (
			this.#laneRoot !== undefined &&
			resolve(record.lane.worktreePath) === managedLaneWorktreePath(this.#laneRoot, name) &&
			record.lane.repositoryPath !== undefined &&
			resolve(record.lane.repositoryPath) === managedLaneRepositoryPath(this.#laneRoot, name)
		);
	}

	#markReclaimed(name: string, expected: LaneJobRecord, now: number): boolean {
		const current = this.#job(name);
		if (
			!current ||
			current === "corrupt" ||
			current.reclaimedAt !== undefined ||
			current.state !== expected.state ||
			current.updatedAt !== expected.updatedAt ||
			current.attempts.some((attempt) => attempt.endedAt === undefined) ||
			this.activeLanes().some((lane) => lane.name === name)
		)
			return false;
		const at = new Date(now).toISOString();
		const reclaimedRecord: LaneJobRecord = { ...current, updatedAt: at, reclaimedAt: at };
		const { laneKey } = laneJobIdentity(name);
		this.#database.transaction((tx) =>
			this.#database.work.putLaneJob(tx, {
				jobId: reclaimedRecord.jobId,
				laneKey,
				state: reclaimedRecord.state,
				createdAt: reclaimedRecord.createdAt,
				updatedAt: reclaimedRecord.updatedAt,
				lane: reclaimedRecord.lane,
				json: JSON.stringify(reclaimedRecord),
			}),
		);
		return true;
	}

	/**
	 * Merged signal for reclaiming an expired `attempt_ended` lane (issue #833,
	 * issuecomment-6960912321/6966669378/6967618377).
	 *
	 * The remote's integration-branch tip is fetched into a scratch namespace
	 * (`+<base-ref>:refs/remotes/origin-fresh/HEAD`), so the branch name is never
	 * hardcoded — gajae-way lands on `dev`, others on `main`) and the lane counts
	 * as merged only when one of these holds, checked in order:
	 *
	 * 1. the lane head is an ancestor of the base tip (regular merge), or
	 * 2. `git cherry` lists every lane commit `-` against the base (each commit's
	 *    patch-id exists on the base side; rebase/cherry-pick merges), or
	 * 3. the combined `git diff merge-base..lane-head` patch-id equals the
	 *    patch-id of a single commit introduced on base since the merge-base
	 *    (a squash of several lane commits).
	 *
	 * Anything else — including a clean push to the lane's own remote branch —
	 * fails closed: the worktree keeps its commits and stays resumable. Every
	 * git failure also fails closed with a distinct logged reason.
	 */
	async #laneMergedIntoBase(
		name: string,
		headSha: string,
		repositoryPath: string,
	): Promise<{ readonly ok: true } | { readonly ok: false; readonly reason: string }> {
		const origin = await runGit(["config", "remote.origin.url"], { cwd: repositoryPath });
		if (origin.exitCode !== 0 || !origin.stdout.trim()) return { ok: false, reason: "remote_url_missing" };
		// The integration base is the remote's default branch, never a hardcoded
		// name: prefer the upstream's symbolic HEAD, else `origin/HEAD` as recorded
		// by the last full clone/fetch. `HEAD` is unresolvable when the upstream
		// never set a default-branch symbol (a bare `git init` points `master` at
		// nothing), so only fetch a ref that exists.
		const baseTip = await runGit(["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"], { cwd: repositoryPath });
		const originHead = baseTip.exitCode === 0 ? baseTip.stdout.trim() : "";
		let fetchRef = originHead.startsWith("refs/remotes/origin/") ? originHead.slice("refs/remotes/".length) : "HEAD";
		// Refresh only that base tip into a scratch namespace; a packed ref survives
		// this no-op fetch, so the base is always resolved with
		// `rev-parse --verify --quiet` below, never via ref files on disk.
		if (fetchRef === "HEAD") {
			// The upstream has no default-branch symbol: fall back to the last branch
			// this lane repository fetched (`origin/HEAD` is equally absent here).
			const heads = await runGit(["for-each-ref", "--format=%(refname)", "--count=1", "refs/remotes/origin/"], {
				cwd: repositoryPath,
			});
			const first = heads.stdout.trim();
			if (heads.exitCode === 0 && first.startsWith("refs/remotes/origin/"))
				fetchRef = `refs/heads/${first.slice("refs/remotes/origin/".length)}`;
		}
		const fetch = await runGit(["fetch", "origin", `+${fetchRef}:refs/remotes/origin-fresh/HEAD`], {
			cwd: repositoryPath,
			timeoutMs: RECLAIM_REMOTE_FETCH_TIMEOUT_MS,
		});
		if (fetch.exitCode !== 0) return { ok: false, reason: "remote_unreachable" };
		const base = await runGit(["rev-parse", "--verify", "--quiet", "refs/remotes/origin-fresh/HEAD^{commit}"], {
			cwd: repositoryPath,
		});
		const baseSha = base.stdout.trim();
		if (base.exitCode !== 0 || !/^[0-9a-f]{40}$/.test(baseSha)) return { ok: false, reason: "base_ref_unresolved" };
		if (baseSha === headSha) return { ok: true };
		const ancestor = await runGit(["merge-base", "--is-ancestor", headSha, baseSha], { cwd: repositoryPath });
		if (ancestor.exitCode === 0) return { ok: true };
		const mergeBase = await runGit(["merge-base", headSha, baseSha], { cwd: repositoryPath });
		const mergeBaseSha = mergeBase.stdout.trim();
		if (mergeBase.exitCode !== 0 || !/^[0-9a-f]{40}$/.test(mergeBaseSha))
			return { ok: false, reason: "head_not_in_base_history" };
		// Patch-id equivalence per lane commit: every `-` line means that commit's
		// changes already exist on the base side of the merge-base.
		const cherry = await runGit(["cherry", mergeBaseSha, headSha], { cwd: repositoryPath });
		if (cherry.exitCode !== 0) return { ok: false, reason: "git_cherry_failed" };
		const laneCommits = cherry.stdout
			.split("\n")
			.map((line) => line.trim())
			.filter((line) => line.length > 0);
		if (laneCommits.length > 0 && laneCommits.every((line) => line.startsWith("-"))) return { ok: true };
		// Squash of several lane commits: the combined diff since the merge-base
		// must equal the patch-id of a single commit introduced on base.
		const laneDiff = await runGit(["diff", `${mergeBaseSha}..${headSha}`], { cwd: repositoryPath });
		if (laneDiff.exitCode !== 0) return { ok: false, reason: "lane_diff_failed" };
		const lanePatch = await runGit(["patch-id", "--stable"], { stdin: laneDiff.stdout, cwd: repositoryPath });
		if (lanePatch.exitCode !== 0 || lanePatch.stdout.trim().length === 0)
			return { ok: false, reason: "git_patch_id_failed" };
		const lanePatchId = lanePatch.stdout.trim().split(/\s+/)[0];
		if (!lanePatchId) return { ok: false, reason: "git_patch_id_failed" };
		const baseLog = await runGit(["log", "--no-merges", "--format=medium", "-p", `${mergeBaseSha}..${baseSha}`], {
			cwd: repositoryPath,
		});
		if (baseLog.exitCode !== 0) return { ok: false, reason: "git_patch_id_failed" };
		const basePatchIds = await runGit(["patch-id", "--stable"], { stdin: baseLog.stdout, cwd: repositoryPath });
		if (basePatchIds.exitCode !== 0) return { ok: false, reason: "git_patch_id_failed" };
		const known = patchIdsFromGitLog(basePatchIds.stdout);
		if (known.has(lanePatchId)) return { ok: true };
		this.#log(
			`lane_reclaim_merged_check name=${name} lane_commits=${laneCommits.length} lane_patch_id=${lanePatchId.slice(0, 12)}`,
			"info",
		);
		return { ok: false, reason: "work_not_merged_into_base" };
	}

	/**
	 * Force-retire a lane whose session is provably dead or disowned by the broker.
	 * Unlike normal retire, this skips attempt-state checks and requires liveness
	 * proof that the session is gone. If successful, closes as host_lost and rebinds
	 * the epoch with reason operator_force.
	 */
	forceRetire(name: string): Promise<LaneRetireOutcome> {
		if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({
				retired: false,
				sessionKey: workSessionKey(name),
				reason: "broker_authority_quarantined",
			});
		if (this.#stopped)
			return Promise.resolve({ retired: false, sessionKey: workSessionKey(name), reason: "gateway is stopping" });
		const task = (async () => {
			try {
				await this.#recoveryGate?.();
				if (this.#stopped)
					return { retired: false as const, sessionKey: workSessionKey(name), reason: "gateway is stopping" };
				return await this.#forceRetire(name);
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_force_retire_failed name=${name} reason=${detail}`);
				return { retired: false as const, sessionKey: workSessionKey(name), reason: detail };
			}
		})();
		this.#mutations.add(task);
		void task.then(
			() => this.#mutations.delete(task),
			() => this.#mutations.delete(task),
		);
		return task;
	}

	#forceRetire(name: string): Promise<LaneRetireOutcome> {
		const sessionKey = workSessionKey(name);
		if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
			return Promise.resolve({ retired: false, sessionKey, reason: "broker_authority_quarantined" });
		// Unlike normal retire, we check the lane exists but do NOT check if an attempt is open.
		// We will only close if liveness proves the session is dead/disowned.
		const preflight = this.activeLanes().find((candidate) => candidate.name === name);
		if (!preflight) return Promise.resolve({ retired: false, sessionKey, reason: "no bound lane for that name" });
		return this.#port.runExclusive(sessionKey, async () => {
			if (this.#database.bindings.isQuarantined("work", laneJobIdentity(name).jobId))
				return { retired: false, sessionKey, reason: "broker_authority_quarantined" };
			if (this.#stopped) return { retired: false, sessionKey, reason: "gateway is stopping" };
			const lane = this.activeLanes().find((candidate) => candidate.name === name);
			if (!lane) return { retired: false, sessionKey, reason: "no bound lane for that name" };
			const job = this.#job(name);
			const record = job === "corrupt" ? undefined : job;
			let repo = record?.lane.worktreePath;
			if (!repo) {
				repo = this.#database.bindings.workLaneRepoBySessionId(lane.sessionId);
			}
			if (!repo) {
				if (this.#database.bindings.isBrokerMode()) {
					this.#log(`lane_retire_failed name=${name} session=${lane.sessionId} reason=orphan_no_repo_found`);
					return { retired: false, sessionKey, reason: "orphan lane: no job record and no owned binding repo" };
				}
				repo = process.cwd();
			}
			// For force retire, we must have broker liveness proof that the session is dead or disowned,
			// OR the session must be live but idle with an open attempt (operator abandonment).
			const liveness = await this.#safeLiveness(lane.sessionId, repo);
			const isIdleEnough = lane.idleMs >= this.idleRetireMs;
			const isOpenAttempt = lane.attemptOpen;

			// For abandonment, we need broker evidence:
			// 1. Session activity state is idle
			// 2. Op status from broker is unknown with receiptState unknown (not in_flight, not accepted)
			// 3. No background jobs (async tasks, bash) still running in the session host
			const lastAttempt = record?.attempts.at(-1);
			let canRetireAsAbandonment = false;
			let brokerActivityIdle = false;
			let opStatusUnknown = false;
			let runningJobsEmpty = true;
			if (liveness.live === true && !liveness.disowned && isIdleEnough && isOpenAttempt && lastAttempt) {
				// Check broker activity state
				try {
					const brokerSession = await this.#port.inspect({ sessionId: lane.sessionId, repo });
					if (brokerSession) {
						brokerActivityIdle = brokerSession.activityState === "idle";
					}
				} catch (error) {
					const detail = sanitizeDiagnostic(diagnostic(error));
					this.#log(`lane_broker_inspect_failed name=${name} session=${lane.sessionId} reason=${detail}`);
					// Conservative: cannot confirm idle, refuse abandonment
					brokerActivityIdle = false;
				}

				// Check op status from broker
				try {
					const status = await this.#port.status({
						sessionId: lane.sessionId,
						repo,
						opRef: lastAttempt.opRef,
					});
					const promptStatus = status.status.status;
					const receiptState = status.status.receiptState;
					// Unknown status with unknown receipt = safe to abandon
					opStatusUnknown = promptStatus === "unknown" && (receiptState === undefined || receiptState === "unknown");
				} catch (error) {
					const detail = sanitizeDiagnostic(diagnostic(error));
					this.#log(`lane_status_check_failed name=${name} opRef=${lastAttempt.opRef} reason=${detail}`);
					// Conservative: cannot confirm status unknown, refuse abandonment
					opStatusUnknown = false;
				}

				// Check for running background jobs (async tasks, bash) that would be terminated by close
				if (brokerActivityIdle && opStatusUnknown && this.#port.runningJobs) {
					try {
						const jobs = await this.#port.runningJobs({ sessionId: lane.sessionId, repo });
						runningJobsEmpty = jobs.length === 0;
						if (!runningJobsEmpty) {
							this.#log(
								`lane_abandonment_refused name=${name} session=${lane.sessionId} reason=running_jobs count=${jobs.length}`,
							);
						}
					} catch (error) {
						const detail = sanitizeDiagnostic(diagnostic(error));
						this.#log(`lane_running_jobs_check_failed name=${name} session=${lane.sessionId} reason=${detail}`);
						// Conservative: cannot confirm jobs are empty, refuse abandonment
						runningJobsEmpty = false;
					}
				}

				// Allow abandonment only if all broker conditions and job status are met
				canRetireAsAbandonment = brokerActivityIdle && opStatusUnknown && runningJobsEmpty;
			}

			// Allow retiring if:
			// 1. Session is dead/disowned (normal case), OR
			// 2. Session is live but idle with broker confirmation (operator abandonment), OR
			// 3. Session is live but idle with an attempt already settled as operator_abandoned (may be retrying close after transient failure)
			//    But only if there are no running jobs to protect independent background work.
			const sessionIsDeadOrDisowned = liveness.live === false || liveness.disowned;
			// For retry-after-transient-close-failure, only allow if the attempt was settled as operator_abandoned,
			// not for other end reasons like session_dead or session_disowned from a reaped wait or other failure.
			// Do not require isIdleEnough for retry: settlement's activity update has made the lane appear newly active.
			// Instead, revalidate current liveness and running jobs; the abandonment intent is persisted in the terminal reason.
			let isAlreadySettledAbandon = false;
			let retryRunningJobsEmpty = true;
			if (lastAttempt && lastAttempt.endedAt && liveness.live === true && !liveness.disowned) {
				// Check if the attempt was settled with operator_abandoned reason
				const runtime = this.#database.work.workAttemptGet(lastAttempt.opRef);
				if (runtime && runtime.terminal?.reasonCode === "operator_abandoned") {
					// Before allowing retry, verify no new running jobs have started since settlement
					if (this.#port.runningJobs) {
						try {
							const jobs = await this.#port.runningJobs({ sessionId: lane.sessionId, repo });
							retryRunningJobsEmpty = jobs.length === 0;
							if (!retryRunningJobsEmpty) {
								this.#log(
									`lane_retry_abandon_refused name=${name} session=${lane.sessionId} reason=running_jobs count=${jobs.length}`,
								);
							}
						} catch (error) {
							const detail = sanitizeDiagnostic(diagnostic(error));
							this.#log(`lane_retry_running_jobs_check_failed name=${name} session=${lane.sessionId} reason=${detail}`);
							// Conservative: cannot confirm jobs are empty, refuse retry
							retryRunningJobsEmpty = false;
						}
					}
					isAlreadySettledAbandon = retryRunningJobsEmpty;
				}
			}

			if (!sessionIsDeadOrDisowned && !canRetireAsAbandonment && !isAlreadySettledAbandon) {
				this.#log(`lane_force_retire_rejected name=${name} session=${lane.sessionId} reason=session_still_live`);
				return {
					retired: false,
					sessionKey,
					reason: "session is still live; cannot force retire",
				};
			}

			// Session is confirmed dead, disowned, or idle+open (operator abandonment).
			// The attempt owner closes the history and runtime row together before we retire the binding.
			// A completed attempt may also own an unresolved checks cycle that must end
			// when its lane is force-retired.
			if (lastAttempt && (!lastAttempt.endedAt || record?.state === "awaiting_checks")) {
				let settleReason: LaneForceRetireReason;
				if (canRetireAsAbandonment) {
					// Operator force-retired an idle open attempt (broker status unknown)
					settleReason = "operator_abandoned";
				} else if (liveness.disowned) {
					settleReason = "session_disowned";
				} else {
					settleReason = "session_dead";
				}
				const endedAt = new Date(this.#now()).toISOString();
				if (!this.#forceRetireSettlement?.(name, lastAttempt.opRef, settleReason, endedAt)) {
					const detail = "work attempt could not be settled";
					this.#log(`lane_force_retire_failed name=${name} opRef=${lastAttempt.opRef} reason=${detail}`);
					return { retired: false, sessionKey, reason: detail };
				}
			}
			// Forced retirement is unattended; preserve the durable worktree and
			// branch so its commits and the stored worktree path remain resumable.
			// Close the session and rebind the epoch.
			let closed = false;
			try {
				await this.#port.close({ sessionId: lane.sessionId, repo });
				closed = true;
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_close_failed name=${name} session=${lane.sessionId} detail=${detail} action=force_close`);
				// On close failure, do a fresh liveness check before proceeding with rotateEpoch.
				// Only proceed if we can confirm the host is dead/disowned; otherwise preserve the binding.
				const closeFailureLiveness = await this.#safeLiveness(lane.sessionId, repo);
				if (closeFailureLiveness.live !== false && !closeFailureLiveness.disowned) {
					// Host may still be live or liveness is unknown; must not rotate epoch without confirmation it is dead/disowned.
					return { retired: false, sessionKey, reason: `session close failed: ${detail}` };
				}
				// For dead/disowned sessions confirmed by fresh liveness check, proceed with rotation.
			}
			this.#database.transaction((tx) => this.#database.bindings.rotateEpoch(tx, sessionKey));
			const logReason = canRetireAsAbandonment ? "operator_abandoned" : "operator_force";
			this.#log(`lane_retired name=${name} session=${lane.sessionId} reason=${logReason} closed=${closed}`);
			return { retired: true, sessionKey, sessionId: lane.sessionId, closed, forced: true };
		});
	}

	/**
	 * Retire all lanes whose sessions are provably dead or disowned.
	 * Returns count and names of lanes successfully retired.
	 */
	async retireAllDead(now = this.#now()): Promise<{ count: number; names: string[] }> {
		if (this.#stopped) return { count: 0, names: [] };
		const retired: string[] = [];
		for (const lane of this.activeLanes(now)) {
			if (this.#database.bindings.isQuarantined("work", laneJobIdentity(lane.name).jobId)) continue;
			const job = this.#job(lane.name);
			const record = job === "corrupt" ? undefined : job;
			let repo = record?.lane.worktreePath;
			if (!repo) {
				repo = this.#database.bindings.workLaneRepoBySessionId(lane.sessionId);
			}
			if (!repo) {
				if (this.#database.bindings.isBrokerMode()) {
					continue; // Skip orphan lanes
				}
				repo = process.cwd();
			}
			const liveness = await this.#safeLiveness(lane.sessionId, repo);
			if (liveness.live !== false && !liveness.disowned) continue; // Session is still live
			try {
				const outcome = await this.forceRetire(lane.name);
				if (outcome.retired) retired.push(lane.name);
			} catch (error) {
				const detail = sanitizeDiagnostic(diagnostic(error));
				this.#log(`lane_retire_failed name=${lane.name} session=${lane.sessionId} reason=${detail}`);
			}
		}
		return { count: retired.length, names: retired };
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
	 * Status errors (e.g. session_unavailable) are also treated as proof the
	 * session is gone: consult liveness and settle if dead/disowned.
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
		let statusError: string | undefined;
		try {
			status = (await this.#port.status({ sessionId, repo, opRef: last.opRef })).status.status;
		} catch (error) {
			// Status unavailable (e.g., session_unavailable) means the broker says
			// the session is gone. Treat it the same as unknown: check liveness.
			statusError = sanitizeDiagnostic(diagnostic(error));
			status = "unknown";
		}
		if (status === "terminal_ok" || status === "failed") return { settled: true };
		if (status === "unknown") {
			const liveness = await this.#liveness(sessionId, repo);
			if (liveness.live === false || liveness.disowned) return { settled: true };
		}
		const detail = statusError ? `status error: ${statusError}` : status;
		return {
			settled: false,
			reason: `attempt ${last.opRef} ended ${last.endState} in the ledger but the broker reports ${detail}`,
		};
	}

	async #liveness(sessionId: string, repo: string): Promise<{ live: boolean | undefined; disowned: boolean }> {
		if (!this.#port.liveness) return { live: undefined, disowned: false };
		return await this.#port.liveness({ sessionId, repo });
	}

	/**
	 * Safe liveness probe: catches errors and logs them instead of throwing.
	 * Errors are conservative: assume session is still alive (live: undefined).
	 */
	async #safeLiveness(sessionId: string, repo: string): Promise<{ live: boolean | undefined; disowned: boolean }> {
		try {
			return await this.#liveness(sessionId, repo);
		} catch (error) {
			const detail = sanitizeDiagnostic(diagnostic(error));
			this.#log(`liveness_probe_failed session=${sessionId} repo=${repo} reason=${detail}`);
			// Conservative: assume session still alive if we can't probe.
			return { live: undefined, disowned: false };
		}
	}

	/** Cleans up a runtime-created worktree and its branch. Throws on error (issue #480). */
	async #cleanupWorktree(
		name: string,
		sessionId: string,
		worktreePath: string,
		repository: string,
		removeRepository: boolean,
	): Promise<void> {
		if (removeRepository) assertManagedRepositoryPath(this.#laneRoot, name, repository);

		// A previous cleanup can have removed the managed bare repository before
		// the binding epoch was rotated. Treat that state as already cleaned up.
		if (removeRepository && !existsSync(repository)) {
			if (existsSync(worktreePath)) throw new Error("managed repository is missing while the worktree still exists");
			return;
		}
		// Remove the worktree using git worktree remove --force
		const removeResult = await runGit(["worktree", "remove", "--force", worktreePath], { cwd: repository });

		if (removeResult.exitCode !== 0) {
			const stderr = removeResult.stderr.trim() || "unknown error";
			if (existsSync(worktreePath)) throw new Error(`git worktree remove failed: ${stderr}`);

			// External deletion leaves a stale administrative worktree record. Prune
			// only after confirming the path is absent, then verify it is unregistered.
			const pruneResult = await runGit(["worktree", "prune"], { cwd: repository });
			if (pruneResult.exitCode !== 0) {
				const pruneError = pruneResult.stderr.trim() || "unknown error";
				throw new Error(`git worktree prune failed: ${pruneError}`);
			}
			const listResult = await runGit(["worktree", "list", "--porcelain"], { cwd: repository });
			if (listResult.exitCode !== 0) {
				const listError = listResult.stderr.trim() || "unknown error";
				throw new Error(`git worktree list failed: ${listError}`);
			}
			const canonicalWorktreePath = canonicalPathIncludingMissing(worktreePath);
			const stillRegistered = listResult.stdout.split(/\r?\n/).some((line) => {
				if (!line.startsWith("worktree ")) return false;
				try {
					return canonicalPathIncludingMissing(line.slice("worktree ".length)) === canonicalWorktreePath;
				} catch {
					// If a listed path cannot be canonicalized, do not delete the repository.
					return true;
				}
			});
			if (stillRegistered) throw new Error(`worktree remains registered after pruning: ${stderr}`);
		}

		// Delete the branch using git branch -D
		const branchName = managedLaneBranchName(name);
		// Branch deletion failure (including its deadline) is non-fatal; only log it
		const branchResult = await runGit(["branch", "-D", branchName], { cwd: repository }).catch((error: unknown) => ({
			exitCode: -1,
			stdout: "",
			stderr: error instanceof Error ? error.message : String(error),
		}));
		if (branchResult.exitCode !== 0) {
			const stderr = branchResult.stderr.trim() || "unknown error";
			this.#log(
				`lane_branch_delete_failed name=${name} session=${sessionId} branch=${branchName} detail=${stderr}`,
				"warn",
			);
		}
		if (removeRepository) rmSync(repository, { recursive: true, force: true });
	}

	/** `undefined` when no job row exists; `"corrupt"` when one exists but cannot be trusted. */
	#job(name: string): LaneJobRecord | "corrupt" | undefined {
		const json = this.#database.work.laneJobJson(laneJobIdentity(name).jobId);
		// A row that exists with an empty body is damage, not absence.
		if (json === undefined) return undefined;
		try {
			return parseLaneJobRecord(json);
		} catch {
			return "corrupt";
		}
	}
}

interface LaneTreeBytes {
	readonly totalBytes: number;
	readonly worktrees: ReadonlyMap<string, number>;
}

async function scanLaneTree(path: string): Promise<LaneTreeBytes> {
	let rootStat: Awaited<ReturnType<typeof lstat>>;
	try {
		rootStat = await lstat(path);
	} catch (error) {
		if (isMissingPath(error)) return { totalBytes: 0, worktrees: new Map() };
		throw error;
	}
	if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) throw new Error("lane root is not a real directory");
	let totalBytes = allocatedBytes(rootStat);
	const worktrees = new Map<string, number>();
	for (const name of await readdir(path)) {
		const entryPath = join(path, name);
		const bytes = await pathAllocatedBytes(entryPath);
		totalBytes += bytes;
		worktrees.set(resolve(entryPath), bytes);
	}
	return { totalBytes, worktrees };
}

async function pathAllocatedBytes(path: string): Promise<number> {
	const stat = await lstat(path);
	let total = allocatedBytes(stat);
	if (!stat.isDirectory() || stat.isSymbolicLink()) return total;
	for (const name of await readdir(path)) total += await pathAllocatedBytes(join(path, name));
	return total;
}

function allocatedBytes(stat: Awaited<ReturnType<typeof lstat>>): number {
	const blocks = typeof stat.blocks === "bigint" ? Number(stat.blocks) : stat.blocks;
	const size = typeof stat.size === "bigint" ? Number(stat.size) : stat.size;
	return Number.isFinite(blocks) ? blocks * 512 : size;
}

function isMissingPath(error: unknown): boolean {
	return typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT";
}

async function worktreeIsUnregistered(repositoryPath: string, worktreePath: string): Promise<boolean> {
	const result = await runGit(["worktree", "list", "--porcelain"], { cwd: repositoryPath });
	if (result.exitCode !== 0) throw new Error(result.stderr.trim() || "git worktree list failed");
	const expected = canonicalPathIncludingMissing(worktreePath);
	for (const line of result.stdout.split(/\r?\n/)) {
		if (!line.startsWith("worktree ")) continue;
		if (canonicalPathIncludingMissing(line.slice("worktree ".length)) === expected) return false;
	}
	return true;
}

function assertManagedWorktreeLocation(laneRoot: string, name: string, worktreePath: string): void {
	if (resolve(worktreePath) !== managedLaneWorktreePath(laneRoot, name))
		throw new Error("managed worktree path does not match its lane name");
	const lanesRoot = join(laneRoot, "lanes");
	const lanesStat = lstatSync(lanesRoot);
	if (!lanesStat.isDirectory() || lanesStat.isSymbolicLink()) throw new Error("lane directory is not a real directory");
	const canonicalLaneRoot = realpathSync(laneRoot);
	const canonicalLanesRoot = realpathSync(lanesRoot);
	const laneRootPrefix = canonicalLaneRoot.endsWith(sep) ? canonicalLaneRoot : `${canonicalLaneRoot}${sep}`;
	const expectedCanonicalPath = join(canonicalLanesRoot, Buffer.from(name, "utf8").toString("hex"));
	if (!canonicalLanesRoot.startsWith(laneRootPrefix)) throw new Error("lane directory escapes its configured root");
	if (canonicalPathIncludingMissing(worktreePath) !== expectedCanonicalPath)
		throw new Error("managed worktree path escapes its configured lane root");
	try {
		const worktreeStat = lstatSync(worktreePath);
		if (!worktreeStat.isDirectory() || worktreeStat.isSymbolicLink())
			throw new Error("managed worktree path is not a real directory");
		if (realpathSync(worktreePath) !== expectedCanonicalPath)
			throw new Error("managed worktree path resolves outside its lane identity");
	} catch (error) {
		if (!isMissingPath(error)) throw error;
	}
}

function assertManagedRepositoryPath(laneRoot: string | undefined, name: string, repository: string): void {
	if (laneRoot === undefined) throw new Error("configured lane root is unavailable for managed repository cleanup");
	const expectedPath = managedLaneRepositoryPath(laneRoot, name);
	if (resolve(repository) !== expectedPath) throw new Error("managed repository path does not match its lane name");
	if (!existsSync(repository)) return;

	const repositoryStat = lstatSync(repository);
	if (!repositoryStat.isDirectory() || repositoryStat.isSymbolicLink())
		throw new Error("managed repository path is not a real directory");
	const canonicalLaneRoot = realpathSync(laneRoot);
	const canonicalRepositoryRoot = realpathSync(join(laneRoot, "repositories"));
	const canonicalRepository = realpathSync(repository);
	const expectedCanonicalPath = join(canonicalRepositoryRoot, Buffer.from(name, "utf8").toString("hex"));
	const laneRootPrefix = canonicalLaneRoot.endsWith(sep) ? canonicalLaneRoot : `${canonicalLaneRoot}${sep}`;
	const repositoryRootPrefix = canonicalRepositoryRoot.endsWith(sep)
		? canonicalRepositoryRoot
		: `${canonicalRepositoryRoot}${sep}`;
	if (
		!canonicalRepository.startsWith(laneRootPrefix) ||
		!canonicalRepositoryRoot.startsWith(laneRootPrefix) ||
		canonicalRepository !== expectedCanonicalPath ||
		!canonicalRepository.startsWith(repositoryRootPrefix)
	)
		throw new Error("managed repository path escapes its configured lane root");
}

function canonicalPathIncludingMissing(target: string): string {
	let current = resolve(target);
	const missing: string[] = [];
	while (true) {
		try {
			return resolve(realpathSync(current), ...missing.reverse());
		} catch (error) {
			const code = typeof error === "object" && error !== null && "code" in error ? error.code : undefined;
			if (code !== "ENOENT" && code !== "ENOTDIR") throw error;
			const parent = dirname(current);
			if (parent === current) throw error;
			missing.push(basename(current));
			current = parent;
		}
	}
}

/**
 * Patch ids from a `git patch-id --stable` run over a `git log -p` feed:
 * the leading hash of every line whose second field (the commit it came
 * from) is a full 40-hex sha. Empty-diff lines (`0000…`) carry no commit
 * and are dropped.
 */
function patchIdsFromGitLog(stdout: string): Set<string> {
	const ids = new Set<string>();
	for (const line of stdout.split("\n")) {
		const [patchId, commit] = line.trim().split(/\s+/);
		if (patchId && commit && /^[0-9a-f]{40}$/.test(commit)) ids.add(patchId);
	}
	return ids;
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
