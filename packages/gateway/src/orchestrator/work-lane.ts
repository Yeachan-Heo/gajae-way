import {
	type ChatMessagePayload,
	containsSilenceToken,
	isSilenceToken,
	type OriginRef,
	type PromptStatusBody,
	ProtocolError,
	validateOriginRef,
	type WorkStartResult,
	type WorkStatusResult,
	type WorkSteerResult,
} from "@gajae-gateway/protocol";
import {
	acknowledgeHold,
	appendAttempt,
	applyReconciliation,
	closeAttempt,
	createLaneJobRecord,
	envelopeErrorCode,
	GjcCliError,
	hasNewCommit,
	type LaneJobRecord,
	newOpRef,
	OpRefRejectedError,
	parseLaneJobRecord,
} from "@gajae-gateway/subsession";
import type { GjcModelSelection } from "../config";
import { buildDeliveryPayload } from "../delivery/delivery";
import {
	type GatewayDatabase,
	type WorkAttemptRuntime,
	type WorkAttemptTerminalEvidence,
	workAttemptDeliveryId,
} from "../store/db";
import { type LaneGovernor, laneJobIdentity, workSessionKey } from "./lane-governor";
import { sanitizeDiagnostic } from "./rebind";
import type { SessionPort } from "./session-port";
import type { TailHandle } from "./tail-runner";

const owners = new WeakSet<GatewayDatabase>();
/** Consecutive failed reconciliations between authority re-checks through recovery. */
const FAILURES_PER_READOPTION = 8;
/** Ceiling for the failure backoff between reconciliation polls. */
const MAX_FAILURE_BACKOFF_MS = 30_000;
const reasons = new Set([
	"end_turn",
	"prompt_deadline_exceeded",
	"cancelled",
	"max_tokens",
	"max_turn_requests",
	"refusal",
	"stopped_incomplete",
	"sdk_failed",
	"send_rejected",
	"terminal_missing_receipt",
	"terminal_uncertain",
	"session_dead",
	"session_disowned",
	"recovery_indeterminate",
	"output_unavailable",
]);
const refusalCodes = new Set([
	"busy",
	"steer_refused",
	"invalid_params",
	"not_running",
	"no_active_turn",
	"client_ref_conflict",
	"session_not_found",
]);
const pendingOutput = () => ({
	disposition: "pending" as const,
	reads: 0,
	nextReadAt: null,
	excerpt: null,
	proof: null,
	knownSilence: null,
});
interface Observer {
	readonly runtime: WorkAttemptRuntime;
	readonly generation: number;
	readonly abort: AbortController;
	readonly binding: { readonly sessionId: string | null; readonly epoch: number } | undefined;
	tail?: TailHandle;
	attaching?: Promise<void>;
	task?: Promise<void>;
	timer?: ReturnType<typeof setTimeout>;
	text?: string;
}
interface Waiter {
	readonly owner: object;
	readonly finish: (error?: Error) => void;
}
export interface WorkLaneManagerOptions {
	readonly database: GatewayDatabase;
	readonly port: SessionPort;
	readonly lanes: LaneGovernor;
	readonly ownerTarget?: () => OriginRef | undefined;
	readonly brokerGeneration?: () => number;
	readonly deliver?: (payload: ChatMessagePayload) => void;
	readonly pollMs?: number;
	readonly waitTimeoutMs?: number;
	readonly now?: () => number;
}
interface WorkInput {
	name: string;
	text: string;
	cwd: string;
	resume: boolean;
	model?: GjcModelSelection;
	target: OriginRef | null;
}

/** Attempt ownership is independent of sockets, response deadlines and broker generations. */
export class WorkLaneManager {
	readonly #options: WorkLaneManagerOptions;
	readonly #db: GatewayDatabase;
	readonly #port: SessionPort;
	readonly #observers = new Map<string, Observer>();
	/** Consecutive reconciliation failures per open attempt; cleared by a successful tick or settlement. */
	readonly #failures = new Map<string, { failures: number; reason: string }>();
	readonly #waiters = new Map<string, Set<Waiter>>();
	readonly #detachedOwners = new WeakSet<object>();
	#generationRecovery?: Promise<void>;
	#recovery?: Promise<void>;
	#stopped = false;
	#stopPromise?: Promise<void>;
	constructor(options: WorkLaneManagerOptions) {
		if (owners.has(options.database)) throw new Error("work manager already registered");
		owners.add(options.database);
		this.#options = options;
		this.#db = options.database;
		this.#port = options.port;
		options.lanes.setRecoveryGate(() => this.recover());
	}
	#now(): number {
		return this.#options.now?.() ?? Date.now();
	}
	#at(): string {
		return new Date(this.#now()).toISOString();
	}
	#live(): void {
		if (this.#stopped) throw new ProtocolError("gateway_shutting_down", "gateway is stopping");
	}
	#binding(runtime: WorkAttemptRuntime): boolean {
		const binding = this.#db.getSessionRecord(runtime.sessionKey);
		return binding?.sessionId === runtime.sessionId && binding.epoch === runtime.epoch;
	}
	#current(observer: Observer): boolean {
		return (
			!this.#stopped &&
			!this.#db.isBrokerQuarantined("work", observer.runtime.jobId) &&
			!observer.abort.signal.aborted &&
			this.#observers.get(observer.runtime.opRef) === observer &&
			observer.generation === (this.#options.brokerGeneration?.() ?? 0)
		);
	}
	#writeCurrent(observer: Observer): boolean {
		if (!this.#current(observer)) return false;
		const binding = this.#db.getSessionRecord(observer.runtime.sessionKey);
		return (
			this.#current(observer) &&
			binding?.sessionId === observer.binding?.sessionId &&
			binding?.epoch === observer.binding?.epoch
		);
	}
	#job(name: string, required = false): LaneJobRecord | undefined {
		const { jobId } = laneJobIdentity(name);
		this.#assertNotQuarantined(jobId, name);
		try {
			const { jobId, laneKey } = laneJobIdentity(name);
			const byId = this.#db.laneJobJson(jobId);
			const byLane = this.#db.laneJobJsonByLaneKey(laneKey);
			if (byId !== byLane) throw new Error("identity mismatch");
			if (byId === undefined) {
				if (required)
					throw new ProtocolError("invalid_params", "unknown work lane", { reasonCode: "unknown_work_lane", name });
				return undefined;
			}
			const job = parseLaneJobRecord(byId);
			if (job.jobId !== jobId) throw new Error("identity mismatch");
			const last = job.attempts.at(-1);
			const saved = last && this.#db.workAttemptGet(last.opRef);
			if (
				last &&
				saved &&
				(saved.jobId !== jobId ||
					saved.laneKey !== laneKey ||
					saved.cwd !== job.lane.worktreePath ||
					saved.sessionId !== last.sessionId ||
					saved.startedAt !== last.startedAt ||
					saved.settledAt !== (last.endedAt ?? null))
			)
				throw new Error("runtime/history mismatch");
			const open = this.#db.workAttemptOpenByLane(laneKey);
			if (open && (job.attempts.at(-1)?.opRef !== open.opRef || job.attempts.at(-1)?.endedAt !== undefined))
				throw new Error("runtime mismatch");
			return job;
		} catch (error) {
			if (error instanceof ProtocolError) throw error;
			throw new ProtocolError("verb_failed", "work lane state unavailable", { reasonCode: "lane_state_corrupt", name });
		}
	}
	#assertNotQuarantined(jobId: string, name?: string): void {
		if (this.#db.isBrokerQuarantined("work", jobId))
			throw new ProtocolError("verb_failed", "work lane belongs to a quarantined broker authority", {
				reasonCode: "broker_authority_quarantined",
				jobId,
				...(name === undefined ? {} : { name }),
			});
	}
	async start(params: unknown): Promise<WorkStartResult> {
		return this.#start(parseInput(params, "start", this.#options.ownerTarget?.()), "start");
	}
	async run(params: unknown, owner: object, signal?: AbortSignal) {
		const started = await this.#start(parseInput(params, "run"), "run");
		if (!started.started) {
			const { started: _, ...held } = started;
			return held;
		}
		await this.#wait(started.opRef, owner, signal);
		const runtime = this.#db.workAttemptGet(started.opRef)!;
		const reason = runtime.terminal?.reasonCode ?? "terminal_uncertain";
		if (reason !== "end_turn") throw workError("work attempt did not complete", reason, runtime);
		const text = this.#observers.get(runtime.opRef)?.text;
		if (text === undefined) throw workError("work output unavailable", "output_unavailable", runtime);
		return { held: false as const, text, jobId: runtime.jobId, opRef: runtime.opRef, sessionKey: runtime.sessionKey };
	}
	async #start(input: WorkInput, mode: "start" | "run"): Promise<WorkStartResult> {
		this.#live();
		this.#job(input.name);
		await this.recover();
		this.#live();
		const sessionKey = workSessionKey(input.name);
		return this.#port.runExclusive(sessionKey, async (): Promise<WorkStartResult> => {
			this.#live();
			let job = this.#job(input.name);
			const { jobId } = laneJobIdentity(input.name);
			if (job && job.lane.worktreePath !== input.cwd)
				throw new ProtocolError("invalid_params", "work lane cwd mismatch", {
					reasonCode: "lane_cwd_mismatch",
					name: input.name,
				});
			const open = job?.attempts.find((attempt) => attempt.endedAt === undefined);
			if (open)
				throw new ProtocolError("invalid_params", "attempt already open; use work.steer or wait", {
					reasonCode: "attempt_open",
					jobId,
					opRef: open.opRef,
					sessionId: open.sessionId,
				});
			if (job && (job.state === "awaiting_operator" || job.state === "stalled")) {
				if (!input.resume)
					return {
						started: false,
						held: true,
						jobId,
						state: job.state,
						reason: `the job is ${job.state}; reconcile, then resume explicitly`,
					};
				job = acknowledgeHold({ record: job, note: "operator resumed the work lane", at: this.#at() });
			}
			if (!job) {
				const facts = await collectRepoFacts(input.cwd);
				job = createLaneJobRecord({
					jobId,
					branch: facts?.branch ?? `work/${input.name.toLowerCase()}`,
					worktreePath: input.cwd,
					baselineSha: facts?.headSha,
				});
			}
			const binding = await this.#port.runExclusive("work/admission", async () => {
				this.#live();
				this.#options.lanes.assertAdmission(input.name);
				try {
					return await this.#port.bind({
						originKey: sessionKey,
						epoch: this.#db.getSessionRecord(sessionKey)?.epoch ?? 0,
						repo: input.cwd,
						codingRegister: true,
						...(input.model ? { model: input.model } : {}),
					});
				} catch {
					throw new ProtocolError("verb_failed", "work lane bind failed", {
						reasonCode: "bind_failed",
						name: input.name,
					});
				}
			});
			this.#live();
			const opRef = newOpRef(`work-${input.name.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`);
			const runtime = makeRuntime(
				this.#db,
				input.name,
				binding.sessionId,
				binding.epoch,
				input.cwd,
				this.#at(),
				opRef,
				mode,
				input.target,
			);
			job = appendAttempt(job, { opRef, sessionId: binding.sessionId, startedAt: runtime.startedAt });
			this.#db.workAttemptPrepare(runtime, job);
			const observer = this.#register(runtime);
			// Submit on the observer's own relay so the host streams this turn's
			// lifecycle to it; the frame callback then wakes reconciliation as the
			// turn progresses instead of on the poll interval alone.
			this.#attach(observer);
			await observer.attaching;
			let accepted = false;
			let rejected = false;
			let source: "receipt" | "status" = "receipt";
			try {
				const receipt = await this.#port.send({
					sessionId: runtime.sessionId,
					repo: runtime.cwd,
					opRef,
					text: input.text,
					codingRegister: true,
					...(observer.tail ? { relay: observer.tail } : {}),
					// Only this bind receipt can prove the requested model was applied at startup.
					...(input.model && binding.startupModelApplied !== true ? { model: input.model } : {}),
				});
				observer.tail?.correlate(opRef, receipt);
				accepted = receipt.operationRef === opRef && receipt.sessionId === runtime.sessionId;
			} catch (error) {
				rejected = definitiveRefusal(error);
				const code =
					typeof error === "object" && error !== null && "details" in error
						? (error.details as { code?: unknown } | undefined)?.code
						: undefined;
				console.error(
					`work_send_error opRef=${opRef} code=${typeof code === "string" && /^[a-z_]{1,64}$/.test(code) ? code : "transport_unavailable"}`,
				);
			}
			if (!this.#writeCurrent(observer)) {
				console.error(
					`work_send_fenced opRef=${opRef} generation=${observer.generation} currentGeneration=${this.#options.brokerGeneration?.() ?? 0} binding=${this.#binding(runtime)}`,
				);
				this.#live();
				throw workError("work send acceptance uncertain", "send_acceptance_uncertain", runtime);
			}
			let latest = this.#db.workAttemptGet(opRef)!;
			if (!accepted && !rejected) {
				try {
					accepted = provesAcceptance(await this.#query(latest));
					source = "status";
				} catch {
					/* Observation owns recovery; never replay. */
				}
			}
			if (!this.#writeCurrent(observer)) {
				console.error(
					`work_send_reconcile_fenced opRef=${opRef} generation=${observer.generation} currentGeneration=${this.#options.brokerGeneration?.() ?? 0} binding=${this.#binding(runtime)}`,
				);
				this.#live();
				throw workError("work send acceptance uncertain", "send_acceptance_uncertain", runtime);
			}
			latest =
				this.#db.workAttemptUpdate(
					opRef,
					latest.version,
					accepted
						? { sendPhase: "accepted", sendEvidence: { source, observedAt: this.#at() } }
						: {
								sendPhase: "uncertain",
								...(rejected
									? { terminal: { kind: "local", observedAt: this.#at(), reasonCode: "send_rejected" } as const }
									: {}),
							},
				) ?? latest;
			this.#schedule(observer, 0);
			if (rejected) throw workError("work send rejected", "send_rejected", latest);
			if (!accepted) throw workError("work send acceptance uncertain", "send_acceptance_uncertain", latest);
			return { started: true, jobId, opRef, sessionKey, sessionId: runtime.sessionId };
		});
	}
	async status(params: unknown): Promise<WorkStatusResult> {
		const name = parseName(params);
		const job = this.#job(name, true)!;
		const key = workSessionKey(name);
		const binding = this.#db.getSessionRecord(key);
		const attempt = job.attempts.at(-1);
		let op: PromptStatusBody | null = null;
		if (attempt && binding?.sessionId === attempt.sessionId) {
			try {
				op = await this.#query({
					jobId: job.jobId,
					opRef: attempt.opRef,
					sessionId: attempt.sessionId,
					cwd: job.lane.worktreePath,
				});
			} catch {
				throw workError("work status unavailable", "status_unavailable", { ...attempt, jobId: job.jobId });
			}
			const current = this.#db.getSessionRecord(key);
			if (current?.sessionId !== binding.sessionId || current.epoch !== binding.epoch) op = null;
		}
		return {
			jobId: job.jobId,
			state: job.state,
			sessionId: this.#db.getSessionRecord(key)?.sessionId ?? "",
			lastActivityAt: this.#db.workLaneRows().find((row) => row.origin_key === key)?.last_activity_at ?? null,
			attempt: attempt
				? {
						opRef: attempt.opRef,
						startedAt: attempt.startedAt,
						...(attempt.endedAt ? { endedAt: attempt.endedAt } : {}),
						...(attempt.endState ? { endState: attempt.endState } : {}),
					}
				: null,
			op,
		};
	}
	async steer(params: unknown): Promise<WorkSteerResult> {
		const name = parseName(params);
		const text = (params as { text?: unknown }).text;
		if (typeof text !== "string" || !text) invalid("text");
		this.#live();
		const job = this.#job(name, true)!;
		const captured = job.attempts.find((attempt) => attempt.endedAt === undefined);
		if (!captured)
			throw new ProtocolError("invalid_params", "no open attempt to steer", {
				reasonCode: "no_open_attempt",
				jobId: job.jobId,
			});
		await this.recover();
		this.#live();
		return this.#port.runExclusive(workSessionKey(name), async (): Promise<WorkSteerResult> => {
			this.#live();
			const current = this.#job(name, true)!;
			const open = current.attempts.at(-1);
			if (open?.opRef !== captured.opRef || open.endedAt !== undefined)
				throw new ProtocolError("invalid_params", "no open attempt to steer", {
					reasonCode: "no_open_attempt",
					jobId: job.jobId,
				});
			const runtime = this.#db.workAttemptGet(open.opRef);
			const clientRef = newOpRef("work-steer");
			if (!runtime || !this.#binding(runtime) || runtime.terminal)
				throw workError(
					"work steer acceptance uncertain",
					"steer_acceptance_uncertain",
					{ ...open, jobId: job.jobId },
					clientRef,
				);
			try {
				await this.#port.steer({ sessionId: open.sessionId, repo: current.lane.worktreePath, text, clientRef });
			} catch (error) {
				if (definitiveSteerRefusal(error)) return { steered: false, reason: `steer_refused:${safeRefusal(error)}` };
				throw workError("work steer acceptance uncertain", "steer_acceptance_uncertain", runtime, clientRef);
			}
			return { steered: true, clientRef };
		});
	}
	async #query(runtime: { jobId: string; sessionId: string; cwd: string; opRef: string }): Promise<PromptStatusBody> {
		this.#assertNotQuarantined(runtime.jobId);
		const report = await this.#port.status({ sessionId: runtime.sessionId, repo: runtime.cwd, opRef: runtime.opRef });
		if (
			report.operationRef !== runtime.opRef ||
			!report.status ||
			!["accepted", "in_flight", "terminal_ok", "failed", "unknown"].includes(report.status.status) ||
			(report.status.clientRef !== undefined && report.status.clientRef !== runtime.opRef)
		)
			throw new Error("invalid status identity");
		return safeStatus(report.status);
	}
	#register(runtime: WorkAttemptRuntime): Observer {
		const existing = this.#observers.get(runtime.opRef);
		if (existing && this.#current(existing)) return existing;
		const binding = this.#db.getSessionRecord(runtime.sessionKey);
		const observer: Observer = {
			runtime,
			generation: this.#options.brokerGeneration?.() ?? 0,
			abort: new AbortController(),
			binding: binding ? { sessionId: binding.sessionId, epoch: binding.epoch } : undefined,
		};
		this.#observers.set(runtime.opRef, observer);
		return observer;
	}
	#schedule(observer: Observer, delay: number): void {
		if (!this.#current(observer) || observer.timer || observer.task) return;
		const opRef = observer.runtime.opRef;
		observer.timer = setTimeout(() => {
			observer.timer = undefined;
			if (!this.#current(observer)) return;
			observer.task = this.#tick(observer)
				.then(
					() => {
						this.#failures.delete(opRef);
					},
					(error: unknown) => this.#failed(observer, error),
				)
				.finally(() => {
					observer.task = undefined;
					if (!this.#current(observer)) return;
					if (this.#db.workAttemptGet(opRef)?.settledAt !== null) {
						this.#observers.delete(opRef);
						this.#failures.delete(opRef);
						return;
					}
					// The lane was rebound under this observer: it can never write again,
					// so polling is pure waste. Recovery re-proves authority and settles.
					if (!this.#writeCurrent(observer)) return this.#readopt(observer, "binding_changed");
					const failures = this.#failures.get(opRef)?.failures ?? 0;
					if (failures > 0 && failures % FAILURES_PER_READOPTION === 0)
						return this.#readopt(observer, "reconciliation_unavailable");
					const pollMs = this.#options.pollMs ?? 250;
					this.#schedule(observer, failures ? Math.min(pollMs * 2 ** (failures - 1), MAX_FAILURE_BACKOFF_MS) : pollMs);
				});
		}, delay);
	}
	/** Counts the failure and reports it once per distinct reason instead of on every poll. */
	#failed(observer: Observer, error: unknown): void {
		const opRef = observer.runtime.opRef;
		const prior = this.#failures.get(opRef);
		const reason = failureReason(error);
		const failures = (prior?.failures ?? 0) + 1;
		this.#failures.set(opRef, { failures, reason });
		if (this.#current(observer) && prior?.reason !== reason)
			console.error(`work_reconciliation_unavailable opRef=${opRef} failures=${failures} reason=${reason}`);
	}
	/**
	 * Retires this observer and hands the open attempt back to recovery, which
	 * re-reads status and broker liveness and either resumes observation on a
	 * live, still-bound session or records a local terminal reason.
	 */
	#readopt(observer: Observer, cause: "binding_changed" | "reconciliation_unavailable"): void {
		const opRef = observer.runtime.opRef;
		console.error(
			`work_observer_readopted opRef=${opRef} cause=${cause} failures=${this.#failures.get(opRef)?.failures ?? 0}`,
		);
		observer.abort.abort();
		void (async () => {
			await this.recover();
			// A recovery pass already past this attempt when it was retired cannot
			// have readopted it; a pass started after retirement always does.
			const current = this.#observers.get(opRef);
			if (
				!this.#stopped &&
				(!current || current.abort.signal.aborted) &&
				this.#db.workAttemptGet(opRef)?.settledAt === null
			)
				await this.recover();
		})().catch(() => {
			/* Recovery reports its own failures; the attempt stays open and observable. */
		});
	}
	#attach(observer: Observer): void {
		if (observer.tail || observer.attaching || !this.#writeCurrent(observer) || !this.#binding(observer.runtime))
			return;
		observer.attaching = (async () => {
			const runtime = observer.runtime;
			const tail = await this.#port.attachTail({
				sessionId: runtime.sessionId,
				brokerGeneration: observer.generation,
				repo: runtime.cwd,
				originKey: runtime.sessionKey,
				onFrame: () => {
					if (this.#writeCurrent(observer)) this.#schedule(observer, 0);
				},
			});
			if (!this.#writeCurrent(observer)) {
				await tail.close();
				return;
			}
			observer.tail = tail;
			await tail.ready;
			if (this.#writeCurrent(observer) && this.#db.workAttemptGet(runtime.opRef)?.settledAt === null) {
				tail.beginTurn(runtime.opRef);
				tail.setTurnRunning(true);
			}
		})()
			.catch(async () => {
				const tail = observer.tail;
				observer.tail = undefined;
				await tail?.close();
			})
			.finally(() => {
				observer.attaching = undefined;
			});
	}
	async #tick(observer: Observer): Promise<void> {
		if (!this.#writeCurrent(observer)) return;
		let runtime = this.#db.workAttemptGet(observer.runtime.opRef)!;
		if (runtime.settledAt) return;
		if (!runtime.terminal) {
			this.#attach(observer);
			const status = await this.#query(runtime);
			if (!this.#writeCurrent(observer)) return;
			if (status.status === "unknown") return;
			const terminal = terminalEvidence(status, this.#at());
			runtime =
				this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
					...(provesAcceptance(status)
						? {
								sendPhase: "accepted" as const,
								sendEvidence: runtime.sendEvidence ?? { source: "status" as const, observedAt: this.#at() },
							}
						: {}),
					...(terminal ? { terminal } : {}),
				}) ?? runtime;
			if (!runtime.terminal) return;
		}
		if (runtime.output.disposition === "pending" && runtime.output.knownSilence) {
			const silent = this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
				output: { ...runtime.output, disposition: "silent", nextReadAt: null },
			});
			if (!silent) return;
			runtime = silent;
		}
		if (runtime.output.disposition === "pending") {
			if (runtime.output.nextReadAt && this.#now() < Date.parse(runtime.output.nextReadAt)) return;
			if (runtime.output.reads >= 3 || runtime.terminal?.kind === "local") {
				runtime =
					this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
						output: { ...runtime.output, disposition: "unavailable", nextReadAt: null },
					}) ?? runtime;
			} else {
				const reads = runtime.output.reads + 1;
				const claimed = this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
					output: {
						...runtime.output,
						reads,
						nextReadAt: reads === 3 ? null : new Date(this.#now() + (reads === 1 ? 1000 : 5000)).toISOString(),
					},
				});
				if (!claimed) return;
				runtime = claimed;
				const result = await this.#port
					.fetchWorkerOutput({
						sessionId: runtime.sessionId,
						repo: runtime.cwd,
						opRef: runtime.opRef,
						notBeforeMs: Math.max(Date.parse(runtime.startedAt), runtime.terminal?.status?.startedAt ?? 0),
						terminalIdentity: runtime.terminal?.status,
						signal: observer.abort.signal,
						isCurrent: () => this.#writeCurrent(observer),
					})
					.catch(() => ({ status: "absent" as const, code: "transport_error" as const }));
				if (!this.#writeCurrent(observer)) return;
				if (result.status === "proven") {
					const proof = {
						...result.provenance,
						epoch: runtime.epoch,
						observedAtMs: result.observedAtMs,
						attribution: "operation_ref" as const,
					};
					const silent = isSilenceToken(result.text) || containsSilenceToken(result.text);
					const updated = this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
						output: {
							...runtime.output,
							disposition: silent ? "silent" : "available",
							nextReadAt: null,
							excerpt: utf8Prefix(result.text),
							proof,
							knownSilence: silent ? proof : null,
						},
					});
					if (!updated) return;
					runtime = updated;
					if (runtime.mode === "run" && this.#waiters.has(runtime.opRef)) observer.text = result.text;
				} else if (result.status === "unavailable" || reads >= 3) {
					runtime =
						this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
							output: { ...runtime.output, disposition: "unavailable", nextReadAt: null },
						}) ?? runtime;
				} else return;
			}
		}
		if (runtime.output.disposition !== "pending") await this.#settle(observer, runtime);
	}
	async #settle(observer: Observer, runtime: WorkAttemptRuntime): Promise<void> {
		const facts = await collectRepoFacts(runtime.cwd);
		await this.#port.runExclusive(runtime.sessionKey, async () => {
			if (!this.#writeCurrent(observer)) return;
			const current = this.#db.workAttemptGet(runtime.opRef);
			if (!current || current.version !== runtime.version || current.settledAt) return;
			const name = runtime.sessionKey.slice("work/task/".length);
			const at = this.#at();
			const reason = runtime.terminal!.reasonCode;
			const endState =
				reason === "end_turn"
					? "completed"
					: reason === "terminal_missing_receipt"
						? "terminal_missing_receipt"
						: ["terminal_uncertain", "session_dead", "session_disowned", "recovery_indeterminate"].includes(reason)
							? "terminal_uncertain"
							: ["sdk_failed", "send_rejected"].includes(reason)
								? "failed"
								: "attempt_ended";
			let job = closeAttempt({
				record: this.#job(name, true)!,
				opRef: runtime.opRef,
				endState,
				errorCode: reason === "end_turn" ? undefined : reason,
				endedAt: at,
			});
			if (facts) {
				const progressed = hasNewCommit({
					...facts,
					observedAt: at,
					knownCheckpoints: job.checkpoints,
					baselineSha: job.baselineSha,
				});
				job = applyReconciliation({
					record: job,
					repository: { ...facts, observedAt: at },
					classification: progressed ? "progressed" : endState === "completed" ? "held" : "stalled",
				});
			}
			const decision = runtime.output.knownSilence ? "suppressed" : runtime.target === null ? "no_target" : "enqueued";
			const label = endState === "completed" ? "completed" : endState === "failed" ? "failed" : "attempt_ended";
			const lead = reason === "end_turn" ? "" : `${reason}: `;
			const body = runtime.output.disposition === "unavailable" ? "output_unavailable" : (runtime.output.excerpt ?? "");
			const content = `${lead}${utf8Prefix(body, 2048 - Buffer.byteLength(lead, "utf8"))}`;
			const payload =
				decision === "enqueued"
					? buildDeliveryPayload(
							runtime.opRef,
							runtime.target!,
							`[lane ${name}] ${label}: ${content}`,
							runtime.deliveryId,
						)
					: undefined;
			const settled = this.#db.workAttemptSettle(
				runtime.opRef,
				runtime.version,
				job,
				{ decision, settledAt: at },
				payload,
			);
			if (!settled) return;
			for (const waiter of [...(this.#waiters.get(runtime.opRef) ?? [])]) waiter.finish();
			if (payload) this.#options.deliver?.(payload);
		});
		if (this.#current(observer) && this.#db.workAttemptGet(runtime.opRef)?.settledAt) {
			observer.tail?.setTurnRunning(false);
			await observer.tail?.close();
			observer.tail = undefined;
		}
	}
	/** Registers recovery work, never waits for a worker's terminal transition. */
	recover(): Promise<void> {
		if (this.#stopped) return Promise.resolve();
		if (this.#generationRecovery) return this.#generationRecovery;
		if (this.#recovery) return this.#recovery;
		const recovery = this.#recover().finally(() => {
			if (this.#recovery === recovery) this.#recovery = undefined;
		});
		this.#recovery = recovery;
		return recovery;
	}
	async #recover(): Promise<void> {
		for (const row of this.#db.laneJobRows()) {
			if (this.#stopped) return;
			if (!row.lane_key.startsWith("work-")) continue;
			const name = row.lane_key.slice(5);
			await this.#port.runExclusive(workSessionKey(name), async () => {
				if (this.#stopped) return;
				let job: LaneJobRecord | undefined;
				try {
					job = this.#job(name);
				} catch {
					return;
				}
				const open = job?.attempts.find((attempt) => attempt.endedAt === undefined);
				if (!job || !open || this.#db.workAttemptGet(open.opRef)) return;
				this.#db.workAttemptPrepare(
					makeRuntime(
						this.#db,
						name,
						open.sessionId,
						this.#db.getSessionRecord(workSessionKey(name))?.epoch ?? 0,
						job.lane.worktreePath,
						open.startedAt,
						open.opRef,
						"historical",
						null,
					),
					job,
				);
			});
		}
		let after = "";
		while (!this.#stopped) {
			const rows = this.#db.workAttemptOpen(100, after);
			if (!rows.length) break;
			for (const runtime of rows) {
				after = runtime.opRef;
				const prior = this.#observers.get(runtime.opRef);
				if (prior && this.#writeCurrent(prior)) continue;
				if (prior) {
					prior.abort.abort();
					if (prior.timer) clearTimeout(prior.timer);
					await prior.attaching;
					await prior.task;
					await prior.tail?.close();
				}
				if (this.#stopped) return;
				const observer = this.#register(runtime);
				if (runtime.terminal) {
					this.#schedule(observer, 0);
					continue;
				}
				let status: PromptStatusBody | undefined;
				try {
					status = await this.#query(runtime);
				} catch {
					/* Liveness decides whether authority can be recovered. */
				}
				if (!this.#current(observer)) continue;
				const terminal = status && terminalEvidence(status, this.#at());
				if (terminal && this.#writeCurrent(observer)) {
					this.#db.workAttemptUpdate(runtime.opRef, runtime.version, { terminal });
					this.#schedule(observer, 0);
					continue;
				}
				let live: { live: boolean | undefined; disowned: boolean } = { live: undefined, disowned: false };
				try {
					live = (await this.#port.liveness?.({ sessionId: runtime.sessionId, repo: runtime.cwd })) ?? live;
				} catch {
					/* Indeterminate authority is not a replay permit. */
				}
				if (!this.#current(observer)) continue;
				if (live.live === true && !live.disowned && this.#binding(runtime)) {
					this.#attach(observer);
					this.#schedule(observer, 0);
					continue;
				}
				if (this.#writeCurrent(observer)) {
					this.#db.workAttemptUpdate(runtime.opRef, runtime.version, {
						terminal: {
							kind: "local",
							observedAt: this.#at(),
							reasonCode:
								live.disowned || !this.#binding(runtime)
									? "session_disowned"
									: live.live === false
										? "session_dead"
										: "recovery_indeterminate",
						},
					});
					this.#schedule(observer, 0);
				}
			}
		}
	}
	onBrokerGeneration(): Promise<void> {
		if (this.#generationRecovery) return this.#generationRecovery;
		const prior = this.#recovery;
		const recovery = (async () => {
			await prior;
			let generation: number;
			do {
				generation = this.#options.brokerGeneration?.() ?? 0;
				await this.#detachObservers();
				if (!this.#stopped) await this.#recover();
			} while (!this.#stopped && generation !== (this.#options.brokerGeneration?.() ?? 0));
		})();
		this.#generationRecovery = recovery.finally(() => {
			this.#generationRecovery = undefined;
		});
		this.#recovery = this.#generationRecovery.finally(() => {
			this.#recovery = undefined;
		});
		return this.#recovery;
	}
	#wait(opRef: string, owner: object, signal?: AbortSignal): Promise<void> {
		this.#live();
		if (this.#detachedOwners.has(owner)) throw new ProtocolError("gateway_shutting_down", "gateway is stopping");
		if (this.#db.workAttemptGet(opRef)?.settledAt) return Promise.resolve();
		return new Promise<void>((resolve, reject) => {
			const set = this.#waiters.get(opRef) ?? new Set<Waiter>();
			this.#waiters.set(opRef, set);
			let timer: ReturnType<typeof setTimeout> | undefined;
			const abort = () =>
				waiter.finish(
					new ProtocolError("verb_failed", "work wait detached; attempt remains observable", {
						reasonCode: "work_wait_detached",
					}),
				);
			const waiter: Waiter = {
				owner,
				finish: (error) => {
					if (!set.delete(waiter)) return;
					if (!set.size) this.#waiters.delete(opRef);
					if (timer) clearTimeout(timer);
					signal?.removeEventListener("abort", abort);
					error ? reject(error) : resolve();
				},
			};
			set.add(waiter);
			timer = setTimeout(
				() =>
					waiter.finish(
						workError(
							"work wait timed out; attempt remains observable",
							"work_wait_timeout",
							this.#db.workAttemptGet(opRef)!,
						),
					),
				this.#options.waitTimeoutMs ?? 30 * 60_000,
			);
			signal?.addEventListener("abort", abort, { once: true });
			if (signal?.aborted) abort();
		});
	}
	detachWaiters(owner: object): void {
		this.#detachedOwners.add(owner);
		for (const set of this.#waiters.values())
			for (const waiter of [...set])
				if (waiter.owner === owner) waiter.finish(new ProtocolError("gateway_shutting_down", "gateway is stopping"));
	}
	async #detachObservers(): Promise<void> {
		const observers = [...this.#observers.values()];
		for (const observer of observers) {
			observer.abort.abort();
			if (observer.timer) clearTimeout(observer.timer);
		}
		await Promise.all(
			observers.map(async (observer) => {
				await observer.attaching;
				await observer.task;
				await observer.tail?.close();
			}),
		);
		this.#observers.clear();
	}
	stop(): Promise<void> {
		if (this.#stopPromise) return this.#stopPromise;
		this.#stopped = true;
		for (const set of this.#waiters.values())
			for (const waiter of [...set]) waiter.finish(new ProtocolError("gateway_shutting_down", "gateway is stopping"));
		const lanesStopped = this.#options.lanes.stop();
		this.#stopPromise = (async () => {
			await this.#detachObservers();
			await this.#recovery;
			await lanesStopped;
			owners.delete(this.#db);
		})();
		return this.#stopPromise;
	}
}

function makeRuntime(
	db: GatewayDatabase,
	name: string,
	sessionId: string,
	epoch: number,
	cwd: string,
	startedAt: string,
	opRef: string,
	mode: WorkAttemptRuntime["mode"],
	target: OriginRef | null,
): WorkAttemptRuntime {
	const { jobId, laneKey } = laneJobIdentity(name);
	return {
		jobId,
		laneKey,
		sessionKey: workSessionKey(name),
		sessionId,
		epoch,
		cwd,
		startedAt,
		opRef,
		mode,
		target: target ? structuredClone(target) : null,
		sendPhase: mode === "historical" ? "uncertain" : "prepared",
		sendEvidence: null,
		terminal: null,
		output: pendingOutput(),
		deliveryId: workAttemptDeliveryId(db.instanceId, jobId, opRef),
		decision: "undecided",
		settledAt: null,
		version: 0,
	};
}
function invalid(field: string): never {
	throw new ProtocolError("invalid_params", "invalid work parameters", { reasonCode: "invalid_work_params", field });
}
function parseName(params: unknown): string {
	const name = (params as { name?: unknown } | null)?.name;
	if (typeof name !== "string" || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(name)) invalid("name");
	return name;
}
function parseInput(params: unknown, mode: "start" | "run", owner?: OriginRef): WorkInput {
	const name = parseName(params);
	const input = params as Record<string, unknown>;
	if (typeof input.text !== "string" || !input.text) invalid("text");
	if (input.cwd !== undefined) {
		if (typeof input.cwd !== "string" || !input.cwd.startsWith("/") || input.cwd.length > 4096) invalid("cwd");
		for (let index = 0; index < input.cwd.length; index++) {
			if (input.cwd.charCodeAt(index) < 32) invalid("cwd");
		}
	}
	if (input.resume !== undefined && typeof input.resume !== "boolean") invalid("resume");
	let model: GjcModelSelection | undefined;
	if (input.model !== undefined) {
		if (typeof input.model === "string" && input.model) model = input.model;
		else if (
			input.model &&
			typeof input.model === "object" &&
			!Array.isArray(input.model) &&
			Object.keys(input.model).length === 1 &&
			typeof (input.model as { preset?: unknown }).preset === "string" &&
			(input.model as { preset: string }).preset
		)
			model = { preset: (input.model as { preset: string }).preset };
		else invalid("model");
	}
	if (mode === "run" && Object.hasOwn(input, "notify")) invalid("notify");
	let target: OriginRef | null = null;
	if (mode === "start" && (input.notify !== undefined || owner !== undefined)) {
		try {
			target = validateOriginRef((input.notify !== undefined ? input.notify : owner) as OriginRef);
		} catch {
			invalid("notify");
		}
	}
	return {
		name,
		text: input.text,
		cwd: (input.cwd as string) ?? process.cwd(),
		resume: input.resume === true,
		model,
		target,
	};
}
function workError(
	message: string,
	reasonCode: string,
	runtime: { jobId: string; opRef: string; sessionId: string },
	clientRef?: string,
): ProtocolError {
	return new ProtocolError("verb_failed", message, {
		reasonCode,
		jobId: runtime.jobId,
		opRef: runtime.opRef,
		sessionId: runtime.sessionId,
		...(clientRef ? { clientRef } : {}),
	});
}
/** Bounded, secret-free error class and message for a log line. */
function failureReason(error: unknown): string {
	const text = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
	return sanitizeDiagnostic(text).slice(0, 200) || "unknown";
}
function safeRefusal(error: unknown): string {
	const code =
		error instanceof OpRefRejectedError
			? error.code
			: error instanceof GjcCliError
				? envelopeErrorCode(error.details)
				: undefined;
	return code && refusalCodes.has(code) ? code : "sdk_refused";
}
function definitiveRefusal(error: unknown): boolean {
	return (
		error instanceof OpRefRejectedError ||
		(error instanceof GjcCliError && refusalCodes.has(envelopeErrorCode(error.details) ?? ""))
	);
}
function definitiveSteerRefusal(error: unknown): boolean {
	if (
		error instanceof GjcCliError &&
		error.details &&
		typeof error.details === "object" &&
		(error.details as { refused?: unknown }).refused === true
	)
		return true;
	return definitiveRefusal(error);
}
function safeStatus(status: PromptStatusBody): PromptStatusBody {
	const result: { -readonly [K in keyof PromptStatusBody]: PromptStatusBody[K] } = { status: status.status };
	for (const key of ["commandId", "turnId", "clientRef"] as const) {
		const value = status[key];
		if (value !== undefined) {
			if (typeof value !== "string" || !/^[A-Za-z0-9._:-]{1,128}$/.test(value))
				throw new Error("invalid status identity");
			result[key] = value;
		}
	}
	for (const key of ["acceptedAt", "startedAt", "terminalAt"] as const) {
		const value = status[key];
		if (value !== undefined) {
			if (!Number.isFinite(value)) throw new Error("invalid status time");
			result[key] = value;
		}
	}
	if (status.receiptState !== undefined) {
		if (!["absent", "present", "missing", "unknown"].includes(status.receiptState))
			throw new Error("invalid receipt state");
		result.receiptState = status.receiptState;
	}
	if (status.outcome)
		result.outcome = {
			...(status.outcome.reason
				? { reason: reasons.has(status.outcome.reason) ? status.outcome.reason : "stopped_incomplete" }
				: {}),
			...(["stopped", "success", "failure", "cancelled", "completed"].includes(status.outcome.kind ?? "")
				? { kind: status.outcome.kind }
				: {}),
			...(["client_cancel", "runtime", "broker", "receipt"].includes(status.outcome.provenance ?? "")
				? { provenance: status.outcome.provenance }
				: {}),
		};
	if (status.error) result.error = { code: reasons.has(status.error.code ?? "") ? status.error.code : "sdk_failed" };
	return result;
}
/** Called only after #query has validated the operation and client reference. */
function provesAcceptance(status: PromptStatusBody): boolean {
	return (
		status.status === "accepted" ||
		status.status === "in_flight" ||
		((status.status === "terminal_ok" || status.status === "failed") && status.receiptState === "present")
	);
}

function terminalEvidence(status: PromptStatusBody, observedAt: string): WorkAttemptTerminalEvidence | undefined {
	if (status.status !== "terminal_ok" && status.status !== "failed") return undefined;
	const reasonCode =
		status.receiptState === "missing"
			? "terminal_missing_receipt"
			: status.receiptState === "unknown"
				? "terminal_uncertain"
				: status.status === "failed"
					? status.error?.code === "prompt_deadline_exceeded"
						? "prompt_deadline_exceeded"
						: "sdk_failed"
					: status.receiptState === "present" && status.outcome?.reason === "end_turn"
						? "end_turn"
						: ["cancelled", "max_tokens", "max_turn_requests", "refusal"].includes(status.outcome?.reason ?? "")
							? status.outcome!.reason!
							: "stopped_incomplete";
	return { kind: "broker", observedAt, reasonCode, status };
}
/** Bound the leading excerpt without splitting a Unicode scalar. */
export function utf8Prefix(text: string, maxBytes = 2048): string {
	let bytes = 0;
	let end = 0;
	for (const scalar of text) {
		const length = Buffer.byteLength(scalar, "utf8");
		if (bytes + length > maxBytes) break;
		bytes += length;
		end += scalar.length;
	}
	return text.slice(0, end);
}
async function collectRepoFacts(
	worktreePath: string,
): Promise<{ headSha?: string; dirtyFiles: number; branch?: string } | undefined> {
	try {
		const head = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "HEAD"], { stdout: "pipe", stderr: "pipe" });
		const branch = Bun.spawnSync(["git", "-C", worktreePath, "rev-parse", "--abbrev-ref", "HEAD"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		const status = Bun.spawnSync(["git", "-C", worktreePath, "status", "--porcelain"], {
			stdout: "pipe",
			stderr: "pipe",
		});
		const headSha = head.stdout.toString().trim();
		if (head.exitCode !== 0 || status.exitCode !== 0 || !/^[0-9a-f]{40}$/.test(headSha)) return undefined;
		return {
			headSha,
			dirtyFiles: status.stdout
				.toString()
				.split("\n")
				.filter((line) => line.trim()).length,
			branch: branch.stdout.toString().trim() || undefined,
		};
	} catch {
		return undefined;
	}
}
