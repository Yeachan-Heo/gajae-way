import { createHash } from "node:crypto";
import {
	assertControlAllowed,
	assertValidOpRef,
	CLIENT_REF_CONFLICT_CODE,
	decideRecovery,
	isOpRefRejection,
	isTerminalStatus,
	OpRefRejectedError,
	projectOpState,
	type BrokerSession,
	type StatusReport,
} from "@gajaeway/subsession";
import type { GjcModelSelection } from "../config";
import type { GatewayDatabase, InboundBatch, InboundMessageRow } from "../store/db";
import { sanitizeDiagnostic } from "./rebind";
import { TailCapacityError, deterministicTailDeliveryId, type TailFrame, type TailHandle } from "./tail-runner";
import type { SessionBinding, SessionPort } from "./session-port";

export const DEFAULT_SETTLE_WINDOW_MS = 2_000;
export const DEFAULT_STALL_TIMEOUT_MS = 120_000;
const RETIRED_REATTACH_DELAY_MS = 25;
const RETIRED_REATTACH_MAX_ATTEMPTS = 3;
/**
 * Grace before a decidable-terminal status may complete a batch whose tail has
 * not produced terminal evidence. One bounded hold keeps the tail the live
 * authority; after the grace the evidence is treated as genuinely unavailable
 * (post-crash ring loss) and status — the subsession reconcile authority —
 * completes with an explicit corroboration log.
 */
const STATUS_TERMINAL_GRACE_MS = 250;

export type PersonaActorState = "idle" | "settling" | "turn-running";

/**
 * Server-owned chat behavior attached to one durable persona batch. The actor
 * owns session/recovery ordering; this lifecycle owns only presentation,
 * bootstrap, context/memory, and delivery side effects.
 */
export interface PersonaTurnLifecycle {
	readonly text: string;
	readonly systemPreamble?: string;
	/** The selection applied by `model.set` before this session's first send. */
	readonly effectiveModel?: GjcModelSelection;
	/** Legacy/send-time fallback only; persistent persona turns leave this unset. */
	readonly sendModelFallback?: GjcModelSelection;
	onFrame?(input: PersonaTailFrameInput): void | Promise<void>;
	onTerminal?(input: PersonaTerminalInput): void | Promise<void>;
	onFailure?(input: PersonaFailureInput): void | Promise<void>;
	onRetired?(input: PersonaTurnIdentity): void | Promise<void>;
	onStall?(input: PersonaTurnIdentity & { elapsedMs: number }): void | Promise<void>;
}

export interface PersonaTurnIdentity {
	readonly originKey: string;
	readonly epoch: number;
	readonly sessionId: string;
	readonly batch: InboundBatch;
}

export interface PersonaTurnStartInput extends PersonaTurnIdentity {
	readonly rows: readonly InboundMessageRow[];
}

export interface PersonaTailFrameInput extends PersonaTurnIdentity {
	readonly frame: TailFrame;
}

export interface PersonaTerminalInput extends PersonaTurnIdentity {
	readonly text: string;
	readonly status: StatusReport;
}

export interface PersonaFailureInput extends PersonaTurnIdentity {
	readonly error: Error;
	readonly status?: StatusReport;
}

export interface PersonaSessionManagerOptions {
	readonly database: GatewayDatabase;
	readonly port: SessionPort;
	readonly instanceId: string;
	readonly repo: string;
	readonly settleWindowMs?: number;
	/** Resolves the live global/per-channel settle window from the first row. */
	readonly settleWindowFor?: (row: InboundMessageRow) => number;
	readonly stallTimeoutMs?: number;
	readonly brokerGeneration?: () => number;
	readonly now?: () => number;
	readonly setTimeout?: (work: () => void, delayMs: number) => unknown;
	readonly clearTimeout?: (timer: unknown) => void;
	/** Builds the server delivery/bootstrap lifecycle before an accepted SDK send. */
	readonly onTurnStart?: (input: PersonaTurnStartInput) => PersonaTurnLifecycle | Promise<PersonaTurnLifecycle>;
	/** Removes ephemeral request ownership after /new discarded unbatched rows. */
	readonly onInboundDiscard?: (messageIds: readonly string[]) => void | Promise<void>;
	/** Compatibility observer for direct actor tests; product delivery belongs in onTurnStart. */
	readonly onAssistantText?: (input: {
		originKey: string;
		sessionId: string;
		eventId: string;
		deliveryId: string;
		text: string;
	}) => void | Promise<void>;
	readonly onSteerAccepted?: (input: { originKey: string; messageId: string; opRef: string }) => void | Promise<void>;
	readonly log?: (line: string) => void;
}

/**
 * One durable mailbox per origin. Every inbound admission, tail event, broker
 * generation change, and reset is ordered by one actor whose binding is fenced
 * by origin, epoch, session, and generation.
 */
export class PersonaSessionManager {
	readonly #database: GatewayDatabase;
	readonly #port: SessionPort;
	readonly #instanceId: string;
	readonly #repo: string;
	readonly #settleWindowMs: number;
	readonly #settleWindowFor: (row: InboundMessageRow) => number;
	#stallTimeoutMs: number;
	readonly #brokerGeneration: () => number;
	readonly #now: () => number;
	readonly #setTimeout: (work: () => void, delayMs: number) => unknown;
	readonly #clearTimeout: (timer: unknown) => void;
	readonly #onTurnStart: PersonaSessionManagerOptions["onTurnStart"];
	readonly #onInboundDiscard: PersonaSessionManagerOptions["onInboundDiscard"];
	readonly #onAssistantText: PersonaSessionManagerOptions["onAssistantText"];
	readonly #onSteerAccepted: PersonaSessionManagerOptions["onSteerAccepted"];
	readonly #log: (line: string) => void;
	readonly #actors = new Map<string, OriginActor>();
	#stopped = false;

	constructor(options: PersonaSessionManagerOptions) {
		this.#database = options.database;
		this.#port = options.port;
		this.#instanceId = options.instanceId;
		this.#repo = options.repo;
		this.#settleWindowMs = nonNegativeInteger(options.settleWindowMs, DEFAULT_SETTLE_WINDOW_MS, "settleWindowMs");
		this.#stallTimeoutMs = positiveInteger(options.stallTimeoutMs, DEFAULT_STALL_TIMEOUT_MS, "stallTimeoutMs");
		this.#settleWindowFor = options.settleWindowFor ?? (() => this.#settleWindowMs);
		this.#brokerGeneration = options.brokerGeneration ?? (() => 0);
		this.#now = options.now ?? (() => Date.now());
		this.#setTimeout = options.setTimeout ?? ((work: () => void, delayMs: number) => setTimeout(work, delayMs));
		this.#clearTimeout =
			options.clearTimeout ?? ((timer: unknown) => clearTimeout(timer as ReturnType<typeof setTimeout>));
		this.#onTurnStart = options.onTurnStart;
		this.#onInboundDiscard = options.onInboundDiscard;
		this.#onAssistantText = options.onAssistantText;
		this.#onSteerAccepted = options.onSteerAccepted;
		this.#log = options.log ?? ((line: string) => console.error(line));
	}

	/** Call only after inboundEnqueue's durable acceptance boundary. */
	notifyInbound(originKey: string): Promise<void> {
		if (this.#stopped) return Promise.resolve();
		return this.#actor(originKey).enqueue(async () => await this.#actor(originKey).admit());
	}

	/** Running-server stall heartbeat: threshold check only, never an abort. */
	checkStalls(): void {
		if (this.#stopped) return;
		this.#port.checkStalls(this.#now());
	}

	get stopped(): boolean {
		return this.#stopped;
	}

	/** Presentation/recovery tick; the port's stall check never sends an abort. */
	tick(originKey?: string): Promise<void> {
		this.#port.checkStalls(this.#now());
		const actors = originKey ? [this.#actor(originKey)] : [...this.#actors.values()];
		return Promise.all(actors.map((actor) => actor.enqueue(async () => await actor.tick()))).then(() => undefined);
	}

	setStallTimeoutMs(timeoutMs: number | undefined): void {
		this.#stallTimeoutMs = positiveInteger(timeoutMs, DEFAULT_STALL_TIMEOUT_MS, "stallTimeoutMs");
		this.#port.setStallTimeoutMs(this.#stallTimeoutMs);
	}

	/**
	 * `/new` is a mailbox transition: idle/settling work bumps immediately; a
	 * running turn is first accepted, then retired and permanently fenced. The
	 * new epoch returns to idle while the old batch remains a terminal-only hold.
	 */
	reset(originKey: string, originRefJson: string, floorAt = new Date(this.#now()).toISOString()): Promise<void> {
		return this.#actor(originKey).enqueue(async () => await this.#actor(originKey).reset(originRefJson, floorAt));
	}

	/**
	 * `/model` is serialized by this same mailbox. Idle binds and controls the
	 * existing epoch immediately; running waits behind its accepted send and never
	 * aborts, retires, or changes that in-flight operation.
	 */
	rebindModel(originKey: string, selection: GjcModelSelection): Promise<void> {
		return this.#actor(originKey).enqueue(async () => await this.#actor(originKey).rebindModel(selection));
	}

	/** Reconstructs durable accepted/settled batches after a gateway restart. */
	recover(): Promise<void> {
		if (this.#stopped) return Promise.resolve();
		return Promise.all(
			this.#database.inboundNonterminalOrigins().map((originKey) =>
				this.#actor(originKey).enqueue(async () => {
					await this.#actor(originKey).recover();
					await this.#actor(originKey).reconcile();
				}),
			),
		).then(() => undefined);
	}

	/** Tail gaps and broker replacements reconcile status; tail is never terminal authority. */
	reconcile(originKey: string): Promise<void> {
		return this.#actor(originKey).enqueue(async () => await this.#actor(originKey).reconcile());
	}

	onBrokerGeneration(generation: number): Promise<void> {
		return Promise.all([...this.#actors.values()].map((actor) => actor.enqueue(async () => await actor.onBrokerGeneration(generation)))).then(
			() => undefined,
		);
	}

	state(originKey: string): PersonaActorState {
		return this.#actors.get(originKey)?.state ?? "idle";
	}

	async stop(): Promise<void> {
		this.#stopped = true;
		await Promise.all([...this.#actors.values()].map((actor) => actor.stop()));
	}

	/**
	 * Reconcile accepted work for a bounded shutdown window; an unresolved SDK
	 * operation stays durable for the next broker generation rather than blocking
	 * shutdown forever or being aborted.
	 */
	async drain(timeoutMs = 5_000): Promise<void> {
		const deadline = this.#now() + Math.max(0, timeoutMs);
		for (;;) {
			const unresolved = await Promise.all(
				[...this.#actors.values()].map((actor) => actor.enqueue(async () => await actor.drain())),
			);
			if (!unresolved.some(Boolean)) return;
			if (this.#now() >= deadline) {
				await Promise.all(
					[...this.#actors.values()].map((actor) => actor.enqueue(async () => await actor.logShutdownHold())),
				);
				return;
			}
			await Bun.sleep(10);
		}
	}

	#actor(originKey: string): OriginActor {
		let actor = this.#actors.get(originKey);
		if (!actor) {
			actor = new OriginActor(this, originKey);
			this.#actors.set(originKey, actor);
		}
		return actor;
	}

	get database(): GatewayDatabase {
		return this.#database;
	}

	get port(): SessionPort {
		return this.#port;
	}

	get instanceId(): string {
		return this.#instanceId;
	}

	get repo(): string {
		return this.#repo;
	}

	get settleWindowMs(): number {
		return this.#settleWindowMs;
	}

	get stallTimeoutMs(): number {
		return this.#stallTimeoutMs;
	}

	get brokerGeneration(): number {
		return this.#brokerGeneration();
	}

	now(): number {
		return this.#now();
	}

	settleWindowFor(row: InboundMessageRow): number {
		return nonNegativeInteger(this.#settleWindowFor(row), this.#settleWindowMs, "settleWindowMs");
	}

	schedule(work: () => void, delayMs: number): unknown {
		return this.#setTimeout(work, delayMs);
	}

	cancel(timer: unknown): void {
		this.#clearTimeout(timer);
	}

	async startTurn(input: PersonaTurnStartInput): Promise<PersonaTurnLifecycle> {
		if (this.#onTurnStart) return await this.#onTurnStart(input);
		return { text: composeBatchText(input.rows) };
	}

	async discardInbound(messageIds: readonly string[]): Promise<void> {
		await this.#onInboundDiscard?.(messageIds);
	}

	async emitAssistant(input: Parameters<NonNullable<PersonaSessionManagerOptions["onAssistantText"]>>[0]): Promise<void> {
		await this.#onAssistantText?.(input);
	}

	async emitSteer(input: Parameters<NonNullable<PersonaSessionManagerOptions["onSteerAccepted"]>>[0]): Promise<void> {
		await this.#onSteerAccepted?.(input);
	}

	log(line: string): void {
		this.#log(line);
	}
}

type BoundTurn = PersonaTurnIdentity & {
	brokerGeneration: number;
	tail?: TailHandle;
	lifecycle: PersonaTurnLifecycle;
	retired: boolean;
	detached: boolean;
	tailTerminalObserved: boolean;
	tailEvidenceUnavailable: boolean;
	/** Reconcile passes that saw decidable-terminal status while tail terminal evidence was still absent. */
	statusTerminalHolds: number;
	replaceAfterTerminal: boolean;
	nonSteerable: boolean;
};

class OriginActor {
	readonly #manager: PersonaSessionManager;
	readonly originKey: string;
	#queue: Promise<void> = Promise.resolve();
	#state: PersonaActorState = "idle";
	#deadline: number | undefined;
	#timer: unknown;
	#current: BoundTurn | undefined;
	readonly #retired = new Map<string, BoundTurn>();
	readonly #retiredReattachTimers = new Map<string, unknown>();
	readonly #graceTimers = new Set<unknown>();
	#stopped = false;
	readonly #deliveredEvents = new Set<string>();
	#recoveryScanned = false;

	constructor(manager: PersonaSessionManager, originKey: string) {
		this.#manager = manager;
		this.originKey = originKey;
	}

	get state(): PersonaActorState {
		return this.#state;
	}

	enqueue<T>(work: () => Promise<T>): Promise<T> {
		const task = this.#queue.then(work);
		this.#queue = task.then(
			() => undefined,
			(error) => {
				this.#manager.log(`persona actor ${this.originKey} failed: ${safeDiagnostic(error)}`);
			},
		);
		return task;
	}

	async admit(): Promise<void> {
		if (!this.#recoveryScanned) await this.recover();
		if (this.#state === "turn-running") {
			await this.#steerPending();
			return;
		}
		await this.#armSettle();
	}

	async tick(): Promise<void> {
		if (this.#state === "settling" && this.#deadline !== undefined && this.#manager.now() >= this.#deadline)
			await this.#settle();
		if (this.#current) await this.#reconcileBound(this.#current);
		for (const retired of [...this.#retired.values()]) await this.#reconcileBound(retired);
	}

	async reset(originRefJson: string, floorAt: string): Promise<void> {
		this.#clearTimer();
		const previous = this.#current;
		let nextEpoch = 0;
		let discarded: string[] = [];
		this.#manager.database.withTransaction(() => {
			nextEpoch = this.#manager.database.bumpEpoch(this.originKey, originRefJson);
			this.#manager.database.contextSetFloor(this.originKey, floorAt);
			discarded = this.#manager.database.inboundDiscardBefore(this.originKey, floorAt);
		});
		await this.#manager.discardInbound(discarded);
		if (previous) {
			previous.retired = true;
			previous.nonSteerable = true;
			previous.tail?.setTurnRunning(false);
			await previous.tail?.close();
			previous.detached = true;
			this.#retired.set(retiredKey(previous), previous);
			this.#current = undefined;
			await previous.lifecycle.onRetired?.(previous);
			this.#manager.log(
				`retired_hold originKey=${this.originKey} batchKey=${previous.batch.batchKey} epoch=${previous.epoch} opRef=${previous.batch.opRef} reason=/new`,
			);
			this.#scheduleRetiredReattach(previous);
		}
		for (const batch of this.#manager.database.inboundNonterminalBatches(this.originKey)) {
			if (batch.epoch < nextEpoch && !this.#retired.has(`${batch.epoch}:${batch.batchKey}`)) {
				this.#manager.log(`retired_hold originKey=${this.originKey} batchKey=${batch.batchKey} epoch=${batch.epoch} opRef=${batch.opRef} reason=/new`);
			}
		}
		this.#state = "idle";
		this.#deadline = undefined;
		this.#manager.log(`persona_new origin=${this.originKey} epoch=${nextEpoch} discarded_unbatched=${discarded.length}`);
		await this.#armSettle();
	}

	/** Mailbox-serialized live rebind. The caller supplies a verified concrete selection. */
	async rebindModel(selection: GjcModelSelection): Promise<void> {
		const binding = await this.#ensureSession(this.#epoch());
		const receipt = await this.#manager.port.setModel({ sessionId: binding.sessionId, repo: this.#manager.repo, selection });
		this.#manager.log(
			`persona_model origin=${this.originKey} epoch=${binding.epoch} session=${binding.sessionId} effective=${describeModel(selection)} changed=${receipt.changed} source=/model`,
		);
	}

	async reconcile(): Promise<void> {
		if (this.#current) await this.#reconcileBound(this.#current);
		for (const retired of [...this.#retired.values()]) await this.#reconcileBound(retired);
	}

	async recover(): Promise<void> {
		this.#recoveryScanned = true;
		for (const batch of this.#manager.database.inboundNonterminalBatches(this.originKey)) {
			if (this.#current?.batch.batchKey === batch.batchKey || this.#retired.has(retiredKey({ epoch: batch.epoch, batch }))) continue;
			await this.#recoverBatch(batch);
		}
		if (!this.#current) await this.#armSettle();
	}

	async #recoverBatch(batch: InboundBatch): Promise<void> {
		const currentEpoch = this.#epoch();
		const retired = batch.epoch < currentEpoch;
		const record = this.#manager.database.getSessionRecord(this.originKey);
		const sessionId = batch.sessionId ?? (record?.epoch === batch.epoch ? record.sessionId : undefined);
		if (!sessionId) {
			this.#manager.log(
				`recovery_hold origin=${this.originKey} epoch=${batch.epoch} opRef=${batch.opRef} reason=${retired ? "retired_session_binding_unavailable" : "session_binding_unavailable"}`,
			);
			return;
		}

		// The table's authority decision is deliberately inspect -> status -> inspect.
		// A broker replacement between either inspect is an authority shift, never a
		// reason to resend an accepted operation.
		const first = await this.#inspectForRecovery(sessionId);
		const status = await this.#statusForRecovery(sessionId, batch.opRef);
		const second = await this.#inspectForRecovery(sessionId);
		const authorityShifted =
			first.failed ||
			second.failed ||
			status.operationRef !== batch.opRef ||
			!sameRecoveryAuthority(first.session, second.session);
		const session = second.session;


		const recoveryInput = {
			session: {
				live: session?.live ?? false,
				deleted: session?.deleted ?? false,
				...(authorityShifted ? { ambiguous: true } : {}),
				savedAuthorityValid:
					session !== undefined && session.sessionId === sessionId && !session.deleted && session.repo === this.#manager.repo,
				locatorMatches: session?.repo === undefined || session.repo === this.#manager.repo,
				// The instance-scoped op-ref namespace and broker lock leave this actor as
				// the only mutation owner for a persona origin.
				duplicateOwner: false,
			},
			operation: {
				status: status.status.status,
				...(status.status.receiptState ? { receiptState: status.status.receiptState } : {}),
				supervisorState: projectOpState(status.status),
			},
			lane: {
				// Persona side effects are durable ledger deliveries keyed by tail event;
				// replay is idempotent, so completed operation evidence is safe to observe.
				sideEffectsVerified: true,
				// This actor owns one origin key; no independent worktree owner exists.
				ownershipUnchanged: true,
			},
		};
		const decision = decideRecovery(recoveryInput);
		switch (decision.action) {
			case "observe": {
				if (batch.state === "settled") this.#manager.database.inboundBatchAccept(batch.batchKey);
				const bound = await this.#adoptRecoveredBatch(batch, sessionId, retired, true);
				await this.#reconcileBound(bound);
				if (!bound.retired && this.#current === bound) await this.#steerPending();
				return;
			}
			case "fresh_turn": {
				if (batch.state === "settled") this.#manager.database.inboundBatchAccept(batch.batchKey);
				const bound = await this.#adoptRecoveredBatch(batch, sessionId, retired, true);
				// A successful terminal only needs evidence/delivery reconciliation. A
				// failed or interrupted terminal releases the durable inbound rows once,
				// then settles a deterministic replacement on the same live session.
				bound.replaceAfterTerminal = status.status.status === "failed" && !retired;
				await this.#reconcileBound(bound);
				return;
			}
			case "session_resume": {
				try {
					await this.#manager.port.resume({
						sessionId,
						repo: this.#manager.repo,
						originKey: this.originKey,
						epoch: batch.epoch,
					});
					await this.#recoverBatch(batch);
				} catch (error) {
					const afterResumeFailure = decideRecovery({
						...recoveryInput,
						lane: { ...recoveryInput.lane, resumeImpossible: true },
					});
					if (afterResumeFailure.action === "recreate")
						await this.#recreateAfterResumeFailure(batch, retired);
					else
						this.#manager.log(
							`recovery_hold origin=${this.originKey} epoch=${batch.epoch} opRef=${batch.opRef} reason=session_resume_failed detail=${safeDiagnostic(error)}`,
						);
				}
				return;
			}
			case "recreate":
				await this.#recreateAfterResumeFailure(batch, retired);
				return;
			case "operator_hold":
				this.#manager.log(`recovery_hold origin=${this.originKey} epoch=${batch.epoch} opRef=${batch.opRef} reason=${decision.reason}`);
				return;
		}
	}


	async #adoptRecoveredBatch(batch: InboundBatch, sessionId: string, retired: boolean, accepted: boolean): Promise<BoundTurn> {
		this.#manager.database.inboundBatchBindSession(batch.batchKey, sessionId);
		const lifecycle = await this.#manager.startTurn({
			originKey: this.originKey,
			epoch: batch.epoch,
			sessionId,
			batch,
			rows: this.#manager.database.inboundBatchRows(batch.batchKey),
		});
		let tail: TailHandle | undefined;
		let detached = false;
		try {
			tail = await this.#attachTail(sessionId, batch.epoch, retired);
		} catch (error) {
			if (!retired || !(error instanceof TailCapacityError)) throw error;
			detached = true;
			this.#manager.log(`retired_hold originKey=${this.originKey} batchKey=${batch.batchKey} epoch=${batch.epoch} opRef=${batch.opRef} reason=tail_capacity`);
		}
		const bound: BoundTurn = {
			originKey: this.originKey,
			epoch: batch.epoch,
			sessionId,
			brokerGeneration: this.#manager.brokerGeneration,
			batch,
			...(tail ? { tail } : {}),
			lifecycle,
			retired,
			detached,
			tailTerminalObserved: false,
			tailEvidenceUnavailable: false,
			statusTerminalHolds: 0,
			replaceAfterTerminal: false,
			nonSteerable: false,
		};
		tail?.setTurnRunning(true);
		if (accepted && tail) await tail.markAccepted(batch.opRef);
		if (retired) {
			this.#retired.set(retiredKey(bound), bound);
			if (detached) this.#scheduleRetiredReattach(bound);
		} else {
			this.#current = bound;
			this.#state = "turn-running";
		}
		return bound;
	}

	async #recreateAfterResumeFailure(batch: InboundBatch, retired: boolean): Promise<void> {
		const reason = retired ? "resume_impossible" : "tail_terminal_evidence_unavailable";
		this.#manager.log(
			`recovery_hold origin=${this.originKey} epoch=${batch.epoch} opRef=${batch.opRef} reason=${reason}`,
		);
		if (retired)
			this.#manager.log(`retired_hold originKey=${this.originKey} batchKey=${batch.batchKey} epoch=${batch.epoch} opRef=${batch.opRef} reason=resume_impossible`);
	}

	async #inspectForRecovery(sessionId: string): Promise<{ readonly session: BrokerSession | undefined; readonly failed: boolean }> {
		try {
			return { session: await this.#manager.port.inspect({ sessionId, repo: this.#manager.repo }), failed: false };
		} catch {
			return { session: undefined, failed: true };
		}
	}

	async #statusForRecovery(sessionId: string, opRef: string): Promise<StatusReport> {
		try {
			return await this.#manager.port.status({ sessionId, repo: this.#manager.repo, opRef });
		} catch {
			return { operationRef: opRef, status: { status: "unknown" }, summaryCompleted: false };
		}
	}

	async onBrokerGeneration(generation: number): Promise<void> {
		this.#recoveryScanned = false;
		for (const bound of [this.#current, ...this.#retired.values()]) {
			if (!bound || bound.brokerGeneration === generation) continue;
			await bound.tail?.close();
			bound.detached = true;
			this.#manager.log(
				`broker_generation_fenced originKey=${this.originKey} epoch=${bound.epoch} oldGeneration=${bound.brokerGeneration} generation=${generation}`,
			);
			await this.#reconcileBound(bound);
		}
	}

	async stop(): Promise<void> {
		this.#stopped = true;
		this.#clearTimer();
		for (const timer of this.#retiredReattachTimers.values()) this.#manager.cancel(timer);
		this.#retiredReattachTimers.clear();
		for (const timer of this.#graceTimers) this.#manager.cancel(timer);
		this.#graceTimers.clear();
		await Promise.all([
			...(this.#current?.tail ? [this.#current.tail.close()] : []),
			...[...this.#retired.values()].flatMap((bound) => (bound.tail ? [bound.tail.close()] : [])),
		]);
	}

	/** Shutdown reconciliation covers the current turn AND retired holds; both stay durable if unresolved. */
	async drain(): Promise<boolean> {
		if (this.#current) await this.#reconcileBound(this.#current);
		for (const retired of [...this.#retired.values()]) await this.#reconcileBound(retired);
		return this.#current !== undefined || this.#retired.size > 0;
	}

	async logShutdownHold(): Promise<void> {
		if (this.#current)
			this.#manager.log(`shutdown_hold origin=${this.originKey} epoch=${this.#current.epoch} opRef=${this.#current.batch.opRef}`);
	}

	async #armSettle(): Promise<void> {
		const first = this.#manager.database.inboundPendingOldest(this.originKey);
		if (!first) {
			this.#state = "idle";
			this.#deadline = undefined;
			return;
		}
		if (this.#state === "settling") return;
		const arrivedAt = Date.parse(first.received_at);
		if (!Number.isFinite(arrivedAt)) throw new Error(`inbound ${first.message_id} has invalid received_at`);
		this.#state = "settling";
		this.#deadline = arrivedAt + this.#manager.settleWindowFor(first);
		const delay = Math.max(0, this.#deadline - this.#manager.now());
		this.#timer = this.#manager.schedule(() => {
			void this.enqueue(async () => await this.tick()).catch((error: unknown) =>
				this.#manager.log(`persona_settle_failed origin=${this.originKey} detail=${safeDiagnostic(error)}`),
			);
		}, delay);
	}

	async #settle(): Promise<void> {
		if (this.#state !== "settling" || this.#deadline === undefined) return;
		this.#clearTimer();
		const first = this.#manager.database.inboundPendingOldest(this.originKey);
		if (!first) {
			this.#state = "idle";
			this.#deadline = undefined;
			return;
		}
		const epoch = this.#epoch();
		const cutoff = new Date(this.#deadline).toISOString();
		const retryAttempt = this.#manager.database.freshTurnAttempt(this.originKey, epoch, first.message_id);
		const batchKey = personaBatchKey(this.originKey, epoch, first.message_id, cutoff, retryAttempt);
		const opRef = personaBatchOpRef(this.#manager.instanceId, this.originKey, epoch, first.message_id, cutoff, retryAttempt);
		const rows = this.#manager.database.inboundSettleBatch({
			originKey: this.originKey,
			epoch,
			cutoff,
			batchKey,
			opRef,
		});
		if (rows.length === 0) {
			this.#state = "idle";
			this.#deadline = undefined;
			await this.#armSettle();
			return;
		}
		const batch = this.#manager.database.inboundNonterminalBatches(this.originKey, epoch).find((candidate) => candidate.batchKey === batchKey);
		if (!batch) throw new Error(`settled batch ${batchKey} disappeared before send`);
		const binding = await this.#ensureSession(epoch);
		this.#manager.database.inboundBatchBindSession(batchKey, binding.sessionId);
		const lifecycle = await this.#manager.startTurn({
			originKey: this.originKey,
			epoch,
			sessionId: binding.sessionId,
			batch,
			rows,
		});
		const tail = await this.#attachTail(binding.sessionId, epoch, false);
		const current: BoundTurn = {
			originKey: this.originKey,
			epoch,
			sessionId: binding.sessionId,
			brokerGeneration: this.#manager.brokerGeneration,
			batch,
			tail,
			lifecycle,
			retired: false,
			detached: false,
			tailTerminalObserved: false,
			tailEvidenceUnavailable: false,
			statusTerminalHolds: 0,
			replaceAfterTerminal: false,
			nonSteerable: false,
		};
		this.#current = current;
		this.#state = "turn-running";
		tail.setTurnRunning(true);
		try {
			// Session state, rather than a per-send selector, is authoritative for persona
			// turns. The optional fallback exists only for an explicitly unsupported SDK
			// control path and is deliberately absent from normal lifecycle construction.
			const modelReceipt = lifecycle.effectiveModel
				? await this.#manager.port.setModel({
					sessionId: binding.sessionId,
					repo: this.#manager.repo,
					selection: lifecycle.effectiveModel,
				})
				: undefined;
			this.#manager.log(
				`persona_model origin=${this.originKey} epoch=${epoch} session=${binding.sessionId} effective=${describeModel(lifecycle.effectiveModel)} changed=${modelReceipt?.changed ?? false} source=turn`,
			);
			await this.#manager.port.send({
				sessionId: binding.sessionId,
				repo: this.#manager.repo,
				text: lifecycle.text,
				opRef,
				...(lifecycle.systemPreamble ? { systemPreamble: lifecycle.systemPreamble } : {}),
				...(lifecycle.sendModelFallback ? { model: lifecycle.sendModelFallback } : {}),
			});
			this.#manager.database.inboundBatchAccept(batchKey);
			await tail.markAccepted(opRef);
		} catch (error) {
			// A command failure can occur after broker acceptance. Reconcile its exact
			// durable op-ref; an unknown status remains held and is never resent.
			if (
				(error instanceof OpRefRejectedError && error.code === CLIENT_REF_CONFLICT_CODE) ||
				isOpRefRejection(error)
			)
				this.#manager.log(`recovery_client_ref_conflict origin=${this.originKey} epoch=${epoch} opRef=${opRef}`);
			else this.#manager.log(`persona_send_ambiguous origin=${this.originKey} opRef=${opRef} detail=${safeDiagnostic(error)}`);
			await this.#reconcileBound(current);
			if (this.#manager.database.inboundBatchRows(batchKey)[0]?.batch_state !== "settled") return;
			throw error;
		} finally {
			this.#deadline = undefined;
		}
		await this.#steerPending();
	}

	async #steerPending(): Promise<void> {
		const current = this.#current;
		if (!current || current.retired || current.nonSteerable || this.#state !== "turn-running") return;
		for (;;) {
			const row = this.#manager.database.inboundPendingOldest(this.originKey);
			if (!row) return;
			assertControlAllowed("turn.steer", { operatorApproval: true });
			const clientRef = steerClientRef(this.#manager.instanceId, this.originKey, current.epoch, row.message_id);
			try {
				await this.#manager.port.steer({
					sessionId: current.sessionId,
					repo: this.#manager.repo,
					text: row.body,
					clientRef,
				});
			} catch (error) {
				this.#manager.log(
					`steer_ambiguous origin=${this.originKey} message=${row.message_id} detail=${safeDiagnostic(error)}`,
				);
				return;
			}
			if (
				this.#manager.database.inboundSteerAccepted({
					messageId: row.message_id,
					batchKey: current.batch.batchKey,
					epoch: current.epoch,
					opRef: current.batch.opRef,
				})
			) {
				await this.#manager.emitSteer({ originKey: this.originKey, messageId: row.message_id, opRef: current.batch.opRef });
				this.#manager.log(`steer_delivered originKey=${this.originKey} opRef=${current.batch.opRef} messageId=${row.message_id}`);
			}
		}
	}

	async #ensureSession(epoch: number): Promise<SessionBinding> {
		const existing = this.#manager.database.getSessionRecord(this.originKey);
		if (existing?.epoch === epoch && existing.sessionId) {
			const binding = { sessionId: existing.sessionId, originKey: this.originKey, epoch, repo: this.#manager.repo };
			// An idle binding may point at a session the broker no longer hosts
			// (broker restart while idle). Dead + saved authority is resumed through
			// the unchanged decision table's `session.resume` branch BEFORE any send;
			// a dead binding is never handed to a send as if it were live.
			const { session, failed } = await this.#inspectForRecovery(existing.sessionId);
			if (failed || session === undefined || session.live) return binding;
			if (!session.deleted && session.repo === this.#manager.repo) {
				try {
					await this.#manager.port.resume({ sessionId: existing.sessionId, repo: this.#manager.repo, originKey: this.originKey, epoch });
					this.#manager.log(`session_resumed origin=${this.originKey} epoch=${epoch} session=${existing.sessionId} reason=idle_dead_binding`);
					return binding;
				} catch (error) {
					this.#manager.log(`session_resume_failed origin=${this.originKey} epoch=${epoch} session=${existing.sessionId} detail=${safeDiagnostic(error)}`);
				}
			}
			// Deleted or unresumable: fall through to the epoch-scoped idempotent bind,
			// whose rebind policy owns condemnation (SessionRebinder), not this actor.
		}
		const binding = await this.#manager.port.bind({ originKey: this.originKey, epoch, repo: this.#manager.repo });
		this.#manager.database.putSession(this.originKey, binding.sessionId);
		return binding;
	}

	async #attachTail(sessionId: string, epoch: number, retired: boolean): Promise<TailHandle> {
		const generation = this.#manager.brokerGeneration;
		const cursor = this.#manager.database.tailCursorGet(sessionId);
		return await this.#manager.port.attachTail({
			sessionId,
			brokerGeneration: generation,
			repo: this.#manager.repo,
			originKey: this.originKey,
			...(cursor ? { cursor } : {}),
			priority: retired ? "retired" : "current",
			onCursorCommitted: async (nextCursor) => {
				this.#manager.database.tailCursorCommit(sessionId, nextCursor);
			},
			// Awaited on purpose: the TailRunner commits the durable cursor only after
			// this resolves, so ledger/delivery/terminal side effects precede the
			// cursor. Mailbox re-entrancy is safe because TailHandle.markAccepted
			// flushes buffered frames outside the caller's turn (see tail-runner.ts).
			onFrame: async (frame) => {
				await this.enqueue(async () => await this.#onTailFrame(sessionId, epoch, generation, retired, frame));
			},
			onRetentionGap: (gap) => {
				void this.enqueue(async () => await this.#onRetentionGap(sessionId, epoch, generation, retired, gap.resync)).catch((error: unknown) =>
					this.#manager.log(`persona_retention_gap_failed origin=${this.originKey} detail=${safeDiagnostic(error)}`),
				);
			},
			onStall: ({ elapsedMs }) => {
				void this.enqueue(async () => await this.#onStall(sessionId, epoch, generation, retired, elapsedMs)).catch(() => {});
			},
			onDiagnostic: (line) => this.#manager.log(line),
		});
	}

	async #onRetentionGap(
		sessionId: string,
		epoch: number,
		brokerGeneration: number,
		retired: boolean,
		resync: unknown,
	): Promise<void> {
		const bound = this.#findBound(sessionId, epoch, brokerGeneration);
		if (!bound) return;
		bound.tailEvidenceUnavailable = true;
		bound.nonSteerable = true;
		bound.detached = true;
		this.#manager.log(`retention_gap origin=${this.originKey} epoch=${epoch} session=${sessionId}`);
		this.#manager.log(
			`recovery_hold origin=${this.originKey} epoch=${epoch} opRef=${bound.batch.opRef} reason=tail_retention_gap resync=${resyncCoordinate(resync)}`,
		);
		if (retired || bound.retired)
			this.#manager.log(`retired_hold originKey=${this.originKey} batchKey=${bound.batch.batchKey} epoch=${epoch} opRef=${bound.batch.opRef} reason=tail_retention_gap`);
	}

	async #onTailFrame(
		sessionId: string,
		epoch: number,
		brokerGeneration: number,
		retired: boolean,
		frame: TailFrame,
	): Promise<void> {
		const bound = this.#findBound(sessionId, epoch, brokerGeneration);
		if (!bound) return;
		if (bound.retired || retired) {
			if (frame.assistantText) this.#manager.log(`stale_output origin=${this.originKey} epoch=${epoch} session=${sessionId}`);
		} else {
			await bound.lifecycle.onFrame?.({ ...bound, frame });
			if (!bound.lifecycle.onFrame && frame.assistantText && frame.eventId && !frame.steerEcho) {
				const key = `${sessionId}:${frame.eventId}`;
				if (!this.#deliveredEvents.has(key)) {
					this.#deliveredEvents.add(key);
					await this.#manager.emitAssistant({
						originKey: this.originKey,
						sessionId,
						eventId: frame.eventId,
						deliveryId: deterministicTailDeliveryId(sessionId, frame.eventId),
						text: frame.assistantText,
					});
				}
			}
		}
		if (isTerminalTailFrame(frame)) {
			bound.tailTerminalObserved = true;
			await this.#reconcileBound(bound);
		}
	}

	async #onStall(
		sessionId: string,
		epoch: number,
		brokerGeneration: number,
		retired: boolean,
		elapsedMs: number,
	): Promise<void> {
		const bound = this.#findBound(sessionId, epoch, brokerGeneration);
		if (!bound) return;
		this.#manager.log(`stall_alert originKey=${this.originKey} sessionId=${sessionId} silentMs=${elapsedMs}`);
		if (!bound.retired && !retired) await bound.lifecycle.onStall?.({ ...bound, elapsedMs });
		if (retired || bound.retired) {
			bound.tail?.setTurnRunning(false);
			await bound.tail?.close();
			bound.detached = true;
			this.#manager.log(`retired_hold originKey=${this.originKey} batchKey=${bound.batch.batchKey} epoch=${epoch} opRef=${bound.batch.opRef} reason=stall`);
			this.#scheduleRetiredReattach(bound);
		}
	}

	async #reconcileByTuple(sessionId: string, epoch: number, generation: number): Promise<void> {
		const bound = this.#findBound(sessionId, epoch, generation);
		if (bound) await this.#reconcileBound(bound);
	}

	async #reconcileBound(bound: BoundTurn): Promise<void> {
		let report: StatusReport;
		try {
			report = await this.#manager.port.status({ sessionId: bound.sessionId, repo: this.#manager.repo, opRef: bound.batch.opRef });
		} catch (error) {
			bound.nonSteerable = true;
			this.#manager.log(`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.batch.opRef} reason=status_unavailable detail=${safeDiagnostic(error)}`);
			return;
		}
		if (report.status.status === "unknown") {
			bound.nonSteerable = true;
			this.#manager.log(`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.batch.opRef} reason=operation_state_unknown`);
			return;
		}
		const rows = this.#manager.database.inboundBatchRows(bound.batch.batchKey);
		if (rows.length > 0 && rows.every((row) => row.batch_state === "settled")) {
			this.#manager.database.inboundBatchAccept(bound.batch.batchKey);
			if (bound.tail) await bound.tail.markAccepted(bound.batch.opRef);
		}
		bound.nonSteerable = false;
		if (!isTerminalStatus(report.status.status)) {
			if (bound.detached && !bound.tailEvidenceUnavailable) {
				if (bound.retired) this.#scheduleRetiredReattach(bound);
				else await this.#reattachCurrentTail(bound);
			}
			return;
		}
		if (!bound.tailTerminalObserved && !bound.tailEvidenceUnavailable && bound.statusTerminalHolds < 1) {
			// Status is decidable-terminal but the tail has not shown its terminal
			// frame yet. Tail stays the live authority: hold ONCE and give the
			// attached tail a bounded grace to deliver the evidence. If it still
			// has not by the next reconcile, the tail evidence is genuinely
			// unavailable (post-crash ring loss) and status — the subsession
			// reconcile authority — completes the batch below, corroborated by an
			// explicit log line instead of a silent shortcut.
			bound.statusTerminalHolds += 1;
			bound.nonSteerable = true;
			this.#manager.log(`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.batch.opRef} reason=tail_terminal_evidence_unavailable`);
			const timer = this.#manager.schedule(() => {
				this.#graceTimers.delete(timer);
				if (this.#stopped || this.#manager.stopped) return;
				void this.enqueue(async () => {
					if (this.#stopped || this.#manager.stopped || bound.tailTerminalObserved) return;
					bound.tailEvidenceUnavailable = true;
					await this.#reconcileBound(bound);
				}).catch((error: unknown) =>
					this.#manager.log(`persona_reconcile_grace_failed origin=${this.originKey} detail=${safeDiagnostic(error)}`),
				);
			}, STATUS_TERMINAL_GRACE_MS);
			this.#graceTimers.add(timer);
			return;
		}
		if (!bound.tailTerminalObserved) {
			this.#manager.log(
				`terminal_status_reconciled origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.batch.opRef} tail_evidence=unavailable`,
			);
		}
		if (bound.replaceAfterTerminal) {
			this.#manager.database.inboundBatchRequeueFreshTurn(bound.batch.batchKey);
			bound.tail?.setTurnRunning(false);
			await bound.tail?.close();
			if (this.#current === bound) {
				this.#current = undefined;
				this.#state = "idle";
				this.#manager.log(`recovery_fresh_turn origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.batch.opRef}`);
				await this.#armSettle();
			}
			return;
		}
		try {
			if (!bound.retired && report.status.status === "terminal_ok") {
				const assistant = await this.#manager.port.fetchLastAssistant({ sessionId: bound.sessionId, repo: this.#manager.repo });
				await bound.lifecycle.onTerminal?.({ ...bound, text: assistant.text, status: report });
			} else if (!bound.retired) {
				await bound.lifecycle.onFailure?.({ ...bound, error: terminalError(report), status: report });
			}
		} catch (error) {
			this.#manager.log(`persona_terminal_delivery_failed origin=${this.originKey} opRef=${bound.batch.opRef} detail=${safeDiagnostic(error)}`);
			throw error;
		}
		const completed = this.#manager.database.inboundBatchComplete(bound.batch.batchKey);
		if (completed === 0) return;
		bound.tail?.setTurnRunning(false);
		await bound.tail?.close();
		if (bound.retired) {
			this.#retired.delete(retiredKey(bound));
			this.#clearRetiredReattach(bound);
			return;
		}
		if (this.#current === bound) {
			this.#current = undefined;
			this.#state = "idle";
			await this.#armSettle();
		}
	}

	async #reattachCurrentTail(bound: BoundTurn): Promise<void> {
		const tail = await this.#attachTail(bound.sessionId, bound.epoch, false);
		bound.tail = tail;
		bound.brokerGeneration = this.#manager.brokerGeneration;
		bound.detached = false;
		tail.setTurnRunning(true);
		await tail.markAccepted(bound.batch.opRef);
	}

	#scheduleRetiredReattach(bound: BoundTurn, attempt = 0): void {
		const key = retiredKey(bound);
		if (this.#retiredReattachTimers.has(key) || attempt >= RETIRED_REATTACH_MAX_ATTEMPTS || bound.tailEvidenceUnavailable) return;
		const timer = this.#manager.schedule(() => {
			this.#retiredReattachTimers.delete(key);
			void this
				.enqueue(async () => {
					if (this.#retired.get(key) !== bound || !bound.detached || bound.tailEvidenceUnavailable) return;
					try {
						const tail = await this.#attachTail(bound.sessionId, bound.epoch, true);
						bound.tail = tail;
						bound.brokerGeneration = this.#manager.brokerGeneration;
						bound.detached = false;
						tail.setTurnRunning(true);
						await tail.markAccepted(bound.batch.opRef);
					} catch (error) {
						if (error instanceof TailCapacityError) {
							this.#scheduleRetiredReattach(bound, attempt + 1);
							return;
						}
						this.#manager.log(`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.batch.opRef} reason=retired_tail_reattach_failed detail=${safeDiagnostic(error)}`);
					}
				})
				.catch(() => {});
		}, RETIRED_REATTACH_DELAY_MS);
		this.#retiredReattachTimers.set(key, timer);
	}

	#clearRetiredReattach(bound: BoundTurn): void {
		const key = retiredKey(bound);
		const timer = this.#retiredReattachTimers.get(key);
		if (timer !== undefined) this.#manager.cancel(timer);
		this.#retiredReattachTimers.delete(key);
	}

	#findBound(sessionId: string, epoch: number, brokerGeneration: number): BoundTurn | undefined {
		if (
			this.#current?.sessionId === sessionId &&
			this.#current.epoch === epoch &&
			this.#current.brokerGeneration === brokerGeneration
		) {
			return this.#current;
		}
		return [...this.#retired.values()].find(
			(bound) =>
				bound.sessionId === sessionId && bound.epoch === epoch && bound.brokerGeneration === brokerGeneration,
		);
	}

	#epoch(): number {
		return this.#manager.database.getSessionRecord(this.originKey)?.epoch ?? 0;
	}

	#clearTimer(): void {
		if (this.#timer !== undefined) this.#manager.cancel(this.#timer);
		this.#timer = undefined;
	}
}

function terminalError(status: StatusReport): Error {
	return new Error(sanitizeDiagnostic(status.status.error?.message ?? status.status.error?.code ?? `session status ${status.status.status}`));
}

function safeDiagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error";
}

function isTerminalTailFrame(frame: TailFrame): boolean {
	return frame.rawKind === "agent_end" || frame.rawKind === "agent_failed" || frame.idle;
}

function resyncCoordinate(value: unknown): string {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return "unavailable";
	const coordinate = value as { revision?: unknown; generation?: unknown; seq?: unknown };
	return [coordinate.revision, coordinate.generation, coordinate.seq].every((part) => typeof part === "number" && Number.isSafeInteger(part) && part >= 0)
		? `${coordinate.revision}:${coordinate.generation}:${coordinate.seq}`
		: "unavailable";
}

/** Keeps raw platform identifiers in SQLite and derives a safe, fixed-length SDK client reference. */
export function personaBatchOpRef(
	instanceId: string,
	originKey: string,
	epoch: number,
	oldestMessageId: string,
	cutoff: string,
	retryAttempt = 0,
): string {
	const digest = createHash("sha256")
		.update(`${instanceId}|${originKey}|${epoch}|${oldestMessageId}|${cutoff}|${retryAttempt}`)
		.digest("hex")
		.slice(0, 32);
	const opRef = `gw-p-${digest}`;
	assertValidOpRef(opRef);
	return opRef;
}

function describeModel(selection: GjcModelSelection | undefined): string {
	return selection === undefined ? "gjc-default" : typeof selection === "string" ? selection : `preset:${selection.preset}`;
}

function sameRecoveryAuthority(left: BrokerSession | undefined, right: BrokerSession | undefined): boolean {
	if (!left || !right) return left === right;
	return (
		left.sessionId === right.sessionId &&
		left.repo === right.repo &&
		left.stateRoot === right.stateRoot &&
		left.pid === right.pid &&
		left.live === right.live &&
		left.deleted === right.deleted
	);
}

export function personaBatchKey(originKey: string, epoch: number, oldestMessageId: string, cutoff: string, retryAttempt = 0): string {
	return `${originKey}|${epoch}|${oldestMessageId}|${cutoff}|${retryAttempt}`;
}

/** The actor's one prompt body is fixed at the durable boundary; later rows route through steering or the next batch. */
export function composeBatchText(rows: readonly InboundMessageRow[]): string {
	return rows.map((row) => row.body).join("\n");
}

function steerClientRef(instanceId: string, originKey: string, epoch: number, messageId: string): string {
	return `gw-s-${createHash("sha256").update(`${instanceId}|${originKey}|${epoch}|${messageId}`).digest("hex").slice(0, 32)}`;
}

function retiredKey(bound: Pick<BoundTurn, "epoch" | "batch">): string {
	return `${bound.epoch}:${bound.batch.batchKey}`;
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result <= 0) throw new Error(`${name} must be a positive integer`);
	return result;
}

function nonNegativeInteger(value: number | undefined, fallback: number, name: string): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result < 0) throw new Error(`${name} must be a non-negative integer`);
	return result;
}
