import { createHash } from "node:crypto";
import {
	assertControlAllowed,
	assertValidOpRef,
	type BrokerSession,
	CLIENT_REF_CONFLICT_CODE,
	decideRecovery,
	GjcCliError,
	isOpRefRejection,
	isTerminalStatus,
	OpRefRejectedError,
	projectOpState,
	type StatusReport,
} from "@gajaeway/subsession";
import type { GjcModelSelection } from "../config";
import type { GatewayDatabase, InboundMessageRow, InboundTurn } from "../store/db";
import { sanitizeDiagnostic } from "./rebind";
import type { SessionBinding, SessionPort } from "./session-port";
import {
	deterministicInterimDeliveryId,
	TailCapacityError,
	type TailFrame,
	type TailHandle,
	tailFrameTimestampMs,
	tailOperationRef,
} from "./tail-runner";

export const DEFAULT_STALL_TIMEOUT_MS = 120_000;
/**
 * How many times a turn may replace its session after the Router disowns it.
 * Each attempt binds a NEW session (bootstrapped with the last 24h of channel
 * context), so this is bounded work, not a resend of an accepted operation.
 */
const MAX_SEND_REBIND_ATTEMPTS = 3;
const RETIRED_REATTACH_DELAY_MS = 25;
const RETIRED_REATTACH_MAX_ATTEMPTS = 3;
/**
 * Grace before a decidable-terminal status may complete a turn whose tail has
 * not produced terminal evidence. One bounded hold keeps the tail the live
 * authority; after the grace the evidence is treated as genuinely unavailable
 * (post-crash ring loss) and status — the subsession reconcile authority —
 * completes with an explicit corroboration log.
 */
const STATUS_TERMINAL_GRACE_MS = 250;
/**
 * A transcript row is judged against the turn's dispatch floor with this much
 * slack: the host stamps rows and the gateway stamps dispatched_at on two
 * clocks. Same tolerance as SessionPort.fetchAssistantSince.
 */
const TURN_FLOOR_SKEW_MS = 2_000;
const DISPATCH_FAILURE_RETRY_MS = 2_000;
/** Torn steer transports are replayed on the same clientRef this many times before the row is held. */
const STEER_REPLAY_ATTEMPTS = 2;
/** Bind failures back off exponentially from DISPATCH_FAILURE_RETRY_MS up to this ceiling. */
const DISPATCH_FAILURE_RETRY_MAX_MS = 60_000;
/** Consecutive recovery sweeps (60s apart) an unknown op on a live idle session is held before release. */
const HOLD_RELEASE_SWEEPS = 2;

export type PersonaActorState = "idle" | "turn-running";

/**
 * Server-owned chat behavior attached to one durable persona turn. The actor
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
	/**
	 * Renders a message that arrives while this turn runs into the steer text.
	 * Owns the same speaker/place/reply header as the trigger so the model can
	 * tell who spoke; the actor wraps the result with the steer framing.
	 */
	renderSteer?(row: InboundMessageRow): string;
	/**
	 * The platform message a steer row carries, when the unread context window
	 * must be told it was read (undefined for loopback). Consumed in the SAME
	 * transaction as the steer acceptance, so a crash cannot separate them.
	 */
	steerContextMessageId?(row: InboundMessageRow): string | undefined;
	/** The steer landed in the session (durably, context consumed): release transient ownership. */
	onSteerAccepted?(input: PersonaSteerInput): void | Promise<void>;
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
	readonly turn: InboundTurn;
}

export interface PersonaTurnStartInput extends PersonaTurnIdentity {
	/** The trigger row; the one prompt body of this turn. */
	readonly trigger: InboundMessageRow;
}

export interface PersonaTailFrameInput extends PersonaTurnIdentity {
	readonly frame: TailFrame;
}

export interface PersonaSteerInput extends PersonaTurnIdentity {
	readonly row: InboundMessageRow;
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
	readonly stallTimeoutMs?: number;
	readonly brokerGeneration?: () => number;
	readonly now?: () => number;
	readonly setTimeout?: (work: () => void, delayMs: number) => unknown;
	readonly clearTimeout?: (timer: unknown) => void;
	/** Builds the server delivery/bootstrap lifecycle before an accepted SDK send. */
	readonly onTurnStart?: (input: PersonaTurnStartInput) => PersonaTurnLifecycle | Promise<PersonaTurnLifecycle>;
	/** Removes ephemeral request ownership after /new discarded not-yet-dispatched rows. */
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
	/**
	 * Lifecycle-less counterpart of `steerContextMessageId` for a hold resolved
	 * after its turn ended or after a restart.
	 */
	readonly heldSteerContextMessageId?: (row: InboundMessageRow) => string | undefined;
	/**
	 * Lifecycle-less counterpart of `onSteerAccepted`: release transient
	 * ownership of a steer whose acceptance was learnt after the turn's
	 * lifecycle was gone. Context is already consumed durably by then.
	 */
	readonly onHeldSteerAccepted?: (input: {
		originKey: string;
		row: InboundMessageRow;
		opRef: string;
	}) => void | Promise<void>;
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
	#stallTimeoutMs: number;
	readonly #brokerGeneration: () => number;
	readonly #now: () => number;
	readonly #setTimeout: (work: () => void, delayMs: number) => unknown;
	readonly #clearTimeout: (timer: unknown) => void;
	readonly #onTurnStart: PersonaSessionManagerOptions["onTurnStart"];
	readonly #onInboundDiscard: PersonaSessionManagerOptions["onInboundDiscard"];
	readonly #onAssistantText: PersonaSessionManagerOptions["onAssistantText"];
	readonly #onSteerAccepted: PersonaSessionManagerOptions["onSteerAccepted"];
	readonly #onHeldSteerAccepted: PersonaSessionManagerOptions["onHeldSteerAccepted"];
	readonly #heldSteerContextMessageId: PersonaSessionManagerOptions["heldSteerContextMessageId"];
	readonly #log: (line: string) => void;
	readonly #actors = new Map<string, OriginActor>();
	#stopped = false;

	constructor(options: PersonaSessionManagerOptions) {
		this.#database = options.database;
		this.#port = options.port;
		this.#instanceId = options.instanceId;
		this.#repo = options.repo;
		this.#stallTimeoutMs = positiveInteger(options.stallTimeoutMs, DEFAULT_STALL_TIMEOUT_MS, "stallTimeoutMs");
		this.#brokerGeneration = options.brokerGeneration ?? (() => 0);
		this.#now = options.now ?? (() => Date.now());
		this.#setTimeout = options.setTimeout ?? ((work: () => void, delayMs: number) => setTimeout(work, delayMs));
		this.#clearTimeout =
			options.clearTimeout ?? ((timer: unknown) => clearTimeout(timer as ReturnType<typeof setTimeout>));
		this.#onTurnStart = options.onTurnStart;
		this.#onInboundDiscard = options.onInboundDiscard;
		this.#onAssistantText = options.onAssistantText;
		this.#onSteerAccepted = options.onSteerAccepted;
		this.#onHeldSteerAccepted = options.onHeldSteerAccepted;
		this.#heldSteerContextMessageId = options.heldSteerContextMessageId;
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
	 * `/new` is a mailbox transition: idle work bumps immediately; a running turn
	 * is first accepted, then retired and permanently fenced. The new epoch
	 * returns to idle while the old turn remains a terminal-only hold.
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

	/** Reconstructs durable bound/accepted turns after a gateway restart. */
	recover(): Promise<void> {
		if (this.#stopped) return Promise.resolve();
		const origins = new Set<string>([
			...this.#database.inboundNonterminalOrigins(),
			...this.#database.inboundPendingOrigins(),
		]);
		return Promise.all(
			[...origins].map((originKey) =>
				this.#actor(originKey).enqueue(async () => {
					await this.#actor(originKey).recover();
					await this.#actor(originKey).reconcile();
					// Pending rows outside a turn (released before the restart, or
					// arrived while down) are dispatched now; nothing is expired.
					await this.#actor(originKey).admit();
				}),
			),
		).then(() => undefined);
	}

	/** Tail gaps and broker replacements reconcile status; tail is never terminal authority. */
	reconcile(originKey: string): Promise<void> {
		return this.#actor(originKey).enqueue(async () => await this.#actor(originKey).reconcile());
	}

	onBrokerGeneration(generation: number): Promise<void> {
		return Promise.all(
			[...this.#actors.values()].map((actor) => actor.enqueue(async () => await actor.onBrokerGeneration(generation))),
		).then(() => undefined);
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

	get stallTimeoutMs(): number {
		return this.#stallTimeoutMs;
	}

	get brokerGeneration(): number {
		return this.#brokerGeneration();
	}

	now(): number {
		return this.#now();
	}

	schedule(work: () => void, delayMs: number): unknown {
		return this.#setTimeout(work, delayMs);
	}

	cancel(timer: unknown): void {
		this.#clearTimeout(timer);
	}

	async startTurn(input: PersonaTurnStartInput): Promise<PersonaTurnLifecycle> {
		if (this.#onTurnStart) return await this.#onTurnStart(input);
		return { text: input.trigger.body };
	}

	async discardInbound(messageIds: readonly string[]): Promise<void> {
		await this.#onInboundDiscard?.(messageIds);
	}

	async emitAssistant(
		input: Parameters<NonNullable<PersonaSessionManagerOptions["onAssistantText"]>>[0],
	): Promise<void> {
		await this.#onAssistantText?.(input);
	}

	async emitSteer(input: Parameters<NonNullable<PersonaSessionManagerOptions["onSteerAccepted"]>>[0]): Promise<void> {
		await this.#onSteerAccepted?.(input);
	}

	heldSteerContextMessageId(row: InboundMessageRow): string | undefined {
		return this.#heldSteerContextMessageId?.(row);
	}

	async emitHeldSteerAccepted(
		input: Parameters<NonNullable<PersonaSessionManagerOptions["onHeldSteerAccepted"]>>[0],
	): Promise<void> {
		await this.#onHeldSteerAccepted?.(input);
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
	/**
	 * A retired turn whose answer the user still wants: the session was replaced
	 * under it (steer failure), not reset by the user (`/new`). Its output is
	 * delivered and its terminal completes the turn like a current one.
	 */
	answerWanted: boolean;
	tailEvidenceUnavailable: boolean;
	/** Reconcile passes that saw decidable-terminal status while tail terminal evidence was still absent. */
	statusTerminalHolds: number;
	/**
	 * Last assistant text observed on the live tail for THIS turn. Only frames
	 * attributed to the op, or un-attributed rows stamped at/after the dispatch
	 * floor, may set it; a cursorless resync replays pre-turn transcript rows
	 * (no opRef) and those must never become this turn's answer.
	 */
	lastAssistantText?: string;
	/** `dispatched_at` of the turn: stamped at bind, before the send. Absent only for a corrupt row. */
	dispatchedAtMs?: number;
	replaceAfterTerminal: boolean;
};

class OriginActor {
	readonly #manager: PersonaSessionManager;
	readonly originKey: string;
	#queue: Promise<void> = Promise.resolve();
	#state: PersonaActorState = "idle";
	#current: BoundTurn | undefined;
	/** A dispatch retry timer is armed; admissions wait for it instead of re-binding at once. */
	#dispatchRetry: unknown;
	readonly #retired = new Map<string, BoundTurn>();
	readonly #retiredReattachTimers = new Map<string, unknown>();
	readonly #graceTimers = new Set<unknown>();
	readonly #appliedModel = new Map<string, string>();
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

	/**
	 * The canonical rule: a running turn takes the message as a steer; an idle
	 * origin sends it as the next turn. No window, no coalescing.
	 */
	async admit(): Promise<void> {
		if (!this.#recoveryScanned) await this.recover();
		if (this.#state === "turn-running") {
			await this.#steerPending();
			return;
		}
		await this.#dispatchNext();
	}

	async tick(): Promise<void> {
		// A steer held on a turn that has already ended is only ever resolved
		// by a clientRef replay; the periodic tick is that retry when nothing
		// else is admitted.
		await this.#resolveStaleHolds();
		if (this.#current) await this.#reconcileBound(this.#current);
		for (const retired of [...this.#retired.values()]) await this.#reconcileBound(retired);
	}

	async reset(originRefJson: string, floorAt: string): Promise<void> {
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
			previous.tail?.setTurnRunning(false);
			await previous.tail?.close();
			previous.detached = true;
			this.#retired.set(retiredKey(previous), previous);
			this.#current = undefined;
			await previous.lifecycle.onRetired?.(previous);
			this.#manager.log(
				`retired_hold originKey=${this.originKey} epoch=${previous.epoch} opRef=${previous.turn.opRef} reason=/new`,
			);
			this.#scheduleRetiredReattach(previous);
		}
		for (const turn of this.#manager.database.inboundNonterminalTurns(this.originKey)) {
			if (turn.epoch < nextEpoch && !this.#retired.has(`${turn.epoch}:${turn.opRef}`)) {
				this.#manager.log(
					`retired_hold originKey=${this.originKey} epoch=${turn.epoch} opRef=${turn.opRef} reason=/new`,
				);
			}
		}
		this.#state = "idle";
		this.#manager.log(`persona_new origin=${this.originKey} epoch=${nextEpoch} discarded_pending=${discarded.length}`);
		await this.#dispatchNext();
	}

	/** Mailbox-serialized live rebind. The caller supplies a verified concrete selection. */
	async rebindModel(selection: GjcModelSelection): Promise<void> {
		const binding = await this.#ensureSession(this.#epoch());
		const receipt = await this.#manager.port.setModel({
			sessionId: binding.sessionId,
			repo: this.#manager.repo,
			selection,
		});
		this.#appliedModel.set(binding.sessionId, describeModel(selection));
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
		for (const turn of this.#manager.database.inboundNonterminalTurns(this.originKey)) {
			if (this.#current?.turn.opRef === turn.opRef || this.#retired.has(retiredKey({ epoch: turn.epoch, turn })))
				continue;
			try {
				await this.#recoverTurn(turn);
			} catch (error) {
				// One unrecoverable turn must not abort recovery of the others.
				this.#manager.log(
					`recovery_turn_failed origin=${this.originKey} epoch=${turn.epoch} opRef=${turn.opRef} detail=${safeDiagnostic(error)}`,
				);
			}
		}
		if (!this.#current) await this.#dispatchNext();
	}

	async #recoverTurn(turn: InboundTurn): Promise<void> {
		const currentEpoch = this.#epoch();
		const retired = turn.epoch < currentEpoch;
		const sessionId = turn.sessionId;
		if (!sessionId) {
			// A turn is bound with its session in one statement; a null session
			// can only be a hand-edited or corrupt row. Hold it for the operator.
			this.#manager.log(
				`recovery_hold origin=${this.originKey} epoch=${turn.epoch} opRef=${turn.opRef} reason=${retired ? "retired_session_binding_unavailable" : "session_binding_unavailable"}`,
			);
			return;
		}

		// The table's authority decision is deliberately inspect -> status -> inspect.
		// A broker replacement between either inspect is an authority shift, never a
		// reason to resend an accepted operation.
		const first = await this.#inspectForRecovery(sessionId);
		const status = await this.#statusForRecovery(sessionId, turn.opRef);
		const second = await this.#inspectForRecovery(sessionId);
		// A bound (never acknowledged) turn whose session the runtime cannot answer
		// for at all (both inspects failed AND status is unreachable) — e.g. a
		// pre-cutover store the current gjc no longer reads — has no operation the
		// runtime could be holding, so releasing its trigger back to pending is
		// exactly-once-safe: the next dispatch binds a live session. A reachable
		// runtime, even with an unknown status, keeps the hold: it may have
		// accepted the send.
		const disownedByBroker =
			status.unreachable === true &&
			(status.unreachableCode === "session_unavailable" || (first.failed && second.failed));
		// An ACCEPTED op is only released when the broker disowns the id AND the
		// session is provably not live (inspect answered live=false, or is gone):
		// nothing can still be running there, so the fresh-turn re-fire is
		// exactly-once-safe. A merely unreachable but possibly-live session holds.
		const raw = this.#manager.port.liveness
			? await this.#manager.port.liveness({ sessionId, repo: this.#manager.repo })
			: undefined;
		const sessionDead =
			(first.session !== undefined && first.session.live === false) ||
			(first.failed && second.failed) ||
			raw?.live === false ||
			raw?.disowned === true;
		const releasable = turn.state === "bound" || (turn.state === "accepted" && sessionDead);
		if (releasable && !retired && status.status.status === "unknown" && disownedByBroker) {
			const attempt = this.#manager.database.inboundTurnRequeue(turn.opRef);
			// The binding itself is unusable: recreate through the existing rebind
			// primitive (epoch bump) so the next dispatch binds a fresh live session.
			const nextEpoch = this.#manager.database.rebindEpoch(this.originKey);
			this.#manager.log(
				`recovery_requeue_unaccepted origin=${this.originKey} epoch=${turn.epoch} nextEpoch=${nextEpoch} opRef=${turn.opRef} session=${sessionId} attempt=${attempt}`,
			);
			return;
		}
		const authorityShifted =
			first.failed ||
			second.failed ||
			status.operationRef !== turn.opRef ||
			!sameRecoveryAuthority(first.session, second.session);
		const session = second.session;

		// gjc >= 0.16.0 omits locator.repo, so the subsession normalizer yields
		// undefined for a perfectly known session; the raw envelope is the
		// liveness authority and the normalized record only adds repo/deleted.
		const rawLive = raw?.live;
		const knownById = rawLive !== undefined && raw?.disowned !== true;
		const recoveryInput = {
			session: {
				live: session?.live ?? rawLive ?? false,
				deleted: session?.deleted ?? false,
				...(authorityShifted ? { ambiguous: true } : {}),
				savedAuthorityValid:
					(session !== undefined &&
						session.sessionId === sessionId &&
						!session.deleted &&
						session.repo === this.#manager.repo) ||
					(session === undefined && knownById),
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
				if (turn.state === "bound") this.#manager.database.inboundTurnAccept(turn.opRef);
				const bound = await this.#adoptRecoveredTurn(turn, sessionId, retired, true);
				await this.#reconcileBound(bound);
				if (!bound.retired && this.#current === bound) await this.#steerPending();
				return;
			}
			case "fresh_turn": {
				if (turn.state === "bound") this.#manager.database.inboundTurnAccept(turn.opRef);
				const bound = await this.#adoptRecoveredTurn(turn, sessionId, retired, true);
				// A successful terminal only needs evidence/delivery reconciliation. A
				// failed or interrupted terminal releases the trigger once, then
				// dispatches a deterministic replacement on the same live session.
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
						epoch: turn.epoch,
					});
					await this.#recoverTurn(turn);
				} catch (error) {
					const afterResumeFailure = decideRecovery({
						...recoveryInput,
						lane: { ...recoveryInput.lane, resumeImpossible: true },
					});
					if (afterResumeFailure.action === "recreate") await this.#recreateAfterResumeFailure(turn, retired);
					else
						this.#manager.log(
							`recovery_hold origin=${this.originKey} epoch=${turn.epoch} opRef=${turn.opRef} reason=session_resume_failed detail=${safeDiagnostic(error)}`,
						);
				}
				return;
			}
			case "recreate":
				await this.#recreateAfterResumeFailure(turn, retired);
				return;
			case "operator_hold": {
				// A live session whose runtime has NO record of this op-ref and whose
				// prompt queue is empty cannot be running it: the send never landed
				// (gateway restarted between send and ack). Release + rebind after the
				// hold has persisted across HOLD_RELEASE_SWEEPS consecutive sweeps so a
				// transient index lag never triggers a duplicate.
				const count = (this.#holdSweeps.get(turn.opRef) ?? 0) + 1;
				this.#holdSweeps.set(turn.opRef, count);
				// Only a BOUND (never acknowledged) turn may be released on a live
				// session. An ACCEPTED op on a live session is held until its terminal
				// arrives: the runtime answering "unknown" while a host boots is not
				// proof the send was lost, and re-firing it double-posts
				// (layofflabs-2, 2026-09-02).
				const liveIdle = status.status.status === "unknown" && raw?.live === true && !retired && turn.state === "bound";
				if (liveIdle && count >= HOLD_RELEASE_SWEEPS && (await this.#queueIsEmpty(sessionId))) {
					this.#holdSweeps.delete(turn.opRef);
					const attempt = this.#manager.database.inboundTurnRequeue(turn.opRef);
					const nextEpoch = this.#manager.database.rebindEpoch(this.originKey);
					this.#manager.log(
						`recovery_requeue_unaccepted origin=${this.originKey} epoch=${turn.epoch} nextEpoch=${nextEpoch} opRef=${turn.opRef} session=${sessionId} attempt=${attempt} reason=unknown_op_on_live_idle_session sweeps=${count}`,
					);
					await this.#dispatchNext();
					return;
				}
				this.#manager.log(
					`recovery_hold origin=${this.originKey} epoch=${turn.epoch} opRef=${turn.opRef} reason=${decision.reason} sweeps=${count}`,
				);
				return;
			}
		}
	}

	async #adoptRecoveredTurn(
		turn: InboundTurn,
		sessionId: string,
		retired: boolean,
		accepted: boolean,
	): Promise<BoundTurn> {
		const trigger = this.#manager.database.inboundTurnRow(turn.opRef);
		if (!trigger) throw new Error(`turn ${turn.opRef} disappeared during recovery`);
		const lifecycle = await this.#manager.startTurn({
			originKey: this.originKey,
			epoch: turn.epoch,
			sessionId,
			turn,
			trigger,
		});
		let tail: TailHandle | undefined;
		let detached = false;
		try {
			tail = await this.#attachTail(sessionId, turn.epoch, retired);
		} catch (error) {
			if (!retired || !(error instanceof TailCapacityError)) throw error;
			detached = true;
			this.#manager.log(
				`retired_hold originKey=${this.originKey} epoch=${turn.epoch} opRef=${turn.opRef} reason=tail_capacity`,
			);
		}
		const dispatchedAtMs = this.#dispatchFloorMs(turn.opRef);
		const bound: BoundTurn = {
			originKey: this.originKey,
			epoch: turn.epoch,
			sessionId,
			brokerGeneration: this.#manager.brokerGeneration,
			turn,
			...(tail ? { tail } : {}),
			lifecycle,
			retired,
			detached,
			tailTerminalObserved: false,
			answerWanted: false,
			tailEvidenceUnavailable: false,
			statusTerminalHolds: 0,
			...(dispatchedAtMs === undefined ? {} : { dispatchedAtMs }),
			replaceAfterTerminal: false,
		};
		tail?.setTurnRunning(true);
		if (accepted && tail) await tail.markAccepted(turn.opRef);
		if (retired) {
			this.#retired.set(retiredKey(bound), bound);
			if (detached) this.#scheduleRetiredReattach(bound);
		} else {
			this.#current = bound;
			this.#state = "turn-running";
		}
		return bound;
	}

	async #recreateAfterResumeFailure(turn: InboundTurn, retired: boolean): Promise<void> {
		const reason = retired ? "resume_impossible" : "tail_terminal_evidence_unavailable";
		this.#manager.log(
			`recovery_hold origin=${this.originKey} epoch=${turn.epoch} opRef=${turn.opRef} reason=${reason}`,
		);
		if (retired)
			this.#manager.log(
				`retired_hold originKey=${this.originKey} epoch=${turn.epoch} opRef=${turn.opRef} reason=resume_impossible`,
			);
	}

	/**
	 * A steer that fails never makes the message disposable. Two causes:
	 *
	 * 1. The turn already ended (nothing to steer into). Reconciling completes
	 *    it and the pending row becomes the next turn's prompt in the SAME
	 *    session - the canonical `idle -> send` rule.
	 * 2. The turn is still running but the session refuses the steer: the
	 *    session is broken. Retire the turn and bump the epoch so the next
	 *    dispatch binds a NEW session (a fresh one is bootstrapped with the last
	 *    24h of channel context) and the still-pending row becomes that turn's
	 *    prompt. Unlike a `/new` retire, the user never asked to discard this
	 *    turn: its tail stays attached and its answer is still delivered.
	 */
	async #recoverFromSteerFailure(current: BoundTurn): Promise<void> {
		await this.#reconcileBound(current);
		if (this.#current !== current) return;
		if (current.statusTerminalHolds > 0 && !current.tailEvidenceUnavailable) {
			// Status is terminal and the bounded tail grace is pending; when it
			// fires the turn completes and the row is dispatched.
			return;
		}
		current.retired = true;
		current.answerWanted = true;
		this.#retired.set(retiredKey(current), current);
		this.#current = undefined;
		const nextEpoch = this.#manager.database.rebindEpoch(this.originKey);
		this.#state = "idle";
		this.#manager.log(
			`session_rebound_after_steer_failure origin=${this.originKey} epoch=${current.epoch} nextEpoch=${nextEpoch} opRef=${current.turn.opRef}`,
		);
		await this.#dispatchNext();
	}

	readonly #holdSweeps = new Map<string, number>();

	async #queueIsEmpty(sessionId: string): Promise<boolean> {
		const port = this.#manager.port;
		if (!port.queueEmpty) return false;
		try {
			return await port.queueEmpty({ sessionId, repo: this.#manager.repo });
		} catch {
			return false;
		}
	}

	async #inspectForRecovery(
		sessionId: string,
	): Promise<{ readonly session: BrokerSession | undefined; readonly failed: boolean }> {
		try {
			return { session: await this.#manager.port.inspect({ sessionId, repo: this.#manager.repo }), failed: false };
		} catch {
			return { session: undefined, failed: true };
		}
	}

	async #statusForRecovery(
		sessionId: string,
		opRef: string,
	): Promise<StatusReport & { unreachable?: boolean; unreachableCode?: string }> {
		try {
			return await this.#manager.port.status({ sessionId, repo: this.#manager.repo, opRef });
		} catch (error) {
			// Transport/session-unreachable, as opposed to a reachable runtime that
			// reported an undecidable operation state. `session_unavailable` is the
			// broker itself disowning the id: nothing can be in flight there.
			return {
				operationRef: opRef,
				status: { status: "unknown" },
				summaryCompleted: false,
				unreachable: true,
				...(sdkStatusErrorCode(error) ? { unreachableCode: sdkStatusErrorCode(error) } : {}),
			};
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
		for (const timer of this.#retiredReattachTimers.values()) this.#manager.cancel(timer);
		this.#retiredReattachTimers.clear();
		for (const timer of this.#graceTimers) this.#manager.cancel(timer);
		this.#graceTimers.clear();
		this.#dispatchRetry = undefined;
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
			this.#manager.log(
				`shutdown_hold origin=${this.originKey} epoch=${this.#current.epoch} opRef=${this.#current.turn.opRef}`,
			);
	}

	/**
	 * Sends the oldest pending row as the next turn, immediately. A dispatch
	 * that cannot even bind a session is retried with exponential backoff
	 * (bounded by DISPATCH_FAILURE_RETRY_MAX_MS, never abandoned); while a retry
	 * is armed, new admissions wait for it instead of hammering the broker. The
	 * row stays pending throughout and is never expired.
	 */
	async #dispatchNext(): Promise<void> {
		if (this.#state !== "idle" || this.#current || this.#dispatchRetry) return;
		await this.#resolveStaleHolds();
		const trigger = this.#manager.database.inboundPendingOldest(this.originKey);
		if (!trigger) return;
		const epoch = this.#epoch();
		const retryAttempt = this.#manager.database.freshTurnAttempt(this.originKey, epoch, trigger.message_id);
		const opRef = personaTurnOpRef(this.#manager.instanceId, this.originKey, epoch, trigger.message_id, retryAttempt);
		let binding: SessionBinding;
		try {
			binding = await this.#ensureSession(epoch);
		} catch (error) {
			this.#noteBindFailure(trigger.message_id, epoch, error);
			return;
		}
		const bound = this.#manager.database.inboundBindTurn({
			messageId: trigger.message_id,
			originKey: this.originKey,
			epoch,
			opRef,
			sessionId: binding.sessionId,
		});
		const turn: InboundTurn = {
			originKey: this.originKey,
			epoch,
			state: "bound",
			opRef,
			sessionId: binding.sessionId,
			triggerMessageId: trigger.message_id,
		};
		const lifecycle = await this.#manager.startTurn({
			originKey: this.originKey,
			epoch,
			sessionId: binding.sessionId,
			turn,
			trigger: bound,
		});
		const tail = await this.#attachTail(binding.sessionId, epoch, false);
		const dispatchedAtMs = this.#dispatchFloorMs(opRef);
		const current: BoundTurn = {
			originKey: this.originKey,
			epoch,
			sessionId: binding.sessionId,
			brokerGeneration: this.#manager.brokerGeneration,
			turn,
			tail,
			lifecycle,
			retired: false,
			detached: false,
			tailTerminalObserved: false,
			answerWanted: false,
			tailEvidenceUnavailable: false,
			statusTerminalHolds: 0,
			...(dispatchedAtMs === undefined ? {} : { dispatchedAtMs }),
			replaceAfterTerminal: false,
		};
		this.#current = current;
		this.#state = "turn-running";
		tail.setTurnRunning(true);
		// Session state, rather than a per-send selector, is authoritative for
		// persona turns. Model selection happens BEFORE send; if it fails, no
		// prompt can have landed, so holding the BOUND row as an ambiguous send
		// would brick the origin. Release it to plain pending and retry with
		// backoff instead.
		const modelKey = describeModel(lifecycle.effectiveModel);
		let modelReceipt: { readonly changed: boolean } | undefined;
		try {
			modelReceipt =
				lifecycle.effectiveModel && this.#appliedModel.get(binding.sessionId) !== modelKey
					? await this.#manager.port.setModel({
							sessionId: binding.sessionId,
							repo: this.#manager.repo,
							selection: lifecycle.effectiveModel,
						})
					: undefined;
			if (lifecycle.effectiveModel) this.#appliedModel.set(binding.sessionId, modelKey);
			this.#preSendFailures = 0;
			this.#manager.log(
				`persona_model origin=${this.originKey} epoch=${epoch} session=${binding.sessionId} effective=${modelKey} changed=${modelReceipt?.changed ?? false} source=turn`,
			);
		} catch (error) {
			await tail.close();
			const attempt = this.#manager.database.inboundTurnRequeue(opRef);
			this.#current = undefined;
			this.#state = "idle";
			this.#preSendFailures += 1;
			const failures = this.#preSendFailures;
			this.#manager.log(
				`persona_model_failed origin=${this.originKey} epoch=${epoch} session=${binding.sessionId} message=${trigger.message_id} attempt=${attempt} failures=${failures} selection=${modelKey} detail=${safeDiagnostic(error)}`,
			);
			this.#scheduleDispatchRetry(
				Math.min(DISPATCH_FAILURE_RETRY_MAX_MS, DISPATCH_FAILURE_RETRY_MS * 2 ** Math.min(failures - 1, 10)),
			);
			return;
		}
		try {
			await this.#manager.port.send({
				sessionId: binding.sessionId,
				repo: this.#manager.repo,
				text: lifecycle.text,
				opRef,
				...(lifecycle.systemPreamble ? { systemPreamble: lifecycle.systemPreamble } : {}),
				...(lifecycle.sendModelFallback ? { model: lifecycle.sendModelFallback } : {}),
			});
			this.#manager.database.inboundTurnAccept(opRef);
			await tail.markAccepted(opRef);
			this.#bindFailures = 0;
			this.#bindEpochPoisoned = false;
		} catch (error) {
			// The Router disowning the session id is NOT ambiguous: it is proof the
			// send never landed, so there is nothing to protect by holding. gjc is
			// allowed to be unreliable here - surviving that is this gateway's job.
			// Holding instead left the conversation dead with one message pending,
			// an empty gjc_session_id and no retry, until a human restarted the
			// daemon (live: epoch 41, session_unavailable, 2026-09-03).
			if (sdkStatusErrorCode(error) === "session_unavailable") {
				await tail.close();
				this.#manager.database.inboundTurnRequeue(opRef);
				const nextEpoch = this.#manager.database.rebindEpoch(this.originKey);
				this.#current = undefined;
				this.#state = "idle";
				// The per-trigger fresh-turn ordinal restarts with the epoch, so the
				// burst is counted at the actor: consecutive "no usable session"
				// failures (bind OR send) until one dispatch succeeds. Bounded burst
				// of immediate replacements - each a new session bootstrapped with
				// the last 24h of channel context - then the condition is logged as
				// unrecoverable for the operator and retried with backoff. The
				// message is never dropped.
				this.#bindFailures += 1;
				const attempts = this.#bindFailures;
				this.#manager.log(
					`persona_send_session_gone origin=${this.originKey} epoch=${epoch} nextEpoch=${nextEpoch} opRef=${opRef} attempt=${attempts}`,
				);
				if (attempts < MAX_SEND_REBIND_ATTEMPTS) await this.#dispatchNext();
				else {
					if (attempts === MAX_SEND_REBIND_ATTEMPTS)
						this.#manager.log(
							`persona_send_unrecoverable origin=${this.originKey} opRef=${opRef} attempts=${attempts} reason=session_unavailable`,
						);
					this.#scheduleDispatchRetry(
						Math.min(DISPATCH_FAILURE_RETRY_MAX_MS, DISPATCH_FAILURE_RETRY_MS * 2 ** Math.min(attempts - 1, 10)),
					);
				}
				return;
			}
			// A command failure can occur after broker acceptance. Reconcile its exact
			// durable op-ref; an unknown status is an operator hold that the periodic
			// reconcile keeps sweeping, and it is never resent.
			if ((error instanceof OpRefRejectedError && error.code === CLIENT_REF_CONFLICT_CODE) || isOpRefRejection(error))
				this.#manager.log(`recovery_client_ref_conflict origin=${this.originKey} epoch=${epoch} opRef=${opRef}`);
			else
				this.#manager.log(
					`persona_send_ambiguous origin=${this.originKey} opRef=${opRef} detail=${safeDiagnostic(error)}`,
				);
			await this.#reconcileBound(current);
			return;
		}
		await this.#steerPending();
	}

	/**
	 * The ONE place a recorded steer acceptance is finalized: durable state
	 * (steer done AND its platform message consumed from the unread window, in
	 * one transaction - a crash cannot leave the message unread), then the
	 * lifecycle's ownership release - or, when the turn's lifecycle is gone
	 * (resolved after terminal or after a restart), the manager-level
	 * equivalent - then the external observer. Every path that learns of an
	 * acceptance (live, replay, terminal, stale hold) goes through here.
	 */
	async #finalizeSteerAcceptance(
		row: InboundMessageRow,
		epoch: number,
		opRef: string,
		bound: BoundTurn | undefined,
	): Promise<boolean> {
		const contextMessageId = bound
			? bound.lifecycle.steerContextMessageId?.(row)
			: this.#manager.heldSteerContextMessageId(row);
		if (!this.#manager.database.inboundSteerAccepted({ messageId: row.message_id, epoch, opRef, contextMessageId }))
			return false;
		if (bound) await bound.lifecycle.onSteerAccepted?.({ ...bound, row });
		else await this.#manager.emitHeldSteerAccepted({ originKey: this.originKey, row, opRef });
		await this.#manager.emitSteer({ originKey: this.originKey, messageId: row.message_id, opRef });
		this.#manager.log(`steer_delivered originKey=${this.originKey} opRef=${opRef} messageId=${row.message_id}`);
		return true;
	}

	/**
	 * A steer held on a turn that has since ended can only be resolved by
	 * replaying its clientRef: the runtime answers with the recorded outcome.
	 * Accepted -> done input of that old turn; refused -> an ordinary pending
	 * row that the dispatch below will send; still torn -> keeps waiting, and
	 * newer rows are NOT blocked behind it.
	 */
	async #resolveStaleHolds(): Promise<void> {
		for (const held of this.#manager.database.inboundSteersHeldAfterTerminal(this.originKey)) {
			const opRef = held.turn_op_ref;
			const epoch = held.turn_epoch;
			const sessionId = held.bound_session_id ?? this.#manager.database.inboundTurnRow(opRef ?? "")?.bound_session_id;
			if (!opRef || epoch === null || !sessionId) continue;
			const clientRef = steerClientRef(this.#manager.instanceId, this.originKey, epoch, held.message_id);
			try {
				await this.#manager.port.steer({
					sessionId,
					repo: this.#manager.repo,
					text: renderSteer(held.body),
					clientRef,
				});
				await this.#finalizeSteerAcceptance(held, epoch, opRef, undefined);
			} catch (error) {
				if (isDefinitiveSteerRejection(error)) this.#manager.database.inboundSteerRefused(held.message_id, opRef);
				else
					this.#manager.log(
						`steer_hold origin=${this.originKey} message=${held.message_id} opRef=${opRef} reason=unresolved_after_terminal detail=${safeDiagnostic(error)}`,
					);
			}
		}
	}

	#bindFailures = 0;
	#preSendFailures = 0;
	/** A terminal_uncertain bind poisons this epoch's idempotency key; retries cannot make that key decidable. */
	#bindEpochPoisoned = false;

	/**
	 * A session could not be bound for the next turn (broker down, Router
	 * disowning every candidate). The message is never dropped; the retry backs
	 * off, and once MAX_SEND_REBIND_ATTEMPTS consecutive attempts have failed
	 * the condition is logged as unrecoverable so an operator sees it - retries
	 * continue at the ceiling because the broker may come back.
	 */
	#noteBindFailure(messageId: string, epoch: number, error: unknown): void {
		this.#bindFailures += 1;
		const attempts = this.#bindFailures;
		const detail = safeDiagnostic(error);
		if (detail.includes("terminal_uncertain")) this.#bindEpochPoisoned = true;
		this.#manager.log(
			`persona_bind_failed origin=${this.originKey} epoch=${epoch} message=${messageId} attempts=${attempts} detail=${detail}`,
		);
		if (attempts === MAX_SEND_REBIND_ATTEMPTS)
			this.#manager.log(
				`persona_send_unrecoverable origin=${this.originKey} message=${messageId} attempts=${attempts} reason=bind_failed`,
			);
		if (this.#bindEpochPoisoned && attempts >= MAX_SEND_REBIND_ATTEMPTS) {
			// bind() failed BEFORE inboundBindTurn, so no prompt was sent and no
			// operation belongs to this row. terminal_uncertain is attached to the
			// epoch-scoped session-create idempotency key; retrying that same key
			// forever cannot recover it (live: one channel repeated it 335 times).
			// Advance the epoch to derive a new key while leaving every inbound row
			// pending and intact, then retry with the normal base delay.
			const nextEpoch = this.#manager.database.rebindEpoch(this.originKey);
			this.#manager.log(
				`persona_bind_epoch_rotated origin=${this.originKey} epoch=${epoch} nextEpoch=${nextEpoch} message=${messageId} attempts=${attempts} reason=terminal_uncertain`,
			);
			this.#bindFailures = 0;
			this.#bindEpochPoisoned = false;
			this.#scheduleDispatchRetry(DISPATCH_FAILURE_RETRY_MS);
			return;
		}
		const delay = Math.min(DISPATCH_FAILURE_RETRY_MAX_MS, DISPATCH_FAILURE_RETRY_MS * 2 ** Math.min(attempts - 1, 10));
		this.#scheduleDispatchRetry(delay);
	}

	#scheduleDispatchRetry(delayMs: number): void {
		if (this.#dispatchRetry) return;
		const timer = this.#manager.schedule(() => {
			this.#graceTimers.delete(timer);
			this.#dispatchRetry = undefined;
			if (this.#stopped || this.#manager.stopped) return;
			void this.enqueue(async () => await this.#dispatchNext()).catch(() => {});
		}, delayMs);
		this.#graceTimers.add(timer);
		this.#dispatchRetry = timer;
	}

	/**
	 * Every message the user sends goes into the live session, immediately.
	 *
	 * A `nonSteerable` flag used to gate this. It was set by seven reconcile
	 * paths that could not decide what happened to the OPERATION - a statement
	 * about completing the turn, never about whether the user may speak. Gating
	 * ingestion on it meant one undecidable turn silenced the conversation: the
	 * rows stayed unbatched, the stale floor deleted them ten minutes later, and
	 * the user's messages were gone without ever reaching the model (live: four
	 * DMs eaten behind an 85-minute wedge, 2026-09-03). A retired turn is still
	 * excluded - it no longer exists to steer into. If the session has in fact
	 * already finished the turn, the steer fails and #recoverFromSteerFailure
	 * turns the row into the next send.
	 */
	async #steerPending(): Promise<void> {
		const current = this.#current;
		if (!current || current.retired || this.#state !== "turn-running") return;
		// Steers whose transport tore before an answer are resolved first, on
		// the same clientRef, before any new row is issued behind them.
		for (const held of this.#manager.database.inboundSteersHeld(current.turn.opRef))
			if (!(await this.#steerRow(current, held))) return;
		for (;;) {
			const row = this.#manager.database.inboundPendingOldest(this.originKey);
			if (!row) return;
			if (!(await this.#steerRow(current, row))) return;
		}
	}

	/**
	 * Issues one row into the running turn. Returns false when the loop must
	 * stop: the outcome is still unknown (row held, durably attributed to this
	 * turn) or the session refused and is being recovered.
	 */
	async #steerRow(current: BoundTurn, row: InboundMessageRow): Promise<boolean> {
		{
			assertControlAllowed("turn.steer", { operatorApproval: true });
			const clientRef = steerClientRef(this.#manager.instanceId, this.originKey, current.epoch, row.message_id);
			// Durable BEFORE the first attempt: from here the row belongs to this
			// turn. A torn transport leaves it `steer/bound` - never dispatched as
			// a trigger, retried on this clientRef by the next admission, tick or
			// restart - until the runtime records acceptance or refuses.
			this.#manager.database.inboundSteerIssued({
				messageId: row.message_id,
				epoch: current.epoch,
				opRef: current.turn.opRef,
			});
			const steer = {
				sessionId: current.sessionId,
				repo: this.#manager.repo,
				text: renderSteer(current.lifecycle.renderSteer?.(row) ?? row.body),
				clientRef,
			};
			let outcome: "accepted" | "refused" | "ambiguous" = "accepted";
			let failure: unknown;
			// The transport can fail AFTER the request landed (CLI killed mid-print,
			// socket reset). The clientRef is durable on the gjc side, so replaying
			// it returns the recorded outcome instead of delivering twice. Only an
			// ok:false envelope is a decision; a torn transport is replayed a
			// bounded number of times and, if it stays torn, the row is HELD - the
			// message may already be inside the running turn, and sending it to a
			// replacement session would deliver it twice. The next admission or
			// reconcile retries the same clientRef.
			for (let attempt = 0; attempt <= STEER_REPLAY_ATTEMPTS; attempt++) {
				try {
					await this.#manager.port.steer(steer);
					outcome = "accepted";
					break;
				} catch (error) {
					failure = error;
					if (isDefinitiveSteerRejection(error)) {
						outcome = "refused";
						break;
					}
					outcome = "ambiguous";
					if (attempt < STEER_REPLAY_ATTEMPTS)
						this.#manager.log(
							`steer_ambiguous origin=${this.originKey} message=${row.message_id} action=replay attempt=${attempt + 1} detail=${safeDiagnostic(error)}`,
						);
				}
			}
			if (outcome === "ambiguous") {
				this.#manager.log(
					`steer_hold origin=${this.originKey} message=${row.message_id} opRef=${current.turn.opRef} reason=transport_torn detail=${safeDiagnostic(failure)}`,
				);
				return false;
			}
			if (outcome === "refused") {
				// The session answered and said no: the message could not reach it,
				// but it is not disposable. It is an ordinary pending row again;
				// either the turn is over (send it when idle) or the session is
				// broken (replace it) - the row stays for the turn that follows.
				this.#manager.database.inboundSteerRefused(row.message_id, current.turn.opRef);
				this.#manager.log(
					`steer_failed origin=${this.originKey} message=${row.message_id} action=recover detail=${safeDiagnostic(failure)}`,
				);
				await this.#recoverFromSteerFailure(current);
				return false;
			}
			await this.#finalizeSteerAcceptance(row, current.epoch, current.turn.opRef, current);
		}
		return true;
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
					await this.#manager.port.resume({
						sessionId: existing.sessionId,
						repo: this.#manager.repo,
						originKey: this.originKey,
						epoch,
					});
					this.#manager.log(
						`session_resumed origin=${this.originKey} epoch=${epoch} session=${existing.sessionId} reason=idle_dead_binding`,
					);
					return binding;
				} catch (error) {
					this.#manager.log(
						`session_resume_failed origin=${this.originKey} epoch=${epoch} session=${existing.sessionId} detail=${safeDiagnostic(error)}`,
					);
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
				void this.enqueue(
					async () => await this.#onRetentionGap(sessionId, epoch, generation, retired, gap.resync),
				).catch((error: unknown) =>
					this.#manager.log(`persona_retention_gap_failed origin=${this.originKey} detail=${safeDiagnostic(error)}`),
				);
			},
			onStall: ({ elapsedMs }) => {
				void this.enqueue(async () => await this.#onStall(sessionId, epoch, generation, retired, elapsedMs)).catch(
					() => {},
				);
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
		bound.detached = true;
		this.#manager.log(`retention_gap origin=${this.originKey} epoch=${epoch} session=${sessionId}`);
		this.#manager.log(
			`recovery_hold origin=${this.originKey} epoch=${epoch} opRef=${bound.turn.opRef} reason=tail_retention_gap resync=${resyncCoordinate(resync)}`,
		);
		if (retired || bound.retired)
			this.#manager.log(
				`retired_hold originKey=${this.originKey} epoch=${epoch} opRef=${bound.turn.opRef} reason=tail_retention_gap`,
			);
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
		if (this.#predatesTurn(bound, frame)) {
			// A cursorless tail re-attach (tail_gap_nonstrict resync=null) replays
			// transcript rows from BEFORE this turn. They carry no opRef, so the
			// runner's attribution fence cannot catch them; their host `ts` can.
			// Letting one through made the previous turn's answer ship under the
			// current trigger (live, 2026-09-03: every reply one turn late).
			this.#manager.log(
				`tail_frame_pre_turn origin=${this.originKey} epoch=${epoch} session=${sessionId} event=${frame.eventId ?? "unidentified"}`,
			);
			return;
		}
		if ((bound.retired || retired) && !bound.answerWanted) {
			if (frame.assistantText)
				this.#manager.log(`stale_output origin=${this.originKey} epoch=${epoch} session=${sessionId}`);
		} else {
			if (frame.assistantText && !frame.steerEcho) bound.lastAssistantText = frame.assistantText;
			await bound.lifecycle.onFrame?.({ ...bound, frame });
			if (!bound.lifecycle.onFrame && frame.assistantText && frame.eventId && !frame.steerEcho) {
				const key = `${sessionId}:${frame.eventId}`;
				if (!this.#deliveredEvents.has(key)) {
					this.#deliveredEvents.add(key);
					await this.#manager.emitAssistant({
						originKey: this.originKey,
						sessionId,
						eventId: frame.eventId,
						deliveryId: deterministicInterimDeliveryId(
							this.originKey,
							bound.turn.triggerMessageId,
							frame.assistantText,
							0,
						),
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
			this.#manager.log(
				`retired_hold originKey=${this.originKey} epoch=${epoch} opRef=${bound.turn.opRef} reason=stall`,
			);
			this.#scheduleRetiredReattach(bound);
		}
	}

	async #reconcileBound(bound: BoundTurn): Promise<void> {
		let report: StatusReport;
		try {
			report = await this.#manager.port.status({
				sessionId: bound.sessionId,
				repo: this.#manager.repo,
				opRef: bound.turn.opRef,
			});
		} catch (error) {
			// The broker disowning the id (session_unavailable) with the session
			// provably not live means nothing is running there: release the turn
			// and rebind instead of holding an adopted turn forever.
			if (sdkStatusErrorCode(error) === "session_unavailable" && !bound.retired) {
				const raw = this.#manager.port.liveness
					? await this.#manager.port.liveness({ sessionId: bound.sessionId, repo: this.#manager.repo })
					: undefined;
				// A BOUND (never acknowledged) turn is safe to re-fire on the broker's
				// word alone. An ACCEPTED turn may have run side effects: it is
				// released only on positive evidence that the session is dead
				// (live=false or disowned); an unanswerable liveness probe holds it.
				const state = this.#manager.database.inboundTurnRow(bound.turn.opRef)?.turn_state;
				const dead = raw?.live === false || raw?.disowned === true;
				if (state === "bound" ? raw?.live !== true : dead) {
					await bound.tail?.close();
					const attempt = this.#manager.database.inboundTurnRequeue(bound.turn.opRef);
					const nextEpoch = this.#manager.database.rebindEpoch(this.originKey);
					if (this.#current === bound) {
						this.#current = undefined;
						this.#state = "idle";
					}
					this.#manager.log(
						`recovery_requeue_unaccepted origin=${this.originKey} epoch=${bound.epoch} nextEpoch=${nextEpoch} opRef=${bound.turn.opRef} session=${bound.sessionId} attempt=${attempt} reason=router_disowned`,
					);
					await this.#dispatchNext();
					return;
				}
			}
			this.#manager.log(
				`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef} reason=status_unavailable detail=${safeDiagnostic(error)}`,
			);
			return;
		}
		if (report.status.status === "unknown") {
			const count = (this.#holdSweeps.get(bound.turn.opRef) ?? 0) + 1;
			this.#holdSweeps.set(bound.turn.opRef, count);
			const state = this.#manager.database.inboundTurnRow(bound.turn.opRef)?.turn_state;
			// This is the live counterpart of startup's operator_hold recovery.
			// A BOUND turn has no broker acknowledgement. When the live session
			// reports no such operation and its prompt queue remains empty across
			// two sweeps, the send did not land; holding it forever bricks the
			// origin. ACCEPTED work is different and remains protected from replay.
			if (state === "bound" && !bound.retired && count >= HOLD_RELEASE_SWEEPS) {
				let live: boolean | undefined;
				let disowned = false;
				try {
					if (this.#manager.port.liveness) {
						const liveness = await this.#manager.port.liveness({
							sessionId: bound.sessionId,
							repo: this.#manager.repo,
						});
						live = liveness.live;
						disowned = liveness.disowned === true;
					} else {
						live = (await this.#manager.port.inspect({ sessionId: bound.sessionId, repo: this.#manager.repo }))?.live;
					}
				} catch {
					// An indeterminate liveness probe is not release evidence.
				}
				const dead = live === false || disowned;
				const liveIdle = live === true && (await this.#queueIsEmpty(bound.sessionId));
				if (dead || liveIdle) {
					this.#holdSweeps.delete(bound.turn.opRef);
					await bound.tail?.close();
					const attempt = this.#manager.database.inboundTurnRequeue(bound.turn.opRef);
					const nextEpoch = this.#manager.database.rebindEpoch(this.originKey);
					if (this.#current === bound) {
						this.#current = undefined;
						this.#state = "idle";
					}
					this.#manager.log(
						`recovery_requeue_unaccepted origin=${this.originKey} epoch=${bound.epoch} nextEpoch=${nextEpoch} opRef=${bound.turn.opRef} session=${bound.sessionId} attempt=${attempt} reason=${dead ? "unknown_op_on_dead_session" : "unknown_op_on_live_idle_session"} sweeps=${count}`,
					);
					await this.#dispatchNext();
					return;
				}
			}
			this.#manager.log(
				`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef} reason=operation_state_unknown sweeps=${count}`,
			);
			return;
		}
		this.#holdSweeps.delete(bound.turn.opRef);
		if (this.#manager.database.inboundTurnRow(bound.turn.opRef)?.turn_state === "bound") {
			this.#manager.database.inboundTurnAccept(bound.turn.opRef);
			if (bound.tail) await bound.tail.markAccepted(bound.turn.opRef);
		}
		if (!isTerminalStatus(report.status.status)) {
			if (bound.detached && !bound.tailEvidenceUnavailable) {
				// A turn the user still wants answered is re-attached like the
				// current one; only a discarded (`/new`) hold is deferred.
				if (bound.retired && !bound.answerWanted) this.#scheduleRetiredReattach(bound);
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
			// reconcile authority — completes the turn below, corroborated by an
			// explicit log line instead of a silent shortcut.
			bound.statusTerminalHolds += 1;
			this.#manager.log(
				`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef} reason=tail_terminal_evidence_unavailable`,
			);
			const timer = this.#manager.schedule(() => {
				this.#graceTimers.delete(timer);
				if (this.#stopped || this.#manager.stopped) return;
				void this.enqueue(async () => {
					if (this.#stopped || this.#manager.stopped || bound.tailTerminalObserved) return;
					// An earlier reconcile (e.g. a refused steer) may already have
					// completed this turn from status; it is no longer tracked then.
					if (this.#current !== bound && !this.#retired.has(retiredKey(bound))) return;
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
				`terminal_status_reconciled origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef} tail_evidence=unavailable`,
			);
		}
		if (bound.replaceAfterTerminal) {
			this.#manager.database.inboundTurnRequeue(bound.turn.opRef);
			bound.tail?.setTurnRunning(false);
			await bound.tail?.close();
			if (this.#current === bound) {
				this.#current = undefined;
				this.#state = "idle";
				this.#manager.log(
					`recovery_fresh_turn origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef}`,
				);
				await this.#dispatchNext();
			}
			return;
		}
		try {
			if ((!bound.retired || bound.answerWanted) && report.status.status === "terminal_ok") {
				// The transcript is the authority for the answer text. The live tail's
				// lastAssistantText is turn-scoped too (see #predatesTurn) but it is
				// an observation of a stream that can be re-attached, replayed and
				// fenced; the durable row produced at/after this turn's floor is what
				// the model actually said for THIS trigger. The runtime's startedAt
				// is stamped by the SAME host clock as the transcript rows, so it is
				// the exact floor whenever it is present; our dispatched_at (stamped
				// at bind, BEFORE the send, cleared on requeue) is the fallback for
				// a runtime that omits startedAt. Mixing the two clocks (a max) was
				// tried and rejected: a gateway clock ahead of the host would hide
				// the real answer.
				const startedAt = report.status.startedAt;
				const notBeforeMs = typeof startedAt === "number" ? startedAt : bound.dispatchedAtMs;
				const port = this.#manager.port;
				let text: string | undefined;
				if (notBeforeMs !== undefined && port.fetchAssistantSince) {
					try {
						const since = await port.fetchAssistantSince({
							sessionId: bound.sessionId,
							repo: this.#manager.repo,
							notBeforeMs,
						});
						text = since?.text;
					} catch (error) {
						this.#manager.log(
							`terminal_text_unavailable origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef} reason=transcript_read_failed detail=${safeDiagnostic(error)}`,
						);
					}
				}
				if (text === undefined && bound.tailTerminalObserved && bound.lastAssistantText !== undefined) {
					// The transcript had no row at/after the floor (or could not be
					// read) but the live tail carried a turn-scoped answer.
					text = bound.lastAssistantText;
				}
				if (text === undefined) {
					if (notBeforeMs === undefined) {
						// No trustworthy floor (a pre-v18 batch already bound before the
						// upgrade, on a runtime that omits startedAt) and no tail text.
						// Completing with "" would discard a real answer; posting the
						// unbounded last row could repost the previous turn. Hold for
						// the operator instead.
						this.#manager.log(
							`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef} reason=no_turn_floor`,
						);
						return;
					}
					this.#manager.log(
						`terminal_text_unavailable origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef} reason=no_assistant_row_since_start`,
					);
					text = "";
				}
				await bound.lifecycle.onTerminal?.({ ...bound, text, status: report });
			} else if (!bound.retired || bound.answerWanted) {
				await bound.lifecycle.onFailure?.({ ...bound, error: terminalError(report), status: report });
			}
		} catch (error) {
			this.#manager.log(
				`persona_terminal_delivery_failed origin=${this.originKey} opRef=${bound.turn.opRef} detail=${safeDiagnostic(error)}`,
			);
			throw error;
		}
		const completed = this.#manager.database.inboundTurnComplete(bound.turn.opRef);
		if (completed === 0) return;
		// A steer issued into this turn whose answer tore: try the clientRef one
		// more time now that the turn is over (the runtime still holds the
		// recorded outcome). What stays unresolved is held on this op-ref for
		// the operator - never dispatched as a new turn, never silently dropped.
		for (const held of this.#manager.database.inboundSteersHeld(bound.turn.opRef)) {
			const clientRef = steerClientRef(this.#manager.instanceId, this.originKey, bound.epoch, held.message_id);
			try {
				await this.#manager.port.steer({
					sessionId: bound.sessionId,
					repo: this.#manager.repo,
					text: renderSteer(bound.lifecycle.renderSteer?.(held) ?? held.body),
					clientRef,
				});
				await this.#finalizeSteerAcceptance(held, bound.epoch, bound.turn.opRef, bound);
			} catch (error) {
				if (isDefinitiveSteerRejection(error)) {
					// Never reached the turn: an ordinary pending message for the next one.
					this.#manager.database.inboundSteerRefused(held.message_id, bound.turn.opRef);
				} else {
					this.#manager.log(
						`steer_hold origin=${this.originKey} message=${held.message_id} opRef=${bound.turn.opRef} reason=unresolved_at_terminal detail=${safeDiagnostic(error)}`,
					);
				}
			}
		}
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
			await this.#dispatchNext();
		}
	}

	async #reattachCurrentTail(bound: BoundTurn): Promise<void> {
		const tail = await this.#attachTail(bound.sessionId, bound.epoch, false);
		bound.tail = tail;
		bound.brokerGeneration = this.#manager.brokerGeneration;
		bound.detached = false;
		tail.setTurnRunning(true);
		await tail.markAccepted(bound.turn.opRef);
	}

	#scheduleRetiredReattach(bound: BoundTurn, attempt = 0): void {
		const key = retiredKey(bound);
		if (
			this.#retiredReattachTimers.has(key) ||
			attempt >= RETIRED_REATTACH_MAX_ATTEMPTS ||
			bound.tailEvidenceUnavailable
		)
			return;
		const timer = this.#manager.schedule(() => {
			this.#retiredReattachTimers.delete(key);
			void this.enqueue(async () => {
				if (this.#retired.get(key) !== bound || !bound.detached || bound.tailEvidenceUnavailable) return;
				try {
					const tail = await this.#attachTail(bound.sessionId, bound.epoch, true);
					bound.tail = tail;
					bound.brokerGeneration = this.#manager.brokerGeneration;
					bound.detached = false;
					tail.setTurnRunning(true);
					await tail.markAccepted(bound.turn.opRef);
				} catch (error) {
					if (error instanceof TailCapacityError) {
						this.#scheduleRetiredReattach(bound, attempt + 1);
						return;
					}
					this.#manager.log(
						`recovery_hold origin=${this.originKey} epoch=${bound.epoch} opRef=${bound.turn.opRef} reason=retired_tail_reattach_failed detail=${safeDiagnostic(error)}`,
					);
				}
			}).catch(() => {});
		}, RETIRED_REATTACH_DELAY_MS);
		this.#retiredReattachTimers.set(key, timer);
	}

	#clearRetiredReattach(bound: BoundTurn): void {
		const key = retiredKey(bound);
		const timer = this.#retiredReattachTimers.get(key);
		if (timer !== undefined) this.#manager.cancel(timer);
		this.#retiredReattachTimers.delete(key);
	}

	#dispatchFloorMs(opRef: string): number | undefined {
		const dispatchedAt = this.#manager.database.inboundTurnDispatchedAt(opRef);
		const at = dispatchedAt ? Date.parse(dispatchedAt) : Number.NaN;
		return Number.isFinite(at) ? at : undefined;
	}

	/**
	 * An un-attributed transcript row stamped before this turn's dispatch floor
	 * is history replayed by a cursorless resync, not this turn's output. A
	 * frame attributed to the accepted op is always this turn's, and a frame
	 * with no host timestamp (live lifecycle frames) cannot be judged and passes.
	 */
	#predatesTurn(bound: BoundTurn, frame: TailFrame): boolean {
		if (bound.dispatchedAtMs === undefined) return false;
		if (tailOperationRef(frame) === bound.turn.opRef) return false;
		const at = tailFrameTimestampMs(frame);
		return at !== undefined && at + TURN_FLOOR_SKEW_MS < bound.dispatchedAtMs;
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
			(bound) => bound.sessionId === sessionId && bound.epoch === epoch && bound.brokerGeneration === brokerGeneration,
		);
	}

	#epoch(): number {
		return this.#manager.database.getSessionRecord(this.originKey)?.epoch ?? 0;
	}
}

function terminalError(status: StatusReport): Error {
	return new Error(
		sanitizeDiagnostic(
			status.status.error?.message ?? status.status.error?.code ?? `session status ${status.status.status}`,
		),
	);
}

function safeDiagnostic(error: unknown): string {
	return sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "sdk_error";
}

function isTerminalTailFrame(frame: TailFrame): boolean {
	return frame.rawKind === "agent_end" || frame.rawKind === "agent_failed" || frame.idle;
}

/**
 * A steer is injected into a turn that is already reasoning about the trigger.
 * Without framing the model treats the newest text as the whole task and drops
 * the original request (live: answered "답하셈", ignored the question).
 */
export function renderSteer(body: string): string {
	return `[Additional message from the user, received while you were still working on their previous request. Finish that request, then also address this. Do not restart or repeat what you already said.]\n${body}`;
}

/**
 * A `turn.steer` the runtime itself answered with `ok:false` (no running turn,
 * rejected text, unknown session) is a decision. A non-zero exit, a torn
 * envelope or a thrown transport error is not: the steer may or may not have
 * been recorded, and only a clientRef replay can tell.
 */
function isDefinitiveSteerRejection(error: unknown): boolean {
	return error instanceof GjcCliError && error.exitCode === 0 && error.details !== undefined;
}

function sdkStatusErrorCode(error: unknown): string | undefined {
	const code = (error as { code?: unknown } | undefined)?.code;
	if (typeof code === "string" && /^[a-z0-9_.-]{1,64}$/i.test(code)) return code;
	const message = error instanceof Error ? error.message : "";
	return /session_unavailable/.test(message) ? "session_unavailable" : undefined;
}

function resyncCoordinate(value: unknown): string {
	if (typeof value !== "object" || value === null || Array.isArray(value)) return "unavailable";
	const coordinate = value as { revision?: unknown; generation?: unknown; seq?: unknown };
	return [coordinate.revision, coordinate.generation, coordinate.seq].every(
		(part) => typeof part === "number" && Number.isSafeInteger(part) && part >= 0,
	)
		? `${coordinate.revision}:${coordinate.generation}:${coordinate.seq}`
		: "unavailable";
}

/** Keeps raw platform identifiers in SQLite and derives a safe, fixed-length SDK client reference. */
export function personaTurnOpRef(
	instanceId: string,
	originKey: string,
	epoch: number,
	triggerMessageId: string,
	retryAttempt = 0,
): string {
	const digest = createHash("sha256")
		.update(`${instanceId}|${originKey}|${epoch}|${triggerMessageId}|${retryAttempt}`)
		.digest("hex")
		.slice(0, 32);
	const opRef = `gw-p-${digest}`;
	assertValidOpRef(opRef);
	return opRef;
}

function describeModel(selection: GjcModelSelection | undefined): string {
	return selection === undefined
		? "gjc-default"
		: typeof selection === "string"
			? selection
			: `preset:${selection.preset}`;
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

function steerClientRef(instanceId: string, originKey: string, epoch: number, messageId: string): string {
	return `gw-s-${createHash("sha256").update(`${instanceId}|${originKey}|${epoch}|${messageId}`).digest("hex").slice(0, 32)}`;
}

function retiredKey(bound: Pick<BoundTurn, "epoch" | "turn">): string {
	return `${bound.epoch}:${bound.turn.opRef}`;
}

function positiveInteger(value: number | undefined, fallback: number, name: string): number {
	const result = value ?? fallback;
	if (!Number.isSafeInteger(result) || result <= 0) throw new Error(`${name} must be a positive integer`);
	return result;
}
