import type { BrokerOperationReceipt } from "../broker/cli";
import { MainSessionGateRegistry, type GateHandle, type MainGateResolution } from "./gates";
import {
	attestsExternalTranscriptGrowth,
	compareTailCheckpoints,
	fingerprintTranscriptEntries,
	GatewayStateStore,
	sameExternalFingerprint,
	sameExternalSession,
	type ExternalSessionIdentity,
	type GrowthIntent,
	type TailCheckpoint,
	type TranscriptDeliveryProgress,
	type TranscriptProof,
} from "./state";
import {
	classifyAdmissionDisposition,
	HostSupervisorError,
	type AdmissionDisposition,
	type HostSupervisor,
	type SupervisorEvent,
	type SupervisorTailEvents,
	type SupervisorTranscriptEntry,
} from "./supervisor";


export interface MainSessionJournal {
	journalAppend(kind: string, payloadJson: string): unknown;
	journalAppendAtTailCheckpoint?(kind: string, payloadJson: string, expected: TailCheckpoint | undefined, checkpoint: TailCheckpoint): unknown;
	/** Atomically persists the journal projection, tail watermark, and delivery replay point. */
	journalAppendTranscriptProjection(
		kind: string,
		payloadJson: string,
		expectedTail: TailCheckpoint | undefined,
		checkpoint: TailCheckpoint,
		expectedDelivery: TranscriptDeliveryProgress | undefined,
		nextDelivery: TranscriptDeliveryProgress,
	): unknown;
	setRpcHealth?(state: "degraded" | "running", reason: string): void;
	setMainSessionStatus?(turnState: "idle" | "busy", followUpQueueDepth: number, verificationState: "pending" | "verified"): void;
	setJournalDegraded?(degraded: boolean): void;
}

export class MainSessionHostError extends Error {
	readonly reason: string;
	/** Present only for a broker admission failure classified at the host boundary. */
	readonly admissionDisposition?: AdmissionDisposition;

	constructor(
		reason: string,
		message = reason,
		options: { readonly cause?: unknown; readonly admissionDisposition?: AdmissionDisposition } = {},
	) {
		super(message, options.cause === undefined ? undefined : { cause: options.cause });
		this.name = "MainSessionHostError";
		this.reason = reason;
		this.admissionDisposition = options.admissionDisposition;
	}
}

/** A durable broker-attempt-to-origin-surface binding retained for delayed projection. */
export interface MainSessionAdmissionAttribution {
	readonly attemptIds: readonly string[];
	readonly surfaceId: string;
}

export interface PersistedMainSessionAdmissionAttribution {
	readonly attemptIdsJson: string;
	readonly surfaceId: string;
}

/** Rejects corrupt durable attribution rather than guessing from operation-ref shapes. */
export function parseMainSessionAdmissionAttributions(
	rows: readonly PersistedMainSessionAdmissionAttribution[],
): readonly MainSessionAdmissionAttribution[] {
	const surfaceByAttemptId = new Map<string, string>();
	return rows.map((row) => {
		if (typeof row.surfaceId !== "string" || !row.surfaceId.trim()) {
			throw new MainSessionHostError("main_admission_attribution_invalid", "A durable main-admission surface attribution is invalid.");
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(row.attemptIdsJson) as unknown;
		} catch (error) {
			throw new MainSessionHostError("main_admission_attribution_invalid", "A durable main-admission attempt attribution is not JSON.", {
				cause: error,
			});
		}
		if (
			!Array.isArray(parsed) ||
			parsed.length === 0 ||
			parsed.some((attemptId) => typeof attemptId !== "string" || !attemptId) ||
			new Set(parsed).size !== parsed.length
		) {
			throw new MainSessionHostError("main_admission_attribution_invalid", "A durable main-admission attempt attribution is malformed.");
		}
		for (const attemptId of parsed) {
			const existing = surfaceByAttemptId.get(attemptId);
			if (existing !== undefined && existing !== row.surfaceId) {
				throw new MainSessionHostError("main_admission_attribution_invalid", "A broker attempt has conflicting durable surface attributions.");
			}
			surfaceByAttemptId.set(attemptId, row.surfaceId);
		}
		return { attemptIds: parsed, surfaceId: row.surfaceId };
	});
}

export interface MainSessionHost {
	readonly sessionId: string;
	readonly identity: ExternalSessionIdentity;
	readonly degraded: boolean;
	readonly turnState: "idle" | "busy";
	readonly followUpQueueDepth: number;
	/** The sole mutation fence consulted before any main-session claim or effect. */
	readonly mutationReadinessReason: string | undefined;
	readonly gates: MainSessionGateRegistry;
	/** Surface origin for the currently busy turn; absent when autonomous or ambiguous. */
	readonly turnOriginSurfaceId: string | undefined;
	/** Resolves once this daemon has a complete, compatible transcript tail. */
	waitForVerifiedTranscript(): Promise<void>;
	/** Waits for broker admission only; successful turn execution is observed asynchronously. */
	/** The finalizer is armed before dispatch; the receipt callback persists identifiers after acceptance. */
	admit(
		deliveredAs: "prompt" | "steer" | "follow_up",
		text: string,
		opRef: string,
		finalizePendingClaim?: () => void,
		recordAttemptIds?: (attemptIds: readonly string[]) => void,
		/** Canonical admitted surface persisted with receipt aliases for journal egress. */
		surfaceId?: string,
	): Promise<void>;
	/** Degrades and fences the host when durable abandonment of a definitive rejection fails. */
	reportAdmissionClaimAbandonFailure(error: unknown): void;
	resolveGate(gateId: string, answer: unknown, idempotencyKey: string): Promise<MainGateResolution>;
	waitForFatalFailure(): Promise<MainSessionHostError>;
	dispose(): Promise<void>;
}

export interface CreateMainSessionHostOptions {
	readonly supervisor: HostSupervisor;
	readonly identity: ExternalSessionIdentity;
	readonly state: GatewayStateStore;
	readonly journal: MainSessionJournal;
	readonly initialTurnState?: "idle" | "busy";
	readonly initialFollowUpQueueDepth?: number;
	/** Per-boot complete-tail verification state returned by strict resume. */
	readonly initialVerificationState?: "pending" | "verified";
	/** Complete restart verification tail, consumed before the first fresh broker tail request. */
	readonly verificationTail?: SupervisorTailEvents;
	/** An active durable growth intent recovered by strict resume. */
	readonly recoveredGrowthIntent?: GrowthIntent;
	/** Receipt-bound origin mappings loaded from durable admission records before tail projection. */
	readonly initialAdmissionAttributions?: readonly MainSessionAdmissionAttribution[];
	/** Test-only seam after durable terminal evidence but before pending-claim finalization. */
	readonly afterTerminalEvidenceBeforeAdmissionFinalize?: () => void;
	readonly now?: () => number;
	readonly gates?: MainSessionGateRegistry;
}

interface GrowthWindow {
	readonly intent: GrowthIntent;
	pendingAdmissions: number;
}

interface AdmittedOperation {
	/** Durable ring watermark immediately before broker acceptance. */
	readonly admittedAt: TailCheckpoint | undefined;
	/** Every broker identifier that can appear in this operation's tail attempt. */
	readonly attemptIds: readonly string[];
	/** Persists receipt-derived attempt identifiers while this exact claim remains pending. */
	readonly recordAttemptIds?: (attemptIds: readonly string[]) => void;
	/** Originating surface when this live admission was accepted through main.submit. */
	readonly surfaceId?: string;
	/** A broker receipt was lost after dispatch, so all mutations stay fenced until a tail settles it. */
	readonly ambiguous: boolean;
	/** Exact idempotency finalizer armed before broker dispatch. */
	readonly finalizePendingClaim?: () => void;
}

/**
 * A bounded bridge for brokers that publish terminal ring evidence before the
 * corresponding transcript row. It carries exact receipt aliases from the
 * settled admission; it is never synthesized from transcript text or suffixes.
 */
interface PendingTranscriptAttribution {
	readonly attemptIds: readonly string[];
	readonly surfaceId: string;
	readonly settlementSequence: number;
	readonly tailEpoch: number;
}

interface JournaledAttemptTransitions {
	started: boolean;
	ended: boolean;
}

interface FinalAssistantMessage {
	readonly key: string;
	readonly payload: {
		readonly finalized: true;
		readonly text: string;
		readonly message_id?: string;
		readonly timestamp?: number;
		readonly surface_id?: string;
	};
}

export interface MainSessionTurnJournalPayload {
	readonly attempt_id: string;
	readonly generation: number;
	readonly lineage: string;
	readonly surface_id?: string;
}

const MAX_JOURNALED_ATTEMPTS = 1_000;

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function eventString(event: Record<string, unknown>, ...keys: string[]): string | undefined {
	for (const key of keys) {
		const value = event[key];
		if (typeof value === "string" && value) return value;
	}
	return undefined;
}

function distinctAttemptIds(values: readonly (string | undefined)[]): string[] {
	const ids = new Set<string>();
	for (const value of values) {
		if (typeof value === "string" && value) ids.add(value);
	}
	return [...ids];
}

/** Matches only a receipt-derived canonical attempt ID; never infer ownership from an ID suffix. */
export function matchesCanonicalAdmissionAttemptId(attemptId: string, canonicalAttemptIds: Iterable<string>): boolean {
	for (const canonicalAttemptId of canonicalAttemptIds) {
		if (attemptId === canonicalAttemptId) return true;
	}
	return false;
}

function admissionAttemptIds(sessionId: string, opRef: string, receipt?: BrokerOperationReceipt): string[] {
	return distinctAttemptIds([
		opRef,
		`${sessionId}:${opRef}`,
		receipt?.operationRef,
		receipt?.commandId,
		receipt?.turnId,
	]);
}

function eventTimestamp(event: Record<string, unknown>): number | undefined {
	for (const key of ["expires_at", "expiresAt", "deadline_at", "deadlineAt", "timestamp"]) {
		const value = event[key];
		if (typeof value === "number" && Number.isFinite(value)) return value;
		if (typeof value === "string") {
			const parsed = Date.parse(value);
			if (Number.isFinite(parsed)) return parsed;
		}
	}
	return undefined;
}

function gateFromEvent(event: unknown, fallbackSessionId: string): GateHandle | undefined {
	if (!isRecord(event)) return undefined;
	const gateId = eventString(event, "gate_id", "gateId", "workflowGateId", "id");
	if (!gateId) return undefined;
	return {
		gateId,
		expectedSessionId: eventString(event, "session_id", "sessionId", "expectedSessionId") ?? fallbackSessionId,
		expiresAt: eventTimestamp(event),
	};
}

function textFromContent(content: unknown): string {
	if (typeof content === "string") return content;
	if (isRecord(content) && typeof content.text === "string") return content.text;
	if (!Array.isArray(content)) return "";
	return content
		.filter(isRecord)
		.filter(block => (block.type === "text" || block.type === "output_text") && typeof block.text === "string")
		.map(block => block.text as string)
		.join("");
}

function transcriptMessageCandidate(event: Record<string, unknown>): Record<string, unknown> {
	if (isRecord(event.message)) return event.message;
	if (isRecord(event.data)) return event.data;
	return event;
}

function messageIds(event: Record<string, unknown>): readonly string[] {
	const candidates: unknown[] = [transcriptMessageCandidate(event)];
	if (Array.isArray(event.messages)) candidates.push(...event.messages);
	return distinctAttemptIds(
		candidates.filter(isRecord).map((candidate) => eventString(candidate, "responseId", "response_id", "id", "message_id")),
	);
}

function isSafelyNonDeliverableTranscriptEntry(event: Record<string, unknown>): boolean {
	const candidate = transcriptMessageCandidate(event);
	if (candidate.role === "user" || candidate.role === "system" || candidate.role === "developer" || candidate.role === "tool") return true;
	if (candidate.role !== "assistant" || !Array.isArray(candidate.content)) return false;
	return candidate.content.every(
		block =>
			isRecord(block) &&
			(block.type === "tool_use" || block.type === "tool_call" || block.type === "thinking" || block.type === "reasoning"),
	);
}

function finalizedAssistantMessage(event: Record<string, unknown>, fallbackKey: string): FinalAssistantMessage | undefined {
	const candidate = transcriptMessageCandidate(event);
	if (candidate.role !== "assistant") return undefined;
	const text = textFromContent(candidate.content);
	if (!text.trim()) return undefined;
	const messageId = eventString(candidate, "responseId", "response_id", "id", "message_id");
	const timestamp = eventTimestamp(candidate);
	return {
		key: messageId ? `response:${messageId}` : fallbackKey,
		payload: {
			finalized: true,
			text,
			...(messageId ? { message_id: messageId } : {}),
			...(timestamp === undefined ? {} : { timestamp }),
		},
	};
}

function payloadJson(payload: unknown): string {
	return JSON.stringify(payload);
}

function markFailedClosed(state: GatewayStateStore, reason: string): void {
	try {
		state.markFailedClosed(reason);
	} catch {
		// A competing daemon may have already captured the authoritative reason.
	}
}


function externalEvent(event: SupervisorEvent): Record<string, unknown> {
	const payload = isRecord(event.payload) ? event.payload : { payload: event.payload };
	return {
		...payload,
		type: typeof payload.type === "string" ? payload.type : event.kind,
		__tail_kind: event.kind,
		...(event.id === undefined ? {} : { __tail_id: event.id }),
		...(event.generation === undefined ? {} : { __tail_generation: event.generation }),
		...(event.seq === undefined ? {} : { __tail_seq: event.seq }),
	};
}

function supervisorEventKey(event: SupervisorEvent): string {
	if (event.id !== undefined) return `id:${event.id}`;
	if (event.generation !== undefined && event.seq !== undefined) return `seq:${event.generation}:${event.seq}`;
	return `payload:${event.kind}:${JSON.stringify(event.payload)}`;
}

function sourceCheckpoint(event: SupervisorEvent, revision: number): TailCheckpoint | undefined {
	if (event.generation === undefined || event.seq === undefined) return undefined;
	return { revision, generation: event.generation, seq: event.seq };
}

function initialRingCheckpoint(tail: SupervisorTailEvents): TailCheckpoint | undefined {
	// An envelope checkpoint is its high-water mark. A resync point is only a
	// fallback when the broker omitted that high-water mark entirely.
	return tail.checkpoint ?? tail.resyncCheckpoint;
}

const FATAL_EXTERNAL_IDENTITY_REASONS = new Set([
	"session_deleted",
	"session_unavailable",
	"session_ambiguous",
	"session_terminal_uncertain",
	"session_locator_mismatch",
	"growth_intent_mismatch",
	"main_identity_mismatch",
	"tail_resync_unavailable",
	"transcript_proof_invalid",
	"transcript_proof_mismatch",
	"transcript_proof_persist_failed",
]);

function isFatalExternalIdentityFailure(reason: string): boolean {
	return FATAL_EXTERNAL_IDENTITY_REASONS.has(reason);
}
class ExternalMainSessionHost implements MainSessionHost {
	readonly sessionId: string;
	readonly gates: MainSessionGateRegistry;
	readonly #supervisor: HostSupervisor;
	readonly #state: GatewayStateStore;
	readonly #journal: MainSessionJournal;
	readonly #now: () => number;
	readonly #afterTerminalEvidenceBeforeAdmissionFinalize: (() => void) | undefined;
	#identity: ExternalSessionIdentity;
	#turnState: "idle" | "busy";
	#followUpQueueDepth: number;
	#degraded = false;
	/** Tail transport is transiently unreadable: health-visible, but NOT a mutation fence. */
	#tailUnavailable = false;
	#failure: MainSessionHostError | undefined;
	#growthWindow: GrowthWindow | undefined;
	#disposed = false;
	#activeAttempt: MainSessionTurnJournalPayload | undefined;
	#nextAttempt = 0;
	readonly #finalizedAssistantMessageKeys = new Set<string>();
	#transcriptDeliveryProgress: TranscriptDeliveryProgress | undefined;
	#transcriptProof: TranscriptProof;
	#verificationState: "pending" | "verified";
	readonly #journaledAttemptTransitions = new Map<string, JournaledAttemptTransitions>();
	readonly #admittedOperations = new Map<string, AdmittedOperation>();
	readonly #surfaceIdByAttemptId = new Map<string, string>();
	readonly #surfaceIdByResponseId = new Map<string, string>();
	readonly #pendingTranscriptAttributions: PendingTranscriptAttribution[] = [];
	readonly #settledTerminalAttemptIds = new Set<string>();
	#terminalSettlementSequence = 0;
	#tailObservationEpoch = 0;
	readonly #unmatchedTerminalAttemptIds = new Set<string>();
	readonly #seenTailEvents = new Set<string>();
	readonly #fatalFailure = Promise.withResolvers<MainSessionHostError>();
	readonly #verificationReady = Promise.withResolvers<void>();
	readonly #tailStop = Promise.withResolvers<void>();
	#tailWake = Promise.withResolvers<void>();
	readonly #highestTailSequenceByGeneration = new Map<number, number>();
	#tailCheckpoint: TailCheckpoint | undefined;
	#verificationTail: SupervisorTailEvents | undefined;
	#journalTailCheckpoint: TailCheckpoint | undefined;

	readonly #tailTask: Promise<void>;

	constructor(options: CreateMainSessionHostOptions) {
		this.sessionId = options.identity.sessionId;
		this.#supervisor = options.supervisor;
		this.#identity = options.identity;
		this.#state = options.state;
		this.#journal = options.journal;
		if (typeof this.#journal.journalAppendTranscriptProjection !== "function") {
			throw new MainSessionHostError(
				"transcript_projection_journal_unavailable",
				"A durable main-session host requires an atomic transcript projection journal.",
			);
		}
		this.#now = options.now ?? Date.now;
		this.#afterTerminalEvidenceBeforeAdmissionFinalize = options.afterTerminalEvidenceBeforeAdmissionFinalize;
		this.gates = options.gates ?? new MainSessionGateRegistry({ now: this.#now });
		this.#turnState = options.initialTurnState ?? "idle";
		this.#followUpQueueDepth = Math.max(0, options.initialFollowUpQueueDepth ?? 0);
		const durable = this.#state.read();
		this.#tailCheckpoint = durable.tailCheckpoint;
		this.#transcriptDeliveryProgress = durable.transcriptDeliveryProgress;
		this.#transcriptProof = durable.transcriptProof;
		this.#verificationState = options.initialVerificationState ?? (durable.transcriptProof === "proven" ? "verified" : "pending");
		this.#verificationTail = options.verificationTail;
		if (durable.transcriptProof === "pending" && this.#verificationState !== "pending") {
			throw new MainSessionHostError("transcript_proof_invalid", "A pending durable transcript proof cannot start as boot-verified.");
		}
		if (options.recoveredGrowthIntent) {
			this.#growthWindow = { intent: options.recoveredGrowthIntent, pendingAdmissions: 0 };
		}
		for (const attribution of options.initialAdmissionAttributions ?? []) this.registerAdmissionAttribution(attribution);

		this.publishStatus();
		if (this.#verificationState === "verified") this.#verificationReady.resolve();
		this.#tailTask = this.observeTail();
	}

	get identity(): ExternalSessionIdentity {
		return this.#identity;
	}

	get degraded(): boolean {
		return this.#degraded;
	}

	get turnOriginSurfaceId(): string | undefined {
		if (this.#turnState !== "busy") return undefined;
		const candidates = new Set<string>();
		for (const admission of this.#admittedOperations.values()) if (admission.surfaceId) candidates.add(admission.surfaceId);
		return candidates.size === 1 ? candidates.values().next().value : undefined;
	}

	get turnState(): "idle" | "busy" {
		return this.#turnState;
	}

	get followUpQueueDepth(): number {
		return this.#followUpQueueDepth;
	}

	get mutationReadinessReason(): string | undefined {
		if (this.#disposed) return "host_disposed";
		if (this.#failure) return this.#failure.reason;
		if (this.#degraded) return "host_degraded";
		if (this.#transcriptProof === "pending") return "transcript_proof_pending";
		if (this.#verificationState === "pending") return "transcript_verification_pending";
		for (const admission of this.#admittedOperations.values()) {
			if (admission.ambiguous) return "admission_recovery_pending";
		}
		return undefined;
	}

	reportAdmissionClaimAbandonFailure(error: unknown): void {
		this.enterFailure("main_admission_claim_abandon_failed", error);
	}

	waitForVerifiedTranscript(): Promise<void> {
		return this.#verificationReady.promise;
	}

	waitForFatalFailure(): Promise<MainSessionHostError> {
		return this.#fatalFailure.promise;
	}

	private wakeTail(): void {
		this.#tailWake.resolve();
		this.#tailWake = Promise.withResolvers<void>();
	}

	private eventAttemptIds(event: Record<string, unknown>): readonly string[] {
		const scope = isRecord(event.scope) ? event.scope : undefined;
		return distinctAttemptIds([
			eventString(event, "operationRef", "operation_ref", "opRef", "op_ref"),
			eventString(event, "clientRef", "client_ref"),
			eventString(event, "commandId", "command_id"),
			eventString(event, "turnId", "turn_id"),
			scope && eventString(scope, "attemptId", "attempt_id", "operationRef", "operation_ref", "opRef", "op_ref"),
			scope && eventString(scope, "clientRef", "client_ref"),
			scope && eventString(scope, "commandId", "command_id"),
			scope && eventString(scope, "turnId", "turn_id"),
		]);
	}

	private registerAdmissionAttribution(attribution: MainSessionAdmissionAttribution): void {
		if (!attribution.surfaceId.trim() || attribution.attemptIds.length === 0) {
			throw new MainSessionHostError("main_admission_attribution_invalid", "A main-admission attribution is malformed.");
		}
		for (const attemptId of attribution.attemptIds) {
			if (!attemptId) throw new MainSessionHostError("main_admission_attribution_invalid", "A main-admission attempt id is empty.");
			const existing = this.#surfaceIdByAttemptId.get(attemptId);
			if (existing !== undefined && existing !== attribution.surfaceId) {
				throw new MainSessionHostError("main_admission_attribution_invalid", "A broker attempt has conflicting surface attributions.");
			}
			this.#surfaceIdByAttemptId.set(attemptId, attribution.surfaceId);
		}
	}

	private mergeSurfaceId(current: string | undefined, candidate: string | undefined): string | undefined {
		if (candidate === undefined) return current;
		if (current !== undefined && current !== candidate) {
			throw new MainSessionHostError("main_admission_attribution_invalid", "One broker observation has conflicting surface attributions.");
		}
		return candidate;
	}

	private surfaceIdForAttemptId(attemptId: string): string | undefined {
		let surfaceId = this.#surfaceIdByAttemptId.get(attemptId);
		for (const admission of this.#admittedOperations.values()) {
			if (!matchesCanonicalAdmissionAttemptId(attemptId, admission.attemptIds)) continue;
			surfaceId = this.mergeSurfaceId(surfaceId, admission.surfaceId);
		}
		return surfaceId;
	}

	private surfaceIdForEvent(event: Record<string, unknown>): string | undefined {
		let surfaceId: string | undefined;
		for (const attemptId of this.eventAttemptIds(event)) {
			surfaceId = this.mergeSurfaceId(surfaceId, this.surfaceIdForAttemptId(attemptId));
		}
		return surfaceId;
	}

	private recordResponseAttribution(event: Record<string, unknown>): void {
		const surfaceId = this.surfaceIdForEvent(event);
		if (surfaceId === undefined) return;
		for (const messageId of messageIds(event)) {
			const existing = this.#surfaceIdByResponseId.get(messageId);
			if (existing !== undefined && existing !== surfaceId) {
				throw new MainSessionHostError("main_admission_attribution_invalid", "A response message has conflicting surface attributions.");
			}
			this.#surfaceIdByResponseId.set(messageId, surfaceId);
		}
		while (this.#surfaceIdByResponseId.size > MAX_JOURNALED_ATTEMPTS) {
			const oldest = this.#surfaceIdByResponseId.keys().next().value;
			if (oldest === undefined) break;
			this.#surfaceIdByResponseId.delete(oldest);
		}
	}

	private prepareTailResponseAttributions(events: readonly SupervisorEvent[]): void {
		for (const source of events) {
			const event = externalEvent(source);
			if (event.type !== "agent_end" && event.type !== "turn_end" && event.type !== "agent_failed") continue;
			const operationRef = this.admittedOperationRef(event);
			const admission = operationRef === undefined ? undefined : this.#admittedOperations.get(operationRef);
			this.rememberTerminalBoundary(event, admission, this.surfaceIdForEvent(event));
			this.recordResponseAttribution(event);
		}
	}

	private surfaceIdForTranscriptEntry(event: Record<string, unknown>): string | undefined {
		let surfaceId = this.surfaceIdForEvent(event);
		for (const messageId of messageIds(event)) {
			surfaceId = this.mergeSurfaceId(surfaceId, this.#surfaceIdByResponseId.get(messageId));
		}
		const pending = this.pendingTranscriptAttributionFor(event);
		return this.mergeSurfaceId(surfaceId, pending?.surfaceId);
	}

	/** Resolves one unambiguous trailing transcript against retained exact durable-attribution aliases. */
	private pendingTranscriptAttributionFor(event: Record<string, unknown>): PendingTranscriptAttribution | undefined {
		if (messageIds(event).length === 0 || this.#admittedOperations.size !== 0) return undefined;
		// Keep the bridge to one subsequent complete tail. A later autonomous or
		// unrelated transcript must not inherit a stale surface merely because a
		// durable idempotency mapping has not yet expired.
		this.#pendingTranscriptAttributions.splice(
			0,
			this.#pendingTranscriptAttributions.length,
			...this.#pendingTranscriptAttributions.filter(attribution => this.#tailObservationEpoch - attribution.tailEpoch <= 1),
		);
		if (this.#pendingTranscriptAttributions.length !== 1) return undefined;
		const attribution = this.#pendingTranscriptAttributions[0];
		if (!attribution || attribution.settlementSequence !== this.#terminalSettlementSequence) return undefined;
		let resolvedSurfaceId: string | undefined;
		for (const attemptId of attribution.attemptIds) {
			resolvedSurfaceId = this.mergeSurfaceId(resolvedSurfaceId, this.#surfaceIdByAttemptId.get(attemptId));
		}
		return resolvedSurfaceId === attribution.surfaceId ? attribution : undefined;
	}

	private consumePendingTranscriptAttribution(attribution: PendingTranscriptAttribution): void {
		const index = this.#pendingTranscriptAttributions.indexOf(attribution);
		if (index >= 0) this.#pendingTranscriptAttributions.splice(index, 1);
	}

	private rememberTerminalBoundary(
		event: Record<string, unknown>,
		admission: AdmittedOperation | undefined,
		surfaceId: string | undefined,
	): void {
		const attemptIds = admission?.attemptIds ?? this.eventAttemptIds(event);
		const duplicate = attemptIds.some(attemptId => this.#settledTerminalAttemptIds.has(attemptId));
		if (duplicate) return;
		this.#terminalSettlementSequence += 1;
		for (const attemptId of attemptIds) this.#settledTerminalAttemptIds.add(attemptId);
		while (this.#settledTerminalAttemptIds.size > MAX_JOURNALED_ATTEMPTS) {
			const oldest = this.#settledTerminalAttemptIds.values().next().value;
			if (oldest === undefined) break;
			this.#settledTerminalAttemptIds.delete(oldest);
		}
		if (attemptIds.length === 0 || surfaceId === undefined) return;
		this.#pendingTranscriptAttributions.push({
			attemptIds: [...attemptIds],
			surfaceId,
			settlementSequence: this.#terminalSettlementSequence,
			tailEpoch: this.#tailObservationEpoch,
		});
		while (this.#pendingTranscriptAttributions.length > MAX_JOURNALED_ATTEMPTS) this.#pendingTranscriptAttributions.shift();
	}

	private shouldDeferTranscriptAttribution(event: Record<string, unknown>, surfaceId: string | undefined): boolean {
		// A transcript entry can arrive in a tail before the ring's terminal event.
		// While an admitted operation is still unresolved, withholding a response-ID
		// bearing entry preserves ordering and avoids permanently routing it as
		// unattributed. Exact terminal correlation is still required before release.
		return surfaceId === undefined && this.#admittedOperations.size > 0 && messageIds(event).length > 0;
	}

	private admittedOperationRef(event: Record<string, unknown>): string | undefined {
		for (const candidate of this.eventAttemptIds(event)) {
			for (const [opRef, admission] of this.#admittedOperations) {
				if (matchesCanonicalAdmissionAttemptId(candidate, admission.attemptIds)) return opRef;
			}
		}
		return undefined;
	}

	private rememberUnmatchedTerminalAttempts(event: Record<string, unknown>): void {
		for (const attemptId of this.eventAttemptIds(event)) this.#unmatchedTerminalAttemptIds.add(attemptId);
		while (this.#unmatchedTerminalAttemptIds.size > MAX_JOURNALED_ATTEMPTS) {
			const oldest = this.#unmatchedTerminalAttemptIds.values().next().value;
			if (!oldest) break;
			this.#unmatchedTerminalAttemptIds.delete(oldest);
		}
	}

	private recordAdmissionAttemptIds(opRef: string, receipt: BrokerOperationReceipt): void {
		const admission = this.#admittedOperations.get(opRef);
		if (!admission) return;
		const attemptIds = admissionAttemptIds(this.sessionId, opRef, receipt);
		this.#admittedOperations.set(opRef, { ...admission, attemptIds });
		try {
			admission.recordAttemptIds?.(attemptIds);
			if (admission.surfaceId !== undefined) {
				this.registerAdmissionAttribution({ attemptIds, surfaceId: admission.surfaceId });
			}
		} catch (error) {
			throw this.enterFailure("main_admission_attempt_ids_persist_failed", error);
		}
		if (!attemptIds.some(attemptId => matchesCanonicalAdmissionAttemptId(attemptId, this.#unmatchedTerminalAttemptIds))) return;
		if (!this.settleAdmittedOperation(opRef)) throw this.#failure ?? new MainSessionHostError("main_admission_finalize_failed");
	}
	private publishStatus(): void {
		try {
			this.#journal.setMainSessionStatus?.(this.#turnState, this.#followUpQueueDepth, this.#verificationState);
		} catch {
			// Status is best-effort observation; durable journal writes remain authoritative.
		}
	}

	private publishVerifiedReadiness(): void {
		try {
			this.#journal.setRpcHealth?.("running", "transcript_verified");
		} catch {
			// The proof state and status publication remain authoritative if readiness publication fails.
		}
	}

	private enterFailure(reason: string, error: unknown, journalFailure = false): MainSessionHostError {
		if (this.#failure) return this.#failure;
		const detail = error instanceof Error ? error.message : String(error);
		const failure = new MainSessionHostError(reason, `${reason}: ${detail}`, { cause: error });
		this.#failure = failure;
		this.#degraded = true;
		if (journalFailure) {
			try {
				this.#journal.setJournalDegraded?.(true);
			} catch {
				// The original journal failure remains authoritative.
			}
		}
		try {
			this.#journal.setRpcHealth?.("degraded", reason);
		} catch {
			// The original failure remains authoritative if health publication also fails.
		}
		if (isFatalExternalIdentityFailure(reason)) {
			markFailedClosed(this.#state, reason);
			this.#fatalFailure.resolve(failure);
		}
		return failure;
	}

	private appendJournalEvent(kind: string, payload: unknown): boolean {
		if (this.#disposed || this.#failure) return false;
		try {
			const encoded = payloadJson(payload);
			const checkpoint = this.#journalTailCheckpoint;
			if (
				checkpoint &&
				this.#journal.journalAppendAtTailCheckpoint &&
				(!this.#tailCheckpoint || compareTailCheckpoints(checkpoint, this.#tailCheckpoint) >= 0)
			) {
				this.#journal.journalAppendAtTailCheckpoint(kind, encoded, this.#tailCheckpoint, checkpoint);
				this.#tailCheckpoint = checkpoint;
			} else {
				this.#journal.journalAppend(kind, encoded);
			}
			return true;
		} catch (error) {
			this.enterFailure("journal_append_failed", error, true);
			return false;
		}
	}

	private attemptPayload(event: Record<string, unknown>): MainSessionTurnJournalPayload {
		const scope = isRecord(event.scope) ? event.scope : undefined;
		const scopedAttempt = scope && eventString(scope, "attemptId");
		const scopedLineage = scope && eventString(scope, "lineage");
		const scopedGeneration = scope?.generation;
		if (scopedAttempt && scopedLineage && typeof scopedGeneration === "number" && Number.isSafeInteger(scopedGeneration)) {
			return { attempt_id: scopedAttempt, generation: scopedGeneration, lineage: scopedLineage };
		}
		if (!this.#activeAttempt) {
			const externalId = eventString(event, "clientRef", "commandId", "turnId", "__tail_id");
			const generation =
				typeof event.__tail_generation === "number" && Number.isSafeInteger(event.__tail_generation)
					? event.__tail_generation
					: ++this.#nextAttempt;
			this.#activeAttempt = {
				attempt_id: externalId ?? `${this.sessionId}:external:${++this.#nextAttempt}`,
				generation,
				lineage: "external",
			};
		}
		if (!this.#activeAttempt) throw new MainSessionHostError("attempt_identity_missing");
		return this.#activeAttempt;
	}

	private appendTurnTransition(kind: "turn_start" | "turn_end", event: Record<string, unknown>): void {
		const attempt = this.attemptPayload(event);
		const surfaceId = this.surfaceIdForEvent(event);
		const payload: MainSessionTurnJournalPayload = {
			...attempt,
			...(surfaceId === undefined ? {} : { surface_id: surfaceId }),
		};
		const key = `${attempt.lineage}\u0000${attempt.attempt_id}\u0000${attempt.generation}`;
		const transitions = this.#journaledAttemptTransitions.get(key) ?? { started: false, ended: false };
		this.#journaledAttemptTransitions.set(key, transitions);
		const field = kind === "turn_start" ? "started" : "ended";
		if (transitions[field] || !this.appendJournalEvent(kind, payload)) return;
		transitions[field] = true;
		while (this.#journaledAttemptTransitions.size > MAX_JOURNALED_ATTEMPTS) {
			const oldest = this.#journaledAttemptTransitions.keys().next().value;
			if (!oldest) break;
			this.#journaledAttemptTransitions.delete(oldest);
		}
		if (kind === "turn_end") this.#activeAttempt = undefined;
	}

	private advanceTranscriptDelivery(expected: TranscriptDeliveryProgress | undefined, next: TranscriptDeliveryProgress): boolean {
		try {
			this.#state.advanceTranscriptDeliveryProgress(expected, next);
			this.#transcriptDeliveryProgress = next;
			return true;
		} catch (error) {
			this.enterFailure("transcript_delivery_progress_write_failed", error, true);
			return false;
		}
	}

	private appendFinalAssistantMessage(
		event: Record<string, unknown>,
		fallbackKey: string,
		expectedDelivery: TranscriptDeliveryProgress | undefined,
		nextDelivery: TranscriptDeliveryProgress,
	): boolean {
		const finalized = finalizedAssistantMessage(event, fallbackKey);
		if (!finalized || this.#finalizedAssistantMessageKeys.has(finalized.key)) {
			return this.advanceTranscriptDelivery(expectedDelivery, nextDelivery);
		}
		try {
			const surfaceId = this.surfaceIdForTranscriptEntry(event);
			const encoded = payloadJson({
				...finalized.payload,
				...(surfaceId === undefined ? {} : { surface_id: surfaceId }),
			});
			const checkpoint = this.#journalTailCheckpoint ?? this.#tailCheckpoint;
			if (!checkpoint) throw new MainSessionHostError("tail_checkpoint_unavailable", "A transcript projection requires a durable broker-tail checkpoint.");
			this.#journal.journalAppendTranscriptProjection(
				"assistant_message",
				encoded,
				this.#tailCheckpoint,
				checkpoint,
				expectedDelivery,
				nextDelivery,
			);
			this.#tailCheckpoint = checkpoint;
			this.#transcriptDeliveryProgress = nextDelivery;
			this.#finalizedAssistantMessageKeys.add(finalized.key);
			while (this.#finalizedAssistantMessageKeys.size > MAX_JOURNALED_ATTEMPTS) {
				const oldest = this.#finalizedAssistantMessageKeys.values().next().value;
				if (!oldest) break;
				this.#finalizedAssistantMessageKeys.delete(oldest);
			}
			return true;
		} catch (error) {
			this.enterFailure("transcript_delivery_progress_write_failed", error, true);
			return false;
		}
	}

	private observeGateOpen(event: Record<string, unknown>): void {
		const gate = gateFromEvent(event, this.sessionId);
		if (!gate || gate.expectedSessionId !== this.sessionId || !this.gates.observeOpen(gate)) return;
		this.appendJournalEvent("gate_open", event);
	}

	private observeGateResolved(event: Record<string, unknown>): void {
		const gate = gateFromEvent(event, this.sessionId);
		if (!gate || gate.expectedSessionId !== this.sessionId || !this.gates.observeResolved(gate.gateId)) return;
		this.appendJournalEvent("gate_resolved", event);
	}

	private observeExternalEvent(source: SupervisorEvent): void {
		if (source.generation !== undefined && source.seq !== undefined) {
			const highest = this.#highestTailSequenceByGeneration.get(source.generation);
			if (highest !== undefined && source.seq <= highest) return;
			this.#highestTailSequenceByGeneration.set(source.generation, source.seq);
			while (this.#highestTailSequenceByGeneration.size > 32) {
				const oldest = this.#highestTailSequenceByGeneration.keys().next().value;
				if (oldest === undefined) break;
				this.#highestTailSequenceByGeneration.delete(oldest);
			}
		}
		const eventKey = supervisorEventKey(source);
		if (this.#seenTailEvents.has(eventKey)) return;
		this.#seenTailEvents.add(eventKey);
		while (this.#seenTailEvents.size > MAX_JOURNALED_ATTEMPTS) {
			const oldest = this.#seenTailEvents.values().next().value;
			if (!oldest) break;
			this.#seenTailEvents.delete(oldest);
		}
		const event = externalEvent(source);
		const type = event.type;
		if (type === "agent_start" || type === "turn_start") {
			this.#turnState = "busy";
			if (type === "agent_start" && this.#followUpQueueDepth > 0) this.#followUpQueueDepth -= 1;
			this.publishStatus();
			this.appendTurnTransition("turn_start", event);
			return;
		}
		if (type === "agent_end" || type === "turn_end") {
			const operationRef = this.admittedOperationRef(event);
			const admission = operationRef === undefined ? undefined : this.#admittedOperations.get(operationRef);
			this.rememberTerminalBoundary(event, admission, operationRef === undefined ? undefined : this.surfaceIdForEvent(event));
			// Persist the terminal boundary before its claim finalizer. If the process
			// dies after this write, restart reconciliation can prove the acceptance.
			this.appendTurnTransition("turn_end", event);
			if (this.#failure) return;
			if (!operationRef) this.rememberUnmatchedTerminalAttempts(event);
			if (operationRef && !this.settleAdmittedOperation(operationRef)) return;
			this.#turnState = this.#admittedOperations.size === 0 ? "idle" : "busy";
			this.publishStatus();
			this.finishGrowthWindowIfSettled();
			return;
		}
		if (type === "agent_failed") {
			const operationRef = this.admittedOperationRef(event);
			const admission = operationRef === undefined ? undefined : this.#admittedOperations.get(operationRef);
			this.rememberTerminalBoundary(event, admission, operationRef === undefined ? undefined : this.surfaceIdForEvent(event));
			this.appendTurnTransition("turn_end", event);
			if (this.#failure) return;
			if (!operationRef) this.rememberUnmatchedTerminalAttempts(event);
			if (operationRef && !this.settleAdmittedOperation(operationRef)) return;
			this.#turnState = this.#admittedOperations.size === 0 ? "idle" : "busy";
			this.publishStatus();
			this.finishGrowthWindowIfSettled();
			this.enterFailure("turn_execution_failed", event);
			return;
		}
		if (type === "gate_expired") {
			const gate = gateFromEvent(event, this.sessionId);
			if (gate) this.gates.observeExpired(gate.gateId);
			return;
		}
		if (type === "gate_open" || type === "workflow_gate" || (type === "action_needed" && event.kind === "ask")) {
			this.observeGateOpen(event);
			return;
		}
		if (type === "gate_resolved" || type === "action_resolved") {
			this.observeGateResolved(event);
			return;
		}
		// The broker's finalized reply is a stable transcript entry, not a ring message_end event.
	}

	private appendTranscriptDeliveryGap(
		expectedDelivery: TranscriptDeliveryProgress | undefined,
		nextDelivery: TranscriptDeliveryProgress,
		payload: Record<string, unknown>,
		surfaceId?: string,
	): boolean {
		try {
			const encoded = payloadJson({ ...payload, ...(surfaceId === undefined ? {} : { surface_id: surfaceId }) });
			const checkpoint = this.#journalTailCheckpoint ?? this.#tailCheckpoint;
			if (!checkpoint) throw new MainSessionHostError("tail_checkpoint_unavailable", "A transcript delivery gap requires a durable broker-tail checkpoint.");
			this.#journal.journalAppendTranscriptProjection(
				"transcript_delivery_gap",
				encoded,
				this.#tailCheckpoint,
				checkpoint,
				expectedDelivery,
				nextDelivery,
			);
			this.#tailCheckpoint = checkpoint;
			this.#transcriptDeliveryProgress = nextDelivery;
			return true;
		} catch (error) {
			this.enterFailure("transcript_delivery_progress_write_failed", error, true);
			return false;
		}
	}

	private transcriptDeliveryAt(entries: readonly SupervisorTranscriptEntry[], index: number): TranscriptDeliveryProgress {
		return {
			lastEntryId: entries[index]?.id,
			fingerprint: fingerprintTranscriptEntries(entries.slice(0, index + 1).map(entry => entry.payload)),
		};
	}

	private deliveryGapProgress(entries: readonly SupervisorTranscriptEntry[]): TranscriptDeliveryProgress {
		return entries.length === 0
			? { fingerprint: fingerprintTranscriptEntries([]) }
			: this.transcriptDeliveryAt(entries, entries.length - 1);
	}

	private transcriptDeliveryIsUnprovable(entries: readonly SupervisorTranscriptEntry[]): boolean {
		const previous = this.#transcriptDeliveryProgress;
		if (!previous) return entries.length > 0;
		if (!previous.lastEntryId) return previous.fingerprint.entryCount !== 0;
		const index = entries.findIndex(entry => entry.id === previous.lastEntryId);
		if (index < 0) return true;
		const observed = fingerprintTranscriptEntries(entries.slice(0, index + 1).map(entry => entry.payload));
		return (
			observed.entryCount !== previous.fingerprint.entryCount ||
			observed.sha256 !== previous.fingerprint.sha256
		);
	}

	private observeTranscript(entries: readonly SupervisorTranscriptEntry[]): void {
		for (const entry of entries) {
			if (!entry.id.trim()) throw new HostSupervisorError("transcript_entry_id_missing", "Broker transcript delivery entry has no stable id.");
		}
		const previous = this.#transcriptDeliveryProgress;
		if (!previous) {
			if (entries.length === 0) return;
			this.appendTranscriptDeliveryGap(undefined, this.deliveryGapProgress(entries), {
				reason: "transcript_delivery_progress_missing",
				available_from_entry_id: entries[0]?.id,
				available_through_entry_id: entries.at(-1)?.id,
			});
			return;
		}
		let start = 0;
		if (previous.lastEntryId) {
			const index = entries.findIndex(entry => entry.id === previous.lastEntryId);
			if (index < 0) {
				this.appendTranscriptDeliveryGap(previous, this.deliveryGapProgress(entries), {
					reason: "transcript_delivery_unprovable",
					delivered_through_entry_id: previous.lastEntryId,
					available_from_entry_id: entries[0]?.id,
					available_through_entry_id: entries.at(-1)?.id,
				});
				return;
			}
			const observed = fingerprintTranscriptEntries(entries.slice(0, index + 1).map(entry => entry.payload));
			if (
				observed.entryCount !== previous.fingerprint.entryCount ||
				observed.sha256 !== previous.fingerprint.sha256
			) {
				this.appendTranscriptDeliveryGap(previous, this.deliveryGapProgress(entries), {
					reason: "transcript_delivery_unprovable",
					delivered_through_entry_id: previous.lastEntryId,
					available_from_entry_id: entries[0]?.id,
					available_through_entry_id: entries.at(-1)?.id,
				});
				return;
			}
			start = index + 1;
		} else if (previous.fingerprint.entryCount !== 0) {
			this.appendTranscriptDeliveryGap(previous, this.deliveryGapProgress(entries), {
				reason: "transcript_delivery_unprovable",
				available_from_entry_id: entries[0]?.id,
				available_through_entry_id: entries.at(-1)?.id,
			});
			return;
		}
		for (let index = start; index < entries.length; index += 1) {
			const entry = entries[index];
			if (!entry) continue;
			const expectedDelivery = this.#transcriptDeliveryProgress;
			const nextDelivery = this.transcriptDeliveryAt(entries, index);
			const event = isRecord(entry.payload) ? entry.payload : undefined;
			const finalized = event ? finalizedAssistantMessage(event, `transcript:${entry.id}`) : undefined;
			const safelyNonDeliverable = event ? isSafelyNonDeliverableTranscriptEntry(event) : false;
			const pendingAttribution = event ? this.pendingTranscriptAttributionFor(event) : undefined;
			const surfaceId = event ? this.surfaceIdForTranscriptEntry(event) : undefined;
			if (event && this.shouldDeferTranscriptAttribution(event, surfaceId)) return;
			const delivered = finalized && event
				? this.appendFinalAssistantMessage(event, `transcript:${entry.id}`, expectedDelivery, nextDelivery)
				: safelyNonDeliverable
					? this.advanceTranscriptDelivery(expectedDelivery, nextDelivery)
					: this.appendTranscriptDeliveryGap(
							expectedDelivery,
							this.deliveryGapProgress(entries),
							{
								reason: "transcript_delivery_unprovable",
								delivered_through_entry_id: expectedDelivery?.lastEntryId,
								unprojectable_entry_id: entry.id,
								available_from_entry_id: entries[0]?.id,
								available_through_entry_id: entries.at(-1)?.id,
							},
							surfaceId,
						);
			if (pendingAttribution && delivered && (finalized !== undefined || !safelyNonDeliverable)) this.consumePendingTranscriptAttribution(pendingAttribution);
			if (!delivered || this.#failure || (!finalized && !safelyNonDeliverable)) return;
		}
	}

	/** Binds the first successful pending-proof tail as both transcript and ring baselines. */
	private bindPendingTranscriptProof(tail: SupervisorTailEvents): boolean {
		if (this.#transcriptProof === "proven") return false;
		const identity = tail.identity;
		const ringCheckpoint = initialRingCheckpoint(tail);
		if (!ringCheckpoint) {
			throw new HostSupervisorError("tail_checkpoint_unavailable", "The first complete broker tail did not carry a ring checkpoint.");
		}
		if (!identity.transcript) {
			throw new HostSupervisorError("transcript_proof_invalid", "A complete broker tail did not carry a transcript fingerprint.");
		}
		if (tail.transcriptEntries.some(entry => !entry.id.trim())) {
			throw new HostSupervisorError("transcript_proof_invalid", "A complete broker tail contained a transcript entry without a stable id.");
		}
		const fingerprint = fingerprintTranscriptEntries(tail.transcriptEntries.map(entry => entry.payload));
		if (fingerprint.entryCount !== identity.transcript.entryCount || fingerprint.sha256 !== identity.transcript.sha256) {
			throw new HostSupervisorError("transcript_proof_mismatch", "The complete broker tail did not match its transcript fingerprint.");
		}
		const durable = this.#state.read();
		if (
			durable.bootstrapState !== "COMMITTED" ||
			!durable.mainIdentity ||
			durable.transcriptProof !== "pending" ||
			durable.tailCheckpoint !== undefined ||
			!sameExternalSession(durable.mainIdentity, identity) ||
			!sameExternalSession(this.#identity, identity)
		) {
			throw new HostSupervisorError("transcript_proof_invalid", "The pending durable identity did not match the first complete broker tail.");
		}
		const transcriptDeliveryProgress = this.deliveryGapProgress(tail.transcriptEntries);
		try {
			this.#state.persistTranscriptProof(durable.mainIdentity, identity, transcriptDeliveryProgress, ringCheckpoint);
		} catch (error) {
			throw new HostSupervisorError("transcript_proof_persist_failed", "Could not durably bind the pending transcript proof.", { cause: error });
		}
		this.#identity = identity;
		this.#tailCheckpoint = ringCheckpoint;
		this.#transcriptDeliveryProgress = transcriptDeliveryProgress;
		this.#transcriptProof = "proven";
		this.#verificationState = "verified";
		this.publishStatus();
		this.publishVerifiedReadiness();
		this.#verificationReady.resolve();
		return true;
	}

	/** Re-attests a durable proven identity after a busy boot deferred its complete tail. */
	private verifyBootTranscriptProof(tail: SupervisorTailEvents): void {
		if (this.#verificationState === "verified") return;
		const durable = this.#state.read();
		const identity = tail.identity;
		if (
			durable.bootstrapState !== "COMMITTED" ||
			durable.transcriptProof !== "proven" ||
			!durable.mainIdentity?.transcript ||
			!identity.transcript ||
			!sameExternalSession(durable.mainIdentity, identity) ||
			!sameExternalSession(this.#identity, identity)
		) {
			throw new HostSupervisorError("transcript_proof_invalid", "The complete tail could not re-attest the durable transcript proof.");
		}
		if (tail.transcriptEntries.some(entry => !entry.id.trim())) {
			throw new HostSupervisorError("transcript_proof_invalid", "A complete broker tail contained a transcript entry without a stable id.");
		}
		const fingerprint = fingerprintTranscriptEntries(tail.transcriptEntries.map(entry => entry.payload));
		if (fingerprint.entryCount !== identity.transcript.entryCount || fingerprint.sha256 !== identity.transcript.sha256) {
			throw new HostSupervisorError("transcript_proof_mismatch", "The complete broker tail did not match its transcript fingerprint.");
		}
		const growth = this.#growthWindow;
		if (growth) {
			if (!sameExternalSession(growth.intent.base, identity) || !attestsExternalTranscriptGrowth(growth.intent.base, tail.transcriptEntries.map(entry => entry.payload))) {
				throw new HostSupervisorError("growth_intent_mismatch", "The complete broker tail did not attest append-only growth from the recovered intent.");
			}
		} else if (!sameExternalFingerprint(durable.mainIdentity, identity)) {
			throw new HostSupervisorError("main_identity_mismatch", "The complete broker tail did not match the durable transcript proof.");
		}
		this.#identity = identity;
		this.#verificationState = "verified";
		this.publishStatus();
		this.publishVerifiedReadiness();
		this.#verificationReady.resolve();
	}

	private observeIdentity(identity: ExternalSessionIdentity, entries: readonly SupervisorTranscriptEntry[]): void {
		if (sameExternalFingerprint(this.#identity, identity)) return;
		const payloads = entries.map(entry => entry.payload);
		if (!this.#growthWindow) {
			// The adopted session is a live agent: the owner drives it directly and it
			// works autonomously, so its transcript routinely changes with no
			// gateway-initiated turn and therefore no growth window. Apply the same
			// rule strict resume applies - absorb attested append-only growth on the
			// same session, fail closed on anything else. Delivery progress is left
			// untouched, so a reply that arrived in this divergence is still projected
			// or journaled as a gap rather than silently baselined.
			if (!sameExternalSession(this.#identity, identity) || !attestsExternalTranscriptGrowth(this.#identity, payloads)) {
				markFailedClosed(this.#state, "main_identity_mismatch");
				throw this.enterFailure("main_identity_mismatch", new Error("External transcript changed outside append-only growth."));
			}
			try {
				this.#state.absorbAutonomousTranscriptGrowth(this.#identity, identity, payloads);
			} catch (error) {
				throw this.enterFailure("main_identity_growth_absorb_failed", error);
			}
			this.#identity = identity;
			return;
		}
		if (!attestsExternalTranscriptGrowth(this.#identity, payloads)) {
			markFailedClosed(this.#state, "growth_intent_mismatch");
			throw this.enterFailure("growth_intent_mismatch", new Error("External transcript changed outside append-only growth."));
		}
		this.#identity = identity;
	}

	/**
	 * The ring is an advisory lifecycle projection channel. Its coordinate is
	 * the lexicographic `(generation, seq)` pair: a new attempt generation is
	 * normal progression, while only a retention floor beyond that pair signals
	 * lifecycle loss. The transcript delivery chain remains the authority for
	 * content loss.
	 */
	private establishProjectionBoundary(tail: SupervisorTailEvents): boolean {
		const previous = this.#tailCheckpoint;
		if (previous) {
			if (!tail.retentionGap) return false;
			const resync = tail.resyncCheckpoint;
			if (!resync) {
				throw new HostSupervisorError("tail_resync_unavailable", "Broker tail reported a retention gap without a resync checkpoint.");
			}
			if (compareTailCheckpoints(resync, previous) <= 0) return false;
			try {
				this.#state.recordTailRingRotation(previous, resync);
			} catch (error) {
				throw new HostSupervisorError("tail_ring_rotation_write_failed", "Could not durably record the broker-tail ring rotation.", {
					cause: error,
				});
			}
			this.#tailCheckpoint = resync;
			return false;
		}
		const boundary = tail.checkpoint ?? tail.resyncCheckpoint;
		if (!boundary) throw new HostSupervisorError("tail_checkpoint_unavailable", "Broker tail did not provide an adoption checkpoint.");
		try {
			this.#state.recordTailAdoptionStart(boundary);
		} catch (error) {
			throw new HostSupervisorError(
				"tail_checkpoint_write_failed",
				"Could not durably record the broker-tail adoption checkpoint.",
				{ cause: error },
			);
		}
		this.#tailCheckpoint = boundary;
		return true;
	}

	private shouldProjectTailEvent(event: SupervisorEvent): boolean {
		const checkpoint = this.#tailCheckpoint;
		if (!checkpoint || event.generation === undefined || event.seq === undefined) return true;
		return event.generation > checkpoint.generation || (event.generation === checkpoint.generation && event.seq > checkpoint.seq);
	}

	private checkpointAfterTail(tail: SupervisorTailEvents, events: readonly SupervisorEvent[]): TailCheckpoint | undefined {
		const previous = this.#tailCheckpoint;
		const revision = Math.max(tail.checkpoint?.revision ?? 0, previous?.revision ?? 0);
		let checkpoint = previous;
		if (tail.checkpoint && (!checkpoint || compareTailCheckpoints(tail.checkpoint, checkpoint) > 0)) checkpoint = tail.checkpoint;
		for (const event of events) {
			const candidate = sourceCheckpoint(event, revision);
			if (!candidate) continue;
			if (!checkpoint || compareTailCheckpoints(candidate, checkpoint) > 0) checkpoint = candidate;
		}
		if (!checkpoint) return undefined;
		if (checkpoint.revision === revision) return checkpoint;
		return { ...checkpoint, revision };
	}

	private advanceTailCheckpointTo(checkpoint: TailCheckpoint | undefined): void {
		const previous = this.#tailCheckpoint;
		if (!checkpoint || (previous && compareTailCheckpoints(checkpoint, previous) <= 0)) return;
		try {
			this.#state.advanceTailCheckpoint(previous, checkpoint);
		} catch (error) {
			throw new HostSupervisorError("tail_checkpoint_write_failed", "Could not durably advance the broker-tail checkpoint.", {
				cause: error,
			});
		}
		this.#tailCheckpoint = checkpoint;
	}

	private advanceTailCheckpoint(tail: SupervisorTailEvents, events: readonly SupervisorEvent[]): void {
		this.advanceTailCheckpointTo(this.checkpointAfterTail(tail, events));
	}

	private terminalTailSettlesAdmission(tail: SupervisorTailEvents, admission: AdmittedOperation): boolean {
		const terminalCheckpoint = tail.checkpoint ?? tail.resyncCheckpoint;
		if (!terminalCheckpoint) return false;
		const admittedAt = admission.admittedAt;
		if (!admittedAt) return true;
		return compareTailCheckpoints(terminalCheckpoint, admittedAt) > 0;
	}

	private finalizeTerminalAdmission(opRef: string, admission: AdmittedOperation): boolean {
		if (!admission.finalizePendingClaim) {
			if (!admission.ambiguous) return true;
			this.enterFailure("main_admission_finalize_missing", new Error(`No durable finalizer was registered for ambiguous admission ${opRef}.`));
			return false;
		}
		try {
			// Terminal broker evidence proves acceptance even when the send command's
			// receipt has not yet arrived (or never will). The native finalizer is
			// idempotent, so it is safe if the normal receipt path races this tail.
			this.#afterTerminalEvidenceBeforeAdmissionFinalize?.();
			admission.finalizePendingClaim();
			return true;
		} catch (error) {
			this.enterFailure("main_admission_finalize_failed", error);
			return false;
		}
	}

	private settleAdmittedOperation(opRef: string): boolean {
		const admission = this.#admittedOperations.get(opRef);
		if (!admission) return true;
		if (!this.finalizeTerminalAdmission(opRef, admission)) return false;
		this.#admittedOperations.delete(opRef);
		return true;
	}

	private settleTerminalTail(tail: SupervisorTailEvents): void {
		if (!tail.terminal) return;
		for (const [opRef, admission] of this.#admittedOperations) {
			if (!this.terminalTailSettlesAdmission(tail, admission)) continue;
			if (!this.settleAdmittedOperation(opRef)) return;
		}
		if (this.#admittedOperations.size !== 0) return;
		this.#turnState = "idle";
		this.#followUpQueueDepth = 0;
		this.publishStatus();
		this.finishGrowthWindowIfSettled();
	}

	private async observeTail(): Promise<void> {
		// Transport-level broker failures (CLI spawn pressure, command timeouts,
		// transient nonzero exits) surface as "tail_unavailable" and are retried
		// with bounded backoff: under load a single slow spawn must not
		// permanently fail-stop the host. Transcript/identity authority failures
		// (or a malformed/unusable ring resync) still fail closed immediately.
		let transientFailures = 0;
		while (!this.#disposed && !this.#failure) {
			try {
				const verificationTail = this.#verificationTail;
				this.#verificationTail = undefined;
				const tail = verificationTail ?? (await this.#supervisor.tailEvents());
				if (this.#disposed) return;
				transientFailures = 0;
				if (this.#tailUnavailable) {
					this.#tailUnavailable = false;
					try {
						this.#journal.setRpcHealth?.("running", "tail_recovered");
					} catch {
						// Recovery reporting is best-effort.
					}
				}
				if (!tail.complete) {
					const wake = this.#tailWake.promise;
					await Promise.race([Bun.sleep(100), wake, this.#tailStop.promise]);
					continue;
				}
				this.#tailObservationEpoch += 1;
				const pendingProofBound = this.bindPendingTranscriptProof(tail);
				if (pendingProofBound) {
					this.settleTerminalTail(tail);
					const wake = this.#tailWake.promise;
					await Promise.race([Bun.sleep(tail.terminal && this.#turnState === "idle" ? 500 : 100), wake, this.#tailStop.promise]);
					continue;
				}
				this.verifyBootTranscriptProof(tail);
				// A rotating broker transcript window can no longer attest the durable
				// delivery point. Record the consumer-visible gap before the identity
				// mismatch closes this unsafe observation path.
				if (
					!sameExternalFingerprint(this.#identity, tail.identity) &&
					this.transcriptDeliveryIsUnprovable(tail.transcriptEntries)
				) {
					const candidate = tail.checkpoint;
					const journalCheckpoint =
						candidate && (!this.#tailCheckpoint || compareTailCheckpoints(candidate, this.#tailCheckpoint) >= 0)
							? candidate
							: this.#tailCheckpoint;
					this.#journalTailCheckpoint = journalCheckpoint;
					try {
						this.observeTranscript(tail.transcriptEntries);
					} finally {
						this.#journalTailCheckpoint = undefined;
					}
					if (this.#failure) return;
				}
				this.observeIdentity(tail.identity, tail.transcriptEntries);
				const boundaryEstablished = this.establishProjectionBoundary(tail);
				if (!boundaryEstablished) {
					const events = tail.events.filter(event => this.shouldProjectTailEvent(event));
					const revision = Math.max(tail.checkpoint?.revision ?? 0, this.#tailCheckpoint?.revision ?? 0);
					this.prepareTailResponseAttributions(events);
					// A finalized reply travels as a transcript entry while lifecycle events
					// travel in the ring. Give a reply the terminal event's checkpoint so
					// it and that lifecycle transition survive/replay as one durable batch.
					let transcriptProjected = false;
					const projectTranscript = (): void => {
						if (transcriptProjected) return;
						transcriptProjected = true;
						this.observeTranscript(tail.transcriptEntries);
					};
					for (const event of events) {
						const checkpoint = sourceCheckpoint(event, revision);
						this.#journalTailCheckpoint = checkpoint;
						try {
							const type = externalEvent(event).type;
							if (type === "agent_end" || type === "turn_end" || type === "agent_failed") {
								projectTranscript();
								if (this.#failure) return;
							}
							this.observeExternalEvent(event);
						} finally {
							this.#journalTailCheckpoint = undefined;
						}
						if (this.#failure) return;
						this.advanceTailCheckpointTo(checkpoint);
					}
					if (this.#failure) return;
					if (!transcriptProjected) {
						this.#journalTailCheckpoint = this.checkpointAfterTail(tail, events) ?? tail.checkpoint;
						try {
							projectTranscript();
						} finally {
							this.#journalTailCheckpoint = undefined;
						}
					}
					if (this.#failure) return;
					// Projection is synchronous. Persist only after every journal append in
					// this tail response has committed, so an interrupted poll replays.
					this.advanceTailCheckpoint(tail, events);
				}
				this.settleTerminalTail(tail);

				const wake = this.#tailWake.promise;
				await Promise.race([Bun.sleep(tail.terminal && this.#turnState === "idle" ? 500 : 100), wake, this.#tailStop.promise]);
			} catch (error) {
				if (this.#disposed) return;
				const reason = error instanceof HostSupervisorError ? error.reason : "tail_observation_failed";
				if (reason === "tail_unavailable") {
					// Transport unavailability is NEVER terminal. A bounded retry budget
					// permanently fail-stopped the host whenever the adopted session sat
					// in a turn longer than the budget (the broker CLI only emits tail
					// envelopes at terminal turn boundaries), which fenced owner ingress
					// forever and silently discarded the owner's messages. Keep retrying
					// with capped backoff and surface it as degraded HEALTH only: the
					// adopted identity is already verified and the broker - not the tail -
					// is the authority for admitting a turn, so accepting ingress here is
					// safe and strictly better than losing it.
					transientFailures += 1;
					if (!this.#tailUnavailable) {
						this.#tailUnavailable = true;
						try {
							this.#journal.setRpcHealth?.("degraded", "tail_unavailable");
						} catch {
							// Health reporting is best-effort and must not stop retrying.
						}
					}
					const backoffMs = Math.min(250 * 2 ** Math.min(transientFailures - 1, 6), 5_000);
					await Promise.race([Bun.sleep(backoffMs), this.#tailStop.promise]);
					continue;
				}
				this.enterFailure(reason, error);
				return;
			}
		}
	}

	private assertUsable(): void {
		const reason = this.mutationReadinessReason;
		if (!reason) return;
		if (this.#failure && this.#failure.reason === reason) throw this.#failure;
		throw new MainSessionHostError(reason);
	}

	private beginGrowthWindow(): GrowthWindow {
		const existing = this.#growthWindow;
		if (existing) return existing;
		this.assertUsable();
		const durable = this.#state.read();
		if (durable.bootstrapState !== "COMMITTED" || !durable.mainIdentity || durable.growthIntent || durable.transcriptProof !== "proven") {
			markFailedClosed(this.#state, "growth_protocol_invalid");
			throw new MainSessionHostError("growth_protocol_invalid");
		}
		if (!sameExternalFingerprint(durable.mainIdentity, this.#identity)) {
			markFailedClosed(this.#state, "main_identity_mismatch");
			throw new MainSessionHostError("main_identity_mismatch");
		}
		const intent: GrowthIntent = { base: this.#identity, startedAt: this.#now() };
		try {
			this.#state.writeGrowthIntent(this.#identity, intent.startedAt);
		} catch (error) {
			markFailedClosed(this.#state, "growth_intent_write_failed");
			throw new MainSessionHostError("growth_intent_write_failed", error instanceof Error ? error.message : String(error), { cause: error });
		}
		const growth = { intent, pendingAdmissions: 0 };
		this.#growthWindow = growth;
		return growth;
	}

	private transcriptDeliverySettled(): boolean {
		const transcript = this.#identity.transcript;
		const delivery = this.#transcriptDeliveryProgress;
		return (
			transcript !== undefined &&
			delivery !== undefined &&
			delivery.fingerprint.entryCount === transcript.entryCount &&
			delivery.fingerprint.sha256 === transcript.sha256
		);
	}

	private finishGrowthWindowIfSettled(): void {
		const growth = this.#growthWindow;
		if (
			!growth ||
			growth.pendingAdmissions > 0 ||
			this.#admittedOperations.size > 0 ||
			this.#turnState !== "idle" ||
			this.#followUpQueueDepth > 0 ||
			!this.transcriptDeliverySettled()
		)
			return;
		this.#growthWindow = undefined;
		try {
			this.#state.refreshAfterGrowth(growth.intent, this.#identity);
		} catch (error) {
			this.enterFailure("growth_refresh_failed", error);
		}
	}

	async admit(
		deliveredAs: "prompt" | "steer" | "follow_up",
		text: string,
		opRef: string,
		finalizePendingClaim?: () => void,
		recordAttemptIds?: (attemptIds: readonly string[]) => void,
		surfaceId?: string,
	): Promise<void> {
		this.assertUsable();
		if (!text.trim()) throw new MainSessionHostError(`${deliveredAs}_empty`, "A main-session message must not be empty.");
		if (!opRef.trim()) throw new MainSessionHostError("operation_ref_empty", "An admitted operation requires an operation reference.");
		const growth = this.beginGrowthWindow();
		growth.pendingAdmissions += 1;
		this.#admittedOperations.set(opRef, {
			admittedAt: this.#tailCheckpoint,
			attemptIds: admissionAttemptIds(this.sessionId, opRef),
			ambiguous: false,
			...(finalizePendingClaim === undefined ? {} : { finalizePendingClaim }),
			...(recordAttemptIds === undefined ? {} : { recordAttemptIds }),
			...(surfaceId === undefined ? {} : { surfaceId }),
		});
		try {
			const receipt =
				deliveredAs === "prompt"
					? await this.#supervisor.sendPrompt(text, opRef)
					: deliveredAs === "steer"
						? await this.#supervisor.sendSteer(text, opRef)
						: await this.#supervisor.followUp(text, opRef);
			this.recordAdmissionAttemptIds(opRef, receipt);
			this.wakeTail();
			if (this.#admittedOperations.has(opRef)) {
				if (deliveredAs === "follow_up") this.#followUpQueueDepth += 1;
				else this.#turnState = "busy";
				this.publishStatus();
			}
		} catch (error) {
			const admissionDisposition = classifyAdmissionDisposition(error);
			if (admissionDisposition === "definitive_rejection") {
				this.#admittedOperations.delete(opRef);
			} else {
				const admitted = this.#admittedOperations.get(opRef);
				// The tail may have already terminally finalized this operation while
				// the broker process was still withholding its receipt. Do not restore
				// busy state or a mutation fence after that authoritative settlement.
				if (admitted) {
					this.#admittedOperations.set(opRef, { ...admitted, ambiguous: true });
					if (deliveredAs === "follow_up") this.#followUpQueueDepth += 1;
					else this.#turnState = "busy";
					this.publishStatus();
					this.wakeTail();
				}
			}
			const reason = error instanceof MainSessionHostError || error instanceof HostSupervisorError ? error.reason : "turn_admission_failed";
			throw new MainSessionHostError(reason, error instanceof Error ? error.message : String(error), {
				cause: error,
				admissionDisposition,
			});
		} finally {
			growth.pendingAdmissions = Math.max(0, growth.pendingAdmissions - 1);
			this.finishGrowthWindowIfSettled();
		}
	}

	async resolveGate(_gateId: string, _answer: unknown, _idempotencyKey: string): Promise<MainGateResolution> {
		// The published spawn-only broker surface does not yet expose a validated
		// workflow.gate_answer receipt. Do not pretend local registry state answered it.
		return "unsupported";
	}

	async dispose(): Promise<void> {
		if (this.#disposed) return;
		this.#disposed = true;
		this.#tailStop.resolve();
		await this.#supervisor.dispose();
		await this.#tailTask;
	}
}

/** Wires a broker-authoritative external session to durable gateway journal projection. */
export function createMainSessionHost(options: CreateMainSessionHostOptions): MainSessionHost {
	return new ExternalMainSessionHost(options);
}
