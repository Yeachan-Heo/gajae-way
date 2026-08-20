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
	HostSupervisorError,
	type HostSupervisor,
	type SupervisorEvent,
	type SupervisorTailEvents,
	type SupervisorTranscriptEntry,
} from "./supervisor";


export interface MainSessionJournal {
	journalAppend(kind: string, payloadJson: string): unknown;
	journalAppendAtTailCheckpoint?(kind: string, payloadJson: string, expected: TailCheckpoint | undefined, checkpoint: TailCheckpoint): unknown;
	journalAppendTranscriptProjection?(
		kind: string,
		payloadJson: string,
		expectedTail: TailCheckpoint | undefined,
		checkpoint: TailCheckpoint,
		expectedDelivery: TranscriptDeliveryProgress | undefined,
		nextDelivery: TranscriptDeliveryProgress,
	): unknown;
	setRpcHealth?(state: "degraded", reason: string): void;
	setMainSessionStatus?(turnState: "idle" | "busy", followUpQueueDepth: number): void;
	setJournalDegraded?(degraded: boolean): void;
}

export class MainSessionHostError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason, options: { readonly cause?: unknown } = {}) {
		super(message, options.cause === undefined ? undefined : { cause: options.cause });
		this.name = "MainSessionHostError";
		this.reason = reason;
	}
}

export interface MainSessionHost {
	readonly sessionId: string;
	readonly identity: ExternalSessionIdentity;
	readonly degraded: boolean;
	readonly turnState: "idle" | "busy";
	readonly followUpQueueDepth: number;
	readonly transcriptProof: TranscriptProof;
	readonly gates: MainSessionGateRegistry;
	/** Waits for broker admission only; successful turn execution is observed asynchronously. */
	admit(deliveredAs: "prompt" | "steer" | "follow_up", text: string, opRef: string): Promise<void>;
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
	/** An active durable growth intent recovered by strict resume. */
	readonly recoveredGrowthIntent?: GrowthIntent;
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
	};
}

export interface MainSessionTurnJournalPayload {
	readonly attempt_id: string;
	readonly generation: number;
	readonly lineage: string;
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
	if (!Array.isArray(content)) return "";
	return content
		.filter(isRecord)
		.filter(block => block.type === "text" && typeof block.text === "string")
		.map(block => block.text as string)
		.join("");
}

function finalizedAssistantMessage(event: Record<string, unknown>, fallbackKey: string): FinalAssistantMessage | undefined {
	const candidate = isRecord(event.message) ? event.message : event;
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
	#identity: ExternalSessionIdentity;
	#turnState: "idle" | "busy";
	#followUpQueueDepth: number;
	#degraded = false;
	#failure: MainSessionHostError | undefined;
	#growthWindow: GrowthWindow | undefined;
	#disposed = false;
	#activeAttempt: MainSessionTurnJournalPayload | undefined;
	#nextAttempt = 0;
	readonly #finalizedAssistantMessageKeys = new Set<string>();
	#transcriptDeliveryProgress: TranscriptDeliveryProgress | undefined;
	#transcriptProof: TranscriptProof;
	readonly #journaledAttemptTransitions = new Map<string, JournaledAttemptTransitions>();
	readonly #admittedOperations = new Map<string, AdmittedOperation>();
	readonly #seenTailEvents = new Set<string>();
	readonly #fatalFailure = Promise.withResolvers<MainSessionHostError>();
	readonly #tailStop = Promise.withResolvers<void>();
	#tailWake = Promise.withResolvers<void>();
	readonly #highestTailSequenceByGeneration = new Map<number, number>();
	#tailCheckpoint: TailCheckpoint | undefined;
	#journalTailCheckpoint: TailCheckpoint | undefined;

	readonly #tailTask: Promise<void>;

	constructor(options: CreateMainSessionHostOptions) {
		this.sessionId = options.identity.sessionId;
		this.#supervisor = options.supervisor;
		this.#identity = options.identity;
		this.#state = options.state;
		this.#journal = options.journal;
		this.#now = options.now ?? Date.now;
		this.gates = options.gates ?? new MainSessionGateRegistry({ now: this.#now });
		this.#turnState = options.initialTurnState ?? "idle";
		this.#followUpQueueDepth = Math.max(0, options.initialFollowUpQueueDepth ?? 0);
		const durable = this.#state.read();
		this.#tailCheckpoint = durable.tailCheckpoint;
		this.#transcriptDeliveryProgress = durable.transcriptDeliveryProgress;
		this.#transcriptProof = durable.transcriptProof;
		if (options.recoveredGrowthIntent) {
			this.#growthWindow = { intent: options.recoveredGrowthIntent, pendingAdmissions: 0 };
		}

		this.publishStatus();
		this.#tailTask = this.observeTail();
	}

	get identity(): ExternalSessionIdentity {
		return this.#identity;
	}

	get degraded(): boolean {
		return this.#degraded;
	}

	get turnState(): "idle" | "busy" {
		return this.#turnState;
	}

	get followUpQueueDepth(): number {
		return this.#followUpQueueDepth;
	}

	get transcriptProof(): TranscriptProof {
		return this.#transcriptProof;
	}

	waitForFatalFailure(): Promise<MainSessionHostError> {
		return this.#fatalFailure.promise;
	}

	private wakeTail(): void {
		this.#tailWake.resolve();
		this.#tailWake = Promise.withResolvers<void>();
	}

	private admittedOperationRef(event: Record<string, unknown>): string | undefined {
		const scope = isRecord(event.scope) ? event.scope : undefined;
		const candidates = [
			eventString(event, "operationRef", "operation_ref", "opRef", "op_ref", "clientRef", "client_ref"),
			scope && eventString(scope, "attemptId", "attempt_id", "operationRef", "operation_ref", "clientRef", "client_ref"),
		];
		for (const candidate of candidates) {
			if (!candidate) continue;
			if (this.#admittedOperations.has(candidate)) return candidate;
			const prefix = `${this.sessionId}:`;
			if (candidate.startsWith(prefix)) {
				const operationRef = candidate.slice(prefix.length);
				if (this.#admittedOperations.has(operationRef)) return operationRef;
			}
		}
		return undefined;
	}
	private publishStatus(): void {
		try {
			this.#journal.setMainSessionStatus?.(this.#turnState, this.#followUpQueueDepth);
		} catch {
			// Status is best-effort observation; durable journal writes remain authoritative.
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
		const payload = this.attemptPayload(event);
		const key = `${payload.lineage}\u0000${payload.attempt_id}\u0000${payload.generation}`;
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
			const encoded = payloadJson(finalized.payload);
			const checkpoint = this.#journalTailCheckpoint ?? this.#tailCheckpoint;
			if (checkpoint && this.#journal.journalAppendTranscriptProjection) {
				this.#journal.journalAppendTranscriptProjection(
					"assistant_message",
					encoded,
					this.#tailCheckpoint,
					checkpoint,
					expectedDelivery,
					nextDelivery,
				);
				this.#tailCheckpoint = checkpoint;
			} else {
				this.#journal.journalAppend("assistant_message", encoded);
				this.#state.advanceTranscriptDeliveryProgress(expectedDelivery, nextDelivery);
			}
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
			if (operationRef) this.#admittedOperations.delete(operationRef);
			this.#turnState = this.#admittedOperations.size === 0 ? "idle" : "busy";
			if (type === "agent_end") this.appendTurnTransition("turn_end", event);
			this.publishStatus();
			this.finishGrowthWindowIfSettled();
			return;
		}
		if (type === "agent_failed") {
			const operationRef = this.admittedOperationRef(event);
			if (operationRef) this.#admittedOperations.delete(operationRef);
			this.#turnState = this.#admittedOperations.size === 0 ? "idle" : "busy";
			this.publishStatus();
			this.appendTurnTransition("turn_end", event);
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
	): boolean {
		try {
			const encoded = payloadJson(payload);
			const checkpoint = this.#journalTailCheckpoint ?? this.#tailCheckpoint;
			if (checkpoint && this.#journal.journalAppendTranscriptProjection) {
				this.#journal.journalAppendTranscriptProjection(
					"transcript_delivery_gap",
					encoded,
					this.#tailCheckpoint,
					checkpoint,
					expectedDelivery,
					nextDelivery,
				);
				this.#tailCheckpoint = checkpoint;
			} else {
				this.#journal.journalAppend("transcript_delivery_gap", encoded);
				this.#state.advanceTranscriptDeliveryProgress(expectedDelivery, nextDelivery);
			}
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
			const event = isRecord(entry.payload) ? (isRecord(entry.payload.message) ? entry.payload.message : entry.payload) : undefined;
			const delivered = event
				? this.appendFinalAssistantMessage(event, `transcript:${entry.id}`, expectedDelivery, nextDelivery)
				: this.advanceTranscriptDelivery(expectedDelivery, nextDelivery);
			if (!delivered || this.#failure) return;
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
		this.publishStatus();
		return true;
	}

	private observeIdentity(identity: ExternalSessionIdentity, entries: readonly SupervisorTranscriptEntry[]): void {
		if (sameExternalFingerprint(this.#identity, identity)) return;
		if (!this.#growthWindow) {
			markFailedClosed(this.#state, "main_identity_mismatch");
			throw this.enterFailure("main_identity_mismatch", new Error("External transcript changed outside a growth window."));
		}
		if (!attestsExternalTranscriptGrowth(this.#identity, entries.map(entry => entry.payload))) {
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

	private settleTerminalTail(tail: SupervisorTailEvents): void {
		if (!tail.terminal) return;
		for (const [opRef, admission] of this.#admittedOperations) {
			if (this.terminalTailSettlesAdmission(tail, admission)) this.#admittedOperations.delete(opRef);
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
				const tail = await this.#supervisor.tailEvents();
				if (this.#disposed) return;
				transientFailures = 0;
				if (!tail.complete) {
					const wake = this.#tailWake.promise;
					await Promise.race([Bun.sleep(100), wake, this.#tailStop.promise]);
					continue;
				}
				const pendingProofBound = this.bindPendingTranscriptProof(tail);
				if (pendingProofBound) {
					this.settleTerminalTail(tail);
					const wake = this.#tailWake.promise;
					await Promise.race([Bun.sleep(tail.terminal && this.#turnState === "idle" ? 500 : 100), wake, this.#tailStop.promise]);
					continue;
				}
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
				if (reason === "tail_unavailable" && transientFailures < 8) {
					transientFailures += 1;
					const backoffMs = Math.min(250 * 2 ** (transientFailures - 1), 4_000);
					await Promise.race([Bun.sleep(backoffMs), this.#tailStop.promise]);
					continue;
				}
				this.enterFailure(reason, error);
				return;
			}
		}
	}

	private assertUsable(): void {
		if (this.#disposed) throw new MainSessionHostError("host_disposed");
		if (this.#failure) throw this.#failure;
		if (this.#degraded) throw new MainSessionHostError("host_degraded");
		if (this.#transcriptProof === "pending") throw new MainSessionHostError("transcript_proof_pending");
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

	async admit(deliveredAs: "prompt" | "steer" | "follow_up", text: string, opRef: string): Promise<void> {
		this.assertUsable();
		if (!text.trim()) throw new MainSessionHostError(`${deliveredAs}_empty`, "A main-session message must not be empty.");
		if (!opRef.trim()) throw new MainSessionHostError("operation_ref_empty", "An admitted operation requires an operation reference.");
		const growth = this.beginGrowthWindow();
		growth.pendingAdmissions += 1;
		this.#admittedOperations.set(opRef, { admittedAt: this.#tailCheckpoint });
		try {
			if (deliveredAs === "prompt") await this.#supervisor.sendPrompt(text, opRef);
			else if (deliveredAs === "steer") await this.#supervisor.sendSteer(text, opRef);
			else await this.#supervisor.followUp(text, opRef);
			this.wakeTail();
			if (this.#admittedOperations.has(opRef)) {
				if (deliveredAs === "follow_up") this.#followUpQueueDepth += 1;
				else this.#turnState = "busy";
				this.publishStatus();
			}
		} catch (error) {
			this.#admittedOperations.delete(opRef);
			const reason = error instanceof HostSupervisorError ? error.reason : "turn_admission_failed";
			throw new MainSessionHostError(reason, error instanceof Error ? error.message : String(error), { cause: error });
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
