import { MainSessionGateRegistry, type GateHandle, type MainGateResolution } from "./gates";
import {
	GatewayStateStore,
	sameExternalFingerprint,
	type ExternalSessionIdentity,
	type GrowthIntent,
} from "./state";
import { HostSupervisorError, type HostSupervisor, type SupervisorEvent } from "./supervisor";

export interface MainSessionJournal {
	journalAppend(kind: string, payloadJson: string): unknown;
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
	readonly now?: () => number;
	readonly gates?: MainSessionGateRegistry;
}

interface GrowthWindow {
	readonly intent: GrowthIntent;
	pendingAdmissions: number;
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


const FATAL_EXTERNAL_IDENTITY_REASONS = new Set([
	"session_deleted",
	"session_unavailable",
	"session_ambiguous",
	"session_terminal_uncertain",
	"session_locator_mismatch",
	"growth_intent_mismatch",
	"main_identity_mismatch",
	"tail_retention_gap",
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
	readonly #journaledAttemptTransitions = new Map<string, JournaledAttemptTransitions>();
	readonly #admittedOperations = new Map<string, "prompt" | "steer" | "follow_up">();
	readonly #seenTailEvents = new Set<string>();
	readonly #fatalFailure = Promise.withResolvers<MainSessionHostError>();
	readonly #tailStop = Promise.withResolvers<void>();
	#tailWake = Promise.withResolvers<void>();
	readonly #highestTailSequenceByGeneration = new Map<number, number>();
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

	waitForFatalFailure(): Promise<MainSessionHostError> {
		return this.#fatalFailure.promise;
	}

	private wakeTail(): void {
		this.#tailWake.resolve();
		this.#tailWake = Promise.withResolvers<void>();
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
			this.#journal.journalAppend(kind, payloadJson(payload));
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

	private appendFinalAssistantMessage(event: Record<string, unknown>, fallbackKey: string): void {
		const finalized = finalizedAssistantMessage(event, fallbackKey);
		if (!finalized || this.#finalizedAssistantMessageKeys.has(finalized.key)) return;
		if (!this.appendJournalEvent("assistant_message", finalized.payload)) return;
		this.#finalizedAssistantMessageKeys.add(finalized.key);
		while (this.#finalizedAssistantMessageKeys.size > MAX_JOURNALED_ATTEMPTS) {
			const oldest = this.#finalizedAssistantMessageKeys.values().next().value;
			if (!oldest) break;
			this.#finalizedAssistantMessageKeys.delete(oldest);
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
			this.#turnState = "idle";
			if (type === "agent_end") {
				this.#admittedOperations.clear();
				this.appendTurnTransition("turn_end", event);
			}
			this.publishStatus();
			this.finishGrowthWindowIfSettled();
			return;
		}
		if (type === "agent_failed") {
			this.#turnState = "idle";
			this.#admittedOperations.clear();
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
		if (type === "message_end") this.appendFinalAssistantMessage(event, `event:${eventString(event, "__tail_id") ?? JSON.stringify(event)}`);
	}

	private observeTranscript(entries: readonly unknown[]): void {
		for (const [index, entry] of entries.entries()) {
			if (!isRecord(entry)) continue;
			const event = isRecord(entry.message) ? entry.message : entry;
			this.appendFinalAssistantMessage(event, `transcript:${index}:${JSON.stringify(entry)}`);
		}
	}

	private observeIdentity(identity: ExternalSessionIdentity): void {
		if (sameExternalFingerprint(this.#identity, identity)) return;
		if (!this.#growthWindow) {
			markFailedClosed(this.#state, "main_identity_mismatch");
			throw this.enterFailure("main_identity_mismatch", new Error("External transcript changed outside a growth window."));
		}
		this.#identity = identity;
	}

	private async observeTail(): Promise<void> {
		while (!this.#disposed && !this.#failure) {
			try {
				const tail = await this.#supervisor.tailEvents();
				if (this.#disposed) return;
				if (tail.retentionGap) {
					this.enterFailure("tail_retention_gap", new Error("Broker tail reported a retention gap."));
					return;
				}
				const priorTranscriptEntries = this.#identity.transcript?.entryCount ?? 0;
				this.observeIdentity(tail.identity);
				for (const event of tail.events) this.observeExternalEvent(event);
				this.observeTranscript(tail.transcriptEntries.slice(priorTranscriptEntries));
				if (tail.terminal) {
					this.#turnState = "idle";
					this.#admittedOperations.clear();
					this.publishStatus();
					this.finishGrowthWindowIfSettled();
				}
				const wake = this.#tailWake.promise;
				await Promise.race([Bun.sleep(tail.terminal && this.#turnState === "idle" ? 500 : 100), wake, this.#tailStop.promise]);
			} catch (error) {
				if (this.#disposed) return;
				const reason = error instanceof HostSupervisorError ? error.reason : "tail_observation_failed";
				this.enterFailure(reason, error);
				return;
			}
		}
	}

	private assertUsable(): void {
		if (this.#disposed) throw new MainSessionHostError("host_disposed");
		if (this.#failure) throw this.#failure;
		if (this.#degraded) throw new MainSessionHostError("host_degraded");
	}

	private beginGrowthWindow(): GrowthWindow {
		const existing = this.#growthWindow;
		if (existing) return existing;
		this.assertUsable();
		const durable = this.#state.read();
		if (durable.bootstrapState !== "COMMITTED" || !durable.mainIdentity || durable.growthIntent) {
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

	private finishGrowthWindowIfSettled(): void {
		const growth = this.#growthWindow;
		if (!growth || growth.pendingAdmissions > 0 || this.#turnState !== "idle" || this.#followUpQueueDepth > 0) return;
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
		try {
			if (deliveredAs === "prompt") await this.#supervisor.sendPrompt(text, opRef);
			else if (deliveredAs === "steer") await this.#supervisor.sendSteer(text, opRef);
			else await this.#supervisor.followUp(text, opRef);
			this.#admittedOperations.set(opRef, deliveredAs);
			this.wakeTail();
			if (deliveredAs === "follow_up") this.#followUpQueueDepth += 1;
			else this.#turnState = "busy";
			this.publishStatus();
		} catch (error) {
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
