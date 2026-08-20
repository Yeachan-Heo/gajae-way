import { MainSessionGateRegistry, type GateHandle } from "./gates";
import type { HostedSdkGate, HostedSdkGateResolution, HostedSdkSession } from "./sdk";
import {
	attestAppendOnlyGrowth,
	fingerprintSessionFile,
	GatewayStateStore,
	sameFingerprint,
	type GrowthIntent,
	type SessionFingerprint,
} from "./state";

export interface MainSessionJournal {
	journalAppend(kind: string, payloadJson: string): unknown;
	setRpcHealth?(state: "degraded", reason: string): void;
	setMainSessionStatus?(turnState: "idle" | "busy", followUpQueueDepth: number): void;
	setJournalDegraded?(degraded: boolean): void;
}

export class MainSessionHostError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason) {
		super(message);
		this.name = "MainSessionHostError";
		this.reason = reason;
	}
}

export interface MainSessionHost {
	readonly sessionId: string;
	readonly identity: SessionFingerprint;
	readonly degraded: boolean;
	readonly turnState: "idle" | "busy";
	readonly followUpQueueDepth: number;
	readonly gates: MainSessionGateRegistry;
	/** Synchronously takes ownership of an admitted operation; completion is tracked in the host. */
	admit(deliveredAs: "prompt" | "steer" | "follow_up", text: string): void;

	prompt(text: string): Promise<void>;
	steer(text: string): Promise<void>;
	followUp(text: string): Promise<void>;
	resolveGate(gateId: string, answer: unknown, idempotencyKey: string): Promise<HostedSdkGateResolution>;
	dispose(): Promise<void>;
}

export interface CreateMainSessionHostOptions {
	readonly session: HostedSdkSession;
	readonly identity: SessionFingerprint;
	readonly state: GatewayStateStore;
	readonly journal: MainSessionJournal;
	readonly now?: () => number;
	readonly gates?: MainSessionGateRegistry;
}

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
	for (const key of ["expires_at", "expiresAt", "deadline_at", "deadlineAt"]) {
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
	const gateId = eventString(event, "gate_id", "gateId", "workflowGateId");
	if (!gateId) return undefined;
	return {
		gateId,
		expectedSessionId: eventString(event, "session_id", "sessionId") ?? fallbackSessionId,
		expiresAt: eventTimestamp(event),
	};
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

/**
 * Stable journal payload for one outer SDK attempt.
 *
 * Both `turn_start` and `turn_end` use this exact shape. Provider messages,
 * tool results, and assistant text deliberately remain out of lifecycle rows;
 * finalized assistant text is published only as `assistant_message`.
 */
export interface MainSessionTurnJournalPayload {
	readonly attempt_id: string;
	readonly generation: number;
	readonly lineage: string;
}

interface JournaledAttemptTransitions {
	started: boolean;
	ended: boolean;
}

const MAX_JOURNALED_ATTEMPTS = 1_000;

function turnJournalPayload(event: Record<string, unknown>): MainSessionTurnJournalPayload | undefined {
	const scope = event.scope;
	if (!isRecord(scope)) return undefined;
	const attemptId = eventString(scope, "attemptId");
	const lineage = eventString(scope, "lineage");
	const generation = scope.generation;
	if (!attemptId || !lineage || typeof generation !== "number" || !Number.isSafeInteger(generation) || generation < 0)
		return undefined;
	return { attempt_id: attemptId, generation, lineage };
}

function turnJournalKey(payload: MainSessionTurnJournalPayload): string {
	return `${payload.lineage}\u0000${payload.attempt_id}\u0000${payload.generation}`;
}

interface GrowthWindow {
	readonly intent: GrowthIntent;
	activeMutations: number;
	activeTurns: number;
}

function finalizedAssistantMessage(event: Record<string, unknown>): FinalAssistantMessage | undefined {
	if (event.type !== "message_end" || !isRecord(event.message) || event.message.role !== "assistant") return undefined;
	const message = event.message;
	const content = message.content;
	const text =
		typeof content === "string"
			? content
			: Array.isArray(content)
				? content
						.filter(isRecord)
						.filter((block) => block.type === "text" && typeof block.text === "string")
						.map((block) => block.text as string)
						.join("")
				: "";
	if (!text.trim()) return undefined;
	const messageId = eventString(message, "responseId", "id");
	const timestamp =
		typeof message.timestamp === "number" && Number.isFinite(message.timestamp) ? message.timestamp : undefined;
	const key = messageId ? `response:${messageId}` : `message:${timestamp ?? "unknown"}:${text}`;
	return {
		key,
		payload: {
			finalized: true,
			text,
			...(messageId ? { message_id: messageId } : {}),
			...(timestamp === undefined ? {} : { timestamp }),
		},
	};
}

function journalPayload(event: unknown): string {
	return JSON.stringify(event);
}

function markFailedClosed(state: GatewayStateStore, reason: string): void {
	try {
		state.markFailedClosed(reason);
	} catch {
		// A competing process may already have closed the gateway; never overwrite
		// its more specific durable failure reason from a stale host.
	}
}

class HostedMainSession implements MainSessionHost {
	readonly sessionId: string;
	readonly gates: MainSessionGateRegistry;
	#identity: SessionFingerprint;
	readonly #session: HostedSdkSession;
	readonly #state: GatewayStateStore;
	readonly #journal: MainSessionJournal;
	readonly #now: () => number;
	readonly #unsubscribe: () => void;
	readonly #unsubscribeGates: () => void;
	#turnState: "idle" | "busy" = "idle";
	#followUpQueueDepth = 0;
	#degraded = false;
	#failure: MainSessionHostError | undefined;
	#growthWindow: GrowthWindow | undefined;
	readonly #finalizedAssistantMessageKeys = new Set<string>();
	readonly #journaledAttemptTransitions = new Map<string, JournaledAttemptTransitions>();
	readonly #inFlightOperations = new Set<Promise<void>>();

	#disposed = false;

	constructor(options: CreateMainSessionHostOptions) {
		this.sessionId = options.session.sessionId;
		this.#identity = options.identity;
		this.#session = options.session;
		this.#state = options.state;
		this.#journal = options.journal;
		this.#now = options.now ?? Date.now;
		this.gates = options.gates ?? new MainSessionGateRegistry({ now: this.#now });
		this.#unsubscribe = this.#session.subscribe((event) => this.observeSessionEvent(event));
		this.#unsubscribeGates = this.#session.subscribeGates((gate) => this.observeSdkGate(gate));
		this.refreshFollowUpQueueDepth();
		this.publishStatus();
	}

	get identity(): SessionFingerprint {
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

	private publishStatus(): void {
		try {
			this.#journal.setMainSessionStatus?.(this.#turnState, this.#followUpQueueDepth);
		} catch {
			// Status is an in-memory observation. Journal durability remains the
			// fail-closed boundary and is handled separately below.
		}
	}

	private refreshFollowUpQueueDepth(): void {
		const depth = this.#session.followUpQueueDepth();
		if (!Number.isSafeInteger(depth) || depth < 0) return;
		this.#followUpQueueDepth = Math.min(depth, 0xffff_ffff);
	}

	private enterFailure(reason: string, error: unknown, journalFailure = false): MainSessionHostError {
		if (this.#failure) return this.#failure;
		const detail = error instanceof Error ? error.message : String(error);
		const failure = new MainSessionHostError(reason, `${reason}: ${detail}`);
		this.#failure = failure;
		this.#degraded = true;
		if (journalFailure) {
			try {
				this.#journal.setJournalDegraded?.(true);
			} catch {
				// The journal failure remains authoritative if status publication is unavailable.
			}
		}
		try {
			this.#journal.setRpcHealth?.("degraded", reason);
		} catch {
			// The original durable failure remains authoritative when health reporting
			// is also unavailable.
		}
		return failure;
	}

	private appendJournalEvent(kind: string, payload: unknown): boolean {
		if (this.#disposed || this.#failure) return false;
		try {
			this.#journal.journalAppend(kind, journalPayload(payload));
			return true;
		} catch (error) {
			this.enterFailure("journal_append_failed", error, true);
			return false;
		}
	}

	private appendFinalAssistantMessage(event: Record<string, unknown>): void {
		const finalized = finalizedAssistantMessage(event);
		if (!finalized || this.#finalizedAssistantMessageKeys.has(finalized.key)) return;
		if (!this.appendJournalEvent("assistant_message", finalized.payload)) return;
		this.#finalizedAssistantMessageKeys.add(finalized.key);
		if (this.#finalizedAssistantMessageKeys.size > 1_000) {
			const oldest = this.#finalizedAssistantMessageKeys.values().next().value;
			if (oldest) this.#finalizedAssistantMessageKeys.delete(oldest);
		}
	}

	private appendTurnTransition(kind: "turn_start" | "turn_end", event: Record<string, unknown>): void {
		const payload = turnJournalPayload(event);
		if (!payload) return;
		const key = turnJournalKey(payload);
		const transitions = this.#journaledAttemptTransitions.get(key) ?? { started: false, ended: false };
		this.#journaledAttemptTransitions.set(key, transitions);
		const field = kind === "turn_start" ? "started" : "ended";
		if (transitions[field] || !this.appendJournalEvent(kind, payload)) return;
		transitions[field] = true;
		while (this.#journaledAttemptTransitions.size > MAX_JOURNALED_ATTEMPTS) {
			const oldest = this.#journaledAttemptTransitions.keys().next().value;
			if (!oldest) return;
			this.#journaledAttemptTransitions.delete(oldest);
		}
	}

	private trackAdmittedOperation(operation: Promise<void>): void {
		this.#inFlightOperations.add(operation);
		void operation.then(
			() => {
				this.#inFlightOperations.delete(operation);
			},
			error => {
				this.#inFlightOperations.delete(operation);
				this.enterFailure(error instanceof MainSessionHostError ? error.reason : "turn_execution_failed", error);
			},
		);
	}

	/**
	 * A durable growth intent covers one contiguous append-only transcript window.
	 * Owner interrupts and queued follow-ups may both append inside that same
	 * window; it is refreshed only after every active turn and queued follow-up
	 * has settled.
	 */
	private beginGrowthWindow(): GrowthWindow {
		const existing = this.#growthWindow;
		if (existing) return existing;
		const growth: GrowthWindow = {
			intent: this.beforeMutation(),
			activeMutations: 0,
			activeTurns: this.#turnState === "busy" ? 1 : 0,
		};
		this.#growthWindow = growth;
		return growth;
	}

	private beginMutation(): GrowthWindow {
		const growth = this.beginGrowthWindow();
		growth.activeMutations += 1;
		return growth;
	}

	private finishMutation(growth: GrowthWindow): void {
		if (this.#growthWindow !== growth) return;
		growth.activeMutations = Math.max(0, growth.activeMutations - 1);
		this.refreshFollowUpQueueDepth();
		this.finishGrowthWindowIfSettled();
	}

	private finishGrowthWindowIfSettled(): void {
		const growth = this.#growthWindow;
		if (!growth || growth.activeMutations > 0 || growth.activeTurns > 0 || this.#followUpQueueDepth > 0) return;
		this.#growthWindow = undefined;
		try {
			this.afterMutation(growth.intent);
		} catch (error) {
			this.enterFailure(error instanceof MainSessionHostError ? error.reason : "growth_refresh_failed", error);
		}
	}

	private observeSdkGate(gate: HostedSdkGate): void {
		if (gate.sessionId !== this.sessionId) return;
		if (!this.gates.observeOpen({ gateId: gate.gateId, expectedSessionId: gate.sessionId, expiresAt: gate.expiresAt }))
			return;
		this.appendJournalEvent("gate_open", {
			gate_id: gate.gateId,
			session_id: gate.sessionId,
			...(gate.expiresAt === undefined ? {} : { expires_at: gate.expiresAt }),
			gate: gate.payload,
		});
	}

	private observeGateResolved(gateId: string, payload: unknown): void {
		if (!this.gates.observeResolved(gateId)) return;
		this.appendJournalEvent("gate_resolved", { gate_id: gateId, session_id: this.sessionId, event: payload });
	}

	private observeSessionEvent(event: unknown): void {
		if (this.#disposed) return;
		const record = isRecord(event) ? event : undefined;
		const type = record?.type;
		if (type === "turn_start" || type === "agent_start") {
			this.#turnState = "busy";
			this.refreshFollowUpQueueDepth();
			if (type === "turn_start" && this.#growthWindow) this.#growthWindow.activeTurns += 1;
			this.publishStatus();
			// `agent_start` and each `turn_start` share the outer attempt scope.
			// A tool-using attempt can have several SDK turns, so journal only once.
			if (record) this.appendTurnTransition("turn_start", record);
			return;
		}
		if (type === "turn_end" || type === "agent_end") {
			this.#turnState = "idle";
			this.refreshFollowUpQueueDepth();
			if (this.#growthWindow) {
				if (type === "turn_end" && this.#growthWindow.activeTurns > 0) this.#growthWindow.activeTurns -= 1;
				if (type === "agent_end") this.#growthWindow.activeTurns = 0;
			}
			this.publishStatus();
			// `turn_end` is per SDK/provider turn. Only terminal `agent_end` closes
			// the outer attempt represented by a journal `turn_end`.
			if (type === "agent_end" && record) this.appendTurnTransition("turn_end", record);
			this.finishGrowthWindowIfSettled();
			return;
		}
		if (type === "gate_expired") {
			const gate = gateFromEvent(event, this.sessionId);
			if (gate) this.gates.observeExpired(gate.gateId);
			return;
		}
		if (type === "gate_open" || type === "workflow_gate" || (type === "action_needed" && record?.kind === "ask")) {
			const gate = gateFromEvent(event, this.sessionId);
			if (gate && gate.expectedSessionId === this.sessionId && this.gates.observeOpen(gate)) {
				this.appendJournalEvent("gate_open", event);
			}
			return;
		}
		if (type === "gate_resolved" || type === "action_resolved") {
			const gate = gateFromEvent(event, this.sessionId);
			if (gate && gate.expectedSessionId === this.sessionId) this.observeGateResolved(gate.gateId, event);
			return;
		}
		if (type === "message_end" && record) this.appendFinalAssistantMessage(record);
	}

	private assertUsable(): void {
		if (this.#disposed) throw new MainSessionHostError("host_disposed");
		if (this.#failure) throw this.#failure;
		if (this.#degraded) throw new MainSessionHostError("journal_degraded");
	}

	private beforeMutation(): GrowthIntent {
		this.assertUsable();
		const durable = this.#state.read();
		if (durable.bootstrapState !== "COMMITTED" || !durable.mainIdentity || durable.growthIntent) {
			markFailedClosed(this.#state, "growth_protocol_invalid");
			throw new MainSessionHostError("growth_protocol_invalid");
		}
		let observed: SessionFingerprint;
		try {
			observed = fingerprintSessionFile(durable.mainIdentity.canonicalPath);
		} catch (error) {
			markFailedClosed(this.#state, "main_identity_unreadable");
			throw new MainSessionHostError(
				"main_identity_unreadable",
				error instanceof Error ? error.message : String(error),
			);
		}
		if (!sameFingerprint(durable.mainIdentity, observed)) {
			markFailedClosed(this.#state, "main_identity_mismatch");
			throw new MainSessionHostError("main_identity_mismatch");
		}
		const startedAt = this.#now();
		try {
			this.#state.writeGrowthIntent(observed, startedAt);
		} catch (error) {
			markFailedClosed(this.#state, "growth_intent_write_failed");
			throw new MainSessionHostError(
				"growth_intent_write_failed",
				error instanceof Error ? error.message : String(error),
			);
		}
		return { base: observed, startedAt };
	}

	private afterMutation(intent: GrowthIntent): void {
		let refreshed: SessionFingerprint;
		try {
			refreshed = fingerprintSessionFile(intent.base.canonicalPath);
			if (!attestAppendOnlyGrowth(intent.base, refreshed)) {
				markFailedClosed(this.#state, "growth_intent_mismatch");
				throw new MainSessionHostError(
					"growth_intent_mismatch",
					"The transcript changed outside the active append-only growth window.",
				);
			}
			this.#state.refreshAfterGrowth(intent, refreshed);
			this.#identity = refreshed;
		} catch (error) {
			if (!(error instanceof MainSessionHostError && error.reason === "growth_intent_mismatch")) {
				markFailedClosed(this.#state, "growth_refresh_failed");
			}
			throw error instanceof MainSessionHostError
				? error
				: new MainSessionHostError("growth_refresh_failed", error instanceof Error ? error.message : String(error));
		}
	}

	private async mutate(action: () => Promise<void>): Promise<void> {
		const growth = this.beginMutation();
		let mutationError: unknown;
		try {
			await action();
			this.assertUsable();
		} catch (error) {
			mutationError = error;
		}
		try {
			this.finishMutation(growth);
		} catch (error) {
			if (!mutationError) mutationError = error;
		}
		if (mutationError) throw mutationError;
		this.assertUsable();
	}

	admit(deliveredAs: "prompt" | "steer" | "follow_up", text: string): void {
		const operation =
			deliveredAs === "prompt" ? this.prompt(text) : deliveredAs === "steer" ? this.steer(text) : this.followUp(text);
		try {
			this.refreshFollowUpQueueDepth();
			this.publishStatus();
			this.assertUsable();
		} catch (error) {
			void operation.catch(() => undefined);
			throw error;
		}
		this.trackAdmittedOperation(operation);
	}

	async prompt(text: string): Promise<void> {
		if (!text.trim()) throw new MainSessionHostError("prompt_empty", "A main-session prompt must not be empty.");
		await this.mutate(() => this.#session.prompt(text));
	}

	async steer(text: string): Promise<void> {
		if (!text.trim()) throw new MainSessionHostError("steer_empty", "A main-session steer must not be empty.");
		await this.mutate(() => this.#session.steer(text));
	}

	async followUp(text: string): Promise<void> {
		if (!text.trim()) throw new MainSessionHostError("follow_up_empty", "A main-session follow-up must not be empty.");
		await this.mutate(() => this.#session.followUp(text));
		this.publishStatus();
	}

	async resolveGate(gateId: string, answer: unknown, idempotencyKey: string): Promise<HostedSdkGateResolution> {
		this.assertUsable();
		const resolution = await this.#session.answerGate(gateId, answer, idempotencyKey);
		if (resolution === "resolved") this.observeGateResolved(gateId, { answer });
		if (resolution === "expired") this.gates.observeExpired(gateId);
		this.assertUsable();
		return resolution;
	}

	async dispose(): Promise<void> {
		if (this.#disposed) return;
		this.#disposed = true;
		this.#unsubscribe();
		this.#unsubscribeGates();
		await this.#session.dispose();
	}
}

/** Wires the strict-resumed SDK session to synchronous durable journal append. */
export function createMainSessionHost(options: CreateMainSessionHostOptions): MainSessionHost {
	if (
		options.session.sessionId !== options.identity.sessionId ||
		options.session.sessionFile !== options.identity.canonicalPath
	) {
		throw new MainSessionHostError("host_identity_mismatch");
	}
	return new HostedMainSession(options);
}
