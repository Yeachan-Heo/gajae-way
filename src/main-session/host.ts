import { MainSessionGateRegistry, type GateHandle } from "./gates";
import type { HostedSdkGate, HostedSdkGateResolution, HostedSdkSession } from "./sdk";
import {
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
	readonly phase: "P4";
	readonly sessionId: string;
	readonly identity: SessionFingerprint;
	readonly degraded: boolean;
	readonly turnState: "idle" | "busy";
	readonly followUpQueueDepth: number;
	readonly gates: MainSessionGateRegistry;
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
	readonly phase = "P4" as const;
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
	#failedJournalEvents: Array<{ kind: string; payload: unknown }> = [];
	#disposed = false;

	constructor(options: CreateMainSessionHostOptions) {
		this.sessionId = options.session.sessionId;
		this.#identity = options.identity;
		this.#session = options.session;
		this.#state = options.state;
		this.#journal = options.journal;
		this.#now = options.now ?? Date.now;
		this.gates = options.gates ?? new MainSessionGateRegistry({ now: this.#now });
		this.#unsubscribe = this.#session.subscribe(event => this.observeSessionEvent(event));
		this.#unsubscribeGates = this.#session.subscribeGates(gate => this.observeSdkGate(gate));
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

	private retainFailedJournalEvent(kind: string, payload: unknown): void {
		this.#failedJournalEvents.push({ kind, payload });
		if (this.#failedJournalEvents.length > 1_000) this.#failedJournalEvents.shift();
	}

	private appendJournalEvent(kind: string, payload: unknown): void {
		if (this.#disposed) return;
		if (this.#degraded) {
			this.retainFailedJournalEvent(kind, payload);
			return;
		}
		try {
			this.#journal.journalAppend(kind, journalPayload(payload));
		} catch {
			this.retainFailedJournalEvent(kind, payload);
			this.#degraded = true;
			try {
				this.#journal.setJournalDegraded?.(true);
				this.#journal.setRpcHealth?.("degraded", "journal_append_failed");
			} catch {
				// The durable journal failure is primary; RPC health is best effort.
			}
		}
	}

	private observeSdkGate(gate: HostedSdkGate): void {
		if (gate.sessionId !== this.sessionId) return;
		if (!this.gates.observeOpen({ gateId: gate.gateId, expectedSessionId: gate.sessionId, expiresAt: gate.expiresAt })) return;
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
			this.publishStatus();
			this.appendJournalEvent("turn_start", event);
			return;
		}
		if (type === "turn_end" || type === "agent_end") {
			this.#turnState = "idle";
			this.refreshFollowUpQueueDepth();
			this.publishStatus();
			this.appendJournalEvent("turn_end", event);
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
		if (
			type === "assistant_message" ||
			(type === "message_update" && isRecord(record?.assistantMessageEvent)) ||
			(type === "message_end" && record?.role === "assistant")
		) {
			this.appendJournalEvent("assistant_message", event);
		}
	}

	private assertUsable(): void {
		if (this.#disposed) throw new MainSessionHostError("host_disposed");
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
			throw new MainSessionHostError("main_identity_unreadable", error instanceof Error ? error.message : String(error));
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
			throw new MainSessionHostError("growth_intent_write_failed", error instanceof Error ? error.message : String(error));
		}
		return { base: observed, startedAt };
	}

	private afterMutation(intent: GrowthIntent): void {
		let refreshed: SessionFingerprint;
		try {
			refreshed = fingerprintSessionFile(intent.base.canonicalPath);
			this.#state.refreshAfterGrowth(intent, refreshed);
			this.#identity = refreshed;
		} catch (error) {
			markFailedClosed(this.#state, "growth_refresh_failed");
			throw new MainSessionHostError("growth_refresh_failed", error instanceof Error ? error.message : String(error));
		}
	}

	private async mutate(action: () => Promise<void>): Promise<void> {
		const intent = this.beforeMutation();
		try {
			await action();
		} finally {
			this.afterMutation(intent);
		}
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
		this.assertUsable();
		await this.#session.followUp(text);
		this.refreshFollowUpQueueDepth();
		this.publishStatus();
	}

	async resolveGate(gateId: string, answer: unknown, idempotencyKey: string): Promise<HostedSdkGateResolution> {
		this.assertUsable();
		const resolution = await this.#session.answerGate(gateId, answer, idempotencyKey);
		if (resolution === "resolved") this.observeGateResolved(gateId, { answer });
		if (resolution === "expired") this.gates.observeExpired(gateId);
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
	if (options.session.sessionId !== options.identity.sessionId || options.session.sessionFile !== options.identity.canonicalPath) {
		throw new MainSessionHostError("host_identity_mismatch");
	}
	return new HostedMainSession(options);
}
