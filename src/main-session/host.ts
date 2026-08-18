import type { HostedSdkSession } from "./sdk";
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
	readonly phase: "P3";
	readonly sessionId: string;
	readonly identity: SessionFingerprint;
	readonly degraded: boolean;
	prompt(text: string): Promise<void>;
	dispose(): Promise<void>;
}

export interface CreateMainSessionHostOptions {
	readonly session: HostedSdkSession;
	readonly identity: SessionFingerprint;
	readonly state: GatewayStateStore;
	readonly journal: MainSessionJournal;
	readonly now?: () => number;
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
	readonly phase = "P3" as const;
	readonly sessionId: string;
	#identity: SessionFingerprint;
	readonly #session: HostedSdkSession;
	readonly #state: GatewayStateStore;
	readonly #journal: MainSessionJournal;
	readonly #now: () => number;
	readonly #unsubscribe: () => void;
	#degraded = false;
	#disposed = false;

	constructor(options: CreateMainSessionHostOptions) {
		this.sessionId = options.session.sessionId;
		this.#identity = options.identity;
		this.#session = options.session;
		this.#state = options.state;
		this.#journal = options.journal;
		this.#now = options.now ?? Date.now;
		this.#unsubscribe = this.#session.subscribe(event => this.appendEvent(event));
	}

	get identity(): SessionFingerprint {
		return this.#identity;
	}

	get degraded(): boolean {
		return this.#degraded;
	}

	private appendEvent(event: unknown): void {
		if (this.#degraded || this.#disposed) return;
		try {
			this.#journal.journalAppend("main_session_event", journalPayload(event));
		} catch {
			this.#degraded = true;
			try {
				this.#journal.setRpcHealth?.("degraded", "journal_append_failed");
			} catch {
				// The durable journal failure is primary; RPC health is best effort.
			}
		}
	}

	private beforeMutation(): GrowthIntent {
		if (this.#disposed) throw new MainSessionHostError("host_disposed");
		if (this.#degraded) throw new MainSessionHostError("journal_degraded");
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

	async prompt(text: string): Promise<void> {
		if (!text.trim()) throw new MainSessionHostError("prompt_empty", "A main-session prompt must not be empty.");
		const intent = this.beforeMutation();
		let promptError: unknown;
		try {
			await this.#session.prompt(text);
		} catch (error) {
			promptError = error;
		} finally {
			this.afterMutation(intent);
		}
		if (promptError) throw promptError;
	}

	async dispose(): Promise<void> {
		if (this.#disposed) return;
		this.#disposed = true;
		this.#unsubscribe();
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
