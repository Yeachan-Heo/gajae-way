import * as fs from "node:fs";
import * as path from "node:path";
import {
	BrokerCli,
	BrokerCliError,
	type BrokerOperationReceipt,
	type SdkSessionRowV1,
	type SdkTailEnvelopeV1,
} from "../broker/cli";
import {
	attestsExternalTranscriptGrowth,
	fingerprintTranscriptEntries,
	type ExternalSessionIdentity,
	type TranscriptFingerprint,
} from "./state";

export type SupervisorTurnState = "idle" | "busy";

export interface SupervisorEvent {
	readonly kind: string;
	readonly id?: string;
	readonly generation?: number;
	readonly seq?: number;
	readonly payload: unknown;
}

export interface SupervisorTailEvents {
	readonly identity: ExternalSessionIdentity;
	readonly transcriptEntries: readonly unknown[];
	readonly events: readonly SupervisorEvent[];
	readonly terminal: boolean;
	readonly retentionGap: boolean;
}

export interface SupervisorVerification {
	readonly identity: ExternalSessionIdentity;
	readonly transcriptEntries?: readonly unknown[];
	readonly turnState: SupervisorTurnState;
	readonly followUpQueueDepth: number;
}

/**
 * Controls an already-running session without becoming its process owner.
 * `dispose` only relinquishes gajaeway's observation/control client state.
 */
export interface HostSupervisor {
	discover(sessionId: string): Promise<SupervisorVerification>;
	verify(identity: ExternalSessionIdentity): Promise<SupervisorVerification>;
	sendPrompt(text: string, opRef: string): Promise<BrokerOperationReceipt>;
	sendSteer(text: string, opRef: string): Promise<BrokerOperationReceipt>;
	followUp(text: string, opRef: string): Promise<BrokerOperationReceipt>;
	tailEvents(): Promise<SupervisorTailEvents>;
	turnState(): Promise<{ readonly turnState: SupervisorTurnState; readonly followUpQueueDepth: number }>;
	dispose(): Promise<void>;
}

export class HostSupervisorError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason, options: { readonly cause?: unknown } = {}) {
		super(message, options.cause === undefined ? undefined : { cause: options.cause });
		this.name = "HostSupervisorError";
		this.reason = reason;
	}
}

export interface ExternalHostSupervisorOptions {
	readonly broker: BrokerCli;
	/** Canonical workspace selected by the profile; the broker locator must match exactly. */
	readonly workspace: string;
	readonly tailTimeoutMs?: number;
	readonly commandTimeoutMs?: number;
}

function transcriptEntries(tail: SdkTailEnvelopeV1): unknown[] {
	return tail.items.filter(item => item.kind === "transcript").map(item => item.payload);
}

function eventItems(tail: SdkTailEnvelopeV1): SupervisorEvent[] {
	return tail.items
		.filter(item => item.kind !== "transcript")
		.map(item => ({
			kind: item.kind,
			...(item.id === undefined ? {} : { id: item.id }),
			...(item.generation === undefined ? {} : { generation: item.generation }),
			...(item.seq === undefined ? {} : { seq: item.seq }),
			payload: item.payload,
		}));
}

function identityFromRow(row: SdkSessionRowV1, transcript: TranscriptFingerprint | undefined): ExternalSessionIdentity {
	return {
		version: 1,
		sessionId: row.sessionId,
		locator: { repo: row.locator.repo, stateRoot: row.locator.stateRoot },
		endpointGeneration: row.endpointGeneration,
		...(row.hostIncarnation === undefined ? {} : { hostIncarnation: row.hostIncarnation }),
		...(transcript === undefined ? {} : { transcript }),
	};
}

function isNormalTailTimeout(error: unknown): boolean {
	return error instanceof BrokerCliError && error.code === "tail_timeout";
}

function isRetentionGap(error: unknown): boolean {
	return error instanceof BrokerCliError && error.code === "retention_gap";
}

function unavailableReason(row: SdkSessionRowV1): string | undefined {
	if (row.deleted) return "session_deleted";
	if (!row.live) return "session_unavailable";
	if (row.ambiguous) return "session_ambiguous";
	if (row.terminalUncertain) return "session_terminal_uncertain";
	return undefined;
}

function samePathIdentity(left: string, right: string): boolean {
	if (path.resolve(left) === path.resolve(right)) return true;
	try {
		return fs.realpathSync.native(left) === fs.realpathSync.native(right);
	} catch {
		return false;
	}
}

/** External-only supervisor backed by `gjc sdk session`; it never creates, resumes, or terminates a GJC session. */
export class ExternalHostSupervisor implements HostSupervisor {
	readonly #broker: BrokerCli;
	readonly #workspace: string;
	readonly #tailTimeoutMs: number;
	readonly #commandTimeoutMs: number;
	#identity: ExternalSessionIdentity | undefined;
	#disposed = false;

	constructor(options: ExternalHostSupervisorOptions) {
		this.#broker = options.broker;
		this.#workspace = path.resolve(options.workspace);
		this.#tailTimeoutMs = options.tailTimeoutMs ?? 1_000;
		this.#commandTimeoutMs = options.commandTimeoutMs ?? 5_000;
		if (!Number.isSafeInteger(this.#tailTimeoutMs) || this.#tailTimeoutMs < 1 || this.#tailTimeoutMs > 20_000) {
			throw new HostSupervisorError("tail_timeout_invalid", "tailTimeoutMs must be an integer in 1..=20000.");
		}
		if (!Number.isSafeInteger(this.#commandTimeoutMs) || this.#commandTimeoutMs < 1 || this.#commandTimeoutMs > 20_000) {
			throw new HostSupervisorError("command_timeout_invalid", "commandTimeoutMs must be an integer in 1..=20000.");
		}
	}

	async discover(sessionId: string): Promise<SupervisorVerification> {
		this.assertUsable();
		if (!sessionId.trim()) throw new HostSupervisorError("session_id_required", "External adoption requires an exact session id.");
		const verified = await this.verifyRow(sessionId, undefined);
		this.#identity = verified.identity;
		return verified;
	}

	async verify(identity: ExternalSessionIdentity): Promise<SupervisorVerification> {
		this.assertUsable();
		const verified = await this.verifyRow(identity.sessionId, identity);
		this.#identity = verified.identity;
		return verified;
	}

	async sendPrompt(text: string, opRef: string): Promise<BrokerOperationReceipt> {
		return await this.#broker.sendPrompt(this.requireIdentity().sessionId, text, opRef, { timeoutMs: this.#commandTimeoutMs });
	}

	async sendSteer(text: string, opRef: string): Promise<BrokerOperationReceipt> {
		return await this.#broker.controlTurn(this.requireIdentity().sessionId, "turn.steer", text, opRef, {
			timeoutMs: this.#commandTimeoutMs,
		});
	}

	async followUp(text: string, opRef: string): Promise<BrokerOperationReceipt> {
		return await this.#broker.controlTurn(this.requireIdentity().sessionId, "turn.follow_up", text, opRef, {
			timeoutMs: this.#commandTimeoutMs,
		});
	}

	async tailEvents(): Promise<SupervisorTailEvents> {
		const identity = this.requireIdentity();
		let tail: SdkTailEnvelopeV1;
		try {
			tail = await this.#broker.tailSession(identity.sessionId, {
				repo: identity.locator.repo,
				untilIdle: true,
				strict: true,
				allEvents: true,
				timeoutMs: this.#tailTimeoutMs,
			});
		} catch (error) {
			if (isNormalTailTimeout(error)) {
				return { identity, transcriptEntries: [], events: [], terminal: false, retentionGap: false };
			}
			if (isRetentionGap(error)) throw new HostSupervisorError("tail_retention_gap", "Broker tail reported a retention gap.", { cause: error });
			throw this.wrapBrokerError("tail_unavailable", error);
		}
		const reason = unavailableReason(tail.session);
		if (reason) throw new HostSupervisorError(reason, `The adopted session ${identity.sessionId} is no longer safely available.`);
		if (!samePathIdentity(tail.session.locator.repo, identity.locator.repo) || !samePathIdentity(tail.session.locator.stateRoot, identity.locator.stateRoot)) {
			throw new HostSupervisorError("session_locator_mismatch", "Broker tail locator did not match the adopted external session.");
		}
		const entries = transcriptEntries(tail);
		const fresh = identityFromRow(tail.session, entries.length > 0 ? fingerprintTranscriptEntries(entries) : identity.transcript);
		if (!attestsExternalTranscriptGrowth(identity, entries.length > 0 ? entries : [])) {
			throw new HostSupervisorError("growth_intent_mismatch", "Broker transcript no longer has the adopted transcript as an append-only prefix.");
		}
		this.#identity = fresh;
		return {
			identity: fresh,
			transcriptEntries: entries,
			events: eventItems(tail),
			terminal: tail.terminal === true,
			retentionGap: tail.gap !== undefined,
		};
	}

	async turnState(): Promise<{ readonly turnState: SupervisorTurnState; readonly followUpQueueDepth: number }> {
		const identity = this.requireIdentity();
		try {
			const current = await this.#broker.contextState(identity.sessionId, { timeoutMs: this.#commandTimeoutMs });
			return {
				turnState: current.isStreaming ? "busy" : "idle",
				followUpQueueDepth: current.followUpQueueDepth,
			};
		} catch (error) {
			throw this.wrapBrokerError("turn_state_unavailable", error);
		}
	}

	async dispose(): Promise<void> {
		this.#disposed = true;
		this.#identity = undefined;
	}

	private async verifyRow(expectedSessionId: string, expected: ExternalSessionIdentity | undefined): Promise<SupervisorVerification> {
		let row: SdkSessionRowV1;
		try {
			row = await this.#broker.inspectSession(expectedSessionId, { timeoutMs: this.#commandTimeoutMs });
		} catch (error) {
			throw this.wrapBrokerError("session_unavailable", error);
		}
		const reason = unavailableReason(row);
		if (reason) throw new HostSupervisorError(reason, `The requested external session ${expectedSessionId} is not safely live.`);
		if (!samePathIdentity(row.locator.repo, this.#workspace)) {
			throw new HostSupervisorError("session_workspace_mismatch", "Broker session locator does not match the profile workspace.");
		}
		if (expected && (!samePathIdentity(row.locator.repo, expected.locator.repo) || !samePathIdentity(row.locator.stateRoot, expected.locator.stateRoot))) {
			throw new HostSupervisorError("session_locator_mismatch", "Broker session locator does not match the durable adopted identity.");
		}
		try {
			const metadata = await this.#broker.sessionMetadata(expectedSessionId, { timeoutMs: this.#commandTimeoutMs });
			if (!samePathIdentity(metadata.cwd, this.#workspace)) {
				throw new HostSupervisorError("session_workspace_mismatch", "Session metadata cwd does not match the profile workspace.");
			}
			if (metadata.kind !== "main") {
				throw new HostSupervisorError("session_kind_mismatch", "Only an operator-run main GJC session can be adopted.");
			}
		} catch (error) {
			if (error instanceof HostSupervisorError) throw error;
			throw this.wrapBrokerError("session_metadata_unavailable", error);
		}
		let tail: SupervisorTailEvents | undefined;
		try {
			const provisional = identityFromRow(row, expected?.transcript);
			this.#identity = provisional;
			tail = await this.tailEvents();
		} catch (error) {
			if (!(error instanceof HostSupervisorError && error.reason === "tail_unavailable")) throw error;
			if (expected?.transcript) throw new HostSupervisorError("main_identity_unreadable", "The adopted transcript fingerprint could not be reverified.", { cause: error });
		}
		const identity = tail?.identity ?? identityFromRow(row, expected?.transcript);
		let turnState: SupervisorTurnState = row.activity?.state === "active" ? "busy" : "idle";
		let followUpQueueDepth = 0;
		try {
			const state = await this.turnState();
			turnState = state.turnState;
			followUpQueueDepth = state.followUpQueueDepth;
		} catch (error) {
			if (error instanceof HostSupervisorError && error.reason === "turn_state_unavailable") {
				// Broker index activity is still an honest fallback for initial admission.
			} else {
				throw error;
			}
		}
		return { identity, ...(tail?.transcriptEntries.length ? { transcriptEntries: tail.transcriptEntries } : {}), turnState, followUpQueueDepth };
	}

	private requireIdentity(): ExternalSessionIdentity {
		this.assertUsable();
		if (!this.#identity) throw new HostSupervisorError("session_not_adopted", "No external GJC session has been adopted.");
		return this.#identity;
	}

	private assertUsable(): void {
		if (this.#disposed) throw new HostSupervisorError("host_disposed");
	}

	private wrapBrokerError(reason: string, error: unknown): HostSupervisorError {
		if (error instanceof HostSupervisorError) return error;
		const detail = error instanceof Error ? error.message : String(error);
		return new HostSupervisorError(reason, `${reason}: ${detail}`, { cause: error });
	}
}

export function createExternalHostSupervisor(options: ExternalHostSupervisorOptions): ExternalHostSupervisor {
	return new ExternalHostSupervisor(options);
}
