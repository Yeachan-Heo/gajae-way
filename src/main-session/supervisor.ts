import * as fs from "node:fs";
import * as path from "node:path";
import {
	type BrokerCli,
	BrokerCliError,
	type BrokerOperationReceipt,
	type BrokerTurnStatus,
	type SdkSessionRowV1,
	type SdkTailEnvelopeV1,
} from "../broker/cli";
import {
	type ExternalSessionIdentity,
	fingerprintTranscriptEntries,
	type TailCheckpoint,
	type TranscriptFingerprint,
	type TranscriptProof,
} from "./state";

export type SupervisorTurnState = "idle" | "busy";

export interface SupervisorEvent {
	readonly kind: string;
	readonly id?: string;
	readonly generation?: number;
	readonly seq?: number;
	readonly payload: unknown;
}

export interface SupervisorTranscriptEntry {
	/** Stable broker transcript entry id; never derive this from a local array index. */
	readonly id: string;
	readonly payload: unknown;
}

export interface SupervisorTailEvents {
	readonly identity: ExternalSessionIdentity;
	readonly transcriptEntries: readonly SupervisorTranscriptEntry[];
	readonly events: readonly SupervisorEvent[];
	readonly terminal: boolean;
	/** False only when the broker timed out before yielding a transcript snapshot. */
	readonly complete: boolean;
	readonly retentionGap: boolean;
	readonly checkpoint?: TailCheckpoint;
	readonly resyncCheckpoint?: TailCheckpoint;
}

export interface SupervisorVerification {
	readonly identity: ExternalSessionIdentity;
	readonly transcriptEntries: readonly SupervisorTranscriptEntry[];
	/** Complete-tail ring watermark used only for a proven adoption boundary. */
	readonly initialRingCheckpoint?: TailCheckpoint;
	/** Exact complete tail used to verify an existing durable adoption; it must be replayed into delivery projection. */
	readonly verificationTail?: SupervisorTailEvents;
	/** A complete tail fingerprint, or an explicit pending proof after a bounded tail wait. */
	readonly transcriptProof: TranscriptProof;
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
	operationStatus(opRef: string): Promise<BrokerTurnStatus>;
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

/**
 * Whether a broker admission failure proves that no external effect was accepted.
 * All callers must use this boundary classification rather than inspecting broker
 * error causes independently.
 */
export type AdmissionDisposition = "definitive_rejection" | "ambiguous";

export function classifyAdmissionDisposition(error: unknown): AdmissionDisposition {
	const seen = new Set<unknown>();
	let current: unknown = error;
	while (current instanceof Error && !seen.has(current)) {
		seen.add(current);
		if (current instanceof BrokerCliError && current.definitive) return "definitive_rejection";
		current = current.cause;
	}
	return "ambiguous";
}

export interface ExternalHostSupervisorOptions {
	readonly broker: BrokerCli;
	/** Canonical workspace selected by the profile; the broker locator must match exactly. */
	readonly workspace: string;
	readonly tailTimeoutMs?: number;
	/** Bounded initial proof wait. Production defaults to 15 seconds; fixtures can shorten it. */
	readonly adoptionTailTimeoutMs?: number;
	readonly commandTimeoutMs?: number;
}

function transcriptEntries(tail: SdkTailEnvelopeV1): SupervisorTranscriptEntry[] {
	return tail.items
		.filter((item) => item.kind === "transcript")
		.map((item, index) => {
			if (!item.id) {
				throw new HostSupervisorError(
					"transcript_entry_id_missing",
					`Broker transcript entry ${index} has no stable id.`,
				);
			}
			return { id: item.id, payload: item.payload };
		});
}

function eventItems(tail: SdkTailEnvelopeV1): SupervisorEvent[] {
	return tail.items
		.filter((item) => item.kind !== "transcript")
		.map((item) => ({
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
	readonly #adoptionTailTimeoutMs: number;
	readonly #commandTimeoutMs: number;
	#identity: ExternalSessionIdentity | undefined;
	#disposed = false;

	constructor(options: ExternalHostSupervisorOptions) {
		this.#broker = options.broker;
		this.#workspace = path.resolve(options.workspace);
		this.#tailTimeoutMs = options.tailTimeoutMs ?? 3_000;
		this.#adoptionTailTimeoutMs = options.adoptionTailTimeoutMs ?? 15_000;
		this.#commandTimeoutMs = options.commandTimeoutMs ?? 30_000;
		if (!Number.isSafeInteger(this.#tailTimeoutMs) || this.#tailTimeoutMs < 1 || this.#tailTimeoutMs > 120_000) {
			throw new HostSupervisorError("tail_timeout_invalid", "tailTimeoutMs must be an integer in 1..=120000.");
		}
		if (
			!Number.isSafeInteger(this.#adoptionTailTimeoutMs) ||
			this.#adoptionTailTimeoutMs < 1 ||
			this.#adoptionTailTimeoutMs > 120_000
		) {
			throw new HostSupervisorError(
				"adoption_tail_timeout_invalid",
				"adoptionTailTimeoutMs must be an integer in 1..=120000.",
			);
		}
		if (
			!Number.isSafeInteger(this.#commandTimeoutMs) ||
			this.#commandTimeoutMs < 1 ||
			this.#commandTimeoutMs > 120_000
		) {
			throw new HostSupervisorError("command_timeout_invalid", "commandTimeoutMs must be an integer in 1..=120000.");
		}
	}

	async discover(sessionId: string): Promise<SupervisorVerification> {
		this.assertUsable();
		if (!sessionId.trim())
			throw new HostSupervisorError("session_id_required", "External adoption requires an exact session id.");
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
		return await this.#broker.sendPrompt(this.requireIdentity().sessionId, text, opRef, {
			timeoutMs: this.#commandTimeoutMs,
		});
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

	async operationStatus(opRef: string): Promise<BrokerTurnStatus> {
		return await this.#broker.turnStatus(this.requireIdentity().sessionId, opRef, {
			timeoutMs: this.#commandTimeoutMs,
		});
	}

	async tailEvents(options: { readonly timeoutMs?: number } = {}): Promise<SupervisorTailEvents> {
		const identity = this.requireIdentity();
		let tail: SdkTailEnvelopeV1;
		try {
			tail = await this.#broker.tailSession(identity.sessionId, {
				repo: identity.locator.repo,
				untilIdle: true,
				allEvents: true,
				timeoutMs: options.timeoutMs ?? this.#tailTimeoutMs,
			});
		} catch (error) {
			if (isNormalTailTimeout(error)) {
				return { identity, transcriptEntries: [], events: [], terminal: false, complete: false, retentionGap: false };
			}
			if (isRetentionGap(error)) {
				throw new HostSupervisorError(
					"tail_unavailable",
					"Broker tail did not return a resynchronizable ring envelope.",
					{ cause: error },
				);
			}
			throw this.wrapBrokerError("tail_unavailable", error);
		}
		const reason = unavailableReason(tail.session);
		if (reason)
			throw new HostSupervisorError(reason, `The adopted session ${identity.sessionId} is no longer safely available.`);
		if (
			!samePathIdentity(tail.session.locator.repo, identity.locator.repo) ||
			!samePathIdentity(tail.session.locator.stateRoot, identity.locator.stateRoot)
		) {
			throw new HostSupervisorError(
				"session_locator_mismatch",
				"Broker tail locator did not match the adopted external session.",
			);
		}
		const entries = transcriptEntries(tail);
		const transcript = fingerprintTranscriptEntries(entries.map((entry) => entry.payload));
		const fresh = identityFromRow(tail.session, transcript);
		this.#identity = fresh;
		return {
			identity: fresh,
			transcriptEntries: entries,
			events: eventItems(tail),
			terminal: tail.terminal === true,
			complete: true,
			retentionGap: tail.gap !== undefined,
			...(tail.checkpoint === undefined ? {} : { checkpoint: tail.checkpoint }),
			...(tail.gap?.resync === undefined ? {} : { resyncCheckpoint: tail.gap.resync }),
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

	private async verifyRow(
		expectedSessionId: string,
		expected: ExternalSessionIdentity | undefined,
	): Promise<SupervisorVerification> {
		let row: SdkSessionRowV1;
		try {
			row = await this.#broker.inspectSession(expectedSessionId, { timeoutMs: this.#commandTimeoutMs });
		} catch (error) {
			throw this.wrapBrokerError("session_unavailable", error);
		}
		const reason = unavailableReason(row);
		if (reason)
			throw new HostSupervisorError(reason, `The requested external session ${expectedSessionId} is not safely live.`);
		if (!samePathIdentity(row.locator.repo, this.#workspace)) {
			throw new HostSupervisorError(
				"session_workspace_mismatch",
				"Broker session locator does not match the profile workspace.",
			);
		}
		if (
			expected &&
			(!samePathIdentity(row.locator.repo, expected.locator.repo) ||
				!samePathIdentity(row.locator.stateRoot, expected.locator.stateRoot))
		) {
			throw new HostSupervisorError(
				"session_locator_mismatch",
				"Broker session locator does not match the durable adopted identity.",
			);
		}
		try {
			const metadata = await this.#broker.sessionMetadata(expectedSessionId, { timeoutMs: this.#commandTimeoutMs });
			if (!samePathIdentity(metadata.cwd, this.#workspace)) {
				throw new HostSupervisorError(
					"session_workspace_mismatch",
					"Session metadata cwd does not match the profile workspace.",
				);
			}
			if (metadata.kind !== "main") {
				throw new HostSupervisorError("session_kind_mismatch", "Only an operator-run main GJC session can be adopted.");
			}
		} catch (error) {
			if (error instanceof HostSupervisorError) throw error;
			throw this.wrapBrokerError("session_metadata_unavailable", error);
		}
		try {
			// This is a liveness/existence query only. Its checkpoint coordinates are
			// not part of the broker event-ring coordinate system.
			await this.#broker.sessionCheckpoint(expectedSessionId, { timeoutMs: this.#commandTimeoutMs });
		} catch (error) {
			throw this.wrapBrokerError("session_checkpoint_unavailable", error);
		}
		const provisional = identityFromRow(row, undefined);
		this.#identity = provisional;
		const { turnState, followUpQueueDepth } = await this.turnState();
		// `tail --until-idle` has no immediate-snapshot mode: it only envelopes
		// when a terminal boundary occurs inside its wait window. Take one bounded
		// proof attempt, then explicitly carry a pending proof rather than treating
		// a long-lived interactive prompt as an unavailable session.
		const tail = await this.tailEvents({ timeoutMs: this.#adoptionTailTimeoutMs });
		if (!tail.complete) {
			return {
				identity: provisional,
				transcriptEntries: [],
				transcriptProof: "pending",
				turnState,
				followUpQueueDepth,
			};
		}
		// The envelope checkpoint is the ring's high-water boundary. On a gap it
		// deliberately wins over `resync`: we must not replay retained pre-adoption
		// events from the first envelope.
		const initialRingCheckpoint = tail.checkpoint ?? tail.resyncCheckpoint;
		if (!initialRingCheckpoint) {
			throw new HostSupervisorError(
				"tail_checkpoint_unavailable",
				"A complete broker tail did not provide an adoption ring checkpoint.",
			);
		}
		return {
			identity: tail.identity,
			transcriptEntries: tail.transcriptEntries,
			initialRingCheckpoint,
			verificationTail: tail,
			transcriptProof: "proven",
			turnState,
			followUpQueueDepth,
		};
	}

	private requireIdentity(): ExternalSessionIdentity {
		this.assertUsable();
		if (!this.#identity)
			throw new HostSupervisorError("session_not_adopted", "No external GJC session has been adopted.");
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
