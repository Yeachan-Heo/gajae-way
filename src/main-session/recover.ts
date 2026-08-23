import { randomUUID } from "node:crypto";
import type { WayProfile } from "../profile";
import {
	attestsExternalTranscriptGrowth,
	GatewayStateError,
	GatewayStateStore,
	sameExternalSession,
} from "./state";
import { HostSupervisorError, type HostSupervisor } from "./supervisor";

export class RecoverError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason, options?: { cause?: unknown }) {
		super(message, options);
		this.name = "RecoverError";
		this.reason = reason;
	}
}

/**
 * Fail-closed reasons whose safety can be re-proven from live evidence.
 *
 * Everything absent from this table stays terminal. In particular an unprovable
 * admission outcome is never cleared by fiat (clearing it could resend or lose
 * an owner command), rewritten history is never accepted as growth, and corrupt
 * durable metadata is never assumed coherent.
 */
const RECOVERABLE_REASONS = {
	/** The transcript diverged; recoverable only as attested append-only growth. */
	main_identity_mismatch: "attested_append_only_growth",
	/** Transient broker/transport unavailability; recoverable by a fresh verify. */
	session_unavailable: "live_verify",
	turn_state_unavailable: "live_verify",
	tail_resync_unavailable: "live_verify",
	/** Durable metadata write failures; recoverable when state reads back coherent. */
	transcript_proof_persist_failed: "coherent_durable_state",
	tail_ring_rotation_write_failed: "coherent_durable_state",
	transcript_delivery_progress_write_failed: "coherent_durable_state",
} as const;

type RecoveryStrategy = (typeof RECOVERABLE_REASONS)[keyof typeof RECOVERABLE_REASONS];

export interface RecoverOptions {
	readonly profile: WayProfile;
	readonly state: GatewayStateStore;
	readonly supervisor: HostSupervisor;
	readonly confirm: boolean;
	readonly now?: () => number;
}

export interface RecoverResult {
	readonly clearedReason: string;
	readonly receiptId: string;
	readonly evidence: string;
	readonly cursor?: string;
}

/**
 * Explicit operator ceremony that clears a durable fail-closed marker ONLY after
 * re-proving, right now, that its cause is gone. It never resets the journal,
 * tail checkpoint, delivery progress, consumer checkpoints, or admission records.
 */
export async function recoverFailedClosedGateway(options: RecoverOptions): Promise<RecoverResult> {
	if (!options.confirm) {
		throw new RecoverError("recovery_not_confirmed", "Recovery is an explicit ceremony: pass --confirm.");
	}
	const durable = options.state.read();
	if (durable.bootstrapState !== "FAILED_CLOSED") {
		throw new RecoverError("not_failed_closed", `The gateway is ${durable.bootstrapState}, not failed closed; there is nothing to recover.`);
	}
	const reason = durable.failedClosedReason;
	if (!reason) {
		throw new RecoverError("failed_closed_reason_missing", "The failed-closed state records no reason, so nothing can be re-verified.");
	}
	if (reason === "profile_drift") {
		throw new RecoverError(
			"use_profile_approval",
			"Profile drift is cleared by the profile-approval ceremony (`gajaeway profile approve`), not by recovery.",
		);
	}
	const strategy: RecoveryStrategy | undefined = RECOVERABLE_REASONS[reason as keyof typeof RECOVERABLE_REASONS];
	if (!strategy) {
		throw new RecoverError(
			"reason_not_recoverable",
			`Fail-closed reason ${reason} cannot be re-verified and stays terminal; use the explicit re-adoption ceremony (bootstrap --confirm) after investigating.`,
		);
	}
	const identity = durable.mainIdentity;
	if (!identity) {
		throw new RecoverError("bootstrap_not_committed", "A gateway without a durable adopted identity cannot be recovered.");
	}
	if (options.profile.externalSessionId && identity.sessionId !== options.profile.externalSessionId) {
		throw new RecoverError("profile_session_mismatch", "The profile selects a different external session than the durable adoption.");
	}

	let verified: Awaited<ReturnType<HostSupervisor["verify"]>>;
	try {
		verified = await options.supervisor.verify(identity);
	} catch (error) {
		const detail = error instanceof HostSupervisorError ? error.reason : error instanceof Error ? error.message : String(error);
		throw new RecoverError("recovery_verification_unavailable", `The original condition still holds or cannot be re-verified: ${detail}`, { cause: error });
	}
	if (!sameExternalSession(identity, verified.identity)) {
		throw new RecoverError("main_identity_mismatch", "The broker now reports a different external session; recovery is refused.");
	}

	let evidence: string;
	let rebind: { readonly observed: typeof verified.identity; readonly entries: readonly unknown[] } | undefined;
	if (strategy === "attested_append_only_growth") {
		if (verified.transcriptProof !== "proven") {
			throw new RecoverError("recovery_verification_unavailable", "A busy or unproven transcript cannot attest append-only growth yet; retry when the session settles.");
		}
		const entries = verified.transcriptEntries.map(entry => entry.payload);
		if (!attestsExternalTranscriptGrowth(identity, entries)) {
			throw new RecoverError(
				"reason_not_recoverable",
				"The broker transcript is not an append-only extension of the persisted prefix, so this is rewritten history and stays terminal.",
			);
		}
		// The re-bind is handed to the clearing transaction so identity and marker
		// move together; a separate write here could not observe a COMMITTED state.
		rebind = { observed: verified.identity, entries };
		evidence = `attested append-only growth re-bound from ${identity.transcript?.entryCount ?? 0} to ${verified.identity.transcript?.entryCount ?? 0} transcript entries`;
	} else if (strategy === "live_verify") {
		evidence = `live broker verification succeeded for session ${identity.sessionId} with turn state ${verified.turnState}`;
	} else {
		// coherent_durable_state: the durable read above already parsed and
		// validated every metadata key, and a corrupt value would have raised
		// metadata_invalid instead of reaching here. Probe one durable write so a
		// still-broken store refuses recovery rather than reporting success.
		try {
			options.state.setTunablesRevision(options.profile.tunablesRevision);
		} catch (error) {
			const detail = error instanceof GatewayStateError ? error.reason : error instanceof Error ? error.message : String(error);
			throw new RecoverError("recovery_verification_unavailable", `The durable store still refuses writes: ${detail}`, { cause: error });
		}
		evidence = `durable metadata read back coherent and accepted a probe write after ${reason}`;
	}

	const receiptId = randomUUID();
	const recoveredAt = (options.now ?? Date.now)();
	try {
		const cursor = options.state.recoverFailedClosed(reason, receiptId, recoveredAt, evidence, rebind);
		return { clearedReason: reason, receiptId, evidence, ...(cursor === undefined ? {} : { cursor }) };
	} catch (error) {
		const detail = error instanceof GatewayStateError ? error.reason : error instanceof Error ? error.message : String(error);
		throw new RecoverError("recovery_write_failed", `Clearing the fail-closed marker failed: ${detail}`, { cause: error });
	}
}
