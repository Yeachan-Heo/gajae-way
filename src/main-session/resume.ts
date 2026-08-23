import type { WayProfile } from "../profile";
import {
	attestsExternalTranscriptGrowth,
	fingerprintTranscriptEntries,
	GatewayStateError,
	GatewayStateStore,
	sameExternalFingerprint,
	sameExternalSession,
	type ExternalSessionIdentity,
	type GrowthIntent,
	type TranscriptDeliveryProgress,
} from "./state";
import { HostSupervisorError, type HostSupervisor, type SupervisorTailEvents } from "./supervisor";

export class ResumeError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason, options: { readonly cause?: unknown } = {}) {
		super(message, options.cause === undefined ? undefined : { cause: options.cause });
		this.name = "ResumeError";
		this.reason = reason;
	}
}

export interface ResumeOptions {
	readonly profile: WayProfile;
	readonly state: GatewayStateStore;
	readonly supervisor: HostSupervisor;
}

export interface ResumedMainSession {
	readonly identity: ExternalSessionIdentity;
	/** Per-daemon proof state; durable proof may remain proven while a busy boot waits for a complete tail. */
	readonly verificationState: "pending" | "verified";
	/** The complete verification envelope for an existing durable proof, replayed before any fresh tail request. */
	readonly verificationTail?: SupervisorTailEvents;
	readonly recoveredGrowthIntent: boolean;
	/** Retained until the terminal event and transcript delivery are durable. */
	readonly growthIntent?: GrowthIntent;
	readonly turnState: "idle" | "busy";
	readonly followUpQueueDepth: number;
}

function failClosed(state: GatewayStateStore, reason: string, message?: string, cause?: unknown): never {
	try {
		state.markFailedClosed(reason);
	} catch {
		// Preserve a competing process' more specific durable failure reason.
	}
	throw new ResumeError(reason, message, { cause });
}

/**
 * Re-verifies the exact broker-adopted identity on every daemon start. This
 * never opens a local SDK session and never offers a recent-session fallback.
 */

export async function strictResumeMainSession(options: ResumeOptions): Promise<ResumedMainSession> {
	const durable = options.state.read();
	if (durable.bootstrapState === "FAILED_CLOSED") {
		throw new ResumeError(durable.failedClosedReason ?? "failed_closed");
	}
	if (durable.bootstrapState !== "COMMITTED" || !durable.mainIdentity) {
		return failClosed(options.state, "bootstrap_not_committed", "The daemon cannot resume before an explicit committed bootstrap.");
	}
	if (
		durable.profileDigest !== options.profile.digest.sha256 ||
		durable.profileDigestVersion !== options.profile.digest.version
	) {
		return failClosed(options.state, "profile_drift", "The identity/security profile projection changed without approval.");
	}
	if (options.profile.externalSessionId && options.profile.externalSessionId !== durable.mainIdentity.sessionId) {
		return failClosed(options.state, "profile_session_mismatch", "The profile selected a different external session than the durable adopted identity.");
	}
	let verified;
	try {
		verified = await options.supervisor.verify(durable.mainIdentity);
	} catch (error) {
		const reason = error instanceof HostSupervisorError ? error.reason : "strict_resume_failed";
		return failClosed(options.state, reason, error instanceof Error ? error.message : String(error), error);
	}
	const current = verified.identity;
	let durableIdentity = durable.mainIdentity;
	if (!sameExternalSession(durableIdentity, current)) {
		return failClosed(options.state, "main_identity_mismatch", "The broker returned a different external session identity.");
	}
	let identity = current;
	let verificationState: "pending" | "verified";
	if (durable.transcriptProof === "pending") {
		if (verified.transcriptProof === "proven") {
			if (!current.transcript || !verified.initialRingCheckpoint || verified.transcriptEntries.some(entry => !entry.id.trim())) {
				return failClosed(options.state, "transcript_proof_invalid", "The broker returned an invalid complete transcript proof or ring boundary.");
			}
			const transcriptDeliveryProgress: TranscriptDeliveryProgress = {
				...(verified.transcriptEntries.at(-1) === undefined ? {} : { lastEntryId: verified.transcriptEntries.at(-1)?.id }),
				fingerprint: fingerprintTranscriptEntries(verified.transcriptEntries.map(entry => entry.payload)),
			};
			if (
				transcriptDeliveryProgress.fingerprint.entryCount !== current.transcript.entryCount ||
				transcriptDeliveryProgress.fingerprint.sha256 !== current.transcript.sha256
			) {
				return failClosed(options.state, "transcript_proof_mismatch", "The broker transcript fingerprint did not match its complete snapshot.");
			}
			try {
				options.state.persistTranscriptProof(durableIdentity, current, transcriptDeliveryProgress, verified.initialRingCheckpoint);
				durableIdentity = current;
			} catch (error) {
				return failClosed(options.state, "transcript_proof_persist_failed", error instanceof Error ? error.message : String(error), error);
			}
			verificationState = "verified";
		} else if (
			verified.transcriptProof !== "pending" ||
			current.transcript ||
			verified.initialRingCheckpoint ||
			verified.transcriptEntries.length !== 0
		) {
			return failClosed(options.state, "transcript_proof_invalid", "The broker returned an invalid pending transcript proof.");
		} else {
			verificationState = "pending";
		}
	} else if (verified.transcriptProof === "proven" && current.transcript) {
		verificationState = "verified";
	} else if (
		verified.transcriptProof === "pending" &&
		!current.transcript &&
		!verified.initialRingCheckpoint &&
		verified.transcriptEntries.length === 0
	) {
		// A busy `tail --until-idle` cannot provide a complete transcript snapshot.
		// Keep the durable proof identity as this boot's authority until the host
		// observes a complete tail and re-attests its prefix.
		identity = durableIdentity;
		verificationState = "pending";
	} else {
		return failClosed(options.state, "transcript_proof_invalid", "The broker returned an invalid proof state for the durable transcript identity.");
	}
	let recoveredGrowthIntent = false;
	if (durable.growthIntent) {
		if (!sameExternalSession(durable.growthIntent.base, current)) {
			return failClosed(options.state, "growth_intent_mismatch", "The adopted session changed while a growth intent was open.");
		}
		if (verificationState === "verified" && !attestsExternalTranscriptGrowth(durable.growthIntent.base, verified.transcriptEntries.map(entry => entry.payload))) {
			return failClosed(options.state, "growth_intent_mismatch", "The broker transcript changed outside append-only growth recovery.");
		}
		recoveredGrowthIntent = true;
	} else if (verificationState === "verified" && !sameExternalFingerprint(durableIdentity, current)) {
		// The adopted persona is a live agent: it can legitimately keep working
		// while the daemon is down, so its transcript grows with no growth intent
		// open. Absorb that growth only when it is an attested append-only
		// extension of the persisted prefix on the same session and locator; a
		// rewritten, reordered, truncated, or replaced history still fails closed.
		// Absence of attestation is never permission.
		if (!sameExternalSession(durableIdentity, current)) {
			return failClosed(options.state, "main_identity_mismatch", "The persisted external session identity no longer matches broker evidence.");
		}
		if (!attestsExternalTranscriptGrowth(durableIdentity, verified.transcriptEntries.map(entry => entry.payload))) {
			return failClosed(options.state, "main_identity_mismatch", "The broker transcript diverged from the persisted prefix outside append-only growth.");
		}
		try {
			options.state.absorbAutonomousTranscriptGrowth(durableIdentity, current, verified.transcriptEntries.map(entry => entry.payload));
		} catch (error) {
			const reason = error instanceof GatewayStateError ? error.reason : "main_identity_growth_absorb_failed";
			return failClosed(options.state, reason, error instanceof Error ? error.message : String(error), error);
		}
		durableIdentity = current;
	}
	try {
		options.state.setTunablesRevision(options.profile.tunablesRevision);
	} catch (error) {
		return failClosed(options.state, "profile_tunables_revision_failed", error instanceof Error ? error.message : String(error), error);
	}
	return {
		identity,
		verificationState,
		...(durable.transcriptProof === "proven" && verified.transcriptProof === "proven" && verified.verificationTail
			? { verificationTail: verified.verificationTail }
			: {}),
		recoveredGrowthIntent,
		...(durable.growthIntent === undefined ? {} : { growthIntent: durable.growthIntent }),
		turnState: verified.turnState,
		followUpQueueDepth: verified.followUpQueueDepth,
	};
}
