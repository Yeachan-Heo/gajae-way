import type { WayProfile } from "../profile";
import {
	attestsExternalTranscriptGrowth,
	fingerprintTranscriptEntries,
	GatewayStateStore,
	sameExternalFingerprint,
	sameExternalSession,
	type ExternalSessionIdentity,
	type GrowthIntent,
	type TranscriptDeliveryProgress,
} from "./state";
import { HostSupervisorError, type HostSupervisor } from "./supervisor";

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
	if (!durableIdentity.transcript) {
		if (!current.transcript || !sameExternalSession(durableIdentity, current)) {
			return failClosed(options.state, "main_identity_mismatch", "The broker did not provide a transcript proof for the committed external session.");
		}
		try {
			const lastEntry = verified.transcriptEntries.at(-1);
			const transcriptDeliveryProgress: TranscriptDeliveryProgress = {
				...(lastEntry === undefined ? {} : { lastEntryId: lastEntry.id }),
				fingerprint: fingerprintTranscriptEntries(verified.transcriptEntries.map(entry => entry.payload)),
			};
			options.state.persistTranscriptProof(durableIdentity, current, transcriptDeliveryProgress);
			durableIdentity = current;
		} catch (error) {
			return failClosed(options.state, "transcript_proof_persist_failed", error instanceof Error ? error.message : String(error), error);
		}
	}
	let recoveredGrowthIntent = false;
	if (durable.growthIntent) {
		if (!sameExternalSession(durable.growthIntent.base, current)) {
			return failClosed(options.state, "growth_intent_mismatch", "The adopted session changed while a growth intent was open.");
		}
		if (!attestsExternalTranscriptGrowth(durable.growthIntent.base, verified.transcriptEntries.map(entry => entry.payload))) {
			return failClosed(options.state, "growth_intent_mismatch", "The broker transcript changed outside append-only growth recovery.");
		}
		recoveredGrowthIntent = true;
	} else if (!sameExternalFingerprint(durableIdentity, current)) {
		return failClosed(options.state, "main_identity_mismatch", "The persisted external session identity no longer matches broker evidence.");
	}
	try {
		options.state.setTunablesRevision(options.profile.tunablesRevision);
	} catch (error) {
		return failClosed(options.state, "profile_tunables_revision_failed", error instanceof Error ? error.message : String(error), error);
	}
	return {
		identity: current,
		recoveredGrowthIntent,
		...(durable.growthIntent === undefined ? {} : { growthIntent: durable.growthIntent }),
		turnState: verified.turnState,
		followUpQueueDepth: verified.followUpQueueDepth,
	};
}
