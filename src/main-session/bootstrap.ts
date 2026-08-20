import * as crypto from "node:crypto";
import type { WayProfile } from "../profile";
import {
	fingerprintTranscriptEntries,
	type BootstrapIntent,
	type BootstrapState,
	type ExternalSessionIdentity,
	type GatewayStateStore,
	type TranscriptDeliveryProgress,
	type TranscriptProof,
} from "./state";
import { HostSupervisorError, type HostSupervisor, type SupervisorVerification } from "./supervisor";

export type { BootstrapState } from "./state";

export class BootstrapError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason, options: { readonly cause?: unknown } = {}) {
		super(message, options.cause === undefined ? undefined : { cause: options.cause });
		this.name = "BootstrapError";
		this.reason = reason;
	}
}

export interface BootstrapHooks {
	readonly afterCreatingIntent?: () => void | Promise<void>;
	readonly afterDiscovery?: () => void | Promise<void>;
	readonly afterCreated?: () => void | Promise<void>;
	readonly beforeCommit?: () => void | Promise<void>;
	readonly afterCommit?: () => void | Promise<void>;
}

export interface BootstrapOptions {
	readonly confirm: boolean;
	readonly profile: WayProfile;
	readonly state: GatewayStateStore;
	readonly supervisor: HostSupervisor;
	/** CLI selection wins only when it exactly matches an identity-bound profile selection. */
	readonly sessionId: string;
	readonly now?: () => number;
	readonly nonce?: () => string;
	readonly hooks?: BootstrapHooks;
}

export interface BootstrapSuccess {
	readonly kind: "committed";
	readonly identity: ExternalSessionIdentity;
	readonly nonce: string;
}

export interface BootstrapRecoveryRequired {
	readonly kind: "bootstrap_required";
}

export interface BootstrapRecoveryFailed {
	readonly kind: "failed_closed";
	readonly reason: string;
}

export type BootstrapRecoveryResult = BootstrapSuccess | BootstrapRecoveryRequired | BootstrapRecoveryFailed;

function requireIntent(state: BootstrapState, intent: BootstrapIntent | undefined): BootstrapIntent {
	if (!intent) throw new BootstrapError("bootstrap_intent_missing", `${state} bootstrap state has no durable external-session intent.`);
	return intent;
}

function failClosed(state: GatewayStateStore, reason: string): BootstrapRecoveryFailed {
	try {
		state.markFailedClosed(reason);
	} catch {
		// A competing process may already have recorded a more specific failure.
	}
	return { kind: "failed_closed", reason };
}

function checkedVerification(
	sessionId: string,
	verified: SupervisorVerification,
): {
	readonly identity: ExternalSessionIdentity;
	readonly ringCheckpoint?: NonNullable<SupervisorVerification["initialRingCheckpoint"]>;
	readonly transcriptProof: TranscriptProof;
	readonly delivery?: TranscriptDeliveryProgress;
} {
	const identity = verified.identity;
	if (identity.sessionId !== sessionId) {
		throw new BootstrapError("session_identity_mismatch", "The broker did not return the requested external session id.");
	}
	if (verified.transcriptProof === "pending") {
		if (identity.transcript || verified.transcriptEntries.length !== 0 || verified.initialRingCheckpoint) {
			throw new BootstrapError("transcript_proof_invalid", "A pending transcript proof carried fingerprint or ring-boundary evidence.");
		}
		return { identity, transcriptProof: "pending" };
	}
	if (verified.transcriptProof !== "proven" || !identity.transcript || !verified.initialRingCheckpoint) {
		throw new BootstrapError("transcript_proof_invalid", "A proven transcript proof requires a complete transcript and its ring boundary.");
	}
	const entries = verified.transcriptEntries;
	if (entries.some(entry => !entry.id.trim())) {
		throw new BootstrapError("transcript_entry_id_missing", "The broker transcript proof contains an entry without a stable id.");
	}
	const observed = fingerprintTranscriptEntries(entries.map(entry => entry.payload));
	if (observed.entryCount !== identity.transcript.entryCount || observed.sha256 !== identity.transcript.sha256) {
		throw new BootstrapError("transcript_proof_mismatch", "The broker transcript fingerprint did not match its complete transcript snapshot.");
	}
	return {
		identity,
		ringCheckpoint: verified.initialRingCheckpoint,
		transcriptProof: "proven",
		delivery: {
			...(entries.at(-1)?.id === undefined ? {} : { lastEntryId: entries.at(-1)?.id }),
			fingerprint: observed,
		},
	};
}

/**
 * Explicit operator-only adoption ceremony. It never creates a session or sends
 * a bootstrap prompt: the selected interactive GJC session must already be
 * alive and broker-verifiable before its identity is committed.
 */
export async function bootstrapMainSession(options: BootstrapOptions): Promise<BootstrapSuccess> {
	if (!options.confirm) throw new BootstrapError("confirmation_required", "gajaeway bootstrap requires --confirm.");
	if (!options.sessionId.trim()) throw new BootstrapError("session_id_required", "gajaeway bootstrap requires an external session id.");
	if (options.profile.externalSessionId && options.profile.externalSessionId !== options.sessionId) {
		throw new BootstrapError("session_id_conflict", "--session-id must match the profile main_session.session_id.");
	}
	const current = options.state.read();
	if (current.bootstrapState !== "ABSENT") {
		throw new BootstrapError("bootstrap_state_not_absent", `Cannot bootstrap while state is ${current.bootstrapState}.`);
	}
	const intent: BootstrapIntent = {
		nonce: (options.nonce ?? crypto.randomUUID)(),
		ts: (options.now ?? Date.now)(),
		sessionId: options.sessionId,
	};
	options.state.markCreating(intent);
	await options.hooks?.afterCreatingIntent?.();
	try {
		const verified = await options.supervisor.discover(intent.sessionId);
		const proof = checkedVerification(intent.sessionId, verified);
		await options.hooks?.afterDiscovery?.();
		options.state.markCreated(intent);
		await options.hooks?.afterCreated?.();
		await options.hooks?.beforeCommit?.();
		options.state.commitBootstrap("CREATED", intent, proof.identity, options.profile, proof.ringCheckpoint, proof.transcriptProof, proof.delivery);
		await options.hooks?.afterCommit?.();
		return { kind: "committed", identity: proof.identity, nonce: intent.nonce };
	} catch (error) {
		if (error instanceof BootstrapError) throw error;
		if (error instanceof HostSupervisorError) throw new BootstrapError(error.reason, error.message, { cause: error });
		throw new BootstrapError("external_session_unavailable", error instanceof Error ? error.message : String(error), { cause: error });
	}
}

/**
 * Replays only the exact session id written in the durable bootstrap intent.
 * There is no transcript scan, recent-session lookup, resume, rebirth, or
 * fallback adoption path.
 */
export async function recoverBootstrap(
	options: Omit<BootstrapOptions, "confirm" | "hooks" | "nonce" | "sessionId">,
): Promise<BootstrapRecoveryResult> {
	const current = options.state.read();
	if (current.bootstrapState === "COMMITTED") {
		if (!current.mainIdentity) throw new BootstrapError("main_identity_missing", "COMMITTED state has no main identity.");
		return { kind: "committed", identity: current.mainIdentity, nonce: "" };
	}
	if (current.bootstrapState === "ABSENT") return { kind: "bootstrap_required" };
	if (current.bootstrapState === "FAILED_CLOSED") {
		return { kind: "failed_closed", reason: current.failedClosedReason ?? "failed_closed" };
	}
	let intent: BootstrapIntent;
	try {
		intent = requireIntent(current.bootstrapState, current.bootstrapIntent);
	} catch (error) {
		return failClosed(options.state, error instanceof BootstrapError ? error.reason : "bootstrap_intent_missing");
	}
	try {
		const verified = await options.supervisor.discover(intent.sessionId);
		const proof = checkedVerification(intent.sessionId, verified);
		if (current.bootstrapState === "CREATING") options.state.markCreated(intent);
		options.state.commitBootstrap("CREATED", intent, proof.identity, options.profile, proof.ringCheckpoint, proof.transcriptProof, proof.delivery);
		return { kind: "committed", identity: proof.identity, nonce: intent.nonce };
	} catch (error) {
		return failClosed(options.state, error instanceof BootstrapError || error instanceof HostSupervisorError ? error.reason : "bootstrap_adoption_unavailable");
	}
}
