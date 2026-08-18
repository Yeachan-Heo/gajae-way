import type { WayProfile } from "../profile";
import { assembleInjection, type ContextFile, type InjectionLogEntry } from "./inject";
import type { HostedSdkSession, MainSessionSdk } from "./sdk";
import {
	attestAppendOnlyGrowth,
	fingerprintSessionFile,
	GatewayStateStore,
	sameFingerprint,
	type SessionFingerprint,
} from "./state";

export class ResumeError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason) {
		super(message);
		this.name = "ResumeError";
		this.reason = reason;
	}
}

export interface ResumeOptions {
	readonly profile: WayProfile;
	readonly state: GatewayStateStore;
	readonly sdk: MainSessionSdk;
	readonly contextFiles?: readonly ContextFile[];
	readonly onInjectionLog?: (entry: InjectionLogEntry) => void;
}

export interface ResumedMainSession {
	readonly session: HostedSdkSession;
	readonly identity: SessionFingerprint;
	readonly recoveredGrowthIntent: boolean;
}

function failClosed(state: GatewayStateStore, reason: string, message?: string): never {
	try {
		state.markFailedClosed(reason);
	} catch {
		// Preserve the original protocol failure. A later boot sees either the
		// original state or the concurrent writer's fail-closed decision.
	}
	throw new ResumeError(reason, message);
}

/**
 * Enforces profile binding and transcript authority before constructing the SDK
 * persona. Every successful path calls openExistingStrict on the exact current
 * fingerprint; no "recent session" fallback is available.
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

	let current: SessionFingerprint;
	try {
		current = fingerprintSessionFile(durable.mainIdentity.canonicalPath);
	} catch (error) {
		return failClosed(options.state, "main_identity_unreadable", error instanceof Error ? error.message : String(error));
	}
	let recoveredGrowthIntent = false;
	if (durable.growthIntent) {
		if (!attestAppendOnlyGrowth(durable.growthIntent.base, current)) {
			return failClosed(options.state, "growth_intent_mismatch", "The transcript changed outside append-only growth recovery.");
		}
		try {
			options.state.refreshRecoveredGrowth(durable.growthIntent, current);
			recoveredGrowthIntent = true;
		} catch (error) {
			return failClosed(options.state, "growth_intent_refresh_failed", error instanceof Error ? error.message : String(error));
		}
	} else if (!sameFingerprint(durable.mainIdentity, current)) {
		return failClosed(options.state, "main_identity_mismatch", "The persisted main transcript fingerprint no longer matches.");
	}

	try {
		options.state.setTunablesRevision(options.profile.tunablesRevision);
	} catch (error) {
		return failClosed(options.state, "profile_tunables_revision_failed", error instanceof Error ? error.message : String(error));
	}
	const contextFiles = options.contextFiles ?? assembleInjection(options.profile, {
		sessionKind: "main",
		onLog: options.onInjectionLog,
	});
	let session: HostedSdkSession;
	try {
		session = await options.sdk.openExistingStrict({
		workspace: options.profile.workspace,
		contextFiles,
		identity: current,
		});
	} catch (error) {
		return failClosed(options.state, "strict_resume_failed", error instanceof Error ? error.message : String(error));
	}
	if (session.sessionId !== current.sessionId || session.sessionFile !== current.canonicalPath) {
		try {
			await session.dispose();
		} finally {
			return failClosed(options.state, "strict_resume_identity_mismatch", "The opened SDK session did not retain the persisted identity.");
		}
	}
	return { session, identity: current, recoveredGrowthIntent };
}