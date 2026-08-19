import * as crypto from "node:crypto";
import * as fs from "node:fs";
import type { WayProfile } from "../profile";
import { assembleInjection, type ContextFile } from "./inject";
import { bootstrapNonceMarker, type MainSessionSdk } from "./sdk";

import {
	fingerprintSessionFile,
	GatewayStateError,
	GatewayStateStore,
	sameFingerprint,
	SessionFingerprintError,
	type BootstrapIntent,
	type BootstrapState,
	type SessionFingerprint,
} from "./state";


export type { BootstrapState } from "./state";

export class BootstrapError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason) {
		super(message);
		this.name = "BootstrapError";
		this.reason = reason;
	}
}

/** The ceremony never waits indefinitely for the SDK's deferred first flush. */
export const BOOTSTRAP_TRANSCRIPT_TIMEOUT_MS = 60_000;
const BOOTSTRAP_TRANSCRIPT_POLL_MS = 50;

class BootstrapTranscriptWaitError extends BootstrapError {
	readonly sawTranscript: boolean;

	constructor(reason: "bootstrap_transcript_timeout" | "bootstrap_transcript_unstable", sawTranscript: boolean) {
		super(
			reason,
			sawTranscript
				? "The SDK transcript did not stabilize before the bootstrap timeout."
				: "The SDK did not persist a transcript before the bootstrap timeout.",
		);
		this.name = "BootstrapTranscriptWaitError";
		this.sawTranscript = sawTranscript;
	}
}

function positiveInteger(value: number | undefined, fallback: number, field: string): number {
	const resolved = value ?? fallback;
	if (!Number.isSafeInteger(resolved) || resolved <= 0) {
		throw new BootstrapError("bootstrap_wait_config_invalid", `${field} must be a positive integer number of milliseconds.`);
	}
	return resolved;
}

function isPendingFingerprintError(error: unknown): boolean {
	return (
		error instanceof SessionFingerprintError &&
		(error.reason === "session_missing" ||
			error.reason === "session_open_failed" ||
			error.reason === "session_unstable" ||
			error.reason === "transcript_malformed")
	);
}

/**
 * The published SDK intentionally keeps a new transcript in memory until an
 * assistant message exists. Require two equal stable snapshots after the
 * nonce-bearing first turn before publishing CREATED/COMMITTED.
 */
async function waitForStableBootstrapTranscript(
	sessionPath: string,
	timeoutMs: number,
	pollMs: number,
): Promise<SessionFingerprint> {
	const deadline = Date.now() + timeoutMs;
	let previous: SessionFingerprint | undefined;
	let sawTranscript = false;
	for (;;) {
		if (fs.existsSync(sessionPath)) sawTranscript = true;
		try {
			const current = fingerprintSessionFile(sessionPath);
			sawTranscript = true;
			if (previous && sameFingerprint(previous, current)) return current;
			previous = current;
		} catch (error) {
			if (!isPendingFingerprintError(error)) throw error;
			previous = undefined;
		}
		const remaining = deadline - Date.now();
		if (remaining <= 0) {
			throw new BootstrapTranscriptWaitError(
				sawTranscript ? "bootstrap_transcript_unstable" : "bootstrap_transcript_timeout",
				sawTranscript,
			);
		}
		await Bun.sleep(Math.min(pollMs, remaining));
	}
}

export interface BootstrapHooks {
	readonly afterCreatingIntent?: () => void | Promise<void>;
	/** Runs after SDK session allocation but before the first nonce-bearing turn. */
	readonly afterSdkCreate?: () => void | Promise<void>;
	/** Runs after the first nonce-bearing turn persisted a stable transcript. */
	readonly afterFirstMessage?: () => void | Promise<void>;
	readonly afterCreated?: () => void | Promise<void>;
	readonly beforeCommit?: () => void | Promise<void>;
	readonly afterCommit?: () => void | Promise<void>;
}

export interface BootstrapOptions {
	readonly confirm: boolean;
	readonly profile: WayProfile;
	readonly state: GatewayStateStore;
	readonly sdk: MainSessionSdk;
	readonly now?: () => number;
	readonly nonce?: () => string;
	readonly contextFiles?: readonly ContextFile[];
	/** Testable bounded wait for the SDK's deferred initial transcript flush. */
	readonly transcriptTimeoutMs?: number;
	readonly transcriptPollMs?: number;
	readonly hooks?: BootstrapHooks;
}

export interface BootstrapSuccess {
	readonly kind: "committed";
	readonly identity: SessionFingerprint;
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
	if (!intent) throw new BootstrapError("bootstrap_intent_missing", `${state} bootstrap state has no durable nonce intent.`);
	return intent;
}

function assertBootstrapIdentity(sessionId: string, identity: SessionFingerprint): void {
	if (sessionId !== identity.sessionId) {
		throw new BootstrapError("session_identity_mismatch", "The SDK session id did not match its durable transcript header.");
	}
}

/**
 * Explicit operator-only creation ceremony. The CREATING nonce write occurs
 * before the SDK creates any transcript; the COMMITTED write atomically binds
 * the final file identity and profile digest.
 */
export async function bootstrapMainSession(options: BootstrapOptions): Promise<BootstrapSuccess> {
	if (!options.confirm) throw new BootstrapError("confirmation_required", "gajaeway bootstrap requires --confirm.");
	const current = options.state.read();
	if (current.bootstrapState !== "ABSENT") {
		throw new BootstrapError("bootstrap_state_not_absent", `Cannot bootstrap while state is ${current.bootstrapState}.`);
	}
	const transcriptTimeoutMs = positiveInteger(
		options.transcriptTimeoutMs,
		BOOTSTRAP_TRANSCRIPT_TIMEOUT_MS,
		"transcriptTimeoutMs",
	);
	const transcriptPollMs = positiveInteger(options.transcriptPollMs, BOOTSTRAP_TRANSCRIPT_POLL_MS, "transcriptPollMs");
	const intent: BootstrapIntent = {
		nonce: (options.nonce ?? crypto.randomUUID)(),
		ts: (options.now ?? Date.now)(),
	};
	options.state.markCreating(intent);
	await options.hooks?.afterCreatingIntent?.();

	let session: Awaited<ReturnType<MainSessionSdk["createNew"]>> | undefined;
	try {
		const contextFiles = options.contextFiles ?? assembleInjection(options.profile, { sessionKind: "main" });
		session = await options.sdk.createNew({ workspace: options.profile.workspace, contextFiles });
		await options.hooks?.afterSdkCreate?.();
		// A custom no-turn marker is not sufficient: the SDK flushes a fresh
		// transcript only after an assistant message. This sends the FIRST user
		// message with the nonce, waits for its turn, then observes two stable
		// file snapshots before durable CREATED publication.
		await session.sendBootstrapMessage(intent.nonce);
		const identity = await waitForStableBootstrapTranscript(session.sessionFile, transcriptTimeoutMs, transcriptPollMs);
		await options.hooks?.afterFirstMessage?.();
		assertBootstrapIdentity(session.sessionId, identity);
		options.state.markCreated(intent);
		await options.hooks?.afterCreated?.();
		await options.hooks?.beforeCommit?.();
		options.state.commitBootstrap("CREATED", intent, identity, options.profile);
		await options.hooks?.afterCommit?.();
		return { kind: "committed", identity, nonce: intent.nonce };
	} catch (error) {
		if (error instanceof BootstrapTranscriptWaitError && !error.sawTranscript) {
			// No transcript exists to reconcile. Return to ABSENT so the operator can
			// immediately retry the explicit ceremony instead of leaving CREATING stuck.
			options.state.clearBootstrapIntent("CREATING", intent);
		}
		throw error;
	} finally {
		await session?.dispose();
	}
}

/**
 * Reconciles an interrupted bootstrap without guessing. A nonce match is only
 * trustworthy after the candidate transcript can be fully fingerprinted.
 */
export async function recoverBootstrap(options: Omit<BootstrapOptions, "confirm" | "hooks" | "contextFiles" | "nonce">): Promise<BootstrapRecoveryResult> {
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
		const reason = error instanceof BootstrapError ? error.reason : "bootstrap_intent_missing";
		options.state.markFailedClosed(reason);
		return { kind: "failed_closed", reason };
	}
	const paths = await options.sdk.findBootstrapNonceCandidates(options.profile.workspace, intent.nonce);
	let uniquePaths: string[];
	try {
		uniquePaths = [
			...new Set(
				paths.map(candidate => {
					if (!fs.readFileSync(candidate, "utf8").includes(bootstrapNonceMarker(intent.nonce))) {
						throw new BootstrapError("bootstrap_orphan_invalid", "Bootstrap nonce candidate did not contain its exact marker.");
					}
					return fingerprintSessionFile(candidate).canonicalPath;
				}),
			),
		];
	} catch {
		const reason = "bootstrap_orphan_invalid";
		options.state.markFailedClosed(reason);
		return { kind: "failed_closed", reason };
	}
	if (uniquePaths.length === 0) {
		options.state.clearBootstrapIntent(current.bootstrapState, intent);
		return { kind: "bootstrap_required" };
	}
	if (uniquePaths.length !== 1) {
		const reason = "bootstrap_orphan_ambiguous";
		options.state.markFailedClosed(reason);
		return { kind: "failed_closed", reason };
	}
	try {
		const identity = fingerprintSessionFile(uniquePaths[0] as string);
		options.state.commitBootstrap(current.bootstrapState, intent, identity, options.profile);
		return { kind: "committed", identity, nonce: intent.nonce };
	} catch (error) {
		if (error instanceof GatewayStateError) throw error;
		const reason = error instanceof BootstrapError ? error.reason : "bootstrap_recovery_failed";
		options.state.markFailedClosed(reason);
		return { kind: "failed_closed", reason };
	}
}
