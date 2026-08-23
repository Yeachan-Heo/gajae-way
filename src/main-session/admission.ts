import * as crypto from "node:crypto";
import { matchesCanonicalAdmissionAttemptId, MainSessionHostError } from "./host";
import type { OwnerSurface, WayProfile } from "../profile";
import { RpcBridgeException } from "../rpc-bridge";
import { canonicalJson } from "./gates";
import type { HostSupervisor } from "./supervisor";

export type DeliveredAs = "prompt" | "steer" | "follow_up";
const MAIN_ADMISSION_SCOPE = "main.submit";

/** Server-derived request after strict JSON-RPC parameter validation. */
export interface AdmissionRequest {
	readonly text: string;
	readonly surfaceId: string;
	readonly idempotencyKey: string;
}

export interface MainAdmissionTarget {
	readonly turnState: "idle" | "busy";
	/** Host-provided readiness is consulted even when a caller did not install an explicit wrapper. */
	readonly mutationReadinessReason?: string;
	/** Broker acceptance boundary; this must not await model-turn completion. */
	/** The callbacks finalize the claim and persist receipt-to-tail identifiers after acceptance. */
	admit(
		deliveredAs: DeliveredAs,
		text: string,
		opRef: string,
		finalizePendingClaim?: () => void,
		recordAttemptIds?: (attemptIds: readonly string[]) => void,
		/** Canonical admitted surface persisted with receipt aliases for journal egress. */
		surfaceId?: string,
	): Promise<void>;
	/** Production hosts publish a terminal fence if a definitive rejection's claim cannot be abandoned. */
	reportAdmissionClaimAbandonFailure?(error: unknown): void;
}

export interface MainAdmissionOperationStore {
	mainAdmissionOperationClaim(input: {
		readonly scope: string;
		readonly key: string;
		readonly requestJson: string;
		readonly intentJson: string;
	}): { readonly claimed: boolean; readonly responseJson?: string };
	mainAdmissionOperationFinalize(input: {
		readonly scope: string;
		readonly key: string;
		readonly requestJson: string;
		readonly intentJson: string;
		readonly responseJson: string;
	}): { readonly responseJson: string };
	mainAdmissionOperationRecordAttemptIds(input: {
		readonly scope: string;
		readonly key: string;
		readonly requestJson: string;
		readonly intentJson: string;
		readonly attemptIdsJson: string;
	}): void;
	mainAdmissionOperationAbandon(input: {
		readonly scope: string;
		readonly key: string;
		readonly requestJson: string;
		readonly intentJson: string;
	}): void;
	mainAdmissionOperationsPending(): readonly {
		readonly scope: string;
		readonly key: string;
		readonly requestJson: string;
		readonly intentJson: string;
		readonly attemptIdsJson?: string;
	}[];
	/** Returns the inclusive durable journal head before any broker effect. */
	journalHeadCursor?(): string;
	/** Reads durable journal evidence after an admission's pre-effect boundary. */
	journalRead?(cursor: string | undefined, limit: number): {
		readonly events: readonly { readonly seq: string; readonly kind: string; readonly payloadJson: string }[];
		readonly nextCursor: string;
		readonly gap?: unknown;
	};

}

export interface CreateMainAdmissionOptions {
	/** P4 has no registry yet; callers can supply its quarantine decision seam. */
	readonly isSurfaceQuarantined?: (surface: OwnerSurface) => boolean;
	readonly newOpRef?: () => string;
	/** Test-only interruption seam after broker acceptance and before durable finalization. */
	readonly afterBrokerAcceptedBeforeFinalize?: () => void | Promise<void>;
	/** The sole main-session mutation fence, checked immediately before a claim or broker effect. */
	readonly mutationReadinessReason?: () => string | undefined;
	/** Captures the inclusive durable journal head before any broker effect. */
	readonly journalHeadCursor?: () => string;

}

interface MainAdmissionResponse {
	readonly accepted: true;
	readonly op_ref: string;
	readonly delivered_as: DeliveredAs;
	readonly journal_head_cursor?: string;
}

interface MainAdmissionIntent {
	readonly version: 1;
	readonly state: "claimed";
	readonly op_ref: string;
	readonly delivered_as: DeliveredAs;
	readonly request_hash: string;
	readonly journal_head_cursor?: string;
}

const JOURNAL_RECOVERY_PAGE_SIZE = 500;

export class MainAdmissionRecoveryError extends Error {
	readonly reason: string;

	constructor(reason: string, message = reason, options: { readonly cause?: unknown } = {}) {
		super(message, options.cause === undefined ? undefined : { cause: options.cause });
		this.name = "MainAdmissionRecoveryError";
		this.reason = reason;
	}
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredString(value: unknown, field: string): string {
	if (typeof value !== "string" || !value.trim()) throw new RpcBridgeException(-32602, `${field} must be a non-empty string.`);
	return value;
}

function parseRequest(params: unknown): AdmissionRequest {
	if (!isRecord(params)) throw new RpcBridgeException(-32602, "params must be an object.");
	const allowed = new Set(["text", "surface_id", "idempotency_key"]);
	for (const key of Object.keys(params)) {
		if (!allowed.has(key)) throw new RpcBridgeException(-32602, `unknown parameter: ${key}`);
	}
	return {
		text: requiredString(params.text, "text"),
		surfaceId: requiredString(params.surface_id, "surface_id"),
		idempotencyKey: requiredString(params.idempotency_key, "idempotency_key"),
	};
}

interface ResolvedAdmissionSurface {
	readonly surface: OwnerSurface;
	/** Registry quarantine applies to the configured parent, never an invented thread row. */
	readonly quarantineSurface: OwnerSurface;
}

/**
 * Admits only a numeric Discord thread suffix below one configured Discord
 * channel surface. This preserves 1300 for every other unknown surface while
 * avoiding dynamic registry mutation for ephemeral Discord thread ids.
 */
function resolveAdmissionSurface(surfaceId: string, knownSurfaces: ReadonlyMap<string, OwnerSurface>): ResolvedAdmissionSurface | undefined {
	const exact = knownSurfaces.get(surfaceId);
	if (exact) return { surface: exact, quarantineSurface: exact };
	const parents = [...knownSurfaces.values()].filter(parent => {
		if (parent.platform !== "discord" || parent.kind !== "channel") return false;
		const prefix = `${parent.id}/thread:`;
		const threadId = surfaceId.slice(prefix.length);
		return surfaceId.startsWith(prefix) && /^\d+$/.test(threadId);
	});
	if (parents.length !== 1) return undefined;
	const parent = parents[0] as OwnerSurface;
	return {
		surface: { id: surfaceId, platform: parent.platform, kind: "thread" },
		quarantineSurface: parent,
	};
}


function idempotencyFailure(error: unknown): never {
	if (error instanceof RpcBridgeException) throw error;
	const message = error instanceof Error ? error.message : String(error);
	if (message.includes("1500 ") || message.includes("idempotency conflict")) {
		throw new RpcBridgeException(1500, "idempotency_conflict");
	}
	throw error;
}

function sha256(value: string): string {
	return crypto.createHash("sha256").update(value).digest("hex");
}

function replayResponse(responseJson: string | undefined): MainAdmissionResponse {
	if (!responseJson) throw new Error("Idempotency replay has no stored response.");
	const response = JSON.parse(responseJson) as Record<string, unknown>;
	if (
		response.accepted !== true ||
		typeof response.op_ref !== "string" ||
		(response.delivered_as !== "prompt" && response.delivered_as !== "steer" && response.delivered_as !== "follow_up")
	) {
		throw new Error("Stored main.submit response is invalid.");
	}
	const journalHeadCursor = optionalJournalHeadCursor(response.journal_head_cursor);
	return {
		accepted: true,
		op_ref: response.op_ref,
		delivered_as: response.delivered_as,
		...(journalHeadCursor === undefined ? {} : { journal_head_cursor: journalHeadCursor }),
	};
}

function optionalJournalHeadCursor(value: unknown): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !/^\d+:\d+$/.test(value)) throw new Error("journal_head_cursor must be a journal cursor.");
	return value;
}

function parseIntent(intentJson: string): MainAdmissionIntent {
	let parsed: unknown;
	try {
		parsed = JSON.parse(intentJson) as unknown;
	} catch (error) {
		throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "The durable main admission intent is not JSON.", { cause: error });
	}
	if (!isRecord(parsed)) {
		throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "The durable main admission intent is malformed.");
	}
	const journalHeadCursor = parsed.journal_head_cursor;
	if (
		parsed.version !== 1 ||
		parsed.state !== "claimed" ||
		typeof parsed.op_ref !== "string" ||
		!parsed.op_ref ||
		(parsed.delivered_as !== "prompt" && parsed.delivered_as !== "steer" && parsed.delivered_as !== "follow_up") ||
		typeof parsed.request_hash !== "string" ||
		!/^[a-f0-9]{64}$/i.test(parsed.request_hash) ||
		(journalHeadCursor !== undefined && (typeof journalHeadCursor !== "string" || !/^\d+:\d+$/.test(journalHeadCursor)))
	) {
		throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "The durable main admission intent is malformed.");
	}
	return {
		version: 1,
		state: "claimed",
		op_ref: parsed.op_ref,
		delivered_as: parsed.delivered_as,
		request_hash: parsed.request_hash,
		...(journalHeadCursor === undefined ? {} : { journal_head_cursor: journalHeadCursor }),
	};
}

function recoveryResponse(intent: MainAdmissionIntent): MainAdmissionResponse {
	return {
		accepted: true,
		op_ref: intent.op_ref,
		delivered_as: intent.delivered_as,
		...(intent.journal_head_cursor === undefined ? {} : { journal_head_cursor: intent.journal_head_cursor }),
	};
}

function parseJournalPayload(payloadJson: string): Record<string, unknown> | undefined {
	try {
		const parsed = JSON.parse(payloadJson) as unknown;
		return isRecord(parsed) ? parsed : undefined;
	} catch {
		return undefined;
	}
}

/**
 * `attempt_ids_json` is absent both for rows migrated from schema v7 and for
 * a current-schema claim that crashed before its broker receipt was persisted.
 * Those states have indistinguishable provenance, so absence is never treated
 * as legacy alias evidence: broker status is the only settlement authority.
 */
function persistedAttemptIds(value: unknown): readonly string[] | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string") {
		throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "The durable main admission attempt identifiers are malformed.");
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(value) as unknown;
	} catch (error) {
		throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "The durable main admission attempt identifiers are not JSON.", { cause: error });
	}
	if (
		!Array.isArray(parsed) ||
		parsed.length === 0 ||
		parsed.some(attemptId => typeof attemptId !== "string" || !attemptId) ||
		new Set(parsed).size !== parsed.length
	) {
		throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "The durable main admission attempt identifiers are malformed.");
	}
	return parsed;
}

function terminalAttemptId(payloadJson: string): string | undefined {
	const payload = parseJournalPayload(payloadJson);
	if (
		!payload ||
		payload.lineage !== "main" ||
		typeof payload.attempt_id !== "string" ||
		!payload.attempt_id ||
		typeof payload.generation !== "number" ||
		!Number.isSafeInteger(payload.generation)
	)
		return undefined;
	return payload.attempt_id;
}


async function hasDurableTerminalEvidence(
	idempotency: MainAdmissionOperationStore,
	intent: MainAdmissionIntent,
	canonicalAttemptIds: readonly string[],
): Promise<boolean> {
	if (!idempotency.journalRead) return false;
	const terminalAttempts: string[] = [];
	let cursor = intent.journal_head_cursor;
	for (;;) {
		let page;
		try {
			page = idempotency.journalRead(cursor, JOURNAL_RECOVERY_PAGE_SIZE);
		} catch (error) {
			throw new MainAdmissionRecoveryError(
				"main_admission_recovery_unavailable",
				"Could not read durable journal evidence for a pending main admission.",
				{ cause: error },
			);
		}
		if (page.gap !== undefined) return false;
		if (!optionalJournalHeadCursor(page.nextCursor)) {
			throw new MainAdmissionRecoveryError("main_admission_recovery_unavailable", "The durable journal returned an invalid recovery cursor.");
		}
		for (const event of page.events) {
			if (event.kind !== "turn_end") continue;
			const attemptId = terminalAttemptId(event.payloadJson);
			if (attemptId) terminalAttempts.push(attemptId);
		}
		if (terminalAttempts.some(attemptId => matchesCanonicalAdmissionAttemptId(attemptId, canonicalAttemptIds))) return true;
		if (page.events.length === 0) return false;
		if (page.nextCursor === cursor) {
			throw new MainAdmissionRecoveryError("main_admission_recovery_unavailable", "The durable journal recovery cursor did not advance.");
		}
		cursor = page.nextCursor;
	}
}


function pendingReplayError(intentJson: string): never {
	parseIntent(intentJson);
	throw new RpcBridgeException(1501, "admission_recovery_pending");
}

async function dispatchAdmittedOperation(
	target: MainAdmissionTarget,
	deliveredAs: DeliveredAs,
	text: string,
	opRef: string,
	finalizePendingClaim: () => void,
	recordAttemptIds: (attemptIds: readonly string[]) => void,
	surfaceId: string,
): Promise<void> {
	await target.admit(deliveredAs, text, opRef, finalizePendingClaim, recordAttemptIds, surfaceId);
}

function abandonDefinitivelyRejectedClaim(
	target: MainAdmissionTarget,
	idempotency: MainAdmissionOperationStore,
	request: AdmissionRequest,
	requestJson: string,
	intentJson: string,
	error: unknown,
): void {
	if (!(error instanceof MainSessionHostError) || error.admissionDisposition !== "definitive_rejection") return;
	try {
		idempotency.mainAdmissionOperationAbandon({
			scope: MAIN_ADMISSION_SCOPE,
			key: request.idempotencyKey,
			requestJson,
			intentJson,
		});
	} catch (abandonError) {
		const recoveryError = new MainAdmissionRecoveryError(
			"main_admission_claim_abandon_failed",
			"The broker definitively rejected a main admission, but its durable pre-effect claim could not be abandoned.",
			{ cause: abandonError },
		);
		target.reportAdmissionClaimAbandonFailure?.(recoveryError);
		throw recoveryError;
	}
}

/**
 * Server-authoritative admission. A caller submits only text and a surface;
 * delivery is always derived from the bound profile and current main turn.
 */
export function createMainAdmissionHandler(
	target: MainAdmissionTarget,
	profile: WayProfile,
	idempotency: MainAdmissionOperationStore,
	options: CreateMainAdmissionOptions = {},
) {
	const ownerSurfaceIds = new Set(profile.ownerSurfaces.map(surface => surface.id));
	const knownSurfaces = new Map(profile.knownSurfaces.map(surface => [surface.id, surface]));
	const newOpRef = options.newOpRef ?? crypto.randomUUID;
	return async (params: unknown): Promise<MainAdmissionResponse> => {

		const request = parseRequest(params);
		const fenceReason = options.mutationReadinessReason?.() ?? target.mutationReadinessReason;
		if (fenceReason) throw new RpcBridgeException(1003, fenceReason);
		const canonicalSurfaceId = request.surfaceId.trim();
		const resolvedSurface = resolveAdmissionSurface(canonicalSurfaceId, knownSurfaces);
		if (!resolvedSurface) throw new RpcBridgeException(1300, "unknown_surface");
		const surface = resolvedSurface.surface;
		if (options.isSurfaceQuarantined?.(resolvedSurface.quarantineSurface)) throw new RpcBridgeException(1302, "session_quarantined");
		const requestJson = canonicalJson({
			idempotency_key: request.idempotencyKey,
			surface_id: surface.id,
			text: request.text,
		});
		const deliveredAs: DeliveredAs = ownerSurfaceIds.has(surface.id)
			? target.turnState === "idle"
				? "prompt"
				: "steer"
			: "follow_up";
		// This direct durable read happens synchronously before the broker effect,
		// so any assistant event for this admission must be strictly later.
		const journalHeadCursor = options.journalHeadCursor
			? optionalJournalHeadCursor(options.journalHeadCursor())
			: idempotency.journalHeadCursor
				? optionalJournalHeadCursor(idempotency.journalHeadCursor())
				: undefined;
		const response: MainAdmissionResponse = {
			accepted: true,
			op_ref: newOpRef(),
			delivered_as: deliveredAs,
			...(journalHeadCursor === undefined ? {} : { journal_head_cursor: journalHeadCursor }),
		};
		const intent: MainAdmissionIntent = {
			version: 1,
			state: "claimed",
			op_ref: response.op_ref,
			delivered_as: deliveredAs,
			request_hash: sha256(requestJson),
			...(journalHeadCursor === undefined ? {} : { journal_head_cursor: journalHeadCursor }),
		};

		const intentJson = canonicalJson(intent);
		const responseJson = canonicalJson(response);
		// The host retains this exact native finalization transaction if broker
		// acceptance loses its receipt, then invokes it only after terminal tail evidence.
		const finalizePendingClaim = (): void => {
			const finalized = idempotency.mainAdmissionOperationFinalize({
				scope: MAIN_ADMISSION_SCOPE,
				key: request.idempotencyKey,
				requestJson,
				intentJson,
				responseJson,
			});
			replayResponse(finalized.responseJson);
		};
		const recordAttemptIds = (attemptIds: readonly string[]): void => {
			idempotency.mainAdmissionOperationRecordAttemptIds({
				scope: MAIN_ADMISSION_SCOPE,
				key: request.idempotencyKey,
				requestJson,
				intentJson,
				attemptIdsJson: canonicalJson([...attemptIds]),
			});
		};
		let claim;
		try {
			claim = idempotency.mainAdmissionOperationClaim({
				scope: MAIN_ADMISSION_SCOPE,
				key: request.idempotencyKey,
				requestJson,
				intentJson,
			});
		} catch (error) {
			idempotencyFailure(error);
		}
		if (!claim.claimed) {
			try {
				return replayResponse(claim.responseJson);
			} catch {
				return pendingReplayError(claim.responseJson ?? "");
			}
		}
		try {
			await dispatchAdmittedOperation(target, deliveredAs, request.text, response.op_ref, finalizePendingClaim, recordAttemptIds, surface.id);
		} catch (error) {
			abandonDefinitivelyRejectedClaim(target, idempotency, request, requestJson, intentJson, error);
			throw error;
		}
		await options.afterBrokerAcceptedBeforeFinalize?.();
		try {
			finalizePendingClaim();
			return response;
		} catch (error) {
			idempotencyFailure(error);
		}
	};
}

/** Reconciles durable pre-effect claims after restart without ever re-sending their broker operation. */
export async function reconcilePendingMainAdmissions(
	idempotency: MainAdmissionOperationStore,
	supervisor: HostSupervisor,
): Promise<void> {
	for (const pending of idempotency.mainAdmissionOperationsPending()) {
		if (pending.scope !== MAIN_ADMISSION_SCOPE) {
			throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "A pending admission has an unexpected idempotency scope.");
		}
		const intent = parseIntent(pending.intentJson);
		if (intent.request_hash !== sha256(pending.requestJson)) {
			throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "A pending admission request hash does not match its durable request.");
		}
		const attemptIds = persistedAttemptIds(pending.attemptIdsJson);
		let brokerProven = false;
		let brokerStatusError: unknown;
		try {
			brokerProven = (await supervisor.operationStatus(intent.op_ref)).status !== "unknown";
		} catch (error) {
			brokerStatusError = error;
		}
		if (!brokerProven && !(attemptIds !== undefined && (await hasDurableTerminalEvidence(idempotency, intent, attemptIds)))) {
			if (brokerStatusError !== undefined) {
				throw new MainAdmissionRecoveryError(
					"main_admission_recovery_unavailable",
					"Could not verify the broker operation for a pending main admission.",
					{ cause: brokerStatusError },
				);
			}
			throw new MainAdmissionRecoveryError(
				"main_admission_recovery_unprovable",
				"Neither the broker nor the durable journal can prove whether a pending main admission was accepted.",
			);
		}
		const response = recoveryResponse(intent);
		try {
			idempotency.mainAdmissionOperationFinalize({
				scope: pending.scope,
				key: pending.key,
				requestJson: pending.requestJson,
				intentJson: pending.intentJson,
				responseJson: canonicalJson(response),
			});
		} catch (error) {
			idempotencyFailure(error);
		}
	}
}