import * as crypto from "node:crypto";
import { BrokerCliError } from "../broker/cli";
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
	/** Broker acceptance boundary; this must not await model-turn completion. */
	/** The supplied finalizer atomically replaces this request's durable intent after terminal evidence. */
	admit(deliveredAs: DeliveredAs, text: string, opRef: string, finalizePendingClaim?: () => void): Promise<void>;
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
	}[];
}

export interface CreateMainAdmissionOptions {
	/** P4 has no registry yet; callers can supply its quarantine decision seam. */
	readonly isSurfaceQuarantined?: (surface: OwnerSurface) => boolean;
	readonly newOpRef?: () => string;
	/** Test-only interruption seam after broker acceptance and before durable finalization. */
	readonly afterBrokerAcceptedBeforeFinalize?: () => void | Promise<void>;
	/** Central main-session mutation fence, checked before a claim or broker effect. */
	readonly admissionFenceReason?: () => string | undefined;
}

interface MainAdmissionIntent {
	readonly version: 1;
	readonly state: "claimed";
	readonly op_ref: string;
	readonly delivered_as: DeliveredAs;
	readonly request_hash: string;
}

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

function replayResponse(responseJson: string | undefined): { accepted: boolean; op_ref: string; delivered_as: DeliveredAs } {
	if (!responseJson) throw new Error("Idempotency replay has no stored response.");
	const response = JSON.parse(responseJson) as Record<string, unknown>;
	if (
		response.accepted !== true ||
		typeof response.op_ref !== "string" ||
		(response.delivered_as !== "prompt" && response.delivered_as !== "steer" && response.delivered_as !== "follow_up")
	) {
		throw new Error("Stored main.submit response is invalid.");
	}
	return response as { accepted: boolean; op_ref: string; delivered_as: DeliveredAs };
}

function parseIntent(intentJson: string): MainAdmissionIntent {
	let parsed: unknown;
	try {
		parsed = JSON.parse(intentJson) as unknown;
	} catch (error) {
		throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "The durable main admission intent is not JSON.", { cause: error });
	}
	if (
		!isRecord(parsed) ||
		parsed.version !== 1 ||
		parsed.state !== "claimed" ||
		typeof parsed.op_ref !== "string" ||
		!parsed.op_ref ||
		(parsed.delivered_as !== "prompt" && parsed.delivered_as !== "steer" && parsed.delivered_as !== "follow_up") ||
		typeof parsed.request_hash !== "string" ||
		!/^[a-f0-9]{64}$/i.test(parsed.request_hash)
	) {
		throw new MainAdmissionRecoveryError("main_admission_intent_invalid", "The durable main admission intent is malformed.");
	}
	return {
		version: 1,
		state: "claimed",
		op_ref: parsed.op_ref,
		delivered_as: parsed.delivered_as,
		request_hash: parsed.request_hash,
	};
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
): Promise<void> {
	await target.admit(deliveredAs, text, opRef, finalizePendingClaim);
}

function isDefinitiveBrokerRejection(error: unknown): boolean {
	const seen = new Set<unknown>();
	let current: unknown = error;
	while (current instanceof Error && !seen.has(current)) {
		seen.add(current);
		if (current instanceof BrokerCliError && current.definitive) return true;
		current = current.cause;
	}
	return false;
}

function abandonDefinitivelyRejectedClaim(
	idempotency: MainAdmissionOperationStore,
	request: AdmissionRequest,
	requestJson: string,
	intentJson: string,
	error: unknown,
): void {
	if (!isDefinitiveBrokerRejection(error)) return;
	try {
		idempotency.mainAdmissionOperationAbandon({
			scope: MAIN_ADMISSION_SCOPE,
			key: request.idempotencyKey,
			requestJson,
			intentJson,
		});
	} catch (abandonError) {
		throw new MainAdmissionRecoveryError(
			"main_admission_claim_abandon_failed",
			"The broker definitively rejected a main admission, but its durable pre-effect claim could not be abandoned.",
			{ cause: abandonError },
		);
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
	return async (params: unknown): Promise<{ accepted: boolean; op_ref: string; delivered_as: DeliveredAs }> => {
		const request = parseRequest(params);
		const fenceReason = options.admissionFenceReason?.();
		if (fenceReason) throw new RpcBridgeException(1003, fenceReason);
		const canonicalSurfaceId = request.surfaceId.trim();
		const surface = knownSurfaces.get(canonicalSurfaceId);
		if (!surface) throw new RpcBridgeException(1300, "unknown_surface");
		if (options.isSurfaceQuarantined?.(surface)) throw new RpcBridgeException(1302, "session_quarantined");
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
		const response = { accepted: true, op_ref: newOpRef(), delivered_as: deliveredAs } as const;
		const intent: MainAdmissionIntent = {
			version: 1,
			state: "claimed",
			op_ref: response.op_ref,
			delivered_as: deliveredAs,
			request_hash: sha256(requestJson),
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
			await dispatchAdmittedOperation(target, deliveredAs, request.text, response.op_ref, finalizePendingClaim);
		} catch (error) {
			abandonDefinitivelyRejectedClaim(idempotency, request, requestJson, intentJson, error);
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
		let status;
		try {
			status = await supervisor.operationStatus(intent.op_ref);
		} catch (error) {
			throw new MainAdmissionRecoveryError(
				"main_admission_recovery_unavailable",
				"Could not verify the broker operation for a pending main admission.",
				{ cause: error },
			);
		}
		if (status.status === "unknown") {
			throw new MainAdmissionRecoveryError(
				"main_admission_recovery_unprovable",
				"The broker cannot prove whether a pending main admission was accepted.",
			);
		}
		const response = { accepted: true, op_ref: intent.op_ref, delivered_as: intent.delivered_as } as const;
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