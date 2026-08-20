import * as crypto from "node:crypto";
import type { OwnerSurface, WayProfile } from "../profile";
import { RpcBridgeException } from "../rpc-bridge";
import { canonicalJson, type GateIdempotencyStore } from "./gates";

export type DeliveredAs = "prompt" | "steer" | "follow_up";

/** Server-derived request after strict JSON-RPC parameter validation. */
export interface AdmissionRequest {
	readonly text: string;
	readonly surfaceId: string;
	readonly idempotencyKey: string;
}

export interface MainAdmissionTarget {
	readonly turnState: "idle" | "busy";
	/** Broker acceptance boundary; this must not await model-turn completion. */
	admit(deliveredAs: DeliveredAs, text: string, opRef: string): Promise<void>;
}

export interface CreateMainAdmissionOptions {
	/** P4 has no registry yet; callers can supply its quarantine decision seam. */
	readonly isSurfaceQuarantined?: (surface: OwnerSurface) => boolean;
	readonly newOpRef?: () => string;
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

async function dispatchAdmittedOperation(
	target: MainAdmissionTarget,
	deliveredAs: DeliveredAs,
	text: string,
	opRef: string,
): Promise<void> {
	await target.admit(deliveredAs, text, opRef);
}

/**
 * Server-authoritative admission. A caller submits only text and a surface;
 * delivery is always derived from the bound profile and current main turn.
 */
export function createMainAdmissionHandler(
	target: MainAdmissionTarget,
	profile: WayProfile,
	idempotency: GateIdempotencyStore,
	options: CreateMainAdmissionOptions = {},
) {
	const ownerSurfaceIds = new Set(profile.ownerSurfaces.map(surface => surface.id));
	const knownSurfaces = new Map(profile.knownSurfaces.map(surface => [surface.id, surface]));
	const newOpRef = options.newOpRef ?? crypto.randomUUID;
	return async (params: unknown): Promise<{ accepted: boolean; op_ref: string; delivered_as: DeliveredAs }> => {
		const request = parseRequest(params);
		const canonicalSurfaceId = request.surfaceId.trim();
		const surface = knownSurfaces.get(canonicalSurfaceId);
		if (!surface) throw new RpcBridgeException(1300, "unknown_surface");
		if (options.isSurfaceQuarantined?.(surface)) throw new RpcBridgeException(1302, "session_quarantined");
		const requestJson = canonicalJson({
			idempotency_key: request.idempotencyKey,
			surface_id: surface.id,
			text: request.text,
		});
		let replay;
		try {
			replay = idempotency.idempotencyReplay({ scope: "main.submit", key: request.idempotencyKey, requestJson });
		} catch (error) {
			idempotencyFailure(error);
		}
		if (replay?.replayed) return replayResponse(replay.responseJson);

		const deliveredAs: DeliveredAs = ownerSurfaceIds.has(surface.id)
			? target.turnState === "idle"
				? "prompt"
				: "steer"
			: "follow_up";
		const response = { accepted: true, op_ref: newOpRef(), delivered_as: deliveredAs } as const;
		await dispatchAdmittedOperation(target, deliveredAs, request.text, response.op_ref);
		try {
			idempotency.idempotencyStore({
				scope: "main.submit",
				key: request.idempotencyKey,
				requestJson,
				responseJson: canonicalJson(response),
			});
		} catch (error) {
			idempotencyFailure(error);
		}
		return response;
	};
}
