import type { JsonRpcClient } from "../../rpc-client";
import { rpcResult } from "../../rpc-client";
import { ADAPTER_PROTOCOL_VERSION, AdapterProtocolUnsupportedError, negotiateProtocolVersion } from "./protocol";

export class AdapterSessionError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AdapterSessionError";
	}
}

export interface SurfaceAssertion {
	readonly surfaceId: string;
	readonly quarantined: boolean;
	readonly sessionId?: string;
}

export function gatewayIsRunning(value: unknown): value is Record<string, unknown> {
	return isRecord(value) && value.status === "healthy" && value.state === "running";
}

export function gatewayHealthSummary(value: unknown): string {
	if (!isRecord(value)) return "invalid way.health result";
	const status = typeof value.status === "string" ? value.status : "unknown";
	const state = typeof value.state === "string" ? value.state : "unknown";
	return `status=${status}, state=${state}`;
}

/**
 * Startup readiness gate plus protocol negotiation.
 *
 * A version mismatch throws BEFORE any `consumer.claim`, so a mismatched build
 * can never settle a delivery it might misinterpret.
 */
export async function assertGatewayReady(rpc: JsonRpcClient, timeoutMs = 5_000): Promise<number> {
	const health = rpcResult<unknown>(await rpc.request("way.health", {}, { timeoutMs }), "way.health");
	if (!gatewayIsRunning(health)) {
		throw new AdapterSessionError(
			`Gateway way.health did not report healthy running status (${gatewayHealthSummary(health)}).`,
		);
	}
	const advertised = isRecord(health) ? health.adapter_protocol : undefined;
	try {
		return negotiateProtocolVersion(advertised);
	} catch (error) {
		if (error instanceof AdapterProtocolUnsupportedError) throw error;
		throw new AdapterSessionError(`Adapter protocol negotiation failed: ${(error as Error).message}`);
	}
}

/**
 * Resolves the configured surface and refuses to start unless it is usable.
 *
 * This is a hard refusal rather than a degrade-to-blind-send. Without it the
 * only quarantine catch is server-side at `main.submit`, i.e. after the inbound
 * message has already been consumed, so a quarantined surface would silently
 * swallow operator traffic.
 */
export async function assertSurfaceUsable(
	rpc: JsonRpcClient,
	surfaceId: string,
	timeoutMs = 5_000,
): Promise<SurfaceAssertion> {
	let resolved: unknown;
	try {
		resolved = rpcResult<unknown>(
			await rpc.request("surface.resolve", { surface_id: surfaceId }, { timeoutMs }),
			"surface.resolve",
		);
	} catch (error) {
		throw new AdapterSessionError(`Could not resolve configured surface ${surfaceId}: ${(error as Error).message}`);
	}
	if (!isRecord(resolved))
		throw new AdapterSessionError(`surface.resolve returned an invalid result for ${surfaceId}.`);
	const quarantined = resolved.quarantined === true;
	if (quarantined) {
		throw new AdapterSessionError(`Configured surface ${surfaceId} is quarantined; refusing to start.`);
	}
	return {
		surfaceId,
		quarantined,
		...(typeof resolved.sessionId === "string" ? { sessionId: resolved.sessionId } : {}),
	};
}

/**
 * Tracks `registry_change` events for the adapter's own surface.
 *
 * When the surface changes, outbound settling must stop and the surface must be
 * re-resolved. Events stay unclaimed and uncommitted so they redeliver:
 * committing a send to a now-quarantined surface is strictly worse than
 * delaying it.
 */
export class SurfaceWatch {
	#stale = false;
	readonly #surfaceId: string;

	constructor(surfaceId: string) {
		this.#surfaceId = surfaceId;
	}

	get stale(): boolean {
		return this.#stale;
	}

	observe(event: { readonly kind: string; readonly payload?: unknown }): void {
		if (event.kind !== "registry_change") return;
		const payload = isRecord(event.payload) ? event.payload : undefined;
		const surfaceId = payload?.surface_id ?? payload?.surfaceId;
		// An unattributed registry change is treated as relevant: failing open
		// here would keep settling against a surface that may have moved.
		if (typeof surfaceId !== "string" || surfaceId === this.#surfaceId) this.#stale = true;
	}

	async reresolve(rpc: JsonRpcClient): Promise<SurfaceAssertion> {
		const assertion = await assertSurfaceUsable(rpc, this.#surfaceId);
		this.#stale = false;
		return assertion;
	}
}

export const ADAPTER_PROTOCOL = ADAPTER_PROTOCOL_VERSION;

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
