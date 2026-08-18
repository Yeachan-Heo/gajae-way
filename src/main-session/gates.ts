import { RpcBridgeException } from "../rpc-bridge";
import type { HostedSdkGateResolution } from "./sdk";

export type GateState = "open" | "resolved" | "expired";

/** Durable SDK gate identity observed by the main-session host. */
export interface GateHandle {
	readonly gateId: string;
	readonly expectedSessionId: string;
	readonly expiresAt?: number;
}

interface GateRecord extends GateHandle {
	state: GateState;
}

/**
 * Tracks only durable workflow gates observed from the main SDK session. The
 * SDK remains the authority for the gate record and answer validation.
 */
export class MainSessionGateRegistry {
	readonly #gates = new Map<string, GateRecord>();
	readonly #now: () => number;

	constructor(options: { now?: () => number } = {}) {
		this.#now = options.now ?? Date.now;
	}

	observeOpen(handle: GateHandle): boolean {
		if (!handle.gateId || !handle.expectedSessionId) return false;
		const existing = this.#gates.get(handle.gateId);
		if (existing) return false;
		this.#gates.set(handle.gateId, { ...handle, state: "open" });
		return true;
	}

	observeResolved(gateId: string): boolean {
		const record = this.#gates.get(gateId);
		if (!record || record.state === "resolved") return false;
		record.state = "resolved";
		return true;
	}

	observeExpired(gateId: string): boolean {
		const record = this.#gates.get(gateId);
		if (!record || record.state === "expired") return false;
		record.state = "expired";
		return true;
	}

	answerState(gateId: string, expectedSessionId: string): "open" | "already_resolved" {
		const record = this.#gates.get(gateId);
		if (!record) throw new RpcBridgeException(1100, "gate_not_found");
		if (record.expectedSessionId !== expectedSessionId) throw new RpcBridgeException(1102, "gate_session_mismatch");
		if (record.state === "expired" || (record.expiresAt !== undefined && record.expiresAt <= this.#now())) {
			record.state = "expired";
			throw new RpcBridgeException(1101, "gate_expired");
		}
		return record.state === "resolved" ? "already_resolved" : "open";
	}
}

export interface GateIdempotencyStore {
	idempotencyReplay(input: { scope: string; key: string; requestJson: string }): { replayed: boolean; responseJson?: string };
	idempotencyStore(input: { scope: string; key: string; requestJson: string; responseJson: string }): void;
}

export interface MainGateAnswerTarget {
	readonly sessionId: string;
	readonly gates: MainSessionGateRegistry;
	resolveGate(gateId: string, answer: unknown, idempotencyKey: string): Promise<HostedSdkGateResolution>;
}

interface GateAnswerRequest {
	readonly gateId: string;
	readonly expectedSessionId: string;
	readonly answer: unknown;
	readonly idempotencyKey: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function requiredString(value: unknown, field: string): string {
	if (typeof value !== "string" || !value.trim()) throw new RpcBridgeException(-32602, `${field} must be a non-empty string.`);
	return value.trim();
}

function parseRequest(params: unknown): GateAnswerRequest {
	if (!isRecord(params)) throw new RpcBridgeException(-32602, "params must be an object.");
	const allowed = new Set(["gate_id", "expected_session_id", "answer", "idempotency_key"]);
	for (const key of Object.keys(params)) {
		if (!allowed.has(key)) throw new RpcBridgeException(-32602, `unknown parameter: ${key}`);
	}
	if (!Object.hasOwn(params, "answer")) throw new RpcBridgeException(-32602, "answer is required.");
	return {
		gateId: requiredString(params.gate_id, "gate_id"),
		expectedSessionId: requiredString(params.expected_session_id, "expected_session_id"),
		answer: params.answer,
		idempotencyKey: requiredString(params.idempotency_key, "idempotency_key"),
	};
}

/** Stable JSON used by the durable idempotency store regardless of request key order. */
export function canonicalJson(value: unknown): string {
	if (value === null || typeof value === "boolean" || typeof value === "number" || typeof value === "string") {
		return JSON.stringify(value);
	}
	if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
	if (isRecord(value)) {
		return `{${Object.keys(value)
			.sort()
			.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
			.join(",")}}`;
	}
	throw new RpcBridgeException(-32602, "params must contain JSON values.");
}

function idempotencyFailure(error: unknown): never {
	if (error instanceof RpcBridgeException) throw error;
	const message = error instanceof Error ? error.message : String(error);
	if (message.includes("1500 ") || message.includes("idempotency conflict")) {
		throw new RpcBridgeException(1500, "idempotency_conflict");
	}
	throw error;
}

/** Bridges one validated main-session gate response to the durable SDK gate. */
export function createMainGateAnswerHandler(target: MainGateAnswerTarget, idempotency: GateIdempotencyStore) {
	return async (params: unknown): Promise<{ accepted: boolean; gate_state: "resolved" | "already_resolved" }> => {
		const request = parseRequest(params);
		const requestJson = canonicalJson({
			answer: request.answer,
			expected_session_id: request.expectedSessionId,
			gate_id: request.gateId,
			idempotency_key: request.idempotencyKey,
		});
		let replay;
		try {
			replay = idempotency.idempotencyReplay({ scope: "main.gate.answer", key: request.idempotencyKey, requestJson });
		} catch (error) {
			idempotencyFailure(error);
		}
		if (replay?.replayed) return { accepted: true, gate_state: "already_resolved" };

		let gateState: "open" | "resolved" | "already_resolved" = target.gates.answerState(request.gateId, request.expectedSessionId);
		if (gateState === "open") {
			const resolution = await target.resolveGate(request.gateId, request.answer, request.idempotencyKey);
			if (resolution === "not_found") throw new RpcBridgeException(1100, "gate_not_found");
			if (resolution === "expired") throw new RpcBridgeException(1101, "gate_expired");
			if (resolution === "rejected") throw new RpcBridgeException(-32602, "gate answer was rejected.");
			if (resolution === "resolved") {
				target.gates.observeResolved(request.gateId);
				gateState = "resolved";
			} else {
				target.gates.observeResolved(request.gateId);
				gateState = "already_resolved";
			}
		}
		const response = { accepted: true, gate_state: gateState } as const;
		try {
			idempotency.idempotencyStore({
				scope: "main.gate.answer",
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
