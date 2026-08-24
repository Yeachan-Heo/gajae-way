import type { RpcBridgeCallback, RpcBridgeRequest, WayCoreHandle } from "./native-loader";

export type RpcBridgeError = {
	code: number;
	message: string;
	data?: unknown;
};

export class RpcBridgeException extends Error {
	readonly code: number;
	readonly data?: unknown;

	constructor(code: number, message: string, data?: unknown) {
		super(message);
		this.code = code;
		this.data = data;
	}
}

export type RpcBridgeHandler = (method: string, params: unknown) => unknown | Promise<unknown>;

function isSafeReason(value: unknown): value is string {
	return typeof value === "string" && /^[a-z][a-z0-9_]{0,63}$/.test(value);
}

function recordReason(value: unknown): string | undefined {
	if (typeof value !== "object" || value === null) return undefined;
	const reason = (value as { reason?: unknown }).reason;
	if (isSafeReason(reason)) return reason;
	// Broker-layer errors carry machine codes (e.g. broker_dto_drift) in .code.
	const code = (value as { code?: unknown }).code;
	return isSafeReason(code) ? code : undefined;
}

function boundedDiagnosticText(value: string): string {
	return value.slice(0, 16_384);
}

function bridgeExceptionPayload(error: unknown): RpcBridgeError {
	const reason =
		recordReason(error) ??
		(error instanceof RpcBridgeException ? recordReason(error.data) : undefined) ??
		"bridge_exception";
	const errorType =
		error instanceof Error
			? boundedDiagnosticText(error.name || error.constructor.name || "Error")
			: boundedDiagnosticText(typeof error);
	const message = boundedDiagnosticText(error instanceof Error ? error.message : String(error));
	const stack =
		error instanceof Error && typeof error.stack === "string" ? boundedDiagnosticText(error.stack) : undefined;
	return {
		code: -32603,
		message: "bridge_exception",
		data: {
			reason,
			diagnostic: { error_type: errorType, message, ...(stack ? { stack } : {}) },
		},
	};
}

function errorPayload(error: unknown): { error: RpcBridgeError } {
	if (error instanceof RpcBridgeException && error.code !== -32603) {
		return { error: { code: error.code, message: error.message, data: error.data } };
	}
	return { error: bridgeExceptionPayload(error) };
}

function unavailableHandler(method: string): { error: RpcBridgeError } {
	return { error: { code: -32601, message: `method not found: ${method}` } };
}

/**
 * Turns the native TSFN callback into a promise-safe dispatcher. Every branch
 * reports exactly one JSON completion; the native side treats late or repeated
 * completions as counted no-ops.
 */
export function createRpcBridge(
	core: WayCoreHandle,
	handler: RpcBridgeHandler = unavailableHandler,
): RpcBridgeCallback {
	return ((error: Error | null | RpcBridgeRequest, request?: RpcBridgeRequest) => {
		const bridgeRequest = request ?? (error as RpcBridgeRequest);
		const callbackError = request ? error : null;
		if (!bridgeRequest || typeof bridgeRequest.reqId !== "number") return;
		void Promise.resolve()
			.then(() => {
				if (callbackError) throw callbackError;
				return JSON.parse(bridgeRequest.paramsJson) as unknown;
			})
			.then((params) => handler(bridgeRequest.method, params))
			.then((result) => core.bridgeComplete(bridgeRequest.reqId, JSON.stringify(result)))
			.catch((error) => {
				try {
					core.bridgeComplete(bridgeRequest.reqId, JSON.stringify(errorPayload(error)));
				} catch {
					// The native server can already have timed out or shut down. There is
					// no second channel on which this completion could be useful.
				}
			});
	}) as RpcBridgeCallback;
}
