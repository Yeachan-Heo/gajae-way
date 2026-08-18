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

function errorPayload(error: unknown): { error: RpcBridgeError } {
	if (error instanceof RpcBridgeException) {
		return { error: { code: error.code, message: error.message, data: error.data } };
	}
	return {
		error: {
			code: -32603,
			message: "bridge_exception",
			data: { detail: error instanceof Error ? error.message : String(error) },
		},
	};
}

function unavailableHandler(method: string): { error: RpcBridgeError } {
	return { error: { code: -32601, message: `method not found: ${method}` } };
}

/**
 * Turns the native TSFN callback into a promise-safe dispatcher. Every branch
 * reports exactly one JSON completion; the native side treats late or repeated
 * completions as counted no-ops.
 */
export function createRpcBridge(core: WayCoreHandle, handler: RpcBridgeHandler = unavailableHandler): RpcBridgeCallback {
	return ((error: Error | null | RpcBridgeRequest, request?: RpcBridgeRequest) => {
		const bridgeRequest = request ?? (error as RpcBridgeRequest);
		const callbackError = request ? error : null;
		if (!bridgeRequest || typeof bridgeRequest.reqId !== "number") return;
		void Promise.resolve()
			.then(() => {
				if (callbackError) throw callbackError;
				return JSON.parse(bridgeRequest.paramsJson) as unknown;
			})
			.then(params => handler(bridgeRequest.method, params))
			.then(result => core.bridgeComplete(bridgeRequest.reqId, JSON.stringify(result)))
			.catch(error => {
				try {
					core.bridgeComplete(bridgeRequest.reqId, JSON.stringify(errorPayload(error)));
				} catch {
					// The native server can already have timed out or shut down. There is
					// no second channel on which this completion could be useful.
				}
			});
	}) as RpcBridgeCallback;
}
