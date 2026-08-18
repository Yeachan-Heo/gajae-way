export class RpcBridgeUnavailableError extends Error {
	constructor() {
		super("The Rust RPC bridge is not available before P2.");
	}
}

/** P0 makes accidental use of the future bridge explicit rather than silently dropping requests. */
export function assertRpcBridgeAvailable(): never {
	throw new RpcBridgeUnavailableError();
}
