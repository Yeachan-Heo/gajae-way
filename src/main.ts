import * as os from "node:os";
import * as path from "node:path";
import { loadWayCore, type WayCoreHandle } from "./native-loader";
import { createRpcBridge, type RpcBridgeHandler } from "./rpc-bridge";

const usage = `Usage: way [serve] [--state-dir PATH] | way --health | way --version`;

export function healthPayload(): Record<string, unknown> {
	const healthInfo = loadWayCore().healthInfo();
	return {
		status: "healthy",
		state: "running",
		version: healthInfo.version,
		bootEpoch: healthInfo.bootEpoch,
	};
}

export function defaultStateDirectory(): string {
	return process.env.WAY_STATE_DIR || path.join(os.homedir(), ".local", "state", "gajae-way");
}

function parseServeArguments(arguments_: readonly string[]): string {
	let stateDirectory = defaultStateDirectory();
	for (let index = 0; index < arguments_.length; index += 1) {
		const argument = arguments_[index];
		if (argument === "serve") continue;
		if (argument === "--state-dir") {
			const value = arguments_[index + 1];
			if (!value) throw new Error("--state-dir requires a path.");
			stateDirectory = value;
			index += 1;
			continue;
		}
		throw new Error(`Unknown argument: ${argument}\n${usage}`);
	}
	return path.resolve(stateDirectory);
}

/** Starts the real native UDS server and returns the handle for orderly shutdown. */
export function startWayServer(stateDirectory = defaultStateDirectory(), bridgeHandler?: RpcBridgeHandler): WayCoreHandle {
	const core = loadWayCore().WayCore.open(stateDirectory);
	core.startRpcServer(path.join(stateDirectory, "rpc.sock"), createRpcBridge(core, bridgeHandler));
	return core;
}

/**
 * `way serve` and a bare `way` are daemon entry points. `--health` remains a
 * synchronous local probe and intentionally never opens a state database.
 */
export function runWay(arguments_ = process.argv.slice(2)): WayCoreHandle | undefined {
	if (arguments_.includes("--help") || arguments_.includes("-h")) {
		console.log(usage);
		return undefined;
	}
	if (arguments_.includes("--version") || arguments_.includes("-V")) {
		console.log(`way ${loadWayCore().healthInfo().version}`);
		return undefined;
	}
	if (arguments_.includes("--health")) {
		console.log(JSON.stringify(healthPayload()));
		return undefined;
	}
	return startWayServer(parseServeArguments(arguments_));
}

async function waitForShutdown(core: WayCoreHandle): Promise<void> {
	await new Promise<void>(resolve => {
		const keepAlive = setInterval(() => undefined, 60_000);
		const stop = () => {
			clearInterval(keepAlive);
			core.shutdownRpcServer();
			resolve();
		};
		process.once("SIGINT", stop);
		process.once("SIGTERM", stop);
	});
}

if (import.meta.main) {
	try {
		const core = runWay();
		if (core) await waitForShutdown(core);
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}
