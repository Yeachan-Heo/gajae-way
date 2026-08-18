import { loadWayCore } from "./native-loader";

const usage = `Usage: way [--help] [--version] [--health]\n\nP0 ships a native health probe. RPC serving begins in P2.`;

export function healthPayload(): Record<string, unknown> {
	const healthInfo = loadWayCore().healthInfo();
	return {
		status: "healthy",
		state: "running",
		version: healthInfo.version,
		bootEpoch: healthInfo.bootEpoch,
	};
}

export function runWay(arguments_ = process.argv.slice(2)): void {
	if (arguments_.includes("--help") || arguments_.includes("-h")) {
		console.log(usage);
		return;
	}

	if (arguments_.includes("--version") || arguments_.includes("-V")) {
		console.log(`way ${loadWayCore().healthInfo().version}`);
		return;
	}

	if (arguments_.length === 0 || arguments_.includes("--health")) {
		console.log(JSON.stringify(healthPayload()));
		return;
	}

	throw new Error(`Unknown argument: ${arguments_[0]}\n${usage}`);
}

if (import.meta.main) {
	try {
		runWay();
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}
