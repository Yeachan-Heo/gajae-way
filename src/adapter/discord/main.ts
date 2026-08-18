import { loadWayCore } from "../../native-loader";

const usage = "Usage: way-discord [--help] [--version]";

export function runDiscordAdapter(arguments_ = process.argv.slice(2)): void {
	if (arguments_.includes("--help") || arguments_.includes("-h")) {
		console.log(usage);
		return;
	}
	if (arguments_.length === 0 || arguments_.includes("--version") || arguments_.includes("-V")) {
		console.log(`way-discord ${loadWayCore().healthInfo().version}`);
		return;
	}
	throw new Error(`Unknown argument: ${arguments_[0]}\n${usage}`);
}

if (import.meta.main) {
	try {
		runDiscordAdapter();
	} catch (error) {
		console.error(error instanceof Error ? error.message : String(error));
		process.exitCode = 1;
	}
}
