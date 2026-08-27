import { bootGateway } from "./boot";

const [command, ...args] = process.argv.slice(2);
if (command !== "daemon") {
	console.error("usage: gajaeway-gateway daemon [--stdio]");
	process.exitCode = 2;
} else {
	// A refused config must exit with the reason, not an unhandled rejection
	// stack: under a launchd KeepAlive an unreadable config would otherwise be a
	// silent crash-loop.
	try {
		const server = await bootGateway({ stdio: args.includes("--stdio") });
		const shutdown = () => void server.stop("signal received");
		process.once("SIGINT", shutdown);
		process.once("SIGTERM", shutdown);
	} catch (error) {
		console.error(`gajaeway-gateway failed to start: ${error instanceof Error ? error.message : String(error)}`);
		process.exit(1);
	}
}
