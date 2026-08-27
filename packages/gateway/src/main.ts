import { bootGateway } from "./boot";
import { checkConfigFile, configCheckExitCode, defaultConfigPath, renderConfigCheck } from "./config-check";

const [command, ...args] = process.argv.slice(2);
if (command === "config" && args[0] === "check") {
	// Validation runs without booting anything: this is the pre-restart gate for
	// a host where an invalid config means launchd respawns a process that exits 1.
	const result = await checkConfigFile(args[1] ?? defaultConfigPath());
	for (const line of renderConfigCheck(result)) console.log(line);
	process.exitCode = configCheckExitCode(result);
} else if (command !== "daemon") {
	console.error("usage: gajaeway-gateway daemon [--stdio] | config check [path]");
	process.exitCode = 2;
} else {
	const server = await bootGateway({ stdio: args.includes("--stdio") });
	const shutdown = () => void server.stop("signal received");
	process.once("SIGINT", shutdown);
	process.once("SIGTERM", shutdown);
}
