import { bootGateway } from "./boot";
import { transportFlag } from "./config";
import { checkConfigFile, configCheckExitCode, defaultConfigPath, renderConfigCheck } from "./config-check";
import { sanitizeDiagnostic } from "./orchestrator/rebind";

const [command, ...args] = process.argv.slice(2);
if (command === "config" && args[0] === "check") {
	// Validation runs without booting anything: this is the pre-restart gate for
	// a host where an invalid config means launchd respawns a process that exits 1.
	const result = await checkConfigFile(args[1] ?? defaultConfigPath());
	for (const line of renderConfigCheck(result)) console.log(line);
	process.exitCode = configCheckExitCode(result);
} else if (command !== "daemon") {
	console.error(
		"usage: gajaeway-gateway daemon [--stdio] [--only-new] [--transport cli|channel] | config check [path]",
	);
	process.exitCode = 2;
} else {
	// A refused config must exit with the reason, not an unhandled rejection
	// stack: under a launchd KeepAlive an unreadable config would otherwise be a
	// silent crash-loop.
	try {
		const transport = transportFlag(args);
		const server = await bootGateway({
			stdio: args.includes("--stdio"),
			onlyNew: args.includes("--only-new"),
			overrides: transport ? { transport } : {},
		});
		// I6d two-stage handler: the first signal starts the bounded ordered stop;
		// a second signal forces an immediate exit instead of being ignored.
		let signalled = false;
		const shutdown = (signal: string) => {
			if (signalled) {
				console.error(`shutdown_forced reason=second_${signal}`);
				process.exit(0);
			}
			signalled = true;
			void server.stop(`signal received (${signal})`);
		};
		process.on("SIGINT", () => shutdown("SIGINT"));
		process.on("SIGTERM", () => shutdown("SIGTERM"));
	} catch (error) {
		console.error(
			`gajaeway-gateway failed to start: ${sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error"}`,
		);
		process.exit(1);
	}
}
