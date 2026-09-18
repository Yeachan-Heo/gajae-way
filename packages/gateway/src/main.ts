import { join } from "node:path";
import { installStructuredLogging } from "@gajaeway/log";
import { bootGateway } from "./boot";
import { gatewayHome } from "./config";
import { checkConfigFile, configCheckExitCode, defaultConfigPath, renderConfigCheck } from "./config-check";
import { installExitReporter } from "./exit-report";
import { sanitizeDiagnostic } from "./orchestrator/rebind";

const [command, ...args] = process.argv.slice(2);
if (command === "config" && args[0] === "check") {
	// Validation runs without booting anything: this is the pre-restart gate for
	// a host where an invalid config means launchd respawns a process that exits 1.
	const result = await checkConfigFile(args[1] ?? defaultConfigPath());
	for (const line of renderConfigCheck(result)) console.log(line);
	process.exitCode = configCheckExitCode(result);
} else if (command !== "daemon") {
	console.error("usage: gajaeway-gateway daemon [--stdio] [--only-new] | config check [path]");
	process.exitCode = 2;
} else {
	const disposeLogging = installStructuredLogging({
		path: join(gatewayHome(), "gateway.log"),
	});
	// A refused config must exit with the reason, not an unhandled rejection
	// stack: under a launchd KeepAlive an unreadable config would otherwise be a
	// silent crash-loop.
	// The reporter is installed before the boot await, not after it: the death
	// this line has to explain is the one that happens while booting, and a crash
	// during boot never reaches a later installer. Each exit path below reports
	// exactly one cause, written synchronously so the last line survives the
	// process (#182).
	const reporter = installExitReporter();
	try {
		const server = await bootGateway({ stdio: args.includes("--stdio"), onlyNew: args.includes("--only-new") });
		const shutdown = (signal: NodeJS.Signals) => {
			reporter.report("signal", signal, 0);
			void server.stop("signal received").finally(disposeLogging);
		};
		process.once("SIGINT", () => shutdown("SIGINT"));
		process.once("SIGTERM", () => shutdown("SIGTERM"));
	} catch (error) {
		reporter.report(
			"boot_failure",
			sanitizeDiagnostic(error instanceof Error ? error.message : String(error)) || "unknown_error",
			1,
		);
		disposeLogging();
		process.exit(1);
	}
}
