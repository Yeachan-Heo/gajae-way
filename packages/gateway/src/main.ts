import { join } from "node:path";
import { installStructuredLogging } from "@gajae-gateway/log";
import { type BootedGateway, bootGateway } from "./boot";
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
	let booted: BootedGateway | undefined;
	const disposeLogging = installStructuredLogging({
		path: join(gatewayHome(), "gateway.log"),
		// A heartbeat that ticks while every request fails must say so (#246).
		heartbeatDetails: () => booted?.broker.outage() ?? "",
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
	// A live broker this process cannot reach is a gateway-side fault that only a
	// restart has ever cleared (#246): exit non-zero and let the service manager
	// bring up a fresh process instead of claiming health while serving nothing.
	let exiting = false;
	const onLiveOutageExceeded = (detail: string) => {
		if (exiting) return;
		exiting = true;
		reporter.report("broker_unreachable", detail, 1);
		const exit = () => {
			disposeLogging();
			process.exit(1);
		};
		if (!booted) return exit();
		// An ordered stop that wedges must not keep the dead gateway resident.
		setTimeout(exit, 30_000);
		void booted.stop("broker unreachable").then(exit, exit);
	};
	try {
		const server = await bootGateway({
			stdio: args.includes("--stdio"),
			onlyNew: args.includes("--only-new"),
			broker: { onLiveOutageExceeded },
		});
		booted = server;
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
