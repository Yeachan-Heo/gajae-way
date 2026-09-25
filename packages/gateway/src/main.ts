import { join } from "node:path";
import { installStructuredLogging } from "@gajae-gateway/log";
import { type BootedGateway, bootGateway } from "./boot";
import { gatewayHome } from "./config";
import { checkConfigFile, configCheckExitCode, defaultConfigPath, renderConfigCheck } from "./config-check";
import { DEFAULT_EXIT_WRITER, installExitReporter } from "./exit-report";
import { sanitizeDiagnostic } from "./orchestrator/rebind";

/**
 * Hard ceiling on an ordered stop. Below the 30s `TimeoutStopSec` the service
 * files ship with, so a stop that cannot settle still exits under its own
 * name instead of being SIGKILLed with nothing in the journal (#225).
 */
const SHUTDOWN_DEADLINE_MS = 25_000;

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
			// The stop is bounded and the exit explicit (#225): relays, session hosts
			// and the broker child keep the event loop alive, so a stop that merely
			// returned burned the unit's TimeoutStopSec and was SIGKILLed every time.
			const watchdog = setTimeout(() => {
				// Synchronous like the exit report: an async stderr write is dropped by
				// the exit that follows it (#182).
				DEFAULT_EXIT_WRITER(`gateway_shutdown_timeout signal=${signal} deadlineMs=${SHUTDOWN_DEADLINE_MS}\n`);
				disposeLogging();
				process.exit(1);
			}, SHUTDOWN_DEADLINE_MS);
			watchdog.unref();
			void server
				.stop("signal received")
				.catch((error: unknown) =>
					console.error(
						`gateway shutdown failed: ${sanitizeDiagnostic(error instanceof Error ? error.message : String(error))}`,
					),
				)
				.finally(() => {
					clearTimeout(watchdog);
					disposeLogging();
					process.exit(0);
				});
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
