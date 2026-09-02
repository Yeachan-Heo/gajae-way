import { COMMANDS, main as cliMain, parseArgs, USAGE_EXIT_CODE } from "@gajaeway/cli";
import { checkConfigFile, configCheckExitCode, defaultConfigPath, renderConfigCheck } from "@gajaeway/gateway";
import { isDaemonLockRefusal, runDaemon } from "./daemon";
import { VERSION } from "./version";

export const APP_USAGE =
	"usage: gajaeway [--socket PATH] daemon run [--only-new]|config check [path]|status|shutdown|chat|sessions ...|ops ...|memory ...|monitors ...|work ...";

export type AppArgs =
	| { readonly kind: "daemon"; readonly onlyNew: boolean }
	| { readonly kind: "config-check"; readonly path?: string }
	| { readonly kind: "help" }
	| { readonly kind: "version" }
	| { readonly kind: "cli"; readonly args: readonly string[] }
	| { readonly kind: "usage" };

/** Resolves app-owned verbs before any filesystem, socket, or daemon work starts. */
export function parseAppArgs(args: readonly string[]): AppArgs {
	if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) return { kind: "help" };
	if (args.length === 1 && (args[0] === "--version" || args[0] === "-v")) return { kind: "version" };
	if (args[0] === "daemon" && args[1] === "run") {
		const flags = args.slice(2);
		if (flags.every((flag) => flag === "--only-new")) return { kind: "daemon", onlyNew: flags.includes("--only-new") };
	}
	if (args[0] === "config" && args[1] === "check" && args.length <= 3)
		return { kind: "config-check", ...(args[2] === undefined ? {} : { path: args[2] }) };
	if (args[0] === "daemon" || args[0] === "config") return { kind: "usage" };

	const parsed = parseArgs([...args]);
	if (parsed.command === "daemon") {
		const [verb, ...flags] = parsed.rest;
		return verb === "run" && flags.every((flag) => flag === "--only-new")
			? { kind: "daemon", onlyNew: flags.includes("--only-new") }
			: { kind: "usage" };
	}
	if (parsed.command === "config")
		return parsed.rest[0] === "check" && parsed.rest.length <= 2
			? { kind: "config-check", ...(parsed.rest[1] === undefined ? {} : { path: parsed.rest[1] }) }
			: { kind: "usage" };
	if (parsed.command && (COMMANDS as readonly string[]).includes(parsed.command)) return { kind: "cli", args };
	return { kind: "usage" };
}

export async function main(args = process.argv.slice(2)): Promise<void> {
	const parsed = parseAppArgs(args);
	switch (parsed.kind) {
		case "help":
			console.log(APP_USAGE);
			return;
		case "version":
			console.log(VERSION);
			return;
		case "config-check": {
			const result = await checkConfigFile(parsed.path ?? defaultConfigPath());
			for (const line of renderConfigCheck(result)) console.log(line);
			process.exitCode = configCheckExitCode(result);
			return;
		}
		case "daemon":
			try {
				const daemon = await runDaemon({ onlyNew: parsed.onlyNew });
				await daemon.stopped;
			} catch (error) {
				console.error(error instanceof Error ? error.message : String(error));
				process.exitCode = isDaemonLockRefusal(error) ? USAGE_EXIT_CODE : 1;
			}
			return;
		case "cli":
			await cliMain([...parsed.args]);
			return;
		case "usage":
			console.error(APP_USAGE);
			process.exitCode = USAGE_EXIT_CODE;
			return;
	}
}

if (import.meta.main) await main();
