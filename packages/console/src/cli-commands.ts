import { parseSetupArgs, type RunSetupOptions, runSetup, SETUP_USAGE } from "../../cli/src/setup";
import { runConsole } from "./console-dashboard";
import { runSetupWizard } from "./setup-wizard";

export const CONSOLE_USAGE = "usage: gajaeway console [--panel {status|sessions|lanes|updates|logs}]";

export interface SetupCommandOptions extends Partial<RunSetupOptions> {
	readonly socket?: string;
	readonly out?: (text: string) => void;
	readonly err?: (text: string) => void;
	readonly wizard?: typeof runSetupWizard;
	readonly legacySetup?: typeof runSetup;
}

export interface ConsoleCommandOptions extends NonNullable<Parameters<typeof runConsole>[0]> {
	readonly out?: (text: string) => void;
	readonly err?: (text: string) => void;
	readonly dashboard?: typeof runConsole;
}

function defaultHome(env: NodeJS.ProcessEnv = process.env): string {
	return env.GAJAEWAY_HOME ?? `${env.HOME ?? "~"}/.gajaeway`;
}

function message(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

export async function dispatchSetupCommand(
	args: readonly string[],
	options: SetupCommandOptions = {},
): Promise<number> {
	const out = options.out ?? options.log ?? console.log;
	const err = options.err ?? console.error;
	if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) {
		out(SETUP_USAGE);
		return 0;
	}
	let parsed: ReturnType<typeof parseSetupArgs>;
	try {
		parsed = parseSetupArgs(args);
	} catch (error) {
		err(message(error));
		return 2;
	}
	try {
		const home = options.home ?? defaultHome(options.env);
		if (parsed.fromEnv || parsed.mode !== "setup") {
			await (options.legacySetup ?? runSetup)(args, { ...options, home, log: out });
			// The legacy broker runner owns its child exit status.
			return parsed.mode === "broker-agent" ? Number(process.exitCode ?? 0) : 0;
		}
		await (options.wizard ?? runSetupWizard)({
			home,
			parsed,
			socket: options.socket,
			env: options.env,
			fetcher: options.fetch,
		});
		return 0;
	} catch (error) {
		err(message(error));
		return 1;
	}
}

export async function dispatchConsoleCommand(
	args: readonly string[],
	options: ConsoleCommandOptions = {},
): Promise<number> {
	const out = options.out ?? console.log;
	const err = options.err ?? console.error;
	if (args.length === 1 && (args[0] === "--help" || args[0] === "-h")) {
		out(CONSOLE_USAGE);
		return 0;
	}
	let panel = options.panel ?? "status";
	for (let i = 0; i < args.length; i++) {
		const flag = args[i];
		const value = args[++i];
		if (
			flag !== "--panel" ||
			(value !== "status" && value !== "sessions" && value !== "lanes" && value !== "updates" && value !== "logs")
		) {
			err(CONSOLE_USAGE);
			return 2;
		}
		panel = value;
	}
	const home = options.home ?? defaultHome();
	const socket = options.socket ?? process.env.GAJAEWAY_SOCKET_PATH ?? `${home}/gateway.sock`;
	if (socket.trim() === "" || socket.includes("\0")) {
		err("console requires a non-empty socket path without NUL characters (--socket PATH)");
		return 2;
	}
	try {
		await (options.dashboard ?? runConsole)({ ...options, home, socket, panel });
		return 0;
	} catch (error) {
		err(`console: ${message(error)}`);
		return 1;
	}
}
