#!/usr/bin/env bun

import pkg from "../package.json";
import { dispatchConsoleCommand, dispatchSetupCommand } from "./cli-commands";

export const CONSOLE_USAGE = "usage: gajaeway-console setup|console [options] | --version";

export async function main(args: readonly string[] = process.argv.slice(2)): Promise<number> {
	const [command, ...rest] = args;
	try {
		if (command === "--version" && rest.length === 0) {
			console.log(pkg.version);
			return 0;
		}
		if ((command === "--help" || command === "-h") && rest.length === 0) {
			console.log(CONSOLE_USAGE);
			return 0;
		}
		if (command === "setup") return await dispatchSetupCommand(rest);
		if (command === "console") return await dispatchConsoleCommand(rest);
		console.error(CONSOLE_USAGE);
		return 2;
	} catch (error) {
		console.error(`gajaeway-console: ${error instanceof Error ? error.message : String(error)}`);
		return 1;
	}
}

if (import.meta.main) {
	process.exitCode = await main();
}
