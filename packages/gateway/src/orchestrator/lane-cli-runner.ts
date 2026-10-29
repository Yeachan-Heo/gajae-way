import type { CliResult, CliRunner } from "@gajae-gateway/subsession";
import { checkGhCommand, formatGhBlockMessage } from "../guard/gh-guard";

/**
 * Creates a CliRunner that enforces no-merge policy for work lanes.
 * Wraps the base CLI runner with gh command guards and environment restrictions.
 */
export function createLaneCliRunner(baseCli: CliRunner): CliRunner {
	return async (args: readonly string[], options?: Parameters<CliRunner>[1]) => {
		// Apply gh command guard
		if (args[0]?.toLowerCase() === "gh" || args[0]?.endsWith("/gh")) {
			const ghArgs = args.slice(1);
			const guardResult = checkGhCommand(ghArgs);
			if (!guardResult.allowed) {
				const errorMessage = formatGhBlockMessage(guardResult);
				// Return a failed result that mimics SDK error format
				return {
					exitCode: 1,
					stdout: "",
					stderr: errorMessage || "gh command blocked",
				} as CliResult;
			}
		}

		// Apply environment restrictions for SDK commands
		let modifiedOptions = options;
		if (args[0] === "sdk" && (args[1] === "session" || args[1] === "serve")) {
			// Create modified environment without GH_TOKEN/GITHUB_TOKEN
			const env = { ...process.env };
			delete env.GH_TOKEN;
			delete env.GITHUB_TOKEN;
			// Set lane-specific GH_CONFIG_DIR if not already set
			if (!env.GH_CONFIG_DIR) {
				// Lane sessions get a config dir in the lane's workspace
				// This will be relative to the lane's worktree
				env.GH_CONFIG_DIR = ".gh-config";
			}
			modifiedOptions = { ...options, env };
		}

		// Call the base CLI runner with potentially modified options
		return await baseCli(args, modifiedOptions);
	};
}
