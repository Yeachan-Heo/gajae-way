/**
 * Lane-scoped GitHub CLI guard: rejects merge operations and enforces no-merge policy.
 * Lanes cannot merge PRs, push to protected branches, or enable auto-merge.
 */

export type GhGuardResult = { allowed: true } | { allowed: false; reason: string };

/**
 * Check if a gh command should be blocked for lane execution.
 * Returns an error message if blocked, undefined if allowed.
 */
export function checkGhCommand(args: readonly string[]): GhGuardResult {
	if (args.length === 0) {
		return { allowed: true };
	}

	const command = args[0]?.toLowerCase() ?? "";
	const subcommand = args[1]?.toLowerCase() ?? "";
	const subcommand2 = args[2]?.toLowerCase() ?? "";

	// Reject: gh pr merge
	if (command === "pr" && subcommand === "merge") {
		return { allowed: false, reason: "lanes cannot merge pull requests" };
	}

	// Reject: gh api .../merge (and .../pulls/*/merge)
	if (command === "api") {
		const apiPath = args[1] ?? "";
		if (apiPath.includes("/merge")) {
			return { allowed: false, reason: "lanes cannot perform merge operations via API" };
		}
		// Check for GraphQL mutations
		const input = args.find((arg) => arg.startsWith("--input") || arg.startsWith("-i"));
		if (input) {
			const nextIndex = args.indexOf(input) + 1;
			if (nextIndex < args.length) {
				const inputJson = args[nextIndex];
				if (inputJson?.includes("mergePullRequest") || inputJson?.includes("enablePullRequestAutoMerge")) {
					return {
						allowed: false,
						reason: "lanes cannot perform merge operations via GraphQL",
					};
				}
			}
		}
	}

	// Note: Direct git push checks would need to be done at a different layer
	// since gh doesn't directly control push operations to protected branches.
	// The main gh guard is for gh-specific operations above.

	return { allowed: true };
}

/**
 * Format a blocked gh command as an error message.
 */
export function formatGhBlockMessage(result: GhGuardResult): string | undefined {
	if (result.allowed) return undefined;
	return `gh: ${result.reason}`;
}
