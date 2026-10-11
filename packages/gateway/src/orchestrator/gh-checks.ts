import type { CheckStatusClient, CheckStatusSnapshot } from "./work-lane";

/** One entry of `gh pr list --json statusCheckRollup`: a CheckRun or a commit StatusContext. */
interface RollupEntry {
	readonly __typename?: string;
	readonly name?: string;
	readonly context?: string;
	readonly status?: string;
	readonly conclusion?: string;
	readonly state?: string;
}

const FAILED_CONCLUSIONS = new Set(["FAILURE", "TIMED_OUT", "CANCELLED", "ACTION_REQUIRED", "STARTUP_FAILURE"]);
const FAILED_STATES = new Set(["FAILURE", "ERROR"]);

/**
 * Collapse a PR status-check rollup into one snapshot. Any failed entry makes it red;
 * otherwise any entry without a final verdict keeps it pending; an empty rollup is pending
 * because CI has not registered yet.
 */
export function classifyCheckRollup(prNumber: number, rollup: readonly RollupEntry[]): CheckStatusSnapshot {
	const failingChecks: string[] = [];
	let pending = rollup.length === 0;
	for (const entry of rollup) {
		const label = entry.name ?? entry.context ?? "unknown";
		if (entry.__typename === "StatusContext" || (entry.state !== undefined && entry.status === undefined)) {
			const state = entry.state?.toUpperCase() ?? "PENDING";
			if (FAILED_STATES.has(state)) failingChecks.push(label);
			else if (state !== "SUCCESS") pending = true;
			continue;
		}
		if (entry.status?.toUpperCase() !== "COMPLETED") {
			pending = true;
			continue;
		}
		if (FAILED_CONCLUSIONS.has(entry.conclusion?.toUpperCase() ?? "")) failingChecks.push(label);
	}
	const status = failingChecks.length > 0 ? "red" : pending ? "pending" : "green";
	return { prNumber, status, failingChecks: [...new Set(failingChecks)] };
}

/** Production client: one async `gh pr list` call bounded by `timeoutMs`; never blocks the event loop. */
export function ghCheckStatusClient(timeoutMs = 5_000): CheckStatusClient {
	return {
		async getCheckStatus(branch, cwd) {
			try {
				const child = Bun.spawn(
					[
						"gh",
						"pr",
						"list",
						"--head",
						branch,
						"--state",
						"open",
						"--limit",
						"1",
						"--json",
						"number,statusCheckRollup",
					],
					{ cwd, stdin: "ignore", stdout: "pipe", stderr: "ignore", timeout: timeoutMs, killSignal: "SIGKILL" },
				);
				const [stdout, exitCode] = await Promise.all([new Response(child.stdout).text(), child.exited]);
				if (exitCode !== 0) return undefined;
				const prs = JSON.parse(stdout) as Array<{ number: number; statusCheckRollup?: RollupEntry[] | null }>;
				const pr = prs[0];
				if (!pr) return null;
				return classifyCheckRollup(pr.number, pr.statusCheckRollup ?? []);
			} catch {
				return undefined;
			}
		},
	};
}
