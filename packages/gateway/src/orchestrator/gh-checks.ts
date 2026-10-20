import { StreamingDiagnosticTail } from "./rebind";
import type { CheckStatusClient, CheckStatusSnapshot } from "./work-lane";

/** One entry of `gh pr view --json statusCheckRollup`: a CheckRun or a commit StatusContext. */
interface RollupEntry {
	readonly __typename?: string;
	readonly name?: string;
	readonly context?: string;
	readonly status?: string;
	readonly conclusion?: string;
	readonly state?: string;
	readonly detailsUrl?: string;
	readonly startedAt?: string;
	readonly createdAt?: string;
	readonly checkSuite?: {
		readonly app?: { readonly id?: string | null } | null;
		readonly workflowRun?: { readonly workflow?: { readonly id?: string | null } | null } | null;
	} | null;
}

interface AssociatedPullRequest {
	readonly number: number;
	readonly state: string;
	readonly merged_at?: string | null;
	readonly updated_at?: string;
	readonly head: { readonly ref: string; readonly sha: string };
}

const FAILED_LOG_TAIL_LIMIT_BYTES = 3_200;
const PR_LOOKUP_LIMIT = 100;

const FAILED_CONCLUSIONS = new Set(["FAILURE", "TIMED_OUT", "CANCELLED", "ACTION_REQUIRED", "STARTUP_FAILURE"]);
const SUCCESS_CONCLUSIONS = new Set(["SUCCESS", "NEUTRAL", "SKIPPED"]);
const FAILED_STATES = new Set(["FAILURE", "ERROR"]);

const PR_CHECK_QUERY = `
query ($owner: String!, $repo: String!, $number: Int!) {
	repository(owner: $owner, name: $repo) {
		pullRequest(number: $number) {
			number
			headRefOid
			state
			mergedAt
			statusCheckRollup {
				contexts(first: 100) {
					nodes {
						__typename
						... on CheckRun {
							name
						status
							conclusion
							startedAt
							detailsUrl
							checkSuite { app { id } workflowRun { workflow { id } } }
						}
						... on StatusContext { context state createdAt targetUrl }
					}
					pageInfo { hasNextPage }
				}
			}
		}
	}
}`;

interface PullRequestLookup {
	readonly results: readonly unknown[] | undefined;
	readonly fullPage: boolean;
}

/**
 * Collapse a PR status-check rollup into one snapshot. Only the latest check
 * with each name is considered; an unfinished latest check keeps the rollup
 * pending, and any terminal failure makes it red. An empty rollup is pending.
 */
export function classifyCheckRollup(
	prNumber: number,
	headSha: string,
	rollup: readonly RollupEntry[],
): CheckStatusSnapshot {
	if (!/^[0-9a-f]{40}$/.test(headSha)) throw new Error("PR head SHA is invalid");
	const latest = latestCheckEntries(rollup);
	const failingChecks: string[] = [];
	let pending = latest.length === 0;
	for (const entry of latest) {
		const label = entry.name ?? entry.context ?? "unknown";
		if (isStatusContext(entry)) {
			const state = entry.state?.toUpperCase() ?? "PENDING";
			if (FAILED_STATES.has(state)) failingChecks.push(label);
			else if (state !== "SUCCESS") pending = true;
			continue;
		}
		if (entry.status?.toUpperCase() !== "COMPLETED") {
			pending = true;
			continue;
		}
		const conclusion = entry.conclusion?.toUpperCase() ?? "";
		if (FAILED_CONCLUSIONS.has(conclusion)) failingChecks.push(label);
		else if (!SUCCESS_CONCLUSIONS.has(conclusion)) pending = true;
	}
	const status = pending ? "pending" : failingChecks.length > 0 ? "red" : "green";
	return { prNumber, headSha, status, failingChecks: status === "red" ? [...new Set(failingChecks)] : [] };
}

function latestCheckEntries(rollup: readonly RollupEntry[]): RollupEntry[] {
	const latest = new Map<string, RollupEntry>();
	for (const [index, entry] of rollup.entries()) {
		const name = entry.name ?? entry.context ?? `unknown:${index}`;
		const key = checkIdentity(entry, name);
		const previous = latest.get(key);
		if (!previous || isLaterCheck(entry, previous)) latest.set(key, entry);
	}
	return [...latest.values()];
}

function isStatusContext(entry: RollupEntry): boolean {
	return entry.__typename === "StatusContext" || (entry.state !== undefined && entry.status === undefined);
}

function checkIdentity(entry: RollupEntry, name: string): string {
	if (isStatusContext(entry)) return `status:${name}`;
	const appId = entry.checkSuite?.app?.id ?? "unknown-app";
	const workflowId = entry.checkSuite?.workflowRun?.workflow?.id ?? "unknown-workflow";
	return `check-run:${appId}:${workflowId}:${name}`;
}

function isLaterCheck(candidate: RollupEntry, previous: RollupEntry): boolean {
	const candidateTime = Date.parse(candidate.startedAt ?? candidate.createdAt ?? "");
	const previousTime = Date.parse(previous.startedAt ?? previous.createdAt ?? "");
	if (Number.isFinite(candidateTime) && Number.isFinite(previousTime)) return candidateTime >= previousTime;
	// GitHub's rollup is newest-last when timestamps are not available.
	return true;
}

/** Production client: bounded async PR/check lookups; failed GitHub Actions logs are a best-effort red-check tail. */
export function ghCheckStatusClient(timeoutMs = 5_000, executable = "gh"): CheckStatusClient {
	return {
		async getCheckStatus(headSha, cwd, preferredPrNumber, branchName) {
			try {
				if (!/^[0-9a-f]{40}$/.test(headSha)) return undefined;
				if (preferredPrNumber !== undefined)
					return await pullRequestCheckStatus(executable, cwd, timeoutMs, preferredPrNumber);
				const [byHeadSha, byBranch] = await Promise.all([
					queryPullRequests(executable, cwd, timeoutMs, [
						"api",
						`repos/{owner}/{repo}/commits/${headSha}/pulls`,
						"-X",
						"GET",
						"-F",
						`per_page=${PR_LOOKUP_LIMIT}`,
					]),
					branchName === undefined
						? Promise.resolve(undefined)
						: queryPullRequests(executable, cwd, timeoutMs, [
								"api",
								"repos/{owner}/{repo}/pulls",
								"-X",
								"GET",
								"-F",
								`head={owner}:${branchName}`,
								"-F",
								"state=all",
								"-F",
								`per_page=${PR_LOOKUP_LIMIT}`,
							]),
				]);
				if (byHeadSha.fullPage || byBranch?.fullPage) return undefined;
				const candidates = new Map<number, AssociatedPullRequest>();
				for (const candidate of (byHeadSha.results as readonly AssociatedPullRequest[] | undefined) ?? [])
					if (candidate.head?.sha === headSha) candidates.set(candidate.number, candidate);
				for (const candidate of (byBranch?.results as readonly AssociatedPullRequest[] | undefined) ?? [])
					if (candidate.head?.sha === headSha) candidates.set(candidate.number, candidate);
				const matchingPrs = [...candidates.values()];
				if (matchingPrs.length === 0) {
					if (byHeadSha.results === undefined || (branchName !== undefined && byBranch?.results === undefined))
						return undefined;
					return null;
				}
				const pr = selectAssociatedPullRequest(matchingPrs, branchName);
				if (!pr) return null;
				return await pullRequestCheckStatus(executable, cwd, timeoutMs, pr.number);
			} catch {
				return undefined;
			}
		},
	};
}

async function pullRequestCheckStatus(
	executable: string,
	cwd: string,
	timeoutMs: number,
	prNumber: number,
): Promise<CheckStatusSnapshot | undefined> {
	try {
		const child = Bun.spawn(
			[
				executable,
				"api",
				"graphql",
				"-F",
				"owner={owner}",
				"-F",
				"repo={repo}",
				"-F",
				`number=${prNumber}`,
				"-f",
				`query=${PR_CHECK_QUERY}`,
			],
			{ cwd, stdin: "ignore", stdout: "pipe", stderr: "ignore", timeout: timeoutMs, killSignal: "SIGKILL" },
		);
		const [stdout, exitCode] = await Promise.all([new Response(child.stdout).text(), child.exited]);
		if (exitCode !== 0) return undefined;
		const result = JSON.parse(stdout) as {
			data?: {
				repository?: {
					pullRequest?: {
						number: number;
						headRefOid: string;
						state: string;
						mergedAt?: string | null;
						statusCheckRollup?: {
							contexts?: { nodes?: RollupEntry[]; pageInfo?: { hasNextPage?: boolean } } | null;
						} | null;
					} | null;
				};
			};
		};
		const current = result.data?.repository?.pullRequest;
		if (!current || current.number !== prNumber || !/^[0-9a-f]{40}$/.test(current.headRefOid)) return undefined;
		const contexts = current.statusCheckRollup?.contexts;
		if (contexts?.pageInfo?.hasNextPage) return undefined;
		const rollup = contexts?.nodes ?? [];
		const snapshot = classifyCheckRollup(prNumber, current.headRefOid, rollup);
		const withPrState: CheckStatusSnapshot = {
			...snapshot,
			prState: current.state.toUpperCase() === "OPEN" ? "open" : "closed",
			merged: Boolean(current.mergedAt),
		};
		if (snapshot.status !== "red") return withPrState;
		const targets = failedCheckLogTargets(rollup).slice(0, 3);
		const tails = await Promise.all(
			targets.map((target) => readFailedLogTail(target, cwd, Math.min(timeoutMs, 2_000))),
		);
		const joinedTail = tails.filter(Boolean).join("\n---\n");
		const failureLogTail = utf8Suffix(joinedTail, FAILED_LOG_TAIL_LIMIT_BYTES);
		return failureLogTail ? { ...withPrState, failureLogTail } : withPrState;
	} catch {
		return undefined;
	}
}

function selectAssociatedPullRequest(
	prs: readonly AssociatedPullRequest[],
	branchName?: string,
): AssociatedPullRequest | undefined {
	return [...prs].sort((left, right) => {
		const leftOpen = left.state.toUpperCase() === "OPEN";
		const rightOpen = right.state.toUpperCase() === "OPEN";
		if (leftOpen !== rightOpen) return leftOpen ? -1 : 1;
		const leftBranch = branchName !== undefined && left.head.ref === branchName;
		const rightBranch = branchName !== undefined && right.head.ref === branchName;
		if (leftBranch !== rightBranch) return leftBranch ? -1 : 1;
		const leftUpdated = Date.parse(left.updated_at ?? "");
		const rightUpdated = Date.parse(right.updated_at ?? "");
		if (Number.isFinite(leftUpdated) && Number.isFinite(rightUpdated) && leftUpdated !== rightUpdated)
			return rightUpdated - leftUpdated;
		return left.number - right.number;
	})[0];
}

async function queryPullRequests(
	executable: string,
	cwd: string,
	timeoutMs: number,
	args: readonly string[],
): Promise<PullRequestLookup> {
	try {
		const child = Bun.spawn([executable, ...args], {
			cwd,
			stdin: "ignore",
			stdout: "pipe",
			stderr: "ignore",
			timeout: timeoutMs,
			killSignal: "SIGKILL",
		});
		const [stdout, exitCode] = await Promise.all([new Response(child.stdout).text(), child.exited]);
		if (exitCode !== 0) return { results: undefined, fullPage: false };
		const results = JSON.parse(stdout) as unknown;
		if (!Array.isArray(results)) return { results: undefined, fullPage: false };
		if (results.length >= PR_LOOKUP_LIMIT) return { results: undefined, fullPage: true };
		return { results, fullPage: false };
	} catch {
		return { results: undefined, fullPage: false };
	}
}

export function failedCheckLogTargets(rollup: readonly RollupEntry[]): Array<{ runId: string; jobId?: string }> {
	const targets = new Map<string, { runId: string; jobId?: string }>();
	for (const entry of latestCheckEntries(rollup)) {
		if (entry.status?.toUpperCase() !== "COMPLETED" || !FAILED_CONCLUSIONS.has(entry.conclusion?.toUpperCase() ?? ""))
			continue;
		if (!entry.detailsUrl) continue;
		try {
			const url = new URL(entry.detailsUrl);
			if (url.hostname !== "github.com") continue;
			const match = url.pathname.match(/^\/[^/]+\/[^/]+\/actions\/runs\/(\d+)(?:\/job\/(\d+))?\/?$/);
			if (!match?.[1]) continue;
			const target = match[2] ? { runId: match[1], jobId: match[2] } : { runId: match[1] };
			targets.set(`${target.runId}/${target.jobId ?? ""}`, target);
		} catch {
			// Non-GitHub checks and malformed links have no GitHub Actions log tail.
		}
	}
	return [...targets.values()];
}

async function readFailedLogTail(
	target: { readonly runId: string; readonly jobId?: string },
	cwd: string,
	timeoutMs: number,
): Promise<string | undefined> {
	try {
		const child = Bun.spawn(
			["gh", "run", "view", target.runId, ...(target.jobId ? ["--job", target.jobId] : []), "--log-failed"],
			{ cwd, stdin: "ignore", stdout: "pipe", stderr: "ignore", timeout: timeoutMs, killSignal: "SIGKILL" },
		);
		const result = await sanitizedFailureLogStreamTail(child.stdout);
		if (result.overflowed) {
			child.kill("SIGKILL");
			await child.exited;
			return undefined;
		}
		const exitCode = await child.exited;
		return exitCode === 0 ? result.tail : undefined;
	} catch {
		return undefined;
	}
}

/** Sanitizes bounded lines incrementally, including multiline credential arrays, retaining only a tail. */
export async function sanitizedFailureLogStreamTail(
	stream: ReadableStream<Uint8Array>,
): Promise<{ readonly tail?: string; readonly overflowed: boolean }> {
	const tail = new StreamingDiagnosticTail(FAILED_LOG_TAIL_LIMIT_BYTES);
	const decoder = new TextDecoder();
	const reader = stream.getReader();
	for (;;) {
		const { done, value } = await reader.read();
		if (done) {
			tail.push(decoder.decode());
			const result = tail.finish();
			return { ...(result ? { tail: result } : {}), overflowed: tail.overflowed };
		}
		if (!value) continue;
		for (let offset = 0; offset < value.byteLength; offset += 16 * 1024) {
			const chunk = value.subarray(offset, Math.min(value.byteLength, offset + 16 * 1024));
			tail.push(decoder.decode(chunk, { stream: true }));
			if (tail.overflowed) {
				await reader.cancel();
				return { overflowed: true };
			}
		}
	}
}

function utf8Suffix(text: string, byteLimit: number): string {
	const bytes = Buffer.from(text, "utf8");
	return bytes.subarray(Math.max(0, bytes.length - byteLimit)).toString("utf8");
}
