import { StreamingDiagnosticTail } from "./rebind";
import type { CheckStatusClient, CheckStatusSnapshot } from "./work-lane";

/** One entry of `gh pr list --json statusCheckRollup`: a CheckRun or a commit StatusContext. */
interface RollupEntry {
	readonly __typename?: string;
	readonly name?: string;
	readonly context?: string;
	readonly status?: string;
	readonly conclusion?: string;
	readonly state?: string;
	readonly detailsUrl?: string;
}

const FAILED_LOG_TAIL_LIMIT_BYTES = 3_200;

const FAILED_CONCLUSIONS = new Set(["FAILURE", "TIMED_OUT", "CANCELLED", "ACTION_REQUIRED", "STARTUP_FAILURE"]);
const SUCCESS_CONCLUSIONS = new Set(["SUCCESS", "NEUTRAL", "SKIPPED"]);
const FAILED_STATES = new Set(["FAILURE", "ERROR"]);

/**
 * Collapse a PR status-check rollup into one snapshot. Any failed entry makes it red;
 * otherwise any entry without a final verdict keeps it pending; an empty rollup is pending
 * because CI has not registered yet.
 */
export function classifyCheckRollup(
	prNumber: number,
	headSha: string,
	rollup: readonly RollupEntry[],
): CheckStatusSnapshot {
	if (!/^[0-9a-f]{40}$/.test(headSha)) throw new Error("PR head SHA is invalid");
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
		const conclusion = entry.conclusion?.toUpperCase() ?? "";
		if (FAILED_CONCLUSIONS.has(conclusion)) failingChecks.push(label);
		else if (!SUCCESS_CONCLUSIONS.has(conclusion)) pending = true;
	}
	const status = failingChecks.length > 0 ? "red" : pending ? "pending" : "green";
	return { prNumber, headSha, status, failingChecks: [...new Set(failingChecks)] };
}

/** Production client: bounded async PR/check lookups; failed GitHub Actions logs are a best-effort red-check tail. */
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
						"all",
						"--limit",
						"1",
						"--json",
						"number,headRefOid,state,mergedAt,statusCheckRollup",
					],
					{ cwd, stdin: "ignore", stdout: "pipe", stderr: "ignore", timeout: timeoutMs, killSignal: "SIGKILL" },
				);
				const [stdout, exitCode] = await Promise.all([new Response(child.stdout).text(), child.exited]);
				if (exitCode !== 0) return undefined;
				const prs = JSON.parse(stdout) as Array<{
					number: number;
					headRefOid: string;
					state: string;
					mergedAt?: string | null;
					statusCheckRollup?: RollupEntry[] | null;
				}>;
				const pr = prs[0];
				if (!pr) return null;
				const headSha = pr.headRefOid;
				if (!headSha) return undefined;
				const rollup = pr.statusCheckRollup ?? [];
				const snapshot = classifyCheckRollup(pr.number, headSha, rollup);
				const withPrState: CheckStatusSnapshot = {
					...snapshot,
					prState: pr.state.toUpperCase() === "OPEN" ? "open" : "closed",
					merged: Boolean(pr.mergedAt),
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
		},
	};
}

export function failedCheckLogTargets(rollup: readonly RollupEntry[]): Array<{ runId: string; jobId?: string }> {
	const targets = new Map<string, { runId: string; jobId?: string }>();
	for (const entry of rollup) {
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
