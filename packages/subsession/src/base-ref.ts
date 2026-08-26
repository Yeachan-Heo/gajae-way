/**
 * Lane baseline resolution.
 *
 * Contract (handed over by gaebal-gajae, 2026-08-26):
 *   "a lane is created from the explicit remote-tracking SHA of the repository's
 *    configured integration base."
 *
 * `origin/dev` is NOT a universal default: it is one repository's configured
 * integration branch. Resolution order is therefore
 *   1. caller-supplied baseRef
 *   2. repository/runtime configured integration branch
 *   3. verified `origin/HEAD`
 *   4. fail closed
 * and the resolved `baseRef` + `baseSha` are always recorded in the result so a
 * lane never depends on implicit inference.
 */

export type GitRunner = (args: readonly string[]) => Promise<GitResult>;

export type GitResult = {
	readonly exitCode: number;
	readonly stdout: string;
	readonly stderr: string;
};

export type BaseRefSource = "explicit" | "configured" | "origin-head";

export type ResolveBaseRefInput = {
	/** Caller-supplied ref, e.g. `origin/main`. Highest precedence. */
	readonly explicitRef?: string | undefined;
	/** Repository/runtime configured integration branch, e.g. `origin/dev`. */
	readonly configuredRef?: string | undefined;
	readonly runGit: GitRunner;
};

export type ResolvedBaseRef = {
	readonly baseRef: string;
	readonly baseSha: string;
	readonly source: BaseRefSource;
};

export class BaseRefResolutionError extends Error {
	readonly attempted: readonly string[];

	constructor(message: string, attempted: readonly string[]) {
		super(message);
		this.name = "BaseRefResolutionError";
		this.attempted = attempted;
	}
}

const SHA_PATTERN = /^[0-9a-f]{40}$/;

async function revParse(runGit: GitRunner, ref: string): Promise<string | undefined> {
	const result = await runGit(["rev-parse", "--verify", "--quiet", `${ref}^{commit}`]);
	if (result.exitCode !== 0) {
		return undefined;
	}
	const sha = result.stdout.trim();
	return SHA_PATTERN.test(sha) ? sha : undefined;
}

async function originHead(runGit: GitRunner): Promise<string | undefined> {
	const result = await runGit(["symbolic-ref", "--quiet", "refs/remotes/origin/HEAD"]);
	if (result.exitCode !== 0) {
		return undefined;
	}
	const ref = result.stdout.trim();
	const prefix = "refs/remotes/";
	if (!ref.startsWith(prefix)) {
		return undefined;
	}
	return ref.slice(prefix.length);
}

/**
 * Resolves the baseline a new lane must branch from.
 *
 * Never guesses: every candidate is verified against the repository, and an
 * unresolvable baseline is an error rather than a silent fallback to any local
 * branch or to the current HEAD.
 */
export async function resolveBaseRef(input: ResolveBaseRefInput): Promise<ResolvedBaseRef> {
	const attempted: string[] = [];

	const candidates: readonly { ref: string; source: BaseRefSource }[] = [
		...(input.explicitRef ? [{ ref: input.explicitRef, source: "explicit" as const }] : []),
		...(input.configuredRef ? [{ ref: input.configuredRef, source: "configured" as const }] : []),
	];

	for (const candidate of candidates) {
		attempted.push(candidate.ref);
		const sha = await revParse(input.runGit, candidate.ref);
		if (sha) {
			return { baseRef: candidate.ref, baseSha: sha, source: candidate.source };
		}
		if (candidate.source === "explicit") {
			throw new BaseRefResolutionError(
				`explicit baseRef ${candidate.ref} does not resolve to a commit in this repository`,
				attempted,
			);
		}
	}

	const head = await originHead(input.runGit);
	if (head) {
		attempted.push(head);
		const sha = await revParse(input.runGit, head);
		if (sha) {
			return { baseRef: head, baseSha: sha, source: "origin-head" };
		}
	}

	throw new BaseRefResolutionError(
		"could not resolve an integration base; pass an explicit baseRef or configure one",
		attempted,
	);
}

/**
 * Guards the "lane must descend from the integration base" invariant.
 *
 * A lane whose head is not a descendant of the resolved base is rejected: that
 * is the state where a worker silently rebuilds work on a stale or unrelated
 * baseline.
 */
export async function assertBaseIsAncestor(runGit: GitRunner, baseSha: string, headRef: string): Promise<void> {
	const result = await runGit(["merge-base", "--is-ancestor", baseSha, headRef]);
	if (result.exitCode !== 0) {
		throw new BaseRefResolutionError(
			`${baseSha} is not an ancestor of ${headRef}; refusing to create a lane on a divergent baseline`,
			[baseSha, headRef],
		);
	}
}
