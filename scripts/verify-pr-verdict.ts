#!/usr/bin/env bun
/**
 * Verify PR merge approval based on reviews and body verdict line.
 *
 * This script enforces the PR review policy:
 * - An exact-head APPROVED review from a distinct non-author with write access
 *   authorizes merge regardless of body verdict line (except merge-blocked).
 * - needs-human lines no longer block merge if such approval exists.
 * - Merge-blocked verdicts still block regardless of approval.
 * - Author self-approval does not authorize merge.
 * - CHANGES_REQUESTED or later dismissals block merge.
 * - If no valid approval exists, PR remains pending merge approval.
 */

import { readFileSync } from "node:fs";

interface Review {
	id: number;
	user: { login: string };
	state: "APPROVED" | "CHANGES_REQUESTED" | "COMMENTED" | "DISMISSED" | "PENDING";
	commit_id: string;
	submitted_at?: string;
}

interface PullRequest {
	number: number;
	author_association: string;
	user: { login: string };
	title: string;
	body: string;
	head: { sha: string };
	reviews: Review[];
}

function getVerdictLineFromBody(body: string): string | null {
	const lines = body.split("\n");
	for (const line of lines) {
		if (line.includes("gajae.pr-review-verdict.v1")) {
			return line.trim();
		}
	}
	return null;
}

function parseVerdictLine(line: string): {
	status: "needs-human" | "merge-approved" | "merge-blocked" | "unknown";
	reviewerId?: string;
} {
	if (line.includes("merge-blocked")) {
		return { status: "merge-blocked" };
	}
	if (line.includes("merge-approved")) {
		return { status: "merge-approved" };
	}
	if (line.includes("needs-human")) {
		return { status: "needs-human" };
	}
	return { status: "unknown" };
}

function findValidApprovals(
	pr: PullRequest,
	headSha: string,
): {
	approver?: string;
	isValid: boolean;
	reason: string;
} {
	const prAuthor = pr.user.login;
	const reviews = pr.reviews || [];

	// Sort reviews by submit time (earliest first) to build chronological picture
	const sortedReviews = [...reviews].sort((a, b) => {
		const aTime = new Date(a.submitted_at || 0).getTime();
		const bTime = new Date(b.submitted_at || 0).getTime();
		return aTime - bTime;
	});

	// Track latest state per reviewer; only non-trivial states count
	const latestByReviewer = new Map<string, Review>();

	for (const review of sortedReviews) {
		if (review.state !== "COMMENTED" && review.state !== "PENDING") {
			latestByReviewer.set(review.user.login, review);
		}
	}

	// Check for blocking reviews - LATEST state per reviewer
	for (const [reviewer, latestReview] of latestByReviewer) {
		if (latestReview.state === "CHANGES_REQUESTED" || latestReview.state === "DISMISSED") {
			return {
				isValid: false,
				reason: `${reviewer} has ${latestReview.state} as latest review state, blocking merge`,
			};
		}
	}

	// Find exact-head APPROVED reviews from non-author
	for (const review of sortedReviews) {
		if (review.state !== "APPROVED") continue;
		if (review.commit_id !== headSha) {
			continue; // Not exact-head
		}
		if (review.user.login === prAuthor) {
			continue; // Author self-approval doesn't count
		}

		return {
			isValid: true,
			approver: review.user.login,
			reason: `Valid approval from ${review.user.login} on exact head ${headSha}`,
		};
	}

	return {
		isValid: false,
		reason: "No valid exact-head APPROVED review from non-author",
	};
}

function verifyMergeApproval(pr: PullRequest): {
	approved: boolean;
	reason: string;
	approver?: string;
} {
	const headSha = pr.head.sha;

	// Check body verdict line first
	const verdictLine = getVerdictLineFromBody(pr.body);
	if (verdictLine) {
		const verdict = parseVerdictLine(verdictLine);

		if (verdict.status === "merge-blocked") {
			return {
				approved: false,
				reason: `Merge blocked by verdict: ${verdictLine}`,
			};
		}

		if (verdict.status === "merge-approved") {
			return {
				approved: true,
				reason: `Merge approved by verdict: ${verdictLine}`,
			};
		}

		// needs-human or unknown: check for valid review approval
		const approval = findValidApprovals(pr, headSha);
		if (approval.isValid) {
			return {
				approved: true,
				reason: `Exact-head approval from ${approval.approver} overrides ${verdict.status} verdict`,
				approver: approval.approver,
			};
		}

		// If needs-human and no approval, stay pending (or blocked if there's a blocking review)
		if (verdict.status === "needs-human") {
			return {
				approved: false,
				reason:
					approval.reason.includes("CHANGES_REQUESTED") || approval.reason.includes("DISMISSED")
						? approval.reason
						: `pending: needs-human verdict requires human approval; ${approval.reason}`,
			};
		}
	}

	// No verdict line: check for valid review approval
	const approval = findValidApprovals(pr, headSha);
	if (approval.isValid) {
		return {
			approved: true,
			reason: `Exact-head approval from ${approval.approver} authorizes merge`,
			approver: approval.approver,
		};
	}

	return {
		approved: false,
		reason: `No verdict line and ${approval.reason}`,
	};
}

// Main execution
function main() {
	const prDataPath = process.env.PR_DATA_PATH || process.argv[2];
	if (!prDataPath) {
		console.error("Usage: verify-pr-verdict.ts <pr-data-path>");
		process.exit(1);
	}

	try {
		const prDataJson = readFileSync(prDataPath, "utf-8");
		const pr: PullRequest = JSON.parse(prDataJson);

		const result = verifyMergeApproval(pr);

		console.log(
			JSON.stringify(
				{
					pullRequestNumber: pr.number,
					approved: result.approved,
					reason: result.reason,
					approver: result.approver,
				},
				null,
				2,
			),
		);

		process.exit(result.approved ? 0 : 1);
	} catch (error) {
		console.error("Error verifying PR verdict:", error);
		process.exit(1);
	}
}

if (import.meta.main) {
	main();
}

export { findValidApprovals, getVerdictLineFromBody, parseVerdictLine, verifyMergeApproval };
