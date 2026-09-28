import { describe, expect, it } from "bun:test";

import { findValidApprovals, getVerdictLineFromBody, parseVerdictLine, verifyMergeApproval } from "./verify-pr-verdict";

describe("parseVerdictLine", () => {
	it("parses merge-blocked verdict", () => {
		const result = parseVerdictLine("gajae.pr-review-verdict.v1 merge-blocked reviewer-id:user1");
		expect(result.status).toBe("merge-blocked");
	});

	it("parses merge-approved verdict", () => {
		const result = parseVerdictLine("gajae.pr-review-verdict.v1 merge-approved reviewer-id:user1");
		expect(result.status).toBe("merge-approved");
	});

	it("parses needs-human verdict", () => {
		const result = parseVerdictLine("gajae.pr-review-verdict.v1 needs-human reviewer-id:pending");
		expect(result.status).toBe("needs-human");
	});

	it("returns unknown for unrecognized verdict", () => {
		const result = parseVerdictLine("gajae.pr-review-verdict.v1 unknown-status");
		expect(result.status).toBe("unknown");
	});
});

describe("getVerdictLineFromBody", () => {
	it("extracts verdict line from PR body", () => {
		const body = `This is a PR description

gajae.pr-review-verdict.v1 needs-human reviewer-id:pending

More description`;
		const verdict = getVerdictLineFromBody(body);
		expect(verdict).toBe("gajae.pr-review-verdict.v1 needs-human reviewer-id:pending");
	});

	it("returns null when no verdict line present", () => {
		const body = `This is a PR description without any verdict line`;
		const verdict = getVerdictLineFromBody(body);
		expect(verdict).toBeNull();
	});

	it("handles verdict line at start of body", () => {
		const body = `gajae.pr-review-verdict.v1 merge-approved reviewer-id:user1
Description after verdict`;
		const verdict = getVerdictLineFromBody(body);
		expect(verdict).toBe("gajae.pr-review-verdict.v1 merge-approved reviewer-id:user1");
	});
});

describe("findValidApprovals", () => {
	const headSha = "abc123def456";

	it("finds valid exact-head APPROVED review from non-author", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
			],
		};

		const result = findValidApprovals(pr, headSha);
		expect(result.isValid).toBe(true);
		expect(result.approver).toBe("reviewer1");
	});

	it("rejects author self-approval", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "author" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
			],
		};

		const result = findValidApprovals(pr, headSha);
		expect(result.isValid).toBe(false);
		expect(result.reason).toContain("No valid exact-head APPROVED review");
	});

	it("rejects stale-head approval", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: "oldsha1234", // Different commit
					submitted_at: "2026-09-28T10:00:00Z",
				},
			],
		};

		const result = findValidApprovals(pr, headSha);
		expect(result.isValid).toBe(false);
		expect(result.reason).toContain("No valid exact-head APPROVED review");
	});

	it("blocks merge when CHANGES_REQUESTED exists", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
				{
					id: 2,
					user: { login: "reviewer2" },
					state: "CHANGES_REQUESTED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T11:00:00Z",
				},
			],
		};

		const result = findValidApprovals(pr, headSha);
		expect(result.isValid).toBe(false);
		expect(result.reason).toContain("CHANGES_REQUESTED");
	});

	it("allows APPROVED after CHANGES_REQUESTED if CHANGES_REQUESTED is older", () => {
		// When checking chronologically, the latest state is checked first
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "CHANGES_REQUESTED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
				{
					id: 2,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T11:00:00Z",
				},
			],
		};

		const result = findValidApprovals(pr, headSha);
		// Should still check by latest first - APPROVED is latest, no CHANGES_REQUESTED after
		expect(result.isValid).toBe(true);
		expect(result.approver).toBe("reviewer1");
	});
});

describe("verifyMergeApproval", () => {
	const headSha = "abc123def456";

	it("approves with needs-human line + exact-head approval", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description\n\ngajae.pr-review-verdict.v1 needs-human reviewer-id:pending\n\nMore text",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
			],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(true);
		expect(result.reason).toContain("overrides");
	});

	it("rejects with needs-human line + stale-head approval", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description\n\ngajae.pr-review-verdict.v1 needs-human reviewer-id:pending\n\nMore text",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: "oldsha1234",
					submitted_at: "2026-09-28T10:00:00Z",
				},
			],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(false);
		expect(result.reason).toContain("pending");
		expect(result.reason).toContain("needs-human");
	});

	it("rejects author self-approval with needs-human", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description\n\ngajae.pr-review-verdict.v1 needs-human reviewer-id:pending",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "author" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
			],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(false);
	});

	it("rejects when CHANGES_REQUESTED after approval", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
				{
					id: 2,
					user: { login: "reviewer1" },
					state: "CHANGES_REQUESTED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T11:00:00Z",
				},
			],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(false);
		expect(result.reason).toContain("CHANGES_REQUESTED");
	});

	it("rejects with merge-blocked verdict regardless of approval", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description\n\ngajae.pr-review-verdict.v1 merge-blocked reviewer-id:user1",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
			],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(false);
		expect(result.reason).toContain("merge-blocked");
	});

	it("approves with merge-approved verdict", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description\n\ngajae.pr-review-verdict.v1 merge-approved reviewer-id:user1",
			head: { sha: headSha },
			reviews: [],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(true);
		expect(result.reason).toContain("merge-approved");
	});

	it("approves with exact-head approval and no verdict line", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description without verdict line",
			head: { sha: headSha },
			reviews: [
				{
					id: 1,
					user: { login: "reviewer1" },
					state: "APPROVED" as const,
					commit_id: headSha,
					submitted_at: "2026-09-28T10:00:00Z",
				},
			],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(true);
		expect(result.approver).toBe("reviewer1");
	});

	it("stays pending with no approval and no verdict line", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description without verdict line",
			head: { sha: headSha },
			reviews: [],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(false);
		expect(result.reason).toContain("No verdict line");
	});

	it("handles empty reviews array", () => {
		const pr = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description\n\ngajae.pr-review-verdict.v1 needs-human reviewer-id:pending",
			head: { sha: headSha },
			reviews: [],
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(false);
	});

	it("handles PR with no reviews field", () => {
		// biome-ignore lint/suspicious/noExplicitAny: Testing edge case
		const pr: any = {
			number: 42,
			author_association: "OWNER",
			user: { login: "author" },
			title: "Test PR",
			body: "Description",
			head: { sha: headSha },
		};

		const result = verifyMergeApproval(pr);
		expect(result.approved).toBe(false);
	});
});
