import { describe, expect, test } from "bun:test";
import { checkGhCommand } from "../src/guard/gh-guard";

describe("gh-guard: merge prevention", () => {
	test("blocks gh pr merge", () => {
		const result = checkGhCommand(["pr", "merge", "123"]);
		expect(result.allowed).toBe(false);
		if (!result.allowed) {
			expect(result.reason).toContain("cannot merge");
		}
	});

	test("blocks gh api with merge in path", () => {
		const result = checkGhCommand(["api", "repos/owner/repo/pulls/123/merge", "-X", "PUT"]);
		expect(result.allowed).toBe(false);
		if (!result.allowed) {
			expect(result.reason).toContain("merge");
		}
	});

	test("blocks gh api .../pulls/*/merge", () => {
		const result = checkGhCommand(["api", "repos/owner/repo/pulls/123/merge"]);
		expect(result.allowed).toBe(false);
		if (!result.allowed) {
			expect(result.reason).toContain("merge");
		}
	});

	test("blocks GraphQL mergePullRequest", () => {
		const result = checkGhCommand([
			"api",
			"graphql",
			"-i",
			'{"query": "mutation { mergePullRequest(input: {pullRequestId: \\"123\\"}) { pullRequest { id } } }"}',
		]);
		expect(result.allowed).toBe(false);
		if (!result.allowed) {
			expect(result.reason).toContain("GraphQL");
		}
	});

	test("blocks GraphQL enablePullRequestAutoMerge", () => {
		const result = checkGhCommand([
			"api",
			"graphql",
			"--input",
			'{"query": "mutation { enablePullRequestAutoMerge(input: {pullRequestId: \\"123\\"}) { pullRequest { id } } }"}',
		]);
		expect(result.allowed).toBe(false);
		if (!result.allowed) {
			expect(result.reason).toContain("GraphQL");
		}
	});
});

describe("gh-guard: allowed operations", () => {
	test("allows gh pr create", () => {
		const result = checkGhCommand(["pr", "create", "--title", "My PR"]);
		expect(result.allowed).toBe(true);
	});

	test("allows gh pr edit", () => {
		const result = checkGhCommand(["pr", "edit", "123", "--title", "Updated"]);
		expect(result.allowed).toBe(true);
	});

	test("allows gh pr comment", () => {
		const result = checkGhCommand(["pr", "comment", "123", "-b", "My comment"]);
		expect(result.allowed).toBe(true);
	});

	test("allows gh pr review", () => {
		const result = checkGhCommand(["pr", "review", "123", "--comment", "-b", "Looks good"]);
		expect(result.allowed).toBe(true);
	});

	test("allows gh api without merge in path", () => {
		const result = checkGhCommand(["api", "repos/owner/repo"]);
		expect(result.allowed).toBe(true);
	});

	test("allows gh api for PR create", () => {
		const result = checkGhCommand([
			"api",
			"repos/owner/repo/pulls",
			"-X",
			"POST",
			"-f",
			"title=My PR",
		]);
		expect(result.allowed).toBe(true);
	});

	test("allows empty command args", () => {
		const result = checkGhCommand([]);
		expect(result.allowed).toBe(true);
	});
});
