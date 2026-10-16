import { describe, expect, test } from "bun:test";
import type { CliResult } from "@gajae-gateway/subsession";
import { createLaneCliRunner } from "../src/orchestrator/lane-cli-runner";

/**
 * Comprehensive tests for the lane no-merge policy.
 * Tests each merge form that must be blocked and allowed operations that must pass through.
 */

describe("lane no-merge policy: merge forms that must be blocked", () => {
	test("blocks gh pr merge with PR number", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "merge", "123"]);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr.toLowerCase()).toContain("merge");
	});

	test("blocks gh pr merge with --merge flag", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "merge", "123", "--merge"]);

		expect(result.exitCode).not.toBe(0);
	});

	test("blocks gh pr merge with --squash flag", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "merge", "123", "--squash"]);

		expect(result.exitCode).not.toBe(0);
	});

	test("blocks gh pr merge with --rebase flag", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "merge", "123", "--rebase"]);

		expect(result.exitCode).not.toBe(0);
	});

	test("blocks gh api repos/{owner}/{repo}/pulls/{number}/merge", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "api", "repos/owner/repo/pulls/123/merge", "-X", "PUT"]);

		expect(result.exitCode).not.toBe(0);
	});

	test("blocks gh api with /merge endpoint", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "api", "repos/myorg/myrepo/pulls/456/merge"]);

		expect(result.exitCode).not.toBe(0);
	});

	test("blocks GraphQL mergePullRequest mutation", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner([
			"gh",
			"api",
			"graphql",
			"-i",
			'{"query":"mutation{mergePullRequest(input:{pullRequestId:\\"123\\"}){pullRequest{id}}}"}',
		]);

		expect(result.exitCode).not.toBe(0);
		expect(result.stderr.toLowerCase()).toContain("graphql");
	});

	test("blocks GraphQL enablePullRequestAutoMerge mutation", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner([
			"gh",
			"api",
			"graphql",
			"--input",
			'{"query":"mutation{enablePullRequestAutoMerge(input:{pullRequestId:\\"456\\"}){pullRequest{id}}}"}',
		]);

		expect(result.exitCode).not.toBe(0);
	});
});

describe("lane no-merge policy: allowed operations", () => {
	test("allows gh pr create", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: '{"number": 1}', stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "create", "--title", "New feature"]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows gh pr edit", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: "", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "edit", "123", "--title", "Updated title"]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows gh pr comment", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: "", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "comment", "123", "-b", "This looks good!"]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows gh pr review", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: "", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "review", "123", "--comment", "-b", "Looks good!"]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows pushing to lane's own branch", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: "", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["git", "push", "origin", "work/lane-branch"]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows gh api for creating a PR via API", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: '{"number": 1}', stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner([
			"gh",
			"api",
			"repos/owner/repo/pulls",
			"-X",
			"POST",
			"-f",
			"title=My PR",
		]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows gh api for PR comments (no /merge in path)", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: "", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner([
			"gh",
			"api",
			"repos/owner/repo/issues/123/comments",
			"-X",
			"POST",
		]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows gh api for getting PR info", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: '{"number": 1}', stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "api", "repos/owner/repo/pulls/123"]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows other git commands", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: "", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["git", "status"]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});

	test("allows SDK commands", async () => {
		let baseCalled = false;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			baseCalled = true;
			return { exitCode: 0, stdout: '{"sessionId": "123"}', stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["sdk", "session", "create", "--cwd", "/repo"]);

		expect(result.exitCode).toBe(0);
		expect(baseCalled).toBe(true);
	});
});
