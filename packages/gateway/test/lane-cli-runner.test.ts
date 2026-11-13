import { describe, expect, test, spyOn } from "bun:test";
import type { CliResult } from "@gajae-gateway/subsession";
import { createLaneCliRunner } from "../src/orchestrator/lane-cli-runner";

describe("lane CLI runner: gh command blocking", () => {
	test("blocks gh pr merge", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "should not reach here",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "merge", "123"]);

		expect(result.exitCode).toBe(1);
		expect(result.stderr).toContain("lanes cannot merge");
	});

	test("blocks gh api merge endpoint", async () => {
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => ({
			exitCode: 0,
			stdout: "should not reach here",
			stderr: "",
		});

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "api", "repos/owner/repo/pulls/123/merge"]);

		expect(result.exitCode).toBe(1);
		expect(result.stderr).toContain("lanes cannot");
	});

	test("allows gh pr create", async () => {
		let calledWithArgs: readonly string[] | undefined;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			calledWithArgs = args;
			return { exitCode: 0, stdout: '{"number": 1}', stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "create", "--title", "My PR"]);

		expect(result.exitCode).toBe(0);
		expect(calledWithArgs).toEqual(["gh", "pr", "create", "--title", "My PR"]);
	});

	test("allows gh pr comment", async () => {
		let calledWithArgs: readonly string[] | undefined;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			calledWithArgs = args;
			return { exitCode: 0, stdout: "Comment added", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["gh", "pr", "comment", "123", "-b", "LGTM"]);

		expect(result.exitCode).toBe(0);
		expect(calledWithArgs).toEqual(["gh", "pr", "comment", "123", "-b", "LGTM"]);
	});
});

describe("lane CLI runner: environment restrictions", () => {
	test("removes GH_TOKEN from environment for SDK session create", async () => {
		let capturedEnv: Record<string, string | undefined> | undefined;
		const baseCliMock = async (args: readonly string[], options?: any): Promise<CliResult> => {
			capturedEnv = options?.env;
			return { exitCode: 0, stdout: '{"sessionId": "123"}', stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		// Set environment variables for the test
		const originalEnv = { ...process.env };
		process.env.GH_TOKEN = "ghp_xxx";
		process.env.GITHUB_TOKEN = "ghu_xxx";

		try {
			await laneRunner(["sdk", "session", "create", "--cwd", "/repo"], {
				env: process.env,
			});

			// Verify environment was modified in the call
			expect(capturedEnv?.GH_TOKEN).toBeUndefined();
			expect(capturedEnv?.GITHUB_TOKEN).toBeUndefined();
		} finally {
			// Restore original environment
			Object.assign(process.env, originalEnv);
		}
	});

	test("sets GH_CONFIG_DIR for lane sessions", async () => {
		let capturedEnv: Record<string, string | undefined> | undefined;
		const baseCliMock = async (args: readonly string[], options?: any): Promise<CliResult> => {
			capturedEnv = options?.env;
			return { exitCode: 0, stdout: '{"sessionId": "123"}', stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		await laneRunner(["sdk", "session", "create", "--cwd", "/repo"], {
			env: {},
		});

		expect(capturedEnv?.GH_CONFIG_DIR).toBe(".gh-config");
	});

	test("preserves non-SDK commands without environment modification", async () => {
		let calledOptions: any;
		const baseCliMock = async (args: readonly string[], options?: any): Promise<CliResult> => {
			calledOptions = options;
			return { exitCode: 0, stdout: "result", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		await laneRunner(["git", "status"], { env: { CUSTOM: "value" } });

		// For non-SDK commands, options should be passed through unchanged
		expect(calledOptions?.env?.CUSTOM).toBe("value");
	});
});

describe("lane CLI runner: pass through non-gh commands", () => {
	test("allows git commands", async () => {
		let calledWithArgs: readonly string[] | undefined;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			calledWithArgs = args;
			return { exitCode: 0, stdout: "master", stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["git", "branch"]);

		expect(result.exitCode).toBe(0);
		expect(calledWithArgs).toEqual(["git", "branch"]);
	});

	test("allows SDK session commands", async () => {
		let calledWithArgs: readonly string[] | undefined;
		const baseCliMock = async (args: readonly string[]): Promise<CliResult> => {
			calledWithArgs = args;
			return { exitCode: 0, stdout: '{"sessionId": "123"}', stderr: "" };
		};

		const laneRunner = createLaneCliRunner(baseCliMock);
		const result = await laneRunner(["sdk", "session", "create"]);

		expect(result.exitCode).toBe(0);
		expect(calledWithArgs).toEqual(["sdk", "session", "create"]);
	});
});
