import { describe, expect, test } from "bun:test";
import { type CliResult, type ControllerOptions, inspectSession, listSessions } from "../src/cli";
import { sendPrompt } from "../src/send";
import { fetchOpState } from "../src/status";
import { fetchLastAssistant, listTranscript } from "../src/transcript";

// GJC 0.19.0 registry: `--agent-dir` is a leaf option, and `--repo` belongs
// only on scoped commands. Explicit `--scope cwd` keeps a controller's list
// scoped to its workspace even outside Git.
const WORKTREE = "/wt/subsession-runtime";
const AGENT_DIR = "/profiles/gjc-agent";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";
const REPO_SCOPED = new Set(["list", "tail"]);

function assertParses(args: readonly string[]): void {
	expect(args.slice(0, 2)).toEqual(["sdk", "session"]);
	expect(args.slice(-2)).toEqual(["--agent-dir", AGENT_DIR]);
	expect(args.indexOf("--agent-dir")).toBe(args.length - 2);
	if (!REPO_SCOPED.has(args[2] ?? "")) expect(args).not.toContain("--repo");
}

function recorder(result: unknown): { options: ControllerOptions; calls: string[][] } {
	const calls: string[][] = [];
	const ok: CliResult = { exitCode: 0, stdout: JSON.stringify({ ok: true, result }), stderr: "" };
	return {
		calls,
		options: {
			repo: WORKTREE,
			agentDir: AGENT_DIR,
			run: async (args) => {
				calls.push([...args]);
				return ok;
			},
		},
	};
}

describe("every sdk session leaf builds argv the 0.19.0 registry parses", () => {
	test("list selects cwd scope and binds the agent dir at the leaf", async () => {
		const { options, calls } = recorder({ sessions: [] });
		await listSessions(options);
		expect(calls[0]).toEqual([
			"sdk",
			"session",
			"list",
			"--scope",
			"cwd",
			"--repo",
			WORKTREE,
			"--agent-dir",
			AGENT_DIR,
		]);
		assertParses(calls[0] ?? []);
	});

	test("inspect resolves by session ID only", async () => {
		const { options, calls } = recorder({});
		await inspectSession(options, SESSION);
		expect(calls[0]).toEqual(["sdk", "session", "inspect", SESSION, "--agent-dir", AGENT_DIR]);
	});

	test("send, status, and raw queries carry no --repo", async () => {
		const { options, calls } = recorder({ commandId: "c", turnId: "t", sessionId: SESSION, page: { complete: true } });
		await sendPrompt(options, { sessionId: SESSION, taskKey: "contract", text: "go", opRef: "gw-contract-1" });
		await fetchOpState(options, SESSION, "gw-contract-1").catch(() => undefined);
		await fetchLastAssistant(options, SESSION).catch(() => undefined);
		await listTranscript(options, SESSION).catch(() => undefined);
		expect(calls.map((args) => args[2])).toEqual(["send", "status", "raw", "raw"]);
		for (const args of calls) assertParses(args);
	});
});
