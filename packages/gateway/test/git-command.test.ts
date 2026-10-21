import { afterEach, expect, test } from "bun:test";
import { ProtocolError } from "@gajae-gateway/protocol";
import { runGit } from "../src/orchestrator/git-command";
import { hangingStatusRepo, hookPid, pidAlive, untilGone } from "./fixtures/git-hang";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

test("#615: a git command past its deadline is killed with its whole process group and keeps the loop serviced", async () => {
	const { repo, pidFile, cleanup } = await hangingStatusRepo();
	cleanups.push(cleanup);
	let ticks = 0;
	const timer = setInterval(() => ticks++, 10);
	try {
		const startedAt = Date.now();
		const status = runGit(["-C", repo, "status", "--porcelain"], { timeoutMs: 300 });
		const pid = await hookPid(pidFile);
		expect(pidAlive(pid)).toBe(true);
		const error = await status.catch((caught: unknown) => caught);
		expect(error).toBeInstanceOf(ProtocolError);
		expect(error).toMatchObject({
			code: "verb_failed",
			detail: { reasonCode: "git_command_timeout", command: "status", timeoutMs: 300 },
		});
		expect(Date.now() - startedAt).toBeLessThan(5_000);
		// The event loop kept running timers while git was stuck.
		expect(ticks).toBeGreaterThan(5);
		// The hook was git's grandchild; it must not outlive the timed-out call.
		expect(await untilGone(pid)).toBe(true);
	} finally {
		clearInterval(timer);
	}
});

test("runGit returns the exit code and output of a command that finishes in time", async () => {
	const { repo, cleanup } = await hangingStatusRepo();
	cleanups.push(cleanup);
	const head = await runGit(["-C", repo, "rev-parse", "--verify", "HEAD"], { timeoutMs: 10_000 });
	expect(head.exitCode).toBe(0);
	expect(head.stdout.trim()).toMatch(/^[0-9a-f]{40}$/);
	const missing = await runGit(["-C", repo, "rev-parse", "--verify", "--quiet", "refs/heads/absent"], {
		timeoutMs: 10_000,
	});
	expect(missing.exitCode).not.toBe(0);
	expect(missing.stdout).toBe("");
});
