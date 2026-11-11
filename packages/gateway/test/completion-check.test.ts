import { afterEach, expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { chmod, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type CompletionCheckInput, commandCompletionCheck } from "../src/orchestrator/completion-check";
import { hookPid, pidAlive, untilGone } from "./fixtures/git-hang";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

async function scratch(): Promise<string> {
	const directory = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-completion-check-")));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	return directory;
}
/** Writes an executable /bin/sh check into `directory` and returns its path. */
async function script(directory: string, body: string): Promise<string> {
	const path = join(directory, "check.sh");
	await writeFile(path, `#!/bin/sh\n${body}\n`);
	await chmod(path, 0o755);
	return path;
}
/** Shell that publishes a pid atomically, so a reader never sees an empty file. */
function recordPid(pid: string, file: string): string {
	return `echo ${pid} > ${JSON.stringify(`${file}.tmp`)} && mv ${JSON.stringify(`${file}.tmp`)} ${JSON.stringify(file)}`;
}
function lane(worktree: string, signal?: AbortSignal): CompletionCheckInput {
	return {
		name: "a",
		laneKey: "work/task/a",
		sessionId: "11111111-1111-4111-8111-111111111111",
		worktree,
		opRef: "work-a-1",
		...(signal ? { signal } : {}),
	};
}

test("completion check maps exit 0 to passed", async () => {
	const directory = await scratch();
	const check = commandCompletionCheck([await script(directory, "echo ignored; exit 0")], 5_000);
	expect(await check.check(lane(directory))).toEqual({ kind: "passed" });
});

test("completion check maps exit 1 to failed with the command's stdout", async () => {
	const directory = await scratch();
	const check = commandCompletionCheck(
		[await script(directory, 'echo "$1: tests not run"; echo noise >&2; exit 1'), "C2"],
		5_000,
	);
	expect(await check.check(lane(directory))).toEqual({
		kind: "failed",
		output: "C2: tests not run\n",
		overflow: false,
	});
});

test("completion check keeps a bounded prefix of an oversized output and flags it", async () => {
	const directory = await scratch();
	// 200 000 bytes of stdout, well past the 64 KiB bound; the exit code still decides the outcome.
	const body = "i=0; while [ $i -lt 2000 ]; do printf '%0100d' 0; i=$((i+1)); done; exit 1";
	const outcome = await commandCompletionCheck([await script(directory, body)], 10_000).check(lane(directory));
	if (outcome.kind !== "failed") throw new Error(`unexpected outcome ${outcome.kind}`);
	expect(outcome.overflow).toBe(true);
	expect(Buffer.byteLength(outcome.output)).toBe(65_536);
});

test("completion check maps exit 3 to waiting", async () => {
	const directory = await scratch();
	const check = commandCompletionCheck([await script(directory, "exit 3")], 5_000);
	expect(await check.check(lane(directory))).toEqual({ kind: "waiting" });
});

test("completion check reports any other exit as abnormal with its code and stderr", async () => {
	const directory = await scratch();
	const check = commandCompletionCheck([await script(directory, "echo usage error >&2; exit 2")], 5_000);
	expect(await check.check(lane(directory))).toEqual({
		kind: "unavailable",
		cause: "abnormal_exit",
		exitCode: 2,
		detail: "usage error\n",
	});
});

test("completion check kills a hung command and its helper at the deadline", async () => {
	const directory = await scratch();
	const pidFile = join(directory, "helper.pid");
	// The helper inherits the pipes: only a process-group kill ends it with the command.
	// The deadline is generous enough that a loaded machine still writes the pid first.
	const body = `sleep 30 &\n${recordPid("$!", pidFile)}\nwait`;
	const pending = commandCompletionCheck([await script(directory, body)], 3_000).check(lane(directory));
	const helper = await hookPid(pidFile);
	expect(await pending).toEqual({ kind: "unavailable", cause: "timeout" });
	expect(await untilGone(helper)).toBe(true);
});

test("completion check treats a missing executable as a spawn failure", async () => {
	const directory = await scratch();
	const outcome = await commandCompletionCheck([join(directory, "absent")], 5_000).check(lane(directory));
	expect(outcome).toMatchObject({ kind: "unavailable", cause: "spawn_failed" });
});

test("completion check treats a missing worktree as a spawn failure", async () => {
	const directory = await scratch();
	const check = commandCompletionCheck([await script(directory, "exit 0")], 5_000);
	const outcome = await check.check(lane(join(directory, "gone")));
	expect(outcome).toMatchObject({ kind: "unavailable", cause: "spawn_failed" });
});

test("completion check runs in the lane worktree with the lane's identity in its environment", async () => {
	const directory = await scratch();
	const worktree = join(directory, "worktree");
	await Bun.write(join(worktree, ".keep"), "");
	const body =
		'printf \'%s\\n\' "$PWD" "$GAJAEWAY_LANE_KEY" "$GAJAEWAY_LANE_NAME" "$GAJAEWAY_SESSION_ID" "$GAJAEWAY_WORKTREE" "$GAJAEWAY_OP_REF"; exit 1';
	const outcome = await commandCompletionCheck([await script(directory, body)], 5_000).check(lane(worktree));
	if (outcome.kind !== "failed") throw new Error(`unexpected outcome ${outcome.kind}`);
	expect(outcome.output.trimEnd().split("\n")).toEqual([
		worktree,
		"work/task/a",
		"a",
		"11111111-1111-4111-8111-111111111111",
		worktree,
		"work-a-1",
	]);
});

test("completion check never starts the command for an already aborted signal", async () => {
	const directory = await scratch();
	const ran = join(directory, "ran");
	const check = commandCompletionCheck([await script(directory, `touch ${JSON.stringify(ran)}`)], 5_000);
	expect(await check.check(lane(directory, AbortSignal.abort()))).toEqual({ kind: "unavailable", cause: "aborted" });
	expect(existsSync(ran)).toBe(false);
});

test("completion check inherits only its allowlisted environment, never gateway secrets", async () => {
	const directory = await scratch();
	const body = [
		'if [ -n "$GAJAEWAY_TEST_SECRET" ]; then echo leaked; fi',
		"printf 'path=%s\\n' \"${PATH:+set}\"",
		"exit 1",
	].join("\n");
	process.env.GAJAEWAY_TEST_SECRET = "must-not-reach-the-check";
	try {
		const outcome = await commandCompletionCheck([await script(directory, body)], 5_000).check(lane(directory));
		if (outcome.kind !== "failed") throw new Error(`unexpected outcome ${outcome.kind}`);
		expect(outcome.output).not.toContain("leaked");
		expect(outcome.output).toContain("path=set");
	} finally {
		delete process.env.GAJAEWAY_TEST_SECRET;
	}
});

test("completion check kills a descendant that left the command's process group", async () => {
	const setsid = Bun.which("setsid");
	if (!setsid) return; // macOS has no setsid; the Linux CI matrix covers this case.
	const directory = await scratch();
	const pidFile = join(directory, "escaped.pid");
	const body = `${setsid} sleep 30 &\n${recordPid("$!", pidFile)}\nwait`;
	const pending = commandCompletionCheck([await script(directory, body)], 3_000).check(lane(directory));
	const escaped = await hookPid(pidFile);
	expect(await pending).toEqual({ kind: "unavailable", cause: "timeout" });
	expect(await untilGone(escaped)).toBe(true);
});

test("completion check kills a running command when its signal aborts", async () => {
	const directory = await scratch();
	const pidFile = join(directory, "check.pid");
	const abort = new AbortController();
	const body = `${recordPid("$$", pidFile)}\nsleep 30`;
	const pending = commandCompletionCheck([await script(directory, body)], 60_000).check(lane(directory, abort.signal));
	const pid = await hookPid(pidFile);
	expect(pidAlive(pid)).toBe(true);
	abort.abort();
	expect(await pending).toEqual({ kind: "unavailable", cause: "aborted" });
	expect(await untilGone(pid)).toBe(true);
});
