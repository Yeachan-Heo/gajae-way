import { ProtocolError } from "@gajae-gateway/protocol";

/** Deadline for one local git command on the work-lane path (rev-parse, status, clone/fetch from a local path, worktree add). */
export const GIT_COMMAND_TIMEOUT_MS = 120_000;

export interface GitCommandResult {
	readonly exitCode: number;
	readonly stdout: string;
	readonly stderr: string;
}

export interface GitCommandOptions {
	readonly cwd?: string;
	readonly timeoutMs?: number;
}

/**
 * Runs `git` asynchronously under a hard deadline (#615).
 *
 * `Bun.spawnSync` parks the whole event loop until the child exits, so one git
 * command that never returns (a hook, fsmonitor, filter, or credential helper
 * stuck on the network) silenced every socket, timer and log line while the
 * process stayed alive. An abort signal alone is not enough either: it kills
 * only `git`, and a helper it spawned (`git-remote-https`, `ssh`, a hook) keeps
 * the pipes open, so reading them never finishes. The child therefore leads its
 * own process group, the deadline SIGKILLs that whole group, and the result
 * never waits on pipes past the deadline.
 */
export async function runGit(args: readonly string[], options: GitCommandOptions = {}): Promise<GitCommandResult> {
	const timeoutMs = options.timeoutMs ?? GIT_COMMAND_TIMEOUT_MS;
	const child = Bun.spawn(["git", ...args], {
		...(options.cwd === undefined ? {} : { cwd: options.cwd }),
		env: { ...process.env, GIT_TERMINAL_PROMPT: "0" },
		stdin: "ignore",
		stdout: "pipe",
		stderr: "pipe",
		detached: true,
	});
	let timer: ReturnType<typeof setTimeout> | undefined;
	const expired = new Promise<"timeout">((resolve) => {
		timer = setTimeout(() => resolve("timeout"), timeoutMs);
	});
	const completed = Promise.all([
		child.exited,
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
	]).then(([exitCode, stdout, stderr]) => ({ exitCode, stdout, stderr }));
	try {
		const outcome = await Promise.race([completed, expired]);
		if (outcome !== "timeout") return outcome;
	} finally {
		clearTimeout(timer);
	}
	killGroup(child.pid);
	// SIGKILL cannot be ignored: reap git itself so no zombie or live child outlives the call.
	await child.exited;
	completed.catch(() => undefined);
	const command = subcommand(args);
	throw new ProtocolError("verb_failed", `git ${command} timed out after ${timeoutMs}ms`, {
		reasonCode: "git_command_timeout",
		command,
		timeoutMs,
	});
}

/** The git subcommand, past any leading `-C <dir>` / `-c <key=value>` globals. */
function subcommand(args: readonly string[]): string {
	let index = 0;
	while (args[index] === "-C" || args[index] === "-c") index += 2;
	return args[index] ?? "git";
}

function killGroup(pid: number): void {
	try {
		process.kill(-pid, "SIGKILL");
	} catch {
		try {
			process.kill(pid, "SIGKILL");
		} catch {
			/* Already gone. */
		}
	}
}
