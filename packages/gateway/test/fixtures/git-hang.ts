import { existsSync } from "node:fs";
import { chmod, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

export interface HangingStatusRepo {
	readonly repo: string;
	readonly pidFile: string;
	readonly cleanup: () => Promise<void>;
}

/**
 * A repository whose `git status` never returns (#615): its fsmonitor hook
 * records its pid and then sleeps, the way a stuck hook, filter or credential
 * helper does.
 */
export async function hangingStatusRepo(): Promise<HangingStatusRepo> {
	const directory = await realpath(await mkdtemp(join(tmpdir(), "gajaeway-git-hang-")));
	const repo = join(directory, "repo");
	const pidFile = join(directory, "hook.pid");
	const hook = join(directory, "fsmonitor.sh");
	await writeFile(hook, `#!/bin/sh\necho $$ > ${JSON.stringify(pidFile)}\nexec sleep 60\n`);
	await chmod(hook, 0o755);
	const env = {
		...process.env,
		GIT_AUTHOR_NAME: "test",
		GIT_AUTHOR_EMAIL: "test@test",
		GIT_COMMITTER_NAME: "test",
		GIT_COMMITTER_EMAIL: "test@test",
	};
	for (const args of [
		["init", "-q", repo],
		["-C", repo, "commit", "-q", "--allow-empty", "-m", "initial"],
		["-C", repo, "config", "core.fsmonitor", hook],
	]) {
		const result = Bun.spawnSync(["git", ...args], { env, stdout: "ignore", stderr: "pipe" });
		if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")}: ${result.stderr.toString()}`);
	}
	return { repo, pidFile, cleanup: () => rm(directory, { recursive: true, force: true }) };
}

export function pidAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

/** Waits for the hook to record its pid. */
export async function hookPid(pidFile: string): Promise<number> {
	for (let i = 0; i < 1_000 && !existsSync(pidFile); i++) await Bun.sleep(5);
	return Number((await readFile(pidFile, "utf8")).trim());
}

/** Waits up to ~2s for a killed pid to disappear. */
export async function untilGone(pid: number): Promise<boolean> {
	for (let i = 0; i < 400 && pidAlive(pid); i++) await Bun.sleep(5);
	return !pidAlive(pid);
}
