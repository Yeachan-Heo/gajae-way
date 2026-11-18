import { expect, test } from "bun:test";
import { existsSync } from "node:fs";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ProtocolError } from "@gajae-gateway/protocol";
import {
	appendAttempt,
	closeAttempt,
	createLaneJobRecord,
	type LaneJobRecord,
	newOpRef,
	parseLaneJobRecord,
} from "@gajae-gateway/subsession";
import { renderCycle } from "../../cli/src/main";
import { parseConfigFile } from "../src/config";
import { RuntimeCycleProjector } from "../src/ops/cycle";
import {
	LaneGovernor,
	laneJobIdentity,
	managedLaneBranchName,
	managedLaneRepositoryPath,
	managedLaneWorktreePath,
} from "../src/orchestrator/lane-governor";
import { WorkLaneManager } from "../src/orchestrator/work-lane";
import { Store } from "../src/store";
import { ScriptedSessionPort } from "./session-port.fake";

const NOW = Date.parse("2026-11-17T12:00:00.000Z");
const SESSION_ID = "0f1e2d3c-4b5a-4678-8796-a5b4c3d2e1f0";
const DAY = 24 * 60 * 60_000;

function git(args: readonly string[], cwd?: string): string {
	const result = Bun.spawnSync(["git", ...args], {
		...(cwd === undefined ? {} : { cwd }),
		stdout: "pipe",
		stderr: "pipe",
	});
	const stdout = result.stdout.toString();
	if (result.exitCode !== 0) throw new Error(`git ${args.join(" ")} failed: ${result.stderr.toString()}`);
	return stdout;
}

/**
 * Configures the upstream's default branch symbol so the merged signal can
 * fetch the base as `+HEAD:...` (simulates any integration base, `dev` or
 * `main`, without hardcoding a branch name in production code).
 */
function setUpstreamDefaultBranch(paths: Awaited<ReturnType<typeof managedWorktree>>): void {
	git(["symbolic-ref", "HEAD", "refs/heads/main"], upstreamOf(paths));
}

/** Move a base branch forward with an unrelated commit and refresh the lane repo's tracking refs. */
async function advanceBaseWithUnrelatedCommit(
	paths: Awaited<ReturnType<typeof managedWorktree>>,
	baseBranch = "main",
): Promise<void> {
	const upstream = upstreamOf(paths);
	const tempDir = await mkdtemp(join(tmpdir(), "temp-base-advance-"));
	try {
		git(["clone", "--quiet", "--shared", upstream, tempDir]);
		git(["config", "user.name", "Lane Reclaimer Test"], tempDir);
		git(["config", "user.email", "lane-reclaimer@example.test"], tempDir);
		await writeFile(join(tempDir, "base-only.txt"), "unrelated base work\n");
		git(["add", "base-only.txt"], tempDir);
		git(["commit", "-m", "unrelated base work"], tempDir);
		git(["push", "--quiet", "origin", `HEAD:refs/heads/${baseBranch}`], tempDir);
	} finally {
		await rm(tempDir, { recursive: true, force: true });
	}
	git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], paths.repositoryPath);
}

/** Squash-merge the lane branch onto a named base branch and delete the remote lane branch. */
async function squashMergeLaneToBase(
	paths: Awaited<ReturnType<typeof managedWorktree>>,
	baseBranch = "main",
): Promise<void> {
	const upstream = upstreamOf(paths);
	const tempDir = await mkdtemp(join(tmpdir(), "temp-squash-merge-"));
	try {
		git(["clone", "--quiet", "--shared", upstream, tempDir]);
		git(["config", "user.name", "Lane Reclaimer Test"], tempDir);
		git(["config", "user.email", "lane-reclaimer@example.test"], tempDir);
		git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], tempDir);
		git(["checkout", "-q", "-B", "squash-merge", `origin/${baseBranch}`], tempDir);
		git(["merge", "--squash", "--quiet", `origin/${paths.branch}`], tempDir);
		git(["commit", "-m", `Squash-merge ${paths.branch} onto the integration base`], tempDir);
		git(["push", "--quiet", "origin", `HEAD:refs/heads/${baseBranch}`], tempDir);
	} finally {
		await rm(tempDir, { recursive: true, force: true });
	}
	gitAllow(["push", "origin", "--delete", paths.branch], paths.worktreePath);
	git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], paths.repositoryPath);
}

/** Cherry-pick each lane commit onto the base one-by-one (rebase merge) and delete the remote lane branch. */
async function rebaseMergeLaneToBase(paths: Awaited<ReturnType<typeof managedWorktree>>): Promise<void> {
	const upstream = upstreamOf(paths);
	const tempDir = await mkdtemp(join(tmpdir(), "temp-rebase-merge-"));
	try {
		git(["clone", "--quiet", "--shared", upstream, tempDir]);
		git(["config", "user.name", "Lane Reclaimer Test"], tempDir);
		git(["config", "user.email", "lane-reclaimer@example.test"], tempDir);
		git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], tempDir);
		git(["checkout", "-q", "-B", "rebase-merge", "origin/main"], tempDir);
		git(["cherry-pick", `origin/main...origin/${paths.branch}`, "--right-only"], tempDir);
		git(["push", "--quiet", "origin", "HEAD:refs/heads/main"], tempDir);
	} finally {
		await rm(tempDir, { recursive: true, force: true });
	}
	gitAllow(["push", "origin", "--delete", paths.branch], paths.worktreePath);
	git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], paths.repositoryPath);
}

/** Fire-and-report git call for fixtures that intentionally drive git into a failure state. */
function gitAllow(args: readonly string[], cwd?: string): { exitCode: number; stdout: string; stderr: string } {
	const result = Bun.spawnSync(["git", ...args], {
		...(cwd === undefined ? {} : { cwd }),
		stdout: "pipe",
		stderr: "pipe",
	});
	return {
		exitCode: result.exitCode,
		stdout: result.stdout.toString(),
		stderr: result.stderr.toString(),
	};
}

/** The upstream path a fixture lane pushes to (the seed's origin remote). */
function upstreamOf(paths: Awaited<ReturnType<typeof managedWorktree>>): string {
	return gitAllow(["config", "remote.origin.url"], paths.worktreePath).stdout.trim();
}

async function fixture(options: Partial<ConstructorParameters<typeof LaneGovernor>[0]> = {}) {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-lane-reclaimer-"));
	const laneRoot = join(directory, "home");
	await mkdir(join(laneRoot, "repositories"), { recursive: true });
	await mkdir(join(laneRoot, "lanes"), { recursive: true });
	const serviceDirectory = join(directory, "service");
	await mkdir(serviceDirectory);
	const database = await Store.open(join(directory, "gateway.sqlite"));
	const logs: string[] = [];
	const sessionPort = new ScriptedSessionPort();
	const governor = new LaneGovernor({
		database,
		sessionPort,
		laneRoot,
		now: () => NOW,
		log: (line) => logs.push(line),
		...options,
	});
	return {
		directory,
		laneRoot,
		serviceDirectory,
		database,
		logs,
		sessionPort,
		governor,
		close: async () => {
			await governor.stop();
			database.close();
			await rm(directory, { recursive: true, force: true });
		},
	};
}

async function managedWorktree(
	f: Awaited<ReturnType<typeof fixture>>,
	name: string,
	options: { readonly baseBranch?: string } = {},
) {
	const upstream = join(f.directory, `upstream-${name}.git`);
	const seed = join(f.directory, `seed-${name}`);
	const repositoryPath = managedLaneRepositoryPath(f.laneRoot, name);
	const worktreePath = managedLaneWorktreePath(f.laneRoot, name);
	await mkdir(seed);
	git(["init", "--bare", upstream]);
	git(["init", "--initial-branch=main"], seed);
	git(["config", "user.name", "Lane Reclaimer Test"], seed);
	git(["config", "user.email", "lane-reclaimer@example.test"], seed);
	await writeFile(join(seed, "README.md"), "seed\n");
	git(["add", "README.md"], seed);
	git(["commit", "-m", "seed"], seed);
	git(["remote", "add", "origin", upstream], seed);
	if (options.baseBranch !== undefined) git(["branch", options.baseBranch, "refs/heads/main"], seed);
	git(["push", "-u", "origin", options.baseBranch ?? "main"], seed);
	git(["clone", "--bare", upstream, repositoryPath]);
	git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], repositoryPath);
	git(["config", "user.name", "Lane Reclaimer Test"], repositoryPath);
	git(["config", "user.email", "lane-reclaimer@example.test"], repositoryPath);
	const branch = managedLaneBranchName(name);
	const worktreeBase = `refs/remotes/origin/${options.baseBranch ?? "main"}`;
	git(["worktree", "add", "-b", branch, worktreePath, worktreeBase], repositoryPath);
	const baselineSha = git(["rev-parse", worktreeBase], repositoryPath).trim();
	return { upstream, repositoryPath, worktreePath, branch, baselineSha };
}

function persistJob(
	database: Store,
	name: string,
	paths: Awaited<ReturnType<typeof managedWorktree>>,
	state: "done" | "attempt_ended" | "awaiting_operator" | "running",
	updatedAt: number,
	options: { readonly baselineSha?: string } = {},
): LaneJobRecord {
	const identity = laneJobIdentity(name);
	const updatedAtIso = new Date(updatedAt).toISOString();
	let record = createLaneJobRecord({
		jobId: identity.jobId,
		branch: paths.branch,
		worktreePath: paths.worktreePath,
		repository: paths.upstream,
		repositoryPath: paths.repositoryPath,
		baselineSha: options.baselineSha ?? paths.baselineSha,
		now: () => new Date(updatedAt),
	});
	if (state === "attempt_ended" || state === "awaiting_operator" || state === "running") {
		const opRef = newOpRef(`reclaimer-${name}`);
		record = appendAttempt(record, {
			opRef,
			sessionId: SESSION_ID,
			startedAt: new Date(updatedAt - 60_000).toISOString(),
		});
		if (state === "attempt_ended" || state === "awaiting_operator")
			record = closeAttempt({
				record,
				opRef,
				endState: state === "attempt_ended" ? "attempt_ended" : "terminal_uncertain",
				endedAt: updatedAtIso,
			});
	}
	record = { ...record, state, updatedAt: updatedAtIso };
	database.transaction((tx) =>
		database.work.putLaneJob(tx, {
			jobId: record.jobId,
			laneKey: identity.laneKey,
			state: record.state,
			createdAt: record.createdAt,
			updatedAt: record.updatedAt,
			lane: record.lane,
			json: JSON.stringify(record),
		}),
	);
	return record;
}

function savedJob(database: Store, name: string): LaneJobRecord {
	const json = database.work.laneJobJson(laneJobIdentity(name).jobId);
	if (json === undefined) throw new Error(`missing lane job ${name}`);
	return parseLaneJobRecord(json);
}

async function addCommit(worktreePath: string, message: string): Promise<void> {
	await writeFile(join(worktreePath, `${message.replaceAll(" ", "-")}.txt`), `${message}\n`);
	git(["add", "-A"], worktreePath);
	git(["commit", "-m", message], worktreePath);
}

async function pushHead(paths: Awaited<ReturnType<typeof managedWorktree>>): Promise<void> {
	git(["push", "origin", `HEAD:refs/heads/${paths.branch}`], paths.worktreePath);
	git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], paths.repositoryPath);
}

test("a done lane past its grace period is removed and marked reclaimed", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "done-old");
		persistJob(f.database, "done-old", paths, "done", NOW - DAY - 1);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
		const job = savedJob(f.database, "done-old");
		expect(job).toMatchObject({ state: "done", reclaimedAt: new Date(NOW).toISOString() });
	} finally {
		await f.close();
	}
});

test("a done lane inside its grace period is kept", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "done-fresh");
		persistJob(f.database, "done-fresh", paths, "done", NOW - DAY + 1);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(0);
		expect(existsSync(paths.worktreePath)).toBe(true);
		expect(savedJob(f.database, "done-fresh").reclaimedAt).toBeUndefined();
	} finally {
		await f.close();
	}
});

test("a dirty stale attempt_ended lane is kept, logged, and exposed in health", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "ended-dirty");
		setUpstreamDefaultBranch(paths);
		persistJob(f.database, "ended-dirty", paths, "attempt_ended", NOW - 8 * DAY);
		await writeFile(join(paths.worktreePath, "untracked.txt"), "keep me\n");
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(0);
		expect(existsSync(paths.worktreePath)).toBe(true);
		expect(f.logs).toContain("lane_reclaim_skipped name=ended-dirty reason=dirty");
		expect(f.governor.laneDiskSnapshot).toMatchObject({ dirtyAttemptEnded: 1, unpushedAttemptEnded: 0 });
		const health = new RuntimeCycleProjector(
			f.database,
			{ queueDepth: 0 },
			{
				laneDisk: () => f.governor.laneDiskSnapshot,
			},
		).project(new Date(NOW));
		expect(health.laneDisk).toMatchObject({
			path: join(f.laneRoot, "lanes"),
			totalBytes: expect.any(Number),
			reclaimableBytes: 0,
			dirtyAttemptEnded: 1,
		});
	} finally {
		await f.close();
	}
});

test("a clean stale attempt_ended lane with an unpushed HEAD is kept and reported", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "ended-unpushed");
		setUpstreamDefaultBranch(paths);
		await addCommit(paths.worktreePath, "unpushed change");
		persistJob(f.database, "ended-unpushed", paths, "attempt_ended", NOW - 8 * DAY);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(0);
		expect(existsSync(paths.worktreePath)).toBe(true);
		// Fail closed: a commit that exists only in the worktree is not on the
		// integration base, so the merged signal refuses regardless of push state.
		expect(f.logs).toContain("lane_reclaim_skipped name=ended-unpushed reason=work_not_merged_into_base");
		expect(f.governor.laneDiskSnapshot?.unpushedAttemptEnded).toBe(0);
	} finally {
		await f.close();
	}
});

test("git evidence errors keep an expired attempt_ended worktree", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "ended-git-error");
		persistJob(f.database, "ended-git-error", paths, "attempt_ended", NOW - 8 * DAY);
		await rm(join(paths.worktreePath, ".git"));
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(0);
		expect(existsSync(paths.worktreePath)).toBe(true);
		expect(f.logs).toContain("lane_reclaim_skipped name=ended-git-error reason=git_head_failed");
	} finally {
		await f.close();
	}
});

test("a clean, pushed stale attempt_ended lane merged into the base is removed and marked reclaimed", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "ended-pushed");
		setUpstreamDefaultBranch(paths);
		await addCommit(paths.worktreePath, "pushed change");
		await pushHead(paths);
		// Land the pushed work on the integration base via squash-merge.
		await advanceBaseWithUnrelatedCommit(paths);
		squashMergeLaneToBase(paths);
		persistJob(f.database, "ended-pushed", paths, "attempt_ended", NOW - 8 * DAY);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
		expect(savedJob(f.database, "ended-pushed").reclaimedAt).toBe(new Date(NOW).toISOString());
	} finally {
		await f.close();
	}
});

test("resumed and live lanes are kept", async () => {
	const f = await fixture();
	try {
		const resumed = await managedWorktree(f, "resumed");
		persistJob(f.database, "resumed", resumed, "running", NOW - 8 * DAY);
		const held = await managedWorktree(f, "held");
		persistJob(f.database, "held", held, "awaiting_operator", NOW - 8 * DAY);
		const live = await managedWorktree(f, "live-ended");
		persistJob(f.database, "live-ended", live, "attempt_ended", NOW - 8 * DAY);
		f.database.transaction((tx) => f.database.bindings.bind(tx, "work/task/live-ended", SESSION_ID));
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(0);
		expect(existsSync(resumed.worktreePath)).toBe(true);
		expect(existsSync(held.worktreePath)).toBe(true);
		expect(existsSync(live.worktreePath)).toBe(true);
		expect(f.logs).toContain("lane_reclaim_skipped name=live-ended reason=live");
	} finally {
		await f.close();
	}
});

test("health includes lane disk totals and low-free-disk warning gate", async () => {
	const f = await fixture({ lowDiskFreeBytes: Number.MAX_SAFE_INTEGER });
	try {
		const paths = await managedWorktree(f, "health");
		persistJob(f.database, "health", paths, "done", NOW - DAY - 1);
		await f.governor.reclaimTerminalWorktrees(NOW);
		const snapshot = f.governor.laneDiskSnapshot;
		expect(snapshot?.totalBytes).toBeNumber();
		expect(snapshot?.reclaimableBytes).toBeNumber();
		expect(snapshot?.lowFreeDisk).toBe(true);
		const projector = new RuntimeCycleProjector(f.database, { queueDepth: 0 }, { laneDisk: () => snapshot });
		const health = projector.project(new Date(NOW));
		expect(health.laneDisk).toMatchObject({
			totalBytes: expect.any(Number),
			reclaimableBytes: expect.any(Number),
			lowFreeDisk: true,
		});
		expect(health.gates).toContain("lane_disk_headroom");
		const diskLine = renderCycle(health).find((line) => line.startsWith("lane_disk:"));
		expect(diskLine).toContain(`used_bytes=${snapshot?.totalBytes}`);
		expect(diskLine).toContain(`reclaimable_bytes=${snapshot?.reclaimableBytes}`);
	} finally {
		await f.close();
	}
});

test("reclaimed work status and resume return an explicit error instead of recreating state", async () => {
	const f = await fixture();
	let manager: WorkLaneManager | undefined;
	try {
		const paths = await managedWorktree(f, "reclaimed");
		persistJob(f.database, "reclaimed", paths, "done", NOW - DAY * 2);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
		manager = new WorkLaneManager({
			database: f.database,
			port: f.sessionPort,
			lanes: f.governor,
			gajaewayHome: f.laneRoot,
			serviceDirectory: f.serviceDirectory,
			deliverFallback: () => {},
			now: () => NOW,
			brokerWait: { deadlineMs: 0 },
		});
		await expect(manager.status({ name: "reclaimed" })).rejects.toBeInstanceOf(ProtocolError);
		await expect(manager.status({ name: "reclaimed" })).rejects.toMatchObject({
			detail: { reasonCode: "lane_reclaimed" },
		});
		await expect(manager.start({ name: "reclaimed", text: "resume", resume: true })).rejects.toMatchObject({
			detail: { reasonCode: "lane_reclaimed" },
		});
		expect(
			parseConfigFile({
				schemaVersion: 1,
				work: {
					doneReclaimGraceMs: DAY,
					attemptEndedReclaimAfterMs: DAY * 7,
					lowDiskFreeBytes: 1024,
					lowDiskFreeRatio: 0.25,
				},
			}).work,
		).toEqual({
			doneReclaimGraceMs: DAY,
			attemptEndedReclaimAfterMs: DAY * 7,
			lowDiskFreeBytes: 1024,
			lowDiskFreeRatio: 0.25,
		});
	} finally {
		await manager?.stop();
		await f.close();
	}
});

test("regression: stale git worktree registration after external deletion is pruned before reclaim", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "stale-registration");
		persistJob(f.database, "stale-registration", paths, "done", NOW - DAY - 1);
		// Simulate external deletion of the worktree directory
		await rm(paths.worktreePath, { recursive: true });
		// Verify it's still registered in git
		const listBefore = Bun.spawnSync(["git", "worktree", "list", "--porcelain"], {
			cwd: paths.repositoryPath,
			stdout: "pipe",
		}).stdout.toString();
		expect(listBefore).toContain(paths.worktreePath);
		// Reclaim should handle the stale registration by pruning
		const reclaimed = await f.governor.reclaimTerminalWorktrees(NOW);
		expect(reclaimed).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
		// Verify the registration is gone after reclaim
		const listAfter = Bun.spawnSync(["git", "worktree", "list", "--porcelain"], {
			cwd: paths.repositoryPath,
			stdout: "pipe",
		}).stdout.toString();
		expect(listAfter).not.toContain(paths.worktreePath);
		const job = savedJob(f.database, "stale-registration");
		expect(job).toMatchObject({ state: "done", reclaimedAt: expect.any(String) });
	} finally {
		await f.close();
	}
});

test("regression: reclaim requires remote reachability to verify pushed HEAD (issue #833, finding #2)", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "remote-unreachable");
		// Create and push a commit
		await addCommit(paths.worktreePath, "test commit");
		await pushHead(paths);
		persistJob(f.database, "remote-unreachable", paths, "attempt_ended", NOW - DAY * 7);
		// Point remote URL to a non-existent location to simulate unreachable remote
		Bun.spawnSync(["git", "remote", "set-url", "origin", "https://invalid.example.test/repo.git"], {
			cwd: paths.repositoryPath,
			stdout: "pipe",
		});
		// Reclaim should fail when remote is unreachable, fail closed (with 10s timeout)
		const reclaimed = await f.governor.reclaimTerminalWorktrees(NOW);
		expect(reclaimed).toBe(0);
		expect(existsSync(paths.worktreePath)).toBe(true);
		expect(f.logs).toContain("lane_reclaim_skipped name=remote-unreachable reason=remote_unreachable");
	} finally {
		await f.close();
	}
});

test("regression: reclaim detects when HEAD was not pushed to current remote state", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "stale-ref");
		// Create and push a commit on the lane branch
		await addCommit(paths.worktreePath, "test commit");
		await pushHead(paths);
		// Now delete the upstream branch to simulate it being force-deleted or moved
		const upstreamPath = Bun.spawnSync(["git", "config", "remote.origin.url"], {
			cwd: paths.repositoryPath,
			stdout: "pipe",
		})
			.stdout.toString()
			.trim();
		// Delete the branch from upstream
		Bun.spawnSync(["git", "-C", upstreamPath, "update-ref", "-d", `refs/heads/${paths.branch}`], { stdout: "pipe" });
		persistJob(f.database, "stale-ref", paths, "attempt_ended", NOW - DAY * 7);
		// Fail closed: the worktree stays because its work never landed on a base.
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(0);
		expect(existsSync(paths.worktreePath)).toBe(true);
		expect(f.logs).toContain("lane_reclaim_skipped name=stale-ref reason=work_not_merged_into_base");
	} finally {
		await f.close();
	}
});

test("squash-merged lane is reclaimed against a base that moved on (issue #833)", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "true-squash-merged");
		setUpstreamDefaultBranch(paths);
		// Real file changes on the lane: two commits pushed to the lane branch.
		await addCommit(paths.worktreePath, "squashed work part one");
		await addCommit(paths.worktreePath, "squashed work part two");
		await pushHead(paths);
		// The base moves on with unrelated work BEFORE the squash lands.
		await advanceBaseWithUnrelatedCommit(paths);
		// GitHub-style squash-merge onto the base; the lane branch is deleted after.
		squashMergeLaneToBase(paths);
		persistJob(f.database, "true-squash-merged", paths, "attempt_ended", NOW - DAY * 7);
		// Reclaimed via rule 3: the combined lane diff's patch-id equals the
		// single squashed commit's patch-id on the base.
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
		const job = savedJob(f.database, "true-squash-merged");
		expect(job).toMatchObject({ state: "attempt_ended", reclaimedAt: expect.any(String) });
	} finally {
		await f.close();
	}
});

test("two-commit lane rebase-merged onto a moved base is reclaimed via git cherry (issue #834)", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "rebase-merged");
		setUpstreamDefaultBranch(paths);
		await addCommit(paths.worktreePath, "rebase first");
		await addCommit(paths.worktreePath, "rebase second");
		await pushHead(paths);
		await advanceBaseWithUnrelatedCommit(paths);
		await rebaseMergeLaneToBase(paths);
		persistJob(f.database, "rebase-merged", paths, "attempt_ended", NOW - DAY * 7);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
	} finally {
		await f.close();
	}
});

test("squash-merged lane created from dev is reclaimed against dev while main stays default (issue #834)", async () => {
	const f = await fixture();
	try {
		// gajae-way topology: the upstream's default branch is `main`, but this
		// lane was created from `dev` and its work lands on `dev`.
		const paths = await managedWorktree(f, "dev-squash-merged", { baseBranch: "dev" });
		const upstream = upstreamOf(paths);
		// `dev` exists (the lane branched from it); point the default-branch
		// symbol at a separate `main` that never receives the lane's work.
		git(["branch", "main", "refs/heads/dev"], upstream);
		git(["symbolic-ref", "HEAD", "refs/heads/main"], upstream);
		// The clone recorded `origin/HEAD -> origin/dev` (dev was the only branch
		// at clone time); repoint the remote's default-branch symbol at `main`.
		git(["symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/main"], paths.repositoryPath);
		await addCommit(paths.worktreePath, "dev lane work");
		await pushHead(paths);
		await advanceBaseWithUnrelatedCommit(paths, "dev");
		squashMergeLaneToBase(paths, "dev");
		// Default branch (`main`) never advances past the dev fork point.
		git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], paths.repositoryPath);
		expect(git(["rev-parse", "refs/remotes/origin/main"], paths.repositoryPath).trim()).toBe(paths.baselineSha);
		persistJob(f.database, "dev-squash-merged", paths, "attempt_ended", NOW - DAY * 7);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
		expect(savedJob(f.database, "dev-squash-merged").reclaimedAt).toBeDefined();
	} finally {
		await f.close();
	}
});

test("lane with real unmerged changes while the base moved on is NOT reclaimed (issue #833)", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "unmerged-while-base-moves");
		setUpstreamDefaultBranch(paths);
		await addCommit(paths.worktreePath, "real unmerged work");
		await pushHead(paths);
		// The base advances with unrelated commits; the lane's work never lands there.
		await advanceBaseWithUnrelatedCommit(paths);
		persistJob(f.database, "unmerged-while-base-moves", paths, "attempt_ended", NOW - DAY * 7);
		// Fail closed: pushed but not merged into the integration base.
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(0);
		expect(existsSync(paths.worktreePath)).toBe(true);
		expect(savedJob(f.database, "unmerged-while-base-moves").reclaimedAt).toBeUndefined();
		expect(f.logs).toContain("lane_reclaim_skipped name=unmerged-while-base-moves reason=work_not_merged_into_base");
	} finally {
		await f.close();
	}
});

test("lane whose head is an ancestor of the base is reclaimed (regular merge, issue #833)", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "regular-merged");
		setUpstreamDefaultBranch(paths);
		await addCommit(paths.worktreePath, "feature landed by merge");
		await pushHead(paths);
		// Merge the lane into the base with a real merge commit; HEAD of the lane
		// becomes an ancestor of the base tip.
		const upstream = upstreamOf(paths);
		const tempDir = await mkdtemp(join(tmpdir(), "temp-regular-merge-"));
		try {
			git(["clone", "--quiet", "--shared", upstream, tempDir]);
			git(["config", "user.name", "Lane Reclaimer Test"], tempDir);
			git(["config", "user.email", "lane-reclaimer@example.test"], tempDir);
			git(["fetch", "origin", "+refs/heads/*:refs/remotes/origin/*"], tempDir);
			git(
				[
					"merge",
					"--quiet",
					"--no-ff",
					"-m",
					`Merge ${paths.branch} into the integration base`,
					`origin/${paths.branch}`,
				],
				tempDir,
			);
			git(["push", "--quiet", "origin", "HEAD:refs/heads/main"], tempDir);
		} finally {
			await rm(tempDir, { recursive: true, force: true });
		}
		persistJob(f.database, "regular-merged", paths, "attempt_ended", NOW - DAY * 7);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
		expect(savedJob(f.database, "regular-merged").reclaimedAt).toBeDefined();
	} finally {
		await f.close();
	}
});

test("packed-refs base ref is resolved and a no-op fetch keeps it fresh (issue #833)", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "packed-base");
		setUpstreamDefaultBranch(paths);
		await addCommit(paths.worktreePath, "packed-refs lane work");
		await pushHead(paths);
		// Land the lane work on the integration base before packing refs, so the
		// only remaining merged-signal evidence is the packed base ref.
		await advanceBaseWithUnrelatedCommit(paths);
		squashMergeLaneToBase(paths);
		// Pack every ref in the lane repository (origin-fresh/HEAD included): the
		// merged signal must resolve the base through packed-refs, not ref files.
		git(["pack-refs", "--all"], paths.repositoryPath);
		expect(existsSync(join(paths.repositoryPath, "refs", "remotes", "origin-fresh", "HEAD"))).toBe(false);
		persistJob(f.database, "packed-base", paths, "attempt_ended", NOW - DAY * 7);
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(1);
		expect(existsSync(paths.worktreePath)).toBe(false);
	} finally {
		await f.close();
	}
});

test("dirty lane prevents reclaim even if work is merged (issue #833)", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "dirty-merged");
		setUpstreamDefaultBranch(paths);
		await addCommit(paths.worktreePath, "squashed work");
		await pushHead(paths);
		await advanceBaseWithUnrelatedCommit(paths);
		squashMergeLaneToBase(paths);
		// Add untracked files to dirty the worktree
		await writeFile(join(paths.worktreePath, "untracked.txt"), "dirty content\n");
		persistJob(f.database, "dirty-merged", paths, "attempt_ended", NOW - DAY * 7);
		// Reclaim should fail because worktree is dirty
		expect(await f.governor.reclaimTerminalWorktrees(NOW)).toBe(0);
		expect(existsSync(paths.worktreePath)).toBe(true);
		expect(f.logs).toContain("lane_reclaim_skipped name=dirty-merged reason=dirty");
	} finally {
		await f.close();
	}
});

test("work.start rejects reclaimed lane even inside lock (race with reclamation)", async () => {
	const f = await fixture();
	try {
		const paths = await managedWorktree(f, "race-reclaimed");
		// Create and push a commit to have a valid lane
		await addCommit(paths.worktreePath, "some work");
		await pushHead(paths);
		// Create a lane job record with reclaimedAt set
		const rec = persistJob(f.database, "race-reclaimed", paths, "attempt_ended", NOW - DAY * 7);
		const reclaimedRecord = { ...rec, reclaimedAt: new Date(NOW).toISOString() };
		const identity = laneJobIdentity("race-reclaimed");
		f.database.transaction((tx) =>
			f.database.work.putLaneJob(tx, {
				jobId: identity.jobId,
				laneKey: identity.laneKey,
				state: reclaimedRecord.state,
				createdAt: reclaimedRecord.createdAt,
				updatedAt: reclaimedRecord.updatedAt,
				lane: reclaimedRecord.lane,
				json: JSON.stringify(reclaimedRecord),
			}),
		);
		// Now create a WorkLaneManager and try to start the reclaimed lane
		const manager = new WorkLaneManager({
			database: f.database,
			port: f.sessionPort,
			lanes: f.governor,
			gajaewayHome: f.laneRoot,
			serviceDirectory: f.serviceDirectory,
			deliverFallback: () => {},
			now: () => NOW,
			brokerWait: { deadlineMs: 0 },
		});
		// Attempt to start the reclaimed lane should fail
		await expect(manager.start({ name: "race-reclaimed", text: "work", repo: paths.upstream })).rejects.toMatchObject({
			detail: { reasonCode: "lane_reclaimed" },
		});
	} finally {
		await f.close();
	}
});
