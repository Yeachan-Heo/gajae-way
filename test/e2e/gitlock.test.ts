import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, expect, test } from "bun:test";
import {
	ClosureError,
	createClosureExecutor,
	type ClosureExecutor,
	type ClosureRequest,
} from "../../src/main-session/closure";
import { loadWayCore, type WayCoreHandle } from "../../src/native-loader";

const temporaryDirectories: string[] = [];
const daemonProcesses: ReturnType<typeof Bun.spawn>[] = [];
const holderProcesses: Array<{ child: ChildProcess; pgid: number }> = [];

interface CommandResult {
	readonly exitCode: number;
	readonly stdout: string;
	readonly stderr: string;
}

interface GitFixture {
	readonly root: string;
	readonly corpus: string;
	readonly remote: string;
	readonly state: string;
}

interface Holder {
	readonly child: ChildProcess;
	readonly identity: { pid: number; pidStartTime: string; pgid: number; pgidStartTime?: string };
}

afterEach(async () => {
	for (const process of daemonProcesses.splice(0)) {
		try {
			process.kill("SIGKILL");
		} catch {
			// It is already gone.
		}
		await Promise.race([process.exited, Bun.sleep(1_000)]);
	}
	for (const holder of holderProcesses.splice(0)) {
		try {
			process.kill(-holder.pgid, "SIGKILL");
		} catch {
			// It is already gone.
		}
	}
	for (const directory of temporaryDirectories.splice(0)) fs.rmSync(directory, { force: true, recursive: true });
});

async function command(commandLine: string[], cwd?: string): Promise<CommandResult> {
	const process = Bun.spawn({ cmd: commandLine, ...(cwd ? { cwd } : {}), stdout: "pipe", stderr: "pipe" });
	const [exitCode, stdout, stderr] = await Promise.all([
		process.exited,
		new Response(process.stdout).text(),
		new Response(process.stderr).text(),
	]);
	return { exitCode, stdout, stderr };
}

async function run(commandLine: string[], cwd?: string): Promise<string> {
	const result = await command(commandLine, cwd);
	if (result.exitCode !== 0) {
		throw new Error(`${commandLine.join(" ")} failed (${result.exitCode}): ${result.stderr || result.stdout}`);
	}
	return result.stdout;
}

async function git(corpus: string, args: string[]): Promise<string> {
	return await run(["git", "-C", corpus, ...args]);
}

async function gitResult(corpus: string, args: string[]): Promise<CommandResult> {
	return await command(["git", "-C", corpus, ...args]);
}

async function remoteGit(fixture: GitFixture, args: string[]): Promise<string> {
	return await run(["git", `--git-dir=${fixture.remote}`, ...args]);
}

function fixture(name: string): GitFixture {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), `gajae-way-gitlock-${name}-`));
	temporaryDirectories.push(root);
	return {
		root,
		corpus: path.join(root, "corpus"),
		remote: path.join(root, "remote.git"),
		state: path.join(root, "state"),
	};
}

async function initializedFixture(name: string): Promise<GitFixture> {
	const value = fixture(name);
	await run(["git", "init", "--bare", value.remote]);
	await run(["git", "init", value.corpus]);
	await git(value.corpus, ["config", "user.name", "Git Lock Drill"]);
	await git(value.corpus, ["config", "user.email", "git-lock@example.test"]);
	fs.writeFileSync(path.join(value.corpus, "base.txt"), "base\n");
	await git(value.corpus, ["add", "--", "base.txt"]);
	await git(value.corpus, ["commit", "-m", "base"]);
	await git(value.corpus, ["branch", "-M", "main"]);
	await git(value.corpus, ["remote", "add", "origin", value.remote]);
	await git(value.corpus, ["push", "-u", "origin", "main"]);
	await remoteGit(value, ["symbolic-ref", "HEAD", "refs/heads/main"]);
	return value;
}

function core(fixture: GitFixture, hardHoldCapMs?: number): WayCoreHandle {
	const bindings = loadWayCore();
	return hardHoldCapMs === undefined
		? bindings.WayCore.open(fixture.state)
		: bindings.WayCore.openWithTestHardCap(fixture.state, hardHoldCapMs);
}

function request(fixture: GitFixture, sessionId: string, label: string, file: string, commitMessage: string): ClosureRequest {
	return { sessionId, corpusPath: fixture.corpus, label, paths: [file], commitMessage };
}

async function execute(coreHandle: WayCoreHandle, closure: ClosureRequest): Promise<ReturnType<ClosureExecutor["execute"]>> {
	const executor = createClosureExecutor({ core: coreHandle, heartbeatMs: 100 });
	try {
		return await executor.execute(closure);
	} finally {
		await executor.shutdown();
	}
}

async function eventually<T>(read: () => T | undefined | Promise<T | undefined>, description: string, timeoutMs = 5_000): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	let lastError: unknown;
	while (Date.now() < deadline) {
		try {
			const value = await read();
			if (value !== undefined) return value;
		} catch (error) {
			lastError = error;
		}
		await Bun.sleep(20);
	}
	throw new Error(`${description}${lastError instanceof Error ? `: ${lastError.message}` : ""}`);
}

function nativeErrorCode(error: unknown): number | undefined {
	const match = /(?:^|\s)(120[0-8])\b/.exec(error instanceof Error ? error.message : String(error));
	return match ? Number(match[1]) : undefined;
}

function expectNativeError(action: () => unknown, code: number): void {
	try {
		action();
	} catch (error) {
		expect(nativeErrorCode(error)).toBe(code);
		return;
	}
	throw new Error(`Expected native error ${code}.`);
}

async function spawnHolder(coreHandle: WayCoreHandle): Promise<Holder> {
	const child = spawn("/bin/sh", ["-c", "exec sleep 60"], { detached: true, stdio: "ignore" });
	if (!child.pid) throw new Error("holder did not start");
	const identity = await eventually(() => {
		try {
			return coreHandle.processIdentity(child.pid as number);
		} catch {
			return undefined;
		}
	}, "holder did not publish an incarnation");
	holderProcesses.push({ child, pgid: identity.pgid });
	return { child, identity };
}

async function killHolder(holder: Holder): Promise<void> {
	try {
		process.kill(-holder.identity.pgid, "SIGKILL");
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code !== "ESRCH" && code !== "EPERM") throw error;
	}
	await new Promise<void>(resolve => {
		if (holder.child.exitCode !== null) return resolve();
		holder.child.once("close", () => resolve());
		setTimeout(resolve, 1_000);
	});
}

function acquire(coreHandle: WayCoreHandle, holder: Holder, sessionId: string, label: string) {
	return coreHandle.lockAcquire({
		label,
		waitMs: 0,
		ttlMs: 5_000,
		holder: {
			holderKind: "in_daemon",
			sessionId,
			pid: holder.identity.pid,
			pidStartTime: holder.identity.pidStartTime,
			pgid: holder.identity.pgid,
			...(holder.identity.pgidStartTime ? { pgidStartTime: holder.identity.pgidStartTime } : {}),
			connId: "way.in_daemon_executor.v1",
		},
	});
}

async function remoteSubjects(fixture: GitFixture): Promise<string[]> {
	const output = await remoteGit(fixture, ["log", "--format=%s", "--all"]);
	return output.trim().split("\n").filter(Boolean);
}

test("L1a: daemon SIGKILL reaps the residual closure group and the same session completes its committed push", async () => {
	const value = await initializedFixture("l1a");
	const markerPath = path.join(value.root, "l1a-marker.json");
	fs.writeFileSync(path.join(value.corpus, "l1a.txt"), "l1a\n");
	const closure = request(value, "l1a-session", "l1a", "l1a.txt", "l1a committed once");
	const daemon = Bun.spawn({
		cmd: [process.execPath, path.join(import.meta.dir, "..", "helpers", "gitlock-daemon.ts")],
		env: {
			...process.env,
			GITLOCK_DAEMON_FIXTURE: JSON.stringify({
				stateDir: value.state,
				markerPath,
				request: closure,
				stopAfter: "after_commit",
			}),
		},
		stdout: "ignore",
		stderr: "pipe",
	});
	daemonProcesses.push(daemon);
	const marker = await eventually(() => {
		if (!fs.existsSync(markerPath)) return undefined;
		return JSON.parse(fs.readFileSync(markerPath, "utf8")) as { childPid: number; childPgid: number };
	}, "daemon did not reach post-commit boundary");
	daemon.kill("SIGKILL");
	await daemon.exited;

	const restarted = core(value);
	await eventually(() => (restarted.lockStatus().held ? undefined : true), "restart did not prove and release the residual lease");
	await eventually(() => {
		try {
			restarted.processIdentity(marker.childPid);
			return undefined;
		} catch {
			return true;
		}
	}, "restart did not reap the residual closure process group");
	const recovered = await execute(restarted, closure);
	expect(await recovered).toMatchObject({ committed: false });
	expect((await remoteSubjects(value)).filter(subject => subject === "l1a committed once")).toHaveLength(1);
});

test("L2: expired and provably dead holder is journaled, reaped, and replaced", async () => {
	const value = await initializedFixture("l2");
	const coreHandle = core(value);
	const firstHolder = await spawnHolder(coreHandle);
	const first = acquire(coreHandle, firstHolder, "l2-first", "l2-first");
	await killHolder(firstHolder);
	await Bun.sleep(5_100);
	const successorHolder = await spawnHolder(coreHandle);
	const successor = acquire(coreHandle, successorHolder, "l2-successor", "l2-successor");
	expect(BigInt(successor.fencingToken)).toBeGreaterThan(BigInt(first.fencingToken));
	const events = coreHandle.journalRead("1:0", 100).events;
	expect(events).toContainEqual(
		expect.objectContaining({ kind: "lock_event", payloadJson: expect.stringContaining("proven_death") }),
	);
	coreHandle.lockRelease(successor.leaseId);
	await killHolder(successorHolder);
}, 15_000);

test("L3: expired live holder cannot transfer; force release fences through quarantine until receipt verification", async () => {
	const value = await initializedFixture("l3");
	const coreHandle = core(value);
	const liveHolder = await spawnHolder(coreHandle);
	const lease = acquire(coreHandle, liveHolder, "l3-live", "l3-live");
	await Bun.sleep(5_100);
	const successor = await spawnHolder(coreHandle);
	expectNativeError(() => acquire(coreHandle, successor, "l3-successor", "l3-successor"), 1200);
	expectNativeError(() => coreHandle.lockForceRelease(lease.leaseId, true), 1207);
	coreHandle.lockQuarantineOverride(lease.leaseId, true, true);
	expectNativeError(() => acquire(coreHandle, successor, "l3-fenced", "l3-fenced"), 1208);
	coreHandle.lockClearQuarantine("verified-l3-receipt", true);
	const admitted = acquire(coreHandle, successor, "l3-successor", "l3-successor");
	expect(admitted.leaseId).not.toBe(lease.leaseId);
	coreHandle.lockRelease(admitted.leaseId);
	await killHolder(liveHolder);
	await killHolder(successor);
}, 15_000);

test("L4: PID and process-group incarnation mismatch is proven death without signalling a reused identity", async () => {
	const value = await initializedFixture("l4");
	const coreHandle = core(value);
	const reused = await spawnHolder(coreHandle);
	const mismatched = coreHandle.lockAcquire({
		label: "l4-reused",
		waitMs: 0,
		ttlMs: 5_000,
		holder: {
			holderKind: "in_daemon",
			sessionId: "l4-old-holder",
			pid: reused.identity.pid,
			pidStartTime: `${BigInt(reused.identity.pidStartTime) + 1n}`,
			pgid: reused.identity.pgid,
			pgidStartTime: reused.identity.pgidStartTime ? `${BigInt(reused.identity.pgidStartTime) + 1n}` : `${BigInt(reused.identity.pidStartTime) + 1n}`,
			connId: "way.in_daemon_executor.v1",
		},
	});
	expect(coreHandle.lockForceRelease(mismatched.leaseId, true).released).toBe(true);
	const successor = await spawnHolder(coreHandle);
	const admitted = acquire(coreHandle, successor, "l4-successor", "l4-successor");
	expect(admitted.leaseId).not.toBe(mismatched.leaseId);
	coreHandle.lockRelease(admitted.leaseId);
	await killHolder(reused);
	await killHolder(successor);
});

test("L5: shortened hard-cap revokes, kills, verifies, and releases the in-daemon process group", async () => {
	const value = await initializedFixture("l5");
	const coreHandle = core(value, 250);
	const holder = await spawnHolder(coreHandle);
	const lease = acquire(coreHandle, holder, "l5-holder", "l5-holder");
	await Bun.sleep(350);
	expect(coreHandle.lockFencingValid(lease.leaseId, lease.fencingToken)).toBe(false);
	await eventually(() => (coreHandle.lockStatus().held ? undefined : true), "hard-cap lease did not release");
	await eventually(() => (holder.child.exitCode === null && holder.child.signalCode === null ? undefined : true), "hard-cap did not kill the child group");
	const successor = await spawnHolder(coreHandle);
	const next = acquire(coreHandle, successor, "l5-successor", "l5-successor");
	expect(BigInt(next.fencingToken)).toBeGreaterThan(BigInt(lease.fencingToken));
	coreHandle.lockRelease(next.leaseId);
	await killHolder(successor);
}, 15_000);

test("L6: pre-commit, post-commit, and mid-push kills rerun to exactly one remote commit each", async () => {
	const value = await initializedFixture("l6");
	const coreHandle = core(value);

	fs.writeFileSync(path.join(value.corpus, "pre.txt"), "pre\n");
	const pre = createClosureExecutor({
		core: coreHandle,
		hooks: { after_stage: async context => await context.killChild() },
	});
	await expect(pre.execute(request(value, "l6-pre", "l6-pre", "pre.txt", "l6 pre"))).rejects.toBeInstanceOf(ClosureError);
	await pre.shutdown();
	expect((await gitResult(value.corpus, ["diff", "--cached", "--quiet"])).exitCode).toBe(0);
	expect(await execute(coreHandle, request(value, "l6-pre", "l6-pre-retry", "pre.txt", "l6 pre"))).toMatchObject({ committed: true });

	fs.writeFileSync(path.join(value.corpus, "post.txt"), "post\n");
	const post = createClosureExecutor({
		core: coreHandle,
		hooks: { after_commit: async context => await context.killChild() },
	});
	await expect(post.execute(request(value, "l6-post", "l6-post", "post.txt", "l6 post"))).rejects.toBeInstanceOf(ClosureError);
	await post.shutdown();
	expect(await execute(coreHandle, request(value, "l6-post", "l6-post-retry", "post.txt", "l6 post"))).toMatchObject({ committed: false });

	const pushMarker = path.join(value.root, "mid-push");
	const pushHook = path.join(value.corpus, ".git", "hooks", "pre-push");
	fs.writeFileSync(pushHook, `#!/bin/sh\nprintf ready > "${pushMarker}"\nsleep 30\n`, { mode: 0o755 });
	fs.writeFileSync(path.join(value.corpus, "mid.txt"), "mid\n");
	let killScheduled: Promise<void> | undefined;
	const mid = createClosureExecutor({
		core: coreHandle,
		hooks: {
			after_commit: context => {
				killScheduled = (async () => {
					await eventually(() => (fs.existsSync(pushMarker) ? true : undefined), "push did not reach pre-push hook");
					process.kill(-context.childPgid, "SIGKILL");
				})();
			},
		},
	});
	await expect(mid.execute(request(value, "l6-mid", "l6-mid", "mid.txt", "l6 mid"))).rejects.toBeInstanceOf(ClosureError);
	await killScheduled;
	await mid.shutdown();
	fs.rmSync(pushHook, { force: true });
	expect(await execute(coreHandle, request(value, "l6-mid", "l6-mid-retry", "mid.txt", "l6 mid"))).toMatchObject({ committed: false });
	const subjects = await remoteSubjects(value);
	for (const subject of ["l6 pre", "l6 post", "l6 mid"]) expect(subjects.filter(candidate => candidate === subject)).toHaveLength(1);
}, 20_000);

test("L7: revocation quarantines writes, kills the active group, and a receipt reopens closure writes", async () => {
	const value = await initializedFixture("l7");
	const coreHandle = core(value);
	const marker = path.join(value.root, "l7-pre-push");
	const pushHook = path.join(value.corpus, ".git", "hooks", "pre-push");
	fs.writeFileSync(pushHook, `#!/bin/sh\nprintf ready > "${marker}"\nsleep 30\n`, { mode: 0o755 });
	fs.writeFileSync(path.join(value.corpus, "quarantined.txt"), "quarantined\n");
	const executor = createClosureExecutor({ core: coreHandle, heartbeatMs: 50 });
	const running = executor.execute(request(value, "l7-session", "l7", "quarantined.txt", "l7 quarantined"));
	await eventually(() => (fs.existsSync(marker) ? true : undefined), "closure did not reach a killable push");
	const held = coreHandle.lockStatus().holder;
	if (!held) throw new Error("closure lease was not held at revocation");
	coreHandle.lockQuarantineOverride(held.leaseId, true, true);
	await expect(running).rejects.toBeInstanceOf(ClosureError);
	await executor.shutdown();
	await eventually(() => {
		try {
			coreHandle.processIdentity(held.pid);
			return undefined;
		} catch {
			return true;
		}
	}, "quarantine did not kill the active closure group");
	const fencedHolder = await spawnHolder(coreHandle);
	expectNativeError(() => acquire(coreHandle, fencedHolder, "l7-fenced", "l7-fenced"), 1208);
	coreHandle.lockClearQuarantine("verified-l7-receipt", true);
	fs.rmSync(pushHook, { force: true });
	fs.writeFileSync(path.join(value.corpus, "resumed.txt"), "resumed\n");
	await execute(coreHandle, request(value, "l7-session", "l7-resumed", "resumed.txt", "l7 resumed"));
	expect(await remoteGit(value, ["show", "main:resumed.txt"])).toBe("resumed\n");
	await killHolder(fencedHolder);
});

test("serialization: concurrent in-daemon closures serialize against one bare remote without non-fast-forwards", async () => {
	const value = await initializedFixture("serialization");
	const coreHandle = core(value);
	fs.writeFileSync(path.join(value.corpus, "first.txt"), "first\n");
	fs.writeFileSync(path.join(value.corpus, "second.txt"), "second\n");
	const executor = createClosureExecutor({ core: coreHandle, heartbeatMs: 50 });
	try {
			const results = await Promise.all([
				executor.execute(request(value, "serialize-first", "serialize-first", "first.txt", "serialize first")),
				executor.execute(request(value, "serialize-second", "serialize-second", "second.txt", "serialize second")),
			]);
			expect(results).toHaveLength(2);
		} finally {
			await executor.shutdown();
		}
	const subjects = await remoteSubjects(value);
	expect(subjects).toEqual(expect.arrayContaining(["serialize first", "serialize second"]));
	const commits = (await remoteGit(value, ["log", "--format=%H", "--reverse", "main"])).trim().split("\n");
	expect(commits).toHaveLength(3);
	const parent = (await remoteGit(value, ["show", "-s", "--format=%P", commits[2] as string])).trim();
	expect(parent).toBe(commits[1]);
});

test("working-tree contract: unrelated edits survive while an unrelated dirty index is rejected as 1206", async () => {
	const value = await initializedFixture("working-tree");
	const coreHandle = core(value);
	fs.writeFileSync(path.join(value.corpus, "unrelated.txt"), "leave me unstaged\n");
	fs.writeFileSync(path.join(value.corpus, "closure.txt"), "closure\n");
	await execute(coreHandle, request(value, "working-tree", "working-tree", "closure.txt", "working tree closure"));
	expect(fs.readFileSync(path.join(value.corpus, "unrelated.txt"), "utf8")).toBe("leave me unstaged\n");
	expect((await git(value.corpus, ["status", "--porcelain", "--", "unrelated.txt"])).trim()).toBe("?? unrelated.txt");
	expect((await gitResult(value.corpus, ["diff", "--cached", "--quiet"])).exitCode).toBe(0);

	fs.writeFileSync(path.join(value.corpus, "indexed.txt"), "unrelated staged work\n");
	await git(value.corpus, ["add", "--", "indexed.txt"]);
	fs.writeFileSync(path.join(value.corpus, "blocked.txt"), "must not commit\n");
	const executor = createClosureExecutor({ core: coreHandle });
	try {
			await expect(executor.execute(request(value, "dirty-index", "dirty-index", "blocked.txt", "blocked"))).rejects.toMatchObject({ code: 1206 });
	} finally {
			await executor.shutdown();
	}
	expect((await git(value.corpus, ["diff", "--cached", "--name-only"])).trim()).toBe("indexed.txt");
	await git(value.corpus, ["reset", "--", "indexed.txt"]);
});
