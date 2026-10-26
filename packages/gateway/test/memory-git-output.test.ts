import { afterEach, expect, spyOn, test } from "bun:test";
import * as fsPromises from "node:fs/promises";
import { chmod, mkdir, mkdtemp, readdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryClosureQueue } from "../src/memory/closure";
import * as doctrine from "../src/memory/doctrine";
import { MEMORY_GIT_TIMEOUT_MS, MemoryGitTimeoutError, memoryGit, memoryRoot } from "../src/memory/doctrine";
import { Store } from "../src/store";
import { untilGone } from "./fixtures/git-hang";

// A memory git result must depend neither on Bun reporting git's exit nor on git's
// output reaching end-of-file. Under Bun 1.4.0 a gateway e2e teardown hung in the
// memory drain behind a `git init` that had exited but whose exit (or, in other
// runs, output end-of-file) never reached the event loop. A helper inheriting git's
// output (here a background `sleep`) holds a pipe open past git's exit the same way.

const originalPath = process.env.PATH;
const originalTmpdir = process.env.TMPDIR;
let home = "";
let database: Store | undefined;
// Restored here as well as in each test: a test that times out never reaches its finally.
const spies: Array<{ mockRestore(): void }> = [];

afterEach(async () => {
	for (const spy of spies.splice(0)) spy.mockRestore();
	if (originalPath === undefined) delete process.env.PATH;
	else process.env.PATH = originalPath;
	if (originalTmpdir === undefined) delete process.env.TMPDIR;
	else process.env.TMPDIR = originalTmpdir;
	database?.close();
	database = undefined;
	if (home) {
		const pids = await readFile(join(home, "holders"), "utf8").catch(() => "");
		for (const pid of pids.split("\n").filter(Boolean)) {
			try {
				process.kill(Number(pid), "SIGKILL");
			} catch {
				// Already gone.
			}
		}
		await rm(home, { recursive: true, force: true });
	}
	home = "";
});

/** Every git command run through PATH leaves a 30s holder on its stdout and stderr. */
async function holdGitOutputOpen(): Promise<void> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	const realGit = Bun.which("git");
	if (!realGit) throw new Error("git is required");
	const bin = join(home, "bin");
	await mkdir(bin);
	await writeFile(
		join(bin, "git"),
		`#!/bin/sh\nsleep 30 &\necho $! >> "${join(home, "holders")}"\nexec "${realGit}" "$@"\n`,
	);
	await chmod(join(bin, "git"), 0o755);
	process.env.PATH = `${bin}:${originalPath ?? "/usr/bin:/bin"}`;
}

test("a git result is returned at git's exit even while a helper holds its output open", async () => {
	await holdGitOutputOpen();
	const root = join(home, "repo");
	await mkdir(root);
	const started = performance.now();
	await memoryGit(root, ["init"]);
	expect(await memoryGit(root, ["rev-parse", "--is-inside-work-tree"])).toBe("true");
	// Output that is not a success still carries git's diagnostic.
	await expect(memoryGit(root, ["log", "-1"])).rejects.toThrow("does not have any commits yet");
	// Bounded by git itself, not by the 30s holders.
	expect(performance.now() - started).toBeLessThan(10_000);
}, 20_000);

test("a capture closes and shutdown drains while every git helper holds its output open", async () => {
	await holdGitOutputOpen();
	database = await Store.open(join(home, "gateway.db"));
	const queue = new MemoryClosureQueue(database, home);
	const errors = spyOn(console, "error");
	spies.push(errors);
	try {
		const started = performance.now();
		await queue.initialize();
		const id = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "keep me", replyText: "noted" });
		await queue.drain();
		expect(performance.now() - started).toBeLessThan(10_000);
		expect(queue.failures).toBe(0);
		expect(errors).not.toHaveBeenCalled();
		expect(database.memory.get(id)).toMatchObject({ state: "receipted", attempts: 1 });
		const commits = await memoryGit(memoryRoot(home), ["log", "--format=%H", "--grep", `Gajaeway-Mutation-Id: ${id}`]);
		expect(commits.split("\n").filter(Boolean)).toHaveLength(1);
	} finally {
		errors.mockRestore();
	}
}, 20_000);

// The state a hung teardown showed under Bun 1.4.0: git has exited (a zombie), but
// Bun never reports it, so `exited` never settles and `exitCode` stays null.
function spawnWithLostExit() {
	const realSpawn = Bun.spawn;
	const spy = spyOn(Bun, "spawn").mockImplementation(((...args: Parameters<typeof Bun.spawn>) => {
		const child = realSpawn(...args);
		return new Proxy(child, {
			get(target, property) {
				if (property === "exited") return new Promise<number>(() => {});
				if (property === "exitCode") return null;
				const value = Reflect.get(target, property, target);
				return typeof value === "function" ? value.bind(target) : value;
			},
		});
	}) as typeof Bun.spawn);
	spies.push(spy);
	return spy;
}

test("git that exits while Bun never reports the exit still returns its result", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	const root = join(home, "repo");
	await mkdir(root);
	const spawn = spawnWithLostExit();
	try {
		await memoryGit(root, ["init"]);
		expect(await memoryGit(root, ["rev-parse", "--is-inside-work-tree"])).toBe("true");
		await expect(memoryGit(root, ["log", "-1"])).rejects.toThrow("does not have any commits yet");
	} finally {
		spawn.mockRestore();
	}
}, 20_000);

test("a capture closes and the queue drains while Bun never reports git's exit", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	database = await Store.open(join(home, "gateway.db"));
	const queue = new MemoryClosureQueue(database, home);
	const spawn = spawnWithLostExit();
	let id: string;
	try {
		await queue.initialize();
		id = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "keep me", replyText: "noted" });
		await queue.drain();
	} finally {
		spawn.mockRestore();
	}
	expect(queue.failures).toBe(0);
	expect(database.memory.get(id)).toMatchObject({ state: "receipted", attempts: 1 });
	const commits = await memoryGit(memoryRoot(home), ["log", "--format=%H", "--grep", `Gajaeway-Mutation-Id: ${id}`]);
	expect(commits.split("\n").filter(Boolean)).toHaveLength(1);
}, 20_000);

test("a shell killed before it records git's status fails with the shell's exit", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	const bin = join(home, "bin");
	await mkdir(bin);
	await writeFile(join(bin, "git"), "#!/bin/sh\nkill -9 $PPID\nexit 0\n");
	await chmod(join(bin, "git"), 0o755);
	process.env.PATH = `${bin}:${originalPath ?? "/usr/bin:/bin"}`;
	await expect(memoryGit(home, ["status"])).rejects.toThrow("memory git status failed (exit 137");
});

test("an output file that cannot be read fails the command instead of reading as empty", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	await memoryGit(home, ["init"]);
	const realReadFile = fsPromises.readFile;
	const broken = Object.assign(new Error("EIO: i/o error, read"), { code: "EIO" });
	const spy = spyOn(fsPromises, "readFile").mockImplementation((async (...args: Parameters<typeof realReadFile>) => {
		if (String(args[0]).endsWith("/stdout")) throw broken;
		return realReadFile(...args);
	}) as typeof realReadFile);
	spies.push(spy);
	try {
		// A successful command whose output is unreadable is not an empty result.
		await expect(memoryGit(home, ["rev-parse", "--git-dir"])).rejects.toBe(broken);
	} finally {
		spy.mockRestore();
	}
	expect(await memoryGit(home, ["rev-parse", "--git-dir"])).toBe(".git");
});

/**
 * A git stand-in on PATH whose matching subcommand never exits: it starts a helper
 * (`sleep`) in the background, records both pids in `holders`, and then sleeps
 * itself, the way a stuck hook, filter or credential helper holds git. With
 * `onceMarker`, only the first matching call hangs; later ones run real git.
 */
async function installStuckGit(subcommand: string, onceMarker?: string): Promise<void> {
	const realGit = Bun.which("git");
	if (!realGit) throw new Error("git is required");
	const bin = join(home, "bin");
	await mkdir(bin, { recursive: true });
	const holders = join(home, "holders");
	const once = onceMarker ? `[ -e "${onceMarker}" ] && exec "${realGit}" "$@"; : > "${onceMarker}"; ` : "";
	await writeFile(
		join(bin, "git"),
		`#!/bin/sh\nif [ "$1" = "${subcommand}" ]; then ${once}sleep 60 & echo $! >> "${holders}"; echo $$ >> "${holders}"; exec sleep 60; fi\nexec "${realGit}" "$@"\n`,
	);
	await chmod(join(bin, "git"), 0o755);
	// The first exec of a fresh script can be slow (macOS scans it); keep that out of the deadline.
	if (Bun.spawnSync([join(bin, "git"), "--version"]).exitCode !== 0) throw new Error("git stand-in does not run");
	process.env.PATH = `${bin}:${originalPath ?? "/usr/bin:/bin"}`;
}

/** Short enough for a test, long enough that a healthy git is never what it cuts off. */
const TEST_DEADLINE_MS = 2_000;

async function holderPids(): Promise<number[]> {
	const text = await readFile(join(home, "holders"), "utf8").catch(() => "");
	return text.split("\n").filter(Boolean).map(Number);
}

/** Memory git calls made by the closure queue get `timeoutMs` instead of the production deadline. */
function shortenClosureGitDeadline(timeoutMs: number) {
	const real = doctrine.memoryGit;
	const spy = spyOn(doctrine, "memoryGit").mockImplementation(((root: string, args: readonly string[]) =>
		real(root, args, timeoutMs)) as typeof real);
	spies.push(spy);
	return spy;
}

test("the production deadline is the work lane's local git budget", () => {
	expect(MEMORY_GIT_TIMEOUT_MS).toBe(120_000);
});

test("a git that never exits is killed with its helpers at the deadline and fails typed, leaving no scratch dir", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	const scratchParent = join(home, "tmp");
	await mkdir(scratchParent);
	process.env.TMPDIR = scratchParent;
	await installStuckGit("status");
	const started = performance.now();
	const failure = await memoryGit(home, ["status", "--porcelain"], TEST_DEADLINE_MS).then(
		() => undefined,
		(error: unknown) => error,
	);
	const elapsed = performance.now() - started;
	expect(failure).toBeInstanceOf(MemoryGitTimeoutError);
	if (!(failure instanceof MemoryGitTimeoutError)) throw new Error("expected a typed memory git timeout");
	expect(failure.message).toBe(
		`memory git status timed out after ${TEST_DEADLINE_MS}ms (argv: git status --porcelain); its process group was killed`,
	);
	expect(failure).toMatchObject({ command: "status", timeoutMs: TEST_DEADLINE_MS });
	expect(elapsed).toBeGreaterThanOrEqual(TEST_DEADLINE_MS - 10);
	expect(elapsed).toBeLessThan(TEST_DEADLINE_MS + 5_000);
	// Both the stuck git and the helper it started went down with the group.
	const pids = await holderPids();
	expect(pids).toHaveLength(2);
	for (const pid of pids) expect(await untilGone(pid)).toBe(true);
	expect(await readdir(scratchParent)).toEqual([]);
	// The next command is unaffected.
	if (originalPath === undefined) delete process.env.PATH;
	else process.env.PATH = originalPath;
	await memoryGit(home, ["init"], 5_000);
	expect(await memoryGit(home, ["rev-parse", "--git-dir"], 5_000)).toBe(".git");
}, 20_000);

test("a stuck git fails only its own intent: the FIFO moves on to the next one", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	database = await Store.open(join(home, "gateway.db"));
	const queue = new MemoryClosureQueue(database, home);
	await queue.initialize();
	// The closure's first git call for an intent is its `log --grep` commit lookup; only the first one hangs.
	await installStuckGit("log", join(home, "hung-once"));
	shortenClosureGitDeadline(TEST_DEADLINE_MS);
	const errors = spyOn(console, "error").mockImplementation(() => {});
	spies.push(errors);
	const stuck = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "stuck", replyText: "a" });
	const next = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "next", replyText: "b" });
	const started = performance.now();
	await queue.drain();
	expect(performance.now() - started).toBeLessThan(TEST_DEADLINE_MS + 5_000);
	expect(queue.failures).toBe(1);
	const logged = errors.mock.calls.map(([line]) => String(line)).join("\n");
	expect(logged).toContain(`memory intent ${stuck} failed; left for boot recovery: MemoryGitTimeoutError`);
	expect(database.memory.get(stuck)).toMatchObject({ state: "written", attempts: 1, quarantine_reason: null });
	expect(database.memory.get(next)).toMatchObject({ state: "receipted", attempts: 1 });
	const commits = await memoryGit(memoryRoot(home), ["log", "--format=%H", "--grep", `Gajaeway-Mutation-Id: ${next}`]);
	expect(commits.split("\n").filter(Boolean)).toHaveLength(1);
	const pids = await holderPids();
	expect(pids).toHaveLength(2);
	for (const pid of pids) expect(await untilGone(pid)).toBe(true);
}, 20_000);

test("boot recovery leaves an intent open, not quarantined, when its git times out", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	database = await Store.open(join(home, "gateway.db"));
	const first = new MemoryClosureQueue(database, home);
	await first.initialize();
	await installStuckGit("log");
	shortenClosureGitDeadline(TEST_DEADLINE_MS);
	const errors = spyOn(console, "error").mockImplementation(() => {});
	spies.push(errors);
	const id = first.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "recover me", replyText: "c" });
	await first.drain();
	expect(database.memory.get(id)).toMatchObject({ state: "written", attempts: 1 });

	// A restart whose recovery hits the same stuck git.
	const rebooted = new MemoryClosureQueue(database, home);
	const report = await rebooted.initialize();
	expect(report.quarantined).toBe(0);
	expect(rebooted.failures).toBe(1);
	expect(errors.mock.calls.map(([line]) => String(line)).join("\n")).toContain(
		`memory intent ${id} failed; left for boot recovery: MemoryGitTimeoutError`,
	);
	expect(database.memory.get(id)).toMatchObject({ state: "written", attempts: 2, quarantine_reason: null });
	const pids = await holderPids();
	expect(pids).toHaveLength(4);
	for (const pid of pids) expect(await untilGone(pid)).toBe(true);

	// The next boot with a healthy git closes it.
	if (originalPath === undefined) delete process.env.PATH;
	else process.env.PATH = originalPath;
	const healthy = new MemoryClosureQueue(database, home);
	await healthy.initialize();
	expect(database.memory.get(id)).toMatchObject({ state: "receipted", attempts: 3 });
}, 20_000);

test("a status that cannot be read while git still runs kills git's group before failing", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	await installStuckGit("status");
	const realReadFile = fsPromises.readFile;
	const broken = Object.assign(new Error("EIO: i/o error, read"), { code: "EIO" });
	const spy = spyOn(fsPromises, "readFile").mockImplementation((async (...args: Parameters<typeof realReadFile>) => {
		// Fail once git is surely running: the stand-in has recorded both pids.
		if (String(args[0]).endsWith("/status") && (await holderPids()).length === 2) throw broken;
		return realReadFile(...args);
	}) as typeof realReadFile);
	spies.push(spy);
	const started = performance.now();
	await expect(memoryGit(home, ["status"], 60_000)).rejects.toBe(broken);
	spy.mockRestore();
	expect(performance.now() - started).toBeLessThan(10_000);
	const pids = await holderPids();
	expect(pids).toHaveLength(2);
	for (const pid of pids) expect(await untilGone(pid)).toBe(true);
}, 20_000);

test("a git init killed at its deadline fails initialization, and the half-made .git is repaired next time", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	const root = memoryRoot(home);
	// The stand-in's init leaves what a killed init can: a `.git` without HEAD, objects or refs.
	await installStuckGit("init");
	await writeFile(
		join(home, "bin", "git"),
		(await readFile(join(home, "bin", "git"), "utf8")).replace(
			"then sleep 60 &",
			"then mkdir -p .git/info; sleep 60 &",
		),
	);
	const real = doctrine.memoryGit;
	const spy = spyOn(doctrine, "memoryGit").mockImplementation(((dir: string, args: readonly string[]) =>
		real(dir, args, TEST_DEADLINE_MS)) as typeof real);
	spies.push(spy);
	await expect(doctrine.initializeMemory(home)).rejects.toBeInstanceOf(MemoryGitTimeoutError);
	spy.mockRestore();
	expect((await readdir(join(root, ".git"))).sort()).toEqual(["info"]);
	for (const pid of await holderPids()) expect(await untilGone(pid)).toBe(true);

	if (originalPath === undefined) delete process.env.PATH;
	else process.env.PATH = originalPath;
	database = await Store.open(join(home, "gateway.db"));
	const queue = new MemoryClosureQueue(database, home);
	await queue.initialize();
	expect(await memoryGit(root, ["rev-parse", "--git-dir"])).toBe(".git");
	const id = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "after repair", replyText: "d" });
	await queue.drain();
	expect(queue.failures).toBe(0);
	expect(database.memory.get(id)).toMatchObject({ state: "receipted", attempts: 1 });
}, 20_000);

test("a gateway that exits mid-command takes the memory git group down with it", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	await installStuckGit("status");
	const script = join(home, "exit-mid-command.ts");
	await writeFile(
		script,
		`import { memoryGit } from ${JSON.stringify(join(import.meta.dir, "../src/memory/doctrine"))};
import { existsSync, readFileSync } from "node:fs";
void memoryGit(${JSON.stringify(home)}, ["status"], 60_000).catch(() => {});
const holders = ${JSON.stringify(join(home, "holders"))};
while (!existsSync(holders) || readFileSync(holders, "utf8").split("\\n").filter(Boolean).length < 2) await Bun.sleep(5);
// The shutdown deadline's exit: no deadline timer, no finally runs after this.
process.exit(0);
`,
	);
	const parent = Bun.spawn([process.execPath, script], { env: process.env, stdout: "pipe", stderr: "pipe" });
	expect(await parent.exited).toBe(0);
	const pids = await holderPids();
	expect(pids).toHaveLength(2);
	for (const pid of pids) expect(await untilGone(pid)).toBe(true);
}, 20_000);

test("a shell killed while git and its helper live on takes them down before the call fails", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	const bin = join(home, "bin");
	await mkdir(bin);
	const holders = join(home, "holders");
	await writeFile(
		join(bin, "git"),
		`#!/bin/sh\nsleep 60 &\necho $! >> "${holders}"\necho $$ >> "${holders}"\nkill -9 $PPID\nexec sleep 60\n`,
	);
	await chmod(join(bin, "git"), 0o755);
	process.env.PATH = `${bin}:${originalPath ?? "/usr/bin:/bin"}`;
	await expect(memoryGit(home, ["status"], 60_000)).rejects.toThrow("memory git status failed (exit 137");
	const pids = await holderPids();
	expect(pids).toHaveLength(2);
	for (const pid of pids) expect(await untilGone(pid)).toBe(true);
}, 20_000);

test("a stuck git whose killed shell's exit Bun never reports still fails within the deadline plus the reap grace", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	await installStuckGit("status");
	const spawn = spawnWithLostExit();
	const started = performance.now();
	await expect(memoryGit(home, ["status"], TEST_DEADLINE_MS)).rejects.toBeInstanceOf(MemoryGitTimeoutError);
	spawn.mockRestore();
	const elapsed = performance.now() - started;
	// Deadline, then at most the 1s reap grace, then bookkeeping.
	expect(elapsed).toBeGreaterThanOrEqual(TEST_DEADLINE_MS + 900);
	expect(elapsed).toBeLessThan(TEST_DEADLINE_MS + 1_000 + 5_000);
	const pids = await holderPids();
	expect(pids).toHaveLength(2);
	for (const pid of pids) expect(await untilGone(pid)).toBe(true);
}, 20_000);

/** The exit cleanup memoryGit registers, run without exiting. */
function runMemoryGitExitCleanup(): void {
	const cleanup = process.listeners("exit").filter((listener) => String(listener).includes("liveMemoryGitGroups"));
	expect(cleanup).toHaveLength(1);
	(cleanup[0] as () => void)();
}

test("exit cleanup never signals a finished command, whose shell pid may already be reused", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	await memoryGit(home, ["init"]);
	const realReadFile = fsPromises.readFile;
	let release!: () => void;
	const outputHeld = new Promise<void>((resolve) => {
		release = resolve;
	});
	let reading!: () => void;
	const outputRead = new Promise<void>((resolve) => {
		reading = resolve;
	});
	const read = spyOn(fsPromises, "readFile").mockImplementation((async (...args: Parameters<typeof realReadFile>) => {
		if (String(args[0]).endsWith("/stdout")) {
			reading();
			await outputHeld;
		}
		return realReadFile(...args);
	}) as typeof realReadFile);
	spies.push(read);
	const kill = spyOn(process, "kill");
	spies.push(kill);
	const result = memoryGit(home, ["rev-parse", "--git-dir"]);
	// git has finished and recorded its status; the call is still reading output.
	await outputRead;
	runMemoryGitExitCleanup();
	expect(kill).not.toHaveBeenCalled();
	release();
	expect(await result).toBe(".git");
});

test("cleanup of a stuck or orphaned group signals the group, never a bare pid", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	await installStuckGit("status");
	const kill = spyOn(process, "kill");
	spies.push(kill);
	const result = memoryGit(home, ["status"], 60_000).catch((error: unknown) => error);
	while ((await holderPids()).length < 2) await Bun.sleep(5);
	runMemoryGitExitCleanup();
	expect(await result).toBeInstanceOf(Error);
	await memoryGit(home, ["status"], TEST_DEADLINE_MS).catch(() => undefined);
	const signalled = kill.mock.calls.filter(([, signal]) => signal === "SIGKILL").map(([pid]) => Number(pid));
	expect(signalled.length).toBeGreaterThanOrEqual(2);
	for (const pid of signalled) expect(pid).toBeLessThan(0);
	for (const pid of await holderPids()) expect(await untilGone(pid)).toBe(true);
}, 20_000);

test("a corpus whose .git is a gitfile is recognized, never re-initialized", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
	const root = memoryRoot(home);
	const gitDir = join(home, "separate.git");
	await mkdir(root, { recursive: true });
	await memoryGit(root, ["init", `--separate-git-dir=${gitDir}`]);
	await memoryGit(root, ["config", "core.filemode", "false"]);
	const real = doctrine.memoryGit;
	const calls: string[][] = [];
	const spy = spyOn(doctrine, "memoryGit").mockImplementation(((
		dir: string,
		args: readonly string[],
		timeoutMs?: number,
	) => {
		calls.push([...args]);
		return real(dir, args, timeoutMs);
	}) as typeof real);
	spies.push(spy);
	await doctrine.initializeMemory(home);
	await doctrine.initializeMemory(home);
	spy.mockRestore();
	expect(calls.filter(([command]) => command === "init")).toEqual([]);
	expect(await memoryGit(root, ["config", "core.filemode"])).toBe("false");
	expect(await memoryGit(root, ["rev-parse", "--git-dir"])).toBe(await realpath(gitDir));
});

for (const [label, content] of [
	["points nowhere", "gitdir: /nonexistent/memory.git\n"],
	["is malformed", "not a gitfile\n"],
] as const) {
	test(`a gitfile that ${label} fails initialization and leaves open intents unquarantined`, async () => {
		home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-output-"));
		const root = memoryRoot(home);
		await mkdir(root, { recursive: true });
		await writeFile(join(root, ".git"), content);
		database = await Store.open(join(home, "gateway.db"));
		const id = crypto.randomUUID();
		const payloadJson = JSON.stringify({
			kind: "daily_capture",
			originRefJson: "{}",
			userText: "kept",
			replyText: "e",
		});
		database.transaction((tx) => database?.memory.createIntent(tx, { id, kind: "daily_capture", payloadJson }));
		const queue = new MemoryClosureQueue(database, home);
		await expect(queue.initialize()).rejects.toThrow("memory git rev-parse failed");
		expect(database.memory.get(id)).toMatchObject({ state: "queued", quarantine_reason: null });
		// Nothing was initialized over it.
		expect(await readFile(join(root, ".git"), "utf8")).toBe(content);
	});
}
