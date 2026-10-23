import { afterEach, expect, spyOn, test } from "bun:test";
import * as fsPromises from "node:fs/promises";
import { chmod, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryClosureQueue } from "../src/memory/closure";
import { memoryGit, memoryRoot } from "../src/memory/doctrine";
import { Store } from "../src/store";

// A memory git result must depend neither on Bun reporting git's exit nor on git's
// output reaching end-of-file. Under Bun 1.4.0 a gateway e2e teardown hung in the
// memory drain behind a `git init` that had exited but whose exit (or, in other
// runs, output end-of-file) never reached the event loop. A helper inheriting git's
// output (here a background `sleep`) holds a pipe open past git's exit the same way.

const originalPath = process.env.PATH;
let home = "";
let database: Store | undefined;
// Restored here as well as in each test: a test that times out never reaches its finally.
const spies: Array<{ mockRestore(): void }> = [];

afterEach(async () => {
	for (const spy of spies.splice(0)) spy.mockRestore();
	if (originalPath === undefined) delete process.env.PATH;
	else process.env.PATH = originalPath;
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
