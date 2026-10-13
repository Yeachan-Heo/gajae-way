import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { MemoryClosureQueue } from "../src/memory/closure";
import { MemoryGitLockedError, memoryGit, memoryRoot } from "../src/memory/doctrine";
import { RuntimeCycleProjector } from "../src/ops/cycle";
import { Store } from "../src/store";

let home = "";
let database: Store | undefined;
let queue: MemoryClosureQueue | undefined;

afterEach(async () => {
	database?.close();
	database = undefined;
	queue = undefined;
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

test("a stale git index lock is reported, retained, and leaves an intent retryable", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-lock-"));
	database = await Store.open(join(home, "gateway.db"));
	queue = new MemoryClosureQueue(database, home);
	await queue.initialize();
	const root = memoryRoot(home);
	const lock = join(root, ".git", "index.lock");
	await mkdir(join(root, ".git"), { recursive: true });
	await writeFile(lock, "");
	const old = new Date(Date.now() - 60_000);
	await utimes(lock, old, old);

	const directFailure = await memoryGit(root, ["add", "--all", "."]).then(
		() => undefined,
		(error: unknown) => error,
	);
	expect(directFailure).toBeInstanceOf(MemoryGitLockedError);
	if (!(directFailure instanceof MemoryGitLockedError)) throw new Error("expected typed memory git lock failure");
	expect(directFailure.path).toBe(lock);
	expect(directFailure.ageMs).toBeGreaterThanOrEqual(60_000);

	const errors = spyOn(console, "error").mockImplementation(() => {});
	let id: string;
	try {
		id = queue.enqueue({ kind: "daily_capture", originRefJson: "{}", userText: "retry me", replyText: "later" });
		await queue.drain();
		expect(queue.failures).toBe(1);
		expect(errors.mock.calls.map(([line]) => String(line)).join("\n")).toContain("MemoryGitLockedError");
	} finally {
		errors.mockRestore();
	}

	const pending = database.memory.get(id);
	expect(pending).toMatchObject({ state: "written", attempts: 1, quarantine_reason: null });
	const lockStat = await stat(lock);
	expect(lockStat.isFile()).toBe(true);

	const projector = new RuntimeCycleProjector(database, queue, { memoryRoot: root });
	const blocked = projector.project();
	expect(blocked.gates).toContain("memory_git_index_locked");
	expect(blocked.memoryGitIndexLock?.path).toBe(lock);
	expect(blocked.memoryGitIndexLock?.ageMs).toBeGreaterThanOrEqual(60_000);

	await rm(lock);
	queue.enqueueExistingId(id);
	await queue.drain();
	expect(database.memory.get(id)).toMatchObject({ state: "receipted", attempts: 2 });
	const commits = await memoryGit(root, ["log", "--format=%H", "--grep", `Gajaeway-Mutation-Id: ${id}`]);
	expect(commits.split("\n").filter(Boolean)).toHaveLength(1);
	const recovered = projector.project();
	expect(recovered.gates).not.toContain("memory_git_index_locked");
	expect(recovered.memoryGitIndexLock).toBeNull();
});
