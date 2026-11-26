import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	appendDaily,
	CorpusWriter,
	initializeMemory,
	mapListsAxis,
	memoryGit,
	regenerateMap,
} from "../src/memory/doctrine";
import {
	type AxisDescriptor,
	type AxisRegistry,
	loadRegistry,
	NAVIGATION_SOURCE_MAX_BYTES,
} from "../src/memory/registry";
import { getOrCreatePendingMemoryOp, pruneExpiredMemoryOps } from "../src/server/server";

// A registry holding exactly one axis: createRegistry() would also seed every
// built-in, and this test asserts the byte-for-byte rendering of a single axis.
const singleAxisRegistry = (axis: AxisDescriptor): AxisRegistry => ({
	axes: [axis],
	byPriority: [axis],
	byId: (id) => (id === axis.id ? axis : undefined),
	axisForPath: (path) => (path === axis.root || path.startsWith(`${axis.root}/`) ? axis : undefined),
});

let home = "";
afterEach(async () => {
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

// The memory code recognises git's "does not have any commits" and "nothing to
// commit" by their English text. macOS Homebrew git follows the system language
// even with LANG unset, so git must run in the C locale for those checks to hold.
test("memory git diagnostics stay in the C locale the memory code matches", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-git-locale-"));
	await memoryGit(home, ["init"]);
	await expect(memoryGit(home, ["log", "-1", "--format=%H"])).rejects.toThrow("does not have any commits yet");
	const writer = new CorpusWriter(home);
	expect(await writer.findCommitByTrailer("intent-id: none")).toBeUndefined();
	await writeFile(join(home, "note.md"), "x\n");
	await writer.stageFiles("note.md");
	expect(await writer.commit("first")).toMatch(/^[0-9a-f]{40}$/);
	expect(await writer.commit("nothing staged")).toBeUndefined();
});

test("capture follows the registered root of the capture axis, not a hardcoded daily/", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-capture-root-"));
	const root = join(home, "memory");
	await mkdir(root, { recursive: true, mode: 0o700 });
	await writeFile(
		join(root, "axes.json"),
		`${JSON.stringify({ version: 1, axes: [{ id: "daily", root: "capture" }] })}\n`,
	);
	await initializeMemory(home);

	const path = await appendDaily(root, "{}", "u1", "r1");

	// Written where the registry says, and reachable from the generated map: a
	// hardcoded daily/ would have thrown ENOENT and lost the turn's capture.
	expect(path).toStartWith("capture/");
	expect(await readFile(join(root, path), "utf8")).toContain("- user: u1");
	expect(await readFile(join(root, "MEMORY.md"), "utf8")).toContain(path);
});

test("multi-line reply text cannot forge an entry delimiter or escape its list item", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-doctrine-"));
	const root = await initializeMemory(home);

	const reply = "first line\n\n## 메모리 시스템 현황\n\n- origin: spoofed\n- user: spoofed";
	const path = await appendDaily(root, '{"platform":"discord"}', "line one\nline two", reply);
	const body = await readFile(join(root, path), "utf8");

	// Exactly one entry delimiter: the real timestamp heading.
	const headings = body.split("\n").filter((line) => line.startsWith("## "));
	expect(headings).toHaveLength(1);
	expect(headings[0]).toMatch(/^## \d{4}-\d{2}-\d{2}T/);

	// Every captured field stays on exactly one physical line.
	expect(body.split("\n").filter((line) => line.startsWith("- origin: "))).toHaveLength(1);
	expect(body.split("\n").filter((line) => line.startsWith("- user: "))).toHaveLength(1);
	expect(body.split("\n").filter((line) => line.startsWith("- reply: "))).toHaveLength(1);

	// Content is preserved, with newlines escaped rather than dropped.
	expect(body).toContain("- user: line one\\nline two");
	expect(body).toContain("first line\\n\\n## 메모리 시스템 현황");
});

test("carriage returns are escaped and NUL bytes are stripped", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-doctrine-"));
	const root = await initializeMemory(home);

	const path = await appendDaily(root, "{}", "a\r\nb\rc\u0000d", "reply");
	const body = await readFile(join(root, path), "utf8");

	expect(body).toContain("- user: a\\nb\\ncd");
	expect(body).not.toContain("\u0000");
	expect(body.split("\n").filter((line) => line.startsWith("- user: "))).toHaveLength(1);
});

test("entry count grows by exactly one per capture", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-doctrine-"));
	const root = await initializeMemory(home);

	const path = await appendDaily(root, "{}", "u1", "## fake\n## fake2");
	await appendDaily(root, "{}", "u2", "plain");
	const body = await readFile(join(root, path), "utf8");

	expect(body.split("\n").filter((line) => line.startsWith("## "))).toHaveLength(2);
});

test("the regenerated map reaches nested axis subdirectories", async () => {
	const { mkdtemp } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { mkdir, writeFile, readFile } = await import("node:fs/promises");
	const { initializeMemory, regenerateMap } = await import("../src/memory/doctrine");
	const home = await mkdtemp(join(tmpdir(), "gajaeway-map-"));
	const root = await initializeMemory(home);
	await mkdir(join(root, "daily/2026-08"), { recursive: true });
	await writeFile(join(root, "daily/2026-08/2026-08-26.md"), "# nested\n");
	await writeFile(join(root, "daily/2026-08-26.md"), "# flat\n");
	await regenerateMap(root);
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	expect(map).toContain("daily/2026-08-26.md");
	expect(map).toContain("daily/2026-08-26.md");
});

async function largeMap(withIndex = true): Promise<{
	readonly map: string;
	readonly axes: readonly AxisDescriptor[];
}> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-map-budget-"));
	const root = await initializeMemory(home);
	const directory = join(root, "ops/rules");
	for (let index = 0; index < 201; index++) {
		const name = `entry-${String(index).padStart(3, "0")}-${"x".repeat(180)}.md`;
		await writeFile(join(directory, name), `# entry ${index}\n`);
	}
	if (withIndex) await writeFile(join(directory, "index.md"), "# Rule index\n");
	await regenerateMap(root);
	return { map: await readFile(join(root, "MEMORY.md"), "utf8"), axes: (await loadRegistry(root)).axes };
}

test("a large generated map is trimmed to the shared navigation byte ceiling", async () => {
	const { map } = await largeMap();
	expect(Buffer.byteLength(map, "utf8")).toBeLessThanOrEqual(NAVIGATION_SOURCE_MAX_BYTES);
});

test("a trimmed map retains every registered axis heading", async () => {
	const { map, axes } = await largeMap();
	for (const axis of axes) {
		expect(map).toContain(`## ${axis.id}`);
		expect(mapListsAxis(map, axis)).toBe(true);
	}
});

test("a trimmed axis points to its existing index and states the omitted count", async () => {
	const { map } = await largeMap();
	const section = map.slice(map.indexOf("## ops"), map.indexOf("## reflections"));
	expect(section).toMatch(/_\d+ entries omitted; see \[the index\]\(ops\/rules\/index\.md\)_/);
	// The notice link is the only pointer: the index sorts after the rendered entries.
	expect(section.split("](ops/rules/index.md)")).toHaveLength(2);
});

test("a trimmed axis without an index states the omitted count without inventing a path", async () => {
	const { map } = await largeMap(false);
	const section = map.slice(map.indexOf("## ops"), map.indexOf("## reflections"));
	expect(section).toMatch(/_\d+ entries omitted; no index entry found_/);
	expect(section).not.toContain("ops/rules/index.md");
});

test("every project is mapped when the axis outgrows the recent window, including the alphabetically first", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-map-projects-"));
	const root = await initializeMemory(home);
	await writeFile(join(root, "projects/aie-zeitgeist.md"), "# first\n");
	for (let index = 0; index < 20; index++)
		await writeFile(join(root, `projects/p-${String(index).padStart(2, "0")}.md`), "# p\n");
	await regenerateMap(root);
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	const section = map.slice(map.indexOf("## projects"), map.indexOf("## channels"));
	expect(section).toContain("- [projects/aie-zeitgeist.md](projects/aie-zeitgeist.md)");
	expect(section.match(/^- \[projects\//gm)).toHaveLength(21);
	expect(section).not.toContain("omitted");
});

test("a recent axis over its cap states how many entries the map left out", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-map-recent-cap-"));
	const root = await initializeMemory(home);
	for (let day = 1; day <= 22; day++)
		await writeFile(join(root, `events/2026-10-${String(day).padStart(2, "0")}.md`), "# e\n");
	await regenerateMap(root);
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	const section = map.slice(map.indexOf("## events"), map.indexOf("## tasks"));
	expect(section).toContain("- [events/2026-10-22.md](events/2026-10-22.md)");
	expect(section).not.toContain("events/2026-10-02.md");
	expect(section).toContain("_2 entries omitted; no index entry found_");
});

test("a small map is rendered byte-for-byte, including the index policy marker", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-map-small-"));
	const root = join(home, "memory");
	await mkdir(join(root, "probe"), { recursive: true });
	await writeFile(join(root, "probe/entry.md"), "# entry\n");
	const axis: AxisDescriptor = {
		id: "probe",
		displayName: "Probe",
		root: "probe",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 0,
		orphanPolicy: "any-depth",
		appendOnly: false,
		promotesTo: [],
	};
	await regenerateMap(root, singleAxisRegistry(axis));
	expect(await readFile(join(root, "MEMORY.md"), "utf8")).toBe(
		"# Memory map\n\n<!-- memory-index-policy: 78451ddeef02d503 -->\n\nGenerated pointers; canonical facts live in axis files.\n\n## probe\n\n_Probe_\n\n- [probe/entry.md](probe/entry.md)\n\n",
	);
});

test("map trimming never emits a partial markdown link line", async () => {
	const { map } = await largeMap();
	// Two complete linked forms exist: a bullet and the omission notice's index link.
	for (const line of map.split("\n"))
		if (line.includes("]("))
			expect(line).toMatch(/^(?:- \[[^\]]+\]\([^)]+\)|_\d+ entries omitted; see \[the index\]\([^)]+\)_)$/);
});

test("a failed git operation reports argv, exit status and stdout, never a bare `failed:` (#192)", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-reason-"));
	await initializeMemory(home);
	const root = join(home, "memory");
	// The live crash: `git commit` with nothing staged exits 1 and explains itself
	// on STDOUT, so a stderr-only message was literally `memory git commit failed:`.
	let error: Error | undefined;
	try {
		await memoryGit(root, ["commit", "-m", "nothing staged"]);
	} catch (caught) {
		error = caught as Error;
	}
	expect(error).toBeInstanceOf(Error);
	expect(error?.message).not.toMatch(/failed:\s*$/);
	expect(error?.message).toContain("git commit -m 'nothing staged'");
	expect(error?.message).toContain("exit 1");
	expect(error?.message).toMatch(/nothing (added )?to commit/);
});

test("issue #341: autolinkCorpus and closure intents serialize on the shared FIFO", async () => {
	// The autolink write joins the same queue as intent append, map generation and commit.
	// The intent commit must not absorb edits that autolink has not yet committed.
	home = await mkdtemp(join(tmpdir(), "issue-341-"));
	const root = await initializeMemory(home);
	const { MemoryClosureQueue } = await import("../src/memory/closure");
	const { autolinkCorpus } = await import("../src/memory/autolink");
	const { Store } = await import("../src/store");

	const intentText = "Captured text";
	let autoPromise: Promise<unknown> | undefined;

	// Create closure with afterWrite hook that triggers autolinkCorpus
	const database = await Store.open(join(home, "gateway.db"));
	const closure = new MemoryClosureQueue(database, home, {
		afterWrite: async () => {
			// Start autolink after appendDaily, before intent's commit (potential #341 race)
			autoPromise = autolinkCorpus(root, closure);
		},
	});
	await closure.initialize();

	// Set up: entity + rule
	await mkdir(join(root, "entities"), { recursive: true });
	await writeFile(join(root, "entities/myentity.md"), "# MyEntity\n\nCanonical.");
	await mkdir(join(root, "ops/rules"), { recursive: true });
	await writeFile(join(root, "ops/rules/rule.md"), "# Rule\n\nMyEntity is used.");
	await memoryGit(root, ["add", "-A"]);
	await memoryGit(root, ["commit", "-m", "setup"]);

	// Enqueue real daily_capture intent through the actual API
	const intentId = closure.enqueue({
		kind: "daily_capture",
		originRefJson: JSON.stringify({ platform: "test", kind: "test" }),
		userText: intentText,
		replyText: "response",
	});

	// Process queue and wait for autolink
	await closure.drain();
	if (autoPromise) await autoPromise;

	const intentState = database.memory.get(intentId)?.state;
	database.close();
	expect(intentState).toBe("receipted");
	const intentCommit = (await memoryGit(root, ["log", "--format=%H", "--grep", `Gajaeway-Mutation-Id: ${intentId}`]))
		.split("\n")
		.find(Boolean);
	if (!intentCommit) throw new Error(`missing commit for memory intent ${intentId}`);

	// Verify: the DB settlement is backed by the commit carrying its mutation ID.
	const show = await memoryGit(root, ["show", intentCommit]);
	expect(show).toContain(intentText);

	// CRITICAL #341 FIX: intent commit must NOT contain autolink's link edits
	expect(show).not.toContain("[MyEntity]");

	// Verify: autolink's commit (if exists) contains no intent text
	const autoCommits = await memoryGit(root, ["log", "--format=%H", "--grep", "Memory autolink sweep"]).then((out) =>
		out.split("\n").filter(Boolean),
	);
	for (const commit of autoCommits) {
		const c = await memoryGit(root, ["show", commit]);
		expect(c).not.toContain(intentText);
	}
});

test("issue #473: getOrCreatePendingMemoryOp deduplicates same-kind requests", async () => {
	// Verify the actual getOrCreatePendingMemoryOp function returns created flag and
	// returns the same opRef for concurrent requests of the same kind (autolink or audit).

	interface MemoryOpStatus {
		status: "pending" | "completed" | "failed";
		result?: unknown;
		error?: string;
		startedAt: string;
		expireAt: string;
	}

	const memoryOps = new Map<string, MemoryOpStatus>();

	// First autolink request creates pending operation (created=true)
	const { opRef: opRef1, created: created1 } = getOrCreatePendingMemoryOp(memoryOps, "autolink");
	expect(created1).toBe(true); // First request creates
	expect(memoryOps.has(opRef1)).toBe(true);
	expect(memoryOps.get(opRef1)?.status).toBe("pending");

	// Second autolink request reuses the same opRef (created=false)
	const { opRef: opRef2, created: created2 } = getOrCreatePendingMemoryOp(memoryOps, "autolink");
	expect(created2).toBe(false); // Second request reuses
	expect(opRef2).toBe(opRef1);
	expect(memoryOps.size).toBe(1); // Still only one operation in map

	// Audit request creates separate operation (not dedup'd with autolink)
	const { opRef: auditOpRef, created: auditCreated } = getOrCreatePendingMemoryOp(memoryOps, "audit");
	expect(auditCreated).toBe(true); // Audit creates new operation
	expect(auditOpRef).not.toBe(opRef1); // Different opRef
	expect(auditOpRef.startsWith("gw-ma-")).toBe(true); // Audit prefix
	expect(opRef1.startsWith("gw-ml-")).toBe(true); // Autolink prefix
	expect(memoryOps.size).toBe(2); // Two separate operations

	// Second audit request reuses audit opRef (created=false)
	const { opRef: auditOpRef2, created: auditCreated2 } = getOrCreatePendingMemoryOp(memoryOps, "audit");
	expect(auditCreated2).toBe(false); // Second audit reuses
	expect(auditOpRef2).toBe(auditOpRef);
	expect(memoryOps.size).toBe(2); // Still two operations
});

test("issue #473: pruneExpiredMemoryOps removes settled operations past TTL", async () => {
	// Expired settled operations are removed, but an operation still running past
	// the TTL remains queryable until it settles.

	interface MemoryOpStatus {
		status: "pending" | "completed" | "failed";
		result?: unknown;
		error?: string;
		startedAt: string;
		expireAt: string;
	}

	const memoryOps = new Map<string, MemoryOpStatus>();
	const baseTime = Date.now();

	// Add expired operation (past its 24h TTL)
	memoryOps.set("expired-op", {
		status: "completed",
		result: { filesChanged: 1 },
		startedAt: new Date(baseTime - 48 * 60 * 60 * 1000).toISOString(),
		expireAt: new Date(baseTime - 24 * 60 * 60 * 1000).toISOString(), // Expired
	});

	// Add recent operation (not expired)
	memoryOps.set("recent-op", {
		status: "completed",
		result: { filesChanged: 2 },
		startedAt: new Date(baseTime - 1 * 60 * 60 * 1000).toISOString(),
		expireAt: new Date(baseTime + 23 * 60 * 60 * 1000).toISOString(), // Still valid
	});
	memoryOps.set("pending-op", {
		status: "pending",
		startedAt: new Date(baseTime - 48 * 60 * 60 * 1000).toISOString(),
		expireAt: new Date(baseTime - 24 * 60 * 60 * 1000).toISOString(), // Expired, but still running
	});

	const sizeBefore = memoryOps.size;

	// Run the actual pruning function
	pruneExpiredMemoryOps(memoryOps, baseTime);

	// The settled expired operation is removed; the recent and pending entries remain.
	expect(memoryOps.size).toBe(sizeBefore - 1);
	expect(memoryOps.has("expired-op")).toBe(false);
	expect(memoryOps.has("recent-op")).toBe(true);
	expect(memoryOps.has("pending-op")).toBe(true);
});

test("initializeMemory memoizes per home so the refresh runs once per process", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-initialize-once-"));
	const memory = join(home, "memory");
	await mkdir(memory, { recursive: true });
	// Set up minimal axes
	await writeFile(
		join(memory, "axes.json"),
		`${JSON.stringify(
			{
				version: 1,
				axes: [
					{
						id: "daily",
						root: "daily",
						nesting: "flat",
						index: "recent" as const,
						layout: "dated" as const,
						retrievalPriority: 0,
						orphanPolicy: "any-depth" as const,
					},
				],
			},
			null,
			"\t",
		)}
	`,
	);
	await mkdir(join(memory, "daily"), { recursive: true });
	// First initialization triggers a rebuild
	const root1 = await initializeMemory(home);
	expect(root1).toBe(memory);
	// Verify the map was generated
	const mapAfterFirst = await readFile(join(memory, "MEMORY.md"), "utf8");
	expect(mapAfterFirst).toContain("## daily");
	// Second initialization returns immediately without re-running initializeCorpus
	const start2 = Date.now();
	const root2 = await initializeMemory(home);
	const time2 = Date.now() - start2;
	expect(root2).toBe(memory);
	// The second call should be nearly instant (no regenerateMap); verify < 50ms
	expect(time2).toBeLessThan(50);
	// Verify the map is unchanged
	const mapAfterSecond = await readFile(join(memory, "MEMORY.md"), "utf8");
	expect(mapAfterSecond).toBe(mapAfterFirst);
});
