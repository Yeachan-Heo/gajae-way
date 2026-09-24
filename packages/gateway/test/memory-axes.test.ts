import { afterEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initializeMemory, memoryRoot, regenerateMap } from "../src/memory/doctrine";
import {
	type AxisDescriptor,
	BUILT_IN_AXES,
	createRegistry,
	layoutViolation,
	loadRegistry,
	REGISTRY_FILE,
} from "../src/memory/registry";
import { searchMemory } from "../src/memory/retrieve";
import { validateMemory } from "../src/memory/validator";

let home = "";
afterEach(async () => {
	if (home) await rm(home, { recursive: true, force: true });
	home = "";
});

async function fresh(): Promise<string> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-axes-"));
	return initializeMemory(home);
}

/** A corpus whose deployment registers extra axes before the gateway ever boots. */
async function withCustomAxes(...axes: readonly unknown[]): Promise<string> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-custom-"));
	const root = memoryRoot(home);
	await mkdir(root, { recursive: true, mode: 0o700 });
	await writeFile(join(root, REGISTRY_FILE), `${JSON.stringify({ version: 1, axes }, null, "\t")}\n`);
	return root;
}

/** Every file under the root mapped to the sha256 of its bytes. */
async function fingerprint(root: string): Promise<Map<string, string>> {
	const result = new Map<string, string>();
	const walk = async (directory: string): Promise<void> => {
		for (const entry of await readdir(join(root, directory), { withFileTypes: true })) {
			if (entry.name === ".git") continue;
			const path = directory ? `${directory}/${entry.name}` : entry.name;
			if (entry.isDirectory()) await walk(path);
			else if (entry.isFile())
				result.set(path, new Bun.CryptoHasher("sha256").update(await readFile(join(root, path))).digest("hex"));
		}
	};
	await walk("");
	return result;
}

/**
 * A corpus as an operator left it before the axis set grew: the older seven
 * directories, a hand-written routable ops/ tree, and a map that predates both
 * new axes. Deliberately not created through initializeMemory.
 */
async function legacyCorpus(): Promise<string> {
	home = await mkdtemp(join(tmpdir(), "gajaeway-legacy-"));
	const root = memoryRoot(home);
	for (const axis of ["daily", "events", "tasks", "people", "projects", "channels", "decisions"])
		await mkdir(join(root, axis), { recursive: true, mode: 0o700 });
	await mkdir(join(root, "ops/rules"), { recursive: true, mode: 0o700 });
	await writeFile(join(root, "ops/rules/index.md"), "# Rule index\n\n- [restart](restart.md)\n");
	await writeFile(join(root, "ops/rules/restart.md"), "# Restart\n\nDrain first. Never kill -9 the gateway.\n");
	await writeFile(join(root, "daily/2026-08-01.md"), "# capture\n\n- user: hello\n");
	await writeFile(join(root, "people/owner.md"), "# owner\n\nPrefers terse reports.\n");
	await writeFile(
		join(root, "MEMORY.md"),
		"# Memory map\n\n## daily\n\n- [daily/2026-08-01.md](daily/2026-08-01.md)\n",
	);
	return root;
}

const RUNBOOK: AxisDescriptor = {
	id: "runbooks",
	displayName: "Deployment runbooks",
	root: "runbooks",
	nesting: "nested",
	partitions: ["staging", "production"],
	index: "tree",
	layout: "free",
	retrievalPriority: 90,
	orphanPolicy: "partitioned",
	appendOnly: false,
	promotesTo: ["ops"],
};

// ---------------------------------------------------------------- built-ins

test("ops and reflections are registered axes with the semantics from the handoff", async () => {
	const registry = createRegistry();
	const ops = registry.byId("ops");
	const reflections = registry.byId("reflections");

	expect(ops).toBeDefined();
	expect(ops?.root).toBe("ops");
	expect(ops?.partitions).toEqual(["rules", "distillations", "handoffs"]);
	expect(ops?.orphanPolicy).toBe("partitioned");
	expect(ops?.index).toBe("tree");
	// Above raw capture: ops is the narrowest doctrine the registry ships with.
	expect(ops?.retrievalPriority ?? 0).toBeGreaterThan(registry.byId("daily")?.retrievalPriority ?? 0);

	expect(reflections?.layout).toBe("dated");
	expect(reflections?.appendOnly).toBe(true);
	expect(reflections?.promotesTo).toEqual(["ops", "projects", "channels", "people"]);
});

test("a fresh corpus materialises every registered axis and its partitions", async () => {
	const root = await fresh();

	for (const axis of BUILT_IN_AXES) {
		const entries = await readdir(join(root, axis.root), { withFileTypes: true });
		for (const partition of axis.partitions)
			expect(entries.some((entry) => entry.isDirectory() && entry.name === partition)).toBe(true);
	}
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	for (const axis of BUILT_IN_AXES) expect(map).toContain(`## ${axis.id}`);
	expect(await validateMemory(root)).toEqual([]);
});

// ---------------------------------------------------------------- migration

test("adding axes to a legacy corpus creates directories and leaves human bytes untouched", async () => {
	const root = await legacyCorpus();
	const before = await fingerprint(root);
	expect(before.has("ops/rules/index.md")).toBe(true);

	await initializeMemory(home);

	const after = await fingerprint(root);
	// Every pre-existing file still exists with byte-identical content. MEMORY.md is
	// generated navigation, so migration is allowed to regenerate exactly that one.
	for (const [path, hash] of before) {
		expect(after.has(path)).toBe(true);
		if (path !== "MEMORY.md") expect(after.get(path)).toBe(hash);
	}
	// Migration adds directories only; it invents no Markdown of its own.
	for (const path of after.keys()) expect(before.has(path) || path === "MEMORY.md").toBe(true);

	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	expect(map).toContain("## ops");
	expect(map).toContain("## reflections");
	expect(map).toContain("ops/rules/index.md");
	expect(await validateMemory(root)).toEqual([]);
});

test("a corpus whose new axis directories were pre-created by hand still gets its map repaired", async () => {
	// The operator restores or seeds the tree by hand, so nothing is missing for
	// mkdir to create - only the generated map predates the axis set. A quiescent
	// corpus like this has no capture to heal it, so startup has to.
	const root = await legacyCorpus();
	await mkdir(join(root, "reflections"), { recursive: true, mode: 0o700 });
	await mkdir(join(root, "ops/distillations"), { recursive: true, mode: 0o700 });
	await mkdir(join(root, "ops/handoffs"), { recursive: true, mode: 0o700 });

	await initializeMemory(home);

	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	expect(map).toContain("## ops");
	expect(map).toContain("## reflections");
	expect(await validateMemory(root)).toEqual([]);
});

test("migrating twice is a no-op on the second run", async () => {
	const root = await legacyCorpus();
	await initializeMemory(home);
	const after = await fingerprint(root);
	await initializeMemory(home);
	expect(await fingerprint(root)).toEqual(after);
});

// ---------------------------------------------------------------- ops layout

test("a routable ops tree of index plus rule packs audits clean and is indexed recursively", async () => {
	const root = await fresh();
	await mkdir(join(root, "ops/rules/deploy"), { recursive: true });
	await writeFile(join(root, "ops/rules/index.md"), "# Rule index\n\n- [deploy](deploy/release.md)\n");
	await writeFile(join(root, "ops/rules/forbidden.md"), "# Forbidden\n\nNever run `git push --force`.\n");
	await writeFile(join(root, "ops/rules/deploy/release.md"), "# Release\n\nDrain, verify, then swap.\n");
	await writeFile(join(root, "ops/distillations/retries.md"), "# Retries\n\nOne bounded retry, then report.\n");
	await writeFile(join(root, "ops/handoffs/next.md"), "# Next executor\n\nPick up the memory axis work.\n");
	await regenerateMap(root);

	expect(await validateMemory(root)).toEqual([]);
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	expect(map).toContain("ops/rules/deploy/release.md");
	expect(map).toContain("ops/distillations/retries.md");
	expect(map).toContain("ops/handoffs/next.md");
});

test("an ops file outside a declared partition is a layout violation, not a silent promotion", async () => {
	const root = await fresh();
	await writeFile(join(root, "ops/scratch.md"), "# scratch\n");
	await regenerateMap(root);

	const issues = await validateMemory(root);
	expect(issues.filter((issue) => issue.code === "axis_layout_violation").map((issue) => issue.path)).toEqual([
		"ops/scratch.md",
	]);
});

// -------------------------------------------------------- reflections layout

test("reflections accepts dated entries and rejects a per-subject second authority", async () => {
	const root = await fresh();
	await mkdir(join(root, "reflections/2026-08"), { recursive: true });
	await writeFile(join(root, "reflections/2026-08-27.md"), "# 2026-08-27\n\n- observed: repeated deploy mistake\n");
	await writeFile(join(root, "reflections/2026-08/2026-08-20.md"), "# 2026-08-20\n\n- learned: terse reports land\n");
	await regenerateMap(root);
	expect(await validateMemory(root)).toEqual([]);

	await writeFile(join(root, "reflections/tone.md"), "# tone\n\n- be terse\n");
	await regenerateMap(root);
	const issues = await validateMemory(root);
	expect(issues.filter((issue) => issue.code === "axis_layout_violation").map((issue) => issue.path)).toEqual([
		"reflections/tone.md",
	]);
	expect(issues.some((issue) => issue.code === "orphan_file")).toBe(false);
});

test("a dated axis rejects an impossible date", async () => {
	const root = await fresh();
	await writeFile(join(root, "reflections/2026-99-99.md"), "# not a date\n");
	await regenerateMap(root);

	expect(
		(await validateMemory(root)).filter((issue) => issue.code === "axis_layout_violation").map((issue) => issue.path),
	).toEqual(["reflections/2026-99-99.md"]);
});

test("a partition heading cannot masquerade as the axis heading of a like-named axis", async () => {
	// `ops` is indexed as a tree, so its map section emits `### rules`. An axis
	// literally named `rules` must not count that as its own `## rules` heading.
	const root = await withCustomAxes({ id: "rules", displayName: "Standalone rules", root: "standalone-rules" });
	await initializeMemory(home);
	await writeFile(join(root, "ops/rules/pack.md"), "# pack\n\nsomething routable\n");
	await regenerateMap(root);
	expect(await validateMemory(root)).toEqual([]);

	// Strip only the real `## rules` section heading, leaving ops's `### rules`
	// intact. Note the anchor: the substring "## rules" also occurs inside
	// "### rules", which is exactly the confusion the heading check has to survive.
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	const stripped = map.replace(/\n## rules\n/, "\n## removed\n");
	expect(stripped).not.toBe(map);
	expect(stripped).toContain("### rules");
	await writeFile(join(root, "MEMORY.md"), stripped);
	expect((await validateMemory(root)).map((issue) => issue.code)).toContain("unmapped_axis_dir");
});

test("the regenerated map is published atomically, never as a half-written file", async () => {
	const root = await fresh();
	for (let index = 0; index < 40; index++)
		await writeFile(join(root, `reflections/2026-01-${String((index % 28) + 1).padStart(2, "0")}.md`), `# ${index}\n`);

	// Readers on the memory.audit / memory.search paths run concurrently with a
	// regeneration; every observed map must be a complete one.
	const reads = Array.from({ length: 24 }, () => readFile(join(root, "MEMORY.md"), "utf8"));
	const [, ...observed] = await Promise.all([regenerateMap(root), ...reads]);
	for (const map of observed) {
		expect(map.startsWith("# Memory map")).toBe(true);
		expect(map.endsWith("\n")).toBe(true);
		expect(map).toContain("## reflections");
	}
	// And no staging file is left behind for a walker to trip over.
	expect((await readdir(root)).filter((name) => name.includes(".staging"))).toEqual([]);
});

test("overlapping map writers on one corpus neither throw nor interleave (#227)", async () => {
	const root = await fresh();
	// Writers race while the corpus keeps changing under them, as the closure
	// queue, initializeMemory and the autolink sweep do on a live gateway.
	const writers = Array.from({ length: 16 }, async (_, index) => {
		await writeFile(join(root, `reflections/2026-02-${String(index + 1).padStart(2, "0")}.md`), `# ${index}\n`);
		await regenerateMap(root);
	});
	await Promise.all(writers);

	// The published map is a complete render of the final corpus, not a mix.
	const published = await readFile(join(root, "MEMORY.md"), "utf8");
	await regenerateMap(root);
	expect(await readFile(join(root, "MEMORY.md"), "utf8")).toBe(published);
	expect((await readdir(root)).filter((name) => name.includes(".staging"))).toEqual([]);
});

test("the newest entry of an append-only axis must stay reachable from MEMORY.md", async () => {
	const root = await fresh();
	await writeFile(join(root, "reflections/2099-01-01.md"), "# newest\n\n- learned something\n");

	const drift = (await validateMemory(root)).filter((issue) => issue.code === "map_content_drift");
	expect(drift).toHaveLength(1);
	expect(drift[0].message).toContain("reflections");

	await regenerateMap(root);
	expect(await validateMemory(root)).toEqual([]);
});

// ------------------------------------------------------------- custom axes

test("a custom flat axis rejects a subdirectory, and the map publishes its promotion targets", async () => {
	const root = await withCustomAxes({
		id: "glossary",
		displayName: "Term glossary",
		root: "glossary",
		nesting: "flat",
		index: "recent",
		layout: "free",
		retrievalPriority: 15,
		promotesTo: ["projects"],
	});
	await initializeMemory(home);

	await writeFile(join(root, "glossary/closure-ladder.md"), "# closure ladder\n\nIntent, write, commit, receipt.\n");
	await regenerateMap(root);
	expect(await validateMemory(root)).toEqual([]);
	// promotesTo is navigation the canonicalising writer reads, not a decorative field.
	expect(await readFile(join(root, "MEMORY.md"), "utf8")).toContain("_promotes to: projects_");

	await mkdir(join(root, "glossary/archive"), { recursive: true });
	await writeFile(join(root, "glossary/archive/old.md"), "# old\n");
	await regenerateMap(root);
	const issues = await validateMemory(root);
	expect(issues.filter((issue) => issue.code === "axis_layout_violation").map((issue) => issue.path)).toEqual([
		"glossary/archive/old.md",
	]);
});

test("a custom nested axis participates in creation, indexing, audit and recall", async () => {
	const root = await withCustomAxes(RUNBOOK);
	await initializeMemory(home);

	// Created, including its partitions, by the same code path as the built-ins.
	const entries = await readdir(join(root, "runbooks"), { withFileTypes: true });
	expect(
		entries
			.filter((entry) => entry.isDirectory())
			.map((entry) => entry.name)
			.sort(),
	).toEqual(["production", "staging"]);

	await mkdir(join(root, "runbooks/production/gateway"), { recursive: true });
	await writeFile(
		join(root, "runbooks/production/gateway/failover.md"),
		"# Failover\n\nPromote the standby socket before draining the primary.\n",
	);
	await regenerateMap(root);

	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	expect(map).toContain("## runbooks");
	expect(map).toContain("_Deployment runbooks_");
	expect(map).toContain("runbooks/production/gateway/failover.md");
	expect(await validateMemory(root)).toEqual([]);

	const hits = await searchMemory(root, "standby socket failover", 10);
	expect(hits.map((hit) => hit.path)).toContain("runbooks/production/gateway/failover.md");
});

test("recall breaks score ties by the axis's declared retrieval priority", async () => {
	const root = await fresh();
	// Identical wording in two axes: only the descriptor's priority can order them.
	await writeFile(join(root, "ops/rules/drain.md"), "# drain\n\ndrain the queue before restart\n");
	await writeFile(join(root, "daily/2026-08-27.md"), "# drain\n\ndrain the queue before restart too\n");
	await regenerateMap(root);

	const hits = await searchMemory(root, "drain the queue before restart", 10);
	expect(hits[0].path).toBe("ops/rules/drain.md");
});

test("recall survives a stale map instead of failing the query", async () => {
	const root = await fresh();
	await writeFile(join(root, "ops/rules/drain.md"), "# drain\n\ndrain the queue before restart\n");
	await regenerateMap(root);
	// A pointer to a deleted file is a state the audit merely REPORTS, so the
	// corpus is stale, not corrupt, and recall has to keep answering.
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	await writeFile(join(root, "MEMORY.md"), `${map}\n- [ops/rules/gone.md](ops/rules/gone.md)\n`);

	expect((await validateMemory(root)).some((issue) => issue.code === "map_dangling")).toBe(true);
	const hits = await searchMemory(root, "drain the queue before restart", 10);
	expect(hits.map((hit) => hit.path)).toContain("ops/rules/drain.md");
});

test("a map pointer cannot make recall read outside the memory root", async () => {
	const root = await fresh();
	await writeFile(join(home, "secret.md"), "# secret\n\nthe drain password is hunter2\n");
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	// `%2e%2e` is `..` once decoded: a literal `..` check would wave it through.
	await writeFile(
		join(root, "MEMORY.md"),
		`${map}\n- [a](%2e%2e/secret.md)\n- [b](daily/../../secret.md)\n- [c](/etc/hosts.md)\n`,
	);

	const hits = await searchMemory(root, "hunter2 drain password", 10);
	expect(hits.map((hit) => hit.path).filter((path) => path.includes("secret"))).toEqual([]);
	// And the audit names the escape rather than reporting a missing file.
	const escapes = (await validateMemory(root)).filter((issue) => issue.code === "out_of_root_link");
	expect(escapes).toHaveLength(3);
});

test("the audit inspects every file the map serves, even under a symlinked axis root", async () => {
	// One traversal for indexing, audit and recall: a file served by the map that
	// the audit skipped is how an unpartitioned rule escapes the layout gate.
	home = await mkdtemp(join(tmpdir(), "gajaeway-symlink-"));
	const root = memoryRoot(home);
	await mkdir(join(home, "elsewhere/rules"), { recursive: true });
	await writeFile(join(home, "elsewhere/rules/smuggled.md"), "# smuggled\n\nnever audited?\n");
	await writeFile(join(home, "elsewhere/loose.md"), "# loose\n\noutside every declared partition\n");
	await mkdir(root, { recursive: true, mode: 0o700 });
	await symlink(join(home, "elsewhere"), join(root, "ops"));

	await initializeMemory(home);
	await regenerateMap(root);

	expect(await readFile(join(root, "MEMORY.md"), "utf8")).toContain("ops/rules/smuggled.md");
	expect((await searchMemory(root, "smuggled never audited", 10)).map((hit) => hit.path)).toContain(
		"ops/rules/smuggled.md",
	);
	// Indexed and recalled, therefore audited: ops/loose.md sits in no partition.
	expect(
		(await validateMemory(root)).filter((issue) => issue.code === "axis_layout_violation").map((issue) => issue.path),
	).toEqual(["ops/loose.md"]);
});

test("concurrent cold starts all succeed and leave one coherent corpus", async () => {
	// Adapters, monitors and the closure queue all initialize; unserialized they
	// raced on `git init` and on the generated map.
	home = await mkdtemp(join(tmpdir(), "gajaeway-concurrent-"));
	const results = await Promise.allSettled([1, 2, 3, 4, 5].map(() => initializeMemory(home)));
	expect(results.filter((result) => result.status === "rejected")).toEqual([]);

	const root = memoryRoot(home);
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	// A torn write shows up as a duplicated or truncated header block.
	expect(map.split("# Memory map").length - 1).toBe(1);
	expect(await validateMemory(root)).toEqual([]);
});

test("an unregistered directory is an orphan and is never auto-promoted to canonical", async () => {
	const root = await fresh();
	await mkdir(join(root, "scratch/notes"), { recursive: true });
	await writeFile(join(root, "stray.md"), "# stray\n");
	await writeFile(join(root, "scratch/notes/idea.md"), "# idea\n");
	await regenerateMap(root);

	const orphans = (await validateMemory(root)).filter((issue) => issue.code === "orphan_file");
	expect(orphans.map((issue) => issue.path).sort()).toEqual(["scratch/notes/idea.md", "stray.md"]);
	expect(orphans[0].message).toContain(REGISTRY_FILE);

	// Re-initialising does not adopt the directory: canonical membership only ever
	// comes from the registry.
	await initializeMemory(home);
	await regenerateMap(root);
	expect((await validateMemory(root)).some((issue) => issue.code === "orphan_file")).toBe(true);
	expect(await readFile(join(root, "MEMORY.md"), "utf8")).not.toContain("## scratch");
});

// ------------------------------------------------------- fail-closed registry

test("a declaration that names a built-in overrides it instead of being rejected", async () => {
	const registry = createRegistry([
		{ ...RUNBOOK, id: "ops", root: "ops", partitions: ["rules", "runbooks", "incidents"], promotesTo: [] },
	]);
	const ops = registry.byId("ops");
	expect(ops?.partitions).toEqual(["rules", "runbooks", "incidents"]);
	// Override, not addition: exactly one axis owns the id and the root.
	expect(registry.axes.filter((axis) => axis.id === "ops")).toHaveLength(1);
	expect(registry.axes).toHaveLength(BUILT_IN_AXES.length);
	// The override is what layout policy is judged against, so a corpus partition
	// the built-in never knew about stops being a violation.
	expect(layoutViolation(ops as AxisDescriptor, "ops/incidents/2026-08-14-lock.md")).toBeUndefined();
	expect(layoutViolation(ops as AxisDescriptor, "ops/handoffs/old.md")).toContain("declared partition");
});

test("an omitted field keeps the built-in's value rather than the new-axis default", async () => {
	// The documented minimal repair: widen ops's partitions and state nothing else.
	// Falling back to schema defaults here would silently demote the axis to
	// index=recent, retrievalPriority=0 and displayName="ops".
	const built = BUILT_IN_AXES.find((axis) => axis.id === "ops") as AxisDescriptor;
	const ops = createRegistry([{ id: "ops", partitions: [...built.partitions, "incidents"] }]).byId("ops");
	expect(ops).toEqual({ ...built, partitions: [...built.partitions, "incidents"] });

	// A new axis still takes the documented defaults, because it has nothing to inherit.
	const fresh = createRegistry([{ id: "runbooks" }]).byId("runbooks");
	expect(fresh).toEqual({
		id: "runbooks",
		displayName: "runbooks",
		root: "runbooks",
		nesting: "nested",
		partitions: [],
		index: "recent",
		layout: "free",
		retrievalPriority: 0,
		orphanPolicy: "any-depth",
		appendOnly: false,
		promotesTo: [],
	});
});

test("an override may still relax a built-in, but only by saying so", async () => {
	const daily = createRegistry([{ id: "daily", layout: "free", appendOnly: false }]).byId("daily");
	expect(daily?.appendOnly).toBe(false);
	// Inherited, not defaulted away: the capture axis keeps its promotion targets.
	expect(daily?.promotesTo).toContain("ops");
	// The dated/append-only invariant is still enforced against the merged result.
	expect(() => createRegistry([{ id: "reflections", appendOnly: false }])).toThrow(/must be appendOnly/);
});

test("an override still may not claim another axis's root, and may not be declared twice", async () => {
	expect(() => createRegistry([{ ...RUNBOOK, id: "ops", root: "daily/ops" }])).toThrow(/overlaps axis daily/);
	expect(() =>
		createRegistry([
			{ ...RUNBOOK, id: "ops", root: "ops" },
			{ ...RUNBOOK, id: "ops", root: "ops" },
		]),
	).toThrow(/declared twice/);
});

test("a built-in that another built-in promotes into survives being overridden", async () => {
	// daily promotes into ops; replacing ops must keep that target resolvable.
	const registry = createRegistry([{ ...RUNBOOK, id: "ops", root: "ops", promotesTo: [] }]);
	expect(registry.byId("daily")?.promotesTo).toContain("ops");
	expect(registry.byId("ops")?.displayName).toBe(RUNBOOK.displayName);
});

test("overriding a built-in clears the layout violations its declared partitions caused", async () => {
	// The gajaeway host case: an ops/ tree that grew incidents/, runbooks/ and
	// infra/ long before the built-in's three partitions were written down.
	const root = await withCustomAxes({
		id: "ops",
		displayName: "Operating doctrine",
		root: "ops",
		nesting: "nested",
		partitions: ["rules", "distillations", "handoffs", "incidents", "runbooks", "infra"],
		index: "tree",
		orphanPolicy: "partitioned",
	});
	await initializeMemory(home);
	for (const path of ["ops/incidents/2026-08-14-lock.md", "ops/infra/services.md"]) {
		await mkdir(join(root, path, ".."), { recursive: true });
		await writeFile(join(root, path), `# ${path}\n`);
	}
	await regenerateMap(root);

	expect((await validateMemory(root)).filter((issue) => issue.path.startsWith("ops/"))).toEqual([]);
	// Still partitioned: a file directly under the root remains a violation.
	await writeFile(join(root, "ops/loose.md"), "# loose\n");
	expect((await validateMemory(root)).map((issue) => `${issue.code}:${issue.path}`)).toContain(
		"axis_layout_violation:ops/loose.md",
	);
});

test("a custom axis may not overlap another axis's canonical root", async () => {
	expect(() => createRegistry([{ ...RUNBOOK, id: "opsrules", root: "ops/rules" }])).toThrow(/overlaps axis ops/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "wide", root: "ops" }])).toThrow(/overlaps axis ops/);
	expect(() =>
		createRegistry([
			{ ...RUNBOOK, id: "outer", root: "runbooks" },
			{ ...RUNBOOK, id: "inner", root: "runbooks/staging" },
		]),
	).toThrow(/overlaps axis outer/);
});

test("a malformed descriptor fails closed instead of being partially applied", async () => {
	expect(() => createRegistry([{ ...RUNBOOK, id: "Not An Id" }])).toThrow(/axis id must match/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "escape", root: "../outside" }])).toThrow(/relative lowercase path/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "traverse", root: "runbooks/../../etc" }])).toThrow(
		/relative lowercase path/,
	);
	// A case-insensitive filesystem would let this name a built-in's directory.
	expect(() => createRegistry([{ ...RUNBOOK, id: "shouty", root: "Ops" }])).toThrow(/relative lowercase path/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "dotgit", root: ".git" }])).toThrow(/relative lowercase path/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "theregistry", root: "axes.json" }])).toThrow(/reserved corpus name/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "themap", root: "memory.md" }])).toThrow(/reserved corpus name/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "typo", nesting: "deep" }])).toThrow(/nesting must be one of/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "extra", surprise: true }])).toThrow(/unknown field surprise/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "nan", retrievalPriority: "high" }])).toThrow(/finite number/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "flatpart", nesting: "flat" }])).toThrow(/cannot declare partitions/);
	expect(() =>
		createRegistry([
			{ ...RUNBOOK, id: "mutable", layout: "dated", partitions: [], orphanPolicy: "any-depth", appendOnly: false },
		]),
	).toThrow(/must be appendOnly/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "nowhere", promotesTo: ["ghost"] }])).toThrow(
		/unregistered axis ghost/,
	);
});

test("a circular promotion relation fails closed", async () => {
	expect(() =>
		createRegistry([
			{ ...RUNBOOK, id: "alpha", root: "alpha", promotesTo: ["beta"] },
			{ ...RUNBOOK, id: "beta", root: "beta", promotesTo: ["alpha"] },
		]),
	).toThrow(/circular promotion: alpha -> beta -> alpha/);
	expect(() => createRegistry([{ ...RUNBOOK, id: "self", root: "self", promotesTo: ["self"] }])).toThrow(
		/circular promotion: self -> self/,
	);
});

test("an unreadable registry declaration is fatal, never silently ignored", async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-bad-"));
	const root = memoryRoot(home);
	await mkdir(root, { recursive: true, mode: 0o700 });
	await writeFile(join(root, REGISTRY_FILE), "{ not json\n");

	expect(loadRegistry(root)).rejects.toThrow(/not valid JSON/);
	expect(initializeMemory(home)).rejects.toThrow(/not valid JSON/);
});

test("a corpus without a registry declaration gets exactly the built-ins", async () => {
	const root = await fresh();
	const registry = await loadRegistry(root);
	expect(registry.axes.map((axis) => axis.id)).toEqual(BUILT_IN_AXES.map((axis) => axis.id));
});
