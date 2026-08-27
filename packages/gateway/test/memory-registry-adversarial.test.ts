/**
 * Adversarial suite for the memory axis registry: hostile descriptors, hostile
 * corpora, and the three defects a red-team pass actually found - a `git init`
 * race between concurrent cold starts, a symlinked axis root that was indexed
 * and recalled but never audited, and one dangling map pointer taking down every
 * `memory.search` call. Each of those is a regression test now.
 *
 * Every fixture lives in its own mkdtemp directory and is removed again; nothing
 * here writes into the repository or reads a live corpus.
 */
import { expect, test } from "bun:test";
import { chmod, lstat, mkdir, mkdtemp, readdir, readFile, readlink, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { initializeMemory, memoryRoot, regenerateMap } from "../src/memory/doctrine";
import { AxisRegistryError, createRegistry, loadRegistry, REGISTRY_FILE } from "../src/memory/registry";
import { searchMemory } from "../src/memory/retrieve";
import { type MemoryIssue, validateMemory } from "../src/memory/validator";

// ---------------------------------------------------------------- harness

async function sandbox<T>(run: (home: string, root: string) => Promise<T>): Promise<T> {
	const home = await mkdtemp(join(tmpdir(), "gjc-redteam-"));
	try {
		return await run(home, memoryRoot(home));
	} finally {
		await chmod(memoryRoot(home), 0o700).catch(() => {});
		await rm(home, { recursive: true, force: true });
	}
}

/** A corpus whose deployment declares custom axes before the gateway ever boots. */
async function declare(root: string, body: unknown): Promise<void> {
	await mkdir(root, { recursive: true, mode: 0o700 });
	await writeFile(join(root, REGISTRY_FILE), typeof body === "string" ? body : JSON.stringify(body));
}

/** Every entry under the root: files by sha256, symlinks by target, dirs marked. */
async function fingerprint(root: string): Promise<Map<string, string>> {
	const result = new Map<string, string>();
	const walk = async (directory: string): Promise<void> => {
		for (const entry of await readdir(join(root, directory), { withFileTypes: true })) {
			if (entry.name === ".git") continue;
			const path = directory ? `${directory}/${entry.name}` : entry.name;
			if (entry.isSymbolicLink()) result.set(path, `symlink:${await readlink(join(root, path))}`);
			else if (entry.isDirectory()) {
				result.set(path, "dir");
				await walk(path);
			} else result.set(path, new Bun.CryptoHasher("sha256").update(await readFile(join(root, path))).digest("hex"));
		}
	};
	await walk("");
	return result;
}

function thrown(run: () => unknown): AxisRegistryError | Error {
	try {
		run();
	} catch (error) {
		return error as Error;
	}
	throw new Error("expected a throw, got none");
}

const BASE = {
	id: "runbooks",
	root: "runbooks",
	nesting: "nested",
	partitions: ["staging"],
	index: "tree",
	layout: "free",
	retrievalPriority: 90,
	orphanPolicy: "partitioned",
	appendOnly: false,
	promotesTo: [],
} as const;

const codes = (issues: readonly MemoryIssue[]) => issues.map((issue) => `${issue.code}:${issue.path}`).sort();

// =========================================================== A. fail-closed

test("A1 prototype pollution through axes.json is rejected and pollutes nothing", async () => {
	// JSON.parse materialises __proto__ as an OWN property, so the unknown-field
	// gate is the only thing standing between a corpus file and Object.prototype.
	for (const body of [
		'{"axes":[{"id":"ok","__proto__":{"polluted":"yes"}}]}',
		'{"axes":[{"id":"__proto__"}]}',
		'{"axes":[{"id":"ok","root":"ok","constructor":{"prototype":{"polluted":"yes"}}}]}',
		'{"__proto__":{"axes":[{"id":"ok"}]},"axes":[]}',
	]) {
		await sandbox(async (_home, root) => {
			await declare(root, body);
			if (body.includes('"axes":[]')) {
				// Shape is legal; it must simply yield the built-ins and pollute nothing.
				expect((await loadRegistry(root)).axes.length).toBe(9);
			} else {
				expect(loadRegistry(root)).rejects.toThrow(AxisRegistryError);
			}
		});
	}
	expect(({} as Record<string, unknown>).polluted).toBeUndefined();
	expect((Object.prototype as Record<string, unknown>).polluted).toBeUndefined();
	expect(([] as unknown as Record<string, unknown>).polluted).toBeUndefined();
});

test("A2 hostile axis ids fail closed with malformed_descriptor", async () => {
	const hostile: readonly unknown[] = [
		"__proto__",
		// NOTE: `constructor` and `prototype` are syntactically legal lowercase ids and
		// are accepted; the second loop below proves they stay inert (lookup is a Map).
		"", // empty
		"Ops", // case-only difference from a built-in
		"OPS",
		"ор s", // Cyrillic confusables
		"орs",
		"0ps", // leading digit
		"9",
		"-ops",
		"ops ", // trailing space
		" ops",
		"ops.md",
		"ops/rules",
		null,
		42,
		true,
		{},
		[],
	];
	for (const id of hostile) {
		const error = thrown(() => createRegistry([{ ...BASE, id, root: "somewhere" }]));
		expect(error).toBeInstanceOf(AxisRegistryError);
		expect((error as AxisRegistryError).code).toBe("malformed_descriptor");
	}
	// A syntactically legal but dangerous-looking id must be inert, not magic.
	for (const id of ["constructor", "prototype", "tostring"]) {
		const registry = createRegistry([{ ...BASE, id, root: id }]);
		expect(registry.byId(id)?.id).toBe(id);
		expect(registry.byId("nope")).toBeUndefined();
		expect(registry.byId("hasOwnProperty")).toBeUndefined();
		expect(registry.byId("__proto__")).toBeUndefined();
	}
});

test("A3 hostile roots fail closed with malformed_descriptor", async () => {
	const hostile = [
		"a/../b",
		"./a",
		"../a",
		"a//b",
		"a/",
		"/etc",
		"/",
		"a\\b",
		"a\\..\\b",
		".git",
		".git/hooks",
		"MEMORY.md",
		"memory.md",
		"axes.json",
		"Daily", // case-only shadow of a built-in on a case-insensitive filesystem
		"DAILY",
		"OPS/rules",
		"ops ",
		" ops",
		"~/escape",
		"a\u0000b",
		"a/./b",
		"",
	];
	for (const root of hostile) {
		const error = thrown(() => createRegistry([{ ...BASE, id: "probe", root }]));
		expect(error).toBeInstanceOf(AxisRegistryError);
		expect((error as AxisRegistryError).code).toBe("malformed_descriptor");
	}
	// Roots that overlap a built-in must be caught by the overlap gate, not silently taken.
	for (const root of ["daily", "ops", "ops/rules", "reflections/2026-08"]) {
		const error = thrown(() => createRegistry([{ ...BASE, id: "probe", root }]));
		expect((error as AxisRegistryError).code).toBe("overlapping_axis_root");
	}
});

test("A4 two custom axes that collide with each other fail closed", async () => {
	const dup = thrown(() =>
		createRegistry([
			{ ...BASE, id: "alpha", root: "alpha" },
			{ ...BASE, id: "alpha", root: "beta" },
		]),
	);
	expect((dup as AxisRegistryError).code).toBe("duplicate_axis_id");

	const same = thrown(() =>
		createRegistry([
			{ ...BASE, id: "alpha", root: "shared" },
			{ ...BASE, id: "beta", root: "shared" },
		]),
	);
	expect((same as AxisRegistryError).code).toBe("overlapping_axis_root");

	const nestedUnder = thrown(() =>
		createRegistry([
			{ ...BASE, id: "alpha", root: "alpha" },
			{ ...BASE, id: "beta", root: "alpha/inner" },
		]),
	);
	expect((nestedUnder as AxisRegistryError).code).toBe("overlapping_axis_root");

	const nestedOver = thrown(() =>
		createRegistry([
			{ ...BASE, id: "beta", root: "alpha/inner" },
			{ ...BASE, id: "alpha", root: "alpha" },
		]),
	);
	expect((nestedOver as AxisRegistryError).code).toBe("overlapping_axis_root");

	// A sibling that merely shares a name prefix is legal and must NOT be rejected.
	expect(createRegistry([{ ...BASE, id: "opsx", root: "opsx" }]).byId("opsx")).toBeDefined();
});

test("A5 promotion graph: long cycles and self-cycles fail, forward references do not", async () => {
	const three = thrown(() =>
		createRegistry([
			{ ...BASE, id: "alpha", root: "alpha", promotesTo: ["beta"] },
			{ ...BASE, id: "beta", root: "beta", promotesTo: ["gamma"] },
			{ ...BASE, id: "gamma", root: "gamma", promotesTo: ["alpha"] },
		]),
	);
	expect((three as AxisRegistryError).code).toBe("circular_promotion");

	const four = thrown(() =>
		createRegistry([
			{ ...BASE, id: "a1", root: "a1", promotesTo: ["a2"] },
			{ ...BASE, id: "a2", root: "a2", promotesTo: ["a3"] },
			{ ...BASE, id: "a3", root: "a3", promotesTo: ["a4"] },
			{ ...BASE, id: "a4", root: "a4", promotesTo: ["a2"] },
		]),
	);
	expect((four as AxisRegistryError).code).toBe("circular_promotion");

	const self = thrown(() => createRegistry([{ ...BASE, id: "solo", root: "solo", promotesTo: ["solo"] }]));
	expect((self as AxisRegistryError).code).toBe("circular_promotion");

	// A custom axis promoting INTO a built-in is a legal DAG edge, and a diamond
	// (two axes promoting into the same target) must not be mistaken for a cycle.
	const dag = createRegistry([
		{ ...BASE, id: "hook", root: "hook", promotesTo: ["daily", "ops"] },
		{ ...BASE, id: "hook2", root: "hook2", promotesTo: ["ops", "hook"] },
	]);
	expect(dag.byId("hook2")?.promotesTo).toEqual(["ops", "hook"]);

	// Forward reference: the target is declared later in the same array.
	const forward = createRegistry([
		{ ...BASE, id: "first", root: "first", promotesTo: ["second"] },
		{ ...BASE, id: "second", root: "second" },
	]);
	expect(forward.byId("first")?.promotesTo).toEqual(["second"]);

	const ghost = thrown(() => createRegistry([{ ...BASE, id: "ghosty", root: "ghosty", promotesTo: ["nope"] }]));
	expect((ghost as AxisRegistryError).code).toBe("unknown_promotion_target");
});

test("A6 axes.json that is valid JSON but the wrong shape fails closed", async () => {
	const bodies = [
		'{"axes":{}}',
		'{"axes":[null]}',
		'{"axes":[[]]}',
		'{"axes":["ops"]}',
		'{"axes":[42]}',
		"[]",
		'"string"',
		"null",
		"42",
		"true",
		"",
		"   ",
		'\uFEFF{"axes":[]}',
		'{"axes":[{"id":"ok","root":"ok"},null]}',
		'{"axes":[{"id":"ok","root":"ok"}],"axes":[{"id":"ok"}]}',
	];
	for (const body of bodies) {
		await sandbox(async (_home, root) => {
			await declare(root, body);
			let failure: unknown;
			try {
				await loadRegistry(root);
			} catch (error) {
				failure = error;
			}
			if (body === '{"axes":[{"id":"ok","root":"ok"}],"axes":[{"id":"ok"}]}') {
				// Duplicate JSON keys: last-wins per JSON semantics, leaving one legal
				// axis. Accepting it is correct; the point is that it is deterministic.
				expect(failure).toBeUndefined();
				expect((await loadRegistry(root)).byId("ok")?.root).toBe("ok");
				return;
			}
			expect(failure).toBeInstanceOf(AxisRegistryError);
			expect((failure as AxisRegistryError).code).toBe("malformed_descriptor");
		});
	}
	// A large but legal array is accepted without quadratic collapse.
	const huge = Array.from({ length: 500 }, (_, index) => ({ ...BASE, id: `bulk-${index}`, root: `bulk-${index}` }));
	expect(createRegistry(huge).axes.length).toBe(509);
});

test("A7 a rejected registry creates nothing on disk", async () => {
	await sandbox(async (home, root) => {
		await declare(root, '{"axes":[{"id":"good","root":"good"},{"id":"bad","root":"../escape"}]}');
		const before = await fingerprint(root);
		expect(initializeMemory(home)).rejects.toThrow(AxisRegistryError);
		await Bun.sleep(20);
		const after = await fingerprint(root);
		expect([...after.keys()].sort()).toEqual([...before.keys()].sort());
		expect(after.has("good")).toBe(false);
		expect(after.has("daily")).toBe(false);
		expect(after.has("MEMORY.md")).toBe(false);
	});
});

test("A8 an over-long axis id/root fails closed rather than blowing up mid-migration", async () => {
	const long = "a".repeat(4096);
	await sandbox(async (home, root) => {
		await declare(root, { axes: [{ ...BASE, id: long, root: long, partitions: [] as string[] }] });
		let failure: unknown;
		try {
			await initializeMemory(home);
		} catch (error) {
			failure = error;
		}
		expect(failure).toBeInstanceOf(AxisRegistryError);
	});
});

// ============================================================ B. migration

const EXOTIC: readonly (readonly [string, string])[] = [
	["ops/rules/index.md", "# Rule index\n\n- [restart](restart.md)\n"],
	["ops/rules/restart.md", "# Restart\n\nDrain first.\n"],
	["daily/2026-08-01.md", "# capture\n\n- user: hello\n"],
	["daily/2026-08/디지털-한글.md", "# 한글 제목\n\n요약 라인\n"],
	["people/name with spaces.md", "# spaced\n\nbody\n"],
	["projects/no-trailing-newline.md", "# no newline at eof"],
	["decisions/emoji-🚀.md", "# rocket\n"],
	["channels/CRLF.md", "# crlf\r\n\r\nline\r\n"],
];

async function legacyCorpus(root: string): Promise<void> {
	for (const axis of ["daily", "events", "tasks", "people", "projects", "channels", "decisions"])
		await mkdir(join(root, axis), { recursive: true, mode: 0o700 });
	await mkdir(join(root, "ops/rules"), { recursive: true, mode: 0o700 });
	await mkdir(join(root, "daily/2026-08"), { recursive: true, mode: 0o700 });
	for (const [path, body] of EXOTIC) await writeFile(join(root, path), body);
	await writeFile(join(root, "tasks/big.md"), `# big\n\n${"lorem ipsum dolor ".repeat(58000)}\n`);
	await symlink("../people/name with spaces.md", join(root, "projects/link-to-person.md")).catch(() => {});
	await writeFile(
		join(root, "MEMORY.md"),
		"# Memory map\n\n## daily\n\n- [daily/2026-08-01.md](daily/2026-08-01.md)\n",
	);
}

test("B1 migration is byte-lossless across unicode, spaces, huge, CRLF and symlinked files", async () => {
	await sandbox(async (home, root) => {
		await legacyCorpus(root);
		const before = await fingerprint(root);
		expect(before.get("tasks/big.md")).toBeDefined();

		await initializeMemory(home);
		const after = await fingerprint(root);

		for (const [path, hash] of before) {
			expect(after.has(path)).toBe(true);
			if (path !== "MEMORY.md") expect(`${path}=${after.get(path)}`).toBe(`${path}=${hash}`);
		}
		// Migration invents no Markdown of its own beyond the generated map.
		for (const [path, kind] of after) if (!before.has(path)) expect(kind === "dir" || path === "MEMORY.md").toBe(true);

		const map = await readFile(join(root, "MEMORY.md"), "utf8");
		expect(map).toContain("## ops");
		expect(map).toContain("## reflections");
		expect(map).toContain("ops/rules/index.md");
	});
});

test("B2a five concurrent cold-start initializeMemory calls must all succeed", async () => {
	// server.ts calls initializeMemory on EVERY memory.audit / memory.search RPC and
	// closure.ts calls it per queued intent, so concurrent calls are the normal case.
	await sandbox(async (home, root) => {
		const results = await Promise.allSettled([1, 2, 3, 4, 5].map(() => initializeMemory(home)));
		const rejected = results.filter((result) => result.status === "rejected");
		expect(rejected.map((result) => String((result as PromiseRejectedResult).reason))).toEqual([]);
		expect(await readFile(join(root, "MEMORY.md"), "utf8")).toContain("# Memory map");
	});
});

test("B2b concurrent warm initializeMemory calls never tear MEMORY.md", async () => {
	await sandbox(async (home, root) => {
		await initializeMemory(home); // .git and every axis directory already exist
		await rm(join(root, "MEMORY.md"));
		await writeFile(join(root, "daily/2026-08-27.md"), "# capture\n\nbody\n");
		const results = await Promise.allSettled([1, 2, 3, 4, 5].map(() => initializeMemory(home)));
		expect(results.filter((result) => result.status === "rejected").length).toBe(0);

		const map = await readFile(join(root, "MEMORY.md"), "utf8");
		// A torn or interleaved write shows up as a duplicated or truncated block.
		expect(map.split("# Memory map").length - 1).toBe(1);
		expect(map.split("## daily").length - 1).toBe(1);
		for (const axis of ["daily", "ops", "reflections", "decisions"]) expect(map).toContain(`## ${axis}`);
		expect(map.endsWith("\n")).toBe(true);
		expect(codes(await validateMemory(root))).toEqual([]);
	});
});

test("B3 an `ops` FILE where the axis directory belongs fails loudly and keeps the bytes", async () => {
	await sandbox(async (home, root) => {
		await mkdir(root, { recursive: true, mode: 0o700 });
		await writeFile(join(root, "ops"), "human wrote this as a file\n");
		let failure: unknown;
		try {
			await initializeMemory(home);
		} catch (error) {
			failure = error;
		}
		expect(failure).toBeDefined();
		expect(String((failure as NodeJS.ErrnoException).code)).toMatch(/EEXIST|ENOTDIR/);
		expect(await readFile(join(root, "ops"), "utf8")).toBe("human wrote this as a file\n");
	});
});

test("B4 an axis root that is a symlink is still audited", async () => {
	await sandbox(async (home, root) => {
		await mkdir(join(home, "elsewhere/rules"), { recursive: true });
		await writeFile(join(home, "elsewhere/rules/smuggled.md"), "# smuggled\n\nnever audited?\n");
		await writeFile(join(home, "elsewhere/loose.md"), "# loose\n\noutside every partition\n");
		await mkdir(root, { recursive: true, mode: 0o700 });
		await symlink(join(home, "elsewhere"), join(root, "ops"));

		await initializeMemory(home);
		await regenerateMap(root);
		const map = await readFile(join(root, "MEMORY.md"), "utf8");
		const issues = await validateMemory(root);

		// The index and the audit must agree: a file the map serves and recall
		// returns must also be a file the audit inspects. `ops/loose.md` sits outside
		// every declared ops partition, so a consistent corpus must report it.
		const indexed =
			map.includes("ops/rules/smuggled.md") ||
			(await searchMemory(root, "smuggled never audited", 10)).some((hit) => hit.path === "ops/rules/smuggled.md");
		const audited = issues.some((issue) => issue.path === "ops/loose.md");
		expect({ indexed, audited }).toEqual({ indexed: true, audited: true });
	});
});

test("B5 a read-only memory root fails closed instead of half-migrating", async () => {
	await sandbox(async (home, root) => {
		await mkdir(root, { recursive: true, mode: 0o700 });
		await chmod(root, 0o500);
		let failure: unknown;
		try {
			await initializeMemory(home);
		} catch (error) {
			failure = error;
		}
		await chmod(root, 0o700);
		expect(failure).toBeDefined();
		expect(String((failure as NodeJS.ErrnoException).code)).toMatch(/EACCES|EPERM|EROFS/);
		expect(await readdir(root)).toEqual([]);
	});
});

test("B6 a custom axis rooted on a directory of pre-existing human files adopts them", async () => {
	await sandbox(async (home, root) => {
		await declare(root, { axes: [{ ...BASE, id: "runbooks", root: "runbooks", partitions: ["staging"] }] });
		await mkdir(join(root, "runbooks/staging/deep/deeper"), { recursive: true });
		await writeFile(join(root, "runbooks/staging/deep/deeper/failover.md"), "# Failover\n\nPromote the standby.\n");
		await writeFile(join(root, "runbooks/staging/notes.md"), "# notes\n\nhand written\n");
		const before = await fingerprint(root);

		await initializeMemory(home);
		const after = await fingerprint(root);
		for (const [path, hash] of before) if (path !== "MEMORY.md") expect(after.get(path)).toBe(hash);

		const map = await readFile(join(root, "MEMORY.md"), "utf8");
		expect(map).toContain("## runbooks");
		expect(map).toContain("runbooks/staging/deep/deeper/failover.md");
		expect(codes(await validateMemory(root))).toEqual([]);
	});
});

// ================================================================ C. audit

test("C1 orphans at root and nested keep failing and are never auto-promoted", async () => {
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		await mkdir(join(root, "scratch/notes/deep"), { recursive: true });
		await writeFile(join(root, "stray.md"), "# stray\n\nroot level\n");
		await writeFile(join(root, "scratch/notes/deep/idea.md"), "# idea\n\nnested\n");
		await regenerateMap(root);

		for (let round = 0; round < 3; round++) {
			const orphans = (await validateMemory(root)).filter((issue) => issue.code === "orphan_file");
			expect(orphans.map((issue) => issue.path).sort()).toEqual(["scratch/notes/deep/idea.md", "stray.md"]);
			expect(orphans[0].message).toContain(REGISTRY_FILE);
			await initializeMemory(home);
			await regenerateMap(root);
			expect(await readFile(join(root, "MEMORY.md"), "utf8")).not.toContain("## scratch");
		}
	});
});

test("C2 each layout policy produces exactly one issue with the right code and path", async () => {
	// per-subject reflections file
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		await writeFile(join(root, "reflections/tone.md"), "# tone\n\nbe terse\n");
		await regenerateMap(root);
		expect(codes(await validateMemory(root))).toEqual(["axis_layout_violation:reflections/tone.md"]);
	});
	// reflections nested one level too deep
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		await mkdir(join(root, "reflections/2026-08/extra"), { recursive: true });
		await writeFile(join(root, "reflections/2026-08/extra/2026-08-27.md"), "# deep\n\nbody\n");
		await regenerateMap(root);
		expect(codes(await validateMemory(root))).toEqual([
			"axis_layout_violation:reflections/2026-08/extra/2026-08-27.md",
		]);
	});
	// unpartitioned ops file
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		await writeFile(join(root, "ops/scratch.md"), "# scratch\n\nnot in a partition\n");
		await regenerateMap(root);
		expect(codes(await validateMemory(root))).toEqual(["axis_layout_violation:ops/scratch.md"]);
	});
	// an undeclared ops partition
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		await mkdir(join(root, "ops/secrets"), { recursive: true });
		await writeFile(join(root, "ops/secrets/keys.md"), "# keys\n\nnope\n");
		await regenerateMap(root);
		expect(codes(await validateMemory(root))).toEqual(["axis_layout_violation:ops/secrets/keys.md"]);
	});
	// subdirectory under a flat axis
	await sandbox(async (home, root) => {
		await declare(root, {
			axes: [{ ...BASE, id: "flatx", root: "flatx", nesting: "flat", partitions: [], orphanPolicy: "any-depth" }],
		});
		await initializeMemory(home);
		await mkdir(join(root, "flatx/sub"), { recursive: true });
		await writeFile(join(root, "flatx/ok.md"), "# ok\n\nflat entry\n");
		await writeFile(join(root, "flatx/sub/nope.md"), "# nope\n\nnested under a flat axis\n");
		await regenerateMap(root);
		expect(codes(await validateMemory(root))).toEqual(["axis_layout_violation:flatx/sub/nope.md"]);
	});
});

test("C3 lookalike paths cannot smuggle a file into an axis", async () => {
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		// `ops/../scratch/x.md` resolves to an unregistered directory.
		await mkdir(join(root, "ops/../scratch"), { recursive: true });
		await writeFile(join(root, "ops/../scratch/x.md"), "# escaped\n\none\n");
		// A directory whose name only looks like an axis.
		await mkdir(join(root, "ops "), { recursive: true });
		await writeFile(join(root, "ops /trailing.md"), "# trailing space\n\ntwo\n");
		await mkdir(join(root, "opsx"), { recursive: true });
		await writeFile(join(root, "opsx/prefix.md"), "# prefix\n\nthree\n");
		// `./ops/x.md` is just `ops/x.md`: a real, unpartitioned ops file.
		await writeFile(join(root, "./ops/x.md"), "# dot slash\n\nfour\n");
		// An axis-named directory nested inside another axis.
		await mkdir(join(root, "daily/ops/rules"), { recursive: true });
		await writeFile(join(root, "daily/ops/rules/impostor.md"), "# impostor\n\nfive\n");
		await regenerateMap(root);

		const issues = await validateMemory(root);
		expect(codes(issues)).toEqual(
			[
				"orphan_file:scratch/x.md",
				"orphan_file:ops /trailing.md",
				"orphan_file:opsx/prefix.md",
				"axis_layout_violation:ops/x.md",
			].sort(),
		);
		// The impostor belongs to daily (any-depth) and must not be treated as ops.
		expect(issues.some((issue) => issue.path === "daily/ops/rules/impostor.md")).toBe(false);
	});
});

test("C4 map_content_drift fires and clears for every append-only axis and never spuriously", async () => {
	await sandbox(async (home, root) => {
		await declare(root, {
			axes: [
				{
					...BASE,
					id: "journal",
					root: "journal",
					nesting: "nested",
					partitions: [],
					orphanPolicy: "any-depth",
					layout: "dated",
					appendOnly: true,
					index: "recent",
				},
				{
					...BASE,
					id: "quiet",
					root: "quiet",
					nesting: "nested",
					partitions: [],
					orphanPolicy: "any-depth",
					layout: "dated",
					appendOnly: true,
					index: "recent",
				},
			],
		});
		await initializeMemory(home);
		// Empty append-only axes must not drift.
		expect((await validateMemory(root)).filter((issue) => issue.code === "map_content_drift")).toEqual([]);

		const appendOnly = (await loadRegistry(root)).axes.filter((axis) => axis.appendOnly);
		expect(appendOnly.map((axis) => axis.id).sort()).toEqual(["daily", "journal", "quiet", "reflections"]);

		for (const axis of appendOnly) {
			const name = axis.layout === "dated" ? "2099-01-01.md" : "2099-01-01.md";
			await writeFile(join(root, axis.root, name), `# newest ${axis.id}\n\nbody\n`);
			const drift = (await validateMemory(root)).filter((issue) => issue.code === "map_content_drift");
			expect(drift.map((issue) => issue.message)).toEqual([`map does not include newest ${axis.id} file`]);
			await regenerateMap(root);
			expect((await validateMemory(root)).filter((issue) => issue.code === "map_content_drift")).toEqual([]);
		}
		// `quiet` stayed empty until its turn, proving no cross-axis bleed.
		expect((await validateMemory(root)).filter((issue) => issue.code !== "duplicate_file_hash")).toEqual([]);
	});
});

// ============================================================ D. retrieval

test("D1 recall reaches a deeply nested custom-axis file with no map pointer at all", async () => {
	await sandbox(async (home, root) => {
		await declare(root, { axes: [{ ...BASE, id: "runbooks", root: "runbooks", partitions: ["staging"] }] });
		await initializeMemory(home);
		await mkdir(join(root, "runbooks/staging/a/b/c/d"), { recursive: true });
		await writeFile(
			join(root, "runbooks/staging/a/b/c/d/failover.md"),
			"# Failover\n\nPromote the standby socket before draining the primary.\n",
		);
		// Deliberately do NOT regenerate the map.
		expect(await readFile(join(root, "MEMORY.md"), "utf8")).not.toContain("failover.md");

		const hits = await searchMemory(root, "standby socket draining primary", 10);
		expect(hits.map((hit) => hit.path)).toContain("runbooks/staging/a/b/c/d/failover.md");
	});
});

test("D2 the retrievalPriority tiebreak is real and follows the registry, not the path", async () => {
	await sandbox(async (home, root) => {
		const body = "# drain\n\ndrain the queue before restart\n";
		await declare(root, {
			axes: [
				{
					...BASE,
					id: "alpha",
					root: "alpha",
					nesting: "nested",
					partitions: [],
					orphanPolicy: "any-depth",
					retrievalPriority: 5,
				},
				{
					...BASE,
					id: "zulu",
					root: "zulu",
					nesting: "nested",
					partitions: [],
					orphanPolicy: "any-depth",
					retrievalPriority: 95,
				},
			],
		});
		await initializeMemory(home);
		await writeFile(join(root, "alpha/doc.md"), body);
		await writeFile(join(root, "zulu/doc.md"), body);
		await regenerateMap(root);

		const query = "drain the queue before restart";
		const high = await searchMemory(root, query, 10);
		const both = high.filter((hit) => hit.path === "alpha/doc.md" || hit.path === "zulu/doc.md");
		expect(both.length).toBe(2);
		expect(both[0].score).toBe(both[1].score);
		expect(both[0].path).toBe("zulu/doc.md");

		// Flip the declared priorities: ordering must flip with them.
		const flipped = createRegistry([
			{
				...BASE,
				id: "alpha",
				root: "alpha",
				nesting: "nested",
				partitions: [],
				orphanPolicy: "any-depth",
				retrievalPriority: 95,
			},
			{
				...BASE,
				id: "zulu",
				root: "zulu",
				nesting: "nested",
				partitions: [],
				orphanPolicy: "any-depth",
				retrievalPriority: 5,
			},
		]);
		const low = (await searchMemory(root, query, 10, flipped)).filter(
			(hit) => hit.path === "alpha/doc.md" || hit.path === "zulu/doc.md",
		);
		expect(low[0].path).toBe("alpha/doc.md");
	});
});

test("D3 pathological queries neither crash nor return junk", async () => {
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		await writeFile(join(root, "ops/rules/drain.md"), "# drain\n\ndrain the queue before restart\n");
		await regenerateMap(root);

		for (const query of ["", "   ", "\n\t\r ", ".*+?[](){}|^$\\", "((((", "\u0000", "🚀🚀🚀"])
			expect(await searchMemory(root, query, 10)).toEqual([]);

		expect((await searchMemory(root, "x".repeat(10_000), 10)).length).toBe(0);
		expect((await searchMemory(root, `drain ${"queue ".repeat(2000)}`, 10)).length).toBeGreaterThan(0);
		expect((await searchMemory(root, "드레인 대기열 drain", 10)).map((hit) => hit.path)).toContain(
			"ops/rules/drain.md",
		);
		expect(await searchMemory(root, "drain", 0)).toEqual([]);
		expect((await searchMemory(root, "drain", -5)).length).toBe(0);
		expect((await searchMemory(root, "drain", 10_000)).length).toBeLessThanOrEqual(50);
	});
});

test("D4 recall degrades rather than throwing when the map holds a dangling pointer", async () => {
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		await writeFile(join(root, "ops/rules/drain.md"), "# drain\n\ndrain the queue before restart\n");
		await regenerateMap(root);
		// A dangling pointer is a state the audit merely REPORTS (map_dangling), so
		// recall must survive it: the corpus is not corrupt, only stale.
		const map = await readFile(join(root, "MEMORY.md"), "utf8");
		await writeFile(join(root, "MEMORY.md"), `${map}\n- [ops/rules/gone.md](ops/rules/gone.md)\n`);
		expect((await validateMemory(root)).some((issue) => issue.code === "map_dangling")).toBe(true);

		const hits = await searchMemory(root, "drain the queue before restart", 10);
		expect(hits.map((hit) => hit.path)).toContain("ops/rules/drain.md");
	});
});

test("D5 a map pointer cannot make recall read outside the memory root", async () => {
	await sandbox(async (home, root) => {
		await initializeMemory(home);
		await writeFile(join(home, "secret.md"), "# secret\n\nthe drain password is hunter2\n");
		const map = await readFile(join(root, "MEMORY.md"), "utf8");
		await writeFile(
			join(root, "MEMORY.md"),
			`${map}\n- [a](%2e%2e/secret.md)\n- [b](daily/../../secret.md)\n- [c](/etc/hosts.md)\n`,
		);
		// Containment is the property under test here, kept independent of the D4
		// defect: if recall throws it must be an ENOENT for a path INSIDE the root,
		// and it must never surface content from outside the corpus.
		const outcome = await searchMemory(root, "hunter2 drain password", 10).catch((error) => error as Error);
		if (outcome instanceof Error) {
			const failed = (outcome as NodeJS.ErrnoException).path ?? "";
			expect((outcome as NodeJS.ErrnoException).code).toBe("ENOENT");
			expect(failed.startsWith(root)).toBe(true);
			expect(failed.includes("/memory/secret.md")).toBe(false);
		} else {
			expect(outcome.map((hit) => hit.path).filter((path) => path.includes("secret"))).toEqual([]);
			expect(outcome.map((hit) => hit.excerpt).join(" ")).not.toContain("hunter2");
		}
		await lstat(join(home, "secret.md"));
	});
});
