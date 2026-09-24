import type { Dirent } from "node:fs";
import { appendFile, mkdir, readdir, readFile, realpath, rename, stat, unlink, writeFile } from "node:fs/promises";
import { isAbsolute, join, normalize, relative } from "node:path";
import {
	type AxisDescriptor,
	type AxisRegistry,
	loadRegistry,
	NAVIGATION_SOURCE_MAX_BYTES,
	RECENT_INDEX_CAP,
	TREE_INDEX_CAP,
} from "./registry";

export function memoryRoot(home: string): string {
	return join(home, "memory");
}

function gitEnv(): Record<string, string> {
	return {
		PATH: process.env.PATH ?? "/usr/bin:/bin",
		HOME: process.env.HOME ?? "/tmp",
		GIT_AUTHOR_NAME: "gajaeway",
		GIT_AUTHOR_EMAIL: "gajaeway@local",
		GIT_COMMITTER_NAME: "gajaeway",
		GIT_COMMITTER_EMAIL: "gajaeway@local",
	};
}

/**
 * One git operation per corpus at a time. The closure queue (intent commits),
 * the autolink sweep and any other in-process writer each run `add` then
 * `commit` as separate commands on the SAME repository; interleaved, one of
 * them finds the other's `.git/index.lock` and fails (live: 100 `memory git add
 * failed: index.lock exists`, gaebal, 2026-09-05). Git's lock is per-process,
 * not per-caller, so the serialization has to be ours.
 */
const gitChains = new Map<string, Promise<void>>();

/** Runs `work` after every earlier call on the same `root` in `chains` has settled. */
async function serializedOnRoot<T>(
	chains: Map<string, Promise<void>>,
	root: string,
	work: () => Promise<T>,
): Promise<T> {
	const previous = chains.get(root) ?? Promise.resolve();
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const chain = previous.then(() => gate);
	chains.set(root, chain);
	await previous;
	try {
		return await work();
	} finally {
		release();
		if (chains.get(root) === chain) chains.delete(root);
	}
}

export function memoryGit(root: string, args: readonly string[]): Promise<string> {
	return serializedOnRoot(gitChains, root, () => memoryGitUnserialized(root, args));
}

/** git's lock file; an orphan (no live git on this repo) blocks every later write until removed by hand. */
const INDEX_LOCK_RE = /Unable to create '(.+?\/\.git\/index\.lock)': File exists/;

async function memoryGitUnserialized(root: string, args: readonly string[]): Promise<string> {
	// posix_spawn can transiently fail with ENOENT/EAGAIN on a busy host even
	// though git exists (observed under parallel test load); one bounded retry
	// keeps a durable closure from failing on a scheduler hiccup.
	let lockCleared = false;
	for (let attempt = 0; ; attempt++) {
		let child: ReturnType<typeof Bun.spawn>;
		try {
			child = Bun.spawn(["git", ...args], { cwd: root, env: gitEnv(), stdout: "pipe", stderr: "pipe" });
		} catch (error) {
			if (attempt === 0) {
				await Bun.sleep(50);
				continue;
			}
			throw error;
		}
		const [stdout, stderr, code] = await Promise.all([
			new Response(child.stdout as ReadableStream).text(),
			new Response(child.stderr as ReadableStream).text(),
			child.exited,
		]);
		if (code === 0) return stdout.trim();
		// Serialized above, so a lock we run into was left by a git that died
		// (killed mid-commit on a restart, or a persona tool call). Wait long
		// enough for a legitimately running git to finish, then treat a lock
		// that is still there and older than that wait as orphaned. Once.
		const lockPath = INDEX_LOCK_RE.exec(stderr)?.[1];
		if (lockPath && !lockCleared) {
			lockCleared = true;
			await Bun.sleep(ORPHANED_LOCK_GRACE_MS);
			try {
				const age = Date.now() - (await stat(lockPath)).mtimeMs;
				if (age >= ORPHANED_LOCK_GRACE_MS) {
					await unlink(lockPath);
					console.error(`memory git: removed orphaned ${lockPath} (age ${Math.round(age / 1000)}s)`);
					continue;
				}
			} catch {
				// Gone meanwhile: the retry below decides.
				continue;
			}
		}
		throw new Error(`memory git ${args[0]} failed: ${stderr.trim()}`);
	}
}

/** Longer than any git op on a memory corpus should take; shorter than a monitor tick. */
const ORPHANED_LOCK_GRACE_MS = 5_000;

/**
 * Bring a corpus up to the current axis set, creating only what is missing.
 *
 * Serialized per root by `initializeMemory`: adapters, monitors and the closure
 * queue all initialize, and concurrent cold starts otherwise raced on `git init`
 * (git refuses to copy its templates over an existing `info/exclude`, so four of
 * five callers failed outright) and on the generated map, where simultaneous
 * writes can tear.
 */
async function initializeCorpus(root: string): Promise<string> {
	await mkdir(root, { recursive: true, mode: 0o700 });
	// A malformed registry is fatal here rather than ignored: dropping a declared
	// axis would turn every file beneath it into an orphan on the next audit.
	const registry = await loadRegistry(root);
	// Migration for a corpus written under an older axis set: creating the missing
	// axis directory is the only structural write, and nothing a human wrote is
	// read, moved, rewritten or deleted.
	for (const axis of registry.axes) {
		await mkdir(join(root, axis.root), { recursive: true, mode: 0o700 });
		for (const partition of axis.partitions)
			await mkdir(join(root, axis.root, partition), { recursive: true, mode: 0o700 });
	}
	try {
		await stat(join(root, ".git"));
	} catch {
		try {
			await memoryGit(root, ["init"]);
		} catch (error) {
			// Another *process* can initialize the same corpus in the window between
			// the check and the call. A repository that exists by now is the outcome
			// we wanted; anything else is a real failure.
			await stat(join(root, ".git")).catch(() => {
				throw error;
			});
		}
	}
	// Whether the map covers the current axis set is a question about state, not
	// about what this particular call happened to create. Keying the repair on
	// "mkdir made a directory" left a corpus stuck: an operator who pre-creates the
	// new axis directories by hand, or a `git init` that throws between the mkdir
	// loop and the regeneration, gets a map with no heading for the new axis and
	// every later startup concludes there is nothing to do, so `memory.audit`
	// reports unmapped_axis_dir indefinitely on a corpus nobody mistreated.
	// The map is generated navigation, so regenerating it is repair, not data loss.
	let map: string | undefined;
	try {
		map = await readFile(join(root, "MEMORY.md"), "utf8");
	} catch {
		map = undefined;
	}
	if (map === undefined || registry.axes.some((axis) => !mapListsAxis(map, axis))) await regenerateMap(root, registry);
	return root;
}

/** Per-root serialization for `initializeMemory`. */
const initializations = new Map<string, Promise<unknown>>();

/**
 * Bring the corpus for `home` up to the current axis set. Concurrent callers are
 * serialized per root and each observes the same coherent result. A failure is
 * not contagious: the next waiter runs regardless of how the previous one ended.
 */
export function initializeMemory(home: string): Promise<string> {
	const root = memoryRoot(home);
	const previous = initializations.get(root) ?? Promise.resolve();
	const next = previous.then(
		() => initializeCorpus(root),
		() => initializeCorpus(root),
	);
	const settled = next.catch(() => {});
	initializations.set(root, settled);
	void settled.then(() => {
		if (initializations.get(root) === settled) initializations.delete(root);
	});
	return next;
}

/**
 * Every Markdown file beneath `directory`, root-relative and sorted.
 *
 * The single traversal the whole memory system shares. Index generation, audit
 * and recall must see exactly the same set of files: when they disagreed, a file
 * the map served was a file the audit never inspected, which is precisely how an
 * unpartitioned rule smuggled itself past the layout gate (live QA finding:
 * a symlinked axis root was indexed but not audited).
 *
 * Always recursive, because an axis nests (`daily/2026-08/`, `ops/rules/`); a
 * flat walk made nested files permanently invisible while every capture
 * regenerated the drift back. Directory symlinks are followed, since an operator
 * who roots an axis at a symlink still expects its contents audited, and
 * `realpath` bookkeeping stops a symlink cycle from looping forever. Symlinked
 * *files* are skipped: they are a second name for content that is already
 * indexed under its real path.
 */
async function markdownFiles(base: string, directory: string, seen: Set<string>): Promise<string[]> {
	let entries: Dirent<string>[];
	try {
		const real = await realpath(directory);
		if (seen.has(real)) return [];
		seen.add(real);
		entries = await readdir(directory, { withFileTypes: true });
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "ENOENT" || code === "ENOTDIR") return [];
		throw error;
	}
	const result: string[] = [];
	for (const entry of entries) {
		if (entry.name === ".git") continue;
		const path = join(directory, entry.name);
		let directoryEntry = entry.isDirectory();
		if (!directoryEntry && entry.isSymbolicLink()) {
			try {
				directoryEntry = (await stat(path)).isDirectory();
			} catch {
				continue; // A broken symlink is nothing to walk into and nothing to index.
			}
		}
		if (directoryEntry) result.push(...(await markdownFiles(base, path, seen)));
		else if (entry.isFile() && entry.name.endsWith(".md")) result.push(relative(base, path).replaceAll("\\", "/"));
	}
	return result.sort();
}

/** Every Markdown file in the corpus, including `MEMORY.md`, root-relative. */
export function corpusEntries(root: string): Promise<string[]> {
	return markdownFiles(root, root, new Set());
}

/** Every Markdown entry of one axis, root-relative and sorted. */
export function axisEntries(root: string, axis: AxisDescriptor): Promise<string[]> {
	return markdownFiles(root, join(root, axis.root), new Set());
}

/**
 * The root-relative target of a link, or undefined when it may not be followed.
 *
 * `from` is the root-relative directory the link is written in, so a link inside
 * a file resolves the way a reader would follow it. Percent-escapes are decoded
 * before the confinement test, so `%2e%2e/secret.md` cannot smuggle a traversal
 * past a literal `..` check, and an absolute link is refused outright rather
 * than being quietly reinterpreted as corpus-relative.
 */
export function safePointer(root: string, pointer: string, from = "."): string | undefined {
	let decoded = pointer;
	try {
		decoded = decodeURIComponent(pointer);
	} catch {
		// Malformed escapes: judge the literal text rather than trusting a decode.
	}
	if (decoded.includes("\u0000") || isAbsolute(decoded)) return undefined;
	const target = relative(root, normalize(join(root, from, decoded)));
	if (!target || target.startsWith("..") || isAbsolute(target)) return undefined;
	return target.replaceAll("\\", "/");
}

/**
 * Whether a map body carries the generated section heading for an axis. Anchored
 * to the start of a line, because a `tree` index emits `### <partition>` and an
 * unanchored test would let `### rules` satisfy an axis whose id is `rules`.
 */
export function mapListsAxis(map: string, axis: AxisDescriptor): boolean {
	return new RegExp(`(?:^|\\n)## ${axis.id}(?:\\n|$)`).test(map);
}

/**
 * MEMORY.md contains navigation only; its pointers are regenerated from the
 * canonical tree. Each axis is indexed the way its descriptor asks: a `recent`
 * axis lists its newest entries, a `tree` axis lists its whole hierarchy grouped
 * by partition, so a routable axis stays navigable instead of scrolling off the
 * newest-20 window. Under the shared source ceiling, allowances shrink fairly
 * and a trimmed axis points at an existing index when one is available. No axis
 * is special-cased by id.
 */
export function regenerateMap(root: string, registry?: AxisRegistry): Promise<void> {
	// One regeneration per corpus at a time: the closure queue, `initializeMemory`
	// and the autolink sweep all regenerate the same map, and two overlapping
	// renders of a corpus that changes between them must not interleave (#227).
	return serializedOnRoot(mapChains, root, () => regenerateMapUnserialized(root, registry));
}

const mapChains = new Map<string, Promise<void>>();

async function regenerateMapUnserialized(root: string, registry?: AxisRegistry): Promise<void> {
	const axes = (registry ?? (await loadRegistry(root))).axes;
	const files = await Promise.all(axes.map((axis) => axisEntries(root, axis)));
	const indexed = axes.map((axis, index) => {
		const entries =
			axis.index === "recent" ? files[index].slice(-RECENT_INDEX_CAP).reverse() : files[index].slice(0, TREE_INDEX_CAP);
		// An index is an existing corpus file, never a path invented by the map.
		// Prefer the axis root's conventional index and accept a nested index when
		// that is the only one available (for example ops/rules/index.md).
		const indexPath =
			files[index].find((path) => path === `${axis.root}/index.md`) ??
			files[index].find((path) => path.endsWith("/index.md"));
		return { axis, allFiles: files[index], entries, indexPath };
	});

	const render = (allowances: readonly number[], compact: boolean): string => {
		const lines = compact
			? ["# Memory map"]
			: ["# Memory map", "", "Generated pointers; canonical facts live in axis files.", ""];
		for (const [index, descriptor] of indexed.entries()) {
			const { axis, allFiles, entries, indexPath } = descriptor;
			if (compact) lines.push(`## ${axis.id}`);
			else lines.push(`## ${axis.id}`, "", `_${axis.displayName}_`, "");
			// Promotion targets are navigation too: the writer canonicalising a daily
			// capture or a reflection reads this map, so where a fact promotes to has to
			// be visible here rather than only in the descriptor.
			if (!compact && axis.promotesTo.length) lines.push(`_promotes to: ${axis.promotesTo.join(", ")}_`, "");
			const allowance = allowances[index] ?? 0;
			if (axis.index === "recent") {
				for (const path of entries.slice(0, allowance)) lines.push(`- [${path}](${path})`);
			} else {
				let group = "";
				for (const path of entries.slice(0, allowance)) {
					const branch = path
						.slice(axis.root.length + 1)
						.split("/")
						.slice(0, -1)
						.join("/");
					if (branch !== group) {
						group = branch;
						lines.push("", `### ${branch || axis.root}`, "");
					}
					lines.push(`- [${path}](${path})`);
				}
			}
			const omitted = allFiles.length - allowance;
			if (allowance < entries.length) {
				const noun = omitted === 1 ? "entry" : "entries";
				if (indexPath) {
					lines.push(`_${omitted} ${noun} omitted; see ${indexPath}_`);
					lines.push(`- [${indexPath}](${indexPath})`);
				} else lines.push(`_${omitted} ${noun} omitted; no index entry found_`);
			}
			if (!compact) lines.push("");
		}
		return `${lines.join("\n")}\n`;
	};

	const fit = (compact: boolean): string => {
		const allowances = indexed.map(({ entries }) => entries.length);
		let map = render(allowances, compact);
		if (Buffer.byteLength(map, "utf8") <= NAVIGATION_SOURCE_MAX_BYTES) return map;
		// Shrink the highest per-axis allowance first. Once caps converge, ties are
		// reduced round-robin, so pressure cannot make one registered axis disappear
		// while later axes retain their full allowance.
		outer: while (Buffer.byteLength(map, "utf8") > NAVIGATION_SOURCE_MAX_BYTES) {
			let reduced = false;
			const highest = allowances.reduce((max, allowance) => Math.max(max, allowance), 0);
			for (const index of allowances.keys()) {
				if (allowances[index] !== highest || highest === 0) continue;
				allowances[index]--;
				reduced = true;
				map = render(allowances, compact);
				if (Buffer.byteLength(map, "utf8") <= NAVIGATION_SOURCE_MAX_BYTES) break outer;
			}
			if (!reduced) break;
		}
		return map;
	};

	let map = fit(false);
	// The normal renderer is byte-compatible with the historical small-corpus map.
	// If even empty sections cannot fit because a deployment registered unusually
	// long labels, retry with the same headings and compact section scaffolding.
	if (Buffer.byteLength(map, "utf8") > NAVIGATION_SOURCE_MAX_BYTES) map = fit(true);
	if (Buffer.byteLength(map, "utf8") > NAVIGATION_SOURCE_MAX_BYTES) throw new Error("memory_map_exceeds_byte_budget");
	// Written through a temp file and renamed, because `memory.audit` and
	// `memory.search` both read the map on request paths that can run while a
	// capture regenerates it: a truncate-then-write would let a reader observe a
	// half-written map and report spurious dangling pointers or missing axes. The
	// temp name deliberately does not end in `.md`, so no walker ever indexes it.
	// The name is unique per write: another gateway process on the same corpus
	// (an adopted orphan, #183) is outside the in-process serialization, and a
	// shared name let the loser's rename hit ENOENT after the winner moved it (#227).
	const target = join(root, "MEMORY.md");
	const staging = `${target}.${process.pid}.${crypto.randomUUID()}.staging`;
	try {
		await writeFile(staging, map);
		await rename(staging, target);
	} catch (error) {
		await unlink(staging).catch(() => {});
		throw error;
	}
}

/**
 * The root the capture axis is registered at; `daily/` unless a deployment
 * re-rooted it. The registry always carries the axis, because a declaration can
 * only add or override one, so its absence is a corrupt registry rather than a
 * corpus that opted out of capture.
 */
export async function captureRoot(root: string, registry?: AxisRegistry): Promise<string> {
	const axis = (registry ?? (await loadRegistry(root))).byId("daily");
	if (!axis) throw new Error("memory registry carries no daily capture axis");
	return axis.root;
}

export async function appendDaily(
	root: string,
	originRefJson: string,
	userText: string,
	replyText: string,
): Promise<string> {
	const date = new Date().toISOString().slice(0, 10);
	// Resolved, never hardcoded: a deployment that re-roots the capture axis in
	// axes.json would otherwise append into a directory nothing creates, and every
	// turn would lose its capture to ENOENT.
	const registry = await loadRegistry(root);
	const path = join(root, await captureRoot(root, registry), `${date}.md`);
	// Each field must stay on one physical line: a captured newline would let reply text
	// containing "## " forge an entry delimiter and escape its own list item.
	const bounded = (text: string) =>
		text
			.slice(0, 500)
			.replaceAll("\u0000", "")
			.replaceAll(/\r\n|\r|\n/g, "\\n");
	const entry = `\n## ${new Date().toISOString()}\n\n- origin: ${bounded(originRefJson)}\n- user: ${bounded(userText)}\n- reply: ${bounded(replyText)}\n`;
	await appendFile(path, entry, { encoding: "utf8" });
	await regenerateMap(root, registry);
	return relative(root, path).replaceAll("\\", "/");
}
