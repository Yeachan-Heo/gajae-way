import type { Dirent } from "node:fs";
import { appendFile, mkdir, readdir, readFile, realpath, rename, stat, writeFile } from "node:fs/promises";
import { isAbsolute, join, normalize, relative } from "node:path";
import { type AxisDescriptor, type AxisRegistry, loadRegistry, RECENT_INDEX_CAP, TREE_INDEX_CAP } from "./registry";

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

export async function memoryGit(root: string, args: readonly string[]): Promise<string> {
	// posix_spawn can transiently fail with ENOENT/EAGAIN on a busy host even
	// though git exists (observed under parallel test load); one bounded retry
	// keeps a durable closure from failing on a scheduler hiccup.
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
		if (code !== 0) throw new Error(`memory git ${args[0]} failed: ${stderr.trim()}`);
		return stdout.trim();
	}
}

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
 * newest-20 window. No axis is special-cased by id.
 */
export async function regenerateMap(root: string, registry?: AxisRegistry): Promise<void> {
	const axes = (registry ?? (await loadRegistry(root))).axes;
	const files = await Promise.all(axes.map((axis) => axisEntries(root, axis)));
	const lines = ["# Memory map", "", "Generated pointers; canonical facts live in axis files.", ""];
	for (const [index, axis] of axes.entries()) {
		lines.push(`## ${axis.id}`, "", `_${axis.displayName}_`, "");
		// Promotion targets are navigation too: the writer canonicalising a daily
		// capture or a reflection reads this map, so where a fact promotes to has to
		// be visible here rather than only in the descriptor.
		if (axis.promotesTo.length) lines.push(`_promotes to: ${axis.promotesTo.join(", ")}_`, "");
		if (axis.index === "recent") {
			for (const path of files[index].slice(-RECENT_INDEX_CAP).reverse()) lines.push(`- [${path}](${path})`);
		} else {
			let group = "";
			for (const path of files[index].slice(0, TREE_INDEX_CAP)) {
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
		lines.push("");
	}
	// Written through a temp file and renamed, because `memory.audit` and
	// `memory.search` both read the map on request paths that can run while a
	// capture regenerates it: a truncate-then-write would let a reader observe a
	// half-written map and report spurious dangling pointers or missing axes. The
	// temp name deliberately does not end in `.md`, so no walker ever indexes it.
	const target = join(root, "MEMORY.md");
	const staging = `${target}.staging`;
	await writeFile(staging, `${lines.join("\n")}\n`);
	await rename(staging, target);
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
