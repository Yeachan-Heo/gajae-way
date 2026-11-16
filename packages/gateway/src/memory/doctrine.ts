import type { Dirent } from "node:fs";
import {
	appendFile,
	mkdir,
	mkdtemp,
	readdir,
	readFile,
	realpath,
	rename,
	rm,
	stat,
	unlink,
	writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join, normalize, relative } from "node:path";
import { GIT_COMMAND_TIMEOUT_MS } from "../orchestrator/git-command";
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
		// Callers match git's English diagnostics ("nothing to commit", "does not
		// have any commits"). macOS Homebrew git follows the system language even
		// with LANG unset, so the locale is pinned rather than inherited.
		LC_ALL: "C",
		GIT_AUTHOR_NAME: "gajaeway",
		GIT_AUTHOR_EMAIL: "gajaeway@local",
		GIT_COMMITTER_NAME: "gajaeway",
		GIT_COMMITTER_EMAIL: "gajaeway@local",
	};
}

export class CorpusWriter {
	readonly #root: string;
	constructor(root: string) {
		this.#root = root;
	}
	async stageFiles(...paths: string[]): Promise<void> {
		for (const path of paths) await memoryGit(this.#root, ["add", path]);
	}
	async commit(message: string, trailer?: string): Promise<string | undefined> {
		const msg = trailer ? `${message}\n\n${trailer}` : message;
		try {
			await memoryGit(this.#root, ["commit", "-m", msg]);
			return (await memoryGit(this.#root, ["rev-parse", "HEAD"])).trim();
		} catch (error) {
			if (error instanceof Error && error.message.includes("nothing to commit")) return undefined;
			throw error;
		}
	}
	async findCommitByTrailer(trailer: string): Promise<string | undefined> {
		try {
			const output = await memoryGit(this.#root, ["log", "--format=%H", "--grep", trailer]);
			return output.split("\n").find(Boolean);
		} catch (error) {
			if (error instanceof Error && error.message.includes("does not have any commits")) return undefined;
			throw error;
		}
	}
}

/**
 * Deadline for one memory git command. Memory git is local work on the corpus
 * (init, add, commit, log, rev-parse), the same class of command as the work
 * lane's local git, so it gets the same budget (#615): far above what a healthy
 * command takes on a large corpus, yet a hook, filter or helper that never
 * returns can no longer hold the memory FIFO, and every intent behind it, forever.
 */
export const MEMORY_GIT_TIMEOUT_MS = GIT_COMMAND_TIMEOUT_MS;
/** How long a killed shell gets to be reaped before the call gives up waiting on Bun's exit report. */
const MEMORY_GIT_REAP_GRACE_MS = 1_000;

export function memoryGit(
	root: string,
	args: readonly string[],
	timeoutMs: number = MEMORY_GIT_TIMEOUT_MS,
): Promise<string> {
	return memoryGitUnserialized(root, args, timeoutMs);
}

export class MemoryGitLockedError extends Error {
	readonly path: string;
	readonly ageMs: number | undefined;

	constructor(path: string, ageMs?: number) {
		super(`memory git blocked by index lock ${path}${ageMs === undefined ? "" : ` (age ${ageMs}ms)`}`);
		this.name = "MemoryGitLockedError";
		this.path = path;
		this.ageMs = ageMs;
	}
}

/**
 * A memory git command that outlived {@link MEMORY_GIT_TIMEOUT_MS}; its process
 * group was killed. Transient like an index lock: the intent stays open for a
 * later attempt rather than being quarantined.
 */
export class MemoryGitTimeoutError extends Error {
	readonly command: string;
	readonly timeoutMs: number;

	constructor(args: readonly string[], timeoutMs: number) {
		super(
			`memory git ${args[0]} timed out after ${timeoutMs}ms (argv: ${formatArgv(args)}); its process group was killed`,
		);
		this.name = "MemoryGitTimeoutError";
		this.command = args[0] ?? "git";
		this.timeoutMs = timeoutMs;
	}
}

/**
 * Runs git with its output in files and its exit status in a third file, written
 * (via rename, so it is complete when it appears) by the shell that ran git.
 *
 * Under Bun 1.4.0 a child's completion was seen never to reach the event loop: a
 * gateway e2e teardown hung in the memory drain behind a `git init` that had
 * exited (a zombie nobody reaped, `exitCode` still null, `exited` never settling),
 * and in other runs behind an output pipe whose end-of-file never arrived after
 * git exited 0. Either one wedged the memory FIFO, and with it every later intent
 * and gateway shutdown, for good. So completion is the exit file appearing, polled
 * alongside `exited`, and no result waits on a pipe: a helper that inherits git's
 * output cannot hold it open past git's exit either. A git that never exits at all
 * is bounded by the deadline: the shell leads its own process group, and the whole
 * group is SIGKILLed when the deadline passes.
 */
const MEMORY_GIT_SCRIPT =
	'out=$1 err=$2 status=$3; shift 3; git "$@" >"$out" 2>"$err"; code=$?; printf %s "$code" >"$status.tmp" && /bin/mv "$status.tmp" "$status"; exit "$code"';
const MEMORY_GIT_POLL_MIN_MS = 5;
const MEMORY_GIT_POLL_MAX_MS = 100;

/**
 * Process groups of memory git commands still in flight. Running detached takes git
 * out of the gateway's own process group, so a supervisor that cleans up by group
 * (launchd) no longer reaches it, and the deadline timer dies with the process. A
 * gateway that exits mid-command (its shutdown deadline, or a normal return) kills
 * them itself, so no stuck git outlives it and writes the corpus under the next boot.
 */
const liveMemoryGitGroups = new Set<number>();
process.on("exit", () => {
	for (const group of liveMemoryGitGroups) killGroup(group);
});

async function memoryGitUnserialized(root: string, args: readonly string[], timeoutMs: number): Promise<string> {
	const deadline = performance.now() + timeoutMs;
	const scratch = await mkdtemp(join(tmpdir(), "gajaeway-memory-git-"));
	let group: number | undefined;
	try {
		const stdoutPath = join(scratch, "stdout");
		const stderrPath = join(scratch, "stderr");
		const statusPath = join(scratch, "status");
		// posix_spawn can transiently fail with ENOENT/EAGAIN on a busy host even
		// though the executable exists (observed under parallel test load); one
		// bounded retry of spawning the shell keeps a durable closure from failing on
		// a scheduler hiccup. git's own exit status is never retried: it may have done work.
		let child: ReturnType<typeof Bun.spawn>;
		for (let attempt = 0; ; attempt++) {
			try {
				child = Bun.spawn(["/bin/sh", "-c", MEMORY_GIT_SCRIPT, "sh", stdoutPath, stderrPath, statusPath, ...args], {
					cwd: root,
					env: gitEnv(),
					stdin: "ignore",
					stdout: "ignore",
					stderr: "ignore",
					detached: true,
				});
				group = child.pid;
				liveMemoryGitGroups.add(group);
				break;
			} catch (error) {
				if (attempt > 0) throw error;
				await Bun.sleep(50);
			}
		}
		let stopped = false;
		// Kill git with every hook, filter and helper it started, and give the shell a
		// bounded moment to be reaped (Bun may never report it).
		const stop = async (): Promise<void> => {
			stopped = true;
			killGroup(child.pid);
			await Promise.race([child.exited, Bun.sleep(MEMORY_GIT_REAP_GRACE_MS)]);
			child.unref();
		};
		let code: number | undefined;
		try {
			const status = await memoryGitStatus(child, statusPath, deadline);
			if (status === undefined) {
				await stop();
				// git may have finished between the last poll and the kill; its recorded status is then the truth.
				code = await readStatus(statusPath);
				if (code === undefined) throw new MemoryGitTimeoutError(args, timeoutMs);
			} else {
				code = status.code;
				// The shell died without git's status, so git and its helpers may still be running in the group.
				if (!status.recorded) killGroup(child.pid);
			}
		} catch (error) {
			// A status that cannot be read says nothing about whether git is done: a git
			// still running must not outlive this call and write the corpus under a later intent.
			if (!stopped) await stop();
			throw error;
		}
		// Nothing of this command is left to kill at exit; the shell may be reaped and its pid reused.
		liveMemoryGitGroups.delete(child.pid);
		// Once the status is known nothing here waits on the shell again; a child whose
		// exit Bun never reports must not keep the event loop alive either.
		child.unref();
		const [stdout, stderr] = await Promise.all([
			readIfPresent(stdoutPath).then((text) => text ?? ""),
			readIfPresent(stderrPath).then((text) => text ?? ""),
		]);
		if (code === 0) return stdout.trim();
		const lockPath = join(root, ".git", "index.lock");
		if (stderr.includes("Unable to create") && stderr.includes(".git/index.lock") && stderr.includes("File exists")) {
			let ageMs: number | undefined;
			try {
				ageMs = Math.max(0, Date.now() - (await stat(lockPath)).mtimeMs);
			} catch {
				// The path is still part of git's diagnostic even if it raced away.
			}
			throw new MemoryGitLockedError(lockPath, ageMs);
		}
		throw new Error(gitFailureMessage(args, code, stdout, stderr));
	} finally {
		if (group !== undefined) liveMemoryGitGroups.delete(group);
		// A scratch dir that cannot be removed is a leak to report, never a reason to replace git's result.
		await rm(scratch, { recursive: true, force: true }).catch((error: unknown) => {
			console.warn(
				`memory git scratch dir ${scratch} not removed: ${error instanceof Error ? error.message : String(error)}`,
			);
		});
	}
}

/**
 * A file the shell may never have created (killed first) reads as absent; any other
 * read failure is raised, never mistaken for empty output (an empty `log --grep`
 * means "no such commit" to the closure, which would then commit twice).
 */
async function readIfPresent(path: string): Promise<string | undefined> {
	try {
		return await readFile(path, "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw error;
	}
}

async function readStatus(statusPath: string): Promise<number | undefined> {
	const text = await readIfPresent(statusPath);
	if (text === undefined) return undefined;
	// Written whole via rename, so anything but an exit code is a broken invariant.
	if (!/^\d+$/.test(text)) throw new Error(`memory git status file is malformed: ${JSON.stringify(text)}`);
	return Number(text);
}

/**
 * SIGKILLs the process group a memory git shell leads. Only the group is signalled,
 * never the bare pid: the shell may already be reaped and its pid reused, while a
 * group id stays reserved as long as any member lives.
 */
function killGroup(group: number): void {
	try {
		process.kill(-group, "SIGKILL");
	} catch {
		// No member left.
	}
}

/**
 * git's exit status: the status file once it appears, else the shell's own exit if
 * that is seen first, else `undefined` once `deadline` (a `performance.now()` time) passes.
 * `recorded` says whether git itself recorded it: only that proves git finished.
 */
async function memoryGitStatus(
	child: ReturnType<typeof Bun.spawn>,
	statusPath: string,
	deadline: number,
): Promise<{ readonly code: number; readonly recorded: boolean } | undefined> {
	let exited: number | undefined;
	void child.exited.then((code) => {
		exited = code;
	});
	for (let delay = MEMORY_GIT_POLL_MIN_MS; ; delay = Math.min(delay * 2, MEMORY_GIT_POLL_MAX_MS)) {
		const status = await readStatus(statusPath);
		if (status !== undefined) return { code: status, recorded: true };
		// The shell ended without recording git's status (killed before it could).
		if (exited !== undefined) {
			const late = await readStatus(statusPath);
			return late === undefined ? { code: exited, recorded: false } : { code: late, recorded: true };
		}
		const remaining = deadline - performance.now();
		if (remaining <= 0) return undefined;
		await Promise.race([child.exited, Bun.sleep(Math.min(delay, remaining))]);
	}
}

/**
 * A git failure must carry its reason. `commit` with nothing staged, for one,
 * explains itself on stdout with an empty stderr, so a stderr-only message was
 * the bare `memory git commit failed:` a live gateway died on (#192); the exit
 * status and argv separate that from an index lock, a hook rejection or a
 * detached worktree.
 */
function gitFailureMessage(args: readonly string[], code: number, stdout: string, stderr: string): string {
	const reason = [stderr.trim(), stdout.trim()].filter(Boolean).join(" | ") || "(no output)";
	return `memory git ${args[0]} failed (exit ${code}, argv: ${formatArgv(args)}): ${reason}`;
}

function formatArgv(args: readonly string[]): string {
	return ["git", ...args].map((arg) => (/^[\w./=:@-]+$/.test(arg) ? arg : `'${arg.replace(/'/g, "'\\''")}'`)).join(" ");
}

/**
 * Whether `root/.git` holds what git needs to recognize a repository. A bare `.git`
 * directory is not enough: a `git init` killed at its deadline leaves one behind,
 * and every later command in it fails as "not a git repository".
 *
 * A gitfile (linked worktree, separate git dir) is never re-initialized, which would
 * rewrite settings such as core.filemode; git itself must accept what it points at,
 * or this throws: init cannot repair it, and initialization fails before recovery
 * can quarantine an intent over it.
 */
async function repositoryComplete(root: string): Promise<boolean> {
	const dotGit = join(root, ".git");
	let isGitfile: boolean;
	try {
		isGitfile = (await stat(dotGit)).isFile();
	} catch {
		return false;
	}
	if (isGitfile) {
		await memoryGit(root, ["rev-parse", "--git-dir"]);
		return true;
	}
	const checks = await Promise.all(
		["HEAD", "objects", "refs"].map((entry) =>
			stat(join(dotGit, entry)).then(
				() => true,
				() => false,
			),
		),
	);
	return checks.every(Boolean);
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
	if (!(await repositoryComplete(root))) {
		// Also repairs a `.git` a killed `git init` left half-written: re-running init on an
		// existing repository only adds what is missing and never touches history.
		try {
			await memoryGit(root, ["init"]);
		} catch (error) {
			// Another *process* can initialize the same corpus in the window between
			// the check and the call. A repository that exists by now is the outcome
			// we wanted; anything else is a real failure. A timed-out init is our own
			// git, killed part-way, so whatever it left is never taken as a repository.
			// A gitfile that fails validation here keeps init's own error as the diagnostic.
			if (error instanceof MemoryGitTimeoutError || !(await repositoryComplete(root).catch(() => false))) throw error;
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
	return regenerateMapFiles(root, registry);
}

async function regenerateMapFiles(root: string, registry?: AxisRegistry): Promise<void> {
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
	intentId?: string,
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
	const intentMarker = intentId ? `- intent-id: ${intentId}\n` : "";
	const entry = `\n## ${new Date().toISOString()}\n\n- origin: ${bounded(originRefJson)}\n${intentMarker}- user: ${bounded(userText)}\n- reply: ${bounded(replyText)}\n`;
	await appendFile(path, entry, { encoding: "utf8" });
	await regenerateMap(root, registry);
	return relative(root, path).replaceAll("\\", "/");
}
