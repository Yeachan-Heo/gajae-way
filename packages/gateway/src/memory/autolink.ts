import { readFile, writeFile } from "node:fs/promises";
import { basename, dirname, join, relative } from "node:path";
import type { MemoryAutolinkResult } from "@gajaeway/protocol";
import { corpusEntries, memoryGit } from "./doctrine";
import { loadRegistry } from "./registry";

/**
 * Deterministic crosslinker for the canonical memory corpus.
 *
 * The LLM canonicalization pass was asked to wrap entity mentions in links and
 * did it unevenly (live backfill: good on one host, thin on the others). The
 * mechanical part — "this exact name has a canonical file, link its first
 * mention" — needs no model at all, so it runs as a deterministic sweep:
 *
 * - Every canonical (non-raw-daily) note contributes aliases: its filename
 *   stem, its first `# heading` text, and any `aliases:` list in frontmatter.
 * - Every note gets the FIRST plain-text occurrence of each alias wrapped in a
 *   relative Markdown link (one link per alias per file, longest alias first).
 * - Protected regions are never rewritten: YAML frontmatter, fenced code
 *   blocks, inline code, existing links, and headings.
 *
 * The model's job shrinks to supplying good aliases/tags metadata; the wiring
 * itself is reproducible and idempotent.
 */

export interface AliasEntry {
	readonly alias: string;
	readonly path: string;
}

export type AutolinkReport = MemoryAutolinkResult;

const MIN_ALIAS_LENGTH = 3;

/** Frontmatter block at the very top of a note, if any. */
function frontmatterEnd(text: string): number {
	if (!text.startsWith("---\n")) return 0;
	const end = text.indexOf("\n---", 4);
	if (end === -1) return 0;
	const lineEnd = text.indexOf("\n", end + 4);
	return lineEnd === -1 ? text.length : lineEnd + 1;
}

export function frontmatterList(text: string, key: string): string[] {
	const end = frontmatterEnd(text);
	if (!end) return [];
	const block = text.slice(0, end);
	// Inline list: `key: [a, b]`
	const inline = block.match(new RegExp(`^${key}:\\s*\\[([^\\]]*)\\]`, "m"));
	if (inline?.[1] !== undefined)
		return inline[1]
			.split(",")
			.map((item) => item.trim().replace(/^["']|["']$/g, ""))
			.filter(Boolean);
	// Block list: `key:` followed by `- item` lines.
	const at = block.match(new RegExp(`^${key}:\\s*$`, "m"));
	if (!at || at.index === undefined) return [];
	const items: string[] = [];
	for (const line of block.slice(at.index + at[0].length).split("\n")) {
		const item = line.match(/^\s*-\s+(.+?)\s*$/);
		if (!item) {
			if (line.trim() === "") continue;
			break;
		}
		items.push((item[1] as string).replace(/^["']|["']$/g, ""));
	}
	return items;
}

function headingTitle(text: string): string | undefined {
	const match = text.slice(frontmatterEnd(text)).match(/^#\s+(.+?)\s*$/m);
	return match?.[1];
}

function normalizeAlias(alias: string): string {
	return alias.trim().toLowerCase();
}

export async function buildAliasIndex(
	_root: string,
	files: readonly string[],
	texts: ReadonlyMap<string, string>,
): Promise<AliasEntry[]> {
	const entries = new Map<string, AliasEntry>();
	const claim = (alias: string, path: string) => {
		const normalized = normalizeAlias(alias);
		if (normalized.length < MIN_ALIAS_LENGTH) return;
		// Generic words make terrible link anchors; an alias that two files claim
		// is ambiguous and dropped entirely rather than linked arbitrarily.
		const existing = entries.get(normalized);
		if (existing && existing.path !== path) entries.set(normalized, { alias: normalized, path: "" });
		else entries.set(normalized, { alias: normalized, path });
	};
	for (const path of files) {
		const text = texts.get(path);
		if (text === undefined) continue;
		const stem = basename(path, ".md");
		if (!/^\d{4}-\d{2}(-\d{2})?$/.test(stem) && stem !== "index") claim(stem, path);
		const title = headingTitle(text);
		// Dated event titles ("Event — 2026-08-27 ...") are sentences, not names.
		if (title && title.length <= 40 && !/\d{4}-\d{2}-\d{2}/.test(title)) claim(title, path);
		for (const alias of frontmatterList(text, "aliases")) claim(alias, path);
	}
	return [...entries.values()]
		.filter((entry) => entry.path !== "")
		.sort((a, b) => b.alias.length - a.alias.length || a.alias.localeCompare(b.alias));
}

/** Ranges of `text` that must never be rewritten. */
function protectedRanges(text: string): Array<[number, number]> {
	const ranges: Array<[number, number]> = [];
	const front = frontmatterEnd(text);
	if (front) ranges.push([0, front]);
	for (const match of text.matchAll(/```[\s\S]*?(?:```|$)/g)) ranges.push([match.index, match.index + match[0].length]);
	for (const match of text.matchAll(/`[^`\n]*`/g)) ranges.push([match.index, match.index + match[0].length]);
	for (const match of text.matchAll(/\[[^\]\n]*\]\([^)\n]*\)/g))
		ranges.push([match.index, match.index + match[0].length]);
	for (const match of text.matchAll(/^#{1,6}\s.*$/gm)) ranges.push([match.index, match.index + match[0].length]);
	return ranges;
}

function inRanges(ranges: ReadonlyArray<[number, number]>, start: number, end: number): boolean {
	return ranges.some(([from, to]) => start < to && end > from);
}

function escapePattern(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function autolinkText(
	text: string,
	selfPath: string,
	index: readonly AliasEntry[],
): { text: string; added: number } {
	const ranges = protectedRanges(text);
	const eligible: Array<AliasEntry & { target: string }> = [];
	const byAlias = new Map<string, AliasEntry & { target: string }>();
	for (const entry of index) {
		if (entry.path === selfPath) continue;
		const target = relative(dirname(selfPath), entry.path).replaceAll("\\", "/");
		if (text.includes(`](${target})`)) continue;
		const candidate = { ...entry, target };
		eligible.push(candidate);
		byAlias.set(entry.alias, candidate);
	}
	if (eligible.length === 0) return { text, added: 0 };

	// One alternation scans the body once. The former alias-by-alias regex loop
	// was O(aliases × file bytes), monopolising the gateway event loop on the
	// production corpus even though the surrounding file loop yielded.
	const pattern = new RegExp(eligible.map((entry) => escapePattern(entry.alias)).join("|"), "giu");
	const first = new Map<string, { start: number; end: number }>();
	for (const match of text.matchAll(pattern)) {
		const alias = normalizeAlias(match[0]);
		if (first.has(alias) || !byAlias.has(alias)) continue;
		const start = match.index;
		const end = start + match[0].length;
		if (inRanges(ranges, start, end)) continue;
		const before = text[start - 1] ?? " ";
		const after = text[end] ?? " ";
		if (/[\p{L}\p{N}_]/u.test(before) || /[A-Za-z0-9_]/.test(after)) continue;
		first.set(alias, { start, end });
	}

	const edits: Array<{ start: number; end: number; target: string }> = [];
	const taken: Array<[number, number]> = [];
	const targets = new Set<string>();
	for (const entry of eligible) {
		const match = first.get(entry.alias);
		if (!match || targets.has(entry.target) || inRanges(taken, match.start, match.end)) continue;
		edits.push({ ...match, target: entry.target });
		taken.push([match.start, match.end]);
		targets.add(entry.target);
	}
	let out = text;
	for (const edit of edits.sort((a, b) => b.start - a.start))
		out = `${out.slice(0, edit.start)}[${out.slice(edit.start, edit.end)}](${edit.target})${out.slice(edit.end)}`;
	return { text: out, added: edits.length };
}

const sweeps = new Map<string, Promise<AutolinkReport>>();

function dirtyCorpusPaths(status: string): Set<string> {
	const dirty = new Set<string>();
	const records = status.split("\0");
	for (let index = 0; index < records.length; index++) {
		const record = records[index];
		if (!record) continue;
		const state = record.slice(0, 2);
		dirty.add(record.slice(3));
		if (state.includes("R") || state.includes("C")) {
			const source = records[++index];
			if (source) dirty.add(source);
		}
	}
	return dirty;
}

async function runAutolinkCorpus(root: string): Promise<AutolinkReport> {
	const started = Date.now();
	const runId = crypto.randomUUID();
	const registry = await loadRegistry(root);
	const raw = new Set(registry.byPriority.filter((axis) => axis.appendOnly).map((axis) => axis.root));
	const files = (await corpusEntries(root)).filter(
		(path) => path !== "MEMORY.md" && ![...raw].some((prefix) => path.startsWith(`${prefix}/`)),
	);
	const dirty = dirtyCorpusPaths(
		await memoryGit(root, ["status", "--porcelain=v1", "-z", "--untracked-files=all", "--", "."]),
	);
	const texts = new Map<string, string>();
	for (const path of files) {
		try {
			texts.set(path, await readFile(join(root, path), "utf8"));
		} catch {
			// A mapped file may already be gone; the audit reports that separately.
		}
	}
	const index = await buildAliasIndex(root, files, texts);
	let filesChanged = 0;
	let filesSkippedDirty = files.filter((path) => dirty.has(path)).length;
	let filesScanned = 0;
	let linksAdded = 0;
	const changed: Array<{ path: string; rewritten: string; added: number }> = [];
	for (let position = 0; position < files.length; position++) {
		const path = files[position] as string;
		if (dirty.has(path)) continue;
		const text = texts.get(path);
		if (text === undefined) continue;
		filesScanned++;
		const { text: rewritten, added } = autolinkText(text, path, index);
		if (added > 0) {
			// Do not overwrite a capture/closure/user edit that landed after the
			// snapshot. A later sweep can reconsider the now-current file.
			if ((await readFile(join(root, path), "utf8")) !== text) {
				filesSkippedDirty++;
				continue;
			}
			await writeFile(join(root, path), rewritten);
			filesChanged++;
			linksAdded += added;
			changed.push({ path, rewritten, added });
		}
		// Yield the event loop regularly: the sweep must never freeze live turns.
		if (position % 20 === 19) await Bun.sleep(0);
	}
	if (changed.length > 0) {
		const committable: string[] = [];
		for (const change of changed) {
			if ((await readFile(join(root, change.path), "utf8")) === change.rewritten) committable.push(change.path);
			else {
				filesChanged--;
				linksAdded -= change.added;
				filesSkippedDirty++;
			}
		}
		if (committable.length > 0)
			// A path-limited commit neither sweeps unrelated untracked files nor
			// consumes another writer's staged work. All changed paths were clean at
			// snapshot time, so an existing user edit is never folded into this commit.
			await memoryGit(root, [
				"commit",
				"--only",
				"-m",
				`Memory autolink sweep: ${linksAdded} links in ${filesChanged} files`,
				"--",
				...committable,
			]);
	}
	const completed = Date.now();
	return {
		runId,
		startedAt: new Date(started).toISOString(),
		completedAt: new Date(completed).toISOString(),
		durationMs: completed - started,
		filesScanned,
		filesChanged,
		filesSkippedDirty,
		linksAdded,
		aliases: index.length,
	};
}

/** Concurrent requests for one corpus attach to the same sweep and receipt. */
export function autolinkCorpus(root: string): Promise<AutolinkReport> {
	const active = sweeps.get(root);
	if (active) return active;
	const sweep = runAutolinkCorpus(root);
	sweeps.set(root, sweep);
	const clear = () => {
		if (sweeps.get(root) === sweep) sweeps.delete(root);
	};
	void sweep.then(clear, clear);
	return sweep;
}
