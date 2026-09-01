import { readFile, writeFile } from "node:fs/promises";
import { basename, dirname, join, relative } from "node:path";
import { corpusEntries, memoryGit, regenerateMap } from "./doctrine";
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

export interface AutolinkReport {
	readonly filesChanged: number;
	readonly linksAdded: number;
	readonly aliases: number;
}

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
	root: string,
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

export function autolinkText(
	text: string,
	selfPath: string,
	index: readonly AliasEntry[],
): { text: string; added: number } {
	// Single pass over the file: protected ranges are computed ONCE and edits are
	// collected first, then applied back-to-front so offsets stay valid. The
	// original per-alias rescan was O(aliases x text) and blocked the gateway
	// event loop for minutes on a 2,400-file corpus (live gaebal-gajae incident:
	// 90%+ CPU, status/audit timeouts while the sweep ran).
	const ranges = protectedRanges(text);
	const edits: Array<{ start: number; end: number; target: string }> = [];
	const taken: Array<[number, number]> = [];
	for (const entry of index) {
		if (entry.path === selfPath) continue;
		const target = relative(join(selfPath, ".."), entry.path).replaceAll("\\", "/");
		// One link per target per file, ever: a file that already links this
		// canonical note (by hand or by an earlier sweep) is left alone. This is
		// what makes the sweep idempotent and keeps prose from turning blue.
		if (text.includes(`](${target})`) || edits.some((edit) => edit.target === target)) continue;
		const pattern = new RegExp(entry.alias.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "giu");
		for (const match of text.matchAll(pattern)) {
			const start = match.index;
			const end = start + match[0].length;
			if (inRanges(ranges, start, end) || inRanges(taken, start, end)) continue;
			// Boundaries: prefix-strict (no letter/digit before, so "가재" never
			// matches inside "웨이가재"), suffix Latin-strict only — Korean particles
			// attach directly to the noun ("개발가재를"), so a CJK suffix is a valid
			// mention edge while a Latin/digit suffix is a longer identifier.
			const before = text[start - 1] ?? " ";
			const after = text[end] ?? " ";
			if (/[\p{L}\p{N}_]/u.test(before) || /[A-Za-z0-9_]/.test(after)) continue;
			edits.push({ start, end, target });
			taken.push([start, end]);
			break; // first occurrence per alias per file
		}
	}
	let out = text;
	for (const edit of edits.sort((a, b) => b.start - a.start))
		out = `${out.slice(0, edit.start)}[${out.slice(edit.start, edit.end)}](${edit.target})${out.slice(edit.end)}`;
	return { text: out, added: edits.length };
}

export async function autolinkCorpus(root: string): Promise<AutolinkReport> {
	const registry = await loadRegistry(root);
	const raw = new Set(registry.byPriority.filter((axis) => axis.id === "daily").map((axis) => axis.root));
	const files = (await corpusEntries(root)).filter(
		(path) => path !== "MEMORY.md" && ![...raw].some((prefix) => path.startsWith(`${prefix}/`)),
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
	let linksAdded = 0;
	for (let position = 0; position < files.length; position++) {
		const path = files[position] as string;
		const text = texts.get(path);
		if (text === undefined) continue;
		const { text: rewritten, added } = autolinkText(text, path, index);
		if (added > 0) {
			await writeFile(join(root, path), rewritten);
			filesChanged++;
			linksAdded += added;
		}
		// Yield the event loop regularly: the sweep must never freeze live turns.
		if (position % 20 === 19) await Bun.sleep(0);
	}
	if (filesChanged > 0) {
		await regenerateMap(root, registry);
		try {
			await memoryGit(root, ["add", "-A"]);
			await memoryGit(root, ["commit", "-m", `Memory autolink sweep: ${linksAdded} links in ${filesChanged} files`]);
		} catch {
			// A bare tree without prior commits still gets the file changes.
		}
	}
	return { filesChanged, linksAdded, aliases: index.length };
}
