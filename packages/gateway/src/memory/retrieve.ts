import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { frontmatterList } from "./autolink";
import { axisEntries, safePointer } from "./doctrine";
import { type AxisRegistry, loadRegistry } from "./registry";

export interface MemoryHit {
	path: string;
	score: number;
	excerpt: string;
}
// CJK text is unsegmented/agglutinative: a whole-word tokenizer makes the
// inflected form ("정본화를") a different term from its stem ("정본화"), so
// Korean queries silently missed. CJK words therefore index their character
// bigrams alongside the whole word, symmetrically for documents and queries.
const CJK_RE = /[\u1100-\u11ff\u3040-\u30ff\u3130-\u318f\u4e00-\u9fff\uac00-\ud7af]/u;
const words = (text: string): string[] => {
	const terms = text.toLowerCase().match(/[\p{L}\p{N}_]+/gu) ?? [];
	const out: string[] = [];
	for (const term of terms) {
		out.push(term);
		if (term.length >= 2 && CJK_RE.test(term))
			for (let index = 0; index < term.length - 1; index++) out.push(term.slice(index, index + 2));
	}
	return out;
};

/** Declared topic labels: frontmatter `tags:` plus inline `#tag` (Obsidian syntax). */
export function documentTags(text: string): string[] {
	const found = new Set<string>(frontmatterList(text, "tags").map((tag) => tag.toLowerCase().replace(/^#/, "")));
	for (const match of text.matchAll(/(?:^|\s)#([\p{L}\p{N}_/-]{2,})/gu)) {
		const tag = (match[1] as string).toLowerCase();
		// Markdown headings arrive as "# Title" with a space and never match; pure
		// numbers ("#1234", issue refs) are not topic labels.
		if (!/^\d+$/.test(tag)) found.add(tag);
	}
	return [...found];
}

async function pointers(root: string): Promise<string[]> {
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	// A hand-edited map is untrusted input: a pointer may only ever name a file
	// inside the corpus, and `safePointer` decodes escapes before deciding, so
	// `%2e%2e/secret.md` cannot walk out of the root behind a literal `..` check.
	return [...map.matchAll(/\]\(([^)]+\.md)\)/g)]
		.map((match) => safePointer(root, match[1]))
		.filter((path): path is string => path !== undefined);
}

async function axisFiles(registry: AxisRegistry, root: string): Promise<string[]> {
	// Recursive per axis, because a routable axis nests: ops keeps a rule index
	// plus rule packs under ops/rules/, and a flat walk made every rule pack
	// unsearchable unless the map happened to still point at it. Custom axes are
	// walked by exactly the same code as the built-ins.
	const perAxis = await Promise.all(registry.axes.map((axis) => axisEntries(root, axis)));
	return perAxis.flat().sort();
}

export async function searchMemory(
	root: string,
	query: string,
	limit = 10,
	registry?: AxisRegistry,
): Promise<MemoryHit[]> {
	const resolved = registry ?? (await loadRegistry(root));
	const priority = (path: string) => resolved.axisForPath(path)?.retrievalPriority ?? 0;
	const mapped = await pointers(root);
	const mappedSet = new Set(mapped);
	const files = [...mapped, ...(await axisFiles(resolved, root)).filter((path) => !mappedSet.has(path))];
	const terms = words(query);
	if (!terms.length) return [];
	// A pointer the map still advertises may already be gone; the audit reports
	// that as `map_dangling`, which means the corpus is stale rather than corrupt.
	// Recall must therefore degrade to the files it can actually read instead of
	// failing the whole query on one missing document.
	const documents = (
		await Promise.all(
			files.map(async (path) => {
				try {
					return { path, text: await readFile(join(root, path), "utf8") };
				} catch {
					return undefined;
				}
			}),
		)
	).filter((document): document is { path: string; text: string } => document !== undefined);
	// Obsidian-style tags (frontmatter `tags:` and inline `#tag`) are declared
	// topic labels, not incidental prose: their tokens are weighted 3x so a
	// query naming a tag ranks tagged notes above ones that merely mention it.
	const tags = documents.map((document) => documentTags(document.text));
	const tokens = documents.map((document, index) => {
		const base = words(document.text);
		for (const tag of tags[index] as string[]) for (let boost = 0; boost < 2; boost++) base.push(...words(tag));
		return base;
	});
	const averageLength = tokens.reduce((sum, document) => sum + document.length, 0) / (tokens.length || 1);
	const documentFrequency = new Map<string, number>();
	for (const document of tokens)
		for (const term of new Set(document)) documentFrequency.set(term, (documentFrequency.get(term) ?? 0) + 1);
	const scored = documents
		.map((document, index) => {
			const counts = new Map<string, number>();
			for (const term of tokens[index]) counts.set(term, (counts.get(term) ?? 0) + 1);
			const score = terms.reduce((total, term) => {
				const frequency = counts.get(term) ?? 0;
				if (!frequency) return total;
				const idf = Math.log(
					1 +
						(documents.length - (documentFrequency.get(term) ?? 0) + 0.5) / ((documentFrequency.get(term) ?? 0) + 0.5),
				);
				return (
					total +
					idf * ((frequency * 2.2) / (frequency + 1.2 * (1 - 0.75 + (0.75 * tokens[index].length) / averageLength)))
				);
			}, 0);
			const lower = document.text.toLowerCase();
			const match =
				terms
					.map((term) => lower.indexOf(term))
					.filter((at) => at >= 0)
					.sort((a, b) => a - b)[0] ?? 0;
			return {
				path: document.path,
				score,
				excerpt: document.text.slice(Math.max(0, match - 50), Math.max(0, match - 50) + 200).replace(/\s+/g, " "),
			};
		})
		.filter((hit) => hit.score > 0)
		// Equal-scoring documents are ordered by the axis's declared retrieval
		// priority, so operating doctrine outranks raw capture on a tie instead of
		// whichever path happens to sort first.
		.sort(
			(a, b) =>
				b.score - a.score ||
				priority(b.path) - priority(a.path) ||
				(mappedSet.has(b.path) ? 1 : 0) - (mappedSet.has(a.path) ? 1 : 0) ||
				a.path.localeCompare(b.path),
		);
	const cap = Math.max(0, Math.min(50, Math.floor(limit)));
	const top = scored.slice(0, cap);
	// Link expansion: canonical notes crosslink related people/projects/decisions,
	// so a strong hit pulls its 1-hop neighbours in as discounted secondary hits.
	// That stitches context BM25 alone cannot see (the neighbour may not contain
	// the query terms at all), without ever displacing a direct lexical hit.
	if (top.length && top.length < cap) {
		const have = new Set(top.map((hit) => hit.path));
		const byPath = new Map(documents.map((document) => [document.path, document]));
		for (const hit of [...top]) {
			if (top.length >= cap) break;
			const source = byPath.get(hit.path);
			if (!source) continue;
			for (const match of source.text.matchAll(/\]\(([^)#\s]+\.md)\)/g)) {
				if (top.length >= cap) break;
				const neighbour = safePointer(root, join(hit.path, "..", match[1] as string));
				if (!neighbour || have.has(neighbour) || !byPath.has(neighbour)) continue;
				have.add(neighbour);
				const text = byPath.get(neighbour)?.text ?? "";
				top.push({
					path: neighbour,
					score: hit.score * 0.3,
					excerpt: `[linked from ${hit.path}] ${text.slice(0, 180).replace(/\s+/g, " ")}`,
				});
			}
		}
		// Tag expansion: notes sharing a declared tag with a top hit are the same
		// topic group (Obsidian tag groups), pulled in below link neighbours.
		const tagsByPath = new Map(documents.map((document, index) => [document.path, tags[index] as string[]]));
		for (const hit of [...top]) {
			if (top.length >= cap) break;
			const hitTags = new Set(tagsByPath.get(hit.path) ?? []);
			if (!hitTags.size) continue;
			for (const document of documents) {
				if (top.length >= cap) break;
				if (have.has(document.path)) continue;
				const shared = (tagsByPath.get(document.path) ?? []).find((tag) => hitTags.has(tag));
				if (!shared) continue;
				have.add(document.path);
				top.push({
					path: document.path,
					score: hit.score * 0.2,
					excerpt: `[shared tag #${shared} with ${hit.path}] ${document.text.slice(0, 160).replace(/\s+/g, " ")}`,
				});
			}
		}
		top.sort((a, b) => b.score - a.score || priority(b.path) - priority(a.path) || a.path.localeCompare(b.path));
	}
	return top;
}
