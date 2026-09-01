import { readFile } from "node:fs/promises";
import { join } from "node:path";
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
	const tokens = documents.map((document) => words(document.text));
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
		top.sort((a, b) => b.score - a.score || priority(b.path) - priority(a.path) || a.path.localeCompare(b.path));
	}
	return top;
}
