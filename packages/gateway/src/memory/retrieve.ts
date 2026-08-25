import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { AXES } from "./doctrine";

export interface MemoryHit {
	path: string;
	score: number;
	excerpt: string;
}
const words = (text: string) => text.toLowerCase().match(/[\p{L}\p{N}_]+/gu) ?? [];

async function pointers(root: string): Promise<string[]> {
	const map = await readFile(join(root, "MEMORY.md"), "utf8");
	return [...map.matchAll(/\]\(([^)]+\.md)\)/g)]
		.map((match) => match[1])
		.filter((path) => !path.startsWith("/") && !path.includes(".."));
}

async function axisFiles(root: string): Promise<string[]> {
	const result: string[] = [];
	for (const axis of AXES) {
		for (const entry of await readdir(join(root, axis), { withFileTypes: true }))
			if (entry.isFile() && entry.name.endsWith(".md")) result.push(`${axis}/${entry.name}`);
	}
	return result.sort();
}

export async function searchMemory(root: string, query: string, limit = 10): Promise<MemoryHit[]> {
	const mapped = await pointers(root);
	const mappedSet = new Set(mapped);
	const files = [...mapped, ...(await axisFiles(root)).filter((path) => !mappedSet.has(path))];
	const terms = words(query);
	if (!terms.length) return [];
	const documents = await Promise.all(
		files.map(async (path) => ({ path, text: await readFile(join(root, path), "utf8") })),
	);
	const tokens = documents.map((document) => words(document.text));
	const averageLength = tokens.reduce((sum, document) => sum + document.length, 0) / (tokens.length || 1);
	const documentFrequency = new Map<string, number>();
	for (const document of tokens)
		for (const term of new Set(document)) documentFrequency.set(term, (documentFrequency.get(term) ?? 0) + 1);
	return documents
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
		.sort(
			(a, b) =>
				b.score - a.score ||
				(mappedSet.has(b.path) ? 1 : 0) - (mappedSet.has(a.path) ? 1 : 0) ||
				a.path.localeCompare(b.path),
		)
		.slice(0, Math.max(0, Math.min(50, Math.floor(limit))));
}
