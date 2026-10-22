import { readFile, stat } from "node:fs/promises";
import { join } from "node:path";
import { isGeneratedFile } from "./autolink";
import { corpusEntries } from "./doctrine";

/** Default `memory.debloatByteThreshold`: a memory file over 100 KB gets restructured. */
export const DEFAULT_DEBLOAT_BYTE_THRESHOLD = 102_400;

export interface OversizedMemoryFile {
	/** Corpus-root-relative path. */
	readonly path: string;
	readonly bytes: number;
}

/**
 * Memory files strictly larger than `thresholdBytes`, largest first.
 *
 * Walks the same corpus the validator audits (Markdown only, `.git` excluded),
 * and skips the generated surfaces: `MEMORY.md` and any file whose frontmatter
 * carries `generated_by`, since those are rewritten by their generator rather
 * than restructured by hand.
 */
export async function oversizedMemoryFiles(root: string, thresholdBytes: number): Promise<OversizedMemoryFile[]> {
	const found: OversizedMemoryFile[] = [];
	for (const path of await corpusEntries(root)) {
		if (path === "MEMORY.md") continue;
		const file = join(root, path);
		const bytes = (await stat(file).catch(() => undefined))?.size;
		if (bytes === undefined || bytes <= thresholdBytes) continue;
		const text = await readFile(file, "utf8").catch(() => undefined);
		if (text === undefined || isGeneratedFile(text)) continue;
		found.push({ path, bytes });
	}
	return found.sort((a, b) => b.bytes - a.bytes || a.path.localeCompare(b.path));
}
