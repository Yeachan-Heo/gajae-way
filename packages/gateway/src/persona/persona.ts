import { mkdir, stat } from "node:fs/promises";
import { join } from "node:path";

const PERSONA_FILES = ["SOUL.md", "AGENTS.md", "USER.md"] as const;

interface CachedSection {
	mtimeMs: number | null;
	text: string;
}

/** Loads workspace persona documents once per turn, re-reading only changed files. */
export class PersonaLoader {
	readonly #workspace: string;
	readonly #sections = new Map<string, CachedSection>();

	constructor(home: string) {
		this.#workspace = join(home, "workspace");
	}

	async ensureWorkspace(): Promise<void> {
		await mkdir(this.#workspace, { recursive: true, mode: 0o700 });
	}

	async systemPreamble(): Promise<string> {
		const sections = await Promise.all(PERSONA_FILES.map((name) => this.#read(name)));
		return PERSONA_FILES.map((name, index) => `## ${name}\n${sections[index] ?? ""}`).join("\n\n");
	}

	async #read(name: string): Promise<string> {
		const path = join(this.#workspace, name);
		let mtimeMs: number | null = null;
		try {
			mtimeMs = (await stat(path)).mtimeMs;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		const cached = this.#sections.get(name);
		if (cached?.mtimeMs === mtimeMs) return cached.text;
		const text = mtimeMs === null ? "" : await Bun.file(path).text();
		this.#sections.set(name, { mtimeMs, text });
		return text;
	}
}
