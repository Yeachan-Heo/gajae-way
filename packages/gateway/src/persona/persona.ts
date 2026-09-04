import { mkdir, stat, writeFile } from "node:fs/promises";
import { join } from "node:path";
import selfOpsConfig from "./self-ops/config-and-restarts.md" with { type: "text" };
import selfOpsSkill from "./self-ops/SKILL.md" with { type: "text" };
import selfOpsService from "./self-ops/service-control.md" with { type: "text" };
import selfOpsState from "./self-ops/state-inspection.md" with { type: "text" };

const PERSONA_FILES = ["SOUL.md", "AGENTS.md", "USER.md"] as const;

const SELF_OPS_FILES = {
	"SKILL.md": selfOpsSkill,
	"service-control.md": selfOpsService,
	"config-and-restarts.md": selfOpsConfig,
	"state-inspection.md": selfOpsState,
} as const;

export const SELF_OPS_PREAMBLE_POINTER = [
	"## Runtime operations",
	"Use `/skill:self-ops` before restarting a service, before editing channel config, or when a reply appears lost.",
	"The full procedures are on demand and are not part of this preamble.",
].join("\n");

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
		const skillDirectory = join(this.#workspace, ".gjc", "skills", "self-ops");
		await mkdir(skillDirectory, { recursive: true, mode: 0o700 });
		await Promise.all(
			Object.entries(SELF_OPS_FILES).map(async ([name, body]) => {
				try {
					await writeFile(join(skillDirectory, name), body, { encoding: "utf8", flag: "wx", mode: 0o600 });
				} catch (error) {
					if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
				}
			}),
		);
	}

	async systemPreamble(): Promise<string> {
		const sections = await Promise.all(PERSONA_FILES.map((name) => this.#read(name)));
		return [
			...PERSONA_FILES.map((name, index) => `## ${name}\n${sections[index] ?? ""}`),
			SELF_OPS_PREAMBLE_POINTER,
		].join("\n\n");
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
