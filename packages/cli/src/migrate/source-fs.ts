import { lstat, readdir, readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join, relative } from "node:path";

/**
 * Read-only access to a source tree. Every read is recorded, so whatever the
 * readers did not consume can be reported as unmapped instead of vanishing.
 * Nothing here opens a file for writing.
 */
export class SourceTree {
	readonly root: string;
	readonly #consumed = new Set<string>();

	constructor(root: string) {
		this.root = root;
	}

	/** Relative display path for a source file, `~`-free and stable across hosts. */
	rel(path: string): string {
		const inside = relative(this.root, path);
		return inside.startsWith("..") ? path : inside || ".";
	}

	consume(path: string): void {
		this.#consumed.add(path);
	}

	isConsumed(path: string): boolean {
		for (const consumed of this.#consumed)
			if (path === consumed || path.startsWith(`${consumed}/`) || consumed.startsWith(`${path}/`)) return true;
		return false;
	}

	/** Consumed exactly, or entirely beneath a consumed directory. */
	isFullyConsumed(path: string): boolean {
		for (const consumed of this.#consumed) if (path === consumed || path.startsWith(`${consumed}/`)) return true;
		return false;
	}

	async text(path: string): Promise<string | undefined> {
		const info = await regularFile(path);
		if (!info) return undefined;
		this.consume(path);
		return readFile(path, "utf8");
	}

	async bytes(path: string): Promise<Uint8Array | undefined> {
		const info = await regularFile(path);
		if (!info) return undefined;
		this.consume(path);
		return new Uint8Array(await readFile(path));
	}
}

/** Stat without following symlinks; only a plain regular file qualifies. */
export async function regularFile(path: string): Promise<{ mtime: Date } | undefined> {
	try {
		const info = await lstat(path);
		return info.isFile() ? { mtime: info.mtime } : undefined;
	} catch {
		return undefined;
	}
}

export async function isDirectory(path: string): Promise<boolean> {
	try {
		return (await lstat(path)).isDirectory();
	} catch {
		return false;
	}
}

export async function entries(path: string): Promise<string[]> {
	try {
		return (await readdir(path)).sort();
	} catch {
		return [];
	}
}

/** Every regular file under `dir`, relative to it, skipping symlinks (they may point anywhere). */
export async function walkFiles(dir: string, prefix = ""): Promise<string[]> {
	const out: string[] = [];
	for (const name of await entries(dir)) {
		const full = join(dir, name);
		const info = await lstat(full).catch(() => undefined);
		if (!info) continue;
		if (info.isDirectory()) out.push(...(await walkFiles(full, join(prefix, name))));
		else if (info.isFile()) out.push(join(prefix, name));
	}
	return out;
}

/** Minimal dotenv reader: KEY=VALUE lines, optional `export`, single/double quotes, `#` comments. */
export function parseDotenv(text: string): Map<string, string> {
	const values = new Map<string, string>();
	for (const rawLine of text.split(/\r?\n/)) {
		const line = rawLine.trim();
		if (!line || line.startsWith("#")) continue;
		const match = /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/.exec(line);
		if (!match) continue;
		let value = match[2] ?? "";
		const quote = value[0];
		if ((quote === '"' || quote === "'") && value.lastIndexOf(quote) > 0) value = value.slice(1, value.lastIndexOf(quote));
		else value = value.replace(/\s+#.*$/, "").trim();
		values.set(match[1] as string, value);
	}
	return values;
}

export function expandHome(path: string): string {
	return path === "~" ? homedir() : path.startsWith("~/") ? join(homedir(), path.slice(2)) : path;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** A comma-separated string or a list, as both Hermes and OpenClaw accept. */
export function stringList(value: unknown): string[] {
	const items = Array.isArray(value) ? value : typeof value === "string" ? value.split(",") : [];
	return items.map((item) => String(item).trim()).filter(Boolean);
}

export const DATED_FILE = /^(\d{4})-(\d{2})-(\d{2})\.md$/;
