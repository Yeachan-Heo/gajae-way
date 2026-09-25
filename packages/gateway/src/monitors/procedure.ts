import { lstat, open, realpath, stat } from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import type { MonitorProcedureVersion } from "@gajae-gateway/protocol";

/** Most procedure files one monitor may declare. */
export const MONITOR_PROCEDURE_MAX_FILES = 8;
/** Per-file inline ceiling in UTF-8 bytes; a larger file is versioned but not inlined. */
export const MONITOR_PROCEDURE_MAX_BYTES = 24 * 1024;

/** Validates declared procedure paths: relative to the session workspace, no traversal. */
export function validateProcedureFiles(files: unknown): void {
	if (!Array.isArray(files)) throw new Error("monitor procedureFiles must be a list of relative paths");
	if (files.length > MONITOR_PROCEDURE_MAX_FILES)
		throw new Error(`monitor procedureFiles may list at most ${MONITOR_PROCEDURE_MAX_FILES} files`);
	for (const file of files) {
		if (
			typeof file !== "string" ||
			!file.trim() ||
			file.length > 256 ||
			file.includes("\0") ||
			isAbsolute(file) ||
			file.replaceAll("\\", "/").split("/").includes("..")
		)
			throw new Error("monitor procedureFiles entries must be relative paths inside the workspace");
	}
}

export interface ProcedureSnapshot {
	readonly versions: readonly MonitorProcedureVersion[];
	/** Prompt section carrying the current content, or "" when no files are declared. */
	readonly prompt: string;
}

function inside(root: string, path: string): boolean {
	const child = relative(root, path);
	return child === "" || (!child.startsWith(`..${sep}`) && child !== ".." && !isAbsolute(child));
}

async function readOne(
	workspace: string,
	allowed: readonly string[],
	path: string,
): Promise<{ version: MonitorProcedureVersion; body?: string }> {
	const lexical = resolve(workspace, path);
	if (!inside(workspace, lexical)) return { version: { path, status: "outside_root" } };
	let target: string;
	try {
		await lstat(lexical);
		target = await realpath(lexical);
	} catch (error) {
		return { version: { path, status: (error as NodeJS.ErrnoException).code === "ENOENT" ? "missing" : "unreadable" } };
	}
	if (!allowed.some((root) => inside(root, target))) return { version: { path, status: "outside_root" } };
	try {
		const info = await stat(target);
		if (!info.isFile()) return { version: { path, status: "unreadable" } };
		const handle = await open(target, "r");
		let bytes: Buffer;
		try {
			bytes = await handle.readFile();
		} finally {
			await handle.close();
		}
		const version = {
			path,
			sha256: new Bun.CryptoHasher("sha256").update(bytes).digest("hex"),
			mtime: info.mtime.toISOString(),
		};
		if (bytes.byteLength > MONITOR_PROCEDURE_MAX_BYTES) return { version: { ...version, status: "too_large" } };
		return { version: { ...version, status: "ok" }, body: bytes.toString("utf8") };
	} catch {
		return { version: { path, status: "unreadable" } };
	}
}

/**
 * Re-reads a monitor's declared procedure files for ONE firing (issue #82).
 *
 * A monitor event-type session is long-lived, so anything it read at session
 * start is frozen for the epoch. Procedure content therefore travels with every
 * authoring prompt instead, labelled with the version that produced it, and a
 * doctrine edit takes effect on the very next firing without a session roll.
 * Paths resolve against the session workspace; a symlink may point into the
 * workspace's own `memory/` corpus, never anywhere else.
 */
export async function readMonitorProcedure(workspace: string, files: readonly string[]): Promise<ProcedureSnapshot> {
	if (!files.length) return { versions: [], prompt: "" };
	const root = await realpath(workspace).catch(() => resolve(workspace));
	const memory = await realpath(join(workspace, "memory")).catch(() => undefined);
	const allowed = memory ? [root, memory] : [root];
	const results = await Promise.all(files.map((file) => readOne(workspace, allowed, file)));
	const sections = results.map(({ version, body }) => {
		const label = `### ${version.path} (${version.status}${version.sha256 ? `, sha256 ${version.sha256.slice(0, 12)}` : ""}${version.mtime ? `, mtime ${version.mtime}` : ""})`;
		if (body !== undefined) return `${label}\n${body.trim()}`;
		if (version.status === "too_large")
			return `${label}\nToo large to inline; read this file from disk now, before authoring.`;
		return `${label}\nNot available at this firing.`;
	});
	return {
		versions: results.map(({ version }) => version),
		prompt: [
			"Current procedure for this monitor, re-read from disk at this firing. It supersedes any earlier version of these files you saw in this session; follow it as written below.",
			...sections,
		].join("\n\n"),
	};
}
