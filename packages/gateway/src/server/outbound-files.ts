import { createHash } from "node:crypto";
import type { Stats } from "node:fs";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import { type FileRef, OUTBOUND_FILE_MAX_BYTES } from "@gajae-gateway/protocol";

export type OutboundFileCheck =
	| { readonly ok: true; readonly file: FileRef }
	| { readonly ok: false; readonly name: string; readonly reason: string };

export interface OutboundFileScope {
	/** Gateway home: its secrets, databases and state never leave as an attachment. */
	readonly home: string;
	/** Persona workspace; the one part of the home a file may be sent from. */
	readonly workspace: string;
	readonly maxBytes?: number;
}

function isInside(child: string, parent: string): boolean {
	const relative = path.relative(parent, child);
	return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

async function canonical(target: string): Promise<string> {
	try {
		return await fs.realpath(target);
	} catch {
		return path.resolve(target);
	}
}

/**
 * Decides whether a `MEDIA:<path>` request may be uploaded.
 *
 * The persona can already read any file it can reach, so this is not a sandbox.
 * It keeps the obvious accidents out of a chat room: relative paths (whose base
 * the persona cannot see), directories and devices, oversized files, and the
 * gateway home itself (tokens under `secrets/`, the ledger databases), with the
 * persona workspace as the one exception. Symlinks are resolved first, so a link
 * in the workspace cannot point the check somewhere else.
 */
export async function checkOutboundFile(requested: string, scope: OutboundFileScope): Promise<OutboundFileCheck> {
	const name = path.basename(requested) || requested;
	if (!path.isAbsolute(requested)) return { ok: false, name, reason: "path must be absolute" };
	let resolved: string;
	try {
		resolved = await fs.realpath(requested);
	} catch {
		return { ok: false, name, reason: "file not found" };
	}
	const [home, workspace] = await Promise.all([canonical(scope.home), canonical(scope.workspace)]);
	if (isInside(resolved, home) && !isInside(resolved, workspace))
		return { ok: false, name, reason: "files under the gateway home (outside the workspace) are not sent" };
	let stat: Stats;
	try {
		stat = await fs.stat(resolved);
	} catch {
		return { ok: false, name, reason: "file not found" };
	}
	if (!stat.isFile()) return { ok: false, name, reason: "not a regular file" };
	const maxBytes = scope.maxBytes ?? OUTBOUND_FILE_MAX_BYTES;
	if (stat.size === 0) return { ok: false, name, reason: "file is empty" };
	if (stat.size > maxBytes)
		return { ok: false, name, reason: `file is ${stat.size} bytes, over the ${maxBytes}-byte limit` };
	return { ok: true, file: { path: resolved, name, size: stat.size } };
}

/**
 * One upload slot per (conversation, trigger, requested path): the terminal pass
 * re-reads text the tail already shipped, and a replay after a restart re-runs
 * the same reply; neither may post the file a second time.
 */
export function deterministicFileDeliveryId(originKey: string, triggerMessageId: string, requested: string): string {
	return `gw-f-${createHash("sha256").update(`${originKey}|${triggerMessageId}|`).update(requested).digest("hex").slice(0, 32)}`;
}
