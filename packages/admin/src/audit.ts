/**
 * Audit log for the mutation gate.
 *
 * `MutationGate` only knows how to hand an entry to a sink. An audit trail
 * nobody can read is a compliance ornament, so the log is a two-way surface:
 * `append` for the gate, `tail` for `GET /api/audit` and the console panel that
 * renders it directly under the mutation form.
 */

import { appendFile, mkdir, readFile } from "node:fs/promises";
import { dirname } from "node:path";
import type { AuditEntry } from "./gate";

export type AuditLog = {
	append(entry: AuditEntry): Promise<void>;
	/** Newest first, at most `limit` entries. */
	tail(limit: number): Promise<readonly AuditEntry[]>;
};

/** In-memory log for tests and for a deployment that deliberately keeps no file. */
export function memoryAuditLog(): AuditLog {
	const entries: AuditEntry[] = [];
	return {
		append: async (entry) => {
			entries.push(entry);
		},
		tail: async (limit) => entries.slice(-limit).reverse(),
	};
}

/**
 * Append-only JSONL at `$GAJAEWAY_HOME/admin-audit.jsonl`. Appends are
 * serialised through a promise chain so two concurrent mutations cannot
 * interleave partial lines, and a malformed line never hides the rest of the
 * file from the reader.
 */
export function jsonlAuditLog(path: string): AuditLog {
	let writes: Promise<void> = Promise.resolve();
	let directoryReady: Promise<void> | undefined;

	const ready = (): Promise<void> => {
		directoryReady ??= mkdir(dirname(path), { recursive: true }).then(() => undefined);
		return directoryReady;
	};

	return {
		append: async (entry) => {
			writes = writes.then(async () => {
				await ready();
				await appendFile(path, `${JSON.stringify(entry)}\n`, "utf8");
			});
			await writes;
		},
		tail: async (limit) => {
			let text: string;
			try {
				text = await readFile(path, "utf8");
			} catch {
				return [];
			}
			const entries: AuditEntry[] = [];
			for (const line of text.split("\n")) {
				if (line.trim().length === 0) continue;
				try {
					entries.push(JSON.parse(line) as AuditEntry);
				} catch {
					// A truncated tail line is not a reason to hide the audit trail.
				}
			}
			return entries.slice(-limit).reverse();
		},
	};
}
