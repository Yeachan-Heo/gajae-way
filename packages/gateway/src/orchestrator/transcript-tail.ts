import { constants } from "node:fs";
import { type FileHandle, open } from "node:fs/promises";
import {
	WORK_PROGRESS_INTENT_MAX_CHARS,
	WORK_PROGRESS_TEXT_MAX_CHARS,
	type WorkProgressText,
	type WorkProgressToolCall,
} from "@gajae-gateway/protocol";

/**
 * Read-only progress from the tail of a saved gjc session transcript (#814).
 *
 * The transcript is `<agentDir>/sessions/<scope>/<created>_<sessionId>.jsonl`,
 * one JSON row per line, appended while the lane works and often many MB. It is
 * read backwards from its current end in fixed chunks and the scan stops as soon
 * as the requested rows are found, so cost tracks what is shown, not file size.
 * The file is opened read-only and never written, locked or touched otherwise:
 * the lane cannot tell it was read.
 */

/** Never read further back than this from the end. */
export const TRANSCRIPT_TAIL_MAX_SCAN_BYTES = 16 * 1024 * 1024;
/** A row longer than this (an image or huge tool output) is skipped, never buffered whole. */
export const TRANSCRIPT_TAIL_MAX_ROW_BYTES = 4 * 1024 * 1024;
const CHUNK_BYTES = 256 * 1024;
/** Text handed to redaction: enough to cover the shown prefix, bounded for huge messages. */
const REDACT_INPUT_CHARS = 64 * 1024;
const TOOL_NAME_MAX_CHARS = 64;

export interface TranscriptTailOptions {
	/** Assistant text blocks and tool calls to return, each. */
	readonly limit: number;
	/** Applied to every returned string before it is cut to size. */
	readonly redact: (text: string) => string;
	readonly maxScanBytes?: number;
	readonly maxRowBytes?: number;
}

export interface TranscriptTail {
	readonly bytes: number;
	readonly scannedBytes: number;
	readonly complete: boolean;
	readonly skippedRows: number;
	readonly turnStartedAt: string | null;
	readonly toolInFlight: WorkProgressToolCall | null;
	readonly assistantTexts: readonly WorkProgressText[];
	readonly toolCalls: readonly WorkProgressToolCall[];
}

type Row = Record<string, unknown>;

function record(value: unknown): Row | undefined {
	return value !== null && typeof value === "object" && !Array.isArray(value) ? (value as Row) : undefined;
}

function iso(value: unknown): string | null {
	const time = typeof value === "number" ? value : typeof value === "string" ? Date.parse(value) : Number.NaN;
	return Number.isFinite(time) && time >= 0 && time <= 8.64e15 ? new Date(time).toISOString() : null;
}

/** Redacts first, then cuts, so a cut can never expose part of a secret the full text would hide. */
function bounded(text: string, max: number, redact: (text: string) => string): { text: string; truncated: boolean } {
	const redacted = redact(text.length > REDACT_INPUT_CHARS ? text.slice(0, REDACT_INPUT_CHARS) : text);
	const truncated = text.length > REDACT_INPUT_CHARS || redacted.length > max;
	return { text: truncated ? `${redacted.slice(0, max - 1)}…` : redacted, truncated };
}

/**
 * Feeds complete rows to `visit`, newest first, until it returns true or the
 * scan floor is reached. The bytes after the last newline are a row still being
 * written and are never parsed; the first row of a window that does not start at
 * offset 0 is cut and dropped the same way.
 */
async function scanBackward(
	handle: FileHandle,
	size: number,
	maxScanBytes: number,
	maxRowBytes: number,
	visit: (row: Buffer) => boolean,
	skip: () => void,
): Promise<{ scannedBytes: number; stopped: boolean }> {
	const floor = Math.max(0, size - maxScanBytes);
	let position = size;
	// Pieces of the row that continues before `position`, oldest first.
	let pending: Buffer[] = [];
	let pendingBytes = 0;
	let oversized = false;
	let tail = true;
	while (position > floor) {
		const start = Math.max(floor, position - CHUNK_BYTES);
		const chunk = Buffer.allocUnsafe(position - start);
		let read = 0;
		while (read < chunk.length) {
			const result = await handle.read(chunk, read, chunk.length - read, start + read);
			if (result.bytesRead === 0) throw new Error("transcript shrank while it was read");
			read += result.bytesRead;
		}
		position = start;
		let end = chunk.length;
		let index = end === 0 ? -1 : chunk.lastIndexOf(0x0a, end - 1);
		while (index !== -1) {
			const piece = chunk.subarray(index + 1, end);
			if (tail) tail = false;
			else if (oversized) skip();
			else {
				const row = pending.length === 0 ? piece : Buffer.concat([piece, ...pending]);
				if (row.length > maxRowBytes) skip();
				else if (row.length > 0 && visit(row)) return { scannedBytes: size - start, stopped: true };
			}
			pending = [];
			pendingBytes = 0;
			oversized = false;
			end = index;
			index = end === 0 ? -1 : chunk.lastIndexOf(0x0a, end - 1);
		}
		if (!oversized && end > 0) {
			pendingBytes += end;
			if (pendingBytes > maxRowBytes) {
				oversized = true;
				pending = [];
			} else pending.unshift(chunk.subarray(0, end));
		}
	}
	// Only a window that reaches offset 0 holds the first row whole.
	if (floor === 0 && !tail) {
		if (oversized) skip();
		else if (pendingBytes > 0 && visit(Buffer.concat(pending))) return { scannedBytes: size, stopped: true };
	}
	return { scannedBytes: size - floor, stopped: false };
}

/** Reads the progress view from the end of the transcript at `path`. Throws when it cannot be read. */
export async function readTranscriptTail(path: string, options: TranscriptTailOptions): Promise<TranscriptTail> {
	const limit = options.limit;
	const redact = options.redact;
	const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
	try {
		const stat = await handle.stat();
		if (!stat.isFile()) throw new Error("transcript is not a regular file");
		const results = new Map<string, boolean>();
		const texts: WorkProgressText[] = [];
		const tools: WorkProgressToolCall[] = [];
		let toolInFlight: WorkProgressToolCall | null = null;
		let turnStartedAt: string | null = null;
		// Any assistant or user message newer than the row being visited.
		let newerMessage = false;
		let skippedRows = 0;
		const visit = (bytes: Buffer): boolean => {
			let row: Row | undefined;
			try {
				row = record(JSON.parse(bytes.toString("utf8")));
			} catch {
				skippedRows++;
				return false;
			}
			const message = row?.type === "message" ? record(row.message) : undefined;
			if (!message) return false;
			const at = iso(row?.timestamp) ?? iso(message.timestamp);
			if (message.role === "toolResult") {
				if (typeof message.toolCallId === "string") results.set(message.toolCallId, message.isError === true);
				return false;
			}
			if (message.role === "user") {
				turnStartedAt ??= at;
				newerMessage = true;
			} else if (message.role === "assistant") {
				const content = Array.isArray(message.content) ? message.content : [];
				for (let i = content.length - 1; i >= 0; i--) {
					const block = record(content[i]);
					if (block?.type === "toolCall" && typeof block.name === "string") {
						const id = typeof block.id === "string" ? block.id : "";
						const saved = results.get(id);
						const status =
							saved === undefined
								? skippedRows > 0
									? "unknown"
									: newerMessage
										? "no_result"
										: "running"
								: saved
									? "error"
									: "ok";
						const intent = record(block.arguments)?._i;
						const call: WorkProgressToolCall = {
							name: bounded(block.name, TOOL_NAME_MAX_CHARS, redact).text,
							...(typeof intent === "string" && intent.trim()
								? { intent: bounded(intent.trim(), WORK_PROGRESS_INTENT_MAX_CHARS, redact).text }
								: {}),
							at,
							status,
						};
						if (status === "running") toolInFlight ??= call;
						if (tools.length < limit) tools.push(call);
					} else if (block?.type === "text" && typeof block.text === "string" && block.text.trim()) {
						if (texts.length < limit)
							texts.push({ at, ...bounded(block.text.trim(), WORK_PROGRESS_TEXT_MAX_CHARS, redact) });
					}
				}
				newerMessage = true;
			}
			return turnStartedAt !== null && texts.length >= limit && tools.length >= limit;
		};
		const scan = await scanBackward(
			handle,
			stat.size,
			options.maxScanBytes ?? TRANSCRIPT_TAIL_MAX_SCAN_BYTES,
			options.maxRowBytes ?? TRANSCRIPT_TAIL_MAX_ROW_BYTES,
			visit,
			() => {
				skippedRows++;
			},
		);
		return {
			bytes: stat.size,
			scannedBytes: scan.scannedBytes,
			complete: scan.stopped || scan.scannedBytes === stat.size,
			skippedRows,
			turnStartedAt,
			toolInFlight,
			assistantTexts: texts.reverse(),
			toolCalls: tools.reverse(),
		};
	} finally {
		await handle.close();
	}
}
