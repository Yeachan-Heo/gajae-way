import type { OriginPlatform } from "./origin";

/**
 * Outbound file attachments for the gajaeway profile.
 *
 * A reply part may carry one or more `[FILE:<absolute path>]` tokens anywhere in
 * its text. Each token becomes its own ledger delivery (like a reaction), so an
 * upload is settled, retried and recovered exactly like a message, and the
 * tokens themselves never reach the room.
 *
 * The path names a file on the host the gateway and its adapters share. The
 * gateway checks it (absolute, a regular file, bounded size, outside the
 * gateway home except the workspace) before an adapter is ever asked to read it.
 */
export interface FileRef {
	/** Absolute, symlink-resolved path the adapter reads at delivery time. */
	readonly path: string;
	/** File name shown in the room (the basename of the requested path). */
	readonly name: string;
	/** Size in bytes when the gateway checked it. */
	readonly size: number;
}

export interface FileReply {
	/** Requested paths in reply order, de-duplicated. */
	readonly paths: readonly string[];
	/** Reply text with every token removed. */
	readonly body: string;
}

/** Upper bound per file; Slack accepts far more, but a chat reply is not a file transfer service. */
export const OUTBOUND_FILE_MAX_BYTES = 50 * 1024 * 1024;
/** At most this many files per turn; extra tokens are dropped with a notice. */
export const OUTBOUND_FILES_PER_TURN_CAP = 5;

const FILE_TOKEN = /\[FILE:([^\]\n]+)\]/g;

/** Platforms whose adapter uploads `file` deliveries; others would only see the text fallback. */
const FILE_PLATFORMS: ReadonlySet<OriginPlatform> = new Set(["slack"]);

export function platformSupportsFiles(platform: OriginPlatform): boolean {
	return FILE_PLATFORMS.has(platform);
}

/**
 * Extracts `[FILE:<path>]` tokens. Returns undefined when the text has none, so
 * the common case leaves the reply untouched.
 */
export function parseFileReply(text: string): FileReply | undefined {
	const paths: string[] = [];
	for (const match of text.matchAll(FILE_TOKEN)) {
		const path = (match[1] ?? "").trim();
		if (path && !paths.includes(path)) paths.push(path);
	}
	if (paths.length === 0) return undefined;
	const body = text
		.replace(FILE_TOKEN, "")
		.replace(/[^\S\n]+\n/g, "\n")
		.replace(/\n{3,}/g, "\n\n")
		.trim();
	return { paths, body };
}

/** The visible text of a file delivery: what an adapter without upload support posts instead. */
export function fileFallbackText(file: Pick<FileRef, "name">): string {
	return `📎 ${file.name}`;
}
