import type { OriginPlatform } from "./origin";

/**
 * Outbound file attachments for the gajaeway profile.
 *
 * A reply may carry `MEDIA:<absolute path>` directives, each on a line of its
 * own (the Hermes Agent convention, also used by the Discord adapter's native
 * attachments). Each directive becomes its own ledger delivery (like a
 * reaction), so an upload is settled, retried and recovered exactly like a
 * message, and the directive lines themselves never reach the room.
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
/** At most this many files per turn; extra directives are refused with a notice. */
export const OUTBOUND_FILES_PER_TURN_CAP = 5;

const MEDIA_PREFIX = "MEDIA:";

/** Platforms whose adapter uploads `file` deliveries; others would only see the text fallback. */
const FILE_PLATFORMS: ReadonlySet<OriginPlatform> = new Set(["slack"]);

export function platformSupportsFiles(platform: OriginPlatform): boolean {
	return FILE_PLATFORMS.has(platform);
}

/**
 * Extracts standalone `MEDIA:<path>` lines. Returns undefined when the text has
 * none, so the common case leaves the reply untouched. A line counts only when
 * it starts with `MEDIA:` and names a path; prose that merely mentions the word
 * is left alone.
 */
export function parseFileReply(text: string): FileReply | undefined {
	const paths: string[] = [];
	const kept: string[] = [];
	for (const line of text.split(/\r?\n/)) {
		const path = line.startsWith(MEDIA_PREFIX) ? line.slice(MEDIA_PREFIX.length).trim() : "";
		if (!path) {
			kept.push(line);
			continue;
		}
		if (!paths.includes(path)) paths.push(path);
	}
	if (paths.length === 0) return undefined;
	return {
		paths,
		body: kept
			.join("\n")
			.replace(/\n{3,}/g, "\n\n")
			.trim(),
	};
}

/** The visible text of a file delivery: what an adapter without upload support posts instead. */
export function fileFallbackText(file: Pick<FileRef, "name">): string {
	return `📎 ${file.name}`;
}
