import { type FileHandle, open, realpath } from "node:fs/promises";
import { basename, isAbsolute, relative, resolve, sep } from "node:path";
import { AttachmentBuilder } from "discord.js";

const MAX_DISCORD_FILES = 10;
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024;

export interface ParsedDiscordMedia {
	readonly text: string;
	readonly paths: readonly string[];
}

export interface DiscordMediaResult {
	readonly attachments: readonly AttachmentBuilder[];
	readonly failures: ReadonlyArray<{ readonly path: string; readonly reason: string }>;
}

interface DiscordMediaFileSnapshot {
	readonly size: bigint;
	readonly mtimeNs: bigint;
	readonly ctimeNs: bigint;
}

function isUnderAny(directories: readonly string[], path: string): boolean {
	return directories.some((directory) => {
		const relativePath = relative(directory, path);
		return (
			relativePath === "" ||
			(relativePath !== ".." && !relativePath.startsWith(`..${sep}`) && !isAbsolute(relativePath))
		);
	});
}

/** Extracts explicit MEDIA:/absolute/path directives from standalone response lines outside fenced code. */
export function parseDiscordMedia(text: string): ParsedDiscordMedia {
	const lines = text.split(/\r?\n/);
	const paths: string[] = [];
	const content: string[] = [];
	let fence: { character: string; length: number } | undefined;
	for (const line of lines) {
		const fenceLine = line.match(/^ {0,3}(`{3,}|~{3,})(.*)$/);
		const delimiter = fenceLine?.[1];
		const suffix = fenceLine?.[2] ?? "";
		if (fence) {
			if (delimiter?.charAt(0) === fence.character && delimiter.length >= fence.length && /^[ \t]*$/.test(suffix))
				fence = undefined;
			content.push(line);
			continue;
		}
		// A backtick in a backtick fence's info string makes it ordinary prose.
		if (delimiter && (delimiter.charAt(0) !== "`" || !suffix.includes("`"))) {
			fence = { character: delimiter.charAt(0), length: delimiter.length };
			content.push(line);
			continue;
		}
		if (!line.startsWith("MEDIA:")) {
			content.push(line);
			continue;
		}
		const path = line.slice("MEDIA:".length).trim();
		if (!path) throw new Error("Discord MEDIA directive requires a file path");
		paths.push(path);
	}
	if (paths.length === 0) return { text, paths };
	if (paths.length > MAX_DISCORD_FILES)
		throw new Error(`Discord accepts at most ${MAX_DISCORD_FILES} files per message`);
	return { text: content.join("\n").trim(), paths };
}

/** Reads exactly the observed file size from one handle and rejects concurrent changes. */

export async function readDiscordMediaFile(file: FileHandle, snapshot: DiscordMediaFileSnapshot): Promise<Buffer> {
	const expectedBytes = Number(snapshot.size);
	const bytes = Buffer.alloc(expectedBytes);
	let bytesRead = 0;
	while (bytesRead < bytes.length) {
		const result = await file.read(bytes, bytesRead, bytes.length - bytesRead, bytesRead);
		if (result.bytesRead === 0) break;
		bytesRead += result.bytesRead;
	}
	if (bytesRead !== expectedBytes) throw new Error("Discord MEDIA file changed while being prepared");
	// Read from the same open handle and detect concurrent growth before building the upload.
	const extraByte = Buffer.alloc(1);
	const extra = await file.read(extraByte, 0, 1, bytesRead);
	const after = await file.stat({ bigint: true });
	if (
		bytesRead !== expectedBytes ||
		extra.bytesRead !== 0 ||
		after.size !== snapshot.size ||
		after.mtimeNs !== snapshot.mtimeNs ||
		after.ctimeNs !== snapshot.ctimeNs
	)
		throw new Error("Discord MEDIA file changed while being prepared");
	return bytes;
}

/** Reads explicit local media directives into bounded Discord uploads, continuing on individual failures. */
export async function loadDiscordMedia(
	paths: readonly string[],
	allowedDirectories: readonly string[] = [],
): Promise<DiscordMediaResult> {
	// Resolve allowed directories, filtering out those that don't exist.
	// A non-existent configured directory is treated as unset.
	// The lexical pre-check accepts both the configured spelling and its real path, so a mediaDirectory
	// that is itself a symlink still admits paths written under its configured name.
	const realAllowedDirectories: string[] = [];
	const lexicalAllowedDirectories: string[] = [];
	for (const directory of allowedDirectories) {
		try {
			const realPath = await realpath(directory);
			realAllowedDirectories.push(realPath);
			lexicalAllowedDirectories.push(resolve(directory), realPath);
		} catch {
			// Non-existent or inaccessible directory is silently ignored.
		}
	}

	const attachments: AttachmentBuilder[] = [];
	const failures: Array<{ path: string; reason: string }> = [];
	let totalBytes = 0;

	for (const path of paths) {
		try {
			if (attachments.length >= MAX_DISCORD_FILES) {
				failures.push({ path, reason: `Discord accepts at most ${MAX_DISCORD_FILES} files per message` });
				continue;
			}

			if (!isAbsolute(path)) {
				failures.push({ path, reason: "path must be absolute" });
				continue;
			}

			if (realAllowedDirectories.length === 0) {
				failures.push({ path, reason: "uploads disabled: set mediaDirectories" });
				continue;
			}

			// Lexically check if the resolved path is under an allowed directory.
			// This check happens before any fs call to avoid revealing path existence.
			const resolvedPath = resolve(path);
			if (!isUnderAny(lexicalAllowedDirectories, resolvedPath)) {
				failures.push({ path, reason: "path is outside mediaDirectories" });
				continue;
			}

			// Now resolve symlinks and verify the real path is still under an allowed directory.
			const realFilePath = await realpath(resolvedPath);
			if (!isUnderAny(realAllowedDirectories, realFilePath)) {
				failures.push({ path, reason: "path is outside mediaDirectories" });
				continue;
			}

			const file = await open(realFilePath, "r");
			try {
				const info = await file.stat({ bigint: true });
				if (!info.isFile()) {
					failures.push({ path, reason: "path must name a regular file" });
					continue;
				}
				const remainingBytes = MAX_UPLOAD_BYTES - totalBytes;
				if (info.size > BigInt(remainingBytes)) {
					failures.push({ path, reason: "file exceeds the 25 MiB upload limit" });
					continue;
				}
				const bytes = await readDiscordMediaFile(file, info);
				const name = basename(realFilePath);
				if (!name) {
					failures.push({ path, reason: "file must have a filename" });
					continue;
				}
				totalBytes += bytes.length;
				attachments.push(new AttachmentBuilder(bytes, { name }));
			} finally {
				await file.close();
			}
		} catch (error) {
			// A raw fs error names the host path; the note reaching chat must not.
			const code = (error as { code?: unknown } | undefined)?.code;
			failures.push({ path, reason: code === "ENOENT" ? "file not found" : "file could not be read" });
		}
	}

	return { attachments, failures };
}
