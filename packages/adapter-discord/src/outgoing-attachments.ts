import { type FileHandle, open, realpath } from "node:fs/promises";
import { basename, isAbsolute, relative, resolve, sep } from "node:path";
import { AttachmentBuilder } from "discord.js";

const MAX_DISCORD_FILES = 10;
const MAX_UPLOAD_BYTES = 25 * 1024 * 1024;

export interface ParsedDiscordMedia {
	readonly text: string;
	readonly paths: readonly string[];
}

interface DiscordMediaFileSnapshot {
	readonly size: bigint;
	readonly mtimeNs: bigint;
	readonly ctimeNs: bigint;
}

/** Extracts explicit MEDIA:/absolute/path directives from standalone response lines. */
export function parseDiscordMedia(text: string): ParsedDiscordMedia {
	const lines = text.split(/\r?\n/);
	const paths: string[] = [];
	const content: string[] = [];
	for (const line of lines) {
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

/** Reads explicit local media directives into bounded Discord uploads. */
export async function loadDiscordMedia(
	paths: readonly string[],
	allowedDirectories: readonly string[] = [],
): Promise<AttachmentBuilder[]> {
	if (paths.length > MAX_DISCORD_FILES)
		throw new Error(`Discord accepts at most ${MAX_DISCORD_FILES} files per message`);
	if (paths.length > 0 && allowedDirectories.length === 0)
		throw new Error("Discord MEDIA uploads are disabled until mediaDirectories are configured");
	const realAllowedDirectories = await Promise.all(allowedDirectories.map((directory) => realpath(directory)));
	const attachments: AttachmentBuilder[] = [];
	let totalBytes = 0;
	for (const path of paths) {
		if (!isAbsolute(path)) throw new Error("Discord MEDIA file path must be absolute");
		const resolvedPath = resolve(path);
		const realFilePath = await realpath(resolvedPath);
		if (
			!realAllowedDirectories.some((directory) => {
				const relativePath = relative(directory, realFilePath);
				return (
					relativePath === "" ||
					(relativePath !== ".." && !relativePath.startsWith(`..${sep}`) && !isAbsolute(relativePath))
				);
			})
		)
			throw new Error("Discord MEDIA file is outside configured mediaDirectories");
		const file = await open(realFilePath, "r");
		try {
			const info = await file.stat({ bigint: true });
			if (!info.isFile()) throw new Error("Discord MEDIA path must name a regular file");
			const remainingBytes = MAX_UPLOAD_BYTES - totalBytes;
			if (info.size > BigInt(remainingBytes)) throw new Error("Discord MEDIA files exceed the 25 MiB upload limit");
			const bytes = await readDiscordMediaFile(file, info);
			const name = basename(realFilePath);
			if (!name) throw new Error("Discord MEDIA file must have a filename");
			totalBytes += bytes.length;
			attachments.push(new AttachmentBuilder(bytes, { name }));
		} finally {
			await file.close();
		}
	}
	return attachments;
}
