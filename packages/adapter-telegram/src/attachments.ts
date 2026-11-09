/**
 * Attachment handling for inbound Telegram messages.
 *
 * The adapter used to forward `message.text` and nothing else. A photo, file or
 * voice note carries no `text` (a caption, when present, arrives in `caption`),
 * so every such message was dropped before it reached the gateway — the persona
 * never saw the image and the conversation-context ledger never recorded it.
 *
 * Non-image attachments are rendered into the message body as one bracketed text
 * line. Images are also described there, but their freshly downloaded bytes are
 * separately prepared as native prompt input so the model can actually see them.
 *
 * Unlike Discord, a Telegram file url embeds the bot token
 * (`https://api.telegram.org/file/bot<token>/<path>`), so it must never be put
 * in the body: the body is persisted in the ledger and shown to the persona. The
 * adapter therefore downloads files itself. Non-image files render their local
 * path, while images are read from that freshly returned path and sent natively.
 * Files land under the adapter home with owner-only permissions and are pruned
 * after a retention window, because the Bot API cannot re-serve them forever.
 */

import { constants } from "node:fs";
import { mkdir, open, readdir, rename, rm, stat, writeFile } from "node:fs/promises";
import { extname, join } from "node:path";
import { preparePromptImages } from "@gajae-gateway/images";
import type { PromptImage } from "@gajae-gateway/protocol";

/** The Bot API refuses `getFile` for anything larger than this. */
export const TELEGRAM_BOT_DOWNLOAD_LIMIT = 20 * 1024 * 1024;

/** How long downloaded attachments are kept before the sweep removes them. */
export const ATTACHMENT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

const DOWNLOAD_TIMEOUT_MS = 30_000;

class TelegramAttachmentHttpError extends Error {
	constructor(readonly status: number) {
		super(`file download returned HTTP ${status}`);
	}
}

class TelegramAttachmentSizeError extends Error {}

export interface TelegramFileShape {
	readonly file_id: string;
	readonly file_unique_id: string;
	readonly file_size?: number;
}

export interface TelegramPhotoSize extends TelegramFileShape {
	readonly width: number;
	readonly height: number;
}

export interface TelegramNamedFileShape extends TelegramFileShape {
	readonly file_name?: string;
	readonly mime_type?: string;
}

export interface TelegramTimedFileShape extends TelegramNamedFileShape {
	readonly duration?: number;
}

/** The media fields of a Telegram `Message`; at most one is set per message. */
export interface TelegramAttachmentCarrier {
	readonly photo?: readonly TelegramPhotoSize[];
	readonly animation?: TelegramTimedFileShape;
	readonly video?: TelegramTimedFileShape;
	readonly video_note?: TelegramTimedFileShape;
	readonly voice?: TelegramTimedFileShape;
	readonly audio?: TelegramTimedFileShape;
	readonly document?: TelegramNamedFileShape;
}

export type TelegramAttachmentKind = "image" | "video" | "audio" | "voice message" | "file";

export interface TelegramAttachment {
	readonly kind: TelegramAttachmentKind;
	readonly fileId: string;
	readonly fileUniqueId: string;
	readonly name?: string;
	readonly mimeType?: string;
	readonly size?: number;
	readonly duration?: number;
}

/** Where a downloaded attachment ended up, or why it is not on disk. */
export type StoredAttachment =
	| { readonly path: string }
	| { readonly unavailable: string; readonly retryable?: boolean };

/** A temporary download failure must leave its Telegram update unaccepted. */
export class TelegramAttachmentRetryableError extends Error {
	constructor() {
		super("Telegram attachment download temporarily unavailable");
		this.name = "TelegramAttachmentRetryableError";
	}
}

export interface TelegramFileApi {
	getFile(fileId: string, signal: AbortSignal): Promise<{ readonly file_path?: string; readonly file_size?: number }>;
	downloadFile(filePath: string, signal: AbortSignal): Promise<Response>;
}

export interface TelegramAttachmentSaver {
	save(attachment: TelegramAttachment, chatId: string, messageId: number): Promise<StoredAttachment>;
}

export type TelegramImagePreparation = {
	readonly images: PromptImage[];
	readonly notice: string;
};

/**
 * Reads the image from the freshly saved local attachment and prepares it for
 * native prompt input. Paths are accepted only from the saver result, never
 * from Telegram message fields or gateway payloads.
 */
export async function prepareTelegramImage(
	stored: StoredAttachment,
	prepare: typeof preparePromptImages = preparePromptImages,
): Promise<TelegramImagePreparation> {
	if (!("path" in stored)) throw new Error("image attachment is unavailable");
	const bytes = await readStoredImage(stored.path);
	if (!isPng(bytes) && !isJpeg(bytes)) throw new Error("image attachment must be PNG or JPEG");
	return prepare([bytes]);
}

/**
 * The one attachment a message carries, normalized.
 *
 * Telegram sends a media group as separate messages, so a single message never
 * holds more than one file. `animation` is checked before `document` because
 * Telegram also fills `document` on an animation for old clients, and `photo`
 * lists every resolution of the same image — the largest one is the useful one.
 */
export function telegramAttachment(message: TelegramAttachmentCarrier): TelegramAttachment | undefined {
	const photo = largestPhoto(message.photo);
	if (photo) return fromFile("image", photo);
	if (message.animation) return fromFile("video", message.animation);
	if (message.video) return fromFile("video", message.video);
	if (message.video_note) return fromFile("video", message.video_note);
	if (message.voice) return fromFile("voice message", message.voice);
	if (message.audio) return fromFile("audio", message.audio);
	if (message.document) return fromFile(kindForMime(message.document.mime_type), message.document);
	return undefined;
}

/**
 * Renders one attachment as a single bracketed line.
 *
 * The kind label comes first so the persona can dispatch on it without parsing
 * a mime type, and the location comes last so a truncated body still keeps the
 * human-readable part — the same layout as the Discord adapter.
 */
export function describeTelegramAttachment(attachment: TelegramAttachment, stored: StoredAttachment): string {
	const parts: string[] = [attachment.kind];
	if (attachment.kind === "voice message" || attachment.kind === "audio") {
		if (attachment.duration !== undefined) parts.push(formatDuration(attachment.duration));
	}
	if (attachment.name) parts.push(attachment.name);
	const size = formatSize(attachment.size);
	if (size) parts.push(size);
	parts.push("path" in stored ? stored.path : `not downloaded: ${stored.unavailable}`);
	return `[${parts.join(" · ")}]`;
}

/**
 * Builds the body the gateway receives: the text or caption on its own line with
 * the attachment line appended, so a captioned photo reads as its caption plus
 * the image rather than being rewritten.
 */
export function describeInboundBody(
	content: string,
	attachment: TelegramAttachment | undefined,
	stored: StoredAttachment | undefined,
	/** Marks an image as native input instead of implying the model should read its local path. */
	imageIncluded = false,
): string {
	if (!attachment || !stored) return content;
	const line =
		imageIncluded && attachment.kind === "image"
			? describeTelegramImage(attachment)
			: describeTelegramAttachment(attachment, stored);
	return content.trim() === "" ? line : `${content}\n${line}`;
}

function describeTelegramImage(attachment: TelegramAttachment): string {
	const parts: string[] = [attachment.kind];
	if (attachment.name) parts.push(attachment.name);
	const size = formatSize(attachment.size);
	if (size) parts.push(size);
	return `[${parts.join(" · ")}]`;
}

function isPng(bytes: Uint8Array): boolean {
	return (
		bytes.length >= 8 &&
		bytes[0] === 0x89 &&
		bytes[1] === 0x50 &&
		bytes[2] === 0x4e &&
		bytes[3] === 0x47 &&
		bytes[4] === 0x0d &&
		bytes[5] === 0x0a &&
		bytes[6] === 0x1a &&
		bytes[7] === 0x0a
	);
}

function isJpeg(bytes: Uint8Array): boolean {
	return bytes.length >= 3 && bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff;
}

async function readStoredImage(path: string): Promise<Uint8Array> {
	let handle: Awaited<ReturnType<typeof open>>;
	try {
		handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK | constants.O_NOCTTY);
	} catch {
		throw new Error("image attachment is unavailable");
	}
	try {
		const info = await handle.stat();
		if (!info.isFile() || info.size > TELEGRAM_BOT_DOWNLOAD_LIMIT)
			throw new Error("image attachment exceeds the 20 MB limit or is not a regular file");
		const bytes = Buffer.allocUnsafe(info.size + 1);
		let length = 0;
		while (length < bytes.length) {
			const read = await handle.read(bytes, length, bytes.length - length, null);
			if (read.bytesRead === 0) break;
			length += read.bytesRead;
		}
		if (length > TELEGRAM_BOT_DOWNLOAD_LIMIT || length !== info.size)
			throw new Error("image attachment changed or exceeds the 20 MB limit");
		return bytes.subarray(0, length);
	} catch (error) {
		if (error instanceof Error && error.message.startsWith("image attachment ")) throw error;
		throw new Error("image attachment could not be read");
	} finally {
		await handle.close().catch(() => {});
	}
}

/**
 * Downloads inbound attachments into `directory/<chatId>/` with owner-only
 * permissions. Never throws: deterministic failures retain a sanitized body
 * diagnostic, while temporary failures are marked retryable for the adapter.
 */
export class TelegramAttachmentStore implements TelegramAttachmentSaver {
	constructor(
		readonly directory: string,
		readonly api: TelegramFileApi,
		readonly log: Pick<Console, "error"> = console,
		readonly timeoutMs: number = DOWNLOAD_TIMEOUT_MS,
	) {}

	async save(attachment: TelegramAttachment, chatId: string, messageId: number): Promise<StoredAttachment> {
		if (attachment.size !== undefined && attachment.size > TELEGRAM_BOT_DOWNLOAD_LIMIT)
			return { unavailable: "exceeds the 20 MB bot download limit" };
		let stage: "getFile" | "download" | "store" = "getFile";
		try {
			const controller = new AbortController();
			const timer = setTimeout(() => controller.abort(), this.timeoutMs);
			try {
				const file = await this.api.getFile(attachment.fileId, controller.signal);
				if (!file.file_path) return { unavailable: "Telegram returned no file path" };
				if (validNumber(file.file_size) && file.file_size > TELEGRAM_BOT_DOWNLOAD_LIMIT)
					return { unavailable: "exceeds the 20 MB bot download limit" };
				stage = "download";
				const bytes = await this.download(file.file_path, controller.signal);
				stage = "store";
				const directory = join(this.directory, safeSegment(chatId));
				await mkdir(directory, { recursive: true, mode: 0o700 });
				const extension = safeExtension(file.file_path) ?? safeExtension(attachment.name ?? "") ?? "";
				const path = join(directory, `${messageId}-${safeSegment(attachment.fileUniqueId)}${extension}`);
				const temporary = `${path}.${process.pid}.${crypto.randomUUID()}.tmp`;
				await writeFile(temporary, bytes, { mode: 0o600 });
				await rename(temporary, path);
				return { path };
			} finally {
				clearTimeout(timer);
			}
		} catch (error) {
			// Fetch and Telegram API errors can contain the token-bearing URL. Log only a
			// locally generated HTTP status, never arbitrary error text.
			const detail =
				error instanceof TelegramAttachmentHttpError ? `file download returned HTTP ${error.status}` : "request failed";
			this.log.error(`Telegram attachment download failed: ${detail}`);
			return {
				unavailable: "download failed",
				...(isRetryableAttachmentFailure(error, stage) ? { retryable: true } : {}),
			};
		}
	}

	private async download(filePath: string, signal: AbortSignal): Promise<Uint8Array> {
		const response = await this.api.downloadFile(filePath, signal);
		if (!response.ok) throw new TelegramAttachmentHttpError(response.status);
		if (!response.body) return new Uint8Array();
		const reader = response.body.getReader();
		const chunks: Uint8Array[] = [];
		let size = 0;
		try {
			for (;;) {
				const { done, value } = await reader.read();
				if (done) break;
				size += value.byteLength;
				if (size > TELEGRAM_BOT_DOWNLOAD_LIMIT) {
					await reader.cancel().catch(() => undefined);
					throw new TelegramAttachmentSizeError("file exceeds the bot download limit");
				}
				chunks.push(value);
			}
		} finally {
			reader.releaseLock();
		}
		const bytes = new Uint8Array(size);
		let offset = 0;
		for (const chunk of chunks) {
			bytes.set(chunk, offset);
			offset += chunk.byteLength;
		}
		return bytes;
	}
}

function isRetryableAttachmentFailure(error: unknown, stage: "getFile" | "download" | "store"): boolean {
	if (stage === "store" || error instanceof TelegramAttachmentSizeError) return false;
	const status =
		error instanceof TelegramAttachmentHttpError
			? error.status
			: error instanceof Error && "status" in error && typeof error.status === "number"
				? error.status
				: undefined;
	if (status !== undefined) return status === 408 || status === 429 || (status >= 500 && status <= 599);
	return true;
}

/** Removes downloaded attachments older than `maxAgeMs`; missing directories are fine. */
export async function pruneTelegramAttachments(
	directory: string,
	now: number = Date.now(),
	maxAgeMs: number = ATTACHMENT_RETENTION_MS,
): Promise<number> {
	let removed = 0;
	for (const chat of await readdirOrEmpty(directory)) {
		const chatDirectory = join(directory, chat);
		const files = await readdirOrEmpty(chatDirectory);
		for (const name of files) {
			const path = join(chatDirectory, name);
			const info = await stat(path).catch(() => undefined);
			if (!info?.isFile() || now - info.mtimeMs <= maxAgeMs) continue;
			await rm(path, { force: true });
			removed++;
		}
		if (files.length > 0 && (await readdirOrEmpty(chatDirectory)).length === 0)
			await rm(chatDirectory, { recursive: true, force: true });
	}
	return removed;
}

async function readdirOrEmpty(path: string): Promise<string[]> {
	try {
		return await readdir(path);
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		if (code === "ENOENT" || code === "ENOTDIR") return [];
		throw error;
	}
}

function largestPhoto(sizes: readonly TelegramPhotoSize[] | undefined): TelegramPhotoSize | undefined {
	let best: TelegramPhotoSize | undefined;
	for (const size of sizes ?? []) {
		if (!best || size.width * size.height > best.width * best.height) best = size;
	}
	return best;
}

function fromFile(kind: TelegramAttachmentKind, file: TelegramTimedFileShape | TelegramPhotoSize): TelegramAttachment {
	const named = file as TelegramTimedFileShape;
	const name = nonBlank(named.file_name);
	const mimeType = nonBlank(named.mime_type);
	return {
		kind,
		fileId: file.file_id,
		fileUniqueId: file.file_unique_id,
		...(name ? { name } : {}),
		...(mimeType ? { mimeType } : {}),
		...(validNumber(file.file_size) ? { size: file.file_size } : {}),
		...(validNumber(named.duration) ? { duration: named.duration } : {}),
	};
}

function kindForMime(mimeType: string | undefined): TelegramAttachmentKind {
	const mime = nonBlank(mimeType)?.toLowerCase() ?? "";
	if (mime === "image/png" || mime === "image/jpeg") return "image";
	if (mime.startsWith("video/")) return "video";
	if (mime.startsWith("audio/")) return "audio";
	return "file";
}

/** Telegram ids are attacker-influenced only in theory, but they become path segments. */
function safeSegment(value: string): string {
	const cleaned = value.replace(/[^A-Za-z0-9_-]/g, "_");
	return cleaned === "" ? "_" : cleaned;
}

function safeExtension(value: string): string | undefined {
	const extension = extname(value);
	return /^\.[A-Za-z0-9]{1,10}$/.test(extension) ? extension.toLowerCase() : undefined;
}

function formatDuration(seconds: number): string {
	if (seconds < 60) return `${Math.round(seconds * 10) / 10}s`;
	const minutes = Math.floor(seconds / 60);
	const rest = Math.floor(seconds % 60);
	return `${minutes}:${String(rest).padStart(2, "0")}`;
}

function formatSize(size: number | undefined): string | undefined {
	if (!validNumber(size)) return undefined;
	if (size < 1024) return `${Math.round(size)} B`;
	if (size < 1024 * 1024) return `${Math.round((size / 1024) * 10) / 10} KB`;
	return `${Math.round((size / (1024 * 1024)) * 10) / 10} MB`;
}

function validNumber(value: number | undefined): value is number {
	return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function nonBlank(value: string | undefined): string | undefined {
	return typeof value === "string" && value.trim() !== "" ? value : undefined;
}
