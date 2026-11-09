import type { ImageFetch } from "@gajae-gateway/images";
import { downloadPromptImage, PromptImageDownloadError, preparePromptImages } from "@gajae-gateway/images";
import type { PromptImage } from "@gajae-gateway/protocol";

/**
 * Attachment rendering for inbound Discord messages.
 *
 * The adapter used to forward `message.content` and nothing else, so every
 * attachment-only message became an empty body — and the gateway rejects empty
 * text outright (`chat.send requires non-empty text`). The result was silent
 * loss: a Discord **voice message** carries no content at all, only a
 * `voice-message.ogg` attachment, so it never reached the persona and never even
 * landed in the conversation-context ledger. Images and files sent without a
 * caption disappeared the same way.
 *
 * Native images are downloaded and prepared as bounded prompt inputs; other
 * attachments remain described in text. Image descriptions omit signed CDN urls.
 *
 * Discord CDN urls are signed and expire (roughly a day), so a body kept in
 * memory is not a durable handle to the bytes — anything that needs the file
 * later has to download it during the turn.
 *
 * Kept free of any `discord.js` import so the rules stay unit testable without
 * the gateway client's dependency tree.
 */

/** How many attachments are rendered before the rest are summarized. */
const MAX_RENDERED_ATTACHMENTS = 10;

export type AttachmentLike = {
	readonly name?: string | null;
	readonly contentType?: string | null;
	readonly size?: number | null;
	readonly url?: string | null;
	/**
	 * Voice-message length in seconds (`duration_secs`). Discord sets this and
	 * `waveform` only for voice messages, which is how one is recognized without
	 * plumbing the message-level `IsVoiceMessage` flag through the adapter.
	 */
	readonly duration?: number | null;
	readonly waveform?: string | null;
};

/**
 * Where the attachments come from.
 *
 * discord.js hands over a `Collection`, which extends `Map` — iterating it
 * yields `[id, attachment]` entries, not attachments. So `values()` is used
 * whenever it exists and bare iteration is only the fallback, which also lets a
 * test pass a plain array.
 */
export type AttachmentSource = Iterable<AttachmentLike> | { readonly values: () => Iterable<AttachmentLike> };

export type AttachmentCarrier = {
	readonly attachments?: AttachmentSource | null | undefined;
};

export const DISCORD_IMAGE_ALLOWED_HOSTS = ["cdn.discordapp.com", "media.discordapp.net"] as const;

export type DiscordImagePreparationPorts = {
	readonly download: typeof downloadPromptImage;
	readonly prepare: typeof preparePromptImages;
	readonly fetch?: ImageFetch;
};

export class DiscordImagePreparationError extends Error {
	constructor(
		readonly disposition: "retryable" | "terminal",
		message = disposition === "terminal" ? "Discord image input rejected." : "Discord image download failed.",
	) {
		super(message);
		this.name = "DiscordImagePreparationError";
	}
}

const DEFAULT_IMAGE_PREPARATION_PORTS: DiscordImagePreparationPorts = {
	download: downloadPromptImage,
	prepare: preparePromptImages,
};

/** Attachments whose declared MIME type identifies them as images. */
export function selectImageAttachments(message: AttachmentCarrier): AttachmentLike[] {
	return collectAttachments(message).filter(isImageAttachment);
}

/** Prepare Discord image attachments for a native prompt, never fetching non-image attachments. */
export async function prepareDiscordImages(
	message: AttachmentCarrier,
	ports: DiscordImagePreparationPorts = DEFAULT_IMAGE_PREPARATION_PORTS,
): Promise<{ readonly images: PromptImage[]; readonly notice: string }> {
	const attachments = selectImageAttachments(message);
	if (attachments.length === 0) return { images: [], notice: "" };
	if (attachments.length > 4)
		throw new DiscordImagePreparationError("terminal", "Discord messages support at most 4 images.");
	const urls = attachments.map((attachment) => {
		const url = nonBlank(attachment.url);
		if (!url || (typeof attachment.size === "number" && attachment.size > MAX_DISCORD_IMAGE_SOURCE_BYTES))
			throw new DiscordImagePreparationError("terminal");
		return url;
	});
	const sources = await Promise.all(urls.map((url) => downloadDiscordImage(url, ports)));
	try {
		return await ports.prepare(sources);
	} catch {
		throw new DiscordImagePreparationError("terminal");
	}
}

const MAX_DISCORD_IMAGE_SOURCE_BYTES = 20 * 1024 * 1024;

async function downloadDiscordImage(url: string, ports: DiscordImagePreparationPorts): Promise<Uint8Array> {
	try {
		return await ports.download(url, {
			allowedHosts: DISCORD_IMAGE_ALLOWED_HOSTS,
			...(ports.fetch ? { fetch: ports.fetch } : {}),
		});
	} catch (error) {
		throw new DiscordImagePreparationError(
			error instanceof PromptImageDownloadError && !error.retryable ? "terminal" : "retryable",
		);
	}
}

function isImageAttachment(attachment: AttachmentLike): boolean {
	return nonBlank(attachment.contentType)?.trim().toLowerCase().startsWith("image/") ?? false;
}

/** A voice message is the one attachment kind that carries duration and waveform. */
export function isVoiceMessageAttachment(attachment: AttachmentLike): boolean {
	return (
		typeof attachment.duration === "number" && attachment.duration >= 0 && nonBlank(attachment.waveform) !== undefined
	);
}

/**
 * The voice message on a message, when there is one.
 *
 * Discord's own client sends a voice message alone, and the API rejects mixing
 * one with other attachments, so the first match is the only match in practice —
 * but a hostile or future payload could carry several, and transcribing all of
 * them would multiply a third-party call by an attacker-chosen factor.
 */
export function firstVoiceMessage(message: AttachmentCarrier): AttachmentLike | undefined {
	return collectAttachments(message).find(isVoiceMessageAttachment);
}

/**
 * Renders one attachment as a single bracketed line.
 *
 * The kind label comes first so the persona can dispatch on it without parsing
 * a mime type, and the url comes last so a truncated body still keeps the
 * human-readable part.
 */
export function describeAttachment(attachment: AttachmentLike): string {
	const url = nonBlank(attachment.url);
	const parts: string[] = [];
	if (isVoiceMessageAttachment(attachment)) {
		parts.push("voice message");
		parts.push(`${formatDuration(attachment.duration as number)}`);
	} else {
		parts.push(attachmentKind(attachment));
		const name = nonBlank(attachment.name);
		if (name) parts.push(name);
	}
	const size = formatSize(attachment.size);
	if (size) parts.push(size);
	// Image bytes are sent natively; exposing their signed CDN URL in prompt text
	// would be both misleading and an unnecessary credential leak.
	if (url && !isImageAttachment(attachment)) parts.push(url);
	return `[${parts.join(" · ")}]`;
}

/**
 * Builds the body the gateway receives for an inbound message.
 *
 * A caption keeps its own line and attachments follow, so a message that had
 * text reads unchanged with the attachment appended rather than rewritten.
 * Returns the untouched content when there is nothing attached, and an empty
 * string only when the message truly carried neither — that case is still
 * dropped upstream, deliberately, because there is nothing to say about it.
 */
export function describeInboundBody(message: AttachmentCarrier & { readonly content?: string }): string {
	const content = typeof message.content === "string" ? message.content : "";
	const attachments = collectAttachments(message);
	if (attachments.length === 0) return content;
	const rendered = attachments.slice(0, MAX_RENDERED_ATTACHMENTS).map(describeAttachment);
	const overflow = attachments.length - rendered.length;
	if (overflow > 0) rendered.push(`[+${overflow} more attachment${overflow === 1 ? "" : "s"}]`);
	const lines = rendered.join("\n");
	return content.trim() === "" ? lines : `${content}\n${lines}`;
}

/** Materializes the attachments, tolerating the field being absent or empty. */
export function collectAttachments(message: AttachmentCarrier): AttachmentLike[] {
	const attachments = message.attachments;
	if (!attachments) return [];
	const source =
		"values" in attachments && typeof attachments.values === "function"
			? attachments.values()
			: (attachments as Iterable<AttachmentLike>);
	const collected: AttachmentLike[] = [];
	for (const attachment of source) if (attachment) collected.push(attachment);
	return collected;
}

function attachmentKind(attachment: AttachmentLike): string {
	const contentType = nonBlank(attachment.contentType)?.trim().toLowerCase() ?? "";
	if (contentType.startsWith("image/")) return "image";
	if (contentType.startsWith("video/")) return "video";
	if (contentType.startsWith("audio/")) return "audio";
	return "file";
}

/**
 * Formats a voice-message length. Sub-minute clips read in seconds because that
 * is how Discord itself labels them; longer ones get m:ss.
 */
function formatDuration(seconds: number): string {
	if (seconds < 60) return `${round(seconds, 1)}s`;
	const minutes = Math.floor(seconds / 60);
	const rest = Math.floor(seconds % 60);
	return `${minutes}:${String(rest).padStart(2, "0")}`;
}

function formatSize(size: number | null | undefined): string | undefined {
	if (typeof size !== "number" || !Number.isFinite(size) || size < 0) return undefined;
	if (size < 1024) return `${Math.round(size)} B`;
	if (size < 1024 * 1024) return `${round(size / 1024, 1)} KB`;
	return `${round(size / (1024 * 1024), 1)} MB`;
}

function round(value: number, digits: number): number {
	const factor = 10 ** digits;
	return Math.round(value * factor) / factor;
}

function nonBlank(value: string | null | undefined): string | undefined {
	return typeof value === "string" && value.trim() !== "" ? value : undefined;
}
