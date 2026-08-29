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
 * The fix renders attachments into the message body as a text line per
 * attachment, including the CDN url. That keeps the change inside the adapter:
 * no protocol field, no schema migration, and every downstream consumer
 * (context ledger, memory capture, turn header) sees the attachment because it
 * is simply part of the text. The persona has a shell, so a url is all it needs
 * to fetch and transcribe or inspect the file itself.
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

/** A voice message is the one attachment kind that carries duration and waveform. */
export function isVoiceMessageAttachment(attachment: AttachmentLike): boolean {
	return (
		typeof attachment.duration === "number" && attachment.duration >= 0 && nonBlank(attachment.waveform) !== undefined
	);
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
	if (url) parts.push(url);
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
	const contentType = nonBlank(attachment.contentType)?.toLowerCase() ?? "";
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
