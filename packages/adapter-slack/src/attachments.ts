import { downloadPromptImage, type ImageFetch, preparePromptImages } from "@gajae-gateway/images";
import type { PromptImage } from "@gajae-gateway/protocol";

/** File descriptions preserve attachment-only Slack messages in the text protocol. */
export interface SlackFileLike {
	readonly id?: string | null;
	readonly name?: string | null;
	readonly title?: string | null;
	readonly mimetype?: string | null;
	readonly size?: number | null;
	readonly url_private?: string | null;
	readonly permalink?: string | null;
}

export type SlackFileCarrier = { readonly text?: string | null; readonly files?: readonly SlackFileLike[] | null };

const SLACK_IMAGE_HOSTS = ["files.slack.com"] as const;
const SLACK_IMAGE_LIMIT = 4;

export function isSlackImage(file: SlackFileLike): boolean {
	const name = nonBlank(file.name) ?? nonBlank(file.title) ?? "";
	return (
		(typeof file.mimetype === "string" && file.mimetype.toLowerCase().startsWith("image/")) ||
		/\.(?:avif|bmp|gif|heic|heif|jpe?g|png|svg|tiff?|webp)$/i.test(name)
	);
}

/** Downloads native images only from Slack's private file URL before normalizing them for chat.send. */
export async function prepareSlackImages(
	files: readonly SlackFileLike[] | null | undefined,
	botToken: string,
	fetchImpl?: ImageFetch,
): Promise<{ readonly images: PromptImage[]; readonly notice: string }> {
	const images = (files ?? []).filter(isSlackImage);
	if (images.length === 0) return { images: [], notice: "" };
	if (images.length > SLACK_IMAGE_LIMIT) throw new Error("Slack messages can include at most four images");
	const sources = await Promise.all(
		images.map((file) => {
			const url = nonBlank(file.url_private);
			if (!url) throw new Error("Slack image attachment is missing its private URL");
			return downloadPromptImage(url, {
				allowedHosts: SLACK_IMAGE_HOSTS,
				headers: { Authorization: `Bearer ${botToken}` },
				...(fetchImpl ? { fetch: fetchImpl } : {}),
			});
		}),
	);
	return preparePromptImages(sources);
}

/** Slack file ids: `F` plus upper-case alphanumerics. Anything else is not rendered as an id. */
const SLACK_FILE_ID = /^F[A-Z0-9]{6,}$/;

/**
 * `[kind · name · id F… · size · url]`. The file id is what tells two
 * attachments apart: Slack names every pasted screenshot `image.png`, and a
 * persona that matched by name opened another thread's earlier download as
 * the current message's image (live, sandoh 2026-10-13: F0C8P71RK54 answered
 * for F0C8PPGAV7G). It is also the only handle left when Slack sends a partial
 * file object without `url_private`.
 */
export function describeSlackFile(file: SlackFileLike): string {
	const mime = nonBlank(file.mimetype)?.toLowerCase() ?? "";
	const kind = isSlackImage(file)
		? "image"
		: (["video", "audio"].find((kind) => mime.startsWith(`${kind}/`)) ?? "file");
	const id =
		typeof file.id === "string" && file.id.trim() === file.id && SLACK_FILE_ID.test(file.id)
			? `id ${file.id}`
			: undefined;
	const parts = [
		kind,
		nonBlank(file.name) ?? nonBlank(file.title),
		id,
		formatSize(file.size),
		kind === "image" ? undefined : (nonBlank(file.url_private) ?? nonBlank(file.permalink)),
	];
	return `[${parts.filter((part) => part !== undefined).join(" · ")}]`;
}

export function describeInboundBody(message: SlackFileCarrier): string {
	const text = message.text ?? "";
	const files = message.files ?? [];
	if (files.length === 0) return text;
	const rendered = files.slice(0, 10).map(describeSlackFile);
	const overflow = files.length - rendered.length;
	if (overflow > 0) rendered.push(`[+${overflow} more attachment${overflow === 1 ? "" : "s"}]`);
	return text.trim() === "" ? rendered.join("\n") : `${text}\n${rendered.join("\n")}`;
}

function nonBlank(value: string | null | undefined): string | undefined {
	return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function formatSize(size: number | null | undefined): string | undefined {
	if (typeof size !== "number" || !Number.isFinite(size) || size < 0) return undefined;
	if (size < 1024) return `${Math.round(size)} B`;
	if (size < 1024 * 1024) return `${Math.round((size / 1024) * 10) / 10} KB`;
	return `${Math.round((size / (1024 * 1024)) * 10) / 10} MB`;
}
