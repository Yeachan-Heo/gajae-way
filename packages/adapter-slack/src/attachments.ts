/** File descriptions preserve attachment-only Slack messages in the text protocol. */
export interface SlackFileLike {
	readonly name?: string | null;
	readonly title?: string | null;
	readonly mimetype?: string | null;
	readonly size?: number | null;
	readonly url_private?: string | null;
	readonly permalink?: string | null;
}

export type SlackFileCarrier = { readonly text?: string | null; readonly files?: readonly SlackFileLike[] | null };

export function describeSlackFile(file: SlackFileLike): string {
	const mime = nonBlank(file.mimetype)?.toLowerCase() ?? "";
	const kind = ["image", "video", "audio"].find((kind) => mime.startsWith(`${kind}/`)) ?? "file";
	const parts = [
		kind,
		nonBlank(file.name) ?? nonBlank(file.title),
		formatSize(file.size),
		nonBlank(file.url_private) ?? nonBlank(file.permalink),
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
