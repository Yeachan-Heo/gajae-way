import { Buffer } from "node:buffer";
import type { PromptImage } from "@gajae-gateway/protocol";
import { imageSize } from "image-size";
import { Jimp } from "jimp";

export const MAX_PROMPT_IMAGES = 4;
export const MAX_IMAGE_TOTAL_BYTES = 96 * 1024;

const MAX_SOURCE_IMAGE_BYTES = 20 * 1024 * 1024;
const MAX_IMAGE_PIXELS = 16_000_000;
const MAX_IMAGE_EDGE = 1280;
const PREPARE_QUALITIES = [75, 60, 45, 30, 20, 10] as const;
const MAX_ENCODE_ATTEMPTS = 48;
const JPEG_MIME_TYPE = "image/jpeg" as const;
const INVALID_IMAGES = "Invalid prompt images.";
const INVALID_IMAGE = "Invalid prompt image.";
const DOWNLOAD_FAILED = "Image download failed.";
const URL_NOT_ALLOWED = "Image URL is not allowed.";

/** Sanitized failure whose retry policy is safe for adapter recovery. */
export class PromptImageDownloadError extends Error {
	constructor(
		readonly retryable: boolean,
		message = DOWNLOAD_FAILED,
	) {
		super(message);
		this.name = "PromptImageDownloadError";
	}
}

export class ImagePreparationBudgetError extends Error {
	constructor() {
		super("Image preparation budget exhausted.");
		this.name = "ImagePreparationBudgetError";
	}
}

/** Share one instance across an adapter's live and recovery ingress, never one per origin. */
export class ImagePreparationBudget {
	#active = 0;
	#starts: number[] = [];

	constructor(private readonly now: () => number = () => performance.now()) {}

	/** No queue: admission precedes retrieval, and the slot covers all downloads and decoding. */
	async run<T>(work: () => Promise<T>): Promise<T> {
		const now = this.now();
		this.#starts = this.#starts.filter((start) => now - start < 60_000);
		if (this.#active >= 2 || this.#starts.length >= 8) throw new ImagePreparationBudgetError();
		this.#starts.push(now);
		this.#active += 1;
		try {
			return await work();
		} finally {
			this.#active -= 1;
		}
	}
}

interface ImageDimensions {
	readonly width: number;
	readonly height: number;
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
	return bytes.length >= 2 && bytes[0] === 0xff && bytes[1] === 0xd8;
}

function readUInt32BE(bytes: Uint8Array, offset: number): number {
	return new DataView(bytes.buffer, bytes.byteOffset + offset, 4).getUint32(0, false);
}

function assertStaticPng(bytes: Uint8Array): void {
	let offset = 8;
	let chunkCount = 0;
	let hasImageData = false;
	let hasEnd = false;

	while (offset + 12 <= bytes.length) {
		const length = readUInt32BE(bytes, offset);
		if (length > bytes.length - offset - 12) throw new Error(INVALID_IMAGE);
		const typeOffset = offset + 4;
		const type = String.fromCharCode(
			bytes[typeOffset],
			bytes[typeOffset + 1],
			bytes[typeOffset + 2],
			bytes[typeOffset + 3],
		);

		if (chunkCount === 0 && (type !== "IHDR" || length !== 13)) throw new Error(INVALID_IMAGE);
		if (type === "acTL") throw new Error(INVALID_IMAGE);
		if (type === "IDAT") hasImageData = true;
		if (type === "IEND") {
			if (length !== 0) throw new Error(INVALID_IMAGE);
			hasEnd = true;
			break;
		}

		chunkCount += 1;
		offset += length + 12;
	}

	if (!hasImageData || !hasEnd) throw new Error(INVALID_IMAGE);
}

function checkDimensions(dimensions: ImageDimensions): void {
	if (
		!Number.isSafeInteger(dimensions.width) ||
		!Number.isSafeInteger(dimensions.height) ||
		dimensions.width <= 0 ||
		dimensions.height <= 0 ||
		dimensions.width * dimensions.height > MAX_IMAGE_PIXELS
	) {
		throw new Error(INVALID_IMAGE);
	}
}

function inspectSourceImage(bytes: Uint8Array): ImageDimensions {
	if (!(bytes instanceof Uint8Array) || bytes.length === 0 || bytes.length > MAX_SOURCE_IMAGE_BYTES) {
		throw new Error(INVALID_IMAGE);
	}

	let expectedType: "jpg" | "png";
	if (isPng(bytes)) {
		assertStaticPng(bytes);
		expectedType = "png";
	} else if (isJpeg(bytes)) {
		expectedType = "jpg";
	} else {
		throw new Error(INVALID_IMAGE);
	}

	let dimensions: ReturnType<typeof imageSize>;
	try {
		dimensions = imageSize(bytes);
	} catch {
		throw new Error(INVALID_IMAGE);
	}
	if (dimensions.type !== expectedType) throw new Error(INVALID_IMAGE);
	checkDimensions(dimensions);
	return dimensions;
}

function inspectPromptJpeg(bytes: Uint8Array): void {
	if (!isJpeg(bytes)) throw new Error(INVALID_IMAGES);
	let dimensions: ReturnType<typeof imageSize>;
	try {
		dimensions = imageSize(bytes);
	} catch {
		throw new Error(INVALID_IMAGES);
	}
	if (dimensions.type !== "jpg") throw new Error(INVALID_IMAGES);
	try {
		checkDimensions(dimensions);
	} catch {
		throw new Error(INVALID_IMAGES);
	}
}

/** Validate trusted protocol image values without fully decoding their pixels. */
export function validatePromptImages(value: unknown): PromptImage[] {
	if (value === undefined) return [];
	if (!Array.isArray(value) || value.length > MAX_PROMPT_IMAGES) throw new Error(INVALID_IMAGES);

	const images: PromptImage[] = [];
	let totalBytes = 0;
	for (const candidate of value) {
		if (
			candidate === null ||
			typeof candidate !== "object" ||
			!("data" in candidate) ||
			typeof candidate.data !== "string" ||
			candidate.data.length === 0 ||
			candidate.data.length > (MAX_IMAGE_TOTAL_BYTES * 4) / 3 ||
			!("mimeType" in candidate) ||
			candidate.mimeType !== JPEG_MIME_TYPE
		) {
			throw new Error(INVALID_IMAGES);
		}

		const encoded = candidate.data;
		if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(encoded)) {
			throw new Error(INVALID_IMAGES);
		}
		const bytes = Buffer.from(encoded, "base64");
		if (bytes.length === 0 || bytes.toString("base64") !== encoded) throw new Error(INVALID_IMAGES);
		totalBytes += bytes.length;
		if (totalBytes > MAX_IMAGE_TOTAL_BYTES) throw new Error(INVALID_IMAGES);
		inspectPromptJpeg(bytes);
		images.push({ data: encoded, mimeType: JPEG_MIME_TYPE });
	}

	return images;
}

async function encodeWithinBudget(
	decoded: Awaited<ReturnType<typeof Jimp.read>>,
	byteBudget: number,
): Promise<Uint8Array> {
	const originalDimensions = { width: decoded.bitmap.width, height: decoded.bitmap.height };
	const longestEdge = Math.max(originalDimensions.width, originalDimensions.height);
	let edge = Math.min(MAX_IMAGE_EDGE, longestEdge);
	let attempts = 0;

	while (edge >= 1 && attempts < MAX_ENCODE_ATTEMPTS) {
		const candidate = decoded.clone();
		// Jimp composites alpha against this background when encoding JPEG.
		candidate.background = 0xffffffff;
		const scale = edge / longestEdge;
		if (scale < 1) {
			candidate.resize({
				w: Math.max(1, Math.round(originalDimensions.width * scale)),
				h: Math.max(1, Math.round(originalDimensions.height * scale)),
			});
		}

		for (const quality of PREPARE_QUALITIES) {
			if (attempts >= MAX_ENCODE_ATTEMPTS) break;
			attempts += 1;
			const encoded = await candidate.getBuffer(JPEG_MIME_TYPE, { quality });
			if (encoded.length <= byteBudget) return encoded;
		}
		edge = Math.floor(edge * 0.75);
	}

	throw new Error("Prompt images cannot fit within the model input budget.");
}

/** Convert bounded static PNG/JPEG sources into canonical, budgeted JPEG prompt images. */
export async function preparePromptImages(
	sources: readonly Uint8Array[],
): Promise<{ images: PromptImage[]; notice: string }> {
	if (!Array.isArray(sources) || sources.length > MAX_PROMPT_IMAGES) throw new Error(INVALID_IMAGES);
	if (sources.length === 0) return { images: [], notice: "" };

	for (const source of sources) inspectSourceImage(source);
	const byteBudget = Math.floor(MAX_IMAGE_TOTAL_BYTES / sources.length);
	const images: PromptImage[] = [];

	for (let index = 0; index < sources.length; index += 1) {
		try {
			const decoded = await Jimp.read(Buffer.from(sources[index]));
			const encoded = await encodeWithinBudget(decoded, byteBudget);
			images.push({ data: Buffer.from(encoded).toString("base64"), mimeType: JPEG_MIME_TYPE });
		} catch {
			throw new Error("Prompt image could not be prepared.");
		}
	}

	return {
		images: validatePromptImages(images),
		notice: "Images resized/compressed for model input; fine detail may be lost.",
	};
}

/** Only the callable fetch contract, not Bun's optional connection helpers. */
export type ImageFetch = (...args: Parameters<typeof fetch>) => ReturnType<typeof fetch>;

export interface DownloadPromptImageOptions {
	readonly allowedHosts: readonly string[];
	readonly headers?: Readonly<Record<string, string>>;
	readonly fetch?: ImageFetch;
	/** Explicit manual redirect allowlist. Redirects are limited to three hops. */
	readonly redirectPolicy?: { readonly allowedHosts: readonly string[] };
}

const MAX_IMAGE_REDIRECTS = 3;
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);

function validateImageUrl(value: string, allowedHosts: readonly string[], base?: URL): URL {
	const hasScheme = /^[A-Za-z][A-Za-z0-9+.-]*:/.test(value);
	const authority = /^https:\/\/([^/?#]*)/i.exec(value)?.[1] ?? /^\/\/([^/?#]*)/.exec(value)?.[1];
	if (
		value.includes("#") ||
		(!base && !/^https:\/\//i.test(value)) ||
		(base && hasScheme && !/^https:\/\//i.test(value)) ||
		(authority !== undefined &&
			(authority === "" || authority.includes(":") || authority.includes("@") || !/^[A-Za-z0-9.-]+$/.test(authority)))
	) {
		throw new PromptImageDownloadError(false, URL_NOT_ALLOWED);
	}

	let url: URL;
	try {
		url = base ? new URL(value, base) : new URL(value);
	} catch {
		throw new PromptImageDownloadError(false, URL_NOT_ALLOWED);
	}
	if (
		url.protocol !== "https:" ||
		url.username !== "" ||
		url.password !== "" ||
		url.port !== "" ||
		value.includes("#") ||
		!allowedHosts.some((host) => host.toLowerCase() === url.hostname)
	) {
		throw new PromptImageDownloadError(false, URL_NOT_ALLOWED);
	}
	return url;
}

function headersForImageRequest(headers: Readonly<Record<string, string>> | undefined, url: URL, initialHost: string) {
	if (!headers) return undefined;
	const result = new Headers(headers);
	if (url.hostname !== initialHost) {
		result.delete("authorization");
		result.delete("proxy-authorization");
		result.delete("cookie");
		result.delete("cookie2");
	}
	return result;
}

function isHtmlResponse(response: Response): boolean {
	const contentType = response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
	return contentType === "text/html" || contentType === "application/xhtml+xml";
}

function looksLikeHtml(bytes: Uint8Array): boolean {
	const prefix = new TextDecoder().decode(bytes.subarray(0, Math.min(bytes.length, 512)));
	return /^\uFEFF?\s*(?:<!doctype\s+html\b|<html\b|<head\b|<body\b)/i.test(prefix);
}

async function cancelResponse(response: Response): Promise<void> {
	try {
		await response.body?.cancel();
	} catch {
		// Cancellation is best effort; the caller still receives a sanitized failure.
	}
}

/** Download a bounded image from an explicitly trusted HTTPS host. */
export async function downloadPromptImage(url: string, options: DownloadPromptImageOptions): Promise<Uint8Array> {
	const trustedUrl = validateImageUrl(url, options.allowedHosts);
	if (
		options.redirectPolicy &&
		!options.redirectPolicy.allowedHosts.some((host) => host.toLowerCase() === trustedUrl.hostname)
	) {
		throw new PromptImageDownloadError(false, URL_NOT_ALLOWED);
	}
	const fetcher = options.fetch ?? fetch;
	const controller = new AbortController();
	const timeout = setTimeout(() => controller.abort(), 30_000);

	try {
		let currentUrl = trustedUrl;
		let redirects = 0;
		const visited = new Set([currentUrl.href]);
		while (true) {
			const response = await fetcher(currentUrl, {
				method: "GET",
				headers: headersForImageRequest(options.headers, currentUrl, trustedUrl.hostname),
				redirect: options.redirectPolicy ? "manual" : "error",
				signal: controller.signal,
			});
			if (response.redirected) {
				await cancelResponse(response);
				throw new PromptImageDownloadError(false);
			}
			if (options.redirectPolicy && REDIRECT_STATUSES.has(response.status)) {
				const location = response.headers.get("location");
				await cancelResponse(response);
				if (!location || redirects >= MAX_IMAGE_REDIRECTS) throw new PromptImageDownloadError(false);
				const nextUrl = validateImageUrl(location, options.redirectPolicy.allowedHosts, currentUrl);
				if (visited.has(nextUrl.href)) throw new PromptImageDownloadError(false, URL_NOT_ALLOWED);
				visited.add(nextUrl.href);
				currentUrl = nextUrl;
				redirects += 1;
				continue;
			}
			if (!response.ok) {
				await cancelResponse(response);
				throw new PromptImageDownloadError(
					response.status >= 500 || response.status === 408 || response.status === 429,
				);
			}
			if (isHtmlResponse(response)) {
				await cancelResponse(response);
				throw new PromptImageDownloadError(false);
			}
			const contentLength = Number(response.headers.get("content-length"));
			if (Number.isFinite(contentLength) && contentLength > MAX_SOURCE_IMAGE_BYTES) {
				await cancelResponse(response);
				throw new PromptImageDownloadError(false);
			}
			if (!response.body) throw new PromptImageDownloadError(false);

			const reader = response.body.getReader();
			const chunks: Uint8Array[] = [];
			let totalBytes = 0;
			try {
				while (true) {
					const { done, value } = await reader.read();
					if (done) break;
					totalBytes += value.length;
					if (totalBytes > MAX_SOURCE_IMAGE_BYTES) {
						throw new PromptImageDownloadError(false);
					}
					chunks.push(value);
				}
			} catch (error) {
				try {
					await reader.cancel();
				} catch {
					// Cancellation is best effort after a failed or aborted read.
				}
				throw error;
			} finally {
				reader.releaseLock();
			}

			const result = new Uint8Array(totalBytes);
			let offset = 0;
			for (const chunk of chunks) {
				result.set(chunk, offset);
				offset += chunk.length;
			}
			if (looksLikeHtml(result)) throw new PromptImageDownloadError(false);
			return result;
		}
	} catch (error) {
		throw error instanceof PromptImageDownloadError ? error : new PromptImageDownloadError(true);
	} finally {
		clearTimeout(timeout);
	}
}
