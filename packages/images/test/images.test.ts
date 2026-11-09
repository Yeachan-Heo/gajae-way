import { describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { Jimp } from "jimp";
import {
	downloadPromptImage,
	MAX_IMAGE_TOTAL_BYTES,
	MAX_PROMPT_IMAGES,
	PromptImageDownloadError,
	preparePromptImages,
	validatePromptImages,
} from "../src";

const MAX_SOURCE_IMAGE_BYTES = 20 * 1024 * 1024;
const jpegFrameMarkers = new Set([0xc0, 0xc1, 0xc2, 0xc3, 0xc5, 0xc6, 0xc7, 0xc9, 0xca, 0xcb, 0xcd, 0xce, 0xcf]);

async function makePng(width = 32, height = 24): Promise<Uint8Array> {
	return new Jimp({ width, height, color: 0x4c83baff }).getBuffer("image/png");
}

async function makeJpeg(): Promise<Uint8Array> {
	return new Jimp({ width: 32, height: 24, color: 0x4c83baff }).getBuffer("image/jpeg", { quality: 75 });
}

function setUInt32BE(bytes: Uint8Array, offset: number, value: number): void {
	new DataView(bytes.buffer, bytes.byteOffset + offset, 4).setUint32(0, value, false);
}

function patchPngDimensions(source: Uint8Array, width: number, height: number): Uint8Array {
	const bytes = source.slice();
	setUInt32BE(bytes, 16, width);
	setUInt32BE(bytes, 20, height);
	return bytes;
}

function patchJpegDimensions(source: Uint8Array, width: number, height: number): Uint8Array {
	const bytes = source.slice();
	for (let offset = 2; offset + 9 < bytes.length; ) {
		if (bytes[offset] !== 0xff) throw new Error("Unexpected JPEG fixture.");
		const marker = bytes[offset + 1];
		if (jpegFrameMarkers.has(marker)) {
			bytes[offset + 5] = (height >>> 8) & 0xff;
			bytes[offset + 6] = height & 0xff;
			bytes[offset + 7] = (width >>> 8) & 0xff;
			bytes[offset + 8] = width & 0xff;
			return bytes;
		}
		if (marker === 0xda || marker === 0xd9) break;
		const length = (bytes[offset + 2] << 8) | bytes[offset + 3];
		offset += length + 2;
	}
	throw new Error("JPEG fixture has no frame header.");
}

function crc32(bytes: Uint8Array): number {
	let crc = 0xffffffff;
	for (const byte of bytes) {
		crc ^= byte;
		for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (crc & 1 ? 0xedb88320 : 0);
	}
	return (crc ^ 0xffffffff) >>> 0;
}

function insertPngChunk(source: Uint8Array, type: string, data: Uint8Array): Uint8Array {
	const prefixLength = 33;
	const chunk = new Uint8Array(data.length + 12);
	setUInt32BE(chunk, 0, data.length);
	for (let index = 0; index < 4; index += 1) chunk[4 + index] = type.charCodeAt(index);
	chunk.set(data, 8);
	setUInt32BE(chunk, chunk.length - 4, crc32(chunk.subarray(4, chunk.length - 4)));
	const result = new Uint8Array(source.length + chunk.length);
	result.set(source.subarray(0, prefixLength), 0);
	result.set(chunk, prefixLength);
	result.set(source.subarray(prefixLength), prefixLength + chunk.length);
	return result;
}

function promptImage(bytes: Uint8Array): { data: string; mimeType: "image/jpeg" } {
	return { data: Buffer.from(bytes).toString("base64"), mimeType: "image/jpeg" };
}

describe("prompt image preparation", () => {
	test("returns canonical budgeted JPEGs and an explicit notice", async () => {
		const source = await makePng();
		const result = await preparePromptImages([source, await makeJpeg()]);
		expect(result.images).toHaveLength(2);
		expect(result.notice).toContain("fine detail may be lost");
		for (const image of result.images) {
			const bytes = Buffer.from(image.data, "base64");
			expect(image.mimeType).toBe("image/jpeg");
			expect(bytes[0]).toBe(0xff);
			expect(bytes[1]).toBe(0xd8);
			expect(bytes.toString("base64")).toBe(image.data);
		}
		expect(result.images.reduce((sum, image) => sum + Buffer.from(image.data, "base64").length, 0)).toBeLessThanOrEqual(
			MAX_IMAGE_TOTAL_BYTES,
		);
	});

	test("flattens transparent pixels against white before JPEG encoding", async () => {
		const transparentPng = await new Jimp({ width: 1, height: 1, color: 0x00000000 }).getBuffer("image/png");
		const prepared = await preparePromptImages([transparentPng]);
		const decodedJpeg = await Jimp.read(Buffer.from(prepared.images[0].data, "base64"));
		const pixel = decodedJpeg.getPixelColor(0, 0);
		expect((pixel >>> 24) & 0xff).toBeGreaterThan(235);
		expect((pixel >>> 16) & 0xff).toBeGreaterThan(235);
		expect((pixel >>> 8) & 0xff).toBeGreaterThan(235);
	});

	test("allows the maximum image count and rejects one more", async () => {
		const source = await makePng();
		const prepared = await preparePromptImages(Array.from({ length: MAX_PROMPT_IMAGES }, () => source));
		expect(prepared.images).toHaveLength(MAX_PROMPT_IMAGES);
		await expect(preparePromptImages(Array.from({ length: MAX_PROMPT_IMAGES + 1 }, () => source))).rejects.toThrow();
	});

	test("rejects sources over 20 MiB before decode and handles the exact byte boundary", async () => {
		const source = await makeJpeg();
		const atLimit = new Uint8Array(MAX_SOURCE_IMAGE_BYTES);
		atLimit.set(source);
		expect((await preparePromptImages([atLimit])).images).toHaveLength(1);
		const overLimit = new Uint8Array(MAX_SOURCE_IMAGE_BYTES + 1);
		overLimit.set(source);
		await expect(preparePromptImages([overLimit])).rejects.toThrow();
	});

	test("checks source dimensions before decoding and rejects more than 16 million pixels", async () => {
		const oversized = patchPngDimensions(await makePng(), 4001, 4000);
		await expect(preparePromptImages([oversized])).rejects.toThrow();
	});

	test("rejects APNG by its parsed acTL chunk and rejects unsupported formats", async () => {
		const png = await makePng();
		const animationControl = new Uint8Array(8);
		setUInt32BE(animationControl, 0, 1);
		const apng = insertPngChunk(png, "acTL", animationControl);
		await expect(preparePromptImages([apng])).rejects.toThrow();
		await expect(preparePromptImages([new TextEncoder().encode("GIF89a")])).rejects.toThrow();
	});

	test("does not treat text containing acTL as an animation chunk", async () => {
		const staticPng = insertPngChunk(await makePng(), "tEXt", new TextEncoder().encode("note\0acTL"));
		expect((await preparePromptImages([staticPng])).images).toHaveLength(1);
	});
});

describe("prompt image validation", () => {
	test("accepts undefined and empty arrays", () => {
		expect(validatePromptImages(undefined)).toEqual([]);
		expect(validatePromptImages([])).toEqual([]);
	});

	test("accepts 16 million JPEG pixels and rejects a larger metadata size", async () => {
		const jpeg = await makeJpeg();
		expect(validatePromptImages([promptImage(patchJpegDimensions(jpeg, 4000, 4000))])).toHaveLength(1);
		expect(() => validatePromptImages([promptImage(patchJpegDimensions(jpeg, 4001, 4000))])).toThrow();
		expect(() => validatePromptImages([promptImage(patchJpegDimensions(jpeg, 0, 24))])).toThrow();
	});

	test("rejects malformed or noncanonical base64, wrong MIME, and aggregate budget overruns", async () => {
		const jpeg = await makeJpeg();
		expect(() => validatePromptImages([{ data: "", mimeType: "image/jpeg" }])).toThrow();
		expect(() => validatePromptImages([{ data: "%%%", mimeType: "image/jpeg" }])).toThrow();
		expect(() => validatePromptImages([{ data: "AB==", mimeType: "image/jpeg" }])).toThrow();
		expect(() =>
			validatePromptImages([{ data: Buffer.from(jpeg).toString("base64"), mimeType: "image/png" }]),
		).toThrow();

		const exactBudget = new Uint8Array(Math.floor(MAX_IMAGE_TOTAL_BYTES / MAX_PROMPT_IMAGES));
		exactBudget.set(jpeg);
		expect(
			validatePromptImages(Array.from({ length: MAX_PROMPT_IMAGES }, () => promptImage(exactBudget))),
		).toHaveLength(MAX_PROMPT_IMAGES);
		const overBudget = new Uint8Array(exactBudget.length + 1);
		overBudget.set(jpeg);
		expect(() =>
			validatePromptImages(Array.from({ length: MAX_PROMPT_IMAGES }, () => promptImage(overBudget))),
		).toThrow();
	});

	test("rejects non-JPEG data and image counts over the limit", async () => {
		const jpeg = await makeJpeg();
		const png = await makePng();
		expect(() => validatePromptImages([promptImage(png)])).toThrow();
		expect(() =>
			validatePromptImages(Array.from({ length: MAX_PROMPT_IMAGES + 1 }, () => promptImage(jpeg))),
		).toThrow();
	});
});

describe("trusted image downloads", () => {
	test("accepts an exact trusted HTTPS hostname and requests redirect:error", async () => {
		let requestedRedirect: RequestInit["redirect"];
		const bytes = new Uint8Array([1, 2, 3]);
		const downloaded = await downloadPromptImage("https://cdn.example.test/image", {
			allowedHosts: ["cdn.example.test"],
			fetch: async (_input, init) => {
				requestedRedirect = init?.redirect;
				return new Response(bytes);
			},
		});
		expect([...downloaded]).toEqual([1, 2, 3]);
		expect(requestedRedirect).toBe("error");
	});

	test("rejects untrusted, non-HTTPS, credentialed, ported, and fragment URLs before fetching", async () => {
		let fetchCalls = 0;
		const fetch = async () => {
			fetchCalls += 1;
			return new Response(new Uint8Array());
		};
		for (const url of [
			"http://cdn.example.test/image",
			"https://cdn.example.test.evil.test/image",
			"https://user@cdn.example.test/image",
			"https://cdn.example.test:443/image",
			"https://cdn.example.test/image#fragment",
		]) {
			await expect(downloadPromptImage(url, { allowedHosts: ["cdn.example.test"], fetch })).rejects.toThrow();
		}
		expect(fetchCalls).toBe(0);
	});

	test("rejects HTTP errors and redirects", async () => {
		await expect(
			downloadPromptImage("https://cdn.example.test/image", {
				allowedHosts: ["cdn.example.test"],
				fetch: async () => new Response("private upstream response", { status: 503 }),
			}),
		).rejects.toThrow("Image download failed.");
		await expect(
			downloadPromptImage("https://cdn.example.test/image", {
				allowedHosts: ["cdn.example.test"],
				fetch: async () => new Response(null, { status: 302, headers: { location: "https://elsewhere.test" } }),
			}),
		).rejects.toThrow("Image download failed.");
	});

	test("classifies permanent and transient HTTP failures without leaking response data", async () => {
		for (const [status, retryable] of [
			[403, false],
			[404, false],
			[408, true],
			[429, true],
			[503, true],
		] as const) {
			const error = await downloadPromptImage("https://cdn.example.test/private?token=secret", {
				allowedHosts: ["cdn.example.test"],
				fetch: async () => new Response("private upstream response", { status }),
			}).catch((error: unknown) => error);
			expect(error).toBeInstanceOf(PromptImageDownloadError);
			expect((error as PromptImageDownloadError).retryable).toBe(retryable);
			expect((error as Error).message).toBe("Image download failed.");
		}
	});

	test("cancels a streamed response when it exceeds 20 MiB", async () => {
		let cancelled = false;
		const oversized = new Uint8Array(MAX_SOURCE_IMAGE_BYTES + 1);
		const body = new ReadableStream<Uint8Array>({
			start(controller) {
				controller.enqueue(oversized);
			},
			cancel() {
				cancelled = true;
			},
		});
		const error = await downloadPromptImage("https://cdn.example.test/image", {
			allowedHosts: ["cdn.example.test"],
			fetch: async () => new Response(body),
		}).catch((error: unknown) => error);
		expect(error).toBeInstanceOf(PromptImageDownloadError);
		expect((error as PromptImageDownloadError).retryable).toBe(false);
		expect(cancelled).toBe(true);
	});
});
