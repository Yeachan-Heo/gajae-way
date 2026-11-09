import { expect, test } from "bun:test";
import type { ImageFetch } from "@gajae-gateway/images";
import { downloadPromptImage } from "@gajae-gateway/images";
import {
	collectAttachments,
	DISCORD_IMAGE_ALLOWED_HOSTS,
	describeAttachment,
	describeInboundBody,
	isVoiceMessageAttachment,
	prepareDiscordImages,
	selectImageAttachments,
	selectNativeImageAttachments,
} from "../src/attachments";

const VOICE = {
	name: "voice-message.ogg",
	contentType: "audio/ogg",
	size: 12_400,
	url: "https://cdn.discordapp.com/attachments/1/2/voice-message.ogg?ex=abc",
	duration: 3.24,
	waveform: "MjMzNDU=",
};

test("a voice message is recognized by its duration and waveform", () => {
	expect(isVoiceMessageAttachment(VOICE)).toBe(true);
});

test("an ordinary audio upload is not a voice message even with the same mime type", () => {
	expect(isVoiceMessageAttachment({ ...VOICE, duration: null, waveform: null })).toBe(false);
});

test("a waveform without a duration is not enough to claim a voice message", () => {
	expect(isVoiceMessageAttachment({ waveform: "MjM=", contentType: "audio/ogg" })).toBe(false);
});

test("a voice message renders its length, size and url", () => {
	expect(describeAttachment(VOICE)).toBe(
		"[voice message · 3.2s · 12.1 KB · https://cdn.discordapp.com/attachments/1/2/voice-message.ogg?ex=abc]",
	);
});

test("a voice message over a minute renders as m:ss", () => {
	expect(describeAttachment({ ...VOICE, duration: 95.4, size: null, url: null })).toBe("[voice message · 1:35]");
});

test("attachment kind comes from the mime type so the persona need not parse it", () => {
	expect(describeAttachment({ name: "shot.png", contentType: "image/png", size: 500 })).toBe(
		"[image · shot.png · 500 B]",
	);
	expect(describeAttachment({ name: "clip.mp4", contentType: "video/mp4", size: 3_000_000 })).toBe(
		"[video · clip.mp4 · 2.9 MB]",
	);
	expect(describeAttachment({ name: "report.pdf", contentType: "application/pdf" })).toBe("[file · report.pdf]");
	expect(describeAttachment({ name: "notes.txt" })).toBe("[file · notes.txt]");
});

test("image descriptions identify the native attachment without exposing its signed CDN URL", () => {
	const url = "https://cdn.discordapp.com/attachments/1/2/photo.png?ex=secret-signature";
	expect(describeAttachment({ name: "photo.png", contentType: "image/png", url })).toBe("[image · photo.png]");
	expect(describeAttachment({ name: "archive.bin", contentType: "application/octet-stream", url })).toContain(url);
});

test("native image sources are limited to PNG/JPEG, with filename inference only when MIME is missing", () => {
	const message = {
		attachments: [
			{ name: "photo.data", contentType: " IMAGE/JPEG ", url: "https://cdn.discordapp.com/photo" },
			{ name: "inferred.jpeg", url: "https://cdn.discordapp.com/inferred" },
			{ name: "native.png", contentType: "image/png", url: "https://cdn.discordapp.com/native" },
			{ name: "animated.gif", contentType: "image/gif", url: "https://cdn.discordapp.com/gif" },
			{ name: "preview.webp", contentType: "image/webp", url: "https://cdn.discordapp.com/webp" },
			{ name: "looks-like-image.png", contentType: "application/octet-stream", url: "https://cdn.discordapp.com/file" },
			{ name: "clip.mp4", contentType: "video/mp4", url: "https://cdn.discordapp.com/video" },
		],
	};
	expect(selectNativeImageAttachments(message)).toEqual([
		message.attachments[0],
		message.attachments[1],
		message.attachments[2],
	]);
	expect(selectImageAttachments(message)).toEqual(message.attachments.slice(0, 5));
});

test("unsupported images retain their URLs and captions beside ordinary attachments", async () => {
	const message = {
		content: "Please look at these",
		attachments: [
			{
				name: "animation.gif",
				contentType: "image/gif",
				url: "https://cdn.discordapp.com/animation.gif?sig=gif-secret",
			},
			{
				name: "preview.webp",
				contentType: "image/webp",
				url: "https://cdn.discordapp.com/preview.webp?sig=webp-secret",
			},
			{
				name: "notes.txt",
				contentType: "text/plain",
				url: "https://cdn.discordapp.com/notes.txt?sig=file-secret",
			},
		],
	};
	let downloads = 0;
	expect(describeInboundBody(message)).toBe(
		[
			"Please look at these",
			"[image · animation.gif · https://cdn.discordapp.com/animation.gif?sig=gif-secret]",
			"[image · preview.webp · https://cdn.discordapp.com/preview.webp?sig=webp-secret]",
			"[file · notes.txt · https://cdn.discordapp.com/notes.txt?sig=file-secret]",
		].join("\n"),
	);
	expect(
		await prepareDiscordImages(message, {
			download: async () => {
				downloads++;
				return Uint8Array.of(1);
			},
			prepare: async () => ({ images: [], notice: "" }),
		}),
	).toEqual({ images: [], notice: "" });
	expect(downloads).toBe(0);
});

test("Discord image downloads preserve signed URLs and pass only the approved hosts to the helper", async () => {
	const cdnUrl = "https://cdn.discordapp.com/attachments/1/2/photo.png?ex=abc&is=def&hm=signed";
	const mediaUrl = "https://media.discordapp.net/attachments/1/2/preview.png?width=640&signature=signed";
	const downloads: Array<{ url: string; allowedHosts: readonly string[] }> = [];
	let preparedSources: readonly Uint8Array[] = [];
	const result = await prepareDiscordImages(
		{
			attachments: [
				{ name: "photo.png", contentType: "image/png", url: cdnUrl },
				{ name: "preview.png", contentType: "image/png", url: mediaUrl },
			],
		},
		{
			download: async (url, options) => {
				downloads.push({ url, allowedHosts: options.allowedHosts });
				return Uint8Array.of(downloads.length);
			},
			prepare: async (sources) => {
				preparedSources = sources;
				return { images: [{ data: "AQ==", mimeType: "image/jpeg" }], notice: "resized" };
			},
		},
	);
	expect(downloads).toEqual([
		{ url: cdnUrl, allowedHosts: DISCORD_IMAGE_ALLOWED_HOSTS },
		{ url: mediaUrl, allowedHosts: DISCORD_IMAGE_ALLOWED_HOSTS },
	]);
	expect(preparedSources).toEqual([Uint8Array.of(1), Uint8Array.of(2)]);
	expect(result).toEqual({ images: [{ data: "AQ==", mimeType: "image/jpeg" }], notice: "resized" });
});

test("arbitrary image hosts are rejected before reaching the download helper", async () => {
	let fetchCount = 0;
	const fetchImage: ImageFetch = async () => {
		fetchCount++;
		return new Response(new Uint8Array([1]));
	};
	await expect(
		prepareDiscordImages(
			{
				attachments: [
					{
						name: "photo.png",
						contentType: "image/png",
						url: "https://attacker.example/photo.png?secret=query",
					},
				],
			},
			{
				download: downloadPromptImage,
				prepare: async () => ({ images: [], notice: "" }),
				fetch: fetchImage,
			},
		),
	).rejects.toMatchObject({ disposition: "terminal" });
	expect(fetchCount).toBe(0);
});

test("permanent HTTP image failures are terminal while server failures stay retryable", async () => {
	const message = {
		attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo" }],
	};
	const prepare = async () => ({ images: [], notice: "" });
	const unavailable: ImageFetch = async () => new Response("unavailable", { status: 503 });
	const expired: ImageFetch = async () => new Response("expired", { status: 403 });
	await expect(
		prepareDiscordImages(message, {
			download: downloadPromptImage,
			prepare,
			fetch: unavailable,
		}),
	).rejects.toMatchObject({ disposition: "retryable" });
	await expect(
		prepareDiscordImages(message, {
			download: downloadPromptImage,
			prepare,
			fetch: expired,
		}),
	).rejects.toMatchObject({ disposition: "terminal" });
});

test("more than four images are rejected before any download", async () => {
	let downloadCount = 0;
	await expect(
		prepareDiscordImages(
			{
				attachments: Array.from({ length: 5 }, (_, index) => ({
					name: `${index}.png`,
					contentType: "image/png",
					url: `https://cdn.discordapp.com/${index}`,
				})),
			},
			{
				download: async () => {
					downloadCount++;
					return Uint8Array.of(1);
				},
				prepare: async () => ({ images: [], notice: "" }),
			},
		),
	).rejects.toThrow("at most 4 images");
	expect(downloadCount).toBe(0);
});

test("an attachment-only message produces a body instead of the empty string that was dropped", () => {
	expect(describeInboundBody({ content: "", attachments: [VOICE] })).toBe(describeAttachment(VOICE));
});

test("a caption keeps its own line and the attachment follows it", () => {
	expect(
		describeInboundBody({ content: "이거 봐라", attachments: [{ name: "a.png", contentType: "image/png" }] }),
	).toBe("이거 봐라\n[image · a.png]");
});

test("a whitespace-only caption is treated as no caption rather than kept as a blank line", () => {
	expect(describeInboundBody({ content: "   ", attachments: [{ name: "a.png", contentType: "image/png" }] })).toBe(
		"[image · a.png]",
	);
});

test("a plain text message is passed through untouched", () => {
	expect(describeInboundBody({ content: "안녕", attachments: [] })).toBe("안녕");
	expect(describeInboundBody({ content: "안녕" })).toBe("안녕");
});

test("a message with neither text nor attachments stays empty so it is still dropped", () => {
	expect(describeInboundBody({ content: "" })).toBe("");
	expect(describeInboundBody({})).toBe("");
});

test("every attachment gets a line", () => {
	const body = describeInboundBody({
		content: "",
		attachments: [
			{ name: "a.png", contentType: "image/png" },
			{ name: "b.png", contentType: "image/png" },
		],
	});
	expect(body).toBe("[image · a.png]\n[image · b.png]");
});

test("a flood of attachments is capped with a remainder line so one message cannot swamp the turn", () => {
	const attachments = Array.from({ length: 14 }, (_, index) => ({ name: `f${index}.png`, contentType: "image/png" }));
	const lines = describeInboundBody({ content: "", attachments }).split("\n");
	expect(lines).toHaveLength(11);
	expect(lines[10]).toBe("[+4 more attachments]");
});

test("the remainder line stays singular for exactly one extra attachment", () => {
	const attachments = Array.from({ length: 11 }, (_, index) => ({ name: `f${index}.png`, contentType: "image/png" }));
	expect(describeInboundBody({ content: "", attachments }).split("\n")[10]).toBe("[+1 more attachment]");
});

test("collectAttachments tolerates the field being absent, null or already an array", () => {
	expect(collectAttachments({})).toEqual([]);
	expect(collectAttachments({ attachments: null })).toEqual([]);
	expect(collectAttachments({ attachments: [] })).toEqual([]);
	expect(collectAttachments({ attachments: [VOICE] })).toEqual([VOICE]);
});

// discord.js `Collection` extends `Map`, so bare iteration yields `[id, attachment]`
// entry pairs. Reading those as attachments produced tuples with no fields, rendering
// every line as a bare `[file]`, which is why `values()` is preferred.
test("a Map-like collection is read through values() rather than as entry pairs", () => {
	const collection = new Map([["1", VOICE]]);
	expect(collectAttachments({ attachments: collection })).toEqual([VOICE]);
	expect(describeInboundBody({ content: "", attachments: collection })).toBe(describeAttachment(VOICE));
});
