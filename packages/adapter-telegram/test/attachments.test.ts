import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	describeInboundBody,
	pruneTelegramAttachments,
	TELEGRAM_BOT_DOWNLOAD_LIMIT,
	type TelegramAttachment,
	TelegramAttachmentStore,
	type TelegramFileApi,
	telegramAttachment,
} from "../src/attachments";
import { TelegramBotApi } from "../src/main";

const photo: TelegramAttachment = { kind: "image", fileId: "f", fileUniqueId: "u" };

test("normalizes each Telegram media field to one attachment kind", () => {
	expect(
		telegramAttachment({
			photo: [
				{ file_id: "large", file_unique_id: "ul", width: 800, height: 800 },
				{ file_id: "small", file_unique_id: "us", width: 90, height: 90 },
			],
		}),
	).toEqual({ kind: "image", fileId: "large", fileUniqueId: "ul" });
	// Telegram fills `document` alongside `animation`; the animation wins.
	expect(
		telegramAttachment({
			animation: { file_id: "a", file_unique_id: "ua", mime_type: "video/mp4" },
			document: { file_id: "a", file_unique_id: "ua", mime_type: "video/mp4" },
		})?.kind,
	).toBe("video");
	expect(telegramAttachment({ voice: { file_id: "v", file_unique_id: "uv", duration: 4 } })).toEqual({
		kind: "voice message",
		fileId: "v",
		fileUniqueId: "uv",
		duration: 4,
	});
	expect(telegramAttachment({ document: { file_id: "d", file_unique_id: "ud", mime_type: "IMAGE/PNG" } })?.kind).toBe(
		"image",
	);
	expect(telegramAttachment({ document: { file_id: "d", file_unique_id: "ud", file_name: "a.pdf" } })?.kind).toBe(
		"file",
	);
	expect(telegramAttachment({})).toBeUndefined();
});

test("renders the caption on its own line and the attachment after it", () => {
	const voice: TelegramAttachment = {
		kind: "voice message",
		fileId: "v",
		fileUniqueId: "uv",
		duration: 75,
		size: 2048,
	};
	expect(describeInboundBody("", voice, { path: "/h/v.oga" })).toBe("[voice message · 1:15 · 2 KB · /h/v.oga]");
	expect(describeInboundBody("look", photo, { path: "/h/p.jpg" })).toBe("look\n[image · /h/p.jpg]");
	expect(describeInboundBody("plain text", undefined, undefined)).toBe("plain text");
});

test("stores the download owner-only under the chat directory with a sanitized name", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-telegram-attachments-"));
	try {
		const store = new TelegramAttachmentStore(home, fakeApi({ file_path: "documents/../x.PNG" }));
		const stored = await store.save({ ...photo, fileUniqueId: "../evil" }, "-100/1", 9);
		expect(stored).toEqual({ path: join(home, "-100_1", "9-___evil.png") });
		expect(await readdir(home)).toEqual(["-100_1"]);
		expect((await stat(join(home, "-100_1"))).mode & 0o777).toBe(0o700);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("refuses files over the bot download limit without calling Telegram", async () => {
	let called = false;
	const store = new TelegramAttachmentStore("/nonexistent", {
		getFile: async () => {
			called = true;
			return {};
		},
		downloadFile: async () => new Response(""),
	});
	expect(await store.save({ ...photo, size: TELEGRAM_BOT_DOWNLOAD_LIMIT + 1 }, "1", 1)).toEqual({
		unavailable: "exceeds the 20 MB bot download limit",
	});
	expect(called).toBe(false);
});

test("refuses a Telegram file whose reported size exceeds the bot download limit", async () => {
	let downloaded = false;
	const store = new TelegramAttachmentStore("/nonexistent", {
		getFile: async () => ({ file_path: "photos/p.jpg", file_size: TELEGRAM_BOT_DOWNLOAD_LIMIT + 1 }),
		downloadFile: async () => {
			downloaded = true;
			return new Response("");
		},
	});
	expect(await store.save(photo, "1", 1)).toEqual({
		unavailable: "exceeds the 20 MB bot download limit",
	});
	expect(downloaded).toBe(false);
});

test("reports a failed download as unavailable and logs no url", async () => {
	const logged: string[] = [];
	const store = new TelegramAttachmentStore(
		"/nonexistent",
		{
			getFile: async () => ({ file_path: "photos/p.jpg" }),
			downloadFile: async () => new Response("nope", { status: 404 }),
		},
		{ error: (line: string) => void logged.push(line) },
	);
	expect(await store.save(photo, "1", 1)).toEqual({ unavailable: "download failed" });
	expect(logged).toEqual(["Telegram attachment download failed: file download returned HTTP 404"]);
});

test("does not log a token-bearing URL from a rejected download", async () => {
	const logged: string[] = [];
	const store = new TelegramAttachmentStore(
		"/nonexistent",
		{
			getFile: async () => ({ file_path: "photos/p.jpg" }),
			downloadFile: async () => {
				throw new Error("fetch failed: https://api.telegram.org/file/botsecret-token/photos/p.jpg");
			},
		},
		{ error: (line: string) => void logged.push(line) },
	);
	expect(await store.save(photo, "1", 1)).toEqual({ unavailable: "download failed" });
	expect(logged).toEqual(["Telegram attachment download failed: request failed"]);
	expect(logged.join("\n")).not.toContain("secret-token");
});

test("stops reading a download as soon as it exceeds the bot limit", async () => {
	let canceled = false;
	const store = new TelegramAttachmentStore(
		"/nonexistent",
		{
			getFile: async () => ({ file_path: "photos/p.jpg" }),
			downloadFile: async () =>
				new Response(
					new ReadableStream<Uint8Array>({
						start(controller) {
							controller.enqueue(new Uint8Array(TELEGRAM_BOT_DOWNLOAD_LIMIT + 1));
						},
						cancel() {
							canceled = true;
						},
					}),
				),
		},
		{ error: () => {} },
	);
	expect(await store.save(photo, "1", 1)).toEqual({ unavailable: "download failed" });
	expect(canceled).toBe(true);
});

test("aborts a download that outlives the timeout", async () => {
	const store = new TelegramAttachmentStore(
		"/nonexistent",
		{
			getFile: async () => ({ file_path: "photos/p.jpg" }),
			downloadFile: (_path, signal) =>
				new Promise((_resolve, reject) => signal.addEventListener("abort", () => reject(new Error("aborted")))),
		},
		{ error: () => {} },
		10,
	);
	expect(await store.save(photo, "1", 1)).toEqual({ unavailable: "download failed" });
});

test("aborts a getFile request that outlives the timeout", async () => {
	const bot = new TelegramBotApi(
		"secret-token",
		(_url, init) =>
			new Promise((_resolve, reject) => {
				if (!init?.signal) return reject(new Error("getFile signal was not forwarded"));
				init.signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
			}),
	);
	const store = new TelegramAttachmentStore("/nonexistent", bot, { error: () => {} }, 10);
	expect(await store.save(photo, "1", 1)).toEqual({ unavailable: "download failed" });
});

test("prunes attachments past retention and drops emptied chat directories", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-telegram-attachments-"));
	try {
		const now = Date.now();
		await mkdir(join(home, "a"));
		await mkdir(join(home, "b"));
		await writeFile(join(home, "a", "old.jpg"), "x");
		await writeFile(join(home, "b", "old.jpg"), "x");
		await writeFile(join(home, "b", "new.jpg"), "x");
		const old = new Date(now - 8 * 24 * 60 * 60 * 1000);
		await utimes(join(home, "a", "old.jpg"), old, old);
		await utimes(join(home, "b", "old.jpg"), old, old);
		expect(await pruneTelegramAttachments(home, now)).toBe(2);
		expect((await readdir(home)).sort()).toEqual(["b"]);
		expect(await readdir(join(home, "b"))).toEqual(["new.jpg"]);
		expect(await pruneTelegramAttachments(join(home, "missing"), now)).toBe(0);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

function fakeApi(file: { file_path?: string }): TelegramFileApi {
	return {
		getFile: async () => file,
		downloadFile: async () => new Response(new Uint8Array([1, 2, 3])),
	};
}
