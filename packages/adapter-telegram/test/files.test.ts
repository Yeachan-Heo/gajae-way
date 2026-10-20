import { afterEach, expect, test } from "bun:test";
import { appendFile, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, OriginRef } from "@gajae-gateway/protocol";
import {
	type GatewayClientLike,
	settleTelegramFile,
	subscribeTelegramDeliveries,
	TELEGRAM_PHOTO_MAX_BYTES,
	TelegramBotApi,
	TelegramDeliveryOrder,
	type TelegramUpload,
	type TelegramUploadTarget,
	telegramPhotoEligible,
} from "../src/main";
import { telegramMessageOrigin } from "../src/origin";
import { TelegramAdapterState } from "../src/state";

const TOKEN = "123456:secret-token";
const PNG = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13, 0x49, 0x48, 0x44, 0x52]);
const TOPIC = telegramMessageOrigin({
	chat: { id: -100123, type: "supergroup" },
	from: { id: 42 },
	message_thread_id: 99,
	is_topic_message: true,
});
const DM = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });

let directory = "";
afterEach(async () => {
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

interface Upload {
	readonly method: string;
	readonly contentType: string;
	readonly fields: Record<string, string>;
	readonly file?: { readonly field: string; readonly name: string; readonly bytes: Uint8Array };
}

/** A Bot API double that decodes each multipart request exactly as Telegram would receive it. */
function botFixture(answer: (method: string) => Response | Promise<Response> = () => okMessage()) {
	const uploads: Upload[] = [];
	const urls: string[] = [];
	const bot = new TelegramBotApi(TOKEN, async (input, init) => {
		const url = String(input);
		urls.push(url);
		const request = new Request(url, init);
		const contentType = request.headers.get("content-type") ?? "";
		const form = await request.formData();
		const fields: Record<string, string> = {};
		let file: Upload["file"];
		for (const [field, value] of form.entries()) {
			if (typeof value === "string") fields[field] = value;
			else file = { field, name: value.name, bytes: new Uint8Array(await value.arrayBuffer()) };
		}
		const method = url.split("/").at(-1) ?? "";
		uploads.push({ method, contentType, fields, ...(file ? { file } : {}) });
		return answer(method);
	});
	return { bot, uploads, urls };
}

function okMessage(): Response {
	return Response.json({ ok: true, result: { message_id: 501 } });
}

function badRequest(description: string): Response {
	return Response.json({ ok: false, error_code: 400, description }, { status: 400 });
}

function mockGateway(): Pick<GatewayClientLike, "request"> & { requests: Array<{ verb: string; params: unknown }> } {
	const requests: Array<{ verb: string; params: unknown }> = [];
	return {
		requests,
		request: async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return {} as T;
		},
	};
}

/** A checked file delivery for `name`, with the canonical path the gateway stores in a FileRef. */
async function fileDelivery(
	origin: OriginRef,
	name: string,
	bytes: Uint8Array | string,
	extra: Partial<ChatMessagePayload> = {},
): Promise<ChatMessagePayload> {
	directory ||= await mkdtemp(join(tmpdir(), "gajaeway-telegram-files-"));
	const path = join(directory, name);
	await writeFile(path, bytes);
	return {
		turnId: "turn-1",
		origin,
		role: "assistant",
		text: `📎 ${name}`,
		final: false,
		deliveryId: "file-delivery",
		file: { path: await realpath(path), name, size: typeof bytes === "string" ? bytes.length : bytes.byteLength },
		...extra,
	};
}

async function stateWith(origin: OriginRef, messageThreadId?: number): Promise<TelegramAdapterState> {
	directory ||= await mkdtemp(join(tmpdir(), "gajaeway-telegram-files-"));
	const state = await TelegramAdapterState.load(join(directory, "home"));
	await state.rememberOrigin(origin, messageThreadId);
	// Routing must survive a restart: deliveries settle from the persisted route.
	return TelegramAdapterState.load(join(directory, "home"));
}

test("an image goes out via sendPhoto multipart into the same chat topic, with its name and bytes", async () => {
	const { bot, uploads } = botFixture();
	const gateway = mockGateway();
	const state = await stateWith(TOPIC, 99);
	await settleTelegramFile(gateway, bot, state, await fileDelivery(TOPIC, "chart.png", PNG));
	expect(uploads).toHaveLength(1);
	const [upload] = uploads;
	expect(upload?.method).toBe("sendPhoto");
	expect(upload?.contentType).toStartWith("multipart/form-data; boundary=");
	expect(upload?.fields).toEqual({ chat_id: "-100123", message_thread_id: "99" });
	expect(upload?.file?.field).toBe("photo");
	expect(upload?.file?.name).toBe("chart.png");
	expect(upload?.file?.bytes).toEqual(PNG);
	expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "file-delivery" } }]);
});

test("a non-image goes out via sendDocument and keeps its file name", async () => {
	const { bot, uploads } = botFixture();
	const gateway = mockGateway();
	const state = await stateWith(DM);
	await settleTelegramFile(gateway, bot, state, await fileDelivery(DM, "report.csv", "a,b\n1,2\n"));
	expect(uploads.map((upload) => upload.method)).toEqual(["sendDocument"]);
	expect(uploads[0]?.fields).toEqual({ chat_id: "22" });
	expect(uploads[0]?.file?.field).toBe("document");
	expect(uploads[0]?.file?.name).toBe("report.csv");
	expect(new TextDecoder().decode(uploads[0]?.file?.bytes)).toBe("a,b\n1,2\n");
	expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "file-delivery" } }]);
});

test("a photo Telegram refuses as a photo is re-sent as a document in the same topic", async () => {
	const { bot, uploads } = botFixture((method) =>
		method === "sendPhoto" ? badRequest("Bad Request: PHOTO_INVALID_DIMENSIONS") : okMessage(),
	);
	const gateway = mockGateway();
	const state = await stateWith(TOPIC, 99);
	await settleTelegramFile(gateway, bot, state, await fileDelivery(TOPIC, "panorama.png", PNG));
	expect(uploads.map((upload) => upload.method)).toEqual(["sendPhoto", "sendDocument"]);
	expect(uploads[1]?.fields).toEqual({ chat_id: "-100123", message_thread_id: "99" });
	expect(uploads[1]?.file).toEqual({ field: "document", name: "panorama.png", bytes: PNG });
	expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "file-delivery" } }]);
});

test("a 400 about the chat itself is a definitive failure with no document retry", async () => {
	const { bot, uploads } = botFixture(() => badRequest("Bad Request: message thread not found"));
	const gateway = mockGateway();
	const state = await stateWith(TOPIC, 99);
	await settleTelegramFile(gateway, bot, state, await fileDelivery(TOPIC, "chart.png", PNG));
	expect(uploads.map((upload) => upload.method)).toEqual(["sendPhoto"]);
	expect(gateway.requests).toEqual([
		{
			verb: "delivery.fail",
			params: { deliveryId: "file-delivery", reason: "Bad Request: message thread not found", ambiguous: false },
		},
	]);
});

test("a file whose size changed after the gateway check is refused before any upload", async () => {
	const { bot, uploads } = botFixture();
	const gateway = mockGateway();
	const state = await stateWith(DM);
	const message = await fileDelivery(DM, "report.csv", "a,b\n1,2\n");
	if (!message.file) throw new Error("fixture has no file");
	await appendFile(message.file.path, "3,4\n");
	await settleTelegramFile(gateway, bot, state, message);
	expect(uploads).toHaveLength(0);
	expect(gateway.requests).toEqual([
		{
			verb: "delivery.fail",
			params: {
				deliveryId: "file-delivery",
				reason: "report.csv changed after the gateway checked it",
				ambiguous: false,
			},
		},
	]);
});

test("a file deleted or swapped for a symlink after the gateway check is refused before any upload", async () => {
	const { bot, uploads } = botFixture();
	const state = await stateWith(DM);
	const deleted = await fileDelivery(DM, "gone.csv", "a,b\n");
	if (!deleted.file) throw new Error("fixture has no file");
	await rm(deleted.file.path);
	const gateway = mockGateway();
	await settleTelegramFile(gateway, bot, state, deleted);
	expect(gateway.requests).toEqual([
		{ verb: "delivery.fail", params: expect.objectContaining({ deliveryId: "file-delivery", ambiguous: false }) },
	]);

	const swapped = await fileDelivery(DM, "swapped.csv", "a,b\n");
	if (!swapped.file) throw new Error("fixture has no file");
	const secret = join(directory, "secret.txt");
	await writeFile(secret, "outside");
	await rm(swapped.file.path);
	await symlink(secret, swapped.file.path);
	const second = mockGateway();
	await settleTelegramFile(second, bot, state, swapped);
	expect(second.requests).toEqual([
		{ verb: "delivery.fail", params: expect.objectContaining({ deliveryId: "file-delivery", ambiguous: false }) },
	]);
	expect(uploads).toHaveLength(0);
});

test("a transport failure is ambiguous, never retried as a document, and never leaks the bot token", async () => {
	const urls: string[] = [];
	const bot = new TelegramBotApi(TOKEN, async (input) => {
		urls.push(String(input));
		throw new Error(`connect ECONNRESET ${String(input)}`);
	});
	const gateway = mockGateway();
	const state = await stateWith(TOPIC, 99);
	await settleTelegramFile(gateway, bot, state, await fileDelivery(TOPIC, "chart.png", PNG));
	expect(urls).toHaveLength(1);
	expect(gateway.requests).toHaveLength(1);
	const failure = gateway.requests[0]?.params as { reason: string; ambiguous: boolean };
	expect(gateway.requests[0]?.verb).toBe("delivery.fail");
	expect(failure.ambiguous).toBe(true);
	expect(failure.reason).toStartWith("Telegram sendPhoto request failed: connect ECONNRESET");
	expect(failure.reason).not.toContain(TOKEN);
	expect(failure.reason).toContain("<redacted>");
});

test("a recovered file upload carries the duplicate warning as its caption", async () => {
	const { bot, uploads } = botFixture();
	const gateway = mockGateway();
	const state = await stateWith(DM);
	await settleTelegramFile(
		gateway,
		bot,
		state,
		await fileDelivery(DM, "report.csv", "a,b\n", { duplicateWarning: true }),
	);
	expect(uploads[0]?.fields).toEqual({ chat_id: "22", caption: "[recovered - may be a duplicate] 📎 report.csv" });
});

test("an origin without a persisted route fails definitively without uploading", async () => {
	const { bot, uploads } = botFixture();
	const gateway = mockGateway();
	directory = await mkdtemp(join(tmpdir(), "gajaeway-telegram-files-"));
	const state = await TelegramAdapterState.load(join(directory, "home"));
	await settleTelegramFile(gateway, bot, state, await fileDelivery(DM, "report.csv", "a,b\n"));
	expect(uploads).toHaveLength(0);
	expect(gateway.requests).toEqual([
		{
			verb: "delivery.fail",
			params: { deliveryId: "file-delivery", reason: "No persisted Telegram reply route for 22", ambiguous: false },
		},
	]);
});

test("the delivery subscription uploads a file delivery and never posts its fallback text", async () => {
	const { bot, uploads } = botFixture();
	const state = await stateWith(DM);
	const requests: Array<{ verb: string; params: unknown }> = [];
	const sent: string[] = [];
	let handler: ((message: ChatMessagePayload) => void) | undefined;
	subscribeTelegramDeliveries(
		{
			request: async <T>(verb: string, params?: unknown) => {
				requests.push({ verb, params });
				return {} as T;
			},
			onChatMessage: (given) => {
				handler = given;
				return () => {};
			},
		},
		{
			sendMessage: async (_chatId, text) => void sent.push(text),
			setMessageReaction: async () => {},
			sendPhoto: bot.sendPhoto.bind(bot),
			sendDocument: bot.sendDocument.bind(bot),
		},
		state,
	);
	handler?.(await fileDelivery(DM, "report.csv", "a,b\n"));
	for (let attempt = 0; attempt < 40 && requests.length === 0; attempt++) await Bun.sleep(5);
	expect(uploads.map((upload) => upload.method)).toEqual(["sendDocument"]);
	expect(sent).toEqual([]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "file-delivery" } }]);
});

/**
 * A subscription whose uploads finish when `uploadDelay` says, recording when each
 * Bot API call starts and ends so tests can see settlement order, not just call order.
 */
function orderedSubscription(
	state: TelegramAdapterState,
	uploadDelay: (upload: TelegramUpload, target: TelegramUploadTarget) => Promise<void>,
	request: (verb: string, params: unknown) => Promise<void> = async () => {},
) {
	const events: string[] = [];
	const requests: Array<{ verb: string; params: unknown }> = [];
	const errors: string[] = [];
	const order = new TelegramDeliveryOrder({ error: (message: string) => void errors.push(message) });
	let handler: ((message: ChatMessagePayload) => void) | undefined;
	const upload = async (target: TelegramUploadTarget, file: TelegramUpload): Promise<{ message_id?: number }> => {
		events.push(`start ${file.name}`);
		await uploadDelay(file, target);
		events.push(`end ${file.name}`);
		return { message_id: 501 };
	};
	subscribeTelegramDeliveries(
		{
			request: async <T>(verb: string, params?: unknown) => {
				requests.push({ verb, params });
				await request(verb, params);
				return {} as T;
			},
			onChatMessage: (given) => {
				handler = given;
				return () => {};
			},
		},
		{
			sendMessage: async (_chatId, text) => {
				events.push(`start text ${text}`);
				await Bun.sleep(20);
				events.push(`end text ${text}`);
			},
			setMessageReaction: async () => {},
			sendPhoto: upload,
			sendDocument: upload,
		},
		state,
		order,
	);
	const deliver = (message: ChatMessagePayload): void => handler?.(message);
	return { events, requests, errors, order, deliver };
}

async function stateWithBoth(): Promise<TelegramAdapterState> {
	const state = await stateWith(DM);
	await state.rememberOrigin(TOPIC, 99);
	return state;
}

test("one conversation's text and files settle in gateway order even when a later upload is faster", async () => {
	const state = await stateWith(DM);
	// A is the slow, large upload; B would finish first if both ran at once.
	const { events, requests, order, deliver } = orderedSubscription(state, (file) =>
		Bun.sleep(file.name === "a.png" ? 60 : 1),
	);
	deliver({
		turnId: "turn-1",
		origin: DM,
		role: "assistant",
		text: "here are the charts",
		final: true,
		deliveryId: "text",
	});
	deliver(await fileDelivery(DM, "a.png", PNG, { deliveryId: "a" }));
	deliver(await fileDelivery(DM, "b.png", PNG, { deliveryId: "b" }));
	await order.drain();
	expect(events).toEqual([
		"start text here are the charts",
		"end text here are the charts",
		"start a.png",
		"end a.png",
		"start b.png",
		"end b.png",
	]);
	expect(requests).toEqual([
		{ verb: "delivery.confirm", params: { deliveryId: "text" } },
		{ verb: "delivery.confirm", params: { deliveryId: "a" } },
		{ verb: "delivery.confirm", params: { deliveryId: "b" } },
	]);
});

test("a stalled upload in one chat does not hold back another chat's deliveries", async () => {
	const state = await stateWithBoth();
	let release!: () => void;
	const stalled = new Promise<void>((resolve) => {
		release = resolve;
	});
	const { events, requests, order, deliver } = orderedSubscription(state, (_file, target) =>
		target.chatId === "22" ? stalled : Promise.resolve(),
	);
	deliver(await fileDelivery(DM, "dm.png", PNG, { deliveryId: "dm" }));
	deliver(await fileDelivery(TOPIC, "topic.png", PNG, { deliveryId: "topic" }));
	for (let attempt = 0; attempt < 40 && requests.length === 0; attempt++) await Bun.sleep(5);
	expect(events).toEqual(["start dm.png", "start topic.png", "end topic.png"]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "topic" } }]);
	release();
	await order.drain();
	expect(events.at(-1)).toBe("end dm.png");
	expect(requests.at(-1)).toEqual({ verb: "delivery.confirm", params: { deliveryId: "dm" } });
});

test("a failed settlement does not block the conversation's next delivery", async () => {
	const state = await stateWith(DM);
	const { events, requests, errors, order, deliver } = orderedSubscription(
		state,
		async (file) => {
			if (file.name === "a.png") throw new Error("socket hang up");
		},
		// Even the delivery.fail report for A is lost; B must still go out.
		async (verb, params) => {
			if (verb === "delivery.fail" && (params as { deliveryId: string }).deliveryId === "a")
				throw new Error("gateway is not connected");
		},
	);
	deliver(await fileDelivery(DM, "a.png", PNG, { deliveryId: "a" }));
	deliver(await fileDelivery(DM, "b.png", PNG, { deliveryId: "b" }));
	await order.drain();
	expect(events).toEqual(["start a.png", "start b.png", "end b.png"]);
	expect(requests).toEqual([
		{ verb: "delivery.fail", params: { deliveryId: "a", reason: "socket hang up", ambiguous: true } },
		{ verb: "delivery.confirm", params: { deliveryId: "b" } },
	]);
	expect(errors).toEqual(["Telegram delivery settlement request failed: gateway is not connected"]);
});

test("a Bot API request that never answers times out instead of holding back the chat's next delivery", async () => {
	const state = await stateWith(DM);
	const calls: Array<{ method: string; signalled: boolean }> = [];
	// The real Bot API client over a transport whose sendMessage never answers
	// unless its request is aborted.
	const bot = new TelegramBotApi(
		TOKEN,
		(input, init) => {
			const method = String(input).split("/").at(-1) ?? "";
			calls.push({ method, signalled: init?.signal instanceof AbortSignal });
			if (method !== "sendMessage") return Promise.resolve(okMessage());
			return new Promise<Response>((_resolve, reject) => {
				init?.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true });
			});
		},
		{ requestMs: 20, uploadMs: 1_000 },
	);
	const requests: Array<{ verb: string; params: unknown }> = [];
	const order = new TelegramDeliveryOrder();
	let handler: ((message: ChatMessagePayload) => void) | undefined;
	subscribeTelegramDeliveries(
		{
			request: async <T>(verb: string, params?: unknown) => {
				requests.push({ verb, params });
				return {} as T;
			},
			onChatMessage: (given) => {
				handler = given;
				return () => {};
			},
		},
		bot,
		state,
		order,
	);
	handler?.({ turnId: "turn-1", origin: DM, role: "assistant", text: "stuck", final: true, deliveryId: "text" });
	handler?.(await fileDelivery(DM, "b.png", PNG, { deliveryId: "b" }));
	// Without a request timeout the stalled sendMessage keeps the chain open forever.
	const drained = await Promise.race([order.drain().then(() => true), Bun.sleep(1_000).then(() => false)]);
	expect(drained).toBe(true);
	expect(calls).toEqual([
		{ method: "sendMessage", signalled: true },
		{ method: "sendPhoto", signalled: true },
	]);
	expect(requests).toEqual([
		{ verb: "delivery.fail", params: expect.objectContaining({ deliveryId: "text", ambiguous: true }) },
		{ verb: "delivery.confirm", params: { deliveryId: "b" } },
	]);
});

test("photo eligibility follows the content, not the name, and Telegram's photo size limit", () => {
	const jpeg = new Uint8Array([0xff, 0xd8, 0xff, 0xe0, 0, 16]);
	const webp = new Uint8Array([0x52, 0x49, 0x46, 0x46, 4, 0, 0, 0, 0x57, 0x45, 0x42, 0x50]);
	const gif = new TextEncoder().encode("GIF89a....");
	expect(telegramPhotoEligible(jpeg)).toBe(true);
	expect(telegramPhotoEligible(PNG)).toBe(true);
	expect(telegramPhotoEligible(webp)).toBe(true);
	// A GIF as a document stays animated; sendPhoto would flatten it to one frame.
	expect(telegramPhotoEligible(gif)).toBe(false);
	expect(telegramPhotoEligible(new TextEncoder().encode("not really a png"))).toBe(false);
	const oversized = new Uint8Array(TELEGRAM_PHOTO_MAX_BYTES + 1);
	oversized.set(PNG);
	expect(telegramPhotoEligible(oversized)).toBe(false);
	const atLimit = new Uint8Array(TELEGRAM_PHOTO_MAX_BYTES);
	atLimit.set(PNG);
	expect(telegramPhotoEligible(atLimit)).toBe(true);
});
