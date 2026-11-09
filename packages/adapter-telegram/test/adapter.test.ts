import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, ChatSendParams } from "@gajae-gateway/protocol";
import {
	TelegramAttachmentRetryableError,
	type TelegramAttachmentSaver,
	TelegramAttachmentStore,
} from "../src/attachments";
import { loadTelegramAdapterConfig, TelegramAdapterStartupError } from "../src/config";
import { telegramHtmlToPlainText } from "../src/format";
import {
	type GatewayClientLike,
	renderTelegramDelivery,
	settleTelegramDelivery,
	subscribeTelegramDeliveries,
	TelegramAdapter,
	TelegramApiError,
	TelegramBotApi,
	TelegramDeliveryProgress,
	type TelegramUpdate,
} from "../src/main";
import { telegramMessageOrigin } from "../src/origin";
import { TelegramAdapterState } from "../src/state";

const topicMessage = {
	message_id: 7,
	chat: { id: -100123, type: "supergroup" },
	from: { id: 42 },
	message_thread_id: 99,
	is_topic_message: true,
	text: "hello @agent",
};

const noImageNotice = {
	sendMessage: async () => {
		throw new Error("non-image input must not emit an image notice");
	},
};

const testImage = Buffer.from(
	"iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEklEQVR4nGP4z8DAAMIM/4EAAB/uBfsL2WiLAAAAAElFTkSuQmCC",
	"base64",
);

test("loads a token exclusively from its configured credential file", async () => {
	const home = await temporaryHome();
	try {
		await writeFile(join(home, "token"), " test-token \n");
		await writeFile(
			join(home, "adapter-telegram.json"),
			JSON.stringify({ tokenFile: "token", chats: { "1": { engagement: "open" } } }),
		);
		const config = await loadTelegramAdapterConfig({ GAJAEWAY_HOME: home });
		expect(config.token).toBe("test-token");
		expect(config.tokenFile).toBe(join(home, "token"));
		await writeFile(join(home, "adapter-telegram.json"), "{}");
		await expect(loadTelegramAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toBeInstanceOf(
			TelegramAdapterStartupError,
		);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("maps private chats, groups, and forum topics to isolated origins", () => {
	expect(telegramMessageOrigin({ chat: { id: 10, type: "private" }, from: { id: 11 } })).toEqual({
		platform: "telegram",
		kind: "dm",
		conversationId: "10",
		peerId: "11",
	});
	expect(telegramMessageOrigin({ chat: { id: -20, type: "group" }, from: { id: 11 } })).toEqual({
		platform: "telegram",
		kind: "channel",
		conversationId: "-20",
	});
	expect(telegramMessageOrigin(topicMessage)).toEqual({
		platform: "telegram",
		kind: "topic",
		conversationId: "-100123.99",
		parentId: "-100123",
	});
});

test("persists a forum topic route across restart and sends replies to its topic", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin(topicMessage);
		await state.rememberOrigin(origin, 99);
		const restarted = await TelegramAdapterState.load(home);
		const sent: Array<{ chatId: string; text: string; thread?: number }> = [];
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(
			mockGateway(requests),
			{ sendMessage: async (chatId, text, thread) => void sent.push({ chatId, text, thread }) },
			restarted,
			delivery(origin, "reply"),
		);
		expect(sent).toEqual([{ chatId: "-100123", text: "reply", thread: 99 }]);
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("deduplicates update ids durably after successfully sending an inbound turn", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const adapter = new TelegramAdapter(state, "agent", "900", { chats: {} }, noAttachments, noImageNotice);
		const requests: Array<{ verb: string; params: unknown }> = [];
		const gateway = mockGateway(requests);
		const update: TelegramUpdate = { update_id: 81, message: topicMessage };
		expect(await adapter.handleUpdate(gateway, update)).toBe(true);
		expect(await adapter.handleUpdate(gateway, update)).toBe(false);
		expect(requests).toHaveLength(1);
		const restarted = new TelegramAdapter(
			await TelegramAdapterState.load(home),
			"agent",
			"900",
			{ chats: {} },
			noAttachments,
			noImageNotice,
		);
		expect(await restarted.handleUpdate(gateway, update)).toBe(false);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.params).toEqual({
			origin: { platform: "telegram", kind: "topic", conversationId: "-100123.99", parentId: "-100123" },
			text: "hello @agent",
			engagement: { mentioned: true, group: true, authorId: "42" },
			messageId: "-100123:7",
		});
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("forwards a captioned photo natively without exposing its local path or token url", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const image = testImage;
		const fetched: string[] = [];
		const notices: Array<{ chatId: string; text: string; thread?: number }> = [];
		const bot = new TelegramBotApi("secret-token", async (url, init) => {
			fetched.push(String(url));
			if (String(url).endsWith("/getFile")) {
				expect(JSON.parse(String(init?.body))).toEqual({ file_id: "big" });
				return Response.json({ ok: true, result: { file_path: "photos/file_3.jpg" } });
			}
			return new Response(image);
		});
		const directory = join(home, "attachments", "telegram");
		const adapter = new TelegramAdapter(
			state,
			"agent",
			"900",
			{ chats: {} },
			new TelegramAttachmentStore(directory, bot),
			{
				sendMessage: async (chatId, text, thread) => {
					notices.push({ chatId, text, thread });
				},
			},
		);
		const requests: Array<{ verb: string; params: unknown }> = [];
		const gateway = mockGateway(requests);
		gateway.request = async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			expect(notices).toHaveLength(0);
			return { engaged: true, turnId: "turn-native-image" } as T;
		};
		await adapter.handleUpdate(gateway, {
			update_id: 5,
			message: {
				message_id: 31,
				chat: { id: -20, type: "group" },
				from: { id: 42 },
				caption: "what is this @agent",
				photo: [
					{ file_id: "small", file_unique_id: "u-small", width: 90, height: 60, file_size: 900 },
					{ file_id: "big", file_unique_id: "u-big", width: 1280, height: 853, file_size: 125_000 },
				],
			},
		});
		const path = join(directory, "-20", "31-u-big.jpg");
		expect(fetched).toEqual([
			"https://api.telegram.org/botsecret-token/getFile",
			"https://api.telegram.org/file/botsecret-token/photos/file_3.jpg",
		]);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.verb).toBe("chat.send");
		const params = requests[0]?.params as ChatSendParams;
		expect(params).toMatchObject({
			origin: { platform: "telegram", kind: "channel", conversationId: "-20" },
			engagement: { mentioned: true, group: true, authorId: "42" },
			messageId: "-20:31",
		});
		expect(params.text).toBe("what is this @agent\n[image · 122.1 KB]");
		expect(params.text).not.toContain("Image processing:");
		expect(params.text).not.toContain(path);
		expect(params.images).toHaveLength(1);
		expect(params.images![0]!.mimeType).toBe("image/jpeg");
		expect(Buffer.from(params.images![0]!.data, "base64").subarray(0, 2)).toEqual(Buffer.from([0xff, 0xd8]));
		expect(notices).toHaveLength(1);
		expect(notices[0]?.chatId).toBe("-20");
		expect(notices[0]?.text).toMatch(/compress/i);
		expect(JSON.stringify(requests)).not.toContain("secret-token");
		expect(await readFile(path)).toEqual(image);
		expect((await stat(path)).mode & 0o777).toBe(0o600);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a declined image-only turn never posts a processing notice", async () => {
	const home = await temporaryHome();
	try {
		const path = join(home, "image.png");
		await writeFile(path, testImage);
		const notices: string[] = [];
		const adapter = new TelegramAdapter(
			await TelegramAdapterState.load(home),
			"agent",
			"900",
			{ chats: {} },
			{ save: async () => ({ path }) },
			{
				sendMessage: async (_chatId, text) => {
					notices.push(text);
				},
			},
		);
		const requests: Array<{ verb: string; params: unknown }> = [];
		const gateway = mockGateway(requests);
		gateway.request = async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return { engaged: false } as T;
		};
		await adapter.handleUpdate(gateway, {
			update_id: 12,
			message: {
				message_id: 40,
				chat: { id: 22, type: "private" },
				from: { id: 44 },
				photo: [{ file_id: "photo", file_unique_id: "photo", width: 2, height: 2 }],
			},
		});
		expect(requests).toHaveLength(1);
		expect((requests[0]?.params as ChatSendParams).images).toHaveLength(1);
		expect(notices).toEqual([]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("failed image download cannot dispatch a text-only turn or unauthorized notice", async () => {
	const home = await temporaryHome();
	try {
		const adapter = new TelegramAdapter(
			await TelegramAdapterState.load(home),
			"agent",
			"900",
			{ chats: {} },
			{
				save: async () => ({ unavailable: "download failed" }),
			},
			noImageNotice,
		);
		const requests: Array<{ verb: string; params: unknown }> = [];
		await adapter.handleUpdate(mockGateway(requests), {
			update_id: 6,
			message: {
				message_id: 32,
				chat: { id: 22, type: "private" },
				from: { id: 44 },
				document: { file_id: "d", file_unique_id: "ud", file_name: "scan.png", mime_type: "image/png" },
			},
		});
		expect(requests).toEqual([]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("retries a transient image download after restart without advancing Telegram's update offset", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		await state.acceptUpdate(40);
		let getFileCalls = 0;
		const bot = new TelegramBotApi("secret-token", async (url) => {
			if (String(url).endsWith("/getFile")) {
				getFileCalls++;
				if (getFileCalls === 1)
					return Response.json({ ok: false, description: "temporary service failure" }, { status: 503 });
				return Response.json({ ok: true, result: { file_path: "photos/retry.png" } });
			}
			return new Response(testImage);
		});
		const update: TelegramUpdate = {
			update_id: 41,
			message: {
				message_id: 52,
				chat: { id: 22, type: "private" },
				from: { id: 44 },
				document: { file_id: "photo", file_unique_id: "photo", file_name: "image.png", mime_type: "image/png" },
			},
		};
		const attachmentDirectory = join(home, "attachments");
		const failedAdapter = new TelegramAdapter(
			state,
			"agent",
			"900",
			{ chats: {} },
			new TelegramAttachmentStore(attachmentDirectory, bot, { error: () => {} }),
			noImageNotice,
		);
		await expect(failedAdapter.handleUpdate(mockGateway([]), update)).rejects.toBeInstanceOf(
			TelegramAttachmentRetryableError,
		);
		expect(state.updateId).toBe(40);

		const restartedState = await TelegramAdapterState.load(home);
		expect(restartedState.updateId).toBe(40);
		expect(restartedState.updateId! + 1).toBe(update.update_id);
		const requests: Array<{ verb: string; params: unknown }> = [];
		const restartedAdapter = new TelegramAdapter(
			restartedState,
			"agent",
			"900",
			{ chats: {} },
			new TelegramAttachmentStore(attachmentDirectory, bot, { error: () => {} }),
			noImageNotice,
		);
		expect(await restartedAdapter.handleUpdate(mockGateway(requests), update)).toBe(true);
		expect(restartedState.updateId).toBe(41);
		expect(getFileCalls).toBe(2);
		expect(requests.map(({ verb }) => verb)).toEqual(["chat.send"]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("does not accept messages or reactions whose gateway processing fails", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const adapter = new TelegramAdapter(state, "agent", "900", { chats: {} }, noAttachments, noImageNotice);
		const gateway: GatewayClientLike = {
			request: async () => {
				throw new Error("gateway unavailable");
			},
			onChatMessage: () => () => {},
		};
		await expect(
			adapter.handleUpdate(gateway, {
				update_id: 61,
				message: { message_id: 1, chat: { id: 22, type: "private" }, from: { id: 44 }, text: "hello" },
			}),
		).rejects.toThrow("gateway unavailable");
		await expect(
			adapter.handleUpdate(gateway, {
				update_id: 62,
				message_reaction: {
					chat: { id: 22, type: "private" },
					message_id: 1,
					user: { id: 44 },
					date: 1,
					old_reaction: [],
					new_reaction: [{ type: "emoji", emoji: "👍" }],
				},
			}),
		).rejects.toThrow("gateway unavailable");
		expect(state.updateId).toBeUndefined();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("suppresses image notices when chat.send returns a null turn id", async () => {
	const home = await temporaryHome();
	try {
		const path = join(home, "image.png");
		await writeFile(path, testImage);
		const notices: string[] = [];
		const adapter = new TelegramAdapter(
			await TelegramAdapterState.load(home),
			"agent",
			"900",
			{ chats: {} },
			{ save: async () => ({ path }) },
			{ sendMessage: async (_chatId, text) => void notices.push(text) },
		);
		const requests: Array<{ verb: string; params: unknown }> = [];
		const gateway = mockGateway(requests);
		gateway.request = async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return { engaged: true, turnId: null } as T;
		};
		await adapter.handleUpdate(gateway, {
			update_id: 63,
			message: {
				message_id: 53,
				chat: { id: 22, type: "private" },
				from: { id: 44 },
				photo: [{ file_id: "photo", file_unique_id: "photo", width: 2, height: 2 }],
			},
		});
		expect(requests.map(({ verb }) => verb)).toEqual(["chat.send"]);
		expect(notices).toEqual([]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("chunks under Telegram's 4096 character limit, keeping room for the duplicate warning on any chunk", () => {
	expect(renderTelegramDelivery("x".repeat(8_193)).map((chunk) => chunk.length)).toEqual([4_063, 4_063, 67]);
	expect(renderTelegramDelivery("x".repeat(8_193), true).map((chunk) => chunk.length)).toEqual([4_096, 4_063, 67]);
});

test("chunks formatted replies without splitting a tag, closing and reopening it across chunks", () => {
	const chunks = renderTelegramDelivery(`**${"y".repeat(5_000)}**`);
	expect(chunks).toHaveLength(2);
	for (const chunk of chunks) {
		expect(chunk.length).toBeLessThanOrEqual(4_096);
		expect(chunk.startsWith("<b>")).toBe(true);
		expect(chunk.endsWith("</b>")).toBe(true);
	}
	expect(chunks.join("").replace(/<\/?b>/g, "")).toBe("y".repeat(5_000));
});

test("sends Markdown replies as Telegram HTML, escaping the duplicate prefix and literal text", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });
		await state.rememberOrigin(origin);
		const calls: Array<Record<string, unknown>> = [];
		const bot = new TelegramBotApi("not-a-real-token", async (_url, init) => {
			calls.push(JSON.parse(String(init?.body)));
			return Response.json({ ok: true, result: { message_id: 1 } });
		});
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(mockGateway(requests), bot, state, {
			...delivery(origin, "## 요약\n**굵게** a < b & `x<y>`\n[문서](https://example.com/?a=1&b=2)"),
			duplicateWarning: true,
		});
		expect(calls).toEqual([
			{
				chat_id: "22",
				text: '[recovered - may be a duplicate] <b>요약</b>\n<b>굵게</b> a &lt; b &amp; <code>x&lt;y&gt;</code>\n<a href="https://example.com/?a=1&amp;b=2">문서</a>',
				parse_mode: "HTML",
			},
		]);
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("resends a chunk as plain text when Telegram cannot parse its entities", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });
		await state.rememberOrigin(origin);
		const calls: Array<Record<string, unknown>> = [];
		const bot = new TelegramBotApi("not-a-real-token", async (_url, init) => {
			const body = JSON.parse(String(init?.body));
			calls.push(body);
			return body.parse_mode
				? Response.json(
						{ ok: false, description: "Bad Request: can't parse entities: unexpected end tag" },
						{ status: 400 },
					)
				: Response.json({ ok: true, result: { message_id: 2 } });
		});
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(mockGateway(requests), bot, state, delivery(origin, "**hi** & <tag>"));
		expect(calls).toEqual([
			{ chat_id: "22", text: "<b>hi</b> &amp; &lt;tag&gt;", parse_mode: "HTML" },
			{ chat_id: "22", text: "hi & <tag>" },
		]);
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("multi-chunk entity fallback preserves order and forum topic without resending accepted chunks", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin(topicMessage);
		await state.rememberOrigin(origin, 99);
		const text = `**${"a".repeat(4089)}${"b".repeat(4084)}& tail**`;
		const chunks = renderTelegramDelivery(text);
		expect(chunks).toHaveLength(3);
		const calls: Array<Record<string, unknown>> = [];
		const bot = new TelegramBotApi("not-a-real-token", async (_url, init) => {
			calls.push(JSON.parse(String(init?.body)));
			return calls.length === 2
				? Response.json({ ok: false, description: "Bad Request: can't parse entities" }, { status: 400 })
				: Response.json({ ok: true, result: { message_id: calls.length } });
		});
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(mockGateway(requests), bot, state, delivery(origin, text));
		expect(calls).toEqual([
			{ chat_id: "-100123", message_thread_id: 99, text: chunks[0], parse_mode: "HTML" },
			{ chat_id: "-100123", message_thread_id: 99, text: chunks[1], parse_mode: "HTML" },
			{ chat_id: "-100123", message_thread_id: 99, text: telegramHtmlToPlainText(chunks[1] ?? "") },
			{ chat_id: "-100123", message_thread_id: 99, text: chunks[2], parse_mode: "HTML" },
		]);
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test.each<{ name: string; responses: Array<"ok" | "reject" | "entity" | "transport">; ambiguous: boolean }>([
	{
		name: "an accepted chunk followed by a Telegram 429 is ambiguous",
		responses: ["ok", "reject"],
		ambiguous: true,
	},
	{
		name: "a Telegram rejection of the first chunk is definitive",
		responses: ["reject"],
		ambiguous: false,
	},
	{
		name: "a second-chunk entity fallback transport failure is ambiguous",
		responses: ["ok", "entity", "transport"],
		ambiguous: true,
	},
	{
		name: "an accepted plain-text fallback followed by a Telegram rejection is ambiguous",
		responses: ["entity", "ok", "reject"],
		ambiguous: true,
	},
])("$name", async ({ responses, ambiguous }) => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin(topicMessage);
		await state.rememberOrigin(origin, 99);
		const text = `${"a".repeat(4096)}${"b".repeat(4096)}c`;
		const chunks = renderTelegramDelivery(text);
		const calls: Array<Record<string, unknown>> = [];
		const bot = new TelegramBotApi("not-a-real-token", async (_url, init) => {
			calls.push(JSON.parse(String(init?.body)));
			switch (responses[calls.length - 1]) {
				case "reject":
					return Response.json({ ok: false, description: "Too Many Requests" }, { status: 429 });
				case "entity":
					return Response.json({ ok: false, description: "Bad Request: can't parse entities" }, { status: 400 });
				case "transport":
					throw new Error("fallback transport failed");
				case "ok":
					return Response.json({ ok: true, result: { message_id: calls.length } });
				default:
					throw new Error("unexpected extra send");
			}
		});
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(mockGateway(requests), bot, state, delivery(origin, text));
		expect(calls).toHaveLength(responses.length);
		let chunkIndex = 0;
		for (const [index, response] of responses.entries()) {
			const fallback = index > 0 && responses[index - 1] === "entity";
			expect(calls[index]).toEqual({
				chat_id: "-100123",
				message_thread_id: 99,
				text: chunks[chunkIndex],
				...(fallback ? {} : { parse_mode: "HTML" }),
			});
			if (response === "ok") chunkIndex++;
		}
		expect(requests).toEqual([
			{
				verb: "delivery.fail",
				params: {
					deliveryId: "delivery-1",
					reason: responses.includes("transport")
						? "Telegram sendMessage request failed: fallback transport failed"
						: "Too Many Requests",
					ambiguous,
				},
			},
		]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fails delivery definitively on any other Telegram rejection without a plain-text resend", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });
		await state.rememberOrigin(origin);
		const sent: Array<{ text: string; parseMode?: string }> = [];
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(
			mockGateway(requests),
			{
				sendMessage: async (_chatId, text, _thread, parseMode) => {
					sent.push({ text, ...(parseMode ? { parseMode } : {}) });
					throw new TelegramApiError(403, "Forbidden: bot was blocked by the user");
				},
			},
			state,
			delivery(origin, "**hi**"),
		);
		expect(sent).toEqual([{ text: "<b>hi</b>", parseMode: "HTML" }]);
		expect(requests).toEqual([
			{
				verb: "delivery.fail",
				params: { deliveryId: "delivery-1", reason: "Forbidden: bot was blocked by the user", ambiguous: false },
			},
		]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("prefixes recovered delivery and settles via injected fake fetch", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });
		await state.rememberOrigin(origin);
		const calls: Array<{ url: string; body: Record<string, unknown> }> = [];
		const bot = new TelegramBotApi("not-a-real-token", async (url, init) => {
			calls.push({ url: String(url), body: JSON.parse(String(init?.body)) });
			return Response.json({ ok: true, result: { message_id: 1 } });
		});
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(mockGateway(requests), bot, state, {
			...delivery(origin, "x".repeat(4_090)),
			duplicateWarning: true,
		});
		expect(calls.map((call) => String(call.body.text).length)).toEqual([4_096, 27]);
		expect(calls[0]?.body.text).toBe(`[recovered - may be a duplicate] ${"x".repeat(4_090)}`.slice(0, 4_096));
		expect(calls.every((call) => call.body.parse_mode === "HTML")).toBe(true);
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("fails delivery ambiguously when transport times out after dispatch", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });
		await state.rememberOrigin(origin);
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(
			mockGateway(requests),
			{ sendMessage: async () => Promise.reject(new Error("timeout after dispatch")) },
			state,
			delivery(origin, "reply"),
		);
		expect(requests).toEqual([
			{
				verb: "delivery.fail",
				params: { deliveryId: "delivery-1", reason: "timeout after dispatch", ambiguous: true },
			},
		]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a JSON Bot API transport error is ambiguous and never leaks the bot token", async () => {
	const home = await temporaryHome();
	try {
		const token = "123456:transport-secret";
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });
		await state.rememberOrigin(origin);
		const bot = new TelegramBotApi(token, async (url) => {
			throw new Error(`fetch failed for ${String(url)} (token ${token})`);
		});
		const requests: Array<{ verb: string; params: unknown }> = [];
		await settleTelegramDelivery(mockGateway(requests), bot, state, delivery(origin, "reply"));
		expect(requests).toEqual([
			{
				verb: "delivery.fail",
				params: {
					deliveryId: "delivery-1",
					reason:
						"Telegram sendMessage request failed: fetch failed for https://api.telegram.org/bot<redacted>/sendMessage (token <redacted>)",
					ambiguous: true,
				},
			},
		]);
		const polled = await bot.getUpdates().then(
			() => undefined,
			(error: unknown) => error,
		);
		expect(polled).toBeInstanceOf(Error);
		expect((polled as Error).message).not.toContain(token);
		expect((polled as Error).message).toStartWith("Telegram getUpdates request failed: ");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

// The delivery subscription is the merge seam: it chooses between the reaction
// path and the text path. Deleting that choice leaves every other test green
// while a reaction gets POSTED as the bare emoji, which is exactly the outcome
// the reported-failure policy exists to prevent.
test("the delivery subscription routes a reaction to setMessageReaction and never to sendMessage", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });
		await state.rememberOrigin(origin);
		const requests: Array<{ verb: string; params: unknown }> = [];
		const sent: string[] = [];
		const reacted: Array<{ chatId: string; messageId: string; emoji: string | undefined }> = [];
		let handler: ((message: ChatMessagePayload) => void) | undefined;
		const gateway: GatewayClientLike = {
			request: async <T>(verb: string, params?: unknown) => {
				requests.push({ verb, params });
				return {} as T;
			},
			onChatMessage: (given) => {
				handler = given;
				return () => {};
			},
		};
		subscribeTelegramDeliveries(
			gateway,
			{
				sendMessage: async (_chatId, text) => void sent.push(text),
				setMessageReaction: async (chatId, messageId, emoji?: string) =>
					void reacted.push({ chatId, messageId, emoji }),
			},
			state,
		);
		handler?.({
			...delivery(origin, "👍"),
			reaction: { targetMessageId: "22:77", emoji: "👍", emojiName: "thumbsup" },
		});
		for (let attempt = 0; attempt < 20 && requests.length === 0; attempt++) await Bun.sleep(5);
		expect(reacted).toEqual([{ chatId: "22", messageId: "77", emoji: "👍" }]);
		expect(sent).toEqual([]);
		expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
		// The same subscription still delivers ordinary text through sendMessage.
		requests.length = 0;
		handler?.(delivery(origin, "안녕하세요"));
		for (let attempt = 0; attempt < 20 && requests.length === 0; attempt++) await Bun.sleep(5);
		expect(sent).toEqual(["안녕하세요"]);
		expect(reacted).toHaveLength(1);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

async function temporaryHome(): Promise<string> {
	return mkdtemp(join(tmpdir(), "gajaeway-telegram-adapter-"));
}

const noAttachments: TelegramAttachmentSaver = {
	save: async () => Promise.reject(new Error("a text message never carries an attachment")),
};

function delivery(origin: ChatMessagePayload["origin"], text: string): ChatMessagePayload {
	return { turnId: "turn-1", origin, role: "assistant", text, final: true, deliveryId: "delivery-1" };
}

function mockGateway(requests: Array<{ verb: string; params: unknown }>): GatewayClientLike {
	return {
		request: async <T>(verb: string, params?: unknown) => {
			requests.push({ verb, params });
			return {} as T;
		},
		onChatMessage: () => () => {},
	};
}

const DUPLICATE_PREFIX = "[recovered - may be a duplicate] ";
const threeChunks = `${"a".repeat(4096)}${"b".repeat(4096)}c`;

async function chunkedSettlement(
	respond: (call: number) => "ok" | "transport" | { retryAfter?: number },
	run: (settle: (message: ChatMessagePayload) => Promise<void>) => Promise<void>,
	sleeps: number[] = [],
): Promise<{
	calls: Array<Record<string, unknown>>;
	requests: Array<{ verb: string; params: unknown }>;
	progress: TelegramDeliveryProgress;
}> {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const origin = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });
		await state.rememberOrigin(origin);
		const calls: Array<Record<string, unknown>> = [];
		const bot = new TelegramBotApi("not-a-real-token", async (_url, init) => {
			calls.push(JSON.parse(String(init?.body)));
			const response = respond(calls.length);
			if (response === "ok") return Response.json({ ok: true, result: { message_id: calls.length } });
			if (response === "transport") throw new Error("socket hang up");
			return Response.json(
				{
					ok: false,
					error_code: 429,
					description: `Too Many Requests: retry after ${response.retryAfter ?? 0}`,
					...(response.retryAfter === undefined ? {} : { parameters: { retry_after: response.retryAfter } }),
				},
				{ status: 429 },
			);
		});
		const requests: Array<{ verb: string; params: unknown }> = [];
		const progress = new TelegramDeliveryProgress();
		await run((message) =>
			settleTelegramDelivery(mockGateway(requests), bot, state, message, {
				progress,
				sleep: async (ms) => void sleeps.push(ms),
			}),
		);
		return { calls, requests, progress };
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}

const sentTexts = (calls: Array<Record<string, unknown>>) => calls.map((call) => call.text);
const origin22 = telegramMessageOrigin({ chat: { id: 22, type: "private" }, from: { id: 44 } });

test("a 429 with a short retry_after waits it out and resends only the refused chunk", async () => {
	const chunks = renderTelegramDelivery(threeChunks);
	expect(chunks).toHaveLength(3);
	const sleeps: number[] = [];
	const { calls, requests } = await chunkedSettlement(
		(call) => (call === 2 ? { retryAfter: 3 } : "ok"),
		(settle) => settle(delivery(origin22, threeChunks)),
		sleeps,
	);
	expect(sleeps).toEqual([3_000]);
	expect(sentTexts(calls)).toEqual([chunks[0], chunks[1], chunks[1], chunks[2]]);
	expect(requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } }]);
});

test("a retry_after beyond the settlement wait budget fails at once without waiting", async () => {
	const chunks = renderTelegramDelivery(threeChunks);
	const sleeps: number[] = [];
	const { calls, requests } = await chunkedSettlement(
		(call) => (call === 2 ? { retryAfter: 31 } : "ok"),
		(settle) => settle(delivery(origin22, threeChunks)),
		sleeps,
	);
	expect(sleeps).toEqual([]);
	expect(sentTexts(calls)).toEqual([chunks[0], chunks[1]]);
	expect(requests).toEqual([
		{
			verb: "delivery.fail",
			params: { deliveryId: "delivery-1", reason: "Too Many Requests: retry after 31", ambiguous: true },
		},
	]);
});

test("retries of a partly delivered reply resume at the refused chunk and turn definitive once they stall", async () => {
	const chunks = renderTelegramDelivery(threeChunks);
	const { calls, requests } = await chunkedSettlement(
		(call) => (call === 1 ? "ok" : {}),
		async (settle) => {
			await settle(delivery(origin22, threeChunks));
			for (let retry = 0; retry < 6; retry++)
				await settle({ ...delivery(origin22, threeChunks), redelivered: true, duplicateWarning: true });
		},
	);
	expect(sentTexts(calls)).toEqual([chunks[0], ...Array.from({ length: 7 }, () => chunks[1])]);
	expect(requests.map((request) => (request.params as { ambiguous?: boolean }).ambiguous)).toEqual([
		true,
		true,
		true,
		true,
		true,
		false,
		false,
	]);
});

test("a transport failure after accepted chunks resumes at the uncertain chunk under the duplicate warning", async () => {
	const chunks = renderTelegramDelivery(threeChunks);
	const { calls, requests } = await chunkedSettlement(
		(call) => (call === 2 ? "transport" : "ok"),
		async (settle) => {
			await settle(delivery(origin22, threeChunks));
			await settle({ ...delivery(origin22, threeChunks), redelivered: true, duplicateWarning: true });
		},
	);
	expect(sentTexts(calls)).toEqual([chunks[0], chunks[1], `${DUPLICATE_PREFIX}${chunks[1]}`, chunks[2]]);
	expect(calls.every((call) => String(call.text).length <= 4096)).toBe(true);
	expect(requests).toEqual([
		{
			verb: "delivery.fail",
			params: {
				deliveryId: "delivery-1",
				reason: "Telegram sendMessage request failed: socket hang up",
				ambiguous: true,
			},
		},
		{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } },
	]);
});

test("a chunk Telegram finally accepts resets the stall count for the next chunk", async () => {
	const chunks = renderTelegramDelivery(threeChunks);
	const { calls, requests } = await chunkedSettlement(
		(call) => (call === 1 || call === 7 || call === 10 ? "ok" : {}),
		async (settle) => {
			await settle(delivery(origin22, threeChunks));
			for (let retry = 0; retry < 7; retry++)
				await settle({ ...delivery(origin22, threeChunks), redelivered: true, duplicateWarning: true });
		},
	);
	// Four stalled retries on chunk 2, then it lands; chunk 3's own refusals start a fresh count.
	expect(sentTexts(calls)).toEqual([
		chunks[0],
		...Array.from({ length: 6 }, () => chunks[1]),
		chunks[2],
		chunks[2],
		chunks[2],
	]);
	expect(requests.map((request) => (request.params as { ambiguous?: boolean }).ambiguous)).toEqual([
		true,
		true,
		true,
		true,
		true,
		true,
		true,
		undefined,
	]);
	expect(requests.at(-1)?.verb).toBe("delivery.confirm");
});

test("a chunk that may have landed keeps the duplicate warning after a later definitive refusal", async () => {
	const chunks = renderTelegramDelivery(threeChunks);
	const { calls } = await chunkedSettlement(
		(call) => (call === 2 ? "transport" : call === 3 ? { retryAfter: 600 } : "ok"),
		async (settle) => {
			await settle(delivery(origin22, threeChunks));
			await settle({ ...delivery(origin22, threeChunks), redelivered: true, duplicateWarning: true });
			await settle({ ...delivery(origin22, threeChunks), redelivered: true, duplicateWarning: true });
		},
	);
	// The refusal only proves the labelled resend did not land; the first send still may have.
	expect(sentTexts(calls)).toEqual([
		chunks[0],
		chunks[1],
		`${DUPLICATE_PREFIX}${chunks[1]}`,
		`${DUPLICATE_PREFIX}${chunks[1]}`,
		chunks[2],
	]);
});

test("a delivery replayed after this adapter already posted it is confirmed again without reposting", async () => {
	const chunks = renderTelegramDelivery(threeChunks);
	const { calls, requests } = await chunkedSettlement(
		() => "ok",
		async (settle) => {
			await settle(delivery(origin22, threeChunks));
			await settle({ ...delivery(origin22, threeChunks), redelivered: true, duplicateWarning: true });
		},
	);
	expect(sentTexts(calls)).toEqual(chunks);
	expect(requests).toEqual([
		{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } },
		{ verb: "delivery.confirm", params: { deliveryId: "delivery-1" } },
	]);
});

test("a fully posted delivery is remembered without its rendered chunks", async () => {
	const { calls, requests, progress } = await chunkedSettlement(
		() => "ok",
		async (settle) => {
			await settle(delivery(origin22, threeChunks));
			await settle({ ...delivery(origin22, threeChunks), redelivered: true, duplicateWarning: true });
		},
	);
	expect(progress.get("delivery-1")?.chunks).toEqual([]);
	expect(calls).toHaveLength(3);
	expect(requests.map((request) => request.verb)).toEqual(["delivery.confirm", "delivery.confirm"]);
});

test("repeated retry_after: 0 answers are bounded by the wait budget instead of spinning", async () => {
	const chunks = renderTelegramDelivery(threeChunks);
	const sleeps: number[] = [];
	const { calls, requests } = await chunkedSettlement(
		(call) => (call === 1 ? "ok" : { retryAfter: 0 }),
		(settle) => settle(delivery(origin22, threeChunks)),
		sleeps,
	);
	expect(sleeps).toEqual(Array.from({ length: 30 }, () => 1_000));
	expect(sentTexts(calls)).toEqual([chunks[0], ...Array.from({ length: 31 }, () => chunks[1])]);
	expect(requests).toEqual([
		{
			verb: "delivery.fail",
			params: { deliveryId: "delivery-1", reason: "Too Many Requests: retry after 0", ambiguous: true },
		},
	]);
});

test("progress eviction drops completed deliveries before a partly delivered one", () => {
	const progress = new TelegramDeliveryProgress();
	progress.remember("partial", { chunks: ["a", "b"], sent: 1, uncertain: false, stalls: 0 });
	for (let index = 0; index < 300; index++)
		progress.remember(`done-${index}`, { chunks: ["a"], sent: 1, uncertain: false, stalls: 0 });
	expect(progress.get("partial")?.sent).toBe(1);
	expect(progress.get("done-0")).toBeUndefined();
	expect(progress.get("done-299")?.sent).toBe(1);
});

test("a redelivery with no remembered progress counts a definitive mid-reply refusal as definitive", async () => {
	const chunks = renderTelegramDelivery(threeChunks, true);
	const { calls, requests } = await chunkedSettlement(
		(call) => (call === 1 ? "ok" : {}),
		(settle) => settle({ ...delivery(origin22, threeChunks), redelivered: true, duplicateWarning: true }),
	);
	expect(sentTexts(calls)).toEqual([chunks[0], chunks[1]]);
	expect(requests).toEqual([
		{
			verb: "delivery.fail",
			params: { deliveryId: "delivery-1", reason: "Too Many Requests: retry after 0", ambiguous: false },
		},
	]);
});

test("a reply that cannot be rendered fails definitively without sending", async () => {
	const { calls, requests } = await chunkedSettlement(
		() => "ok",
		(settle) => settle(delivery(origin22, undefined as unknown as string)),
	);
	expect(calls).toEqual([]);
	expect(requests).toHaveLength(1);
	expect(requests[0]).toMatchObject({ verb: "delivery.fail", params: { deliveryId: "delivery-1", ambiguous: false } });
	expect((requests[0]?.params as { reason: string }).reason).toStartWith("Telegram rendering failed: ");
});
