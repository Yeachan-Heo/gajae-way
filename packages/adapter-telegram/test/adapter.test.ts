import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import { type TelegramAttachmentSaver, TelegramAttachmentStore } from "../src/attachments";
import { loadTelegramAdapterConfig, TelegramAdapterStartupError } from "../src/config";
import {
	chunkTelegramMessage,
	type GatewayClientLike,
	settleTelegramDelivery,
	subscribeTelegramDeliveries,
	TelegramAdapter,
	TelegramBotApi,
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

test("deduplicates update ids durably before sending an inbound turn", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const adapter = new TelegramAdapter(state, "agent", "900", { chats: {} }, noAttachments);
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
		);
		expect(await restarted.handleUpdate(gateway, update)).toBe(false);
		expect(requests).toHaveLength(1);
		expect(requests[0]?.params).toEqual({
			origin: { platform: "telegram", kind: "topic", conversationId: "-100123.99", parentId: "-100123" },
			text: "hello @agent",
			engagement: { mentioned: true, group: true, authorId: "42" },
		});
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("forwards a captioned photo with the downloaded local path and never the token url", async () => {
	const home = await temporaryHome();
	try {
		const state = await TelegramAdapterState.load(home);
		const fetched: string[] = [];
		const bot = new TelegramBotApi("secret-token", async (url, init) => {
			fetched.push(String(url));
			if (String(url).endsWith("/getFile")) {
				expect(JSON.parse(String(init?.body))).toEqual({ file_id: "big" });
				return Response.json({ ok: true, result: { file_path: "photos/file_3.jpg" } });
			}
			return new Response(new Uint8Array([0xff, 0xd8, 0xff]));
		});
		const directory = join(home, "attachments", "telegram");
		const adapter = new TelegramAdapter(
			state,
			"agent",
			"900",
			{ chats: {} },
			new TelegramAttachmentStore(directory, bot),
		);
		const requests: Array<{ verb: string; params: unknown }> = [];
		await adapter.handleUpdate(mockGateway(requests), {
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
		expect(requests).toEqual([
			{
				verb: "chat.send",
				params: {
					origin: { platform: "telegram", kind: "channel", conversationId: "-20" },
					text: `what is this @agent\n[image · 122.1 KB · ${path}]`,
					engagement: { mentioned: true, group: true, authorId: "42" },
				},
			},
		]);
		expect(JSON.stringify(requests)).not.toContain("secret-token");
		expect([...(await readFile(path))]).toEqual([0xff, 0xd8, 0xff]);
		expect((await stat(path)).mode & 0o777).toBe(0o600);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("still forwards an attachment-only message when the download fails", async () => {
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
		expect(requests.map((request) => (request.params as { text: string }).text)).toEqual([
			"[image · scan.png · not downloaded: download failed]",
		]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("chunks at Telegram's 4096 character API limit", () => {
	expect(chunkTelegramMessage("x".repeat(8_193)).map((chunk) => chunk.length)).toEqual([4_096, 4_096, 1]);
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
			reaction: { targetMessageId: "77", emoji: "👍", emojiName: "thumbsup" },
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
