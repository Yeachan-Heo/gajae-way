import { expect, spyOn, test } from "bun:test";
import { Buffer } from "node:buffer";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { Client } from "discord.js";
import { engagementForMessage, ReconnectingGateway, startDiscordAdapter } from "../src/main";
import { saveRecoveryCursors, snowflakeFromTimestamp } from "../src/recovery";

// The same valid PNG fixture used by the other adapter image tests.
/** discord.js accepts a bare id or `{ message, force }`; the fake must accept both. */
const singleFetchId = (query: unknown): string | undefined =>
	typeof query === "string" ? query : (query as { message?: string } | undefined)?.message;

const png = Uint8Array.from(
	Buffer.from(
		"iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEklEQVR4nGP4z8DAAMIM/4EAAB/uBfsL2WiLAAAAAElFTkSuQmCC",
		"base64",
	),
);

const bot = { id: "bot" };
const image = { data: "AQID", mimeType: "image/jpeg" as const };

function dmImageMessage(id: string) {
	return {
		id,
		content: "describe this",
		author: { id: "owner", username: "owner", bot: false },
		channel: { id: "new-dm", isDMBased: () => true },
		attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }],
	};
}

function presencePorts() {
	const typing: string[] = [];
	const armed: Array<{ conversationId: string; messageId: string; channelId: string }> = [];
	return {
		typing,
		armed,
		typingPort: {
			begin: (conversationId: string) => typing.push(conversationId),
			refresh: () => {},
			end: () => {},
		},
		statusPort: {
			arm: (conversationId: string, messageId: string, _engagement: unknown, channelId = conversationId) =>
				armed.push({ conversationId, messageId, channelId }),
			clear: () => {},
		},
	};
}

test("a transient image download failure retries a live new DM without reconnecting or sending text alone", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-live-image-retry-"));
	const oldHome = process.env.GAJAEWAY_HOME;
	process.env.GAJAEWAY_HOME = home;
	const sends: Array<{ verb: string; params: any }> = [];
	const transport = {
		onChatMessage: () => () => {},
		onChatProgress: () => () => {},
		request: async (verb: string, params: unknown) => {
			if (verb === "gateway.status") return { database: { identity: "test-db" } };
			sends.push({ verb, params });
			return { engaged: false, turnId: null };
		},
	};
	const connect = spyOn(GajaewayClient, "connectSocketWithBackoff").mockResolvedValue(transport as never);
	const reconnect = spyOn(GajaewayClient, "connectSocket").mockResolvedValue(transport as never);
	let discord: Client | undefined;
	const message = {
		id: snowflakeFromTimestamp(Date.now() - 60_000),
		content: "What is in this picture?",
		author: { id: "owner", username: "owner", bot: false },
		channel: { id: "new-dm", isDMBased: () => true },
		attachments: [
			{
				name: "photo.png",
				contentType: "image/png",
				url: "https://cdn.discordapp.com/attachments/1/2/photo.png?signature=secret",
			},
		],
	};
	const login = spyOn(Client.prototype, "login").mockImplementation(async function (this: Client) {
		discord = this;
		Object.defineProperty(this, "user", { value: { id: "bot" }, configurable: true });
		// Recovery may inspect a newly remembered DM; it must never call Discord REST.
		this.channels.fetch = (async () => ({
			messages: { fetch: async (query: unknown) => (singleFetchId(query) !== undefined ? message : [message]) },
		})) as never;
		return "offline-test-token";
	});
	let downloads = 0;
	const fetchSpy = spyOn(globalThis, "fetch").mockImplementation((async (input: string | URL | Request) => {
		expect(String(input)).toBe("https://cdn.discordapp.com/attachments/1/2/photo.png?signature=secret");
		downloads++;
		return downloads === 1
			? new Response("temporarily unavailable", { status: 503 })
			: new Response(png, { headers: { "content-type": "image/png" } });
	}) as typeof fetch);
	// The adapter owns an unexposed monitor timer. Track and cancel its timers at
	// teardown, while leaving real retry delays and asynchronous file I/O intact.
	const realSetTimeout = globalThis.setTimeout;
	const timers: ReturnType<typeof setTimeout>[] = [];
	const timerSpy = spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
		const timer = realSetTimeout(...args);
		timer.unref?.();
		timers.push(timer);
		return timer;
	}) as typeof setTimeout);
	try {
		await startDiscordAdapter({
			tokenFile: join(home, "unused-token"),
			token: "offline-test-token",
			configPath: join(home, "adapter-discord.json"),
			gatewaySocket: join(home, "unused.sock"),
			statusReactions: "off",
		});
		expect(discord).toBeDefined();
		discord!.emit("messageCreate", message as never);

		// This bounds the expected retry without prescribing an implementation's
		// timer or relying on a second Discord event or a gateway reconnect.
		const deadline = Date.now() + 10_000;
		while (!sends.some((entry) => entry.verb === "chat.send") && Date.now() < deadline) await delay(25);
		const chatSends = sends.filter((entry) => entry.verb === "chat.send");
		expect(downloads).toBe(2);
		expect(chatSends).toHaveLength(1);
		const imageData = chatSends[0].params.images[0].data;
		expect(typeof imageData).toBe("string");
		expect(chatSends[0].params).toMatchObject({
			messageId: message.id,
			origin: { platform: "discord", kind: "dm", conversationId: "new-dm", peerId: "owner" },
			text: "What is in this picture?\n[image · photo.png]",
			images: [{ mimeType: "image/jpeg", data: expect.any(String) }],
		});
		expect(Buffer.from(imageData, "base64").subarray(0, 2)).toEqual(Buffer.from([0xff, 0xd8]));
		expect(connect).toHaveBeenCalledTimes(1);
		expect(reconnect.mock.calls.filter(([path]) => path === join(home, "unused.sock"))).toHaveLength(0);
		expect(login).toHaveBeenCalledTimes(1);
	} finally {
		for (const timer of timers) clearTimeout(timer);
		timerSpy.mockRestore();
		fetchSpy.mockRestore();
		login.mockRestore();
		reconnect.mockRestore();
		connect.mockRestore();
		if (oldHome === undefined) delete process.env.GAJAEWAY_HOME;
		else process.env.GAJAEWAY_HOME = oldHome;
		await rm(home, { recursive: true, force: true });
	}
}, 15_000);

test("live image acknowledgements restore presence without declining, duplicating, or restoring replayed intent", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-live-image-presence-"));
	const cursorPath = join(home, "recovery-cursor.json");
	await saveRecoveryCursors(cursorPath, {
		recoveredThrough: {},
		quarantined: {},
		knownDms: {},
		attempts: {},
		deadLetters: [],
		deadLetterDigest: {},
		sequence: 0,
		gatewayDbIdentity: "test-db",
	});
	const ports = presencePorts();
	const requests: Array<{ verb: string; params: any }> = [];
	let engaged = true;
	let gate: Promise<void> | undefined;
	let releaseHeld: (() => void) | undefined;
	let sendStarted: (() => void) | undefined;
	const channel = { messages: { fetch: async () => [] } };
	const gateway = new ReconnectingGateway(
		"unused-socket",
		{ channels: { fetch: async () => channel } } as never,
		{
			token: "unused",
			tokenFile: "unused",
			configPath: "unused",
			channels: { "channel-thread": { engagement: "open" } },
		} as never,
		ports.typingPort as never,
		ports.statusPort as never,
		cursorPath,
		() => bot,
		{
			request: async (verb: string, params: unknown) => {
				if (verb === "gateway.status") return { database: { identity: "test-db" } };
				requests.push({ verb, params });
				if (verb === "chat.send" && gate) {
					sendStarted?.();
					await gate;
				}
				return { engaged, turnId: "turn-1" };
			},
		} as never,
		async () => {},
		undefined,
		undefined,
		async () => ({ images: [image], notice: "" }),
	);
	try {
		const live = dmImageMessage(snowflakeFromTimestamp(Date.now() - 90_000));
		expect(await gateway.processImage(live as never, engagementForMessage(live as never, bot))).toBe("acked");
		expect(ports.typing).toEqual(["new-dm"]);
		expect(ports.armed).toEqual([{ conversationId: "new-dm", messageId: live.id, channelId: "new-dm" }]);

		// An explicit decline is an acknowledged no-op, not a live turn to decorate.
		engaged = false;
		const declined = dmImageMessage(snowflakeFromTimestamp(Date.now() - 80_000));
		expect(await gateway.processImage(declined as never, engagementForMessage(declined as never, bot))).toBe("acked");
		expect(ports.typing).toEqual(["new-dm"]);
		expect(ports.armed).toHaveLength(1);

		// A duplicate Discord delivery shares the owner and must not arm presence twice.
		engaged = true;
		gate = new Promise<void>((resolve) => {
			releaseHeld = resolve;
		});
		const openedSend = new Promise<void>((resolve) => {
			sendStarted = resolve;
		});
		const duplicate = dmImageMessage(snowflakeFromTimestamp(Date.now() - 70_000));
		const first = gateway.processImage(duplicate as never, engagementForMessage(duplicate as never, bot));
		await openedSend;
		const second = gateway.processImage(duplicate as never, engagementForMessage(duplicate as never, bot));
		releaseHeld?.();
		gate = undefined;
		sendStarted = undefined;
		expect(await first).toBe("acked");
		expect(await second).toBe("acked");
		expect(ports.typing).toEqual(["new-dm", "new-dm"]);
		expect(ports.armed).toHaveLength(2);

		// A recovered history delivery is not a fresh live owner, even when engaged.
		const recovered = dmImageMessage(snowflakeFromTimestamp(Date.now() - 60_000));
		expect(
			await gateway.processImage(
				recovered as never,
				engagementForMessage(recovered as never, bot),
				undefined,
				undefined,
				true,
				true,
			),
		).toBe("acked");
		expect(ports.typing).toEqual(["new-dm", "new-dm"]);
		expect(ports.armed).toHaveLength(2);

		// A channel mention answered in a thread arms presence on the thread origin.
		const mentioned = {
			id: snowflakeFromTimestamp(Date.now() - 50_000),
			content: "<@bot> describe this",
			author: { id: "owner", username: "owner", bot: false },
			channel: { id: "channel-thread", name: "general" },
			attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }],
			startThread: async () => ({ id: "thread-live" }),
		};
		expect(await gateway.processImage(mentioned as never, engagementForMessage(mentioned as never, bot))).toBe("acked");
		expect(ports.typing.at(-1)).toBe("thread-live");
		expect(ports.armed.at(-1)).toEqual({
			conversationId: "thread-live",
			messageId: mentioned.id,
			channelId: "channel-thread",
		});
	} finally {
		releaseHeld?.();
		await rm(home, { recursive: true, force: true });
	}
}, 15_000);

test("a persisted live image replayed after restart does not restore presence", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-live-image-restart-presence-"));
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const ports = presencePorts();
	const requests: Array<{ verb: string; params: any }> = [];
	const id = snowflakeFromTimestamp(Date.now() - 90_000);
	const channelId = "new-dm";
	try {
		await saveRecoveryCursors(cursorPath, {
			// The watermark is past the intent, so it is eligible for independent durable
			// replay instead of waiting for a history pass this fixture does not model.
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 30_000) },
			quarantined: {},
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			attempts: {},
			deadLetters: [],
			deadLetterDigest: {},
			sequence: 1,
			gatewayDbIdentity: "test-db",
		});
		// A crash after admission but before the first send leaves `live:true`; that
		// marker proves where the intent started, not that its owner is live now.
		await writeFile(
			outboxPath,
			JSON.stringify([
				{
					messageId: id,
					channelId,
					live: true,
					request: {
						messageId: id,
						text: "describe this\n[image · photo.png]",
						images: [image],
						origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "owner" },
						engagement: { mentioned: false, group: false, authorId: "owner" },
					},
				},
			]),
		);
		const channel = {
			messages: {
				fetch: async (query: unknown) =>
					singleFetchId(query) === undefined ? [] : dmImageMessage(singleFetchId(query) as string),
			},
		};
		const gateway = new ReconnectingGateway(
			"unused-socket",
			{ channels: { fetch: async () => channel } } as never,
			{ token: "unused", tokenFile: "unused", configPath: "unused", channels: {} } as never,
			ports.typingPort as never,
			ports.statusPort as never,
			cursorPath,
			() => bot,
			{
				request: async (verb: string, params: unknown) => {
					if (verb === "gateway.status") return { database: { identity: "test-db" } };
					requests.push({ verb, params });
					return { engaged: true, turnId: "turn-1" };
				},
			} as never,
			async () => {},
			undefined,
			undefined,
			async () => ({ images: [image], notice: "" }),
		);
		await gateway.recoverMissedMessages();
		await gateway.cursorsFlushed;
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([id]);
		expect(ports.typing).toEqual([]);
		expect(ports.armed).toEqual([]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}, 15_000);
