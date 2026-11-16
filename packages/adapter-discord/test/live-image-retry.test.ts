import { expect, spyOn, test } from "bun:test";
import { Buffer } from "node:buffer";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { Client } from "discord.js";
import { startDiscordAdapter } from "../src/main";
import { snowflakeFromTimestamp } from "../src/recovery";

// The same valid PNG fixture used by the other adapter image tests.
const png = Uint8Array.from(
	Buffer.from(
		"iVBORw0KGgoAAAANSUhEUgAAAAIAAAACCAIAAAD91JpzAAAAEklEQVR4nGP4z8DAAMIM/4EAAB/uBfsL2WiLAAAAAElFTkSuQmCC",
		"base64",
	),
);

test("a transient image download failure retries a live new DM without reconnecting or sending text alone", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-live-image-retry-"));
	const oldHome = process.env.GAJAEWAY_HOME;
	process.env.GAJAEWAY_HOME = home;
	const sends: Array<{ verb: string; params: any }> = [];
	const transport = {
		onChatMessage: () => () => {},
		onChatProgress: () => () => {},
		request: async (verb: string, params: unknown) => {
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
			messages: { fetch: async (query: unknown) => (typeof query === "string" ? message : [message]) },
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
