import { expect, spyOn, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { prepareDiscordImages } from "../src/attachments";
import { imageSourceIdentity } from "../src/attachments";
import { type DiscordInboundMessage, decideInbound, ReconnectingGateway } from "../src/main";
import { loadRecoveryCursors } from "../src/recovery";

const bot = { id: "bot" };
const image = { mimeType: "image/jpeg" as const, data: "AQIDBA==" };
const message = {
	id: "123456789012345678",
	content: "What is in this picture?",
	author: { id: "owner", username: "owner", bot: false },
	channel: { id: "new-dm", isDMBased: () => true },
	attachments: [
		{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/attachments/1/2/photo.png" },
	],
} as DiscordInboundMessage;

function gateway(
	cursorPath: string,
	fetchChannel: (id: string) => Promise<unknown>,
	request: (verb: string, params: unknown) => Promise<unknown>,
	prepare: typeof prepareDiscordImages,
) {
	return new ReconnectingGateway(
		"unused-socket",
		{ channels: { fetch: fetchChannel } } as never,
		{ tokenFile: "unused", token: "unused", configPath: "unused", channels: {} } as never,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		{ request } as never,
		async () => {},
		undefined,
		undefined,
		prepare,
	);
}

async function outbox(home: string): Promise<unknown> {
	return JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"));
}

// Failed sends schedule background retries; a restarted process would not retain
// those timers. Track and cancel them before removing the temporary state.
function trackTimers() {
	const realSetTimeout = globalThis.setTimeout;
	const timers: ReturnType<typeof setTimeout>[] = [];
	const spy = spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
		const timer = realSetTimeout(...args);
		timer.unref?.();
		timers.push(timer);
		return timer;
	}) as typeof setTimeout);
	return () => {
		for (const timer of timers) clearTimeout(timer);
		spy.mockRestore();
	};
}

test("a first-time DM image intent survives restart before preparation", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-intent-restart-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const sends: Array<{ verb: string; params: unknown }> = [];
	const request = async (verb: string, params: unknown) => {
		// Identity polling is additive infrastructure, not a send.
		if (verb === "gateway.status") return {};
		sends.push({ verb, params });
		return { engaged: false, turnId: null };
	};
	const fetched: string[] = [];
	const fetchChannel = async (id: string) => {
		fetched.push(id);
		return {
			messages: {
				fetch: async (query: string | object) => {
					if (typeof query !== "string") return [];
					expect(query).toBe(message.id);
					return message;
				},
			},
		};
	};
	try {
		const first = gateway(cursorPath, fetchChannel, request, async () => {
			throw new Error("download unavailable");
		});
		const engagement = decideInbound(message, bot, {});
		expect(engagement).toBeDefined();
		expect(await first.processImage(message, engagement!)).toBe("unavailable");
		expect(sends).toEqual([]);
		expect(await outbox(home)).toEqual([
			{
				messageId: message.id,
				channelId: "new-dm",
				dm: true,
				live: true,
				createdAt: expect.any(Number),
				sourceIdentity: imageSourceIdentity(message),
				sourceText: message.content,
				failureAt: expect.any(Number),
			},
		]);
		expect((await loadRecoveryCursors(cursorPath)).knownDms["new-dm"]).toBeDefined();

		let preparations = 0;
		const restarted = gateway(cursorPath, fetchChannel, request, async (inbound) => {
			preparations++;
			expect(inbound).toEqual(message);
			return { images: [image], notice: "" };
		});
		await restarted.recoverMissedMessages();
		expect(fetched).toEqual(["new-dm", "new-dm"]);
		expect(preparations).toBe(1);
		expect(sends).toHaveLength(1);
		expect(sends[0]).toMatchObject({
			verb: "chat.send",
			params: {
				messageId: message.id,
				origin: { platform: "discord", kind: "dm", conversationId: "new-dm", peerId: "owner" },
				text: "What is in this picture?\n[image · photo.png]",
				images: [image],
			},
		});
		expect(await outbox(home)).toEqual([
			{
				messageId: message.id,
				channelId: "new-dm",
				completed: true,
				completedAt: expect.any(Number),
				declined: true,
				dm: true,
			},
		]);
		await restarted.recoverMissedMessages();
		expect(sends).toHaveLength(1);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
});

test("an ambiguous gateway ACK replays the exact persisted image request after restart", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-ack-restart-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const sends: Array<{ verb: string; params: unknown }> = [];
	let preparations = 0;
	const fetchChannel = async () => ({
		messages: {
			fetch: async (query: string | object) => {
				if (typeof query === "string") throw new Error("persisted requests must not refetch the message");
				return [];
			},
		},
	});
	try {
		const first = gateway(
			cursorPath,
			fetchChannel,
			async (verb, params) => {
				// Identity polling is additive infrastructure, not a send.
				if (verb === "gateway.status") return {};
				sends.push({ verb, params });
				throw new Error("connection lost after gateway accepted request");
			},
			async () => {
				preparations++;
				return { images: [image], notice: "" };
			},
		);
		const engagement = decideInbound(message, bot, {});
		expect(engagement).toBeDefined();
		expect(await first.processImage(message, engagement!)).toBe("unavailable");
		expect(sends).toHaveLength(1);
		const persisted = (await outbox(home)) as Array<{ messageId: string; channelId: string; request: unknown }>;
		expect(persisted).toHaveLength(1);
		expect(persisted[0]).toMatchObject({
			messageId: message.id,
			channelId: "new-dm",
			request: {
				origin: { platform: "discord", kind: "dm", conversationId: "new-dm", peerId: "owner" },
				images: [image],
			},
		});
		expect(sends[0]).toEqual({ verb: "chat.send", params: persisted[0].request });

		const restarted = gateway(
			cursorPath,
			fetchChannel,
			async (verb, params) => {
				if (verb === "gateway.status") return {};
				sends.push({ verb, params });
				return { engaged: false, turnId: null };
			},
			async () => {
				preparations++;
				throw new Error("persisted requests must not be prepared again");
			},
		);
		await restarted.recoverMissedMessages();
		expect(preparations).toBe(1);
		expect(sends).toHaveLength(2);
		expect(sends[1]).toEqual({ verb: "chat.send", params: persisted[0].request });
		expect(await outbox(home)).toEqual([
			{
				messageId: message.id,
				channelId: "new-dm",
				completed: true,
				completedAt: expect.any(Number),
				declined: true,
				dm: true,
			},
		]);
		await restarted.recoverMissedMessages();
		expect(sends).toHaveLength(2);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
});
