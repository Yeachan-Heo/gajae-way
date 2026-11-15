import { expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { engagementForMessage, ReconnectingGateway } from "../src/main";
import { saveRecoveryCursors, snowflakeFromTimestamp } from "../src/recovery";

const bot = { id: "bot" };
const image = { data: "AQID", mimeType: "image/jpeg" as const };

async function fixture(options: { dm?: boolean; watermarkPast?: boolean } = {}) {
	const home = await mkdtemp(join(tmpdir(), "discord-image-race-"));
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const channelId = options.dm ? "new-dm" : "channel-1";
	if (options.watermarkPast) {
		await saveRecoveryCursors(cursorPath, {
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now()) },
			quarantined: {},
			knownDms: {},
			attempts: {},
			deadLetters: [],
			deadLetterDigest: {},
			sequence: 0,
		});
	}
	let release!: () => void;
	const held = new Promise<void>((resolve) => {
		release = resolve;
	});
	let started!: () => void;
	const preparing = new Promise<void>((resolve) => {
		started = resolve;
	});
	let preparations = 0;
	let threads = 0;
	let historyFetches = 0;
	const requests: Array<{ verb: string; params: any }> = [];
	const message = {
		id,
		content: options.dm ? "describe this" : "<@bot> describe this",
		author: { id: "human", username: "human", bot: false },
		channel: { id: channelId, ...(options.dm ? { isDMBased: () => true } : { name: "general" }) },
		attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }],
		startThread: async () => {
			threads++;
			return { id: "thread-1" };
		},
	};
	const channel = {
		messages: {
			fetch: async (query: string | { after?: string }) => {
				if (typeof query === "string") return message;
				historyFetches++;
				return !query.after || BigInt(query.after) < BigInt(id) ? [message] : [];
			},
		},
	};
	const gateway = new ReconnectingGateway(
		"unused-socket",
		{ channels: { fetch: async () => channel } } as never,
		{
			token: "unused",
			tokenFile: "unused",
			configPath: "unused",
			channels: options.dm ? {} : { [channelId]: { engagement: "open" } },
		} as never,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		{
			request: async (verb: string, params: unknown) => {
				requests.push({ verb, params });
				return { engaged: false, turnId: null };
			},
			onChatMessage: () => () => {},
		} as never,
		async () => {},
		undefined,
		undefined,
		async () => {
			preparations++;
			started();
			await held;
			return { images: [image], notice: "" };
		},
	);
	const live = () => gateway.processImage(message as never, engagementForMessage(message as never, bot));
	return {
		home,
		outboxPath,
		id,
		channelId,
		gateway,
		live,
		preparing,
		release,
		requests,
		counts: () => ({ preparations, threads, historyFetches }),
	};
}

async function assertDelivered(f: Awaited<ReturnType<typeof fixture>>, dm = false, threaded = true) {
	expect(f.requests).toHaveLength(1);
	expect(f.requests[0]).toMatchObject({
		verb: "chat.send",
		params: {
			messageId: f.id,
			origin: dm
				? { platform: "discord", kind: "dm", conversationId: f.channelId, peerId: "human" }
				: threaded
					? { platform: "discord", kind: "thread", conversationId: "thread-1", parentId: f.channelId }
					: { platform: "discord", kind: "channel", conversationId: f.channelId },
			images: [image],
		},
	});
	expect(f.counts().preparations).toBe(1);
	expect(f.counts().threads).toBe(dm || !threaded ? 0 : 1);
	expect(JSON.parse(await readFile(f.outboxPath, "utf8"))).toEqual([
		{
			messageId: f.id,
			channelId: f.channelId,
			completed: true,
			completedAt: expect.any(Number),
			...(dm ? { dm: true } : {}),
		},
	]);
}

test("recovery and a concurrent live event share image preparation without a recovery-side thread", async () => {
	const f = await fixture();
	try {
		const recovery = f.gateway.recoverMissedMessages();
		await f.preparing;
		const live = f.live();
		expect(f.requests).toEqual([]);
		expect(f.counts()).toMatchObject({ preparations: 1, threads: 0 });
		f.release();
		expect(await live).toBe("acked");
		await recovery;
		await assertDelivered(f, false, false);
		await f.gateway.recoverMissedMessages();
		await assertDelivered(f, false, false);
	} finally {
		f.release();
		await rm(f.home, { recursive: true, force: true });
	}
});

for (const scenario of [
	{ name: "a recovery watermark already past the image", options: { watermarkPast: true }, dm: false },
	{ name: "a first-time DM not yet in recovery cursors", options: { dm: true }, dm: true },
]) {
	test(`live ownership survives concurrent recovery with ${scenario.name}`, async () => {
		const f = await fixture(scenario.options);
		try {
			const live = f.live();
			await f.preparing;
			const recovery = f.gateway.recoverMissedMessages();
			// Allow recovery to encounter the persisted intent while preparation is held.
			await delay(50);
			expect(f.requests).toEqual([]);
			expect(f.counts()).toMatchObject({ preparations: 1, threads: 0 });
			f.release();
			expect(await live).toBe("acked");
			await recovery;
			await assertDelivered(f, scenario.dm);
			await f.gateway.recoverMissedMessages();
			await assertDelivered(f, scenario.dm);
		} finally {
			f.release();
			await rm(f.home, { recursive: true, force: true });
		}
	});
}

test("an unreadable image outbox fails closed until durable storage is repaired", async () => {
	const f = await fixture({ dm: true });
	try {
		await writeFile(f.outboxPath, "not valid JSON");
		expect(await f.live()).toBe("unavailable");
		await f.gateway.recoverMissedMessages();
		expect(f.requests).toEqual([]);
		expect(f.counts()).toMatchObject({ preparations: 0, threads: 0 });
		expect(await readFile(f.outboxPath, "utf8")).toBe("not valid JSON");
		expect(f.gateway.recoveryRetryPending).toBe(true);
		await writeFile(f.outboxPath, "[]\n");
		const live = f.live();
		await f.preparing;
		f.release();
		expect(await live).toBe("acked");
		await f.gateway.recoverMissedMessages();
		await assertDelivered(f, true);
	} finally {
		f.release();
		await rm(f.home, { recursive: true, force: true });
	}
});
