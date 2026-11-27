import { expect, spyOn, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { ImageOutbox, type PendingImage } from "../src/image-outbox";
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
				// Identity polling is additive infrastructure, not image delivery.
				if (verb !== "gateway.status") requests.push({ verb, params });
				if (verb === "gateway.status") return { database: { identity: "test-db" } };
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
		cursorPath,
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

/** Keep scheduled retries from firing during an assertion that inspects intermediate state. */
function postponeTimers() {
	const realSetTimeout = globalThis.setTimeout;
	const timers: ReturnType<typeof setTimeout>[] = [];
	const spy = spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
		const timer = realSetTimeout(args[0], 60_000, ...args.slice(2));
		timer.unref?.();
		timers.push(timer);
		return timer;
	}) as typeof setTimeout);
	return () => {
		for (const timer of timers) clearTimeout(timer);
		spy.mockRestore();
	};
}

async function invalidationFixture(options: { summary: string; failTerminalWrite?: boolean }) {
	const home = await mkdtemp(join(tmpdir(), "discord-image-invalidation-"));
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const channelId = "new-dm";
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
	const message = {
		id,
		content: "describe this",
		author: { id: "human", username: "human", bot: false },
		channel: { id: channelId, isDMBased: () => true },
		attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }],
	};
	let releaseAdmission!: () => void;
	const admission = new Promise<void>((resolve) => {
		releaseAdmission = resolve;
	});
	let reachedAdmission!: () => void;
	const reached = new Promise<void>((resolve) => {
		reachedAdmission = resolve;
	});
	const outbox = new ImageOutbox(outboxPath);
	const realGet = outbox.get.bind(outbox);
	let blockInitialRead = true;
	outbox.get = async (messageId: string) => {
		const entry = await realGet(messageId);
		if (blockInitialRead) {
			blockInitialRead = false;
			reachedAdmission();
			await admission;
		}
		return entry;
	};
	if (options.failTerminalWrite) {
		const realPut = outbox.put.bind(outbox);
		let failOnce = true;
		outbox.put = async (entry: PendingImage) => {
			if (entry.terminal && failOnce) {
				failOnce = false;
				throw new Error("transient terminal journal failure");
			}
			return realPut(entry);
		};
	}
	const requests: Array<{ verb: string; params: any }> = [];
	const channel = {
		messages: {
			fetch: async (query: unknown) =>
				typeof query === "string" || (query as { message?: string }).message !== undefined ? message : [],
		},
	};
	const gateway = new ReconnectingGateway(
		"unused-socket",
		{ channels: { fetch: async () => channel } } as never,
		{ token: "unused", tokenFile: "unused", configPath: "unused", channels: {} } as never,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		{
			request: async (verb: string, params: unknown) => {
				if (verb !== "gateway.status") requests.push({ verb, params });
				if (verb === "gateway.status") return { database: { identity: "test-db" } };
				return { engaged: true, turnId: "turn-1" };
			},
			onChatMessage: () => () => {},
		} as never,
		async () => {},
		undefined,
		undefined,
		async () => ({ images: [image], notice: "" }),
		outbox,
	);
	return {
		home,
		outboxPath,
		id,
		message,
		gateway,
		reached,
		releaseAdmission,
		requests,
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
			// The fake gateway returns `engaged:false`, which is an explicit decline:
			// the tombstone stays for dedupe but is reclaimable evidence, not quota.
			declined: true,
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
		await saveRecoveryCursors(f.cursorPath, {
			recoveredThrough: {},
			quarantined: {},
			knownDms: {},
			attempts: {},
			deadLetters: [],
			deadLetterDigest: {},
			sequence: 0,
			gatewayDbIdentity: "test-db",
		});
		// Adopt the seeded generation before storage breaks.
		await f.gateway.recoverMissedMessages();
		await writeFile(f.outboxPath, "not valid JSON");
		await f.gateway.recoverMissedMessages();
		expect(f.requests).toEqual([]);
		expect(f.counts()).toMatchObject({ preparations: 0, threads: 0 });
		expect(await readFile(f.outboxPath, "utf8")).toBe("not valid JSON");
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

for (const scenario of [
	{ label: "delete", summary: "image source message was deleted" },
	{ label: "image edit", summary: "image source message was edited" },
]) {
	test(`a ${scenario.label} between the initial journal read and first put never sends the stale image`, async () => {
		const cleanupTimers = trackTimers();
		const f = await invalidationFixture({ summary: scenario.summary });
		try {
			const live = f.gateway.processImage(f.message as never, engagementForMessage(f.message as never, bot));
			await f.reached;
			// The owner has observed "no row" and is still holding its first admission
			// write. The invalidation must arrive as a synchronous fence, not a lookup
			// that can miss the intent about to land.
			await f.gateway.invalidateImageSource(f.id, scenario.summary);
			f.releaseAdmission();
			expect(await live).toBe("skip");
			expect(f.requests).toEqual([]);
			expect(JSON.parse(await readFile(f.outboxPath, "utf8"))).toEqual([
				{ messageId: f.id, channelId: "new-dm", terminal: true, audited: true, dm: true },
			]);
		} finally {
			f.releaseAdmission();
			cleanupTimers();
			await rm(f.home, { recursive: true, force: true });
		}
	}, 12_000);
}

test("a transient terminal journal failure keeps the invalidation fence until the row is durable", async () => {
	const f = await invalidationFixture({
		summary: "image source message was deleted",
		failTerminalWrite: true,
	});
	// Postpone the owner's retry timer: this test drives the next attempt itself, and
	// an early timer would race the interrupted-row assertion.
	const cleanupTimers = postponeTimers();
	try {
		const live = f.gateway.processImage(f.message as never, engagementForMessage(f.message as never, bot));
		await f.reached;
		await f.gateway.invalidateImageSource(f.id, "image source message was deleted");
		f.releaseAdmission();
		// The terminal write failed, so the owner cannot claim the intent is resolved;
		// it must still refuse to send.
		expect(await live).toBe("unavailable");
		expect(f.requests).toEqual([]);
		const interrupted = JSON.parse(await readFile(f.outboxPath, "utf8"));
		expect(interrupted[0].terminal).toBeUndefined();
		// Recovery retries the persisted intent and the retained in-memory fence
		// terminalizes it instead of answering the deleted source.
		await f.gateway.recoverMissedMessages();
		await f.gateway.cursorsFlushed;
		expect(JSON.parse(await readFile(f.outboxPath, "utf8"))).toEqual([
			{ messageId: f.id, channelId: "new-dm", terminal: true, audited: true, dm: true },
		]);
		expect(f.requests).toEqual([]);
	} finally {
		f.releaseAdmission();
		cleanupTimers();
		await rm(f.home, { recursive: true, force: true });
	}
}, 12_000);
