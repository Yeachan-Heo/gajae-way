import { expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { DiscordImagePreparationError } from "../src/attachments";
import { decideInbound, ReconnectingGateway } from "../src/main";
import { loadRecoveryCursors, saveRecoveryCursors, snowflakeFromTimestamp } from "../src/recovery";

const bot = { id: "bot" };
const image = { data: "AQID", mimeType: "image/jpeg" as const };
const emptyCursors = () => ({
	recoveredThrough: {},
	quarantined: {},
	knownDms: {},
	attempts: {},
	deadLetters: [],
	deadLetterDigest: {},
	sequence: 0,
});

function dmMessage(id: string, channelId: string, withImage = false) {
	return {
		id,
		content: withImage ? "describe this" : "later text",
		author: { id: "human", username: "human", bot: false },
		channel: { id: channelId, isDMBased: () => true },
		attachments: withImage
			? [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }]
			: [],
	};
}

function deferred() {
	let release!: () => void;
	const promise = new Promise<void>((resolve) => {
		release = resolve;
	});
	return { promise, release };
}

async function bounded<T>(promise: Promise<T>, ms = 8_000): Promise<T> {
	return Promise.race([
		promise,
		delay(ms).then(() => {
			throw new Error("Timed out waiting for ingress progress");
		}),
	]);
}

async function eventually(predicate: () => boolean, ms = 8_000) {
	const deadline = Date.now() + ms;
	while (!predicate() && Date.now() < deadline) await delay(25);
	expect(predicate()).toBe(true);
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

function gateway(
	cursorPath: string,
	fetchChannel: (id: string) => Promise<unknown>,
	requests: Array<{ verb: string; params: any }>,
	channels: Record<string, { engagement: string }> = {},
	requestImpl?: (verb: string, params: unknown) => Promise<unknown>,
	prepareImpl?: () => Promise<{ images: Array<{ data: string; mimeType: "image/jpeg" }>; notice: string }>,
) {
	return new ReconnectingGateway(
		"unused-socket",
		{ channels: { fetch: fetchChannel } } as never,
		{ token: "unused", tokenFile: "unused", configPath: "unused", channels } as never,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		{
			request: async (verb: string, params: unknown) => {
				requests.push({ verb, params });
				return requestImpl ? requestImpl(verb, params) : { engaged: false, turnId: null };
			},
		} as never,
		async () => {},
		undefined,
		undefined,
		prepareImpl ?? (async () => ({ images: [image], notice: "" })),
	);
}

test("recovery delivers an older durable image below the watermark before newer history text", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-history-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "dm-history";
	const olderId = snowflakeFromTimestamp(Date.now() - 120_000);
	const watermark = snowflakeFromTimestamp(Date.now() - 90_000);
	const newerId = snowflakeFromTimestamp(Date.now() - 60_000);
	const later = dmMessage(newerId, channelId);
	const request = {
		messageId: olderId,
		text: "describe this\n[image · photo.png]",
		images: [image],
		origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
		engagement: { mentioned: false, group: false, authorId: "human" },
	};
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: watermark },
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(join(home, "image-outbox.json"), JSON.stringify([{ messageId: olderId, channelId, request }]));
		const channel = {
			messages: {
				fetch: async (query: string | { after?: string }) => {
					if (typeof query === "string") throw new Error("prepared request should not need message refetch");
					return !query.after || BigInt(query.after) < BigInt(newerId) ? [later] : [];
				},
			},
		};
		const adapter = gateway(cursorPath, async () => channel, requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([
			olderId,
			newerId,
		]);
		expect(requests[0].params).toEqual(request);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toHaveLength(2);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
});

test("a failing unrelated channel cannot starve a repaired live image intent below its watermark", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-isolation-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "healthy-dm";
	const id = snowflakeFromTimestamp(Date.now() - 120_000);
	const message = dmMessage(id, channelId, true);
	const requests: Array<{ verb: string; params: any }> = [];
	let failedHistoryFetches = 0;
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 60_000) },
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(outboxPath, "not JSON");
		const adapter = gateway(
			cursorPath,
			async (id) =>
				id === "broken-channel"
					? {
							messages: {
								fetch: async () => {
									failedHistoryFetches++;
									throw new Error("temporary history failure");
								},
							},
						}
					: { messages: { fetch: async (query: string | object) => (typeof query === "string" ? message : []) } },
			requests,
			{ "broken-channel": { engagement: "open" } },
		);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("unavailable");
		expect(requests).toEqual([]);
		await bounded(adapter.recoverMissedMessages());
		expect(failedHistoryFetches).toBeGreaterThan(0);
		await writeFile(outboxPath, "[]\n");
		await eventually(() => requests.some((entry) => entry.verb === "chat.send"));
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([id]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a cursor-store write failure does not block image delivery or duplicate it after repair", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-turn-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "new-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		const channel = {
			messages: { fetch: async (query: string | object) => (typeof query === "string" ? message : [message]) },
		};
		const adapter = gateway(cursorPath, async () => channel, requests);
		await bounded(adapter.recoverMissedMessages());
		// Replacing the cursor file with a directory makes the DM registration's
		// atomic rename fail after the outbox intent has already been persisted.
		await rm(cursorPath);
		await mkdir(cursorPath);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("acked");
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))).toEqual([
			{ messageId: id, channelId, completed: true, completedAt: expect.any(Number), dm: true },
		]);
		await rm(cursorPath, { recursive: true });
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toHaveLength(1);
		// The registration is re-persisted once the store is repaired, and the
		// completed tombstone keeps the repaired pass from replaying the send.
		expect((await loadRecoveryCursors(cursorPath)).knownDms[channelId]).toBeDefined();
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 15_000);

test("a deleted image message is dead-lettered instead of retrying forever", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-deleted-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "gone-dm";
	const id = snowflakeFromTimestamp(Date.now() - 120_000);
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(join(home, "image-outbox.json"), JSON.stringify([{ messageId: id, channelId }]));
		const channel = {
			messages: {
				fetch: async (query: string | object) => {
					if (typeof query !== "string") return [];
					throw Object.assign(new Error("Unknown Message"), { code: 10_008, status: 404 });
				},
			},
		};
		const adapter = gateway(cursorPath, async () => channel, requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect(adapter.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))).toEqual([
			{ messageId: id, channelId, terminal: true, audited: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("an unreadable history channel still reaches a terminal image diagnosis", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-blocked-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "broken-dm";
	const id = snowflakeFromTimestamp(Date.now() - 120_000);
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(join(home, "image-outbox.json"), JSON.stringify([{ messageId: id, channelId }]));
		const denied = Object.assign(new Error("Missing Access"), { code: 50_001, status: 403 });
		const channel = {
			messages: {
				fetch: async () => {
					throw denied;
				},
			},
		};
		const adapter = gateway(cursorPath, async () => channel, requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect(adapter.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))).toEqual([
			{ messageId: id, channelId, terminal: true, audited: true },
		]);
		await bounded(adapter.waitForImage({ id } as never, Promise.resolve("unavailable")), 2_000);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a terminal chat.send rejection is dead-lettered after the bounded attempt budget", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-terminal-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "new-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	const requests: Array<{ verb: string; params: any }> = [];
	const channel = {
		messages: { fetch: async (query: string | object) => (typeof query === "string" ? message : []) },
	};
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		const adapter = gateway(
			cursorPath,
			async () => channel,
			requests,
			{},
			async () => {
				throw Object.assign(new Error("frame exceeds limit"), { code: "payload_too_large" });
			},
		);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		let sends = 0;
		const client = {
			request: async () => {
				sends++;
				throw Object.assign(new Error("frame exceeds limit"), { code: "payload_too_large" });
			},
		};
		adapter.adoptClient(client as never);
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("unavailable");
		adapter.adoptClient(client as never);
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("unavailable");
		const waiting = adapter.waitForImage(message as never, Promise.resolve("unavailable"));
		adapter.adoptClient(client as never);
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("skip");
		await bounded(waiting, 2_000);
		expect(sends).toBe(3);
		expect(adapter.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))).toEqual([
			{ messageId: id, channelId, terminal: true, audited: true, dm: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("recovery does not wait behind a live image whose ingress turn is still held", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-wait-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "new-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	const requests: Array<{ verb: string; params: any }> = [];
	const turn = deferred();
	const channel = {
		messages: { fetch: async (query: string | object) => (typeof query === "string" ? message : [message]) },
	};
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		const adapter = gateway(cursorPath, async () => channel, requests);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		const live = adapter.processImage(message as never, engagement!, undefined, turn.promise);
		await delay(100);
		await bounded(adapter.recoverMissedMessages(), 3_000);
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		turn.release();
		expect(await bounded(live)).toBe("acked");
		expect(requests.filter((entry) => entry.verb === "chat.send")).toHaveLength(1);
	} finally {
		turn.release();
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("consecutive preparation failures re-arm the image retry timer", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-rearm-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "new-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	const requests: Array<{ verb: string; params: any }> = [];
	let preparations = 0;
	const channel = {
		messages: { fetch: async (query?: unknown) => (typeof query === "string" ? message : []) },
	};
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		const adapter = gateway(
			cursorPath,
			async () => channel,
			requests,
			{},
			undefined,
			async () => {
				preparations++;
				if (preparations <= 6) throw new DiscordImagePreparationError("retryable");
				return { images: [image], notice: "" };
			},
		);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("unavailable");
		// Two timer-driven attempts must follow the first in-invocation budget.
		await eventually(() => requests.some((entry) => entry.verb === "chat.send"), 8_000);
		expect(preparations).toBe(7);
		expect(requests.filter((entry) => entry.verb === "chat.send")).toHaveLength(1);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("an intent unacknowledged for 24 hours is dead-lettered instead of wedging ingress", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-expired-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "stuck-dm";
	const id = snowflakeFromTimestamp(Date.now() - 120_000);
	const requests: Array<{ verb: string; params: any }> = [];
	const channel = {
		messages: { fetch: async (query?: unknown) => (typeof query === "string" ? dmMessage(id, channelId, true) : []) },
	};
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		const request = {
			messageId: id,
			text: "look\n[image · photo.png]",
			origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
			engagement: { mentioned: false, group: false, authorId: "human" },
			images: [image],
		};
		await writeFile(
			join(home, "image-outbox.json"),
			JSON.stringify([{ messageId: id, channelId, request, dm: true, failureAt: Date.now() - 25 * 60 * 60 * 1000 }]),
		);
		const adapter = gateway(cursorPath, async () => channel, requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect(adapter.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))).toEqual([
			{ messageId: id, channelId, terminal: true, audited: true, dm: true },
		]);
		await bounded(adapter.waitForImage({ id } as never, Promise.resolve("unavailable")), 2_000);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a persisted image request replays even when the recovery cursor is unreadable", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-cursor-read-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "new-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await mkdir(cursorPath);
		const request = {
			messageId: id,
			text: "look\n[image · photo.png]",
			origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
			engagement: { mentioned: false, group: false, authorId: "human" },
			images: [image],
		};
		await writeFile(join(home, "image-outbox.json"), JSON.stringify([{ messageId: id, channelId, request, dm: true }]));
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))).toEqual([
			{ messageId: id, channelId, completed: true, completedAt: expect.any(Number), dm: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("terminal image rows are compacted only after the watermark passes them", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-compact-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "channel-1";
	const below = snowflakeFromTimestamp(Date.now() - 120_000);
	const above = snowflakeFromTimestamp(Date.now() + 60_000);
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 60_000) },
		});
		await writeFile(
			join(home, "image-outbox.json"),
			JSON.stringify([
				{ messageId: below, channelId, terminal: true, audited: true },
				{ messageId: above, channelId, terminal: true, audited: true },
			]),
		);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), []);
		await bounded(adapter.recoverMissedMessages());
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))).toEqual([
			{ messageId: above, channelId, terminal: true, audited: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);
