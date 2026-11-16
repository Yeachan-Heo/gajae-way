import { expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { ImagePreparationBudgetError } from "@gajae-gateway/images";
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
	gatewayDbIdentity: "test-db",
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
	withClient = true,
	socketPath = "unused-socket",
) {
	return new ReconnectingGateway(
		socketPath,
		{ channels: { fetch: fetchChannel } } as never,
		{ token: "unused", tokenFile: "unused", configPath: "unused", channels } as never,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		withClient
			? ({
					request: async (verb: string, params: unknown) => {
						if (verb !== "gateway.status") requests.push({ verb, params });
						if (!requestImpl && verb === "gateway.status") return { database: { identity: "test-db" } };
						return requestImpl ? requestImpl(verb, params) : { engaged: false, turnId: null };
					},
				} as never)
			: undefined,
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
		expect(requests.find((entry) => entry.verb === "chat.send")?.params).toEqual(request);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toHaveLength(2);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
});

test("without a cursor, durable replay waits behind the bootstrap history window", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-bootstrap-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "bootstrap-dm";
	const olderId = snowflakeFromTimestamp(Date.now() - 180_000);
	const imageId = snowflakeFromTimestamp(Date.now() - 120_000);
	const request = {
		messageId: imageId,
		text: "describe this\n[image · photo.png]",
		images: [image],
		origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
		engagement: { mentioned: false, group: false, authorId: "human" },
	};
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(join(home, "image-outbox.json"), JSON.stringify([{ messageId: imageId, channelId, request }]));
		const channel = {
			messages: {
				fetch: async (query: string | { after?: string }) =>
					typeof query === "string"
						? dmMessage(imageId, channelId, true)
						: !query.after || BigInt(query.after) < BigInt(imageId)
							? [dmMessage(olderId, channelId), dmMessage(imageId, channelId, true)]
							: [],
			},
		};
		const adapter = gateway(cursorPath, async () => channel, requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([
			olderId,
			imageId,
		]);
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
			{ messageId: id, channelId, completed: true, completedAt: expect.any(Number), declined: true, dm: true },
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
			request: async (verb: string) => {
				// The first status poll of the database identity is additive, not a send.
				if (verb !== "chat.send") return {};
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

test("an image refused by the admission bound releases the conversation chain", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-capacity-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	await saveRecoveryCursors(cursorPath, emptyCursors());
	const channelId = "capacity-dm";
	const base = 1571558400000000000n;
	await writeFile(
		outboxPath,
		JSON.stringify(
			Array.from({ length: 1_000 }, (_, index) => ({
				messageId: String(base + BigInt(index)),
				channelId,
				dm: true,
			})),
		),
	);
	try {
		const requests: Array<{ verb: string; params: unknown }> = [];
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), requests);
		const id = String(base + 10_000n);
		const message = dmMessage(id, channelId, true);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		const first = adapter.processImage(message as never, engagement!);
		const waiter = adapter.waitForImage(message as never, first);
		expect(await bounded(first)).toBe("unavailable");
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("unavailable");
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("skip");
		await bounded(waiter, 2_000);
		expect(adapter.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(adapter.deadLetters[0]!.summary).toContain("admission");
		expect(
			JSON.parse(await readFile(outboxPath, "utf8")).some((entry: { messageId: string }) => entry.messageId === id),
		).toBe(false);
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect((await loadRecoveryCursors(cursorPath)).knownDms[channelId]).toBeDefined();
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 30_000);

test("a transient source-fetch failure starts the failure clock and expires", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-source-fail-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "source-fail-dm";
	const id = snowflakeFromTimestamp(Date.now() - 120_000);
	await saveRecoveryCursors(cursorPath, {
		...emptyCursors(),
		knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
		sequence: 1,
	});
	await writeFile(outboxPath, JSON.stringify([{ messageId: id, channelId, dm: true }]));
	try {
		const failing = gateway(
			cursorPath,
			async () => ({
				messages: {
					fetch: async () => {
						throw new Error("temporary source failure");
					},
				},
			}),
			[],
		);
		await bounded(failing.recoverMissedMessages());
		const afterFailure = JSON.parse(await readFile(outboxPath, "utf8"));
		expect(afterFailure[0].failureAt).toEqual(expect.any(Number));
		expect(afterFailure[0].terminal).toBeUndefined();
		await writeFile(outboxPath, JSON.stringify([{ ...afterFailure[0], failureAt: Date.now() - 25 * 60 * 60 * 1000 }]));
		const expired = gateway(
			cursorPath,
			async () => ({
				messages: {
					fetch: async () => {
						throw new Error("temporary source failure");
					},
				},
			}),
			[],
		);
		await bounded(expired.recoverMissedMessages());
		expect(expired.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([
			{ messageId: id, channelId, terminal: true, audited: true, dm: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("an in-window pending image stays behind older missed history", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-inwindow-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "dm-inwindow";
	const watermark = snowflakeFromTimestamp(Date.now() - 180_000);
	const textId = snowflakeFromTimestamp(Date.now() - 120_000);
	const imageId = snowflakeFromTimestamp(Date.now() - 60_000);
	const text = dmMessage(textId, channelId);
	const imageMessage = dmMessage(imageId, channelId, true);
	const request = {
		messageId: imageId,
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
		await writeFile(join(home, "image-outbox.json"), JSON.stringify([{ messageId: imageId, channelId, request }]));
		const channel = {
			messages: {
				fetch: async (query: string | { after?: string }) => {
					if (typeof query === "string") return imageMessage;
					return !query.after || BigInt(query.after) < BigInt(imageId) ? [imageMessage, text] : [];
				},
			},
		};
		const adapter = gateway(cursorPath, async () => channel, requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([
			textId,
			imageId,
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a truncated backfill does not let an above-horizon image overtake history", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-truncated-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "truncated-dm";
	const watermark = snowflakeFromTimestamp(Date.now() - 180_000);
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const request = {
		messageId: id,
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
		await writeFile(outboxPath, JSON.stringify([{ messageId: id, channelId, request }]));
		const adapter = gateway(
			cursorPath,
			async () => ({
				messages: {
					fetch: async () => {
						throw new Error("temporary history truncation");
					},
				},
			}),
			requests,
		);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		const persisted = JSON.parse(await readFile(outboxPath, "utf8"));
		expect(persisted).toEqual([{ messageId: id, channelId, request, failureAt: expect.any(Number) }]);
		await writeFile(
			outboxPath,
			JSON.stringify([{ messageId: id, channelId, request, failureAt: Date.now() - 25 * 60 * 60 * 1000 }]),
		);
		const expired = gateway(
			cursorPath,
			async () => ({
				messages: {
					fetch: async () => {
						throw new Error("temporary history truncation");
					},
				},
			}),
			requests,
		);
		await bounded(expired.recoverMissedMessages());
		expect(expired.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([
			{ messageId: id, channelId, terminal: true, audited: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a first-time channel uses the bootstrap floor before independent image replay", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-bootstrap-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "dm-bootstrap";
	const textId = snowflakeFromTimestamp(Date.now() - 120_000);
	const imageId = snowflakeFromTimestamp(Date.now() - 60_000);
	const text = dmMessage(textId, channelId);
	const imageMessage = dmMessage(imageId, channelId, true);
	const request = {
		messageId: imageId,
		text: "describe this\n[image · photo.png]",
		images: [image],
		origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
		engagement: { mentioned: false, group: false, authorId: "human" },
	};
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(join(home, "image-outbox.json"), JSON.stringify([{ messageId: imageId, channelId, request }]));
		const channel = {
			messages: {
				fetch: async (query: string | { after?: string }) => {
					if (typeof query === "string") return imageMessage;
					return !query.after || BigInt(query.after) < BigInt(imageId) ? [imageMessage, text] : [];
				},
			},
		};
		const adapter = gateway(cursorPath, async () => channel, requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([
			textId,
			imageId,
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a recovered image is admitted without creating a thread first", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-nothread-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "guild-channel";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	let created = 0;
	const message = {
		id,
		content: "<@bot> look",
		author: { id: "human", username: "human", bot: false },
		channel: { id: channelId, isDMBased: () => false },
		guild: { id: "guild" },
		mentions: { has: () => true },
		attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }],
		startThread: async () => {
			created++;
			return { id: "created-thread" };
		},
	};
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		const channel = {
			messages: { fetch: async (query?: unknown) => (typeof query === "string" ? message : [message]) },
		};
		const adapter = gateway(cursorPath, async () => channel, requests, { [channelId]: { engagement: "open" } });
		await bounded(adapter.recoverMissedMessages());
		expect(created).toBe(0);
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([id]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a duplicate live image event is coalesced while its first send is settling", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-duplicate-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "duplicate-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	const requests: Array<{ verb: string; params: any }> = [];
	let sends = 0;
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			requests,
			{},
			async (verb) => {
				if (verb !== "chat.send") return { engaged: false, turnId: null };
				sends++;
				if (sends === 1) throw new Error("temporary send failure");
				return { engaged: false, turnId: null };
			},
		);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		const first = adapter.processImage(message as never, engagement!);
		const waiter = adapter.waitForImage(message as never, first);
		expect(await bounded(first)).toBe("unavailable");
		adapter.adoptClient({
			request: async (verb: string, params: unknown) => {
				requests.push({ verb, params });
				// The first status poll of the database identity is additive, not a send.
				if (verb === "chat.send") sends++;
				return { engaged: false, turnId: null };
			},
		} as never);
		expect(adapter.imageInFlight(id)).toBe(true);
		expect(["unavailable", "acked"]).toContain(await adapter.processImage(message as never, engagement!));
		await bounded(waiter);
		expect(sends).toBe(2);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))).toEqual([
			{ messageId: id, channelId, completed: true, completedAt: expect.any(Number), declined: true, dm: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a request older than gateway dedupe retention is not replayed", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-dedupe-age-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "old-dm";
	const id = snowflakeFromTimestamp(Date.now() - 31 * 24 * 60 * 60 * 1000);
	const request = {
		messageId: id,
		text: "describe this\n[image · photo.png]",
		images: [image],
		origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
		engagement: { mentioned: false, group: false, authorId: "human" },
	};
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(
			outboxPath,
			JSON.stringify([{ messageId: id, channelId, request, createdAt: Date.now() - 31 * 24 * 60 * 60 * 1000 }]),
		);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect(adapter.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(adapter.deadLetters[0]!.summary).toContain("dedupe retention");
		// The bootstrap watermark passed this id, so the terminal evidence compacted safely.
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("an old source is rejected even when its outbox row was stamped recently", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-source-age-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "source-age-dm";
	const id = snowflakeFromTimestamp(Date.now() - 31 * 24 * 60 * 60 * 1000);
	const request = {
		messageId: id,
		text: "describe this\n[image · photo.png]",
		images: [image],
		origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
		engagement: { mentioned: false, group: false, authorId: "human" },
	};
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(outboxPath, JSON.stringify([{ messageId: id, channelId, request, createdAt: Date.now() }]));
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), requests);
		await bounded(adapter.recoverMissedMessages());
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect(adapter.deadLetters[0]?.summary).toContain("dedupe retention");
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("an unknown database identity holds recovery once a generation was adopted", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-identity-unknown-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "identity-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	const requests: Array<{ verb: string; params: any }> = [];
	let fetches = 0;
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			gatewayDbIdentity: "known-db",
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		const adapter = gateway(
			cursorPath,
			async () => {
				fetches++;
				return { messages: { fetch: async () => [message] } };
			},
			requests,
			{},
			async (verb) => (verb === "gateway.status" ? { database: {} } : { engaged: false, turnId: null }),
		);
		await bounded(adapter.recoverMissedMessages());
		expect(fetches).toBe(0);
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a deleted source invalidates a pending exact-request replay", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-source-delete-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "delete-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const request = {
		messageId: id,
		text: "describe this\n[image · photo.png]",
		images: [image],
		origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
		engagement: { mentioned: false, group: false, authorId: "human" },
	};
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await writeFile(outboxPath, JSON.stringify([{ messageId: id, channelId, request, dm: true }]));
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), []);
		await adapter.invalidateImageSource(id, "image source message was deleted");
		const persisted = JSON.parse(await readFile(outboxPath, "utf8"));
		expect(persisted[0]).toMatchObject({ messageId: id, terminal: true });
		expect(adapter.deadLetters[0]?.summary).toContain("deleted");
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("completed image evidence compacts only below the recovery watermark", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-completed-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "completed-dm";
	const watermark = snowflakeFromTimestamp(Date.now() - 60_000);
	const below = snowflakeFromTimestamp(Date.now() - 120_000);
	const above = snowflakeFromTimestamp(Date.now() + 60_000);
	const row = (messageId: string, completedAt: number) => ({ messageId, channelId, completed: true, completedAt });
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: watermark },
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(
			outboxPath,
			JSON.stringify([row(below, Date.now() - 8 * 24 * 60 * 60 * 1000), row(above, Date.now())]),
		);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), []);
		await bounded(adapter.recoverMissedMessages());
		expect(
			JSON.parse(await readFile(outboxPath, "utf8")).map((entry: { messageId: string }) => entry.messageId),
		).toEqual([above]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a persistent pre-intent outbox failure releases ingress after bounded attempts", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-unwritable-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "unwritable-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await mkdir(outboxPath);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), requests);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		const first = adapter.processImage(message as never, engagement!);
		const waiter = adapter.waitForImage(message as never, first);
		expect(await bounded(first)).toBe("unavailable");
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("unavailable");
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("skip");
		expect(requests).toEqual([]);
		expect(adapter.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(adapter.deadLetters[0]!.summary).toContain("3 admission attempts");
		await bounded(waiter, 2_000);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("recovered admission failures never take the live terminal release", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-recovered-admission-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "recovered-dm";
	const base = 1571558400000000000n;
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await writeFile(
			outboxPath,
			JSON.stringify(
				Array.from({ length: 1_000 }, (_, index) => ({
					messageId: String(base + BigInt(index)),
					channelId,
				})),
			),
		);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), requests);
		const id = String(base + 10_000n);
		const message = dmMessage(id, channelId, true);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		expect(await bounded(adapter.processImage(message as never, engagement!, undefined, undefined, true))).toBe(
			"unavailable",
		);
		expect(adapter.deadLetters).toEqual([]);
		expect(requests).toEqual([]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("recovered pre-intent outbox failures hold progress without dead-lettering", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-recovered-io-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "recovered-io-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await mkdir(outboxPath);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), []);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		for (let attempt = 0; attempt < 5; attempt++)
			expect(await bounded(adapter.processImage(message as never, engagement!, undefined, undefined, true))).toBe(
				"unavailable",
			);
		expect(adapter.deadLetters).toEqual([]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a history join inherits a capacity hold from the live owner", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-join-capacity-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "join-capacity-dm";
	const base = 1571558400000000000n;
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await writeFile(
			outboxPath,
			JSON.stringify(
				Array.from({ length: 1_000 }, (_, index) => ({
					messageId: String(base + BigInt(index)),
					channelId,
				})),
			),
		);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), requests);
		const id = String(base + 10_000n);
		const message = dmMessage(id, channelId, true);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		const live = adapter.processImage(message as never, engagement!);
		const waiter = adapter.waitForImage(message as never, live);
		const history = adapter.processImage(message as never, engagement!, undefined, undefined, true);
		expect(await bounded(live)).toBe("unavailable");
		expect(await bounded(history)).toBe("unavailable");
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("unavailable");
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("skip");
		await bounded(waiter, 2_000);
		expect(requests).toEqual([]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a replaced gateway database invalidates completion tombstones", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-restore-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "restore-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 60_000) },
			gatewayDbIdentity: "0:0",
		});
		await writeFile(
			outboxPath,
			JSON.stringify([{ messageId: id, channelId, completed: true, completedAt: Date.now() }]),
		);
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			[],
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "new-db" } } : { engaged: false, turnId: null },
		);
		await bounded(adapter.recoverMissedMessages());
		await adapter.cursorsFlushed;
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([]);
		const cursors = await loadRecoveryCursors(cursorPath);
		expect(cursors.gatewayDbIdentity).toBe("new-db");
		const watermark = cursors.recoveredThrough[channelId] ?? cursors.quarantined[channelId]?.watermark;
		expect(watermark).toBeDefined();
		expect(BigInt(watermark!)).toBeLessThan(BigInt(snowflakeFromTimestamp(Date.now() - 29 * 24 * 60 * 60 * 1000)));
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("live completion compacts eligible evidence without a recovery pass", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-live-compact-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "live-compact-dm";
	const old = snowflakeFromTimestamp(Date.now() - 8 * 24 * 60 * 60 * 1000);
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 7 * 24 * 60 * 60 * 1000) },
			knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
			sequence: 1,
		});
		await writeFile(
			outboxPath,
			JSON.stringify([
				{ messageId: old, channelId, completed: true, completedAt: Date.now() - 8 * 24 * 60 * 60 * 1000 },
			]),
		);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), []);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("acked");
		await delay(20);
		const rows = JSON.parse(await readFile(outboxPath, "utf8")) as Array<{ messageId: string }>;
		expect(rows.map((row) => row.messageId)).not.toContain(old);
		expect(rows.map((row) => row.messageId)).toContain(id);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a declined image is marked reclaimable evidence instead of completion quota", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-declined-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	// A bot-visible channel that is never configured for recovery: its row has no
	// watermark to compact against, so a quota-charged tombstone would accumulate
	// until it blocked images in every other channel.
	const channelId = "bot-visible-guild-channel";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = {
		id,
		content: "look at this",
		author: { id: "human", username: "human", bot: false },
		channel: { id: channelId, name: "general" },
		attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }],
	};
	try {
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), [], {});
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("acked");
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([
			{
				messageId: id,
				channelId,
				completed: true,
				completedAt: expect.any(Number),
				declined: true,
			},
		]);
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
	const id = snowflakeFromTimestamp(Date.now() - 25 * 60 * 60 * 1000);
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
			{ messageId: id, channelId, completed: true, completedAt: expect.any(Number), declined: true, dm: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a fresh durable image waits for history when the recovery cursor is unreadable", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-cursor-read-fresh-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "fresh-dm";
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
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))[0].messageId).toBe(id);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("an admitted source identity is retained across a restart before request persistence", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-source-retain-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "retained-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await writeFile(
			join(home, "image-outbox.json"),
			JSON.stringify([
				{ messageId: id, channelId, dm: true, sourceIdentity: "attachment-old", sourceText: "describe this" },
			]),
		);
		const edited = dmMessage(id, channelId, true);
		edited.content = "describe this differently";
		const replacement: any = {
			name: "photo.png",
			contentType: "image/png",
			url: "https://cdn.discordapp.com/photo.png",
			id: "attachment-new",
		};
		edited.attachments = [replacement];
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), requests);
		const engagement = decideInbound(edited as never, bot, {});
		expect(engagement).toBeDefined();
		expect(await bounded(adapter.processImage(edited as never, engagement!))).toBe("skip");
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect(JSON.parse(await readFile(join(home, "image-outbox.json"), "utf8"))[0].terminal).toBe(true);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a budget-refused live image releases ingress while retrying", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-budget-release-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "budget-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmMessage(id, channelId, true);
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await writeFile(outboxPath, JSON.stringify([{ messageId: id, channelId, dm: true }]));
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => message } }),
			[],
			{},
			undefined,
			async () => {
				throw new ImagePreparationBudgetError();
			},
		);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		const first = adapter.processImage(message as never, engagement!);
		const waiter = adapter.waitForImage(message as never, first);
		await bounded(waiter, 1_500);
		expect(await bounded(first)).toBe("unavailable");
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
	const below = snowflakeFromTimestamp(Date.now() - 8 * 24 * 60 * 60 * 1000);
	const above = snowflakeFromTimestamp(Date.now() + 60_000);
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 7 * 24 * 60 * 60 * 1000) },
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

test("a gateway-unavailable image intent records its failure clock and expires", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-noclient-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "new-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		const request = {
			messageId: id,
			text: "look\n[image · photo.png]",
			origin: { platform: "discord", kind: "dm", conversationId: channelId, peerId: "human" },
			engagement: { mentioned: false, group: false, authorId: "human" },
			images: [image],
		};
		const outboxPath = join(home, "image-outbox.json");
		await writeFile(outboxPath, JSON.stringify([{ messageId: id, channelId, request, dm: true }]));
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			requests,
			{},
			undefined,
			undefined,
			false,
		);
		const message = dmMessage(id, channelId, true);
		const engagement = decideInbound(message as never, bot, {});
		expect(engagement).toBeDefined();
		expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("unavailable");
		const afterFailure = JSON.parse(await readFile(outboxPath, "utf8"));
		expect(afterFailure[0].failureAt).toEqual(expect.any(Number));
		await writeFile(outboxPath, JSON.stringify([{ ...afterFailure[0], failureAt: Date.now() - 25 * 60 * 60 * 1000 }]));
		const restarted = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			requests,
			{},
			undefined,
			undefined,
			false,
		);
		expect(await bounded(restarted.processImage(message as never, engagement!))).toBe("skip");
		expect(requests.filter((entry) => entry.verb === "chat.send")).toEqual([]);
		expect(restarted.deadLetters.map((entry) => entry.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([
			{ messageId: id, channelId, terminal: true, audited: true, dm: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a retried image adopts its persisted thread destination", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-origin-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "origin-guild";
	const threadOrigin = {
		platform: "discord",
		kind: "thread",
		conversationId: "thread-1",
		parentId: channelId,
	};
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const inbound = {
		id,
		content: "describe this",
		author: { id: "human", username: "human", bot: false },
		channel: { id: channelId, name: "general" },
		attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }],
	};
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await writeFile(
			outboxPath,
			JSON.stringify([{ messageId: id, channelId, origin: threadOrigin, createdAt: Date.now(), live: true }]),
		);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [inbound] } }), requests, {
			[channelId]: { engagement: "open" },
		});
		await bounded(adapter.recoverMissedMessages());
		const send = requests.find((entry) => entry.verb === "chat.send");
		expect(send?.params.origin).toMatchObject({ kind: "thread", conversationId: "thread-1" });
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("completed DM tombstones are rebound into recovery targets before invalidation", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-restore-dm-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { other: snowflakeFromTimestamp(Date.now() - 60_000) },
			gatewayDbIdentity: "0:0",
		});
		await writeFile(
			outboxPath,
			JSON.stringify([
				{
					messageId: snowflakeFromTimestamp(Date.now() - 60_000),
					channelId: "lost-dm",
					dm: true,
					completed: true,
					completedAt: Date.now(),
				},
			]),
		);
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			[],
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "new-db" } } : { engaged: false, turnId: null },
		);
		await bounded(adapter.recoverMissedMessages());
		await adapter.cursorsFlushed;
		expect((await loadRecoveryCursors(cursorPath)).knownDms["lost-dm"]).toBeDefined();
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
});

test("completed rows outside every recovery target are reclaimed", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-reclaim-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const orphanId = snowflakeFromTimestamp(Date.now() - 60_000);
	const durableId = snowflakeFromTimestamp(Date.now() - 120_000);
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { other: snowflakeFromTimestamp(Date.now() - 60_000) },
			gatewayDbIdentity: "0:0",
		});
		await writeFile(
			outboxPath,
			JSON.stringify([
				{
					messageId: orphanId,
					channelId: "unconfigured-orphan",
					completed: true,
					completedAt: Date.now(),
				},
				{
					// Durable scope: this channel has a watermark, so a later pass can read
					// it even though neither config nor knownDms names it.
					messageId: durableId,
					channelId: "other",
					completed: true,
					completedAt: Date.now() - 60_000,
				},
			]),
		);
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			[],
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "0:0" } } : { engaged: false, turnId: null },
		);
		await bounded(adapter.recoverMissedMessages());
		await adapter.cursorsFlushed;
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([
			{
				messageId: durableId,
				channelId: "other",
				completed: true,
				completedAt: expect.any(Number),
			},
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a restore never displaces recent live known DMs with completed tombstones", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-restore-cap-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const recent = new Date(Date.now() - 60_000).toISOString();
	const knownDms = Object.fromEntries(
		Array.from({ length: 100 }, (_, index) => [`dm-${index}`, { lastSeenAt: recent, seq: index + 1 }]),
	);
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { other: snowflakeFromTimestamp(Date.now() - 60_000) },
			knownDms,
			sequence: 100,
			gatewayDbIdentity: "0:0",
		});
		await writeFile(
			outboxPath,
			JSON.stringify([
				{
					messageId: snowflakeFromTimestamp(Date.now() - 24 * 60 * 60 * 1000),
					channelId: "dm-a",
					dm: true,
					completed: true,
					completedAt: Date.now() - 24 * 60 * 60 * 1000,
				},
				{
					messageId: snowflakeFromTimestamp(Date.now() - 48 * 60 * 60 * 1000),
					channelId: "dm-b",
					dm: true,
					completed: true,
					completedAt: Date.now() - 48 * 60 * 60 * 1000,
				},
			]),
		);
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			[],
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "new-db" } } : { engaged: false, turnId: null },
		);
		await bounded(adapter.recoverMissedMessages());
		await adapter.cursorsFlushed;
		const cursors = await loadRecoveryCursors(cursorPath);
		expect(Object.keys(cursors.knownDms)).toHaveLength(100);
		expect(cursors.knownDms["dm-0"]).toBeDefined();
		expect(cursors.knownDms["dm-a"]).toBeUndefined();
		expect(cursors.knownDms["dm-b"]).toBeUndefined();
		expect(cursors.sequence).toBeGreaterThanOrEqual(100);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("incomplete thread discovery never reclaims completed evidence", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-incomplete-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const parent = {
		messages: { fetch: async () => [] },
		threads: {
			fetchActive: async () => {
				throw new Error("temporary thread discovery failure");
			},
			fetchArchived: async () => ({ threads: [], hasMore: false }),
		},
	};
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { "channel-1": snowflakeFromTimestamp(Date.now() - 60_000) },
			gatewayDbIdentity: "0:0",
		});
		await writeFile(
			outboxPath,
			JSON.stringify([
				{
					messageId: snowflakeFromTimestamp(Date.now() - 60_000),
					channelId: "orphan-thread",
					completed: true,
					completedAt: Date.now(),
				},
			]),
		);
		const adapter = gateway(
			cursorPath,
			async () => parent,
			[],
			{ "channel-1": { engagement: "open" } },
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "0:0" } } : { engaged: false, turnId: null },
		);
		await bounded(adapter.recoverMissedMessages());
		await adapter.cursorsFlushed;
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toHaveLength(1);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a restore keeps the newest completed DM tombstones when one slot is free", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-restore-recency-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const recent = new Date(Date.now() - 60_000).toISOString();
	const knownDms = Object.fromEntries(
		Array.from({ length: 99 }, (_, index) => [`dm-${index}`, { lastSeenAt: recent, seq: index + 1 }]),
	);
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { other: snowflakeFromTimestamp(Date.now() - 60_000) },
			knownDms,
			sequence: 99,
			gatewayDbIdentity: "0:0",
		});
		await writeFile(
			outboxPath,
			JSON.stringify([
				{
					messageId: snowflakeFromTimestamp(Date.now() - 24 * 60 * 60 * 1000),
					channelId: "dm-a",
					dm: true,
					completed: true,
					completedAt: Date.now() - 24 * 60 * 60 * 1000,
				},
				{
					messageId: snowflakeFromTimestamp(Date.now() - 48 * 60 * 60 * 1000),
					channelId: "dm-b",
					dm: true,
					completed: true,
					completedAt: Date.now() - 48 * 60 * 60 * 1000,
				},
			]),
		);
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			[],
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "new-db" } } : { engaged: false, turnId: null },
		);
		await bounded(adapter.recoverMissedMessages());
		await adapter.cursorsFlushed;
		const cursors = await loadRecoveryCursors(cursorPath);
		expect(Object.keys(cursors.knownDms)).toHaveLength(100);
		expect(cursors.knownDms["dm-0"]).toBeDefined();
		// The newer completion holds the only free slot; the older one is behind it.
		expect(cursors.knownDms["dm-a"]).toBeDefined();
		expect(cursors.knownDms["dm-b"]).toBeUndefined();
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("persistent partial thread discovery stops driving retries but never reclaims", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-partial-bounded-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const parent = {
		messages: { fetch: async () => [] },
		threads: {
			fetchActive: async () => {
				throw new Error("temporary thread discovery failure");
			},
			fetchArchived: async () => ({ threads: [], hasMore: false }),
		},
	};
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { "channel-1": snowflakeFromTimestamp(Date.now() - 60_000) },
			gatewayDbIdentity: "0:0",
		});
		await writeFile(
			outboxPath,
			JSON.stringify([
				{
					messageId: snowflakeFromTimestamp(Date.now() - 60_000),
					channelId: "orphan-thread",
					completed: true,
					completedAt: Date.now(),
				},
			]),
		);
		const adapter = gateway(
			cursorPath,
			async () => parent,
			[],
			{ "channel-1": { engagement: "open" } },
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "0:0" } } : { engaged: false, turnId: null },
		);
		const warnings: string[] = [];
		const originalWarn = console.warn;
		console.warn = (...args: unknown[]) => {
			warnings.push(args.map(String).join(" "));
		};
		try {
			// A retry timer from an earlier pass may add a pass of its own; four awaited
			// passes guarantee the bounded attempt count is reached either way.
			for (let pass = 0; pass < 4; pass++) {
				await bounded(adapter.recoverMissedMessages());
				await adapter.cursorsFlushed;
			}
		} finally {
			console.warn = originalWarn;
		}
		expect(warnings.some((line) => line.includes("quarantined partial thread discovery"))).toBe(true);
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toHaveLength(1);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a failed cursor checkpoint preserves completed DM evidence until restore can retry", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-restore-durable-dm-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const cursorTemp = `${cursorPath}.${process.pid}.tmp`;
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const row = { messageId: id, channelId: "lost-dm", dm: true, completed: true, completedAt: Date.now() };
	try {
		await saveRecoveryCursors(cursorPath, { ...emptyCursors(), gatewayDbIdentity: "old-db" });
		await writeFile(outboxPath, JSON.stringify([row]));
		await mkdir(cursorTemp);
		const requests: Array<{ verb: string; params: any }> = [];
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			requests,
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "new-db" } } : { engaged: false, turnId: null },
		);
		await bounded(adapter.recoverMissedMessages());
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([row]);
		expect((await loadRecoveryCursors(cursorPath)).knownDms["lost-dm"]).toBeUndefined();
		expect(adapter.recoveryRetryPending).toBe(true);

		await rm(cursorTemp, { recursive: true });
		const restarted = gateway(
			cursorPath,
			async () => ({
				messages: {
					fetch: async (query: string | { after?: string }) =>
						typeof query === "string"
							? dmMessage(id, "lost-dm", true)
							: !query.after || BigInt(query.after) < BigInt(id)
								? [dmMessage(id, "lost-dm", true)]
								: [],
				},
			}),
			requests,
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "new-db" } } : { engaged: false, turnId: null },
		);
		await bounded(restarted.recoverMissedMessages());
		expect((await loadRecoveryCursors(cursorPath)).knownDms["lost-dm"]).toBeDefined();
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([id]);
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toMatchObject([
			{ messageId: id, dm: true, completed: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
});

test("restore preserves and visits DM targets beyond the known-DM cap", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-restore-dm-overflow-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const now = Date.now();
	const rows = Array.from({ length: 101 }, (_, index) => ({
		messageId: snowflakeFromTimestamp(now - 60_000 + index),
		channelId: `dm-${index}`,
		dm: true,
		completed: true,
		completedAt: now,
	}));
	const fetched = new Set<string>();
	try {
		await saveRecoveryCursors(cursorPath, { ...emptyCursors(), gatewayDbIdentity: "old-db" });
		await writeFile(outboxPath, JSON.stringify(rows));
		const adapter = gateway(
			cursorPath,
			async (id) => {
				fetched.add(id);
				throw new Error("temporarily unavailable");
			},
			[],
			{},
			async (verb) => (verb === "gateway.status" ? { database: { identity: "new-db" } } : {}),
		);
		await bounded(adapter.recoverMissedMessages(), 30_000);
		const cursors = await loadRecoveryCursors(cursorPath);
		expect(Object.keys(cursors.knownDms)).toHaveLength(100);
		expect(fetched.size).toBe(101);
		const pending = JSON.parse(await readFile(outboxPath, "utf8")) as Array<{ channelId: string; completed?: boolean }>;
		expect(new Set(pending.map((entry) => entry.channelId))).toEqual(new Set(rows.map((entry) => entry.channelId)));
		expect(pending.every((entry) => !entry.completed)).toBe(true);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 35_000);

test("audited terminal rows in unconfigured channels get a recovery watermark and are reclaimed", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-terminal-orphan-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "unconfigured-channel";
	const id = snowflakeFromTimestamp(Date.now() - 8 * 24 * 60 * 60 * 1000);
	const message = { ...dmMessage(id, channelId, true), channel: { id: channelId } };
	let channelFetches = 0;
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 9 * 24 * 60 * 60 * 1000) },
		});
		await writeFile(outboxPath, JSON.stringify([{ messageId: id, channelId, terminal: true, audited: true }]));
		const adapter = gateway(
			cursorPath,
			async () => {
				channelFetches++;
				return {
					messages: {
						fetch: async ({ after }: { after?: string }) => (!after || BigInt(after) < BigInt(id) ? [message] : []),
					},
				};
			},
			requests,
		);
		await bounded(adapter.recoverMissedMessages());
		expect(channelFetches).toBe(1);
		expect(requests.filter((entry) => entry.verb !== "gateway.status")).toEqual([]);
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toEqual([]);
		const cursors = await loadRecoveryCursors(cursorPath);
		expect(cursors.recoveredThrough[channelId] ?? cursors.quarantined[channelId]?.watermark).toBe(id);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
});

test("an edited caption or replaced attachment is not mixed with refreshed image bytes", async () => {
	for (const edit of ["caption", "attachment"] as const) {
		const home = await mkdtemp(join(tmpdir(), "discord-image-refresh-snapshot-"));
		const cleanupTimers = trackTimers();
		const cursorPath = join(home, "recovery-cursor.json");
		const channelId = "snapshot-dm";
		const id = snowflakeFromTimestamp(Date.now() - 60_000);
		const message = dmMessage(id, channelId, true);
		const fresh =
			edit === "caption"
				? { ...message, content: "edited caption" }
				: {
						...message,
						attachments: [
							{ name: "replacement.png", contentType: "image/png", url: "https://cdn.discordapp.com/new.png" },
						],
					};
		let preparations = 0;
		const requests: Array<{ verb: string; params: any }> = [];
		try {
			await saveRecoveryCursors(cursorPath, emptyCursors());
			const adapter = gateway(
				cursorPath,
				async () => ({ messages: { fetch: async () => fresh } }),
				requests,
				{},
				undefined,
				async () => {
					preparations++;
					if (preparations === 1) throw new DiscordImagePreparationError("retryable", undefined, true);
					return { images: [image], notice: "" };
				},
			);
			const engagement = decideInbound(message as never, bot, {});
			expect(engagement).toBeDefined();
			expect(await bounded(adapter.processImage(message as never, engagement!))).toBe("skip");
			expect(preparations).toBe(1);
			expect(requests).toEqual([]);
			expect(adapter.deadLetters.map((entry) => entry.summary)).toContain(
				"image source changed after the message was edited",
			);
		} finally {
			cleanupTimers();
			await rm(home, { recursive: true, force: true });
		}
	}
});

test("completed DM tombstones do not refresh recovery recency", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-recency-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	try {
		await saveRecoveryCursors(cursorPath, emptyCursors());
		await writeFile(
			outboxPath,
			JSON.stringify([{ messageId: id, channelId: "old-dm", completed: true, completedAt: Date.now(), dm: true }]),
		);
		const adapter = gateway(cursorPath, async () => ({ messages: { fetch: async () => [] } }), []);
		await bounded(adapter.recoverMissedMessages());
		expect((await loadRecoveryCursors(cursorPath)).knownDms["old-dm"]).toBeUndefined();
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("legacy populated cursors rewind on the first gateway identity", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-order-legacy-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const channelId = "legacy-dm";
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 60_000) },
		});
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			[],
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "first-db" } } : { engaged: false, turnId: null },
		);
		await bounded(adapter.recoverMissedMessages());
		const cursors = await loadRecoveryCursors(cursorPath);
		const watermark = cursors.recoveredThrough[channelId] ?? cursors.quarantined[channelId]?.watermark;
		expect(watermark).toBeDefined();
		expect(BigInt(watermark!)).toBeLessThan(BigInt(snowflakeFromTimestamp(Date.now() - 29 * 24 * 60 * 60 * 1000)));
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a restored DM replay never evicts a live known DM", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-restore-replay-cap-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const recent = new Date(Date.now() - 60_000).toISOString();
	const completedAt = Date.now() - 60 * 60 * 1000;
	const row = {
		messageId: snowflakeFromTimestamp(completedAt),
		channelId: "dm-a",
		dm: true,
		completed: true,
		completedAt,
	};
	const message = dmMessage(row.messageId, "dm-a", true);
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			knownDms: Object.fromEntries(
				Array.from({ length: 100 }, (_, index) => [`dm-${index}`, { lastSeenAt: recent, seq: index + 1 }]),
			),
			sequence: 100,
			gatewayDbIdentity: "0:0",
		});
		await writeFile(outboxPath, JSON.stringify([row]));
		const adapter = gateway(
			cursorPath,
			async (id) =>
				id === "dm-a"
					? {
							messages: {
								fetch: async (query: unknown) => (typeof query === "string" ? message : [message]),
							},
						}
					: { messages: { fetch: async () => [] } },
			requests,
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "new-db" } } : { engaged: true, turnId: "turn-1" },
		);
		await bounded(adapter.recoverMissedMessages());
		await adapter.cursorsFlushed;
		const cursors = await loadRecoveryCursors(cursorPath);
		expect(Object.keys(cursors.knownDms)).toHaveLength(100);
		expect(cursors.knownDms["dm-0"]).toBeDefined();
		// The restored target replays from the outbox row, never from a promoted slot.
		expect(cursors.knownDms["dm-a"]).toBeUndefined();
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([
			row.messageId,
		]);
		expect(JSON.parse(await readFile(outboxPath, "utf8"))).toMatchObject([
			{ messageId: row.messageId, completed: true, dm: true },
		]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 12_000);

test("a request-bearing restored DM retry never evicts a live DM", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-image-restore-replay-retry-"));
	const cleanupTimers = trackTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const recent = new Date(Date.now() - 60_000).toISOString();
	const completedAt = Date.now() - 60 * 60 * 1000;
	const id = snowflakeFromTimestamp(completedAt);
	const request = {
		messageId: id,
		text: "describe this\n[image · photo.png]",
		images: [image],
		origin: { platform: "discord", kind: "dm", conversationId: "dm-a", peerId: "human" },
		engagement: { mentioned: false, group: false, authorId: "human" },
	};
	const requests: Array<{ verb: string; params: any }> = [];
	try {
		await saveRecoveryCursors(cursorPath, {
			...emptyCursors(),
			// The watermark is past the intent, so the request retry runs instead of
			// deferring behind a history pass.
			recoveredThrough: { "dm-a": snowflakeFromTimestamp(Date.now()) },
			knownDms: Object.fromEntries(
				Array.from({ length: 100 }, (_, index) => [`dm-${index}`, { lastSeenAt: recent, seq: index + 1 }]),
			),
			sequence: 100,
			gatewayDbIdentity: "0:0",
		});
		// The state left after a restored replay prepared its message and the first send
		// failed: the persisted request still carries the restore marker.
		await writeFile(
			outboxPath,
			JSON.stringify([{ messageId: id, channelId: "dm-a", dm: true, restored: true, request, failureAt: Date.now() }]),
		);
		const adapter = gateway(
			cursorPath,
			async () => ({ messages: { fetch: async () => [] } }),
			requests,
			{},
			async (verb) =>
				verb === "gateway.status" ? { database: { identity: "0:0" } } : { engaged: true, turnId: "turn-1" },
		);
		await bounded(adapter.recoverMissedMessages());
		await adapter.cursorsFlushed;
		const cursors = await loadRecoveryCursors(cursorPath);
		expect(Object.keys(cursors.knownDms)).toHaveLength(100);
		expect(cursors.knownDms["dm-0"]).toBeDefined();
		expect(cursors.knownDms["dm-a"]).toBeUndefined();
		expect(requests.filter((entry) => entry.verb === "chat.send").map((entry) => entry.params.messageId)).toEqual([id]);
	} finally {
		cleanupTimers();
		await rm(home, { recursive: true, force: true });
	}
}, 20_000);
