import { expect, spyOn, test } from "bun:test";
import { Buffer } from "node:buffer";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { GajaewayClient } from "@gajae-gateway/sdk";
import { Client } from "discord.js";
import { DiscordImagePreparationError } from "../src/attachments";
import { ImageOutbox, type PendingImage } from "../src/image-outbox";
import { engagementForMessage, type ImageOutcome, ReconnectingGateway, startDiscordAdapter } from "../src/main";
import {
	RECOVERY_MAX_ATTEMPTS,
	RECOVERY_RETRY_BASE_MS,
	RECOVERY_RETRY_MAX_MS,
	saveRecoveryCursors,
	snowflakeFromTimestamp,
} from "../src/recovery";

// The same valid PNG fixture used by the other adapter image tests.
/** discord.js accepts a bare id or `{ message, force }`; the fake must accept both. */
const singleFetchId = (query: unknown): string | undefined =>
	typeof query === "string" ? query : (query as { message?: string } | undefined)?.message;
const historyAfter = (query: unknown): string | undefined =>
	typeof query === "string" ? undefined : (query as { after?: string } | undefined)?.after;

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

/** Every delay `scheduleImageRetry`/`scheduleRecoveryRetry` can arm. */
const RETRY_BACKOFF_MS = new Set(
	Array.from({ length: 7 }, (_, index) => Math.min(RECOVERY_RETRY_MAX_MS, RECOVERY_RETRY_BASE_MS * 2 ** index)),
);

/**
 * Hold the timers the adapter arms with the recovery backoff so the test fires the armed
 * retry path itself. Every backoff-schedule timer is cleared and cancelled again at
 * teardown, so neither a wall-clock delay nor a leaked pass can decide the assertion.
 * Sleep on these paths is injected, so nothing awaited is held.
 */
function holdRetryTimers() {
	const realSetTimeout = globalThis.setTimeout;
	const handles: ReturnType<typeof setTimeout>[] = [];
	const armed: Array<{ delay: number; fire: () => void }> = [];
	const spy = spyOn(globalThis, "setTimeout").mockImplementation(((...args: Parameters<typeof setTimeout>) => {
		const timer = realSetTimeout(...args);
		timer.unref?.();
		const delay = typeof args[1] === "number" ? args[1] : 0;
		// Only the recovery backoff schedule is held; other product timers keep their
		// schedule, and the runner's own test timeout stays intact.
		if (!RETRY_BACKOFF_MS.has(delay)) return timer;
		handles.push(timer);
		clearTimeout(timer);
		armed.push({
			delay,
			fire: () => (args[0] as unknown as (...rest: unknown[]) => void)(...args.slice(2)),
		});
		return timer;
	}) as typeof setTimeout);
	return {
		/** One entry per retry timer armed at the backoff floor (the first re-arm). */
		retries: () => armed.filter((entry) => entry.delay === RECOVERY_RETRY_BASE_MS),
		cleanup() {
			for (const handle of handles) clearTimeout(handle);
			spy.mockRestore();
		},
	};
}

/** Capture every recovery pass this adapter starts so a fired retry can be joined. */
function trackRecoveryPasses(adapter: ReconnectingGateway) {
	const passes: Array<Promise<void>> = [];
	const seam = adapter as unknown as { recoverMissedMessages(): Promise<void> };
	const real = seam.recoverMissedMessages.bind(adapter);
	seam.recoverMissedMessages = () => {
		const pass = real();
		passes.push(pass);
		return pass;
	};
	return {
		passes,
		restore() {
			seam.recoverMissedMessages = real;
		},
	};
}

/**
 * Capture the closure each scheduled retry will run, so its outcome can be awaited. The
 * fired timer invokes that closure synchronously before it yields, which makes the captured
 * promise the authoritative completion of the armed path.
 */
function trackScheduledRetries(adapter: ReconnectingGateway) {
	const outcomes = new Map<string, Promise<ImageOutcome>>();
	const seam = adapter as unknown as {
		scheduleImageRetry(id: string, channelId: string, retry: () => Promise<ImageOutcome>): void;
	};
	const real = seam.scheduleImageRetry.bind(adapter);
	seam.scheduleImageRetry = (id, channelId, retry) =>
		real(id, channelId, () => {
			const outcome = retry();
			outcomes.set(id, outcome);
			return outcome;
		});
	return {
		outcomes,
		restore() {
			seam.scheduleImageRetry = real;
		},
	};
}

async function bounded<T>(promise: Promise<T>, ms = 8_000): Promise<T> {
	return Promise.race([
		promise,
		delay(ms).then(() => {
			throw new Error("Timed out waiting for the retried image");
		}),
	]);
}

async function eventually(predicate: () => boolean, ms = 8_000) {
	const deadline = Date.now() + ms;
	while (!predicate() && Date.now() < deadline) await delay(25);
	expect(predicate()).toBe(true);
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

test("a same-process live image retry arms presence once when the timer pass takes over", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-live-image-takeover-"));
	const held = holdRetryTimers();
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
	const sent: string[] = [];
	let preparations = 0;
	const mentioned = {
		id: snowflakeFromTimestamp(Date.now() - 90_000),
		content: "<@bot> describe this",
		author: { id: "owner", username: "owner", bot: false },
		channel: { id: "channel-thread", name: "general" },
		attachments: [{ name: "photo.png", contentType: "image/png", url: "https://cdn.discordapp.com/photo.png" }],
		startThread: async () => ({ id: "thread-live" }),
	};
	const channel = {
		messages: {
			fetch: async (query: unknown) => {
				const single = singleFetchId(query);
				if (single !== undefined) return mentioned;
				const after = historyAfter(query);
				// The bootstrap window starts behind this mention; later pages are empty.
				return after === undefined || BigInt(after) < BigInt(mentioned.id) ? [mentioned] : [];
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
			channels: { "channel-thread": { engagement: "open" } },
		} as never,
		ports.typingPort as never,
		ports.statusPort as never,
		cursorPath,
		() => bot,
		{
			request: async (verb: string, params: unknown) => {
				if (verb === "gateway.status") return { database: { identity: "test-db" } };
				if (verb !== "chat.send") return { engaged: false, turnId: null };
				requests.push({ verb, params });
				sent.push((params as { messageId: string }).messageId);
				return { engaged: true, turnId: "fresh" };
			},
		} as never,
		async () => {},
		undefined,
		undefined,
		async () => {
			preparations++;
			if (preparations <= RECOVERY_MAX_ATTEMPTS) throw new DiscordImagePreparationError("retryable");
			return { images: [image], notice: "" };
		},
	);
	const passes = trackRecoveryPasses(gateway);
	const scheduled = trackScheduledRetries(gateway);
	try {
		// The live attempt burns its whole in-invocation budget before the timer runs.
		expect(await bounded(gateway.processImage(mentioned as never, engagementForMessage(mentioned as never, bot)))).toBe(
			"unavailable",
		);
		expect(preparations).toBe(RECOVERY_MAX_ATTEMPTS);
		expect(ports.typing).toEqual([]);
		expect(ports.armed).toEqual([]);
		const retries = held.retries();
		expect(retries).toHaveLength(1);

		retries[0].fire();
		await bounded(passes.passes[0]);
		// Recovery acquired the still-live row and prepared it successfully on the next
		// attempt; the engaged acknowledgement must decorate the live turn exactly once,
		// on the thread this mention created.
		expect(sent).toEqual([mentioned.id]);
		expect(ports.typing).toEqual(["thread-live"]);
		expect(ports.armed).toEqual([
			{ conversationId: "thread-live", messageId: mentioned.id, channelId: "channel-thread" },
		]);
		// The original owner's closure must not restore presence a second time.
		const overtaking = scheduled.outcomes.get(mentioned.id);
		if (overtaking) await bounded(overtaking);
		expect(ports.typing).toEqual(["thread-live"]);
		expect(ports.armed).toHaveLength(1);
	} finally {
		scheduled.restore();
		passes.restore();
		held.cleanup();
		await rm(home, { recursive: true, force: true });
	}
}, 15_000);

test("a duplicate gateway engagement without a turn does not arm live presence", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-live-image-duplicate-turn-"));
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
	const channel = { messages: { fetch: async () => [] } };
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
				// The gateway already owns a turn for this delivery: no new turn was opened.
				return { engaged: true, turnId: null };
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
		expect(requests.filter((entry) => entry.verb === "chat.send")).toHaveLength(1);
		expect(ports.typing).toEqual([]);
		expect(ports.armed).toEqual([]);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}, 15_000);

test("a historical image retry keeps recovered provenance and delivers silently once after history repair", async () => {
	const home = await mkdtemp(join(tmpdir(), "discord-live-image-history-provenance-"));
	const held = holdRetryTimers();
	const cursorPath = join(home, "recovery-cursor.json");
	const outboxPath = join(home, "image-outbox.json");
	const channelId = "new-dm";
	const id = snowflakeFromTimestamp(Date.now() - 60_000);
	const message = dmImageMessage(id);
	await saveRecoveryCursors(cursorPath, {
		// The watermark stays behind the image, so an independent retry must not send it
		// ahead of the sorted history pass that still owns the gap.
		recoveredThrough: { [channelId]: snowflakeFromTimestamp(Date.now() - 5 * 60_000) },
		quarantined: {},
		knownDms: { [channelId]: { lastSeenAt: new Date().toISOString(), seq: 1 } },
		attempts: {},
		deadLetters: [],
		deadLetterDigest: {},
		sequence: 1,
		gatewayDbIdentity: "test-db",
	});
	const ports = presencePorts();
	const requests: Array<{ verb: string; params: any }> = [];
	const sent: string[] = [];
	let historyReadable = true;
	const channel = {
		messages: {
			fetch: async (query: unknown) => {
				const single = singleFetchId(query);
				if (single !== undefined) return message;
				if (!historyReadable) throw Object.assign(new Error("history temporarily unavailable"), { code: "EAI_AGAIN" });
				const after = historyAfter(query);
				return after === undefined || BigInt(after) < BigInt(id) ? [message] : [];
			},
		},
	};
	const outbox = new ImageOutbox(outboxPath);
	const realPut = outbox.put.bind(outbox);
	// The history-discovered intent cannot be persisted before the timer runs, so its
	// only surviving provenance is the retry closure's captured livePresence.
	let putFailures = 1;
	outbox.put = async (entry: PendingImage) => {
		if (putFailures > 0) {
			putFailures--;
			throw Object.assign(new Error("EIO: i/o error, write"), { code: "EIO" });
		}
		return realPut(entry);
	};
	let preparations = 0;
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
				if (verb !== "chat.send") return { engaged: false, turnId: null };
				requests.push({ verb, params });
				sent.push((params as { messageId: string }).messageId);
				return { engaged: true, turnId: "fresh" };
			},
		} as never,
		async () => {},
		undefined,
		undefined,
		async () => {
			preparations++;
			return { images: [image], notice: "" };
		},
		outbox,
	);
	const passes = trackRecoveryPasses(gateway);
	const scheduled = trackScheduledRetries(gateway);
	try {
		// The history pass finds the image, but its first intent write fails before any
		// row exists; the owner can only arm its backoff retry.
		expect(await bounded(gateway.recoverMissedMessages())).toBeUndefined();
		await gateway.cursorsFlushed;
		expect(putFailures).toBe(0);
		expect(preparations).toBe(0);
		expect(sent).toEqual([]);
		expect(ports.typing).toEqual([]);
		expect(ports.armed).toEqual([]);
		// The failed image delivery armed its retry before the incomplete pass armed its
		// own recovery retry; only the image timer is fired here.
		const retries = held.retries();
		expect(retries).toHaveLength(2);
		const imageRetry = retries[0];
		expect(imageRetry.delay).toBe(RECOVERY_RETRY_BASE_MS);

		// The timer's preliminary recovery cannot read history, so it cannot re-admit the
		// image. The repaired journal lets the no-row fallback persist the intent, which
		// must stay above the horizon and out of the live-presence set.
		historyReadable = false;
		imageRetry.fire();
		await eventually(() => passes.passes.length >= 2);
		await bounded(passes.passes[1]);
		await eventually(() => scheduled.outcomes.has(id));
		expect(await bounded(scheduled.outcomes.get(id) as Promise<ImageOutcome>)).toBe("unavailable");
		const admitted = JSON.parse(await readFile(outboxPath, "utf8")) as Array<Record<string, unknown>>;
		expect(admitted).toEqual([expect.objectContaining({ messageId: id, channelId, request: expect.any(Object) })]);
		expect(admitted[0]?.live).toBeUndefined();
		expect(sent).toEqual([]);
		expect(ports.typing).toEqual([]);
		expect(ports.armed).toEqual([]);

		// History repairs: the durable intent is retried inside the ordered pass and the
		// engaged fresh turn is delivered exactly once without decorating the replay.
		historyReadable = true;
		expect(await bounded(gateway.recoverMissedMessages())).toBeUndefined();
		await gateway.cursorsFlushed;
		expect(sent).toEqual([id]);
		expect(requests.filter((entry) => entry.verb === "chat.send")).toHaveLength(1);
		expect(ports.typing).toEqual([]);
		expect(ports.armed).toEqual([]);
	} finally {
		scheduled.restore();
		passes.restore();
		held.cleanup();
		await rm(home, { recursive: true, force: true });
	}
}, 15_000);
