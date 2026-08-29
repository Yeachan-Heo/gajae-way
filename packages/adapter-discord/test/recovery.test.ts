import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { DiscordInboundMessage } from "../src/main";
import { decideInbound, LruSet, ReconnectingGateway } from "../src/main";
import {
	classifyRecoveryFailure,
	loadRecoveryCursors,
	RECOVERY_DEAD_LETTER_CAP,
	RECOVERY_MAX_ATTEMPTS,
	RECOVERY_MAX_PAGES,
	RECOVERY_PAGE_LIMIT,
	RECOVERY_RETRY_BASE_MS,
	RECOVERY_UNIFORM_FAILURE_LIMIT,
	type RecoverableChannel,
	type RecoveryCursorState,
	RecoveryGate,
	recordDeadLetter,
	recoverConversation,
	recoveryCursorPath,
	retainRecoveryCursors,
	saveRecoveryCursors,
	snowflakeFromTimestamp,
	snowflakeIsAfter,
} from "../src/recovery";

const bot = { id: "bot-9" };

/** Full cursor-store state from just its watermarks. */
function cursorState(recoveredThrough: Record<string, string>): RecoveryCursorState {
	return { recoveredThrough, quarantined: {}, attempts: {}, deadLetters: [], deadLetterDigest: {}, sequence: 0 };
}

/** Quarantined watermarks without their ordering metadata, for readable assertions. */
function watermarks(parked: Readonly<Record<string, { watermark: string }>>): Record<string, string> {
	return Object.fromEntries(Object.entries(parked).map(([id, entry]) => [id, entry.watermark]));
}

function message(id: string, overrides: Partial<DiscordInboundMessage> = {}): DiscordInboundMessage {
	return {
		id,
		content: `hello <@${bot.id}>`,
		author: { id: `author-${id}` },
		channel: { id: "channel-1", name: "general" },
		guild: { name: "guild" },
		mentions: { has: (user: unknown) => user === bot },
		...overrides,
	} as DiscordInboundMessage;
}

/** Fake Discord channel: paginates forward from `after`, ascending by id, like the REST API. */
function fakeChannel(
	all: DiscordInboundMessage[],
): RecoverableChannel & { fetches: Array<{ after?: string; limit: number }> } {
	const fetches: Array<{ after?: string; limit: number }> = [];
	return {
		fetches,
		messages: {
			async fetch(options) {
				fetches.push(options);
				const after = options.after ?? "0";
				return all
					.filter((m) => BigInt(m.id) > BigInt(after))
					.sort((a, b) => (BigInt(a.id) < BigInt(b.id) ? -1 : 1))
					.slice(0, options.limit);
			},
		},
	};
}

/** Fake gateway with the same durable contract as chat.send: dedupe by message id. */
function fakeGateway() {
	const sent: Array<{ messageId: string; engagement: unknown }> = [];
	const seen = new Set<string>();
	return {
		sent,
		send(messageId: string, engagement: unknown): "acked" | "duplicate" {
			if (seen.has(messageId)) return "duplicate";
			seen.add(messageId);
			sent.push({ messageId, engagement });
			return "acked";
		},
	};
}

/** Mirrors the live path: adapter LRU + decideInbound + gateway durable dedupe. */
function wiredDeliver(
	gateway: ReturnType<typeof fakeGateway>,
	lru: LruSet,
	channels?: Record<string, { engagement?: "open" }>,
) {
	return async (m: ReturnType<typeof message>) => {
		if (!lru.addIfAbsent(m.id)) return "duplicate";
		const engagement = decideInbound(m, bot, channels);
		if (!engagement) return "duplicate";
		return gateway.send(m.id, engagement);
	};
}

let home: string;
beforeAll(async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-discord-recovery-"));
});
afterAll(async () => {
	await rm(home, { recursive: true, force: true });
});

test("cursor path lives under the adapter home", () => {
	expect(recoveryCursorPath(home)).toBe(join(home, "adapters", "discord", "recovery-cursor.json"));
});

test("cursor store round-trips and ignores malformed state", async () => {
	const path = recoveryCursorPath(home);
	expect(await loadRecoveryCursors(join(home, "missing.json"))).toEqual(cursorState({}));
	await saveRecoveryCursors(path, cursorState({ "channel-1": "1000", bad: "x" }));
	const state = await loadRecoveryCursors(path);
	expect(state.recoveredThrough["channel-1"]).toBe("1000");
	expect(state.recoveredThrough.bad).toBeUndefined();
	await writeFile(path, "not json{", "utf8");
	expect(await loadRecoveryCursors(path)).toEqual(cursorState({}));
});

test("snowflake ordering and bootstrap cursor are time-correct", () => {
	expect(snowflakeIsAfter("2000", "1000")).toBe(true);
	expect(snowflakeIsAfter("1000", "2000")).toBe(false);
	const at = Date.UTC(2026, 7, 28, 3, 49, 13);
	const cursor = snowflakeFromTimestamp(at);
	expect(Number((BigInt(cursor) >> 22n) + 1_420_070_400_000n)).toBe(at);
});

test("non-engaged missed message is delivered exactly once and advances the cursor", async () => {
	const gateway = fakeGateway();
	const lru = new LruSet();
	const channel = fakeChannel([
		message("100", { content: "talking to <@other-bot> here", mentions: { has: () => false } }),
	]);
	const deliver = wiredDeliver(gateway, lru);
	const first = await recoverConversation(channel, { nowMs: 0, deliver });
	expect(first).toEqual({
		advancedTo: "100",
		delivered: 1,
		skipped: 0,
		discarded: 0,
		held: 0,
		truncated: false,
		failed: false,
	});
	const [send] = gateway.sent;
	expect(send.messageId).toBe("100");
	expect((send.engagement as { mentioned: boolean }).mentioned).toBe(false);
	// A second recovery run (e.g. restart with a stale in-memory LRU) must not re-deliver.
	const second = await recoverConversation(channel, { cursor: first.advancedTo, nowMs: 0, deliver });
	expect(second.delivered).toBe(0);
	expect(gateway.sent).toHaveLength(1);
});

test("self-addressed missed message creates exactly one turn when authorized", async () => {
	const gateway = fakeGateway();
	const lru = new LruSet();
	const channel = fakeChannel([message("200")]);
	const outcome = await recoverConversation(channel, { nowMs: 0, deliver: wiredDeliver(gateway, lru) });
	expect(outcome.delivered).toBe(1);
	expect((gateway.sent[0].engagement as { mentioned: boolean }).mentioned).toBe(true);
	expect(gateway.sent).toHaveLength(1);
});

test("other-bot mention stays context-only and never triggers engagement", async () => {
	const gateway = fakeGateway();
	const lru = new LruSet();
	const channel = fakeChannel([message("300", { content: "hey <@999> do a thing", mentions: { has: () => false } })]);
	await recoverConversation(channel, { nowMs: 0, deliver: wiredDeliver(gateway, lru) });
	expect((gateway.sent[0].engagement as { mentioned: boolean }).mentioned).toBe(false);
});

test("live/backfill race delivers exactly once by message id", async () => {
	const gateway = fakeGateway();
	const lru = new LruSet();
	const channel = fakeChannel([message("400")]);
	const deliver = wiredDeliver(gateway, lru, { "channel-1": { engagement: "open" } });
	// Live messageCreate wins the race; recovery replays the same id moments later.
	const engagement = decideInbound(message("400"), bot, { "channel-1": { engagement: "open" } });
	expect(gateway.send("400", engagement)).toBe("acked");
	const outcome = await recoverConversation(channel, { cursor: "0", nowMs: 0, deliver });
	expect(outcome.delivered).toBe(1);
	expect(gateway.sent).toHaveLength(1);
});

test("shared gate joins live and backfill attempts and retries a rejected send", async () => {
	const gate = new RecoveryGate(2);
	let attempts = 0;
	let reject = true;
	const attempt = async () => {
		attempts++;
		await Promise.resolve();
		if (reject) {
			reject = false;
			return "unavailable" as const;
		}
		return "acked" as const;
	};
	await Promise.all([gate.join("401", attempt), gate.join("401", attempt)]);
	expect(attempts).toBe(1);
	expect(gate.acked("401")).toBe(false);
	expect(await gate.join("401", attempt)).toBe("acked");
	expect(attempts).toBe(2);
	expect(await gate.join("401", attempt)).toBe("duplicate");
	expect(attempts).toBe(2);
});

test("interrupted recovery before ack leaves the message retryable", async () => {
	const gateway = fakeGateway();
	const attempted: string[] = [];
	const deliver = async (m: ReturnType<typeof message>) => {
		attempted.push(m.id);
		if (m.id === "502" && attempted.filter((id) => id === "502").length === 1) return "unavailable";
		return gateway.send(m.id, { mentioned: true });
	};
	const channel = fakeChannel([message("501"), message("502"), message("503")]);
	const outcome = await recoverConversation(channel, { cursor: "0", nowMs: 0, deliver });
	expect(outcome).toEqual({
		advancedTo: "501",
		delivered: 1,
		skipped: 0,
		discarded: 0,
		held: 0,
		truncated: false,
		failed: true,
	});
	// Retry after reconnect: the failed message is still first, later ones still pending.
	const retry = await recoverConversation(channel, { cursor: outcome.advancedTo, nowMs: 0, deliver });
	expect(attempted.filter((id) => id === "502")).toHaveLength(2);
	expect(retry.failed).toBe(false);
	expect(retry.delivered).toBe(2);
});

test("gateway rejection is retried inside the pass and then persisted once", async () => {
	const cursorPath = join(home, "retry", "recovery-cursor.json");
	const inbound = message(snowflakeFromTimestamp(Date.now()));
	const channel = fakeChannel([inbound]);
	const requests: string[] = [];
	let reject = true;
	const client = {
		request: async (verb: string) => {
			requests.push(verb);
			if (reject) {
				reject = false;
				throw new Error("gateway unavailable");
			}
			return {};
		},
	};
	const firstGateway = wiredGateway(channel, client, cursorPath);
	const first = await firstGateway.recoverMissedMessages();
	expect(first).toBeUndefined();
	// The rejected send burned one attempt and the in-pass retry acked it: two chat.sends,
	// one watermark. A reconnect afterwards has nothing left to replay.
	expect(requests).toEqual(["chat.send", "chat.send"]);
	await settle();
	const persisted = await loadRecoveryCursors(cursorPath);
	expect(persisted.recoveredThrough).toEqual({ "channel-1": inbound.id });
	// The ledger entry for the retried message is released once it lands.
	expect(persisted.attempts).toEqual({});
	const secondGateway = wiredGateway(fakeChannel([inbound]), client, cursorPath);
	await secondGateway.recoverMissedMessages();
	await secondGateway.recoverMissedMessages();
	expect(requests).toEqual(["chat.send", "chat.send"]);
});

test("cold-start recovery waits for both gateway client and Discord user", async () => {
	const cursorPath = join(home, "cold-start", "recovery-cursor.json");
	const inbound = message(snowflakeFromTimestamp(Date.now()));
	let fetches = 0;
	let botUser: unknown;
	const channel = fakeChannel([inbound]);
	const client = {
		request: async () => ({}),
		onChatMessage: () => () => {},
	};
	const config = {
		tokenFile: "token",
		token: "redacted",
		configPath: "config",
		channels: { "channel-1": {} },
	} as const;
	const gateway = new ReconnectingGateway(
		"socket",
		{
			channels: {
				fetch: async () => {
					fetches++;
					return channel;
				},
			},
		},
		config,
		undefined,
		undefined,
		cursorPath,
		() => botUser,
		client as never,
	);
	await gateway.recoverMissedMessages();
	expect(fetches).toBe(0);
	botUser = bot;
	await Promise.all([gateway.recoverMissedMessages(), gateway.recoverMissedMessages()]);
	expect(fetches).toBe(1);
});

test("long gaps paginate forward and report truncation at the page bound", async () => {
	const ids = ["10", "20", "30", "40", "50", "60"];
	const channel = fakeChannel(ids.map((id) => message(id)));
	const delivered: string[] = [];
	const outcome = await recoverConversation(channel, {
		nowMs: 0,
		pageLimit: 2,
		maxPages: 2,
		deliver: async (m) => {
			delivered.push(m.id);
			return "acked";
		},
	});
	expect(outcome.truncated).toBe(true);
	expect(outcome.delivered).toBe(4);
	expect(outcome.advancedTo).toBe("40");
	expect(delivered).toEqual(["10", "20", "30", "40"]);
	expect(channel.fetches[1].after).toBe("20");
	// Defaults stay bounded.
	expect(RECOVERY_PAGE_LIMIT).toBe(100);
	expect(RECOVERY_MAX_PAGES).toBeLessThanOrEqual(10);
});

test("normalizes Discord Collections and delivers every page in ascending snowflake order", async () => {
	const values = [message("901"), message("903"), message("902")];
	const channel: RecoverableChannel = {
		messages: {
			async fetch(options) {
				const after = options.after ?? "0";
				return new Map(
					values
						.filter((item) => BigInt(item.id) > BigInt(after))
						.toReversed()
						.map((item) => [item.id, item]),
				);
			},
		},
	};
	const delivered: string[] = [];
	const outcome = await recoverConversation(channel, {
		cursor: "0",
		nowMs: 0,
		pageLimit: 3,
		deliver: async (item) => {
			delivered.push(item.id);
			return "acked";
		},
	});
	expect(delivered).toEqual(["901", "902", "903"]);
	expect(outcome.advancedTo).toBe("903");
});

test("bootstrap lookback bounds the first-run gap to 24h", async () => {
	const now = Date.UTC(2026, 7, 28, 3, 0, 0);
	const inside = message(snowflakeFromTimestamp(now - 3600_000));
	const outside = message(snowflakeFromTimestamp(now - 48 * 3600_000));
	const channel = fakeChannel([outside, inside]);
	const delivered: string[] = [];
	const outcome = await recoverConversation(channel, {
		nowMs: now,
		deliver: async (m) => {
			delivered.push(m.id);
			return "acked";
		},
	});
	expect(delivered).toEqual([inside.id]);
	expect(outcome.truncated).toBe(false);
});

test("cursor file survives an interrupted persist without truncation", async () => {
	const path = join(home, "atomic", "recovery-cursor.json");
	await saveRecoveryCursors(path, cursorState({ c: "777" }));
	expect(await readFile(path, "utf8")).toContain("777");
	expect(await loadRecoveryCursors(path)).toEqual(cursorState({ c: "777" }));
});

/** Fake gateway client + one fake Discord channel behind a real ReconnectingGateway. */
function wiredGateway(
	channel: RecoverableChannel,
	client: { request: (verb: string, params?: unknown) => Promise<unknown> },
	cursorPath: string,
	channels: Record<string, Record<string, never>> = { "channel-1": {} },
	getBotUser: () => unknown = () => bot,
): ReconnectingGateway {
	return new ReconnectingGateway(
		"socket",
		{ channels: { fetch: async () => channel } },
		{ tokenFile: "token", token: "redacted", configPath: "config", channels } as never,
		undefined,
		undefined,
		cursorPath,
		getBotUser,
		{ ...client, onChatMessage: () => () => {} } as never,
		async () => {},
	);
}

/** Lets the fire-and-forget cursor persist chain settle. */
function settle(ms = 10): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

const channelOrigin = { platform: "discord", kind: "channel", conversationId: "channel-1" } as const;

test("live sends never advance the recovery watermark past an unrecovered gap", async () => {
	const cursorPath = join(home, "live-vs-recovery", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	const missedFirst = message(snowflakeFromTimestamp(base));
	const missedSecond = message(snowflakeFromTimestamp(base + 1_000));
	const liveId = snowflakeFromTimestamp(Date.now());
	const sent: string[] = [];
	let gatewayHealthy = false;
	const client = {
		request: async (verb: string, params?: unknown) => {
			if (verb === "gateway.status") {
				if (!gatewayHealthy) throw new Error("gateway down");
				return {};
			}
			const id = (params as { messageId: string }).messageId;
			if (id === missedSecond.id && !gatewayHealthy) throw new Error("gateway down");
			sent.push(id);
			return { engaged: false };
		},
	};
	const live = wiredGateway(fakeChannel([missedFirst, missedSecond]), client, cursorPath);
	// Long-running process: the cursor store is already loaded when live traffic arrives.
	await settle();
	expect(live.cursorFault).toBeUndefined();
	// A live message arrives while the backfill is still incomplete: it must not be mistaken
	// for recovery progress (issue #33 recurrence).
	await live.requestInbound(liveId, channelOrigin, "live traffic", { mentioned: true } as never);
	await live.recoverMissedMessages();
	await settle();
	expect(sent).toContain(liveId);
	expect((await loadRecoveryCursors(cursorPath)).recoveredThrough["channel-1"]).toBe(missedFirst.id);
	// Restart with a healthy gateway: the gap behind the live message is still recovered.
	gatewayHealthy = true;
	const restarted = wiredGateway(fakeChannel([missedFirst, missedSecond]), client, cursorPath);
	await restarted.recoverMissedMessages();
	await settle();
	expect(sent).toContain(missedSecond.id);
	expect((await loadRecoveryCursors(cursorPath)).recoveredThrough["channel-1"]).toBe(missedSecond.id);
});

test("unrecoverable watermarks are quarantined, bounded, and never deleted", async () => {
	const retained = retainRecoveryCursors(cursorState({ "channel-1": "10", "thread-7": "20", "dm-3": "30" }), [
		"channel-1",
	]);
	expect(retained.recoveredThrough).toEqual({ "channel-1": "10" });
	expect(watermarks(retained.quarantined)).toEqual({ "thread-7": "20", "dm-3": "30" });
	const cursorPath = join(home, "prune", "recovery-cursor.json");
	const inbound = message(snowflakeFromTimestamp(Date.now() - 60_000));
	// Boundary: a thread key already on disk moves to quarantine, not into recoveredThrough.
	await saveRecoveryCursors(cursorPath, cursorState({ "thread-7": "1", "channel-1": "2" }));
	const gateway = wiredGateway(fakeChannel([inbound]), { request: async () => ({}) }, cursorPath);
	await gateway.requestInbound(
		snowflakeFromTimestamp(Date.now()),
		{ platform: "discord", kind: "thread", conversationId: "thread-9", parentId: "channel-1" },
		"in a thread",
		{ mentioned: true } as never,
	);
	await gateway.recoverMissedMessages();
	await settle();
	const state = await loadRecoveryCursors(cursorPath);
	expect(state.recoveredThrough).toEqual({ "channel-1": inbound.id });
	// A live thread send never creates a key at all; the pre-existing thread key survives.
	expect(watermarks(state.quarantined)).toEqual({ "thread-7": "1" });
});

test("quarantine pruning uses insertion sequence, not JS key order", async () => {
	// Numeric-looking conversation ids: JS object key order sorts these numerically, so a
	// cap that trusted key order would prune "2" (newest) and keep "10" (oldest).
	let state = cursorState({});
	for (const id of ["10", "9", "2"]) {
		state = retainRecoveryCursors({ ...state, recoveredThrough: { ...state.recoveredThrough, [id]: "1000" } }, [id]);
		state = retainRecoveryCursors(state, []);
	}
	expect(Object.keys(watermarks(state.quarantined)).sort()).toEqual(["10", "2", "9"]);
	const capped = retainRecoveryCursors(state, [], 2);
	// Oldest insertion ("10") is the one pruned; the two newest survive.
	expect(Object.keys(capped.quarantined).sort()).toEqual(["2", "9"]);
	const path = join(home, "quarantine-order", "recovery-cursor.json");
	await saveRecoveryCursors(path, capped);
	const reloaded = await loadRecoveryCursors(path);
	expect(Object.keys(reloaded.quarantined).sort()).toEqual(["2", "9"]);
	// A restart must not restart the sequence, or new entries would tie with old ones.
	expect(reloaded.sequence).toBeGreaterThanOrEqual(3);
});

test("skip-heavy gap keeps durable progress across the page bound", async () => {
	const cursorPath = join(home, "skip-heavy", "recovery-cursor.json");
	const base = Date.now() - 12 * 60 * 60 * 1000;
	// Empty content = skipped without a send: the durable watermark must still advance, or a
	// gap longer than the page bound stays truncated forever.
	const all = Array.from({ length: RECOVERY_PAGE_LIMIT * RECOVERY_MAX_PAGES + 20 }, (_, index) =>
		message(snowflakeFromTimestamp(base + index * 10), { content: "   " }),
	);
	const bound = all[RECOVERY_PAGE_LIMIT * RECOVERY_MAX_PAGES - 1];
	const gateway = wiredGateway(fakeChannel(all), { request: async () => ({}) }, cursorPath);
	await gateway.recoverMissedMessages();
	await settle();
	expect((await loadRecoveryCursors(cursorPath)).recoveredThrough["channel-1"]).toBe(bound.id);
	await gateway.recoverMissedMessages();
	await settle();
	expect((await loadRecoveryCursors(cursorPath)).recoveredThrough["channel-1"]).toBe(all[all.length - 1].id);
});

test("a terminal message is discarded after the attempt threshold and dead-lettered", async () => {
	const cursorPath = join(home, "poison", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	const good = message(snowflakeFromTimestamp(base));
	const poison = message(snowflakeFromTimestamp(base + 1_000));
	const later = message(snowflakeFromTimestamp(base + 2_000));
	const sends: string[] = [];
	const client = {
		request: async (_verb: string, params?: unknown) => {
			const id = (params as { messageId: string }).messageId;
			sends.push(id);
			if (id === poison.id) throw Object.assign(new Error("content exceeds limit"), { code: "payload_too_large" });
			return {};
		},
	};
	const gateway = wiredGateway(fakeChannel([good, poison, later]), client, cursorPath);
	await gateway.recoverMissedMessages();
	await settle();
	expect(sends.filter((id) => id === poison.id)).toHaveLength(RECOVERY_MAX_ATTEMPTS);
	// Head-of-line block broken: the message after the discarded one still gets delivered.
	expect(sends).toContain(later.id);
	const state = await loadRecoveryCursors(cursorPath);
	expect(state.recoveredThrough["channel-1"]).toBe(later.id);
	expect(state.deadLetters).toHaveLength(1);
	expect(state.deadLetters[0]).toMatchObject({
		messageId: poison.id,
		conversationId: "channel-1",
		classification: "terminal-message",
		attempts: RECOVERY_MAX_ATTEMPTS,
	});
	expect(state.deadLetters[0].summary).toContain("payload_too_large");
	expect(Date.parse(state.deadLetters[0].at)).toBeGreaterThan(0);
	expect(gateway.deadLetters).toHaveLength(1);
});

test("repeated-page bail logs the cursor as unchanged instead of claiming progress", async () => {
	const cursorPath = join(home, "repeated-page", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	const page = Array.from({ length: RECOVERY_PAGE_LIMIT }, (_, index) =>
		message(snowflakeFromTimestamp(base + index * 10)),
	);
	const stuck = page[page.length - 1].id;
	await saveRecoveryCursors(cursorPath, cursorState({ "channel-1": stuck }));
	// A channel that ignores `after` returns the same full page forever.
	const channel: RecoverableChannel = { messages: { fetch: async () => page } };
	const errors: string[] = [];
	const original = console.error;
	console.error = (...args: unknown[]) => {
		errors.push(args.map(String).join(" "));
	};
	try {
		await wiredGateway(channel, { request: async () => ({}) }, cursorPath).recoverMissedMessages();
	} finally {
		console.error = original;
	}
	expect(errors.some((line) => line.includes(`cursor unchanged at ${stuck}`))).toBe(true);
	expect(errors.some((line) => line.includes("cursor advanced to"))).toBe(false);
});

test("an unreadable cursor store fails recovery closed and stays observable", async () => {
	// A directory where the cursor file belongs: readFile fails with EISDIR, not ENOENT.
	const cursorPath = join(home, "unreadable", "recovery-cursor.json");
	await mkdir(cursorPath, { recursive: true });
	const channel = fakeChannel([message(snowflakeFromTimestamp(Date.now() - 60_000))]);
	const gateway = wiredGateway(channel, { request: async () => ({}) }, cursorPath);
	await gateway.recoverMissedMessages();
	expect(channel.fetches).toHaveLength(0);
	expect(gateway.cursorFault).toContain("EISDIR");
});

/** Fails every chat.send with `error`, recording the ids it was asked to send. */
function failingClient(error: unknown, sends: string[]) {
	return {
		request: async (verb: string, params?: unknown) => {
			if (verb !== "chat.send") return {};
			sends.push((params as { messageId: string }).messageId);
			throw error;
		},
	};
}

test("failure classification never guesses in favour of discarding", () => {
	expect(classifyRecoveryFailure(Object.assign(new Error("nope"), { code: "payload_too_large" }))).toBe(
		"terminal-message",
	);
	expect(classifyRecoveryFailure(new Error("chat.send: message too long for this platform"))).toBe("terminal-message");
	expect(classifyRecoveryFailure(Object.assign(new Error("reset"), { code: "ECONNRESET" }))).toBe("retryable");
	expect(classifyRecoveryFailure(new Error("request timed out after 5000ms"))).toBe("retryable");
	expect(classifyRecoveryFailure(new Error("SQLITE_BUSY: database is locked"))).toBe("retryable");
	expect(classifyRecoveryFailure(Object.assign(new Error("boom"), { code: "verb_failed" }))).toBe("write-path-unknown");
	expect(classifyRecoveryFailure(new Error("gateway said no"))).toBe("write-path-unknown");
});

test("a transient failure outlasting the in-pass budget neither advances nor discards", async () => {
	const cursorPath = join(home, "transient", "recovery-cursor.json");
	const inbound = message(snowflakeFromTimestamp(Date.now() - 60 * 60 * 1000));
	const sends: string[] = [];
	const client = failingClient(Object.assign(new Error("connection reset"), { code: "ECONNRESET" }), sends);
	const gateway = wiredGateway(fakeChannel([inbound]), client, cursorPath);
	await gateway.recoverMissedMessages();
	await settle();
	expect(sends).toHaveLength(RECOVERY_MAX_ATTEMPTS);
	const state = await loadRecoveryCursors(cursorPath);
	// The cursor may record the bootstrap floor it scanned from, but never the failed message.
	expect(BigInt(state.recoveredThrough["channel-1"] ?? "0") < BigInt(inbound.id)).toBe(true);
	expect(state.deadLetters).toEqual([]);
	expect(gateway.recoveryRetryPending).toBe(true);
});

test("an unclassifiable failure never advances the cursor and is never dead-lettered", async () => {
	const cursorPath = join(home, "unknown-class", "recovery-cursor.json");
	const inbound = message(snowflakeFromTimestamp(Date.now() - 60 * 60 * 1000));
	const sends: string[] = [];
	const client = failingClient(Object.assign(new Error("write path did something"), { code: "verb_failed" }), sends);
	const gateway = wiredGateway(fakeChannel([inbound]), client, cursorPath);
	await gateway.recoverMissedMessages();
	await settle();
	expect(sends).toHaveLength(RECOVERY_MAX_ATTEMPTS);
	const state = await loadRecoveryCursors(cursorPath);
	expect(BigInt(state.recoveredThrough["channel-1"] ?? "0") < BigInt(inbound.id)).toBe(true);
	expect(state.deadLetters).toEqual([]);
});

test("a scheduled retry that cannot proceed reschedules itself", async () => {
	const cursorPath = join(home, "reschedule", "recovery-cursor.json");
	const channel = fakeChannel([message(snowflakeFromTimestamp(Date.now() - 60_000))]);
	let botUser: unknown;
	const gateway = wiredGateway(channel, { request: async () => ({}) }, cursorPath, { "channel-1": {} }, () => botUser);
	await gateway.recoverMissedMessages();
	expect(channel.fetches).toHaveLength(0);
	expect(gateway.recoveryRetryPending).toBe(true);
	// Discord becomes ready: the armed retry resumes the pass instead of evaporating.
	botUser = bot;
	await new Promise((resolve) => setTimeout(resolve, RECOVERY_RETRY_BASE_MS + 200));
	expect(channel.fetches.length).toBeGreaterThan(0);
});

test("a channel dropped from config and re-added resumes from its quarantined watermark", async () => {
	const cursorPath = join(home, "readd", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	const first = message(snowflakeFromTimestamp(base));
	const second = message(snowflakeFromTimestamp(base + 1_000));
	const client = { request: async () => ({}) };
	await wiredGateway(fakeChannel([first]), client, cursorPath).recoverMissedMessages();
	await settle();
	// Config edit: channel-1 is gone, so its watermark is quarantined on the next save.
	await wiredGateway(fakeChannel([first]), client, cursorPath, { "channel-2": {} }).recoverMissedMessages();
	await settle();
	expect((await loadRecoveryCursors(cursorPath)).quarantined["channel-1"].watermark).toBe(first.id);
	// Re-added: recovery resumes after the retained watermark, not from the 24h bootstrap.
	const channel = fakeChannel([first, second]);
	await wiredGateway(channel, client, cursorPath).recoverMissedMessages();
	await settle();
	expect(channel.fetches[0].after).toBe(first.id);
	expect((await loadRecoveryCursors(cursorPath)).recoveredThrough["channel-1"]).toBe(second.id);
});

test("mass discards keep an audit digest after the dead-letter cap evicts records", async () => {
	const cursorPath = join(home, "dead-letter-cap", "recovery-cursor.json");
	const base = Date.now() - 6 * 60 * 60 * 1000;
	// Alternating oversized/deliverable pairs: every oversized message is provably
	// message-specific (the next one lands), so each discard commits.
	const pairs = RECOVERY_DEAD_LETTER_CAP + 3;
	const all = Array.from({ length: pairs * 2 }, (_, index) =>
		message(snowflakeFromTimestamp(base + index * 10), {
			content: index % 2 === 0 ? `oversized ${index}` : `hello <@${bot.id}>`,
		}),
	);
	const oversized = all.filter((_, index) => index % 2 === 0);
	const client = {
		request: async (_verb: string, params?: unknown) => {
			const id = (params as { messageId: string }).messageId;
			if (oversized.some((item) => item.id === id)) {
				throw Object.assign(new Error("message too long for this platform"), { code: "payload_too_large" });
			}
			return {};
		},
	};
	const gateway = wiredGateway(fakeChannel(all), client, cursorPath);
	await gateway.recoverMissedMessages();
	expect(gateway.deadLetters).toHaveLength(RECOVERY_DEAD_LETTER_CAP);
	// The aggregate survives eviction: the mass event stays auditable.
	expect(gateway.deadLetterDigest["channel-1"]).toMatchObject({
		conversationId: "channel-1",
		classification: "terminal-message",
		count: pairs,
		firstMessageId: oversized[0].id,
		lastMessageId: oversized[oversized.length - 1].id,
	});
	// One serialized persist per discard: give the chain room to drain before reading.
	await settle(1_000);
	const state = await loadRecoveryCursors(cursorPath);
	expect(state.deadLetters).toHaveLength(RECOVERY_DEAD_LETTER_CAP);
	expect(state.deadLetters[0].messageId).toBe(oversized[3].id);
	expect(state.deadLetters[RECOVERY_DEAD_LETTER_CAP - 1].messageId).toBe(oversized[oversized.length - 1].id);
	expect(state.deadLetterDigest["channel-1"].count).toBe(pairs);
});

test("a contract-level invalid_params failure is never treated as message-specific", async () => {
	const cursorPath = join(home, "contract-regression", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	const all = Array.from({ length: 5 }, (_, index) => message(snowflakeFromTimestamp(base + index * 10)));
	const sends: string[] = [];
	// Exactly what the gateway raises for a contract regression: every message fails.
	const client = failingClient(
		Object.assign(new Error("non-loopback chat.send requires engagement"), { code: "invalid_params" }),
		sends,
	);
	const gateway = wiredGateway(fakeChannel(all), client, cursorPath);
	await gateway.recoverMissedMessages();
	await settle();
	expect(classifyRecoveryFailure(Object.assign(new Error("requires a valid origin"), { code: "invalid_params" }))).toBe(
		"write-path-unknown",
	);
	expect(gateway.deadLetters).toEqual([]);
	const state = await loadRecoveryCursors(cursorPath);
	expect(BigInt(state.recoveredThrough["channel-1"] ?? "0") < BigInt(all[0].id)).toBe(true);
	expect(gateway.recoveryRetryPending).toBe(true);
});

test("a uniform terminal-looking failure across messages discards nothing", async () => {
	const cursorPath = join(home, "uniform-terminal", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	const all = Array.from({ length: 6 }, (_, index) => message(snowflakeFromTimestamp(base + index * 10)));
	const sends: string[] = [];
	// payload_too_large IS per-payload evidence, but here it hits every message: that is a
	// write-path problem, so the uniform-failure guard must refuse to discard anything.
	const client = failingClient(Object.assign(new Error("message too long"), { code: "payload_too_large" }), sends);
	const gateway = wiredGateway(fakeChannel(all), client, cursorPath);
	await gateway.recoverMissedMessages();
	await settle();
	expect(gateway.deadLetters).toEqual([]);
	expect(gateway.deadLetterDigest).toEqual({});
	// Bailed out at the uniform-failure limit instead of grinding through the whole page.
	expect(new Set(sends).size).toBe(RECOVERY_UNIFORM_FAILURE_LIMIT);
	const state = await loadRecoveryCursors(cursorPath);
	expect(BigInt(state.recoveredThrough["channel-1"] ?? "0") < BigInt(all[0].id)).toBe(true);
	expect(gateway.recoveryRetryPending).toBe(true);
});

test("a single oversized message is discarded once a later message proves the path works", async () => {
	const cursorPath = join(home, "oversized-one", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	// The oversized message is FIRST: head-of-line, with no earlier success to lean on.
	const oversized = message(snowflakeFromTimestamp(base), { content: "oversized payload" });
	const good = message(snowflakeFromTimestamp(base + 1_000));
	const client = {
		request: async (_verb: string, params?: unknown) => {
			if ((params as { messageId: string }).messageId === oversized.id) {
				throw Object.assign(new Error("content too long"), { code: "payload_too_large" });
			}
			return {};
		},
	};
	const gateway = wiredGateway(fakeChannel([oversized, good]), client, cursorPath);
	await gateway.recoverMissedMessages();
	await settle();
	expect(gateway.deadLetters).toHaveLength(1);
	expect(gateway.deadLetters[0]).toMatchObject({ messageId: oversized.id, classification: "terminal-message" });
	const state = await loadRecoveryCursors(cursorPath);
	expect(state.recoveredThrough["channel-1"]).toBe(good.id);
	// The ledger entry is released once the message is resolved.
	expect(state.attempts[oversized.id]).toBeUndefined();
});

test("a throwing history fetch leaves other channels recovered and arms a retry", async () => {
	const cursorPath = join(home, "history-throw", "recovery-cursor.json");
	const inbound = message(snowflakeFromTimestamp(Date.now() - 60_000));
	const healthy = fakeChannel([inbound]);
	const broken: RecoverableChannel = {
		messages: {
			fetch: async () => {
				throw new Error("discord history 500");
			},
		},
	};
	const gateway = new ReconnectingGateway(
		"socket",
		{ channels: { fetch: async (id: string) => (id === "channel-broken" ? broken : healthy) } },
		{
			tokenFile: "token",
			token: "redacted",
			configPath: "config",
			channels: { "channel-broken": {}, "channel-1": {} },
		} as never,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		{ request: async () => ({}), onChatMessage: () => () => {} } as never,
		async () => {},
	);
	await gateway.recoverMissedMessages();
	await settle();
	// The broken channel did not abort the pass: the channel behind it still backfilled.
	expect((await loadRecoveryCursors(cursorPath)).recoveredThrough["channel-1"]).toBe(inbound.id);
	expect(gateway.recoveryRetryPending).toBe(true);
});

test("a throwing history fetch is contained in the outcome instead of thrown", async () => {
	const channel: RecoverableChannel = {
		messages: {
			fetch: async () => {
				throw new Error("discord history 500");
			},
		},
	};
	const outcome = await recoverConversation(channel, { cursor: "10", nowMs: 0, deliver: async () => "acked" });
	expect(outcome.failed).toBe(true);
	expect(outcome.fetchError).toBe("discord history 500");
	expect(outcome.advancedTo).toBe("10");
});

test("one channel's unexpected throw leaves later channels recovered and arms a retry", async () => {
	const cursorPath = join(home, "channel-throw", "recovery-cursor.json");
	const inbound = message(snowflakeFromTimestamp(Date.now() - 60_000));
	// A thread message with no parentId makes origin normalization throw inside deliver: an
	// unexpected error from the middle of one channel's replay, not a guarded fetch.
	const poisonOrigin = message(snowflakeFromTimestamp(Date.now() - 120_000), {
		channel: { id: "channel-broken", isThread: () => true, parentId: null },
	} as Partial<DiscordInboundMessage>);
	const gateway = new ReconnectingGateway(
		"socket",
		{
			channels: {
				fetch: async (id: string) => (id === "channel-broken" ? fakeChannel([poisonOrigin]) : fakeChannel([inbound])),
			},
		},
		{
			tokenFile: "token",
			token: "redacted",
			configPath: "config",
			channels: { "channel-broken": {}, "channel-1": {} },
		} as never,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		{ request: async () => ({}), onChatMessage: () => () => {} } as never,
		async () => {},
	);
	await gateway.recoverMissedMessages();
	await settle();
	expect((await loadRecoveryCursors(cursorPath)).recoveredThrough["channel-1"]).toBe(inbound.id);
	expect(gateway.recoveryRetryPending).toBe(true);
});

test("cross-pass accounting discards a message that alternates terminal and transient failures", async () => {
	const cursorPath = join(home, "alternating", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	const flaky = message(snowflakeFromTimestamp(base), { content: "oversized payload" });
	const good = message(snowflakeFromTimestamp(base + 1_000));
	let call = 0;
	const client = {
		request: async (_verb: string, params?: unknown) => {
			if ((params as { messageId: string }).messageId !== flaky.id) return {};
			call++;
			// Alternates: no single pass ever collects RECOVERY_MAX_ATTEMPTS terminal failures.
			throw call % 2 === 1
				? Object.assign(new Error("content too long"), { code: "payload_too_large" })
				: Object.assign(new Error("connection reset"), { code: "ECONNRESET" });
		},
	};
	const gateway = wiredGateway(fakeChannel([flaky, good]), client, cursorPath);
	await gateway.recoverMissedMessages();
	expect(gateway.deadLetters).toEqual([]);
	expect(gateway.deadLetters).toHaveLength(0);
	// Later passes keep accumulating the terminal half of the alternation.
	await gateway.recoverMissedMessages();
	await gateway.recoverMissedMessages();
	await settle();
	expect(gateway.deadLetters).toHaveLength(1);
	expect(gateway.deadLetters[0].messageId).toBe(flaky.id);
	expect(gateway.deadLetters[0].attempts).toBeGreaterThanOrEqual(RECOVERY_MAX_ATTEMPTS);
	expect((await loadRecoveryCursors(cursorPath)).recoveredThrough["channel-1"]).toBe(good.id);
});

test("dead-letter recording is idempotent per message id", async () => {
	const entry = {
		messageId: "42",
		conversationId: "channel-1",
		classification: "terminal-message",
		attempts: 3,
		at: new Date().toISOString(),
		summary: "content too long",
	} as const;
	const once = recordDeadLetter(cursorState({}), entry);
	const twice = recordDeadLetter(once, { ...entry, at: new Date().toISOString() });
	expect(twice).toBe(once);
	expect(twice.deadLetters).toHaveLength(1);
	expect(twice.deadLetterDigest["channel-1"].count).toBe(1);
});

test("a replayed discard after a crash does not double-count the dead letter", async () => {
	const cursorPath = join(home, "replay-discard", "recovery-cursor.json");
	const base = Date.now() - 60 * 60 * 1000;
	const oversized = message(snowflakeFromTimestamp(base), { content: "oversized payload" });
	const good = message(snowflakeFromTimestamp(base + 1_000));
	const client = {
		request: async (_verb: string, params?: unknown) => {
			if ((params as { messageId: string }).messageId === oversized.id) {
				throw Object.assign(new Error("content too long"), { code: "payload_too_large" });
			}
			return {};
		},
	};
	await wiredGateway(fakeChannel([oversized, good]), client, cursorPath).recoverMissedMessages();
	await settle();
	const afterCrash = await loadRecoveryCursors(cursorPath);
	expect(afterCrash.deadLetters).toHaveLength(1);
	// "Crash" before the watermark advanced: rewind the cursor and replay the same window.
	await saveRecoveryCursors(cursorPath, { ...afterCrash, recoveredThrough: {} });
	await wiredGateway(fakeChannel([oversized, good]), client, cursorPath).recoverMissedMessages();
	await settle();
	const replayed = await loadRecoveryCursors(cursorPath);
	expect(replayed.deadLetters).toHaveLength(1);
	expect(replayed.deadLetterDigest["channel-1"].count).toBe(1);
});
