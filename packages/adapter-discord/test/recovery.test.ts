import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { DiscordInboundMessage } from "../src/main";
import { decideInbound, LruSet, ReconnectingGateway } from "../src/main";
import {
	loadRecoveryCursors,
	RECOVERY_MAX_PAGES,
	RECOVERY_PAGE_LIMIT,
	type RecoverableChannel,
	type RecoveryCursorState,
	RecoveryGate,
	recoverConversation,
	recoveryCursorPath,
	saveRecoveryCursors,
	snowflakeFromTimestamp,
	snowflakeIsAfter,
} from "../src/recovery";

const bot = { id: "bot-9" };

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
	expect(await loadRecoveryCursors(join(home, "missing.json"))).toEqual({ conversations: {} });
	await saveRecoveryCursors(path, { conversations: { "channel-1": "1000", bad: "x" } } as RecoveryCursorState);
	const state = await loadRecoveryCursors(path);
	expect(state.conversations["channel-1"]).toBe("1000");
	expect(state.conversations.bad).toBeUndefined();
	await writeFile(path, "not json{", "utf8");
	expect(await loadRecoveryCursors(path)).toEqual({ conversations: {} });
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
	expect(first).toEqual({ advancedTo: "100", delivered: 1, truncated: false, failed: false });
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
	expect(outcome).toEqual({ advancedTo: "501", delivered: 1, truncated: false, failed: true });
	// Retry after reconnect: the failed message is still first, later ones still pending.
	const retry = await recoverConversation(channel, { cursor: outcome.advancedTo, nowMs: 0, deliver });
	expect(attempted.filter((id) => id === "502")).toHaveLength(2);
	expect(retry.failed).toBe(false);
	expect(retry.delivered).toBe(2);
});

test("gateway rejection is retryable through the real recovered send path", async () => {
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
		onChatMessage: () => () => {},
	};
	const config = {
		tokenFile: "token",
		token: "redacted",
		configPath: "config",
		channels: { "channel-1": {} },
	} as const;
	const firstGateway = new ReconnectingGateway(
		"socket",
		{ channels: { fetch: async () => channel } },
		config,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		client as never,
	);
	const first = await firstGateway.recoverMissedMessages();
	expect(first).toBeUndefined();
	// A failed request did not poison the process-local gate or persist a watermark. A
	// reconnect gets a fresh gateway instance and retries the same oldest message.
	const secondGateway = new ReconnectingGateway(
		"socket",
		{ channels: { fetch: async () => channel } },
		config,
		undefined,
		undefined,
		cursorPath,
		() => bot,
		client as never,
	);
	await secondGateway.recoverMissedMessages();
	await secondGateway.recoverMissedMessages();
	expect(requests).toEqual(["chat.send", "chat.send"]);
	await new Promise((resolve) => setTimeout(resolve, 10));
	expect(await loadRecoveryCursors(cursorPath)).toEqual({ conversations: { "channel-1": inbound.id } });
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
	await saveRecoveryCursors(path, { conversations: { c: "777" } });
	expect(await readFile(path, "utf8")).toContain("777");
	expect(await loadRecoveryCursors(path)).toEqual({ conversations: { c: "777" } });
});
