import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SlackApiError, type SlackHistoryPage } from "../src/api";
import type { SlackInboundMessage } from "../src/main";
import { slackMessageOrigin } from "../src/origin";
import {
	EMPTY_RECOVERY_STATE,
	loadRecoveryCursors,
	pruneKnownDms,
	RECOVERY_BOOTSTRAP_LOOKBACK_MS,
	RECOVERY_KNOWN_DM_TTL_MS,
	type RecoveryCursorState,
	type RecoveryHistoryPort,
	RecoveryScheduler,
	recoverConversation,
	recoveryCursorPath,
	rememberKnownDm,
	STALE_BACKFILL_MS,
	saveRecoveryCursors,
	staleBackfillIds,
	tsFromTimestamp,
	tsIsAfter,
} from "../src/recovery";

const empty = (): RecoveryCursorState => ({ ...EMPTY_RECOVERY_STATE });
const page = (messages: readonly Record<string, unknown>[], next_cursor?: string): SlackHistoryPage => ({
	messages,
	has_more: !!next_cursor,
	next_cursor,
});
const message = (ts: string, fields: Record<string, unknown> = {}) => ({ ts, user: "U1", ...fields });
const portFor = (messages: readonly Record<string, unknown>[]): RecoveryHistoryPort => ({
	history: async () => page(messages),
	replies: async () => page([]),
});
const base = { cursor: "0.0", nowMs: 2_000_000_000_000, botUserId: "BOT" };
const flush = async () => {
	for (let i = 0; i < 20; i++) await Promise.resolve();
};
function deferred<T>() {
	let resolve!: (value: T) => void;
	const promise = new Promise<T>((done) => {
		resolve = done;
	});
	return { promise, resolve };
}

describe("stale backfill", () => {
	// nowMs = 2_000_000_000_000 ms → 2_000_000_000 s. Slack ts are seconds.
	const now = base.nowMs;
	const sec = (offsetS: number) => `${now / 1000 + offsetS}.000100`;
	const old = -(STALE_BACKFILL_MS / 1000) - 60; // just past the window
	const fresh = -60; // one minute ago

	test("old + bot answered later in the same conversation → stale", () => {
		const human = message(sec(old));
		const bot = message(sec(old + 30), { user: "BOT" });
		expect([...staleBackfillIds([human, bot] as never, "BOT", now)]).toEqual([human.ts]);
	});

	test("old but the bot never answered → not stale (it is still owed an answer)", () => {
		const human = message(sec(old));
		expect(staleBackfillIds([human] as never, "BOT", now).size).toBe(0);
	});

	test("recent → never stale, even with a later bot post", () => {
		const human = message(sec(fresh));
		const bot = message(sec(fresh + 10), { user: "BOT" });
		expect(staleBackfillIds([human, bot] as never, "BOT", now).size).toBe(0);
	});

	test("conversation is the thread: a bot post in another thread does not make this one stale", () => {
		const inA = message(sec(old), { thread_ts: "1.0" });
		const botInB = message(sec(old + 30), { user: "BOT", thread_ts: "2.0" });
		expect(staleBackfillIds([inA, botInB] as never, "BOT", now).size).toBe(0);
		const botInA = message(sec(old + 30), { user: "BOT", thread_ts: "1.0" });
		expect([...staleBackfillIds([inA, botInA] as never, "BOT", now)]).toEqual([inA.ts]);
	});

	test("a bot post BEFORE the message is not evidence it was answered", () => {
		const bot = message(sec(old - 30), { user: "BOT" });
		const human = message(sec(old));
		expect(staleBackfillIds([bot, human] as never, "BOT", now).size).toBe(0);
	});

	test("recoverConversation passes the stale verdict to deliver", async () => {
		const human = message(sec(old));
		const bot = message(sec(old + 30), { user: "BOT" });
		const late = message(sec(fresh));
		const seen: [string, boolean][] = [];
		await recoverConversation(portFor([late, bot, human]), "C1", {
			...base,
			deliver: async (msg, stale) => {
				seen.push([msg.ts, stale]);
				return "acked";
			},
		});
		// ts order; the bot's own row is skipped, not delivered.
		expect(seen).toEqual([
			[human.ts, true],
			[late.ts, false],
		]);
	});
});

describe("Slack recovery state", () => {
	test("path, missing state, atomic private round trip, corrupt input", async () => {
		const home = await mkdtemp(join(tmpdir(), "slack-recovery-"));
		try {
			const path = recoveryCursorPath(home);
			expect(path).toBe(join(home, "adapters/slack/recovery-cursor.json"));
			expect(await loadRecoveryCursors(path)).toEqual(empty());
			const state: RecoveryCursorState = {
				...EMPTY_RECOVERY_STATE,
				recoveredThrough: { C1: "1700000000.123456" },
				continuation: { C3: { olderThan: "1700000000.000001", through: "1700000009.000001" } },
				knownDms: { D1: { lastSeenAt: new Date(base.nowMs).toISOString() } },
				participatedThreads: { "C1:1.0": { lastSeenAt: new Date(base.nowMs).toISOString(), through: "1.5" } },
				quarantined: { C2: { reason: "missing_scope", failures: 3, since: "2026-01-01" } },
				attempts: {
					"C1:2.0": {
						conversationId: "C1",
						attempts: 1,
						classification: "terminal-message",
						reason: "invalid_params",
						lastAt: "2026-01-01",
					},
				},
				deadLetters: [
					{
						messageId: "C1:1.0",
						conversationId: "C1",
						classification: "terminal-message",
						attempts: 3,
						reason: "Slack rejected",
						at: "2026-01-01",
					},
				],
				deadLetterDigest: {
					C1: {
						conversationId: "C1",
						classification: "terminal-message",
						count: 1,
						firstAt: "2026-01-01",
						lastAt: "2026-01-01",
						lastReason: "Slack rejected",
					},
				},
			};
			await saveRecoveryCursors(path, state);
			expect(await loadRecoveryCursors(path)).toEqual(state);
			expect((await stat(path)).mode & 0o777).toBe(0o600);
			expect(JSON.parse(await readFile(path, "utf8"))).toEqual(state);
			await writeFile(path, "{");
			await expect(loadRecoveryCursors(path)).rejects.toThrow(path);
			await writeFile(path, "null");
			await expect(loadRecoveryCursors(path)).rejects.toThrow(path);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	});

	test("DM retention is bounded and expires at the TTL boundary", () => {
		let state = empty();
		for (let i = 0; i < 101; i++) state = rememberKnownDm(state, `D${i}`, base.nowMs + i);
		expect(Object.keys(state.knownDms)).toHaveLength(100);
		expect(state.knownDms.D0).toBeUndefined();
		state = rememberKnownDm(state, "D1", base.nowMs + 101);
		expect(state.knownDms.D1?.lastSeenAt).toBe(new Date(base.nowMs + 101).toISOString());
		expect(rememberKnownDm(state, "C1", base.nowMs + 101).knownDms.C1).toBeUndefined();
		expect(Object.keys(pruneKnownDms(state, base.nowMs + 101 + RECOVERY_KNOWN_DM_TTL_MS).knownDms)).toHaveLength(0);
	});

	test("timestamps compare decimal seconds without lexical or floating point errors", () => {
		expect(tsIsAfter("1700000000.9", "1700000000.10")).toBe(true);
		expect(tsIsAfter("10.0", "9.99")).toBe(true);
		expect(tsIsAfter("1700000000.100000", "1700000000.1")).toBe(false);
		expect(tsIsAfter("1700000000.1000001", "1700000000.1000000")).toBe(true);
		expect(tsFromTimestamp(1999)).toBe("1.000000");
	});
});

describe("Slack conversation replay", () => {
	test("bootstrap lookback and exclusive cursor", async () => {
		let oldest = "";
		const port = portFor([]);
		port.history = async (_channel, options) => {
			oldest = options.oldest;
			return page([]);
		};
		await recoverConversation(port, "C1", { ...base, cursor: undefined, deliver: async () => "acked" });
		expect(oldest).toBe(tsFromTimestamp(base.nowMs - RECOVERY_BOOTSTRAP_LOOKBACK_MS));
		const seen: string[] = [];
		const result = await recoverConversation(portFor([message("1.0"), message("2.0"), message("3.0")]), "C1", {
			...base,
			cursor: "2.0",
			deliver: async (msg) => {
				seen.push(msg.ts);
				expect(msg.channel).toBe("C1");
				return "acked";
			},
		});
		expect(seen).toEqual(["3.0"]);
		expect(result.advancedTo).toBe("3.0");
	});

	test("newest-first pages replay globally oldest-first using next_cursor", async () => {
		const cursors: (string | undefined)[] = [];
		const port = portFor([]);
		port.history = async (_channel, options) => {
			cursors.push(options.cursor);
			expect(options.limit).toBe(200);
			return options.cursor ? page([message("2.0"), message("1.0")]) : page([message("4.0"), message("3.0")], "next");
		};
		const seen: string[] = [];
		const result = await recoverConversation(port, "C1", {
			...base,
			deliver: async (msg) => {
				seen.push(msg.ts);
				return "acked";
			},
		});
		expect(cursors).toEqual([undefined, "next"]);
		expect(seen).toEqual(["1.0", "2.0", "3.0", "4.0"]);
		expect(result).toMatchObject({ advancedTo: "4.0", delivered: 4, failed: false, truncated: false });
		const truncated = await recoverConversation(port, "C1", { ...base, maxPages: 1, deliver: async () => "acked" });
		expect(truncated.truncated).toBe(true);
		expect(truncated.advancedTo).toBeUndefined();
	});

	test("thread parents fetch paged replies, never duplicate parents, and preserve origins", async () => {
		const port = portFor([message("1.0", { reply_count: 2 }), message("2.0", { thread_ts: "2.0" })]);
		const calls: string[] = [];
		port.replies = async (_channel, ts, options) => {
			calls.push(`${ts}/${options.cursor ?? ""}`);
			if (ts === "2.0") return page([message("2.0")]);
			return options.cursor ? page([message("4.0")]) : page([message("1.0"), message("3.0")], "reply-next");
		};
		const seen: SlackInboundMessage[] = [];
		await recoverConversation(port, "C1", {
			...base,
			deliver: async (msg) => {
				seen.push(msg);
				return "acked";
			},
		});
		expect(calls).toEqual(["1.0/", "1.0/reply-next", "2.0/"]);
		expect(seen.map((msg) => msg.ts)).toEqual(["1.0", "2.0", "3.0", "4.0"]);
		expect(seen[2]?.thread_ts).toBe("1.0");
		expect(slackMessageOrigin(seen[2] as SlackInboundMessage)).toMatchObject({
			kind: "thread",
			conversationId: "C1:1.0",
		});
	});

	test("own bot skips, duplicate and skip advance, unavailable stops without crossing", async () => {
		const seen: string[] = [];
		const result = await recoverConversation(
			portFor([message("1.0", { user: "BOT" }), message("2.0"), message("3.0"), message("4.0"), message("5.0")]),
			"C1",
			{
				...base,
				deliver: async (msg) => {
					seen.push(msg.ts);
					return msg.ts === "2.0" ? "duplicate" : msg.ts === "3.0" ? "skip" : "unavailable";
				},
			},
		);
		expect(seen).toEqual(["2.0", "3.0", "4.0"]);
		expect(result).toMatchObject({ advancedTo: "3.0", duplicates: 1, skipped: 2, failed: true });
		const first = await recoverConversation(portFor([message("1.0")]), "C1", {
			...base,
			deliver: async () => "unavailable",
		});
		expect(first.advancedTo).toBeUndefined();
	});

	test("history and reply API errors classify permanence without cursor advancement", async () => {
		for (const code of ["channel_not_found", "ratelimited"]) {
			for (const source of ["history", "replies"] as const) {
				const port = portFor([message("1.0", { reply_count: 1 })]);
				port[source] = async () => {
					throw new SlackApiError(400, code);
				};
				const result = await recoverConversation(port, "C1", { ...base, deliver: async () => "acked" });
				expect(result).toMatchObject({
					failed: true,
					fetchError: code,
					permanent: code === "channel_not_found",
					delivered: 0,
				});
				expect(result.advancedTo).toBeUndefined();
			}
		}
	});
});

describe("Slack recovery scheduler", () => {
	test("single flight and one coalesced follow-up", async () => {
		const first = deferred<boolean>();
		let calls = 0;
		const scheduler = new RecoveryScheduler(async () => {
			calls++;
			return calls === 1 ? first.promise : true;
		});
		scheduler.trigger();
		scheduler.trigger();
		scheduler.trigger();
		expect(calls).toBe(1);
		first.resolve(true);
		await flush();
		expect(calls).toBe(2);
		scheduler.stop();
		scheduler.trigger();
		expect(calls).toBe(2);
	});

	test("incomplete backoff, jitter, ceiling, reset, and cancellation use injected waits", async () => {
		const waits: number[] = [];
		let pending = deferred<void>();
		let complete = false;
		let calls = 0;
		const scheduler = new RecoveryScheduler(
			async () => {
				calls++;
				return complete;
			},
			async (ms) => {
				waits.push(ms);
				pending = deferred<void>();
				await pending.promise;
			},
			() => 0.5,
		);
		scheduler.trigger();
		await flush();
		expect(scheduler.retryPending).toBe(true);
		for (let i = 0; i < 7; i++) {
			pending.resolve();
			await flush();
		}
		expect(waits).toEqual([1100, 2200, 4400, 8800, 17600, 35200, 60000, 60000]);
		complete = true;
		pending.resolve();
		await flush();
		expect(scheduler.retryPending).toBe(false);
		complete = false;
		scheduler.trigger();
		await flush();
		expect(waits.at(-1)).toBe(1100);
		const before = calls;
		scheduler.stop();
		await flush();
		expect(scheduler.retryPending).toBe(false);
		pending.resolve();
		scheduler.trigger();
		await flush();
		expect(calls).toBe(before);
	});

	test("a synchronously throwing run remains retryable", async () => {
		const waits: number[] = [];
		const scheduler = new RecoveryScheduler(
			() => {
				throw new Error("Slack unavailable");
			},
			async (ms) => {
				waits.push(ms);
				await new Promise<void>(() => {});
			},
			() => 0,
		);
		scheduler.trigger();
		await flush();
		expect(waits).toEqual([1000]);
		expect(scheduler.retryPending).toBe(true);
		scheduler.stop();
		await flush();
		expect(scheduler.retryPending).toBe(false);
	});

	test("trigger wakes a pending retry, stop during a run prevents follow-up", async () => {
		let calls = 0;
		const running = deferred<boolean>();
		const scheduler = new RecoveryScheduler(
			async () => {
				calls++;
				return calls === 1 ? false : running.promise;
			},
			() => new Promise(() => {}),
		);
		scheduler.trigger();
		await flush();
		scheduler.trigger();
		await flush();
		expect(calls).toBe(2);
		scheduler.trigger();
		scheduler.stop();
		running.resolve(false);
		await flush();
		expect(calls).toBe(2);
	});
});
