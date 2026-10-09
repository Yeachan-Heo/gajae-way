import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadDiscordAdapterConfig } from "../src/config";
import { GatewayDownAlarm, lastLogLine } from "../src/down-alert";
import { ReconnectingGateway } from "../src/main";

const MINUTE = 60_000;
const quiet = { log: () => {}, error: () => {} };

let home: string;
beforeAll(async () => {
	home = await mkdtemp(join(tmpdir(), "gajaeway-down-alert-"));
});
afterAll(async () => {
	await rm(home, { recursive: true, force: true });
});

function harness(options: { failPosts?: number; lastError?: string } = {}) {
	let now = Date.parse("2026-10-03T04:03:00Z");
	const posts: string[] = [];
	let failures = options.failPosts ?? 0;
	const alarm = new GatewayDownAlarm(
		{ channelId: "room", mentionUserId: "morgan" },
		{
			now: () => now,
			post: async (text) => {
				if (failures > 0) {
					failures -= 1;
					throw new Error("discord unavailable");
				}
				posts.push(text);
			},
			lastError: async () => options.lastError,
			log: quiet,
		},
	);
	return {
		alarm,
		posts,
		advance(ms: number) {
			now += ms;
		},
	};
}

test("nothing at 4:59 down, one mention line at 5:00 with the start time and last error", async () => {
	const { alarm, posts, advance } = harness({ lastError: "gateway_exit cause=boot_failure authority_mismatch" });
	await alarm.down();
	advance(5 * MINUTE - 1000);
	await alarm.down();
	expect(posts).toEqual([]);
	advance(1000);
	await alarm.down();
	expect(posts).toHaveLength(1);
	const start = new Date(Date.parse("2026-10-03T04:03:00Z"));
	const hhmm = `${String(start.getHours()).padStart(2, "0")}:${String(start.getMinutes()).padStart(2, "0")}`;
	expect(posts[0]).toBe(
		`<@morgan> 가재 중계 서버가 ${hhmm}부터 안 붙어. 마지막 오류: gateway_exit cause=boot_failure authority_mismatch. 살펴줘`,
	);
});

test("a long outage posts the down line once, and the return line once with the outage length", async () => {
	const { alarm, posts, advance } = harness();
	await alarm.down();
	for (let i = 0; i < 40; i += 1) {
		advance(30_000);
		await alarm.down();
	}
	expect(posts).toHaveLength(1);
	expect(posts[0]).toContain("마지막 오류: -.");
	await alarm.up();
	await alarm.up();
	expect(posts).toEqual([posts[0] as string, "가재 중계 서버 돌아왔어(20분 꺼져 있었어)."]);
});

test("a one-minute restart says nothing, going down or coming back", async () => {
	const { alarm, posts, advance } = harness();
	await alarm.down();
	advance(MINUTE);
	await alarm.down();
	await alarm.up();
	// The next outage runs on its own clock, not the earlier one's.
	advance(10 * MINUTE);
	await alarm.down();
	advance(4 * MINUTE);
	await alarm.down();
	expect(posts).toEqual([]);
});

test("a down line that failed is retried on the next reconnect, and no return line follows an unsent one", async () => {
	const failing = harness({ failPosts: 1 });
	await failing.alarm.down();
	failing.advance(5 * MINUTE);
	await failing.alarm.down();
	expect(failing.posts).toEqual([]);
	failing.advance(30_000);
	await failing.alarm.down();
	expect(failing.posts).toHaveLength(1);

	const neverSent = harness({ failPosts: 99 });
	await neverSent.alarm.down();
	neverSent.advance(6 * MINUTE);
	await neverSent.alarm.down();
	await neverSent.alarm.up();
	expect(neverSent.posts).toEqual([]);
});

test("coming back while the down line is still posting waits for it, then says it is back", async () => {
	let release: () => void = () => {};
	let entered: () => void = () => {};
	const posting = new Promise<void>((resolve) => (entered = resolve));
	const posts: string[] = [];
	let now = 0;
	const alarm = new GatewayDownAlarm(
		{ channelId: "room", afterMs: 1000 },
		{
			now: () => now,
			post: async (text) => {
				if (posts.length === 0)
					await new Promise<void>((resolve) => {
						release = resolve;
						entered();
					});
				posts.push(text);
			},
			lastError: async () => undefined,
			log: quiet,
		},
	);
	await alarm.down();
	now = 2 * MINUTE;
	const downing = alarm.down();
	await posting;
	const upping = alarm.up();
	release();
	await Promise.all([downing, upping]);
	expect(posts).toHaveLength(2);
	expect(posts[0]?.startsWith("가재 중계 서버가")).toBe(true);
	expect(posts[1]).toBe("가재 중계 서버 돌아왔어(2분 꺼져 있었어).");
	// The outage is settled: a fresh short drop says nothing.
	await alarm.down();
	expect(posts).toHaveLength(2);
});

test("lastLogLine quotes the last non-empty line of a large log, capped at 200 characters", async () => {
	const path = join(home, "gateway.stderr.log");
	await writeFile(path, `${"old line\n".repeat(5000)}${"x".repeat(300)}\n\n`);
	expect(await lastLogLine(path)).toBe("x".repeat(200));
	await writeFile(path, "");
	expect(await lastLogLine(path)).toBeUndefined();
});

test("config resolves the log file next to the config and rejects a malformed section", async () => {
	await writeFile(join(home, "token"), "redacted\n");
	const write = (section: unknown) =>
		writeFile(join(home, "adapter-discord.json"), JSON.stringify({ tokenFile: "token", gatewayDownAlert: section }));
	await write({ channelId: "room", mentionUserId: "morgan", logFile: "gateway.stderr.log" });
	const loaded = await loadDiscordAdapterConfig({ GAJAEWAY_HOME: home });
	expect(loaded.gatewayDownAlert).toEqual({
		channelId: "room",
		mentionUserId: "morgan",
		logFile: join(home, "gateway.stderr.log"),
	});
	for (const bad of [{}, { channelId: "" }, { channelId: "room", afterMs: 0 }, { channelId: "room", logFile: 7 }]) {
		await write(bad);
		await expect(loadDiscordAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toThrow("gatewayDownAlert");
	}
});

test("the reconnect loop drives the alarm: failed connects count as down, a returning link as up", async () => {
	let now = 0;
	const sent: string[] = [];
	const channel = { send: async (text: string) => void sent.push(text) };
	const alarm = new GatewayDownAlarm(
		{ channelId: "room", mentionUserId: "morgan" },
		{
			now: () => now,
			post: async (text) => {
				await channel.send(text);
			},
			lastError: async () => undefined,
			log: quiet,
		},
	);
	const gateway = new ReconnectingGateway(
		join(home, "no-gateway.sock"),
		{ channels: { fetch: async () => channel } } as never,
		{ tokenFile: "token", token: "redacted", configPath: "config" } as never,
		undefined,
		undefined,
		join(home, "wired", "recovery-cursor.json"),
		() => undefined,
		undefined,
		async () => {},
		undefined,
		alarm,
	);
	await gateway.connect();
	now = 5 * MINUTE;
	// The first retry is scheduled within 625ms (500ms backoff plus jitter).
	for (let waited = 0; sent.length === 0 && waited < 3000; waited += 50) await Bun.sleep(50);
	expect(sent).toHaveLength(1);
	expect(sent[0]).toStartWith("<@morgan> 가재 중계 서버가");
	gateway.adoptClient({ request: async () => ({}), onChatMessage: () => () => {} } as never);
	for (let waited = 0; sent.length === 1 && waited < 1000; waited += 50) await Bun.sleep(50);
	expect(sent[1]).toBe("가재 중계 서버 돌아왔어(5분 꺼져 있었어).");
});
