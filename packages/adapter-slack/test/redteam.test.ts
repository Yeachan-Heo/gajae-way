import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, readdir, readFile, rename, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ChatMessagePayload, isPlatformMessageId } from "@gajaeway/protocol";
import {
	deliveryFailureIsAmbiguous,
	OutboundLimiter,
	SlackApiError,
	type SlackHistoryPage,
	SlackRateLimitedError,
	SlackUnreadableResponseError,
	SlackWebApi,
} from "../src/api";
import { loadSlackAdapterConfig } from "../src/config";
import { AdapterAlreadyRunningError, AdapterLock } from "../src/lock";
import {
	engagementForMessage,
	type GatewayClientLike,
	ReconnectingGateway,
	replyThreadTs,
	SKIPPED_SUBTYPES,
	settleSlackDelivery,
	settleSlackReaction,
	startSlackAdapter,
} from "../src/main";
import { chunkSlackMessage } from "../src/mrkdwn";
import { parseSlackMessageId, slackMessageOrigin } from "../src/origin";
import {
	loadRecoveryCursors,
	pruneParticipatedThreads,
	RECOVERY_PARTICIPATED_THREAD_TTL_MS,
	RecoveryScheduler,
	recoverConversation,
} from "../src/recovery";
import { SlackSocketMode, type WebSocketLike } from "../src/socket";
import { WorkingStatus } from "../src/status";
import { normalizeSlackText } from "../src/text";

// The adapter defaults its recovery store to $GAJAEWAY_HOME; a test must never
// be able to reach a real operator home, whatever a fixture forgets to pass.
process.env.GAJAEWAY_HOME = `/tmp/slack-test-home-${crypto.randomUUID()}`;

const origin = { platform: "slack", kind: "channel", conversationId: "C1" } as const;
const engagement = { mentioned: false, group: true, authorId: "U1" };
const names = { userName: () => undefined, userHandle: () => undefined, channelName: () => undefined };
const cleanups: Array<() => Promise<unknown> | void> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});
async function flush() {
	for (let i = 0; i < 100; i++) await Promise.resolve();
}
class Socket implements WebSocketLike {
	onopen: WebSocketLike["onopen"] = null;
	onmessage: WebSocketLike["onmessage"] = null;
	onclose: WebSocketLike["onclose"] = null;
	onerror: WebSocketLike["onerror"] = null;
	acks: string[] = [];
	send(data: string) {
		this.acks.push(data);
	}
	close() {}
	frame(frame: unknown) {
		this.onmessage?.({ data: JSON.stringify(frame) });
	}
}
class Client implements GatewayClientLike {
	calls: Array<{ verb: string; params: any }> = [];
	failure?: Error;
	engaged = false;
	async request<T>(verb: string, params?: unknown): Promise<T> {
		this.calls.push({ verb, params });
		if (this.failure) throw this.failure;
		return { engaged: this.engaged } as T;
	}
	onChatMessage() {
		return () => {};
	}
}
class Api extends SlackWebApi {
	posts: unknown[][] = [];
	reactions: unknown[][] = [];
	historyCalls = 0;
	unreadable = false;
	historyMessages: Record<string, unknown>[] = [];
	replyMessages: Record<string, unknown>[] = [];
	responses: unknown[] = [];
	constructor() {
		super("unused", async () => {
			throw new Error("network forbidden");
		});
	}
	override async authTest() {
		return { user_id: "UBOT", bot_id: "BBOT", team_id: "T1", user: "bot", team: "Test workspace" };
	}
	override async usersInfo(id: string) {
		return { id, name: id };
	}
	override async conversationsInfo(id: string) {
		return { id, name: id };
	}
	override async connectionsOpen() {
		return { url: "wss://invalid.test" };
	}
	override async conversationsHistory() {
		this.historyCalls++;
		if (this.unreadable) throw new SlackApiError(200, "missing_scope");
		return { messages: this.historyMessages, has_more: false };
	}
	override async conversationsReplies(
		_channel: string,
		_ts: string,
		_options?: { oldest?: string; cursor?: string; limit?: number },
	): Promise<SlackHistoryPage> {
		return { messages: this.replyMessages, has_more: false };
	}
	override async respond(_url: string, payload: Record<string, unknown>) {
		this.responses.push(payload);
	}
	override async postMessage(channel: string, text: string, threadTs?: string) {
		const args: [string, string, string?] = [channel, text, threadTs];
		this.posts.push(args);
		return { channel: args[0], ts: "2.0" };
	}
	override async addReaction(channel: string, ts: string, name: string) {
		this.reactions.push([channel, ts, name]);
	}
	removed: unknown[][] = [];
	override async removeReaction(channel: string, ts: string, name: string) {
		this.removed.push([channel, ts, name]);
	}
}
async function fixture(
	channels?: Record<string, { engagement: "open" }>,
	options: { nested?: boolean; keepRecovery?: boolean; now?: () => number } = {},
) {
	const home = await mkdtemp("/tmp/slack-recovery-");
	const cursorPath = options.nested ? join(home, "adapters/slack/recovery-cursor.json") : join(home, "cursor.json");
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const api = new Api();
	const adapter = await startSlackAdapter(
		{
			botToken: "xoxb-test",
			appToken: "xapp-test",
			botTokenFile: "bot",
			appTokenFile: "app",
			configPath: "unused",
			gatewaySocket: join(home, "absent.sock"),
			channels,
		},
		{
			api,
			recoveryCursorPath: cursorPath,
			now: options.now ?? (() => 10_000),
			log: { log() {}, error() {} },
			socketFactory: () => {
				const socket = new Socket();
				queueMicrotask(() => socket.onopen?.({}));
				return socket;
			},
		},
	);
	if (!options.keepRecovery) {
		adapter.recovery.stop();
		await adapter.recovery.idle();
		adapter.gateway.onConnected = undefined;
	}
	const client = new Client();
	adapter.gateway.adoptClient(client);
	if (options.keepRecovery) {
		await flush();
		await adapter.recovery.idle();
	}
	cleanups.push(() => {
		adapter.socket.stop();
		adapter.recovery.stop();
	});
	return {
		...adapter,
		api,
		client,
		home,
		cursorPath,
		async event(extra: Record<string, unknown>) {
			await adapter.handleEvent({ type: "message", channel: "C1", user: "U1", ts: "1.0", text: "body", ...extra });
			await adapter.ingress.drain();
			await flush();
		},
	};
}
function delivery(extra: Partial<ChatMessagePayload> = {}): ChatMessagePayload {
	return { turnId: "turn", role: "assistant", origin, text: "reply", final: true, deliveryId: "delivery", ...extra };
}

test("RT-SLACK-01 throwing handlers ack first, retries dedupe, malformed frames survive and disconnect reconnects", async () => {
	const sockets: Socket[] = [];
	const events: string[] = [];
	const mode = new SlackSocketMode(
		async () => ({ url: "wss://invalid.test" }),
		{
			onEvent(_event, envelope) {
				expect(sockets.at(-1)?.acks).toContain(JSON.stringify({ envelope_id: envelope.envelope_id }));
				events.push(envelope.envelope_id);
				if (envelope.envelope_id === "one") throw new Error("handler");
			},
			onSlashCommand() {},
		},
		{
			factory: () => {
				const socket = new Socket();
				sockets.push(socket);
				queueMicrotask(() => socket.onopen?.({}));
				return socket;
			},
			sleep: async () => {},
			log: { log() {}, error() {} },
		},
	);
	cleanups.push(() => mode.stop());
	await mode.start();
	const socket = sockets[0]!;
	socket.frame({ type: "events_api", envelope_id: "one", payload: { event: {} } });
	socket.frame({ type: "events_api", envelope_id: "one", retry_attempt: 1, payload: { event: {} } });
	socket.onmessage?.({ data: "{broken" });
	socket.frame({ type: "events_api", payload: { event: {} } });
	socket.frame({ type: "events_api", envelope_id: "two", payload: { event: {} } });
	await flush();
	expect(events).toEqual(["one", "two"]);
	expect(socket.acks).toHaveLength(3);
	socket.frame({ type: "disconnect" });
	await flush();
	expect(sockets).toHaveLength(2);
});

test("RT-SLACK-02 hostile timestamps never emit non-platform message ids", async () => {
	const f = await fixture();
	for (const ts of ["1.2.3", "abc", "", "1".repeat(200), "1.0\n", "C1:1.0"]) await f.event({ ts });
	expect(f.client.calls.filter((c) => c.verb === "chat.send")).toEqual([]);
	await f.event({ ts: "2.0" });
	for (const call of f.client.calls.filter((c) => c.verb === "chat.send"))
		expect(isPlatformMessageId(call.params.messageId)).toBe(true);
});

test("RT-SLACK-03 foreign reply target is a definitive delivery failure, never thread_ts", async () => {
	const api = new Api();
	const client = new Client();
	expect(parseSlackMessageId("C2:1.0")).toEqual({ channel: "C2", ts: "1.0" });
	expect(() => replyThreadTs(delivery({ replyToMessageId: "C2:1.0" }))).toThrow(/foreign channel/);
	await settleSlackDelivery(client, api, delivery({ replyToMessageId: "C2:1.0" }));
	expect(api.posts).toEqual([]);
	expect(client.calls).toEqual([
		{ verb: "delivery.fail", params: { deliveryId: "delivery", reason: expect.any(String), ambiguous: false } },
	]);
});

test("RT-SLACK-04 literal mentions in code count; entities decode once and broadcasts normalize", () => {
	expect(
		engagementForMessage(
			{ channel: "C1", ts: "1.0", text: "`<@UBOT>`", user: "U1" },
			origin,
			{ botUserId: "UBOT" },
			names,
			undefined,
		).mentioned,
	).toBe(true);
	expect(normalizeSlackText("&lt;script&gt; &amp;lt;@UBOT&amp;gt; <!channel> <!subteam^S1|@eng>", names)).toBe(
		"<script> &lt;@UBOT&gt; @channel @eng",
	);
});

test("RT-SLACK-05 fifty-thousand-character fenced text has bounded balanced chunks", () => {
	const chunks = chunkSlackMessage("```ts\n" + "x".repeat(50_000) + "\n```");
	expect(chunks.length).toBeGreaterThan(12);
	for (const chunk of chunks) {
		expect(chunk.length).toBeLessThanOrEqual(4000);
		expect((chunk.match(/^```/gm) ?? []).length % 2).toBe(0);
	}
});

test("RT-SLACK-06 file-only body survives and empty body drops", async () => {
	const f = await fixture();
	await f.event({
		text: "",
		files: [{ name: "report.pdf", mimetype: "application/pdf", url_private: "https://files.test/report", size: 20 }],
	});
	await f.event({ ts: "2.0", text: "", files: [] });
	const sends = f.client.calls.filter((c) => c.verb === "chat.send");
	expect(sends).toHaveLength(1);
	expect(sends[0]!.params.text).toContain("report.pdf");
});

test("RT-SLACK-07 own, hidden, every skipped subtype, unchanged/empty edits and app_mention never send", async () => {
	const f = await fixture();
	for (const extra of [
		{ user: "UBOT" },
		{ bot_id: "BBOT" },
		{ hidden: true },
		...[...SKIPPED_SUBTYPES].map((subtype) => ({ subtype })),
		{ type: "app_mention" },
		...["body", ""].map((text) => ({
			subtype: "message_changed",
			message: { channel: "C1", user: "U1", ts: "1.0", text },
			previous_message: { text: "body" },
		})),
	])
		await f.event(extra);
	expect(f.client.calls).toEqual([]);
});

test("RT-SLACK-08 bot parent mentions; unknown parent omits fromSelf", async () => {
	const f = await fixture();
	await f.event({ ts: "2.0", thread_ts: "1.0", parent_user_id: "UBOT" });
	await f.event({ ts: "3.0", thread_ts: "1.0" });
	const sends = f.client.calls.filter((c) => c.verb === "chat.send");
	expect(sends[0]!.params.engagement.mentioned).toBe(true);
	expect(sends[1]!.params.engagement.replyTo).not.toHaveProperty("fromSelf");
});

test("RT-SLACK-09 transport failure ambiguous, Slack API failure definitive", async () => {
	for (const [error, ambiguous] of [
		[new TypeError("transport"), true],
		[new SlackApiError(200, "channel_not_found"), false],
	] as const) {
		const client = new Client();
		await settleSlackDelivery(
			client,
			{
				async postMessage() {
					throw error;
				},
				async addReaction() {},
			},
			delivery(),
		);
		expect(client.calls[0]).toMatchObject({ verb: "delivery.fail", params: { ambiguous } });
	}
});

test("RT-SLACK-10 all three chunks keep thread and duplicate warning", async () => {
	const api = new Api();
	const client = new Client();
	await settleSlackDelivery(
		client,
		api,
		delivery({
			origin: { platform: "slack", kind: "thread", conversationId: "C1:1.0", parentId: "C1" },
			text: "x".repeat(9000),
			duplicateWarning: true,
		}),
	);
	expect(api.posts).toHaveLength(3);
	for (const post of api.posts) expect(post[2]).toBe("1.0");
	expect(api.posts[0]![1]).toStartWith("[recovered - may be a duplicate]");
	expect(client.calls[0]!.verb).toBe("delivery.confirm");
});

test("RT-SLACK-11 malformed and unknown reactions fail without API; already_reacted confirms", async () => {
	for (const reaction of [
		{ targetMessageId: "bad", emoji: "✅", emojiName: "check" },
		{ targetMessageId: "C1:1.0", emoji: "🚀", emojiName: "unknown" },
	]) {
		const api = new Api();
		const client = new Client();
		await settleSlackDelivery(client, api, delivery({ reaction } as Partial<ChatMessagePayload>));
		expect(api.reactions).toEqual([]);
		expect(client.calls[0]).toMatchObject({ verb: "delivery.fail", params: { ambiguous: false } });
	}
	const api = new SlackWebApi(
		"unused",
		async () => new Response(JSON.stringify({ ok: false, error: "already_reacted" })),
	);
	const client = new Client();
	await settleSlackDelivery(
		client,
		api,
		delivery({ reaction: { targetMessageId: "C1:1.0", emoji: "✅", emojiName: "check" } }),
	);
	expect(client.calls[0]!.verb).toBe("delivery.confirm");
});

test("RT-SLACK-12 disconnected edit overflow retains newest 256 and replays ordered", async () => {
	const log = spyOn(console, "error").mockImplementation(() => {});
	cleanups.push(() => log.mockRestore());
	const adapter = new ReconnectingGateway("/tmp/no-redteam.sock", new Api());
	for (let i = 0; i < 300; i++) adapter.sendEdit(`C1:${i}.0`, origin, `edit-${i}`, engagement);
	await flush();
	expect(adapter.pendingEdits).toHaveLength(256);
	expect(adapter.pendingEdits[0]!.text).toBe("edit-44");
	expect(log.mock.calls.filter((c) => String(c[0]).includes("dropped the oldest"))).toHaveLength(44);
	const client = new Client();
	adapter.adoptClient(client);
	for (let i = 0; i < 10; i++) await flush();
	expect(client.calls.filter((c) => c.verb === "chat.edit").map((c) => c.params.text)).toEqual(
		Array.from({ length: 256 }, (_, i) => `edit-${i + 44}`),
	);
	expect(adapter.pendingEdits).toEqual([]);
});

test("RT-SLACK-13 failed edit remains queued and reconnects for replay", async () => {
	const client = new Client();
	client.failure = new Error("offline");
	const adapter = new ReconnectingGateway("/tmp/no-redteam.sock", new Api(), client);
	adapter.sendEdit("C1:1.0", origin, "edit", engagement);
	await flush();
	expect(adapter.connected).toBe(false);
	expect(adapter.pendingEdits).toHaveLength(1);
	const healthy = new Client();
	adapter.adoptClient(healthy);
	await flush();
	expect(healthy.calls[0]!.verb).toBe("chat.edit");
	expect(adapter.pendingEdits).toEqual([]);
});

test("RT-SLACK-14 duplicate inbound dedupes but unavailable is forgotten", async () => {
	const client = new Client();
	const adapter = new ReconnectingGateway("/tmp/no-redteam.sock", new Api(), client);
	expect((await adapter.requestRecovered("C1:1.0", origin, "x", engagement)).verdict).toBe("acked");
	expect((await adapter.requestRecovered("C1:1.0", origin, "x", engagement)).verdict).toBe("duplicate");
	expect(client.calls).toHaveLength(1);
	client.failure = new Error("offline");
	expect((await adapter.requestRecovered("C1:2.0", origin, "x", engagement)).verdict).toBe("unavailable");
	const healthy = new Client();
	adapter.adoptClient(healthy);
	expect((await adapter.requestRecovered("C1:2.0", origin, "x", engagement)).verdict).toBe("acked");
	expect(healthy.calls).toHaveLength(1);
});

test("RT-SLACK-15 newest-first recovery delivers ascending and cannot cross unavailable", async () => {
	const seen: string[] = [];
	const result = await recoverConversation(
		{
			async history(_channel, options) {
				return options.cursor
					? { messages: [{ ts: "1.0", user: "U1" }], has_more: false }
					: {
							messages: [
								{ ts: "3.0", user: "U1" },
								{ ts: "2.0", user: "U1" },
							],
							has_more: true,
							next_cursor: "older",
						};
			},
			async replies() {
				return { messages: [], has_more: false };
			},
		},
		"C1",
		{
			nowMs: 1000,
			cursor: "0.0",
			botUserId: "UBOT",
			async deliver(message) {
				seen.push(message.ts);
				return message.ts === "2.0" ? "unavailable" : "acked";
			},
		},
	);
	expect(seen).toEqual(["1.0", "2.0"]);
	expect(result.advancedTo).toBe("1.0");
	expect(result.failed).toBe(true);
});

test("RT-SLACK-16 recovered replies retain thread origin", async () => {
	const origins: unknown[] = [];
	await recoverConversation(
		{
			async history() {
				return { messages: [{ ts: "1.0", user: "UBOT", reply_count: 1 }], has_more: false };
			},
			async replies() {
				return { messages: [{ ts: "2.0", user: "U1" }], has_more: false };
			},
		},
		"C1",
		{
			nowMs: 1000,
			cursor: "0.0",
			botUserId: "UBOT",
			async deliver(message) {
				origins.push(slackMessageOrigin(message));
				return "acked";
			},
		},
	);
	expect(origins).toEqual([{ platform: "slack", kind: "thread", conversationId: "C1:1.0", parentId: "C1" }]);
});

test("RT-SLACK-17 three-strike quarantine is re-probed after gateway reconnect", async () => {
	const f = await fixture({ C1: { engagement: "open" } }, { keepRecovery: true });
	const before = f.api.historyCalls;
	f.api.unreadable = true;
	for (let i = 0; i < 3; i++) await f.recoverMissedMessages();
	expect((await loadRecoveryCursors(join(f.home, "cursor.json"))).quarantined.C1?.failures).toBe(3);
	f.api.unreadable = false;
	f.gateway.adoptClient(new Client());
	await flush();
	await f.recovery.idle();
	expect(f.api.historyCalls).toBe(before + 4);
	expect((await loadRecoveryCursors(join(f.home, "cursor.json"))).quarantined.C1).toBeUndefined();
});

test("RT-SLACK-18 twenty recovery triggers remain single flight", async () => {
	let active = 0;
	let peak = 0;
	let passes = 0;
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const scheduler = new RecoveryScheduler(async () => {
		active++;
		peak = Math.max(peak, active);
		passes++;
		await gate;
		active--;
		return true;
	});
	cleanups.push(() => scheduler.stop());
	for (let i = 0; i < 20; i++) scheduler.trigger();
	expect(passes).toBe(1);
	release();
	await flush();
	expect(peak).toBe(1);
	expect(passes).toBe(2);
});

test("RT-SLACK-19 credential prefixes fail without secrets, relative files resolve and unknown channel keys reject", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-config-redteam-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const config = { botTokenFile: "bot", appTokenFile: "app" };
	await Bun.write(join(home, "adapter-slack.json"), JSON.stringify(config));
	await Bun.write(join(home, "bot"), "xoxb-good\n");
	await Bun.write(join(home, "app"), "xapp-good\n");
	const loaded = await loadSlackAdapterConfig({ GAJAEWAY_HOME: home });
	expect(loaded.botTokenFile).toBe(join(home, "bot"));
	expect(loaded.appTokenFile).toBe(join(home, "app"));
	for (const [file, good] of [
		["bot", "xoxb-good"],
		["app", "xapp-good"],
	]) {
		const secret = "wrong-prefix-sensitive-token";
		await Bun.write(join(home, file!), secret);
		let error: unknown;
		try {
			await loadSlackAdapterConfig({ GAJAEWAY_HOME: home });
		} catch (caught) {
			error = caught;
		}
		expect(error).toBeInstanceOf(Error);
		expect(String(error)).not.toContain(secret);
		await Bun.write(join(home, file!), good!);
	}
	await Bun.write(
		join(home, "adapter-slack.json"),
		JSON.stringify({ ...config, channels: { C1: { engagement: "open", unknown: true } } }),
	);
	await expect(loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toThrow("channels");
});

test("RT-SLACK-25 already_reacted Web API response confirms delivery", async () => {
	const api = new SlackWebApi(
		"unused",
		async () => new Response(JSON.stringify({ ok: false, error: "already_reacted" })),
	);
	const client = new Client();
	await settleSlackDelivery(
		client,
		api,
		delivery({ reaction: { targetMessageId: "C1:1.0", emoji: "✅", emojiName: "check" } }),
	);
	expect(client.calls).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery" } }]);
});

test("RT-SLACK-30 pending sends share failure and survive client adoption", async () => {
	for (const adoptWhilePending of [false, true]) {
		const client = new Client();
		let reject!: (error: Error) => void;
		const gate = new Promise<never>((_resolve, fail) => {
			reject = fail;
		});
		client.request = async <T>(verb: string): Promise<T> => {
			expect(verb).toBe("chat.send");
			return gate;
		};
		const adapter = new ReconnectingGateway("/tmp/no-redteam.sock", new Api(), client);
		const live = adapter.requestInbound("C1:1.0", origin, "body", engagement);
		const recovered = adapter.requestRecovered("C1:1.0", origin, "body", engagement);
		const healthy = new Client();
		if (adoptWhilePending) adapter.adoptClient(healthy);
		reject(new Error("gateway connection closed"));
		expect(await live).toBeUndefined();
		expect((await recovered).verdict).toBe("unavailable");
		if (!adoptWhilePending) adapter.adoptClient(healthy);
		expect((await adapter.requestRecovered("C1:1.0", origin, "body", engagement)).verdict).toBe("acked");
		expect((await adapter.requestRecovered("C1:1.0", origin, "body", engagement)).verdict).toBe("duplicate");
		expect(healthy.calls.filter((c) => c.verb === "chat.send")).toHaveLength(1);
	}
});

test("RT-SLACK-31 unreadable success bodies are ambiguous but explicit refusal is definitive", async () => {
	const responses = [
		() => new Response("{}"),
		() => new Response('{"ok":1}'),
		() =>
			new Response(
				new ReadableStream({
					start(controller) {
						controller.error(new Error("broken body"));
					},
				}),
			),
	];
	for (const response of responses) {
		const api = new SlackWebApi("unused", async () => response());
		let error: unknown;
		try {
			await api.call("chat.postMessage");
		} catch (caught) {
			error = caught;
		}
		expect(error).toBeInstanceOf(SlackUnreadableResponseError);
		expect(error).not.toBeInstanceOf(SlackApiError);
		expect(deliveryFailureIsAmbiguous(error)).toBe(true);
		const client = new Client();
		await settleSlackDelivery(client, api, delivery());
		expect(client.calls).toEqual([
			{ verb: "delivery.fail", params: { deliveryId: "delivery", ambiguous: true, reason: expect.any(String) } },
		]);
	}
	const api = new SlackWebApi("unused", async () => new Response('{"ok":false,"error":"x"}'));
	await expect(api.call("chat.postMessage")).rejects.toMatchObject({ name: "SlackApiError", code: "x" });
	const client = new Client();
	await settleSlackDelivery(client, api, delivery());
	expect(client.calls[0]).toMatchObject({ verb: "delivery.fail", params: { ambiguous: false } });
});

test("RT-SLACK-33 slack stale lock has exactly one winner under 20 concurrent reclaims", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-lock-redteam-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const path = join(home, "adapter-slack.pid");
	await writeFile(path, "99999\n");
	const results = await Promise.allSettled(
		Array.from({ length: 20 }, (_, i) => AdapterLock.acquire(home, { pid: i + 1, alive: () => false })),
	);
	const winners = results.filter((r) => r.status === "fulfilled");
	expect(winners).toHaveLength(1);
	for (const result of results)
		if (result.status === "rejected") expect(result.reason).toBeInstanceOf(AdapterAlreadyRunningError);
	expect((await readFile(path, "utf8")).trim()).toBe(String(winners[0]?.value.pid));
});

test("RT-SLACK-35 bounded continuation closes the gap without losing newer arrivals", async () => {
	const messages = [6, 5, 4, 3, 2, 1].map((n) => ({ ts: `${n}.0`, user: "U1", text: `message ${n}` }));
	const seen: string[] = [];
	const port = {
		async history(_channel: string, options: { oldest: string; latest?: string; cursor?: string; limit: number }) {
			const filtered = messages.filter(
				(m) => Number(m.ts) > Number(options.oldest) && (!options.latest || Number(m.ts) < Number(options.latest)),
			);
			const offset = Number(options.cursor ?? 0);
			const more = offset + options.limit < filtered.length;
			return {
				messages: filtered.slice(offset, offset + options.limit),
				has_more: more,
				...(more ? { next_cursor: String(offset + options.limit) } : {}),
			};
		},
		async replies() {
			return { messages: [], has_more: false };
		},
	};
	const options = {
		nowMs: 10_000,
		cursor: "0.0",
		pageLimit: 2,
		maxPages: 2,
		botUserId: "UBOT",
		async deliver(message: { ts: string }) {
			seen.push(message.ts);
			return "acked" as const;
		},
	};
	const first = await recoverConversation(port, "C1", options);
	expect(seen).toEqual(["3.0", "4.0", "5.0", "6.0"]);
	expect(first.advancedTo).toBeUndefined();
	expect(first.continuation).toEqual({ olderThan: "3.0", through: "6.0" });
	if (!first.continuation) throw new Error("Expected a continuation for the bounded first pass");
	messages.unshift({ ts: "7.0", user: "U1", text: "new arrival" });
	const second = await recoverConversation(port, "C1", { ...options, latest: first.continuation.olderThan });
	expect(second.continuation).toBeUndefined();
	expect(second.failed).toBe(false);
	expect(seen.slice(4)).toEqual(["1.0", "2.0"]);
	const third = await recoverConversation(port, "C1", { ...options, cursor: first.continuation.through });
	expect(third.advancedTo).toBe("7.0");
	expect(seen).toEqual(["3.0", "4.0", "5.0", "6.0", "1.0", "2.0", "7.0"]);
});

test("RT-SLACK-36 participated threads recover without history and expire", async () => {
	const f = await fixture({ C1: { engagement: "open" } });
	f.client.engaged = true;
	await f.event({ ts: "2.0", thread_ts: "1.0" });
	await f.recoverMissedMessages();
	let state = await loadRecoveryCursors(f.cursorPath);
	expect(state.participatedThreads["C1:1.0"]).toBeDefined();
	f.api.replyMessages = [{ ts: "3.0", user: "U1", text: "late thread reply" }];
	expect(await f.recoverMissedMessages()).toBe(true);
	expect(f.api.historyMessages).toEqual([]);
	expect(f.client.calls.filter((c) => c.verb === "chat.send").at(-1)?.params).toMatchObject({
		messageId: "C1:3.0",
		origin: { kind: "thread", conversationId: "C1:1.0", parentId: "C1" },
	});
	state = await loadRecoveryCursors(f.cursorPath);
	expect(pruneParticipatedThreads(state, 10_001 + RECOVERY_PARTICIPATED_THREAD_TTL_MS).participatedThreads).toEqual({});
});

test("RT-SLACK-37 only terminal refusals consume dead-letter budget", async () => {
	const f = await fixture({ C1: { engagement: "open" } });
	f.api.historyMessages = [{ ts: "1.0", user: "U1", text: "poison" }];
	const terminal = Object.assign(new Error("refused"), { code: "invalid_params" });
	for (const [error, attempts] of [
		[terminal, 1],
		[new Error("connection closed"), 1],
		[terminal, 2],
		[terminal, 3],
	] as const) {
		const client = new Client();
		client.failure = error;
		f.gateway.adoptClient(client);
		await f.recoverMissedMessages();
		const state = await loadRecoveryCursors(f.cursorPath);
		if (attempts < 3) {
			expect(state.attempts["C1:1.0"]?.attempts).toBe(attempts);
			expect(state.deadLetters).toEqual([]);
			expect(state.recoveredThrough.C1).toBeUndefined();
		}
	}
	const state = await loadRecoveryCursors(f.cursorPath);
	expect(state.deadLetters).toHaveLength(1);
	expect(state.deadLetters[0]).toMatchObject({ classification: "terminal-message", attempts: 3, messageId: "C1:1.0" });
	expect(state.deadLetterDigest.C1?.count).toBe(1);
	expect(state.recoveredThrough.C1).toBe("1.0");
	f.api.historyMessages = [{ ts: "2.0", user: "U1", text: "unknown" }];
	for (let i = 0; i < 7; i++) {
		const client = new Client();
		client.failure = new Error("boom");
		f.gateway.adoptClient(client);
		expect(await f.recoverMissedMessages()).toBe(false);
	}
	const unknown = await loadRecoveryCursors(f.cursorPath);
	expect(unknown.attempts["C1:2.0"]).toMatchObject({ attempts: 0, classification: "write-path-unknown" });
	expect(unknown.deadLetters).toHaveLength(1);
	expect(unknown.recoveredThrough.C1).toBe("1.0");
});

test("RT-SLACK-38 unsaved recovery state prevents success until disk is restored", async () => {
	const f = await fixture({ C1: { engagement: "open" } }, { nested: true });
	await f.recoverMissedMessages();
	const store = join(f.home, "adapters/slack");
	await rm(store, { recursive: true });
	await writeFile(store, "blocked directory");
	f.api.historyMessages = [{ ts: "1.0", user: "U1", text: "persist me" }];
	expect(await f.recoverMissedMessages()).toBe(false);
	await rm(store);
	await mkdir(store);
	expect(await f.recoverMissedMessages()).toBe(true);
	expect((await loadRecoveryCursors(f.cursorPath)).recoveredThrough.C1).toBe("1.0");
	expect(f.client.calls.filter((c) => c.verb === "chat.send")).toHaveLength(1);
});

test("RT-SLACK-39 slash acknowledgements reflect restart failure duplicates and unknown commands", async () => {
	const f = await fixture();
	f.client.engaged = true;
	const command = {
		command: "/restart",
		channel_id: "C1",
		user_id: "U1",
		trigger_id: "one",
		response_url: "https://invalid.test/response",
		text: "",
	};
	await f.handleSlashCommand(command);
	await f.handleSlashCommand(command);
	f.client.failure = new Error("gateway connection closed");
	await f.handleSlashCommand({ ...command, trigger_id: "two" });
	await f.handleSlashCommand({ ...command, command: "/unknown", trigger_id: "three" });
	expect(f.api.responses).toEqual(
		[
			"🦞 restarting the gateway",
			"already handled",
			"the gateway is unreachable right now; try again shortly",
			"unknown command",
		].map((text) => ({ response_type: "ephemeral", text })),
	);
});

test("RT-SLACK-40 addressed accepted turns get a presence reaction on the triggering message before any progress", async () => {
	const api = new Api();
	const client = new Client();
	client.engaged = true;
	const status = new WorkingStatus(api);
	const gateway = new ReconnectingGateway("/tmp/no-redteam.sock", api, client, status);
	await gateway.requestInbound("C1:1.0", origin, "overheard", engagement);
	await flush();
	expect(api.reactions).toEqual([]);
	expect(api.posts).toEqual([]);
	await gateway.requestInbound("C1:2.0", origin, "addressed", { ...engagement, mentioned: true });
	await flush();
	// Presence is a reaction on the message itself, never a posted message.
	expect(api.reactions).toEqual([["C1", "2.0", "hourglass_flowing_sand"]]);
	expect(api.posts).toEqual([]);
	const thread = { platform: "slack", kind: "thread", conversationId: "C1:1.0", parentId: "C1" } as const;
	await gateway.requestInbound("C1:3.0", thread, "thread", { ...engagement, mentioned: true });
	await flush();
	expect(api.reactions[1]).toEqual(["C1", "3.0", "hourglass_flowing_sand"]);
	await status.clear("C1");
	await status.clear("C1:1.0");
	expect(api.removed).toEqual([
		["C1", "2.0", "hourglass_flowing_sand"],
		["C1", "3.0", "hourglass_flowing_sand"],
	]);
});

test("RT-SLACK-41 unknown rocket reaction fails definitively without a Slack call", async () => {
	const api = new Api();
	const client = new Client();
	await settleSlackReaction(
		client,
		api,
		delivery({
			reaction: { targetMessageId: "C1:1.0", emoji: "🚀", emojiName: "rocket" },
		} as Partial<ChatMessagePayload>),
	);
	expect(api.reactions).toEqual([]);
	expect(client.calls).toEqual([
		{ verb: "delivery.fail", params: { deliveryId: "delivery", reason: expect.any(String), ambiguous: false } },
	]);
});
test("RT-SLACK-42 orphaned reclaim marker permits acquisition within two seconds and is removed", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-orphan-redteam-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const path = join(home, "adapter-slack.pid");
	await writeFile(path, "99999\n");
	// An election directory left by a crashed reclaimer, old enough to be judged abandoned.
	await mkdir(`${path}.reclaim.d`);
	await writeFile(`${path}.reclaim.d/owner`, "99998\n");
	const aged = new Date(Date.now() - 2000);
	await utimes(`${path}.reclaim.d`, aged, aged);
	const started = performance.now();
	const lock = await AdapterLock.acquire(home, { pid: 42, alive: () => false });
	expect(performance.now() - started).toBeLessThan(2000);
	expect(await readFile(path, "utf8")).toBe("42\n");
	await expect(stat(`${path}.reclaim.d`)).rejects.toMatchObject({ code: "ENOENT" });
	// No tombstones are left behind either.
	expect((await readdir(home)).filter((entry) => entry.endsWith(".dead"))).toEqual([]);
	await lock.release();
});

test("RT-SLACK-43 live holder arriving during election is never replaced by waiting contenders", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-live-election-redteam-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const path = join(home, "adapter-slack.pid");
	await writeFile(path, "99999\n");
	await mkdir(`${path}.reclaim.d`);
	await writeFile(`${path}.reclaim.d/owner`, "99998\n");
	let probes = 0;
	const pending = Promise.allSettled(
		Array.from({ length: 20 }, (_, i) =>
			AdapterLock.acquire(home, {
				pid: i + 1,
				alive: (pid) => {
					if (pid === 99999) probes++;
					return pid === process.pid;
				},
			}),
		),
	);
	const deadline = performance.now() + 500;
	while (probes < 20 && performance.now() < deadline) await Bun.sleep(1);
	const waiting = probes;
	await writeFile(path, `${process.pid}\n`);
	const results = await pending;
	expect(waiting).toBe(20);
	for (const result of results) {
		expect(result.status).toBe("rejected");
		if (result.status === "rejected") {
			expect(result.reason).toBeInstanceOf(AdapterAlreadyRunningError);
			expect(result.reason.holderPid).toBe(process.pid);
		}
	}
	expect(await readFile(path, "utf8")).toBe(`${process.pid}\n`);
});

test("RT-SLACK-44 release by a former holder after losing election preserves the winner pidfile", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-release-redteam-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const former = await AdapterLock.acquire(home, { pid: 100, alive: () => false });
	const results = await Promise.allSettled(
		Array.from({ length: 20 }, (_, i) =>
			AdapterLock.acquire(home, {
				pid: i + 1,
				alive: () => false,
			}),
		),
	);
	const winners = results.filter((result) => result.status === "fulfilled");
	expect(winners).toHaveLength(1);
	await expect(AdapterLock.acquire(home, { pid: 100, alive: () => true })).rejects.toBeInstanceOf(
		AdapterAlreadyRunningError,
	);
	await former.release();
	expect(await readFile(former.path, "utf8")).toBe(`${winners[0]?.value.pid}\n`);
	await winners[0]?.value.release();
	await expect(readFile(former.path)).rejects.toMatchObject({ code: "ENOENT" });
});

for (const live of [true, false]) {
	test(`RT-SLACK-45 ${live ? "live 0.9s marker survives timeout naming new holder" : "two waiters reclaim old dead marker with one winner"}`, async () => {
		const home = await mkdtemp(join(tmpdir(), "slack-marker-g4-"));
		cleanups.push(() => rm(home, { recursive: true, force: true }));
		const path = join(home, "adapter-slack.pid");
		const marker = `${path}.reclaim.d`;
		await writeFile(path, "99999\n");
		await mkdir(marker);
		await writeFile(join(marker, "owner"), "88888\n");
		const aged = new Date(Date.now() - (live ? 900 : 2000));
		await utimes(marker, aged, aged);
		const inode = (await stat(marker)).ino;
		const pending = Promise.allSettled(
			[1, 2].map((pid) =>
				AdapterLock.acquire(home, { pid, alive: (owner) => live && (owner === 88888 || owner === 77777) }),
			),
		);
		if (live) {
			await Bun.sleep(30);
			await writeFile(path, "77777\n");
		}
		const results = await pending;
		if (live) {
			expect((await stat(marker)).ino).toBe(inode);
			expect(await readFile(join(marker, "owner"), "utf8")).toBe("88888\n");
			for (const result of results) {
				expect(result.status).toBe("rejected");
				if (result.status === "rejected") {
					expect(result.reason.holderPid).toBe(77777);
					expect(result.reason.message).toContain("77777");
				}
			}
		} else {
			const winners = results.filter((result) => result.status === "fulfilled");
			expect(winners).toHaveLength(1);
			expect(await readFile(path, "utf8")).toBe(`${winners[0]?.value.pid}\n`);
			await expect(stat(marker)).rejects.toMatchObject({ code: "ENOENT" });
		}
	});
}

for (const [failures, header, wait] of [
	[4, "2", 2000],
	[2, "2", 2000],
	[2, "99", 30000],
	[2, null, 1000],
] as const) {
	test(`RT-SLACK-47 rate-limit settlement ${failures} refusals Retry-After ${header}`, async () => {
		let requests = 0;
		const sleeps: number[] = [];
		const api = new SlackWebApi("unused", {
			fetcher: async () =>
				++requests <= failures
					? new Response("", { status: 429, headers: header ? { "Retry-After": header } : {} })
					: Response.json({ ok: true, channel: "C1", ts: "2.0" }),
			sleep: async (ms) => {
				sleeps.push(ms);
			},
		});
		const client = new Client();
		await settleSlackDelivery(client, api, delivery());
		expect(requests).toBe(failures === 4 ? 4 : 3);
		expect(sleeps).toEqual(Array(failures === 4 ? 3 : 2).fill(wait));
		expect(client.calls).toHaveLength(1);
		expect(client.calls[0]?.verb).toBe(failures === 4 ? "delivery.fail" : "delivery.confirm");
		if (failures === 4) {
			expect(client.calls[0]?.params.ambiguous).toBe(true);
			requests = 0;
			const error = await api.postMessage("C1", "again").catch((error) => error);
			expect(error).toBeInstanceOf(SlackRateLimitedError);
			expect(error).not.toBeInstanceOf(SlackApiError);
		}
	});
}

test("RT-SLACK-48 concurrent delivery queue drains before cosmetic without starvation across channels", async () => {
	let now = 0;
	const timers: Array<{ at: number; resolve: () => void }> = [];
	const limiter = new OutboundLimiter(
		100,
		() => now,
		(ms) => new Promise<void>((resolve) => timers.push({ at: now + ms, resolve })),
	);
	const order: string[] = [];
	const work = Array.from({ length: 5 }, (_, i) =>
		limiter.acquire("C1", "delivery").then(() => {
			order.push(`delivery-${i}`);
		}),
	);
	work.push(
		limiter.acquire("C1", "cosmetic").then(() => {
			order.push("cosmetic");
		}),
	);
	await limiter.acquire("C2", "delivery");
	expect(now).toBe(0);
	await flush();
	while (timers.length) {
		timers.sort((a, b) => a.at - b.at);
		const timer = timers.shift()!;
		now = timer.at;
		timer.resolve();
		await flush();
	}
	await Promise.all(work);
	expect(order).toEqual(["delivery-0", "delivery-1", "delivery-2", "delivery-3", "delivery-4", "cosmetic"]);
	// Right after the last slot fires only its cooldown remains; once that elapses the lane is idle.
	expect(limiter.pendingMs("C1")).toBeLessThanOrEqual(100);
	now += 100;
	expect(limiter.pendingMs("C1")).toBe(0);
});

test("RT-SLACK-49 presence coalesces thirty ticks into a bounded number of reaction swaps and clears once", async () => {
	let now = 0;
	const api = new Api();
	const status = new WorkingStatus(
		api,
		console,
		() => ({}),
		() => {},
		() => now,
	);
	status.arm(origin, "C1:1.0");
	await flush();
	expect(api.reactions).toEqual([["C1", "1.0", "hourglass_flowing_sand"]]);
	// Thirty ticks in twenty seconds, phase flipping every tick: at most one swap
	// per 15s window, and no swap at all while the buckets are unchanged.
	for (let i = 1; i <= 30; i++) {
		now = (i * 20000) / 30;
		await status.update({
			turnId: "turn",
			origin,
			elapsedMs: now,
			toolCalls: 0,
			outputTokens: 0,
			final: false,
			activity: i % 2 ? { kind: "tool", label: "bash" } : { kind: "thinking", label: "thinking" },
		});
	}
	expect(api.removed.length).toBeLessThanOrEqual(2);
	expect(api.reactions.length).toBeLessThanOrEqual(3);
	expect(api.posts).toEqual([]);
	// Clear removes every marker we own, exactly once, and ignores a second clear.
	await status.clear("C1");
	const removedAfterClear = api.removed.length;
	await status.clear("C1");
	expect(api.removed.length).toBe(removedAfterClear);
	const shown = new Set(api.reactions.map((r) => r[2]));
	for (const name of shown) expect(api.removed.some((r) => r[2] === name)).toBe(true);
});

test("RT-SLACK-51 unengaged truncated reply walks persist and drain pending roots without aging participation", async () => {
	let now = 10000;
	const f = await fixture({ C1: { engagement: "open" } }, { now: () => now });
	f.api.historyMessages = [{ ts: "1.0", user: "U1", text: "root", reply_count: 100 }];
	let truncated = true;
	spyOn(f.api, "conversationsReplies").mockImplementation(async () => ({
		messages: [{ ts: "2.0", user: "U1", text: "unengaged", thread_ts: "1.0" }],
		has_more: truncated,
		next_cursor: truncated ? "next" : undefined,
	}));
	expect(await f.recoverMissedMessages()).toBe(false);
	let state = await loadRecoveryCursors(f.cursorPath);
	expect(state.pendingThreads["C1:1.0"]).toBeDefined();
	expect(state.participatedThreads["C1:1.0"]).toBeUndefined();
	truncated = false;
	f.api.historyMessages = [];
	expect(await f.recoverMissedMessages()).toBe(true);
	state = await loadRecoveryCursors(f.cursorPath);
	expect(state.pendingThreads["C1:1.0"]).toBeUndefined();
	f.client.engaged = true;
	await f.event({ ts: "3.0", thread_ts: "1.0" });
	await f.recoverMissedMessages();
	const before = (await loadRecoveryCursors(f.cursorPath)).participatedThreads["C1:1.0"]!.lastSeenAt;
	spyOn(f.api, "conversationsReplies").mockImplementationOnce(async () => {
		now = 20000;
		await f.event({ ts: "4.0", thread_ts: "1.0" });
		return { messages: [{ ts: "5.0", user: "U1", text: "walked", thread_ts: "1.0" }], has_more: false };
	});
	await f.recoverMissedMessages();
	expect((await loadRecoveryCursors(f.cursorPath)).participatedThreads["C1:1.0"]!.lastSeenAt >= before).toBe(true);
	expect((await loadRecoveryCursors(f.cursorPath)).participatedThreads["C1:1.0"]!.lastSeenAt).toBe(
		new Date(20000).toISOString(),
	);
});

for (const [outage, quarantine] of [
	[2000, false],
	[6000, false],
	[2000, true],
] as const) {
	test(`RT-SLACK-52 recent clean recovery gate outage ${outage} quarantine ${quarantine}`, async () => {
		let now = 10000;
		const f = await fixture({ C1: { engagement: "open" } }, { keepRecovery: true, now: () => now });
		await f.recoverMissedMessages();
		if (quarantine) {
			f.api.unreadable = true;
			for (let i = 0; i < 3; i++) await f.recoverMissedMessages();
			f.api.unreadable = false;
		}
		now = 20000 - outage;
		f.gateway.onDisconnected?.();
		now = 20000;
		const before = f.api.historyCalls;
		f.gateway.onConnected?.();
		await f.recovery.idle();
		expect(f.api.historyCalls - before).toBe(outage >= 5000 || quarantine ? 1 : 0);
	});
}

test("RT-SLACK-53 gradient buckets coalescing exact cleanup and stale timeout", async () => {
	let now = 0;
	let stale = () => {};
	const api = new Api();
	const status = new WorkingStatus(
		api,
		console,
		(fn) => {
			stale = fn;
			return {};
		},
		() => {},
		() => now,
	);
	status.arm(origin, "C1:1.0");
	await flush();
	expect(api.reactions).toEqual([["C1", "1.0", "hourglass_flowing_sand"]]);
	for (let i = 1; i <= 30; i++) {
		now = (i * 20000) / 30;
		await status.update({
			turnId: "t",
			origin,
			elapsedMs: now,
			toolCalls: 0,
			outputTokens: 0,
			final: false,
			activity: { kind: i % 2 ? "tool" : "thinking", label: "phase" },
		});
	}
	expect(api.reactions.length).toBeLessThanOrEqual(3);
	expect(api.removed.length).toBeLessThanOrEqual(2);
	for (const [time, tools, tokens, clock, effort] of [
		[61000, 3, 0, "clock1", "three"],
		[121000, 40, 0, "clock2", "100"],
		[181000, 0, 5000, "clock3", "keycap_ten"],
	] as const) {
		now = time;
		await status.update({ turnId: "t", origin, elapsedMs: now, toolCalls: tools, outputTokens: tokens, final: false });
		expect(api.reactions.map((r) => r[2])).toContain(clock);
		expect(api.reactions.map((r) => r[2])).toContain(effort);
	}
	await status.clear("C1");
	expect(api.removed.map((r) => JSON.stringify(r)).sort()).toEqual(api.reactions.map((r) => JSON.stringify(r)).sort());
	const count = api.removed.length;
	await status.clear("C1");
	expect(api.removed).toHaveLength(count);
	status.arm(origin, "C1:2.0");
	await flush();
	stale();
	await flush();
	expect(api.removed.at(-1)).toEqual(["C1", "2.0", "hourglass_flowing_sand"]);
	expect(api.posts).toEqual([]);
	expect(api.posts).toEqual([]);
});

test("RT-SLACK-54 clear during add removes late marker and replacement cleans old message", async () => {
	const api = new Api();
	let release!: () => void;
	const original = api.addReaction.bind(api);
	spyOn(api, "addReaction").mockImplementationOnce(async (...args) => {
		await new Promise<void>((r) => {
			release = r;
		});
		await original(...args);
	});
	const status = new WorkingStatus(api);
	status.arm(origin, "C1:1.0");
	await status.clear("C1");
	release();
	await flush();
	expect(api.removed).toEqual(api.reactions);
	status.arm(origin, "C1:2.0");
	await flush();
	status.arm(origin, "C1:3.0");
	await flush();
	expect(api.removed).toContainEqual(["C1", "2.0", "hourglass_flowing_sand"]);
	await status.clear("C1");
	expect(api.removed).toEqual(api.reactions);
});

test("RT-SLACK-54 cleanup errors are logged without blocking delivery confirmation", async () => {
	const api = new Api();
	const errors: unknown[] = [];
	const status = new WorkingStatus(api, {
		error: (...args) => {
			errors.push(args);
		},
	});
	status.arm(origin, "C1:1.0");
	await flush();
	spyOn(api, "removeReaction").mockRejectedValue(new Error("remove denied"));
	const client = new Client();
	await settleSlackDelivery(client, api, delivery(), console, status);
	expect(client.calls).toContainEqual({ verb: "delivery.confirm", params: { deliveryId: "delivery" } });
	expect(errors.length).toBeGreaterThan(0);
});

test("RT-SLACK-57 persona allowlist is disjoint and thumbs up leaves presence alone", async () => {
	const { REACTION_ALLOWLIST } = await import("@gajaeway/protocol");
	const { SLACK_REACTION_NAMES } = await import("../src/reactions");
	const { isPresenceReaction } = await import("../src/status");
	for (const { name } of REACTION_ALLOWLIST) expect(isPresenceReaction(SLACK_REACTION_NAMES[name]!)).toBe(false);
	const api = new Api();
	const status = new WorkingStatus(api);
	status.arm(origin, "C1:1.0");
	await flush();
	await settleSlackReaction(
		new Client(),
		api,
		delivery({ reaction: { targetMessageId: "C1:1.0", emoji: "👍", emojiName: "thumbsup" } }),
	);
	expect(api.reactions).toContainEqual(["C1", "1.0", "+1"]);
	expect(api.removed).toEqual([]);
	await status.clear("C1");
});

for (const becomesLive of [false, true])
	test(`RT-SLACK-58 orphan election tombstone cleanup live transition ${becomesLive}`, async () => {
		const { readdir } = await import("node:fs/promises");
		const home = await mkdtemp(join(tmpdir(), "slack-g5-lock-"));
		cleanups.push(() => rm(home, { recursive: true, force: true }));
		const path = join(home, "adapter-slack.pid");
		const marker = `${path}.reclaim.d`;
		await writeFile(path, "99999\n");
		await mkdir(marker);
		await writeFile(join(marker, "owner"), "88888\n");
		const old = new Date(Date.now() - 3000);
		await utimes(marker, old, old);
		const inode = (await stat(marker)).ino;
		const started = performance.now();
		const results = await Promise.allSettled(
			[1, 2].map((pid) =>
				AdapterLock.acquire(home, {
					pid,
					alive: (owner) => becomesLive && owner === 88888 && performance.now() - started > 500,
				}),
			),
		);
		const winners = results.filter((r) => r.status === "fulfilled");
		expect(winners).toHaveLength(becomesLive ? 0 : 1);
		if (becomesLive) expect((await stat(marker)).ino).toBe(inode);
		else await expect(stat(marker)).rejects.toMatchObject({ code: "ENOENT" });
		expect((await readdir(home)).filter((name) => name.endsWith(".dead") || name.endsWith(".tmp"))).toEqual([]);
	});

for (const cosmetics of [1, 20])
	test(`RT-SLACK-59 dispatch prioritizes late delivery over ${cosmetics} queued cosmetics`, async () => {
		let now = 0;
		const timers: Array<{ ms: number; resolve: () => void }> = [];
		const limiter = new OutboundLimiter(
			100,
			() => now,
			(ms) => new Promise<void>((resolve) => timers.push({ ms, resolve })),
		);
		const order: string[] = [];
		await limiter.acquire("C1", "delivery");
		order.push("D1");
		const work = Array.from({ length: cosmetics }, () =>
			limiter.acquire("C1", "cosmetic").then(() => {
				order.push("cosmetic");
			}),
		);
		await flush();
		work.push(
			limiter.acquire("C1", "delivery").then(() => {
				order.push("D2");
			}),
		);
		while (timers.length) {
			const timer = timers.shift()!;
			now += timer.ms;
			timer.resolve();
			await flush();
		}
		await Promise.all(work);
		expect(order.slice(0, 2)).toEqual(["D1", "D2"]);
		now += 100;
		expect(limiter.pendingMs("C1")).toBe(0);
	});

for (const failures of [2, 4])
	test(`RT-SLACK-60 body ratelimited ${failures} refusals retries exact Retry-After`, async () => {
		let requests = 0;
		const sleeps: number[] = [];
		const api = new SlackWebApi("unused", {
			fetcher: async () =>
				Response.json(
					++requests <= failures ? { ok: false, error: "ratelimited" } : { ok: true, channel: "C1", ts: "1.0" },
					{ headers: { "Retry-After": "2" } },
				),
			sleep: async (ms) => {
				sleeps.push(ms);
			},
		});
		const result = await api.postMessage("C1", "reply").catch((error) => error);
		expect(requests).toBe(failures === 2 ? 3 : 4);
		expect(sleeps).toEqual(Array(failures === 2 ? 2 : 3).fill(2000));
		if (failures === 4) {
			expect(result).toBeInstanceOf(SlackRateLimitedError);
			expect(result.attempts).toBe(4);
		} else expect(result.ts).toBe("1.0");
	});

test("RT-SLACK-61 bounded pending thread cursor drains five replies in two passes without resend", async () => {
	const { recoverThread } = await import("../src/recovery");
	const seen: string[] = [];
	const messages = [2, 3, 4, 5, 6].map((n) => ({ ts: `${n}.0`, user: "U1", text: "reply" }));
	const port = {
		async history() {
			return { messages: [], has_more: false };
		},
		async replies(_channel: string, _root: string, options: { oldest: string; cursor?: string; limit: number }) {
			const eligible = messages.filter((m) => Number(m.ts) > Number(options.oldest));
			const offset = Number(options.cursor ?? 0);
			const more = offset + options.limit < eligible.length;
			return {
				messages: eligible.slice(offset, offset + options.limit),
				has_more: more,
				next_cursor: more ? String(offset + options.limit) : undefined,
			};
		},
	};
	const options = {
		nowMs: 10000,
		cursor: "1.0",
		pageLimit: 2,
		maxPages: 2,
		botUserId: "UBOT",
		async deliver(m: { ts: string }) {
			seen.push(m.ts);
			return "acked" as const;
		},
	};
	const first = await recoverThread(port, "C1", "1.0", options);
	expect(first.truncated).toBe(true);
	expect(first.advancedTo).toBe("5.0");
	const second = await recoverThread(port, "C1", "1.0", { ...options, cursor: first.advancedTo });
	expect(second.truncated).toBe(false);
	expect(seen).toEqual(["2.0", "3.0", "4.0", "5.0", "6.0"]);
});

test("RT-SLACK-62 overlapping link outages retain long socket outage despite short gateway blip", async () => {
	let now = 10000;
	const f = await fixture({ C1: { engagement: "open" } }, { keepRecovery: true, now: () => now });
	await f.recoverMissedMessages();
	const before = f.api.historyCalls;
	now = 11000;
	f.links.disconnected("socket");
	now = 12000;
	f.links.disconnected("gateway");
	now = 13000;
	f.links.reconnected("gateway");
	await f.recovery.idle();
	expect(f.api.historyCalls).toBe(before);
	now = 20000;
	f.links.reconnected("socket");
	await f.recovery.idle();
	expect(f.api.historyCalls).toBe(before + 1);
});

test("RT-SLACK-61 adapter persists pending through across bounded walks and clears completed root", async () => {
	const f = await fixture({ C1: { engagement: "open" } });
	f.api.historyMessages = [{ ts: "1.0", user: "U1", text: "root", reply_count: 15 }];
	const replies = Array.from({ length: 15 }, (_, i) => ({
		ts: `${i + 2}.0`,
		user: "U1",
		text: "unengaged reply",
		thread_ts: "1.0",
	}));
	spyOn(f.api, "conversationsReplies").mockImplementation(
		async (_channel: string, _root: string, options?: { oldest?: string; cursor?: string }) => {
			const eligible = replies.filter((m) => Number(m.ts) > Number(options?.oldest ?? 0));
			const offset = Number(options?.cursor ?? 0);
			const more = offset + 1 < eligible.length;
			return {
				messages: eligible.slice(offset, offset + 1),
				has_more: more,
				next_cursor: more ? String(offset + 1) : undefined,
			};
		},
	);
	expect(await f.recoverMissedMessages()).toBe(false);
	const first = await loadRecoveryCursors(f.cursorPath);
	expect(first.pendingThreads["C1:1.0"]?.through).toBe("11.0");
	expect(first.participatedThreads["C1:1.0"]).toBeUndefined();
	f.api.historyMessages = [];
	expect(await f.recoverMissedMessages()).toBe(true);
	const second = await loadRecoveryCursors(f.cursorPath);
	expect(second.pendingThreads["C1:1.0"]).toBeUndefined();
	const sent = f.client.calls.filter((c) => c.verb === "chat.send").map((c) => c.params.messageId);
	expect(sent).toEqual(Array.from({ length: 16 }, (_, i) => `C1:${i + 1}.0`));
});
// Generation 6: directory-election and desired/applied-state boundary attacks.
test("RT-SLACK-64 twenty contenders elect exactly one winner in twenty stale elections", async () => {
	for (let iteration = 0; iteration < 20; iteration++) {
		const home = await mkdtemp(join(tmpdir(), "slack-g6-election-"));
		try {
			await writeFile(join(home, "adapter-slack.pid"), "99999\n");
			const results = await Promise.allSettled(
				Array.from({ length: 20 }, (_, i) => AdapterLock.acquire(home, { pid: i + 1, alive: () => false })),
			);
			const winners = results.filter((r) => r.status === "fulfilled");
			expect(winners).toHaveLength(1);
			for (const result of results)
				if (result.status === "rejected") expect(result.reason).toBeInstanceOf(AdapterAlreadyRunningError);
			expect(await readFile(join(home, "adapter-slack.pid"), "utf8")).toBe(`${winners[0]!.value.pid}\n`);
			expect((await readdir(home)).filter((name) => name.includes(".reclaim.d") || name.endsWith(".dead"))).toEqual([]);
			await winners[0]!.value.release();
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}
}, 60000);

// A critical section longer than the 1s reclaim wait window is the live
// incident shape (#213): under CI load the winner was judged abandoned and its
// election was stolen while it was still installing the pidfile.
test("RT-SLACK-64 a winner slower than the reclaim window keeps its election", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-g6-slow-winner-"));
	try {
		const path = join(home, "adapter-slack.pid");
		await writeFile(path, "99999\n");
		let reachedPidfileWrite = 0;
		const results = await Promise.allSettled(
			Array.from({ length: 20 }, (_, i) =>
				AdapterLock.acquire(home, {
					pid: i + 1,
					alive: () => false,
					beforePidfileWrite: async () => {
						reachedPidfileWrite++;
						await Bun.sleep(1250);
					},
				}),
			),
		);
		const winners = results.filter((result) => result.status === "fulfilled");
		expect(winners).toHaveLength(1);
		// The heartbeat is what makes this exact: an election refreshed by its live
		// owner is never reclaimable, so no second contender is ever elected and
		// only one acquisition ever reaches the pidfile write.
		expect(reachedPidfileWrite).toBe(1);
		for (const result of results)
			if (result.status === "rejected") expect(result.reason).toBeInstanceOf(AdapterAlreadyRunningError);
		const winner = winners[0]!.value;
		expect(await readFile(path, "utf8")).toBe(`${winner.pid}\n`);
		expect((await readdir(home)).filter((name) => name.includes(".reclaim.d") || name.endsWith(".dead"))).toEqual([]);
		await winner.release();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}, 10000);

// And when the election IS stolen anyway - a hostile or clock-skewed reclaimer
// backdates it past the window - the displaced winner must fail closed instead
// of overwriting the new holder's pidfile.
test("RT-SLACK-64 a displaced winner never overwrites the pidfile of the contender that replaced it", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-g6-displaced-winner-"));
	try {
		const path = join(home, "adapter-slack.pid");
		const election = `${path}.reclaim.d`;
		await writeFile(path, "99999\n");
		let backdated = false;
		const results = await Promise.allSettled(
			Array.from({ length: 20 }, (_, i) =>
				AdapterLock.acquire(home, {
					pid: i + 1,
					alive: () => false,
					beforePidfileWrite: async () => {
						// Only the first contender to get here is the original winner.
						if (backdated) return;
						backdated = true;
						// Outrun the winner's own heartbeat: the election looks abandoned for
						// the whole critical section, so a waiting contender really does
						// reclaim it and get elected while this winner is still working.
						for (let tick = 0; tick < 80; tick++) {
							const aged = new Date(Date.now() - 5000);
							await utimes(election, aged, aged).catch(() => {});
							await Bun.sleep(50);
						}
					},
				}),
			),
		);
		const winners = results.filter((result) => result.status === "fulfilled");
		expect(winners).toHaveLength(1);
		for (const result of results)
			if (result.status === "rejected") expect(result.reason).toBeInstanceOf(AdapterAlreadyRunningError);
		const winner = winners[0]!.value;
		expect(await readFile(path, "utf8")).toBe(`${winner.pid}\n`);
		await winner.release();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}, 30000);

test("RT-SLACK-65 aged ownerless election recovers within two seconds", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-g6-ownerless-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const path = join(home, "adapter-slack.pid");
	await writeFile(path, "99999\n");
	await mkdir(`${path}.reclaim.d`);
	const old = new Date(Date.now() - 3000);
	await utimes(`${path}.reclaim.d`, old, old);
	const started = performance.now();
	const lock = await AdapterLock.acquire(home, { pid: 42, alive: () => false });
	expect(performance.now() - started).toBeLessThan(2100);
	expect(await readdir(home)).toEqual(["adapter-slack.pid"]);
	await lock.release();
});

test("RT-SLACK-66 fresh live election replacing abandoned directory survives waiting contender", async () => {
	const home = await mkdtemp(join(tmpdir(), "slack-g6-restore-"));
	cleanups.push(() => rm(home, { recursive: true, force: true }));
	const path = join(home, "adapter-slack.pid");
	const dir = `${path}.reclaim.d`;
	await writeFile(path, "99999\n");
	await mkdir(dir);
	await writeFile(join(dir, "owner"), "88888\n");
	const old = new Date(Date.now() - 3000);
	await utimes(dir, old, old);
	let fresh = false;
	const pending = AdapterLock.acquire(home, { pid: 42, alive: (pid) => fresh && pid === 7 }).then(
		(lock) => ({ lock, error: undefined }),
		(error) => ({ lock: undefined, error }),
	);
	await Bun.sleep(100);
	// A real contender never leaves the slot empty: it prepares its election in
	// private and swaps it in. Reproduce that (rename over the abandoned dir
	// after moving it aside) so the waiter never sees a free slot.
	const staging = `${dir}.staging`;
	await mkdir(staging);
	await writeFile(join(staging, "owner"), "7\n");
	await rename(dir, `${dir}.old`);
	await rename(staging, dir);
	await rm(`${dir}.old`, { recursive: true, force: true });
	fresh = true;
	const inode = (await stat(dir)).ino;
	const result = await pending;
	expect(result.error).toBeInstanceOf(AdapterAlreadyRunningError);
	expect(result.error.holderPid).toBe(7);
	expect((await stat(dir)).ino).toBe(inode);
	expect(await readFile(join(dir, "owner"), "utf8")).toBe("7\n");
	expect(await readFile(path, "utf8")).toBe("99999\n");
});

test("RT-SLACK-67 failed add retries on later update and each failed remove logs without blocking delivery", async () => {
	let now = 0;
	const api = new Api();
	const errors: unknown[] = [];
	const status = new WorkingStatus(
		api,
		{
			error: (...args) => {
				errors.push(args);
			},
		},
		() => ({}),
		() => {},
		() => now,
	);
	spyOn(api, "addReaction").mockRejectedValueOnce(new Error("add denied"));
	const remove = spyOn(api, "removeReaction").mockRejectedValue(new Error("remove denied"));
	status.arm(origin, "C1:1.0");
	await flush();
	expect(api.reactions).toEqual([]);
	expect(errors).toHaveLength(1);
	now = 16000;
	await status.update({
		turnId: "t",
		origin,
		elapsedMs: now,
		toolCalls: 0,
		outputTokens: 0,
		final: false,
		activity: { kind: "thinking", label: "thinking" },
	});
	expect(api.reactions.length).toBeGreaterThan(0);
	now = 32000;
	await status.update({
		turnId: "t",
		origin,
		elapsedMs: now,
		toolCalls: 1,
		outputTokens: 0,
		final: false,
		activity: { kind: "tool", label: "tool" },
	});
	const client = new Client();
	await settleSlackDelivery(client, api, delivery(), console, status);
	expect(client.calls).toContainEqual({ verb: "delivery.confirm", params: { deliveryId: "delivery" } });
	expect(remove.mock.calls.length).toBeGreaterThan(0);
	expect(errors.length).toBe(1 + remove.mock.calls.length);
});

test("RT-SLACK-67 fetcher flipping desired state on every add is bounded to eight passes", async () => {
	let now = 0;
	let adds = 0;
	let removes = 0;
	let flipping = true;
	let status: WorkingStatus;
	const api = new SlackWebApi("unused", async (input) => {
		if (String(input).endsWith("reactions.add")) {
			adds++;
			if (flipping) {
				now += 16000;
				await status.update({
					turnId: "t",
					origin,
					elapsedMs: 0,
					toolCalls: 0,
					outputTokens: 0,
					final: false,
					activity: { kind: adds % 2 ? "tool" : "thinking", label: "flip" },
				});
			}
		} else removes++;
		return Response.json({ ok: true });
	});
	status = new WorkingStatus(
		api,
		console,
		() => ({}),
		() => {},
		() => now,
	);
	status.arm(origin, "C1:1.0");
	for (let i = 0; i < 30; i++) await flush();
	expect(adds).toBe(8);
	expect(removes).toBe(7);
	flipping = false;
	await status.clear("C1");
});

test("RT-SLACK-69 retirement frees idle lane and preserves waiter arriving during retirement sleep", async () => {
	let now = 0;
	const timers: Array<{ at: number; resolve: () => void }> = [];
	const limiter = new OutboundLimiter(
		100,
		() => now,
		(ms) => new Promise<void>((resolve) => timers.push({ at: now + ms, resolve })),
	);
	await limiter.acquire("C1");
	expect(timers).toHaveLength(1);
	now = 100;
	timers.shift()!.resolve();
	await flush();
	expect(limiter.pendingMs("C1")).toBe(0);
	// Rewind the injected clock: a retained old lane would wait for its old nextAt.
	now = 0;
	let fresh = false;
	const first = limiter.acquire("C1").then(() => {
		fresh = true;
	});
	await flush();
	expect(fresh).toBe(true);
	await first;
	let arrived = false;
	const waiter = limiter.acquire("C1").then(() => {
		arrived = true;
	});
	await flush();
	expect(arrived).toBe(false);
	now = 100;
	for (const timer of timers.splice(0)) timer.resolve();
	await flush();
	expect(arrived).toBe(true);
	await waiter;
	expect(limiter.pendingMs("C1")).toBe(100);
	now = 200;
	for (const timer of timers.splice(0)) timer.resolve();
	await flush();
	expect(limiter.pendingMs("C1")).toBe(0);
});

for (const adapter of ["slack", "discord"] as const)
	test(`RT-SLACK-70 ${adapter}: a crash between election and ownership cannot lock the adapter out`, async () => {
		// The election is prepared with its owner in private and renamed into place,
		// so an ownerless election directory can only be a hand-made artifact - and
		// even that must age out, not lock restarts forever.
		const Lock = adapter === "slack" ? AdapterLock : (await import("../../adapter-discord/src/lock")).AdapterLock;
		const file = adapter === "slack" ? "adapter-slack.pid" : "adapter-discord.pid";
		const home = await mkdtemp(join(tmpdir(), `${adapter}-ownerless-`));
		cleanups.push(() => rm(home, { recursive: true, force: true }));
		const path = join(home, file);
		await writeFile(path, "99999\n");
		await mkdir(`${path}.reclaim.d`); // no owner file
		const aged = new Date(Date.now() - 3000);
		await utimes(`${path}.reclaim.d`, aged, aged);
		const started = performance.now();
		const lock = await Lock.acquire(home, { pid: 42, alive: () => false });
		expect(performance.now() - started).toBeLessThan(3000);
		expect(await readFile(path, "utf8")).toBe("42\n");
		expect((await readdir(home)).filter((e) => e.includes("reclaim"))).toEqual([]);
		await lock.release();
	});
