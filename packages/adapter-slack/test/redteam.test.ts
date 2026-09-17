import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ChatMessagePayload, isPlatformMessageId } from "@gajaeway/protocol";
import { deliveryFailureIsAmbiguous, SlackApiError, SlackUnreadableResponseError, SlackWebApi } from "../src/api";
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
	override async conversationsReplies() {
		return { messages: this.replyMessages, has_more: false };
	}
	override async respond(_url: string, payload: Record<string, unknown>) {
		this.responses.push(payload);
	}
	override async updateMessage() {}
	override async deleteMessage() {}
	override async postMessage(...args: [string, string, string?]) {
		this.posts.push(args);
		return { channel: args[0], ts: "2.0" };
	}
	override async addReaction(...args: [string, string, string]) {
		this.reactions.push(args);
	}
}
async function fixture(
	channels?: Record<string, { engagement: "open" }>,
	options: { nested?: boolean; keepRecovery?: boolean } = {},
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
			now: () => 10_000,
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

test("RT-SLACK-40 addressed accepted turns post working status before any progress", async () => {
	const api = new Api();
	const client = new Client();
	client.engaged = true;
	const status = new WorkingStatus(api);
	const gateway = new ReconnectingGateway("/tmp/no-redteam.sock", api, client, status);
	await gateway.requestInbound("C1:1.0", origin, "overheard", engagement);
	await flush();
	expect(api.posts).toEqual([]);
	await gateway.requestInbound("C1:2.0", origin, "addressed", { ...engagement, mentioned: true });
	await flush();
	expect(api.posts).toEqual([["C1", "⏳ working…", undefined]]);
	const thread = { platform: "slack", kind: "thread", conversationId: "C1:1.0", parentId: "C1" } as const;
	await gateway.requestInbound("C1:3.0", thread, "thread", { ...engagement, mentioned: true });
	await flush();
	expect(api.posts[1]).toEqual(["C1", "⏳ working…", "1.0"]);
	await status.clear("C1");
	await status.clear("C1:1.0");
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
