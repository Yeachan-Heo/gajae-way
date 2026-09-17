import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ChatMessagePayload, isPlatformMessageId } from "@gajaeway/protocol";
import { SlackApiError, SlackWebApi } from "../src/api";
import { loadSlackAdapterConfig } from "../src/config";
import {
	engagementForMessage,
	type GatewayClientLike,
	ReconnectingGateway,
	replyThreadTs,
	SKIPPED_SUBTYPES,
	settleSlackDelivery,
	startSlackAdapter,
} from "../src/main";
import { chunkSlackMessage } from "../src/mrkdwn";
import { parseSlackMessageId, slackMessageOrigin } from "../src/origin";
import { loadRecoveryCursors, RecoveryScheduler, recoverConversation } from "../src/recovery";
import { SlackSocketMode, type WebSocketLike } from "../src/socket";
import { normalizeSlackText } from "../src/text";

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
	async request<T>(verb: string, params?: unknown): Promise<T> {
		this.calls.push({ verb, params });
		if (this.failure) throw this.failure;
		return { engaged: false } as T;
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
		return { messages: [], has_more: false };
	}
	override async postMessage(...args: [string, string, string?]) {
		this.posts.push(args);
		return { channel: args[0], ts: "2.0" };
	}
	override async addReaction(...args: [string, string, string]) {
		this.reactions.push(args);
	}
}
async function fixture(channels?: Record<string, { engagement: "open" }>) {
	const home = await mkdtemp(join(tmpdir(), "slack-redteam-"));
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
			recoveryCursorPath: join(home, "cursor.json"),
			log: { log() {}, error() {} },
			socketFactory: () => {
				const socket = new Socket();
				queueMicrotask(() => socket.onopen?.({}));
				return socket;
			},
		},
	);
	adapter.recovery.stop();
	adapter.gateway.onConnected = undefined;
	const client = new Client();
	adapter.gateway.adoptClient(client);
	cleanups.push(() => {
		adapter.socket.stop();
		adapter.recovery.stop();
	});
	return {
		...adapter,
		api,
		client,
		home,
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
	const f = await fixture({ C1: { engagement: "open" } });
	f.api.unreadable = true;
	for (let i = 0; i < 3; i++) await f.recoverMissedMessages();
	expect((await loadRecoveryCursors(join(f.home, "cursor.json"))).quarantined.C1?.failures).toBe(3);
	// The fixture disables the connect hook for deterministic hand-driven passes;
	// re-arm the real one so a reconnect is what resets the strike count.
	// The fixture stopped the scheduler for hand-driven passes, so the reconnect
	// pass is reproduced here as the scheduler runs it: reprobe, then recover.
	f.api.unreadable = false;
	f.gateway.adoptClient(new Client());
	await flush();
	await f.reprobeQuarantined();
	await f.recoverMissedMessages();
	expect(f.api.historyCalls).toBe(4);
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
