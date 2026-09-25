import { expect, spyOn, test } from "bun:test";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChatMessagePayload, EngagementContext, OriginRef } from "@gajae-gateway/protocol";
import { SlackApiError, type SlackHistoryPage, SlackWebApi } from "../src/api";
import {
	decideInbound,
	describeMessageEdit,
	engagementForMessage,
	type GatewayClientLike,
	LruSet,
	monitorFailureDecision,
	OrderedIngress,
	ReconnectingGateway,
	renderInboundText,
	replyThreadTs,
	SKIPPED_SUBTYPES,
	type SlackInboundMessage,
	settleSlackDelivery,
	settleSlackReaction,
	startSlackAdapter,
	subscribeSlackDeliveries,
} from "../src/main";
import { slackMessageOrigin } from "../src/origin";
import { loadRecoveryCursors } from "../src/recovery";
import type { WebSocketLike } from "../src/socket";

// The adapter defaults its recovery store to $GAJAEWAY_HOME; a test must never
// be able to reach a real operator home, whatever a fixture forgets to pass.
process.env.GAJAEWAY_HOME = join(tmpdir(), `slack-test-home-${crypto.randomUUID()}`);

const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C1" };
const engagement: EngagementContext = { mentioned: false, group: true, authorId: "U1" };
const identity = { botUserId: "UBOT", botId: "B1", teamName: "Workspace" };
const names = {
	userName: (id: string) => (id === "U1" ? "Alice" : undefined),
	userHandle: (id: string) => (id === "U1" ? "alice" : undefined),
	channelName: () => "general",
};
const inbound = (extra: Partial<SlackInboundMessage> = {}): SlackInboundMessage => ({
	type: "message",
	channel: "C1",
	ts: "1700000000.123456",
	user: "U1",
	text: "hello",
	...extra,
});
const delivery = (extra: Partial<ChatMessagePayload> = {}): ChatMessagePayload => ({
	turnId: "turn",
	origin,
	role: "assistant",
	text: "hello",
	final: true,
	deliveryId: "delivery",
	...extra,
});
async function flush() {
	for (let i = 0; i < 40; ++i) await Promise.resolve();
}

class Gateway implements GatewayClientLike {
	readonly requests: { verb: string; params: unknown }[] = [];
	readonly handlers = new Set<(message: ChatMessagePayload) => void>();
	engaged = true;
	failure?: Error;
	async request<T = unknown>(verb: string, params?: unknown): Promise<T> {
		this.requests.push({ verb, params });
		if (this.failure) throw this.failure;
		return { engaged: this.engaged } as T;
	}
	onChatMessage(handler: (message: ChatMessagePayload) => void) {
		this.handlers.add(handler);
		return () => {
			this.handlers.delete(handler);
		};
	}
}
class Api extends SlackWebApi {
	readonly posts: unknown[][] = [];
	readonly reactions: unknown[][] = [];
	readonly responses: unknown[][] = [];
	readonly users: string[] = [];
	readonly conversations: string[] = [];
	failure?: Error;
	constructor() {
		super("unused", async () => {
			throw new Error("Slack test must not fetch");
		});
	}
	override async postMessage(channel: string, text: string, threadTs?: string) {
		this.posts.push([channel, text, threadTs]);
		if (this.failure) throw this.failure;
		return { channel, ts: "2.000" };
	}
	override async addReaction(channel: string, ts: string, name: string) {
		this.reactions.push([channel, ts, name]);
		if (this.failure) throw this.failure;
	}
	override async authTest() {
		return { user_id: "UBOT", bot_id: "B1", user: "bot", team_id: "T1", team: "Workspace" };
	}
	override async usersInfo(id: string) {
		this.users.push(id);
		return { id, name: id === "U1" ? "alice" : id, profile: { display_name: id === "U1" ? "Alice" : id } };
	}
	override async conversationsInfo(id: string) {
		this.conversations.push(id);
		return { id, name: "general" };
	}
	override async connectionsOpen() {
		return { url: "wss://slack.test" };
	}
	/** Recovery pages, keyed by channel; newest-first like Slack. */
	history: Record<string, Record<string, unknown>[]> = {};
	readonly historyCalls: string[] = [];
	override async conversationsHistory(channel: string) {
		this.historyCalls.push(channel);
		const messages = this.history[channel];
		if (!messages) throw new SlackApiError(200, "channel_not_found");
		return { messages, has_more: false };
	}
	override async conversationsReplies(_channel: string, _ts: string): Promise<SlackHistoryPage> {
		return { messages: [], has_more: false };
	}
	override async respond(url: string, payload: Record<string, unknown>) {
		this.responses.push([url, payload]);
	}
}
class Socket implements WebSocketLike {
	onopen: WebSocketLike["onopen"] = null;
	onmessage: WebSocketLike["onmessage"] = null;
	onclose: WebSocketLike["onclose"] = null;
	onerror: WebSocketLike["onerror"] = null;
	send() {}
	close() {}
}
async function fixture(
	channels?: Record<string, { engagement: "open" | "mention-open" | "closed" }>,
	options: { readonly autoRecover?: boolean } = {},
) {
	const api = new Api();
	const gateway = new Gateway();
	const recoveryCursorPath = join(
		tmpdir(),
		`slack-recovery-${crypto.randomUUID()}`,
		"adapters/slack/recovery-cursor.json",
	);
	const adapter = await startSlackAdapter(
		{
			botToken: "xoxb-test",
			appToken: "xapp-test",
			botTokenFile: "bot",
			appTokenFile: "app",
			configPath: "test",
			gatewaySocket: `/tmp/slack-missing-${crypto.randomUUID()}.sock`,
			...(channels ? { channels } : {}),
		},
		{
			api,
			recoveryCursorPath,
			now: () => 1_700_000_100_000,
			log: { log() {}, error() {} },
			socketFactory: () => {
				const socket = new Socket();
				queueMicrotask(() => socket.onopen?.({}));
				return socket;
			},
		},
	);
	// Recovery passes run on every connect; tests that drive passes by hand opt out
	// so the fixture's own adoptClient cannot race their counts.
	if (options.autoRecover === false) adapter.gateway.onConnected = undefined;
	adapter.gateway.adoptClient(gateway);
	// One stop covers both background loops so no test can leak a scheduler.
	const socket = { ...adapter.socket, stop: () => {} };
	const stopAll = () => {
		adapter.socket.stop();
		adapter.recovery.stop();
	};
	return {
		...adapter,
		socket: Object.assign(socket, { stop: stopAll }) as typeof adapter.socket,
		api,
		client: gateway,
		recoveryCursorPath,
		async event(event: Record<string, unknown>) {
			await adapter.handleEvent(event);
			await adapter.ingress.drain();
			await flush();
		},
	};
}

for (const shape of [
	{ channel: "D1" },
	{ channel: "C1" },
	{ channel: "C1", thread_ts: "1699999999.000001" },
	{ channel: "C1", thread_ts: "1699999999.000001", parent_user_id: "U2" },
	{ channel: "C1", thread_ts: "1699999999.000001", parent_user_id: "UBOT" },
])
	test(`Slack exact inbound routing ${JSON.stringify(shape)}`, async () => {
		const f = await fixture();
		try {
			await f.event({ ...inbound(shape) });
			const dm = shape.channel === "D1";
			const thread = "thread_ts" in shape;
			expect(f.client.requests).toEqual([
				{
					verb: "chat.send",
					params: {
						origin: dm
							? { platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" }
							: thread
								? { platform: "slack", kind: "thread", conversationId: `C1:${shape.thread_ts}`, parentId: "C1" }
								: origin,
						messageId: `${shape.channel}:1700000000.123456`,
						text: "hello",
						receivedAt: "2023-11-14T22:13:20.123Z",
						engagement: {
							mentioned: shape.parent_user_id === "UBOT",
							group: !dm,
							authorId: "U1",
							authorName: "Alice",
							authorHandle: "alice",
							serverLabel: "Workspace",
							...(!dm ? { channelLabel: "#general" } : {}),
							...(thread
								? {
										replyTo: {
											messageId: `C1:${shape.thread_ts}`,
											...(shape.parent_user_id
												? { authorId: shape.parent_user_id, fromSelf: shape.parent_user_id === "UBOT" }
												: {}),
										},
									}
								: {}),
						},
					},
				},
			]);
			if (dm) expect(f.api.conversations).toEqual([]);
		} finally {
			f.socket.stop();
		}
	});

test("Slack rejects self, service, hidden, authorless and empty messages and ignores app_mention", async () => {
	const f = await fixture();
	try {
		for (const extra of [
			{ user: "UBOT" },
			{ bot_id: "B1" },
			{ hidden: true },
			{ user: undefined },
			{ ts: "" },
			{ text: "" },
			{ subtype: "message_changed" },
			...[...SKIPPED_SUBTYPES].map((subtype) => ({ subtype })),
		]) {
			await f.event({ ...inbound(extra) });
		}
		await f.event({ ...inbound(), type: "app_mention", text: "<@UBOT>" });
		expect(f.client.requests).toEqual([]);
	} finally {
		f.socket.stop();
	}
});

test("Slack mention, open-channel and parent-bot promotion; bot authors remain metadata", () => {
	for (const extra of [
		{ text: "<@UBOT> hi" },
		{ text: "<@UBOT|name> hi" },
		{ thread_ts: "1.000", parent_user_id: "UBOT" },
	]) {
		const message = inbound(extra);
		expect(engagementForMessage(message, slackMessageOrigin(message), identity, names, undefined).mentioned).toBe(true);
	}
	expect(engagementForMessage(inbound({ text: "<@UBOT2>" }), origin, identity, names, undefined).mentioned).toBe(false);
	expect(engagementForMessage(inbound(), origin, identity, names, { C1: { engagement: "open" } }).mentioned).toBe(true);
	expect(
		engagementForMessage(
			inbound({ channel: "D1" }),
			{ platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" },
			identity,
			names,
			{ D1: { engagement: "open" } },
		).mentioned,
	).toBe(false);
	const bot = decideInbound(
		inbound({ user: undefined, bot_id: "B2", subtype: "bot_message", username: "Other" }),
		identity,
		names,
		undefined,
	);
	expect(bot?.engagement).toEqual({
		mentioned: false,
		group: true,
		authorId: "B2",
		authorIsBot: true,
		authorName: "Other",
		channelLabel: "#general",
		serverLabel: "Workspace",
	});
});

test("Slack rendering preserves attachments and primes at most ten mentioned users", async () => {
	const f = await fixture();
	try {
		await f.event({ ...inbound({ text: Array.from({ length: 12 }, (_, i) => `<@W${i}>`).join(" ") }) });
		expect(f.api.users.filter((id) => id.startsWith("W"))).toHaveLength(10);
		expect(renderInboundText(inbound({ text: "<@U1>", files: [{ name: "x", mimetype: "image/png" }] }), names)).toBe(
			"@Alice\n[image · x]",
		);
	} finally {
		f.socket.stop();
	}
});

test("Slack edits use nested identity, edited timestamp and rendered body equality", async () => {
	const f = await fixture();
	try {
		const event = {
			type: "message",
			subtype: "message_changed",
			channel: "C1",
			ts: "1700000002.000000",
			message: { ts: "1700000000.123456", user: "U1", text: "updated", edited: { ts: "1700000001.500000" } },
			previous_message: { text: "hello" },
		};
		await f.event(event);
		expect(f.client.requests).toEqual([
			{
				verb: "chat.edit",
				params: {
					origin,
					messageId: "C1:1700000000.123456",
					text: "updated",
					receivedAt: "2023-11-14T22:13:21.500Z",
					engagement: {
						...engagement,
						authorName: "Alice",
						authorHandle: "alice",
						channelLabel: "#general",
						serverLabel: "Workspace",
					},
				},
			},
		]);
		await f.event({ ...event, previous_message: { text: "updated" } });
		expect(f.client.requests).toHaveLength(1);
		const fallback = describeMessageEdit(
			{ ...inbound(), subtype: "message_changed", message: inbound({ text: "new" }) },
			identity,
			names,
			undefined,
		);
		expect(fallback?.receivedAt).toBe("2023-11-14T22:13:20.123Z");
		expect(
			describeMessageEdit(
				{ ...inbound(), subtype: "message_changed", message: inbound({ text: "" }) },
				identity,
				names,
				undefined,
			),
		).toBeUndefined();
	} finally {
		f.socket.stop();
	}
});

test("Slack edit outbox replays oldest first, supersedes live edits, and caps at 256", async () => {
	const log = spyOn(console, "error").mockImplementation(() => {});
	try {
		const api = new Api();
		const gateway = new ReconnectingGateway("/tmp/slack-not-connected.sock", api);
		for (let i = 0; i < 257; ++i) gateway.sendEdit(`C1:${i}.000`, origin, String(i), engagement);
		expect(gateway.pendingEdits).toHaveLength(256);
		expect(gateway.pendingEdits[0]?.messageId).toBe("C1:1.000");
		expect(log).toHaveBeenCalledWith("Slack edit outbox full; dropped the oldest queued edit (message C1:0.000).");
		await flush();
		const client = new Gateway();
		gateway.adoptClient(client);
		for (let i = 0; i < 300; ++i) await Promise.resolve();
		expect(client.requests.map((r) => (r.params as { text: string }).text)).toEqual(
			Array.from({ length: 256 }, (_, i) => String(i + 1)),
		);
		expect(gateway.pendingEdits).toEqual([]);
		let release!: () => void;
		const requests: unknown[] = [];
		gateway.adoptClient({
			onChatMessage: () => () => {},
			async request<T>(_verb: string, params?: unknown) {
				requests.push(params);
				if (requests.length === 1)
					await new Promise<void>((resolve) => {
						release = resolve;
					});
				return {} as T;
			},
		});
		gateway.sendEdit("C1:1.000", origin, "first", engagement);
		await flush();
		gateway.sendEdit("C1:1.000", origin, "latest", engagement);
		gateway.sendEdit("C1:2.000", origin, "second", engagement);
		release();
		await flush();
		expect(requests.map((r) => (r as { text: string }).text)).toEqual(["first", "latest", "second"]);
		expect(gateway.pendingEdits).toEqual([]);
	} finally {
		log.mockRestore();
	}
});

test("Slack failed edit stays queued until a fresh client acknowledges it", async () => {
	const log = spyOn(console, "error").mockImplementation(() => {});
	try {
		const bad = new Gateway();
		bad.failure = new Error("Slack offline");
		const gateway = new ReconnectingGateway("/tmp/slack-not-connected.sock", new Api(), bad);
		gateway.sendEdit("C1:1.000", origin, "edit", engagement);
		await flush();
		expect(gateway.pendingEdits).toHaveLength(1);
		const good = new Gateway();
		gateway.adoptClient(good);
		await flush();
		expect(good.requests[0]?.verb).toBe("chat.edit");
		expect(gateway.pendingEdits).toEqual([]);
	} finally {
		log.mockRestore();
	}
});

test("Slack LRU refreshes duplicates and gateway never re-sends a duplicate message id", async () => {
	const lru = new LruSet(2);
	expect(() => new LruSet(0)).toThrow("Slack");
	expect(lru.addIfAbsent("a")).toBe(true);
	lru.addIfAbsent("b");
	expect(lru.addIfAbsent("a")).toBe(false);
	lru.addIfAbsent("c");
	expect(lru.addIfAbsent("b")).toBe(true);
	const client = new Gateway();
	const gateway = new ReconnectingGateway("unused", new Api(), client);
	await gateway.requestInbound("C1:1.000", origin, "hi", engagement);
	expect(await gateway.requestInbound("C1:1.000", origin, "hi", engagement)).toBeUndefined();
	expect(client.requests).toHaveLength(1);
});

test("Slack OrderedIngress preserves per-conversation ordering and isolates failures", async () => {
	const log = spyOn(console, "error").mockImplementation(() => {});
	try {
		const ingress = new OrderedIngress();
		const order: string[] = [];
		let release!: () => void;
		ingress.run("a", async () => {
			await new Promise<void>((resolve) => {
				release = resolve;
			});
			order.push("a1");
			throw new Error("Slack test");
		});
		ingress.run("a", async () => {
			order.push("a2");
		});
		ingress.run("b", async () => {
			order.push("b");
		});
		await flush();
		expect(order).toEqual(["b"]);
		release();
		await ingress.drain();
		expect(order).toEqual(["b", "a1", "a2"]);
	} finally {
		log.mockRestore();
	}
});

test("Slack delivery converts markdown, warns of duplicates, ignores voice and confirms", async () => {
	const api = new Api();
	const gateway = new Gateway();
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ text: "**bold** & [link](https://x)", duplicateWarning: true, voiceText: "spoken" }),
	);
	expect(api.posts).toEqual([["C1", "[recovered - may be a duplicate] *bold* &amp; <https://x|link>", undefined]]);
	expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery" } }]);
});

test("Slack delivery repairs leaked mentions through the directory before mrkdwn conversion", async () => {
	const api = new Api();
	const gateway = new Gateway();
	const mentions = {
		knownUsers: () => [{ id: "U0C2GSKTA6M", name: "bellman", profile: { display_name: "Bellman" } }],
	};
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ text: "`<@U0C2GSKTA6M>` / @bellman / @U0BT1S5UGS1 / @unknown / **ok**" }),
		console,
		undefined,
		mentions,
	);
	expect(api.posts).toEqual([["C1", "<@U0C2GSKTA6M> / <@U0C2GSKTA6M> / <@U0BT1S5UGS1> / @unknown / *ok*", undefined]]);
	// Without a directory nothing is repaired: the delivery is still posted as-is.
	const plain = new Api();
	await settleSlackDelivery(new Gateway(), plain, delivery({ text: "@bellman hi" }));
	expect(plain.posts).toEqual([["C1", "@bellman hi", undefined]]);
});

test("Slack threaded delivery keeps every chunk in the thread; explicit same-channel replies thread", async () => {
	for (const extra of [
		{ origin: { platform: "slack", kind: "thread", conversationId: "C1:1.000", parentId: "C1" } as OriginRef },
		{ replyToMessageId: "C1:1.000" },
	]) {
		const api = new Api();
		await settleSlackDelivery(new Gateway(), api, delivery({ ...extra, text: "a".repeat(8001) }));
		expect(api.posts).toHaveLength(3);
		expect(api.posts.map((p) => p[2])).toEqual(["1.000", "1.000", "1.000"]);
		expect(api.posts.map((p) => p[1]).join("")).toBe("a".repeat(8001));
	}
	// A reply target the adapter cannot honour is a definitive failure with no
	// post at all: answering at the top level would confirm a reply nobody saw.
	for (const replyToMessageId of ["C2:1.000", "bad", "C1:", ":1.0"]) {
		const api = new Api();
		const gateway = new Gateway();
		await settleSlackDelivery(gateway, api, delivery({ replyToMessageId }));
		expect(api.posts).toEqual([]);
		expect(gateway.requests).toEqual([
			{
				verb: "delivery.fail",
				params: { deliveryId: "delivery", reason: expect.stringMatching(/malformed|foreign/), ambiguous: false },
			},
		]);
		expect(() => replyThreadTs(delivery({ replyToMessageId }))).toThrow(SlackApiError);
	}
	// A thread origin whose id is not channel:ts is refused the same way.
	const api = new Api();
	const gateway = new Gateway();
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ origin: { platform: "slack", kind: "thread", conversationId: "garbage", parentId: "C1" } }),
	);
	expect(api.posts).toEqual([]);
	expect(gateway.requests[0]?.verb).toBe("delivery.fail");
});

for (const error of [new TypeError("Slack network lost"), new SlackApiError(403, "not_allowed")])
	test(`Slack delivery failure classifies ${error.name}`, async () => {
		const api = new Api();
		api.failure = error;
		const gateway = new Gateway();
		await settleSlackDelivery(gateway, api, delivery());
		expect(gateway.requests).toEqual([
			{
				verb: "delivery.fail",
				params: { deliveryId: "delivery", reason: error.message, ambiguous: error instanceof TypeError },
			},
		]);
	});

test("Slack delivery ignores foreign origins and absent delivery ids", async () => {
	const api = new Api();
	const gateway = new Gateway();
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ origin: { platform: "discord", kind: "channel", conversationId: "C1" } }),
	);
	await settleSlackDelivery(gateway, api, delivery({ deliveryId: undefined }));
	expect(api.posts).toEqual([]);
	expect(gateway.requests).toEqual([]);
});

for (const [emoji, emojiName, name] of [
	["👍", "thumbsup", "+1"],
	["🦞", "lobster", "lobster"],
] as const)
	test(`Slack reaction ${emoji} maps without a text fallback`, async () => {
		const api = new Api();
		const gateway = new Gateway();
		await settleSlackDelivery(gateway, api, delivery({ reaction: { targetMessageId: "C1:1.000", emoji, emojiName } }));
		expect(api.reactions).toEqual([["C1", "1.000", name]]);
		expect(api.posts).toEqual([]);
		expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery" } }]);
	});

for (const [target, reason] of [
	["broken", "malformed"],
	["C2:1.000", "foreign channel"],
])
	test(`Slack reaction rejects ${reason} target definitively`, async () => {
		const api = new Api();
		const gateway = new Gateway();
		await settleSlackReaction(
			gateway,
			api,
			delivery({ reaction: { targetMessageId: target as string, emoji: "👍", emojiName: "thumbsup" } }),
		);
		expect(api.reactions).toEqual([]);
		expect(gateway.requests).toEqual([
			{
				verb: "delivery.fail",
				params: { deliveryId: "delivery", ambiguous: false, reason: expect.stringContaining(reason as string) },
			},
		]);
	});

for (const code of ["already_reacted", "not_reactable"])
	test(`Slack real API reaction settlement handles ${code}`, async () => {
		const api = new SlackWebApi("test", async () => Response.json({ ok: false, error: code }));
		const gateway = new Gateway();
		await settleSlackReaction(
			gateway,
			api,
			delivery({ reaction: { targetMessageId: "C1:1.000", emoji: "👍", emojiName: "thumbsup" } }),
		);
		expect(gateway.requests).toEqual([
			{
				verb: code === "already_reacted" ? "delivery.confirm" : "delivery.fail",
				params: {
					deliveryId: "delivery",
					...(code === "already_reacted"
						? {}
						: { ambiguous: false, reason: "Slack API request failed: not_reactable" }),
				},
			},
		]);
	});

test("Slack inbound reactions carry action metadata and ignore the bot", async () => {
	const f = await fixture();
	try {
		for (const type of ["reaction_added", "reaction_removed"])
			await f.event({
				type,
				user: "U1",
				reaction: "+1",
				item: { type: "message", channel: "C1", ts: "1.000" },
				event_ts: "2.000",
			});
		await f.event({
			type: "reaction_added",
			user: "UBOT",
			reaction: "+1",
			item: { type: "message", channel: "C1", ts: "1.000" },
			event_ts: "2.000",
		});
		expect(f.client.requests).toEqual(
			["add", "remove"].map((action) => ({
				verb: "engagement.reaction",
				params: { origin, targetMessageId: "C1:1.000", emoji: "👍", action, engagement },
			})),
		);
	} finally {
		f.socket.stop();
	}
});

test("Slack reaction request rejection does not reconnect the healthy client", async () => {
	const log = spyOn(console, "error").mockImplementation(() => {});
	try {
		const client = new Gateway();
		const gateway = new ReconnectingGateway("unused", new Api(), client);
		client.failure = new Error("Slack metadata rejected");
		gateway.sendReaction({ origin, targetMessageId: "C1:1.000", emoji: "👍", action: "add", engagement });
		await flush();
		client.failure = undefined;
		await gateway.requestInbound("C1:2.000", origin, "hi", engagement);
		expect(client.requests.map((r) => r.verb)).toEqual(["engagement.reaction", "chat.send"]);
	} finally {
		log.mockRestore();
	}
});

test("Slack subscription and adopted client handlers unsubscribe cleanly", async () => {
	const api = new Api();
	const client = new Gateway();
	const off = subscribeSlackDeliveries(client, api);
	for (const handler of client.handlers) handler(delivery());
	await flush();
	expect(api.posts).toHaveLength(1);
	off();
	expect(client.handlers.size).toBe(0);
	const gateway = new ReconnectingGateway("unused", api, client);
	const seen: ChatMessagePayload[] = [];
	const unlisten = gateway.onChatMessage((message) => seen.push(message));
	for (const handler of client.handlers) handler(delivery({ deliveryId: undefined }));
	expect(seen).toHaveLength(1);
	unlisten();
	const next = new Gateway();
	gateway.adoptClient(next);
	expect(client.handlers.size).toBe(0);
});

test("Slack monitor tolerates two strikes and reconnects on the third", () => {
	expect(monitorFailureDecision(0)).toEqual({ action: "retry", strikes: 1 });
	expect(monitorFailureDecision(1)).toEqual({ action: "retry", strikes: 2 });
	expect(monitorFailureDecision(2)).toEqual({ action: "reconnect" });
});

for (const command of ["/new", "/reset", "/restart", "/model", "/unknown"])
	test(`Slack slash ${command} delegates authorization and responds honestly`, async () => {
		const f = await fixture();
		try {
			for (const engaged of [true, false]) {
				f.client.engaged = engaged;
				await f.handleSlashCommand({
					command,
					text: "",
					user_id: "U1",
					user_name: "alice",
					channel_id: "D1",
					trigger_id: String(engaged),
					response_url: "https://hooks.slack.test/response",
				});
				expect(f.api.responses.at(-1)).toEqual([
					"https://hooks.slack.test/response",
					{
						response_type: "ephemeral",
						text:
							command === "/unknown"
								? "unknown command"
								: engaged
									? command === "/restart"
										? "🦞 restarting the gateway"
										: command === "/model"
											? "🦞 model command accepted"
											: "🦞 session reset"
									: "not authorized for session commands here",
					},
				]);
			}
			expect(f.client.requests).toEqual(
				command === "/unknown"
					? []
					: [true, false].map((engaged) => ({
							verb: "chat.send",
							params: {
								messageId: `slash-${engaged}`,
								origin: { platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" },
								text: command,
								engagement: { mentioned: true, group: false, authorId: "U1", authorHandle: "alice" },
							},
						})),
			);
		} finally {
			f.socket.stop();
		}
	});

test("Slack slash arguments reach the gateway as one command line", async () => {
	const f = await fixture();
	try {
		const send = async (text: string, trigger: string) =>
			await f.handleSlashCommand({
				command: "/model",
				text,
				user_id: "U1",
				user_name: "alice",
				channel_id: "D1",
				trigger_id: trigger,
				response_url: "https://hooks.slack.test/response",
			});
		// An argument must survive: a bare `/model` only READS the selection, so a
		// dropped argument silently turns a rebind into a no-op that acks success.
		await send("  preset frontier-default  ", "with-argument");
		await send("", "bare");
		expect(f.client.requests.map((request) => (request.params as { text: string }).text)).toEqual([
			"/model preset frontier-default",
			"/model",
		]);
	} finally {
		f.socket.stop();
	}
});

test("Slack startup awaits Socket Mode start and slow name lookup cannot reorder a conversation", async () => {
	const api = new Api();
	let releaseUser!: () => void;
	const usersInfo = api.usersInfo.bind(api);
	api.usersInfo = async (id) => {
		if (id === "U1")
			await new Promise<void>((resolve) => {
				releaseUser = resolve;
			});
		return usersInfo(id);
	};
	let created!: (socket: Socket) => void;
	const socketReady = new Promise<Socket>((resolve) => {
		created = resolve;
	});
	let resolved = false;
	const starting = startSlackAdapter(
		{
			botToken: "unused",
			appToken: "unused",
			botTokenFile: "bot",
			appTokenFile: "app",
			configPath: "test",
			gatewaySocket: `/tmp/slack-missing-${crypto.randomUUID()}.sock`,
		},
		{
			api,
			// Never the ambient $GAJAEWAY_HOME store: this process may be a real host.
			recoveryCursorPath: join(tmpdir(), `slack-recovery-${crypto.randomUUID()}.json`),
			log: { log() {}, error() {} },
			socketFactory: () => {
				const socket = new Socket();
				created(socket);
				return socket;
			},
		},
	).then((adapter) => {
		resolved = true;
		return adapter;
	});
	const socket = await socketReady;
	await flush();
	expect(resolved).toBe(false);
	socket.onopen?.({});
	const adapter = await starting;
	const client = new Gateway();
	adapter.gateway.adoptClient(client);
	try {
		await adapter.handleEvent({ ...inbound() });
		await adapter.handleEvent({ ...inbound({ ts: "1700000001.000", user: "U2", text: "second" }) });
		await adapter.handleEvent({ ...inbound({ channel: "C2", user: "U3", text: "parallel" }) });
		await flush();
		expect(client.requests.map((r) => (r.params as { text: string }).text)).toEqual(["parallel"]);
		releaseUser();
		await adapter.ingress.drain();
		expect(client.requests.map((r) => (r.params as { text: string }).text)).toEqual(["parallel", "hello", "second"]);
	} finally {
		adapter.socket.stop();
		adapter.recovery.stop();
	}
});

test("Slack recovery replays a configured channel's gap through chat.send and advances the watermark only on ack", async () => {
	const f = await fixture({ C1: { engagement: "open" } }, { autoRecover: false });
	try {
		// Newest first, as Slack pages them; one of ours, one from a human, one edit-free repeat.
		f.api.history.C1 = [
			{ type: "message", user: "U1", ts: "1700000050.000002", text: "second" },
			{ type: "message", user: "UBOT", ts: "1700000050.000001", text: "ours" },
			{ type: "message", user: "U1", ts: "1700000040.000001", text: "first" },
		];
		expect(await f.recoverMissedMessages()).toBe(true);
		const sends = f.client.requests.filter((request) => request.verb === "chat.send");
		expect(sends.map((request) => (request.params as { messageId: string; text: string }).text)).toEqual([
			"first",
			"second",
		]);
		expect((sends[0]?.params as { messageId: string }).messageId).toBe("C1:1700000040.000001");
		const cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.recoveredThrough.C1).toBe("1700000050.000002");
		// A second pass is a no-op: everything is behind the watermark.
		f.client.requests.length = 0;
		expect(await f.recoverMissedMessages()).toBe(true);
		expect(f.client.requests.filter((request) => request.verb === "chat.send")).toHaveLength(0);
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack recovery keeps the watermark when the gateway is unavailable and quarantines unreadable channels", async () => {
	const f = await fixture({ C1: { engagement: "open" }, C9: { engagement: "open" } }, { autoRecover: false });
	try {
		f.api.history.C1 = [{ type: "message", user: "U1", ts: "1700000040.000001", text: "first" }];
		f.client.failure = new Error("gateway down");
		expect(await f.recoverMissedMessages()).toBe(false);
		let cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.recoveredThrough.C1).toBeUndefined();
		// C9 has no history at all: channel_not_found is permanent and counted.
		expect(cursors.quarantined.C9?.failures).toBe(1);
		// A failed send detaches the link; without one a pass refuses to run at all.
		expect(f.gateway.connected).toBe(false);
		expect(await f.recoverMissedMessages()).toBe(false);
		expect(f.api.historyCalls).toHaveLength(2);
		// The link comes back: the same message is retried, not remembered as seen.
		f.client.failure = undefined;
		f.gateway.adoptClient(f.client);
		expect(await f.recoverMissedMessages()).toBe(false);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.recoveredThrough.C1).toBe("1700000040.000001");
		expect(cursors.quarantined.C9?.failures).toBe(2);
		// Three strikes: the channel is skipped until the next connect probes it again.
		for (let pass = 0; pass < 4; pass++) await f.recoverMissedMessages();
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.quarantined.C9?.failures).toBe(3);
		expect(f.api.historyCalls.filter((channel) => channel === "C9")).toHaveLength(3);
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack recovery revisits DMs seen live and a reconnect triggers a pass", async () => {
	const f = await fixture();
	try {
		await f.event({ type: "message", channel: "D1", channel_type: "im", user: "U1", ts: "1700000000.5", text: "hi" });
		f.api.history.D1 = [{ type: "message", user: "U1", ts: "1700000001.000000", text: "missed while down" }];
		f.client.requests.length = 0;
		expect(await f.recoverMissedMessages()).toBe(true);
		const sent = f.client.requests.find((request) => request.verb === "chat.send")?.params as {
			origin: OriginRef;
			text: string;
		};
		expect(sent.origin).toEqual({ platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" });
		expect(sent.text).toBe("missed while down");
		// Reconnect: the scheduler runs another pass, deduped by the LRU and the watermark.
		f.client.requests.length = 0;
		f.gateway.adoptClient(f.client);
		await flush();
		expect(f.api.historyCalls.filter((channel) => channel === "D1").length).toBeGreaterThanOrEqual(2);
		expect(f.client.requests.filter((request) => request.verb === "chat.send")).toHaveLength(0);
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack recovery joins a still-pending live send instead of calling it a duplicate", async () => {
	// ARCH-02: the id must not be marked seen until the gateway acknowledges it.
	const f = await fixture({ C1: { engagement: "open" } }, { autoRecover: false });
	try {
		let release!: (error?: Error) => void;
		const held = new Promise<void>((resolve, reject) => {
			release = (error) => (error ? reject(error) : resolve());
		});
		const request = f.client.request.bind(f.client);
		f.client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
			if (verb === "chat.send") await held;
			return request<T>(verb, params);
		};
		const live = f.gateway.requestInbound("C1:1700000040.000001", origin, "live", engagement);
		const recovered = f.gateway.requestRecovered("C1:1700000040.000001", origin, "live", engagement);
		await flush();
		// The live send fails before acceptance: recovery must see "unavailable", never "duplicate".
		release(new Error("gateway connection closed"));
		expect(await live).toBeUndefined();
		expect((await recovered).verdict).toBe("unavailable");
		// Now it is retried and acknowledged; a later join reports duplicate. The failed
		// send detached the link (as a real closed socket would), so it reconnects first.
		f.client.request = request;
		f.client.failure = undefined;
		f.gateway.adoptClient(f.client);
		expect((await f.gateway.requestRecovered("C1:1700000040.000001", origin, "live", engagement)).verdict).toBe(
			"acked",
		);
		expect((await f.gateway.requestRecovered("C1:1700000040.000001", origin, "live", engagement)).verdict).toBe(
			"duplicate",
		);
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack recovery revisits a thread the persona joined after its parent fell behind the watermark", async () => {
	// ARCH-03: history only lists parents; replies to an old parent are found per thread.
	const f = await fixture({ C1: { engagement: "open" } }, { autoRecover: false });
	try {
		const replies: Record<string, Record<string, unknown>[]> = {};
		f.api.conversationsReplies = async (channel: string, ts: string) => ({
			messages: replies[`${channel}:${ts}`] ?? [],
			has_more: false,
		});
		// The persona is engaged in a thread rooted at 1.0 (live traffic).
		await f.event({ ...inbound({ ts: "1700000010.000001", thread_ts: "1700000001.000000", text: "in thread" }) });
		f.api.history.C1 = [{ type: "message", user: "U1", ts: "1700000050.000001", text: "unrelated newer" }];
		expect(await f.recoverMissedMessages()).toBe(true);
		let cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.recoveredThrough.C1).toBe("1700000050.000001");
		expect(Object.keys(cursors.participatedThreads)).toEqual(["C1:1700000001.000000"]);
		// A reply lands in that old thread while the adapter is down; history shows no parent.
		replies["C1:1700000001.000000"] = [
			{ type: "message", user: "U1", ts: "1700000001.000000", text: "root" },
			{ type: "message", user: "U1", ts: "1700000060.000001", text: "late reply" },
		];
		f.api.history.C1 = [];
		f.client.requests.length = 0;
		expect(await f.recoverMissedMessages()).toBe(true);
		const sent = f.client.requests.filter((request) => request.verb === "chat.send");
		expect(sent).toHaveLength(1);
		expect((sent[0]?.params as { origin: OriginRef; text: string }).text).toBe("late reply");
		expect((sent[0]?.params as { origin: OriginRef }).origin).toEqual({
			platform: "slack",
			kind: "thread",
			conversationId: "C1:1700000001.000000",
			parentId: "C1",
		});
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.participatedThreads["C1:1700000001.000000"]?.through).toBe("1700000060.000001");
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack recovery drains an oversized gap across passes and only then moves the watermark", async () => {
	// ARCH-04: a truncated newest-first window resumes below what it delivered.
	const f = await fixture({ C1: { engagement: "open" } }, { autoRecover: false });
	try {
		const all = Array.from({ length: 6 }, (_, index) => ({
			type: "message",
			user: "U1",
			ts: `17000000${10 + index}.000001`,
			text: `m${index}`,
		})).reverse(); // newest first, as Slack returns them
		f.api.conversationsHistory = async (
			_channel: string,
			options: { oldest?: string; latest?: string; cursor?: string; limit?: number } = {},
		) => {
			const window = all.filter(
				(message) =>
					Number(message.ts) > Number(options.oldest ?? 0) &&
					(!options.latest || Number(message.ts) < Number(options.latest)),
			);
			const page = options.cursor === "p2" ? window.slice(2, 4) : window.slice(0, 2);
			const hasMore = options.cursor === "p2" ? window.length > 4 : window.length > 2;
			return {
				messages: page,
				has_more: hasMore,
				...(hasMore ? { next_cursor: options.cursor === "p2" ? "p3" : "p2" } : {}),
			};
		};
		const texts = () =>
			f.client.requests.filter((r) => r.verb === "chat.send").map((r) => (r.params as { text: string }).text);
		// Pass 1: two pages of two, truncated; delivers the newest four, no watermark yet.
		expect(await f.recoverMissedMessages()).toBe(false);
		expect(texts()).toEqual(["m2", "m3", "m4", "m5"]);
		let cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.recoveredThrough.C1).toBeUndefined();
		expect(cursors.continuation.C1).toEqual({ olderThan: "1700000012.000001", through: "1700000015.000001" });
		// Pass 2 resumes below m2 and closes the gap: watermark becomes the newest of the whole gap.
		expect(await f.recoverMissedMessages()).toBe(true);
		expect(texts()).toEqual(["m2", "m3", "m4", "m5", "m0", "m1"]);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.continuation.C1).toBeUndefined();
		expect(cursors.recoveredThrough.C1).toBe("1700000015.000001");
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack recovery dead-letters a payload the gateway keeps refusing but never a link outage", async () => {
	// ARCH-12: only terminal, payload-specific refusals burn the budget.
	const f = await fixture({ C1: { engagement: "open" } }, { autoRecover: false });
	try {
		f.api.history.C1 = [{ type: "message", user: "U1", ts: "1700000040.000001", text: "poison" }];
		const refused = Object.assign(new Error("chat.send requires non-empty text"), { code: "invalid_params" });
		// Every failed send detaches the link as a real closed socket would; each pass
		// therefore starts by re-adopting the client, like a reconnect.
		const pass = async (failure?: Error) => {
			f.client.failure = failure;
			f.gateway.adoptClient(f.client);
			return f.recoverMissedMessages();
		};
		expect(await pass(refused)).toBe(false);
		expect(await pass(refused)).toBe(false);
		let cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.attempts["C1:1700000040.000001"]?.attempts).toBe(2);
		expect(cursors.recoveredThrough.C1).toBeUndefined();
		// An outage in between does not count against the message.
		expect(await pass(new Error("gateway connection closed"))).toBe(false);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.attempts["C1:1700000040.000001"]?.attempts).toBe(2);
		// Third terminal refusal: dead-lettered, digest recorded, channel moves on.
		expect(await pass(refused)).toBe(true);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.deadLetters).toHaveLength(1);
		expect(cursors.deadLetters[0]).toMatchObject({
			messageId: "C1:1700000040.000001",
			classification: "terminal-message",
			attempts: 3,
		});
		expect(cursors.deadLetterDigest.C1?.count).toBe(1);
		expect(cursors.attempts["C1:1700000040.000001"]).toBeUndefined();
		expect(cursors.recoveredThrough.C1).toBe("1700000040.000001");
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack recovery keeps retrying until its cursor state is actually on disk", async () => {
	// ARCH-14: in-memory progress that failed to persist is not completion.
	const f = await fixture({ C1: { engagement: "open" } }, { autoRecover: false });
	try {
		f.api.history.C1 = [{ type: "message", user: "U1", ts: "1700000040.000001", text: "first" }];
		const path = f.recoveryCursorPath;
		// Make the store unwritable by putting a FILE where its directory must go.
		const { mkdir, rm, writeFile } = await import("node:fs/promises");
		const { dirname } = await import("node:path");
		const storeDir = dirname(path);
		expect(storeDir.startsWith(join(tmpdir(), "slack-recovery-"))).toBe(true);
		await mkdir(dirname(storeDir), { recursive: true });
		await writeFile(storeDir, "not a directory");
		expect(await f.recoverMissedMessages()).toBe(false);
		await rm(storeDir, { force: true });
		await mkdir(storeDir, { recursive: true });
		expect(await f.recoverMissedMessages()).toBe(true);
		expect((await loadRecoveryCursors(path)).recoveredThrough.C1).toBe("1700000040.000001");
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack first-contact DMs arriving together are all remembered for recovery", async () => {
	// ARCH-06: concurrent first loads must not overwrite each other's registration.
	const f = await fixture(undefined, { autoRecover: false });
	try {
		await Promise.all([
			f.handleEvent({ type: "message", channel: "D1", channel_type: "im", user: "U1", ts: "1700000000.1", text: "a" }),
			f.handleEvent({ type: "message", channel: "D2", channel_type: "im", user: "U2", ts: "1700000000.2", text: "b" }),
			f.handleEvent({ type: "message", channel: "D3", channel_type: "im", user: "U3", ts: "1700000000.3", text: "c" }),
		]);
		await f.ingress.drain();
		await flush();
		await f.recoverMissedMessages();
		expect(Object.keys((await loadRecoveryCursors(f.recoveryCursorPath)).knownDms).sort()).toEqual(["D1", "D2", "D3"]);
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack drains queued edits before a reconnect starts recovery", async () => {
	// ARCH-15: a backfilled message must not overtake an edit made before the outage.
	const f = await fixture({ C1: { engagement: "open" } }, { autoRecover: false });
	try {
		f.api.history.C1 = [{ type: "message", user: "U1", ts: "1700000040.000001", text: "recovered" }];
		const disconnected = new Gateway();
		disconnected.failure = new Error("gateway connection closed");
		f.gateway.adoptClient(disconnected);
		f.gateway.sendEdit("C1:1700000001.000001", origin, "edited while down", engagement);
		await flush();
		expect(f.gateway.pendingEdits).toHaveLength(1);
		const order: string[] = [];
		const client = new Gateway();
		const request = client.request.bind(client);
		client.request = async <T>(verb: string, params?: unknown): Promise<T> => {
			order.push(`${verb}:${(params as { text?: string }).text ?? ""}`);
			if (verb === "chat.edit") await flush();
			return request<T>(verb, params);
		};
		f.gateway.onConnected = () => f.recovery.trigger();
		f.gateway.adoptClient(client);
		// The edit is slow on purpose; recovery may not start until it is acknowledged.
		while (f.gateway.pendingEdits.length > 0) await flush();
		expect(order).toEqual(["chat.edit:edited while down"]);
		for (let i = 0; i < 20 && !order.includes("chat.send:recovered"); i++) {
			await flush();
			await f.recovery.idle();
		}
		expect(order).toContain("chat.send:recovered");
		expect(order.indexOf("chat.edit:edited while down")).toBeGreaterThanOrEqual(0);
		expect(order.indexOf("chat.edit:edited while down")).toBeLessThan(order.indexOf("chat.send:recovered"));
	} finally {
		f.socket.stop();
		f.recovery.stop();
	}
});

test("Slack recovery drains an unengaged truncated thread across passes instead of re-walking its prefix", async () => {
	// G3-THREAD-CONTINUATION: a pending root carries its own cursor.
	const f = await fixture({ C1: { engagement: "open" } }, { autoRecover: false });
	try {
		f.client.engaged = false; // nobody engages: no participatedThreads entry ever appears
		const replies = Array.from({ length: 5 }, (_, i) => ({
			type: "message",
			user: "U1",
			ts: `1700000001.00000${i + 1}`,
			text: `reply ${i + 1}`,
		}));
		f.api.history.C1 = [{ type: "message", user: "U1", ts: "1700000001.000000", text: "root", reply_count: 5 }];
		f.api.conversationsReplies = async (
			_channel: string,
			_ts: string,
			options: { oldest?: string; cursor?: string; limit?: number } = {},
		) => {
			const after = replies.filter((r) => Number(r.ts) > Number(options.oldest ?? 0));
			const page = options.cursor === "p2" ? after.slice(2, 4) : after.slice(0, 2);
			const more = options.cursor === "p2" ? after.length > 4 : after.length > 2;
			return {
				messages: page,
				has_more: more,
				...(more ? { next_cursor: options.cursor === "p2" ? "p3" : "p2" } : {}),
			};
		};
		const sent = () =>
			f.client.requests.filter((r) => r.verb === "chat.send").map((r) => (r.params as { text: string }).text);
		expect(await f.recoverMissedMessages()).toBe(false);
		let cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(Object.keys(cursors.participatedThreads)).toEqual([]);
		expect(cursors.pendingThreads["C1:1700000001.000000"]?.through).toBe("1700000001.000004");
		expect(sent()).toEqual(["root", "reply 1", "reply 2", "reply 3", "reply 4"]);
		// The next pass resumes after reply 4, never re-sending 1-4.
		expect(await f.recoverMissedMessages()).toBe(true);
		cursors = await loadRecoveryCursors(f.recoveryCursorPath);
		expect(cursors.pendingThreads["C1:1700000001.000000"]).toBeUndefined();
		expect(sent()).toEqual(["root", "reply 1", "reply 2", "reply 3", "reply 4", "reply 5"]);
	} finally {
		f.socket.stop();
	}
});

test("Slack recovery gate times socket and gateway outages independently", async () => {
	// G4-OVERLAPPING-OUTAGES: a quick gateway reconnect must not erase a long socket outage.
	let clock = 1_700_000_100_000;
	const api = new Api();
	const gateway = new Gateway();
	const adapter = await startSlackAdapter(
		{
			botToken: "xoxb-test",
			appToken: "xapp-test",
			botTokenFile: "bot",
			appTokenFile: "app",
			configPath: "test",
			gatewaySocket: `/tmp/slack-missing-${crypto.randomUUID()}.sock`,
			channels: { C1: { engagement: "open" } },
		},
		{
			api,
			recoveryCursorPath: join(
				tmpdir(),
				`slack-recovery-${crypto.randomUUID()}`,
				"adapters/slack/recovery-cursor.json",
			),
			now: () => clock,
			log: { log() {}, error() {} },
			socketFactory: () => {
				const socket = new Socket();
				queueMicrotask(() => socket.onopen?.({}));
				return socket;
			},
		},
	);
	try {
		adapter.gateway.onConnected = undefined;
		adapter.gateway.adoptClient(gateway);
		api.history.C1 = [];
		expect(await adapter.recoverMissedMessages()).toBe(true); // clean pass now
		const calls = () => api.historyCalls.length;
		const before = calls();
		// Socket drops at t+1s; gateway drops at t+2s and is back at t+3s; socket back at t+10s.
		clock += 1_000;
		adapter.links.disconnected("socket");
		clock += 1_000;
		adapter.links.disconnected("gateway");
		clock += 1_000;
		adapter.links.reconnected("gateway");
		await adapter.recovery.idle();
		// Gateway blip of 1s after a clean pass 3s ago: gated, no history reads.
		expect(calls()).toBe(before);
		clock += 7_000;
		adapter.links.reconnected("socket");
		await adapter.recovery.idle();
		// The socket was down 9s: that outage earns a pass even though the gateway blip did not.
		expect(calls()).toBeGreaterThan(before);
	} finally {
		adapter.socket.stop();
		adapter.recovery.stop();
	}
});
