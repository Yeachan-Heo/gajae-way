import { expect, spyOn, test } from "bun:test";
import type { ChatMessagePayload, EngagementContext, OriginRef } from "@gajaeway/protocol";
import { SlackApiError, SlackWebApi } from "../src/api";
import {
	addressedTurn,
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
	override async conversationsReplies() {
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
	const recoveryCursorPath = `/tmp/slack-recovery-${crypto.randomUUID()}.json`;
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
	return {
		...adapter,
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
	expect(addressedTurn({ group: false, mentioned: false })).toBe(true);
	expect(addressedTurn({ group: true, mentioned: false })).toBe(false);
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
	const api = new Api();
	await settleSlackDelivery(new Gateway(), api, delivery({ replyToMessageId: "C2:1.000" }));
	expect(api.posts).toEqual([["C1", "hello", undefined]]);
	expect(replyThreadTs(delivery({ replyToMessageId: "bad" }))).toBeUndefined();
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

for (const command of ["/new", "/reset", "/restart", "/unknown"])
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
									? "🦞 session reset"
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
