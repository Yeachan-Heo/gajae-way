import { expect, test } from "bun:test";
import type { ChatMessagePayload, ChatProgressPayload, OriginRef } from "@gajae-gateway/protocol";
import { SlackApiError } from "../src/api";
import { LIVE_REPLY_MAX_ENTRIES, LIVE_REPLY_TTL_MS, type LiveDeliveryInput, LiveReplyTracker } from "../src/live-reply";
import { type GatewayClientLike, settleSlackDelivery, subscribeSlackProgress } from "../src/main";
import { SLACK_MESSAGE_LIMIT } from "../src/mrkdwn";

const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C1" };

class LiveApi {
	readonly posts: unknown[][] = [];
	readonly updates: unknown[][] = [];
	postFailure?: Error;
	updateFailure?: Error;
	#seq = 0;
	async postMessage(channel: string, text: string, threadTs?: string) {
		this.posts.push([channel, text, threadTs]);
		if (this.postFailure) throw this.postFailure;
		this.#seq += 1;
		return { channel, ts: `1.00${this.#seq}` };
	}
	async updateMessage(channel: string, ts: string, text: string) {
		this.updates.push([channel, ts, text]);
		if (this.updateFailure) throw this.updateFailure;
	}
	async addReaction(_channel: string, _ts: string, _name: string) {}
}

const part = (extra: Partial<LiveDeliveryInput> = {}): LiveDeliveryInput => ({
	turnId: "turn",
	deliveryId: "d1",
	channel: "C1",
	text: "one",
	final: false,
	...extra,
});

const tracker = (api: LiveApi, now: () => number = Date.now) => new LiveReplyTracker(api, { error: () => {} }, now);

test("live reply posts the first mid-turn part and folds later parts in order", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	expect(await live.deliver(part())).toEqual({ kind: "posted" });
	expect(api.posts).toEqual([["C1", "one", undefined]]);
	expect(await live.deliver(part({ deliveryId: "d2", text: "two" }))).toEqual({ kind: "folded" });
	expect(api.updates).toEqual([["C1", "1.001", "one\n\ntwo"]]);
	expect(await live.deliver(part({ deliveryId: "d3", text: "three", final: true }))).toEqual({ kind: "folded" });
	expect(api.updates.at(-1)).toEqual(["C1", "1.001", "one\n\ntwo\n\nthree"]);
	expect(live.tracked("turn")).toBe(false);
	// The turn is over: a further part posts as an ordinary message.
	expect(await live.deliver(part({ deliveryId: "d4", text: "late" }))).toEqual({ kind: "untracked" });
	expect(api.updates).toHaveLength(2);
});

test("live reply threads the first post and ignores later threading directives", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	await live.deliver(part({ text: "one", threadTs: "1.000" }));
	expect(api.posts).toEqual([["C1", "one", "1.000"]]);
	await live.deliver(part({ deliveryId: "d2", text: "two", threadTs: "9.999" }));
	expect(api.updates).toEqual([["C1", "1.001", "one\n\ntwo"]]);
});

test("a single-part turn never opens an entry", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	expect(await live.deliver(part({ text: "the answer", final: true }))).toEqual({ kind: "untracked" });
	expect(api.posts).toEqual([]);
	expect(api.updates).toEqual([]);
});

test("a ledger replay of an applied part re-confirms without a platform call", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	await live.deliver(part());
	await live.deliver(part({ deliveryId: "d2", text: "two" }));
	live.close("turn");
	// The turn ended but d2's confirm may have been lost: the replay is answered.
	expect(await live.deliver(part({ deliveryId: "d2", text: "[recovered - may be a duplicate] two" }))).toEqual({
		kind: "replayed",
	});
	expect(api.updates).toHaveLength(1);
});

test("an unapplied middle part retries into its stored position, not after its neighbours", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	await live.deliver(part());
	// d2's edit fails ambiguously: the part is not applied, the delivery fails upstream.
	api.updateFailure = new Error("transport died before the response");
	await expect(live.deliver(part({ deliveryId: "d2", text: "two" }))).rejects.toThrow("transport died");
	api.updateFailure = undefined;
	// d3 applies without the missing part.
	await live.deliver(part({ deliveryId: "d3", text: "three" }));
	expect(api.updates.at(-1)).toEqual(["C1", "1.001", "one\n\nthree"]);
	// The ledger retries d2 with a duplicate-warning prefix; the stored position
	// and the original (unprefixed) text are used, so nothing is duplicated.
	await live.deliver(part({ deliveryId: "d2", text: "[recovered - may be a duplicate] two" }));
	expect(api.updates.at(-1)).toEqual(["C1", "1.001", "one\n\ntwo\n\nthree"]);
});

test("a render that would exceed the message limit seals the entry", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	await live.deliver(part({ text: "short" }));
	const huge = "x".repeat(SLACK_MESSAGE_LIMIT);
	expect(await live.deliver(part({ deliveryId: "d2", text: huge }))).toEqual({ kind: "untracked" });
	expect(api.updates).toEqual([]);
	// Sealed: later parts post as ordinary messages too.
	expect(await live.deliver(part({ deliveryId: "d3", text: "tail" }))).toEqual({ kind: "untracked" });
	expect(live.tracked("turn")).toBe(false);
});

test("a Slack refusal to edit seals the entry", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	await live.deliver(part());
	api.updateFailure = new SlackApiError(200, "message_not_found");
	expect(await live.deliver(part({ deliveryId: "d2", text: "two" }))).toEqual({ kind: "untracked" });
	api.updateFailure = undefined;
	expect(await live.deliver(part({ deliveryId: "d3", text: "three" }))).toEqual({ kind: "untracked" });
	// Only the refused d2 edit was attempted; the sealed entry never edits again.
	expect(api.updates).toHaveLength(1);
});

test("a first-part post failure drops the reservation so a retry can re-open", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	api.postFailure = new Error("post rejected");
	await expect(live.deliver(part())).rejects.toThrow("post rejected");
	api.postFailure = undefined;
	// The ledger retries the same delivery: it opens a fresh live message.
	expect(await live.deliver(part())).toEqual({ kind: "posted" });
	// The stub records the refused attempt too: rejection, then the retry's post.
	expect(api.posts).toHaveLength(2);
});

test("concurrent parts of one turn apply in arrival order", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	await live.deliver(part());
	await Promise.all([
		live.deliver(part({ deliveryId: "d2", text: "two" })),
		live.deliver(part({ deliveryId: "d3", text: "three" })),
	]);
	expect(api.updates.map((update) => update[2])).toEqual(["one\n\ntwo", "one\n\ntwo\n\nthree"]);
});

test("entries expire after the TTL and the table evicts the least recently touched", async () => {
	let clock = 1_000;
	const api = new LiveApi();
	const live = tracker(api, () => clock);
	await live.deliver(part({ turnId: "old" }));
	clock += LIVE_REPLY_TTL_MS + 1;
	// The sweep runs on the next deliver; the expired turn opens a fresh message.
	expect(await live.deliver(part({ turnId: "old", deliveryId: "d2", text: "again" }))).toEqual({
		kind: "posted",
	});
	expect(api.posts).toHaveLength(2);
	for (let index = 0; index < LIVE_REPLY_MAX_ENTRIES; index += 1) {
		await live.deliver(part({ turnId: `fill-${index}`, deliveryId: `f${index}`, text: "x" }));
	}
	// The table is at capacity: the next open evicts the least recently touched
	// turn, while a turn still inside the table keeps folding.
	await live.deliver(part({ turnId: "one-more", deliveryId: "m1", text: "x" }));
	expect(await live.deliver(part({ turnId: "one-more", deliveryId: "m2", text: "y" }))).toEqual({
		kind: "folded",
	});
	expect(await live.deliver(part({ turnId: "fill-0", deliveryId: "f0b", text: "z" }))).toEqual({
		kind: "posted",
	});
});

class Gateway implements GatewayClientLike {
	readonly requests: { verb: string; params: unknown }[] = [];
	readonly progressHandlers = new Set<(progress: ChatProgressPayload) => void>();
	async request<T = unknown>(verb: string, params?: unknown): Promise<T> {
		this.requests.push({ verb, params });
		return {} as T;
	}
	onChatMessage() {
		return () => {};
	}
	onChatProgress(handler: (progress: ChatProgressPayload) => void) {
		this.progressHandlers.add(handler);
		return () => {
			this.progressHandlers.delete(handler);
		};
	}
}

const delivery = (extra: Partial<ChatMessagePayload> = {}): ChatMessagePayload => ({
	turnId: "turn",
	origin,
	role: "assistant",
	text: "hello",
	final: true,
	deliveryId: "delivery",
	...extra,
});

test("settleSlackDelivery folds a streaming turn into one message", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	const gateway = new Gateway();
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ text: "starting", deliveryId: "d1", final: false }),
		console,
		undefined,
		undefined,
		live,
	);
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ text: "middle", deliveryId: "d2", final: false }),
		console,
		undefined,
		undefined,
		live,
	);
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ text: "done", deliveryId: "d3", final: true }),
		console,
		undefined,
		undefined,
		live,
	);
	expect(api.posts).toEqual([["C1", "starting", undefined]]);
	expect(api.updates.map((update) => update[2])).toEqual(["starting\n\nmiddle", "starting\n\nmiddle\n\ndone"]);
	expect(gateway.requests).toEqual([
		{ verb: "delivery.confirm", params: { deliveryId: "d1" } },
		{ verb: "delivery.confirm", params: { deliveryId: "d2" } },
		{ verb: "delivery.confirm", params: { deliveryId: "d3" } },
	]);
});

test("settleSlackDelivery posts normally when the tracker declines", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	const gateway = new Gateway();
	// Single-part turn: untracked, ordinary post.
	await settleSlackDelivery(
		gateway,
		api,
		delivery({ text: "answer", final: true }),
		console,
		undefined,
		undefined,
		live,
	);
	expect(api.posts).toEqual([["C1", "answer", undefined]]);
	expect(api.updates).toEqual([]);
	expect(gateway.requests).toEqual([{ verb: "delivery.confirm", params: { deliveryId: "delivery" } }]);
});

test("the final progress tick closes the live entry even without a terminal delivery", async () => {
	const api = new LiveApi();
	const live = tracker(api);
	const gateway = new Gateway();
	await live.deliver(part({ text: "working" }));
	const off = subscribeSlackProgress(gateway, undefined, { error: () => {} }, live);
	for (const handler of gateway.progressHandlers)
		handler({
			turnId: "turn",
			origin,
			elapsedMs: 1_000,
			toolCalls: 0,
			outputTokens: 0,
			final: true,
		});
	off();
	expect(live.tracked("turn")).toBe(false);
	expect(await live.deliver(part({ deliveryId: "d2", text: "late" }))).toEqual({ kind: "untracked" });
});
