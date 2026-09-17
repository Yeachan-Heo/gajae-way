import { expect, test } from "bun:test";
import type { ChatMessagePayload, ChatProgressPayload, OriginRef } from "@gajaeway/protocol";
import { type GatewayClientLike, ReconnectingGateway, settleSlackDelivery, subscribeSlackProgress } from "../src/main";
import { WORKING_STATUS_STALE_MS, WorkingStatus, workingStatusText } from "../src/status";

const origin: OriginRef = { platform: "slack", kind: "channel", conversationId: "C1" };
const progress = (extra: Partial<ChatProgressPayload> = {}): ChatProgressPayload => ({
	turnId: "turn",
	origin,
	elapsedMs: 125_000,
	toolCalls: 3,
	outputTokens: 1200,
	...extra,
});
async function flush() {
	for (let i = 0; i < 20; i++) await Promise.resolve();
}
function fixture() {
	const posts: unknown[][] = [];
	const updates: unknown[][] = [];
	const deletes: unknown[][] = [];
	const errors: string[] = [];
	const timers = new Set<{ fn: () => void; ms: number }>();
	const api = {
		async postMessage(channel: string, text: string, threadTs?: string) {
			posts.push([channel, text, threadTs]);
			return { channel, ts: "10.001" };
		},
		async updateMessage(channel: string, ts: string, text: string) {
			updates.push([channel, ts, text]);
		},
		async deleteMessage(channel: string, ts: string) {
			deletes.push([channel, ts]);
		},
		async addReaction() {},
	};
	const status = new WorkingStatus(
		api,
		{ error: (text: string) => errors.push(text) },
		(fn, ms) => {
			const timer = { fn, ms, unref() {} };
			timers.add(timer);
			return timer;
		},
		(timer) => {
			timers.delete(timer as { fn: () => void; ms: number });
		},
	);
	return { api, status, posts, updates, deletes, errors, timers };
}

test("Slack working status formatting omits unknown counters", () => {
	expect(workingStatusText(progress())).toBe("⏳ working… (2m 05s, 3 tools, 1.2k tok)");
	expect(workingStatusText(progress({ elapsedMs: 999, toolCalls: 0, outputTokens: 0 }))).toBe("⏳ working… (0s)");
	expect(workingStatusText(progress({ elapsedMs: 1000, toolCalls: 1, outputTokens: 999 }))).toBe(
		"⏳ working… (1s, 1 tool, 999 tok)",
	);
	expect(workingStatusText(progress({ toolCalls: 0, outputTokens: 1000 }))).toBe("⏳ working… (2m 05s, 1.0k tok)");
});

for (const routed of [
	origin,
	{ platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" },
	{ platform: "slack", kind: "thread", conversationId: "C1:9.001", parentId: "C1" },
] as const) {
	test(`Slack status routes ${routed.kind}: posts on arm, updates one message, and disarms`, async () => {
		const f = fixture();
		const tick = progress({ origin: routed });
		// Not armed: an overheard turn shows nothing.
		await f.status.update(tick);
		expect(f.posts).toHaveLength(0);
		// Armed: the hint is posted immediately, before any progress tick arrives.
		f.status.arm(routed);
		await flush();
		const channel = routed.kind === "thread" ? routed.parentId : routed.conversationId;
		expect(f.posts).toEqual([[channel, "⏳ working…", routed.kind === "thread" ? "9.001" : undefined]]);
		await f.status.update(tick);
		await f.status.update({ ...tick, elapsedMs: 126_000 });
		expect(f.posts).toHaveLength(1);
		expect(f.updates).toEqual([
			[channel, "10.001", workingStatusText(tick)],
			[channel, "10.001", workingStatusText({ ...tick, elapsedMs: 126_000 })],
		]);
		expect(f.timers.size).toBe(1);
		await f.status.clear(routed.conversationId);
		expect(f.deletes).toEqual([[channel, "10.001"]]);
		expect(f.timers.size).toBe(0);
		await f.status.update(tick);
		expect(f.posts).toHaveLength(1);
	});
}

test("Slack pending post is unique and a clear deletes its late result without touching a newer turn", async () => {
	const f = fixture();
	let finish!: (message: { channel: string; ts: string }) => void;
	f.api.postMessage = async () =>
		new Promise((resolve) => {
			finish = resolve;
		});
	f.status.arm(origin);
	await flush();
	await f.status.update(progress());
	await f.status.clear("C1");
	f.api.postMessage = async () => ({ channel: "C1", ts: "20.001" });
	f.status.arm(origin);
	await flush();
	await f.status.update(progress());
	finish({ channel: "C1", ts: "10.001" });
	await flush();
	await f.status.update(progress());
	expect(f.deletes).toEqual([["C1", "10.001"]]);
	expect(f.updates[0]?.[1]).toBe("20.001");
});

test("Slack stale timer clears and disarms the status", async () => {
	const f = fixture();
	f.status.arm(origin);
	await flush();
	const timer = [...f.timers][0];
	expect(timer?.ms).toBe(WORKING_STATUS_STALE_MS);
	timer?.fn();
	await flush();
	expect(f.deletes).toEqual([["C1", "10.001"]]);
	await f.status.update(progress());
	expect(f.posts).toHaveLength(1);
});

test("Slack post and update failures are logged, retain an existing message, and never throw", async () => {
	const f = fixture();
	const post = f.api.postMessage;
	f.api.postMessage = async () => {
		throw new Error("Slack post failed");
	};
	f.status.arm(origin);
	await flush();
	f.api.postMessage = post;
	await f.status.update(progress());
	f.api.updateMessage = async () => {
		throw new Error("Slack update failed");
	};
	await f.status.update(progress());
	await f.status.update(progress());
	expect(f.posts).toHaveLength(1);
	expect(f.errors).toHaveLength(3);
	await f.status.clear("C1");
	expect(f.deletes).toHaveLength(1);
	f.status.arm(origin);
	await flush();
	f.api.deleteMessage = async () => {
		throw new Error("Slack already deleted");
	};
	await f.status.clear("C1");
});

test("Slack ignores foreign origins even when armed", async () => {
	const f = fixture();
	f.status.arm({ ...origin, platform: "discord" });
	await flush();
	f.status.arm(origin);
	await flush();
	await f.status.update(progress({ origin: { ...origin, platform: "discord" } }));
	expect(f.posts).toHaveLength(1);
	expect(f.updates).toHaveLength(0);
});

class Gateway implements GatewayClientLike {
	readonly handlers = new Set<(p: ChatProgressPayload) => void>();
	readonly requests: string[] = [];
	engaged = true;
	async request<T>(verb: string): Promise<T> {
		this.requests.push(verb);
		return { engaged: this.engaged } as T;
	}
	onChatMessage() {
		return () => {};
	}
	onChatProgress(handler: (p: ChatProgressPayload) => void) {
		this.handlers.add(handler);
		return () => {
			this.handlers.delete(handler);
		};
	}
	emit(p: ChatProgressPayload) {
		for (const handler of this.handlers) handler(p);
	}
}

test("Slack progress final clears silent turns, logs failures, and unsubscribes", async () => {
	const f = fixture();
	const gateway = new Gateway();
	const off = subscribeSlackProgress(gateway, f.status);
	f.status.arm(origin);
	await flush();
	gateway.emit(progress());
	await flush();
	gateway.emit(progress({ final: true }));
	await flush();
	expect(f.deletes).toHaveLength(1);
	off();
	expect(gateway.handlers.size).toBe(0);
	const errors: string[] = [];
	const failingOff = subscribeSlackProgress(
		gateway,
		{
			async update() {
				throw new Error("Slack update failed");
			},
			async clear() {
				throw new Error("Slack clear failed");
			},
		},
		{ error: (text: string) => errors.push(text) },
	);
	gateway.emit(progress());
	gateway.emit(progress({ final: true }));
	await flush();
	expect(errors).toHaveLength(2);
	failingOff();
});

for (const reaction of [false, true]) {
	for (const fails of [false, true]) {
		test(`Slack delivery clears status for reaction=${reaction} failure=${fails}`, async () => {
			const f = fixture();
			const gateway = new Gateway();
			let cleared = 0;
			if (fails) {
				f.api.postMessage = async () => {
					throw new Error("Slack post failed");
				};
				f.api.addReaction = async () => {
					throw new Error("Slack reaction failed");
				};
			}
			const message = {
				origin,
				text: "reply",
				deliveryId: "delivery",
				...(reaction
					? {
							reaction: { targetMessageId: "C1:1.001", emoji: "👍", emojiName: "thumbsup" },
						}
					: {}),
			} as ChatMessagePayload;
			await settleSlackDelivery(gateway, f.api, message, console, {
				async clear(id) {
					expect(id).toBe("C1");
					cleared++;
					throw new Error("Slack cleanup failed");
				},
			});
			expect(cleared).toBe(1);
			expect(gateway.requests).toEqual([fails ? "delivery.fail" : "delivery.confirm"]);
		});
	}
}

test("Slack inbound arms only engaged addressed turns; edits and adopted progress follow the same lifecycle", async () => {
	for (const engaged of [true, false]) {
		for (const engagement of [
			{ group: false, mentioned: false, authorId: "U1" },
			{ group: true, mentioned: true, authorId: "U1" },
			{ group: true, mentioned: false, authorId: "U1" },
		]) {
			const f = fixture();
			const client = new Gateway();
			client.engaged = engaged;
			const gateway = new ReconnectingGateway("unused", f.api, client, f.status);
			await gateway.requestInbound("C1:1.001", origin, "hello", engagement);
			await flush();
			// Presence appears on acceptance itself, with no progress tick needed.
			expect(f.posts.length).toBe(engaged && (!engagement.group || engagement.mentioned) ? 1 : 0);
			client.emit(progress());
			await flush();
			expect(f.posts.length).toBe(engaged && (!engagement.group || engagement.mentioned) ? 1 : 0);
			await f.status.clear("C1");
			gateway.sendEdit("C1:1.001", origin, "edited", engagement);
			await flush();
			expect(f.posts.length).toBe(engaged && (!engagement.group || engagement.mentioned) ? 2 : 0);
			gateway.adoptClient(new Gateway());
			expect(client.handlers.size).toBe(0);
			await f.status.clear("C1");
		}
	}
});
