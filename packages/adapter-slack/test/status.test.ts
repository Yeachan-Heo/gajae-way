import { expect, test } from "bun:test";
import {
	type ChatMessagePayload,
	type ChatProgressPayload,
	type OriginRef,
	PRESENCE_MIN_SWAP_MS,
	presenceEffortBucket,
	presenceMarkersFor,
	presenceSnapshot,
} from "@gajaeway/protocol";
import { type GatewayClientLike, ReconnectingGateway, settleSlackDelivery, subscribeSlackProgress } from "../src/main";
import { isPresenceReaction, WORKING_STATUS_STALE_MS, WorkingStatus } from "../src/status";

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
	let clock = 0;
	const adds: string[] = [];
	const removes: string[] = [];
	const posts: unknown[] = [];
	const errors: string[] = [];
	const timers = new Set<{ fn: () => void; ms: number }>();
	const api = {
		async addReaction(channel: string, ts: string, name: string) {
			adds.push(`${channel}:${ts}:${name}`);
		},
		async removeReaction(channel: string, ts: string, name: string) {
			removes.push(`${channel}:${ts}:${name}`);
		},
		async postMessage(channel: string, text: string, threadTs?: string) {
			posts.push([channel, text, threadTs]);
			return { channel, ts: "10.001" };
		},
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
		() => clock,
	);
	return {
		api,
		status,
		adds,
		removes,
		posts,
		errors,
		timers,
		tick(ms: number) {
			clock += ms;
		},
	};
}
const names = (entries: string[]) => entries.map((entry) => entry.split(":").at(-1));

test("presence buckets: phase from activity, clock per minute, effort from tool calls then tokens", () => {
	expect(presenceSnapshot(progress({ elapsedMs: 5_000, toolCalls: 0, outputTokens: 0 }))).toEqual({
		phase: "queued",
		clock: 0,
		effort: -1,
	});
	expect(presenceSnapshot(progress({ activity: { kind: "tool", label: "bash" } }))).toEqual({
		phase: "tool",
		clock: 2,
		effort: 2,
	});
	expect(presenceEffortBucket({ toolCalls: 0, outputTokens: 5_000 })).toBe(4);
	expect(presenceEffortBucket({ toolCalls: 40, outputTokens: 0 })).toBe(5);
	expect(presenceMarkersFor({ phase: "writing", clock: 12, effort: 5 }).map((m) => m.slackName)).toEqual([
		"writing_hand",
		"clock12",
		"100",
	]);
	// A 20-minute turn is still capped at the twelfth clock face.
	expect(presenceSnapshot(progress({ elapsedMs: 20 * 60_000 })).clock).toBe(12);
});

test("presence markers never collide with the persona's reaction allowlist", async () => {
	const { REACTION_ALLOWLIST } = await import("@gajaeway/protocol");
	const { SLACK_REACTION_NAMES } = await import("../src/reactions");
	for (const entry of REACTION_ALLOWLIST)
		expect(isPresenceReaction(SLACK_REACTION_NAMES[entry.name] ?? "")).toBe(false);
	expect(isPresenceReaction("wrench")).toBe(true);
	expect(isPresenceReaction("+1")).toBe(false);
});

test("Slack presence is reactions on the triggering message, never a posted or edited message", async () => {
	const f = fixture();
	f.status.arm(origin, "C1:1.000");
	await flush();
	expect(f.adds).toEqual(["C1:1.000:hourglass_flowing_sand"]);
	expect(f.posts).toEqual([]);
	// First tick past the window: phase → tool, one minute, three tools.
	f.tick(PRESENCE_MIN_SWAP_MS);
	await f.status.update(progress({ elapsedMs: 61_000, activity: { kind: "tool", label: "bash" } }));
	expect(names(f.removes)).toEqual(["hourglass_flowing_sand"]);
	expect(names(f.adds)).toEqual(["hourglass_flowing_sand", "wrench", "clock1", "three"]);
	// Same buckets: nothing.
	f.tick(PRESENCE_MIN_SWAP_MS);
	await f.status.update(progress({ elapsedMs: 90_000, activity: { kind: "tool", label: "read" } }));
	expect(f.adds).toHaveLength(4);
	expect(f.removes).toHaveLength(1);
	// Only the clock advances: one remove, one add - the phase and effort stay.
	f.tick(PRESENCE_MIN_SWAP_MS);
	await f.status.update(progress({ elapsedMs: 125_000, activity: { kind: "tool", label: "read" } }));
	expect(names(f.removes)).toEqual(["hourglass_flowing_sand", "clock1"]);
	expect(names(f.adds).at(-1)).toBe("clock2");
	// Delivery: every marker we own comes off, nothing else is touched.
	await f.status.clear("C1");
	expect(new Set(names(f.removes))).toEqual(new Set(["hourglass_flowing_sand", "clock1", "wrench", "clock2", "three"]));
	expect(f.posts).toEqual([]);
	expect(f.timers.size).toBe(0);
});

test("Slack presence swaps are coalesced to one per window even when the phase flips", async () => {
	const f = fixture();
	f.status.arm(origin, "C1:1.000");
	await flush();
	await f.status.update(
		progress({ elapsedMs: 3_000, toolCalls: 0, outputTokens: 0, activity: { kind: "tool", label: "bash" } }),
	);
	f.tick(5_000);
	await f.status.update(
		progress({ elapsedMs: 8_000, toolCalls: 0, outputTokens: 0, activity: { kind: "thinking", label: "thinking" } }),
	);
	expect(f.adds).toHaveLength(1);
	expect(f.removes).toHaveLength(0);
	f.tick(PRESENCE_MIN_SWAP_MS);
	await f.status.update(
		progress({ elapsedMs: 20_000, toolCalls: 1, outputTokens: 0, activity: { kind: "writing", label: "writing" } }),
	);
	expect(names(f.removes)).toEqual(["hourglass_flowing_sand"]);
	expect(names(f.adds)).toEqual(["hourglass_flowing_sand", "writing_hand", "one"]);
	await f.status.clear("C1");
});

test("Slack presence: a newer turn on the same conversation takes over, and the stale timer cleans up", async () => {
	const f = fixture();
	f.status.arm(origin, "C1:1.000");
	await flush();
	f.status.arm(origin, "C1:2.000");
	await flush();
	expect(f.removes).toEqual(["C1:1.000:hourglass_flowing_sand"]);
	expect(f.adds).toEqual(["C1:1.000:hourglass_flowing_sand", "C1:2.000:hourglass_flowing_sand"]);
	const timer = [...f.timers][0];
	expect(timer?.ms).toBe(WORKING_STATUS_STALE_MS);
	timer?.fn();
	await flush();
	expect(f.removes.at(-1)).toBe("C1:2.000:hourglass_flowing_sand");
	// Cleared: later progress is ignored.
	f.tick(PRESENCE_MIN_SWAP_MS);
	await f.status.update(progress());
	expect(f.adds).toHaveLength(2);
});

test("Slack presence failures are logged, never thrown, and a clear during a swap still cleans up", async () => {
	const f = fixture();
	f.api.addReaction = async () => {
		throw new Error("Slack reaction failed");
	};
	f.status.arm(origin, "C1:1.000");
	await flush();
	expect(f.errors).toHaveLength(1);
	// Slow add: clear runs while it is in flight; the late marker is removed.
	let finish!: () => void;
	f.api.addReaction = async (channel: string, ts: string, name: string) => {
		f.adds.push(`${channel}:${ts}:${name}`);
		await new Promise<void>((resolve) => {
			finish = resolve;
		});
	};
	f.status.arm(origin, "C1:3.000");
	await flush();
	await f.status.clear("C1");
	finish();
	await flush();
	expect(f.removes.at(-1)).toBe("C1:3.000:hourglass_flowing_sand");
	// Malformed message id: nothing is attempted.
	f.status.arm(origin, "not-an-id");
	await flush();
	expect(f.adds.filter((entry) => entry.includes("not-an-id"))).toEqual([]);
	// Foreign platform: ignored.
	f.status.arm({ ...origin, platform: "discord" }, "C1:4.000");
	await flush();
	expect(f.adds.some((entry) => entry.includes("4.000"))).toBe(false);
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

test("Slack progress final clears the gradient of silent turns, logs failures, and unsubscribes", async () => {
	const f = fixture();
	const gateway = new Gateway();
	const off = subscribeSlackProgress(gateway, f.status);
	f.status.arm(origin, "C1:1.000");
	await flush();
	gateway.emit(progress());
	await flush();
	gateway.emit(progress({ final: true }));
	await flush();
	expect(names(f.removes)).toContain("hourglass_flowing_sand");
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
		test(`Slack delivery clears presence for reaction=${reaction} failure=${fails}`, async () => {
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
				...(reaction ? { reaction: { targetMessageId: "C1:1.001", emoji: "👍", emojiName: "thumbsup" } } : {}),
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

test("Slack inbound arms presence only for engaged addressed turns, on the triggering message", async () => {
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
			const expected = engaged && (!engagement.group || engagement.mentioned);
			expect(f.adds).toEqual(expected ? ["C1:1.001:hourglass_flowing_sand"] : []);
			expect(f.posts).toEqual([]);
			await f.status.clear("C1");
			gateway.sendEdit("C1:1.001", origin, "edited", engagement);
			await flush();
			expect(f.adds).toHaveLength(expected ? 2 : 0);
			gateway.adoptClient(new Gateway());
			expect(client.handlers.size).toBe(0);
			await f.status.clear("C1");
		}
	}
});

test("Slack presence: re-arming the same message keeps ownership of markers already on it", async () => {
	// G5-PRESENCE-SAME-MESSAGE-REARM: an accepted edit re-arms the same id after a
	// completed multi-marker swap; the old markers must still be ours to remove.
	const f = fixture();
	f.status.arm(origin, "C1:1.000");
	await flush();
	f.tick(PRESENCE_MIN_SWAP_MS);
	await f.status.update(progress({ elapsedMs: 61_000, activity: { kind: "tool", label: "bash" } }));
	expect(names(f.adds)).toEqual(["hourglass_flowing_sand", "wrench", "clock1", "three"]);
	// Same message re-armed: gradient restarts from queued; nothing is orphaned.
	f.status.arm(origin, "C1:1.000");
	await flush();
	expect(new Set(names(f.removes))).toEqual(new Set(["hourglass_flowing_sand", "wrench", "clock1", "three"]));
	expect(names(f.adds).at(-1)).toBe("hourglass_flowing_sand");
	await f.status.clear("C1");
	// Every marker ever added was removed; the message is clean.
	const balance = new Map<string, number>();
	for (const name of names(f.adds)) balance.set(name as string, (balance.get(name as string) ?? 0) + 1);
	for (const name of names(f.removes)) balance.set(name as string, (balance.get(name as string) ?? 0) - 1);
	for (const [, count] of balance) expect(count).toBe(0);
});

test("Slack presence: a swap requested while a slow call is in flight is applied afterwards, not lost", async () => {
	// G5-PRESENCE-BUSY-STATE-LOSS: desired state is reconciled after the in-flight
	// operation, and identical later heartbeats do not need to re-request it.
	const f = fixture();
	let release!: () => void;
	const gate = new Promise<void>((resolve) => {
		release = resolve;
	});
	const add = f.api.addReaction;
	f.api.addReaction = async (channel: string, ts: string, name: string) => {
		await add(channel, ts, name);
		if (name === "hourglass_flowing_sand") await gate;
	};
	f.status.arm(origin, "C1:1.000");
	await flush();
	// The queued add is still pending; a phase change arrives past the window.
	f.tick(PRESENCE_MIN_SWAP_MS);
	await f.status.update(
		progress({ elapsedMs: 20_000, toolCalls: 1, outputTokens: 0, activity: { kind: "tool", label: "bash" } }),
	);
	expect(names(f.removes)).toEqual([]);
	release();
	await flush();
	await flush();
	// After the slow add resolved, the loop re-diffed and applied the tool phase.
	expect(names(f.removes)).toEqual(["hourglass_flowing_sand"]);
	expect(names(f.adds)).toEqual(["hourglass_flowing_sand", "wrench", "one"]);
	await f.status.clear("C1");
});

test("Slack presence: cleanup failures are logged, never thrown, and never block delivery settlement", async () => {
	// RT-SLACK-54 / CLEAN-G5-04.
	const f = fixture();
	f.status.arm(origin, "C1:1.000");
	await flush();
	f.api.removeReaction = async () => {
		throw new Error("remove denied");
	};
	const gateway = new Gateway();
	await settleSlackDelivery(
		gateway,
		f.api,
		{ origin, text: "reply", deliveryId: "d" } as ChatMessagePayload,
		console,
		f.status,
	);
	expect(gateway.requests).toEqual(["delivery.confirm"]);
	expect(f.errors.some((line) => line.includes("remove denied"))).toBe(true);
});
