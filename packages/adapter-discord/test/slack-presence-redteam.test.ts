import { expect, spyOn, test } from "bun:test";
import type { ChatProgressPayload } from "@gajae-gateway/protocol";
import { type DiscordClientLike, isPresenceReaction, ReconnectingGateway, WorkingStatus } from "../src/main";

async function flush() {
	for (let i = 0; i < 100; i++) await Promise.resolve();
}

test("RT-SLACK-55 Discord gradient coalesces buckets and removes variation-selector writing marker", async () => {
	let now = 0;
	const added: string[] = [];
	const removed: string[] = [];
	const fetched: string[] = [];
	const active = new Set<string>();
	const message = {
		async react(emoji: string) {
			added.push(emoji);
			active.add(emoji);
		},
		reactions: {
			resolve(emoji: string) {
				return active.has(emoji)
					? {
							users: {
								async remove(id: string) {
									expect(id).toBe("BOT");
									removed.push(emoji);
									active.delete(emoji);
								},
							},
						}
					: undefined;
			},
		},
	};
	const discord = {
		channels: {
			async fetch(id: string) {
				expect(id).toBe("C1");
				return {
					messages: {
						async fetch(id: string) {
							fetched.push(id);
							return message;
						},
					},
					async send() {
						throw new Error("Presence must never send");
					},
				};
			},
		},
	} as unknown as DiscordClientLike;
	const status = new WorkingStatus(
		discord,
		console,
		() => ({ id: "BOT" }),
		() => now,
	);
	const origin = { platform: "discord", kind: "channel", conversationId: "C1" } as const;
	const progress = (extra: Partial<ChatProgressPayload> = {}): ChatProgressPayload => ({
		origin,
		turnId: "t",
		elapsedMs: now,
		toolCalls: 0,
		outputTokens: 0,
		final: false,
		...extra,
	});
	status.arm("C1", "123");
	await flush();
	expect(added).toEqual(["⏳"]);
	for (let i = 1; i <= 30; i++) {
		now = (i * 20000) / 30;
		await status.update(progress({ activity: { kind: i % 2 ? "tool" : "thinking", label: "phase" } }));
	}
	expect(added.length).toBeLessThanOrEqual(3);
	expect(removed.length).toBeLessThanOrEqual(2);
	for (const [time, tools, tokens, clock, effort] of [
		[61000, 3, 0, "🕐", "3️⃣"],
		[121000, 40, 0, "🕑", "💯"],
		[181000, 0, 5000, "🕒", "🔟"],
	] as const) {
		now = time;
		await status.update(
			progress({ toolCalls: tools, outputTokens: tokens, activity: { kind: "writing", label: "writing" } }),
		);
		expect(active.has(clock)).toBe(true);
		expect(active.has(effort)).toBe(true);
	}
	expect(active.has("✍️")).toBe(true);
	expect(isPresenceReaction("✍️")).toBe(true);
	expect(isPresenceReaction("✍")).toBe(true);
	await status.clear("C1");
	expect(removed).toContain("✍️");
	expect([...added].sort()).toEqual([...removed].sort());
	const count = removed.length;
	await status.clear("C1");
	expect(removed).toHaveLength(count);
	expect(fetched).toEqual(["123"]);
	let stale: (() => void) | undefined;
	const originalTimer = globalThis.setTimeout;
	const timer = spyOn(globalThis, "setTimeout").mockImplementationOnce(((callback: () => void, ms: number) => {
		stale = callback;
		return originalTimer(callback, ms);
	}) as typeof setTimeout);
	try {
		status.arm("C1", "124");
	} finally {
		timer.mockRestore();
	}
	await flush();
	expect(active.has("⏳")).toBe(true);
	expect(stale).toBeDefined();
	stale!();
	await flush();
	expect(active.size).toBe(0);
	expect([...added].sort()).toEqual([...removed].sort());
});

test("RT-SLACK-55 Discord own presence event filtered but human presence emoji is engagement", async () => {
	const calls: Array<{ verb: string; params: unknown }> = [];
	const client = {
		async request(verb: string, params: unknown) {
			calls.push({ verb, params });
			return {};
		},
		onChatMessage() {
			return () => {};
		},
		onChatProgress() {
			return () => {};
		},
	};
	const gateway = new ReconnectingGateway(
		"/tmp/absent-presence.sock",
		{} as DiscordClientLike,
		{} as any,
		undefined,
		undefined,
		"/tmp/no-presence-cursors",
		() => ({ id: "BOT" }),
		client as any,
	);
	const reaction = { emoji: { name: "✍️" }, message: { id: "123", channel: { id: "C1", type: 0, name: "test" } } };
	gateway.sendReaction(reaction, { id: "BOT", bot: true }, "add", { id: "BOT" });
	await flush();
	expect(calls).toEqual([]);
	gateway.sendReaction(reaction, { id: "HUMAN", username: "human" }, "add", { id: "BOT" });
	await flush();
	expect(calls).toHaveLength(1);
	expect(calls[0]).toMatchObject({
		verb: "engagement.reaction",
		params: { emoji: "✍️", engagement: { authorId: "HUMAN" } },
	});
});
test("RT-SLACK-68 rejected message fetch logs without markers and later update retries", async () => {
	let now = 0;
	let fail = true;
	let fetches = 0;
	const errors: unknown[] = [];
	const added: string[] = [];
	const discord = {
		channels: {
			async fetch() {
				return {
					messages: {
						async fetch() {
							fetches++;
							if (fail) throw new Error("message fetch denied");
							return {
								async react(emoji: string) {
									added.push(emoji);
								},
								reactions: {
									resolve() {
										return { users: { async remove() {} } };
									},
								},
							};
						},
					},
				};
			},
		},
	} as unknown as DiscordClientLike;
	const status = new WorkingStatus(
		discord,
		{
			error: (...args) => {
				errors.push(args);
			},
		},
		() => ({ id: "BOT" }),
		() => now,
	);
	status.arm("C1", "123");
	await flush();
	expect(fetches).toBe(1);
	expect(errors).toHaveLength(1);
	expect(String(errors[0])).toContain("message fetch denied");
	expect(added).toEqual([]);
	fail = false;
	now = 16000;
	await status.update({
		origin: { platform: "discord", kind: "channel", conversationId: "C1" },
		turnId: "t",
		elapsedMs: now,
		toolCalls: 0,
		outputTokens: 0,
		final: false,
		activity: { kind: "thinking", label: "thinking" },
	});
	expect(fetches).toBe(2);
	expect(added.length).toBeGreaterThan(0);
	expect(errors).toHaveLength(1);
	await status.clear("C1");
});
