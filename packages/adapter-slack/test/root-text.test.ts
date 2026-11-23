import { expect, test } from "bun:test";
import { type SlackHistoryPage, SlackWebApi } from "../src/api";
import {
	ROOT_TEXT_LIMIT,
	ROOT_TEXT_LOOKUPS_PER_WINDOW,
	ROOT_TEXT_MAX_IN_FLIGHT,
	ROOT_TEXT_WINDOW_MS,
	SlackRootTexts,
} from "../src/root-text";

const page = (ts: string, text: unknown): SlackHistoryPage => ({ messages: [{ ts, text }], has_more: false });

test("a root the bot posted is quoted from memory without a lookup", () => {
	const calls: string[] = [];
	const roots = new SlackRootTexts({
		conversationsReplies: async (_channel, ts) => {
			calls.push(ts);
			return page(ts, "fetched");
		},
	});
	roots.remember("C1", "1.0", "IOPS-2232 작업\n\n  스레드");
	expect(roots.excerpt("C1:1.0")).toBe("IOPS-2232 작업 스레드");
	expect(calls).toEqual([]);
});

test("an unknown root answers nothing, is fetched once when warmed, and is quoted afterwards", async () => {
	const calls: string[] = [];
	const { promise, resolve } = Promise.withResolvers<void>();
	const roots = new SlackRootTexts(
		{
			conversationsReplies: async (_channel, ts) => {
				calls.push(ts);
				await promise;
				return page(ts, "<@U1> root");
			},
		},
		(text) => text.replace("<@U1>", "@alice"),
	);
	// Reading never fetches: only an engaged reply warms the root.
	expect(roots.excerpt("C1:1.0")).toBeUndefined();
	expect(calls).toEqual([]);
	roots.warm("C1:1.0");
	roots.warm("C1:1.0");
	expect(roots.excerpt("C1:1.0")).toBeUndefined();
	resolve();
	await roots.idle();
	expect(roots.excerpt("C1:1.0")).toBe("@alice root");
	expect(calls).toEqual(["1.0"]);
});

test("a failed lookup is retried by a later reply; a root without text is not refetched", async () => {
	let fail = true;
	let calls = 0;
	const roots = new SlackRootTexts({
		conversationsReplies: async (_channel, ts) => {
			calls++;
			if (fail) throw new Error("ratelimited");
			return page(ts, undefined);
		},
	});
	roots.warm("C1:1.0");
	await roots.idle();
	fail = false;
	roots.warm("C1:1.0");
	await roots.idle();
	roots.warm("C1:1.0");
	await roots.idle();
	expect(roots.excerpt("C1:1.0")).toBeUndefined();
	expect(calls).toBe(2);
});

test("a long root is capped to a short single line", () => {
	const roots = new SlackRootTexts({ conversationsReplies: async (_channel, ts) => page(ts, "") });
	roots.remember("C1", "1.0", `${"가".repeat(ROOT_TEXT_LIMIT)}\nmore`);
	expect([...(roots.excerpt("C1:1.0") ?? "")]).toHaveLength(ROOT_TEXT_LIMIT);
	expect(roots.excerpt("C1:1.0")).not.toContain("\n");
});

test("a burst of cold roots is shed past the in-flight bound and asked again later", async () => {
	const calls: string[] = [];
	const { promise, resolve } = Promise.withResolvers<void>();
	const roots = new SlackRootTexts({
		conversationsReplies: async (_channel, ts) => {
			calls.push(ts);
			await promise;
			return page(ts, `root ${ts}`);
		},
	});
	const ids = Array.from({ length: ROOT_TEXT_MAX_IN_FLIGHT + 2 }, (_, index) => `C1:${index + 1}.0`);
	for (const id of ids) roots.warm(id);
	expect(calls).toHaveLength(ROOT_TEXT_MAX_IN_FLIGHT);
	resolve();
	await roots.idle();
	const shed = ids.at(-1) ?? "";
	expect(roots.excerpt(shed)).toBeUndefined();
	roots.warm(shed);
	await roots.idle();
	expect(roots.excerpt(shed)).toBe(`root ${shed.slice(3)}`);
});

test("cold-root lookups past the per-window budget are shed until the window moves on", async () => {
	let clock = 0;
	const calls: string[] = [];
	const roots = new SlackRootTexts(
		{
			conversationsReplies: async (_channel, ts) => {
				calls.push(ts);
				return page(ts, `root ${ts}`);
			},
		},
		undefined,
		() => clock,
	);
	const ids = Array.from({ length: ROOT_TEXT_LOOKUPS_PER_WINDOW + 1 }, (_, index) => `C1:${index + 1}.0`);
	for (const id of ids) {
		roots.warm(id);
		await roots.idle();
	}
	expect(calls).toHaveLength(ROOT_TEXT_LOOKUPS_PER_WINDOW);
	const shed = ids.at(-1) ?? "";
	expect(roots.excerpt(shed)).toBeUndefined();

	clock += ROOT_TEXT_WINDOW_MS - 1;
	roots.warm(shed);
	await roots.idle();
	expect(calls).toHaveLength(ROOT_TEXT_LOOKUPS_PER_WINDOW);

	clock += 1;
	roots.warm(shed);
	await roots.idle();
	expect(roots.excerpt(shed)).toBe(`root ${shed.slice(3)}`);
});

test("a rate-limited cold lookup makes one HTTP attempt and pauses lookups for its Retry-After", async () => {
	let clock = 0;
	let attempts = 0;
	let limited = true;
	const api = new SlackWebApi("xoxb-test", {
		fetcher: async () => {
			attempts++;
			if (limited) return new Response("", { status: 429, headers: { "retry-after": "30" } });
			return Response.json({ ok: true, messages: [{ ts: "2.0", text: "root" }], has_more: false });
		},
		sleep: async () => {},
	});
	const roots = new SlackRootTexts(api, undefined, () => clock);
	roots.warm("C1:1.0");
	await roots.idle();
	expect(attempts).toBe(1);

	// Slack asked for 30 s: no cold lookup goes out before then, for any root.
	clock += 29_999;
	roots.warm("C1:2.0");
	await roots.idle();
	expect(attempts).toBe(1);

	limited = false;
	clock += 1;
	roots.warm("C1:2.0");
	await roots.idle();
	expect(attempts).toBe(2);
	expect(roots.excerpt("C1:2.0")).toBe("root");
});
