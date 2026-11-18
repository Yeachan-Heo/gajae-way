import { expect, test } from "bun:test";
import type { SlackHistoryPage } from "../src/api";
import { ROOT_TEXT_LIMIT, ROOT_TEXT_MAX_IN_FLIGHT, SlackRootTexts } from "../src/root-text";

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

test("an unknown root answers nothing now, is fetched once, and is quoted afterwards", async () => {
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
	expect(roots.excerpt("C1:1.0")).toBeUndefined();
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
	expect(roots.excerpt("C1:1.0")).toBeUndefined();
	await roots.idle();
	fail = false;
	expect(roots.excerpt("C1:1.0")).toBeUndefined();
	await roots.idle();
	expect(roots.excerpt("C1:1.0")).toBeUndefined();
	await roots.idle();
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
	for (const id of ids) expect(roots.excerpt(id)).toBeUndefined();
	expect(calls).toHaveLength(ROOT_TEXT_MAX_IN_FLIGHT);
	resolve();
	await roots.idle();
	const shed = ids.at(-1) ?? "";
	expect(roots.excerpt(shed)).toBeUndefined();
	await roots.idle();
	expect(roots.excerpt(shed)).toBe(`root ${shed.slice(3)}`);
});
