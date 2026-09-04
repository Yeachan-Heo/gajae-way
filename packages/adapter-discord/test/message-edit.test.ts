import { expect, test } from "bun:test";
import { describeMessageEdit, ReconnectingGateway } from "../src/main";

/**
 * An edited Discord message is forwarded as chat.edit (an update of the
 * original message id), never as a new chat.send. The gateway decides whether
 * the original was ever ingested; the adapter's job is the same admission as
 * a new message, on the NEW content.
 */

const SELF = { id: "self-bot" };
const OPEN = { "chan-1": { engagement: "open" as const } };

const edited = (over: Record<string, unknown> = {}) =>
	({
		id: "m1",
		content: "hello, edited",
		editedTimestamp: 1_756_900_000_000,
		author: { id: "human-1", bot: false, username: "human" },
		channel: { id: "chan-1", type: 0 },
		...over,
	}) as never;

test("an edited human message in an open channel becomes a chat.edit with the new body and the original id", () => {
	const edit = describeMessageEdit(edited(), SELF, OPEN);
	expect(edit).toMatchObject({
		messageId: "m1",
		text: "hello, edited",
		origin: { platform: "discord", kind: "channel", conversationId: "chan-1" },
		engagement: { mentioned: false, group: true, authorId: "human-1", authorName: "human", authorHandle: "human" },
		receivedAt: new Date(1_756_900_000_000).toISOString(),
	});
});

test("our own edited messages and edits to an empty body are not forwarded", () => {
	expect(describeMessageEdit(edited({ author: { id: "self-bot", bot: true } }), SELF, OPEN)).toBeUndefined();
	expect(describeMessageEdit(edited({ content: "" }), SELF, OPEN)).toBeUndefined();
});

test("a messageUpdate that is not a user edit is not forwarded: no edit timestamp, or an unchanged rendered body (embed/link-preview/pin updates)", () => {
	expect(describeMessageEdit(edited({ editedTimestamp: null }), SELF, OPEN)).toBeUndefined();
	expect(describeMessageEdit(edited({ editedTimestamp: undefined }), SELF, OPEN)).toBeUndefined();
	const before = { content: "hello, edited", partial: false };
	expect(describeMessageEdit(edited(), SELF, OPEN, before)).toBeUndefined();
	// A partial `before` (uncached) cannot prove the body was unchanged: forward.
	expect(describeMessageEdit(edited(), SELF, OPEN, { content: "", partial: true })).toBeDefined();
	// A real content change with a cached before: forward.
	expect(describeMessageEdit(edited(), SELF, OPEN, { content: "hello", partial: false })?.text).toBe("hello, edited");
});

function gateway(client: { request: (verb: string, params?: unknown) => Promise<unknown> }) {
	return new ReconnectingGateway(
		"socket",
		{ channels: { fetch: async () => undefined } },
		{ tokenFile: "token", token: "redacted", configPath: "config", channels: {} } as never,
		undefined,
		undefined,
		"/dev/null/recovery-cursor.json",
		() => SELF,
		{ ...client, onChatMessage: () => () => {} } as never,
		async () => {},
	);
}

test("sendEdit sends chat.edit, not chat.send, and is not swallowed by the message-id dedupe of the original", async () => {
	const requests: Array<{ verb: string; params: unknown }> = [];
	const gw = gateway({
		request: async (verb, params) => {
			requests.push({ verb, params });
			return { engaged: true };
		},
	});
	const origin = { platform: "discord", kind: "channel", conversationId: "chan-1" } as const;
	const engagement = { mentioned: true, group: true, authorId: "human-1" };
	await gw.requestInbound("m1", origin, "hello", engagement);
	gw.sendEdit("m1", origin, "hello, edited", engagement, "2026-09-03T00:00:00.000Z");
	await new Promise((resolve) => setTimeout(resolve, 10));
	gw.sendEdit("m1", origin, "hello, edited again", engagement);
	await new Promise((resolve) => setTimeout(resolve, 10));
	expect(requests.map((request) => request.verb)).toEqual(["chat.send", "chat.edit", "chat.edit"]);
	expect(requests[1]?.params).toEqual({
		origin,
		messageId: "m1",
		text: "hello, edited",
		engagement,
		receivedAt: "2026-09-03T00:00:00.000Z",
	});
	expect(requests[2]?.params).toEqual({ origin, messageId: "m1", text: "hello, edited again", engagement });
	expect(gw.pendingEdits).toEqual([]);
});

test("an edit that fails to reach the gateway is kept and replayed on the next connect; a newer edit of the same message supersedes it", async () => {
	const errors: string[] = [];
	const original = console.error;
	console.error = (line: unknown) => {
		errors.push(String(line));
	};
	const requests: Array<{ verb: string; params: { messageId?: string; text?: string } }> = [];
	let linkUp = false;
	const client = {
		request: async (verb: string, params: unknown) => {
			if (!linkUp) throw new Error("gateway down");
			requests.push({ verb, params: params as { messageId?: string; text?: string } });
			return { engaged: true };
		},
	};
	try {
		const gw = gateway(client);
		const origin = { platform: "discord", kind: "channel", conversationId: "chan-1" } as const;
		const engagement = { mentioned: true, group: true, authorId: "human-1" };
		gw.sendEdit("m1", origin, "v1", engagement);
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(errors.some((line) => line.includes("kept for replay"))).toBe(true);
		expect(gw.pendingEdits.map((edit) => [edit.messageId, edit.text])).toEqual([["m1", "v1"]]);
		// A newer edit of the same message while the link is down replaces the
		// queued one; another message queues alongside.
		gw.sendEdit("m1", origin, "v2", engagement);
		gw.sendEdit("m2", origin, "other", engagement);
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(gw.pendingEdits.map((edit) => [edit.messageId, edit.text])).toEqual([
			["m1", "v2"],
			["m2", "other"],
		]);
		expect(requests).toEqual([]);
		// The link comes back: the queue is replayed in order and drained.
		linkUp = true;
		gw.adoptClient({ ...client, onChatMessage: () => () => {} } as never);
		await new Promise((resolve) => setTimeout(resolve, 20));
		gw.sendEdit("m3", origin, "third", engagement);
		await new Promise((resolve) => setTimeout(resolve, 20));
		expect(requests.map((r) => [r.verb, r.params.messageId, r.params.text])).toEqual([
			["chat.edit", "m1", "v2"],
			["chat.edit", "m2", "other"],
			["chat.edit", "m3", "third"],
		]);
		expect(gw.pendingEdits).toEqual([]);
	} finally {
		console.error = original;
	}
});
