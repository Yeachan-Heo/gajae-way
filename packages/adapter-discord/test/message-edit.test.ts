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
		engagement: { mentioned: true, group: true, authorId: "human-1" },
		receivedAt: new Date(1_756_900_000_000).toISOString(),
	});
});

test("our own edited messages and edits to an empty body are not forwarded", () => {
	expect(describeMessageEdit(edited({ author: { id: "self-bot", bot: true } }), SELF, OPEN)).toBeUndefined();
	expect(describeMessageEdit(edited({ content: "" }), SELF, OPEN)).toBeUndefined();
});

test("an edit without an edit timestamp carries no receivedAt rather than inventing one", () => {
	expect(describeMessageEdit(edited({ editedTimestamp: null }), SELF, OPEN)?.receivedAt).toBeUndefined();
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
});

test("a failed chat.edit is logged and never throws out of the event handler", async () => {
	const errors: string[] = [];
	const original = console.error;
	console.error = (line: unknown) => {
		errors.push(String(line));
	};
	try {
		const gw = gateway({
			request: async () => {
				throw new Error("gateway down");
			},
		});
		gw.sendEdit("m1", { platform: "discord", kind: "channel", conversationId: "chan-1" }, "edited", {
			mentioned: true,
			group: true,
			authorId: "human-1",
		});
		await new Promise((resolve) => setTimeout(resolve, 10));
		expect(errors.some((line) => line.includes("Discord chat.edit failed: gateway down"))).toBe(true);
	} finally {
		console.error = original;
	}
});
