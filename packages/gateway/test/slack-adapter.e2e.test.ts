import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SlackWebApi } from "../../adapter-slack/src/api";
import { ReconnectingGateway, settleSlackDelivery } from "../../adapter-slack/src/main";
import { slackMessageId, slackMessageOrigin } from "../../adapter-slack/src/origin";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { GatewayDatabase } from "../src/store/db";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

/**
 * End to end through the real gateway: the Slack adapter's own reconnecting
 * client speaks the SDK socket, the persona answers through a scripted session
 * port, and the reply comes back out through `settleSlackDelivery` against a
 * recording Slack Web API. Nothing here is mocked at the gateway boundary, so a
 * platform that the gateway silently refuses (the pre-slack `discord|telegram`
 * guards) or a delivery the adapter cannot settle shows up as a missing row.
 */

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

async function settle(): Promise<void> {
	for (let attempt = 0; attempt < 60; attempt++) await Bun.sleep(5);
}

/** Records every Slack Web API call the delivery path makes; never talks to Slack. */
function fakeSlackApi(): {
	readonly api: Pick<SlackWebApi, "postMessage" | "addReaction">;
	readonly posts: Array<{ channel: string; text: string; threadTs?: string }>;
	readonly reactions: Array<{ channel: string; ts: string; name: string }>;
} {
	const posts: Array<{ channel: string; text: string; threadTs?: string }> = [];
	const reactions: Array<{ channel: string; ts: string; name: string }> = [];
	return {
		posts,
		reactions,
		api: {
			async postMessage(channel, text, threadTs) {
				posts.push({ channel, text, ...(threadTs === undefined ? {} : { threadTs }) });
				return { ts: `${Date.now() / 1000}`, channel };
			},
			async addReaction(channel, ts, name) {
				reactions.push({ channel, ts, name });
			},
		},
	};
}

async function gateway(respond: (text: string) => string): Promise<{
	readonly config: GatewayConfig;
	readonly database: GatewayDatabase;
	readonly turns: string[];
}> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-slack-e2e-"));
	const config: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "slack:C1": { engagement: "open" } },
		dmPolicy: "open",
	};
	const database = await GatewayDatabase.open(config.dbPath);
	const turns: string[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async (_sessionId, text) => {
			turns.push(text);
			return respond(text);
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config, database, sessionPort, onStop: () => database.close() });
	return { config, database, turns };
}

const CHANNEL_ORIGIN = slackMessageOrigin({ channel: "C1", user: "U1", ts: "1726543210.000100" });

test("a Slack channel message becomes a turn and its reply is posted as mrkdwn and confirmed", async () => {
	const { config, database, turns } = await gateway(() => "**done** — see [the doc](https://x.test/d) & more");
	const slack = fakeSlackApi();
	const adapter = new ReconnectingGateway(config.socketPath, slack.api);
	await adapter.connect();
	const result = await adapter.requestInbound(
		slackMessageId("C1", "1726543210.000100"),
		CHANNEL_ORIGIN,
		"형님 이거 봐주세요",
		{ mentioned: true, group: true, authorId: "U1", authorName: "형님" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result?.engaged).toBe(true);
	await settle();
	expect(turns).toHaveLength(1);
	// The persona's Markdown reached Slack as mrkdwn, at the top level of the channel.
	expect(slack.posts).toEqual([{ channel: "C1", text: "*done* — see <https://x.test/d|the doc> &amp; more" }]);
	const rows = database.deliveryRows();
	expect(rows).toHaveLength(1);
	expect(rows[0]?.origin_key).toBe("slack/channel/C1");
	expect(rows[0]?.state).toBe("confirmed");
});

test("a [REACT:👍] reply reacts by Slack emoji name and settles the same ledger row", async () => {
	const { config, database } = await gateway(() => "[REACT:👍]");
	const slack = fakeSlackApi();
	const adapter = new ReconnectingGateway(config.socketPath, slack.api);
	await adapter.connect();
	await adapter.requestInbound(slackMessageId("C1", "1726543210.000200"), CHANNEL_ORIGIN, "ㅇㅋ?", {
		mentioned: true,
		group: true,
		authorId: "U1",
	});
	await settle();
	expect(slack.posts).toEqual([]);
	expect(slack.reactions).toEqual([{ channel: "C1", ts: "1726543210.000200", name: "+1" }]);
	const rows = database.deliveryRows();
	expect(rows).toHaveLength(1);
	expect(rows[0]?.state).toBe("confirmed");
	expect(JSON.parse(rows[0]?.payload_json ?? "{}").reaction.emojiName).toBe("thumbsup");
});

test("a human's thread reply is its own session and the answer stays in that thread", async () => {
	const { config, database, turns } = await gateway((text) => (text.includes("thread") ? "in thread" : "top level"));
	const slack = fakeSlackApi();
	const adapter = new ReconnectingGateway(config.socketPath, slack.api);
	await adapter.connect();
	await adapter.requestInbound(slackMessageId("C1", "1726543210.000300"), CHANNEL_ORIGIN, "top question", {
		mentioned: true,
		group: true,
		authorId: "U1",
	});
	const threadOrigin = slackMessageOrigin({
		channel: "C1",
		user: "U1",
		ts: "1726543210.000400",
		thread_ts: "1726543210.000300",
	});
	expect(threadOrigin).toEqual({
		platform: "slack",
		kind: "thread",
		conversationId: "C1:1726543210.000300",
		parentId: "C1",
	});
	await adapter.requestInbound(slackMessageId("C1", "1726543210.000400"), threadOrigin, "thread question", {
		mentioned: true,
		group: true,
		authorId: "U1",
	});
	await settle();
	expect(turns).toHaveLength(2);
	// Two origins, two sessions: the thread does not share the channel's head.
	const sessions = database.sessionRows().map((row) => JSON.parse(row.origin_ref_json ?? "{}").conversationId);
	expect(sessions.sort()).toEqual(["C1", "C1:1726543210.000300"]);
	// Different origins run concurrently, so the two replies may land in either order.
	expect(slack.posts.sort((a, b) => a.text.localeCompare(b.text))).toEqual([
		{ channel: "C1", text: "in thread", threadTs: "1726543210.000300" },
		{ channel: "C1", text: "top level" },
	]);
	expect(database.deliveryRows().map((row) => row.state)).toEqual(["confirmed", "confirmed"]);
});

test("a delivery the adapter cannot settle is a failed ledger row, not a silent drop", async () => {
	const { config, database } = await gateway(() => "[REACT:🦞]");
	const slack = fakeSlackApi();
	// The gateway hands the adapter a reaction whose target is not a channel:ts id;
	// the adapter must refuse it definitively rather than guess a channel.
	const failing: Pick<SlackWebApi, "postMessage" | "addReaction"> = {
		postMessage: slack.api.postMessage,
		addReaction: async () => {
			throw new TypeError("fetch failed");
		},
	};
	const adapter = new ReconnectingGateway(config.socketPath, failing);
	await adapter.connect();
	await adapter.requestInbound(slackMessageId("C1", "1726543210.000500"), CHANNEL_ORIGIN, "x", {
		mentioned: true,
		group: true,
		authorId: "U1",
	});
	await settle();
	const rows = database.deliveryRows();
	expect(rows).toHaveLength(1);
	// A transport error is ambiguous: the row is retained for redelivery, never confirmed.
	expect(rows[0]?.state).not.toBe("confirmed");
	// And the direct settlement path reports the same thing for a malformed target.
	const requests: Array<{ verb: string; params: unknown }> = [];
	await settleSlackDelivery(
		{
			async request<T>(verb: string, params?: unknown): Promise<T> {
				requests.push({ verb, params });
				return undefined as T;
			},
		},
		slack.api,
		{
			turnId: "t",
			origin: CHANNEL_ORIGIN,
			role: "assistant",
			text: "🦞",
			final: true,
			deliveryId: "d-malformed",
			reaction: { targetMessageId: "not-a-slack-id", emoji: "🦞", emojiName: "lobster" },
		},
	);
	expect(slack.reactions).toEqual([]);
	expect(requests).toEqual([
		{
			verb: "delivery.fail",
			params: { deliveryId: "d-malformed", reason: expect.stringContaining("malformed"), ambiguous: false },
		},
	]);
});

test("a delivery pending in the ledger before the adapter connects is replayed and settled, not dropped", async () => {
	// ARCH-08: the gateway writes `negotiated` and the pending replay in one burst,
	// which the SDK dispatches before `connectSocket` resolves - before the adapter
	// could subscribe. The SDK now holds pre-subscription events for the first
	// subscriber, so the row is settled instead of stranded until the next reconnect.
	const { config, database } = await gateway(() => "answer");
	// Seed an undelivered Slack row with nobody connected to settle it.
	const seedApi = fakeSlackApi();
	const seedAdapter = new ReconnectingGateway(config.socketPath, {
		postMessage: async () => {
			throw new TypeError("link died mid-post");
		},
		addReaction: seedApi.api.addReaction,
	});
	await seedAdapter.connect();
	await seedAdapter.requestInbound(slackMessageId("C1", "1726543210.000600"), CHANNEL_ORIGIN, "hello?", {
		mentioned: true,
		group: true,
		authorId: "U1",
	});
	await settle();
	expect(database.deliveryRows().map((row) => row.state)).not.toEqual(["confirmed"]);
	// A fresh adapter connects; the replay arrives with negotiation.
	const slack = fakeSlackApi();
	const adapter = new ReconnectingGateway(config.socketPath, slack.api);
	await adapter.connect();
	await settle();
	expect(slack.posts).toEqual([{ channel: "C1", text: "[recovered - may be a duplicate] answer" }]);
	expect(database.deliveryRows().map((row) => row.state)).toEqual(["confirmed"]);
});

test("a plain answer to a threaded DM stays in that DM thread without a [REPLY] token", async () => {
	// ARCH-05: a threaded DM keeps its DM session identity, so the thread root only
	// survives in engagement.replyTo. The gateway uses it as the default reply target.
	const { config, database } = await gateway(() => "in your thread");
	const slack = fakeSlackApi();
	const adapter = new ReconnectingGateway(config.socketPath, slack.api);
	await adapter.connect();
	const dm = slackMessageOrigin({ channel: "D1", channel_type: "im", user: "U1", ts: "1726543210.000700" });
	expect(dm.kind).toBe("dm");
	await adapter.requestInbound(slackMessageId("D1", "1726543210.000700"), dm, "question in a thread", {
		mentioned: true,
		group: false,
		authorId: "U1",
		replyTo: { messageId: slackMessageId("D1", "1726543200.000001"), authorId: "UBOT", fromSelf: true },
	});
	await settle();
	expect(slack.posts).toEqual([{ channel: "D1", text: "in your thread", threadTs: "1726543200.000001" }]);
	expect(database.deliveryRows().map((row) => row.state)).toEqual(["confirmed"]);
});
