import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SlackWebApi } from "../../adapter-slack/src/api";
import { slackMessageId, slackMessageOrigin } from "../../adapter-slack/src/origin";
import { liveGateway, stopLiveGatewaysAfterEach } from "../../adapter-slack/test/live-gateways";
import type { GatewayConfig } from "../src/config";
import { type GatewayServer, startUnixServer } from "../src/server/server";
import { Store } from "../src/store";
import { attachTestBrokerOwnership, sessionPortFromResponder } from "./session-port.fake";

stopLiveGatewaysAfterEach();

let directory = "";
let server: GatewayServer | undefined;
afterEach(async () => {
	await server?.stop();
	server = undefined;
	if (directory) await rm(directory, { recursive: true, force: true });
	directory = "";
});

function fakeSlackApi(): {
	readonly api: Pick<SlackWebApi, "postMessage" | "addReaction" | "removeReaction">;
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
			async removeReaction(_channel, _ts, _name) {
				// Mock remove
			},
		},
	};
}

async function gateway(
	respond: (text: string) => string | Promise<string>,
	config?: Partial<GatewayConfig>,
): Promise<{
	readonly config: GatewayConfig;
	readonly database: Store;
	readonly turns: string[];
}> {
	directory = await mkdtemp(join(tmpdir(), "gajaeway-guest-policy-"));
	const fullConfig: GatewayConfig = {
		schemaVersion: 1,
		home: directory,
		gjc: { agentDir: join(directory, "agent") },
		configPath: join(directory, "config.json"),
		socketPath: join(directory, "gateway.sock"),
		dbPath: join(directory, "gateway.db"),
		logVerbosity: "info",
		channels: { "slack:C1": { engagement: "open" } },
		dmPolicy: "open",
		...config,
	};
	const database = await Store.open(fullConfig.dbPath);
	const turns: string[] = [];
	const sessionPort = sessionPortFromResponder({
		bind: async (originKey, epoch) => `session-${originKey}-${epoch}`,
		respond: async (_sessionId, text) => {
			turns.push(text);
			return respond(text);
		},
	});
	attachTestBrokerOwnership(database, sessionPort, join(directory, "agent"));
	server = await startUnixServer({ config: fullConfig, database, sessionPort, onStop: () => database.close() });
	return { config: fullConfig, database, turns };
}

const CHANNEL_ORIGIN = slackMessageOrigin({ channel: "C1", user: "U_GUEST", ts: "1726543210.000100" });
const DM_ORIGIN = slackMessageOrigin({ channel: "D1", channel_type: "im", user: "U_GUEST", ts: "1726543210.000100" });

test("a guest in DM with dm=drop receives no response and no model call", async () => {
	const { config, database, turns } = await gateway(
		() => "should not be called",
		{
			dmPolicy: "open",
			slack: {
				guest_policy: {
					dm: "drop",
					channel: "restricted",
				},
			},
		},
	);
	const slack = fakeSlackApi();
	const adapter = liveGateway(config.socketPath, slack.api);
	await adapter.connect();
	const result = await adapter.requestInbound(
		slackMessageId("D1", "1726543210.000100"),
		DM_ORIGIN,
		"hello guest",
		{ mentioned: true, group: false, authorId: "U_GUEST", trust: "guest" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result?.engaged).toBe(false);
	await Bun.sleep(100);
	expect(turns).toHaveLength(0);
	expect(slack.posts).toHaveLength(0);
});

test("a guest in channel with channel=drop receives no response and no model call", async () => {
	const { config, database, turns } = await gateway(
		() => "should not be called",
		{
			channels: { "slack:C1": { engagement: "open" } },
			slack: {
				guest_policy: {
					dm: "drop",
					channel: "drop",
				},
			},
		},
	);
	const slack = fakeSlackApi();
	const adapter = liveGateway(config.socketPath, slack.api);
	await adapter.connect();
	const result = await adapter.requestInbound(
		slackMessageId("C1", "1726543210.000100"),
		CHANNEL_ORIGIN,
		"hello channel",
		{ mentioned: true, group: true, authorId: "U_GUEST", trust: "guest" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result?.engaged).toBe(false);
	await Bun.sleep(100);
	expect(turns).toHaveLength(0);
	expect(slack.posts).toHaveLength(0);
});

test("a guest in channel with channel=restricted runs with no bootstrap and restricted prompt", async () => {
	let turnContent = "";
	const { config, database, turns } = await gateway(
		(text) => {
			turnContent = text;
			// Check that bootstrap is NOT in the prompt (would contain "[Current conversation metadata]" or similar)
			expect(text).not.toContain("[Current conversation metadata]");
			expect(text).not.toContain("[Unread messages");
			return "restricted response";
		},
		{
			channels: { "slack:C1": { engagement: "open" } },
			slack: {
				guest_policy: {
					dm: "drop",
					channel: "restricted",
				},
			},
		},
	);
	const slack = fakeSlackApi();
	const adapter = liveGateway(config.socketPath, slack.api);
	await adapter.connect();
	const result = await adapter.requestInbound(
		slackMessageId("C1", "1726543210.000100"),
		CHANNEL_ORIGIN,
		"hello guest",
		{ mentioned: true, group: true, authorId: "U_GUEST", trust: "guest" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result?.engaged).toBe(true);
	await Bun.sleep(100);
	expect(turns).toHaveLength(1);
	// Verify the turn text does not contain bootstrap (which is for restricted guests)
	expect(turnContent).not.toContain("[Current conversation metadata]");
	expect(slack.posts).toHaveLength(1);
	expect(slack.posts[0]?.text).toContain("restricted response");
});

test("a regular member in channel is unaffected by guest policy", async () => {
	const { config, database, turns } = await gateway(
		() => "member response",
		{
			channels: { "slack:C1": { engagement: "open" } },
			slack: {
				guest_policy: {
					dm: "drop",
					channel: "restricted",
				},
			},
		},
	);
	const slack = fakeSlackApi();
	const adapter = liveGateway(config.socketPath, slack.api);
	await adapter.connect();
	const memberOrigin = slackMessageOrigin({ channel: "C1", user: "U_MEMBER", ts: "1726543210.000200" });
	const result = await adapter.requestInbound(
		slackMessageId("C1", "1726543210.000200"),
		memberOrigin,
		"hello member",
		{ mentioned: true, group: true, authorId: "U_MEMBER" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result?.engaged).toBe(true);
	await Bun.sleep(100);
	expect(turns).toHaveLength(1);
	expect(slack.posts).toHaveLength(1);
	expect(slack.posts[0]?.text).toContain("member response");
});

test("a guest on allow_users list bypasses guest policy restrictions", async () => {
	const { config, database, turns } = await gateway(
		() => "allowed guest response",
		{
			channels: { "slack:C1": { engagement: "open" } },
			slack: {
				guest_policy: {
					dm: "drop",
					channel: "restricted",
					allow_users: ["U_ALLOWLISTED"],
				},
			},
		},
	);
	const slack = fakeSlackApi();
	const adapter = liveGateway(config.socketPath, slack.api);
	await adapter.connect();
	const allowlistOrigin = slackMessageOrigin({ channel: "C1", user: "U_ALLOWLISTED", ts: "1726543210.000300" });
	const result = await adapter.requestInbound(
		slackMessageId("C1", "1726543210.000300"),
		allowlistOrigin,
		"hello allowlisted",
		{ mentioned: true, group: true, authorId: "U_ALLOWLISTED", trust: "guest" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result?.engaged).toBe(true);
	await Bun.sleep(100);
	expect(turns).toHaveLength(1);
	expect(slack.posts).toHaveLength(1);
	expect(slack.posts[0]?.text).toContain("allowed guest response");
});

test("a guest on deny_users list is always dropped", async () => {
	const { config, database, turns } = await gateway(
		() => "should not be called",
		{
			channels: { "slack:C1": { engagement: "open" } },
			slack: {
				guest_policy: {
					dm: "allow",
					channel: "allow",
					deny_users: ["U_DENIED"],
				},
			},
		},
	);
	const slack = fakeSlackApi();
	const adapter = liveGateway(config.socketPath, slack.api);
	await adapter.connect();
	const deniedOrigin = slackMessageOrigin({ channel: "C1", user: "U_DENIED", ts: "1726543210.000400" });
	const result = await adapter.requestInbound(
		slackMessageId("C1", "1726543210.000400"),
		deniedOrigin,
		"hello denied",
		{ mentioned: true, group: true, authorId: "U_DENIED", trust: "guest" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result?.engaged).toBe(false);
	await Bun.sleep(100);
	expect(turns).toHaveLength(0);
	expect(slack.posts).toHaveLength(0);
});
