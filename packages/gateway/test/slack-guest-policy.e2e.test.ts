import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { SlackWebApi } from "../../adapter-slack/src/api";
import { slackMessageId, slackMessageOrigin } from "../../adapter-slack/src/origin";
import { liveGateway, stopLiveGatewaysAfterEach } from "../../adapter-slack/test/live-gateways";
import type { GatewayConfig } from "../src/config";
import { checkSlackGuestPolicy } from "../src/engagement/policy";
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
	const { config, turns } = await gateway(() => "should not be called", {
		dmPolicy: "open",
		slack: {
			guestPolicy: {
				dm: "drop",
				channel: "allow",
			},
		},
	});
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
	const { config, turns } = await gateway(() => "should not be called", {
		channels: { "slack:C1": { engagement: "open" } },
		slack: {
			guestPolicy: {
				dm: "drop",
				channel: "drop",
			},
		},
	});
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

test("a regular member in channel is unaffected by guest policy", async () => {
	const { config, turns } = await gateway(() => "member response", {
		channels: { "slack:C1": { engagement: "open" } },
		slack: {
			guestPolicy: {
				dm: "drop",
				channel: "allow",
			},
		},
	});
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

test("a guest on allowUsers list bypasses guest policy restrictions", async () => {
	const { config, turns } = await gateway(() => "allowed guest response", {
		channels: { "slack:C1": { engagement: "open" } },
		slack: {
			guestPolicy: {
				dm: "drop",
				channel: "allow",
				allowUsers: ["U_ALLOWLISTED"],
			},
		},
	});
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

test("a guest on denyUsers list is always dropped", async () => {
	const { config, turns } = await gateway(() => "should not be called", {
		channels: { "slack:C1": { engagement: "open" } },
		slack: {
			guestPolicy: {
				dm: "allow",
				channel: "allow",
				denyUsers: ["U_DENIED"],
			},
		},
	});
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

test("an unverified author is dropped in a DM by default and admitted when failClosed is false", async () => {
	for (const failClosed of [undefined, false] as const) {
		const { config, turns } = await gateway(() => "reply", {
			dmPolicy: "open",
			...(failClosed === undefined ? {} : { slack: { guestPolicy: { failClosed } } }),
		});
		const slack = fakeSlackApi();
		const adapter = liveGateway(config.socketPath, slack.api);
		await adapter.connect();
		const result = await adapter.requestInbound(
			slackMessageId("D1", "1726543210.000100"),
			DM_ORIGIN,
			"hello",
			{ mentioned: true, group: false, authorId: "U_GUEST", trust: "unverified" },
			"2026-09-17T00:00:10.000Z",
		);
		expect(result?.engaged).toBe(failClosed === false);
		await server?.stop();
		server = undefined;
		await rm(directory, { recursive: true, force: true });
		directory = "";
		if (failClosed === undefined) expect(turns).toHaveLength(0);
	}
});

test("the owner is never dropped by the guest policy, even when the lookup failed", () => {
	const config = {
		ownerTarget: { origin: { platform: "slack", kind: "dm", conversationId: "D1", peerId: "U_OWNER" } },
		slack: { guestPolicy: { dm: "drop", channel: "drop" } },
	} as unknown as GatewayConfig;
	const owner = { mentioned: true, group: false, authorId: "U_OWNER", trust: "unverified" } as const;
	expect(checkSlackGuestPolicy(owner, { platform: "slack", kind: "dm" }, config)).toBe("allow");
	expect(checkSlackGuestPolicy({ ...owner, trust: "guest" }, { platform: "slack", kind: "channel" }, config)).toBe(
		"allow",
	);
	expect(checkSlackGuestPolicy({ ...owner, authorId: "U_OTHER" }, { platform: "slack", kind: "dm" }, config)).toBe(
		"drop",
	);
});

test("a trust claim on a non-Slack origin is ignored", () => {
	const config = { slack: { guestPolicy: { dm: "drop", channel: "drop" } } } as unknown as GatewayConfig;
	const guest = { mentioned: true, group: false, authorId: "123", trust: "guest" } as const;
	expect(checkSlackGuestPolicy(guest, { platform: "discord", kind: "dm" }, config)).toBe("allow");
	expect(checkSlackGuestPolicy(guest, { platform: "slack", kind: "dm" }, config)).toBe("drop");
});

test("missing trust field (omitted by older adapter) is treated as unverified and dropped by default", () => {
	const config = {
		slack: { guestPolicy: { dm: "drop", channel: "allow" } },
	} as unknown as GatewayConfig;
	// No trust field: should be treated as unverified and dropped in DM
	const noTrust = { mentioned: true, group: false, authorId: "U_NO_TRUST" } as const;
	expect(checkSlackGuestPolicy(noTrust, { platform: "slack", kind: "dm" }, config)).toBe("drop");
	// Same author with explicit trust=member should be allowed
	const withMember = { mentioned: true, group: false, authorId: "U_NO_TRUST", trust: "member" as const } as const;
	expect(checkSlackGuestPolicy(withMember, { platform: "slack", kind: "dm" }, config)).toBe("allow");
});

test("missing trust field is allowed when failClosed is false", () => {
	const config = {
		slack: { guestPolicy: { dm: "drop", channel: "allow", failClosed: false } },
	} as unknown as GatewayConfig;
	const noTrust = { mentioned: true, group: false, authorId: "U_NO_TRUST" } as const;
	// Missing trust with failClosed=false should allow in DM
	expect(checkSlackGuestPolicy(noTrust, { platform: "slack", kind: "dm" }, config)).toBe("allow");
});

test("denyUsers takes precedence over allowUsers (deny list must override allow list)", () => {
	const config = {
		slack: { guestPolicy: { dm: "allow", channel: "allow", allowUsers: ["U_BOTH"], denyUsers: ["U_BOTH"] } },
	} as unknown as GatewayConfig;
	// User listed in both allow and deny should be dropped (deny takes precedence)
	const overlap = { mentioned: true, group: true, authorId: "U_BOTH", trust: "guest" } as const;
	expect(checkSlackGuestPolicy(overlap, { platform: "slack", kind: "channel" }, config)).toBe("drop");
});

test("a member's DM is answered under the default fail-closed policy", async () => {
	const { config, turns } = await gateway(() => "member response", { dmPolicy: "open" });
	const slack = fakeSlackApi();
	const adapter = liveGateway(config.socketPath, slack.api);
	await adapter.connect();
	const result = await adapter.requestInbound(
		slackMessageId("D1", "1726543210.000500"),
		DM_ORIGIN,
		"hello member",
		{ mentioned: true, group: false, authorId: "U_MEMBER", trust: "member" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result?.engaged).toBe(true);
	await Bun.sleep(100);
	expect(turns).toHaveLength(1);
});

test("trust propagates on the recovery path and guest policy drops correctly", async () => {
	const { config, turns } = await gateway(() => "should not be called", {
		channels: { "slack:C1": { engagement: "open" } },
		slack: {
			guestPolicy: {
				dm: "drop",
				channel: "drop",
			},
		},
	});
	const slack = fakeSlackApi();
	const adapter = liveGateway(config.socketPath, slack.api);
	await adapter.connect();
	const guestOrigin = slackMessageOrigin({ channel: "C1", user: "U_GUEST_REC", ts: "1726543210.000600" });
	// Recovery path: guest with trust:guest should be dropped by policy
	const result = await adapter.requestRecovered(
		slackMessageId("C1", "1726543210.000600"),
		guestOrigin,
		"recovered message",
		{ mentioned: true, group: true, authorId: "U_GUEST_REC", trust: "guest" },
		"2026-09-17T00:00:10.000Z",
	);
	expect(result.verdict).toBe("acked");
	expect(result.result?.engaged).toBe(false);
	await Bun.sleep(100);
	expect(turns).toHaveLength(0);
	// Note: edit and slash-command paths also use withAuthorTrust and are covered
	// by the same code path that propagates trust on live/recovered messages.
});
