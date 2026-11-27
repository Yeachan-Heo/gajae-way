import { describe, expect, test } from "bun:test";
import type { GatewayConfig } from "../src/config";
import { decideEngagement } from "../src/engagement/policy";

describe("channel ownerTarget owner authorization (#901)", () => {
	test("owner identity from DM ownerTarget peerId still works", () => {
		const ownerDM = { platform: "discord", kind: "dm", conversationId: "dm", peerId: "owner-1" } as const;
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: "/tmp",
			gjc: { agentDir: "/tmp/agent" },
			configPath: "/tmp/config.json",
			socketPath: "/tmp/socket",
			dbPath: "/tmp/db.sqlite",
			ownerTarget: { origin: ownerDM },
		};

		// Owner's command in a channel should be engaged
		const ownerEngagement = { mentioned: true, group: true, authorId: "owner-1" };
		const channel = { platform: "discord" as const, kind: "channel" as const, conversationId: "ch" };
		const ownerDecision = decideEngagement(channel, ownerEngagement, config);
		expect(ownerDecision.engaged).toBe(true);

		// Non-owner's command in closed channel should not be engaged
		const nonOwnerEngagement = { mentioned: true, group: true, authorId: "user-1" };
		const nonOwnerDecision = decideEngagement(channel as typeof channel, nonOwnerEngagement, config);
		expect(nonOwnerDecision.engaged).toBe(false);
	});

	test("channel ownerTarget does not use channel peer identity (there is none)", () => {
		// Channels don't have peerId, so ownerTarget platform/channel origin cannot provide owner identity
		const ownerChannel = {
			platform: "discord",
			kind: "channel",
			conversationId: "owner-channel",
		} as const;
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: "/tmp",
			gjc: { agentDir: "/tmp/agent" },
			configPath: "/tmp/config.json",
			socketPath: "/tmp/socket",
			dbPath: "/tmp/db.sqlite",
			ownerTarget: { origin: ownerChannel },
		};

		// Without adapter-reported owner IDs, no one should be authorized as owner in channels
		const ownerEngagement = { mentioned: true, group: true, authorId: "user-1" };
		const channel = { platform: "discord" as const, kind: "channel" as const, conversationId: "ch" };

		// In a closed channel (no policy specified), only owner or allowlist members can engage
		// Since no one is owner (no adapter IDs, no DM ownerTarget), no one is engaged
		const ownerDecision = decideEngagement(channel, ownerEngagement, config);
		expect(ownerDecision.engaged).toBe(false);
	});

	test("DM engagement with channel ownerTarget falls back to policies", () => {
		const ownerChannel = {
			platform: "discord",
			kind: "channel",
			conversationId: "owner-channel",
		} as const;
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: "/tmp",
			gjc: { agentDir: "/tmp/agent" },
			configPath: "/tmp/config.json",
			socketPath: "/tmp/socket",
			dbPath: "/tmp/db.sqlite",
			dmPolicy: "open",
			ownerTarget: { origin: ownerChannel },
		};

		// With dmPolicy: "open", any DM should be engaged
		const dm = { platform: "discord" as const, kind: "dm" as const, conversationId: "dm", peerId: "user-1" } as const;
		const engagement = { mentioned: false, group: false, authorId: "user-1" };
		const decision = decideEngagement(dm, engagement, config);
		expect(decision.engaged).toBe(true);
	});

	test("DM allowlist with channel ownerTarget does not grant owner authority", () => {
		const ownerChannel = {
			platform: "discord",
			kind: "channel",
			conversationId: "owner-channel",
		} as const;
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: "/tmp",
			gjc: { agentDir: "/tmp/agent" },
			configPath: "/tmp/config.json",
			socketPath: "/tmp/socket",
			dbPath: "/tmp/db.sqlite",
			dmPolicy: "allowlist",
			mentionAllowlist: ["user-1"],
			ownerTarget: { origin: ownerChannel },
		};

		// Allowlisted user can engage DMs, but is not the owner
		const dm = { platform: "discord" as const, kind: "dm" as const, conversationId: "dm", peerId: "user-1" } as const;
		const engagement = { mentioned: false, group: false, authorId: "user-1" };
		const decision = decideEngagement(
			{
				platform: dm.platform,
				kind: dm.kind,
				conversationId: dm.conversationId,
			} as Pick<typeof dm, "platform" | "kind" | "conversationId">,
			engagement,
			config,
		);
		expect(decision.engaged).toBe(true);

		// Allowlisted user can also engage in channels (they're in the allowlist)
		// But they're NOT the owner (allowlist != owner)
		const channel = { platform: "discord" as const, kind: "channel" as const, conversationId: "ch" };
		const channelEngagement = { mentioned: true, group: true, authorId: "user-1" };
		const channelDecision = decideEngagement(channel, channelEngagement, config);
		expect(channelDecision.engaged).toBe(true); // Closed channel, in allowlist

		// A non-allowlisted user is not engaged
		const nonAllowlistedEngagement = { mentioned: true, group: true, authorId: "user-2" };
		const nonAllowlistedDecision = decideEngagement(channel, nonAllowlistedEngagement, config);
		expect(nonAllowlistedDecision.engaged).toBe(false); // Closed channel, not in allowlist, not owner
	});
});

describe("DM engagement with channel ownerTarget and adapter owners (#901)", () => {
	test("owner's DM is engaged with channel ownerTarget and adapter-reported owners", () => {
		const ownerChannel = {
			platform: "discord" as const,
			kind: "channel" as const,
			conversationId: "owner-channel",
		} as const;
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: "/tmp",
			gjc: { agentDir: "/tmp/agent" },
			configPath: "/tmp/config.json",
			socketPath: "/tmp/socket",
			dbPath: "/tmp/db.sqlite",
			dmPolicy: "allowlist",
			ownerTarget: { origin: ownerChannel },
		};

		// Mock connection with adapter-reported owner IDs
		const connection = {
			platformOwnerIds: ["owner-1"],
			platformOwnerPlatform: "discord",
		};

		// Owner's DM should be engaged (owner from adapter)
		const dm = { platform: "discord" as const, kind: "dm" as const, conversationId: "dm" };
		const ownerDM = { mentioned: false, group: false, authorId: "owner-1" };
		const ownerDecision = decideEngagement(dm, ownerDM, config, false, false, connection);
		expect(ownerDecision.engaged).toBe(true);

		// Non-owner's DM should not be engaged (no allowlist policy)
		const nonOwnerDM = { mentioned: false, group: false, authorId: "user-1" };
		const nonOwnerDecision = decideEngagement(dm, nonOwnerDM, config, false, false, connection);
		expect(nonOwnerDecision.engaged).toBe(false);
	});

	test("owner's DM is engaged even with channel ownerTarget if DM policy is open", () => {
		const ownerChannel = {
			platform: "discord" as const,
			kind: "channel" as const,
			conversationId: "owner-channel",
		} as const;
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: "/tmp",
			gjc: { agentDir: "/tmp/agent" },
			configPath: "/tmp/config.json",
			socketPath: "/tmp/socket",
			dbPath: "/tmp/db.sqlite",
			dmPolicy: "open",
			ownerTarget: { origin: ownerChannel },
		};

		const connection = {
			platformOwnerIds: ["owner-1"],
			platformOwnerPlatform: "discord",
		};

		// Any DM is engaged with open policy
		const dm = { platform: "discord" as const, kind: "dm" as const, conversationId: "dm" };
		const anyDM = { mentioned: false, group: false, authorId: "anyone" };
		const decision = decideEngagement(dm, anyDM, config, false, false, connection);
		expect(decision.engaged).toBe(true);
	});
});

describe("channel ownerTarget with adapter owner IDs (#901)", () => {
	test("adapter-reported owner IDs authorize when ownerTarget is a channel", () => {
		const ownerChannel = {
			platform: "discord",
			kind: "channel",
			conversationId: "owner-channel",
		} as const;
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: "/tmp",
			gjc: { agentDir: "/tmp/agent" },
			configPath: "/tmp/config.json",
			socketPath: "/tmp/socket",
			dbPath: "/tmp/db.sqlite",
			ownerTarget: { origin: ownerChannel },
		};

		// Mock connection with adapter-reported owner IDs
		const connection = {
			platformOwnerIds: ["owner-1"],
			platformOwnerPlatform: "discord",
		};

		// Owner (from adapter) can engage in a channel
		const ownerEngagement = { mentioned: true, group: true, authorId: "owner-1" };
		const channel = { platform: "discord" as const, kind: "channel" as const, conversationId: "ch" };
		const ownerDecision = decideEngagement(channel, ownerEngagement, config, false, false, connection);
		expect(ownerDecision.engaged).toBe(true);

		// Non-owner cannot
		const nonOwnerEngagement = { mentioned: true, group: true, authorId: "user-1" };
		const nonOwnerDecision = decideEngagement(
			channel as typeof channel,
			nonOwnerEngagement,
			config,
			false,
			false,
			connection,
		);
		expect(nonOwnerDecision.engaged).toBe(false);
	});

	test("platform scoping respects adapter platform (#862)", () => {
		const ownerChannel = {
			platform: "discord",
			kind: "channel",
			conversationId: "owner-channel",
		} as const;
		const config: GatewayConfig = {
			schemaVersion: 1,
			home: "/tmp",
			gjc: { agentDir: "/tmp/agent" },
			configPath: "/tmp/config.json",
			socketPath: "/tmp/socket",
			dbPath: "/tmp/db.sqlite",
			ownerTarget: { origin: ownerChannel },
		};

		// Connection reported Discord owner, but channel is Slack
		const connection = {
			platformOwnerIds: ["discord-owner"],
			platformOwnerPlatform: "discord",
		};

		// Slack channel should not use Discord adapter's owner IDs
		const slackChannel = { platform: "slack" as const, kind: "channel" as const, conversationId: "ch" };
		const engagement = { mentioned: true, group: true, authorId: "discord-owner" };
		const decision = decideEngagement(slackChannel, engagement, config, false, false, connection);
		expect(decision.engaged).toBe(false);

		// Discord channel can use Discord adapter's owner IDs
		const discordChannel = { platform: "discord" as const, kind: "channel" as const, conversationId: "ch" };
		const decision2 = decideEngagement(discordChannel, engagement, config, false, false, connection);
		expect(decision2.engaged).toBe(true);
	});
});
