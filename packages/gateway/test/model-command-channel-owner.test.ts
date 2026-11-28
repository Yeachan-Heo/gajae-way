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

describe("channel ownerTarget names its owner explicitly (#901)", () => {
	const ownerChannel = { platform: "discord", kind: "channel", conversationId: "owner-channel" } as const;
	const base: GatewayConfig = {
		schemaVersion: 1,
		home: "/tmp",
		gjc: { agentDir: "/tmp/agent" },
		configPath: "/tmp/config.json",
		socketPath: "/tmp/socket",
		dbPath: "/tmp/db.sqlite",
		dmPolicy: "allowlist",
	};
	// The adapter reports a different application owner: it must never count once ownerTarget is set.
	const connection = { platformOwnerIds: ["app-owner"], platformOwnerPlatform: "discord" };
	const dm = { platform: "discord" as const, kind: "dm" as const, conversationId: "dm" };
	const channel = { platform: "discord" as const, kind: "channel" as const, conversationId: "ch" };
	const by = (authorId: string, group: boolean) => ({ mentioned: true, group, authorId });

	test("the configured owner's DM and closed-channel mention engage; the adapter's app owner does not", () => {
		const config = { ...base, ownerTarget: { origin: ownerChannel, owner: "owner-1" } };
		expect(decideEngagement(dm, by("owner-1", false), config, false, false, connection).engaged).toBe(true);
		expect(decideEngagement(channel, by("owner-1", true), config, false, false, connection).engaged).toBe(true);
		expect(decideEngagement(dm, by("app-owner", false), config, false, false, connection).engaged).toBe(false);
		expect(decideEngagement(channel, by("app-owner", true), config, false, false, connection).engaged).toBe(false);
		expect(decideEngagement(dm, by("user-1", false), config, false, false, connection).engaged).toBe(false);
	});

	test("a channel ownerTarget without owner configures nobody: adapter owners do not fill in", () => {
		const config = { ...base, ownerTarget: { origin: ownerChannel } };
		expect(decideEngagement(dm, by("app-owner", false), config, false, false, connection).engaged).toBe(false);
		expect(decideEngagement(channel, by("app-owner", true), config, false, false, connection).engaged).toBe(false);
	});

	test("the configured owner counts only on the ownerTarget's platform", () => {
		const config = { ...base, ownerTarget: { origin: ownerChannel, owner: "owner-1" } };
		const slackDm = { platform: "slack" as const, kind: "dm" as const, conversationId: "D1", peerId: "owner-1" };
		expect(decideEngagement(slackDm, by("owner-1", false), config).engaged).toBe(false);
	});
});

test("ownerTarget.owner is accepted only on a non-DM origin and must be a non-empty string", async () => {
	const { parseConfigFile } = await import("../src/config");
	const channel = { platform: "discord", kind: "channel", conversationId: "reports" } as const;
	expect(parseConfigFile({ schemaVersion: 1, ownerTarget: { origin: channel, owner: "owner-1" } }).ownerTarget).toEqual(
		{
			origin: channel,
			owner: "owner-1",
		},
	);
	const dm = { platform: "discord", kind: "dm", conversationId: "dm", peerId: "owner-1" };
	expect(() => parseConfigFile({ schemaVersion: 1, ownerTarget: { origin: dm, owner: "owner-2" } })).toThrow(
		"ownerTarget.owner is only for a non-DM origin",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, ownerTarget: { origin: channel, owner: "" } })).toThrow(
		"ownerTarget.owner must be a non-empty string",
	);
});
