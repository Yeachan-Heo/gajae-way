import { describe, expect, test } from "bun:test";
import type { EngagementContext } from "@gajaeway/protocol";
import type { GatewayConfig } from "../src/config";
import { ENGAGEMENT_GATES, CONFIG_SCHEMA_VERSION as SCHEMA } from "../src/config";
import { decideEngagement } from "../src/engagement/policy";

const OWNER = "660473980301344768";
const STRANGER = "999999999999999999";
const CHANNEL = { platform: "discord", kind: "channel", conversationId: "c1" };

function config(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
	return {
		home: "/tmp",
		configPath: "/tmp/config.json",
		socketPath: "/tmp/gateway.sock",
		dbPath: "/tmp/gateway.db",
		schemaVersion: 1,
		ownerTarget: { origin: { platform: "discord", kind: "dm", conversationId: "d1", peerId: OWNER } },
		...overrides,
	} as GatewayConfig;
}

function ctx(overrides: Partial<EngagementContext> = {}): EngagementContext {
	return { mentioned: false, group: true, authorId: STRANGER, ...overrides } as EngagementContext;
}

const gate = (engagement: string) =>
	({ channels: { "discord:c1": { engagement } } }) as unknown as Partial<GatewayConfig>;

describe("open", () => {
	test("every human message is a turn, mention or not", () => {
		expect(decideEngagement(CHANNEL, ctx(), config(gate("open"))).engaged).toBe(true);
	});

	test("a bot still needs a mention", () => {
		const c = config(gate("open"));
		expect(decideEngagement(CHANNEL, ctx({ authorIsBot: true }), c).engaged).toBe(false);
		expect(decideEngagement(CHANNEL, ctx({ authorIsBot: true, mentioned: true, authorId: OWNER }), c).engaged).toBe(
			true,
		);
	});
});

describe("open-mention-only", () => {
	const c = config(gate("open-mention-only"));

	test("an unlisted stranger may address the persona", () => {
		expect(decideEngagement(CHANNEL, ctx({ mentioned: true }), c).engaged).toBe(true);
	});

	test("but only by addressing it", () => {
		expect(decideEngagement(CHANNEL, ctx(), c).engaged).toBe(false);
	});

	test("the allowlist does not gate it", () => {
		const gated = config({ ...gate("open-mention-only"), mentionAllowlist: [OWNER] });
		expect(decideEngagement(CHANNEL, ctx({ mentioned: true }), gated).engaged).toBe(true);
	});
});

describe("closed", () => {
	test("refuses the same stranger that open-mention-only admits", () => {
		const c = config({ ...gate("closed"), mentionAllowlist: [OWNER] });
		expect(decideEngagement(CHANNEL, ctx({ mentioned: true }), c).engaged).toBe(false);
		expect(decideEngagement(CHANNEL, ctx({ mentioned: true, authorId: OWNER }), c).engaged).toBe(true);
	});

	test("an EMPTY allowlist is owner-only, not everyone", () => {
		const c = config(gate("closed"));
		expect(decideEngagement(CHANNEL, ctx({ mentioned: true }), c).engaged).toBe(false);
		expect(decideEngagement(CHANNEL, ctx({ mentioned: true, authorId: OWNER }), c).engaged).toBe(true);
	});
});

describe("default", () => {
	test("a channel with no entry is closed, not open", () => {
		const c = config({ mentionAllowlist: [OWNER] });
		expect(decideEngagement(CHANNEL, ctx(), c).engaged).toBe(false);
		expect(decideEngagement(CHANNEL, ctx({ mentioned: true }), c).engaged).toBe(false);
		expect(decideEngagement(CHANNEL, ctx({ mentioned: true, authorId: OWNER }), c).engaged).toBe(true);
	});

	test("DMs and loopback are unaffected by the gates", () => {
		const c = config(gate("closed"));
		// `loopback` is local and stays exempt. A DM no longer is: see the
		// "direct messages" block below.
		expect(decideEngagement({ ...CHANNEL, platform: "loopback" }, ctx(), c).engaged).toBe(true);
	});
});

describe("direct messages", () => {
	const DM = { platform: "discord", kind: "dm", conversationId: "d1" };
	const ALLOWED = "111111111111111111";

	test("a stranger gets no turn under the default policy", () => {
		// The regression this file exists for: the gate used to return engaged
		// for every DM before the allowlist was ever consulted.
		expect(decideEngagement(DM, ctx(), config({ mentionAllowlist: [ALLOWED] })).engaged).toBe(false);
	});

	test("the owner is always engaged", () => {
		expect(decideEngagement(DM, ctx({ authorId: OWNER }), config()).engaged).toBe(true);
		expect(decideEngagement(DM, ctx({ authorId: OWNER }), config({ dmPolicy: "owner-only" })).engaged).toBe(true);
		expect(decideEngagement(DM, ctx({ authorId: OWNER }), config({ mentionAllowlist: [ALLOWED] })).engaged).toBe(
			true,
		);
	});

	test("an allowlisted author is engaged by default", () => {
		expect(decideEngagement(DM, ctx({ authorId: ALLOWED }), config({ mentionAllowlist: [ALLOWED] })).engaged).toBe(
			true,
		);
	});

	test("an empty allowlist narrows to the owner rather than widening to everyone", () => {
		const c = config({ mentionAllowlist: [] });
		expect(decideEngagement(DM, ctx(), c).engaged).toBe(false);
		expect(decideEngagement(DM, ctx({ authorId: OWNER }), c).engaged).toBe(true);
	});

	test("owner-only declines an allowlisted author", () => {
		const c = config({ dmPolicy: "owner-only", mentionAllowlist: [ALLOWED] });
		expect(decideEngagement(DM, ctx({ authorId: ALLOWED }), c).engaged).toBe(false);
	});

	test("open engages anyone, including an author we cannot identify", () => {
		const c = config({ dmPolicy: "open" });
		expect(decideEngagement(DM, ctx(), c).engaged).toBe(true);
		expect(decideEngagement(DM, ctx({ authorId: undefined }), c).engaged).toBe(true);
	});

	test("an unidentifiable author is declined unless the policy is open", () => {
		expect(decideEngagement(DM, ctx({ authorId: undefined }), config({ mentionAllowlist: [ALLOWED] })).engaged).toBe(
			false,
		);
	});

	test("a bot DM is subject to the same authorisation", () => {
		const c = config({ mentionAllowlist: [ALLOWED] });
		expect(decideEngagement(DM, ctx({ authorIsBot: true }), c).engaged).toBe(false);
		expect(decideEngagement(DM, ctx({ authorIsBot: true, authorId: ALLOWED }), c).engaged).toBe(true);
	});
});

describe("config validation", () => {
	// The parser takes a parsed object and pins the current schema version.
	test("an unknown gate is rejected rather than treated as unset", async () => {
		const { parseConfigFile } = await import("../src/config");
		expect(() => parseConfigFile({ schemaVersion: SCHEMA, channels: { c1: { engagement: "kinda-open" } } })).toThrow(
			/must be one of open, open-mention-only, closed/,
		);
	});

	test("an unknown dmPolicy is rejected", async () => {
		const { parseConfigFile } = await import("../src/config");
		expect(() => parseConfigFile({ schemaVersion: SCHEMA, dmPolicy: "sure-why-not" })).toThrow(
			/dmPolicy must be one of owner-only, allowlist, open/,
		);
	});

	test("each valid dmPolicy parses", async () => {
		const { parseConfigFile, DM_POLICIES } = await import("../src/config");
		for (const policy of DM_POLICIES) {
			expect(parseConfigFile({ schemaVersion: SCHEMA, dmPolicy: policy }).dmPolicy).toBe(policy);
		}
	});

	test("dmPolicy is reloadable without a restart", async () => {
		const { RELOADABLE_FIELDS } = await import("../src/config");
		expect(RELOADABLE_FIELDS).toContain("dmPolicy");
	});

	test("each valid gate parses", async () => {
		const { parseConfigFile } = await import("../src/config");
		for (const gateName of ENGAGEMENT_GATES) {
			const parsed = parseConfigFile({
				schemaVersion: SCHEMA,
				channels: { c1: { engagement: gateName } },
			});
			expect(parsed.channels?.c1?.engagement).toBe(gateName);
		}
	});
});
