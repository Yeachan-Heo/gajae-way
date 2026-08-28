import { describe, expect, test } from "bun:test";
import type { EngagementContext } from "@gajaeway/protocol";
import type { GatewayConfig } from "../src/config";
import { CONFIG_SCHEMA_VERSION as SCHEMA, ENGAGEMENT_GATES } from "../src/config";
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
		expect(decideEngagement({ ...CHANNEL, kind: "dm" }, ctx(), c).engaged).toBe(true);
		expect(decideEngagement({ ...CHANNEL, platform: "loopback" }, ctx(), c).engaged).toBe(true);
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
