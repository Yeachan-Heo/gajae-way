import { expect, test } from "bun:test";
import type { GatewayConfig } from "../src/config";
import { decideEngagement } from "../src/engagement/policy";

const config: GatewayConfig = {
	schemaVersion: 1,
	home: "/tmp/home",
	configPath: "/tmp/home/config.json",
	socketPath: "/tmp/home/gateway.sock",
	dbPath: "/tmp/home/gateway.db",
	logVerbosity: "info",
};
const engagement = { mentioned: false, group: true, authorId: "author" };
test("DMs and loopback engage while unmentioned groups decline", () => {
	expect(decideEngagement({ platform: "discord", kind: "dm", conversationId: "dm" }, undefined, config)).toEqual({
		engaged: true,
	});
	expect(
		decideEngagement({ platform: "loopback", kind: "loopback", conversationId: "loopback" }, undefined, config),
	).toEqual({ engaged: true });
	expect(
		decideEngagement({ platform: "discord", kind: "channel", conversationId: "channel" }, engagement, config),
	).toEqual({ engaged: false });
});
test("per-channel open override engages group messages", () => {
	expect(
		decideEngagement({ platform: "discord", kind: "channel", conversationId: "channel" }, engagement, {
			...config,
			channels: { channel: { engagement: "open" } },
		}),
	).toEqual({ engaged: true });
});
