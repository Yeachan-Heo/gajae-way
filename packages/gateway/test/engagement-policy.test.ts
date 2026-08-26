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

test("mention allowlist gates group mention commands but never open channels or DMs", () => {
	const base = {
		schemaVersion: 1 as const,
		home: "/tmp/x",
		configPath: "/tmp/x/config.json",
		socketPath: "/tmp/x/s",
		dbPath: "/tmp/x/db",
		logVerbosity: "info" as const,
		mentionAllowlist: ["owner-1"],
	};
	const channel = { platform: "discord", kind: "channel", conversationId: "c1" };
	const allowed = { mentioned: true, group: true, authorId: "owner-1" };
	const stranger = { mentioned: true, group: true, authorId: "intruder-9" };
	expect(decideEngagement(channel, allowed, base as never).engaged).toBe(true);
	expect(decideEngagement(channel, stranger, base as never).engaged).toBe(false);
	// Open channels are rooms the persona inhabits: allowlist does not gate listening.
	const open = { ...base, channels: { c1: { engagement: "open" as const } } };
	expect(
		decideEngagement(channel, { mentioned: false, group: true, authorId: "intruder-9" }, open as never).engaged,
	).toBe(true);
	// DMs engage regardless: the DM peer is the conversation itself.
	const dm = { platform: "discord", kind: "dm", conversationId: "d1", peerId: "p" };
	expect(decideEngagement(dm, stranger, base as never).engaged).toBe(true);
});
