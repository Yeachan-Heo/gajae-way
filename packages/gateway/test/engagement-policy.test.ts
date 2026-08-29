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
test("loopback engages while unmentioned groups and unauthorised DMs decline", () => {
	// A DM from nobody in particular is no longer a free turn: with no owner and
	// no allowlist configured, the DM path fails closed.
	expect(decideEngagement({ platform: "discord", kind: "dm", conversationId: "dm" }, undefined, config)).toEqual({
		engaged: false,
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

test("mention allowlist gates group mention commands and DMs but never open channels", () => {
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
	// DMs are authorised like anything else: allowlisted in, stranger out.
	const dm = { platform: "discord", kind: "dm", conversationId: "d1", peerId: "p" };
	expect(decideEngagement(dm, stranger, base as never).engaged).toBe(false);
	expect(
		decideEngagement(dm, { mentioned: false, group: false, authorId: "owner-1" }, base as never).engaged,
	).toBe(true);
});

test("bot authors never get the open-channel free pass; a bot mention still engages", () => {
	const base = {
		schemaVersion: 1 as const,
		home: "/tmp/x",
		configPath: "/tmp/x/config.json",
		socketPath: "/tmp/x/s",
		dbPath: "/tmp/x/db",
		logVerbosity: "info" as const,
		mentionAllowlist: ["owner-1", "sibling-bot"],
	};
	const channel = { platform: "discord", kind: "channel", conversationId: "c1" };
	const open = { ...base, channels: { c1: { engagement: "open" as const } } };
	// Sibling-bot chatter (progress spam, replies to each other) must not burn turns.
	expect(
		decideEngagement(
			channel,
			{ mentioned: false, group: true, authorId: "sibling-bot", authorIsBot: true },
			open as never,
		).engaged,
	).toBe(false);
	// A bot that explicitly mentions us gets a turn through the normal allowlisted mention path.
	expect(
		decideEngagement(
			channel,
			{ mentioned: true, group: true, authorId: "sibling-bot", authorIsBot: true },
			open as never,
		).engaged,
	).toBe(true);
	// An unlisted bot mention stays context, never a turn.
	expect(
		decideEngagement(channel, { mentioned: true, group: true, authorId: "rogue-bot", authorIsBot: true }, open as never)
			.engaged,
	).toBe(false);
	// Humans keep the open-channel free pass.
	expect(decideEngagement(channel, { mentioned: false, group: true, authorId: "human-2" }, open as never).engaged).toBe(
		true,
	);
});
