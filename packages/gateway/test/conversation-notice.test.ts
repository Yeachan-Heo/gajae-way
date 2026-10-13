import { expect, test } from "bun:test";
import type { OriginRef } from "@gajae-gateway/protocol";
import { currentConversationNotice } from "../src/server/server";

const slackDm: OriginRef = { platform: "slack", kind: "dm", conversationId: "D1", peerId: "U1" };
const slackChannel: OriginRef = { platform: "slack", kind: "channel", conversationId: "C1" };
const loopback: OriginRef = { platform: "loopback", kind: "loopback", conversationId: "loopback" };

// Live finding (slack DM, 2026-09-17): reply-threading was fully implemented but
// never named in the session context, so threaded messages were answered at the
// conversation root and the persona looked like it ignored the thread.
test("a chat conversation notice names the reply-threading token", () => {
	for (const origin of [slackDm, slackChannel]) {
		const notice = currentConversationNotice(origin);
		expect(notice).toContain("[REPLY:<message id>]");
		expect(notice).toContain("thread's parent message id");
	}
});

// Owner request 2026-10-01: post a work-done line to another configured room.
test("a chat conversation notice names the cross-room [POST:] token and its limits", () => {
	const discordChannel: OriginRef = { platform: "discord", kind: "channel", conversationId: "1480171104348930221" };
	const discord = currentConversationNotice(discordChannel);
	expect(discord).toContain("[POST:<origin key>]");
	expect(discord).toContain("[POST:discord/channel/<channel id>]");
	expect(discord).toContain("Only discord channels configured in the gateway are allowed");
	expect(discord).toContain("never a DM or a thread");
	expect(discord).toContain("a refused target is posted here with the token removed");
	expect(discord).toContain("<#channel id>");
	// No specific room is baked into the notice.
	expect(discord).not.toContain("1554742285260816455");
	expect(discord).not.toContain("업무보고");
	expect(currentConversationNotice(slackDm)).toContain("[POST:slack/channel/<channel id>]");
	const lines = discord.split("\n");
	expect(lines.filter((line) => line.includes("[POST:")).length).toBe(1);
});

test("loopback has no platform threads, so it is not told about the token", () => {
	const notice = currentConversationNotice(loopback);
	expect(notice).not.toContain("[REPLY:");
	expect(notice).not.toContain("[POST:");
	expect(notice).toContain("loopback");
});

test("the threading guidance stays separate from the reaction guidance", () => {
	const lines = currentConversationNotice(slackDm).split("\n");
	expect(lines.filter((line) => line.includes("[REPLY:")).length).toBe(1);
	expect(lines.filter((line) => line.includes("[REACT:")).length).toBe(1);
});

// Live 2026-09-25: a 13-minute foreground `gh run watch` held an omc-dev turn, and
// owner steers were answered with a bare [REACT:👀] while the persona polled lanes.
test("every conversation is told to stay responsive: no foreground waits, answer steers in words", () => {
	const channel: OriginRef = { platform: "discord", kind: "channel", conversationId: "1480171104348930221" };
	for (const origin of [slackDm, channel, loopback]) {
		const notice = currentConversationNotice(origin);
		expect(notice).toContain("## Staying responsive while you work");
		expect(notice).toContain("Never block a conversation turn on a foreground wait");
		expect(notice).toContain("`gh run watch`");
		expect(notice).toContain("Always answer it in words, right away");
	}
	// Starting a lane from this session routes its completion back as an internal report.
	const notice = currentConversationNotice(channel);
	expect(notice).toContain('gajaeway work start <name> "<task>"');
	expect(notice).toContain("internal report, not a chat post");
	expect(notice).not.toContain("--notify");
	// Only chat platforms get the gateway's 👀 acknowledgement, so only they are told about it.
	expect(currentConversationNotice(channel)).toContain("The gateway already marks the message 👀");
	expect(currentConversationNotice(loopback)).not.toContain("👀");
});
