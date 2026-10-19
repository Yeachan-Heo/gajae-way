import { expect, test } from "bun:test";
import type { GatewayConfig } from "../src/config";
import { decideEngagement, threadFollowUpEngaged, threadMuteKey } from "../src/engagement/policy";

const OWNER = "U-owner";
const THREAD = {
	platform: "slack" as const,
	kind: "thread" as const,
	conversationId: "C1:1700000000.000100",
	parentId: "C1",
};
const CHANNEL = { platform: "slack" as const, kind: "channel" as const, conversationId: "C1" };

function config(overrides: Partial<GatewayConfig> = {}): GatewayConfig {
	return {
		schemaVersion: 1,
		home: "/tmp/home",
		configPath: "/tmp/home/config.json",
		socketPath: "/tmp/home/gateway.sock",
		dbPath: "/tmp/home/gateway.db",
		stallTimeoutMs: 120_000,
		ownerTarget: { origin: { platform: "slack", kind: "dm", conversationId: "D1", peerId: OWNER } },
		...overrides,
	} as GatewayConfig;
}

const speaker = (authorId: string, mentioned = false) => ({ mentioned, group: true, authorId });

test("a mention-open thread keeps listening after the persona answered once", () => {
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	// Without the opening mention and without a prior turn, the gate holds.
	expect(decideEngagement(THREAD, speaker("U-stranger"), mentionOpen, false).engaged).toBe(false);
	// After one answered turn in this thread, plain follow-ups are admitted.
	expect(decideEngagement(THREAD, speaker("U-stranger"), mentionOpen, true).engaged).toBe(true);
});

test("follow-up admission is scoped to threads, never to the channel root", () => {
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	expect(decideEngagement(CHANNEL, speaker("U-stranger"), mentionOpen, true).engaged).toBe(false);
});

test("a closed channel still authorises the author on every follow-up", () => {
	const closed = config({ mentionAllowlist: [OWNER] });
	expect(decideEngagement(THREAD, speaker(OWNER), closed, true).engaged).toBe(true);
	expect(decideEngagement(THREAD, speaker("U-intruder"), closed, true).engaged).toBe(false);
	// The mention alone never buys a stranger in: closed needs addressed AND authorised.
	expect(decideEngagement(THREAD, speaker("U-intruder", true), closed, true).engaged).toBe(false);
});

test("audience rules still decide bots in a followed-up thread", () => {
	const humanOnly = config({ channels: { "slack:C1": { engagement: "mention-open", audience: "human-only" } } });
	expect(decideEngagement(THREAD, { ...speaker("U-bot"), authorIsBot: true }, humanOnly, true).engaged).toBe(false);
	const all = config({ channels: { "slack:C1": { engagement: "mention-open", audience: "all" } } });
	expect(decideEngagement(THREAD, { ...speaker("U-bot"), authorIsBot: true }, all, true)).toEqual({
		engaged: true,
		botAudienceAdmission: true,
	});
});

const THREAD_KEY = "slack/thread/C1:1700000000.000100/parent=C1";

function store(
	options: { triggeredOrigins?: readonly string[]; triggeredMessages?: readonly [string, string][] } = {},
) {
	return {
		originTriggeredTurn: (key: string) => (options.triggeredOrigins ?? []).includes(key),
		messageJoinedTurn: (key: string, messageId: string) =>
			(options.triggeredMessages ?? []).some(([k, m]) => k === key && m === messageId),
	};
}

test("a mention written inside the thread marks the thread engaged", () => {
	expect(threadFollowUpEngaged(THREAD, THREAD_KEY, store({ triggeredOrigins: [THREAD_KEY] }))).toBe(true);
	expect(threadFollowUpEngaged(THREAD, THREAD_KEY, store())).toBe(false);
});

test("a channel mention answered INTO a thread marks that thread engaged", () => {
	// The trigger belongs to the channel origin and its message id is the thread
	// root, which is exactly the thread's conversation id.
	const opened = store({ triggeredMessages: [["slack/channel/C1", "C1:1700000000.000100"]] });
	expect(threadFollowUpEngaged(THREAD, THREAD_KEY, opened)).toBe(true);
	// A different thread in the same channel is not engaged by that root.
	const otherThread = { ...THREAD, conversationId: "C1:1700000000.000999" };
	expect(threadFollowUpEngaged(otherThread, "slack/thread/C1:1700000000.000999/parent=C1", opened)).toBe(false);
});

test("the signal never promotes a channel origin and needs a parent to look one up", () => {
	const opened = store({
		triggeredOrigins: ["slack/channel/C1"],
		triggeredMessages: [["slack/channel/C1", "C1:1700000000.000100"]],
	});
	expect(threadFollowUpEngaged(CHANNEL, "slack/channel/C1", opened)).toBe(false);
	expect(threadFollowUpEngaged({ ...THREAD, parentId: undefined }, THREAD_KEY, opened)).toBe(false);
});

test("defaulting the parameter keeps every existing caller mention-gated", () => {
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	expect(decideEngagement(THREAD, speaker("U-stranger"), mentionOpen).engaged).toBe(false);
	expect(decideEngagement(THREAD, speaker("U-stranger", true), mentionOpen).engaged).toBe(true);
});

test("a muted thread suppresses follow-ups without mention or reply-to (issue #625)", () => {
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	const followUp = threadFollowUpEngaged(
		THREAD,
		THREAD_KEY,
		store({ triggeredMessages: [["slack/channel/C1", THREAD.conversationId]] }),
	);
	expect(followUp).toBe(true);
	const noMentionNoReply = { mentioned: false, explicitMention: false, group: true, authorId: "U-stranger" };
	expect(decideEngagement(THREAD, noMentionNoReply, mentionOpen, followUp, true).engaged).toBe(false);
	expect(decideEngagement(THREAD, noMentionNoReply, mentionOpen, followUp, false).engaged).toBe(true);
});

test("mute overrides open-channel admission unless there is a real mention or reply", () => {
	const open = config({ channels: { "slack:C1": { engagement: "open", audience: "all" } } });
	const openChannelFollowUp = { mentioned: true, explicitMention: false, group: true, authorId: OWNER };
	expect(decideEngagement(THREAD, openChannelFollowUp, open, true, true).engaged).toBe(false);
	const explicitMention = { ...openChannelFollowUp, explicitMention: true };
	expect(decideEngagement(THREAD, explicitMention, open, true, true).engaged).toBe(true);
});

test("a muted thread engages with explicit mention (issue #625)", () => {
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	const withMention = { mentioned: true, explicitMention: true, group: true, authorId: "U-stranger" };
	expect(decideEngagement(THREAD, withMention, mentionOpen, true, true).engaged).toBe(true);
});

test("a muted thread engages with reply-to (issue #625)", () => {
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	const withReplyToBot = {
		mentioned: false,
		explicitMention: false,
		group: true,
		authorId: "U-stranger",
		replyTo: { messageId: "C1:1700000000.000200", fromSelf: true },
	};
	expect(decideEngagement(THREAD, withReplyToBot, mentionOpen, true, true).engaged).toBe(true);
});

test("a legacy adapter mention unmutes except where an open channel marks every message mentioned", () => {
	const legacyMention = { mentioned: true, group: true, authorId: OWNER };
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	expect(decideEngagement(THREAD, legacyMention, mentionOpen, true, true).engaged).toBe(true);
	const open = config({ channels: { "slack:C1": { engagement: "open", audience: "all" } } });
	expect(decideEngagement(THREAD, legacyMention, open, true, true).engaged).toBe(false);
	// No policy entry is a closed channel: `mentioned` is still a real mention there.
	expect(decideEngagement(THREAD, legacyMention, config(), true, true).engaged).toBe(true);
});

test("a Slack thread kept on the channel session mutes under its own thread key", () => {
	const botRootReply = {
		mentioned: true,
		explicitMention: false,
		group: true,
		authorId: OWNER,
		replyTo: { messageId: THREAD.conversationId, fromSelf: true },
	};
	expect(threadMuteKey(CHANNEL, "slack/channel/C1", botRootReply)).toBe(THREAD_KEY);
	expect(threadMuteKey(THREAD, THREAD_KEY, undefined)).toBe(THREAD_KEY);
	expect(threadMuteKey(CHANNEL, "slack/channel/C1", { ...botRootReply, replyTo: undefined })).toBeUndefined();
	const discordChannel = { platform: "discord" as const, kind: "channel" as const, conversationId: "C1" };
	expect(threadMuteKey(discordChannel, "discord/channel/C1", botRootReply)).toBeUndefined();
	// Every reply in that thread is fromSelf, so only a mention admits it while muted.
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	expect(decideEngagement(CHANNEL, botRootReply, mentionOpen, false, true).engaged).toBe(false);
	expect(decideEngagement(CHANNEL, { ...botRootReply, explicitMention: true }, mentionOpen, false, true).engaged).toBe(
		true,
	);
});
