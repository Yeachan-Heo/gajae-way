import { expect, test } from "bun:test";
import type { GatewayConfig } from "../src/config";
import { decideEngagement, threadFollowUpEngaged } from "../src/engagement/policy";

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

test("the follow-up signal reads completed turns of the thread's own session row", () => {
	const store = { sessionTurnCount: (key: string) => (key === "slack/thread/C1:1/parent=C1" ? 2 : 0) };
	expect(threadFollowUpEngaged(THREAD, "slack/thread/C1:1/parent=C1", store)).toBe(true);
	// A fresh thread (no answered turn yet) and a channel origin both stay false.
	expect(threadFollowUpEngaged(THREAD, "slack/thread/C1:9/parent=C1", store)).toBe(false);
	expect(threadFollowUpEngaged(CHANNEL, "slack/thread/C1:1/parent=C1", store)).toBe(false);
});

test("defaulting the parameter keeps every existing caller mention-gated", () => {
	const mentionOpen = config({ channels: { "slack:C1": { engagement: "mention-open" } } });
	expect(decideEngagement(THREAD, speaker("U-stranger"), mentionOpen).engaged).toBe(false);
	expect(decideEngagement(THREAD, speaker("U-stranger", true), mentionOpen).engaged).toBe(true);
});
