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

const THREAD_KEY = "slack/thread/C1:1700000000.000100/parent=C1";

function store(
	options: { triggeredOrigins?: readonly string[]; triggeredMessages?: readonly [string, string][] } = {},
) {
	return {
		originTriggeredTurn: (key: string) => (options.triggeredOrigins ?? []).includes(key),
		messageTriggeredTurn: (key: string, messageId: string) =>
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
