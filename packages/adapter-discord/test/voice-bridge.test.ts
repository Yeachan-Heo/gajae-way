import { describe, expect, test } from "bun:test";
import type { ChatContextParams, EngagementContext, OriginRef } from "@gajaeway/protocol";
import { OriginTurnBook } from "@gajaeway/voice-core";
import { createVoiceTurnBridge } from "../src/voice/bridge";

const ORIGIN: OriginRef = { platform: "discord", kind: "channel", conversationId: "voice-channel" };
const CONFIG = { mergeWindowMs: 1_500, unread: { maxItems: 20, maxCharsPerItem: 200 } } as const;
const engagement = (authorId: string): EngagementContext => ({
	mentioned: false,
	group: true,
	authorId,
	authorName: authorId,
});
const utterance = (speakerId: string, text: string, endedAtMs: number) => ({
	speakerId,
	text,
	endedAtMs,
	engagement: engagement(speakerId),
});

class FakeClock {
	atMs = 10_000;
	now(): number {
		return this.atMs;
	}
}

function bridgeFor(
	options: {
		readonly turnBook?: OriginTurnBook;
		readonly submitTurn?: (
			text: string,
			context: EngagementContext,
			messageId: string,
		) => Promise<{ engaged?: boolean; turnId?: string | null }>;
		readonly submitContext?: (params: ChatContextParams) => Promise<undefined>;
		readonly onTurnStart?: (turnId: string) => void;
	} = {},
) {
	const turnBook = options.turnBook ?? new OriginTurnBook(32);
	const turns: Array<{ text: string; engagement: EngagementContext; messageId: string }> = [];
	const contexts: ChatContextParams[] = [];
	const clock = new FakeClock();
	const bridge = createVoiceTurnBridge({
		config: CONFIG,
		origin: ORIGIN,
		originKey: "discord/channel/voice-channel",
		voiceChannelId: "voice-channel",
		turnBook,
		submitTurn: async (text, context, messageId) => {
			turns.push({ text, engagement: context, messageId });
			return options.submitTurn?.(text, context, messageId) ?? { engaged: true, turnId: "turn-1" };
		},
		submitContext: async (params) => {
			contexts.push(params);
			await options.submitContext?.(params);
			return undefined;
		},
		clock,
		onTurnStart: options.onTurnStart,
	});
	return { bridge, turnBook, turns, contexts, clock };
}

describe("voice turn bridge", () => {
	test("submits one merged idle turn and admits only a fully engaged response", async () => {
		const started: string[] = [];
		const first = bridgeFor({ onTurnStart: (turnId) => started.push(turnId) });
		await first.bridge.handleUtterances([utterance("alice", "hello", 1_000), utterance("bob", "there", 1_500)]);
		expect(first.turns).toHaveLength(1);
		expect(first.turns[0]?.text).toBe("alice: hello\nbob: there");
		expect(first.turnBook.isBusy()).toBe(true);
		expect(started).toEqual(["turn-1"]);

		const declined = bridgeFor({ submitTurn: async () => ({ engaged: false }) });
		await declined.bridge.handleUtterances([utterance("alice", "no", 2_000)]);
		expect(declined.turnBook.isBusy()).toBe(false);

		const duplicateAck = bridgeFor({ submitTurn: async () => ({ engaged: true, turnId: null }) });
		await duplicateAck.bridge.handleUtterances([utterance("alice", "again", 2_000)]);
		expect(duplicateAck.turnBook.isBusy()).toBe(false);
	});

	test("busy voice and text turns create no new turns and submit one context batch", async () => {
		for (const modality of ["voice", "text"] as const) {
			const turnBook = new OriginTurnBook(4);
			turnBook.admit({ messageId: "existing", turnId: `in-flight-${modality}`, modality, acceptedAtMs: 1 }, 1);
			const state = bridgeFor({ turnBook });
			await state.bridge.handleUtterances([utterance("alice", "held one", 1_000), utterance("bob", "held two", 1_200)]);
			expect(state.turns).toHaveLength(0);
			expect(state.contexts).toHaveLength(1);
			expect(state.contexts[0]?.entries.map((entry) => entry.messageId)).toEqual([
				"voice:voice-channel:alice:1000",
				"voice:voice-channel:bob:1200",
			]);
			expect(state.contexts[0]?.entries.map((entry) => entry.at)).toEqual([
				"1970-01-01T00:00:01.000Z",
				"1970-01-01T00:00:01.200Z",
			]);
			expect(state.contexts[0]?.entries.map((entry) => entry.engagement.authorId)).toEqual(["alice", "bob"]);
		}
	});

	test("applies the configured cap in the built payload and drops the oldest", async () => {
		const turnBook = new OriginTurnBook(1);
		turnBook.admit({ messageId: "existing", turnId: "in-flight", modality: "voice", acceptedAtMs: 1 }, 1);
		const state = bridgeFor({ turnBook });
		await state.bridge.handleUtterances(
			Array.from({ length: 21 }, (_, index) => utterance(`speaker-${index}`, "x".repeat(201), index)),
		);
		const context = state.contexts[0];
		expect(context).toBeDefined();
		expect(context?.entries).toHaveLength(20);
		expect(context?.entries[0]?.messageId).toBe("voice:voice-channel:speaker-1:1");
		expect(context?.entries[19]?.messageId).toBe("voice:voice-channel:speaker-20:20");
		expect(context?.entries.every((entry) => entry.text.length === 200)).toBe(true);
	});

	test("keeps a multi-speaker merge inside the window and splits outside it", async () => {
		const inside = bridgeFor({ submitTurn: async () => ({ engaged: false }) });
		await inside.bridge.handleUtterances([utterance("alice", "one", 1_000), utterance("bob", "two", 2_000)]);
		expect(inside.turns).toHaveLength(1);
		expect(inside.turns[0]?.text).toBe("alice: one\nbob: two");

		const outside = bridgeFor({ submitTurn: async () => ({ engaged: false }) });
		await outside.bridge.handleUtterances([utterance("alice", "one", 1_000), utterance("bob", "two", 2_501)]);
		expect(outside.turns).toHaveLength(2);
	});

	test("settles the turn book and runs the announce hook at most once", async () => {
		const started: string[] = [];
		const state = bridgeFor({ onTurnStart: (turnId) => started.push(turnId) });
		expect(state.bridge.announceOnce("manual")).toBe(true);
		expect(state.bridge.announceOnce("manual")).toBe(false);
		await state.bridge.handleUtterances([utterance("alice", "hello", 1_000)]);
		expect(state.bridge.announceOnce("turn-1")).toBe(false);
		expect(started).toEqual(["manual", "turn-1"]);
		state.bridge.handleTurnEnd({ turnId: "turn-1" });
		expect(state.turnBook.isBusy()).toBe(false);
	});
});
