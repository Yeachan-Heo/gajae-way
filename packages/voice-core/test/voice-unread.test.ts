import { describe, expect, test } from "bun:test";
import type { EngagementContext } from "../src/ports";
import { buildVoiceUnreadPayload } from "../src/voice-unread";

const engagement = (authorId: string): EngagementContext => ({ authorId, authorName: `Name ${authorId}` });
const item = (speakerId: string, endedAtMs: number, body = `body-${speakerId}-${endedAtMs}`) => ({
	voiceChannelId: "channel",
	speakerId,
	endedAtMs,
	body,
	engagement: engagement(speakerId),
});

describe("buildVoiceUnreadPayload", () => {
	test("keeps all 20 items and drops the oldest item from 21", () => {
		const utterances = Array.from({ length: 21 }, (_, index) => item(`speaker-${index}`, index));
		const payload = buildVoiceUnreadPayload(utterances, { maxItems: 20, maxCharsPerItem: 200 });
		expect(payload.entries).toHaveLength(20);
		expect(payload.dropped).toBe(1);
		expect(payload.truncated).toBe(0);
		expect(payload.entries[0]?.messageId).toBe("voice:channel:speaker-1:1");
		expect(payload.entries[19]?.messageId).toBe("voice:channel:speaker-20:20");
	});

	test("truncates 201 characters with a visible marker while preserving the cap", () => {
		const payload = buildVoiceUnreadPayload([item("speaker", 1234, "x".repeat(201))], {
			maxItems: 20,
			maxCharsPerItem: 200,
		});
		const entry = payload.entries[0];
		expect(payload.truncated).toBe(1);
		expect(entry?.text.endsWith("…")).toBe(true);
		expect(entry?.text).toHaveLength(200);
		expect(entry?.at).toBe("1970-01-01T00:00:01.234Z");
		expect(entry?.engagement).toEqual(engagement("speaker"));
	});

	test("orders same-millisecond speakers by stable message id", () => {
		const payload = buildVoiceUnreadPayload([item("z", 500), item("a", 500), item("m", 500)], {
			maxItems: 20,
			maxCharsPerItem: 200,
		});
		expect(payload.entries.map((entry) => entry.messageId)).toEqual([
			"voice:channel:a:500",
			"voice:channel:m:500",
			"voice:channel:z:500",
		]);
	});

	test("sorts chronological entries independently of arrival order", () => {
		const payload = buildVoiceUnreadPayload([item("later", 200), item("earlier", 100)], {
			maxItems: 20,
			maxCharsPerItem: 200,
		});
		expect(payload.entries.map((entry) => entry.at)).toEqual(["1970-01-01T00:00:00.100Z", "1970-01-01T00:00:00.200Z"]);
	});
});
