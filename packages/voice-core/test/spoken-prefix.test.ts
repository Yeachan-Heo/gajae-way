import { describe, expect, test } from "bun:test";
import type { TtsChunk } from "../src/ports";
import { accumulateAlignment, spokenPrefixFromChunks, truncateSpokenTimeline } from "../src/spoken-prefix";

const audio = (
	chars: readonly string[],
	charStartMs: readonly number[],
	charDurationMs: readonly number[],
): TtsChunk => ({ kind: "audio", audio: new Uint8Array(), chars, charStartMs, charDurationMs });

describe("spoken-prefix", () => {
	const chunks: readonly TtsChunk[] = [
		audio(["A", "B"], [0, 100], [100, 100]),
		audio(["C", "D"], [0, 50], [50, 100]),
		{ kind: "final" },
	];

	test("accumulates chunk-relative alignment into an absolute timeline", () => {
		const timeline = accumulateAlignment(chunks);
		expect(timeline.text).toBe("ABCD");
		expect(timeline.durationMs).toBe(350);
		expect(timeline.points.map((point) => [point.char, point.startMs, point.endMs])).toEqual([
			["A", 0, 100],
			["B", 100, 200],
			["C", 200, 250],
			["D", 250, 350],
		]);
	});

	test("returns an empty spoken prefix at zero milliseconds", () => {
		expect(spokenPrefixFromChunks(chunks, 0)).toEqual({ charIndex: 0, spokenPrefix: "", remainder: "ABCD" });
	});

	test("leaves a character unspoken at an exact character-start boundary", () => {
		expect(spokenPrefixFromChunks(chunks, 100)).toEqual({ charIndex: 1, spokenPrefix: "A", remainder: "BCD" });
	});

	test("returns the entire prefix beyond the timeline end", () => {
		expect(spokenPrefixFromChunks(chunks, 999)).toEqual({ charIndex: 4, spokenPrefix: "ABCD", remainder: "" });
	});

	test("truncates at an interior point only after fully ended characters", () => {
		const timeline = accumulateAlignment(chunks);
		expect(truncateSpokenTimeline(timeline, 240)).toEqual({ charIndex: 2, spokenPrefix: "AB", remainder: "CD" });
	});
});
