import { describe, expect, test } from "bun:test";
import { advanceUtterance, createUtteranceState } from "../src/utterance";

describe("utterance boundary state machine", () => {
	test.each([
		[699, false],
		[700, true],
		[701, true],
	] as const)("ends at the configured silence threshold (%dms)", (silenceAtMs, ended) => {
		let state = createUtteranceState("speaker-1", 700);
		state = advanceUtterance(state, "speech", 0).state;
		const result = advanceUtterance(state, "silence", silenceAtMs);
		expect(result.utterance !== null).toBe(ended);
		if (ended) {
			expect(result.utterance).toEqual({
				speakerId: "speaker-1",
				startedAtMs: 0,
				endedAtMs: silenceAtMs,
				boundary: "silence_end",
			});
		} else {
			expect(result.state.phase).toBe("silence");
		}
	});

	test("does not mistake repeated short intra-speech gaps for an utterance end", () => {
		let state = createUtteranceState("speaker-1", 700);
		state = advanceUtterance(state, "speech", 0).state;
		for (const [silenceAtMs, speechAtMs] of [
			[200, 400],
			[600, 800],
			[1_000, 1_200],
			[1_400, 1_600],
		] as const) {
			const gap = advanceUtterance(state, "silence", silenceAtMs);
			expect(gap.utterance).toBeNull();
			state = advanceUtterance(gap.state, "speech", speechAtMs).state;
		}
		const boundary = advanceUtterance(state, "silence", 2_299);
		expect(boundary.utterance).toBeNull();
		const ended = advanceUtterance(boundary.state, "silence", 2_300);
		expect(ended.utterance?.boundary).toBe("silence_end");
	});

	test("starts a fresh record after a completed boundary", () => {
		let state = createUtteranceState("speaker-1", 700);
		state = advanceUtterance(state, "speech", 10).state;
		const first = advanceUtterance(state, "silence", 709);
		expect(first.utterance).toBeNull();
		const ended = advanceUtterance(first.state, "silence", 1_410);
		expect(ended.utterance?.startedAtMs).toBe(10);
		state = ended.state;
		const next = advanceUtterance(state, "speech", 2_000);
		expect(next.state.startedAtMs).toBe(2_000);
	});
});
