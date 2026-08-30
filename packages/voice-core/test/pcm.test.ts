import { describe, expect, test } from "bun:test";
import {
	clampPcm16Sample,
	decodePcm16Le,
	encodePcm16Le,
	interleaveMonoToStereo,
	OPUS_FRAME_DURATION_MS,
	opusDurationMsForFrameCount,
	opusFrameCountForDuration,
	PCM_DISCORD_SAMPLE_RATE_HZ,
	pcmDurationMs,
	pcmFrameInfo,
	resamplePcm24kTo48k,
} from "../src/pcm";

describe("PCM16 encoding", () => {
	test("round-trips signed little-endian samples", () => {
		const source = new Int16Array([-32_768, -1, 0, 1, 32_767]);
		expect(Array.from(decodePcm16Le(encodePcm16Le(source)))).toEqual(Array.from(source));
	});

	test("clips both int16 bounds", () => {
		expect(clampPcm16Sample(-40_000)).toBe(-32_768);
		expect(clampPcm16Sample(40_000)).toBe(32_767);
		expect(Array.from(encodePcm16Le([-40_000, 40_000]))).toEqual([0, 128, 255, 127]);
	});
});

describe("PCM resampling and channel layout", () => {
	test("doubles 24k sample count with linear interpolation", () => {
		expect(Array.from(resamplePcm24kTo48k(new Int16Array([0, 1_000])))).toEqual([0, 500, 1_000, 1_000]);
		expect(resamplePcm24kTo48k(new Int16Array([1, 2, 3]))).toHaveLength(6);
	});

	test("interleaves mono samples as left/right pairs", () => {
		expect(Array.from(interleaveMonoToStereo(new Int16Array([1, -2, 3])))).toEqual([1, 1, -2, -2, 3, 3]);
	});
});

describe("frame and duration arithmetic", () => {
	test("accounts for exact 20ms Opus frames", () => {
		expect(opusFrameCountForDuration(0)).toBe(0);
		expect(opusFrameCountForDuration(OPUS_FRAME_DURATION_MS)).toBe(1);
		expect(opusFrameCountForDuration(OPUS_FRAME_DURATION_MS + 1)).toBe(2);
		expect(opusDurationMsForFrameCount(3)).toBe(60);
	});

	test("derives duration and ceiling frame count from PCM samples", () => {
		expect(pcmDurationMs(PCM_DISCORD_SAMPLE_RATE_HZ / 2, PCM_DISCORD_SAMPLE_RATE_HZ)).toBe(500);
		expect(pcmFrameInfo(960, PCM_DISCORD_SAMPLE_RATE_HZ)).toEqual({
			sampleCount: 960,
			sampleRateHz: PCM_DISCORD_SAMPLE_RATE_HZ,
			durationMs: 20,
			frameDurationMs: OPUS_FRAME_DURATION_MS,
			frameCount: 1,
		});
	});
});
