/**
 * Pure PCM16 conversion helpers for the Discord voice path.
 *
 * Discord's raw playback path is 48 kHz stereo while the verified TTS format is
 * 24 kHz mono. Resampling uses linear interpolation and repeats the final sample
 * when needed so the output has exactly twice the input sample count.
 */

export const PCM16_MIN = -32_768;
export const PCM16_MAX = 32_767;
export const PCM_TTS_SAMPLE_RATE_HZ = 24_000;
export const PCM_DISCORD_SAMPLE_RATE_HZ = 48_000;
export const OPUS_FRAME_DURATION_MS = 20;
export const OPUS_FRAME_SAMPLES_48KHZ = (PCM_DISCORD_SAMPLE_RATE_HZ * OPUS_FRAME_DURATION_MS) / 1_000;

export interface PcmFrameInfo {
	readonly sampleCount: number;
	readonly sampleRateHz: number;
	readonly durationMs: number;
	readonly frameDurationMs: number;
	readonly frameCount: number;
}

export type PcmSamples = readonly number[] | Int16Array;

function assertFinite(value: number, name: string): void {
	if (!Number.isFinite(value)) throw new RangeError(`${name} must be finite`);
}

function assertNonNegativeInteger(value: number, name: string): void {
	if (!Number.isInteger(value) || value < 0) throw new RangeError(`${name} must be a non-negative integer`);
}

function assertPositive(value: number, name: string): void {
	if (!Number.isFinite(value) || value <= 0) throw new RangeError(`${name} must be positive`);
}

/** Clips and rounds a sample to the representable signed PCM16 range. */
export function clampPcm16Sample(sample: number): number {
	assertFinite(sample, "sample");
	return Math.min(PCM16_MAX, Math.max(PCM16_MIN, Math.round(sample)));
}

/** Decodes little-endian signed 16-bit samples. */
export function decodePcm16Le(bytes: Uint8Array): Int16Array {
	if (bytes.byteLength % 2 !== 0) throw new RangeError("PCM16LE data must contain an even number of bytes");
	const samples = new Int16Array(bytes.byteLength / 2);
	const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
	for (let index = 0; index < samples.length; index += 1) {
		samples[index] = view.getInt16(index * 2, true);
	}
	return samples;
}

/** Encodes samples as little-endian signed 16-bit PCM, clipping before encoding. */
export function encodePcm16Le(samples: PcmSamples): Uint8Array {
	const bytes = new Uint8Array(samples.length * 2);
	const view = new DataView(bytes.buffer);
	for (let index = 0; index < samples.length; index += 1) {
		view.setInt16(index * 2, clampPcm16Sample(samples[index] ?? 0), true);
	}
	return bytes;
}

/**
 * Resamples 24 kHz mono samples to 48 kHz mono samples using linear interpolation.
 * The final input sample is repeated for the final output slot to preserve the
 * exact two-to-one frame count and duration relationship.
 */
export function resamplePcm24kTo48k(samples: PcmSamples): Int16Array {
	if (samples.length === 0) return new Int16Array(0);
	const output = new Int16Array(samples.length * 2);
	for (let outputIndex = 0; outputIndex < output.length; outputIndex += 1) {
		const sourcePosition = outputIndex / 2;
		const leftIndex = Math.floor(sourcePosition);
		if (leftIndex >= samples.length - 1) {
			output[outputIndex] = clampPcm16Sample(samples[samples.length - 1] ?? 0);
			continue;
		}
		const fraction = sourcePosition - leftIndex;
		const left = samples[leftIndex] ?? 0;
		const right = samples[leftIndex + 1] ?? left;
		output[outputIndex] = clampPcm16Sample(left + (right - left) * fraction);
	}
	return output;
}

/** Duplicates each mono sample into left/right channels in interleaved order. */
export function interleaveMonoToStereo(samples: PcmSamples): Int16Array {
	const output = new Int16Array(samples.length * 2);
	for (let index = 0; index < samples.length; index += 1) {
		const sample = clampPcm16Sample(samples[index] ?? 0);
		output[index * 2] = sample;
		output[index * 2 + 1] = sample;
	}
	return output;
}

/** Converts a PCM sample count to its exact duration in milliseconds. */
export function pcmDurationMs(sampleCount: number, sampleRateHz: number): number {
	assertNonNegativeInteger(sampleCount, "sampleCount");
	assertPositive(sampleRateHz, "sampleRateHz");
	return (sampleCount * 1_000) / sampleRateHz;
}

/** Returns the number of fixed-duration Opus frames needed to cover a duration. */
export function opusFrameCountForDuration(durationMs: number, frameDurationMs = OPUS_FRAME_DURATION_MS): number {
	assertFinite(durationMs, "durationMs");
	if (durationMs < 0) throw new RangeError("durationMs must be non-negative");
	assertPositive(frameDurationMs, "frameDurationMs");
	return Math.ceil(durationMs / frameDurationMs);
}

/** Converts a consumed Opus frame count into playback duration. */
export function opusDurationMsForFrameCount(frameCount: number, frameDurationMs = OPUS_FRAME_DURATION_MS): number {
	assertNonNegativeInteger(frameCount, "frameCount");
	assertPositive(frameDurationMs, "frameDurationMs");
	return frameCount * frameDurationMs;
}

/**
 * Derives playback frame accounting from a PCM buffer. Partial frames count as a
 * frame because the playback layer must account for the packet that carries them.
 */
export function pcmFrameInfo(
	sampleCount: number,
	sampleRateHz: number,
	frameDurationMs = OPUS_FRAME_DURATION_MS,
): PcmFrameInfo {
	const durationMs = pcmDurationMs(sampleCount, sampleRateHz);
	return {
		sampleCount,
		sampleRateHz,
		durationMs,
		frameDurationMs,
		frameCount: opusFrameCountForDuration(durationMs, frameDurationMs),
	};
}
