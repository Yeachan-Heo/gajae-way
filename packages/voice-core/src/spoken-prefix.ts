/**
 * Converts provider chunk-relative TTS alignment into a single absolute character timeline.
 *
 * The caller supplies the duration actually consumed by the player. This module never
 * estimates playback from bytes written, so truncation remains exact at character edges.
 */

import type { TtsChunk } from "./ports";

export interface AbsoluteAlignmentPoint {
	readonly index: number;
	readonly char: string;
	readonly startMs: number;
	readonly durationMs: number;
	readonly endMs: number;
}

export interface AlignmentTimeline {
	readonly points: readonly AbsoluteAlignmentPoint[];
	readonly text: string;
	readonly durationMs: number;
}

export interface SpokenPrefixResult {
	readonly charIndex: number;
	readonly spokenPrefix: string;
	readonly remainder: string;
}

function validateAudioChunk(chunk: Extract<TtsChunk, { readonly kind: "audio" }>): void {
	if (chunk.chars.length !== chunk.charStartMs.length || chunk.chars.length !== chunk.charDurationMs.length) {
		throw new RangeError("TTS alignment arrays must have equal lengths");
	}
}

/** Accumulates each audio chunk's relative alignment into absolute offsets. */
export function accumulateAlignment(chunks: readonly TtsChunk[]): AlignmentTimeline {
	const points: AbsoluteAlignmentPoint[] = [];
	let chunkOffsetMs = 0;
	for (const chunk of chunks) {
		if (chunk.kind === "final") continue;
		validateAudioChunk(chunk);
		let chunkDurationMs = 0;
		for (let index = 0; index < chunk.chars.length; index += 1) {
			const startMs = chunk.charStartMs[index];
			const durationMs = chunk.charDurationMs[index];
			if (
				startMs === undefined ||
				durationMs === undefined ||
				!Number.isFinite(startMs) ||
				!Number.isFinite(durationMs) ||
				startMs < 0 ||
				durationMs < 0
			) {
				throw new RangeError("TTS alignment offsets must be finite and non-negative");
			}
			const endMs = startMs + durationMs;
			points.push({
				index: points.length,
				char: chunk.chars[index] ?? "",
				startMs: chunkOffsetMs + startMs,
				durationMs,
				endMs: chunkOffsetMs + endMs,
			});
			chunkDurationMs = Math.max(chunkDurationMs, endMs);
		}
		chunkOffsetMs += chunkDurationMs;
	}
	return {
		points,
		text: points.map((point) => point.char).join(""),
		durationMs: chunkOffsetMs,
	};
}

/**
 * Selects only characters whose aligned audio has fully ended at truncationMs. A timestamp
 * exactly at a character start leaves that character in the remainder; a timestamp at the
 * end of the final character includes the complete timeline.
 */
export function truncateSpokenTimeline(timeline: AlignmentTimeline, truncationMs: number): SpokenPrefixResult {
	if (!Number.isFinite(truncationMs)) throw new RangeError("truncationMs must be finite");
	const cutMs = Math.max(0, truncationMs);
	let charIndex = 0;
	while (charIndex < timeline.points.length) {
		const point = timeline.points[charIndex];
		if (point === undefined || point.endMs > cutMs) break;
		charIndex += 1;
	}
	const spokenPrefix = timeline.points
		.slice(0, charIndex)
		.map((point) => point.char)
		.join("");
	const remainder = timeline.points
		.slice(charIndex)
		.map((point) => point.char)
		.join("");
	return { charIndex, spokenPrefix, remainder };
}

/** Accumulates alignment and immediately computes the consumed prefix. */
export function spokenPrefixFromChunks(chunks: readonly TtsChunk[], truncationMs: number): SpokenPrefixResult {
	return truncateSpokenTimeline(accumulateAlignment(chunks), truncationMs);
}
