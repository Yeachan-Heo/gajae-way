/** Provider-to-Discord TTS orchestration without AudioResource or player ownership. */

import {
	type AlignmentTimeline,
	accumulateAlignment,
	decodePcm16Le,
	encodePcm16Le,
	interleaveMonoToStereo,
	resamplePcm24kTo48k,
	TtsAlignmentMissingError,
	type TtsChunk,
	type TtsProvider,
} from "@gajaeway/voice-core";

export interface TtsSynthesisOptions {
	readonly provider: TtsProvider;
}

export interface TtsSynthesis {
	synthesize(text: string, signal: AbortSignal): Promise<SynthesisResult>;
}

export interface SynthesisSuccess {
	readonly kind: "ok";
	/** Complete 48 kHz stereo signed-16 little-endian PCM payload. */
	readonly audio: Uint8Array;
	/** Absolute alignment timeline accumulated from the provider's chunk-relative frames. */
	readonly timeline: AlignmentTimeline;
	readonly chunks: readonly TtsChunk[];
}

export interface SynthesisTextFallback {
	readonly kind: "text_fallback";
	readonly error: TtsAlignmentMissingError;
}

export type SynthesisResult = SynthesisSuccess | SynthesisTextFallback;

export function createTtsSynthesis(options: TtsSynthesisOptions): TtsSynthesis {
	return {
		synthesize: async (text, signal): Promise<SynthesisResult> => {
			const chunks: TtsChunk[] = [];
			const audioParts: Uint8Array[] = [];
			try {
				for await (const chunk of options.provider.synthesize(text, signal)) {
					chunks.push(chunk);
					if (chunk.kind === "audio") audioParts.push(convertAudio(chunk.audio));
				}
			} catch (error) {
				if (error instanceof TtsAlignmentMissingError) return { kind: "text_fallback", error };
				throw error;
			}
			return {
				kind: "ok",
				audio: concatenate(audioParts),
				timeline: accumulateAlignment(chunks),
				chunks,
			};
		},
	};
}

function convertAudio(audio: Uint8Array): Uint8Array {
	const mono24k = decodePcm16Le(audio);
	const mono48k = resamplePcm24kTo48k(mono24k);
	const stereo48k = interleaveMonoToStereo(mono48k);
	return encodePcm16Le(stereo48k);
}

function concatenate(parts: readonly Uint8Array[]): Uint8Array {
	let totalLength = 0;
	for (const part of parts) totalLength += part.byteLength;
	const result = new Uint8Array(totalLength);
	let offset = 0;
	for (const part of parts) {
		result.set(part, offset);
		offset += part.byteLength;
	}
	return result;
}
