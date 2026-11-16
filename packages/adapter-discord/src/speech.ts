/**
 * Outbound text-to-speech for Discord voice replies.
 *
 * The owner's instruction: when *his* turn is a voice message, the reply must
 * arrive as voice **and** text, and the persona must write that reply exactly
 * once — the runtime does the synthesis and the second send. Doing it in the
 * persona meant a hand-run pipeline on every single turn, which is what this
 * module deletes.
 *
 * Discord will not carry both in one message: a voice message
 * (`flags: IsVoiceMessage`) must have empty `content`. So a voice reply is two
 * messages, text first — history stays readable without playing audio, which is
 * the whole point of pairing them.
 *
 * Everything here fails open. Text is the deliverable; voice is the courtesy.
 * A synthesis outage, a missing key, a blown quota or a decoder that is not
 * installed must never stop the words from arriving.
 *
 * Kept free of any `discord.js` import so the rules stay unit testable without
 * the gateway client's dependency tree.
 */

import { classifyProviderError, DEFAULT_MAX_SPOKEN_CHARS, speakable } from "@gajae-gateway/voice";

/** ElevenLabs returns Ogg/Opus directly at this format — no transcoding step. */
const DEFAULT_OUTPUT_FORMAT = "opus_48000_64";
const DEFAULT_VOICE_ID = "pNInz6obpgDQGcFmaJgB";
const DEFAULT_MODEL = "eleven_multilingual_v2";
const DEFAULT_TTS_ENDPOINT = "https://api.elevenlabs.io/v1/text-to-speech";
const DEFAULT_TIMEOUT_MS = 30_000;
/**
 * Playback speed. The owner asked for faster delivery, and 1.2 is the provider's
 * ceiling — measured: the same line runs 3.77s at 1.0 and 2.56s at 1.2, and 1.3
 * is rejected outright. Faster is also cheaper in listening time, not in quota:
 * billing is per character, so speed costs nothing.
 */
const DEFAULT_SPEED = 1.2;
/** Provider-enforced bounds; outside them the request is a validation error. */
export const MIN_SPEED = 0.7;
export const MAX_SPEED = 1.2;
/** Opus in Ogg is always timestamped at 48 kHz regardless of the input rate. */
const OPUS_GRANULE_RATE = 48_000;
const WAVEFORM_BUCKETS = 64;

export interface SpeechConfig {
	readonly apiKey: string;
	readonly voiceId?: string;
	readonly model?: string;
	readonly outputFormat?: string;
	readonly endpoint?: string;
	readonly maxSpokenChars?: number;
	readonly timeoutMs?: number;
	/** Playback speed, clamped to the provider's 0.7-1.2 range. */
	readonly speed?: number;
}

export interface SpeechPorts {
	readonly fetch: typeof globalThis.fetch;
	/**
	 * Optional PCM decoder used only to draw the waveform bar. Absent means a
	 * flat bar, which is cosmetic — never a reason to skip the voice message.
	 */
	readonly decodePcm?: (ogg: Uint8Array) => Promise<Int16Array | undefined>;
	readonly log?: (message: string) => void;
}

export interface SynthesizedVoice {
	readonly ogg: Uint8Array;
	readonly seconds: number;
	/** Base64 byte array, 0-255 per bucket, as Discord's waveform field wants. */
	readonly waveform: string;
}

/**
 * Synthesizes one voice reply, or undefined when it cannot be produced.
 *
 * Undefined is a normal outcome, not an error: the caller has already sent the
 * text, so the only thing lost is the audio.
 */
export async function synthesizeVoice(
	text: string,
	config: SpeechConfig,
	ports: SpeechPorts,
): Promise<SynthesizedVoice | undefined> {
	const spoken = speakable(text, config.maxSpokenChars ?? DEFAULT_MAX_SPOKEN_CHARS);
	if (spoken === "") return undefined;
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), config.timeoutMs ?? DEFAULT_TIMEOUT_MS);
	try {
		const base = config.endpoint ?? DEFAULT_TTS_ENDPOINT;
		const voiceId = config.voiceId ?? DEFAULT_VOICE_ID;
		const format = config.outputFormat ?? DEFAULT_OUTPUT_FORMAT;
		const response = await ports.fetch(`${base}/${voiceId}?output_format=${format}`, {
			method: "POST",
			headers: { "xi-api-key": config.apiKey, "Content-Type": "application/json" },
			body: JSON.stringify({
				text: spoken,
				model_id: config.model ?? DEFAULT_MODEL,
				// Clamped rather than passed through: an out-of-range speed is a 400
				// from the provider, which would cost the whole voice reply over a
				// config typo.
				voice_settings: { speed: clampSpeed(config.speed ?? DEFAULT_SPEED) },
			}),
			signal: controller.signal,
		});
		if (!response.ok) {
			const failure = await classifyProviderError(response);
			ports.log?.(`voice synthesis failed: text-to-speech ${failure.diagnostic}`);
			return undefined;
		}
		const ogg = new Uint8Array(await response.arrayBuffer());
		if (ogg.byteLength === 0) {
			ports.log?.("voice synthesis failed: text-to-speech returned an empty body");
			return undefined;
		}
		const seconds = oggOpusDuration(ogg);
		if (seconds === undefined) {
			ports.log?.("voice synthesis skipped: could not read a duration from the audio");
			return undefined;
		}
		return { ogg, seconds, waveform: await waveformFor(ogg, ports) };
	} catch (error) {
		ports.log?.(`voice synthesis failed: ${error instanceof Error ? error.message : String(error)}`);
		return undefined;
	} finally {
		clearTimeout(timer);
	}
}

/** Keeps a configured speed inside the provider's accepted range. */
export function clampSpeed(speed: number): number {
	if (!Number.isFinite(speed)) return DEFAULT_SPEED;
	return Math.min(MAX_SPEED, Math.max(MIN_SPEED, speed));
}

/**
 * Reads the duration out of an Ogg stream's final granule position.
 *
 * Deliberately dependency-free: Discord requires `duration_secs` on a voice
 * message, so needing an external decoder just to state the length would make
 * the whole feature contingent on ffmpeg being installed. The last page's
 * granule position is the total sample count at Opus's fixed 48 kHz clock.
 */
export function oggOpusDuration(ogg: Uint8Array): number | undefined {
	let granule: number | undefined;
	// Scan for "OggS" page headers; the last one carries the end-of-stream granule.
	for (let offset = 0; offset + 27 <= ogg.byteLength; offset++) {
		if (ogg[offset] !== 0x4f || ogg[offset + 1] !== 0x67 || ogg[offset + 2] !== 0x67 || ogg[offset + 3] !== 0x53)
			continue;
		let value = 0;
		// 64-bit little-endian, read as a float: audio lengths are far inside 2^53.
		for (let byte = 7; byte >= 0; byte--) value = value * 256 + (ogg[offset + 6 + byte] as number);
		granule = value;
		offset += 26;
	}
	if (granule === undefined || granule <= 0) return undefined;
	return Math.round((granule / OPUS_GRANULE_RATE) * 1000) / 1000;
}

/**
 * Peak amplitude per bucket, or a flat bar when no decoder is available.
 *
 * The waveform is the drawing under the play button. Discord accepts any byte
 * array, so a missing decoder costs appearance and nothing else — which is why
 * this never fails the send.
 */
async function waveformFor(ogg: Uint8Array, ports: SpeechPorts): Promise<string> {
	if (ports.decodePcm) {
		try {
			const pcm = await ports.decodePcm(ogg);
			if (pcm && pcm.length > 0) return encodeWaveform(peaks(pcm));
		} catch (error) {
			ports.log?.(`voice waveform unavailable: ${error instanceof Error ? error.message : String(error)}`);
		}
	}
	return encodeWaveform(new Uint8Array(WAVEFORM_BUCKETS).fill(128));
}

function peaks(pcm: Int16Array): Uint8Array {
	const out = new Uint8Array(WAVEFORM_BUCKETS);
	for (let bucket = 0; bucket < WAVEFORM_BUCKETS; bucket++) {
		const start = Math.floor((bucket * pcm.length) / WAVEFORM_BUCKETS);
		const end = Math.max(start + 1, Math.floor(((bucket + 1) * pcm.length) / WAVEFORM_BUCKETS));
		let peak = 0;
		for (let index = start; index < end && index < pcm.length; index++) {
			const magnitude = Math.abs(pcm[index] as number);
			if (magnitude > peak) peak = magnitude;
		}
		out[bucket] = Math.min(255, Math.round((peak / 32_768) * 255));
	}
	return out;
}

function encodeWaveform(bytes: Uint8Array): string {
	let binary = "";
	for (const byte of bytes) binary += String.fromCharCode(byte);
	return btoa(binary);
}
