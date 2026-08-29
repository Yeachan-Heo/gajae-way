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

/** ElevenLabs returns Ogg/Opus directly at this format — no transcoding step. */
const DEFAULT_OUTPUT_FORMAT = "opus_48000_64";
const DEFAULT_VOICE_ID = "pNInz6obpgDQGcFmaJgB";
const DEFAULT_MODEL = "eleven_multilingual_v2";
const DEFAULT_TTS_ENDPOINT = "https://api.elevenlabs.io/v1/text-to-speech";
/**
 * Spoken length cap. Text is always complete; the voice is a convenience, and
 * reading a long report aloud burns character quota that cannot even be
 * inspected (the key's scope excludes `user_read`).
 */
const DEFAULT_MAX_SPOKEN_CHARS = 600;
const DEFAULT_TIMEOUT_MS = 30_000;
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
 * Reduces a reply to something worth listening to.
 *
 * A spoken reply is not a rendered document: urls, code fences, table pipes and
 * heading markers are noise read aloud and cost quota per character. Removing
 * them is not cosmetic — a url alone can be a third of a short reply.
 */
export function speakable(text: string, maxChars: number = DEFAULT_MAX_SPOKEN_CHARS): string {
	const stripped = text
		// Fenced code first: its contents must not survive as loose lines.
		.replace(/```[\s\S]*?```/g, " ")
		.replace(/`([^`]*)`/g, "$1")
		// A markdown link keeps its label and loses its target.
		.replace(/\[([^\]]*)\]\((?:[^)]*)\)/g, "$1")
		.replace(/https?:\/\/\S+/g, " ")
		.replace(/^\s*[|>#-]+\s*/gm, " ")
		.replace(/\*\*?([^*]*)\*\*?/g, "$1")
		.replace(/[ \t]+/g, " ")
		.replace(/\s*\n\s*/g, "\n")
		.trim();
	if (stripped.length <= maxChars) return stripped;
	// Cut on a sentence boundary when one is near the cap, so the voice does not
	// stop mid-word. The text message carries the remainder either way.
	const window = stripped.slice(0, maxChars);
	const boundary = Math.max(window.lastIndexOf(". "), window.lastIndexOf("? "), window.lastIndexOf("다. "));
	return (boundary > maxChars * 0.6 ? window.slice(0, boundary + 1) : window).trim();
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
			body: JSON.stringify({ text: spoken, model_id: config.model ?? DEFAULT_MODEL }),
			signal: controller.signal,
		});
		if (!response.ok) {
			ports.log?.(`voice synthesis failed: text-to-speech returned ${response.status}`);
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
