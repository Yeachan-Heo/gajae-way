/**
 * Voice-message transcription for inbound Discord messages.
 *
 * A Discord voice message carries no text, so after the attachment renderer
 * landed the persona received only `[voice message · 4s · <url>]` and had to
 * fetch and transcribe the audio itself, by hand, on every single turn. That
 * burns tokens and wall-clock for something mechanical, and it leaves the
 * conversation history unreadable: scrolling back shows a url, not what was
 * said. The owner's instruction was explicit — handle it at the runtime level,
 * always, not in the persona.
 *
 * So the adapter transcribes on ingress and injects the text into the body it
 * forwards. The url stays on its own line, because the transcript is lossy and
 * the audio is the original.
 *
 * Everything here fails open. Speech-to-text is an outbound network call to a
 * third party with its own quota and outages; a voice message must still arrive
 * — with its url and nothing else — when transcription is unconfigured, denied,
 * slow, or broken. Losing the message would be a worse bug than losing the
 * transcript, and losing the message is exactly what this subsystem replaced.
 *
 * The CDN url is signed and expires in roughly a day, so transcription has to
 * happen on ingress. There is no later.
 */

/** ElevenLabs Scribe endpoint. Overridable so tests never touch the network. */
const DEFAULT_STT_ENDPOINT = "https://api.elevenlabs.io/v1/speech-to-text";

/**
 * `scribe_v1` with no `language_code`. Auto-detection was measured at p=1.0 on
 * Korean speech, and the owner's servers mix Korean and English, so pinning a
 * language would only ever be wrong in the mixed case.
 */
const DEFAULT_STT_MODEL = "scribe_v1";

/** Beyond this the turn is waiting on a third party; the url alone is better. */
const DEFAULT_TIMEOUT_MS = 20_000;

/** Discord caps voice messages well below this; the guard is against a hostile url. */
const MAX_AUDIO_BYTES = 25 * 1024 * 1024;

export interface VoiceTranscriptionConfig {
	readonly apiKey: string;
	readonly endpoint?: string;
	readonly model?: string;
	/** Pinned only when set; absent means auto-detect, which is the default. */
	readonly languageCode?: string;
	readonly timeoutMs?: number;
}

export interface TranscriptionPorts {
	readonly fetch: typeof fetch;
	/** Diagnostics sink; transcription failures are logged, never thrown. */
	readonly log?: (message: string) => void;
}

/**
 * Fetches and transcribes one voice-message attachment.
 *
 * Returns undefined on every failure path rather than throwing, so the caller
 * can forward the message unchanged. A returned string is always non-empty:
 * silence transcribes to `""`, which carries no information and would render as
 * a stray blank line.
 */
export async function transcribeVoiceMessage(
	url: string,
	config: VoiceTranscriptionConfig,
	ports: TranscriptionPorts,
): Promise<TranscriptResult | undefined> {
	const timeoutMs = config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
	try {
		const audio = await withTimeout(timeoutMs, (signal) => ports.fetch(url, { signal }));
		if (!audio.ok) {
			ports.log?.(`voice transcription skipped: attachment fetch failed with ${audio.status}`);
			return undefined;
		}
		const bytes = await audio.arrayBuffer();
		if (bytes.byteLength === 0 || bytes.byteLength > MAX_AUDIO_BYTES) {
			ports.log?.(`voice transcription skipped: attachment size ${bytes.byteLength} out of range`);
			return undefined;
		}
		const form = new FormData();
		form.set("model_id", config.model ?? DEFAULT_STT_MODEL);
		if (config.languageCode) form.set("language_code", config.languageCode);
		form.set("file", new Blob([bytes], { type: "audio/ogg" }), "voice-message.ogg");
		const response = await withTimeout(timeoutMs, (signal) =>
			ports.fetch(config.endpoint ?? DEFAULT_STT_ENDPOINT, {
				method: "POST",
				headers: { "xi-api-key": config.apiKey },
				body: form,
				signal,
			}),
		);
		if (!response.ok) {
			ports.log?.(`voice transcription failed: speech-to-text returned ${response.status}`);
			return undefined;
		}
		return readTranscript(await response.json());
	} catch (error) {
		ports.log?.(`voice transcription failed: ${error instanceof Error ? error.message : String(error)}`);
		return undefined;
	}
}

/**
 * Non-speech audio events, which Scribe reports as a bracketed tag instead of
 * words: `[mumbling]`, `[노크 소리]`, `(laughs)`, `[BLANK_AUDIO]`.
 *
 * These describe the recording; they are not things the speaker said, and the
 * wording is the transcriber's rather than the speaker's. Passing one through as
 * a transcript attributes it to the author — a knock on a desk would enter the
 * permanent history as the owner having said "[노크 소리]".
 *
 * Dropping it silently is equally wrong: "this clip contains no speech" is a
 * real fact about the message, and the persona needs it to decide whether to
 * respond, ask, or ignore. So it is neither attributed nor discarded — it is
 * reported as what it is.
 *
 * Matched structurally rather than by keyword, because the tag vocabulary is the
 * provider's and is localized: any transcript consisting only of bracketed or
 * parenthesized runs is an event report, not speech.
 */
const NON_SPEECH_TAG = /^(?:\s*(?:\[[^\]]*\]|\([^)]*\)))+\s*$/;

/**
 * What a transcription attempt actually produced.
 *
 * Three outcomes rather than `string | undefined`, because "the speaker said X",
 * "the clip held no speech" and "transcription did not happen" are different
 * facts and the persona acts differently on each. Collapsing the middle case
 * into either neighbour is what made the first version of this wrong.
 */
export type TranscriptResult =
	| { readonly kind: "speech"; readonly text: string }
	| {
			readonly kind: "non-speech";
			/** The transcriber's own description, tags stripped: `mumbling`, `노크 소리`. */
			readonly label: string;
			readonly language?: string;
			readonly languageProbability?: number;
	  };

/**
 * Reads the transcription outcome out of a Scribe response.
 *
 * Kept separate and exported because the shape is the one part of this module a
 * provider change would break, and a unit test on it is worth more than a mock
 * of the whole call. Returns undefined only when there is no usable answer at
 * all — a malformed payload, or silence, which the attachment line already
 * conveys on its own.
 */
export function readTranscript(payload: unknown): TranscriptResult | undefined {
	if (typeof payload !== "object" || payload === null) return undefined;
	const record = payload as { text?: unknown; language_code?: unknown; language_probability?: unknown };
	if (typeof record.text !== "string") return undefined;
	const trimmed = record.text.trim();
	if (trimmed === "") return undefined;
	if (!NON_SPEECH_TAG.test(trimmed)) return { kind: "speech", text: trimmed };
	return {
		kind: "non-speech",
		label: stripTags(trimmed),
		// Detection confidence is the diagnostic that matters here: a non-speech
		// result at p=0.27 means auto-detection gave up, which is actionable in a
		// way that the same result at p=1.0 is not.
		...(typeof record.language_code === "string" ? { language: record.language_code } : {}),
		...(typeof record.language_probability === "number" ? { languageProbability: record.language_probability } : {}),
	};
}

/** Unwraps `[a] (b)` to `a, b` so the label reads as prose inside our own brackets. */
function stripTags(tagged: string): string {
	const labels = [...tagged.matchAll(/\[([^\]]*)\]|\(([^)]*)\)/g)]
		.map((match) => (match[1] ?? match[2] ?? "").trim())
		.filter((label) => label !== "");
	return labels.length > 0 ? labels.join(", ") : tagged;
}

/**
 * Appends the transcription outcome to an inbound body.
 *
 * Speech is appended bare, as the speaker's words. A non-speech result is
 * appended in the adapter's own bracketed style, the same shape as the
 * attachment line above it, so it reads unambiguously as the runtime reporting
 * on the recording rather than as anything the author said.
 */
export function withTranscript(body: string, result: TranscriptResult | undefined): string {
	const line = renderTranscript(result);
	if (line === undefined) return body;
	return body === "" ? line : `${body}\n${line}`;
}

/** The body line for a transcription outcome, or undefined when there is none. */
export function renderTranscript(result: TranscriptResult | undefined): string | undefined {
	if (!result) return undefined;
	if (result.kind === "speech") return result.text;
	const parts = ["no speech", result.label];
	if (result.language !== undefined && result.languageProbability !== undefined)
		parts.push(`detected ${result.language} p=${round2(result.languageProbability)}`);
	return `[${parts.join(" · ")}]`;
}

function round2(value: number): number {
	return Math.round(value * 100) / 100;
}

/** Rejects rather than hanging a turn on a third party that stopped answering. */
async function withTimeout(timeoutMs: number, run: (signal: AbortSignal) => Promise<Response>): Promise<Response> {
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), timeoutMs);
	try {
		return await run(controller.signal);
	} finally {
		clearTimeout(timer);
	}
}
