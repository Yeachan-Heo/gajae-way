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
): Promise<string | undefined> {
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
 * Reads the transcript out of a Scribe response.
 *
 * Kept separate and exported because the shape is the one part of this module a
 * provider change would break, and a unit test on it is worth more than a mock
 * of the whole call.
 */
export function readTranscript(payload: unknown): string | undefined {
	if (typeof payload !== "object" || payload === null) return undefined;
	const text = (payload as { text?: unknown }).text;
	if (typeof text !== "string") return undefined;
	const trimmed = text.trim();
	return trimmed === "" ? undefined : trimmed;
}

/**
 * Appends a transcript to an inbound body.
 *
 * The transcript goes after the attachment line so the body reads
 * "here is the thing, here is what it said", and the url stays first because it
 * is the original and the transcript is a derivative.
 */
export function withTranscript(body: string, transcript: string | undefined): string {
	if (!transcript) return body;
	return body === "" ? transcript : `${body}\n${transcript}`;
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
