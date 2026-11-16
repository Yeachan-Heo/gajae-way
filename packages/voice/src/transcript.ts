/**
 * The transcript contract every chat adapter shares.
 *
 * A voice message reaches the persona as its attachment line plus one
 * transcript line. The line has to read the same whichever platform the audio
 * came from: the persona and the stored history learn one convention, and a
 * Slack clip and a Discord voice message saying the same words are the same
 * fact.
 */

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
 * Four outcomes rather than `string | undefined`, because "the speaker said X",
 * "the clip held no speech", "the provider was unavailable", and "no usable
 * transcription exists" are different facts. The persona acts differently on
 * each, so collapsing them would make stored history lie by omission.
 */
export type TranscriptResult =
	| { readonly kind: "speech"; readonly text: string }
	| {
			readonly kind: "non-speech";
			/** The transcriber's own description, tags stripped: `mumbling`, `노크 소리`. */
			readonly label: string;
			readonly language?: string;
			readonly languageProbability?: number;
	  }
	| {
			readonly kind: "unavailable";
			/** Safe provider code only; the detailed diagnostic stays in operator logs. */
			readonly reason: string;
	  };

/**
 * Reads the transcription outcome out of a transcription response.
 *
 * Both Scribe and OpenAI-compatible `/audio/transcriptions` (`response_format=json`)
 * answer with a top-level `text`; Scribe additionally reports the detected
 * language. Kept separate and exported because the shape is the one part a
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
 * Speech is appended bare, as the speaker's words. Non-speech and provider
 * unavailability are appended in the adapter's own bracketed style, so they read
 * unambiguously as runtime observations rather than anything the author said.
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
	if (result.kind === "unavailable") return `[transcription unavailable · ${result.reason}]`;
	const parts = ["no speech", result.label];
	if (result.language !== undefined && result.languageProbability !== undefined)
		parts.push(`detected ${result.language} p=${round2(result.languageProbability)}`);
	return `[${parts.join(" · ")}]`;
}

/**
 * Formats a voice-message length for the `[voice message · 4s · …]` line.
 * Sub-minute clips read in seconds because that is how the chat clients label
 * them; longer ones get m:ss.
 */
export function formatVoiceDuration(seconds: number): string {
	if (seconds < 60) return `${Math.round(seconds * 10) / 10}s`;
	const minutes = Math.floor(seconds / 60);
	const rest = Math.floor(seconds % 60);
	return `${minutes}:${String(rest).padStart(2, "0")}`;
}

function round2(value: number): number {
	return Math.round(value * 100) / 100;
}
