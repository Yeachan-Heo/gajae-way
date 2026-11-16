/**
 * Voice in and out for Slack: speech-to-text on an inbound audio clip, and a
 * spoken copy of the reply uploaded next to its text.
 *
 * The Discord adapter does the same with ElevenLabs; the transcript line, the
 * speakable-text rules and the provider error boundary are shared through
 * `@gajae-gateway/voice` so the persona reads one convention whichever room the
 * audio came from. The transport here is the OpenAI-compatible pair
 * `/audio/transcriptions` and `/audio/speech`, and nothing about the provider
 * is defaulted: endpoints, models and the voice are all named in config.
 *
 * Everything fails open. A clip still reaches the persona as its attachment
 * line when transcription is unconfigured, slow, refused or broken, and a reply
 * is already in the thread as text before synthesis is even attempted.
 */

import type { ChatMessagePayload } from "@gajae-gateway/protocol";
import { classifyProviderError, readTranscript, speakable, type TranscriptResult } from "@gajae-gateway/voice";
import type { SlackFileLike } from "./attachments";

/** Beyond this the turn is waiting on a third party; the attachment line alone is better. */
const DEFAULT_TRANSCRIPTION_TIMEOUT_MS = 20_000;
const DEFAULT_SPEECH_TIMEOUT_MS = 30_000;
/** Slack clips are far below this; the guard is against a hostile or mislabeled upload. */
export const MAX_VOICE_CLIP_BYTES = 25 * 1024 * 1024;
const MAX_REDIRECTS = 3;
const REDIRECT_STATUSES = new Set([301, 302, 303, 307, 308]);
const SLACK_FILE_HOSTS = ["files.slack.com"] as const;
const SLACK_FILE_REDIRECT_HOSTS = ["files.slack.com", "files-edge.slack.com"] as const;
/** Turn ids remembered for picking the speaker's voice; bounded like every other adapter memory. */
const MAX_REMEMBERED_SPEAKERS = 1_000;

export interface SlackTranscriptionConfig {
	readonly endpoint: string;
	readonly model: string;
	readonly apiKey: string;
	/** Pinned only when set; absent lets the model detect the language. */
	readonly language?: string;
	readonly timeoutMs?: number;
}

export interface SlackSpeechConfig {
	readonly endpoint: string;
	readonly model: string;
	readonly apiKey: string;
	/** The voice used when no override matches. */
	readonly voice: string;
	/**
	 * Overrides keyed by Slack user id (`U…`/`W…`) or conversation id
	 * (`C…`/`D…`/`G…`). The speaker's own entry wins over the conversation's.
	 */
	readonly voices: Readonly<Record<string, string>>;
	readonly maxSpokenChars?: number;
	readonly timeoutMs?: number;
}

export interface LoadedSlackVoiceConfig {
	readonly transcription: SlackTranscriptionConfig;
	/** Absent means inbound transcription only; replies stay text. */
	readonly speech?: SlackSpeechConfig;
}

export interface VoicePorts {
	readonly fetch: typeof fetch;
	/** Diagnostics sink; voice failures are logged, never thrown. */
	readonly log?: (message: string) => void;
}

/**
 * A Slack audio clip: the in-app recorder marks it `slack_audio`; an uploaded
 * audio file is recognized by its mime type.
 */
export function isSlackVoiceClip(file: SlackFileLike): boolean {
	if (file.subtype === "slack_audio") return true;
	return typeof file.mimetype === "string" && file.mimetype.toLowerCase().startsWith("audio/");
}

/**
 * The clip to transcribe, when there is one.
 *
 * Only the first: Slack's recorder sends one clip per message, and transcribing
 * every audio file of a hostile message would multiply a third-party call by an
 * attacker-chosen factor.
 */
export function firstSlackVoiceClip(files: readonly SlackFileLike[] | null | undefined): SlackFileLike | undefined {
	return (files ?? []).find(isSlackVoiceClip);
}

/**
 * Downloads and transcribes one clip.
 *
 * Provider failures return an explicit unavailable outcome; download, network
 * and timeout failures return undefined. Neither throws, so the caller always
 * forwards the original message.
 */
export async function transcribeSlackVoiceClip(
	file: SlackFileLike,
	config: SlackTranscriptionConfig,
	botToken: string,
	ports: VoicePorts,
): Promise<TranscriptResult | undefined> {
	const url = nonBlank(file.url_private_download) ?? nonBlank(file.url_private);
	if (!url) {
		ports.log?.("voice transcription skipped: clip has no private url");
		return undefined;
	}
	const timeoutMs = config.timeoutMs ?? DEFAULT_TRANSCRIPTION_TIMEOUT_MS;
	try {
		const audio = await withTimeout(timeoutMs, (signal) => downloadSlackFile(url, botToken, ports.fetch, signal));
		if (audio.kind === "skipped") {
			ports.log?.(`voice transcription skipped: ${audio.reason}`);
			return undefined;
		}
		const mimetype = nonBlank(file.mimetype) ?? "application/octet-stream";
		const form = new FormData();
		form.set("model", config.model);
		if (config.language) form.set("language", config.language);
		// `json` is the portable shape: a `text` field, no provider-specific extras.
		form.set("response_format", "json");
		form.set("file", new Blob([audio.bytes], { type: mimetype }), clipFileName(file, mimetype));
		return await withTimeout(timeoutMs, async (signal) => {
			const response = await ports.fetch(config.endpoint, {
				method: "POST",
				headers: { Authorization: `Bearer ${config.apiKey}` },
				body: form,
				signal,
			});
			if (!response.ok) {
				const failure = await classifyProviderError(response);
				ports.log?.(`voice transcription failed: speech-to-text ${failure.diagnostic}`);
				return { kind: "unavailable", reason: failure.code } as const;
			}
			return readTranscript(await response.json());
		});
	} catch (error) {
		ports.log?.(`voice transcription failed: ${errorText(error)}`);
		return undefined;
	}
}

type Download =
	| { readonly kind: "ok"; readonly bytes: Uint8Array }
	| { readonly kind: "skipped"; readonly reason: string };

/**
 * Fetches a private Slack file with the bot token.
 *
 * Redirects are followed by hand so the token never leaves Slack's file hosts:
 * a hop to any other host is refused, and the header is dropped on a hop to a
 * different Slack host. An HTML body is Slack's sign-in page — a missing
 * `files:read` scope — not audio.
 */
async function downloadSlackFile(
	url: string,
	botToken: string,
	fetcher: typeof fetch,
	signal: AbortSignal,
): Promise<Download> {
	const first = trustedUrl(url, SLACK_FILE_HOSTS);
	if (!first) return { kind: "skipped", reason: "clip url is not a Slack file url" };
	let current: URL = first;
	const origin = current.hostname;
	for (let redirects = 0; ; redirects++) {
		const response = await fetcher(current, {
			method: "GET",
			headers: current.hostname === origin ? { Authorization: `Bearer ${botToken}` } : {},
			redirect: "manual",
			signal,
		});
		if (REDIRECT_STATUSES.has(response.status)) {
			await response.body?.cancel().catch(() => {});
			const location = response.headers.get("location");
			const next =
				location && redirects < MAX_REDIRECTS ? trustedUrl(location, SLACK_FILE_REDIRECT_HOSTS, current) : undefined;
			if (!next) return { kind: "skipped", reason: "clip download redirected off Slack" };
			current = next;
			continue;
		}
		if (!response.ok) {
			await response.body?.cancel().catch(() => {});
			return { kind: "skipped", reason: `clip download failed with ${response.status}` };
		}
		const type = response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
		if (type === "text/html") {
			await response.body?.cancel().catch(() => {});
			return { kind: "skipped", reason: "clip download returned html (check the files:read scope)" };
		}
		const declared = Number(response.headers.get("content-length"));
		if (Number.isFinite(declared) && declared > MAX_VOICE_CLIP_BYTES) {
			await response.body?.cancel().catch(() => {});
			return { kind: "skipped", reason: `clip size ${declared} out of range` };
		}
		const bytes = await readBounded(response, MAX_VOICE_CLIP_BYTES);
		if (!bytes || bytes.byteLength === 0)
			return { kind: "skipped", reason: `clip size ${bytes ? 0 : `>${MAX_VOICE_CLIP_BYTES}`} out of range` };
		return { kind: "ok", bytes };
	}
}

async function readBounded(response: Response, limit: number): Promise<Uint8Array | undefined> {
	if (!response.body) return new Uint8Array(await response.arrayBuffer());
	const reader = response.body.getReader();
	const chunks: Uint8Array[] = [];
	let total = 0;
	try {
		for (;;) {
			const { done, value } = await reader.read();
			if (done) break;
			total += value.byteLength;
			if (total > limit) {
				await reader.cancel().catch(() => {});
				return undefined;
			}
			chunks.push(value);
		}
	} finally {
		reader.releaseLock();
	}
	const bytes = new Uint8Array(total);
	let offset = 0;
	for (const chunk of chunks) {
		bytes.set(chunk, offset);
		offset += chunk.byteLength;
	}
	return bytes;
}

function trustedUrl(value: string, hosts: readonly string[], base?: URL): URL | undefined {
	let url: URL;
	try {
		url = base ? new URL(value, base) : new URL(value);
	} catch {
		return undefined;
	}
	if (url.protocol !== "https:" || url.username !== "" || url.password !== "" || url.port !== "") return undefined;
	return hosts.includes(url.hostname.toLowerCase()) ? url : undefined;
}

/** A provider sniffs the container from the filename as often as from the bytes. */
function clipFileName(file: SlackFileLike, mimetype: string): string {
	const name = nonBlank(file.name);
	if (name && /\.[A-Za-z0-9]{2,5}$/.test(name)) return name;
	const extension =
		{ "audio/webm": "webm", "audio/mp4": "m4a", "audio/x-m4a": "m4a", "audio/mpeg": "mp3", "audio/ogg": "ogg" }[
			mimetype.toLowerCase()
		] ?? "bin";
	return `voice-message.${extension}`;
}

/**
 * The voice for a reply: the speaker's override, else the conversation's, else
 * the default.
 */
export function selectVoice(speech: SlackSpeechConfig, speakerId: string | undefined, channelId: string): string {
	return (speakerId ? speech.voices[speakerId] : undefined) ?? speech.voices[channelId] ?? speech.voice;
}

/** Synthesizes a WAV reply, or undefined when it cannot be produced. Never throws. */
export async function synthesizeSlackSpeech(
	text: string,
	voice: string,
	config: SlackSpeechConfig,
	ports: VoicePorts,
): Promise<Uint8Array | undefined> {
	const spoken = config.maxSpokenChars === undefined ? speakable(text) : speakable(text, config.maxSpokenChars);
	if (spoken === "") return undefined;
	try {
		return await withTimeout(config.timeoutMs ?? DEFAULT_SPEECH_TIMEOUT_MS, async (signal) => {
			const response = await ports.fetch(config.endpoint, {
				method: "POST",
				headers: { Authorization: `Bearer ${config.apiKey}`, "Content-Type": "application/json" },
				body: JSON.stringify({ model: config.model, input: spoken, voice, response_format: "wav" }),
				signal,
			});
			if (!response.ok) {
				const failure = await classifyProviderError(response);
				ports.log?.(`voice synthesis failed: text-to-speech ${failure.diagnostic}`);
				return undefined;
			}
			const wav = new Uint8Array(await response.arrayBuffer());
			// "RIFF": anything else is an error body served with a 200, not audio.
			if (wav.byteLength < 44 || wav[0] !== 0x52 || wav[1] !== 0x49 || wav[2] !== 0x46 || wav[3] !== 0x46) {
				ports.log?.(`voice synthesis failed: text-to-speech returned ${wav.byteLength} bytes that are not WAV`);
				return undefined;
			}
			return wav;
		});
	} catch (error) {
		ports.log?.(`voice synthesis failed: ${errorText(error)}`);
		return undefined;
	}
}

export interface VoiceUploadApi {
	uploadFile(
		channel: string,
		file: { readonly name: string; readonly bytes: Uint8Array },
		threadTs?: string,
	): Promise<void>;
}

/**
 * Outbound voice for Slack replies.
 *
 * Slack has no bot voice messages, so the spoken copy is an uploaded WAV in the
 * same thread, posted after the text. It also remembers who spoke each voice
 * turn: a delivery names its turn but not its author, and the speaker's own
 * voice override outranks the conversation's.
 */
export class SlackVoiceReplies {
	readonly #speakers = new Map<string, string>();

	constructor(
		readonly config: SlackSpeechConfig,
		private readonly ports: VoicePorts,
	) {}

	rememberSpeaker(turnId: string, userId: string): void {
		this.#speakers.delete(turnId);
		this.#speakers.set(turnId, userId);
		if (this.#speakers.size > MAX_REMEMBERED_SPEAKERS)
			this.#speakers.delete(this.#speakers.keys().next().value as string);
	}

	/**
	 * Posts the spoken copy of a delivered reply. Never throws: the text is
	 * already in the thread, so a failure here loses only the courtesy.
	 */
	async reply(
		api: Partial<VoiceUploadApi>,
		message: Pick<ChatMessagePayload, "turnId" | "voiceText">,
		channel: string,
		threadTs: string | undefined,
	): Promise<void> {
		if (!message.voiceText) return;
		try {
			if (!api.uploadFile) throw new Error("this Slack client cannot upload files");
			const speaker = message.turnId === undefined ? undefined : this.#speakers.get(message.turnId);
			const voice = selectVoice(this.config, speaker, channel);
			const wav = await synthesizeSlackSpeech(message.voiceText, voice, this.config, this.ports);
			if (!wav) return;
			await api.uploadFile(channel, { name: "voice-reply.wav", bytes: wav }, threadTs);
		} catch (error) {
			this.ports.log?.(`voice reply failed: ${errorText(error)}`);
		}
	}
}

/** Rejects rather than hanging a turn on a third party that stopped answering. */
async function withTimeout<T>(timeoutMs: number, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
	const controller = new AbortController();
	const timer = setTimeout(() => controller.abort(), timeoutMs);
	try {
		return await run(controller.signal);
	} finally {
		clearTimeout(timer);
	}
}

function nonBlank(value: string | null | undefined): string | undefined {
	return typeof value === "string" && value.trim() !== "" ? value : undefined;
}

function errorText(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
