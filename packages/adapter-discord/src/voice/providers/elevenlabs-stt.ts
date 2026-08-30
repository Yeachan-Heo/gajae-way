import type {
	Admission,
	SttCommitReason,
	SttEvent,
	SttFatalCode,
	SttOpenOptions,
	SttProvider,
	SttRetryableCode,
	SttStream,
} from "@gajaeway/voice-core";
import { deriveVoiceVadKnobs, type VoiceConfig } from "../../config";

/** Provider contract endpoint; this is not a configurable behavior knob. */
const ELEVENLABS_STT_ENDPOINT = "wss://api.elevenlabs.io/v1/speech-to-text/realtime";
/** Provider contract message type; this is not a configurable behavior knob. */
const INPUT_AUDIO_CHUNK_MESSAGE = "input_audio_chunk";
/** Provider contract message type; this is not a configurable behavior knob. */
const SESSION_STARTED_MESSAGE = "session_started";
/** Provider contract message type; this is not a configurable behavior knob. */
const PARTIAL_TRANSCRIPT_MESSAGE = "partial_transcript";
/** Provider contract message type; this is not a configurable behavior knob. */
const COMMITTED_TRANSCRIPT_MESSAGE = "committed_transcript";
/** Provider contract message type; this is not a configurable behavior knob. */
const COMMITTED_TRANSCRIPT_WITH_TIMESTAMPS_MESSAGE = "committed_transcript_with_timestamps";
/** Provider contract message type; this is not a configurable behavior knob. */
const WARNING_MESSAGE = "warning";
/** Provider contract message type; this is not a configurable behavior knob. */
const ERROR_MESSAGE = "error";
/** Provider contract value for a commit-only audio chunk; this is not a tunable. */
const EMPTY_AUDIO_BASE64 = "";
/** Provider contract timing (not tunable): transcription processing starts after about two seconds of audio. */
const PROVIDER_TRANSCRIPTION_START_DELAY_MS = 2_000;
/** Provider contract timing (not tunable): an uncommitted stream auto-commits near thirty-six seconds. */
const PROVIDER_AUTO_COMMIT_AUDIO_MS = 36_000;
/** Provider contract guidance (not tunable): rapid successive manual commits can degrade transcription quality. */
const PROVIDER_MANUAL_COMMIT_QUALITY_WARNING = "Rapid successive manual commits can degrade transcription quality.";
/** Provider contract error code (not tunable): the socket may end when the session time limit is reached. */
const PROVIDER_SESSION_TIME_LIMIT_ERROR = "session_time_limit_exceeded";
/** Provider contract audio rate for the pcm_16000 format (not configurable). */
const PROVIDER_PCM_16000_SAMPLE_RATE_HZ = 16_000;
export const ELEVENLABS_STT_PROVIDER_CONTRACT = {
	transcriptionStartDelayMs: PROVIDER_TRANSCRIPTION_START_DELAY_MS,
	autoCommitAudioMs: PROVIDER_AUTO_COMMIT_AUDIO_MS,
	manualCommitQualityWarning: PROVIDER_MANUAL_COMMIT_QUALITY_WARNING,
	sessionTimeLimitError: PROVIDER_SESSION_TIME_LIMIT_ERROR,
	pcm16000SampleRateHz: PROVIDER_PCM_16000_SAMPLE_RATE_HZ,
} as const;

/**
 * The clock is injected so event timestamps do not depend on a wall clock in tests.
 * It intentionally matches the voice session clock shape without importing the session.
 */
export interface ElevenLabsSttClock {
	readonly now: () => number;
	readonly setTimeout: (callback: () => void, delayMs: number) => unknown;
	readonly clearTimeout: (handle: unknown) => void;
}

/** Narrow socket boundary used by the provider and by the runtime's WebSocket adapter. */
export interface ElevenLabsSttSocket {
	send(payload: string): void;
	close(): void | Promise<void>;
	onMessage(listener: (payload: unknown) => void): void;
	onError(listener: (error: unknown) => void): void;
	onClose?(listener: (reason?: unknown) => void): void;
}

export interface ElevenLabsSttSocketFactoryOptions {
	readonly url: string;
	readonly headers: Readonly<Record<string, string>>;
	readonly query: Readonly<Record<string, string | readonly string[]>>;
}

export type ElevenLabsSttSocketFactory = (
	options: ElevenLabsSttSocketFactoryOptions,
) => ElevenLabsSttSocket | Promise<ElevenLabsSttSocket>;

export interface ElevenLabsSttProviderOptions {
	readonly config: VoiceConfig;
	readonly apiKey: string;
	readonly socketFactory: ElevenLabsSttSocketFactory;
	readonly clock?: ElevenLabsSttClock;
}

const RETRYABLE_CODES: Readonly<Record<string, SttRetryableCode>> = {
	rate_limited: "rate_limited",
	commit_throttled: "commit_throttled",
	queue_overflow: "queue_overflow",
	transcriber_error: "transcriber_error",
	[PROVIDER_SESSION_TIME_LIMIT_ERROR]: PROVIDER_SESSION_TIME_LIMIT_ERROR,

	insufficient_audio_activity: "insufficient_audio_activity",
	network: "network",
};

const FATAL_CODES: Readonly<Record<string, SttFatalCode>> = {
	auth: "auth",
	auth_error: "auth",
	quota_exceeded: "quota_exceeded",
	unaccepted_terms: "unaccepted_terms",
	resource_exhausted: "resource_exhausted",
	invalid_request: "invalid_request",
	input_error: "input_error",
	chunk_size_exceeded: "chunk_size_exceeded",
};

const defaultClock: ElevenLabsSttClock = {
	now: () => Date.now(),
	setTimeout: (callback, delayMs) => setTimeout(callback, delayMs),
	clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

interface AudioOutbound {
	readonly kind: "audio";
	readonly pcm16: Uint8Array;
}

interface CommitOutbound {
	readonly kind: "commit";
	readonly reason: SttCommitReason;
}

type Outbound = AudioOutbound | CommitOutbound;

type WireRecord = Record<string, unknown>;

/** The v1 ElevenLabs Scribe realtime STT implementation. */
export class ElevenLabsSttProvider implements SttProvider {
	readonly supportsStreamingPartials: true = true;
	readonly #config: VoiceConfig;
	readonly #apiKey: string;
	readonly #socketFactory: ElevenLabsSttSocketFactory;
	readonly #clock: ElevenLabsSttClock;

	constructor(options: ElevenLabsSttProviderOptions) {
		this.#config = options.config;
		this.#apiKey = options.apiKey;
		this.#socketFactory = options.socketFactory;
		this.#clock = options.clock ?? defaultClock;
	}

	async open(opts: SttOpenOptions, sink: (event: SttEvent) => void): Promise<SttStream> {
		validateOpenOptions(opts);
		if (this.#apiKey.trim() === "") throw new Error("ElevenLabs API key is empty.");

		const query = buildQuery(this.#config);
		const socketOptions: ElevenLabsSttSocketFactoryOptions = {
			url: buildUrl(query),
			headers: { "xi-api-key": this.#apiKey },
			query,
		};

		let socket: ElevenLabsSttSocket;
		try {
			socket = await this.#socketFactory(socketOptions);
		} catch (error) {
			throw safeError("Unable to establish ElevenLabs STT session", error, this.#apiKey);
		}

		const stream = new ElevenLabsSttStream({
			options: opts,
			sink,
			socket,
			clock: this.#clock,
			includeLanguageDetection: this.#config.elevenlabs.stt.includeLanguageDetection,
			apiKey: this.#apiKey,
		});
		try {
			stream.attach();
		} catch (error) {
			await stream.close("open_error");
			throw safeError("Unable to initialize ElevenLabs STT session", error, this.#apiKey);
		}
		return stream;
	}
}

interface ElevenLabsSttStreamOptions {
	readonly options: SttOpenOptions;
	readonly sink: (event: SttEvent) => void;
	readonly socket: ElevenLabsSttSocket;
	readonly clock: ElevenLabsSttClock;
	readonly includeLanguageDetection: boolean;
	readonly apiKey: string;
}

class ElevenLabsSttStream implements SttStream {
	readonly #options: SttOpenOptions;
	readonly #sink: (event: SttEvent) => void;
	readonly #socket: ElevenLabsSttSocket;
	readonly #clock: ElevenLabsSttClock;
	readonly #includeLanguageDetection: boolean;
	readonly #apiKey: string;
	readonly #queue: Outbound[] = [];
	#queuedAudioFrames = 0;
	#droppedAudioFrames = 0;
	#flushScheduled = false;
	#flushing = false;
	#previousTextPending: boolean;
	#closed = false;
	#socketCloseRequested = false;
	#failureSignaled = false;
	#closePromise: Promise<void> | undefined;

	constructor(options: ElevenLabsSttStreamOptions) {
		this.#options = options.options;
		this.#sink = options.sink;
		this.#socket = options.socket;
		this.#clock = options.clock;
		this.#includeLanguageDetection = options.includeLanguageDetection;
		this.#apiKey = options.apiKey;
		this.#previousTextPending = options.options.previousText !== undefined;
	}

	get queuedFrames(): number {
		return this.#queuedAudioFrames;
	}

	get droppedFrames(): number {
		return this.#droppedAudioFrames;
	}

	attach(): void {
		this.#socket.onMessage((payload) => this.#handleMessage(payload));
		this.#socket.onError((error) => this.#handleSocketError(error));
		this.#socket.onClose?.((reason) => this.#handleSocketClose(reason));
	}

	push(pcm16: Uint8Array): Admission {
		if (this.#closed || this.#socketCloseRequested) return "rejected_closed";

		const frame = new Uint8Array(pcm16);
		let admission: Admission = "accepted";
		if (this.#queuedAudioFrames >= this.#options.maxQueuedFrames) {
			const oldestAudioIndex = this.#queue.findIndex((item) => item.kind === "audio");
			if (oldestAudioIndex >= 0) {
				this.#queue.splice(oldestAudioIndex, 1);
				this.#queuedAudioFrames -= 1;
				this.#droppedAudioFrames += 1;
				admission = "dropped_oldest";
			}
		}
		this.#queue.push({ kind: "audio", pcm16: frame });
		this.#queuedAudioFrames += 1;
		this.#scheduleFlush();
		return admission;
	}

	commit(reason: SttCommitReason): void {
		if (this.#closed || this.#socketCloseRequested) return;
		this.#queue.push({ kind: "commit", reason });
		this.#scheduleFlush();
	}

	async close(_reason: string): Promise<void> {
		if (this.#closePromise !== undefined) {
			await this.#closePromise;
			return;
		}
		this.#flush();
		this.#closed = true;
		this.#queue.length = 0;
		this.#queuedAudioFrames = 0;
		this.#closePromise = this.#closeSocket();
		await this.#closePromise;
	}

	#scheduleFlush(): void {
		if (this.#flushScheduled || this.#closed || this.#socketCloseRequested) return;
		this.#flushScheduled = true;
		queueMicrotask(() => {
			this.#flushScheduled = false;
			this.#flush();
		});
	}

	#flush(): void {
		if (this.#flushing || this.#closed || this.#socketCloseRequested) return;
		this.#flushing = true;
		try {
			while (this.#queue.length > 0 && !this.#closed && !this.#socketCloseRequested) {
				const item = this.#queue.shift();
				if (item === undefined) break;
				if (item.kind === "audio") this.#queuedAudioFrames -= 1;
				try {
					this.#socket.send(JSON.stringify(this.#wireMessage(item)));
				} catch (error) {
					this.#queue.length = 0;
					this.#queuedAudioFrames = 0;
					this.#handleSocketError(error);
					break;
				}
			}
		} finally {
			this.#flushing = false;
		}
	}

	#wireMessage(item: Outbound): WireRecord {
		if (item.kind === "commit") {
			return {
				message_type: INPUT_AUDIO_CHUNK_MESSAGE,
				audio_base_64: EMPTY_AUDIO_BASE64,
				commit: true,
				sample_rate: this.#options.sampleRateHz,
			};
		}

		const message: WireRecord = {
			message_type: INPUT_AUDIO_CHUNK_MESSAGE,
			audio_base_64: Buffer.from(item.pcm16).toString("base64"),
			commit: false,
			sample_rate: this.#options.sampleRateHz,
		};
		if (this.#previousTextPending) {
			this.#previousTextPending = false;
			if (this.#options.previousText !== undefined) message.previous_text = this.#options.previousText;
		}
		return message;
	}

	#handleMessage(payload: unknown): void {
		if (this.#closed) return;
		const record = parseWireRecord(payload);
		if (record === undefined) {
			this.#signalFatal("invalid_request", "ElevenLabs STT returned an invalid message.");
			return;
		}
		const messageType = stringField(record, "message_type");
		if (messageType === undefined) {
			this.#signalFatal("invalid_request", "ElevenLabs STT message omitted message_type.");
			return;
		}

		switch (messageType) {
			case SESSION_STARTED_MESSAGE:
			case "session_open": {
				const sessionId = stringField(record, "session_id") ?? stringField(record, "sessionId");
				if (sessionId === undefined || sessionId.length === 0) {
					this.#signalFatal("invalid_request", "ElevenLabs STT session-open message omitted session_id.");
					return;
				}
				this.#emit({ kind: "ready", sessionId });
				return;
			}
			case PARTIAL_TRANSCRIPT_MESSAGE: {
				const text = stringField(record, "text");
				if (text === undefined) {
					this.#signalFatal("invalid_request", "ElevenLabs STT partial transcript omitted text.");
					return;
				}
				this.#emit({ kind: "partial", text, atMs: this.#clock.now() });
				return;
			}
			case COMMITTED_TRANSCRIPT_MESSAGE:
			case COMMITTED_TRANSCRIPT_WITH_TIMESTAMPS_MESSAGE: {
				const text = stringField(record, "text");
				if (text === undefined) {
					this.#signalFatal("invalid_request", "ElevenLabs STT committed transcript omitted text.");
					return;
				}
				const languageCode = this.#includeLanguageDetection
					? (stringField(record, "language_code") ?? stringField(record, "languageCode"))
					: undefined;
				const event: SttEvent = {
					kind: "committed",
					text,
					atMs: this.#clock.now(),
					...(languageCode === undefined ? {} : { languageCode }),
				};
				this.#emit(event);
				return;
			}
			case WARNING_MESSAGE: {
				const code = stringField(record, "code") ?? WARNING_MESSAGE;
				const message = this.#messageDetail(record, code);
				this.#emit({ kind: "warning", code, message });
				return;
			}
			case ERROR_MESSAGE:
			case "auth_error":
			case "auth":
			case "rate_limited":
			case "commit_throttled":
			case "queue_overflow":
			case "transcriber_error":
			case PROVIDER_SESSION_TIME_LIMIT_ERROR:
			case "insufficient_audio_activity":
			case "quota_exceeded":
			case "unaccepted_terms":
			case "resource_exhausted":
			case "invalid_request":
			case "input_error":
			case "chunk_size_exceeded":
			case "network": {
				this.#signalProviderError(record, messageType);
				return;
			}
			case "committed_transcript_entities":
				return;
			default:
				this.#signalFatal(
					"invalid_request",
					`Unrecognized ElevenLabs STT message type "${sanitize(messageType, this.#apiKey)}": ${this.#messageDetail(record, messageType)}`,
				);
		}
	}

	#signalProviderError(record: WireRecord, messageType: string): void {
		if (this.#closed || this.#failureSignaled) return;
		this.#failureSignaled = true;
		const rawCode = resolveErrorCode(record, messageType);

		const retryableCode = RETRYABLE_CODES[rawCode];
		const fatalCode = FATAL_CODES[rawCode];
		const detail = this.#messageDetail(record, rawCode);
		if (retryableCode !== undefined) {
			this.#emit({ kind: "retryable", code: retryableCode, message: detail });
		} else if (fatalCode !== undefined) {
			this.#emit({ kind: "fatal", code: fatalCode, message: detail });
		} else {
			const unknownMessage = `Unrecognized ElevenLabs STT error code "${sanitize(rawCode, this.#apiKey)}": ${detail}`;
			this.#emit({ kind: "fatal", code: "invalid_request", message: unknownMessage });
		}
		this.#queue.length = 0;
		this.#queuedAudioFrames = 0;
		void this.#closeSocket();
	}

	#signalFatal(code: SttFatalCode, message: string): void {
		if (this.#closed || this.#failureSignaled) return;
		this.#failureSignaled = true;
		this.#emit({ kind: "fatal", code, message: sanitize(message, this.#apiKey) });
		this.#queue.length = 0;
		this.#queuedAudioFrames = 0;
		void this.#closeSocket();
	}

	#handleSocketError(error: unknown): void {
		if (this.#closed || this.#failureSignaled) return;
		this.#failureSignaled = true;
		this.#emit({
			kind: "retryable",
			code: "network",
			message: `ElevenLabs STT network error: ${sanitize(errorDetail(error), this.#apiKey)}`,
		});
		this.#queue.length = 0;
		this.#queuedAudioFrames = 0;
		void this.#closeSocket();
	}

	#handleSocketClose(reason: unknown): void {
		if (this.#closed || this.#failureSignaled) return;
		const closeCode = closeReasonCode(reason);
		const retryableCode = RETRYABLE_CODES[closeCode];
		this.#failureSignaled = true;
		this.#emit({
			kind: "retryable",
			code: retryableCode ?? "network",
			message: `ElevenLabs STT socket closed: ${sanitize(closeCode, this.#apiKey)}`,
		});
		this.#queue.length = 0;
		this.#queuedAudioFrames = 0;
		void this.#closeSocket();
	}

	#messageDetail(record: WireRecord, fallback: string): string {
		const detail = stringField(record, "error") ?? stringField(record, "message") ?? stringField(record, "detail");
		return sanitize(detail ?? fallback, this.#apiKey);
	}

	#emit(event: SttEvent): void {
		try {
			this.#sink(event);
		} catch {
			// Sink failures cannot be allowed to escape a provider callback.
		}
	}

	async #closeSocket(): Promise<void> {
		if (this.#socketCloseRequested) return;
		this.#socketCloseRequested = true;
		try {
			await this.#socket.close();
		} catch {
			// Closing is best effort; the caller's lifecycle owner remains authoritative.
		}
	}
}

function buildQuery(config: VoiceConfig): Readonly<Record<string, string | readonly string[]>> {
	const stt = config.elevenlabs.stt;
	const vad = deriveVoiceVadKnobs(config);
	return {
		model_id: stt.model,
		audio_format: stt.audioFormat,
		commit_strategy: stt.commitStrategy,
		vad_silence_threshold_secs: String(vad.vad_silence_threshold_secs),
		min_speech_duration_ms: String(vad.min_speech_duration_ms),
		min_silence_duration_ms: String(vad.min_silence_duration_ms),
		include_language_detection: String(stt.includeLanguageDetection),
		filter_background_audio: String(stt.filterBackgroundAudio),
		keyterms: stt.keyterms,
	};
}

function buildUrl(query: Readonly<Record<string, string | readonly string[]>>): string {
	const params = new URLSearchParams();
	for (const [key, value] of Object.entries(query)) {
		if (typeof value === "string") {
			params.set(key, value);
			continue;
		}
		for (const entry of value) params.append(key, entry);
	}
	return `${ELEVENLABS_STT_ENDPOINT}?${params.toString()}`;
}

function validateOpenOptions(options: SttOpenOptions): void {
	if (options.speakerId.trim() === "") throw new Error("ElevenLabs STT speakerId is empty.");
	if (options.sampleRateHz !== PROVIDER_PCM_16000_SAMPLE_RATE_HZ) {
		throw new Error("ElevenLabs STT sampleRateHz must match pcm_16000.");
	}
	if (!Number.isSafeInteger(options.maxQueuedFrames) || options.maxQueuedFrames <= 0) {
		throw new Error("ElevenLabs STT maxQueuedFrames must be a positive integer.");
	}
}

function parseWireRecord(payload: unknown): WireRecord | undefined {
	const data = extractMessageData(payload);
	if (isRecord(data)) return data;
	if (typeof data !== "string") return undefined;
	try {
		const parsed: unknown = JSON.parse(data);
		return isRecord(parsed) ? parsed : undefined;
	} catch {
		return undefined;
	}
}

function extractMessageData(payload: unknown): unknown {
	if (typeof payload !== "object" || payload === null) return payload;
	if ("data" in payload) return payload.data;
	if (payload instanceof Uint8Array) return new TextDecoder().decode(payload);
	if (payload instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(payload));
	return payload;
}

function isRecord(value: unknown): value is WireRecord {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function stringField(record: WireRecord, key: string): string | undefined {
	const value = record[key];
	return typeof value === "string" ? value : undefined;
}

function resolveErrorCode(record: WireRecord, messageType: string): string {
	const explicitCode = stringField(record, "code") ?? stringField(record, "error_code");
	if (explicitCode !== undefined) return explicitCode;
	if (messageType === ERROR_MESSAGE) {
		const errorValue = stringField(record, "error");
		if (errorValue !== undefined) return errorValue;
	}
	return messageType;
}

function closeReasonCode(reason: unknown): string {
	if (typeof reason === "string" && reason.length > 0) return reason;
	if (isRecord(reason)) return stringField(reason, "code") ?? stringField(reason, "reason") ?? "network";
	return "network";
}

function errorDetail(error: unknown): string {
	if (error instanceof Error) return error.message || "unknown error";
	if (typeof error === "string") return error;
	return "unknown error";
}

function sanitize(value: string, apiKey: string): string {
	if (apiKey.length === 0) return value;
	return value.split(apiKey).join("[redacted]");
}

function safeError(prefix: string, error: unknown, apiKey: string): Error {
	return new Error(`${prefix}: ${sanitize(errorDetail(error), apiKey)}`);
}
