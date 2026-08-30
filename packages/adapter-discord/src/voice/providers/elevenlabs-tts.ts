/** ElevenLabs Flash v2.5 stream-input TTS provider for Discord voice. */

import type { TtsChunk, TtsProvider } from "@gajaeway/voice-core";
import { TtsAlignmentMissingError } from "@gajaeway/voice-core";
import type { VoiceConfig } from "../../config";

/** Provider contract endpoint; the fixed voice id is part of this path. */
const ELEVENLABS_TTS_ENDPOINT = "wss://api.elevenlabs.io/v1/text-to-speech";
/** Provider contract default inactivity timeout; this is not an operator tuning knob. */
const ELEVENLABS_TTS_DEFAULT_INACTIVITY_TIMEOUT_SECS = 20;
/** Provider contract maximum inactivity timeout; ElevenLabs rejects larger values. */
const ELEVENLABS_TTS_MAX_INACTIVITY_TIMEOUT_SECS = 180;
/** Provider contract default output format; it is MP3 unless pcm_24000 is sent explicitly. */
const ELEVENLABS_TTS_DEFAULT_OUTPUT_FORMAT = "mp3";
/** The stream-input protocol requires a single blank initialization text frame. */
const INITIALIZATION_TEXT = " ";
/** The stream-input protocol uses an empty text frame to close the input. */
const CLOSE_INPUT_TEXT = "";

export const ELEVENLABS_TTS_PROVIDER_CONTRACT = {
	defaultInactivityTimeoutSecs: ELEVENLABS_TTS_DEFAULT_INACTIVITY_TIMEOUT_SECS,
	maxInactivityTimeoutSecs: ELEVENLABS_TTS_MAX_INACTIVITY_TIMEOUT_SECS,
	defaultOutputFormat: ELEVENLABS_TTS_DEFAULT_OUTPUT_FORMAT,
	requiredOutputFormat: "pcm_24000",
} as const;

/** Optional clock boundary retained for parity with the STT provider's construction shape. */
export interface ElevenLabsTtsClock {
	readonly now: () => number;
}

/** Narrow socket boundary used by the provider and by the runtime's WebSocket adapter. */
export interface ElevenLabsTtsSocket {
	send(payload: string): void;
	close(): void | Promise<void>;
	onMessage(listener: (payload: unknown) => void): void;
	onError(listener: (error: unknown) => void): void;
	onClose?(listener: (reason?: unknown) => void): void;
}

export interface ElevenLabsTtsSocketFactoryOptions {
	readonly url: string;
	readonly headers: Readonly<Record<string, string>>;
	readonly query: Readonly<Record<string, string | readonly string[]>>;
}

export type ElevenLabsTtsSocketFactory = (
	options: ElevenLabsTtsSocketFactoryOptions,
) => ElevenLabsTtsSocket | Promise<ElevenLabsTtsSocket>;

export interface ElevenLabsTtsProviderOptions {
	readonly config: VoiceConfig;
	readonly apiKey: string;
	readonly socketFactory: ElevenLabsTtsSocketFactory;
	readonly clock?: ElevenLabsTtsClock;
}

export type ElevenLabsTtsErrorKind = "retryable" | "fatal";

/** A classified provider failure; unknown provider codes are fatal and fail closed. */
export class ElevenLabsTtsProviderError extends Error {
	readonly kind: ElevenLabsTtsErrorKind;
	readonly code: string;
	readonly providerCode: string;

	constructor(kind: ElevenLabsTtsErrorKind, code: string, message: string, providerCode = code) {
		super(message);
		this.name = "ElevenLabsTtsProviderError";
		this.kind = kind;
		this.code = code;
		this.providerCode = providerCode;
	}
}

const RETRYABLE_CODES: Readonly<Record<string, string>> = {
	rate_limited: "rate_limited",
	commit_throttled: "commit_throttled",
	queue_overflow: "queue_overflow",
	transcriber_error: "transcriber_error",
	session_time_limit_exceeded: "session_time_limit_exceeded",
	insufficient_audio_activity: "insufficient_audio_activity",
	network: "network",
};

const FATAL_CODES: Readonly<Record<string, string>> = {
	auth: "auth",
	auth_error: "auth",
	quota_exceeded: "quota_exceeded",
	unaccepted_terms: "unaccepted_terms",
	resource_exhausted: "resource_exhausted",
	invalid_request: "invalid_request",
	input_error: "input_error",
	chunk_size_exceeded: "chunk_size_exceeded",
};

type WireRecord = Record<string, unknown>;

type QueueWaiter = {
	readonly resolve: (result: IteratorResult<TtsChunk>) => void;
	readonly reject: (reason: unknown) => void;
};

class TtsChunkQueue {
	readonly #items: TtsChunk[] = [];
	readonly #waiters: QueueWaiter[] = [];
	#completed = false;
	#failure: unknown;

	push(chunk: TtsChunk): void {
		if (this.#completed) return;
		const waiter = this.#waiters.shift();
		if (waiter !== undefined) {
			waiter.resolve({ done: false, value: chunk });
			return;
		}
		this.#items.push(chunk);
	}

	complete(): void {
		if (this.#completed) return;
		this.#completed = true;
		while (this.#waiters.length > 0) {
			const waiter = this.#waiters.shift();
			waiter?.resolve({ done: true, value: undefined });
		}
	}

	fail(error: unknown): void {
		if (this.#completed) return;
		this.#completed = true;
		this.#items.length = 0;
		this.#failure = error;
		while (this.#waiters.length > 0) {
			const waiter = this.#waiters.shift();
			waiter?.reject(error);
		}
	}

	next(): Promise<IteratorResult<TtsChunk>> {
		const item = this.#items.shift();
		if (item !== undefined) return Promise.resolve({ done: false, value: item });
		if (this.#failure !== undefined) return Promise.reject(this.#failure);
		if (this.#completed) return Promise.resolve({ done: true, value: undefined });
		return new Promise<IteratorResult<TtsChunk>>((resolve, reject) => {
			this.#waiters.push({ resolve, reject });
		});
	}
}

/** The v1 ElevenLabs Flash TTS implementation. */
export class ElevenLabsTtsProvider implements TtsProvider {
	readonly voiceId: string;
	readonly outputFormat: "pcm_24000";
	readonly #config: VoiceConfig;
	readonly #apiKey: string;
	readonly #socketFactory: ElevenLabsTtsSocketFactory;

	constructor(options: ElevenLabsTtsProviderOptions) {
		this.#config = options.config;
		this.#apiKey = options.apiKey;
		this.#socketFactory = options.socketFactory;
		this.voiceId = options.config.elevenlabs.tts.voiceId;
		this.outputFormat = options.config.elevenlabs.tts.outputFormat;
		void options.clock;
	}

	synthesize(text: string, signal: AbortSignal): AsyncIterable<TtsChunk> {
		return this.#synthesize(text, signal);
	}

	async *#synthesize(text: string, signal: AbortSignal): AsyncIterable<TtsChunk> {
		validateSynthesisInput(text, this.#apiKey, this.#config);
		if (signal.aborted) throw createAbortError();

		const queue = new TtsChunkQueue();
		let socket: ElevenLabsTtsSocket | undefined;
		let closeRequested = false;
		let closePromise: Promise<void> | undefined;
		let ended = false;
		let abortError: Error | undefined;

		const closeSocket = (): Promise<void> => {
			if (closePromise !== undefined) return closePromise;
			closeRequested = true;
			if (socket === undefined) return Promise.resolve();
			try {
				closePromise = Promise.resolve(socket.close()).catch(() => undefined);
			} catch {
				closePromise = Promise.resolve();
			}
			return closePromise;
		};

		const fail = (error: unknown): void => {
			if (ended) return;
			ended = true;
			queue.fail(error);
			void closeSocket();
		};

		const abort = (): void => {
			if (abortError !== undefined || ended) return;
			abortError = createAbortError();
			ended = true;
			queue.fail(abortError);
			void closeSocket();
		};

		const onAbort = (): void => abort();
		signal.addEventListener("abort", onAbort, { once: true });

		try {
			const query = buildQuery(this.#config);
			const socketOptions: ElevenLabsTtsSocketFactoryOptions = {
				url: buildUrl(this.voiceId, query),
				headers: { "xi-api-key": this.#apiKey },
				query,
			};
			try {
				socket = await this.#socketFactory(socketOptions);
			} catch (error) {
				if (abortError !== undefined) throw abortError;
				throw safeError("Unable to establish ElevenLabs TTS session", error, this.#apiKey);
			}
			if (abortError !== undefined || signal.aborted) {
				abort();
				throw abortError ?? createAbortError();
			}

			try {
				socket.onMessage((payload) => {
					if (ended) return;
					try {
						this.#handleMessage(payload, queue, () => {
							if (ended) return;
							ended = true;
							queue.push({ kind: "final" });
							queue.complete();
							void closeSocket();
						});
					} catch (error) {
						if (error instanceof TtsAlignmentMissingError || error instanceof ElevenLabsTtsProviderError) {
							fail(error);
							return;
						}
						fail(safeError("ElevenLabs TTS message failed", error, this.#apiKey));
					}
				});
				socket.onError((error) => {
					if (ended) return;
					fail(
						new ElevenLabsTtsProviderError(
							"retryable",
							"network",
							`ElevenLabs TTS network error: ${sanitize(errorDetail(error), this.#apiKey)}`,
						),
					);
				});
				socket.onClose?.((reason) => {
					if (ended || closeRequested) return;
					const closeCode = closeReasonCode(reason);
					const retryableCode = RETRYABLE_CODES[closeCode] ?? "network";
					fail(
						new ElevenLabsTtsProviderError(
							"retryable",
							retryableCode,
							`ElevenLabs TTS socket closed: ${sanitize(closeCode, this.#apiKey)}`,
							closeCode,
						),
					);
				});
			} catch (error) {
				throw safeError("Unable to initialize ElevenLabs TTS session", error, this.#apiKey);
			}

			if (signal.aborted) {
				abort();
				throw abortError ?? createAbortError();
			}

			try {
				if (!ended) socket.send(JSON.stringify(initializationMessage(this.#config)));
				if (!ended) socket.send(JSON.stringify(textMessage(text)));
				if (!ended) socket.send(JSON.stringify({ text: CLOSE_INPUT_TEXT }));
				if (abortError !== undefined) throw abortError;
			} catch (error) {
				if (error instanceof Error && error.name === "AbortError") throw error;
				fail(
					new ElevenLabsTtsProviderError(
						"retryable",
						"network",
						`ElevenLabs TTS network error: ${sanitize(errorDetail(error), this.#apiKey)}`,
					),
				);
			}

			while (true) {
				const next = await queue.next();
				if (next.done) return;
				yield next.value;
			}
		} finally {
			signal.removeEventListener("abort", onAbort);
			ended = true;
			await closeSocket();
		}
	}

	#handleMessage(payload: unknown, queue: TtsChunkQueue, onFinal: () => void): void {
		const record = parseWireRecord(payload);
		if (record === undefined) {
			throw classifyError("invalid_request", "ElevenLabs TTS returned an invalid message.", this.#apiKey);
		}
		if (record.isFinal === true || record.is_final === true) {
			onFinal();
			return;
		}

		const messageType = stringField(record, "message_type");
		if (messageType === "error" || messageType === "error_message") {
			throw providerError(record, messageType, this.#apiKey);
		}
		if (
			messageType !== undefined &&
			(RETRYABLE_CODES[messageType] !== undefined || FATAL_CODES[messageType] !== undefined)
		) {
			throw providerError(record, messageType, this.#apiKey);
		}
		const explicitCode = stringField(record, "code") ?? stringField(record, "error_code");
		if (explicitCode !== undefined && record.audio === undefined) {
			throw providerError(record, messageType ?? explicitCode, this.#apiKey);
		}
		if (record.error !== undefined && record.audio === undefined) {
			throw providerError(record, messageType ?? "error", this.#apiKey);
		}

		if (record.audio === undefined) {
			throw classifyError(
				messageType ?? "invalid_request",
				`ElevenLabs TTS message omitted audio or isFinal: ${messageType ?? "unknown"}`,
				this.#apiKey,
			);
		}
		const audio = decodeAudio(record.audio);
		if (audio === undefined) {
			throw classifyError("invalid_request", "ElevenLabs TTS audio was not valid base64.", this.#apiKey);
		}
		const alignment = extractAlignment(record);
		if (alignment === undefined) throw new TtsAlignmentMissingError();
		queue.push({
			kind: "audio",
			audio,
			chars: alignment.chars,
			charStartMs: alignment.charStartMs,
			charDurationMs: alignment.charDurationMs,
		});
	}
}

interface AlignmentData {
	readonly chars: readonly string[];
	readonly charStartMs: readonly number[];
	readonly charDurationMs: readonly number[];
}

function validateSynthesisInput(text: string, apiKey: string, config: VoiceConfig): void {
	if (apiKey.trim() === "") throw new Error("ElevenLabs API key is empty.");
	if (config.elevenlabs.tts.voiceId.trim() === "") throw new Error("ElevenLabs TTS voiceId is empty.");
	if (!Number.isInteger(config.elevenlabs.tts.inactivityTimeoutSecs)) {
		throw new Error("ElevenLabs TTS inactivityTimeoutSecs must be an integer.");
	}
	if (
		config.elevenlabs.tts.inactivityTimeoutSecs < 1 ||
		config.elevenlabs.tts.inactivityTimeoutSecs > ELEVENLABS_TTS_MAX_INACTIVITY_TIMEOUT_SECS
	) {
		throw new Error(
			`ElevenLabs TTS inactivityTimeoutSecs must be between 1 and ${ELEVENLABS_TTS_MAX_INACTIVITY_TIMEOUT_SECS} seconds.`,
		);
	}
	if (typeof text !== "string") throw new Error("ElevenLabs TTS text must be a string.");
}

function buildQuery(config: VoiceConfig): Readonly<Record<string, string | readonly string[]>> {
	const tts = config.elevenlabs.tts;
	return {
		model_id: tts.model,
		output_format: tts.outputFormat,
		inactivity_timeout: String(tts.inactivityTimeoutSecs),
		sync_alignment: String(tts.syncAlignment),
		apply_text_normalization: tts.applyTextNormalization,
	};
}

function buildUrl(voiceId: string, query: Readonly<Record<string, string | readonly string[]>>): string {
	const params = new URLSearchParams();
	for (const [key, value] of Object.entries(query)) {
		if (typeof value === "string") {
			params.set(key, value);
			continue;
		}
		for (const entry of value) params.append(key, entry);
	}
	return `${ELEVENLABS_TTS_ENDPOINT}/${encodeURIComponent(voiceId)}/stream-input?${params.toString()}`;
}

function initializationMessage(config: VoiceConfig): WireRecord {
	return {
		text: INITIALIZATION_TEXT,
		generation_config: { chunk_length_schedule: config.elevenlabs.tts.chunkLengthSchedule },
	};
}

function textMessage(text: string): WireRecord {
	return { text: text.endsWith(" ") ? text : `${text} `, flush: true };
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
	if (payload instanceof Uint8Array) return new TextDecoder().decode(payload);
	if (payload instanceof ArrayBuffer) return new TextDecoder().decode(new Uint8Array(payload));
	if (isRecord(payload) && "data" in payload) return extractMessageData(payload.data);
	return payload;
}

function isRecord(value: unknown): value is WireRecord {
	return (
		typeof value === "object" &&
		value !== null &&
		!(value instanceof Uint8Array) &&
		!(value instanceof ArrayBuffer) &&
		!Array.isArray(value)
	);
}

function stringField(record: WireRecord, key: string): string | undefined {
	const value = record[key];
	return typeof value === "string" ? value : undefined;
}

function extractAlignment(record: WireRecord): AlignmentData | undefined {
	for (const value of [record.alignment, record.normalizedAlignment, record.normalized_alignment]) {
		const alignment = parseAlignment(value);
		if (alignment !== undefined) return alignment;
	}
	return undefined;
}

function parseAlignment(value: unknown): AlignmentData | undefined {
	if (!isRecord(value)) return undefined;
	const charsValue = value.chars;
	const startsValue = value.charStartTimesMs;
	const durationsValue = value.charDurationsMs;
	if (!Array.isArray(charsValue) || !Array.isArray(startsValue) || !Array.isArray(durationsValue)) return undefined;
	if (
		charsValue.length === 0 ||
		charsValue.length !== startsValue.length ||
		charsValue.length !== durationsValue.length
	) {
		return undefined;
	}
	const chars: string[] = [];
	const charStartMs: number[] = [];
	const charDurationMs: number[] = [];
	for (let index = 0; index < charsValue.length; index += 1) {
		const char = charsValue[index];
		const startMs = startsValue[index];
		const durationMs = durationsValue[index];
		if (
			typeof char !== "string" ||
			typeof startMs !== "number" ||
			typeof durationMs !== "number" ||
			!Number.isFinite(startMs) ||
			!Number.isFinite(durationMs) ||
			startMs < 0 ||
			durationMs < 0
		) {
			return undefined;
		}
		chars.push(char);
		charStartMs.push(startMs);
		charDurationMs.push(durationMs);
	}
	return { chars, charStartMs, charDurationMs };
}

function decodeAudio(value: unknown): Uint8Array | undefined {
	if (typeof value !== "string") return undefined;
	try {
		return new Uint8Array(Buffer.from(value, "base64"));
	} catch {
		return undefined;
	}
}

function providerError(record: WireRecord, messageType: string, apiKey: string): ElevenLabsTtsProviderError {
	const rawCode = resolveErrorCode(record, messageType);
	const detail = messageDetail(record, rawCode, apiKey);
	return classifyError(rawCode, detail, apiKey);
}

function classifyError(rawCode: string, detail: string, apiKey: string): ElevenLabsTtsProviderError {
	const retryableCode = RETRYABLE_CODES[rawCode];
	if (retryableCode !== undefined) {
		return new ElevenLabsTtsProviderError("retryable", retryableCode, sanitize(detail, apiKey), rawCode);
	}
	const fatalCode = FATAL_CODES[rawCode];
	if (fatalCode !== undefined) {
		return new ElevenLabsTtsProviderError("fatal", fatalCode, sanitize(detail, apiKey), rawCode);
	}
	return new ElevenLabsTtsProviderError(
		"fatal",
		"invalid_request",
		`Unrecognized ElevenLabs TTS error code "${sanitize(rawCode, apiKey)}": ${sanitize(detail, apiKey)}`,
		rawCode,
	);
}

function resolveErrorCode(record: WireRecord, messageType: string): string {
	const explicitCode = stringField(record, "code") ?? stringField(record, "error_code");
	if (explicitCode !== undefined) return explicitCode;
	const nestedError = record.error;
	if (isRecord(nestedError)) {
		return stringField(nestedError, "code") ?? stringField(nestedError, "status") ?? messageType;
	}
	if (messageType === "error") {
		const errorValue = stringField(record, "error");
		if (errorValue !== undefined) return errorValue;
	}
	return messageType;
}

function messageDetail(record: WireRecord, fallback: string, apiKey: string): string {
	const direct = stringField(record, "message") ?? stringField(record, "detail");
	if (direct !== undefined) return sanitize(direct, apiKey);
	const error = record.error;
	if (typeof error === "string") return sanitize(error, apiKey);
	if (isRecord(error)) {
		const nested = stringField(error, "message") ?? stringField(error, "detail");
		if (nested !== undefined) return sanitize(nested, apiKey);
	}
	return sanitize(fallback, apiKey);
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

function createAbortError(): Error {
	const error = new Error("ElevenLabs TTS synthesis aborted.");
	error.name = "AbortError";
	return error;
}
