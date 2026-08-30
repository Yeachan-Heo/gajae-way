/**
 * Provider-agnostic ports for the Discord voice interface.
 *
 * This package has zero runtime dependencies on purpose: nothing here may import
 * `discord.js`, a provider SDK, or any I/O. Discord and provider bindings live in
 * `packages/adapter-discord/src/voice/*` and depend on these types, never the reverse.
 */

/** Streaming STT events, discriminated so callers can route retryable and fatal separately. */
export type SttEvent =
	| { readonly kind: "ready"; readonly sessionId: string }
	| { readonly kind: "partial"; readonly text: string; readonly atMs: number }
	| { readonly kind: "committed"; readonly text: string; readonly atMs: number; readonly languageCode?: string }
	| { readonly kind: "warning"; readonly code: string; readonly message: string }
	| { readonly kind: "retryable"; readonly code: SttRetryableCode; readonly message: string }
	| { readonly kind: "fatal"; readonly code: SttFatalCode; readonly message: string };

export type SttRetryableCode =
	| "rate_limited"
	| "commit_throttled"
	| "queue_overflow"
	| "transcriber_error"
	| "session_time_limit_exceeded"
	| "insufficient_audio_activity"
	| "network";

export type SttFatalCode =
	| "auth"
	| "quota_exceeded"
	| "unaccepted_terms"
	| "resource_exhausted"
	| "invalid_request"
	| "input_error"
	| "chunk_size_exceeded";

/**
 * Ingress admission result. The v1 overflow policy is `drop_oldest` only, so the
 * result union has exactly three members and every configured policy has a
 * representable outcome.
 */
export type Admission = "accepted" | "dropped_oldest" | "rejected_closed";

/** Logical commit boundaries only; the utterance state machine owns turn boundaries. */
export type SttCommitReason = "teardown" | "speaker_left" | "idle";

export interface SttStream {
	push(pcm16: Uint8Array): Admission;
	readonly queuedFrames: number;
	readonly droppedFrames: number;
	commit(reason: SttCommitReason): void;
	close(reason: string): Promise<void>;
}

export interface SttOpenOptions {
	readonly speakerId: string;
	readonly sampleRateHz: number;
	readonly maxQueuedFrames: number;
	/** Set on reconnect only, and only for the first chunk after reopening. */
	readonly previousText?: string;
}

export interface SttProvider {
	readonly supportsStreamingPartials: true;
	open(opts: SttOpenOptions, sink: (event: SttEvent) => void): Promise<SttStream>;
}

/**
 * TTS chunks are a discriminated union and alignment is REQUIRED on audio chunks.
 *
 * Providers open with `sync_alignment=true`. When an audio frame arrives without
 * alignment the provider throws `TtsAlignmentMissingError` and fails that synthesis
 * instead of playing audio whose truncation point could never be reported.
 *
 * `kind: "final"` means the terminal server frame was consumed: the iterable then
 * completes normally and the socket closes exactly once. Abort also closes exactly once.
 */
export type TtsChunk =
	| {
			readonly kind: "audio";
			readonly audio: Uint8Array;
			readonly chars: readonly string[];
			/** Chunk-relative start offsets, accumulated into an absolute timeline by `spoken-prefix`. */
			readonly charStartMs: readonly number[];
			readonly charDurationMs: readonly number[];
	  }
	| { readonly kind: "final" };

export class TtsAlignmentMissingError extends Error {
	constructor(message = "TTS provider returned an audio frame without character alignment") {
		super(message);
		this.name = "TtsAlignmentMissingError";
	}
}

export interface TtsProvider {
	/** Single fixed voice for v1; the voice id is part of the provider endpoint path. */
	readonly voiceId: string;
	/** The only format verified for v1. */
	readonly outputFormat: "pcm_24000";
	synthesize(text: string, signal: AbortSignal): AsyncIterable<TtsChunk>;
}

/**
 * A turn the gateway has already accepted. Busy knowledge about an accepted turn is
 * never evicted and never clamped away; see `turn-gate.ts`.
 */
export interface OutstandingTurn {
	readonly seq: number;
	readonly messageId: string;
	readonly turnId: string;
	readonly modality: "voice" | "text";
	readonly acceptedAtMs: number;
}

/** Both outcomes mean busy. There is deliberately no rejection value. */
export type AdmitOutcome = "tracked" | "saturated";

export interface SettleResult {
	readonly cleared: readonly OutstandingTurn[];
	readonly debtCleared: number;
	readonly unknownTerminal: boolean;
}

export interface SweepResult {
	readonly cleared: readonly OutstandingTurn[];
	readonly debtCleared: number;
}

export interface OriginTurnBookCounters {
	readonly tracked: number;
	readonly saturatedAdmits: number;
	readonly settled: number;
	readonly unknownTerminal: number;
	readonly stale: number;
	readonly debt: number;
}

/**
 * Per-utterance speaker identity, mirroring the gateway inbound engagement fields so a
 * voice turn carries exactly the same speaker header as a text turn.
 */
export interface EngagementContext {
	readonly authorId: string;
	readonly authorName?: string;
	readonly authorHandle?: string;
	readonly channelLabel?: string;
	readonly serverLabel?: string;
}
