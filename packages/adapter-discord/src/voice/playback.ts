/**
 * Discord voice playback binding.
 *
 * The synthesis provider remains independent of Discord. This module only translates its
 * 24 kHz mono PCM chunks to Discord's raw 48 kHz stereo input, owns one player/resource
 * pair, and reports a cut based on packets the player actually consumed.
 */

import { Readable } from "node:stream";
import { createAudioPlayer, createAudioResource, StreamType } from "@discordjs/voice";
import type { ChatMessagePayload } from "@gajaeway/protocol";
import {
	BargeInGate,
	type BargeInResult,
	decideBargeIn,
	decodePcm16Le,
	encodePcm16Le,
	interleaveMonoToStereo,
	resamplePcm24kTo48k,
	spokenPrefixFromChunks,
	TtsAlignmentMissingError,
	type TtsChunk,
	type TtsProvider,
} from "@gajaeway/voice-core";
import type { VoiceConfig } from "../config";
import type { VoicePlaybackOutcome } from "./router";

/** The clock is supplied so interruption timestamps are deterministic in tests. */
export interface VoicePlaybackClock {
	now(): number;
}

/** A resource exposes only the playback accounting needed by the truncation contract. */
export interface VoicePlaybackResource {
	readonly playbackDuration: number;
}

/** State values are intentionally reduced to the status field used by this lifecycle owner. */
export interface VoicePlaybackPlayerState {
	readonly status?: string;
}

/** Narrow player surface; no Discord player is needed by playback tests. */
export interface VoicePlaybackPlayer {
	play(resource: VoicePlaybackResource): void;
	stop(force?: boolean): boolean | undefined;
	on(event: "error", listener: (error: unknown) => void): this;
	on(
		event: "stateChange",
		listener: (oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void,
	): this;
	off?(event: "error", listener: (error: unknown) => void): this;
	off?(
		event: "stateChange",
		listener: (oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void,
	): this;
}

/** The subscription returned by a voice connection's player subscription. */
export interface VoicePlaybackSubscription {
	unsubscribe?(): void | Promise<void>;
}

export interface VoicePlaybackResourceFactoryOptions {
	readonly inputType: StreamType;
}

export type VoicePlaybackResourceFactory = (
	stream: Readable,
	options: VoicePlaybackResourceFactoryOptions,
) => VoicePlaybackResource;

export type VoicePlaybackPlayerFactory = () => VoicePlaybackPlayer;

export type VoicePlaybackSubscribe = (player: VoicePlaybackPlayer) => VoicePlaybackSubscription | undefined;

/** A transcript candidate supplied by the receiver/STT path while a reply is playing. */
export interface VoicePlaybackBargeInInput {
	readonly audioPassed: boolean;
	readonly transcript: string;
	readonly atMs?: number;
}

interface PlaybackOutcomeDetails {
	readonly consumedMs?: number;
	readonly truncationMs?: number;
	readonly spokenPrefix?: string;
	readonly remainder?: string;
	readonly charIndex?: number;
	readonly atMs?: number;
	readonly at?: string;
	readonly error?: unknown;
	readonly decision?: BargeInResult["decision"];
	readonly reason?: BargeInResult["reason"];
	readonly audioPassed?: boolean;
	readonly transcriptChars?: number;
	readonly echoSimilarity?: number;
	readonly cooldownActive?: boolean;
	readonly textFallback?: boolean;
}

type PlaybackOutcome = VoicePlaybackOutcome & PlaybackOutcomeDetails;

interface ActivePlayback {
	readonly stream: Readable;
	readonly resource: VoicePlaybackResource;
	readonly player: VoicePlaybackPlayer;
	subscription: VoicePlaybackSubscription | undefined;
	readonly controller: AbortController;
	readonly chunks: TtsChunk[];
	readonly resolve: (outcome: PlaybackOutcome) => void;
	iterator: AsyncIterator<TtsChunk> | undefined;
	iteratorClosed: boolean;
	finalSeen: boolean;
	synthesisDone: boolean;
	cancelled: boolean;
	settling: boolean;
	onError?: (error: unknown) => void;
	onStateChange?: (oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void;
}

/** Returned by createVoicePlayback; one instance owns one active reply at a time. */
export interface VoicePlaybackHandle {
	play(message: ChatMessagePayload): Promise<PlaybackOutcome>;
	handleBargeIn(input: VoicePlaybackBargeInInput): BargeInResult;
	abort(reason?: unknown): Promise<void>;
}

export interface VoicePlaybackOptions {
	readonly config: Pick<VoiceConfig, "bargeIn">;
	readonly tts: TtsProvider;
	readonly subscribe: VoicePlaybackSubscribe;
	readonly resourceFactory?: VoicePlaybackResourceFactory;
	readonly playerFactory?: VoicePlaybackPlayerFactory;
	readonly clock?: VoicePlaybackClock;
}

const defaultClock: VoicePlaybackClock = { now: () => Date.now() };

const defaultResourceFactory: VoicePlaybackResourceFactory = (stream, options) => createAudioResource(stream, options);

const defaultPlayerFactory: VoicePlaybackPlayerFactory = () => createAudioPlayer();

function asError(value: unknown): Error {
	return value instanceof Error ? value : new Error(String(value));
}

function hasAlignment(chunk: TtsChunk): boolean {
	if (chunk.kind !== "audio") return true;
	return (
		chunk.chars !== undefined &&
		chunk.charStartMs !== undefined &&
		chunk.charDurationMs !== undefined &&
		Array.isArray(chunk.chars) &&
		Array.isArray(chunk.charStartMs) &&
		Array.isArray(chunk.charDurationMs) &&
		chunk.chars.length === chunk.charStartMs.length &&
		chunk.chars.length === chunk.charDurationMs.length
	);
}

function isIdle(state: VoicePlaybackPlayerState): boolean {
	return state.status === "idle";
}

function playbackDuration(resource: VoicePlaybackResource): number {
	const consumedMs = resource.playbackDuration;
	return Number.isFinite(consumedMs) && consumedMs >= 0 ? consumedMs : 0;
}

function noAudioOutcome(error: unknown, textFallback = false): PlaybackOutcome {
	return {
		outcome: "failed",
		error,
		audioPlayed: false,
		...(textFallback ? { textFallback: true } : {}),
	};
}

/**
 * Creates the resource/player lifecycle for one reply. The subscribe callback is deliberately
 * injected because a real VoiceConnection is an external resource and should not appear in tests.
 */
export function createVoicePlayback(options: VoicePlaybackOptions): VoicePlaybackHandle {
	const clock = options.clock ?? defaultClock;
	const resourceFactory = options.resourceFactory ?? defaultResourceFactory;
	const playerFactory = options.playerFactory ?? defaultPlayerFactory;
	const gate = new BargeInGate(options.config.bargeIn);
	let active: ActivePlayback | undefined;

	const removeListeners = (state: ActivePlayback): void => {
		if (state.onError !== undefined) state.player.off?.("error", state.onError);
		if (state.onStateChange !== undefined) state.player.off?.("stateChange", state.onStateChange);
	};

	const closeIterator = async (state: ActivePlayback): Promise<void> => {
		const iterator = state.iterator;
		if (iterator === undefined || state.iteratorClosed) return;
		state.iteratorClosed = true;
		if (iterator.return === undefined) return;
		try {
			await iterator.return();
		} catch {
			// The abort signal is the provider's primary cancellation path; return is best effort.
		}
	};

	const cleanup = async (state: ActivePlayback, stopPlayer: boolean): Promise<void> => {
		if (stopPlayer) {
			try {
				state.player.stop(true);
			} catch {
				// The original playback failure is the useful error; cleanup must remain bounded.
			}
		}
		if (!state.stream.destroyed) state.stream.destroy();
		try {
			await state.subscription?.unsubscribe?.();
		} catch {
			// A failed unsubscribe cannot change the already-latched playback outcome.
		}
		removeListeners(state);
		if (active === state) active = undefined;
	};

	const finish = async (state: ActivePlayback, outcome: PlaybackOutcome, stopPlayer: boolean): Promise<void> => {
		if (state.settling) return;
		state.settling = true;
		if (outcome.outcome !== "completed") {
			state.cancelled = true;
			state.controller.abort();
			void closeIterator(state);
		}
		await cleanup(state, stopPlayer);
		state.resolve(outcome);
	};

	const finishFailed = async (state: ActivePlayback, error: unknown): Promise<void> => {
		const consumedMs = playbackDuration(state.resource);
		await finish(
			state,
			error instanceof TtsAlignmentMissingError
				? {
						...noAudioOutcome(error, true),
						audioPlayed: consumedMs > 0,
						consumedMs,
					}
				: {
						outcome: "failed",
						error,
						audioPlayed: consumedMs > 0,
						consumedMs,
					},
			true,
		);
	};

	const finishCompleted = async (state: ActivePlayback): Promise<void> => {
		const consumedMs = playbackDuration(state.resource);
		await finish(state, { outcome: "completed", audioPlayed: consumedMs > 0, consumedMs }, false);
	};

	const onSynthesis = async (state: ActivePlayback): Promise<void> => {
		const iterator = state.iterator;
		if (iterator === undefined) return;
		try {
			while (!state.cancelled) {
				const next = await iterator.next();
				if (state.cancelled) break;
				if (next.done) break;
				const chunk = next.value;
				if (!hasAlignment(chunk)) throw new TtsAlignmentMissingError();
				if (chunk.kind === "final") {
					if (state.finalSeen) throw new Error("TTS synthesis emitted more than one final chunk");
					state.finalSeen = true;
					continue;
				}
				if (state.finalSeen) throw new Error("TTS synthesis emitted audio after its final chunk");
				state.chunks.push(chunk);
				const samples = decodePcm16Le(chunk.audio);
				const resampled = resamplePcm24kTo48k(samples);
				const stereo = interleaveMonoToStereo(resampled);
				state.stream.push(encodePcm16Le(stereo));
			}
			if (state.cancelled) return;
			if (!state.finalSeen) throw new Error("TTS synthesis ended without a final chunk");
			state.synthesisDone = true;
			state.stream.push(null);
		} catch (error) {
			if (state.cancelled) return;
			if (!state.stream.destroyed) state.stream.destroy(asError(error));
			await finishFailed(state, error);
		} finally {
			if (state.cancelled && !state.synthesisDone) void closeIterator(state);
		}
	};

	const handleBargeIn = (input: VoicePlaybackBargeInInput): BargeInResult => {
		const nowMs = clock.now();
		const state = active;
		const playedText =
			state === undefined ? "" : spokenPrefixFromChunks(state.chunks, playbackDuration(state.resource)).spokenPrefix;
		const decision =
			state === undefined
				? decideBargeIn(
						{ audioPassed: input.audioPassed, transcript: input.transcript, playedText, nowMs },
						options.config.bargeIn,
					)
				: gate.decide({ audioPassed: input.audioPassed, transcript: input.transcript, playedText, nowMs });
		if (!decision.shouldInterrupt || state === undefined || state.settling) return decision;

		const consumedMs = playbackDuration(state.resource);
		const truncationMs = Math.max(0, consumedMs - options.config.bargeIn.frameCorrectionMs);
		const prefix = spokenPrefixFromChunks(state.chunks, truncationMs);
		const atMs = input.atMs ?? nowMs;
		const outcome: PlaybackOutcome = {
			outcome: "barge_in",
			audioPlayed: consumedMs > 0,
			spokenPrefix: prefix.spokenPrefix,
			remainder: prefix.remainder,
			charIndex: prefix.charIndex,
			truncationMs,
			atMs,
			at: new Date(atMs).toISOString(),
			decision: decision.decision,
			reason: decision.reason,
			audioPassed: decision.audioPassed,
			transcriptChars: decision.transcriptChars,
			echoSimilarity: decision.echoSimilarity,
			cooldownActive: decision.cooldownActive,
			consumedMs,
		};
		state.cancelled = true;
		state.controller.abort();
		void finish(state, outcome, true);
		return decision;
	};

	const abort = async (reason: unknown = new Error("voice playback aborted")): Promise<void> => {
		const state = active;
		if (state === undefined || state.settling) return;
		await finishFailed(state, reason);
	};

	const play = (message: ChatMessagePayload): Promise<PlaybackOutcome> => {
		if (active !== undefined) {
			const consumedMs = playbackDuration(active.resource);
			return Promise.resolve({
				outcome: "failed",
				error: new Error("voice playback is already active"),
				audioPlayed: consumedMs > 0,
				consumedMs,
			});
		}
		return new Promise<PlaybackOutcome>((resolve) => {
			const stream = new Readable({ read() {} });
			stream.on("error", () => undefined);
			let resource: VoicePlaybackResource;
			let player: VoicePlaybackPlayer;
			try {
				resource = resourceFactory(stream, { inputType: StreamType.Raw });
				player = playerFactory();
			} catch (error) {
				if (!stream.destroyed) stream.destroy();
				resolve(noAudioOutcome(error));
				return;
			}
			const controller = new AbortController();
			const state: ActivePlayback = {
				stream,
				resource,
				player,
				subscription: undefined,
				controller,
				chunks: [],
				resolve,
				iterator: undefined,
				iteratorClosed: false,
				finalSeen: false,
				synthesisDone: false,
				cancelled: false,
				settling: false,
			};
			state.onError = (error: unknown) => {
				void finishFailed(state, error);
			};
			state.onStateChange = (_oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => {
				if (!isIdle(newState)) return;
				if (state.cancelled || state.settling) return;
				if (!state.synthesisDone) {
					void finishFailed(state, new Error("audio player became idle before synthesis completed"));
					return;
				}
				void finishCompleted(state);
			};
			active = state;
			try {
				if (state.onError !== undefined) player.on("error", state.onError);
				if (state.onStateChange !== undefined) player.on("stateChange", state.onStateChange);
				state.subscription = options.subscribe(player);
				player.play(resource);
				const iterable = options.tts.synthesize(message.text, controller.signal);
				state.iterator = iterable[Symbol.asyncIterator]();
				void onSynthesis(state);
			} catch (error) {
				void finishFailed(state, error);
			}
		});
	};

	return { play, handleBargeIn, abort };
}
