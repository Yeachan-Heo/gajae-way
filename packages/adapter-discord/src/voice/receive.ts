import type { VoiceReceiver } from "@discordjs/voice";

import {
	type Admission,
	advanceUtterance,
	computePcm16Rms,
	createEnergyGate,
	createSttMachine,
	createUtteranceState,
	decodePcm16Le,
	type EnergyGateConfig,
	type EnergyGateState,
	encodePcm16Le,
	type SttEvent,
	type SttMachineAction,
	type SttMachineConfig,
	type SttMachineInput,
	type SttMachineState,
	type SttStream,
	transitionSttMachine,
	type UtteranceState,
	updateEnergyGate,
} from "@gajaeway/voice-core";

/** Discord's voice receive contract: Opus decodes to interleaved 48 kHz stereo PCM16. */
export const DISCORD_RECEIVE_SAMPLE_RATE_HZ = 48_000;
/** Discord's voice receive contract channel count. */
export const DISCORD_RECEIVE_CHANNELS = 2;
/** ElevenLabs Scribe v2 Realtime's verified PCM input rate. */
export const STT_RECEIVE_SAMPLE_RATE_HZ = 16_000;
/** The STT input contract is mono PCM16. */
export const STT_RECEIVE_CHANNELS = 1;
/** Integer resampling ratio required by the Discord-to-STT audio contract. */
export const DISCORD_TO_STT_SAMPLE_RATE_RATIO = DISCORD_RECEIVE_SAMPLE_RATE_HZ / STT_RECEIVE_SAMPLE_RATE_HZ;

/** Narrow subscription shape used by the lifecycle owner and tests. */
export interface VoiceAudioSubscriptionLike extends AsyncIterable<Uint8Array> {
	destroy?(error?: Error): void;
	return?(value?: unknown): Promise<IteratorResult<Uint8Array>> | IteratorResult<Uint8Array>;
}

/** Speaking lifecycle boundary exposed by @discordjs/voice's SpeakingMap. */
export interface VoiceSpeakingLike {
	on(event: "start" | "end", listener: (userId: string) => void): unknown;
	off?(event: "start" | "end", listener: (userId: string) => void): unknown;
	removeListener?(event: "start" | "end", listener: (userId: string) => void): unknown;
}

/** Narrow receiver shape; Discord's concrete VoiceReceiver is only a type boundary. */
export interface VoiceReceiverLike {
	subscribe(userId: string, options?: Parameters<VoiceReceiver["subscribe"]>[1]): VoiceAudioSubscriptionLike;
	readonly speaking?: VoiceSpeakingLike;
}

/** Decoder boundary for @discordjs/opus or a deterministic test fake. */
export interface VoiceReceiveDecoderLike {
	decode(opus: Uint8Array): Uint8Array;
	release?(): void | Promise<void>;
	close?(): void | Promise<void>;
	destroy?(): void | Promise<void>;
}

export type VoiceReceiveDecoderFactory = () => VoiceReceiveDecoderLike | Promise<VoiceReceiveDecoderLike>;

export interface VoiceReceiveClock {
	now(): number;
}

export interface VoiceReceiveIngressEvent {
	readonly speakerId: string;
	readonly admission: Admission;
	readonly ingress: "recorded" | "dropped";
	readonly atMs: number;
}

export interface VoiceReceiveCounters {
	readonly accepted: number;
	readonly dropped: number;
	readonly rejectedClosed: number;
}

export interface VoiceReceiveUtterance {
	readonly speakerId: string;
	readonly startedAtMs: number;
	readonly endedAtMs: number;
	readonly boundary: "silence_end";
	readonly energyRms: number;
	readonly energyThreshold: number;
	readonly energyHoldMs: number;
	readonly energyGatePassed: true;
	/** Dropped ingress is provenance, not a synthetic dropped-noise boundary. */
	readonly ingress: "recorded" | "dropped";
}

export interface VoiceReceiveOptions {
	readonly receiver: VoiceReceiverLike;
	readonly speakerId: string;
	readonly subscribeOptions?: Parameters<VoiceReceiver["subscribe"]>[1];
	readonly decoder: VoiceReceiveDecoderLike;
	readonly stream?: SttStream;
	readonly clock: VoiceReceiveClock;
	readonly energyGate: EnergyGateConfig;
	readonly silenceEndMs: number;
	/** The provider SttStream owns this bound and returns the authoritative Admission. */
	readonly maxQueuedFrames?: number;
	readonly sttMachine?: SttMachineConfig;
	readonly onAdmission?: (event: VoiceReceiveIngressEvent) => void;
	readonly onUtterance?: (utterance: VoiceReceiveUtterance) => void | Promise<void>;
	readonly onMachineAction?: (action: SttMachineAction) => void;
	readonly onError?: (error: unknown) => void;
	readonly onEnd?: () => void;
}

export interface SpeakerReceiveHandle {
	readonly speakerId: string;
	readonly subscription: VoiceAudioSubscriptionLike;
	readonly counters: VoiceReceiveCounters;
	readonly closed: boolean;
	readonly energyState: EnergyGateState;
	readonly utteranceState: UtteranceState;
	readonly machineState: SttMachineState | undefined;
	attachStream(stream: SttStream): void;
	detachStream(): void;
	providerOpened(): void;
	handleSttEvent(event: SttEvent): void;
	close(reason?: string): Promise<void>;
}

/**
 * Converts one decoded interleaved stereo PCM16 frame to mono PCM16 at 16 kHz.
 * Each output sample is the rounded average of three source stereo frames, with
 * each source frame first downmixed by averaging its left/right channels. Incomplete
 * source frames at the tail are ignored, as they cannot form a complete stereo sample.
 */
export function downsamplePcm48kStereoTo16kMono(decodedPcm16Le: Uint8Array): Uint8Array {
	const decoded = decodePcm16Le(decodedPcm16Le);
	const stereoFrameCount = Math.floor(decoded.length / DISCORD_RECEIVE_CHANNELS);
	const outputSampleCount = Math.floor((stereoFrameCount * STT_RECEIVE_CHANNELS) / DISCORD_TO_STT_SAMPLE_RATE_RATIO);
	const mono = new Int16Array(outputSampleCount);
	for (let outputIndex = 0; outputIndex < outputSampleCount; outputIndex += 1) {
		const firstSourceFrame = outputIndex * DISCORD_TO_STT_SAMPLE_RATE_RATIO;
		let sum = 0;
		for (let offset = 0; offset < DISCORD_TO_STT_SAMPLE_RATE_RATIO; offset += 1) {
			const sourceSampleIndex = (firstSourceFrame + offset) * DISCORD_RECEIVE_CHANNELS;
			const left = decoded[sourceSampleIndex] ?? 0;
			const right = decoded[sourceSampleIndex + 1] ?? left;
			sum += (left + right) / DISCORD_RECEIVE_CHANNELS;
		}
		mono[outputIndex] = Math.round(sum / DISCORD_TO_STT_SAMPLE_RATE_RATIO);
	}
	return encodePcm16Le(mono);
}

/** Decodes one Opus packet and applies the verified Discord-to-STT conversion. */
export function decodeAndDownsamplePcm16(opusPacket: Uint8Array, decoder: VoiceReceiveDecoderLike): Uint8Array {
	return downsamplePcm48kStereoTo16kMono(decoder.decode(opusPacket));
}

/**
 * Subscribes one Discord speaker and starts the decode/VAD/ingress pump. The returned
 * handle owns the subscription and can detach/re-attach its STT stream during a
 * provider reconnect while recording rejected ingress as dropped.
 */
export function createSpeakerReceiver(options: VoiceReceiveOptions): SpeakerReceiveHandle {
	if (options.speakerId.trim() === "") throw new RangeError("speakerId must not be blank");
	if (!Number.isFinite(options.silenceEndMs) || options.silenceEndMs < 0) {
		throw new RangeError("silenceEndMs must be finite and non-negative");
	}
	if (
		options.maxQueuedFrames !== undefined &&
		(!Number.isInteger(options.maxQueuedFrames) || options.maxQueuedFrames < 0)
	) {
		throw new RangeError("maxQueuedFrames must be a non-negative integer");
	}

	const subscription = options.receiver.subscribe(options.speakerId, options.subscribeOptions);
	let targetStream = options.stream;
	let stopped = false;
	let closing = false;
	let closePromise: Promise<void> | undefined;
	let energyState = createEnergyGate(options.energyGate);
	let utteranceState = createUtteranceState(options.speakerId, options.silenceEndMs);
	let machineState = options.sttMachine === undefined ? undefined : createSttMachine(options.sttMachine);
	let droppedSinceBoundary = false;
	let speechEnergyRms = 0;
	let speechEnergyHoldMs = 0;
	let accepted = 0;
	let dropped = 0;
	let rejectedClosed = 0;

	const machineStep = (input: SttMachineInput): void => {
		if (machineState === undefined) return;
		const transition = transitionSttMachine(machineState, input);
		machineState = transition.state;
		for (const action of transition.actions) options.onMachineAction?.(action);
	};

	const recordAdmission = (admission: Admission, atMs: number): void => {
		if (admission === "accepted") accepted += 1;
		if (admission === "dropped_oldest") {
			dropped += 1;
			droppedSinceBoundary = true;
		}
		if (admission === "rejected_closed") {
			rejectedClosed += 1;
			dropped += 1;
			droppedSinceBoundary = true;
		}
		options.onAdmission?.({
			speakerId: options.speakerId,
			admission,
			ingress: admission === "accepted" ? "recorded" : "dropped",
			atMs,
		});
	};

	const processPacket = (opusPacket: Uint8Array): void => {
		if (stopped) return;
		const pcm16 = decodeAndDownsamplePcm16(opusPacket, options.decoder);
		const atMs = options.clock.now();
		const previousGatePassed = energyState.energyGatePassed;
		energyState = updateEnergyGate(energyState, computePcm16Rms(pcm16), atMs);
		if (energyState.energyGatePassed) {
			speechEnergyRms = energyState.energyRms;
			speechEnergyHoldMs = energyState.energyHoldMs;
		}
		if (!previousGatePassed && energyState.energyGatePassed) {
			machineStep({
				kind: "energy_gate_passed",
				atMs,
				energyRms: energyState.energyRms,
				energyThreshold: energyState.energyThreshold,
				energyHoldMs: energyState.energyHoldMs,
			});
		}

		const activity = energyState.energyGatePassed ? "speech" : "silence";
		const advanced = advanceUtterance(utteranceState, activity, atMs);
		utteranceState = advanced.state;
		let completedUtterance: VoiceReceiveUtterance | undefined;
		if (advanced.utterance !== null) {
			machineStep({ kind: "silence_boundary", atMs });
			completedUtterance = {
				speakerId: advanced.utterance.speakerId,
				startedAtMs: advanced.utterance.startedAtMs,
				endedAtMs: advanced.utterance.endedAtMs,
				boundary: advanced.utterance.boundary,
				energyRms: speechEnergyRms,
				energyThreshold: energyState.energyThreshold,
				energyHoldMs: speechEnergyHoldMs,
				energyGatePassed: true,
				ingress: "recorded",
			};
		}

		const admission = targetStream?.push(pcm16) ?? "rejected_closed";
		recordAdmission(admission, atMs);
		if (completedUtterance !== undefined) {
			const utterance = { ...completedUtterance, ingress: droppedSinceBoundary ? "dropped" : "recorded" } as const;
			droppedSinceBoundary = false;
			void options.onUtterance?.(utterance);
		}
	};

	const processing = (async (): Promise<void> => {
		try {
			for await (const packet of subscription) processPacket(packet);
		} catch (error) {
			if (!closing) options.onError?.(error);
		} finally {
			if (!closing) options.onEnd?.();
		}
	})();

	const handle: SpeakerReceiveHandle = {
		get speakerId() {
			return options.speakerId;
		},
		get subscription() {
			return subscription;
		},
		get counters() {
			return { accepted, dropped, rejectedClosed };
		},
		get closed() {
			return stopped;
		},
		get energyState() {
			return energyState;
		},
		get utteranceState() {
			return utteranceState;
		},
		get machineState() {
			return machineState;
		},
		attachStream(stream: SttStream): void {
			if (stopped) return;
			targetStream = stream;
		},
		detachStream(): void {
			targetStream = undefined;
		},
		providerOpened(): void {
			machineStep({ kind: "open" });
		},
		handleSttEvent(event: SttEvent): void {
			machineStep(event);
		},
		async close(reason = "teardown"): Promise<void> {
			if (closePromise !== undefined) return closePromise;
			closing = true;
			stopped = true;
			targetStream = undefined;
			machineStep({ kind: "teardown" });
			closePromise = (async () => {
				try {
					subscription.destroy?.();
					await subscription.return?.();
					await processing;
				} catch {
					// A destroyed Discord receive stream may reject its iterator during teardown.
				}
				void reason;
			})();
			return closePromise;
		},
	};

	return handle;
}

/** Convenience form for callers that already own the receiver binding. */
export function subscribeSpeaker(
	receiver: VoiceReceiverLike,
	options: Omit<VoiceReceiveOptions, "receiver">,
): SpeakerReceiveHandle {
	return createSpeakerReceiver({ ...options, receiver });
}
