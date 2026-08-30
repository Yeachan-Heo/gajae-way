import { expect, test } from "bun:test";
import type { Readable } from "node:stream";
import { StreamType } from "@discordjs/voice";
import type { ChatMessagePayload, OriginRef } from "@gajaeway/protocol";
import type { BargeInReason, TtsChunk, TtsProvider } from "@gajaeway/voice-core";

import type { VoiceConfig } from "../src/config";
import {
	createVoicePlayback,
	type VoicePlaybackBargeInInput,
	type VoicePlaybackPlayer,
	type VoicePlaybackPlayerState,
	type VoicePlaybackResource,
	type VoicePlaybackResourceFactoryOptions,
} from "../src/voice/playback";

const ORIGIN: OriginRef = { platform: "discord", kind: "channel", conversationId: "voice-1" };
const MESSAGE: ChatMessagePayload = {
	turnId: "turn-1",
	origin: ORIGIN,
	role: "assistant",
	text: "ABCDEFGH",
	final: true,
	deliveryId: "delivery-1",
};

const config = (frameCorrectionMs = 20, cooldownMs = 1_000): Pick<VoiceConfig, "bargeIn"> => ({
	bargeIn: {
		minTranscriptChars: 3,
		cooldownMs,
		echoSimilarity: 0.6,
		frameCorrectionMs,
	},
});

class FakeClock {
	nowMs = 1_000;

	now(): number {
		return this.nowMs;
	}
}

class Deferred {
	readonly promise: Promise<void>;
	#resolve: (() => void) | undefined;

	constructor() {
		this.promise = new Promise<void>((resolve) => {
			this.#resolve = resolve;
		});
	}

	resolve(): void {
		this.#resolve?.();
	}
}

class FakeResource implements VoicePlaybackResource {
	readonly input: Readable;
	playbackDuration = 0;
	readonly realFrameBytes = 3_840;

	constructor(input: Readable) {
		this.input = input;
	}

	consumeRealFrames(frameCount: number): void {
		const bytesToRead = frameCount * this.realFrameBytes;
		let remaining = bytesToRead;
		while (remaining > 0) {
			const chunk = this.input.read(remaining);
			if (chunk === null) throw new Error(`expected ${remaining} buffered audio bytes`);
			remaining -= chunk.byteLength;
		}
		this.playbackDuration += frameCount * 20;
	}

	consumeSilenceFrames(_frameCount: number): void {
		// Discord's silence padding is consumed by the player but not by AudioResource.playbackDuration.
	}
}

class FakePlayer implements VoicePlaybackPlayer {
	resource: VoicePlaybackResource | undefined;
	stopCalls = 0;
	#errorListeners: Array<(error: unknown) => void> = [];
	#stateListeners: Array<(oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void> = [];

	on(event: "error", listener: (error: unknown) => void): this;
	on(
		event: "stateChange",
		listener: (oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void,
	): this;
	on(
		event: "error" | "stateChange",
		listener:
			| ((error: unknown) => void)
			| ((oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void),
	): this {
		if (event === "error") this.#errorListeners.push(listener as (error: unknown) => void);
		else
			this.#stateListeners.push(
				listener as (oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void,
			);
		return this;
	}

	off(event: "error", listener: (error: unknown) => void): this;
	off(
		event: "stateChange",
		listener: (oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void,
	): this;
	off(
		event: "error" | "stateChange",
		listener:
			| ((error: unknown) => void)
			| ((oldState: VoicePlaybackPlayerState, newState: VoicePlaybackPlayerState) => void),
	): this {
		if (event === "error") {
			this.#errorListeners = this.#errorListeners.filter((candidate) => candidate !== listener);
		} else {
			this.#stateListeners = this.#stateListeners.filter((candidate) => candidate !== listener);
		}
		return this;
	}

	play(resource: VoicePlaybackResource): void {
		this.resource = resource;
	}

	stop(_force?: boolean): boolean {
		this.stopCalls += 1;
		return true;
	}

	emitIdle(): void {
		for (const listener of this.#stateListeners) listener({ status: "playing" }, { status: "idle" });
	}

	emitError(error: unknown): void {
		for (const listener of this.#errorListeners) listener(error);
	}
}

function audioChunk(frameCount = 8): TtsChunk {
	const samples = new Uint8Array(frameCount * 480 * 2);
	const chars = Array.from("ABCDEFGH").slice(0, frameCount);
	return {
		kind: "audio",
		audio: samples,
		chars,
		charStartMs: chars.map((_, index) => index * 20),
		charDurationMs: chars.map(() => 20),
	};
}

function providerFor(chunks: readonly TtsChunk[], finished: Deferred): TtsProvider {
	return {
		voiceId: "voice-1",
		outputFormat: "pcm_24000",
		synthesize: async function* (_text: string, signal: AbortSignal): AsyncGenerator<TtsChunk> {
			try {
				for (const chunk of chunks) {
					if (signal.aborted) return;
					yield chunk;
				}
				if (!signal.aborted) yield { kind: "final" };
			} finally {
				finished.resolve();
			}
		},
	};
}

function createSetup(
	chunks: readonly TtsChunk[] = [audioChunk()],
	overrides: { readonly frameCorrectionMs?: number; readonly cooldownMs?: number } = {},
) {
	const finished = new Deferred();
	const clock = new FakeClock();
	const players: FakePlayer[] = [];
	const resources: FakeResource[] = [];
	let unsubscribeCalls = 0;
	const resourceFactory = (input: Readable, options: VoicePlaybackResourceFactoryOptions): VoicePlaybackResource => {
		expect(options.inputType).toBe(StreamType.Raw);
		const resource = new FakeResource(input);
		resources.push(resource);
		return resource;
	};
	const playback = createVoicePlayback({
		config: config(overrides.frameCorrectionMs ?? 20, overrides.cooldownMs ?? 1_000),
		tts: providerFor(chunks, finished),
		subscribe: () => ({
			unsubscribe: () => {
				unsubscribeCalls += 1;
			},
		}),

		resourceFactory,
		playerFactory: () => {
			const player = new FakePlayer();
			players.push(player);
			return player;
		},
		clock,
	});
	return {
		playback,
		clock,
		finished,
		players,
		resources,
		get unsubscribeCalls() {
			return unsubscribeCalls;
		},
	};
}

async function flush(): Promise<void> {
	await Promise.resolve();
	await Promise.resolve();
	await Promise.resolve();
}

async function prepared(setup: ReturnType<typeof createSetup>): Promise<{
	playPromise: ReturnType<ReturnType<typeof createSetup>["playback"]["play"]>;
	player: FakePlayer;
	resource: FakeResource;
}> {
	const playPromise = setup.playback.play(MESSAGE);
	await setup.finished.promise;
	await flush();
	const player = setup.players[0];
	const resource = setup.resources[0];
	if (player === undefined || resource === undefined) throw new Error("playback setup did not create player/resource");
	return { playPromise, player, resource };
}

function input(audioPassed: boolean, transcript: string, atMs?: number): VoicePlaybackBargeInInput {
	return { audioPassed, transcript, ...(atMs === undefined ? {} : { atMs }) };
}

test("pre-buffered audio truncates from consumed packets, not written bytes, and reports the correction", async () => {
	const setup = createSetup();
	const { playPromise, resource } = await prepared(setup);
	const writtenBytes = resource.input.readableLength;
	expect(writtenBytes).toBe(8 * resource.realFrameBytes);
	resource.consumeRealFrames(3);
	const decision = setup.playback.handleBargeIn(input(true, "human interruption", 1_234));
	expect(decision.shouldInterrupt).toBe(true);
	const outcome = await playPromise;
	expect(outcome.outcome).toBe("barge_in");
	expect(outcome.consumedMs).toBe(60);
	expect(outcome.truncationMs).toBe(40);
	expect(outcome.spokenPrefix).toBe("AB");
	expect(outcome.remainder).toBe("CDEFGH");
	expect(outcome.charIndex).toBe(2);
	expect(outcome.atMs).toBe(1_234);
	expect(outcome.audioPlayed).toBe(true);
	expect(setup.unsubscribeCalls).toBe(1);
});

test("silence padding does not advance the consumed duration or inflate the remainder cut", async () => {
	const setup = createSetup();
	const { playPromise, resource } = await prepared(setup);
	resource.consumeRealFrames(3);
	resource.consumeSilenceFrames(5);
	setup.playback.handleBargeIn(input(true, "human interruption"));
	const outcome = await playPromise;
	expect(outcome.outcome).toBe("barge_in");
	expect(outcome.consumedMs).toBe(60);
	expect(outcome.truncationMs).toBe(40);
	expect(outcome.remainder).toBe("CDEFGH");
});

test("frame correction clamps below zero and never claims beyond consumed audio", async () => {
	const setup = createSetup([audioChunk(2)], { frameCorrectionMs: 50 });
	const { playPromise, resource } = await prepared(setup);
	resource.consumeRealFrames(1);
	setup.playback.handleBargeIn(input(true, "human interruption"));
	const outcome = await playPromise;
	expect(outcome.consumedMs).toBe(20);
	expect(outcome.truncationMs).toBe(0);
	expect(outcome.charIndex).toBe(0);
	expect(outcome.remainder).toBe("AB");
});

test("only audio-passed plus real text interrupts across all four gate combinations", async () => {
	const cases: readonly [boolean, string, BargeInReason, boolean][] = [
		[false, "hello", "audio_gate", false],
		[true, "", "transcript_gate", false],
		[true, "hi", "min_chars", false],
		[true, "hello", "passed", true],
	];
	for (const [audioPassed, transcript, reason, shouldInterrupt] of cases) {
		const setup = createSetup();
		const { playPromise } = await prepared(setup);
		const decision = setup.playback.handleBargeIn(input(audioPassed, transcript));
		expect(decision.reason).toBe(reason);
		expect(decision.audioPassed).toBe(audioPassed);
		expect(decision.transcriptChars).toBe(transcript.replace(/[^\p{L}\p{N}]+/gu, "").length);
		expect(decision.shouldInterrupt).toBe(shouldInterrupt);
		if (shouldInterrupt) await playPromise;
		else await setup.playback.abort();
	}
});

test("echo, minimum-character, and cooldown defenses expose their decision details", async () => {
	const setup = createSetup();
	const first = await prepared(setup);
	first.resource.consumeRealFrames(4);
	const echo = setup.playback.handleBargeIn(input(true, "ABCD"));
	expect(echo.shouldInterrupt).toBe(false);
	expect(echo.reason).toBe("echo");
	expect(echo.echoSimilarity).toBe(1);
	const short = setup.playback.handleBargeIn(input(true, "ok"));
	expect(short.shouldInterrupt).toBe(false);
	expect(short.reason).toBe("min_chars");
	const firstInterruption = setup.playback.handleBargeIn(input(true, "please stop"));
	expect(firstInterruption.shouldInterrupt).toBe(true);
	const firstOutcome = await first.playPromise;
	expect(firstOutcome.outcome).toBe("barge_in");
	expect(firstOutcome.cooldownActive).toBe(false);

	const second = await prepared(setup);
	setup.clock.nowMs += 100;
	const cooldown = setup.playback.handleBargeIn(input(true, "another request"));
	expect(cooldown.shouldInterrupt).toBe(false);
	expect(cooldown.reason).toBe("cooldown");
	expect(cooldown.cooldownActive).toBe(true);
	await setup.playback.abort();

	const third = await prepared(setup);
	setup.clock.nowMs += 1_000;
	const afterCooldown = setup.playback.handleBargeIn(input(true, "new request"));
	expect(afterCooldown.shouldInterrupt).toBe(true);
	expect(afterCooldown.cooldownActive).toBe(false);
	await third.playPromise;
	void second;
});

test("alignment-missing synthesis returns a text-fallback failure without claiming played audio", async () => {
	const malformed = { kind: "audio", audio: new Uint8Array(4) } as unknown as TtsChunk;
	const setup = createSetup([malformed]);
	const outcome = await setup.playback.play(MESSAGE);
	expect(outcome.outcome).toBe("failed");
	expect(outcome.textFallback).toBe(true);
	expect(outcome.audioPlayed).toBe(false);
	expect(outcome.consumedMs).toBe(0);
	expect(outcome.error).toBeInstanceOf(Error);
	expect((outcome.error as Error).name).toBe("TtsAlignmentMissingError");
});

test("a player failure after partial consumption reports ambiguous played audio", async () => {
	const setup = createSetup();
	const { playPromise, player, resource } = await prepared(setup);
	resource.consumeRealFrames(2);
	player.emitError(new Error("speaker stopped"));
	const outcome = await playPromise;
	expect(outcome.outcome).toBe("failed");
	expect(outcome.audioPlayed).toBe(true);
	expect(outcome.consumedMs).toBe(40);
});

test("a clean player idle after final synthesis reports completion and no interruption", async () => {
	const setup = createSetup([audioChunk(2)]);
	const { playPromise, player, resource } = await prepared(setup);
	resource.consumeRealFrames(2);
	player.emitIdle();
	const outcome = await playPromise;
	expect(outcome).toEqual({ outcome: "completed", audioPlayed: true, consumedMs: 40 });
	expect(setup.unsubscribeCalls).toBe(1);
});
