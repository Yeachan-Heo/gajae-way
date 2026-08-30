import { describe, expect, test } from "bun:test";
import {
	decodePcm16Le,
	encodePcm16Le,
	TtsAlignmentMissingError,
	type TtsChunk,
	type TtsProvider,
} from "@gajaeway/voice-core";
import type { VoiceConfig } from "../src/config";
import {
	ELEVENLABS_TTS_PROVIDER_CONTRACT,
	ElevenLabsTtsProvider,
	ElevenLabsTtsProviderError,
	type ElevenLabsTtsSocket,
	type ElevenLabsTtsSocketFactory,
	type ElevenLabsTtsSocketFactoryOptions,
} from "../src/voice/providers/elevenlabs-tts";
import { createTtsSynthesis } from "../src/voice/tts-synthesis";

const API_KEY = "elevenlabs-test-secret";

const config: VoiceConfig = {
	enabled: true,
	silenceEndMs: 700,
	energyGate: { minDurationMs: 300, rmsThreshold: 0.02 },
	idleLeaveMs: 300_000,
	unread: { maxItems: 20, maxCharsPerItem: 200 },
	mergeWindowMs: 1_500,
	transcriptWaitMs: 1_500,
	sessionMaxMs: 3_600_000,
	turnMapTtlMs: 900_000,
	outstanding: { ttlMs: 900_000, maxEntries: 32 },
	bargeIn: { minTranscriptChars: 3, cooldownMs: 1_500, echoSimilarity: 0.6, frameCorrectionMs: 20 },
	ingress: { maxQueuedFramesPerSpeaker: 20, overflow: "drop_oldest" },
	reconnect: { initialBackoffMs: 500, maxBackoffMs: 30_000, maxAttempts: 6 },
	joinCommandAllowlist: [],
	announceOnTurnStart: true,
	drillLog: { enabled: false, path: "artifacts" },
	elevenlabs: {
		apiKeyFile: "elevenlabs-key",
		stt: {
			model: "scribe_v2_realtime",
			audioFormat: "pcm_16000",
			commitStrategy: "vad",
			includeLanguageDetection: true,
			filterBackgroundAudio: true,
			keyterms: [],
		},
		tts: {
			model: "eleven_flash_v2_5",
			voiceId: "voice-fixed",
			outputFormat: "pcm_24000",
			syncAlignment: true,
			inactivityTimeoutSecs: 45,
			applyTextNormalization: "on",
			chunkLengthSchedule: [100, 200, 400],
		},
	},
};

class FakeSocket implements ElevenLabsTtsSocket {
	readonly sent: string[] = [];
	closeCalls = 0;
	#messageListener: ((payload: unknown) => void) | undefined;
	#errorListener: ((error: unknown) => void) | undefined;
	#closeListener: ((reason?: unknown) => void) | undefined;

	send(payload: string): void {
		this.sent.push(payload);
	}

	close(): void {
		this.closeCalls += 1;
	}

	onMessage(listener: (payload: unknown) => void): void {
		this.#messageListener = listener;
	}

	onError(listener: (error: unknown) => void): void {
		this.#errorListener = listener;
	}

	onClose(listener: (reason?: unknown) => void): void {
		this.#closeListener = listener;
	}

	emitMessage(payload: unknown): void {
		this.#messageListener?.(payload);
	}

	emitError(error: unknown): void {
		this.#errorListener?.(error);
	}

	emitClose(reason?: unknown): void {
		this.#closeListener?.(reason);
	}
}

function factoryFor(sockets: FakeSocket[], seen: ElevenLabsTtsSocketFactoryOptions[] = []): ElevenLabsTtsSocketFactory {
	return (options) => {
		seen.push(options);
		const socket = new FakeSocket();
		sockets.push(socket);
		return socket;
	};
}

async function flushMicrotasks(): Promise<void> {
	await Promise.resolve();
	await Promise.resolve();
}

function wire(socket: FakeSocket): Array<Record<string, unknown>> {
	return socket.sent.map((payload) => JSON.parse(payload) as Record<string, unknown>);
}

function audioMessage(
	audio: Uint8Array,
	chars: string[],
	starts: number[],
	durations: number[],
): Record<string, unknown> {
	return {
		audio: Buffer.from(audio).toString("base64"),
		alignment: { chars, charStartTimesMs: starts, charDurationsMs: durations },
	};
}

async function openIterator(
	provider: ElevenLabsTtsProvider,
	signal: AbortSignal,
	sockets: FakeSocket[],
): Promise<{
	readonly iterator: AsyncIterator<TtsChunk>;
	readonly first: Promise<IteratorResult<TtsChunk>>;
}> {
	const iterator = provider.synthesize("hello", signal)[Symbol.asyncIterator]();
	const first = iterator.next();
	await flushMicrotasks();
	expect(sockets).toHaveLength(1);
	return { iterator, first };
}

describe("ElevenLabs Flash TTS provider", () => {
	test("opens with the fixed voice path and explicit protocol fields", async () => {
		const sockets: FakeSocket[] = [];
		const seen: ElevenLabsTtsSocketFactoryOptions[] = [];
		const controller = new AbortController();
		const provider = new ElevenLabsTtsProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets, seen) });
		const { iterator, first } = await openIterator(provider, controller.signal, sockets);

		expect(seen).toHaveLength(1);
		const options = seen[0];
		expect(options).toBeDefined();
		const url = new URL(options?.url ?? "");
		expect(url.pathname).toBe("/v1/text-to-speech/voice-fixed/stream-input");
		expect(url.searchParams.get("model_id")).toBe("eleven_flash_v2_5");
		expect(url.searchParams.get("output_format")).toBe("pcm_24000");
		expect(url.searchParams.get("sync_alignment")).toBe("true");
		expect(url.searchParams.get("inactivity_timeout")).toBe("45");
		expect(url.searchParams.get("apply_text_normalization")).toBe("on");
		expect(url.searchParams.has("language_code")).toBe(false);
		expect(options?.headers).toEqual({ "xi-api-key": API_KEY });
		expect(wire(sockets[0] as FakeSocket)).toEqual([
			{ text: " ", generation_config: { chunk_length_schedule: [100, 200, 400] } },
			{ text: "hello ", flush: true },
			{ text: "" },
		]);

		controller.abort();
		await expect(first).rejects.toMatchObject({ name: "AbortError" });
		expect((sockets[0] as FakeSocket).closeCalls).toBe(1);
	});

	test("requires alignment and preserves chunk-relative data for absolute accumulation", async () => {
		const sockets: FakeSocket[] = [];
		const controller = new AbortController();
		const provider = new ElevenLabsTtsProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		const { iterator, first: firstNext } = await openIterator(provider, controller.signal, sockets);
		(sockets[0] as FakeSocket).emitMessage(audioMessage(new Uint8Array([1, 2]), ["A", "B"], [0, 10], [10, 20]));
		const first = await firstNext;
		expect(first.done).toBe(false);
		if (!first.done) {
			expect(first.value).toEqual({
				kind: "audio",
				audio: new Uint8Array([1, 2]),
				chars: ["A", "B"],
				charStartMs: [0, 10],
				charDurationMs: [10, 20],
			});
		}

		const secondNext = iterator.next();
		(sockets[0] as FakeSocket).emitMessage(audioMessage(new Uint8Array([3]), ["C"], [0], [30]));
		const second = await secondNext;
		const finalNext = iterator.next();
		(sockets[0] as FakeSocket).emitMessage({ isFinal: true });
		const final = await finalNext;
		expect(final).toEqual({ value: { kind: "final" }, done: false });
		await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true });
		expect(second.done).toBe(false);
		expect((sockets[0] as FakeSocket).closeCalls).toBe(1);

		const { accumulateAlignment } = await import("@gajaeway/voice-core");
		if (!first.done && !second.done) {
			const timeline = accumulateAlignment([first.value, second.value, { kind: "final" }]);
			expect(timeline.text).toBe("ABC");
			expect(timeline.points.map((point) => point.startMs)).toEqual([0, 10, 30]);
			expect(timeline.points.map((point) => point.endMs)).toEqual([10, 30, 60]);
			expect(timeline.durationMs).toBe(60);
		}
	});

	test("fails the synthesis when an audio frame has missing or empty alignment", async () => {
		for (const alignment of [undefined, { chars: [], charStartTimesMs: [], charDurationsMs: [] }]) {
			const sockets: FakeSocket[] = [];
			const provider = new ElevenLabsTtsProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
			const { first: next } = await openIterator(provider, new AbortController().signal, sockets);
			(sockets[0] as FakeSocket).emitMessage({ audio: "AQI=", ...(alignment === undefined ? {} : { alignment }) });
			await expect(next).rejects.toBeInstanceOf(TtsAlignmentMissingError);
			expect((sockets[0] as FakeSocket).closeCalls).toBe(1);
		}
	});

	test("consumes isFinal as one terminal chunk and closes exactly once", async () => {
		const sockets: FakeSocket[] = [];
		const provider = new ElevenLabsTtsProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		const { iterator, first: next } = await openIterator(provider, new AbortController().signal, sockets);
		(sockets[0] as FakeSocket).emitMessage({ isFinal: true });
		expect(await next).toEqual({ value: { kind: "final" }, done: false });
		expect(await iterator.next()).toEqual({ value: undefined, done: true });
		(sockets[0] as FakeSocket).emitMessage({ isFinal: true });
		expect((sockets[0] as FakeSocket).closeCalls).toBe(1);
	});

	test("abort closes exactly once and drops queued server audio", async () => {
		const sockets: FakeSocket[] = [];
		const controller = new AbortController();
		const provider = new ElevenLabsTtsProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		const { iterator, first: next } = await openIterator(provider, controller.signal, sockets);
		controller.abort();
		controller.abort();
		(sockets[0] as FakeSocket).emitMessage(audioMessage(new Uint8Array([1]), ["A"], [0], [10]));
		await expect(next).rejects.toMatchObject({ name: "AbortError" });
		await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true });
		expect((sockets[0] as FakeSocket).closeCalls).toBe(1);
	});

	test("classifies known errors, fails closed for unknown codes, and redacts the key", async () => {
		for (const [code, kind, canonical] of [
			["rate_limited", "retryable", "rate_limited"],
			["transcriber_error", "retryable", "transcriber_error"],
			["auth_error", "fatal", "auth"],
			["quota_exceeded", "fatal", "quota_exceeded"],
		] as const) {
			const sockets: FakeSocket[] = [];
			const provider = new ElevenLabsTtsProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
			const { first: next } = await openIterator(provider, new AbortController().signal, sockets);
			(sockets[0] as FakeSocket).emitMessage({ message_type: "error", code, error: `detail-${code}` });
			await expect(next).rejects.toMatchObject({ kind, code: canonical });
			expect((sockets[0] as FakeSocket).closeCalls).toBe(1);
		}

		const sockets: FakeSocket[] = [];
		const provider = new ElevenLabsTtsProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		const { first: next } = await openIterator(provider, new AbortController().signal, sockets);
		(sockets[0] as FakeSocket).emitMessage({ message_type: "error", code: "future_provider_code", error: API_KEY });
		let error: ElevenLabsTtsProviderError | undefined;
		try {
			await next;
		} catch (caught) {
			error = caught instanceof ElevenLabsTtsProviderError ? caught : undefined;
		}
		expect(error?.kind).toBe("fatal");
		expect(error?.code).toBe("invalid_request");
		expect(error?.providerCode).toBe("future_provider_code");
		expect(error?.message).toContain("future_provider_code");
		expect(error?.message).not.toContain(API_KEY);
	});

	test("redacts the API key from socket-factory failures", async () => {
		const provider = new ElevenLabsTtsProvider({
			config,
			apiKey: API_KEY,
			socketFactory: () => {
				throw new Error(`connection failed for ${API_KEY}`);
			},
		});
		const iterator = provider.synthesize("hello", new AbortController().signal)[Symbol.asyncIterator]();
		let error: Error | undefined;
		try {
			await iterator.next();
		} catch (caught) {
			error = caught instanceof Error ? caught : new Error(String(caught));
		}
		expect(error).toBeDefined();
		expect(error?.message).not.toContain(API_KEY);
		expect(error?.message).toContain("[redacted]");
	});

	test("converts pcm_24000 mono chunks and exposes the absolute alignment timeline", async () => {
		const source = encodePcm16Le(new Int16Array([1_000, -1_000]));
		const chunks: TtsChunk[] = [
			{ kind: "audio", audio: source, chars: ["A", "B"], charStartMs: [0, 20], charDurationMs: [20, 20] },
			{ kind: "final" },
		];
		const provider: TtsProvider = {
			voiceId: "voice-fixed",
			outputFormat: "pcm_24000",
			synthesize: async function* (): AsyncIterable<TtsChunk> {
				yield* chunks;
			},
		};
		const result = await createTtsSynthesis({ provider }).synthesize("hello", new AbortController().signal);
		expect(result.kind).toBe("ok");
		if (result.kind !== "ok") return;
		expect(result.audio.byteLength).toBe(16);
		expect(Array.from(decodePcm16Le(result.audio))).toEqual([1_000, 1_000, 0, 0, -1_000, -1_000, -1_000, -1_000]);
		expect(result.timeline.text).toBe("AB");
		expect(result.timeline.points.map((point) => point.startMs)).toEqual([0, 20]);
		expect(result.timeline.durationMs).toBe(40);
	});

	test("keeps alignment failure as a text-fallback result without audio", async () => {
		const provider: TtsProvider = {
			voiceId: "voice-fixed",
			outputFormat: "pcm_24000",
			synthesize: (): AsyncIterable<TtsChunk> => ({
				[Symbol.asyncIterator]: () => ({
					next: () => Promise.reject(new TtsAlignmentMissingError()),
				}),
			}),
		};
		const result = await createTtsSynthesis({ provider }).synthesize("hello", new AbortController().signal);
		expect(result.kind).toBe("text_fallback");
		if (result.kind === "text_fallback") expect(result.error).toBeInstanceOf(TtsAlignmentMissingError);
	});

	test("exports fixed provider contract values", () => {
		expect(ELEVENLABS_TTS_PROVIDER_CONTRACT.defaultInactivityTimeoutSecs).toBe(20);
		expect(ELEVENLABS_TTS_PROVIDER_CONTRACT.maxInactivityTimeoutSecs).toBe(180);
		expect(ELEVENLABS_TTS_PROVIDER_CONTRACT.defaultOutputFormat).toBe("mp3");
		expect(ELEVENLABS_TTS_PROVIDER_CONTRACT.requiredOutputFormat).toBe("pcm_24000");
	});
});
