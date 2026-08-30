import { describe, expect, test } from "bun:test";
import type { SttEvent, SttFatalCode, SttRetryableCode } from "@gajaeway/voice-core";
import type { VoiceConfig } from "../src/config";
import {
	type ElevenLabsSttClock,
	ElevenLabsSttProvider,
	type ElevenLabsSttSocket,
	type ElevenLabsSttSocketFactory,
	type ElevenLabsSttSocketFactoryOptions,
} from "../src/voice/providers/elevenlabs-stt";

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
			keyterms: ["Gajae Way", "Scribe"],
		},
		tts: {
			model: "eleven_flash_v2_5",
			voiceId: "voice-1",
			outputFormat: "pcm_24000",
			syncAlignment: true,
			inactivityTimeoutSecs: 20,
			applyTextNormalization: "auto",
			chunkLengthSchedule: [120, 160, 250, 300],
		},
	},
};

class FakeClock implements ElevenLabsSttClock {
	#nowMs = 1_234;

	now = (): number => this.#nowMs;
	setTimeout = (callback: () => void, delayMs: number): unknown => setTimeout(callback, delayMs);
	clearTimeout = (handle: unknown): void => clearTimeout(handle as ReturnType<typeof setTimeout>);
}

class FakeSocket implements ElevenLabsSttSocket {
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

function factoryFor(sockets: FakeSocket[], seen: ElevenLabsSttSocketFactoryOptions[] = []): ElevenLabsSttSocketFactory {
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

describe("ElevenLabs Scribe v2 realtime STT provider", () => {
	test("opens with the configured protocol fields and derived VAD knobs without a language field", async () => {
		const sockets: FakeSocket[] = [];
		const seen: ElevenLabsSttSocketFactoryOptions[] = [];
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets, seen) });

		await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, () => {});

		expect(seen).toHaveLength(1);
		const options = seen[0];
		expect(options).toBeDefined();
		const url = new URL(options?.url ?? "");
		expect(url.searchParams.get("model_id")).toBe("scribe_v2_realtime");
		expect(url.searchParams.get("audio_format")).toBe("pcm_16000");
		expect(url.searchParams.get("commit_strategy")).toBe("vad");
		expect(url.searchParams.get("vad_silence_threshold_secs")).toBe("0.7");
		expect(url.searchParams.get("min_speech_duration_ms")).toBe("300");
		expect(url.searchParams.get("min_silence_duration_ms")).toBe("700");
		expect(url.searchParams.get("include_language_detection")).toBe("true");
		expect(url.searchParams.get("filter_background_audio")).toBe("true");
		expect(url.searchParams.getAll("keyterms")).toEqual(["Gajae Way", "Scribe"]);
		expect(url.searchParams.has("language_code")).toBe(false);
		expect(options?.headers).toEqual({ "xi-api-key": API_KEY });
	});

	test("sends previous_text only on the first audio chunk of a reconnect-opened stream", async () => {
		const sockets: FakeSocket[] = [];
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });

		const initial = await provider.open(
			{ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 },
			() => {},
		);
		initial.push(new Uint8Array([1, 2]));
		await flushMicrotasks();
		const reconnect = await provider.open(
			{ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20, previousText: "last committed" },
			() => {},
		);
		reconnect.push(new Uint8Array([3, 4]));
		reconnect.push(new Uint8Array([5, 6]));
		await flushMicrotasks();

		const initialMessages = wire(sockets[0] as FakeSocket);
		const reconnectMessages = wire(sockets[1] as FakeSocket);
		expect(initialMessages[0]?.previous_text).toBeUndefined();
		expect(reconnectMessages[0]?.previous_text).toBe("last committed");
		expect(reconnectMessages[1]?.previous_text).toBeUndefined();
	});

	test("maps session-open, partial, and committed messages, including detected language", async () => {
		const sockets: FakeSocket[] = [];
		const events: SttEvent[] = [];
		const clock = new FakeClock();
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets), clock });
		await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, (event) =>
			events.push(event),
		);
		const socket = sockets[0] as FakeSocket;

		socket.emitMessage({ message_type: "session_started", session_id: "session-1" });
		socket.emitMessage(JSON.stringify({ message_type: "partial_transcript", text: "hello" }));
		socket.emitMessage({ message_type: "committed_transcript", text: "hello world", language_code: "en" });

		expect(events).toEqual([
			{ kind: "ready", sessionId: "session-1" },
			{ kind: "partial", text: "hello", atMs: 1_234 },
			{ kind: "committed", text: "hello world", atMs: 1_234, languageCode: "en" },
		]);
	});

	const retryableCases: Array<[string, SttRetryableCode]> = [
		["rate_limited", "rate_limited"],
		["commit_throttled", "commit_throttled"],
		["queue_overflow", "queue_overflow"],
		["transcriber_error", "transcriber_error"],
		["session_time_limit_exceeded", "session_time_limit_exceeded"],
		["insufficient_audio_activity", "insufficient_audio_activity"],
		["network", "network"],
	];

	test.each(retryableCases)("classifies %s as retryable", async (messageType, expectedCode) => {
		const sockets: FakeSocket[] = [];
		const events: SttEvent[] = [];
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, (event) =>
			events.push(event),
		);
		(sockets[0] as FakeSocket).emitMessage({ message_type: messageType, error: `detail-${messageType}` });

		expect(events).toEqual([{ kind: "retryable", code: expectedCode, message: `detail-${messageType}` }]);
	});

	const fatalCases: Array<[string, SttFatalCode, string]> = [
		["auth_error", "auth", "auth_error"],
		["quota_exceeded", "quota_exceeded", "quota_exceeded"],
		["unaccepted_terms", "unaccepted_terms", "unaccepted_terms"],
		["resource_exhausted", "resource_exhausted", "resource_exhausted"],
		["invalid_request", "invalid_request", "invalid_request"],
		["input_error", "input_error", "input_error"],
		["chunk_size_exceeded", "chunk_size_exceeded", "chunk_size_exceeded"],
	];

	test.each(fatalCases)("classifies %s as fatal", async (messageType, expectedCode, detailCode) => {
		const sockets: FakeSocket[] = [];
		const events: SttEvent[] = [];
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, (event) =>
			events.push(event),
		);
		(sockets[0] as FakeSocket).emitMessage({ message_type: messageType, error: detailCode });

		expect(events).toEqual([{ kind: "fatal", code: expectedCode, message: detailCode }]);
	});

	test("fails closed for an unknown provider error code and preserves that raw code in the message", async () => {
		const sockets: FakeSocket[] = [];
		const events: SttEvent[] = [];
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, (event) =>
			events.push(event),
		);
		(sockets[0] as FakeSocket).emitMessage({ message_type: "error", error: "future_provider_code" });

		expect(events).toHaveLength(1);
		const event = events[0];
		expect(event?.kind).toBe("fatal");
		if (event?.kind === "fatal") {
			expect(event.code).toBe("invalid_request");
			expect(event.message).toContain("future_provider_code");
		}
	});

	test("bounds pending audio with drop_oldest admission and counters", async () => {
		const sockets: FakeSocket[] = [];
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		const stream = await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 2 }, () => {});

		expect(stream.push(new Uint8Array([1]))).toBe("accepted");
		expect(stream.push(new Uint8Array([2]))).toBe("accepted");
		expect(stream.push(new Uint8Array([3]))).toBe("dropped_oldest");
		expect(stream.queuedFrames).toBe(2);
		expect(stream.droppedFrames).toBe(1);
		await flushMicrotasks();
		expect(wire(sockets[0] as FakeSocket).map((message) => message.audio_base_64)).toEqual(["Ag==", "Aw=="]);
	});

	test("commits only for logical boundaries and never on ordinary audio frames", async () => {
		const sockets: FakeSocket[] = [];
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		const stream = await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, () => {});
		stream.push(new Uint8Array([1, 2, 3]));
		stream.commit("teardown");
		stream.commit("speaker_left");
		stream.commit("idle");
		await flushMicrotasks();

		const messages = wire(sockets[0] as FakeSocket);
		expect(messages.map((message) => message.commit)).toEqual([false, true, true, true]);
		expect(messages.every((message) => message.message_type === "input_audio_chunk")).toBe(true);
	});

	test("close is idempotent and push is rejected after close", async () => {
		const sockets: FakeSocket[] = [];
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		const stream = await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, () => {});
		stream.commit("teardown");

		await Promise.all([stream.close("teardown"), stream.close("speaker_left"), stream.close("idle")]);
		expect((sockets[0] as FakeSocket).closeCalls).toBe(1);
		expect(stream.push(new Uint8Array([1]))).toBe("rejected_closed");
	});

	test("never leaks the API key through a thrown or emitted error", async () => {
		const throwingFactory: ElevenLabsSttSocketFactory = () => {
			throw new Error(`connection failed for ${API_KEY}`);
		};
		const provider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: throwingFactory });

		let thrown: Error | undefined;
		try {
			await provider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, () => {});
		} catch (error) {
			thrown = error instanceof Error ? error : new Error(String(error));
		}
		expect(thrown?.message).not.toContain(API_KEY);

		const sockets: FakeSocket[] = [];
		const events: SttEvent[] = [];
		const safeProvider = new ElevenLabsSttProvider({ config, apiKey: API_KEY, socketFactory: factoryFor(sockets) });
		await safeProvider.open({ speakerId: "speaker-1", sampleRateHz: 16_000, maxQueuedFrames: 20 }, (event) =>
			events.push(event),
		);
		(sockets[0] as FakeSocket).emitMessage({ message_type: "error", code: "unknown", error: API_KEY });
		const event = events[0];
		expect(event?.kind).toBe("fatal");
		if (event?.kind === "fatal") expect(event.message).not.toContain(API_KEY);
	});
});
