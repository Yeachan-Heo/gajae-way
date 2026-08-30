import { expect, test } from "bun:test";
import type { SttEvent, SttProvider, SttStream } from "@gajaeway/voice-core";
import type { VoiceConfig } from "../src/config";
import type {
	VoiceAudioSubscriptionLike,
	VoiceReceiveDecoderLike,
	VoiceReceiverLike,
	VoiceSpeakingLike,
} from "../src/voice/receive";
import {
	type VoiceClock,
	type VoiceConnectionLike,
	type VoicePlayerLike,
	type VoiceReceiverFactory,
	VoiceRoomSession,
	type VoiceRoomSessionOptions,
} from "../src/voice/session";

const config: VoiceConfig = {
	enabled: true,
	silenceEndMs: 700,
	energyGate: { minDurationMs: 300, rmsThreshold: 0.001 },
	idleLeaveMs: 300_000,
	unread: { maxItems: 20, maxCharsPerItem: 200 },
	mergeWindowMs: 1_500,
	transcriptWaitMs: 1_500,
	sessionMaxMs: 3_600_000,
	turnMapTtlMs: 900_000,
	outstanding: { ttlMs: 900_000, maxEntries: 32 },
	bargeIn: { minTranscriptChars: 3, cooldownMs: 1_500, echoSimilarity: 0.6, frameCorrectionMs: 20 },
	ingress: { maxQueuedFramesPerSpeaker: 2, overflow: "drop_oldest" },
	reconnect: { initialBackoffMs: 100, maxBackoffMs: 250, maxAttempts: 2 },
	joinCommandAllowlist: [],
	announceOnTurnStart: true,
	drillLog: { enabled: false, path: "artifacts" },
	elevenlabs: {
		apiKeyFile: "key",
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
			voiceId: "voice-1",
			outputFormat: "pcm_24000",
			syncAlignment: true,
			inactivityTimeoutSecs: 20,
			applyTextNormalization: "auto",
			chunkLengthSchedule: [120, 160, 250, 300],
		},
	},
};

class FakeClock implements VoiceClock {
	#nowMs = 0;
	#nextId = 0;
	readonly #timers = new Map<number, { readonly atMs: number; readonly callback: () => void }>();

	now(): number {
		return this.#nowMs;
	}

	setTimeout(callback: () => void, delayMs: number): number {
		const id = this.#nextId++;
		this.#timers.set(id, { atMs: this.#nowMs + delayMs, callback });
		return id;
	}

	clearTimeout(handle: unknown): void {
		if (typeof handle === "number") this.#timers.delete(handle);
	}

	advance(ms: number): void {
		this.#nowMs += ms;
		while (true) {
			const due = [...this.#timers.entries()]
				.filter(([, timer]) => timer.atMs <= this.#nowMs)
				.sort((left, right) => left[1].atMs - right[1].atMs)[0];
			if (due === undefined) return;
			this.#timers.delete(due[0]);
			due[1].callback();
		}
	}
}

class FakeSpeaking implements VoiceSpeakingLike {
	readonly #listeners = new Map<"start" | "end", Set<(speakerId: string) => void>>([
		["start", new Set()],
		["end", new Set()],
	]);

	on(event: "start" | "end", listener: (speakerId: string) => void): void {
		this.#listeners.get(event)?.add(listener);
	}

	off(event: "start" | "end", listener: (speakerId: string) => void): void {
		this.#listeners.get(event)?.delete(listener);
	}

	emit(event: "start" | "end", speakerId: string): void {
		for (const listener of this.#listeners.get(event) ?? []) listener(speakerId);
	}
}

class FakeAudioStream implements VoiceAudioSubscriptionLike {
	readonly #queue: Uint8Array[] = [];
	readonly #waiters: Array<(result: IteratorResult<Uint8Array>) => void> = [];
	#ended = false;

	push(packet: Uint8Array): void {
		if (this.#ended) return;
		const waiter = this.#waiters.shift();
		if (waiter === undefined) this.#queue.push(packet);
		else waiter({ done: false, value: packet });
	}

	destroy(): void {
		this.#ended = true;
		for (const waiter of this.#waiters.splice(0)) waiter({ done: true, value: undefined });
	}

	async return(): Promise<IteratorResult<Uint8Array>> {
		this.destroy();
		return { done: true, value: undefined };
	}

	[Symbol.asyncIterator](): AsyncIterator<Uint8Array> {
		return this;
	}

	next(): Promise<IteratorResult<Uint8Array>> {
		const value = this.#queue.shift();
		if (value !== undefined) return Promise.resolve({ done: false, value });
		if (this.#ended) return Promise.resolve({ done: true, value: undefined });
		return new Promise((resolve) => this.#waiters.push(resolve));
	}
}

class FakeReceiver implements VoiceReceiverLike {
	readonly speaking = new FakeSpeaking();
	readonly streams = new Map<string, FakeAudioStream>();

	subscribe(speakerId: string): FakeAudioStream {
		const stream = new FakeAudioStream();
		this.streams.set(speakerId, stream);
		return stream;
	}
}

class FakeDecoder implements VoiceReceiveDecoderLike {
	decode(): Uint8Array {
		const pcm = new Uint8Array(6);
		const view = new DataView(pcm.buffer);
		for (let index = 0; index < 3; index += 1) view.setInt16(index * 2, 100, true);
		return pcm;
	}
}

class FakeSttStream implements SttStream {
	closed = false;
	readonly commits: string[] = [];
	readonly closeReasons: string[] = [];
	readonly frames: Uint8Array[] = [];

	get queuedFrames(): number {
		return this.frames.length;
	}

	get droppedFrames(): number {
		return 0;
	}

	push(frame: Uint8Array): "accepted" | "dropped_oldest" | "rejected_closed" {
		if (this.closed) return "rejected_closed";
		this.frames.push(frame);
		return "accepted";
	}

	commit(reason: "teardown" | "speaker_left" | "idle"): void {
		this.commits.push(reason);
	}

	async close(reason: string): Promise<void> {
		this.closeReasons.push(reason);
		this.closed = true;
	}
}

class FakeProvider implements SttProvider {
	readonly supportsStreamingPartials = true as const;
	readonly streams: FakeSttStream[] = [];
	readonly sinks: Array<(event: SttEvent) => void> = [];
	readonly opens: Array<{ readonly speakerId: string; readonly previousText?: string }> = [];

	async open(
		options: {
			readonly speakerId: string;
			readonly sampleRateHz: number;
			readonly maxQueuedFrames: number;
			readonly previousText?: string;
		},
		sink: (event: SttEvent) => void,
	): Promise<SttStream> {
		this.opens.push({
			speakerId: options.speakerId,
			...(options.previousText === undefined ? {} : { previousText: options.previousText }),
		});
		this.sinks.push(sink);
		const stream = new FakeSttStream();
		this.streams.push(stream);
		return stream;
	}
}

function createSetup(overrides: Partial<VoiceRoomSessionOptions> = {}): {
	session: VoiceRoomSession;
	clock: FakeClock;
	receiver: FakeReceiver;
	provider: FakeProvider;
} {
	const clock = new FakeClock();
	const receiver = new FakeReceiver();
	const provider = new FakeProvider();
	const connection: VoiceConnectionLike = { destroy: () => {} };
	const player: VoicePlayerLike = { abort: () => {} };
	const receiverFactory: VoiceReceiverFactory = () => receiver;
	const session = new VoiceRoomSession({
		config,
		channelId: "room-1",
		botUserId: "bot",
		connectionFactory: async () => connection,
		playerFactory: async () => player,
		sttProvider: provider,
		clock,
		receiverFactory,
		decoderFactory: () => new FakeDecoder(),
		...overrides,
	});
	return { session, clock, receiver, provider };
}

async function settle(): Promise<void> {
	await Promise.resolve();
	await Promise.resolve();
	await Promise.resolve();
}

const packet = new Uint8Array([1]);

test("opens on first speech, closes only the leaving speaker, and tears down remaining streams", async () => {
	const setup = createSetup();
	await setup.session.join();
	setup.receiver.speaking.emit("start", "speaker-1");
	setup.receiver.speaking.emit("start", "speaker-2");
	await settle();
	expect(setup.session.activeStreams).toBe(2);

	setup.session.handleVoiceStateChange({
		userId: "speaker-1",
		newChannelId: null,
		members: [{ id: "bot" }, { id: "speaker-2" }],
	});
	await settle();
	expect(setup.session.activeStreams).toBe(1);
	expect(setup.provider.streams[0]?.commits).toEqual(["speaker_left"]);
	expect(setup.provider.streams[1]?.commits).toEqual([]);

	await setup.session.close("command");
	expect(setup.provider.streams[1]?.commits).toEqual(["teardown"]);
	expect(setup.session.activeStreams).toBe(0);
});

test("reconnects close-before-open with exponential capped delays and drops ingress while detached", async () => {
	const ingress: Array<{ readonly ingress: string; readonly admission: string }> = [];
	const setup = createSetup({ onIngress: (event) => ingress.push(event) });
	await setup.session.join();
	setup.receiver.speaking.emit("start", "speaker-1");
	await settle();
	expect(setup.provider.opens).toHaveLength(1);

	setup.provider.sinks[0]?.({ kind: "retryable", code: "network", message: "temporary" });
	await settle();
	expect(setup.provider.streams[0]?.closeReasons).toEqual(["retryable"]);
	setup.receiver.streams.get("speaker-1")?.push(packet);
	await settle();
	expect(ingress.at(-1)).toMatchObject({ ingress: "dropped", admission: "rejected_closed" });

	setup.clock.advance(100);
	await settle();
	expect(setup.provider.opens).toHaveLength(2);
	expect(setup.provider.opens[1]).toEqual({ speakerId: "speaker-1" });

	setup.provider.sinks[1]?.({ kind: "retryable", code: "network", message: "temporary" });
	await settle();
	setup.clock.advance(200);
	await settle();
	expect(setup.provider.opens).toHaveLength(3);
	expect(setup.provider.sinks[2]).toBeDefined();

	setup.provider.sinks[2]?.({ kind: "retryable", code: "network", message: "temporary" });
	await settle();
	expect(setup.session.closeReason).toBe("provider_fatal");
	expect(setup.session.activeStreams).toBe(0);
	expect(setup.provider.opens).toHaveLength(3);
});

test("fatal closes the session once and prevents any later stream reopen", async () => {
	const setup = createSetup();
	await setup.session.join();
	setup.receiver.speaking.emit("start", "speaker-1");
	await settle();
	setup.provider.sinks[0]?.({ kind: "fatal", code: "auth", message: "bad credentials" });
	await settle();
	await setup.session.close("command");
	expect(setup.session.closeReason).toBe("provider_fatal");
	expect(setup.session.activeStreams).toBe(0);
	setup.clock.advance(10_000);
	await settle();
	expect(setup.provider.opens).toHaveLength(1);
});
