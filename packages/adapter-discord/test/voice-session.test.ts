import { describe, expect, test } from "bun:test";
import type { SttProvider, SttStream } from "@gajaeway/voice-core";
import type { VoiceConfig } from "../src/config";
import {
	type VoiceClock,
	type VoiceCloseReason,
	type VoiceConnectionLike,
	type VoiceDrillLogSink,
	type VoicePlayerLike,
	VoiceRejoinRequiredError,
	VoiceRoomSession,
	type VoiceRoomSessionOptions,
	VoiceSessionManager,
} from "../src/voice/session";

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
	ingress: { maxQueuedFramesPerSpeaker: 200, overflow: "drop_oldest" },
	reconnect: { initialBackoffMs: 500, maxBackoffMs: 30_000, maxAttempts: 6 },
	joinCommandAllowlist: ["owner"],
	announceOnTurnStart: true,
	drillLog: { enabled: true, path: "artifacts" },
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

class FakeStream implements SttStream {
	readonly events: string[];
	readonly #pushResult: "accepted" | "dropped_oldest" = "accepted";
	closed = false;
	queuedFrames = 0;
	droppedFrames = 0;

	constructor(events: string[], pushResult: "accepted" | "dropped_oldest" = "accepted") {
		this.events = events;
		this.#pushResult = pushResult;
	}

	push(_pcm16: Uint8Array): "accepted" | "dropped_oldest" | "rejected_closed" {
		if (this.closed) return "rejected_closed";
		if (this.#pushResult === "dropped_oldest") this.droppedFrames += 1;
		return this.#pushResult;
	}

	commit(reason: "teardown" | "speaker_left" | "idle"): void {
		this.events.push(`stream.commit:${reason}`);
	}

	async close(reason: string): Promise<void> {
		this.events.push(`stream.close:${reason}`);
		this.closed = true;
	}
}

function providerFor(events: string[], streams: FakeStream[]): SttProvider {
	return {
		supportsStreamingPartials: true,
		open: async () => {
			const stream = new FakeStream(events);
			streams.push(stream);
			return stream;
		},
	};
}

function createSession(overrides: Partial<VoiceRoomSessionOptions> = {}): {
	session: VoiceRoomSession;
	clock: FakeClock;
	events: string[];
	streams: FakeStream[];
} {
	const events: string[] = [];
	const streams: FakeStream[] = [];
	const clock = new FakeClock();
	const connection: VoiceConnectionLike = {
		destroy: () => {
			events.push("connection.destroy");
		},
	};
	const player: VoicePlayerLike = {
		abort: () => {
			events.push("player.abort");
		},
	};
	const sink: VoiceDrillLogSink = {
		write: () => {
			events.push("drill.write");
		},
		flush: () => {
			events.push("drill.flush");
		},
		close: () => {
			events.push("drill.close");
		},
	};
	const session = new VoiceRoomSession({
		config,
		originKey: "discord/channel/room-1",
		channelId: "room-1",
		botUserId: "bot",
		connectionFactory: async () => connection,
		playerFactory: async () => player,
		sttProvider: providerFor(events, streams),
		clock,
		drillLogSink: sink,
		...overrides,
	});
	return { session, clock, events, streams };
}

async function openSession(): Promise<ReturnType<typeof createSession>> {
	const setup = createSession();
	await setup.session.join();
	return setup;
}

async function settleAsync(): Promise<void> {
	await Promise.resolve();
	await Promise.resolve();
}

describe("VoiceRoomSession lifecycle", () => {
	test("close is idempotent, keeps the first reason, and performs ordered teardown", async () => {
		const { session, events, streams } = await openSession();
		const stream = await session.openSpeaker("speaker-1", {
			subscription: {
				unsubscribe: () => {
					events.push("subscription.unsubscribe");
				},
			},
			decoder: {
				release: () => {
					events.push("decoder.release");
				},
			},
		});
		expect(stream).toBe(streams[0]);
		session.setCurrentSynthesis({
			abort: () => {
				events.push("synthesis.abort");
			},
		});
		session.registerModality("turn-1", "voice");
		session.admitTurn({ messageId: "message-1", turnId: "turn-1", modality: "voice", acceptedAtMs: 0 });
		const first = session.close("command");
		const second = session.close("shutdown");
		expect(first).toBe(second);
		await Promise.all([first, second]);
		expect(session.closeReason).toBe("command");
		expect(events).toEqual([
			"player.abort",
			"synthesis.abort",
			"stream.commit:teardown",
			"stream.close:teardown",
			"subscription.unsubscribe",
			"decoder.release",
			"connection.destroy",
			"drill.flush",
			"drill.close",
		]);
		expect(session.state).toBe("closed");
		expect(session.activeStreams).toBe(0);
		expect(session.liveTimerCount).toBe(0);
		expect(session.reopened).toBe(false);
		expect(session.modality.size).toBe(0);
		expect(session.turnBook.isBusy()).toBe(false);
	});

	test("two different callers racing close tear down once and preserve the first reason", async () => {
		const { session, events } = await openSession();
		await session.openSpeaker("speaker-1");
		const first = session.close("gateway_lost");
		const second = session.close("kicked");
		await Promise.all([first, second]);
		expect(session.closeReason).toBe("gateway_lost");
		expect(events.filter((event) => event === "player.abort")).toHaveLength(1);
		expect(events.filter((event) => event === "connection.destroy")).toHaveLength(1);
	});

	test("push on a stream that was open before close is rejected", async () => {
		const { session } = await openSession();
		await session.openSpeaker("speaker-1");
		await session.close("empty");
		expect(session.push("speaker-1", new Uint8Array([1]))).toBe("rejected_closed");
	});

	test("all nine close triggers map to their typed reasons", async () => {
		const triggers: readonly [string, (session: VoiceRoomSession) => void, VoiceCloseReason][] = [
			["kicked", (session) => session.handleVoiceStateChange({ userId: "bot", newChannelId: null }), "kicked"],
			["connection", (session) => session.handleConnectionState("Disconnected"), "connection_lost"],
			["provider", (session) => session.handleProviderFatal(), "provider_fatal"],
			["gateway", (session) => session.handleGatewayLost(), "gateway_lost"],
			["shutdown", (session) => void session.close("shutdown"), "shutdown"],
			[
				"empty",
				(session) =>
					session.handleVoiceStateChange({ userId: "speaker-1", newChannelId: null, members: [{ id: "bot" }] }),
				"empty",
			],
			["idle", (session) => session.idleTimerTick(), "idle"],
			["session max", (session) => session.sessionMaxTimerTick(), "session_max"],
			["command", (session) => void session.close("command"), "command"],
		];
		for (const [name, trigger, expected] of triggers) {
			const { session } = await openSession();
			if (name === "idle") {
				const setup = createSession({ config: { ...config, idleLeaveMs: 30_000 } });
				await setup.session.join();
				setup.clock.advance(30_000);
				await settleAsync();
				expect(setup.session.closeReason).toBe(expected);
				continue;
			}
			if (name === "session max") {
				const setup = createSession({ config: { ...config, sessionMaxMs: 60_000 } });
				await setup.session.join();
				setup.clock.advance(60_000);
				await settleAsync();
				expect(setup.session.closeReason).toBe(expected);
				continue;
			}
			trigger(session);
			await settleAsync();
			expect(session.closeReason).toBe(expected);
			expect(session.activeStreams).toBe(0);
		}
	});

	test("idle and session maximum timers close at their inclusive boundaries", async () => {
		const setup = createSession({ config: { ...config, idleLeaveMs: 30_000, sessionMaxMs: 60_000 } });
		await setup.session.join();
		setup.clock.advance(29_999);
		expect(setup.session.state).toBe("active");
		setup.clock.advance(1);
		await setup.session.close("idle");
		expect(setup.session.closeReason).toBe("idle");
		expect(setup.session.liveTimerCount).toBe(0);

		const max = createSession({ config: { ...config, idleLeaveMs: 300_000, sessionMaxMs: 60_000 } });
		await max.session.join();
		max.clock.advance(59_999);
		expect(max.session.state).toBe("active");
		max.clock.advance(1);
		await max.session.close("session_max");
		expect(max.session.closeReason).toBe("session_max");
	});

	test("provider fatal stays latched until an explicit manager rejoin", async () => {
		let providerOpens = 0;
		const setup = createSession();
		const manager = new VoiceSessionManager({
			config,
			clock: setup.clock,
			connectionFactory: async () => ({ destroy: () => {} }),
			playerFactory: async () => ({ abort: () => {} }),
			sttProvider: {
				supportsStreamingPartials: true,
				open: async () => {
					providerOpens += 1;
					return new FakeStream([]);
				},
			},
		});
		const first = await manager.join({ originKey: "discord/channel/room-1", channelId: "room-1" });
		await first.openSpeaker("speaker-1");
		await first.close("provider_fatal");
		expect(first.fatalUntilRejoin).toBe(true);
		await expect(manager.join({ originKey: "discord/channel/room-1", channelId: "room-1" })).rejects.toBeInstanceOf(
			VoiceRejoinRequiredError,
		);
		const reopened = await manager.rejoin({ originKey: "discord/channel/room-1", channelId: "room-1" });
		expect(reopened).not.toBe(first);
		await reopened.openSpeaker("speaker-1");
		expect(providerOpens).toBe(2);
	});
});
