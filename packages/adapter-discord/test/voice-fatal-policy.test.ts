import { describe, expect, test } from "bun:test";
import type { SttEvent, SttProvider, SttStream } from "@gajaeway/voice-core";
import type { VoiceConfig } from "../src/config";
import { settleDiscordDelivery } from "../src/main";
import { type VoiceProviderFatalInfo, VoiceRoomSession } from "../src/voice/session";

const config = {
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
	joinCommandAllowlist: [],
	announceOnTurnStart: true,
	drillLog: { enabled: false, path: "artifacts" },
	elevenlabs: {
		apiKeyFile: "/secrets/key",
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
			voiceId: "fixed",
			outputFormat: "pcm_24000",
			syncAlignment: true,
			inactivityTimeoutSecs: 20,
			applyTextNormalization: "auto",
			chunkLengthSchedule: [120],
		},
	},
} as unknown as VoiceConfig;

function sessionWith(onProviderFatal: (info: VoiceProviderFatalInfo) => void) {
	const opened: string[] = [];
	const closedStreams: string[] = [];
	let sink: ((event: SttEvent) => void) | undefined;
	const stream: SttStream = {
		push: () => "accepted",
		queuedFrames: 0,
		droppedFrames: 0,
		commit: () => {},
		close: async (reason) => void closedStreams.push(reason),
	};
	const sttProvider: SttProvider = {
		supportsStreamingPartials: true,
		open: async (opts, eventSink) => {
			opened.push(opts.speakerId);
			sink = eventSink;
			return stream;
		},
	};
	let destroyed = 0;
	const session = new VoiceRoomSession({
		config,
		originKey: "discord/channel/c1",
		channelId: "c1",
		guildId: "g1",
		connectionFactory: () => ({
			destroy: () => {
				destroyed += 1;
			},
		}),
		playerFactory: () => ({ abort: () => {} }),
		sttProvider,
		onProviderFatal,
	});
	return { session, opened, closedStreams, emit: (event: SttEvent) => sink?.(event), destroyedCount: () => destroyed };
}

describe("vendor fatal policy", () => {
	test("a fatal provider error closes voice once with one visible diagnostic and latches until rejoin", async () => {
		const diagnostics: VoiceProviderFatalInfo[] = [];
		const live = sessionWith((info) => void diagnostics.push(info));
		await live.session.join();
		await live.session.openSpeaker("u1");
		expect(live.opened).toEqual(["u1"]);

		live.emit({ kind: "fatal", code: "quota_exceeded", message: "no quota left" });
		await live.session.close("provider_fatal");

		expect(live.session.state).toBe("closed");
		expect(live.session.fatalUntilRejoin).toBe(true);
		expect(live.destroyedCount()).toBe(1);
		// Exactly one diagnostic, carrying the provider code and no credential material.
		expect(diagnostics).toHaveLength(1);
		expect(diagnostics[0]?.code).toBe("quota_exceeded");
		expect(diagnostics[0]?.channelId).toBe("c1");
		expect(JSON.stringify(diagnostics[0])).not.toContain("apiKey");

		// A latched room refuses to reopen a provider stream until an explicit rejoin.
		expect(await live.session.openSpeaker("u2")).toBeUndefined();
		expect(live.opened).toEqual(["u1"]);

		// Repeated closes never produce a second diagnostic.
		await live.session.close("provider_fatal");
		expect(diagnostics).toHaveLength(1);

		expect(live.session.rejoin()).toBe(true);
		expect(live.session.fatalUntilRejoin).toBe(false);
	});

	test("the text delivery path keeps working after voice has failed, with no provider involved", async () => {
		const sent: string[] = [];
		const requests: string[] = [];
		const live = sessionWith(() => {});
		await live.session.join();
		live.emit({ kind: "fatal", code: "auth", message: "bad key" });
		await live.session.close("provider_fatal");
		expect(live.session.state).toBe("closed");

		await settleDiscordDelivery(
			{ request: (async (verb: string) => void requests.push(verb)) as never },
			{ channels: { fetch: async () => ({ send: async (text: string) => void sent.push(text) }) } },
			{
				turnId: "t-text",
				origin: { platform: "discord", kind: "channel", conversationId: "c1" },
				role: "assistant",
				text: "still here",
				final: true,
				deliveryId: "d-text",
			} as never,
		);

		expect(sent).toEqual(["still here"]);
		expect(requests).toEqual(["delivery.confirm"]);
	});
});
