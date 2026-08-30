import { describe, expect, test } from "bun:test";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { VoiceDrillRecord } from "@gajaeway/voice-core";
import type { LoadedDiscordAdapterConfig, VoiceConfig } from "../src/config";
import { ReconnectingGateway } from "../src/main";
import { createDrillLogSink, createVoiceRuntime } from "../src/voice/runtime";

const voiceConfig = (overrides: Partial<VoiceConfig> = {}): VoiceConfig =>
	({
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
		drillLog: { enabled: true, path: "artifacts" },
		elevenlabs: {
			apiKeyFile: "/secrets/elevenlabs-key",
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
				voiceId: "fixed-voice",
				outputFormat: "pcm_24000",
				syncAlignment: true,
				inactivityTimeoutSecs: 20,
				applyTextNormalization: "auto",
				chunkLengthSchedule: [120, 160, 250, 290],
			},
		},
		...overrides,
	}) as VoiceConfig;

const loaded = (voice: VoiceConfig | undefined, apiKey?: string): LoadedDiscordAdapterConfig =>
	({
		tokenFile: "/secrets/discord-token",
		token: "discord-token",
		configPath: "/state/adapter-discord.json",
		...(voice ? { voice } : {}),
		...(apiKey === undefined ? {} : { elevenLabsApiKey: apiKey }),
	}) as LoadedDiscordAdapterConfig;

const guildSource = { guilds: { fetch: async () => ({ id: "g1", voiceAdapterCreator: (() => {}) as never }) } };

const drillRecord = (utteranceId: string): VoiceDrillRecord =>
	({
		schema: "voice-drill-log.v1",
		utteranceId,
		ts: 1_700_000_000_000,
		speakerUserId: "u1",
		speakerDisplayName: "Speaker",
		rawTranscript: "hello",
		referenceTranscript: null,
		textSource: "committed",
		events: [{ kind: "partial", ts: 1_699_999_999_000 }],
		energyRms: 0.1,
		energyThreshold: 0.02,
		energyHoldMs: 300,
		energyGatePassed: true,
		boundary: "silence_end",
		boundaryAtMs: 700,
		mergedInto: null,
		turnId: "turn-1",
		ingress: "recorded",
		playback: "none",
		bargeIn: {
			decision: "continue",
			audioPassed: false,
			transcriptChars: 5,
			echoSimilarity: 0,
			cooldownActive: false,
			consumedMs: 0,
			truncationMs: 0,
		},
		detectedLanguage: null,
	}) as VoiceDrillRecord;

describe("createVoiceRuntime", () => {
	test("stays absent when no voice block is configured, leaving a text-only adapter", () => {
		expect(createVoiceRuntime(loaded(undefined), guildSource)).toBeUndefined();
	});

	test("stays absent when voice is explicitly disabled", () => {
		expect(createVoiceRuntime(loaded(voiceConfig({ enabled: false }), "key"), guildSource)).toBeUndefined();
	});

	test("fails startup instead of silently degrading when the credential produced no key", () => {
		expect(() => createVoiceRuntime(loaded(voiceConfig()), guildSource)).toThrow(/produced no key/);
		expect(() => createVoiceRuntime(loaded(voiceConfig(), "   "), guildSource)).toThrow(/produced no key/);
	});

	test("builds a session manager bound to the configured voice settings", () => {
		const runtime = createVoiceRuntime(loaded(voiceConfig(), "key"), guildSource);
		expect(runtime?.sessions.config.silenceEndMs).toBe(700);
		expect(runtime?.sessions.get("discord/channel/absent")).toBeUndefined();
	});
});

describe("createDrillLogSink", () => {
	test("appends one parseable JSON record per line", async () => {
		const directory = await mkdtemp(join(tmpdir(), "voice-drill-"));
		try {
			const sink = createDrillLogSink(voiceConfig({ drillLog: { enabled: true, path: directory } }));
			expect(sink).toBeDefined();
			await sink?.write(drillRecord("u-1"));
			await sink?.write(drillRecord("u-2"));
			await sink?.flush();
			await sink?.close();

			const day = new Date().toISOString().slice(0, 10);
			const contents = await readFile(join(directory, `${day}-discord-live-voice-utterances.jsonl`), "utf8");
			const lines = contents.trimEnd().split("\n");
			expect(lines).toHaveLength(2);
			expect(lines.map((line) => (JSON.parse(line) as VoiceDrillRecord).utteranceId)).toEqual(["u-1", "u-2"]);
			expect((JSON.parse(lines[0] as string) as VoiceDrillRecord).schema).toBe("voice-drill-log.v1");
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	test("writes nothing when drill logging is disabled", () => {
		expect(createDrillLogSink(voiceConfig({ drillLog: { enabled: false, path: "artifacts" } }))).toBeUndefined();
	});
});

describe("gateway loss closes voice rooms", () => {
	test("a failed gateway connect closes every live room with gateway_lost", async () => {
		const closed: string[] = [];
		const gateway = new ReconnectingGateway(
			join(tmpdir(), "definitely-absent-gateway.sock"),
			{ channels: { fetch: async () => undefined } },
			loaded(voiceConfig(), "key"),
			undefined,
			undefined,
			{
				sessions: {} as never,
				resolveModality: () => undefined,
				recordDeliveryPart: () => false,
				playbackFor: () => undefined,
				closeAll: async (reason) => void closed.push(reason),
			},
		);
		// connect() cannot reach a socket that does not exist, which is exactly the
		// runtime path that must tear the audio side down.
		await gateway.connect();
		expect(closed).toEqual(["gateway_lost"]);
	});
});
