import { expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	DiscordAdapterStartupError,
	deriveVoiceVadKnobs,
	loadDiscordAdapterConfig,
	VOICE_CONFIG_DEFAULTS,
	VOICE_CONFIG_RANGES,
	type VoiceConfig,
} from "../src/config";

type JsonObject = Record<string, unknown>;

const SECRET = "elevenlabs-secret-value";
const voiceFixture: JsonObject = {
	enabled: true,
	silenceEndMs: 1_234,
	energyGate: { minDurationMs: 456, rmsThreshold: 0.25 },
	idleLeaveMs: 456_789,
	unread: { maxItems: 42, maxCharsPerItem: 678 },
	mergeWindowMs: 2_345,
	transcriptWaitMs: 2_456,
	sessionMaxMs: 7_200_000,
	turnMapTtlMs: 1_200_000,
	outstanding: { ttlMs: 1_100_000, maxEntries: 64 },
	bargeIn: { minTranscriptChars: 5, cooldownMs: 2_000, echoSimilarity: 0.75, frameCorrectionMs: 30 },
	ingress: { maxQueuedFramesPerSpeaker: 400, overflow: "drop_oldest" },
	reconnect: { initialBackoffMs: 600, maxBackoffMs: 45_000, maxAttempts: 8 },
	joinCommandAllowlist: ["owner-1", "owner-2"],
	announceOnTurnStart: false,
	drillLog: { enabled: false, path: "voice-artifacts" },
	elevenlabs: {
		apiKeyFile: "credentials/elevenlabs-key",
		stt: {
			model: "scribe_v2_realtime",
			audioFormat: "pcm_16000",
			commitStrategy: "manual",
			includeLanguageDetection: false,
			filterBackgroundAudio: false,
			keyterms: ["Gajae", "voice"],
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

async function createHome(): Promise<string> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-voice-config-"));
	await writeFile(join(home, "token"), "discord-token\n");
	await mkdir(join(home, "credentials"), { recursive: true });
	await writeFile(join(home, "credentials", "elevenlabs-key"), `  ${SECRET}  \n`);
	return home;
}

async function writeConfig(home: string, voice?: JsonObject): Promise<void> {
	await writeFile(
		join(home, "adapter-discord.json"),
		JSON.stringify(voice === undefined ? { tokenFile: "token" } : { tokenFile: "token", voice }),
	);
}

async function load(home: string): Promise<Awaited<ReturnType<typeof loadDiscordAdapterConfig>>> {
	return loadDiscordAdapterConfig({ GAJAEWAY_HOME: home });
}

function cloneVoice(): JsonObject {
	return structuredClone(voiceFixture) as JsonObject;
}

function setNested(root: JsonObject, path: readonly string[], value: unknown): JsonObject {
	const result = structuredClone(root) as JsonObject;
	let cursor = result;
	for (const key of path.slice(0, -1)) {
		const child = cursor[key];
		if (typeof child !== "object" || child === null || Array.isArray(child))
			throw new Error(`Invalid fixture path ${path.join(".")}`);
		cursor = child as JsonObject;
	}
	cursor[path[path.length - 1] as string] = value;
	return result;
}

async function expectRejected(home: string, voice: JsonObject, message: RegExp): Promise<void> {
	await writeConfig(home, voice);
	const error = await load(home).catch((value: unknown) => value);
	expect(error).toBeInstanceOf(DiscordAdapterStartupError);
	expect(error).toMatchObject({ message: expect.stringMatching(message) });
}

async function expectAccepted(home: string, voice: JsonObject): Promise<VoiceConfig> {
	await writeConfig(home, voice);
	const loaded = await load(home);
	expect(loaded.voice).toBeDefined();
	return loaded.voice as VoiceConfig;
}

test("loads a complete voice config, resolves its credential file, and derives VAD knobs", async () => {
	const home = await createHome();
	try {
		const loaded = await expectAccepted(home, voiceFixture);
		expect(loaded).toEqual(voiceFixtureWithResolvedKey(home));
		const config = await load(home);
		expect(config.elevenLabsApiKey).toBe(SECRET);
		expect(config.elevenLabsApiKey).not.toBe(voiceFixture.elevenlabs);
		expect(deriveVoiceVadKnobs(loaded)).toEqual({
			vad_silence_threshold_secs: 1.234,
			min_speech_duration_ms: 456,
			min_silence_duration_ms: 1_234,
		});
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("leaves the existing config behavior intact when voice is absent", async () => {
	const home = await createHome();
	try {
		await writeConfig(home);
		const loaded = await load(home);
		expect(loaded.token).toBe("discord-token");
		expect(loaded.tokenFile).toBe(join(home, "token"));
		expect(loaded.voice).toBeUndefined();
		expect(loaded.elevenLabsApiKey).toBeUndefined();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("applies every documented default to an omitted voice setting", async () => {
	const home = await createHome();
	try {
		const config = await expectAccepted(home, {});
		expect(config).toEqual(VOICE_CONFIG_DEFAULTS);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("accepts each numeric minimum and maximum and rejects values immediately outside every range", async () => {
	const home = await createHome();
	try {
		const cases: ReadonlyArray<{
			readonly path: readonly string[];
			readonly range: { readonly min: number; readonly max: number; readonly minInclusive?: boolean };
		}> = [
			{ path: ["silenceEndMs"], range: VOICE_CONFIG_RANGES.silenceEndMs },
			{ path: ["energyGate", "minDurationMs"], range: VOICE_CONFIG_RANGES.energyGate.minDurationMs },
			{ path: ["energyGate", "rmsThreshold"], range: VOICE_CONFIG_RANGES.energyGate.rmsThreshold },
			{ path: ["idleLeaveMs"], range: VOICE_CONFIG_RANGES.idleLeaveMs },
			{ path: ["unread", "maxItems"], range: VOICE_CONFIG_RANGES.unread.maxItems },
			{ path: ["unread", "maxCharsPerItem"], range: VOICE_CONFIG_RANGES.unread.maxCharsPerItem },
			{ path: ["mergeWindowMs"], range: VOICE_CONFIG_RANGES.mergeWindowMs },
			{ path: ["transcriptWaitMs"], range: VOICE_CONFIG_RANGES.transcriptWaitMs },
			{ path: ["sessionMaxMs"], range: VOICE_CONFIG_RANGES.sessionMaxMs },
			{ path: ["turnMapTtlMs"], range: VOICE_CONFIG_RANGES.turnMapTtlMs },
			{ path: ["outstanding", "ttlMs"], range: VOICE_CONFIG_RANGES.outstanding.ttlMs },
			{ path: ["outstanding", "maxEntries"], range: VOICE_CONFIG_RANGES.outstanding.maxEntries },
			{ path: ["bargeIn", "minTranscriptChars"], range: VOICE_CONFIG_RANGES.bargeIn.minTranscriptChars },
			{ path: ["bargeIn", "cooldownMs"], range: VOICE_CONFIG_RANGES.bargeIn.cooldownMs },
			{ path: ["bargeIn", "echoSimilarity"], range: VOICE_CONFIG_RANGES.bargeIn.echoSimilarity },
			{ path: ["bargeIn", "frameCorrectionMs"], range: VOICE_CONFIG_RANGES.bargeIn.frameCorrectionMs },
			{ path: ["ingress", "maxQueuedFramesPerSpeaker"], range: VOICE_CONFIG_RANGES.ingress.maxQueuedFramesPerSpeaker },
			{ path: ["reconnect", "initialBackoffMs"], range: VOICE_CONFIG_RANGES.reconnect.initialBackoffMs },
			{ path: ["reconnect", "maxBackoffMs"], range: VOICE_CONFIG_RANGES.reconnect.maxBackoffMs },
			{ path: ["reconnect", "maxAttempts"], range: VOICE_CONFIG_RANGES.reconnect.maxAttempts },
			{
				path: ["elevenlabs", "tts", "inactivityTimeoutSecs"],
				range: VOICE_CONFIG_RANGES.elevenlabs.tts.inactivityTimeoutSecs,
			},
		];
		for (const { path, range } of cases) {
			const label = path.join(".");
			const below = range.min - (range.min === 0 ? 0.1 : 1);
			await expectRejected(
				home,
				setNested(cloneVoice(), path, below),
				new RegExp(label.replaceAll(".", "\\.") + ".*range"),
			);
			if (range.minInclusive === false) {
				await expectRejected(
					home,
					setNested(cloneVoice(), path, range.min),
					new RegExp(label.replaceAll(".", "\\.") + ".*range"),
				);
				await expectAccepted(home, setNested(cloneVoice(), path, range.min + 0.0001));
			} else {
				await expectAccepted(home, setNested(cloneVoice(), path, range.min));
			}
			await expectAccepted(home, setNested(cloneVoice(), path, range.max));
			await expectRejected(
				home,
				setNested(cloneVoice(), path, range.max + 1),
				new RegExp(label.replaceAll(".", "\\.") + ".*range"),
			);
		}
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("rejects every invalid enum and validates arrays", async () => {
	const home = await createHome();
	try {
		const enumCases: ReadonlyArray<{ readonly path: readonly string[]; readonly value: unknown }> = [
			{ path: ["ingress", "overflow"], value: "other" },
			{ path: ["elevenlabs", "stt", "model"], value: "scribe_v1" },
			{ path: ["elevenlabs", "stt", "audioFormat"], value: "pcm_48000" },
			{ path: ["elevenlabs", "stt", "commitStrategy"], value: "automatic" },
			{ path: ["elevenlabs", "tts", "model"], value: "eleven_turbo" },
			{ path: ["elevenlabs", "tts", "outputFormat"], value: "wav" },
			{ path: ["elevenlabs", "tts", "syncAlignment"], value: false },
			{ path: ["elevenlabs", "tts", "applyTextNormalization"], value: "sometimes" },
		];
		for (const { path, value } of enumCases) {
			await expectRejected(
				home,
				setNested(cloneVoice(), path, value),
				new RegExp(path.join("\\.") + ".*(one of|exactly true)"),
			);
		}
		await expectRejected(home, setNested(cloneVoice(), ["joinCommandAllowlist"], [" "]), /non-empty strings/);
		await expectRejected(
			home,
			setNested(cloneVoice(), ["elevenlabs", "stt", "keyterms"], ["ok", ""]),
			/non-empty strings/,
		);
		await expectRejected(
			home,
			setNested(cloneVoice(), ["elevenlabs", "tts", "chunkLengthSchedule"], [100, 100]),
			/ascending/,
		);
		await expectRejected(
			home,
			setNested(cloneVoice(), ["elevenlabs", "tts", "chunkLengthSchedule"], [100, 1.5]),
			/integers/,
		);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("rejects each removed voice setting with an actionable error", async () => {
	const home = await createHome();
	try {
		await expectRejected(
			home,
			setNested(cloneVoice(), ["outstanding", "debtMax"], 4),
			/voice\.outstanding\.debtMax.*removed/,
		);
		await expectRejected(home, setNested(cloneVoice(), ["ingress", "overflow"], "drop_newest"), /drop_newest.*removed/);
		await expectRejected(
			home,
			setNested(cloneVoice(), ["elevenlabs", "stt", "languageCode"], "en"),
			/languageCode.*removed/,
		);
		await expectRejected(
			home,
			setNested(cloneVoice(), ["elevenlabs", "tts", "outputFormat"], "pcm_48000"),
			/pcm_48000.*removed/,
		);
		await expectRejected(
			home,
			setNested(cloneVoice(), ["elevenlabs", "tts", "outputFormat"], "opus_48000_64"),
			/opus_48000_64.*removed/,
		);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("requires voice credentials when enabled and never echoes the API key", async () => {
	const home = await createHome();
	try {
		await expectRejected(home, setNested(cloneVoice(), ["elevenlabs", "tts", "voiceId"], ""), /voiceId.*required/);
		await expectRejected(home, setNested(cloneVoice(), ["elevenlabs", "apiKeyFile"], ""), /apiKeyFile.*required/);
		await expectRejected(
			home,
			setNested(cloneVoice(), ["elevenlabs", "apiKeyFile"], "missing-key"),
			/Unable to read.*credential file/,
		);
		await writeConfig(home, setNested(cloneVoice(), ["elevenlabs", "apiKeyFile"], "missing-key"));
		const unreadableError = await load(home).catch((value: unknown) => value);
		expect(String(unreadableError)).not.toContain(SECRET);
		await writeFile(join(home, "empty-key"), " \n\t");
		const emptyKeyVoice = setNested(cloneVoice(), ["elevenlabs", "apiKeyFile"], "empty-key");
		await writeConfig(home, emptyKeyVoice);
		const emptyError = await load(home).catch((value: unknown) => value);
		expect(emptyError).toBeInstanceOf(DiscordAdapterStartupError);
		expect(emptyError).toMatchObject({ message: expect.stringContaining("is empty") });
		expect(String(emptyError)).not.toContain(SECRET);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

function voiceFixtureWithResolvedKey(home: string): VoiceConfig {
	return {
		...voiceFixture,
		elevenlabs: {
			...(voiceFixture.elevenlabs as JsonObject),
			apiKeyFile: join(home, "credentials", "elevenlabs-key"),
		},
	} as unknown as VoiceConfig;
}
