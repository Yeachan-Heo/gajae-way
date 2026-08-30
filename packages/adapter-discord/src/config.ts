import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";

export interface VoiceElevenLabsSttConfig {
	readonly model: "scribe_v2_realtime";
	readonly audioFormat: "pcm_16000";
	readonly commitStrategy: "manual" | "vad";
	readonly includeLanguageDetection: boolean;
	readonly filterBackgroundAudio: boolean;
	readonly keyterms: readonly string[];
}

export interface VoiceElevenLabsTtsConfig {
	readonly model: "eleven_flash_v2_5";
	readonly voiceId: string;
	readonly outputFormat: "pcm_24000";
	readonly syncAlignment: true;
	readonly inactivityTimeoutSecs: number;
	readonly applyTextNormalization: "auto" | "on" | "off";
	readonly chunkLengthSchedule: readonly number[];
}

export interface VoiceElevenLabsConfig {
	readonly apiKeyFile: string;
	readonly stt: VoiceElevenLabsSttConfig;
	readonly tts: VoiceElevenLabsTtsConfig;
}

export interface VoiceConfig {
	readonly enabled: boolean;
	readonly silenceEndMs: number;
	readonly energyGate: { readonly minDurationMs: number; readonly rmsThreshold: number };
	readonly idleLeaveMs: number;
	readonly unread: { readonly maxItems: number; readonly maxCharsPerItem: number };
	readonly mergeWindowMs: number;
	readonly transcriptWaitMs: number;
	readonly sessionMaxMs: number;
	readonly turnMapTtlMs: number;
	readonly outstanding: { readonly ttlMs: number; readonly maxEntries: number };
	readonly bargeIn: {
		readonly minTranscriptChars: number;
		readonly cooldownMs: number;
		readonly echoSimilarity: number;
		readonly frameCorrectionMs: number;
	};
	readonly ingress: { readonly maxQueuedFramesPerSpeaker: number; readonly overflow: "drop_oldest" };
	readonly reconnect: {
		readonly initialBackoffMs: number;
		readonly maxBackoffMs: number;
		readonly maxAttempts: number;
	};
	readonly joinCommandAllowlist: readonly string[];
	readonly announceOnTurnStart: boolean;
	readonly drillLog: { readonly enabled: boolean; readonly path: string };
	readonly elevenlabs: VoiceElevenLabsConfig;
}

export interface DiscordAdapterConfig {
	readonly tokenFile: string;
	readonly gatewaySocket?: string;
	readonly intents?: readonly number[];
	readonly channels?: Readonly<Record<string, { readonly engagement?: "open" }>>;
	readonly voice?: VoiceConfig;
}

export interface LoadedDiscordAdapterConfig extends DiscordAdapterConfig {
	readonly token: string;
	readonly configPath: string;
	readonly elevenLabsApiKey?: string;
}

export interface VoiceVadKnobs {
	readonly vad_silence_threshold_secs: number;
	readonly min_speech_duration_ms: number;
	readonly min_silence_duration_ms: number;
}

export const VOICE_CONFIG_RANGES = {
	silenceEndMs: { min: 100, max: 5_000 },
	energyGate: {
		minDurationMs: { min: 50, max: 2_000 },
		rmsThreshold: { min: 0, max: 1, minInclusive: false },
	},
	idleLeaveMs: { min: 30_000, max: 3_600_000 },
	unread: {
		maxItems: { min: 1, max: 100 },
		maxCharsPerItem: { min: 50, max: 1_000 },
	},
	mergeWindowMs: { min: 200, max: 10_000 },
	transcriptWaitMs: { min: 200, max: 10_000 },
	sessionMaxMs: { min: 60_000, max: 86_400_000 },
	turnMapTtlMs: { min: 60_000, max: 3_600_000 },
	outstanding: {
		ttlMs: { min: 60_000, max: 3_600_000 },
		maxEntries: { min: 1, max: 256 },
	},
	bargeIn: {
		minTranscriptChars: { min: 1, max: 50 },
		cooldownMs: { min: 0, max: 10_000 },
		echoSimilarity: { min: 0, max: 1 },
		frameCorrectionMs: { min: 0, max: 200 },
	},
	ingress: { maxQueuedFramesPerSpeaker: { min: 10, max: 2_000 } },
	reconnect: {
		initialBackoffMs: { min: 100, max: 10_000 },
		maxBackoffMs: { min: 1_000, max: 300_000 },
		maxAttempts: { min: 1, max: 20 },
	},
	elevenlabs: { tts: { inactivityTimeoutSecs: { min: 1, max: 180 } } },
} as const;

export const VOICE_CONFIG_DEFAULTS = {
	enabled: false,
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
		apiKeyFile: "",
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
			voiceId: "",
			outputFormat: "pcm_24000",
			syncAlignment: true,
			inactivityTimeoutSecs: 20,
			applyTextNormalization: "auto",
			chunkLengthSchedule: [120, 160, 250, 290],
		},
	},
} as const;

const MILLISECONDS_PER_SECOND = 1_000;

export class DiscordAdapterStartupError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "DiscordAdapterStartupError";
	}
}

export function adapterHome(env: NodeJS.ProcessEnv = process.env): string {
	return env.GAJAEWAY_HOME || join(homedir(), ".gajaeway");
}

export async function loadDiscordAdapterConfig(
	env: NodeJS.ProcessEnv = process.env,
): Promise<LoadedDiscordAdapterConfig> {
	const configPath = join(adapterHome(env), "adapter-discord.json");
	let raw: unknown;
	try {
		raw = JSON.parse(await readFile(configPath, "utf8"));
	} catch (_error) {
		throw new DiscordAdapterStartupError(
			`Unable to read Discord adapter config at ${configPath}. Create it with a tokenFile credential-file path.`,
		);
	}
	if (!isObject(raw) || typeof raw.tokenFile !== "string" || raw.tokenFile.trim() === "") {
		throw new DiscordAdapterStartupError(
			`Discord adapter config at ${configPath} requires a non-empty tokenFile credential-file path.`,
		);
	}
	if (raw.gatewaySocket !== undefined && typeof raw.gatewaySocket !== "string") {
		throw new DiscordAdapterStartupError("Discord adapter gatewaySocket must be a string when set.");
	}
	if (
		raw.intents !== undefined &&
		(!Array.isArray(raw.intents) || raw.intents.some((intent) => !Number.isInteger(intent)))
	) {
		throw new DiscordAdapterStartupError("Discord adapter intents must be an array of integer intent values.");
	}
	if (raw.channels !== undefined && !validChannels(raw.channels)) {
		throw new DiscordAdapterStartupError('Discord adapter channels entries may only set engagement to "open".');
	}
	const tokenFile = isAbsolute(raw.tokenFile) ? raw.tokenFile : resolve(dirname(configPath), raw.tokenFile);
	let token: string;
	try {
		token = (await readFile(tokenFile, "utf8")).trim();
	} catch {
		throw new DiscordAdapterStartupError(
			`Unable to read Discord token credential file ${tokenFile}. Check tokenFile and file permissions.`,
		);
	}
	if (!token) {
		throw new DiscordAdapterStartupError(`Discord token credential file ${tokenFile} is empty.`);
	}

	const voice = raw.voice === undefined ? undefined : normalizeVoiceConfig(raw.voice, configPath);
	let elevenLabsApiKey: string | undefined;
	if (voice?.enabled) {
		try {
			elevenLabsApiKey = (await readFile(voice.elevenlabs.apiKeyFile, "utf8")).trim();
		} catch {
			throw new DiscordAdapterStartupError(
				`Unable to read ElevenLabs API key credential file ${voice.elevenlabs.apiKeyFile}. Check elevenlabs.apiKeyFile and file permissions.`,
			);
		}
		if (!elevenLabsApiKey) {
			throw new DiscordAdapterStartupError(
				`ElevenLabs API key credential file ${voice.elevenlabs.apiKeyFile} is empty. Set elevenlabs.apiKeyFile to a readable, non-empty credential file.`,
			);
		}
	}

	return {
		...raw,
		tokenFile,
		token,
		configPath,
		...(voice === undefined ? {} : { voice }),
		...(elevenLabsApiKey === undefined ? {} : { elevenLabsApiKey }),
	} as LoadedDiscordAdapterConfig;
}

export function deriveVoiceVadKnobs(config: VoiceConfig): VoiceVadKnobs {
	return {
		vad_silence_threshold_secs: config.silenceEndMs / MILLISECONDS_PER_SECOND,
		min_speech_duration_ms: config.energyGate.minDurationMs,
		min_silence_duration_ms: config.silenceEndMs,
	};
}

function normalizeVoiceConfig(value: unknown, configPath: string): VoiceConfig {
	if (!isObject(value)) {
		throw new DiscordAdapterStartupError("Discord adapter voice must be an object when set.");
	}
	rejectRemovedVoiceKeys(value);
	assertKnownKeys(value, "voice", [
		"enabled",
		"silenceEndMs",
		"energyGate",
		"idleLeaveMs",
		"unread",
		"mergeWindowMs",
		"transcriptWaitMs",
		"sessionMaxMs",
		"turnMapTtlMs",
		"outstanding",
		"bargeIn",
		"ingress",
		"reconnect",
		"joinCommandAllowlist",
		"announceOnTurnStart",
		"drillLog",
		"elevenlabs",
	]);

	const enabled = optionalBoolean(value.enabled, "voice.enabled", VOICE_CONFIG_DEFAULTS.enabled);
	const energyGate = objectOrEmpty(value.energyGate, "voice.energyGate");
	assertKnownKeys(energyGate, "voice.energyGate", ["minDurationMs", "rmsThreshold"]);
	const unread = objectOrEmpty(value.unread, "voice.unread");
	assertKnownKeys(unread, "voice.unread", ["maxItems", "maxCharsPerItem"]);
	const outstanding = objectOrEmpty(value.outstanding, "voice.outstanding");
	assertKnownKeys(outstanding, "voice.outstanding", ["ttlMs", "maxEntries"]);
	const bargeIn = objectOrEmpty(value.bargeIn, "voice.bargeIn");
	assertKnownKeys(bargeIn, "voice.bargeIn", [
		"minTranscriptChars",
		"cooldownMs",
		"echoSimilarity",
		"frameCorrectionMs",
	]);
	const ingress = objectOrEmpty(value.ingress, "voice.ingress");
	assertKnownKeys(ingress, "voice.ingress", ["maxQueuedFramesPerSpeaker", "overflow"]);
	const reconnect = objectOrEmpty(value.reconnect, "voice.reconnect");
	assertKnownKeys(reconnect, "voice.reconnect", ["initialBackoffMs", "maxBackoffMs", "maxAttempts"]);
	const drillLog = objectOrEmpty(value.drillLog, "voice.drillLog");
	assertKnownKeys(drillLog, "voice.drillLog", ["enabled", "path"]);
	const elevenlabs = objectOrEmpty(value.elevenlabs, "voice.elevenlabs");
	assertKnownKeys(elevenlabs, "voice.elevenlabs", ["apiKeyFile", "stt", "tts"]);
	const stt = objectOrEmpty(elevenlabs.stt, "voice.elevenlabs.stt");
	assertKnownKeys(stt, "voice.elevenlabs.stt", [
		"model",
		"audioFormat",
		"commitStrategy",
		"includeLanguageDetection",
		"filterBackgroundAudio",
		"keyterms",
	]);
	const tts = objectOrEmpty(elevenlabs.tts, "voice.elevenlabs.tts");
	assertKnownKeys(tts, "voice.elevenlabs.tts", [
		"model",
		"voiceId",
		"outputFormat",
		"syncAlignment",
		"inactivityTimeoutSecs",
		"applyTextNormalization",
		"chunkLengthSchedule",
	]);

	const silenceEndMs = numberInRange(
		value.silenceEndMs,
		"voice.silenceEndMs",
		VOICE_CONFIG_RANGES.silenceEndMs,
		VOICE_CONFIG_DEFAULTS.silenceEndMs,
	);
	const normalizedEnergyGate = {
		minDurationMs: numberInRange(
			energyGate.minDurationMs,
			"voice.energyGate.minDurationMs",
			VOICE_CONFIG_RANGES.energyGate.minDurationMs,
			VOICE_CONFIG_DEFAULTS.energyGate.minDurationMs,
		),
		rmsThreshold: numberInRange(
			energyGate.rmsThreshold,
			"voice.energyGate.rmsThreshold",
			VOICE_CONFIG_RANGES.energyGate.rmsThreshold,
			VOICE_CONFIG_DEFAULTS.energyGate.rmsThreshold,
		),
	};
	const normalizedUnread = {
		maxItems: numberInRange(
			unread.maxItems,
			"voice.unread.maxItems",
			VOICE_CONFIG_RANGES.unread.maxItems,
			VOICE_CONFIG_DEFAULTS.unread.maxItems,
		),
		maxCharsPerItem: numberInRange(
			unread.maxCharsPerItem,
			"voice.unread.maxCharsPerItem",
			VOICE_CONFIG_RANGES.unread.maxCharsPerItem,
			VOICE_CONFIG_DEFAULTS.unread.maxCharsPerItem,
		),
	};
	const normalizedOutstanding = {
		ttlMs: numberInRange(
			outstanding.ttlMs,
			"voice.outstanding.ttlMs",
			VOICE_CONFIG_RANGES.outstanding.ttlMs,
			VOICE_CONFIG_DEFAULTS.outstanding.ttlMs,
		),
		maxEntries: numberInRange(
			outstanding.maxEntries,
			"voice.outstanding.maxEntries",
			VOICE_CONFIG_RANGES.outstanding.maxEntries,
			VOICE_CONFIG_DEFAULTS.outstanding.maxEntries,
		),
	};
	const normalizedBargeIn = {
		minTranscriptChars: numberInRange(
			bargeIn.minTranscriptChars,
			"voice.bargeIn.minTranscriptChars",
			VOICE_CONFIG_RANGES.bargeIn.minTranscriptChars,
			VOICE_CONFIG_DEFAULTS.bargeIn.minTranscriptChars,
		),
		cooldownMs: numberInRange(
			bargeIn.cooldownMs,
			"voice.bargeIn.cooldownMs",
			VOICE_CONFIG_RANGES.bargeIn.cooldownMs,
			VOICE_CONFIG_DEFAULTS.bargeIn.cooldownMs,
		),
		echoSimilarity: numberInRange(
			bargeIn.echoSimilarity,
			"voice.bargeIn.echoSimilarity",
			VOICE_CONFIG_RANGES.bargeIn.echoSimilarity,
			VOICE_CONFIG_DEFAULTS.bargeIn.echoSimilarity,
		),
		frameCorrectionMs: numberInRange(
			bargeIn.frameCorrectionMs,
			"voice.bargeIn.frameCorrectionMs",
			VOICE_CONFIG_RANGES.bargeIn.frameCorrectionMs,
			VOICE_CONFIG_DEFAULTS.bargeIn.frameCorrectionMs,
		),
	};
	const normalizedIngress = {
		maxQueuedFramesPerSpeaker: numberInRange(
			ingress.maxQueuedFramesPerSpeaker,
			"voice.ingress.maxQueuedFramesPerSpeaker",
			VOICE_CONFIG_RANGES.ingress.maxQueuedFramesPerSpeaker,
			VOICE_CONFIG_DEFAULTS.ingress.maxQueuedFramesPerSpeaker,
		),
		overflow: enumValue(
			ingress.overflow,
			"voice.ingress.overflow",
			["drop_oldest"],
			VOICE_CONFIG_DEFAULTS.ingress.overflow,
		),
	};
	const normalizedReconnect = {
		initialBackoffMs: numberInRange(
			reconnect.initialBackoffMs,
			"voice.reconnect.initialBackoffMs",
			VOICE_CONFIG_RANGES.reconnect.initialBackoffMs,
			VOICE_CONFIG_DEFAULTS.reconnect.initialBackoffMs,
		),
		maxBackoffMs: numberInRange(
			reconnect.maxBackoffMs,
			"voice.reconnect.maxBackoffMs",
			VOICE_CONFIG_RANGES.reconnect.maxBackoffMs,
			VOICE_CONFIG_DEFAULTS.reconnect.maxBackoffMs,
		),
		maxAttempts: numberInRange(
			reconnect.maxAttempts,
			"voice.reconnect.maxAttempts",
			VOICE_CONFIG_RANGES.reconnect.maxAttempts,
			VOICE_CONFIG_DEFAULTS.reconnect.maxAttempts,
		),
	};

	const normalizedStt = {
		model: enumValue(
			stt.model,
			"voice.elevenlabs.stt.model",
			["scribe_v2_realtime"],
			VOICE_CONFIG_DEFAULTS.elevenlabs.stt.model,
		),
		audioFormat: enumValue(
			stt.audioFormat,
			"voice.elevenlabs.stt.audioFormat",
			["pcm_16000"],
			VOICE_CONFIG_DEFAULTS.elevenlabs.stt.audioFormat,
		),
		commitStrategy: enumValue(
			stt.commitStrategy,
			"voice.elevenlabs.stt.commitStrategy",
			["manual", "vad"],
			VOICE_CONFIG_DEFAULTS.elevenlabs.stt.commitStrategy,
		),
		includeLanguageDetection: optionalBoolean(
			stt.includeLanguageDetection,
			"voice.elevenlabs.stt.includeLanguageDetection",
			VOICE_CONFIG_DEFAULTS.elevenlabs.stt.includeLanguageDetection,
		),
		filterBackgroundAudio: optionalBoolean(
			stt.filterBackgroundAudio,
			"voice.elevenlabs.stt.filterBackgroundAudio",
			VOICE_CONFIG_DEFAULTS.elevenlabs.stt.filterBackgroundAudio,
		),
		keyterms: stringArray(stt.keyterms, "voice.elevenlabs.stt.keyterms", VOICE_CONFIG_DEFAULTS.elevenlabs.stt.keyterms),
	};
	const normalizedTts = {
		model: enumValue(
			tts.model,
			"voice.elevenlabs.tts.model",
			["eleven_flash_v2_5"],
			VOICE_CONFIG_DEFAULTS.elevenlabs.tts.model,
		),
		voiceId: optionalString(tts.voiceId, "voice.elevenlabs.tts.voiceId", VOICE_CONFIG_DEFAULTS.elevenlabs.tts.voiceId),
		outputFormat: enumValue(
			tts.outputFormat,
			"voice.elevenlabs.tts.outputFormat",
			["pcm_24000"],
			VOICE_CONFIG_DEFAULTS.elevenlabs.tts.outputFormat,
		),
		syncAlignment: requiredTrue(tts.syncAlignment, "voice.elevenlabs.tts.syncAlignment"),
		inactivityTimeoutSecs: numberInRange(
			tts.inactivityTimeoutSecs,
			"voice.elevenlabs.tts.inactivityTimeoutSecs",
			VOICE_CONFIG_RANGES.elevenlabs.tts.inactivityTimeoutSecs,
			VOICE_CONFIG_DEFAULTS.elevenlabs.tts.inactivityTimeoutSecs,
		),
		applyTextNormalization: enumValue(
			tts.applyTextNormalization,
			"voice.elevenlabs.tts.applyTextNormalization",
			["auto", "on", "off"],
			VOICE_CONFIG_DEFAULTS.elevenlabs.tts.applyTextNormalization,
		),
		chunkLengthSchedule: integerSchedule(
			tts.chunkLengthSchedule,
			"voice.elevenlabs.tts.chunkLengthSchedule",
			VOICE_CONFIG_DEFAULTS.elevenlabs.tts.chunkLengthSchedule,
		),
	};

	let apiKeyFile = optionalString(
		elevenlabs.apiKeyFile,
		"voice.elevenlabs.apiKeyFile",
		VOICE_CONFIG_DEFAULTS.elevenlabs.apiKeyFile,
	);
	if (enabled && apiKeyFile.trim() === "") {
		throw new DiscordAdapterStartupError(
			"Discord adapter voice.elevenlabs.apiKeyFile is required and must be a non-empty credential-file path when voice.enabled is true.",
		);
	}
	if (enabled && normalizedTts.voiceId.trim() === "") {
		throw new DiscordAdapterStartupError(
			"Discord adapter voice.elevenlabs.tts.voiceId is required and must be a non-empty string when voice.enabled is true.",
		);
	}
	if (enabled) apiKeyFile = isAbsolute(apiKeyFile) ? apiKeyFile : resolve(dirname(configPath), apiKeyFile);

	return {
		enabled,
		silenceEndMs,
		energyGate: normalizedEnergyGate,
		idleLeaveMs: numberInRange(
			value.idleLeaveMs,
			"voice.idleLeaveMs",
			VOICE_CONFIG_RANGES.idleLeaveMs,
			VOICE_CONFIG_DEFAULTS.idleLeaveMs,
		),
		unread: normalizedUnread,
		mergeWindowMs: numberInRange(
			value.mergeWindowMs,
			"voice.mergeWindowMs",
			VOICE_CONFIG_RANGES.mergeWindowMs,
			VOICE_CONFIG_DEFAULTS.mergeWindowMs,
		),
		transcriptWaitMs: numberInRange(
			value.transcriptWaitMs,
			"voice.transcriptWaitMs",
			VOICE_CONFIG_RANGES.transcriptWaitMs,
			VOICE_CONFIG_DEFAULTS.transcriptWaitMs,
		),
		sessionMaxMs: numberInRange(
			value.sessionMaxMs,
			"voice.sessionMaxMs",
			VOICE_CONFIG_RANGES.sessionMaxMs,
			VOICE_CONFIG_DEFAULTS.sessionMaxMs,
		),
		turnMapTtlMs: numberInRange(
			value.turnMapTtlMs,
			"voice.turnMapTtlMs",
			VOICE_CONFIG_RANGES.turnMapTtlMs,
			VOICE_CONFIG_DEFAULTS.turnMapTtlMs,
		),
		outstanding: normalizedOutstanding,
		bargeIn: normalizedBargeIn,
		ingress: normalizedIngress,
		reconnect: normalizedReconnect,
		joinCommandAllowlist: stringArray(
			value.joinCommandAllowlist,
			"voice.joinCommandAllowlist",
			VOICE_CONFIG_DEFAULTS.joinCommandAllowlist,
		),
		announceOnTurnStart: optionalBoolean(
			value.announceOnTurnStart,
			"voice.announceOnTurnStart",
			VOICE_CONFIG_DEFAULTS.announceOnTurnStart,
		),
		drillLog: {
			enabled: optionalBoolean(drillLog.enabled, "voice.drillLog.enabled", VOICE_CONFIG_DEFAULTS.drillLog.enabled),
			path: optionalString(drillLog.path, "voice.drillLog.path", VOICE_CONFIG_DEFAULTS.drillLog.path),
		},
		elevenlabs: {
			apiKeyFile,
			stt: normalizedStt,
			tts: normalizedTts,
		},
	};
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validChannels(value: unknown): value is Record<string, { readonly engagement?: "open" }> {
	return (
		isObject(value) &&
		Object.values(value).every(
			(entry) => isObject(entry) && (entry.engagement === undefined || entry.engagement === "open"),
		)
	);
}

function assertKnownKeys(value: Record<string, unknown>, path: string, allowed: readonly string[]): void {
	for (const key of Object.keys(value)) {
		if (!allowed.includes(key)) {
			throw new DiscordAdapterStartupError(`Discord adapter ${path}.${key} is not a supported voice setting.`);
		}
	}
}

function rejectRemovedVoiceKeys(value: unknown, path = "voice"): void {
	if (Array.isArray(value)) {
		for (const [index, entry] of value.entries()) rejectRemovedVoiceKeys(entry, `${path}[${index}]`);
		return;
	}
	if (!isObject(value)) return;
	for (const [key, entry] of Object.entries(value)) {
		const entryPath = `${path}.${key}`;
		if (key === "languageCode") {
			throw new DiscordAdapterStartupError(
				`Discord adapter ${entryPath} was removed; language is provider-detected and languageCode must be deleted.`,
			);
		}
		if (key === "debtMax") {
			throw new DiscordAdapterStartupError(
				`Discord adapter ${entryPath} was removed; outstanding debt is unbounded and debtMax must be deleted.`,
			);
		}
		if (key === "overflow" && entry === "drop_newest") {
			throw new DiscordAdapterStartupError(
				`Discord adapter ${entryPath} value "drop_newest" was removed; use "drop_oldest" instead.`,
			);
		}
		if (key === "outputFormat" && (entry === "pcm_48000" || entry === "opus_48000_64")) {
			throw new DiscordAdapterStartupError(
				`Discord adapter ${entryPath} value "${entry}" was removed; use "pcm_24000" instead.`,
			);
		}
		rejectRemovedVoiceKeys(entry, entryPath);
	}
}

function objectOrEmpty(value: unknown, path: string): Record<string, unknown> {
	if (value === undefined) return {};
	if (!isObject(value)) throw new DiscordAdapterStartupError(`Discord adapter ${path} must be an object when set.`);
	return value;
}

function optionalBoolean(value: unknown, path: string, fallback: boolean): boolean {
	if (value === undefined) return fallback;
	if (typeof value !== "boolean") throw new DiscordAdapterStartupError(`Discord adapter ${path} must be a boolean.`);
	return value;
}

type NumericRange = { readonly min: number; readonly max: number; readonly minInclusive?: boolean };

function numberInRange(value: unknown, path: string, range: NumericRange, fallback: number): number {
	if (value === undefined) return fallback;
	const rangeText = range.minInclusive === false ? `(${range.min}, ${range.max}]` : `[${range.min}, ${range.max}]`;
	if (typeof value !== "number" || !Number.isFinite(value)) {
		throw new DiscordAdapterStartupError(`Discord adapter ${path} must be a finite number in the range ${rangeText}.`);
	}
	if (value < range.min || (range.minInclusive === false && value <= range.min) || value > range.max) {
		throw new DiscordAdapterStartupError(`Discord adapter ${path} must be in the range ${rangeText}.`);
	}
	return value;
}

function optionalString(value: unknown, path: string, fallback: string): string {
	if (value === undefined) return fallback;
	if (typeof value !== "string") throw new DiscordAdapterStartupError(`Discord adapter ${path} must be a string.`);
	return value;
}

function stringArray(value: unknown, path: string, fallback: readonly string[]): readonly string[] {
	if (value === undefined) return [...fallback];
	if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string" || entry.trim() === "")) {
		throw new DiscordAdapterStartupError(`Discord adapter ${path} must be an array of non-empty strings.`);
	}
	return value.map((entry) => entry.trim());
}

function integerSchedule(value: unknown, path: string, fallback: readonly number[]): readonly number[] {
	if (value === undefined) return [...fallback];
	if (!Array.isArray(value) || value.some((entry) => !Number.isInteger(entry))) {
		throw new DiscordAdapterStartupError(`Discord adapter ${path} must be an array of integers in ascending order.`);
	}
	for (let index = 1; index < value.length; index += 1) {
		if (value[index] <= value[index - 1]) {
			throw new DiscordAdapterStartupError(`Discord adapter ${path} must be in strictly ascending order.`);
		}
	}
	return [...value];
}

function enumValue<T extends string>(value: unknown, path: string, allowed: readonly T[], fallback: T): T {
	if (value === undefined) return fallback;
	if (typeof value !== "string" || !allowed.some((entry) => entry === value)) {
		throw new DiscordAdapterStartupError(`Discord adapter ${path} must be one of: ${allowed.join(", ")}.`);
	}
	return value as T;
}

function requiredTrue(value: unknown, path: string): true {
	if (value !== undefined && value !== true) {
		throw new DiscordAdapterStartupError(`Discord adapter ${path} must be exactly true.`);
	}
	return true;
}
