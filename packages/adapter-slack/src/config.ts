import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { type ChannelEngagementPolicy, ENGAGEMENT_AUDIENCES, ENGAGEMENT_MODES } from "@gajae-gateway/protocol";
import { MIN_SOCKET_LIVENESS_TIMEOUT_MS } from "./socket";
import type { LoadedSlackVoiceConfig } from "./voice";

export interface SlackAdapterConfig {
	readonly botTokenFile: string;
	readonly appTokenFile: string;
	readonly gatewaySocket?: string;
	readonly channels?: Readonly<Record<string, ChannelEngagementPolicy>>;
	/**
	 * Fold a turn's streaming parts into one live-edited message (chat.update)
	 * instead of posting each part separately. Opt-in: the default keeps one
	 * message per part, matching the reaction-gradient presence design.
	 */
	readonly liveReplies?: boolean;
	/**
	 * Force-close the Socket Mode connection after this many milliseconds without
	 * any frame, ping, or pong from Slack so the reconnect loop replaces a
	 * half-open socket. Default 15 minutes; minimum 30 seconds.
	 */
	readonly socketLivenessTimeoutMs?: number;
	/** Voice clips in, spoken replies out. Absent keeps every voice behaviour off. */
	readonly voice?: SlackVoiceConfig;
}

/**
 * OpenAI-compatible speech endpoints. Nothing is defaulted: the endpoints, the
 * models and the voice are named here, so the adapter never talks to a provider
 * the operator did not write down.
 *
 * The key comes from a credential file or an environment variable, never inline:
 * a config is readable, quotable, and gets pasted into issues.
 */
export interface SlackVoiceConfig {
	/** Full `/audio/transcriptions` url. */
	readonly endpoint: string;
	readonly model: string;
	/** Pinned transcription language (`ko`); absent lets the model detect it. */
	readonly language?: string;
	readonly apiKeyFile?: string;
	readonly apiKeyEnv?: string;
	readonly timeoutMs?: number;
	/** Full `/audio/speech` url. Spoken replies need it, `speechModel` and `voice` together. */
	readonly speechEndpoint?: string;
	readonly speechModel?: string;
	readonly voice?: string;
	/** Voice overrides by Slack user id or conversation id; the speaker wins over the conversation. */
	readonly voices?: Readonly<Record<string, string>>;
	readonly maxSpokenChars?: number;
	readonly speechTimeoutMs?: number;
}

export interface LoadedSlackAdapterConfig extends Omit<SlackAdapterConfig, "voice"> {
	readonly botToken: string;
	readonly appToken: string;
	readonly configPath: string;
	readonly voice?: LoadedSlackVoiceConfig;
}

export class SlackAdapterStartupError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "SlackAdapterStartupError";
	}
}

export function adapterHome(env: NodeJS.ProcessEnv = process.env): string {
	return env.GAJAEWAY_HOME || join(homedir(), ".gajaeway");
}

export async function loadSlackAdapterConfig(
	env: NodeJS.ProcessEnv = process.env,
	warn: (message: string) => void = (message) => console.warn(message),
): Promise<LoadedSlackAdapterConfig> {
	const configPath = join(adapterHome(env), "adapter-slack.json");
	let raw: unknown;
	try {
		raw = JSON.parse(await readFile(configPath, "utf8"));
	} catch {
		throw new SlackAdapterStartupError(
			`Unable to read Slack adapter config at ${configPath}. Create it with botTokenFile and appTokenFile credential-file paths.`,
		);
	}
	for (const field of ["botTokenFile", "appTokenFile"] as const) {
		if (!isObject(raw) || typeof raw[field] !== "string" || raw[field].trim() === "") {
			throw new SlackAdapterStartupError(
				`Slack adapter config at ${configPath} requires a non-empty ${field} credential-file path.`,
			);
		}
	}
	const config = raw as Record<string, unknown>;
	if (config.gatewaySocket !== undefined && typeof config.gatewaySocket !== "string") {
		throw new SlackAdapterStartupError("Slack adapter gatewaySocket must be a string when set.");
	}
	if (config.liveReplies !== undefined && typeof config.liveReplies !== "boolean") {
		throw new SlackAdapterStartupError("Slack adapter liveReplies must be a boolean when set.");
	}
	if (
		config.socketLivenessTimeoutMs !== undefined &&
		(!Number.isInteger(config.socketLivenessTimeoutMs) ||
			(config.socketLivenessTimeoutMs as number) < MIN_SOCKET_LIVENESS_TIMEOUT_MS)
	) {
		throw new SlackAdapterStartupError(
			`Slack adapter socketLivenessTimeoutMs must be an integer of at least ${MIN_SOCKET_LIVENESS_TIMEOUT_MS} when set.`,
		);
	}
	if (config.channels !== undefined && !validChannels(config.channels)) {
		throw new SlackAdapterStartupError(
			`Slack adapter channels entries may only set engagement to ${ENGAGEMENT_MODES.join(", ")} and audience to ${ENGAGEMENT_AUDIENCES.join(", ")}.`,
		);
	}
	const bot = await loadToken(config.botTokenFile as string, "botTokenFile", "xoxb-", configPath);
	const app = await loadToken(config.appTokenFile as string, "appTokenFile", "xapp-", configPath);
	const voice = await loadVoiceConfig(config.voice, configPath, env, warn);
	const { voice: _rawVoice, ...rest } = config;
	return {
		...rest,
		botTokenFile: bot.path,
		appTokenFile: app.path,
		botToken: bot.token,
		appToken: app.token,
		configPath,
		...(voice ? { voice } : {}),
	} as LoadedSlackAdapterConfig;
}

const SLACK_VOICE_KEY = /^[UWCDG][A-Z0-9]+$/;

/**
 * Resolves the voice section, or undefined when it is absent.
 *
 * A present but broken section is a startup error: runtime voice failures fail
 * open on purpose, but a misspelled key path is a deployment mistake that must
 * not hide behind behaviour that merely looks unconfigured. The one exception
 * is a `voices` override: a bad entry is warned about and dropped, and its
 * speaker falls back to the default voice.
 */
async function loadVoiceConfig(
	raw: unknown,
	configPath: string,
	env: NodeJS.ProcessEnv,
	warn: (message: string) => void,
): Promise<LoadedSlackVoiceConfig | undefined> {
	if (raw === undefined) return undefined;
	if (!isObject(raw)) throw new SlackAdapterStartupError("Slack adapter voice must be an object when set.");
	for (const field of ["endpoint", "model"] as const) {
		if (typeof raw[field] !== "string" || raw[field].trim() === "")
			throw new SlackAdapterStartupError(`Slack adapter voice requires a non-empty ${field}.`);
	}
	for (const field of ["language", "apiKeyFile", "apiKeyEnv", "speechEndpoint", "speechModel", "voice"] as const) {
		if (raw[field] !== undefined && (typeof raw[field] !== "string" || raw[field].trim() === ""))
			throw new SlackAdapterStartupError(`Slack adapter voice ${field} must be a non-empty string when set.`);
	}
	for (const field of ["timeoutMs", "maxSpokenChars", "speechTimeoutMs"] as const) {
		const value = raw[field];
		if (value !== undefined && (!Number.isInteger(value) || (value as number) <= 0))
			throw new SlackAdapterStartupError(`Slack adapter voice ${field} must be a positive integer when set.`);
	}
	for (const field of ["endpoint", "speechEndpoint"] as const) {
		if (raw[field] !== undefined && !isHttpsUrl(raw[field] as string))
			throw new SlackAdapterStartupError(`Slack adapter voice ${field} must be an https url.`);
	}
	const speechFields = [raw.speechEndpoint, raw.speechModel, raw.voice].filter((value) => value !== undefined);
	if (speechFields.length !== 0 && speechFields.length !== 3)
		throw new SlackAdapterStartupError(
			"Slack adapter voice speechEndpoint, speechModel and voice must be set together for spoken replies.",
		);
	if (raw.voices !== undefined && (!isObject(raw.voices) || speechFields.length === 0))
		throw new SlackAdapterStartupError(
			"Slack adapter voice voices must be an object, and needs speechEndpoint, speechModel and voice.",
		);
	const apiKey = await loadVoiceKey(raw, configPath, env);
	const transcription = {
		endpoint: raw.endpoint as string,
		model: raw.model as string,
		apiKey,
		...(raw.language !== undefined ? { language: raw.language as string } : {}),
		...(raw.timeoutMs !== undefined ? { timeoutMs: raw.timeoutMs as number } : {}),
	};
	if (speechFields.length === 0) return { transcription };
	const voices: Record<string, string> = {};
	for (const [key, value] of Object.entries((raw.voices ?? {}) as Record<string, unknown>)) {
		if (!SLACK_VOICE_KEY.test(key)) {
			warn(`Slack adapter voice.voices key ${JSON.stringify(key)} is not a Slack user or conversation id; ignored.`);
			continue;
		}
		if (typeof value !== "string" || value.trim() === "") {
			warn(
				`Slack adapter voice.voices.${key} ${JSON.stringify(value)} is not a non-empty voice name; the default voice is used.`,
			);
			continue;
		}
		voices[key] = value.trim();
	}
	return {
		transcription,
		speech: {
			endpoint: raw.speechEndpoint as string,
			model: raw.speechModel as string,
			voice: raw.voice as string,
			voices,
			apiKey,
			...(raw.maxSpokenChars !== undefined ? { maxSpokenChars: raw.maxSpokenChars as number } : {}),
			...(raw.speechTimeoutMs !== undefined ? { timeoutMs: raw.speechTimeoutMs as number } : {}),
		},
	};
}

/** Exactly one key source; errors name the file or variable, never the value. */
async function loadVoiceKey(raw: Record<string, unknown>, configPath: string, env: NodeJS.ProcessEnv): Promise<string> {
	const file = raw.apiKeyFile as string | undefined;
	const variable = raw.apiKeyEnv as string | undefined;
	if ((file === undefined) === (variable === undefined))
		throw new SlackAdapterStartupError("Slack adapter voice requires exactly one of apiKeyFile or apiKeyEnv.");
	if (variable !== undefined) {
		const key = env[variable]?.trim();
		if (!key) throw new SlackAdapterStartupError(`Voice API key environment variable ${variable} is unset or empty.`);
		return key;
	}
	const path = isAbsolute(file as string) ? (file as string) : resolve(dirname(configPath), file as string);
	let key: string;
	try {
		key = (await readFile(path, "utf8")).trim();
	} catch {
		throw new SlackAdapterStartupError(
			`Unable to read voice API key credential file ${path}. Check voice.apiKeyFile and file permissions.`,
		);
	}
	if (!key) throw new SlackAdapterStartupError(`Voice API key credential file ${path} is empty.`);
	return key;
}

function isHttpsUrl(value: string): boolean {
	try {
		return new URL(value).protocol === "https:";
	} catch {
		return false;
	}
}

/** Credential errors identify the file, never the secret that might be pasted into an issue. */
async function loadToken(
	file: string,
	field: string,
	prefix: string,
	configPath: string,
): Promise<{ path: string; token: string }> {
	const path = isAbsolute(file) ? file : resolve(dirname(configPath), file);
	let token: string;
	try {
		token = (await readFile(path, "utf8")).trim();
	} catch {
		throw new SlackAdapterStartupError(
			`Unable to read Slack token credential file ${path}. Check ${field} and file permissions.`,
		);
	}
	if (!token) throw new SlackAdapterStartupError(`Slack token credential file ${path} is empty.`);
	if (!token.startsWith(prefix))
		throw new SlackAdapterStartupError(`Slack token credential file ${path} must start with ${prefix}.`);
	return { path, token };
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validChannels(value: unknown): value is Record<string, ChannelEngagementPolicy> {
	return (
		isObject(value) &&
		Object.values(value).every(
			(entry) =>
				isObject(entry) &&
				Object.keys(entry).every((key) => key === "engagement" || key === "audience") &&
				(entry.engagement === undefined || ENGAGEMENT_MODES.includes(entry.engagement as never)) &&
				(entry.audience === undefined || ENGAGEMENT_AUDIENCES.includes(entry.audience as never)),
		)
	);
}
