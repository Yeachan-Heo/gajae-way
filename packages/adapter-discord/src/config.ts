import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";

/**
 * Inbound voice-message transcription.
 *
 * The key lives in a file, never inline, for the same reason the Discord token
 * does: a config is readable, quotable, and gets pasted into issues.
 *
 * Absent `voice` means transcription is off and voice messages arrive with
 * their url alone — the pre-existing behaviour — so omitting this section can
 * never break an existing deployment.
 */
export interface DiscordVoiceConfig {
	readonly apiKeyFile: string;
	/**
	 * Pinned language for transcription; omitted means auto-detect.
	 *
	 * Auto-detect measured p=1.0 on long Korean speech but failed *confidently*
	 * on short clips — a 1.9s Korean message came back as Spanish at p=0.999 —
	 * so a single-language deployment should pin this. A confidence floor cannot
	 * substitute: the wrong answer arrived at maximum confidence.
	 */
	readonly languageCode?: string;
	readonly endpoint?: string;
	readonly model?: string;
	readonly timeoutMs?: number;
	/** Outbound speech; omitted fields fall back to the tested ElevenLabs defaults. */
	readonly voiceId?: string;
	readonly speechModel?: string;
	readonly speechEndpoint?: string;
	readonly outputFormat?: string;
	/**
	 * Optional spoken length cap. Unset means the whole reply is spoken.
	 *
	 * Capping was tried and reverted: a listener cannot read the remainder out of
	 * the text, so a truncated utterance is a truncated answer for the only
	 * person the audio exists for.
	 */
	readonly maxSpokenChars?: number;
	readonly speechTimeoutMs?: number;
}

export interface LoadedDiscordVoiceConfig extends DiscordVoiceConfig {
	readonly apiKey: string;
}

export interface DiscordAdapterConfig {
	readonly tokenFile: string;
	readonly gatewaySocket?: string;
	readonly intents?: readonly number[];
	readonly channels?: Readonly<Record<string, { readonly engagement?: "open" }>>;
	readonly voice?: DiscordVoiceConfig;
}

export interface LoadedDiscordAdapterConfig extends DiscordAdapterConfig {
	readonly token: string;
	readonly configPath: string;
	readonly voice?: LoadedDiscordVoiceConfig;
}

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
	const voice = await loadVoiceConfig(raw.voice, configPath);
	return { ...raw, tokenFile, token, configPath, ...(voice ? { voice } : {}) } as LoadedDiscordAdapterConfig;
}

/**
 * Resolves the voice section, or undefined when it is absent.
 *
 * A *present but broken* voice section is a startup error, not a silent
 * downgrade. Transcription failures at runtime fail open on purpose, but a
 * misspelled key path is a deployment mistake the operator has to see — falling
 * back to "no transcription" would hide it behind behaviour that looks merely
 * unconfigured.
 */
async function loadVoiceConfig(raw: unknown, configPath: string): Promise<LoadedDiscordVoiceConfig | undefined> {
	if (raw === undefined) return undefined;
	if (!isObject(raw) || typeof raw.apiKeyFile !== "string" || raw.apiKeyFile.trim() === "") {
		throw new DiscordAdapterStartupError("Discord adapter voice requires a non-empty apiKeyFile credential-file path.");
	}
	for (const field of [
		"languageCode",
		"endpoint",
		"model",
		"voiceId",
		"speechModel",
		"speechEndpoint",
		"outputFormat",
	] as const) {
		if (raw[field] !== undefined && typeof raw[field] !== "string")
			throw new DiscordAdapterStartupError(`Discord adapter voice ${field} must be a string when set.`);
	}
	for (const field of ["timeoutMs", "maxSpokenChars", "speechTimeoutMs"] as const) {
		const value = raw[field];
		if (value !== undefined && (!Number.isInteger(value) || (value as number) <= 0))
			throw new DiscordAdapterStartupError(`Discord adapter voice ${field} must be a positive integer when set.`);
	}
	const apiKeyFile = isAbsolute(raw.apiKeyFile) ? raw.apiKeyFile : resolve(dirname(configPath), raw.apiKeyFile);
	let apiKey: string;
	try {
		apiKey = (await readFile(apiKeyFile, "utf8")).trim();
	} catch {
		throw new DiscordAdapterStartupError(
			`Unable to read voice API key credential file ${apiKeyFile}. Check voice.apiKeyFile and file permissions.`,
		);
	}
	if (!apiKey) throw new DiscordAdapterStartupError(`Voice API key credential file ${apiKeyFile} is empty.`);
	return { ...(raw as unknown as DiscordVoiceConfig), apiKeyFile, apiKey };
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
