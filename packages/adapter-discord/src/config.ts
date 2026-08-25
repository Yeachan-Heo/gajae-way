import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";

export interface DiscordAdapterConfig {
	readonly tokenFile: string;
	readonly gatewaySocket?: string;
	readonly intents?: readonly number[];
	readonly channels?: Readonly<Record<string, { readonly engagement?: "open" }>>;
}

export interface LoadedDiscordAdapterConfig extends DiscordAdapterConfig {
	readonly token: string;
	readonly configPath: string;
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
	} catch (error) {
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
	return { ...raw, tokenFile, token, configPath } as LoadedDiscordAdapterConfig;
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
