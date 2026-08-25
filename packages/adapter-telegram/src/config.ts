import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";

export interface TelegramAdapterConfig {
	readonly tokenFile: string;
	readonly gatewaySocket?: string;
	readonly chats?: Readonly<Record<string, { readonly engagement?: "open" }>>;
}

export interface LoadedTelegramAdapterConfig extends TelegramAdapterConfig {
	readonly token: string;
	readonly configPath: string;
}

export class TelegramAdapterStartupError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "TelegramAdapterStartupError";
	}
}

export function adapterHome(env: NodeJS.ProcessEnv = process.env): string {
	return env.GAJAEWAY_HOME || join(homedir(), ".gajaeway");
}

export async function loadTelegramAdapterConfig(
	env: NodeJS.ProcessEnv = process.env,
): Promise<LoadedTelegramAdapterConfig> {
	const configPath = join(adapterHome(env), "adapter-telegram.json");
	let raw: unknown;
	try {
		raw = JSON.parse(await readFile(configPath, "utf8"));
	} catch {
		throw new TelegramAdapterStartupError(
			`Unable to read Telegram adapter config at ${configPath}. Create it with a tokenFile credential-file path.`,
		);
	}
	if (!isObject(raw) || typeof raw.tokenFile !== "string" || raw.tokenFile.trim() === "") {
		throw new TelegramAdapterStartupError(
			`Telegram adapter config at ${configPath} requires a non-empty tokenFile credential-file path.`,
		);
	}
	if (raw.gatewaySocket !== undefined && typeof raw.gatewaySocket !== "string") {
		throw new TelegramAdapterStartupError("Telegram adapter gatewaySocket must be a string when set.");
	}
	if (raw.chats !== undefined && !validChats(raw.chats)) {
		throw new TelegramAdapterStartupError('Telegram adapter chats entries may only set engagement to "open".');
	}
	const tokenFile = isAbsolute(raw.tokenFile) ? raw.tokenFile : resolve(dirname(configPath), raw.tokenFile);
	let token: string;
	try {
		token = (await readFile(tokenFile, "utf8")).trim();
	} catch {
		throw new TelegramAdapterStartupError(
			`Unable to read Telegram token credential file ${tokenFile}. Check tokenFile and file permissions.`,
		);
	}
	if (!token) throw new TelegramAdapterStartupError(`Telegram token credential file ${tokenFile} is empty.`);
	return { ...raw, tokenFile, token, configPath } as LoadedTelegramAdapterConfig;
}

function isObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validChats(value: unknown): value is Record<string, { readonly engagement?: "open" }> {
	return (
		isObject(value) &&
		Object.values(value).every(
			(entry) => isObject(entry) && (entry.engagement === undefined || entry.engagement === "open"),
		)
	);
}
