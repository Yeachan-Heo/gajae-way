import { readFile } from "node:fs/promises";
import { join } from "node:path";
import type { DiscordAdapterInput } from "@gajaeway/adapter-discord";
import type { TelegramAdapterInput } from "@gajaeway/adapter-telegram";
import type { GatewayConfig, LocalGatewayPort } from "@gajaeway/gateway";

const CREDENTIAL_NAMES = ["discord", "discordVoice", "telegram"] as const;
const ADMIN_EVENTS = ["chat.message", "chat.progress", "monitor.event", "gateway.stopping"] as const;

type CredentialName = (typeof CREDENTIAL_NAMES)[number];
type ReadText = (path: string) => Promise<string>;

export class AdapterInputError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "AdapterInputError";
	}
}

export interface AdapterInputPorts {
	readonly readText?: ReadText;
}

export interface AdapterInputs {
	readonly discord?: DiscordAdapterInput;
	readonly telegram?: TelegramAdapterInput;
}

/**
 * Reads every configured adapter credential exactly once, then derives the immutable
 * startup inputs from the same GatewayConfig snapshot given to the gateway.
 */
export async function adapterInputs(
	config: GatewayConfig,
	home: string,
	ports: AdapterInputPorts = {},
): Promise<AdapterInputs> {
	const readText = ports.readText ?? (async (path: string) => await readFile(path, "utf8"));
	const credentials = await readCredentials(config, readText);
	const discordConfig = config.adapters?.discord;
	const telegramConfig = config.adapters?.telegram;

	let discord: DiscordAdapterInput | undefined;
	if (discordConfig) {
		const token = credential(credentials, "discord");
		const voice = discordConfig.voice
			? {
					...discordConfig.voice,
					apiKey: credential(credentials, "discordVoice"),
					apiKeyFile: credentialPath(config, "discordVoice"),
				}
			: undefined;
		discord = {
			token,
			...(discordConfig.intents ? { intents: discordConfig.intents } : {}),
			...(voice ? { voice } : {}),
			recoveryChannels: recoveryChannels(config),
			recoveryCursorPath: join(home, "adapters", "discord", "recovery-cursor.json"),
		};
	}

	let telegram: TelegramAdapterInput | undefined;
	if (telegramConfig) telegram = { token: credential(credentials, "telegram") };
	return { ...(discord ? { discord } : {}), ...(telegram ? { telegram } : {}) };
}

/** Discord's restart-bound recovery list accepts `discord:<id>` and legacy bare ids only. */
export function recoveryChannels(config: Pick<GatewayConfig, "channels">): readonly string[] {
	const channels = new Set<string>();
	for (const key of Object.keys(config.channels ?? {})) {
		if (key.startsWith("discord:")) {
			const id = key.slice("discord:".length);
			if (id) channels.add(id);
		} else if (!key.includes(":")) {
			if (key) channels.add(key);
		}
	}
	return [...channels];
}

/** Delays an admin HTTP request until its local gateway port has replayed and negotiated. */
export function requestAfterOpen(
	port: Pick<LocalGatewayPort, "request">,
	opened: Promise<unknown>,
): <T = unknown>(method: string, params?: unknown) => Promise<T> {
	return async <T = unknown>(method: string, params?: unknown): Promise<T> =>
		await opened.then(() => port.request<T>(method, params));
}

/** The console subscribes synchronously during startAdminServer, before `port.open()` replays. */
export function adminEvents(
	port: Pick<LocalGatewayPort, "on">,
): (handler: (event: string, payload: unknown) => void) => () => void {
	return (handler) => {
		const offs = ADMIN_EVENTS.map((event) => port.on(event, (payload) => handler(event, payload)));
		return () => {
			for (const off of offs) off();
		};
	};
}

async function readCredentials(
	config: GatewayConfig,
	readText: ReadText,
): Promise<Partial<Record<CredentialName, string>>> {
	const configured = CREDENTIAL_NAMES.filter((name) => config.credentials?.[name] !== undefined);
	const values = await Promise.all(
		configured.map(async (name) => [name, await readCredential(config, name, readText)] as const),
	);
	return Object.fromEntries(values) as Partial<Record<CredentialName, string>>;
}

async function readCredential(config: GatewayConfig, name: CredentialName, readText: ReadText): Promise<string> {
	const path = credentialPath(config, name);
	let text: string;
	try {
		text = await readText(path);
	} catch {
		throw new AdapterInputError(`credentials.${name} credential file is unreadable: ${path}`);
	}
	const value = text.trim();
	if (!value) throw new AdapterInputError(`credentials.${name} credential file is empty: ${path}`);
	return value;
}

function credential(values: Partial<Record<CredentialName, string>>, name: CredentialName): string {
	const value = values[name];
	if (value) return value;
	throw new AdapterInputError(`credentials.${name} credential file is required`);
}

function credentialPath(config: GatewayConfig, name: CredentialName): string {
	const path = config.credentials?.[name]?.credentialFile;
	if (path) return path;
	throw new AdapterInputError(`credentials.${name} credential file is required`);
}
