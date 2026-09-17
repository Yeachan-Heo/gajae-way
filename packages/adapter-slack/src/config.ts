import { readFile } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { type ChannelEngagementPolicy, ENGAGEMENT_AUDIENCES, ENGAGEMENT_MODES } from "@gajaeway/protocol";

export interface SlackAdapterConfig {
	readonly botTokenFile: string;
	readonly appTokenFile: string;
	readonly gatewaySocket?: string;
	readonly channels?: Readonly<Record<string, ChannelEngagementPolicy>>;
}

export interface LoadedSlackAdapterConfig extends SlackAdapterConfig {
	readonly botToken: string;
	readonly appToken: string;
	readonly configPath: string;
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

export async function loadSlackAdapterConfig(env: NodeJS.ProcessEnv = process.env): Promise<LoadedSlackAdapterConfig> {
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
	if (config.channels !== undefined && !validChannels(config.channels)) {
		throw new SlackAdapterStartupError(
			`Slack adapter channels entries may only set engagement to ${ENGAGEMENT_MODES.join(", ")} and audience to ${ENGAGEMENT_AUDIENCES.join(", ")}.`,
		);
	}
	const bot = await loadToken(config.botTokenFile as string, "botTokenFile", "xoxb-", configPath);
	const app = await loadToken(config.appTokenFile as string, "appTokenFile", "xapp-", configPath);
	return {
		...config,
		botTokenFile: bot.path,
		appTokenFile: app.path,
		botToken: bot.token,
		appToken: app.token,
		configPath,
	} as LoadedSlackAdapterConfig;
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
