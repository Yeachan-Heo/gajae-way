import * as fs from "node:fs";
import * as path from "node:path";
import { defaultConfig } from "../../config";
import { type CanonicalValue, loadWayProfile, type WayProfile } from "../../profile";

/** One static v1 owner-DM route from a Discord channel to a configured surface. */
export interface DiscordRoute {
	readonly surfaceId: string;
	readonly channelId: string;
}

export const DISCORD_CONSUMER_ID = "gajaeway-discord";
export const DEFAULT_DISCORD_TOKEN_ENV = "GAJAEWAY_DISCORD_BOT_TOKEN";

export interface DiscordAdapterConfig {
	readonly rpcSocketPath: string;
	readonly token: string;
	readonly route: DiscordRoute;
	readonly ackBudgetMs: number;
	readonly claimTtlMs: number;
	readonly readWaitMs: number;
	readonly apiBaseUrl?: string;
	readonly gatewayUrl?: string;
}

export interface LoadDiscordAdapterConfigOptions {
	readonly environment?: NodeJS.ProcessEnv;
	readonly stateDir?: string;
	readonly profilePath?: string;
	readonly profile?: WayProfile;
}

export class DiscordAdapterConfigError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "DiscordAdapterConfigError";
	}
}

/**
 * Loads mutable adapter configuration from `[adapter.discord]` (or the
 * equivalent `[adapters.discord]`) and resolves its secret reference at
 * process start. Adapter settings deliberately remain outside the profile's
 * digest-bound identity projection.
 */
export function loadDiscordAdapterConfig(options: LoadDiscordAdapterConfigOptions = {}): DiscordAdapterConfig {
	const environment = options.environment ?? process.env;
	const processConfig = defaultConfig(environment);
	const profilePath = options.profilePath ?? processConfig.profilePath;
	const profile = options.profile ?? loadWayProfile(profilePath);
	const adapter = adapterTable(profile);
	const routeTable = optionalRecord(adapter.route, "adapter.discord.route") ?? {};
	validateKnownFields(
		routeTable,
		["channel_id", "owner_dm_channel_id", "surface_id", "owner_surface_id"],
		"adapter.discord.route",
	);
	const channelId = envOrString(
		environment.GAJAEWAY_DISCORD_CHANNEL_ID,
		firstString(routeTable, ["channel_id", "owner_dm_channel_id"], "adapter.discord.route") ??
			firstString(adapter, ["channel_id", "owner_dm_channel_id"], "adapter.discord"),
		"Discord channel id",
	);
	const surfaceId = envOrString(
		environment.GAJAEWAY_DISCORD_SURFACE_ID,
		firstString(routeTable, ["surface_id", "owner_surface_id"], "adapter.discord.route") ??
			firstString(adapter, ["surface_id", "owner_surface_id"], "adapter.discord"),
		"Discord surface id",
	);
	if (!/^\d+$/.test(channelId))
		throw new DiscordAdapterConfigError("Discord channel id must be a numeric Discord snowflake.");
	const ownerSurface = profile.ownerSurfaces.find((surface) => surface.id === surfaceId);
	if (!ownerSurface || ownerSurface.platform !== "discord" || ownerSurface.kind !== "dm") {
		throw new DiscordAdapterConfigError("Discord route surface_id must name a configured owner Discord DM surface.");
	}

	const token = resolveToken(adapter, environment, path.dirname(profile.sourcePath));
	const configuredSocket = firstString(adapter, ["rpc_socket", "rpc_socket_path"], "adapter.discord");
	const stateDir = options.stateDir ?? processConfig.stateDir;
	const rpcSocketPath = path.resolve(
		environment.GAJAEWAY_DISCORD_RPC_SOCKET?.trim() || configuredSocket || path.join(stateDir, "rpc.sock"),
	);
	const ackBudgetMs = optionalInteger(adapter.ack_budget_ms, "adapter.discord.ack_budget_ms", 1, 60_000) ?? 2_000;
	const claimTtlMs = optionalInteger(adapter.claim_ttl_ms, "adapter.discord.claim_ttl_ms", 5_000, 600_000) ?? 5_000;
	const readWaitMs = optionalInteger(adapter.read_wait_ms, "adapter.discord.read_wait_ms", 0, 60_000) ?? 1_000;
	if (readWaitMs >= claimTtlMs)
		throw new DiscordAdapterConfigError("adapter.discord.read_wait_ms must be shorter than claim_ttl_ms.");
	const apiBaseUrl = optionalString(adapter.api_base_url, "adapter.discord.api_base_url");
	const gatewayUrl = optionalString(adapter.gateway_url, "adapter.discord.gateway_url");
	return {
		rpcSocketPath,
		token,
		route: { channelId, surfaceId },
		ackBudgetMs,
		claimTtlMs,
		readWaitMs,
		...(apiBaseUrl ? { apiBaseUrl } : {}),
		...(gatewayUrl ? { gatewayUrl } : {}),
	};
}

function adapterTable(profile: WayProfile): Record<string, CanonicalValue> {
	const adapter = optionalRecord(profile.tunables.adapter, "adapter");
	const adapters = optionalRecord(profile.tunables.adapters, "adapters");
	const fromAdapter = adapter ? optionalRecord(adapter.discord, "adapter.discord") : undefined;
	const fromAdapters = adapters ? optionalRecord(adapters.discord, "adapters.discord") : undefined;
	if (fromAdapter && fromAdapters)
		throw new DiscordAdapterConfigError(
			"Define Discord configuration in only one of adapter.discord or adapters.discord.",
		);
	const selected = fromAdapter ?? fromAdapters;
	if (!selected) throw new DiscordAdapterConfigError("Profile is missing [adapter.discord] configuration.");
	const allowed = new Set([
		"token_env",
		"token_file",
		"credential_file",
		"channel_id",
		"owner_dm_channel_id",
		"surface_id",
		"owner_surface_id",
		"route",
		"rpc_socket",
		"rpc_socket_path",
		"ack_budget_ms",
		"claim_ttl_ms",
		"read_wait_ms",
		"api_base_url",
		"gateway_url",
	]);
	for (const key of Object.keys(selected)) {
		if (!allowed.has(key)) throw new DiscordAdapterConfigError(`Unknown adapter.discord configuration field: ${key}.`);
	}
	return selected;
}

function resolveToken(
	adapter: Record<string, CanonicalValue>,
	environment: NodeJS.ProcessEnv,
	profileDirectory: string,
): string {
	const tokenEnv = optionalString(adapter.token_env, "adapter.discord.token_env") ?? DEFAULT_DISCORD_TOKEN_ENV;
	if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tokenEnv)) {
		throw new DiscordAdapterConfigError("adapter.discord.token_env must be an environment variable name.");
	}
	const tokenFromEnvironment = environment[tokenEnv]?.trim() || environment.GAJAEWAY_DISCORD_TOKEN?.trim();
	const fileReference =
		environment.GAJAEWAY_DISCORD_TOKEN_FILE?.trim() ??
		firstString(adapter, ["token_file", "credential_file"], "adapter.discord");
	if (tokenFromEnvironment && fileReference) {
		throw new DiscordAdapterConfigError(
			"Configure the Discord token through either an environment variable or a credential file, not both.",
		);
	}
	if (tokenFromEnvironment) return normalizeToken(tokenFromEnvironment, `environment variable ${tokenEnv}`);
	if (!fileReference) {
		throw new DiscordAdapterConfigError(
			`Discord token is missing: set ${tokenEnv} or configure adapter.discord.token_file.`,
		);
	}
	const credentialPath = path.resolve(profileDirectory, fileReference);
	let contents: string;
	try {
		const stat = fs.statSync(credentialPath);
		if (!stat.isFile()) throw new Error("not a regular file");
		contents = fs.readFileSync(credentialPath, "utf8");
	} catch (error) {
		throw new DiscordAdapterConfigError(
			`Could not read Discord credential file ${credentialPath}: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	return normalizeToken(contents, `credential file ${credentialPath}`);
}

function normalizeToken(value: string, source: string): string {
	const token = value.trim();
	if (!token) throw new DiscordAdapterConfigError(`Discord token from ${source} is empty.`);
	if (/\s/.test(token)) throw new DiscordAdapterConfigError(`Discord token from ${source} contains whitespace.`);
	return token;
}

function envOrString(environmentValue: string | undefined, configuredValue: string | undefined, label: string): string {
	const value = environmentValue?.trim() || configuredValue;
	if (!value) throw new DiscordAdapterConfigError(`${label} is required.`);
	return value;
}

function firstString(
	record: Record<string, CanonicalValue>,
	keys: readonly string[],
	field: string,
): string | undefined {
	for (const key of keys) {
		if (record[key] !== undefined) return optionalString(record[key], `${field}.${key}`);
	}
	return undefined;
}

function optionalString(value: CanonicalValue | undefined, field: string): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !value.trim())
		throw new DiscordAdapterConfigError(`${field} must be a non-empty string.`);
	return value.trim();
}

function optionalInteger(
	value: CanonicalValue | undefined,
	field: string,
	minimum: number,
	maximum: number,
): number | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new DiscordAdapterConfigError(`${field} must be an integer in ${minimum}..=${maximum}.`);
	}
	return value;
}

function optionalRecord(value: CanonicalValue | undefined, field: string): Record<string, CanonicalValue> | undefined {
	if (value === undefined) return undefined;
	if (!isRecord(value)) throw new DiscordAdapterConfigError(`${field} must be a table.`);
	return value;
}

function validateKnownFields(record: Record<string, CanonicalValue>, allowed: readonly string[], field: string): void {
	for (const key of Object.keys(record)) {
		if (!allowed.includes(key)) throw new DiscordAdapterConfigError(`Unknown ${field} configuration field: ${key}.`);
	}
}

function isRecord(value: unknown): value is Record<string, CanonicalValue> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}
