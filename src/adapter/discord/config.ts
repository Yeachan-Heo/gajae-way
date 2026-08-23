import * as fs from "node:fs";
import * as path from "node:path";
import { defaultConfig } from "../../config";
import { loadWayProfile, type CanonicalValue, type WayProfile } from "../../profile";
import { DiscordRouteError, validateDiscordRoutes, type DiscordEngagementMode, type DiscordRoute, type DiscordRouteKind } from "./route";


export const DEFAULT_DISCORD_TOKEN_ENV = "GAJAEWAY_DISCORD_BOT_TOKEN";
export type DiscordUnattributedDelivery = "owner-dm" | "suppress";

export interface DiscordAdapterConfig {
	readonly rpcSocketPath: string;
	readonly token: string;
	readonly routes: readonly DiscordRoute[];
	readonly blockedAuthorIds: readonly string[];

	readonly unattributedDelivery: DiscordUnattributedDelivery;
	/** The unique configured owner DM used when unattributed delivery is enabled. */
	readonly unattributedRoute?: DiscordRoute;
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
	const routes = configuredRoutes(adapter, environment, profile);
	const blockedAuthorIds = parseBlockedAuthorIds(adapter.blocked_author_ids);

	const unattributedDelivery = parseUnattributedDelivery(adapter.unattributed_delivery);
	const unattributedRoute = unattributedDelivery === "owner-dm" ? configuredOwnerDmRoute(routes, profile) : undefined;

	const token = resolveToken(adapter, environment, path.dirname(profile.sourcePath));
	const configuredSocket = firstString(adapter, ["rpc_socket", "rpc_socket_path"], "adapter.discord");
	const stateDir = options.stateDir ?? processConfig.stateDir;
	const rpcSocketPath = path.resolve(environment.GAJAEWAY_DISCORD_RPC_SOCKET?.trim() || configuredSocket || path.join(stateDir, "rpc.sock"));
	const ackBudgetMs = optionalInteger(adapter.ack_budget_ms, "adapter.discord.ack_budget_ms", 1, 60_000) ?? 2_000;
	const claimTtlMs = optionalInteger(adapter.claim_ttl_ms, "adapter.discord.claim_ttl_ms", 5_000, 600_000) ?? 5_000;
	const readWaitMs = optionalInteger(adapter.read_wait_ms, "adapter.discord.read_wait_ms", 0, 60_000) ?? 1_000;
	if (readWaitMs >= claimTtlMs) throw new DiscordAdapterConfigError("adapter.discord.read_wait_ms must be shorter than claim_ttl_ms.");
	const apiBaseUrl = optionalString(adapter.api_base_url, "adapter.discord.api_base_url");
	const gatewayUrl = optionalString(adapter.gateway_url, "adapter.discord.gateway_url");
	return {
		rpcSocketPath,
		token,
		routes,
		unattributedDelivery,
		blockedAuthorIds,

		...(unattributedRoute === undefined ? {} : { unattributedRoute }),
		ackBudgetMs,
		claimTtlMs,
		readWaitMs,
		...(apiBaseUrl ? { apiBaseUrl } : {}),
		...(gatewayUrl ? { gatewayUrl } : {}),
	};
}

function configuredRoutes(
	adapter: Record<string, CanonicalValue>,
	environment: NodeJS.ProcessEnv,
	profile: WayProfile,
): readonly DiscordRoute[] {
	if (adapter.routes !== undefined) {
		if (!Array.isArray(adapter.routes)) throw new DiscordAdapterConfigError("adapter.discord.routes must be an array of route tables.");
		if (hasLegacyRouteConfiguration(adapter, environment)) {
			throw new DiscordAdapterConfigError("Define Discord routes through either adapter.discord.routes or the legacy single-route fields, not both.");
		}
		const routes = adapter.routes.map((value, index) => configuredRoute(requiredRecord(value, `adapter.discord.routes[${index}]`), `adapter.discord.routes[${index}]`, profile));
		validateConfiguredRoutes(routes);
		return routes;
	}

	const routeTable = optionalRecord(adapter.route, "adapter.discord.route") ?? {};
	validateKnownFields(routeTable, ["channel_id", "owner_dm_channel_id", "surface_id", "owner_surface_id", "kind", "engagement"], "adapter.discord.route");

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
	const engagement = routeTable.engagement ?? adapter.engagement;
	const route = configuredRoute(
		{
			channel_id: channelId,
			surface_id: surfaceId,
			...(routeTable.kind === undefined ? {} : { kind: routeTable.kind }),
			...(engagement === undefined ? {} : { engagement }),
		},
		"adapter.discord.route",
		profile,
	);

	validateConfiguredRoutes([route]);
	return [route];
}

function hasLegacyRouteConfiguration(adapter: Record<string, CanonicalValue>, environment: NodeJS.ProcessEnv): boolean {
	return (
		adapter.route !== undefined ||
		adapter.channel_id !== undefined ||
		adapter.owner_dm_channel_id !== undefined ||
		adapter.surface_id !== undefined ||
		adapter.owner_surface_id !== undefined ||
		adapter.engagement !== undefined ||

		Boolean(environment.GAJAEWAY_DISCORD_CHANNEL_ID?.trim()) ||
		Boolean(environment.GAJAEWAY_DISCORD_SURFACE_ID?.trim())
	);
}

function configuredRoute(record: Record<string, CanonicalValue>, field: string, profile: WayProfile): DiscordRoute {
	validateKnownFields(record, ["channel_id", "surface_id", "kind", "engagement"], field);
	const channelId = requiredString(record.channel_id, `${field}.channel_id`);
	if (!/^\d+$/.test(channelId)) throw new DiscordAdapterConfigError(`${field}.channel_id must be a numeric Discord snowflake.`);
	const surfaceId = requiredString(record.surface_id, `${field}.surface_id`);
	const surface = profile.knownSurfaces.find(candidate => candidate.id === surfaceId);
	if (!surface || surface.platform !== "discord") {
		throw new DiscordAdapterConfigError(`${field}.surface_id must name a profile-known Discord surface.`);
	}
	const configuredKind = optionalRouteKind(record.kind, `${field}.kind`);
	if (configuredKind !== undefined && configuredKind !== surface.kind) {
		throw new DiscordAdapterConfigError(`${field}.kind must match the profile-known surface kind.`);
	}
	if (surface.kind !== "channel" && surface.kind !== "dm") {
		throw new DiscordAdapterConfigError(`${field}.surface_id must name a Discord dm or channel surface.`);
	}
	const engagement = optionalEngagementMode(record.engagement, `${field}.engagement`);
	if (surface.kind === "dm") {
		if (engagement !== undefined) throw new DiscordAdapterConfigError(`${field}.engagement is not configurable for Discord dm routes.`);
		return { channelId, surfaceId, kind: "dm" };
	}
	return { channelId, surfaceId, kind: "channel", engagement: engagement ?? "mention" };
}

function configuredOwnerDmRoute(routes: readonly DiscordRoute[], profile: WayProfile): DiscordRoute {
	const ownerDmSurfaceIds = new Set(profile.ownerSurfaces.filter(surface => surface.platform === "discord" && surface.kind === "dm").map(surface => surface.id));
	const candidates = routes.filter(route => route.kind === "dm" && ownerDmSurfaceIds.has(route.surfaceId));
	if (candidates.length !== 1) {
		throw new DiscordAdapterConfigError("adapter.discord.unattributed_delivery=owner-dm requires exactly one configured owner Discord DM route.");
	}
	return candidates[0] as DiscordRoute;
}

function parseUnattributedDelivery(value: CanonicalValue | undefined): DiscordUnattributedDelivery {
	const policy = optionalString(value, "adapter.discord.unattributed_delivery") ?? "owner-dm";
	if (policy !== "owner-dm" && policy !== "suppress") {
		throw new DiscordAdapterConfigError("adapter.discord.unattributed_delivery must be owner-dm or suppress.");
	}
	return policy;
}

function validateConfiguredRoutes(routes: readonly DiscordRoute[]): void {
	try {
		validateDiscordRoutes(routes);
	} catch (error) {
		const message = error instanceof DiscordRouteError ? error.message : String(error);
		throw new DiscordAdapterConfigError(message);
	}
}

function adapterTable(profile: WayProfile): Record<string, CanonicalValue> {
	const adapter = optionalRecord(profile.tunables.adapter, "adapter");
	const adapters = optionalRecord(profile.tunables.adapters, "adapters");
	const fromAdapter = adapter ? optionalRecord(adapter.discord, "adapter.discord") : undefined;
	const fromAdapters = adapters ? optionalRecord(adapters.discord, "adapters.discord") : undefined;
	if (fromAdapter && fromAdapters) throw new DiscordAdapterConfigError("Define Discord configuration in only one of adapter.discord or adapters.discord.");
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
		"routes",
		"unattributed_delivery",
		"blocked_author_ids",
		"engagement",

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

function resolveToken(adapter: Record<string, CanonicalValue>, environment: NodeJS.ProcessEnv, profileDirectory: string): string {
	const tokenEnv = optionalString(adapter.token_env, "adapter.discord.token_env") ?? DEFAULT_DISCORD_TOKEN_ENV;
	if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tokenEnv)) {
		throw new DiscordAdapterConfigError("adapter.discord.token_env must be an environment variable name.");
	}
	const tokenFromEnvironment = environment[tokenEnv]?.trim() || environment.GAJAEWAY_DISCORD_TOKEN?.trim();
	const fileReference = environment.GAJAEWAY_DISCORD_TOKEN_FILE?.trim() ?? firstString(adapter, ["token_file", "credential_file"], "adapter.discord");
	if (tokenFromEnvironment && fileReference) {
		throw new DiscordAdapterConfigError("Configure the Discord token through either an environment variable or a credential file, not both.");
	}
	if (tokenFromEnvironment) return normalizeToken(tokenFromEnvironment, `environment variable ${tokenEnv}`);
	if (!fileReference) {
		throw new DiscordAdapterConfigError(`Discord token is missing: set ${tokenEnv} or configure adapter.discord.token_file.`);
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

function firstString(record: Record<string, CanonicalValue>, keys: readonly string[], field: string): string | undefined {
	for (const key of keys) {
		if (record[key] !== undefined) return optionalString(record[key], `${field}.${key}`);
	}
	return undefined;
}

function requiredString(value: CanonicalValue | undefined, field: string): string {
	const string = optionalString(value, field);
	if (string === undefined) throw new DiscordAdapterConfigError(`${field} is required.`);
	return string;
}

function optionalString(value: CanonicalValue | undefined, field: string): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !value.trim()) throw new DiscordAdapterConfigError(`${field} must be a non-empty string.`);
	return value.trim();
}

function optionalRouteKind(value: CanonicalValue | undefined, field: string): DiscordRouteKind | undefined {
	const kind = optionalString(value, field);
	if (kind === undefined) return undefined;
	if (kind !== "channel" && kind !== "dm") throw new DiscordAdapterConfigError(`${field} must be channel or dm.`);
	return kind;
}

function optionalEngagementMode(value: CanonicalValue | undefined, field: string): DiscordEngagementMode | undefined {
	const engagement = optionalString(value, field);
	if (engagement === undefined) return undefined;
	if (engagement !== "mention" && engagement !== "always") {
		throw new DiscordAdapterConfigError(`${field} must be mention or always.`);
	}
	return engagement;
}

function parseBlockedAuthorIds(value: CanonicalValue | undefined): readonly string[] {
	if (value === undefined) return [];
	if (!Array.isArray(value)) throw new DiscordAdapterConfigError("adapter.discord.blocked_author_ids must be an array of Discord snowflakes.");
	const ids = value.map((entry, index) => {
		const id = optionalString(entry, `adapter.discord.blocked_author_ids[${index}]`);
		if (id === undefined || !/^\d+$/.test(id)) {
			throw new DiscordAdapterConfigError(`adapter.discord.blocked_author_ids[${index}] must be a Discord snowflake.`);
		}
		return id;
	});
	if (new Set(ids).size !== ids.length) throw new DiscordAdapterConfigError("adapter.discord.blocked_author_ids must not contain duplicates.");
	return ids;
}

function optionalInteger(value: CanonicalValue | undefined, field: string, minimum: number, maximum: number): number | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new DiscordAdapterConfigError(`${field} must be an integer in ${minimum}..=${maximum}.`);
	}
	return value;
}

function requiredRecord(value: CanonicalValue, field: string): Record<string, CanonicalValue> {
	if (!isRecord(value)) throw new DiscordAdapterConfigError(`${field} must be a table.`);
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
