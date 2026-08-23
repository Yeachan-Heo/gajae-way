import * as fs from "node:fs";
import * as path from "node:path";
import { defaultConfig } from "../../config";
import { loadWayProfile, type CanonicalValue, type WayProfile } from "../../profile";
import type { DiscordRoute } from "../discord/route";

export interface TelegramAdapterConfig {
	readonly enabled: boolean;
	readonly token: string;
	readonly routes: readonly DiscordRoute[];
	readonly stateDir: string;
	readonly rpcSocketPath: string;
	readonly allowBots: boolean;
	readonly blockedAuthorIds: readonly string[];
}

export function loadTelegramAdapterConfig(options: { environment?: NodeJS.ProcessEnv; profile?: WayProfile; profilePath?: string; stateDir?: string } = {}): TelegramAdapterConfig {
	const environment = options.environment ?? process.env;
	const processConfig = defaultConfig(environment);
	const profile = options.profile ?? loadWayProfile(options.profilePath ?? processConfig.profilePath);
	const root = profile.tunables.adapter && record(profile.tunables.adapter).telegram !== undefined ? record(record(profile.tunables.adapter).telegram) : record(record(profile.tunables.adapters).telegram);
	const enabled = root.enabled === true;
	if (!enabled) {
		return {
			enabled: false,
			token: "",
			routes: [],
			stateDir: path.resolve(options.stateDir ?? path.join(processConfig.stateDir, "telegram")),
			rpcSocketPath: path.resolve(environment.GAJAEWAY_DISCORD_RPC_SOCKET?.trim() || path.join(processConfig.stateDir, "rpc.sock")),
			allowBots: true,
			blockedAuthorIds: [],
		};
	}
	const token = resolveToken(root, environment, path.dirname(profile.sourcePath));
	const routes = Array.isArray(root.routes) ? root.routes.map((value, index) => telegramRoute(value, index, profile)) : [];
	return {
		enabled,
		token,
		routes,
		stateDir: path.resolve(options.stateDir ?? path.join(processConfig.stateDir, "telegram")),
		rpcSocketPath: path.resolve(environment.GAJAEWAY_DISCORD_RPC_SOCKET?.trim() || path.join(processConfig.stateDir, "rpc.sock")),
		allowBots: root.allowBots !== false,
		blockedAuthorIds: Array.isArray(root.blocked_author_ids) ? root.blocked_author_ids.map(String) : [],
	};
}

function telegramRoute(value: CanonicalValue, index: number, profile: WayProfile): DiscordRoute {
	const route = record(value);
	const surfaceId = String(route.surface_id ?? "");
	const channelId = String(route.chat_id ?? route.channel_id ?? "");
	const surface = profile.knownSurfaces.find(candidate => candidate.id === surfaceId);
	if (!surface || surface.platform !== "telegram") throw new Error(`adapter.telegram.routes[${index}].surface_id must name a Telegram surface.`);
	if (!channelId) throw new Error(`adapter.telegram.routes[${index}].chat_id is required.`);
	return {
		surfaceId,
		channelId,
		kind: surface.kind === "dm" ? "dm" : "channel",
		...(typeof route.groupPolicy === "string" ? { groupPolicy: route.groupPolicy as "open" | "mention" } : {}),
	};
}

function resolveToken(adapter: Record<string, any>, environment: NodeJS.ProcessEnv, profileDirectory: string): string {
	const envName = typeof adapter.token_env === "string" ? adapter.token_env : "GAJAEWAY_TELEGRAM_BOT_TOKEN";
	const fromEnv = environment[envName]?.trim();
	const file = environment.GAJAEWAY_TELEGRAM_TOKEN_FILE?.trim() ?? (typeof adapter.token_file === "string" ? adapter.token_file : undefined);
	if (fromEnv && file) throw new Error("Configure Telegram token through either an environment variable or credential file, not both.");
	if (fromEnv) return fromEnv;
	if (!file) throw new Error(`Telegram token is missing: set ${envName} or configure adapter.telegram.token_file.`);
	return fs.readFileSync(path.resolve(profileDirectory, file), "utf8").trim();
}

function record(value: unknown): Record<string, any> {
	return typeof value === "object" && value !== null && !Array.isArray(value) ? value as Record<string, any> : {};
}
