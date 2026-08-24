import * as fs from "node:fs";
import * as path from "node:path";
import { loadWayProfile, type WayProfile } from "../../profile";

export const TELEGRAM_TOKEN_ENV = "GAJAEWAY_TELEGRAM_BOT_TOKEN";
export const TELEGRAM_CONSUMER_ID = "gajaeway-telegram";

export interface TelegramAdapterConfig {
	readonly rpcSocketPath: string;
	readonly token: string;
	readonly chatId: string;
	readonly surfaceId: string;
	readonly ackBudgetMs: number;
	readonly claimTtlMs: number;
	readonly readWaitMs: number;
	readonly apiBaseUrl?: string;
}

export interface LoadTelegramAdapterConfigOptions {
	readonly environment?: NodeJS.ProcessEnv;
	readonly stateDir?: string;
	readonly profilePath?: string;
	readonly profile?: WayProfile;
}

export class TelegramAdapterConfigError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "TelegramAdapterConfigError";
	}
}

/**
 * Loads mutable adapter configuration from `[adapter.telegram]`.
 *
 * Adapter settings deliberately stay outside the profile's digest-bound
 * identity projection: they are the hot-reload tunable class, not gateway
 * identity, so changing a poll budget must never require a profile approval.
 */
export function loadTelegramAdapterConfig(options: LoadTelegramAdapterConfigOptions = {}): TelegramAdapterConfig {
	const environment = options.environment ?? process.env;
	const profilePath = options.profilePath ?? environment.GAJAEWAY_PROFILE;
	if (!options.profile && !profilePath) {
		throw new TelegramAdapterConfigError("A profile path is required to load the Telegram adapter configuration.");
	}
	const profile = options.profile ?? loadWayProfile(profilePath as string);
	const adapters = asRecord(profile.tunables.adapter) ?? asRecord(profile.tunables.adapters);
	const adapter = asRecord(adapters?.telegram);
	if (!adapter) throw new TelegramAdapterConfigError("[adapter.telegram] is not configured in the profile.");
	noUnknownKeys(adapter, [
		"token_env",
		"token_file",
		"rpc_socket_path",
		"chat_id",
		"surface_id",
		"ack_budget_ms",
		"claim_ttl_ms",
		"read_wait_ms",
		"api_base_url",
	]);

	const chatId = requiredString(adapter.chat_id, "adapter.telegram.chat_id");
	// Telegram supergroup ids are negative, so only the digits are constrained.
	if (!/^-?\d+$/.test(chatId)) {
		throw new TelegramAdapterConfigError("adapter.telegram.chat_id must be a Telegram chat id.");
	}
	const surfaceId = requiredString(adapter.surface_id, "adapter.telegram.surface_id");
	if (!profile.knownSurfaces.some((surface) => surface.id === surfaceId)) {
		throw new TelegramAdapterConfigError(`adapter.telegram.surface_id ${surfaceId} is not a declared surface.`);
	}

	const stateDir = options.stateDir ?? environment.GAJAEWAY_STATE_DIR;
	const configuredSocket = optionalString(adapter.rpc_socket_path, "adapter.telegram.rpc_socket_path");
	const socketSource =
		environment.GAJAEWAY_TELEGRAM_RPC_SOCKET?.trim() ||
		configuredSocket ||
		(stateDir ? path.join(stateDir, "rpc.sock") : undefined);
	if (!socketSource) throw new TelegramAdapterConfigError("A gateway RPC socket path could not be resolved.");

	const claimTtlMs = boundedInteger(adapter.claim_ttl_ms, "adapter.telegram.claim_ttl_ms", 5_000, 600_000) ?? 5_000;
	const readWaitMs = boundedInteger(adapter.read_wait_ms, "adapter.telegram.read_wait_ms", 0, 60_000) ?? 1_000;
	if (readWaitMs >= claimTtlMs) {
		throw new TelegramAdapterConfigError("adapter.telegram.read_wait_ms must be shorter than claim_ttl_ms.");
	}

	return {
		rpcSocketPath: path.resolve(socketSource),
		token: resolveToken(adapter, environment),
		chatId,
		surfaceId,
		ackBudgetMs: boundedInteger(adapter.ack_budget_ms, "adapter.telegram.ack_budget_ms", 1, 60_000) ?? 2_000,
		claimTtlMs,
		readWaitMs,
		...(optionalString(adapter.api_base_url, "adapter.telegram.api_base_url")
			? { apiBaseUrl: optionalString(adapter.api_base_url, "adapter.telegram.api_base_url") as string }
			: {}),
	};
}

/**
 * A credential file takes precedence over an env name, matching the Discord
 * adapter: systemd `LoadCredential` is the deployment path, and the env-name
 * fallback exists for local development only.
 */
function resolveToken(adapter: Record<string, unknown>, environment: NodeJS.ProcessEnv): string {
	const tokenFileEnv = environment.GAJAEWAY_TELEGRAM_TOKEN_FILE?.trim();
	const tokenFile = tokenFileEnv || optionalString(adapter.token_file, "adapter.telegram.token_file");
	if (tokenFile) {
		const contents = fs.readFileSync(tokenFile, "utf8").trim();
		if (!contents) throw new TelegramAdapterConfigError(`Telegram token file ${tokenFile} is empty.`);
		return contents;
	}
	const tokenEnv = optionalString(adapter.token_env, "adapter.telegram.token_env") ?? TELEGRAM_TOKEN_ENV;
	const token = environment[tokenEnv]?.trim();
	if (!token) throw new TelegramAdapterConfigError(`Telegram bot token environment variable ${tokenEnv} is empty.`);
	return token;
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
	return typeof value === "object" && value !== null && !Array.isArray(value)
		? (value as Record<string, unknown>)
		: undefined;
}

function requiredString(value: unknown, field: string): string {
	if (typeof value !== "string" || !value.trim())
		throw new TelegramAdapterConfigError(`${field} must be a non-empty string.`);
	return value.trim();
}

function optionalString(value: unknown, field: string): string | undefined {
	if (value === undefined) return undefined;
	return requiredString(value, field);
}

function boundedInteger(value: unknown, field: string, minimum: number, maximum: number): number | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
		throw new TelegramAdapterConfigError(`${field} must be an integer in ${minimum}..=${maximum}.`);
	}
	return value;
}

function noUnknownKeys(record: Record<string, unknown>, allowed: readonly string[]): void {
	for (const key of Object.keys(record)) {
		if (!allowed.includes(key))
			throw new TelegramAdapterConfigError(`adapter.telegram.${key} is not a supported field.`);
	}
}
