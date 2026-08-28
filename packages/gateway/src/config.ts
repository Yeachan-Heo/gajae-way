import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { type OriginRef, validateOriginRef } from "@gajaeway/protocol";

export const CONFIG_SCHEMA_VERSION = 1;

export interface CredentialFileReference {
	readonly credentialFile: string;
}

export type GjcModelSelection = string | { readonly preset: string };

export interface GatewayConfigFile {
	readonly schemaVersion: typeof CONFIG_SCHEMA_VERSION;
	readonly logVerbosity?: "debug" | "info" | "warn" | "error";
	readonly socketPath?: string;
	readonly dbPath?: string;
	/** Per-turn gjc ceiling in milliseconds; default 300000. Long agentic turns need more. */
	readonly turnTimeoutMs?: number;
	/** Explicit gjc model selector, or a model profile preset whose default role may contain a fallback chain. */
	readonly model?: GjcModelSelection;
	readonly credentials?: Readonly<Record<string, CredentialFileReference>>;
	readonly channels?: Readonly<Record<string, ChannelPolicy>>;
	/** Default inbound debounce window in milliseconds; per-channel `debounceMs` overrides it. */
	readonly debounceMs?: number;
	/** Author ids allowed to trigger mention-gated group turns; absent/empty = anyone. */
	readonly mentionAllowlist?: readonly string[];
	/** Default recipient origin for monitor/maintenance notes without their own channel target. */
	readonly ownerTarget?: { readonly origin: OriginRef };
	readonly webhook?: { readonly bind?: string; readonly port: number; readonly exposeNonLoopback?: boolean };
	readonly watcherRoots?: readonly string[];
	readonly scriptRoot?: string;
}

export interface GatewayConfig extends GatewayConfigFile {
	readonly home: string;
	readonly configPath: string;
	readonly socketPath: string;
	readonly dbPath: string;
}
export interface ChannelPolicy {
	readonly engagement?: "open";
	/** Per-channel inbound debounce override in milliseconds. */
	readonly debounceMs?: number;
}

export interface ConfigOverrides {
	readonly logVerbosity?: GatewayConfigFile["logVerbosity"];
	readonly socketPath?: string;
	readonly dbPath?: string;
}

export class ConfigError extends Error {
	readonly code: "config_invalid" | "secret_source_conflict";
	constructor(code: ConfigError["code"], message: string) {
		super(message);
		this.name = "ConfigError";
		this.code = code;
	}
}

export interface ReloadDiagnostic {
	readonly code: ConfigError["code"];
	readonly message: string;
}

export type ReloadResult =
	| { readonly ok: true; readonly config: GatewayConfig; readonly changed: readonly string[] }
	| { readonly ok: false; readonly config: GatewayConfig; readonly diagnostics: readonly ReloadDiagnostic[] };

export function gatewayHome(env: NodeJS.ProcessEnv = process.env): string {
	return env.GAJAEWAY_HOME || join(homedir(), ".gajaeway");
}

function requireObject(value: unknown, field: string): Record<string, unknown> {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new ConfigError("config_invalid", `${field} must be an object`);
	}
	return value as Record<string, unknown>;
}

function optionalString(value: unknown, field: string): string | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || value.length === 0) {
		throw new ConfigError("config_invalid", `${field} must be a non-empty string`);
	}
	return value;
}

function parseModel(value: unknown): GjcModelSelection | undefined {
	if (value === undefined) return undefined;
	if (typeof value === "string") return optionalString(value, "model");
	const input = requireObject(value, "model");
	const preset = optionalString(input.preset, "model.preset");
	if (!preset || Object.keys(input).length !== 1) {
		throw new ConfigError("config_invalid", "model must be a non-empty string or contain only preset");
	}
	return { preset };
}

function parseCredentials(value: unknown): Readonly<Record<string, CredentialFileReference>> | undefined {
	if (value === undefined) return undefined;
	const input = requireObject(value, "credentials");
	const credentials: Record<string, CredentialFileReference> = {};
	const paths = new Set<string>();
	for (const [name, reference] of Object.entries(input)) {
		const item = requireObject(reference, `credentials.${name}`);
		const credentialFile = optionalString(item.credentialFile, `credentials.${name}.credentialFile`);
		if (!credentialFile || Object.keys(item).length !== 1) {
			throw new ConfigError("config_invalid", `credentials.${name} must contain only credentialFile`);
		}
		if (paths.has(credentialFile)) {
			throw new ConfigError(
				"secret_source_conflict",
				"a credential file may only be reachable through one configured credential",
			);
		}
		paths.add(credentialFile);
		credentials[name] = { credentialFile };
	}
	return credentials;
}

function parseChannels(value: unknown): Readonly<Record<string, ChannelPolicy>> | undefined {
	if (value === undefined) return undefined;
	const input = requireObject(value, "channels");
	const channels: Record<string, ChannelPolicy> = {};
	for (const [conversationId, channel] of Object.entries(input)) {
		const item = requireObject(channel, `channels.${conversationId}`);
		if (item.engagement !== undefined && item.engagement !== "open")
			throw new ConfigError("config_invalid", `channels.${conversationId}.engagement must be open`);
		if (item.debounceMs !== undefined) parseDebounce(item.debounceMs, `channels.${conversationId}.debounceMs`);
		if (Object.keys(item).some((key) => !["engagement", "debounceMs"].includes(key)))
			throw new ConfigError("config_invalid", `channels.${conversationId} contains an unknown field`);
		channels[conversationId] = {
			...(item.engagement === "open" ? { engagement: "open" as const } : {}),
			...(item.debounceMs === undefined ? {} : { debounceMs: item.debounceMs as number }),
		};
	}
	return channels;
}

function parseDebounce(value: unknown, field: string): number {
	// 0 disables; above 60s is an operator mistake, not a debounce.
	if (!Number.isInteger(value) || (value as number) < 0 || (value as number) > 60_000)
		throw new ConfigError("config_invalid", `${field} must be an integer between 0 and 60000`);
	return value as number;
}

function parseOwnerTarget(value: unknown): { readonly origin: OriginRef } {
	const input = requireObject(value, "ownerTarget");
	if (Object.keys(input).some((key) => key !== "origin"))
		throw new ConfigError("config_invalid", "ownerTarget may only contain origin");
	try {
		return { origin: validateOriginRef(input.origin as OriginRef) };
	} catch (error) {
		throw new ConfigError(
			"config_invalid",
			`ownerTarget.origin is invalid: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
}

function parseStringArray(value: unknown, field: string): readonly string[] {
	if (!Array.isArray(value) || value.some((item) => typeof item !== "string" || !item))
		throw new ConfigError("config_invalid", `${field} must be a non-empty string array`);
	return value;
}
function parseWebhook(value: unknown): {
	readonly bind?: string;
	readonly port: number;
	readonly exposeNonLoopback?: boolean;
} {
	const input = requireObject(value, "webhook");
	const bind = optionalString(input.bind, "webhook.bind");
	if (!Number.isInteger(input.port) || (input.port as number) < 1 || (input.port as number) > 65535)
		throw new ConfigError("config_invalid", "webhook.port must be a valid port");
	if (input.exposeNonLoopback !== undefined && typeof input.exposeNonLoopback !== "boolean")
		throw new ConfigError("config_invalid", "webhook.exposeNonLoopback must be boolean");
	if (Object.keys(input).some((key) => !["bind", "port", "exposeNonLoopback"].includes(key)))
		throw new ConfigError("config_invalid", "webhook contains an unknown field");
	return {
		...(bind ? { bind } : {}),
		port: input.port as number,
		...(input.exposeNonLoopback ? { exposeNonLoopback: true } : {}),
	};
}

function parseTurnTimeout(value: unknown): number {
	// Bounded 30s..3600s: below breaks trivial turns, above is an operator mistake.
	if (!Number.isInteger(value) || (value as number) < 30_000 || (value as number) > 3_600_000)
		throw new ConfigError("config_invalid", "turnTimeoutMs must be an integer between 30000 and 3600000");
	return value as number;
}

export function parseConfigFile(value: unknown): GatewayConfigFile {
	const input = requireObject(value, "config");
	if (input.schemaVersion !== CONFIG_SCHEMA_VERSION) {
		throw new ConfigError("config_invalid", `schemaVersion must be ${CONFIG_SCHEMA_VERSION}`);
	}
	const logVerbosity = optionalString(input.logVerbosity, "logVerbosity");
	if (logVerbosity !== undefined && !["debug", "info", "warn", "error"].includes(logVerbosity)) {
		throw new ConfigError("config_invalid", "logVerbosity must be debug, info, warn, or error");
	}
	const model = parseModel(input.model);
	return {
		schemaVersion: CONFIG_SCHEMA_VERSION,
		...(logVerbosity ? { logVerbosity: logVerbosity as GatewayConfigFile["logVerbosity"] } : {}),
		...(optionalString(input.socketPath, "socketPath")
			? { socketPath: optionalString(input.socketPath, "socketPath") }
			: {}),
		...(optionalString(input.dbPath, "dbPath") ? { dbPath: optionalString(input.dbPath, "dbPath") } : {}),
		...(parseCredentials(input.credentials) ? { credentials: parseCredentials(input.credentials) } : {}),
		...(parseChannels(input.channels) ? { channels: parseChannels(input.channels) } : {}),
		...(input.webhook === undefined ? {} : { webhook: parseWebhook(input.webhook) }),
		...(input.watcherRoots === undefined ? {} : { watcherRoots: parseStringArray(input.watcherRoots, "watcherRoots") }),
		...(input.scriptRoot === undefined ? {} : { scriptRoot: optionalString(input.scriptRoot, "scriptRoot") }),
		...(input.turnTimeoutMs === undefined ? {} : { turnTimeoutMs: parseTurnTimeout(input.turnTimeoutMs) }),
		...(input.mentionAllowlist === undefined
			? {}
			: { mentionAllowlist: parseStringArray(input.mentionAllowlist, "mentionAllowlist") }),
		...(input.debounceMs === undefined ? {} : { debounceMs: parseDebounce(input.debounceMs, "debounceMs") }),
		...(model ? { model } : {}),
		...(input.ownerTarget === undefined ? {} : { ownerTarget: parseOwnerTarget(input.ownerTarget) }),
	};
}

export async function loadConfig(
	options: { readonly home?: string; readonly env?: NodeJS.ProcessEnv; readonly overrides?: ConfigOverrides } = {},
): Promise<GatewayConfig> {
	const home = options.home ?? gatewayHome(options.env);
	const configPath = join(home, "config.json");
	let fileConfig: GatewayConfigFile = { schemaVersion: CONFIG_SCHEMA_VERSION };
	if (await Bun.file(configPath).exists()) {
		try {
			fileConfig = parseConfigFile(JSON.parse(await Bun.file(configPath).text()));
		} catch (error) {
			if (error instanceof ConfigError) throw error;
			throw new ConfigError("config_invalid", `could not parse ${configPath}`);
		}
	}
	const overrides = options.overrides ?? {};
	return {
		...fileConfig,
		home,
		configPath,
		socketPath: overrides.socketPath ?? fileConfig.socketPath ?? join(home, "gateway.sock"),
		dbPath: overrides.dbPath ?? fileConfig.dbPath ?? join(home, "gateway.db"),
		logVerbosity: overrides.logVerbosity ?? fileConfig.logVerbosity ?? "info",
		debounceMs: fileConfig.debounceMs ?? 1_000,
	};
}

/** Validates a complete replacement before publishing it; failed reloads retain current. */
export async function reloadConfig(current: GatewayConfig, overrides: ConfigOverrides = {}): Promise<ReloadResult> {
	try {
		const candidate = await loadConfig({ home: current.home, overrides });
		const changed = (["logVerbosity", "socketPath", "dbPath"] as const).filter(
			(field) => candidate[field] !== current[field],
		);
		return { ok: true, config: candidate, changed };
	} catch (error) {
		const diagnostic =
			error instanceof ConfigError
				? { code: error.code, message: error.message }
				: { code: "config_invalid" as const, message: "configuration reload failed" };
		return { ok: false, config: current, diagnostics: [diagnostic] };
	}
}

export const RELOADABLE_FIELDS = ["logVerbosity"] as const;
export const RESTART_REQUIRED_FIELDS = ["socketPath", "dbPath"] as const;

export function configDirectory(config: GatewayConfig): string {
	return dirname(config.configPath);
}
