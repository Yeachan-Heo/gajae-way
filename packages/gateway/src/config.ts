import { homedir } from "node:os";
import { dirname, join } from "node:path";

export const CONFIG_SCHEMA_VERSION = 1;

export interface CredentialFileReference {
	readonly credentialFile: string;
}

export interface GatewayConfigFile {
	readonly schemaVersion: typeof CONFIG_SCHEMA_VERSION;
	readonly logVerbosity?: "debug" | "info" | "warn" | "error";
	readonly socketPath?: string;
	readonly dbPath?: string;
	readonly credentials?: Readonly<Record<string, CredentialFileReference>>;
	readonly channels?: Readonly<Record<string, { readonly engagement?: "open" }>>;
}

export interface GatewayConfig extends GatewayConfigFile {
	readonly home: string;
	readonly configPath: string;
	readonly socketPath: string;
	readonly dbPath: string;
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

function parseChannels(value: unknown): Readonly<Record<string, { readonly engagement?: "open" }>> | undefined {
	if (value === undefined) return undefined;
	const input = requireObject(value, "channels");
	const channels: Record<string, { readonly engagement?: "open" }> = {};
	for (const [conversationId, channel] of Object.entries(input)) {
		const item = requireObject(channel, `channels.${conversationId}`);
		if (item.engagement !== undefined && item.engagement !== "open")
			throw new ConfigError("config_invalid", `channels.${conversationId}.engagement must be open`);
		if (Object.keys(item).some((key) => key !== "engagement"))
			throw new ConfigError("config_invalid", `channels.${conversationId} contains an unknown field`);
		channels[conversationId] = item.engagement === "open" ? { engagement: "open" } : {};
	}
	return channels;
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
	return {
		schemaVersion: CONFIG_SCHEMA_VERSION,
		...(logVerbosity ? { logVerbosity: logVerbosity as GatewayConfigFile["logVerbosity"] } : {}),
		...(optionalString(input.socketPath, "socketPath")
			? { socketPath: optionalString(input.socketPath, "socketPath") }
			: {}),
		...(optionalString(input.dbPath, "dbPath") ? { dbPath: optionalString(input.dbPath, "dbPath") } : {}),
		...(parseCredentials(input.credentials) ? { credentials: parseCredentials(input.credentials) } : {}),
		...(parseChannels(input.channels) ? { channels: parseChannels(input.channels) } : {}),
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
