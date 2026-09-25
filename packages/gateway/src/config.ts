import { lstat } from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import {
	type ChannelEngagementPolicy,
	ENGAGEMENT_AUDIENCES,
	ENGAGEMENT_MODES,
	type EngagementAudience,
	type EngagementMode,
	type OriginRef,
	parseRuntimeConfig as parseSharedRuntimeConfig,
	type RuntimeConfig,
	RuntimeConfigError,
	validateOriginRef,
} from "@gajae-gateway/protocol";

export type { RuntimeConfig } from "@gajae-gateway/protocol";
export const CONFIG_SCHEMA_VERSION = 1;

export interface CredentialFileReference {
	readonly credentialFile: string;
}

export type GjcModelSelection = string | { readonly preset: string };

/** Upper bound on concurrently bound `work.run` lanes; more are refused with `lane_capacity`. */
export const DEFAULT_WORK_MAX_LANES = 8;
/** A bound worker lane quiet for this long is closed by the sweep and rebound on its next run. */
export const DEFAULT_WORK_IDLE_RETIRE_MS = 6 * 60 * 60_000;
const WORK_IDLE_RETIRE_MIN_MS = 60_000;
const WORK_IDLE_RETIRE_MAX_MS = 7 * 24 * 60 * 60_000;

export interface WorkLaneConfig {
	readonly maxLanes?: number;
	readonly idleRetireMs?: number;
}
export type GjcServiceTier =
	| "none"
	| "auto"
	| "default"
	| "flex"
	| "scale"
	| "priority"
	| "openai-only"
	| "claude-only";

const GJC_SERVICE_TIERS: readonly GjcServiceTier[] = [
	"none",
	"auto",
	"default",
	"flex",
	"scale",
	"priority",
	"openai-only",
	"claude-only",
];

export interface GatewayConfigFile {
	readonly schemaVersion: typeof CONFIG_SCHEMA_VERSION;
	readonly logVerbosity?: "debug" | "info" | "warn" | "error";
	readonly socketPath?: string;
	readonly dbPath?: string;
	/** Explicit gjc model selector, or a model profile preset whose default role may contain a fallback chain. */
	readonly model?: GjcModelSelection;
	/** Processing tier applied to every gateway persona session; `priority` is GJC fast mode. */
	readonly serviceTier?: GjcServiceTier;
	readonly credentials?: Readonly<Record<string, CredentialFileReference>>;
	readonly channels?: Readonly<Record<string, ChannelPolicy>>;
	/** Tail liveness alarm threshold in milliseconds. It never kills a running turn. */
	readonly stallTimeoutMs?: number;
	/** Author ids allowed to trigger mention-gated group turns; absent/empty = anyone. */
	readonly mentionAllowlist?: readonly string[];
	/**
	 * Who may open a direct-message turn. DMs used to bypass authorisation
	 * entirely, which made the private surface the unauthenticated one.
	 * Unset means `allowlist`.
	 */
	readonly dmPolicy?: DmPolicy;
	/** Default recipient origin for monitor/maintenance notes without their own channel target. */
	readonly ownerTarget?: { readonly origin: OriginRef };
	/**
	 * Consecutive context-class authoring failures (empty response,
	 * context-length rejection, zero-token completion) before the monitor
	 * safety net rolls that session — and only when a native-compaction request
	 * did not succeed (issue #68). This is NOT a turn ceiling: a monitor that
	 * keeps answering is never rolled. Default
	 * MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD.
	 */
	readonly monitorContextFailureRollThreshold?: number;
	/**
	 * Cron catch-up ceiling (issue #157). After downtime every slot since a
	 * monitor's durable cursor is replayed, except slots older than `maxAgeMs`
	 * and the oldest overflow beyond `maxSlots`; those are recorded as skipped.
	 * Default DEFAULT_CRON_CATCH_UP (24 slots, 24 hours).
	 */
	readonly monitorCatchUp?: MonitorCatchUpConfig;
	readonly webhook?: { readonly bind?: string; readonly port: number; readonly exposeNonLoopback?: boolean };
	readonly watcherRoots?: readonly string[];
	readonly scriptRoot?: string;
	readonly runtime?: RuntimeConfig;
	/** Worker-lane governance: admission cap and idle retirement for `work.run` sessions. */
	readonly work?: WorkLaneConfig;
	/** Global default bot-audience budget; channel entries override it field by field. */
	readonly botAudience?: BotAudienceConfig;
}

export interface MonitorCatchUpConfig {
	readonly maxSlots?: number;
	readonly maxAgeMs?: number;
}

/** Bounds for `monitorCatchUp`: at least one slot, at most a week of lookback. */
export const MONITOR_CATCH_UP_MAX_SLOTS = 1000;
export const MONITOR_CATCH_UP_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

export interface BotAudienceConfig {
	/**
	 * Consecutive bot-authored turns per conversation before a human message is
	 * required. Unset is unlimited: multi-agent collaboration in one thread is a
	 * normal pattern, and runaway loops are bounded by the rate limit instead.
	 */
	readonly maxConsecutiveTurns?: number;
	/**
	 * Bot admissions per conversation inside a rolling minute. Always active;
	 * unset uses DEFAULT_BOT_AUDIENCE_TURNS_PER_WINDOW.
	 */
	readonly maxTurnsPerWindow?: number;
}

export interface GatewayConfig extends GatewayConfigFile {
	readonly home: string;
	readonly configPath: string;
	readonly socketPath: string;
	readonly dbPath: string;
}
/**
 * The three gates. Unset means `closed`: the safe default under the
 * prompt-injection posture this runtime states elsewhere.
 */
export const ENGAGEMENT_GATES = ENGAGEMENT_MODES;
export type EngagementGate = EngagementMode;
export { ENGAGEMENT_AUDIENCES };

/**
 * Direct-message gates. Unset means `allowlist`: the owner plus explicitly
 * named authors. `open` exists so that widening this surface is a deliberate,
 * auditable edit rather than a default nobody chose.
 */
export const DM_POLICIES = ["owner-only", "allowlist", "open"] as const;
export type DmPolicy = (typeof DM_POLICIES)[number];

export interface ChannelPolicy extends ChannelEngagementPolicy {
	/**
	 * Which gate this channel is on. Explicit, because the previous two-state
	 * shape ("open" or unset) silently changed meaning depending on whether
	 * `mentionAllowlist` happened to be populated.
	 *
	 * - `open`: matching-audience messages are turns without addressing
	 * - `mention-open`: matching-audience messages require a mention or native reply
	 * - `closed`: every author requires addressing and allowlist authorization
	 */
	readonly engagement?: EngagementGate;
	/** Authors who receive the open/mention-open behavior. Unset is `human-only`. */
	readonly audience?: EngagementAudience;
	/** Per-channel override of `botAudience.maxConsecutiveTurns`. Unset inherits; the global default is unlimited. */
	readonly botAudienceMaxConsecutiveTurns?: number;
	/** Per-channel override of `botAudience.maxTurnsPerWindow` (rolling minute). */
	readonly botAudienceMaxTurnsPerWindow?: number;
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
	| {
			readonly ok: true;
			readonly config: GatewayConfig;
			/** Reloadable fields whose value actually changed and was applied. */
			readonly changed: readonly string[];
			/**
			 * Fields the operator edited that CANNOT be applied live. They are
			 * reported explicitly and left at their old values: a reload that
			 * pretends to apply a restart-only field is worse than refusing.
			 */
			readonly restartRequired: readonly string[];
			/**
			 * Fields the operator edited that no code reads at all, so the edit has
			 * no effect and no restart would give it one. Reported rather than
			 * counted as applied, because claiming to apply a field nothing consumes
			 * is the same operator lie in a quieter form.
			 */
			readonly ignored: readonly string[];
	  }
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

function parseServiceTier(value: unknown): GjcServiceTier | undefined {
	if (value === undefined) return undefined;
	if (typeof value !== "string" || !GJC_SERVICE_TIERS.includes(value as GjcServiceTier))
		throw new ConfigError("config_invalid", `serviceTier must be one of ${GJC_SERVICE_TIERS.join(", ")}`);
	return value as GjcServiceTier;
}

export function parseRuntimeConfig(value: unknown): RuntimeConfig | undefined {
	try {
		return parseSharedRuntimeConfig(value);
	} catch (error) {
		if (error instanceof RuntimeConfigError) throw new ConfigError("config_invalid", error.message);
		throw error;
	}
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
		if (item.engagement !== undefined && !ENGAGEMENT_GATES.includes(item.engagement as EngagementGate))
			throw new ConfigError(
				"config_invalid",
				`channels.${conversationId}.engagement must be one of ${ENGAGEMENT_GATES.join(", ")}`,
			);
		if (item.audience !== undefined && !ENGAGEMENT_AUDIENCES.includes(item.audience as EngagementAudience))
			throw new ConfigError(
				"config_invalid",
				`channels.${conversationId}.audience must be one of ${ENGAGEMENT_AUDIENCES.join(", ")}`,
			);
		for (const removed of ["debounceMs", "settleWindowMs"] as const)
			if (item[removed] !== undefined)
				throw new ConfigError(
					"config_invalid",
					`channels.${conversationId}.${removed} was removed: every message is steered or sent immediately; delete it from the configuration`,
				);
		const known = ["engagement", "audience", "botAudienceMaxConsecutiveTurns", "botAudienceMaxTurnsPerWindow"] as const;
		if (Object.keys(item).some((key) => !known.includes(key as (typeof known)[number])))
			throw new ConfigError("config_invalid", `channels.${conversationId} contains an unknown field`);
		const maxConsecutiveTurns = parsePositiveTurnCount(
			item.botAudienceMaxConsecutiveTurns,
			`channels.${conversationId}.botAudienceMaxConsecutiveTurns`,
		);
		const maxTurnsPerWindow = parsePositiveTurnCount(
			item.botAudienceMaxTurnsPerWindow,
			`channels.${conversationId}.botAudienceMaxTurnsPerWindow`,
		);
		channels[conversationId] = {
			...(item.engagement === undefined ? {} : { engagement: item.engagement as EngagementGate }),
			...(item.audience === undefined ? {} : { audience: item.audience as EngagementAudience }),
			...(maxConsecutiveTurns === undefined ? {} : { botAudienceMaxConsecutiveTurns: maxConsecutiveTurns }),
			...(maxTurnsPerWindow === undefined ? {} : { botAudienceMaxTurnsPerWindow: maxTurnsPerWindow }),
		};
	}
	return channels;
}

/** Turn budgets are whole positive counts; 0 would mean "never admit", which `audience` already expresses. */
function parsePositiveTurnCount(value: unknown, field: string): number | undefined {
	if (value === undefined) return undefined;
	if (!Number.isSafeInteger(value) || (value as number) < 1)
		throw new ConfigError("config_invalid", `${field} must be an integer of at least 1`);
	return value as number;
}

function parseBotAudience(value: unknown): BotAudienceConfig | undefined {
	if (value === undefined) return undefined;
	const input = requireObject(value, "botAudience");
	for (const key of Object.keys(input))
		if (key !== "maxConsecutiveTurns" && key !== "maxTurnsPerWindow")
			throw new ConfigError("config_invalid", "botAudience contains an unknown field");
	const maxConsecutiveTurns = parsePositiveTurnCount(input.maxConsecutiveTurns, "botAudience.maxConsecutiveTurns");
	const maxTurnsPerWindow = parsePositiveTurnCount(input.maxTurnsPerWindow, "botAudience.maxTurnsPerWindow");
	return {
		...(maxConsecutiveTurns === undefined ? {} : { maxConsecutiveTurns }),
		...(maxTurnsPerWindow === undefined ? {} : { maxTurnsPerWindow }),
	};
}

function parseStallTimeout(value: unknown): number {
	if (!Number.isInteger(value) || (value as number) < 1_000 || (value as number) > 3_600_000)
		throw new ConfigError("config_invalid", "stallTimeoutMs must be an integer between 1000 and 3600000");
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

/**
 * Bounded 1..20. 1 rolls on the first context-class failure, which is
 * aggressive but a legitimate operator choice; above 20 the safety net would
 * never fire before the monitor had spent hours failing every turn.
 */
function parseMonitorContextFailureRollThreshold(value: unknown): number {
	if (!Number.isInteger(value) || (value as number) < 1 || (value as number) > 20)
		throw new ConfigError("config_invalid", "monitorContextFailureRollThreshold must be an integer between 1 and 20");
	return value as number;
}

function parseMonitorCatchUp(value: unknown): MonitorCatchUpConfig {
	const input = requireObject(value, "monitorCatchUp");
	if (Object.keys(input).some((key) => key !== "maxSlots" && key !== "maxAgeMs"))
		throw new ConfigError("config_invalid", "monitorCatchUp may only contain maxSlots and maxAgeMs");
	if (
		input.maxSlots !== undefined &&
		(!Number.isInteger(input.maxSlots) ||
			(input.maxSlots as number) < 1 ||
			(input.maxSlots as number) > MONITOR_CATCH_UP_MAX_SLOTS)
	)
		throw new ConfigError(
			"config_invalid",
			`monitorCatchUp.maxSlots must be an integer between 1 and ${MONITOR_CATCH_UP_MAX_SLOTS}`,
		);
	if (
		input.maxAgeMs !== undefined &&
		(!Number.isInteger(input.maxAgeMs) ||
			(input.maxAgeMs as number) < 60_000 ||
			(input.maxAgeMs as number) > MONITOR_CATCH_UP_MAX_AGE_MS)
	)
		throw new ConfigError(
			"config_invalid",
			`monitorCatchUp.maxAgeMs must be an integer between 60000 and ${MONITOR_CATCH_UP_MAX_AGE_MS}`,
		);
	return {
		...(input.maxSlots === undefined ? {} : { maxSlots: input.maxSlots as number }),
		...(input.maxAgeMs === undefined ? {} : { maxAgeMs: input.maxAgeMs as number }),
	};
}

function parseWork(value: unknown): WorkLaneConfig {
	const input = requireObject(value, "work");
	if (Object.keys(input).some((key) => key !== "maxLanes" && key !== "idleRetireMs"))
		throw new ConfigError("config_invalid", "work may only contain maxLanes and idleRetireMs");
	if (
		input.maxLanes !== undefined &&
		(!Number.isInteger(input.maxLanes) || (input.maxLanes as number) < 1 || (input.maxLanes as number) > 256)
	)
		throw new ConfigError("config_invalid", "work.maxLanes must be an integer between 1 and 256");
	if (
		input.idleRetireMs !== undefined &&
		(!Number.isInteger(input.idleRetireMs) ||
			(input.idleRetireMs as number) < WORK_IDLE_RETIRE_MIN_MS ||
			(input.idleRetireMs as number) > WORK_IDLE_RETIRE_MAX_MS)
	)
		throw new ConfigError(
			"config_invalid",
			`work.idleRetireMs must be an integer between ${WORK_IDLE_RETIRE_MIN_MS} and ${WORK_IDLE_RETIRE_MAX_MS}`,
		);
	return {
		...(input.maxLanes === undefined ? {} : { maxLanes: input.maxLanes as number }),
		...(input.idleRetireMs === undefined ? {} : { idleRetireMs: input.idleRetireMs as number }),
	};
}

function parseDmPolicy(value: unknown): DmPolicy {
	if (typeof value !== "string" || !DM_POLICIES.includes(value as DmPolicy))
		throw new ConfigError("config_invalid", `dmPolicy must be one of ${DM_POLICIES.join(", ")}`);
	return value as DmPolicy;
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
	for (const removed of ["debounceMs", "settleWindowMs", "maxInboundAgeMs"] as const)
		if (input[removed] !== undefined)
			throw new ConfigError(
				"config_invalid",
				`${removed} was removed: every message is steered into the running turn or sent as the next one, and nothing expires while queued; delete it from the configuration`,
			);
	if (input.turnTimeoutMs !== undefined) {
		throw new ConfigError(
			"config_invalid",
			"turnTimeoutMs was removed with persistent SDK sessions; use stallTimeoutMs for tail-stall alerts (it never kills a running turn)",
		);
	}
	const model = parseModel(input.model);
	const serviceTier = parseServiceTier(input.serviceTier);
	const runtime = parseRuntimeConfig(input.runtime);
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
		...(runtime === undefined ? {} : { runtime }),
		...(input.mentionAllowlist === undefined
			? {}
			: { mentionAllowlist: parseStringArray(input.mentionAllowlist, "mentionAllowlist") }),
		...(input.stallTimeoutMs === undefined ? {} : { stallTimeoutMs: parseStallTimeout(input.stallTimeoutMs) }),
		...(model ? { model } : {}),
		...(serviceTier ? { serviceTier } : {}),
		...(input.dmPolicy === undefined ? {} : { dmPolicy: parseDmPolicy(input.dmPolicy) }),
		...(input.ownerTarget === undefined ? {} : { ownerTarget: parseOwnerTarget(input.ownerTarget) }),
		...(input.work === undefined ? {} : { work: parseWork(input.work) }),
		...(input.monitorCatchUp === undefined ? {} : { monitorCatchUp: parseMonitorCatchUp(input.monitorCatchUp) }),
		...(input.botAudience === undefined ? {} : { botAudience: parseBotAudience(input.botAudience) }),
		...(input.monitorContextFailureRollThreshold === undefined
			? {}
			: {
					monitorContextFailureRollThreshold: parseMonitorContextFailureRollThreshold(
						input.monitorContextFailureRollThreshold,
					),
				}),
	};
}

export async function loadConfig(
	options: {
		readonly home?: string;
		readonly env?: NodeJS.ProcessEnv;
		readonly overrides?: ConfigOverrides;
		/**
		 * Require config.json to exist and be readable. Absent means defaults, which
		 * is right at BOOT and wrong on RELOAD: publishing defaults over live policy
		 * drops mentionAllowlist and channels and opens a mention-gated room. The
		 * check lives here, next to the read, so there is no window between a
		 * caller's stat and this one — and so a future reload-ish caller cannot
		 * reintroduce the hole by forgetting to guard.
		 */
		readonly requireFile?: boolean;
	} = {},
): Promise<GatewayConfig> {
	const home = options.home ?? gatewayHome(options.env);
	const configPath = join(home, "config.json");
	let fileConfig: GatewayConfigFile = { schemaVersion: CONFIG_SCHEMA_VERSION };
	let raw: string | undefined;
	try {
		raw = await Bun.file(configPath).text();
	} catch (error) {
		const code = (error as NodeJS.ErrnoException).code;
		// ENOENT is genuinely "no configuration", and defaults are correct for a
		// first boot. Anything else (EISDIR, EACCES, EIO) is a config file that
		// EXISTS and cannot be read, which must never silently become a
		// defaults-only open policy — not at reload, and not at boot either.
		//
		// A DANGLING SYMLINK also reports ENOENT while the directory entry plainly
		// exists, so lstat decides: an entry that is there but unresolvable is
		// unreadable, not absent. Without this, config.json symlinked into a
		// dotfiles repo whose target moved booted the daemon on defaults.
		const entryExists = await lstat(configPath).then(
			() => true,
			() => false,
		);
		if (options.requireFile || code !== "ENOENT" || entryExists)
			throw new ConfigError(
				"config_invalid",
				`${configPath} is ${code === "ENOENT" && !entryExists ? "missing" : `unreadable (${code ?? "unknown"})`}${options.requireFile ? "; keeping the previous configuration" : "; refusing to start on defaults"}`,
			);
	}
	if (raw !== undefined) {
		try {
			fileConfig = parseConfigFile(JSON.parse(raw));
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
		stallTimeoutMs: fileConfig.stallTimeoutMs ?? 120_000,
	};
}

/**
 * Re-reads config.json and publishes ONLY the reloadable fields, so the daemon
 * can never end up half-applied: a parse or validation error retains the current
 * config untouched, and an edited restart-only field is reported rather than
 * silently ignored or half-honoured.
 */
export async function reloadConfig(current: GatewayConfig, overrides: ConfigOverrides = {}): Promise<ReloadResult> {
	let candidate: GatewayConfig;
	try {
		candidate = await loadConfig({ home: current.home, overrides, requireFile: true });
	} catch (error) {
		const diagnostic =
			error instanceof ConfigError
				? { code: error.code, message: error.message }
				: { code: "config_invalid" as const, message: "configuration reload failed" };
		return { ok: false, config: current, diagnostics: [diagnostic] };
	}
	const changed = RELOADABLE_FIELDS.filter((field) => !Bun.deepEquals(candidate[field], current[field]));
	const restartRequired = RESTART_REQUIRED_FIELDS.filter((field) => !Bun.deepEquals(candidate[field], current[field]));
	const ignored = UNCONSUMED_FIELDS.filter((field) => !Bun.deepEquals(candidate[field], current[field]));
	const next: Record<string, unknown> = { ...current };
	for (const field of RELOADABLE_FIELDS) {
		if (candidate[field] === undefined) delete next[field];
		else next[field] = candidate[field];
	}
	return { ok: true, config: next as unknown as GatewayConfig, changed, restartRequired, ignored };
}

/**
 * Fields genuinely re-read at runtime: `mentionAllowlist` (server.ts chat dispatch +
 * engagement/policy.ts), channels (engagement gates), `botAudience` (bot budgets,
 * resolved per inbound message) and `stallTimeoutMs` (tail liveness alarms). Each
 * applies to the next actor event.
 */
export const RELOADABLE_FIELDS = ["mentionAllowlist", "channels", "stallTimeoutMs", "dmPolicy", "botAudience"] as const;

/** Fields bound to live startup resources and therefore changeable only by restart. */
export const RESTART_REQUIRED_FIELDS = [
	"socketPath",
	"dbPath",
	"model",
	"serviceTier",
	"credentials",
	"webhook",
	"watcherRoots",
	"scriptRoot",
	"runtime",
	"ownerTarget",
	"monitorContextFailureRollThreshold",
	"monitorCatchUp",
	"work",
] as const;

/**
 * Parsed and validated, but read by nothing: `logVerbosity` has no consumer in
 * any package (every log site is an unconditional console call). Editing it can
 * therefore neither be applied nor fixed by a restart, so a reload reports it as
 * ignored instead of claiming `applied=[logVerbosity]` for a no-op.
 */
export const UNCONSUMED_FIELDS = ["logVerbosity"] as const;

/**
 * Compile-time proof that every config field is classified. Add a field to
 * GatewayConfigFile without deciding whether it is live, restart-only, or
 * unconsumed and this stops building — silently ignoring an edited field is the
 * behaviour the reload path exists to eliminate.
 */
type ClassifiedField =
	| (typeof RELOADABLE_FIELDS)[number]
	| (typeof RESTART_REQUIRED_FIELDS)[number]
	| (typeof UNCONSUMED_FIELDS)[number]
	| "schemaVersion";
export type UnclassifiedConfigField = Exclude<keyof GatewayConfigFile, ClassifiedField>;
export const CONFIG_PARTITION_IS_EXHAUSTIVE: UnclassifiedConfigField extends never ? true : false = true;

export function configDirectory(config: GatewayConfig): string {
	return dirname(config.configPath);
}
