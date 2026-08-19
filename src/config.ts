import * as os from "node:os";
import * as path from "node:path";

export const DEFAULT_FAIL_CLOSED_LINGER_MS = 300_000;

export interface WayConfig {
	readonly stateDir: string;
	readonly profilePath: string;
	readonly failClosedLingerMs: number;
	readonly brokerCliPath: string;
	readonly reconcilePollMs: number;
}

export class ConfigValidationError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ConfigValidationError";
	}
}

/** Process-level paths and liveness tuning; profile identity lives in profile.ts. */
export function defaultConfig(environment: NodeJS.ProcessEnv = process.env): WayConfig {
	return {
		stateDir: path.resolve(environment.GAJAEWAY_STATE_DIR || path.join(os.homedir(), ".local", "state", "gajaeway")),
		profilePath: path.resolve(environment.GAJAEWAY_PROFILE || "ops/profiles/gaebal-gajae.example.toml"),
		failClosedLingerMs:
			parseLingerMs(environment.GAJAEWAY_FAIL_CLOSED_LINGER_MS, "GAJAEWAY_FAIL_CLOSED_LINGER_MS") ?? DEFAULT_FAIL_CLOSED_LINGER_MS,
		brokerCliPath: environment.GAJAEWAY_BROKER_CLI?.trim() || "gjc",
		reconcilePollMs: parsePositiveMs(environment.GAJAEWAY_RECONCILE_POLL_MS, "GAJAEWAY_RECONCILE_POLL_MS") ?? 15_000,
	};
}

function parseLingerMs(value: string | undefined, name: string): number | undefined {
	if (value === undefined) return undefined;
	if (!/^\d+$/.test(value)) throw new ConfigValidationError(`${name} must be a non-negative integer number of milliseconds.`);
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed) || parsed > 86_400_000) {
		throw new ConfigValidationError(`${name} must be between 0 and 86400000 milliseconds.`);
	}
	return parsed;
}

function parsePositiveMs(value: string | undefined, name: string): number | undefined {
	if (value === undefined) return undefined;
	if (!/^\d+$/.test(value)) throw new ConfigValidationError(`${name} must be a positive integer number of milliseconds.`);
	const parsed = Number(value);
	if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > 600_000) {
		throw new ConfigValidationError(`${name} must be between 1 and 600000 milliseconds.`);
	}
	return parsed;
}

export interface ParsedWayConfig {
	readonly config: WayConfig;
	readonly remaining: readonly string[];
}

/** Parses only process-wide options so command-specific parsing stays explicit. */
export function parseWayConfig(arguments_: readonly string[], initial = defaultConfig()): ParsedWayConfig {
	let stateDir = initial.stateDir;
	let profilePath = initial.profilePath;
	let failClosedLingerMs = initial.failClosedLingerMs;
	let brokerCliPath = initial.brokerCliPath;
	let reconcilePollMs = initial.reconcilePollMs;
	const remaining: string[] = [];
	for (let index = 0; index < arguments_.length; index += 1) {
		const argument = arguments_[index] as string;
		if (
			argument === "--state-dir" ||
			argument === "--profile" ||
			argument === "--fail-closed-linger-ms" ||
			argument === "--broker-cli" ||
			argument === "--reconcile-poll-ms"
		) {
			const value = arguments_[index + 1];
			if (!value) throw new ConfigValidationError(`${argument} requires a value.`);
			if (argument === "--state-dir") stateDir = path.resolve(value);
			if (argument === "--profile") profilePath = path.resolve(value);
			if (argument === "--fail-closed-linger-ms") {
				failClosedLingerMs = parseLingerMs(value, "--fail-closed-linger-ms") as number;
			}
			if (argument === "--broker-cli") brokerCliPath = value;
			if (argument === "--reconcile-poll-ms") reconcilePollMs = parsePositiveMs(value, "--reconcile-poll-ms") as number;
			index += 1;
			continue;
		}
		remaining.push(argument);
	}
	return { config: { stateDir, profilePath, failClosedLingerMs, brokerCliPath, reconcilePollMs }, remaining };
}