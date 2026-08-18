import * as os from "node:os";
import * as path from "node:path";

export const DEFAULT_FAIL_CLOSED_LINGER_MS = 300_000;

export interface WayConfig {
	readonly stateDir: string;
	readonly profilePath: string;
	readonly failClosedLingerMs: number;
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
		stateDir: path.resolve(environment.WAY_STATE_DIR || path.join(os.homedir(), ".local", "state", "gajae-way")),
		profilePath: path.resolve(environment.WAY_PROFILE || "ops/profiles/gaebal-gajae.example.toml"),
		failClosedLingerMs: parseLingerMs(environment.WAY_FAIL_CLOSED_LINGER_MS, "WAY_FAIL_CLOSED_LINGER_MS") ?? DEFAULT_FAIL_CLOSED_LINGER_MS,
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

export interface ParsedWayConfig {
	readonly config: WayConfig;
	readonly remaining: readonly string[];
}

/** Parses only process-wide options so command-specific parsing stays explicit. */
export function parseWayConfig(arguments_: readonly string[], initial = defaultConfig()): ParsedWayConfig {
	let stateDir = initial.stateDir;
	let profilePath = initial.profilePath;
	let failClosedLingerMs = initial.failClosedLingerMs;
	const remaining: string[] = [];
	for (let index = 0; index < arguments_.length; index += 1) {
		const argument = arguments_[index] as string;
		if (argument === "--state-dir" || argument === "--profile" || argument === "--fail-closed-linger-ms") {
			const value = arguments_[index + 1];
			if (!value) throw new ConfigValidationError(`${argument} requires a value.`);
			if (argument === "--state-dir") stateDir = path.resolve(value);
			if (argument === "--profile") profilePath = path.resolve(value);
			if (argument === "--fail-closed-linger-ms") {
				failClosedLingerMs = parseLingerMs(value, "--fail-closed-linger-ms") as number;
			}
			index += 1;
			continue;
		}
		remaining.push(argument);
	}
	return { config: { stateDir, profilePath, failClosedLingerMs }, remaining };
}