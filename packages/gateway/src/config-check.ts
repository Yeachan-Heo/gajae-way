import { readFile } from "node:fs/promises";
import { ConfigError, gatewayHome, parseConfigFile } from "./config";

/**
 * Offline config validation (`gajaeway-gateway config check [path]`).
 *
 * Channel policy is read only at boot, so an invalid value never degrades a
 * running gateway — it stops the *next* one from starting. Observed twice in one
 * day on the resident host: a hand-edited `config.json` carried
 * `engagement: "closed"` (the schema accepts only `"open"`; mention-only is
 * expressed by omitting the field), `parseChannels` threw `config_invalid`, and
 * launchd respawned a process that exited 1 while the Discord adapter stayed up.
 * Messages kept arriving with nothing behind them, so the bot looked slow rather
 * than dead, and it went unnoticed for hours.
 *
 * This check needs no socket on purpose: by the time you want it, the gateway is
 * down. It is a pre-restart gate, not a post-mortem.
 */
export interface ConfigCheckOk {
	readonly ok: true;
	readonly path: string;
	readonly channels: readonly string[];
	readonly openChannels: readonly string[];
}

export interface ConfigCheckFailure {
	readonly ok: false;
	readonly path: string;
	readonly code: string;
	readonly message: string;
}

export type ConfigCheckResult = ConfigCheckOk | ConfigCheckFailure;

export function defaultConfigPath(env: NodeJS.ProcessEnv = process.env): string {
	return `${gatewayHome(env)}/config.json`;
}

export async function checkConfigFile(path: string): Promise<ConfigCheckResult> {
	let raw: string;
	try {
		raw = await readFile(path, "utf8");
	} catch (error) {
		return { ok: false, path, code: "unreadable", message: messageOf(error) };
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch (error) {
		// A stray comma is the most common hand-edit break and the parser reports
		// it by offset, so the raw message is more useful than a summary.
		return { ok: false, path, code: "not_json", message: messageOf(error) };
	}
	try {
		const config = parseConfigFile(parsed);
		const channels = Object.keys(config.channels ?? {});
		return {
			ok: true,
			path,
			channels,
			openChannels: channels.filter((id) => config.channels?.[id]?.engagement === "open"),
		};
	} catch (error) {
		return {
			ok: false,
			path,
			code: error instanceof ConfigError ? error.code : "invalid",
			message: messageOf(error),
		};
	}
}

export function renderConfigCheck(result: ConfigCheckResult): string[] {
	if (!result.ok) return [`FAIL ${result.path}`, `  ${result.code}: ${result.message}`];
	const mentionOnly = result.channels.length - result.openChannels.length;
	return [
		`OK ${result.path}`,
		`  channels: ${result.channels.length} (open ${result.openChannels.length}, mention-only ${mentionOnly})`,
		"  channel policy applies at gateway start only: restart, then confirm the new pid started after this file's mtime.",
	];
}

/** 0 when the file would boot, 1 when it would not. */
export function configCheckExitCode(result: ConfigCheckResult): number {
	return result.ok ? 0 : 1;
}

function messageOf(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
