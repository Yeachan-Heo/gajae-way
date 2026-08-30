import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type ConfigError, loadConfig, parseConfigFile, reloadConfig } from "../src/config";

const homes: string[] = [];
afterEach(async () => {
	await Promise.all(homes.splice(0).map((home) => rm(home, { recursive: true, force: true })));
});
async function home(): Promise<string> {
	const path = await mkdtemp(join(tmpdir(), "gajaeway-config-"));
	homes.push(path);
	return path;
}

test("file config is canonical while per-process CLI overrides win", async () => {
	const path = await home();
	await Bun.write(
		join(path, "config.json"),
		JSON.stringify({ schemaVersion: 1, socketPath: "/file.sock", dbPath: "/file.db", logVerbosity: "warn" }),
	);
	const config = await loadConfig({
		home: path,
		env: { GAJAEWAY_HOME: "/ignored" },
		overrides: { socketPath: "/cli.sock" },
	});
	expect(config.socketPath).toBe("/cli.sock");
	expect(config.dbPath).toBe("/file.db");
	expect(config.logVerbosity).toBe("warn");
});

test("rejects a credential file reachable through more than one source", async () => {
	const path = await home();
	await Bun.write(
		join(path, "config.json"),
		JSON.stringify({
			schemaVersion: 1,
			credentials: { one: { credentialFile: "/credential" }, two: { credentialFile: "/credential" } },
		}),
	);
	await expect(loadConfig({ home: path })).rejects.toMatchObject({
		code: "secret_source_conflict",
	} satisfies Partial<ConfigError>);
});

test("atomic reload keeps the last valid config on validation failure", async () => {
	const path = await home();
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, logVerbosity: "info" }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), "{ invalid");
	const result = await reloadConfig(current);
	expect(result.ok).toBe(false);
	if (!result.ok) {
		expect(result.config).toBe(current);
		expect(result.diagnostics[0]?.code).toBe("config_invalid");
	}
});

test("turnTimeoutMs parses when bounded and rejects out-of-range values", () => {
	expect(parseConfigFile({ schemaVersion: 1, turnTimeoutMs: 900_000 }).turnTimeoutMs).toBe(900_000);
	expect(parseConfigFile({ schemaVersion: 1 }).turnTimeoutMs).toBeUndefined();
	expect(() => parseConfigFile({ schemaVersion: 1, turnTimeoutMs: 5_000 })).toThrow("turnTimeoutMs");
	expect(() => parseConfigFile({ schemaVersion: 1, turnTimeoutMs: "long" })).toThrow("turnTimeoutMs");
});

test("monitorContextFailureThreshold parses when bounded and rejects out-of-range values", () => {
	expect(parseConfigFile({ schemaVersion: 1, monitorContextFailureThreshold: 3 }).monitorContextFailureThreshold).toBe(
		3,
	);
	// Unset means the code default (MONITOR_CONTEXT_FAILURE_THRESHOLD), not a config value.
	expect(parseConfigFile({ schemaVersion: 1 }).monitorContextFailureThreshold).toBeUndefined();
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureThreshold: 0 })).toThrow(
		"monitorContextFailureThreshold",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureThreshold: 11 })).toThrow(
		"monitorContextFailureThreshold",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureThreshold: 2.5 })).toThrow(
		"monitorContextFailureThreshold",
	);
});

test("model accepts an explicit selector or a preset", () => {
	expect(parseConfigFile({ schemaVersion: 1, model: "openai/gpt-5.2" }).model).toBe("openai/gpt-5.2");
	expect(parseConfigFile({ schemaVersion: 1, model: { preset: "reliable" } }).model).toEqual({ preset: "reliable" });
	expect(() => parseConfigFile({ schemaVersion: 1, model: { preset: "" } })).toThrow("model.preset");
	expect(() => parseConfigFile({ schemaVersion: 1, model: { preset: "reliable", extra: true } })).toThrow(
		"contain only preset",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, model: ["one", "two"] })).toThrow("model must be an object");
});
