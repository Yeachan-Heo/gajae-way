import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ConfigError, loadConfig, parseConfigFile, reloadConfig } from "../src/config";

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

test("turnTimeoutMs is rejected with persistent-session migration guidance", () => {
	expect(() => parseConfigFile({ schemaVersion: 1, turnTimeoutMs: 900_000 })).toThrow(
		"turnTimeoutMs was removed with persistent SDK sessions",
	);
});

test("monitorContextFailureRollThreshold parses when bounded and rejects out-of-range values", () => {
	expect(
		parseConfigFile({ schemaVersion: 1, monitorContextFailureRollThreshold: 3 }).monitorContextFailureRollThreshold,
	).toBe(3);
	// Unset means the code default (MONITOR_CONTEXT_FAILURE_ROLL_THRESHOLD), not a config value.
	expect(parseConfigFile({ schemaVersion: 1 }).monitorContextFailureRollThreshold).toBeUndefined();
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureRollThreshold: 0 })).toThrow(
		"monitorContextFailureRollThreshold",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureRollThreshold: 21 })).toThrow(
		"monitorContextFailureRollThreshold",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, monitorContextFailureRollThreshold: 2.5 })).toThrow(
		"monitorContextFailureRollThreshold",
	);
});

test("model accepts an explicit selector or a preset", () => {
	expect(parseConfigFile({ schemaVersion: 1, model: "openai/gpt-5.2" }).model).toBe("openai/gpt-5.2");
	expect(parseConfigFile({ schemaVersion: 1, model: { preset: "reliable" } }).model).toEqual({ preset: "reliable" });
	expect(() => parseConfigFile({ schemaVersion: 1, model: { preset: "" } })).toThrow("model.preset");
	expect(() => parseConfigFile({ schemaVersion: 1, model: { preset: "reliable", extra: true } })).toThrow(
		"contain only preset",
	);
	expect(parseConfigFile({ schemaVersion: 1, serviceTier: "priority" }).serviceTier).toBe("priority");
	expect(() => parseConfigFile({ schemaVersion: 1, serviceTier: "fast" })).toThrow("serviceTier must be one of");
	expect(() => parseConfigFile({ schemaVersion: 1, model: ["one", "two"] })).toThrow("model must be an object");
});

test("stallTimeoutMs has an operational default and the removed batching fields give a migration error", async () => {
	const path = await home();
	const defaults = await loadConfig({ home: path });
	expect(defaults.stallTimeoutMs).toBe(120_000);
	expect(parseConfigFile({ schemaVersion: 1, stallTimeoutMs: 120_000 }).stallTimeoutMs).toBe(120_000);
	for (const removed of ["debounceMs", "settleWindowMs", "maxInboundAgeMs"])
		expect(() => parseConfigFile({ schemaVersion: 1, [removed]: 1_000 })).toThrow(`${removed} was removed`);
	for (const removed of ["debounceMs", "settleWindowMs"])
		expect(() => parseConfigFile({ schemaVersion: 1, channels: { c1: { [removed]: 1_000 } } })).toThrow(
			`channels.c1.${removed} was removed`,
		);
});

test("stallTimeoutMs reloads as live actor policy", async () => {
	const path = await home();
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, stallTimeoutMs: 120_000 }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, stallTimeoutMs: 240_000 }));
	const result = await reloadConfig(current);
	expect(result.ok).toBe(true);
	if (!result.ok) return;
	expect(result.changed).toEqual(["stallTimeoutMs"]);
	expect(result.config.stallTimeoutMs).toBe(240_000);
});

test("work lane limits parse without materializing omitted configuration", () => {
	const work = { maxLanes: 4, idleRetireMs: 3_600_000 };
	expect(parseConfigFile({ schemaVersion: 1, work }).work).toEqual(work);
	expect(parseConfigFile({ schemaVersion: 1 })).not.toHaveProperty("work");
});

test("work.allowNested is a boolean restart-required policy and defaults disabled", async () => {
	const path = await home();
	const defaults = await loadConfig({ home: path });
	expect(defaults.work?.allowNested).toBeUndefined();
	expect(parseConfigFile({ schemaVersion: 1, work: { allowNested: true } }).work).toEqual({ allowNested: true });
	expect(parseConfigFile({ schemaVersion: 1, work: { allowNested: false } }).work).toEqual({ allowNested: false });
	for (const invalid of ["true", 1, null])
		expect(() => parseConfigFile({ schemaVersion: 1, work: { allowNested: invalid } })).toThrow("work.allowNested");
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, work: { allowNested: false } }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, work: { allowNested: true } }));
	const result = await reloadConfig(current);
	expect(result).toMatchObject({ ok: true, changed: [], restartRequired: ["work"] });
	if (result.ok) expect(result.config.work).toEqual({ allowNested: false });
});

for (const [work, field] of [
	[{ maxLanes: 0 }, "work.maxLanes"],
	[{ maxLanes: 257 }, "work.maxLanes"],
	[{ maxLanes: 1.5 }, "work.maxLanes"],
	[{ idleRetireMs: 1_000 }, "work.idleRetireMs"],
	[{ foo: 1 }, "work"],
] as const) {
	test(`rejects invalid work configuration ${JSON.stringify(work)}`, () => {
		const parse = () => parseConfigFile({ schemaVersion: 1, work });
		expect(parse).toThrow(ConfigError);
		expect(parse).toThrow(field);
		try {
			parse();
		} catch (error) {
			expect(error).toMatchObject({ code: "config_invalid" });
		}
	});
}

test("work lane limit changes require restart and retain the live limits", async () => {
	const path = await home();
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, work: { maxLanes: 4 } }));
	const current = await loadConfig({ home: path });
	await Bun.write(join(path, "config.json"), JSON.stringify({ schemaVersion: 1, work: { maxLanes: 8 } }));
	const result = await reloadConfig(current);
	expect(result.ok).toBe(true);
	if (!result.ok) return;
	expect(result.restartRequired).toContain("work");
	expect(result.changed).not.toContain("work");
	expect(result.config.work).toEqual({ maxLanes: 4 });
});

test("bot audience budgets parse, default to unset, and reject non-positive counts", () => {
	expect(parseConfigFile({ schemaVersion: 1 })).not.toHaveProperty("botAudience");
	expect(parseConfigFile({ schemaVersion: 1, botAudience: { maxConsecutiveTurns: 3 } }).botAudience).toEqual({
		maxConsecutiveTurns: 3,
	});
	expect(
		parseConfigFile({
			schemaVersion: 1,
			channels: { c1: { engagement: "open", audience: "all", botAudienceMaxTurnsPerWindow: 5 } },
		}).channels?.c1,
	).toEqual({ engagement: "open", audience: "all", botAudienceMaxTurnsPerWindow: 5 });
	for (const invalid of [0, -1, 2.5, "3"])
		expect(() => parseConfigFile({ schemaVersion: 1, botAudience: { maxTurnsPerWindow: invalid } })).toThrow(
			"botAudience.maxTurnsPerWindow must be an integer of at least 1",
		);
	expect(() => parseConfigFile({ schemaVersion: 1, botAudience: { maxTurns: 4 } })).toThrow(
		"botAudience contains an unknown field",
	);
	expect(() => parseConfigFile({ schemaVersion: 1, channels: { c1: { botAudienceCap: 4 } } })).toThrow(
		"channels.c1 contains an unknown field",
	);
});
