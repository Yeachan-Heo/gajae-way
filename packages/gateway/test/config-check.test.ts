import { expect, test } from "bun:test";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CONFIG_SCHEMA_VERSION, ENGAGEMENT_GATES, parseConfigFile, RESTART_REQUIRED_FIELDS } from "../src/config";
import { checkConfigFile, configCheckExitCode, defaultConfigPath, renderConfigCheck } from "../src/config-check";

async function configFile(body: string): Promise<string> {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-config-check-"));
	const path = join(dir, "config.json");
	await writeFile(path, body);
	return path;
}

const VALID = JSON.stringify({
	schemaVersion: 1,
	channels: {
		"1469222606497648690": { engagement: "open" },
		"1508664765415690340": {},
	},
});

test("a bootable config reports open and mention-only channel counts", async () => {
	const result = await checkConfigFile(await configFile(VALID));
	expect(result.ok).toBe(true);
	if (!result.ok) return;
	expect(result.channels).toHaveLength(2);
	expect(result.openChannels).toEqual(["1469222606497648690"]);
	expect(configCheckExitCode(result)).toBe(0);
	expect(renderConfigCheck(result)[1]).toContain("open 1, mention-only 1");
});

test("an unknown engagement gate is rejected before a restart can strand the host", async () => {
	// The exact live break: an invalid gate value must fail the offline preflight
	// instead of the gateway exiting 1 on boot while the adapter stayed up.
	// (#31 made "closed" and "open-mention-only" valid gates; only unknown
	// values are rejected.)
	const path = await configFile(
		JSON.stringify({ schemaVersion: 1, channels: { "1508664765415690340": { engagement: "mention-only" } } }),
	);
	const result = await checkConfigFile(path);
	expect(result.ok).toBe(false);
	if (result.ok) return;
	expect(result.code).toBe("config_invalid");
	expect(result.message).toContain("engagement");
	expect(configCheckExitCode(result)).toBe(1);
	expect(renderConfigCheck(result)[0]).toStartWith("FAIL ");
});

test("every defined engagement gate passes the offline preflight", async () => {
	for (const gate of ENGAGEMENT_GATES) {
		const path = await configFile(
			JSON.stringify({ schemaVersion: 1, channels: { "1508664765415690340": { engagement: gate } } }),
		);
		const result = await checkConfigFile(path);
		expect(result.ok, `gate ${gate}`).toBe(true);
	}
});

test("a removed per-channel settle window is rejected with a migration hint", async () => {
	const path = await configFile(
		JSON.stringify({ schemaVersion: 1, channels: { "1": { engagement: "open", settleWindowMs: 500 } } }),
	);
	const result = await checkConfigFile(path);
	expect(result.ok).toBe(false);
	if (result.ok) return;
	expect(result.code).toBe("config_invalid");
});

test("runtime PATH settings are validated and classified as restart-required", () => {
	const config = parseConfigFile({
		schemaVersion: CONFIG_SCHEMA_VERSION,
		runtime: { path: ["~/bin", "/opt/bin"], inheritLoginPath: false },
	});
	expect(config.runtime).toEqual({ path: ["~/bin", "/opt/bin"], inheritLoginPath: false });
	expect(RESTART_REQUIRED_FIELDS).toContain("runtime");
});

test("invalid runtime PATH settings fail the offline config check", async () => {
	for (const runtime of [
		{ path: [] },
		{ path: ["/ok", ""] },
		{ path: ["/ok", 1] },
		{ path: "/not-an-array" },
		{ inheritLoginPath: "yes" },
		{ path: ["/ok"], inheritLoginPath: true },
		{ unexpected: true },
	]) {
		const result = await checkConfigFile(
			await configFile(JSON.stringify({ schemaVersion: CONFIG_SCHEMA_VERSION, runtime })),
		);
		expect(result.ok, JSON.stringify(runtime)).toBe(false);
		if (!result.ok) expect(result.code).toBe("config_invalid");
	}
});

test("malformed JSON is reported as not_json rather than crashing", async () => {
	const result = await checkConfigFile(await configFile('{"schemaVersion": 1,}'));
	expect(result.ok).toBe(false);
	if (result.ok) return;
	expect(result.code).toBe("not_json");
	expect(configCheckExitCode(result)).toBe(1);
});

test("a missing file is reported as unreadable, not as a valid config", async () => {
	const result = await checkConfigFile(join(tmpdir(), "gajaeway-absent-config-should-not-exist.json"));
	expect(result.ok).toBe(false);
	if (result.ok) return;
	expect(result.code).toBe("unreadable");
});

test("the default path follows GAJAEWAY_HOME", () => {
	expect(defaultConfigPath({ GAJAEWAY_HOME: "/tmp/home" } as NodeJS.ProcessEnv)).toBe("/tmp/home/config.json");
});
