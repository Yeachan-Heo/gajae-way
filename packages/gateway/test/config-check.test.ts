import { expect, test } from "bun:test";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
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
		"1469222606497648690": { engagement: "open", debounceMs: 10_000 },
		"1508664765415690340": { debounceMs: 10_000 },
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

test('engagement "closed" is rejected before a restart can strand the host', async () => {
	// The exact live break: mention-only was spelled as a value instead of an
	// omitted field, so the gateway exited 1 on boot while the adapter stayed up.
	const path = await configFile(
		JSON.stringify({ schemaVersion: 1, channels: { "1508664765415690340": { engagement: "closed" } } }),
	);
	const result = await checkConfigFile(path);
	expect(result.ok).toBe(false);
	if (result.ok) return;
	expect(result.code).toBe("config_invalid");
	expect(result.message).toContain("engagement");
	expect(configCheckExitCode(result)).toBe(1);
	expect(renderConfigCheck(result)[0]).toStartWith("FAIL ");
});

test("an out-of-range debounce is rejected", async () => {
	const path = await configFile(
		JSON.stringify({ schemaVersion: 1, channels: { "1": { engagement: "open", debounceMs: 90_000 } } }),
	);
	const result = await checkConfigFile(path);
	expect(result.ok).toBe(false);
	if (result.ok) return;
	expect(result.code).toBe("config_invalid");
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
