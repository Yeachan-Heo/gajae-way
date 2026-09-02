import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadConfig, parseConfigFile, RESTART_REQUIRED_FIELDS, reloadConfig } from "../src/config";
import { checkConfigFile, renderConfigCheck } from "../src/config-check";

const creds = {
	discord: { credentialFile: "/secrets/discord" },
	discordVoice: { credentialFile: "/secrets/voice" },
	telegram: { credentialFile: "/secrets/telegram" },
};

function parse(value: Record<string, unknown>) {
	return parseConfigFile({ schemaVersion: 1, ...value });
}

test("adapters.telegram: {} is valid with credentials.telegram and enables the adapter", () => {
	const config = parse({ credentials: { telegram: creds.telegram }, adapters: { telegram: {} } });
	expect(config.adapters).toEqual({ telegram: {} });
});

test("adapters.discord accepts intents and voice and normalises the section", () => {
	const config = parse({
		credentials: creds,
		adapters: { discord: { intents: [1, 512], voice: { languageCode: "ko", speechSpeed: 1.2, timeoutMs: 5000 } } },
	});
	expect(config.adapters?.discord).toEqual({
		intents: [1, 512],
		voice: { languageCode: "ko", speechSpeed: 1.2, timeoutMs: 5000 },
	});
});

test("absent adapters means a gateway-only daemon", () => {
	expect(parse({}).adapters).toBeUndefined();
});

const rejections: Array<[string, Record<string, unknown>, string]> = [
	[
		"discord without credential",
		{ adapters: { discord: {} } },
		"adapters.discord requires credentials.discord.credentialFile",
	],
	[
		"voice without credential",
		{ credentials: { discord: creds.discord }, adapters: { discord: { voice: {} } } },
		"adapters.discord.voice requires credentials.discordVoice.credentialFile",
	],
	[
		"telegram without credential",
		{ adapters: { telegram: {} } },
		"adapters.telegram requires credentials.telegram.credentialFile",
	],
	["unknown adapter", { adapters: { slack: {} } }, "adapters.slack is not a supported adapter"],
	[
		"unknown discord field",
		{ credentials: creds, adapters: { discord: { tokenFile: "x" } } },
		"adapters.discord.tokenFile is not a recognised field",
	],
	[
		"gatewaySocket is gone",
		{ credentials: creds, adapters: { discord: { gatewaySocket: "x" } } },
		"adapters.discord.gatewaySocket is not a recognised field",
	],
	[
		"unknown telegram field",
		{ credentials: creds, adapters: { telegram: { chats: {} } } },
		"adapters.telegram.chats is not a recognised field",
	],
	[
		"non-integer intents",
		{ credentials: creds, adapters: { discord: { intents: [1, "x"] } } },
		"adapters.discord.intents must be an array of integer intent values",
	],
	[
		"voice string field",
		{ credentials: creds, adapters: { discord: { voice: { voiceId: 3 } } } },
		"adapters.discord.voice.voiceId must be a string when set",
	],
	[
		"voice integer field",
		{ credentials: creds, adapters: { discord: { voice: { timeoutMs: 0 } } } },
		"adapters.discord.voice.timeoutMs must be a positive integer when set",
	],
	[
		"voice speed",
		{ credentials: creds, adapters: { discord: { voice: { speechSpeed: "fast" } } } },
		"adapters.discord.voice.speechSpeed must be a finite number when set",
	],
	[
		"voice unknown field",
		{ credentials: creds, adapters: { discord: { voice: { apiKeyFile: "x" } } } },
		"adapters.discord.voice.apiKeyFile is not a recognised field",
	],
];

for (const [name, value, message] of rejections) {
	test(`rejects ${name}`, () => {
		expect(() => parse(value)).toThrow(message);
	});
}

test("adapters is restart-required on reload", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-config-adapters-"));
	try {
		await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }));
		const current = await loadConfig({ home });
		await writeFile(
			join(home, "config.json"),
			JSON.stringify({ schemaVersion: 1, credentials: { telegram: creds.telegram }, adapters: { telegram: {} } }),
		);
		const result = await reloadConfig(current);
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.restartRequired).toContain("adapters");
			expect(result.restartRequired).toContain("credentials");
		}
		expect(RESTART_REQUIRED_FIELDS).toContain("adapters");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("config check lists enabled adapters and per-platform channel counts", async () => {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-config-check-adapters-"));
	try {
		const path = join(home, "config.json");
		await writeFile(
			path,
			JSON.stringify({
				schemaVersion: 1,
				credentials: creds,
				adapters: { discord: {}, telegram: {} },
				channels: { "discord:1": { engagement: "open" }, "telegram:-2": {}, "3": {} },
			}),
		);
		const result = await checkConfigFile(path);
		expect(result.ok).toBe(true);
		const lines = renderConfigCheck(result);
		expect(lines[1]).toBe("  adapters: discord, telegram");
		expect(lines[2]).toContain("discord 1, telegram 1");
		await writeFile(path, JSON.stringify({ schemaVersion: 1 }));
		expect(renderConfigCheck(await checkConfigFile(path))[1]).toBe("  adapters: none");
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});
