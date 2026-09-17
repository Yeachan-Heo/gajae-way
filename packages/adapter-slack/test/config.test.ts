import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadSlackAdapterConfig, SlackAdapterStartupError } from "../src/config";

async function fixture(body: (home: string, save: (value: unknown) => Promise<void>) => Promise<void>) {
	const home = await mkdtemp(join(tmpdir(), "slack-config-"));
	try {
		await writeFile(join(home, "bot"), "  xoxb-secret\n");
		await writeFile(join(home, "app"), "\nxapp-secret  ");
		await body(home, (value) => writeFile(join(home, "adapter-slack.json"), JSON.stringify(value)));
	} finally {
		await rm(home, { recursive: true, force: true });
	}
}

const valid = { botTokenFile: "bot", appTokenFile: "app" };

test("Slack missing or unreadable JSON config identifies its path", async () => {
	await fixture(async (home) => {
		await expect(loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toThrow(join(home, "adapter-slack.json"));
		await writeFile(join(home, "adapter-slack.json"), "{");
		await expect(loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toBeInstanceOf(SlackAdapterStartupError);
	});
});

test("Slack requires both nonblank credential paths", async () => {
	await fixture(async (home, save) => {
		for (const field of ["botTokenFile", "appTokenFile"]) {
			for (const value of [undefined, "", "  ", 42]) {
				await save({ ...valid, [field]: value });
				await expect(loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toThrow(`non-empty ${field}`);
			}
		}
	});
});

test("Slack relative paths use config directory and tokens are trimmed", async () => {
	await fixture(async (home, save) => {
		// Empty policies are valid regardless of future allowlist changes.
		await save({ ...valid, channels: { C1: {} } });
		const config = await loadSlackAdapterConfig({ GAJAEWAY_HOME: home });
		expect(config).toEqual({
			botTokenFile: join(home, "bot"),
			appTokenFile: join(home, "app"),
			botToken: "xoxb-secret",
			appToken: "xapp-secret",
			configPath: join(home, "adapter-slack.json"),
			channels: { C1: {} },
		});
	});
});

test("Slack rejects missing, empty, and wrong-prefix credentials without disclosing values", async () => {
	await fixture(async (home, save) => {
		await save(valid);
		for (const [file, prefix, bad] of [
			["bot", "xoxb-", "xoxp-private-secret"],
			["app", "xapp-", "xoxb-private-secret"],
		]) {
			await writeFile(join(home, file as string), bad as string);
			try {
				await loadSlackAdapterConfig({ GAJAEWAY_HOME: home });
				throw new Error("Expected Slack credential rejection");
			} catch (error) {
				expect(error).toBeInstanceOf(SlackAdapterStartupError);
				expect((error as Error).message).toContain(join(home, file as string));
				expect((error as Error).message).toContain(prefix as string);
				expect((error as Error).message).not.toContain(bad as string);
			}
			await writeFile(join(home, file as string), " \n");
			await expect(loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toThrow("is empty");
			await writeFile(join(home, file as string), `${prefix}valid`);
		}
		await save({ ...valid, appTokenFile: "missing" });
		await expect(loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toThrow("Check appTokenFile");
	});
});

test("Slack rejects malformed channel policies and gateway socket", async () => {
	await fixture(async (home, save) => {
		for (const channels of [
			null,
			[],
			{ C: null },
			{ C: { unknown: true } },
			{ C: { engagement: "bad" } },
			{ C: { audience: "bad" } },
		]) {
			await save({ ...valid, channels });
			await expect(loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toThrow("Slack adapter channels entries");
		}
		await save({ ...valid, gatewaySocket: 7 });
		await expect(loadSlackAdapterConfig({ GAJAEWAY_HOME: home })).rejects.toThrow("gatewaySocket must be a string");
	});
});
