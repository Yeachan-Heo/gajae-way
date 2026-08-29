import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DiscordAdapterStartupError, loadDiscordAdapterConfig } from "../src/config";

/** Builds an adapter home with a valid token and the given config body. */
async function withHome(config: Record<string, unknown>, files: Record<string, string> = {}) {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-voice-config-"));
	await writeFile(join(home, "token"), "secret-token\n");
	for (const [name, body] of Object.entries(files)) await writeFile(join(home, name), body);
	await writeFile(join(home, "adapter-discord.json"), JSON.stringify({ tokenFile: "token", ...config }));
	return { home, load: () => loadDiscordAdapterConfig({ GAJAEWAY_HOME: home }) };
}

test("an omitted voice section leaves transcription off instead of failing startup", async () => {
	const { home, load } = await withHome({});
	try {
		expect((await load()).voice).toBeUndefined();
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("the voice key is read from its file and the path is resolved against the config", async () => {
	const { home, load } = await withHome({ voice: { apiKeyFile: "el-key" } }, { "el-key": " sk_secret \n" });
	try {
		const config = await load();
		expect(config.voice?.apiKey).toBe("sk_secret");
		expect(config.voice?.apiKeyFile).toBe(join(home, "el-key"));
		// The key must never be inlined into the config surface itself.
		expect(JSON.stringify(config)).not.toContain('apiKey":"sk_secret","apiKeyFile');
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("optional voice fields are carried through when set", async () => {
	const { home, load } = await withHome(
		{ voice: { apiKeyFile: "k", languageCode: "ko", model: "scribe_v2", endpoint: "https://x/stt", timeoutMs: 5000 } },
		{ k: "sk_1" },
	);
	try {
		const voice = (await load()).voice;
		expect(voice).toMatchObject({
			languageCode: "ko",
			model: "scribe_v2",
			endpoint: "https://x/stt",
			timeoutMs: 5000,
		});
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

// A present-but-broken voice section is an operator mistake. Downgrading to "no
// transcription" would hide a typo behind behaviour that looks merely unconfigured.
test("a voice section without an apiKeyFile is a startup error, not a silent downgrade", async () => {
	const { home, load } = await withHome({ voice: {} });
	try {
		await expect(load()).rejects.toBeInstanceOf(DiscordAdapterStartupError);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("a blank apiKeyFile is rejected", async () => {
	const { home, load } = await withHome({ voice: { apiKeyFile: "   " } });
	try {
		await expect(load()).rejects.toBeInstanceOf(DiscordAdapterStartupError);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("an unreadable key file is a startup error naming the field to fix", async () => {
	const { home, load } = await withHome({ voice: { apiKeyFile: "missing-key" } });
	try {
		await expect(load()).rejects.toThrow(/voice\.apiKeyFile/);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("an empty key file is a startup error rather than an empty auth header", async () => {
	const { home, load } = await withHome({ voice: { apiKeyFile: "k" } }, { k: "  \n" });
	try {
		await expect(load()).rejects.toBeInstanceOf(DiscordAdapterStartupError);
	} finally {
		await rm(home, { recursive: true, force: true });
	}
});

test("non-string voice fields are rejected", async () => {
	for (const field of ["languageCode", "endpoint", "model"]) {
		const { home, load } = await withHome({ voice: { apiKeyFile: "k", [field]: 7 } }, { k: "sk_1" });
		try {
			await expect(load()).rejects.toBeInstanceOf(DiscordAdapterStartupError);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}
});

test("a non-positive or fractional timeout is rejected", async () => {
	for (const timeoutMs of [0, -1, 1.5, "5000"]) {
		const { home, load } = await withHome({ voice: { apiKeyFile: "k", timeoutMs } }, { k: "sk_1" });
		try {
			await expect(load()).rejects.toBeInstanceOf(DiscordAdapterStartupError);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}
});
