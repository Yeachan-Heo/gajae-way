import { describe, expect, test } from "bun:test";
import { mkdtemp, readdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { main } from "../src/main";
import {
	type FetchLike,
	type Prompter,
	parseSetupArgs,
	REDACTED,
	redact,
	runSetup,
	SETUP_ENV,
	SetupError,
	tokensFromEnv,
	validateAll,
} from "../src/setup";

const DISCORD_TOKEN = "MTIzNDU2Nzg5MDEyMzQ1Njc4.GxYzAb.discord-secret-value-000";
const SLACK_BOT = "xoxb-1111-2222-slack-bot-secret";
const SLACK_APP = "xapp-1-A111-3333-slack-app-secret";
const TELEGRAM_TOKEN = "123456789:telegram-secret-value-AAAA";
const ALL_SECRETS = [DISCORD_TOKEN, SLACK_BOT, SLACK_APP, TELEGRAM_TOKEN];

function json(status: number, body: unknown): Response {
	return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

interface FakePlatform {
	readonly fetch: FetchLike;
	readonly calls: { url: string; authorization: string | undefined }[];
}

/** A platform API that accepts exactly the known-good tokens above. */
function fakePlatform(overrides: { discordAppId?: string; slackSocketError?: string } = {}): FakePlatform {
	const calls: FakePlatform["calls"] = [];
	const fetch: FetchLike = async (url, init) => {
		const authorization = new Headers(init?.headers).get("authorization") ?? undefined;
		calls.push({ url, authorization });
		if (url === "https://discord.com/api/v10/users/@me")
			return authorization === `Bot ${DISCORD_TOKEN}`
				? json(200, { id: "900", username: "gajae", bot: true })
				: json(401, { message: "401: Unauthorized" });
		if (url === "https://discord.com/api/v10/applications/@me")
			return json(200, { id: overrides.discordAppId ?? "700", owner: { id: "4242" }, team: null });
		if (url === "https://slack.com/api/auth.test")
			return authorization === `Bearer ${SLACK_BOT}`
				? json(200, { ok: true, user: "gajae", user_id: "U1", team: "T", bot_id: "B1" })
				: json(200, { ok: false, error: "invalid_auth" });
		if (url === "https://slack.com/api/apps.connections.open") {
			if (overrides.slackSocketError) return json(200, { ok: false, error: overrides.slackSocketError });
			return authorization === `Bearer ${SLACK_APP}`
				? json(200, { ok: true, url: "wss://example.invalid/socket" })
				: json(200, { ok: false, error: "invalid_auth" });
		}
		if (url === `https://api.telegram.org/bot${TELEGRAM_TOKEN}/getMe`)
			return json(200, { ok: true, result: { id: 5, username: "gajae_bot" } });
		if (url.startsWith("https://api.telegram.org/bot")) return json(401, { ok: false, description: "Unauthorized" });
		throw new Error(`unexpected url ${url}`);
	};
	return { fetch, calls };
}

async function withHome<T>(run: (home: string) => Promise<T>): Promise<T> {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-setup-"));
	try {
		return await run(join(root, "home"));
	} finally {
		await rm(root, { recursive: true, force: true });
	}
}

async function allFiles(dir: string): Promise<string[]> {
	const entries = await readdir(dir, { recursive: true, withFileTypes: true }).catch(() => []);
	return entries.filter((entry) => entry.isFile()).map((entry) => join(entry.parentPath, entry.name));
}

function expectNoSecret(text: string): void {
	for (const secret of ALL_SECRETS) expect(text).not.toContain(secret);
}

const fullEnv = {
	[SETUP_ENV.discordToken]: DISCORD_TOKEN,
	[SETUP_ENV.slackBotToken]: SLACK_BOT,
	[SETUP_ENV.slackAppToken]: SLACK_APP,
	[SETUP_ENV.telegramToken]: TELEGRAM_TOKEN,
};

describe("setup arguments", () => {
	test("tokens are refused as arguments, and the value is never echoed", () => {
		for (const flag of ["--discord-token", "--slack-bot-token=xoxb-abc", "--token"]) {
			let message = "";
			try {
				parseSetupArgs([flag, DISCORD_TOKEN]);
			} catch (error) {
				message = (error as Error).message;
			}
			expect(message).toContain("tokens are never accepted as arguments");
			expectNoSecret(message);
			expect(message).not.toContain("xoxb-abc");
		}
	});

	test("--adapters rejects unknown adapters and --status stands alone", () => {
		expect(() => parseSetupArgs(["--adapters", "discord,irc"])).toThrow("unknown adapter: irc");
		expect(() => parseSetupArgs(["--status", "--from-env"])).toThrow("--status takes no other options");
		expect(parseSetupArgs(["--from-env", "--adapters", "slack,discord,slack", "--owner", "1,2"])).toEqual({
			mode: "setup",
			fromEnv: true,
			adapters: ["slack", "discord"],
			owners: ["1", "2"],
		});
	});

	test("--from-env requires the full Slack pair instead of silently skipping it", () => {
		expect(() => tokensFromEnv({ [SETUP_ENV.slackBotToken]: SLACK_BOT }, undefined, undefined)).toThrow(
			`slack: ${SETUP_ENV.slackAppToken} is not set`,
		);
		expect(() => tokensFromEnv({}, undefined, undefined)).toThrow("found no adapter tokens");
		expect(() => tokensFromEnv({ [SETUP_ENV.discordToken]: DISCORD_TOKEN }, ["telegram"], undefined)).toThrow(
			`telegram: ${SETUP_ENV.telegramToken} is not set`,
		);
	});
});

describe("setup validation", () => {
	test("each platform API is called with its token and the right endpoint", async () => {
		const platform = fakePlatform();
		const identities = await validateAll(
			{
				discord: { token: DISCORD_TOKEN, appId: "700" },
				slack: { botToken: SLACK_BOT, appToken: SLACK_APP },
				telegram: { token: TELEGRAM_TOKEN },
			},
			platform.fetch,
		);
		expect(identities.map((identity) => identity.platform)).toEqual(["discord", "slack", "telegram"]);
		expect(identities[0]?.ownerId).toBe("4242");
		expect(platform.calls.map((call) => call.url)).toContain("https://slack.com/api/apps.connections.open");
		for (const identity of identities) expectNoSecret(identity.summary);
	});

	test("a rejected Discord token is a clear error that does not contain the token", async () => {
		const wrong = "MTIzNDU2.wrong-discord-token-value";
		const error = await validateAll({ discord: { token: wrong } }, fakePlatform().fetch).catch((e: Error) => e);
		expect(error).toBeInstanceOf(SetupError);
		expect((error as Error).message).toContain("discord: the platform rejected the bot token (HTTP 401)");
		expect((error as Error).message).not.toContain(wrong);
	});

	test("a Discord token for a different application is refused", async () => {
		await expect(
			validateAll({ discord: { token: DISCORD_TOKEN, appId: "701" } }, fakePlatform().fetch),
		).rejects.toThrow("discord: the token belongs to application 700, not 701");
	});

	test("Slack prefixes, bad bot tokens, and a Socket-Mode-less app token are each named", async () => {
		const platform = fakePlatform();
		await expect(validateAll({ slack: { botToken: SLACK_APP, appToken: SLACK_APP } }, platform.fetch)).rejects.toThrow(
			"must start with xoxb-",
		);
		await expect(validateAll({ slack: { botToken: SLACK_BOT, appToken: SLACK_BOT } }, platform.fetch)).rejects.toThrow(
			"must start with xapp-",
		);
		await expect(
			validateAll({ slack: { botToken: "xoxb-9999-wrong-bot-token", appToken: SLACK_APP } }, platform.fetch),
		).rejects.toThrow("slack: the platform rejected the bot token (invalid_auth)");
		await expect(
			validateAll(
				{ slack: { botToken: SLACK_BOT, appToken: SLACK_APP } },
				fakePlatform({ slackSocketError: "not_allowed_token_type" }).fetch,
			),
		).rejects.toThrow("cannot open a Socket Mode connection (not_allowed_token_type)");
	});

	test("a network failure that echoes the request URL is redacted (Telegram puts the token in the URL)", async () => {
		const leaky: FetchLike = async (url) => {
			throw new Error(`connect ECONNREFUSED ${url}`);
		};
		const error = (await validateAll({ telegram: { token: TELEGRAM_TOKEN } }, leaky).catch((e: Error) => e)) as Error;
		expect(error.message).toContain("telegram: could not reach the platform API");
		expect(error.message).toContain(REDACTED);
		expectNoSecret(error.message);
	});

	test("redact replaces every occurrence and ignores trivially short strings", () => {
		expect(redact(`a ${SLACK_BOT} b ${SLACK_BOT}`, [SLACK_BOT])).toBe(`a ${REDACTED} b ${REDACTED}`);
		expect(redact("abc", ["a"])).toBe("abc");
	});
});

describe("setup writing", () => {
	test("--from-env writes 0600 secrets, 0700 dirs, adapter configs, and a config that references files", async () => {
		await withHome(async (home) => {
			const lines: string[] = [];
			const platform = fakePlatform();
			await runSetup(["--from-env", "--owner", "U-OWNER"], {
				home,
				env: { ...fullEnv, [SETUP_ENV.discordAppId]: "700" },
				fetch: platform.fetch,
				log: (line) => lines.push(line),
			});
			expect((await stat(home)).mode & 0o777).toBe(0o700);
			expect((await stat(join(home, "secrets"))).mode & 0o777).toBe(0o700);
			for (const file of await allFiles(home)) expect((await stat(file)).mode & 0o777).toBe(0o600);

			expect((await readFile(join(home, "secrets", "discord-token"), "utf8")).trim()).toBe(DISCORD_TOKEN);
			expect((await readFile(join(home, "secrets", "slack-bot-token"), "utf8")).trim()).toBe(SLACK_BOT);
			expect((await readFile(join(home, "secrets", "slack-app-token"), "utf8")).trim()).toBe(SLACK_APP);
			expect((await readFile(join(home, "secrets", "telegram-token"), "utf8")).trim()).toBe(TELEGRAM_TOKEN);

			const config = JSON.parse(await readFile(join(home, "config.json"), "utf8"));
			expect(config.schemaVersion).toBe(1);
			expect(config.credentials.discord.credentialFile).toBe(join(home, "secrets", "discord-token"));
			expect(config.credentials["slack-app"].credentialFile).toBe(join(home, "secrets", "slack-app-token"));
			expect(config.mentionAllowlist).toEqual(["U-OWNER"]);
			expect(JSON.parse(await readFile(join(home, "adapter-slack.json"), "utf8"))).toEqual({
				botTokenFile: "secrets/slack-bot-token",
				appTokenFile: "secrets/slack-app-token",
			});
			expect(JSON.parse(await readFile(join(home, "adapter-discord.json"), "utf8"))).toEqual({
				tokenFile: "secrets/discord-token",
			});

			// Nothing but the secrets/ files holds a token: not configs, not output.
			for (const file of await allFiles(home))
				if (!file.includes(`${join(home, "secrets")}/`)) expectNoSecret(await readFile(file, "utf8"));
			expectNoSecret(lines.join("\n"));
			expect(lines.at(-1)).toBe("enabled adapters: discord,slack,telegram");
		});
	});

	test("an invalid token writes nothing at all, even when another token was valid", async () => {
		await withHome(async (home) => {
			const error = await runSetup(["--from-env"], {
				home,
				env: { ...fullEnv, [SETUP_ENV.slackBotToken]: "xoxb-0000-revoked-bot-token" },
				fetch: fakePlatform().fetch,
				log: () => {},
			}).catch((e: Error) => e);
			expect((error as Error).message).toContain("slack: the platform rejected the bot token");
			expect((error as Error).message).not.toContain("xoxb-0000-revoked-bot-token");
			expect(await allFiles(home)).toEqual([]);
		});
	});

	test("rotating one adapter keeps every other adapter and unrelated config field", async () => {
		await withHome(async (home) => {
			await runSetup(["--from-env"], { home, env: fullEnv, fetch: fakePlatform().fetch, log: () => {} });
			const configPath = join(home, "config.json");
			const edited = JSON.parse(await readFile(configPath, "utf8"));
			await writeFile(
				configPath,
				JSON.stringify({ ...edited, stallTimeoutMs: 90_000, channels: { "slack:C1": { engagement: "open" } } }),
			);
			await writeFile(
				join(home, "adapter-slack.json"),
				JSON.stringify({
					botTokenFile: "secrets/slack-bot-token",
					appTokenFile: "secrets/slack-app-token",
					channels: { C1: { engagement: "open" } },
				}),
			);

			const rotated = "xoxb-5555-rotated-bot-token";
			const platform = fakePlatform();
			const accepting: FetchLike = (url, init) =>
				url === "https://slack.com/api/auth.test"
					? Promise.resolve(json(200, { ok: true, user: "g", user_id: "U1", team: "T", bot_id: "B1" }))
					: platform.fetch(url, init);
			await runSetup(["--from-env", "--adapters", "slack"], {
				home,
				env: { [SETUP_ENV.slackBotToken]: rotated, [SETUP_ENV.slackAppToken]: SLACK_APP },
				fetch: accepting,
				log: () => {},
			});
			expect((await readFile(join(home, "secrets", "slack-bot-token"), "utf8")).trim()).toBe(rotated);
			expect((await readFile(join(home, "secrets", "discord-token"), "utf8")).trim()).toBe(DISCORD_TOKEN);
			const config = JSON.parse(await readFile(configPath, "utf8"));
			expect(config.stallTimeoutMs).toBe(90_000);
			expect(config.channels).toEqual({ "slack:C1": { engagement: "open" } });
			expect(Object.keys(config.credentials).sort()).toEqual(["discord", "slack-app", "slack-bot", "telegram"]);
			expect(config.mentionAllowlist).toEqual(["4242"]);
			expect(JSON.parse(await readFile(join(home, "adapter-slack.json"), "utf8")).channels).toEqual({
				C1: { engagement: "open" },
			});
		});
	});

	test("the Discord application owner is allowlisted when no owner is named", async () => {
		await withHome(async (home) => {
			await runSetup(["--from-env", "--adapters", "discord"], {
				home,
				env: fullEnv,
				fetch: fakePlatform().fetch,
				log: () => {},
			});
			expect(JSON.parse(await readFile(join(home, "config.json"), "utf8")).mentionAllowlist).toEqual(["4242"]);
			expect(await allFiles(join(home, "secrets"))).toEqual([join(home, "secrets", "discord-token")]);
		});
	});

	test("interactive setup reads tokens through the secret prompt only and never prints them", async () => {
		await withHome(async (home) => {
			const asked: string[] = [];
			const secretAsked: string[] = [];
			const answers = ["n", "y", "", "U-ME"];
			const secretAnswers = [SLACK_BOT, SLACK_APP];
			const prompter: Prompter = {
				ask: async (question) => {
					asked.push(question);
					return answers.shift() ?? "";
				},
				askSecret: async (question) => {
					secretAsked.push(question);
					return secretAnswers.shift() ?? "";
				},
				close: () => {},
			};
			const lines: string[] = [];
			await runSetup([], { home, prompter: () => prompter, fetch: fakePlatform().fetch, log: (l) => lines.push(l) });
			expect(secretAsked).toHaveLength(2);
			expect(asked[0]).toContain("Enable Discord?");
			expect(asked[1]).toContain("Enable Slack");
			expect(asked[2]).toContain("Enable Telegram?");
			expectNoSecret([...asked, ...secretAsked, ...lines].join("\n"));
			expect(lines.at(-1)).toBe("enabled adapters: slack");
			expect(JSON.parse(await readFile(join(home, "config.json"), "utf8")).mentionAllowlist).toEqual(["U-ME"]);
		});
	});

	test("setup --status lists configured adapters for the one-touch entry script", async () => {
		await withHome(async (home) => {
			const lines: string[] = [];
			await runSetup(["--status"], { home, log: (line) => lines.push(line) });
			expect(lines).toEqual([""]);
			await runSetup(["--from-env", "--adapters", "slack"], {
				home,
				env: fullEnv,
				fetch: fakePlatform().fetch,
				log: () => {},
			});
			await runSetup(["--status"], { home, log: (line) => lines.push(line) });
			expect(lines.at(-1)).toBe("slack");
		});
	});

	test("the CLI entry reports a rejected token on stderr, exits 1, and leaks nothing", async () => {
		await withHome(async (home) => {
			const previousHome = process.env.GAJAEWAY_HOME;
			const previousExit = process.exitCode;
			const errors: string[] = [];
			const logs: string[] = [];
			const originalError = console.error;
			const originalLog = console.log;
			console.error = (message: unknown) => errors.push(String(message));
			console.log = (message: unknown) => logs.push(String(message));
			process.env.GAJAEWAY_HOME = home;
			const wrong = "MTIzNDU2.cli-wrong-discord-token";
			try {
				await main(["setup", "--from-env"], {
					setup: { env: { [SETUP_ENV.discordToken]: wrong }, fetch: fakePlatform().fetch },
				});
				expect(process.exitCode).toBe(1);
				expect(errors.join("\n")).toContain("discord: the platform rejected the bot token");
				expect([...errors, ...logs].join("\n")).not.toContain(wrong);
				expect(await allFiles(home)).toEqual([]);
			} finally {
				console.error = originalError;
				console.log = originalLog;
				if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
				else process.env.GAJAEWAY_HOME = previousHome;
				process.exitCode = previousExit ?? 0;
			}
		});
	});
});
