import { describe, expect, test } from "bun:test";
import { mkdtemp, readdir, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type FetchLike, SETUP_ENV } from "../../cli/src/setup";
import { runSetupWizard } from "../src/setup-wizard";

const DISCORD_TOKEN = "MTIzNDU2Nzg5MDEyMzQ1Njc4.GxYzAb.discord-secret-value-000";
const SLACK_BOT = "xoxb-1111-2222-slack-bot-secret";
const SLACK_APP = "xapp-1-A111-3333-slack-app-secret";
const TELEGRAM_TOKEN = "123456789:telegram-secret-value-AAAA";
const SECRETS = [DISCORD_TOKEN, SLACK_BOT, SLACK_APP, TELEGRAM_TOKEN];
const fullEnv = {
	[SETUP_ENV.discordToken]: DISCORD_TOKEN,
	[SETUP_ENV.slackBotToken]: SLACK_BOT,
	[SETUP_ENV.slackAppToken]: SLACK_APP,
	[SETUP_ENV.telegramToken]: TELEGRAM_TOKEN,
};

function json(status: number, body: unknown): Response {
	return new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
}

// Same token-sensitive platform responses as the CLI setup tests; no real network calls.
function fakePlatform(): { fetch: FetchLike; calls: string[] } {
	const calls: string[] = [];
	return {
		calls,
		fetch: async (url, init) => {
			calls.push(url);
			const authorization = new Headers(init?.headers).get("authorization");
			if (url === "https://discord.com/api/v10/users/@me")
				return authorization === `Bot ${DISCORD_TOKEN}`
					? json(200, { id: "900", username: "gajae", bot: true })
					: json(401, { message: "401: Unauthorized" });
			if (url === "https://discord.com/api/v10/applications/@me")
				return json(200, { id: "700", owner: { id: "4242" }, team: null });
			if (url === "https://slack.com/api/auth.test")
				return authorization === `Bearer ${SLACK_BOT}`
					? json(200, { ok: true, user: "gajae", user_id: "U1", team: "T", bot_id: "B1" })
					: json(200, { ok: false, error: "invalid_auth" });
			if (url === "https://slack.com/api/apps.connections.open")
				return authorization === `Bearer ${SLACK_APP}`
					? json(200, { ok: true, url: "wss://example.invalid/socket" })
					: json(200, { ok: false, error: "invalid_auth" });
			if (url === `https://api.telegram.org/bot${TELEGRAM_TOKEN}/getMe`)
				return json(200, { ok: true, result: { id: 5, username: "gajae_bot" } });
			if (url.startsWith("https://api.telegram.org/bot")) return json(401, { ok: false });
			throw new Error("Unexpected platform API endpoint");
		},
	};
}

class MockRenderer {
	readonly frames: string[] = [];
	cursorVisible = true;
	styleResets = 0;
	write(text: string): void {
		this.frames.push(text);
	}
	clear(): void {}
	clearLine(): void {}
	moveCursor(_row: number, _column: number): void {}
	resetStyle(): void {
		this.styleResets++;
	}
	hideCursor(): void {
		this.cursorVisible = false;
	}
	showCursor(): void {
		this.cursorVisible = true;
	}
	get output(): string {
		return this.frames.join("");
	}
}

class MockKeyboardInput {
	closed = false;
	readonly consumed: string[] = [];
	constructor(readonly keys: string[] = []) {}
	async readKey() {
		const key = this.keys.shift();
		if (key === undefined) throw new Error("Mock keyboard sequence exhausted");
		this.consumed.push(key);
		const codes: Record<string, string> = {
			enter: "\r",
			up: "\x1b[A",
			down: "\x1b[B",
			escape: "\x1b",
			backspace: "\x7f",
		};
		return { key, code: codes[key] ?? key, isShift: false, isCtrl: false, isAlt: false };
	}
	close(): void {
		this.closed = true;
	}
}

async function withHome(run: (home: string) => Promise<void>): Promise<void> {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-wizard-test-"));
	try {
		await run(join(root, "home"));
	} finally {
		await rm(root, { recursive: true, force: true });
	}
}

async function files(home: string): Promise<string[]> {
	const entries = await readdir(home, { recursive: true, withFileTypes: true }).catch(
		(error: NodeJS.ErrnoException) => {
			if (error.code === "ENOENT") return [];
			throw error;
		},
	);
	return entries.filter((entry) => entry.isFile()).map((entry) => join(entry.parentPath, entry.name));
}

function expectNoSecrets(output: string, additional: string[] = []): void {
	for (const secret of [...SECRETS, ...additional]) expect(output).not.toContain(secret);
}

function envOptions(home: string, env: NodeJS.ProcessEnv = fullEnv) {
	return {
		home,
		env: { GAJAEWAY_HOME: home, ...env },
		fromEnv: true,
		renderer: new MockRenderer(),
		input: new MockKeyboardInput(),
		fetcher: fakePlatform().fetch,
	};
}

describe("setup wizard", () => {
	test("Escape cancels selection without writing and restores terminal state", async () => {
		await withHome(async (home) => {
			const input = new MockKeyboardInput(["escape"]);
			const renderer = new MockRenderer();
			const platform = fakePlatform();
			const result = await runSetupWizard({
				home,
				env: { GAJAEWAY_HOME: home },
				input,
				renderer,
				fetcher: platform.fetch,
			}).catch((error: Error) => error);
			if (result instanceof Error) expect(result.message.toLowerCase()).toContain("cancel");
			expect(input.keys).toEqual([]);
			expect(input.closed).toBe(true);
			expect(renderer.cursorVisible).toBe(true);
			expect(renderer.styleResets).toBeGreaterThan(0);
			expect(platform.calls).toEqual([]);
			expect(await files(home)).toEqual([]);
		});
	});

	test("interactive arrows select Telegram, credential text stays hidden, and Enter confirms", async () => {
		await withHome(async (home) => {
			const input = new MockKeyboardInput([
				"down",
				"down",
				"up",
				"down",
				" ",
				"enter",
				...TELEGRAM_TOKEN,
				"enter",
				..."owner-42",
				"enter",
				"enter",
				"enter",
			]);
			const renderer = new MockRenderer();
			await runSetupWizard({ home, env: { GAJAEWAY_HOME: home }, input, renderer, fetcher: fakePlatform().fetch });
			expect(input.keys).toEqual([]);
			expect(input.consumed).toContain("up");
			expect(input.closed).toBe(true);
			expect(renderer.cursorVisible).toBe(true);
			expect(renderer.styleResets).toBeGreaterThan(0);
			expect((await readFile(join(home, "secrets/telegram-token"), "utf8")).trim()).toBe(TELEGRAM_TOKEN);
			expect(JSON.parse(await readFile(join(home, "config.json"), "utf8")).mentionAllowlist).toEqual(["owner-42"]);
			expect(await files(home)).toHaveLength(3);
			expectNoSecrets(renderer.output);
			for (const frame of renderer.frames) expectNoSecrets(frame);
		});
	});

	test("interactive validation retries a network error without re-entering credentials", async () => {
		await withHome(async (home) => {
			const input = new MockKeyboardInput([
				"down",
				"down",
				" ",
				"enter",
				...TELEGRAM_TOKEN,
				"enter",
				"enter",
				"enter",
				"enter",
				"enter",
			]);
			const renderer = new MockRenderer();
			const platform = fakePlatform();
			let attempts = 0;
			const fetcher: FetchLike = async (url, init) => {
				attempts++;
				if (attempts === 1) throw new Error(`Connection reset: ${url}`);
				return platform.fetch(url, init);
			};
			await runSetupWizard({ home, env: { GAJAEWAY_HOME: home }, input, renderer, fetcher });
			expect(attempts).toBe(2);
			expect(input.keys).toEqual([]);
			expect(input.closed).toBe(true);
			expect((await readFile(join(home, "secrets/telegram-token"), "utf8")).trim()).toBe(TELEGRAM_TOKEN);
			expect(renderer.output).toContain("Retry");
			expectNoSecrets(renderer.output);
		});
	});

	test("sets up Discord alone from environment without keyboard prompts", async () => {
		await withHome(async (home) => {
			const options = envOptions(home, { [SETUP_ENV.discordToken]: DISCORD_TOKEN });
			await runSetupWizard(options);
			expect((await readFile(join(home, "secrets/discord-token"), "utf8")).trim()).toBe(DISCORD_TOKEN);
			expect(JSON.parse(await readFile(join(home, "adapter-discord.json"), "utf8"))).toEqual({
				tokenFile: "secrets/discord-token",
			});
			expect(await files(home)).toHaveLength(3);
			expect(options.input.consumed).toEqual([]);
			expectNoSecrets(options.renderer.output);
		});
	});

	test("sets up all adapters using their authenticated API endpoints", async () => {
		await withHome(async (home) => {
			const platform = fakePlatform();
			const options = { ...envOptions(home), fetcher: platform.fetch };
			await runSetupWizard(options);
			const config = JSON.parse(await readFile(join(home, "config.json"), "utf8"));
			expect(Object.keys(config.credentials).sort()).toEqual(["discord", "slack-app", "slack-bot", "telegram"]);
			expect(platform.calls).toHaveLength(5);
			for (const [name, token] of [
				["discord-token", DISCORD_TOKEN],
				["slack-bot-token", SLACK_BOT],
				["slack-app-token", SLACK_APP],
				["telegram-token", TELEGRAM_TOKEN],
			]) {
				expect((await readFile(join(home, "secrets", name as string), "utf8")).trim()).toBe(token);
			}
			expectNoSecrets(options.renderer.output);
		});
	});

	test("rejects an invalid token without writing files or disclosing it", async () => {
		await withHome(async (home) => {
			const invalid = "invalid-discord-token-secret";
			const options = envOptions(home, { [SETUP_ENV.discordToken]: invalid });
			const error = await runSetupWizard(options).catch((caught: Error) => caught);
			expect(error).toBeInstanceOf(Error);
			expect((error as Error).message).toContain("rejected");
			expect(await files(home)).toEqual([]);
			expectNoSecrets(`${options.renderer.output}${(error as Error).message}`, [invalid]);
		});
	});

	test("redacts tokens embedded in network errors and request URLs", async () => {
		await withHome(async (home) => {
			const options = {
				...envOptions(home, { [SETUP_ENV.telegramToken]: TELEGRAM_TOKEN }),
				fetcher: async (url: string): Promise<Response> => {
					throw new Error(`Cannot connect to ${url}`);
				},
			};
			const error = await runSetupWizard(options).catch((caught: Error) => caught);
			expect(error).toBeInstanceOf(Error);
			expect((error as Error).message).toContain("Cannot connect");
			expect((error as Error).message).toContain("[redacted]");
			expectNoSecrets(`${options.renderer.output}${(error as Error).message}`);
			expect(await files(home)).toEqual([]);
		});
	});

	test("does not partially write valid adapters when a later adapter is rejected", async () => {
		await withHome(async (home) => {
			const invalid = "xoxb-invalid-private-token";
			const options = envOptions(home, { ...fullEnv, [SETUP_ENV.slackBotToken]: invalid });
			const error = await runSetupWizard(options).catch((caught: Error) => caught);
			expect(error).toBeInstanceOf(Error);
			expect((error as Error).message).toContain("slack");
			expect(await files(home)).toEqual([]);
			expectNoSecrets(`${options.renderer.output}${(error as Error).message}`, [invalid]);
		});
	});

	test("writes every credential and configuration file with mode 0600", async () => {
		await withHome(async (home) => {
			await runSetupWizard(envOptions(home));
			const written = await files(home);
			expect(written).toHaveLength(8);
			for (const path of written) expect((await stat(path)).mode & 0o777).toBe(0o600);
		});
	});

	test("does not place secrets in any rendered frame or non-secret file", async () => {
		await withHome(async (home) => {
			const options = envOptions(home);
			await runSetupWizard(options);
			expect(options.renderer.frames.length).toBeGreaterThan(0);
			for (const frame of options.renderer.frames) expectNoSecrets(frame);
			for (const path of await files(home)) {
				if (!path.startsWith(`${join(home, "secrets")}/`)) expectNoSecrets(await readFile(path, "utf8"));
			}
		});
	});
});
