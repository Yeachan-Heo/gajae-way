import { randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

/**
 * `gajaeway setup`: guided credential setup for the platform adapters.
 *
 * Invariants, each pinned by tests:
 * - a token is validated against its platform API before anything is written;
 *   one rejected token writes nothing at all, so a half-configured home never
 *   exists;
 * - tokens are only ever read from the environment or a no-echo prompt, never
 *   from argv (argv is visible in `ps` and shell history);
 * - secret files are written 0600 inside a 0700 `secrets/` directory, atomically;
 * - no message this module produces contains a token: every error passes
 *   through `redact` with every secret collected so far.
 */

export const SETUP_PLATFORMS = ["discord", "slack", "telegram"] as const;
export type SetupPlatform = (typeof SETUP_PLATFORMS)[number];

/** Environment variables `--from-env` reads. Tokens have no flag spelling on purpose. */
export const SETUP_ENV = {
	discordToken: "GAJAEWAY_DISCORD_TOKEN",
	discordAppId: "GAJAEWAY_DISCORD_APP_ID",
	slackBotToken: "GAJAEWAY_SLACK_BOT_TOKEN",
	slackAppToken: "GAJAEWAY_SLACK_APP_TOKEN",
	telegramToken: "GAJAEWAY_TELEGRAM_TOKEN",
	ownerIds: "GAJAEWAY_OWNER_IDS",
} as const;

export const SETUP_USAGE =
	"usage: gajaeway setup [--from-env] [--adapters discord,slack,telegram] [--owner ID[,ID...]] [--discord-app-id ID] | setup --status";

/** Secret file names under `$GAJAEWAY_HOME/secrets`, matching docs/deployment.md. */
export const SECRET_FILES = {
	discord: "discord-token",
	slackBot: "slack-bot-token",
	slackApp: "slack-app-token",
	telegram: "telegram-token",
} as const;

export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

export class SetupError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "SetupError";
	}
}

export const REDACTED = "[redacted]";

/** Replaces every occurrence of every secret. Short strings are skipped so a stray "a" cannot shred output. */
export function redact(text: string, secrets: Iterable<string>): string {
	let out = text;
	for (const secret of secrets) if (secret.length >= 8) out = out.split(secret).join(REDACTED);
	return out;
}

export interface ParsedSetupArgs {
	readonly mode: "setup" | "status";
	readonly fromEnv: boolean;
	readonly adapters?: readonly SetupPlatform[];
	readonly owners: readonly string[];
	readonly discordAppId?: string;
}

export function parseSetupArgs(args: readonly string[]): ParsedSetupArgs {
	let fromEnv = false;
	let status = false;
	let adapters: SetupPlatform[] | undefined;
	const owners: string[] = [];
	let discordAppId: string | undefined;
	for (let i = 0; i < args.length; i++) {
		const flag = args[i] as string;
		if (flag === "--from-env") fromEnv = true;
		else if (flag === "--status") status = true;
		else if (flag === "--adapters" || flag === "--owner" || flag === "--discord-app-id") {
			const value = args[++i];
			if (value === undefined || value.trim() === "" || value.startsWith("--"))
				throw new SetupError(`${flag} expects a value\n${SETUP_USAGE}`);
			if (flag === "--adapters") adapters = parseAdapterList(value);
			else if (flag === "--owner") owners.push(...splitList(value));
			else discordAppId = value.trim();
		} else if (/token/i.test(flag)) {
			throw new SetupError(
				`${flag.split("=")[0]}: tokens are never accepted as arguments (argv is visible to every local user); export ${SETUP_ENV.discordToken}/${SETUP_ENV.slackBotToken}/${SETUP_ENV.slackAppToken}/${SETUP_ENV.telegramToken} and pass --from-env, or run setup interactively`,
			);
		} else throw new SetupError(`unknown option: ${flag.split("=")[0]}\n${SETUP_USAGE}`);
	}
	if (status && (fromEnv || adapters || owners.length > 0 || discordAppId))
		throw new SetupError(`--status takes no other options\n${SETUP_USAGE}`);
	return {
		mode: status ? "status" : "setup",
		fromEnv,
		...(adapters ? { adapters } : {}),
		owners,
		...(discordAppId ? { discordAppId } : {}),
	};
}

function splitList(value: string): string[] {
	return value
		.split(",")
		.map((part) => part.trim())
		.filter((part) => part.length > 0);
}

export function parseAdapterList(value: string): SetupPlatform[] {
	const list = splitList(value);
	if (list.length === 0) throw new SetupError("--adapters expects at least one of discord, slack, telegram");
	for (const name of list)
		if (!(SETUP_PLATFORMS as readonly string[]).includes(name))
			throw new SetupError(`unknown adapter: ${name} (expected discord, slack, or telegram)`);
	return [...new Set(list)] as SetupPlatform[];
}

/** Tokens collected for the adapters being (re)configured. */
export interface SetupTokens {
	readonly discord?: { readonly token: string; readonly appId?: string };
	readonly slack?: { readonly botToken: string; readonly appToken: string };
	readonly telegram?: { readonly token: string };
}

function nonEmpty(value: string | undefined): string | undefined {
	const trimmed = value?.trim();
	return trimmed ? trimmed : undefined;
}

/**
 * The `--from-env` token set. With `--adapters`, every named adapter must be
 * fully present; without it, every adapter whose variables are present is
 * enabled, and a half-present Slack pair is an error rather than a skip.
 */
export function tokensFromEnv(
	env: NodeJS.ProcessEnv,
	adapters: readonly SetupPlatform[] | undefined,
	discordAppId: string | undefined,
): SetupTokens {
	const discord = nonEmpty(env[SETUP_ENV.discordToken]);
	const slackBot = nonEmpty(env[SETUP_ENV.slackBotToken]);
	const slackApp = nonEmpty(env[SETUP_ENV.slackAppToken]);
	const telegram = nonEmpty(env[SETUP_ENV.telegramToken]);
	const appId = discordAppId ?? nonEmpty(env[SETUP_ENV.discordAppId]);
	const wanted = adapters ?? SETUP_PLATFORMS.filter((platform) => presentFor(platform));
	function presentFor(platform: SetupPlatform): boolean {
		if (platform === "discord") return discord !== undefined;
		if (platform === "telegram") return telegram !== undefined;
		return slackBot !== undefined || slackApp !== undefined;
	}
	if (wanted.length === 0)
		throw new SetupError(
			`--from-env found no adapter tokens; set ${SETUP_ENV.discordToken}, ${SETUP_ENV.slackBotToken} and ${SETUP_ENV.slackAppToken}, or ${SETUP_ENV.telegramToken}`,
		);
	const tokens: { -readonly [K in keyof SetupTokens]: SetupTokens[K] } = {};
	for (const platform of wanted) {
		if (platform === "discord") {
			if (!discord) throw new SetupError(`discord: ${SETUP_ENV.discordToken} is not set`);
			tokens.discord = { token: discord, ...(appId ? { appId } : {}) };
		} else if (platform === "slack") {
			if (!slackBot) throw new SetupError(`slack: ${SETUP_ENV.slackBotToken} is not set`);
			if (!slackApp) throw new SetupError(`slack: ${SETUP_ENV.slackAppToken} is not set (Socket Mode app-level token)`);
			tokens.slack = { botToken: slackBot, appToken: slackApp };
		} else {
			if (!telegram) throw new SetupError(`telegram: ${SETUP_ENV.telegramToken} is not set`);
			tokens.telegram = { token: telegram };
		}
	}
	return tokens;
}

export function secretsOf(tokens: SetupTokens): string[] {
	return [tokens.discord?.token, tokens.slack?.botToken, tokens.slack?.appToken, tokens.telegram?.token].filter(
		(value): value is string => value !== undefined,
	);
}

/** What each platform said about the credentials; never contains a token. */
export interface ValidatedIdentity {
	readonly platform: SetupPlatform;
	readonly summary: string;
	/** A user id the platform names as the app owner, when it names one. */
	readonly ownerId?: string;
}

async function requestJson(
	fetcher: FetchLike,
	platform: string,
	url: string,
	init: RequestInit,
	secrets: readonly string[],
): Promise<{ status: number; body: Record<string, unknown> }> {
	let response: Response;
	try {
		response = await fetcher(url, { ...init, signal: AbortSignal.timeout(15_000) });
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		throw new SetupError(`${platform}: could not reach the platform API (${redact(message, secrets)})`);
	}
	let body: unknown;
	try {
		body = await response.json();
	} catch {
		body = {};
	}
	return {
		status: response.status,
		body: typeof body === "object" && body !== null && !Array.isArray(body) ? (body as Record<string, unknown>) : {},
	};
}

/** Discord: the bot token must authenticate `/users/@me` as a bot and, when given, belong to the expected application. */
export async function validateDiscord(
	credentials: NonNullable<SetupTokens["discord"]>,
	fetcher: FetchLike,
): Promise<ValidatedIdentity> {
	const secrets = [credentials.token];
	const headers = { authorization: `Bot ${credentials.token}` };
	const me = await requestJson(fetcher, "discord", "https://discord.com/api/v10/users/@me", { headers }, secrets);
	if (me.status === 401)
		throw new SetupError(
			"discord: the platform rejected the bot token (HTTP 401); copy a fresh token from the Bot page",
		);
	if (me.status !== 200) throw new SetupError(`discord: token check failed with HTTP ${me.status}`);
	if (me.body.bot !== true)
		throw new SetupError("discord: the token authenticates a user account, not a bot; use the Bot page token");
	const app = await requestJson(
		fetcher,
		"discord",
		"https://discord.com/api/v10/applications/@me",
		{ headers },
		secrets,
	);
	if (app.status !== 200) throw new SetupError(`discord: application lookup failed with HTTP ${app.status}`);
	const appId = String(app.body.id ?? "");
	if (credentials.appId !== undefined && appId !== credentials.appId)
		throw new SetupError(`discord: the token belongs to application ${appId}, not ${credentials.appId}`);
	const owner = app.body.owner as { id?: unknown } | undefined;
	const hasTeam = app.body.team !== null && app.body.team !== undefined;
	const ownerId = !hasTeam && typeof owner?.id === "string" ? owner.id : undefined;
	return {
		platform: "discord",
		summary: `bot ${String(me.body.username ?? "?")} (${String(me.body.id ?? "?")}) of application ${appId}`,
		...(ownerId ? { ownerId } : {}),
	};
}

/**
 * Slack: the bot token must pass `auth.test` as a bot, and the app-level token
 * must be able to open a Socket Mode connection (`apps.connections.open`) —
 * the exact call the adapter makes at startup. The returned URL is discarded.
 */
export async function validateSlack(
	credentials: NonNullable<SetupTokens["slack"]>,
	fetcher: FetchLike,
): Promise<ValidatedIdentity> {
	if (!credentials.botToken.startsWith("xoxb-"))
		throw new SetupError("slack: the bot token must start with xoxb- (OAuth & Permissions > Bot User OAuth Token)");
	if (!credentials.appToken.startsWith("xapp-"))
		throw new SetupError(
			"slack: the app-level token must start with xapp- (Basic Information > App-Level Tokens, scope connections:write)",
		);
	const secrets = [credentials.botToken, credentials.appToken];
	const call = (method: string, token: string) =>
		requestJson(
			fetcher,
			"slack",
			`https://slack.com/api/${method}`,
			{ method: "POST", headers: { authorization: `Bearer ${token}` } },
			secrets,
		);
	const auth = await call("auth.test", credentials.botToken);
	if (auth.body.ok !== true)
		throw new SetupError(`slack: the platform rejected the bot token (${slackError(auth.body, auth.status)})`);
	if (typeof auth.body.bot_id !== "string")
		throw new SetupError("slack: the bot token does not authenticate a bot user");
	const socket = await call("apps.connections.open", credentials.appToken);
	if (socket.body.ok !== true)
		throw new SetupError(
			`slack: the app-level token cannot open a Socket Mode connection (${slackError(socket.body, socket.status)}); enable Socket Mode and grant connections:write`,
		);
	return {
		platform: "slack",
		summary: `bot ${String(auth.body.user ?? "?")} (${String(auth.body.user_id ?? "?")}) in workspace ${String(auth.body.team ?? "?")}`,
	};
}

function slackError(body: Record<string, unknown>, status: number): string {
	// Slack error codes are fixed identifiers such as invalid_auth; still bounded to that shape.
	const code = typeof body.error === "string" && /^[a-z_]{1,64}$/.test(body.error) ? body.error : `HTTP ${status}`;
	return code;
}

/** Telegram: `getMe` with the token. The token is part of the URL, so fetch errors are redacted. */
export async function validateTelegram(
	credentials: NonNullable<SetupTokens["telegram"]>,
	fetcher: FetchLike,
): Promise<ValidatedIdentity> {
	const me = await requestJson(fetcher, "telegram", `https://api.telegram.org/bot${credentials.token}/getMe`, {}, [
		credentials.token,
	]);
	const result = me.body.result as { username?: unknown; id?: unknown } | undefined;
	if (me.body.ok !== true || !result)
		throw new SetupError(`telegram: the platform rejected the bot token (HTTP ${me.status})`);
	return { platform: "telegram", summary: `bot @${String(result.username ?? "?")} (${String(result.id ?? "?")})` };
}

export async function validateAll(tokens: SetupTokens, fetcher: FetchLike): Promise<ValidatedIdentity[]> {
	const secrets = secretsOf(tokens);
	const identities: ValidatedIdentity[] = [];
	try {
		if (tokens.discord) identities.push(await validateDiscord(tokens.discord, fetcher));
		if (tokens.slack) identities.push(await validateSlack(tokens.slack, fetcher));
		if (tokens.telegram) identities.push(await validateTelegram(tokens.telegram, fetcher));
	} catch (error) {
		// Last line of defence: whatever escaped a validator is scrubbed of every token.
		const message = error instanceof Error ? error.message : String(error);
		throw new SetupError(redact(message, secrets));
	}
	return identities;
}

/** Atomic 0600 write: a temp file created 0600, then renamed over the target. */
export async function writePrivateFile(path: string, contents: string): Promise<void> {
	const temporary = `${path}.${randomUUID()}.tmp`;
	try {
		await writeFile(temporary, contents, { flag: "wx", mode: 0o600 });
		await chmod(temporary, 0o600);
		await rename(temporary, path);
	} finally {
		await rm(temporary, { force: true });
	}
}

async function readJsonObject(path: string): Promise<Record<string, unknown> | undefined> {
	let text: string;
	try {
		text = await readFile(path, "utf8");
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
		throw new SetupError(`cannot read ${path}: ${(error as NodeJS.ErrnoException).code ?? "unreadable"}`);
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch {
		throw new SetupError(`${path} is not valid JSON; fix or remove it, then run setup again`);
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
		throw new SetupError(`${path} must contain a JSON object`);
	return parsed as Record<string, unknown>;
}

export interface ApplySetupOptions {
	readonly home: string;
	readonly tokens: SetupTokens;
	readonly owners: readonly string[];
}

export interface AppliedSetup {
	readonly written: readonly string[];
	readonly owners: readonly string[];
}

/**
 * Writes secrets, adapter configs, and the gateway config. Existing files are
 * merged, not replaced: re-running setup for one adapter (token rotation)
 * leaves the other adapters and every unrelated config field untouched.
 * All inputs are read and parsed before the first write.
 */
export async function applySetup(options: ApplySetupOptions): Promise<AppliedSetup> {
	const { home, tokens } = options;
	const secretsDir = join(home, "secrets");
	const configPath = join(home, "config.json");
	const adapterPath = (platform: SetupPlatform) => join(home, `adapter-${platform}.json`);
	const existingConfig = await readJsonObject(configPath);
	const existingAdapters = new Map<SetupPlatform, Record<string, unknown> | undefined>();
	for (const platform of SETUP_PLATFORMS)
		if (tokens[platform]) existingAdapters.set(platform, await readJsonObject(adapterPath(platform)));

	await mkdir(home, { recursive: true, mode: 0o700 });
	await chmod(home, 0o700);
	await mkdir(secretsDir, { recursive: true, mode: 0o700 });
	await chmod(secretsDir, 0o700);

	const written: string[] = [];
	const secret = async (name: string, value: string) => {
		const path = join(secretsDir, name);
		await writePrivateFile(path, `${value}\n`);
		written.push(path);
		return path;
	};
	const credentials: Record<string, { credentialFile: string }> = {};
	const adapterConfigs: [SetupPlatform, Record<string, unknown>][] = [];
	if (tokens.discord) {
		credentials.discord = { credentialFile: await secret(SECRET_FILES.discord, tokens.discord.token) };
		adapterConfigs.push(["discord", { tokenFile: `secrets/${SECRET_FILES.discord}` }]);
	}
	if (tokens.slack) {
		credentials["slack-bot"] = { credentialFile: await secret(SECRET_FILES.slackBot, tokens.slack.botToken) };
		credentials["slack-app"] = { credentialFile: await secret(SECRET_FILES.slackApp, tokens.slack.appToken) };
		adapterConfigs.push([
			"slack",
			{ botTokenFile: `secrets/${SECRET_FILES.slackBot}`, appTokenFile: `secrets/${SECRET_FILES.slackApp}` },
		]);
	}
	if (tokens.telegram) {
		credentials.telegram = { credentialFile: await secret(SECRET_FILES.telegram, tokens.telegram.token) };
		adapterConfigs.push(["telegram", { tokenFile: `secrets/${SECRET_FILES.telegram}` }]);
	}
	for (const [platform, references] of adapterConfigs) {
		const path = adapterPath(platform);
		await writePrivateFile(
			path,
			`${JSON.stringify({ ...existingAdapters.get(platform), ...references }, null, "\t")}\n`,
		);
		written.push(path);
	}

	const base = existingConfig ?? { schemaVersion: 1 };
	const previousCredentials =
		typeof base.credentials === "object" && base.credentials !== null && !Array.isArray(base.credentials)
			? (base.credentials as Record<string, unknown>)
			: {};
	const previousAllowlist = Array.isArray(base.mentionAllowlist)
		? base.mentionAllowlist.filter((id): id is string => typeof id === "string")
		: [];
	const owners = [...new Set([...previousAllowlist, ...options.owners])];
	const config = {
		...base,
		credentials: { ...previousCredentials, ...credentials },
		...(owners.length > 0 ? { mentionAllowlist: owners } : {}),
	};
	await writePrivateFile(configPath, `${JSON.stringify(config, null, "\t")}\n`);
	written.push(configPath);
	return { written, owners };
}

/** Adapters with a config file in `home`, in canonical order: the compose profiles to run. */
export async function enabledAdapters(home: string): Promise<SetupPlatform[]> {
	const enabled: SetupPlatform[] = [];
	for (const platform of SETUP_PLATFORMS) {
		if (await Bun.file(join(home, `adapter-${platform}.json`)).exists()) enabled.push(platform);
	}
	return enabled;
}

export interface Prompter {
	ask(question: string): Promise<string>;
	/** Reads a line without echoing it. */
	askSecret(question: string): Promise<string>;
	close(): void;
}

async function askYes(prompter: Prompter, question: string): Promise<boolean> {
	for (;;) {
		const answer = (await prompter.ask(`${question} [y/N] `)).trim().toLowerCase();
		if (answer === "" || answer === "n" || answer === "no") return false;
		if (answer === "y" || answer === "yes") return true;
	}
}

async function askRequiredSecret(prompter: Prompter, question: string): Promise<string> {
	for (;;) {
		const answer = (await prompter.askSecret(question)).trim();
		if (answer) return answer;
	}
}

/** Interactive collection. Tokens are read without echo and never repeated back. */
export async function tokensFromPrompts(
	prompter: Prompter,
	parsed: ParsedSetupArgs,
): Promise<{ tokens: SetupTokens; owners: string[] }> {
	const wanted = parsed.adapters ?? [];
	const enable = async (platform: SetupPlatform, label: string) =>
		parsed.adapters ? wanted.includes(platform) : await askYes(prompter, `Enable ${label}?`);
	const tokens: { -readonly [K in keyof SetupTokens]: SetupTokens[K] } = {};
	if (await enable("discord", "Discord")) {
		const token = await askRequiredSecret(prompter, "Discord bot token (input hidden): ");
		const appId = parsed.discordAppId ?? (await prompter.ask("Discord application id (optional): ")).trim();
		tokens.discord = { token, ...(appId ? { appId } : {}) };
	}
	if (await enable("slack", "Slack (Socket Mode)")) {
		const botToken = await askRequiredSecret(prompter, "Slack bot token xoxb-… (input hidden): ");
		const appToken = await askRequiredSecret(prompter, "Slack app-level token xapp-… (input hidden): ");
		tokens.slack = { botToken, appToken };
	}
	if (await enable("telegram", "Telegram")) {
		tokens.telegram = { token: await askRequiredSecret(prompter, "Telegram bot token (input hidden): ") };
	}
	if (!tokens.discord && !tokens.slack && !tokens.telegram) throw new SetupError("no adapter selected; nothing to do");
	const owners =
		parsed.owners.length > 0
			? [...parsed.owners]
			: splitList(
					await prompter.ask(
						"Your platform user id(s), comma-separated, allowed to DM the bot (blank: Discord app owner only): ",
					),
				);
	return { tokens, owners };
}

/** Terminal prompter: raw-mode, no-echo secret entry. Requires a TTY. */
export function terminalPrompter(): Prompter {
	const stdin = process.stdin;
	if (!stdin.isTTY)
		throw new SetupError(
			"interactive setup needs a terminal; run it with a TTY (docker compose run -it …) or use --from-env",
		);
	const readLine = (question: string, echo: boolean): Promise<string> =>
		new Promise((resolve, reject) => {
			process.stdout.write(question);
			let line = "";
			stdin.setRawMode(true);
			stdin.resume();
			const onData = (chunk: Buffer) => {
				for (const char of chunk.toString("utf8")) {
					if (char === "\u0003") {
						finish();
						process.stdout.write("\n");
						reject(new SetupError("setup cancelled"));
						return;
					}
					if (char === "\r" || char === "\n") {
						finish();
						process.stdout.write("\n");
						resolve(line);
						return;
					}
					if (char === "\u007f" || char === "\b") {
						if (line.length > 0) {
							line = line.slice(0, -1);
							if (echo) process.stdout.write("\b \b");
						}
						continue;
					}
					line += char;
					if (echo) process.stdout.write(char);
				}
			};
			const finish = () => {
				stdin.off("data", onData);
				stdin.setRawMode(false);
				stdin.pause();
			};
			stdin.on("data", onData);
		});
	return {
		ask: (question) => readLine(question, true),
		askSecret: (question) => readLine(question, false),
		close: () => stdin.pause(),
	};
}

export interface RunSetupOptions {
	readonly home: string;
	readonly env?: NodeJS.ProcessEnv;
	readonly fetch?: FetchLike;
	readonly prompter?: () => Prompter;
	readonly log?: (line: string) => void;
}

/** The whole command. Returns the lines it printed so tests can assert nothing secret escaped. */
export async function runSetup(args: readonly string[], options: RunSetupOptions): Promise<void> {
	const log = options.log ?? ((line: string) => console.log(line));
	const parsed = parseSetupArgs(args);
	if (parsed.mode === "status") {
		log((await enabledAdapters(options.home)).join(","));
		return;
	}
	let tokens: SetupTokens;
	let owners: string[];
	if (parsed.fromEnv) {
		tokens = tokensFromEnv(options.env ?? process.env, parsed.adapters, parsed.discordAppId);
		owners = [...parsed.owners, ...splitList((options.env ?? process.env)[SETUP_ENV.ownerIds] ?? "")];
	} else {
		const prompter = (options.prompter ?? terminalPrompter)();
		try {
			({ tokens, owners } = await tokensFromPrompts(prompter, parsed));
		} finally {
			prompter.close();
		}
	}
	const secrets = secretsOf(tokens);
	try {
		const identities = await validateAll(tokens, options.fetch ?? ((input, init) => fetch(input, init)));
		for (const identity of identities) log(`${identity.platform}: token valid — ${identity.summary}`);
		for (const identity of identities)
			if (identity.ownerId && owners.length === 0) {
				owners.push(identity.ownerId);
				log(`${identity.platform}: allowing DMs from the application owner ${identity.ownerId}`);
			}
		const applied = await applySetup({ home: options.home, tokens, owners });
		for (const path of applied.written) log(`wrote ${path}`);
		if (applied.owners.length === 0)
			log(
				"warning: no owner id configured; the gateway declines every DM until config.json mentionAllowlist names you (rerun setup with --owner ID)",
			);
		log(`enabled adapters: ${(await enabledAdapters(options.home)).join(",")}`);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		throw new SetupError(redact(message, secrets));
	}
}
