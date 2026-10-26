import { stat } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import {
	applySetup,
	type FetchLike,
	type ParsedSetupArgs,
	redact,
	SECRET_FILES,
	SETUP_ENV,
	SETUP_PLATFORMS,
	SetupError,
	type SetupPlatform,
	type SetupTokens,
	secretsOf,
	tokensFromEnv,
	type ValidatedIdentity,
	validateAll,
} from "../../cli/src/setup";
import { ProgressBar, SelectInput, TextInput } from "./tui-components";
import { ANSIRenderer, type Color, KeyboardInput } from "./tui-core";

export interface SetupWizardOptions {
	readonly home?: string;
	readonly renderer?: Pick<ANSIRenderer, "clear" | "moveCursor" | "write" | "resetStyle" | "hideCursor" | "showCursor">;
	readonly input?: Pick<KeyboardInput, "readKey" | "close">;
	/** Socket used in the suggested status command; setup itself does not contact a daemon. */
	readonly socket?: string;
	readonly fetcher?: FetchLike;
	readonly fromEnv?: boolean;
	readonly env?: NodeJS.ProcessEnv;
	readonly parsed?: ParsedSetupArgs;
}

const list = (value: string): string[] =>
	value
		.split(",")
		.map((part) => part.trim())
		.filter(Boolean);
const shellQuote = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;

/** Collect, validate, and persist credentials without exposing them in terminal output. */
export async function runSetupWizard(options: SetupWizardOptions = {}): Promise<void> {
	const env = options.env ?? process.env;
	const home = options.home ?? env.GAJAEWAY_HOME ?? join(env.HOME ?? homedir(), ".gajaeway");
	const renderer = options.renderer ?? new ANSIRenderer();
	const fromEnv = options.fromEnv ?? options.parsed?.fromEnv ?? false;
	let input = options.input;
	const secrets = new Set<string>();
	const safe = (text: string): string => {
		let result = redact(text, secrets);
		for (const secret of [...secrets].sort((a, b) => b.length - a.length)) {
			if (secret) result = result.split(secret).join("[redacted]");
		}
		return [...result]
			.filter((character) => {
				const code = character.codePointAt(0) ?? 0;
				return code === 10 || (code >= 32 && !(code >= 127 && code <= 159));
			})
			.join("");
	};
	for (const name of [
		SETUP_ENV.discordToken,
		SETUP_ENV.slackBotToken,
		SETUP_ENV.slackAppToken,
		SETUP_ENV.telegramToken,
	]) {
		const value = env[name]?.trim();
		if (value) secrets.add(value);
	}
	const write = (text: string, color?: Color): void => renderer.write(safe(text), color);
	const screen = (title: string, instructions: string): void => {
		renderer.clear();
		write(`Gajaeway Setup — ${title}\n`, "cyan");
		write(`${instructions}\n\n`);
	};
	const key = async () => {
		if (!input) throw new SetupError("interactive setup needs a terminal; use --from-env instead");
		const event = await input.readKey();
		if (event.key === "escape" || (event.isCtrl && event.key.toLowerCase() === "c"))
			throw new SetupError("setup cancelled; no further changes were made");
		return event;
	};
	const choose = async <T extends string>(
		title: string,
		instructions: string,
		items: readonly { label: string; value: T }[],
	): Promise<T> => {
		const selection = new SelectInput({ items });
		for (;;) {
			screen(title, instructions);
			selection.render(renderer, { row: instructions.split("\n").length + 3, col: 1 });
			const event = await key();
			if (event.key === "enter") {
				const value = selection.getValue();
				if (value !== undefined) return value;
			}
			selection.handleKey(event);
		}
	};
	const ask = async (label: string, hidden = false, optional = false): Promise<string> => {
		const field = new TextInput({ label, hidden, maxLength: 4096 });
		for (;;) {
			screen(
				"Credentials and access",
				`${optional ? "Optional; leave blank to skip. " : ""}Enter to continue; Escape to cancel.`,
			);
			field.render(renderer, { row: 4, col: 1 });
			const event = await key();
			if (event.key === "enter") {
				const value = field.getValue().trim();
				if (value || optional) {
					if (hidden && value) secrets.add(value);
					return value;
				}
			} else field.handleKey(event);
		}
	};
	let spinner: ReturnType<typeof setInterval> | undefined;
	try {
		if (!fromEnv && !input) {
			if (!process.stdin.isTTY) throw new SetupError("interactive setup needs a terminal; use --from-env instead");
			input = new KeyboardInput();
		}
		renderer.hideCursor();
		screen("Welcome", "Checking configuration...");
		const frames = ["|", "/", "-", "\\"];
		let frame = 0;
		const spin = () => {
			renderer.moveCursor(4, 1);
			write(`${frames[frame++ % frames.length]} Checking configuration`, "yellow");
		};
		spin();
		spinner = setInterval(spin, 100);
		let existing = false;
		try {
			await stat(join(home, "config.json"));
			existing = true;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}
		clearInterval(spinner);
		spinner = undefined;
		let fresh = false;
		if (existing && !fromEnv)
			fresh =
				(await choose(
					"Existing configuration",
					"Update existing or start fresh? Fresh replaces adapter configuration after confirmation.",
					[
						{ label: "Update existing", value: "update" },
						{ label: "Start fresh", value: "fresh" },
					],
				)) === "fresh";
		let adapters: readonly SetupPlatform[] = options.parsed?.adapters ?? [];
		if (!fromEnv && !options.parsed?.adapters) {
			const selection = new SelectInput<SetupPlatform>({
				multiSelect: true,
				items: SETUP_PLATFORMS.map((value) => ({ label: value[0]?.toUpperCase() + value.slice(1), value })),
			});
			for (;;) {
				screen("Select adapters", "Arrow keys navigate; Space toggles; Enter continues. Select at least one adapter.");
				selection.render(renderer, { row: 4, col: 1 });
				const event = await key();
				if (event.key === "enter" && selection.getSelectedValues().length > 0) {
					adapters = selection.getSelectedValues();
					break;
				}
				selection.handleKey(event);
			}
		}
		const collect = async (): Promise<SetupTokens> => {
			const tokens: { -readonly [K in keyof SetupTokens]: SetupTokens[K] } = {};
			for (const adapter of SETUP_PLATFORMS) {
				if (!adapters.includes(adapter)) continue;
				if (adapter === "discord") {
					const token = await ask("Discord bot token:", true);
					const appId = options.parsed?.discordAppId ?? (await ask("Discord application ID:", false, true));
					tokens.discord = { token, ...(appId ? { appId } : {}) };
				} else if (adapter === "slack")
					tokens.slack = {
						botToken: await ask("Slack bot token:", true),
						appToken: await ask("Slack app token:", true),
					};
				else tokens.telegram = { token: await ask("Telegram bot token:", true) };
			}
			return tokens;
		};
		let tokens = fromEnv ? tokensFromEnv(env, options.parsed?.adapters, options.parsed?.discordAppId) : await collect();
		let identities: ValidatedIdentity[];
		for (;;) {
			for (const secret of secretsOf(tokens)) secrets.add(secret);
			const platforms = SETUP_PLATFORMS.filter((platform) => tokens[platform]);
			const total = platforms.reduce((count, platform) => count + (platform === "telegram" ? 1 : 2), 0);
			const progress = new ProgressBar({ label: "Validating tokens", width: 24 });
			let completed = 0;
			screen("Validate credentials", "Checking platform APIs. Nothing is written until all credentials pass.");
			progress.render(renderer, { row: 4, col: 1 });
			try {
				identities = await validateAll(tokens, async (url, init) => {
					const response = await (options.fetcher ?? fetch)(url, init);
					progress.setValue((++completed / total) * 100);
					progress.render(renderer, { row: 4, col: 1 });
					return response;
				});
				progress.setValue(100);
				progress.render(renderer, { row: 4, col: 1 });
				write("\nAll selected credentials validated.\n", "green");
				break;
			} catch (error) {
				const message = safe(error instanceof Error ? error.message : String(error));
				write(`\n${message}\n`, "red");
				if (fromEnv) throw error;
				const action = await choose(
					"Validation failed",
					`${message}\nCheck your network and token permissions, then retry.`,
					[
						{ label: "Retry validation", value: "retry" },
						{ label: "Re-enter credentials", value: "edit" },
						{ label: "Cancel", value: "cancel" },
					],
				);
				if (action === "cancel") throw new SetupError("setup cancelled; configuration was not written");
				if (action === "edit") tokens = await collect();
			}
		}
		const owners = [
			...(options.parsed?.owners ?? []),
			...list(
				fromEnv
					? (env[SETUP_ENV.ownerIds] ?? "")
					: options.parsed?.owners.length
						? ""
						: await ask("Owner user IDs (comma-separated):", false, true),
			),
		];
		if (owners.length === 0) {
			const owner = identities.find((identity) => identity.ownerId)?.ownerId;
			if (owner) owners.push(owner);
		}
		const names = SETUP_PLATFORMS.filter((platform) => tokens[platform]);
		const paths = [
			"config.json",
			...names.map((name) => `adapter-${name}.json`),
			...names
				.flatMap((name) => (name === "slack" ? [SECRET_FILES.slackBot, SECRET_FILES.slackApp] : [SECRET_FILES[name]]))
				.map((name) => `secrets/${name}`),
		];
		const summary = `${fresh ? "Replace" : "Update"} configuration in ${home}\n${paths.join("\n")}\nOwners: ${owners.length ? owners.join(", ") : "none — DMs will be declined"}\nSecrets are stored privately (0600); their values are never shown.`;
		if (
			!fromEnv &&
			(await choose("Summary", summary, [
				{ label: "Write configuration", value: "write" },
				{ label: "Cancel", value: "cancel" },
			])) === "cancel"
		)
			throw new SetupError("setup cancelled; configuration was not written");
		screen("Write configuration", summary);
		const applied = await applySetup({ home, tokens, owners, fresh });
		for (const path of applied.written) write(`Wrote ${path}\n`, "green");
		if (applied.owners.length === 0)
			write("Warning: no owner IDs configured. Rerun setup with --owner ID to allow DMs.\n", "yellow");
		write("Setup complete.\n", "green");
		if (!fromEnv) {
			const action = await choose(
				"Next steps",
				"Configuration is saved. Choose instructions; no services are started automatically.",
				[
					{ label: "Finish", value: "finish" },
					{ label: "Register as a service", value: "service" },
					{ label: "Start daemon", value: "daemon" },
				],
			);
			screen("Complete", "Configuration saved successfully.");
			const prefix = `GAJAEWAY_HOME=${shellQuote(home)} `;
			if (action === "service")
				write(
					`${prefix}gajaeway services install --bin-dir /usr/local/bin\nReplace /usr/local/bin with your installed binary directory.\n`,
				);
			if (action === "daemon") {
				write(`${prefix}gajaeway-gateway daemon\n`);
				if (options.socket) write(`Check status: gajaeway --socket ${shellQuote(options.socket)} status\n`);
			}
			write("Setup complete.\n", "green");
		}
	} catch (error) {
		const code = (error as NodeJS.ErrnoException)?.code;
		let message = error instanceof Error ? error.message : String(error);
		if (code === "EACCES" || code === "EPERM")
			message +=
				"\nPermission denied: use a writable GAJAEWAY_HOME or repair its ownership with sudo chown. Avoid running setup as root.";
		message = safe(message);
		write(`\nSetup failed: ${message}\n`, "red");
		throw new SetupError(message);
	} finally {
		if (spinner) clearInterval(spinner);
		try {
			input?.close();
		} finally {
			renderer.resetStyle();
			renderer.showCursor();
		}
	}
}
