import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { parseRuntimeConfig, type RuntimeConfig, RuntimeConfigError } from "@gajaeway/protocol";

const CONFIG_SCHEMA_VERSION = 1;

export const FALLBACK_PATH_DIRS = ["/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"] as const;
const LOGIN_PATH_COMMAND = 'printf %s "$PATH"';

export type LoginPathRunner = (command: readonly string[]) => string | PromiseLike<string>;
export type PlistWriter = (path: string, contents: string) => void | PromiseLike<void>;

export interface ResolvePathOptions {
	readonly binDir: string;
	/** The operator home used for managed PATH entries and `~/` expansion. */
	readonly home: string;
	readonly runtime?: RuntimeConfig;
	readonly env?: NodeJS.ProcessEnv;
	/** A supplied PATH bypasses shell discovery and is useful for pure tests. */
	readonly loginPath?: string;
	/** Test seam for the `${SHELL} -lc` invocation used by the default path. */
	readonly loginPathRunner?: LoginPathRunner;
}

export interface InstallServicesOptions {
	readonly binDir: string;
	readonly launchAgentsDir?: string;
	readonly env?: NodeJS.ProcessEnv;
	/** Test seam for the `${SHELL} -lc` invocation used by the default path. */
	readonly loginPathRunner?: LoginPathRunner;
	/** Test seam for counting or inspecting the three plist writes. */
	readonly writeFile?: PlistWriter;
}

interface ServiceSpec {
	readonly id: string;
	readonly label: string;
	readonly binary: string;
	readonly args: readonly string[];
}

const SERVICE_SPECS: readonly ServiceSpec[] = [
	{ id: "gateway", label: "dev.gajaeway.gateway", binary: "gajaeway-gateway", args: ["daemon"] },
	{
		id: "adapter-discord",
		label: "dev.gajaeway.adapter-discord",
		binary: "gajaeway-discord",
		args: [],
	},
	{ id: "admin", label: "dev.gajaeway.admin", binary: "gajaeway-admin", args: ["serve"] },
];

function homeForEnvironment(env: NodeJS.ProcessEnv): string {
	return env.GAJAEWAY_HOME || join(env.HOME || homedir(), ".gajaeway");
}

function expandHome(value: string, home: string): string {
	return value.startsWith("~/") ? join(home, value.slice(2)) : value;
}

function normalizedEntries(entries: readonly string[], home: string): string[] {
	const seen = new Set<string>();
	const result: string[] = [];
	for (const raw of entries) {
		if (raw.length === 0) continue;
		const entry = expandHome(raw, home);
		if (seen.has(entry)) continue;
		seen.add(entry);
		result.push(entry);
	}
	return result;
}

export function mergePathEntries(binDir: string, userHome: string, loginPath: string): string[] {
	return normalizedEntries(
		[
			expandHome(binDir, userHome),
			join(userHome, "bin"),
			join(userHome, ".local", "bin"),
			join(userHome, ".bun", "bin"),
			...loginPath.replace(/(?:\r\n|\n)+$/, "").split(":"),
			...FALLBACK_PATH_DIRS,
		],
		userHome,
	);
}

export async function defaultLoginPathRunner(command: readonly string[]): Promise<string> {
	let child: ReturnType<typeof Bun.spawn>;
	try {
		child = Bun.spawn([...command], { stdout: "pipe", stderr: "pipe" });
	} catch (error) {
		throw new Error(`could not run login shell: ${error instanceof Error ? error.message : String(error)}`);
	}
	if (!(child.stdout instanceof ReadableStream)) throw new Error("login shell stdout was not piped");
	const stdout = await new Response(child.stdout).text();
	const exitCode = await child.exited;
	if (exitCode !== 0) throw new Error(`login shell exited with status ${exitCode}`);
	return stdout;
}

export async function discoverLoginPath(
	env: NodeJS.ProcessEnv = process.env,
	runner: LoginPathRunner = defaultLoginPathRunner,
): Promise<string> {
	const shell = env.SHELL || "/bin/zsh";
	return await runner([shell, "-lc", LOGIN_PATH_COMMAND]);
}

export async function resolvePath(options: ResolvePathOptions): Promise<string> {
	const runtime = options.runtime;
	const env = options.env;
	const shellEnv = env ?? process.env;
	const userHome = options.home;
	let loginPath = "";
	if (runtime?.path !== undefined) loginPath = runtime.path.join(":");
	else if (runtime?.inheritLoginPath !== false)
		loginPath =
			options.loginPath ?? (await discoverLoginPath(shellEnv, options.loginPathRunner ?? defaultLoginPathRunner));
	return mergePathEntries(options.binDir, userHome, loginPath).join(":");
}

export function escapeXml(value: string): string {
	return value
		.replaceAll("&", "&amp;")
		.replaceAll("<", "&lt;")
		.replaceAll(">", "&gt;")
		.replaceAll('"', "&quot;")
		.replaceAll("'", "&apos;");
}

function plistString(value: string): string {
	return `<string>${escapeXml(value)}</string>`;
}

export function renderLaunchAgent(spec: ServiceSpec, binDir: string, home: string, path: string): string {
	const program = join(binDir, spec.binary);
	const argumentsXml = [program, ...spec.args].map(plistString).join("");
	return `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key>${plistString(spec.label)}
  <key>ProgramArguments</key><array>${argumentsXml}</array>
  <key>WorkingDirectory</key>${plistString(binDir)}
  <key>EnvironmentVariables</key><dict>
    <key>GAJAEWAY_HOME</key>${plistString(home)}
    <key>PATH</key>${plistString(path)}
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key>${plistString(join(home, `${spec.id}.stdout.log`))}
  <key>StandardErrorPath</key>${plistString(join(home, `${spec.id}.stderr.log`))}
</dict></plist>
`;
}

async function readRuntime(home: string): Promise<RuntimeConfig | undefined> {
	const configPath = join(home, "config.json");
	let raw: string;
	try {
		raw = await readFile(configPath, "utf8");
	} catch (error) {
		throw new Error(`could not read ${configPath}: ${error instanceof Error ? error.message : String(error)}`);
	}
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		throw new Error(`could not parse ${configPath}: malformed JSON`);
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
		throw new Error(`invalid configuration ${configPath}: config must be an object`);
	const input = parsed as Record<string, unknown>;
	if (input.schemaVersion !== CONFIG_SCHEMA_VERSION)
		throw new Error(`invalid configuration ${configPath}: schemaVersion must be ${CONFIG_SCHEMA_VERSION}`);
	try {
		return parseRuntimeConfig(input.runtime);
	} catch (error) {
		const message = error instanceof RuntimeConfigError ? error.message : String(error);
		throw new Error(`invalid runtime configuration ${configPath}: ${message}`);
	}
}

export async function installServices(options: InstallServicesOptions): Promise<readonly string[]> {
	if (options.binDir.length === 0) throw new Error("services requires a non-empty --bin-dir DIR");
	const env = options.env ?? process.env;
	const userHome = env.HOME || homedir();
	const home = homeForEnvironment(env);
	const binDir = expandHome(options.binDir, userHome);
	const launchAgentsDir = expandHome(options.launchAgentsDir ?? join(userHome, "Library", "LaunchAgents"), userHome);
	const runtime = await readRuntime(home);
	const path = await resolvePath({
		binDir,
		home: userHome,
		runtime,
		env,
		loginPathRunner: options.loginPathRunner,
	});
	await mkdir(launchAgentsDir, { recursive: true, mode: 0o700 });
	const plists = SERVICE_SPECS.map((spec) => ({
		path: join(launchAgentsDir, `dev.gajaeway.${spec.id}.plist`),
		contents: renderLaunchAgent(spec, binDir, home, path),
	}));
	const write = options.writeFile;
	for (const plist of plists) {
		if (write) await write(plist.path, plist.contents);
		else {
			await writeFile(plist.path, plist.contents, { encoding: "utf8", mode: 0o600 });
			await chmod(plist.path, 0o600);
		}
	}
	return plists.map((plist) => plist.path);
}

export function serviceUsage(): string {
	return "usage: gajaeway services install|repair --bin-dir DIR [--launch-agents-dir DIR]";
}
