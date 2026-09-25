import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { parseRuntimeConfig, type RuntimeConfig, RuntimeConfigError } from "@gajae-gateway/protocol";

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

/** Service managers this CLI can write definitions for. */
export type ServicePlatform = "darwin" | "linux";

export interface InstallServicesOptions {
	readonly binDir: string;
	readonly launchAgentsDir?: string;
	/** systemd user-unit directory; defaults to ${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user. */
	readonly unitDir?: string;
	/** Service manager to generate for; defaults to the running platform. */
	readonly platform?: ServicePlatform;
	readonly env?: NodeJS.ProcessEnv;
	/** Test seam for the `${SHELL} -lc` invocation used by the default path. */
	readonly loginPathRunner?: LoginPathRunner;
	/** Test seam for counting or inspecting the definition writes. */
	readonly writeFile?: PlistWriter;
}

interface ServiceSpec {
	readonly id: string;
	readonly label: string;
	readonly binary: string;
	readonly args: readonly string[];
	/**
	 * True for every process that holds a live connection to the gateway. A
	 * gateway restart leaves such a process attached to the previous generation:
	 * it neither dies nor loses messages, so no health check catches it, and the
	 * symptom is replies arriving one beat late (issue #251). The service
	 * definition, not an operator convention, has to restart it.
	 */
	readonly dependsOnGateway: boolean;
}

export const GATEWAY_UNIT = "gajaeway-gateway.service";

const SERVICE_SPECS: readonly ServiceSpec[] = [
	{
		id: "gateway",
		label: "dev.gajaeway.gateway",
		binary: "gajaeway-gateway",
		args: ["daemon"],
		dependsOnGateway: false,
	},
	{
		id: "adapter-discord",
		label: "dev.gajaeway.adapter-discord",
		binary: "gajaeway-discord",
		args: [],
		dependsOnGateway: true,
	},
	{
		id: "adapter-slack",
		label: "dev.gajaeway.adapter-slack",
		binary: "gajaeway-slack",
		args: [],
		dependsOnGateway: true,
	},
	{ id: "admin", label: "dev.gajaeway.admin", binary: "gajaeway-admin", args: ["serve"], dependsOnGateway: true },
];

export function serviceSpecs(): readonly ServiceSpec[] {
	return SERVICE_SPECS;
}

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

/**
 * systemd reads `Environment=` with its own quoting rules, so a value is wrapped
 * in double quotes with backslashes and quotes escaped. PATH entries containing
 * a space would otherwise silently split into two assignments.
 */
function unitEnvironment(name: string, value: string): string {
	return `Environment="${name}=${value.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

export function systemdUnitName(spec: ServiceSpec): string {
	return `gajaeway-${spec.id}.service`;
}

export function renderSystemdUnit(spec: ServiceSpec, binDir: string, home: string, path: string): string {
	const execStart = [join(binDir, spec.binary), ...spec.args].join(" ");
	const unit = [
		"[Unit]",
		`Description=gajaeway ${spec.id}`,
		// BindsTo restarts and stops this unit with the gateway; PartOf propagates a
		// gateway restart even when this unit is started on its own; After orders
		// the boot so the adapter connects to a listening socket.
		...(spec.dependsOnGateway ? [`BindsTo=${GATEWAY_UNIT}`, `After=${GATEWAY_UNIT}`, `PartOf=${GATEWAY_UNIT}`] : []),
		"",
		"[Service]",
		`ExecStart=${execStart}`,
		`WorkingDirectory=${binDir}`,
		unitEnvironment("GAJAEWAY_HOME", home),
		unitEnvironment("PATH", path),
		"Restart=always",
		"RestartSec=2",
		// Only the gateway: SIGTERM its main process for an ordered shutdown, then
		// kill whatever is left in its cgroup so no child outlives the unit and
		// gets re-adopted by the next start (#183). The gateway exits on its own
		// inside 25s (SHUTDOWN_DEADLINE_MS in the gateway main); the unit's stop
		// window is pinned so that ceiling always fits (#225). The gateway moves a GJC broker
		// it autostarted into a scope of its own, so this never reaches the shared
		// broker or its session hosts.
		...(spec.dependsOnGateway ? [] : ["KillMode=mixed", "TimeoutStopSec=30s"]),
		"",
		"[Install]",
		// Enabling the gateway pulls the whole stack in; a dependent is never
		// wanted by default.target on its own, because it cannot run without one.
		`WantedBy=${spec.dependsOnGateway ? GATEWAY_UNIT : "default.target"}`,
		"",
	];
	return unit.join("\n");
}

function systemdUnitDir(env: NodeJS.ProcessEnv, userHome: string): string {
	const base = env.XDG_CONFIG_HOME || join(userHome, ".config");
	return join(base, "systemd", "user");
}

/**
 * The ordered commands that realign the whole stack.
 *
 * systemd expresses the dependency itself, so one restart of the gateway unit
 * is the entire operation. launchd has no BindsTo/PartOf equivalent and
 * `WatchPaths` does not restart an already-running job, so the ordering is
 * explicit here instead — and `bootout` never appears, because removing a job
 * leaves it with no automatic recovery.
 */
export function restartStackCommands(platform: ServicePlatform, uid?: number): readonly (readonly string[])[] {
	if (platform === "linux") return [["systemctl", "--user", "restart", GATEWAY_UNIT]];
	if (platform === "darwin") {
		const target = uid ?? process.getuid?.() ?? 0;
		return restartOrder().map((spec) => ["launchctl", "kickstart", "-k", `gui/${target}/${spec.label}`]);
	}
	throw new Error(`unsupported service platform: ${platform}`);
}

/** The gateway first, then every job that holds a connection to it. */
export function restartOrder(): readonly ServiceSpec[] {
	return [
		...SERVICE_SPECS.filter((spec) => !spec.dependsOnGateway),
		...SERVICE_SPECS.filter((spec) => spec.dependsOnGateway),
	];
}

function requirePlatform(platform: string): ServicePlatform {
	if (platform !== "darwin" && platform !== "linux") throw new Error(`unsupported service platform: ${platform}`);
	return platform;
}

export function currentPlatform(): ServicePlatform {
	if (process.platform === "darwin" || process.platform === "linux") return process.platform;
	throw new Error(`unsupported service platform: ${process.platform}`);
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
	const platform = options.platform === undefined ? currentPlatform() : requirePlatform(options.platform);
	const userHome = env.HOME || homedir();
	const home = homeForEnvironment(env);
	const binDir = expandHome(options.binDir, userHome);
	const targetDir =
		platform === "darwin"
			? expandHome(options.launchAgentsDir ?? join(userHome, "Library", "LaunchAgents"), userHome)
			: expandHome(options.unitDir ?? systemdUnitDir(env, userHome), userHome);
	const runtime = await readRuntime(home);
	const path = await resolvePath({
		binDir,
		home: userHome,
		runtime,
		env,
		loginPathRunner: options.loginPathRunner,
	});
	await mkdir(targetDir, { recursive: true, mode: 0o700 });
	const definitions = SERVICE_SPECS.map((spec) =>
		platform === "darwin"
			? {
					path: join(targetDir, `dev.gajaeway.${spec.id}.plist`),
					contents: renderLaunchAgent(spec, binDir, home, path),
				}
			: { path: join(targetDir, systemdUnitName(spec)), contents: renderSystemdUnit(spec, binDir, home, path) },
	);
	const write = options.writeFile;
	for (const definition of definitions) {
		if (write) await write(definition.path, definition.contents);
		else {
			await writeFile(definition.path, definition.contents, { encoding: "utf8", mode: 0o600 });
			await chmod(definition.path, 0o600);
		}
	}
	return definitions.map((definition) => definition.path);
}

export function serviceUsage(): string {
	return "usage: gajaeway services install|repair --bin-dir DIR [--launch-agents-dir DIR] [--unit-dir DIR] [--platform darwin|linux]";
}
