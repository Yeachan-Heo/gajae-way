import {
	chmod,
	copyFile,
	mkdir,
	mkdtemp,
	readdir,
	readFile,
	realpath,
	rename,
	rm,
	stat,
	writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { acquireLease, homeLayout, type Lease, LeaseHeldError } from "@gajae-gateway/home";
import pkg from "../package.json";
import { type GjcInstallRunner, installPinnedGjc } from "./gjc-install";
import { launchRestartStack } from "./restart-stack";
import {
	type InstallServicesOptions,
	installServices,
	type ServicePlatform,
	serviceSpecs,
	systemdUnitName,
} from "./services";

/**
 * `gajaeway update`: self-update a binary install from the project's GitHub
 * releases, the way `gjc update` does for gajae-code. Production hosts run
 * standalone binaries under launchd/systemd without a source checkout, so the
 * CLI owns its own upgrade path: compare against the latest release, replace
 * the installed binaries with a backup, and queue the existing restart-stack
 * supervisor. A source checkout keeps upgrading through `git` + `bun run
 * build`; `--bin-dir` covers hosts that drive the update from elsewhere.
 */

export const UPDATE_USAGE = "usage: gajaeway update [--check] [--force] [--bin-dir DIR] [--no-restart]";

export const DEFAULT_RELEASE_REPO = "Yeachan-Heo/gajae-way";
const RELEASE_REPO_OVERRIDE_ENV = "GAJAEWAY_UPDATE_REPO";
/** A fork can point updates at its own releases; the declared upstream stays the default. */
export function releaseRepo(env: NodeJS.ProcessEnv = process.env): string {
	const override = env[RELEASE_REPO_OVERRIDE_ENV]?.trim();
	return override ? override : declaredReleaseRepo();
}

function declaredReleaseRepo(): string {
	const url = pkg.repository?.url;
	const match =
		typeof url === "string" ? /^git\+https:\/\/github\.com\/([^/\s]+\/[^/\s]+?)\.git$/.exec(url) : undefined;
	return match ? match[1] : DEFAULT_RELEASE_REPO;
}

export interface ReleaseAsset {
	readonly name: string;
	readonly url: string;
}

export interface ReleaseInfo {
	readonly tag: string;
	readonly assets: readonly ReleaseAsset[];
}

export interface ParsedUpdateArgs {
	readonly check: boolean;
	readonly force: boolean;
	readonly noRestart: boolean;
	readonly binDir?: string;
}

export function parseUpdateArgs(args: readonly string[]): ParsedUpdateArgs {
	let check = false;
	let force = false;
	let noRestart = false;
	let binDir: string | undefined;
	for (let i = 0; i < args.length; i++) {
		const flag = args[i];
		if (flag === "--check") check = true;
		else if (flag === "--force") force = true;
		else if (flag === "--no-restart") noRestart = true;
		else if (flag === "--bin-dir") {
			const value = args[++i];
			if (value === undefined || value.length === 0 || value.startsWith("--"))
				throw new Error(`${flag} expects a non-empty DIR`);
			binDir = value;
		} else throw new Error(`${UPDATE_USAGE} (unknown option: ${flag})`);
	}
	return {
		check,
		force,
		noRestart,
		...(binDir === undefined ? {} : { binDir }),
	};
}

/** Release tags are `vX.Y.Z` (optionally with a prerelease); anything else is not comparable. */
export function releaseVersion(tag: string): string | undefined {
	const match = /^v?(\d+)\.(\d+)\.(\d+)(-[0-9A-Za-z.-]+)?$/.exec(tag.trim());
	return match ? `${match[1]}.${match[2]}.${match[3]}${match[4] ?? ""}` : undefined;
}

/** The release workflow publishes one tarball per platform/arch of the whole `dist/`. */
export function releaseAssetName(platform: NodeJS.Platform = process.platform, arch: string = process.arch): string {
	return `gajaeway-${platform}-${arch}.tar.gz`;
}

export function selectReleaseAsset(release: ReleaseInfo, name: string): ReleaseAsset | undefined {
	return release.assets.find((asset) => asset.name === name);
}

export function isNewer(current: string, latest: string): boolean {
	return Bun.semver.order(latest, current) > 0;
}

/**
 * Every binary a release tarball can contain, in the order updates report
 * them. An operator's bin dir may carry only a subset (for example no
 * Discord/Telegram adapters); the update replaces what is installed plus the
 * core pair, never adding adapters the host did not choose.
 */
export const KNOWN_BINARIES = [
	"gajaeway",
	"gajaeway-gateway",
	"gajaeway-admin",
	"gajaeway-slack",
	"gajaeway-discord",
	"gajaeway-telegram",
] as const;

export const CORE_BINARIES = ["gajaeway", "gajaeway-gateway"] as const;

/** Compiled binaries are tens of MB; a staged file below this size is a broken extraction. */
const MIN_BINARY_BYTES = 1_048_576;
const MAX_ASSET_BYTES = 512 * 1024 * 1024;

const RELEASE_MARKER = ".gajaeway-release";

export function releaseMarkerPath(binDir: string): string {
	return join(binDir, RELEASE_MARKER);
}

export async function readInstalledRelease(binDir: string): Promise<string | undefined> {
	try {
		const version = releaseVersion(await readFile(releaseMarkerPath(binDir), "utf8"));
		return version;
	} catch {
		return undefined;
	}
}

/**
 * The bin dir of the running compiled binary. Under `bun packages/cli/src/main.ts`
 * the executable is the interpreter, not an install, so the caller must pass
 * `--bin-dir`; a renamed binary directory is adopted as long as the file still
 * carries a `gajaeway…` name.
 */
export async function resolveBinDir(
	flag: string | undefined,
	execPath = process.execPath,
): Promise<string | undefined> {
	if (flag !== undefined) return flag;
	const real = await realpath(execPath).catch(() => undefined);
	if (real === undefined) return undefined;
	return basename(real).startsWith("gajaeway") ? dirname(real) : undefined;
}

export interface UpdateResult {
	readonly current: string;
	readonly latest: string;
	readonly tag: string;
	readonly repo: string;
	readonly checkedOnly: boolean;
	readonly upToDate: boolean;
	readonly installed: readonly string[];
	readonly backupDir?: string;
	readonly restartQueued?: string;
	readonly brokerNotEnabled?: boolean;
	readonly restartSkipped?: boolean;
}

export function renderUpdate(result: UpdateResult): string[] {
	const lines: string[] = [];
	if (result.upToDate) {
		lines.push(
			result.checkedOnly
				? `up to date: ${result.current} (latest release ${result.tag})`
				: `already up to date: ${result.current} (latest release ${result.tag}); use --force to reinstall`,
		);
		return lines;
	}
	if (result.checkedOnly) {
		lines.push(`update available: ${result.current} → ${result.latest} (release ${result.tag})`);
		return lines;
	}
	lines.push(`updated ${result.current} → ${result.latest} from ${result.repo} (${result.tag})`);
	lines.push(`installed: ${result.installed.join(", ")}`);
	if (result.backupDir !== undefined) lines.push(`backup: ${result.backupDir}`);
	if (result.restartQueued !== undefined)
		lines.push(`restart: queued ${result.restartQueued} — check with: gajaeway ops restart-stack --status`);
	else if (result.restartSkipped) lines.push("restart: skipped (--no-restart)");
	if (result.brokerNotEnabled) lines.push("broker unit installed, not enabled: run gajaeway setup --broker-agent");
	return lines;
}

export interface RestartQueueReceipt {
	readonly receiptId: string;
	readonly supervisorPid: number;
}

export type ReleaseFetcher = (repo: string) => Promise<ReleaseInfo>;
export type AssetDownloader = (asset: ReleaseAsset) => Promise<Uint8Array>;
export type TarballExtractor = (tarball: Uint8Array, intoDir: string) => Promise<void>;
export type RestartQueuer = (home: string, binDir: string, trigger: "update") => Promise<RestartQueueReceipt>;
export type UpdateCommandRunner = (command: readonly string[]) => Promise<number>;

type UpdateServiceOptions = Pick<
	InstallServicesOptions,
	"launchAgentsDir" | "unitDir" | "loginPathRunner" | "writeFile"
>;

export interface UpdateDeps {
	readonly fetchLatestRelease?: ReleaseFetcher;
	readonly downloadAsset?: AssetDownloader;
	readonly extractTarball?: TarballExtractor;
	readonly queueRestart?: RestartQueuer;
	readonly platform?: NodeJS.Platform;
	readonly arch?: string;
	readonly env?: NodeJS.ProcessEnv;
	readonly execPath?: string;
	readonly now?: () => number;
	readonly gjcInstallRunner?: GjcInstallRunner;
	readonly serviceRunner?: UpdateCommandRunner;
	readonly serviceOptions?: UpdateServiceOptions;
}

export interface RunUpdateOptions {
	readonly args: ParsedUpdateArgs;
	readonly home: string;
	readonly deps?: UpdateDeps;
}

async function readPinnedGjcVersion(path: string): Promise<string> {
	let parsed: unknown;
	try {
		parsed = JSON.parse(await readFile(path, "utf8"));
	} catch (error) {
		throw new Error(
			`could not read pinned gjc version from ${path}: ${error instanceof Error ? error.message : String(error)}`,
		);
	}
	if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed))
		throw new Error(`invalid release package metadata at ${path}`);
	const gjc = (parsed as { gjc?: unknown }).gjc;
	if (typeof gjc !== "object" || gjc === null || Array.isArray(gjc))
		throw new Error(`release package metadata at ${path} has no gjc.version`);
	const version = (gjc as { version?: unknown }).version;
	if (typeof version !== "string" || version.trim() === "")
		throw new Error(`release package metadata at ${path} has no gjc.version`);
	return version;
}

async function defaultUpdateCommandRunner(command: readonly string[]): Promise<number> {
	const child = Bun.spawn([...command], { stdin: "ignore", stdout: "ignore", stderr: "pipe" });
	const [stderr, exitCode] = await Promise.all([new Response(child.stderr).text(), child.exited]);
	if (exitCode !== 0) {
		const detail = stderr.trim();
		throw new Error(`${command.join(" ")} exited with status ${exitCode}${detail.length === 0 ? "" : `: ${detail}`}`);
	}
	return exitCode;
}

async function installUpdatedServices(options: {
	readonly home: string;
	readonly binDir: string;
	readonly platform: ServicePlatform;
	readonly noRestart: boolean;
	readonly deps: UpdateDeps;
}): Promise<boolean> {
	const { home, binDir, platform, noRestart, deps } = options;
	const brokerProvisioned = await exists(homeLayout(home).brokerProvisionManifest);
	const env = { ...(deps.env ?? process.env), GAJAEWAY_HOME: home };
	await installServices({
		binDir,
		platform,
		env,
		configuredAdaptersOnly: true,
		compareAndWrite: true,
		...deps.serviceOptions,
	});
	if (platform === "linux") {
		const run = deps.serviceRunner ?? defaultUpdateCommandRunner;
		const execute = async (command: readonly string[]): Promise<void> => {
			const exitCode = await run(command);
			if (exitCode !== 0) throw new Error(`${command.join(" ")} exited with status ${exitCode}`);
		};
		await execute(["systemctl", "--user", "daemon-reload"]);
		if (!noRestart) {
			const enableUnits: string[] = [];
			for (const spec of serviceSpecs()) {
				if (spec.id === "broker" && !brokerProvisioned) continue;
				if (
					spec.id.startsWith("adapter-") &&
					!(await Bun.file(join(home, `adapter-${spec.id.slice(8)}.json`)).exists())
				)
					continue;
				enableUnits.push(systemdUnitName(spec));
			}
			if (enableUnits.length > 0) await execute(["systemctl", "--user", "enable", ...enableUnits]);
		}
	}
	return !brokerProvisioned;
}
export async function runUpdate(options: RunUpdateOptions): Promise<UpdateResult> {
	const deps = options.deps ?? {};
	const platform = deps.platform ?? process.platform;
	if (platform !== "darwin" && platform !== "linux")
		throw new Error(`update supports darwin and linux release assets, not ${platform}`);
	const repo = releaseRepo(deps.env ?? process.env);
	const release = await (deps.fetchLatestRelease ?? defaultFetchLatestRelease)(repo);
	const latest = releaseVersion(release.tag);
	if (latest === undefined) throw new Error(`latest release tag is not a version: ${release.tag}`);
	const binDir = await resolveBinDir(options.args.binDir, deps.execPath ?? process.execPath);
	const current = (binDir === undefined ? undefined : await readInstalledRelease(binDir)) ?? pkg.version;
	const upToDate = !isNewer(current, latest);
	if (upToDate && !options.args.force)
		return { current, latest, tag: release.tag, repo, checkedOnly: options.args.check, upToDate: true, installed: [] };
	const asset = selectReleaseAsset(release, releaseAssetName(platform, deps.arch ?? process.arch));
	if (asset === undefined)
		throw new Error(
			`release ${release.tag} has no asset ${releaseAssetName(platform, deps.arch ?? process.arch)} (assets: ${
				release.assets.map((candidate) => candidate.name).join(", ") || "none"
			})`,
		);
	if (options.args.check)
		return { current, latest, tag: release.tag, repo, checkedOnly: true, upToDate: false, installed: [] };
	if (binDir === undefined) throw new Error("run the update from a compiled gajaeway binary, or pass --bin-dir DIR");

	let lease: Lease;
	try {
		lease = await acquireLease(options.home, "update");
		console.info(`lease_acquired name=update pid=${process.pid}`);
	} catch (error) {
		if (error instanceof LeaseHeldError) {
			console.error(error.message);
			throw new Error(`another gajaeway update is running (${error.message})`, { cause: error });
		}
		console.error(`lease_failed name=update detail=${error instanceof Error ? error.message : String(error)}`);
		throw error;
	}

	const installed = await (async () => {
		let staged: StagedRelease | undefined;
		try {
			staged = await stageRelease(asset, deps);
			const installedNames = await knownBinariesIn(binDir);
			const toInstall = binariesToInstall(staged.names, installedNames);
			const missingCore = CORE_BINARIES.filter((name) => !toInstall.includes(name));
			if (missingCore.length > 0)
				throw new Error(`release tarball is missing core binaries: ${missingCore.join(", ")}`);
			await assertStagedBinariesUsable(staged, toInstall);

			const gjcVersion = await readPinnedGjcVersion(join(staged.dir, "package.json"));
			await installPinnedGjc({
				home: options.home,
				version: gjcVersion,
				...(deps.gjcInstallRunner === undefined ? {} : { runner: deps.gjcInstallRunner }),
			});

			const stamp = new Date((deps.now ?? Date.now)()).toISOString().replace(/[-:]/g, "").slice(0, 15);
			const backupDir = join(binDir, `backup-${current}-${stamp}Z`);
			await mkdir(backupDir, { recursive: false });
			for (const name of toInstall) {
				const target = join(binDir, name);
				if (await exists(target)) await copyFile(target, join(backupDir, name));
			}
			for (const name of toInstall) {
				const stagedPath = join(staged.dir, name);
				const tempPath = join(binDir, `.update-${name}`);
				await copyFile(stagedPath, tempPath);
				await chmod(tempPath, 0o755);
				await rename(tempPath, join(binDir, name));
			}
			await writeFile(releaseMarkerPath(binDir), `${latest}\n`, "utf8");
			const brokerNotEnabled = await installUpdatedServices({
				home: options.home,
				binDir,
				platform,
				noRestart: options.args.noRestart,
				deps,
			});
			return { toInstall, backupDir, brokerNotEnabled };
		} finally {
			if (staged !== undefined) await rm(staged.dir, { recursive: true, force: true });
			await lease.release();
		}
	})();

	if (options.args.noRestart)
		return {
			current,
			latest,
			tag: release.tag,
			repo,
			checkedOnly: false,
			upToDate: false,
			installed: installed.toInstall,
			backupDir: installed.backupDir,
			brokerNotEnabled: installed.brokerNotEnabled,
			restartSkipped: true,
		};
	const receipt = await (deps.queueRestart ?? defaultQueueRestart)(options.home, binDir, "update");

	return {
		current,
		latest,
		tag: release.tag,
		repo,
		checkedOnly: false,
		upToDate: false,
		installed: installed.toInstall,
		backupDir: installed.backupDir,
		brokerNotEnabled: installed.brokerNotEnabled,
		restartQueued: receipt.receiptId,
	};
}

function binariesToInstall(stagedNames: readonly string[], installedNames: readonly string[]): string[] {
	const installed = new Set(installedNames);
	return KNOWN_BINARIES.filter(
		(name) =>
			stagedNames.includes(name) && (installed.has(name) || (CORE_BINARIES as readonly string[]).includes(name)),
	);
}

async function knownBinariesIn(dir: string): Promise<string[]> {
	const entries = await readdir(dir).catch(() => [] as string[]);
	const names = new Set(entries);
	return KNOWN_BINARIES.filter((name) => names.has(name));
}

async function assertStagedBinariesUsable(
	staged: { readonly dir: string; readonly names: readonly string[] },
	toInstall: readonly string[],
): Promise<void> {
	for (const name of toInstall) {
		const info = await stat(join(staged.dir, name));
		if (!info.isFile()) throw new Error(`staged ${name} is not a regular file`);
		if (info.size < MIN_BINARY_BYTES)
			throw new Error(`staged ${name} is only ${info.size} bytes; refusing a broken extraction`);
	}
}

async function exists(path: string): Promise<boolean> {
	try {
		await stat(path);
		return true;
	} catch {
		return false;
	}
}

interface StagedRelease {
	readonly dir: string;
	readonly names: readonly string[];
}

async function stageRelease(asset: ReleaseAsset, deps: UpdateDeps): Promise<StagedRelease> {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-update-"));
	const tarball = await (deps.downloadAsset ?? defaultDownloadAsset)(asset);
	await (deps.extractTarball ?? defaultExtractTarball)(tarball, dir);
	const names = (await readdir(dir)).filter((name) => (KNOWN_BINARIES as readonly string[]).includes(name));
	return { dir, names };
}

async function defaultFetchLatestRelease(repo: string): Promise<ReleaseInfo> {
	const response = await fetch(`https://api.github.com/repos/${repo}/releases/latest`, {
		headers: { Accept: "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28" },
	});
	if (!response.ok) throw new Error(`release lookup failed: HTTP ${response.status} for ${repo}/releases/latest`);
	const body = (await response.json()) as {
		tag_name?: unknown;
		assets?: Array<{ name?: unknown; browser_download_url?: unknown }>;
	};
	const tag = typeof body.tag_name === "string" ? body.tag_name : undefined;
	if (tag === undefined) throw new Error(`latest release of ${repo} has no tag_name`);
	const assets = (body.assets ?? []).flatMap((asset) =>
		typeof asset.name === "string" && typeof asset.browser_download_url === "string"
			? [{ name: asset.name, url: asset.browser_download_url }]
			: [],
	);
	return { tag, assets };
}

async function defaultDownloadAsset(asset: ReleaseAsset): Promise<Uint8Array> {
	const response = await fetch(asset.url);
	if (!response.ok) throw new Error(`download failed: HTTP ${response.status} for ${asset.name}`);
	const bytes = new Uint8Array(await response.arrayBuffer());
	if (bytes.byteLength > MAX_ASSET_BYTES)
		throw new Error(`${asset.name} is ${bytes.byteLength} bytes; refusing anything over ${MAX_ASSET_BYTES}`);
	return bytes;
}

async function defaultExtractTarball(tarball: Uint8Array, intoDir: string): Promise<void> {
	const tarballPath = join(intoDir, "release.tar.gz");
	await writeFile(tarballPath, tarball);
	const process2 = Bun.spawn(["tar", "-xzf", tarballPath, "-C", intoDir], {
		stdout: "ignore",
		stderr: "pipe",
	});
	const exitCode = await process2.exited;
	if (exitCode !== 0) {
		const detail = await new Response(process2.stderr).text();
		throw new Error(`extracting ${basename(tarballPath)} failed (tar exit ${exitCode}): ${detail.trim()}`);
	}
	await rm(tarballPath, { force: true });
}

async function defaultQueueRestart(home: string, binDir: string, trigger: "update"): Promise<RestartQueueReceipt> {
	const { receipt, supervisorPid } = await launchRestartStack({ home, binDir, trigger });
	return { receiptId: receipt.id, supervisorPid };
}
