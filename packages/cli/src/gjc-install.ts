import { randomUUID } from "node:crypto";
import { access, chmod, lstat, mkdir, mkdtemp, readlink, readdir, rename, rm, symlink, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";

export interface GjcInstallRunResult {
	readonly exitCode: number;
	readonly stdout: string;
	readonly stderr: string;
}

export type GjcInstallRunner = (command: readonly string[], cwd: string) => Promise<GjcInstallRunResult>;

const VERSION_PATTERN = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;

async function defaultRunner(command: readonly string[], cwd: string): Promise<GjcInstallRunResult> {
	const child = Bun.spawn([...command], { cwd, stdin: "ignore", stdout: "pipe", stderr: "pipe" });
	const [stdout, stderr, exitCode] = await Promise.all([
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		child.exited,
	]);
	return { exitCode, stdout, stderr };
}

function assertSuccessful(command: readonly string[], result: GjcInstallRunResult): void {
	if (result.exitCode !== 0) {
		const detail = result.stderr.trim();
		throw new Error(
			`${command.join(" ")} exited with status ${result.exitCode}${detail.length === 0 ? "" : `: ${detail}`}`,
		);
	}
}

function versionFromOutput(output: string): string | undefined {
	return output.match(/\b\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?\b/)?.[0];
}

async function verifyGjc(path: string, cwd: string, version: string, runner: GjcInstallRunner): Promise<void> {
	await access(path);
	const command = [path, "--version"];
	const result = await runner(command, cwd);
	assertSuccessful(command, result);
	const reported = versionFromOutput(`${result.stdout}\n${result.stderr}`);
	if (reported !== version) throw new Error(`gjc version verification failed: expected ${version}, got ${reported ?? "unknown"}`);
}

async function currentVersion(prefix: string, current: string): Promise<string | undefined> {
	try {
		const target = await readlink(current);
		if (target.includes("/") || !VERSION_PATTERN.test(target)) return undefined;
		const targetPath = join(prefix, target);
		if (!(await lstat(targetPath)).isDirectory()) return undefined;
		return target;
	} catch {
		return undefined;
	}
}

async function pruneVersions(prefix: string, keep: readonly string[]): Promise<void> {
	const retained = new Set(keep);
	const entries = await readdir(prefix, { withFileTypes: true });
	const versions = entries
		.filter((entry) => entry.isDirectory() && VERSION_PATTERN.test(entry.name))
		.map((entry) => entry.name)
		.sort((left, right) => Bun.semver.order(right, left));
	for (const version of versions) {
		if (retained.size >= 2) break;
		retained.add(version);
	}
	for (const version of versions)
		if (!retained.has(version)) await rm(join(prefix, version), { recursive: true, force: true });
}

/** Install the gateway-pinned gjc into its versioned private prefix. */
export async function installPinnedGjc(options: {
	readonly home: string;
	readonly version: string;
	readonly runner?: GjcInstallRunner;
}): Promise<string> {
	const { home, version } = options;
	if (!VERSION_PATTERN.test(version)) throw new Error(`invalid pinned gjc version: ${version}`);
	const layout = homeLayout(home);
	const runner = options.runner ?? defaultRunner;
	await mkdir(layout.gjcPrefix, { recursive: true, mode: 0o700 });
	await chmod(layout.gjcPrefix, 0o700);

	const versionDir = join(layout.gjcPrefix, version);
	const previousVersion = await currentVersion(layout.gjcPrefix, layout.gjcCurrent);
	let staging: string | undefined;
	try {
		let installed = false;
		try {
			const info = await lstat(versionDir);
			if (!info.isDirectory()) throw new Error(`gjc version path is not a directory: ${versionDir}`);
			installed = true;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
		}

		if (!installed) {
			staging = await mkdtemp(join(layout.gjcPrefix, ".staging-"));
			await chmod(staging, 0o700);
			await writeFile(
				join(staging, "package.json"),
				`${JSON.stringify({ private: true, dependencies: { "gajae-code": version } }, null, 2)}\n`,
				{ mode: 0o600 },
			);
			const installCommand = ["bun", "install", "--production"];
			assertSuccessful(installCommand, await runner(installCommand, staging));
			const installedBin = join(staging, "node_modules", ".bin", "gjc");
			await access(installedBin);
			const binDir = join(staging, "bin");
			await mkdir(binDir, { mode: 0o700 });
			await symlink("../node_modules/.bin/gjc", join(binDir, "gjc"), "file");
			await verifyGjc(join(binDir, "gjc"), staging, version, runner);
			await rename(staging, versionDir);
			staging = undefined;
		} else {
			await verifyGjc(join(versionDir, "bin", "gjc"), versionDir, version, runner);
		}

		await pruneVersions(layout.gjcPrefix, [version, ...(previousVersion === undefined ? [] : [previousVersion])]);
		const temporaryLink = join(layout.gjcPrefix, `.current-${randomUUID()}`);
		try {
			await symlink(version, temporaryLink, "dir");
			await rename(temporaryLink, layout.gjcCurrent);
		} finally {
			await rm(temporaryLink, { force: true });
		}
		return layout.gjcExecutable();
	} finally {
		if (staging !== undefined) await rm(staging, { recursive: true, force: true });
	}
}
