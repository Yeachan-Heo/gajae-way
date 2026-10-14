import { describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, readFile, readlink, realpath, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { acquireLease, homeLayout, leaseHeld } from "@gajae-gateway/home";

import pkg from "../package.json";
import { main } from "../src/main";
import {
	type AssetDownloader,
	CORE_BINARIES,
	isNewer,
	KNOWN_BINARIES,
	parseUpdateArgs,
	type ReleaseFetcher,
	type ReleaseInfo,
	type RunUpdateOptions,
	releaseAssetName,
	releaseRepo,
	releaseVersion,
	renderUpdate,
	resolveBinDir,
	runUpdate,
	selectReleaseAsset,
	UPDATE_USAGE,
	type UpdateDeps,
} from "../src/update";

const PINNED_GJC = "0.18.11";

async function homeWithConfig(): Promise<string> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-update-home-"));
	await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }), "utf8");
	return home;
}

const gjcInstallRunner: NonNullable<UpdateDeps["gjcInstallRunner"]> = async (command, cwd) => {
	if (command[0] === "bun") {
		const bin = join(cwd, "node_modules", ".bin");
		await mkdir(bin, { recursive: true });
		await writeFile(join(bin, "gjc"), "test gjc executable");
		return { exitCode: 0, stdout: "", stderr: "" };
	}
	const manifest = JSON.parse(await readFile(join(cwd, "package.json"), "utf8")) as {
		dependencies: { "gajae-code": string };
	};
	return { exitCode: 0, stdout: `gjc/${manifest.dependencies["gajae-code"]}`, stderr: "" };
};

function serviceOptions(
	home: string,
	write: NonNullable<UpdateDeps["serviceOptions"]>["writeFile"] = async () => {},
): NonNullable<UpdateDeps["serviceOptions"]> {
	return {
		unitDir: join(home, "units"),
		launchAgentsDir: join(home, "agents"),
		loginPathRunner: async () => "/usr/bin:/bin",
		writeFile: write,
	};
}
const DARWIN_ASSET = "gajaeway-darwin-arm64.tar.gz";

function release(tag: string, assetNames: readonly string[]): ReleaseInfo {
	return {
		tag,
		assets: assetNames.map((name) => ({ name, url: `https://example.com/releases/${tag}/${name}` })),
	};
}

/** A staged binary the size checks accept; the bytes identify it in assertions. */
async function stagedBinary(dir: string, name: string, marker: string): Promise<void> {
	await writeFile(join(dir, name), `${marker}\n${"x".repeat(1_100_000)}`);
}

const noBytes: AssetDownloader = async () => new Uint8Array(0);

/** Stages the given binaries the way a real tarball extraction would. */
function extractorFor(
	binaries: readonly { name: string; marker: string }[],
	gjcVersion = PINNED_GJC,
): UpdateDeps["extractTarball"] {
	return async (_tarball, intoDir) => {
		await writeFile(join(intoDir, "package.json"), JSON.stringify({ gjc: { version: gjcVersion } }), "utf8");
		for (const binary of binaries) await stagedBinary(intoDir, binary.name, binary.marker);
	};
}

interface Recording {
	readonly downloads: string[];
	readonly restarts: string[];
}

function recorder(): Recording {
	const downloads: string[] = [];
	const restarts: string[] = [];
	return { downloads, restarts };
}

describe("update arguments", () => {
	test("defaults to a real update with restarts", () => {
		expect(parseUpdateArgs([])).toEqual({ check: false, force: false, noRestart: false });
	});

	test("accepts every documented flag", () => {
		expect(parseUpdateArgs(["--check", "--force", "--no-restart", "--bin-dir", "/opt/gajaeway"])).toEqual({
			check: true,
			force: true,
			noRestart: true,
			binDir: "/opt/gajaeway",
		});
	});

	test("refuses unknown flags and empty --bin-dir values with the usage", () => {
		expect(() => parseUpdateArgs(["--wat"])).toThrow(UPDATE_USAGE);
		expect(() => parseUpdateArgs(["--wat"])).toThrow("unknown option: --wat");
		expect(() => parseUpdateArgs(["--bin-dir"])).toThrow("--bin-dir expects a non-empty DIR");
		expect(() => parseUpdateArgs(["--bin-dir", "--check"])).toThrow("--bin-dir expects a non-empty DIR");
	});
});

describe("update release metadata", () => {
	test("accepts v-prefixed, plain, and prerelease tags; refuses anything else", () => {
		expect(releaseVersion("v0.1.2")).toBe("0.1.2");
		expect(releaseVersion("0.1.2")).toBe("0.1.2");
		expect(releaseVersion("v1.2.3-rc.1")).toBe("1.2.3-rc.1");
		expect(releaseVersion("v0.1")).toBeUndefined();
		expect(releaseVersion("release-2026")).toBeUndefined();
	});

	test("selects this platform's tarball and nothing else", () => {
		expect(releaseAssetName("darwin", "arm64")).toBe(DARWIN_ASSET);
		const info = release("v0.2.0", ["gajaeway-linux-x64.tar.gz", DARWIN_ASSET]);
		expect(selectReleaseAsset(info, DARWIN_ASSET)?.name).toBe(DARWIN_ASSET);
		expect(selectReleaseAsset(info, "gajaeway-win32-x64.tar.gz")).toBeUndefined();
	});

	test("only a strictly newer release counts", () => {
		expect(isNewer("0.1.2", "0.1.3")).toBe(true);
		expect(isNewer("0.1.2", "0.1.2")).toBe(false);
		expect(isNewer("0.1.3", "0.1.2")).toBe(false);
		expect(isNewer("0.1.3", "0.1.3-rc.1")).toBe(false);
	});

	test("the repo default comes from the package declaration; the env override wins", () => {
		expect(releaseRepo({})).toBe("Yeachan-Heo/gajae-way");
		expect(releaseRepo({ GAJAEWAY_UPDATE_REPO: "VC-Kyeongmin/gajae-way" })).toBe("VC-Kyeongmin/gajae-way");
		expect(releaseRepo({ GAJAEWAY_UPDATE_REPO: "  " })).toBe("Yeachan-Heo/gajae-way");
	});
});

describe("update bin dir resolution", () => {
	test("a compiled gajaeway binary names its own directory", async () => {
		const dir = await mkdtemp(join(tmpdir(), "gajaeway-update-bindir-"));
		try {
			const binary = join(dir, "gajaeway");
			await writeFile(binary, "stub");
			expect(await resolveBinDir(undefined, binary)).toBe(await realpath(dir));
			expect(await resolveBinDir("/elsewhere", binary)).toBe("/elsewhere");
		} finally {
			await rm(dir, { recursive: true, force: true });
		}
	});

	test("an interpreter exec path is not an install", async () => {
		const dir = await mkdtemp(join(tmpdir(), "gajaeway-update-bindir-"));
		try {
			const bun = join(dir, "bun");
			await writeFile(bun, "stub");
			expect(await resolveBinDir(undefined, bun)).toBeUndefined();
		} finally {
			await rm(dir, { recursive: true, force: true });
		}
	});
});

describe("runUpdate", () => {
	async function binDirWith(marker: string, binaries: readonly string[]): Promise<string> {
		const dir = await mkdtemp(join(tmpdir(), "gajaeway-update-bin-"));
		for (const name of binaries) await writeFile(join(dir, name), `old-${name}`);
		// A host installed by an older release carries that release's gjc pin beside its binaries.
		await writeFile(join(dir, "package.json"), JSON.stringify({ gjc: { version: "0.18.8" } }));
		if (marker !== "") await writeFile(join(dir, ".gajaeway-release"), `${marker}\n`);
		return dir;
	}

	test("--check against the running package version reads as up to date and touches nothing", async () => {
		const calls = recorder();
		const result = await runUpdate({
			args: { check: true, force: false, noRestart: false },
			home: "/tmp/unused-home",
			deps: {
				fetchLatestRelease: async () => release(`v${pkg.version}`, [DARWIN_ASSET]),
				downloadAsset: async (asset) => {
					calls.downloads.push(asset.name);
					return new Uint8Array(0);
				},
				extractTarball: async () => {
					throw new Error("check must not extract");
				},
				platform: "darwin",
				arch: "arm64",
				execPath: "/usr/local/bin/bun",
			},
		});
		expect(result).toMatchObject({ checkedOnly: true, upToDate: true, installed: [] });
		expect(calls.downloads).toHaveLength(0);
		expect(renderUpdate(result)).toEqual([`up to date: ${pkg.version} (latest release v${pkg.version})`]);
	});

	test("--check with a newer release reports availability using the installed marker", async () => {
		const dir = await binDirWith("0.1.1", []);
		try {
			const result = await runUpdate({
				args: { check: true, force: false, noRestart: false, binDir: dir },
				home: "/tmp/unused-home",
				deps: {
					fetchLatestRelease: async () => release("v0.2.0", [DARWIN_ASSET]),
					platform: "darwin",
					arch: "arm64",
				},
			});
			expect(result.current).toBe("0.1.1");
			expect(result.upToDate).toBe(false);
			expect(result.checkedOnly).toBe(true);
			expect(renderUpdate(result)).toEqual(["update available: 0.1.1 → 0.2.0 (release v0.2.0)"]);
			// A check never stages, backs up, or marks anything.
		} finally {
			await rm(dir, { recursive: true, force: true });
		}
	});

	test("an install replaces installed binaries, backs them up, marks the release, and queues the restart", async () => {
		const dir = await binDirWith("0.1.1", ["gajaeway", "gajaeway-gateway", "gajaeway-slack"]);
		const home = await homeWithConfig();
		const calls = recorder();
		const extractor = extractorFor(KNOWN_BINARIES.map((name) => ({ name, marker: `staged-${name}` })));
		try {
			const result = await runUpdate({
				args: { check: false, force: false, noRestart: false, binDir: dir },
				home,
				deps: {
					fetchLatestRelease: async () => release("v0.2.0", [DARWIN_ASSET]),
					downloadAsset: async (asset) => {
						calls.downloads.push(asset.name);
						return new Uint8Array(0);
					},
					extractTarball: extractor,
					gjcInstallRunner,
					serviceOptions: serviceOptions(home),
					queueRestart: async (restartHome, _binDir, trigger) => {
						calls.restarts.push(restartHome);
						expect(trigger).toBe("update");
						expect(await leaseHeld(restartHome, "update")).toBe(false);
						return { receiptId: "r-1", supervisorPid: 42 };
					},
					platform: "darwin",
					arch: "arm64",
				},
			});
			// Adapters the host never installed stay uninstalled; the core pair is refreshed.
			expect(result.installed).toEqual(["gajaeway", "gajaeway-gateway", "gajaeway-slack"]);
			expect(result.restartQueued).toBe("r-1");
			expect(result.upToDate).toBe(false);
			expect(calls.downloads).toEqual([DARWIN_ASSET]);
			expect(calls.restarts).toEqual([home]);
			expect(await leaseHeld(home, "update")).toBe(false);
			for (const name of ["gajaeway", "gajaeway-gateway", "gajaeway-slack"]) {
				const body = await readFile(join(dir, name), "utf8");
				expect(body.startsWith(`staged-${name}\n`)).toBe(true);
				expect((await stat(join(dir, name))).mode & 0o777).toBe(0o755);
			}
			expect(await readFile(join(dir, ".gajaeway-release"), "utf8")).toBe("0.2.0\n");
			expect(await readlink(homeLayout(home).gjcCurrent)).toBe(PINNED_GJC);
			// The pin the new binaries read beside themselves matches the gjc just installed.
			expect(JSON.parse(await readFile(join(dir, "package.json"), "utf8")).gjc.version).toBe(PINNED_GJC);
			expect((await stat(join(dir, "package.json"))).mode & 0o777).toBe(0o644);
			const backups = (await readdir(dir)).filter((name) => name.startsWith("backup-0.1.1-"));
			expect(backups).toHaveLength(1);
			for (const name of ["gajaeway", "gajaeway-gateway", "gajaeway-slack"])
				expect(await readFile(join(dir, backups[0], name), "utf8")).toBe(`old-${name}`);
			expect(JSON.parse(await readFile(join(dir, backups[0], "package.json"), "utf8")).gjc.version).toBe("0.18.8");
			for (const entry of await readdir(dir)) expect(entry.startsWith(".update-")).toBe(false);
			expect(await readdir(dir)).not.toContain("gajaeway-admin");
			const lines = renderUpdate(result);
			expect(lines[0]).toBe("updated 0.1.1 → 0.2.0 from Yeachan-Heo/gajae-way (v0.2.0)");
			expect(lines[1]).toBe("installed: gajaeway, gajaeway-gateway, gajaeway-slack");
			expect(lines[2]).toBe(`backup: ${join(dir, backups[0])}`);
			expect(lines[3]).toBe("restart: queued r-1 — check with: gajaeway ops restart-stack --status");
		} finally {
			await rm(dir, { recursive: true, force: true });
			await rm(home, { recursive: true, force: true });
		}
	});

	test("--no-restart replaces binaries without touching the service manager", async () => {
		const dir = await binDirWith("0.1.1", ["gajaeway", "gajaeway-gateway"]);
		const home = await homeWithConfig();
		const calls = recorder();
		try {
			const result = await runUpdate({
				args: { check: false, force: false, noRestart: true, binDir: dir },
				home,
				deps: {
					fetchLatestRelease: async () => release("v0.2.0", [DARWIN_ASSET]),
					downloadAsset: noBytes,
					extractTarball: extractorFor(CORE_BINARIES.map((name) => ({ name, marker: `staged-${name}` }))),
					gjcInstallRunner,
					serviceOptions: serviceOptions(home),
					queueRestart: async (restartHome) => {
						calls.restarts.push(restartHome);
						return { receiptId: "r-1", supervisorPid: 42 };
					},
					platform: "darwin",
					arch: "arm64",
				},
			});
			expect(result.restartSkipped).toBe(true);
			expect(result.restartQueued).toBeUndefined();
			expect(calls.restarts).toHaveLength(0);
			expect(renderUpdate(result)).toContain("restart: skipped (--no-restart)");
			expect(renderUpdate(result)).toContain("broker unit installed, not enabled: run gajaeway setup --broker-agent");
		} finally {
			await rm(dir, { recursive: true, force: true });
			await rm(home, { recursive: true, force: true });
		}
	});

	test("systemd update writes only configured adapter units and no-start refresh is daemon-reload only", async () => {
		const dir = await binDirWith("0.1.1", ["gajaeway", "gajaeway-gateway"]);
		const home = await homeWithConfig();
		const unitFiles = new Map<string, string>();
		const serviceCommands: string[][] = [];
		try {
			await writeFile(join(home, "adapter-discord.json"), "{}", "utf8");
			const result = await runUpdate({
				args: { check: false, force: false, noRestart: true, binDir: dir },
				home,
				deps: {
					fetchLatestRelease: async () => release("v0.2.0", [releaseAssetName("linux", "arm64")]),
					downloadAsset: noBytes,
					extractTarball: extractorFor(
						CORE_BINARIES.map((name) => ({ name, marker: `staged-${name}` })),
						"0.19.2",
					),
					gjcInstallRunner,
					serviceOptions: serviceOptions(home, async (path: string, contents: string) => {
						unitFiles.set(path, contents);
					}),
					serviceRunner: async (command: readonly string[]) => {
						serviceCommands.push([...command]);
						return 0;
					},
					platform: "linux",
					arch: "arm64",
				},
			});
			expect(result.restartSkipped).toBe(true);
			expect(result.brokerNotEnabled).toBe(true);
			expect(renderUpdate(result)).toContain("broker unit installed, not enabled: run gajaeway setup --broker-agent");
			expect(serviceCommands).toEqual([["systemctl", "--user", "daemon-reload"]]);
			expect([...unitFiles.keys()]).toContain(join(home, "units", "gajaeway-broker.service"));
			expect([...unitFiles.keys()]).toContain(join(home, "units", "gajaeway-adapter-discord.service"));
			expect([...unitFiles.keys()]).not.toContain(join(home, "units", "gajaeway-adapter-slack.service"));
			expect(await readlink(homeLayout(home).gjcCurrent)).toBe("0.19.2");
		} finally {
			await rm(dir, { recursive: true, force: true });
			await rm(home, { recursive: true, force: true });
		}
	});

	test("systemd update enables broker only with its provisioning manifest", async () => {
		const dir = await binDirWith("0.1.1", ["gajaeway", "gajaeway-gateway"]);
		const home = await homeWithConfig();
		const serviceCommands: string[][] = [];
		try {
			await writeFile(join(home, "adapter-slack.json"), "{}", "utf8");
			const options: RunUpdateOptions = {
				args: { check: false, force: false, noRestart: false, binDir: dir },
				home,
				deps: {
					fetchLatestRelease: async () => release("v0.2.0", [releaseAssetName("linux", "arm64")]),
					downloadAsset: noBytes,
					extractTarball: extractorFor(CORE_BINARIES.map((name) => ({ name, marker: `staged-${name}` }))),
					gjcInstallRunner,
					serviceOptions: serviceOptions(home),
					serviceRunner: async (command: readonly string[]) => {
						serviceCommands.push([...command]);
						return 0;
					},
					queueRestart: async (_home: string, _binDir: string, trigger: "update") => {
						expect(trigger).toBe("update");
						return { receiptId: "update-receipt", supervisorPid: 1 };
					},
					platform: "linux",
					arch: "arm64",
				},
			};
			const notProvisioned = await runUpdate(options);
			expect(notProvisioned.brokerNotEnabled).toBe(true);
			expect(renderUpdate(notProvisioned)).toContain(
				"broker unit installed, not enabled: run gajaeway setup --broker-agent",
			);
			expect(serviceCommands[0]).toEqual(["systemctl", "--user", "daemon-reload"]);
			expect(serviceCommands[1]).toEqual([
				"systemctl",
				"--user",
				"enable",
				"gajaeway-gateway.service",
				"gajaeway-adapter-slack.service",
				"gajaeway-admin.service",
			]);
			await mkdir(join(home, "broker", "provision"), { recursive: true });
			await writeFile(join(home, "broker", "provision", "manifest.json"), "{}", "utf8");
			serviceCommands.length = 0;
			await runUpdate({ ...options, args: { ...options.args, force: true } });
			expect(serviceCommands[1]).toEqual([
				"systemctl",
				"--user",
				"enable",
				"gajaeway-broker.service",
				"gajaeway-gateway.service",
				"gajaeway-adapter-slack.service",
				"gajaeway-admin.service",
			]);
		} finally {
			await rm(dir, { recursive: true, force: true });
			await rm(home, { recursive: true, force: true });
		}
	});
	test("--force reinstalls even when the latest release is already installed", async () => {
		const dir = await binDirWith("0.2.0", ["gajaeway", "gajaeway-gateway"]);
		const home = await homeWithConfig();
		try {
			const result = await runUpdate({
				args: { check: false, force: true, noRestart: true, binDir: dir },
				home,
				deps: {
					fetchLatestRelease: async () => release("v0.2.0", [DARWIN_ASSET]),
					downloadAsset: noBytes,
					extractTarball: extractorFor(CORE_BINARIES.map((name) => ({ name, marker: `again-${name}` }))),
					gjcInstallRunner,
					serviceOptions: serviceOptions(home),
					platform: "darwin",
					arch: "arm64",
				},
			});
			expect(result.installed).toEqual(["gajaeway", "gajaeway-gateway"]);
			expect((await readFile(join(dir, "gajaeway"), "utf8")).startsWith("again-gajaeway\n")).toBe(true);
		} finally {
			await rm(dir, { recursive: true, force: true });
			await rm(home, { recursive: true, force: true });
		}
	});

	test("without --force an up-to-date install is a no-op", async () => {
		const dir = await binDirWith("0.2.0", ["gajaeway", "gajaeway-gateway"]);
		try {
			const result = await runUpdate({
				args: { check: false, force: false, noRestart: false, binDir: dir },
				home: "/tmp/gajaeway-home",
				deps: {
					fetchLatestRelease: async () => release("v0.2.0", [DARWIN_ASSET]),
					downloadAsset: async () => {
						throw new Error("up to date must not download");
					},
					platform: "darwin",
					arch: "arm64",
				},
			});
			expect(result.upToDate).toBe(true);
			expect((await readFile(join(dir, "gajaeway"), "utf8")).startsWith("old-")).toBe(true);
		} finally {
			await rm(dir, { recursive: true, force: true });
		}
	});

	test("a release without this platform's asset is a hard error naming the assets", async () => {
		const dir = await binDirWith("0.1.1", ["gajaeway"]);
		try {
			await expect(
				runUpdate({
					args: { check: true, force: false, noRestart: false, binDir: dir },
					home: "/tmp/gajaeway-home",
					deps: {
						fetchLatestRelease: async () => release("v0.2.0", ["gajaeway-linux-x64.tar.gz"]),
						platform: "darwin",
						arch: "arm64",
					},
				}),
			).rejects.toThrow("has no asset gajaeway-darwin-arm64.tar.gz");
		} finally {
			await rm(dir, { recursive: true, force: true });
		}
	});

	test("a tarball missing a core binary is refused", async () => {
		const dir = await binDirWith("0.1.1", ["gajaeway"]);
		try {
			await expect(
				runUpdate({
					args: { check: false, force: false, noRestart: false, binDir: dir },
					home: "/tmp/gajaeway-home",
					deps: {
						fetchLatestRelease: async () => release("v0.2.0", [DARWIN_ASSET]),
						downloadAsset: noBytes,
						extractTarball: extractorFor([{ name: "gajaeway", marker: "staged" }]),
						platform: "darwin",
						arch: "arm64",
					},
				}),
			).rejects.toThrow("missing core binaries: gajaeway-gateway");
		} finally {
			await rm(dir, { recursive: true, force: true });
		}
	});

	test("a staging failure releases the update lease", async () => {
		const dir = await binDirWith("0.1.1", ["gajaeway", "gajaeway-gateway"]);
		const home = await mkdtemp(join(tmpdir(), "gajaeway-update-failed-stage-"));
		try {
			await expect(
				runUpdate({
					args: { check: false, force: false, noRestart: false, binDir: dir },
					home,
					deps: {
						fetchLatestRelease: async () => release("v0.2.0", [DARWIN_ASSET]),
						downloadAsset: noBytes,
						extractTarball: async (_tarball, intoDir) => {
							await writeFile(join(intoDir, "gajaeway"), "x");
							await writeFile(join(intoDir, "gajaeway-gateway"), "x");
						},
						platform: "darwin",
						arch: "arm64",
					},
				}),
			).rejects.toThrow("refusing a broken extraction");
			expect(await leaseHeld(home, "update")).toBe(false);
		} finally {
			await rm(dir, { recursive: true, force: true });
			await rm(home, { recursive: true, force: true });
		}
	});

	test("an install without a resolvable bin dir asks for --bin-dir instead of guessing", async () => {
		const dir = await mkdtemp(join(tmpdir(), "gajaeway-update-bunpath-"));
		try {
			const bun = join(dir, "bun");
			await writeFile(bun, "stub");
			await expect(
				runUpdate({
					args: { check: false, force: false, noRestart: true },
					home: "/tmp/gajaeway-home",
					deps: {
						fetchLatestRelease: async () => release("v0.3.0", [DARWIN_ASSET]),
						downloadAsset: noBytes,
						extractTarball: extractorFor(CORE_BINARIES.map((name) => ({ name, marker: `staged-${name}` }))),
						platform: "darwin",
						arch: "arm64",
						execPath: bun,
					},
				}),
			).rejects.toThrow("--bin-dir");
		} finally {
			await rm(dir, { recursive: true, force: true });
		}
	});

	test("an update refuses while the update lease is held", async () => {
		const dir = await binDirWith("0.1.1", ["gajaeway", "gajaeway-gateway"]);
		const home = await mkdtemp(join(tmpdir(), "gajaeway-update-held-"));
		const holder = await acquireLease(home, "update");
		try {
			await expect(
				runUpdate({
					args: { check: false, force: false, noRestart: false, binDir: dir },
					home,
					deps: {
						fetchLatestRelease: async () => release("v0.2.0", [DARWIN_ASSET]),
						downloadAsset: async () => {
							throw new Error("a fenced update must not download");
						},
						platform: "darwin",
						arch: "arm64",
					},
				}),
			).rejects.toThrow("another gajaeway update is running (lease_held name=update");
			expect((await readFile(join(dir, "gajaeway"), "utf8")).startsWith("old-")).toBe(true);
		} finally {
			await holder.release();
			await rm(dir, { recursive: true, force: true });
			await rm(home, { recursive: true, force: true });
		}
	});
});

describe("update dispatch through main", () => {
	test("gajaeway update --check prints availability without a gateway socket", async () => {
		const output: string[] = [];
		const originalLog = console.log;
		console.log = (line: unknown) => output.push(String(line));
		try {
			await main(["update", "--check"], {
				update: {
					fetchLatestRelease: (async () => release("v99.0.0", [DARWIN_ASSET])) satisfies ReleaseFetcher,
					platform: "darwin",
					arch: "arm64",
					execPath: "/usr/local/bin/bun",
				},
			});
		} finally {
			console.log = originalLog;
		}
		expect(output.join("\n")).toContain("update available:");
	});

	test("an unknown update flag is refused with the usage", async () => {
		const errors: string[] = [];
		const originalError = console.error;
		console.error = (message: unknown) => errors.push(String(message));
		const previousExit = process.exitCode;
		try {
			await main(["update", "--wat"]);
			expect(errors.join("\n")).toContain("unknown option: --wat");
			expect(process.exitCode).toBe(1);
		} finally {
			console.error = originalError;
			process.exitCode = previousExit ?? 0;
		}
	});
	test("the update command exits nonzero when another update owns the lease", async () => {
		const dir = await mkdtemp(join(tmpdir(), "gajaeway-update-cli-bin-"));
		const home = await mkdtemp(join(tmpdir(), "gajaeway-update-cli-held-"));
		const previousHome = process.env.GAJAEWAY_HOME;
		const previousExitCode = process.exitCode;
		const originalError = console.error;
		const errors: string[] = [];
		const holder = await acquireLease(home, "update");
		try {
			process.env.GAJAEWAY_HOME = home;
			console.error = (message: unknown) => errors.push(String(message));
			await main(["update", "--bin-dir", dir], {
				update: {
					fetchLatestRelease: async () => release("v99.0.0", [DARWIN_ASSET]),
					platform: "darwin",
					arch: "arm64",
				},
			});
			expect(process.exitCode).toBe(1);
			expect(errors.join("\n")).toContain("another gajaeway update is running (lease_held name=update");
			expect(await readdir(dir)).toEqual([]);
		} finally {
			console.error = originalError;
			process.exitCode = previousExitCode;
			if (previousHome === undefined) delete process.env.GAJAEWAY_HOME;
			else process.env.GAJAEWAY_HOME = previousHome;
			await holder.release();
			await rm(dir, { recursive: true, force: true });
			await rm(home, { recursive: true, force: true });
		}
	});
});
