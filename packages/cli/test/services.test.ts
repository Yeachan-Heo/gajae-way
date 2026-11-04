import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, readlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import type { GjcInstallRunner } from "../src/gjc-install";
import {
	BROKER_UNIT,
	discoverLoginPath,
	GATEWAY_UNIT,
	homeForEnvironment,
	installPinnedGjcForServices,
	installServices,
	restartStackCommands,
	servicePinCandidates,
	serviceSpecs,
	systemdUnitName,
} from "../src/services";

async function configuredHome(): Promise<{ home: string; cleanup: () => Promise<void> }> {
	const home = await mkdtemp(join(tmpdir(), "gajaeway-services-"));
	await writeFile(join(home, "config.json"), JSON.stringify({ schemaVersion: 1 }), "utf8");
	return { home, cleanup: () => rm(home, { recursive: true, force: true }) };
}

async function generate(platform: "darwin" | "linux") {
	const { home, cleanup } = await configuredHome();
	const binDir = join(home, "bin");
	await mkdir(binDir);
	for (const spec of serviceSpecs()) await writeFile(join(binDir, spec.binary), "stub");
	const written = new Map<string, string>();
	const paths = await installServices({
		binDir,
		platform,
		unitDir: join(home, "units"),
		launchAgentsDir: join(home, "agents"),
		env: { HOME: home, GAJAEWAY_HOME: home, SHELL: "/bin/sh" },
		loginPath: undefined,
		loginPathRunner: () => "/usr/bin:/bin",
		writeFile: (path, contents) => {
			written.set(path, contents);
		},
	} as Parameters<typeof installServices>[0]);
	return { home, paths, written, cleanup };
}

test("a linux install writes one bound systemd user unit per service", async () => {
	const { home, paths, written, cleanup } = await generate("linux");
	try {
		expect(paths).toHaveLength(serviceSpecs().length);
		for (const spec of serviceSpecs()) {
			const unitPath = join(home, "units", systemdUnitName(spec));
			expect(paths).toContain(unitPath);
			const unit = written.get(unitPath) ?? "";
			expect(unit).toContain("[Service]");
			expect(unit).toContain(`Environment="GAJAEWAY_HOME=${home}"`);
			if (spec.id === "broker") {
				expect(unit).toContain(`ConditionPathExists=${join(home, "broker", "provision", "manifest.json")}\n`);
				expect(unit).toContain(`EnvironmentFile=-${join(home, "broker.env")}\n`);
				expect(unit).toContain("Restart=always");
				expect(unit).toContain("RestartSec=2s");
				expect(unit).toContain("KillMode=control-group");
				expect(unit).toContain("SuccessExitStatus=143\n");
				expect(unit).toContain("TimeoutStartSec=60s");
				expect(unit).toContain("TimeoutStopSec=30s");
			} else if (spec.id === "gateway") {
				expect(unit).toContain(`Wants=${BROKER_UNIT}`);
				expect(unit).toContain(`After=${BROKER_UNIT}`);
				expect(unit).not.toContain("Requires=");
				expect(unit).toContain("KillMode=mixed");
				expect(unit).toContain("TimeoutStartSec=60s");
				expect(unit).toContain("TimeoutStopSec=40s");
			} else {
				expect(unit).toContain(`BindsTo=${GATEWAY_UNIT}`);
				expect(unit).toContain(`After=${GATEWAY_UNIT}`);
				expect(unit).toContain(`PartOf=${GATEWAY_UNIT}`);
				expect(unit).toContain(`WantedBy=${GATEWAY_UNIT}`);
				expect(unit).not.toContain("KillMode=process");
				expect(unit).toContain("TimeoutStartSec=60s");
				expect(unit).toContain("TimeoutStopSec=60s");
			}
		}
	} finally {
		await cleanup();
	}
});

test("a darwin install still writes launch agents, not units", async () => {
	const { home, paths, written, cleanup } = await generate("darwin");
	try {
		expect(paths).toHaveLength(serviceSpecs().length);
		for (const path of paths) {
			expect(path.startsWith(join(home, "agents"))).toBe(true);
			expect(path.endsWith(".plist")).toBe(true);
			const contents = [...written.values()].find((candidate) =>
				candidate.includes(path.split("/").at(-1)?.replace(".plist", "") ?? ""),
			);
			expect(contents).toBeDefined();
			expect(contents).toContain("<key>RunAtLoad</key><true/>");
			expect(contents).toContain("<key>KeepAlive</key><true/>");
			expect(contents).not.toContain("AbandonProcessGroup");
		}
	} finally {
		await cleanup();
	}
});

test("a darwin update skips a launch agent when its binary is missing", async () => {
	const { home, cleanup } = await configuredHome();
	const binDir = join(home, "bin");
	const written = new Map<string, string>();
	try {
		await mkdir(binDir);
		await writeFile(join(binDir, "gajaeway-gateway"), "gateway stub");
		const paths = await installServices({
			binDir,
			platform: "darwin",
			launchAgentsDir: join(home, "agents"),
			env: { HOME: home, GAJAEWAY_HOME: home },
			configuredAdaptersOnly: true,
			loginPathRunner: () => "/usr/bin:/bin",
			writeFile: (path, contents) => {
				written.set(path, contents);
			},
		});

		expect(paths).toContain(join(home, "agents", "dev.gajaeway.gateway.plist"));
		expect(paths).not.toContain(join(home, "agents", "dev.gajaeway.admin.plist"));
		expect(written.has(join(home, "agents", "dev.gajaeway.admin.plist"))).toBe(false);
	} finally {
		await cleanup();
	}
});

test("compareAndWrite leaves unchanged service definitions untouched", async () => {
	const { home, cleanup } = await configuredHome();
	const unitDir = join(home, "units");
	const binDir = join(home, "bin");
	let writes = 0;
	const write = async (path: string, contents: string) => {
		writes++;
		await writeFile(path, contents, "utf8");
	};
	const options = {
		binDir,
		unitDir,
		platform: "linux" as const,
		env: { HOME: home, GAJAEWAY_HOME: home },
		loginPathRunner: () => "/usr/bin:/bin",
		writeFile: write,
	};
	try {
		await mkdir(binDir);
		for (const spec of serviceSpecs()) await writeFile(join(binDir, spec.binary), "stub");
		await installServices(options);
		expect(writes).toBe(serviceSpecs().length);
		writes = 0;
		await installServices({ ...options, compareAndWrite: true });
		expect(writes).toBe(0);
	} finally {
		await cleanup();
	}
});
test("an unsupported platform is refused by name", async () => {
	const { home, cleanup } = await configuredHome();
	try {
		await expect(
			installServices({
				binDir: "bin",
				platform: "windows" as never,
				env: { HOME: home, GAJAEWAY_HOME: home },
			}),
		).rejects.toThrow("unsupported service platform");
	} finally {
		await cleanup();
	}
});

test("restart-stack is ordered systemctl calls on linux and an ordered kickstart chain on darwin", () => {
	const linuxCmds = restartStackCommands("linux");
	expect(linuxCmds).toEqual([
		["systemctl", "--user", "restart", "gajaeway-broker.service"],
		["systemctl", "--user", "restart", "gajaeway-gateway.service"],
	]);
	const darwinCmds = restartStackCommands("darwin");
	expect(darwinCmds[0][0]).toBe("launchctl");
	expect(darwinCmds[0][3]).toContain("dev.gajaeway.broker");
	const darwinCmds42 = restartStackCommands("darwin", 42);
	expect(darwinCmds42[0]).toEqual(["launchctl", "kickstart", "-k", "gui/42/dev.gajaeway.broker"]);
});

function recordingRunner(succeedOn?: string) {
	const ran: string[] = [];
	const runner = async (command: readonly string[]) => {
		ran.push(command[0] ?? "");
		expect(command.slice(1)).toEqual(["-lc", 'printf %s "$PATH"']);
		if (command[0] === succeedOn) return `PATH from ${succeedOn}`;
		throw new Error(`ENOENT: posix_spawn '${command[0]}'`);
	};
	return { ran, runner };
}

test("discoverLoginPath uses SHELL when set and executable", async () => {
	const { ran, runner } = recordingRunner("/bin/bash");
	const result = await discoverLoginPath({ SHELL: "/bin/bash" }, runner);
	expect(result).toBe("PATH from /bin/bash");
	expect(ran).toEqual(["/bin/bash"]);
});

test("discoverLoginPath with SHELL unset uses the passwd shell, never /bin/zsh", async () => {
	const { ran, runner } = recordingRunner("/usr/bin/fish");
	const result = await discoverLoginPath({}, runner, () => "/usr/bin/fish");
	expect(result).toBe("PATH from /usr/bin/fish");
	expect(ran).toEqual(["/usr/bin/fish"]);
});

test("discoverLoginPath falls back SHELL -> passwd shell -> /bin/bash -> /bin/sh", async () => {
	const { ran, runner } = recordingRunner("/bin/sh");
	const result = await discoverLoginPath({ SHELL: "/opt/missing/zsh" }, runner, () => "/usr/bin/fish");
	expect(result).toBe("PATH from /bin/sh");
	expect(ran).toEqual(["/opt/missing/zsh", "/usr/bin/fish", "/bin/bash", "/bin/sh"]);
});

test("discoverLoginPath tries each shell once when SHELL equals the passwd shell", async () => {
	const { ran, runner } = recordingRunner();
	await discoverLoginPath({ SHELL: "/bin/bash" }, runner, () => "/bin/bash");
	expect(ran).toEqual(["/bin/bash", "/bin/sh"]);
});

test("discoverLoginPath returns an empty login PATH instead of throwing when no shell runs", async () => {
	const { ran, runner } = recordingRunner();
	expect(await discoverLoginPath({}, runner, () => undefined)).toBe("");
	expect(ran).toEqual(["/bin/bash", "/bin/sh"]);
	expect(ran).not.toContain("/bin/zsh");
});

test("installServices succeeds even when shell discovery returns empty", async () => {
	const { home, cleanup } = await configuredHome();
	const binDir = join(home, "bin");
	const written = new Map<string, string>();
	try {
		await mkdir(binDir);
		for (const spec of serviceSpecs()) await writeFile(join(binDir, spec.binary), "stub");

		const failingRunner = async () => {
			throw new Error("shell discovery failed");
		};

		const paths = await installServices({
			binDir,
			platform: "linux",
			unitDir: join(home, "units"),
			env: { HOME: home, GAJAEWAY_HOME: home },
			loginPathRunner: failingRunner,
			writeFile: (path, contents) => {
				written.set(path, contents);
			},
		});

		// Should still write all units using FALLBACK_PATH_DIRS.
		expect(paths).toHaveLength(serviceSpecs().length);
		expect(written.size).toBe(serviceSpecs().length);

		// Verify at least one unit was written and contains fallback paths.
		const firstUnit = [...written.values()][0] ?? "";
		expect(firstUnit).toContain("/usr/local/bin");
		expect(firstUnit).toContain("/usr/bin");
		expect(firstUnit).toContain("/bin");
	} finally {
		await cleanup();
	}
});

function stubGjcRunner(installed: string[]): GjcInstallRunner {
	return async (command, cwd) => {
		if (command[0] === "bun") {
			const dependency = (
				JSON.parse(await readFile(join(cwd, "package.json"), "utf8")) as {
					dependencies: { "gajae-code": string };
				}
			).dependencies["gajae-code"];
			installed.push(dependency);
			await mkdir(join(cwd, "node_modules", ".bin"), { recursive: true });
			await writeFile(join(cwd, "node_modules", ".bin", "gjc"), dependency);
			return { exitCode: 0, stdout: "", stderr: "" };
		}
		return { exitCode: 0, stdout: `gjc/${await readFile(command[0] as string, "utf8")}`, stderr: "" };
	};
}

async function pinFile(dir: string, name: string, version?: string): Promise<string> {
	const path = join(dir, name);
	await writeFile(path, JSON.stringify(version === undefined ? { name } : { name, gjc: { version } }));
	return path;
}

test("installPinnedGjcForServices installs the first candidate that carries a pin", async () => {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-service-pin-"));
	try {
		const installed: string[] = [];
		const candidates = [
			join(dir, "missing.json"),
			await pinFile(dir, "nopin.json"),
			await pinFile(dir, "checkout.json", "0.19.3"),
			await pinFile(dir, "release.json", "0.19.0"),
		];
		const version = await installPinnedGjcForServices({ home: dir, candidates, runner: stubGjcRunner(installed) });
		expect(version).toBe("0.19.3");
		expect(installed).toEqual(["0.19.3"]);
		expect(await readlink(homeLayout(dir).gjcCurrent)).toBe("0.19.3");
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("installPinnedGjcForServices does not fall through to a later pin when the install fails", async () => {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-service-pin-"));
	try {
		const commands: string[][] = [];
		const failing: GjcInstallRunner = async (command) => {
			commands.push([...command]);
			return { exitCode: 1, stdout: "", stderr: "registry unavailable" };
		};
		const candidates = [await pinFile(dir, "checkout.json", "0.19.3"), await pinFile(dir, "release.json", "0.19.0")];
		await expect(installPinnedGjcForServices({ home: dir, candidates, runner: failing })).rejects.toThrow(
			"registry unavailable",
		);
		expect(commands).toHaveLength(1);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("installPinnedGjcForServices names every candidate when none carries a pin", async () => {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-service-pin-"));
	try {
		const candidates = [join(dir, "missing.json"), await pinFile(dir, "nopin.json")];
		await expect(installPinnedGjcForServices({ home: dir, candidates, runner: stubGjcRunner([]) })).rejects.toThrow(
			`could not find a pinned gjc version in any of: ${candidates.join(", ")}`,
		);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("servicePinCandidates checks the binDir package before the checkout gateway package (Finding 1)", () => {
	const [binDirCandidate, checkoutCandidate] = servicePinCandidates("~/bin", { HOME: "/home/op" });
	expect(binDirCandidate).toBe("/home/op/bin/package.json");
	expect(checkoutCandidate).toBe(join(import.meta.dir, "..", "..", "gateway", "package.json"));
});

test("servicePinCandidates uses the bin directory from the passed environment (Finding 1 - env-based fallback)", () => {
	const candidates = servicePinCandidates("/opt/binaries", { HOME: "/nonstandard" });
	expect(candidates[0]).toBe("/opt/binaries/package.json");
});

test("installPinnedGjcForServices uses binDir pin when available, falls back to checkout only when no manifest (Finding 1)", async () => {
	const dir = await mkdtemp(join(tmpdir(), "gajaeway-service-pin-order-"));
	try {
		const installed: string[] = [];
		// Test case: binDir has a pin, should be used
		const binDirCandidatePath = await pinFile(dir, "bindir.json", "0.19.5");
		const checkoutCandidatePath = await pinFile(dir, "checkout.json", "0.19.3");
		const version = await installPinnedGjcForServices({
			home: dir,
			candidates: [binDirCandidatePath, checkoutCandidatePath],
			runner: stubGjcRunner(installed),
		});
		expect(version).toBe("0.19.5");
		expect(installed).toEqual(["0.19.5"]);
	} finally {
		await rm(dir, { recursive: true, force: true });
	}
});

test("homeForEnvironment resolves from GAJAEWAY_HOME when set (Finding 3)", () => {
	const home = homeForEnvironment({ GAJAEWAY_HOME: "/custom/gajaeway" });
	expect(home).toBe("/custom/gajaeway");
});

test("homeForEnvironment falls back to HOME/.gajaeway when GAJAEWAY_HOME is not set (Finding 3)", () => {
	const home = homeForEnvironment({ HOME: "/home/user" });
	expect(home).toBe("/home/user/.gajaeway");
});

test("homeForEnvironment uses default home dir when both GAJAEWAY_HOME and HOME are missing (Finding 3)", () => {
	const home = homeForEnvironment({});
	expect(home).toMatch(/\.gajaeway$/);
	// We can't check the exact path since homedir() depends on the system,
	// but we can verify it ends with .gajaeway
});

test("homeForEnvironment expands empty GAJAEWAY_HOME to HOME/.gajaeway (Finding 2 - consistent with update)", () => {
	// When GAJAEWAY_HOME is explicitly set to empty string, it should fall back to HOME/.gajaeway
	// This ensures services and update use the same lease home
	const home = homeForEnvironment({ GAJAEWAY_HOME: "", HOME: "/home/operator" });
	expect(home).toBe("/home/operator/.gajaeway");
});

test("servicePinCandidates prefers sidecar manifest before checkout (Finding 1)", () => {
	// Both services installer and broker runtime should prefer the sidecar manifest (in binDir)
	// over the checkout manifest to ensure consistent pin enforcement
	const candidates = servicePinCandidates("/app/bin", { HOME: "/home/op" });
	
	// First candidate should be the sidecar (in binDir)
	expect(candidates[0]).toContain("/app/bin");
	expect(candidates[0]).toEndWith("package.json");
	
	// Second candidate should be the checkout gateway package
	expect(candidates[1]).toContain("gateway/package.json");
});
