import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { $ } from "bun";
import { validateGeneratedBindings } from "./gen-types";

const repoRoot = path.join(import.meta.dir, "..");
const rustDir = path.join(repoRoot, "crates", "way-core");
const nativeDir = path.join(repoRoot, "native");
const packageJsonPath = path.join(repoRoot, "package.json");
const binaryName = "way_core";
const supportedTargets = new Set(["darwin-arm64", "linux-x64", "linux-arm64"]);

const crossTarget = Bun.env.CROSS_TARGET;
const targetPlatform = Bun.env.TARGET_PLATFORM || process.platform;
const targetArch = Bun.env.TARGET_ARCH || process.arch;
const platformTag = `${targetPlatform}-${targetArch}`;
const isCrossCompile = Boolean(crossTarget) || platformTag !== `${process.platform}-${process.arch}`;

if (!supportedTargets.has(platformTag)) {
	throw new Error(`Unsupported native target ${platformTag}. Supported targets: ${[...supportedTargets].join(", ")}.`);
}

if (isCrossCompile && !crossTarget) {
	throw new Error(`Cross-building ${platformTag} requires CROSS_TARGET to name the Rust target triple.`);
}

function pinnedTargetCpu(): string {
	if (targetArch === "x64") return "x86-64-v2";
	return targetPlatform === "darwin" ? "apple-m1" : "generic";
}

// Never inherit the host CPU ISA: each published platform has one reproducible addon variant.
if (!Bun.env.RUSTFLAGS) {
	Bun.env.RUSTFLAGS = `-C target-cpu=${pinnedTargetCpu()}`;
}

async function cleanupStaleTemps(dir: string): Promise<void> {
	try {
		const entries = await fs.readdir(dir);
		await Promise.all(
			entries
				.filter(entry => entry.includes(".tmp.") || entry.includes(".old.") || entry.includes(".new."))
				.map(entry => fs.rm(path.join(dir, entry), { force: true, recursive: true })),
		);
	} catch (error) {
		if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
	}
}

async function installFileAtomically(source: string, destination: string): Promise<void> {
	const temporary = `${destination}.tmp.${process.pid}`;
	await fs.copyFile(source, temporary);

	try {
		await fs.rename(temporary, destination);
	} catch (renameError) {
		try {
			await fs.rm(destination, { force: true });
			await fs.rename(temporary, destination);
		} catch (fallbackError) {
			await fs.rm(temporary, { force: true });
			throw new Error(
				`Failed to atomically install ${path.basename(destination)}: ${
					fallbackError instanceof Error ? fallbackError.message : String(renameError)
				}`,
			);
		}
	}
}

async function resolveBuiltAddonPath(outputDir: string, canonicalFilename: string): Promise<string> {
	const entries = await fs.readdir(outputDir);
	if (entries.includes(canonicalFilename)) return path.join(outputDir, canonicalFilename);

	const candidates = entries.filter(
		entry => entry.startsWith(`${binaryName}.${targetPlatform}-${targetArch}`) && entry.endsWith(".node"),
	);
	if (candidates.length === 1) return path.join(outputDir, candidates[0]);
	if (candidates.length === 0) {
		throw new Error(
			`napi build emitted no addon for ${platformTag}. Expected ${canonicalFilename}; output contained: ${
				entries.join(", ") || "(empty)"
			}.`,
		);
	}
	throw new Error(`napi build emitted multiple addon candidates for ${platformTag}: ${candidates.join(", ")}.`);
}

function prependPathEntry(currentPath: string, entry: string): string {
	const separator = process.platform === "win32" ? ";" : ":";
	return [entry, ...currentPath.split(separator).filter(candidate => candidate && candidate !== entry)].join(separator);
}

async function resolveCargoToolchainPath(): Promise<string | null> {
	const currentPath = Bun.env.PATH ?? "";
	const cargoHome = Bun.env.CARGO_HOME || path.join(os.homedir(), ".cargo");
	const rustupCandidates = [
		Bun.which("rustup", { PATH: currentPath }),
		path.join(cargoHome, "bin", process.platform === "win32" ? "rustup.exe" : "rustup"),
	];

	for (const rustup of [...new Set(rustupCandidates.filter((candidate): candidate is string => Boolean(candidate)))]) {
		const result = await $`${rustup} which cargo`.cwd(repoRoot).quiet().nothrow();
		if (result.exitCode !== 0) continue;
		const cargoBinary = result.stdout.toString("utf8").trim();
		if (cargoBinary) return prependPathEntry(currentPath, path.dirname(cargoBinary));
	}

	const cargoBinary = Bun.which("cargo", { PATH: currentPath });
	return cargoBinary ? prependPathEntry(currentPath, path.dirname(cargoBinary)) : null;
}

const profile = Bun.env.CI || isCrossCompile ? "release" : "local";
const canonicalAddonFilename = `${binaryName}.${platformTag}.node`;
const canonicalAddonPath = path.join(nativeDir, canonicalAddonFilename);
const napiBin = Bun.which("napi", {
	PATH: `${path.join(repoRoot, "node_modules", ".bin")}:${process.env.PATH ?? ""}`,
});

if (!napiBin) {
	throw new Error("Could not locate @napi-rs/cli `napi` in node_modules/.bin. Run bun install first.");
}

const cargoPath = await resolveCargoToolchainPath();
if (!cargoPath) {
	throw new Error("Could not locate Cargo. Install Rust with rustup or put cargo on PATH.");
}
Bun.env.PATH = cargoPath;

await fs.mkdir(nativeDir, { recursive: true });
await cleanupStaleTemps(nativeDir);
await fs.mkdir(path.join(nativeDir, ".build"), { recursive: true });
const outputDir = await fs.mkdtemp(path.join(nativeDir, ".build", `${platformTag}-${profile}-`));

const napiArgs = [
	"build",
	"--manifest-path",
	path.join(rustDir, "Cargo.toml"),
	"--package-json-path",
	packageJsonPath,
	"--platform",
	"--no-js",
	"--dts",
	"index.d.ts",
	"-o",
	outputDir,
	"--profile",
	profile,
];
if (crossTarget) napiArgs.push("--target", crossTarget);

console.log(`Building ${binaryName} for ${platformTag} (${profile}, target-cpu=${pinnedTargetCpu()})…`);
try {
	const result = await $`${napiBin} ${napiArgs}`.nothrow();
	if (result.exitCode !== 0) {
		const details = [result.stderr, result.stdout]
			.map(stream => stream.toString("utf8").trim())
			.filter(Boolean)
			.join("\n");
		throw new Error(`napi build failed${details ? `:\n${details}` : ""}`);
	}

	const builtAddon = await resolveBuiltAddonPath(outputDir, canonicalAddonFilename);
	await installFileAtomically(builtAddon, canonicalAddonPath);
	await installFileAtomically(path.join(outputDir, "index.d.ts"), path.join(nativeDir, "index.d.ts"));
	await fs.writeFile(
		`${canonicalAddonPath}.build.json`,
		`${JSON.stringify({ profile, targetCpu: pinnedTargetCpu(), builtAt: new Date().toISOString() }, null, 2)}\n`,
	);
	await validateGeneratedBindings(nativeDir);
	console.log(`Installed ${path.relative(repoRoot, canonicalAddonPath)}.`);
} finally {
	await fs.rm(outputDir, { force: true, recursive: true });
}
