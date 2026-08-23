import * as fs from "node:fs/promises";
import * as path from "node:path";

type Executable = {
	name: "gajaeway" | "gajaeway-discord" | "gajaeway-telegram";
	entrypoint: string;
};

const repoRoot = path.join(import.meta.dir, "..");
const nativeDir = path.join(repoRoot, "native");
const distDir = path.join(repoRoot, "dist");
const targetPlatform = Bun.env.TARGET_PLATFORM || process.platform;
const targetArch = Bun.env.TARGET_ARCH || process.arch;
const platformTag = `${targetPlatform}-${targetArch}`;
const bunTargets: Record<string, string> = {
	"darwin-arm64": "bun-darwin-arm64",
	"linux-x64": "bun-linux-x64",
	"linux-arm64": "bun-linux-arm64",
};
const executables: readonly Executable[] = [
	{ name: "gajaeway", entrypoint: "src/main.ts" },
	{ name: "gajaeway-discord", entrypoint: "src/adapter/main.ts" },
];

if (!bunTargets[platformTag]) {
	throw new Error(`Unsupported compile target ${platformTag}. Supported targets: ${Object.keys(bunTargets).join(", ")}.`);
}

async function runCommand(command: string[], env: NodeJS.ProcessEnv = Bun.env): Promise<void> {
	const processHandle = Bun.spawn(command, {
		cwd: repoRoot,
		env,
		stdout: "inherit",
		stderr: "inherit",
	});
	const exitCode = await processHandle.exited;
	if (exitCode !== 0) throw new Error(`Command failed with exit code ${exitCode}: ${command.join(" ")}`);
}

async function ensureNativeAddon(): Promise<void> {
	const addonPath = path.join(nativeDir, `way_core.${platformTag}.node`);
	try {
		await fs.access(addonPath);
	} catch {
		throw new Error(`Missing ${addonPath}. Run bun scripts/build-native.ts before compiling.`);
	}
}

async function compileExecutable(executable: Executable): Promise<void> {
	const outputPath = path.join(distDir, executable.name);
	const embedEnv = { ...Bun.env, TARGET_PLATFORM: targetPlatform, TARGET_ARCH: targetArch };
	await runCommand(["bun", "scripts/embed-native.ts", "--for", executable.name], embedEnv);
	try {
		const compileEnv =
			targetPlatform === "darwin" && process.platform === "darwin"
				? { ...Bun.env, BUN_NO_CODESIGN_MACHO_BINARY: "1" }
				: Bun.env;
		await runCommand(
			[
				"bun",
				"build",
				"--compile",
				"--minify",
				"--keep-names",
				"--no-compile-autoload-bunfig",
				"--no-compile-autoload-dotenv",
				"--no-compile-autoload-tsconfig",

				"--no-compile-autoload-package-json",
				// The published SDK intentionally leaves MuPDF external: its package
				// has top-level await behind a CommonJS require and Bun cannot compile
				// that edge into a standalone executable. This matches Gajae-Code's
				// own supported compile policy.
				"--external",
				"mupdf",
				"--root",
				".",
				"--target",
				bunTargets[platformTag],
				executable.entrypoint,
				"--outfile",
				outputPath,
			],
			compileEnv,
		);
		if (targetPlatform === "darwin" && process.platform === "darwin") {
			await runCommand(["codesign", "--force", "--sign", "-", outputPath]);
		}
	} finally {
		await runCommand(["bun", "scripts/embed-native.ts", "--reset"]);
	}
}

await ensureNativeAddon();
await fs.mkdir(distDir, { recursive: true });
await Promise.all(
	(await fs.readdir(distDir)).filter((entry) => entry.startsWith("way")).map((entry) => fs.rm(path.join(distDir, entry), { force: true, recursive: true })),
);
for (const executable of executables) {
	console.log(`Compiling ${executable.name} for ${platformTag}…`);
	await compileExecutable(executable);
}
