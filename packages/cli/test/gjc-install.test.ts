import { expect, test } from "bun:test";
import { mkdir, mkdtemp, readdir, readFile, readlink, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { homeLayout } from "@gajae-gateway/home";
import { type GjcInstallRunner, installPinnedGjc } from "../src/gjc-install";

async function home(): Promise<string> {
	return await mkdtemp(join(tmpdir(), "gajaeway-gjc-install-"));
}

function installRunner(): { runner: GjcInstallRunner; commands: string[][] } {
	const commands: string[][] = [];
	const runner: GjcInstallRunner = async (command, cwd) => {
		commands.push([...command]);
		if (command[0] === "bun") {
			const binDir = join(cwd, "node_modules", ".bin");
			await mkdir(binDir, { recursive: true });
			await writeFile(join(binDir, "gjc"), "gjc shim");
			return { exitCode: 0, stdout: "", stderr: "" };
		}
		const packageJson = JSON.parse(await readFile(join(cwd, "package.json"), "utf8")) as {
			dependencies: { "gajae-code": string };
		};
		return { exitCode: 0, stdout: `gjc/${packageJson.dependencies["gajae-code"]}`, stderr: "" };
	};
	return { runner, commands };
}

test("installPinnedGjc installs in a versioned prefix, verifies it, and flips current", async () => {
	const directory = await home();
	const { runner, commands } = installRunner();
	try {
		const executable = await installPinnedGjc({ home: directory, version: "0.18.8", runner });
		const layout = homeLayout(directory);
		expect(executable).toBe(layout.gjcExecutable());
		expect(await readlink(layout.gjcCurrent)).toBe("0.18.8");
		expect(await readlink(layout.gjcExecutable("0.18.8"))).toBe("../node_modules/.bin/gjc");
		expect(commands[0]).toEqual(["bun", "install", "--production"]);
		expect(commands[1]?.[0]).toMatch(/\/\.staging-[^/]+\/bin\/gjc$/);
		expect(commands[1]?.[1]).toBe("--version");
		expect(await readdir(layout.gjcPrefix)).toContain("0.18.8");
		expect((await readdir(layout.gjcPrefix)).some((entry) => entry.startsWith(".staging-"))).toBe(false);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("a failed isolated install leaves the active current link unchanged and removes staging", async () => {
	const directory = await home();
	const { runner } = installRunner();
	try {
		const layout = homeLayout(directory);
		await installPinnedGjc({ home: directory, version: "0.18.8", runner });
		const failing: GjcInstallRunner = async () => ({ exitCode: 1, stdout: "", stderr: "registry unavailable" });
		await expect(installPinnedGjc({ home: directory, version: "0.19.0", runner: failing })).rejects.toThrow(
			"registry unavailable",
		);
		expect(await readlink(layout.gjcCurrent)).toBe("0.18.8");
		expect(await readdir(layout.gjcPrefix)).toEqual(["0.18.8", "current"]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});

test("installPinnedGjc prunes old versions while retaining current and its predecessor", async () => {
	const directory = await home();
	const { runner } = installRunner();
	try {
		for (const version of ["0.18.8", "0.19.0", "0.20.0"]) await installPinnedGjc({ home: directory, version, runner });
		const layout = homeLayout(directory);
		expect(await readlink(layout.gjcCurrent)).toBe("0.20.0");
		expect((await readdir(layout.gjcPrefix)).sort()).toEqual(["0.19.0", "0.20.0", "current"]);
	} finally {
		await rm(directory, { recursive: true, force: true });
	}
});
