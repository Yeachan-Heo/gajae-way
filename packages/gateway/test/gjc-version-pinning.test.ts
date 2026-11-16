import { describe, expect, it } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import {
	isCompiledRelease,
	pinnedGjcCandidatePaths,
	preflightGjcRuntime,
	readPinnedGjcVersion,
} from "../src/orchestrator/broker";
import { isEmbeddedModuleDir } from "../src/orchestrator/embedded-module";

/** One installed release plus a checkout that still pins a different version. */
interface PinLayout {
	readonly root: string;
	readonly execPath: string;
	readonly releaseManifest: string;
	readonly checkoutManifest: string;
	readonly sourceModulePath: string;
}

const CHECKOUT_PIN = "8.8.8";

async function pinLayout(): Promise<PinLayout> {
	const root = await mkdtemp(join(tmpdir(), "gajaeway-gjc-pin-"));
	const execPath = join(root, "bin", "gajaeway-gateway");
	await mkdir(dirname(execPath), { recursive: true });
	await writeFile(execPath, "");
	const checkoutManifest = join(root, "checkout", "packages", "gateway", "package.json");
	await mkdir(dirname(checkoutManifest), { recursive: true });
	await writeFile(checkoutManifest, JSON.stringify({ name: "@gajae-gateway/gateway", gjc: { version: CHECKOUT_PIN } }));
	return {
		root,
		execPath,
		releaseManifest: join(root, "bin", "package.json"),
		checkoutManifest,
		sourceModulePath: join(root, "checkout", "packages", "gateway", "src", "orchestrator", "broker.ts"),
	};
}

/** The pin inputs of one delivery mode: a compiled release or a source checkout run. */
function pinInputs(
	layout: PinLayout,
	mode: "compiled" | "source",
): { readonly compiled: boolean; readonly modulePath: string; readonly candidates: readonly string[] } {
	const modulePath = mode === "compiled" ? "/$bunfs/root/gajaeway-gateway" : layout.sourceModulePath;
	return {
		compiled: isCompiledRelease(modulePath),
		modulePath,
		candidates: pinnedGjcCandidatePaths(layout.execPath, modulePath),
	};
}

/** Ways the release manifest beside a binary can fail to supply a pin. */
const BROKEN_RELEASE_MANIFESTS: readonly { readonly state: string; readonly write: (path: string) => Promise<void> }[] =
	[
		{ state: "missing", write: async () => {} },
		{ state: "malformed", write: (path) => writeFile(path, "{ not json") },
		{ state: "pin-less", write: (path) => writeFile(path, JSON.stringify({ name: "gajaeway" })) },
	];

describe("GJC version pinning", () => {
	it("reads pinned gjc version from gateway package.json", () => {
		const version = readPinnedGjcVersion();
		expect(version).toMatch(/^\d+\.\d+\.\d+$/);
		// Version is dynamic; verify it matches what we expect from package.json
		expect(version.length).toBeGreaterThan(0);
	});

	it("validates exact version match with pinnedVersion option", async () => {
		const pinnedVersion = readPinnedGjcVersion();
		const mockRun: CliRunner = async (args) => {
			if (args[0] === "--version") {
				return { exitCode: 0, stdout: `gjc/${pinnedVersion}\n`, stderr: "" };
			}
			return { exitCode: 1, stdout: "", stderr: "unknown command" };
		};

		const result = await preflightGjcRuntime(mockRun, "0.16.0", undefined, {
			pinnedVersion,
		});

		expect(result.version).toBe(pinnedVersion);
	});

	it("fails when running version doesn't match pinned version", async () => {
		const pinnedVersion = readPinnedGjcVersion();
		const mockRun: CliRunner = async (args) => {
			if (args[0] === "--version") {
				return { exitCode: 0, stdout: "gjc/0.16.2\n", stderr: "" };
			}
			return { exitCode: 1, stdout: "", stderr: "unknown command" };
		};

		await expect(
			preflightGjcRuntime(mockRun, "0.16.0", undefined, {
				pinnedVersion,
			}),
		).rejects.toThrow(/version mismatch.*Run 'gajaeway update'/);
	});

	it("treats the pin as a floor so a gjc upgraded underneath the gateway still boots", async () => {
		const runVersion =
			(version: string): CliRunner =>
			async (args) =>
				args[0] === "--version"
					? { exitCode: 0, stdout: `gjc/${version}\n`, stderr: "" }
					: { exitCode: 1, stdout: "", stderr: "unknown command" };
		const pinned = { pinnedVersion: "0.18.6" };
		for (const newer of ["0.18.7", "0.19.0", "1.0.0"])
			expect((await preflightGjcRuntime(runVersion(newer), "0.16.0", undefined, pinned)).version).toBe(newer);
		for (const older of ["0.18.5", "0.17.9"])
			await expect(preflightGjcRuntime(runVersion(older), "0.16.0", undefined, pinned)).rejects.toThrow(
				"requires >= 0.18.6",
			);
		await expect(
			preflightGjcRuntime(runVersion("0.18.6"), "0.16.0", undefined, { pinnedVersion: "0.18" }),
		).rejects.toThrow("invalid pinned version spec");
	});

	it("falls back to minimum version check when pinnedVersion is not set", async () => {
		const mockRun: CliRunner = async (args) => {
			if (args[0] === "--version") {
				return { exitCode: 0, stdout: "gjc/0.17.0\n", stderr: "" };
			}
			return { exitCode: 1, stdout: "", stderr: "unknown command" };
		};

		const result = await preflightGjcRuntime(mockRun, "0.16.0");

		expect(result.version).toBe("0.17.0");
	});

	it("fails when version is below minimum", async () => {
		const mockRun: CliRunner = async (args) => {
			if (args[0] === "--version") {
				return { exitCode: 0, stdout: "gjc/0.15.0\n", stderr: "" };
			}
			return { exitCode: 1, stdout: "", stderr: "unknown command" };
		};

		await expect(preflightGjcRuntime(mockRun, "0.16.0")).rejects.toThrow("requires gjc >= 0.16.0");
	});

	it("reads the pin beside an installed binary when the build-time checkout is gone", async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-gjc-pin-"));
		try {
			const beside = join(directory, "package.json");
			await writeFile(beside, JSON.stringify({ name: "@gajae-gateway/gateway", gjc: { version: "9.9.9" } }));
			expect(readPinnedGjcVersion([beside, join(directory, "absent", "package.json")])).toBe("9.9.9");
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("keeps the release beside the binary authoritative when the checkout manifest is readable too", async () => {
		const directory = await mkdtemp(join(tmpdir(), "gajaeway-gjc-pin-"));
		try {
			const beside = join(directory, "package.json");
			await writeFile(beside, JSON.stringify({ name: "gajaeway", gjc: { version: "9.9.9" } }));
			const candidates = pinnedGjcCandidatePaths(join(directory, "gajaeway-gateway"));
			// Both manifests are readable and disagree: the checkout candidate still pins the
			// version this repository builds against. The release beside the binary must win, or
			// the broker refuses its own install with broker_host_version_mismatch.
			const checkout = join(import.meta.dir, "..", "package.json");
			expect(candidates.at(-1)).toBe(checkout);
			expect(readPinnedGjcVersion([checkout])).not.toBe("9.9.9");
			expect(readPinnedGjcVersion(candidates)).toBe("9.9.9");
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("names every candidate it tried when none carries a gjc pin", () => {
		expect(() => readPinnedGjcVersion(["/nonexistent/package.json"])).toThrow("/nonexistent/package.json");
	});

	it("offers the running binary's own directory as the first pin location", () => {
		const candidates = pinnedGjcCandidatePaths("/opt/gajaeway/bin/gajaeway-gateway");
		expect(candidates[0]).toBe("/opt/gajaeway/bin/package.json");
		expect(candidates[1]).toBe(join(import.meta.dir, "..", "package.json"));
	});

	it("detects the delivery mode from how the running module is embedded", () => {
		expect(isCompiledRelease(join(import.meta.dir, "..", "src", "orchestrator", "broker.ts"))).toBe(false);
		expect(isCompiledRelease("/$bunfs/root/gajaeway-gateway")).toBe(true);
		expect(isEmbeddedModuleDir("C:\\~BUN\\root")).toBe(true);
	});

	it("gives a compiled release no checkout candidate", () => {
		const sourceModule = join(import.meta.dir, "..", "src", "orchestrator", "broker.ts");
		expect(pinnedGjcCandidatePaths("/opt/gajaeway/bin/gajaeway-gateway", "/$bunfs/root/gajaeway-gateway")).toEqual([
			"/opt/gajaeway/bin/package.json",
		]);
		expect(pinnedGjcCandidatePaths("/opt/gajaeway/bin/gajaeway-gateway", sourceModule)).toEqual([
			"/opt/gajaeway/bin/package.json",
			join(import.meta.dir, "..", "package.json"),
		]);
	});

	for (const broken of BROKEN_RELEASE_MANIFESTS) {
		it(`fails a compiled release closed when its release manifest is ${broken.state}`, async () => {
			const layout = await pinLayout();
			try {
				await broken.write(layout.releaseManifest);
				const { compiled, modulePath, candidates } = pinInputs(layout, "compiled");
				expect(compiled).toBe(true);
				expect(candidates).toEqual([layout.releaseManifest]);
				// The checkout manifest still pins a version, so this only fails if the compiled
				// release refuses to borrow it.
				expect(() => readPinnedGjcVersion(candidates, modulePath)).toThrow(layout.releaseManifest);
				expect(() => readPinnedGjcVersion(candidates, modulePath)).toThrow(
					`gajaeway update --force --bin-dir ${dirname(layout.execPath)}`,
				);
			} finally {
				await rm(layout.root, { recursive: true, force: true });
			}
		});

		it(`keeps the checkout fallback for a source run when its release manifest is ${broken.state}`, async () => {
			const layout = await pinLayout();
			try {
				await broken.write(layout.releaseManifest);
				const { compiled, modulePath, candidates } = pinInputs(layout, "source");
				expect(compiled).toBe(false);
				expect(candidates).toEqual([layout.releaseManifest, layout.checkoutManifest]);
				expect(readPinnedGjcVersion(candidates, modulePath)).toBe(CHECKOUT_PIN);
			} finally {
				await rm(layout.root, { recursive: true, force: true });
			}
		});
	}

	it("keeps a valid release manifest authoritative for a compiled release", async () => {
		const layout = await pinLayout();
		try {
			await writeFile(layout.releaseManifest, JSON.stringify({ name: "gajaeway", gjc: { version: "9.9.9" } }));
			const { compiled, modulePath, candidates } = pinInputs(layout, "compiled");
			expect(compiled).toBe(true);
			expect(readPinnedGjcVersion(candidates, modulePath)).toBe("9.9.9");
		} finally {
			await rm(layout.root, { recursive: true, force: true });
		}
	});
});
