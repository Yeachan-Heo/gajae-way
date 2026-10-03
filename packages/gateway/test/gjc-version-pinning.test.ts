import { describe, expect, it } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CliRunner } from "@gajae-gateway/subsession";
import { pinnedGjcCandidatePaths, preflightGjcRuntime, readPinnedGjcVersion } from "../src/orchestrator/broker";

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
		).rejects.toThrow("version mismatch");
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
			expect(readPinnedGjcVersion([join(directory, "absent", "package.json"), beside])).toBe("9.9.9");
		} finally {
			await rm(directory, { recursive: true, force: true });
		}
	});

	it("names every candidate it tried when none carries a gjc pin", () => {
		expect(() => readPinnedGjcVersion(["/nonexistent/package.json"])).toThrow("/nonexistent/package.json");
	});

	it("offers the running binary's own directory as a pin location", () => {
		expect(pinnedGjcCandidatePaths("/opt/gajaeway/bin/gajaeway-gateway")).toContain("/opt/gajaeway/bin/package.json");
	});
});
