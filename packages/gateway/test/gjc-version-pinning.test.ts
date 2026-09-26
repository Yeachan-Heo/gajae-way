import { describe, expect, it } from "bun:test";
import type { CliRunner } from "@gajae-gateway/subsession";
import { preflightGjcRuntime, readPinnedGjcVersion } from "../src/orchestrator/broker";

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
});
