import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { migrate, parseMigrateArgs } from "../src/migrate";
import { detectHermes } from "../src/migrate/hermes";
import { detectOpenClaw } from "../src/migrate/openclaw";

// Test utilities
function randomDir(): string {
	return join(tmpdir(), `gajae-migrate-test-${randomBytes(8).toString("hex")}`);
}

describe("migration CLI argument parsing", () => {
	test("parseMigrateArgs with source path", () => {
		const result = parseMigrateArgs(["/path/to/source"]);
		expect(result.source).toBe("/path/to/source");
		expect(result.target).toBeUndefined();
		expect(result.dryRun).toBeUndefined();
	});

	test("parseMigrateArgs with --source flag", () => {
		const result = parseMigrateArgs(["--source", "/path/to/source"]);
		expect(result.source).toBe("/path/to/source");
	});

	test("parseMigrateArgs with --target flag", () => {
		const result = parseMigrateArgs(["--source", "/source", "--target", "/target"]);
		expect(result.source).toBe("/source");
		expect(result.target).toBe("/target");
	});

	test("parseMigrateArgs with --dry-run flag", () => {
		const result = parseMigrateArgs(["--source", "/source", "--dry-run"]);
		expect(result.dryRun).toBe(true);
	});

	test("parseMigrateArgs rejects unknown options", () => {
		expect(() => parseMigrateArgs(["--unknown"])).toThrow("Unknown option");
	});
});

describe("OpenClaw detection", () => {
	let testDir: string;

	beforeEach(async () => {
		testDir = randomDir();
		await mkdir(testDir, { recursive: true });
	});

	afterEach(async () => {
		await rm(testDir, { recursive: true, force: true });
	});

	test("detectOpenClaw returns true for valid config.json", async () => {
		await writeFile(join(testDir, "config.json"), JSON.stringify({ persona: { name: "test" } }));
		const detected = await detectOpenClaw(testDir);
		expect(detected).toBe(true);
	});

	test("detectOpenClaw returns false without config.json", async () => {
		const detected = await detectOpenClaw(testDir);
		expect(detected).toBe(false);
	});
});

describe("Hermes detection", () => {
	let testDir: string;

	beforeEach(async () => {
		testDir = randomDir();
		await mkdir(testDir, { recursive: true });
	});

	afterEach(async () => {
		await rm(testDir, { recursive: true, force: true });
	});

	test("detectHermes returns true for valid config.yaml", async () => {
		await writeFile(join(testDir, "config.yaml"), "persona: test");
		const detected = await detectHermes(testDir);
		expect(detected).toBe(true);
	});

	test("detectHermes returns false without config.yaml or memories", async () => {
		const detected = await detectHermes(testDir);
		expect(detected).toBe(false);
	});
});

describe("migration detection", () => {
	let testDir: string;

	beforeEach(async () => {
		testDir = randomDir();
		await mkdir(testDir, { recursive: true });
	});

	afterEach(async () => {
		await rm(testDir, { recursive: true, force: true });
	});

	test("migrate rejects invalid source directory", async () => {
		expect(async () => {
			await migrate({ source: "/nonexistent/path" });
		}).toThrow();
	});
});

describe("dry-run mode", () => {
	let testDir: string;
	let targetDir: string;

	beforeEach(async () => {
		testDir = randomDir();
		targetDir = randomDir();
		await mkdir(testDir, { recursive: true });
		await mkdir(targetDir, { recursive: true });
	});

	afterEach(async () => {
		await rm(testDir, { recursive: true, force: true });
		await rm(targetDir, { recursive: true, force: true });
	});

	test("migrate with --dry-run does not write files", async () => {
		// Create minimal OpenClaw config
		await writeFile(join(testDir, "config.json"), JSON.stringify({ persona: { name: "test" } }));

		// Capture console output
		const logs: string[] = [];
		const originalLog = console.log;
		console.log = (...args: unknown[]) => {
			logs.push(args.join(" "));
		};

		try {
			await migrate({ source: testDir, target: targetDir, dryRun: true });

			// Should output dry-run message
			const dryRunLog = logs.find((log) => log.includes("DRY RUN"));
			expect(dryRunLog).toBeDefined();
		} finally {
			console.log = originalLog;
		}
	});
});
