import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { APP_USAGE, parseAppArgs } from "../src/main";

const directories: string[] = [];
afterEach(async () => {
	await Promise.all(directories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })));
});

async function run(
	args: string[],
): Promise<{ readonly code: number; readonly stdout: string; readonly stderr: string }> {
	const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), ...args], {
		cwd: join(import.meta.dir, "../../.."),
		stdout: "pipe",
		stderr: "pipe",
		env: { ...process.env, GAJAEWAY_HOME: join(tmpdir(), "gajaeway-app-main-test") },
	});
	const [stdout, stderr, code] = await Promise.all([
		new Response(child.stdout).text(),
		new Response(child.stderr).text(),
		child.exited,
	]);
	return { code, stdout, stderr };
}

test("no argv and unknown verbs print app usage to stderr with usage status", async () => {
	for (const args of [[], ["not-a-command"]]) {
		const result = await run(args);
		expect(result.code).toBe(2);
		expect(result.stderr).toContain(APP_USAGE);
		expect(result.stdout).toBe("");
	}
});

test("help and version are import-safe argv paths with stdout success", async () => {
	const help = await run(["--help"]);
	expect(help.code).toBe(0);
	expect(help.stdout).toContain("daemon run");
	expect(help.stderr).toBe("");
	const version = await run(["--version"]);
	expect(version.code).toBe(0);
	expect(version.stdout.trim()).toMatch(/^\d+\.\d+\.\d+$/);
});

test("config check exits zero for valid input and one for invalid input", async () => {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-app-config-check-"));
	directories.push(directory);
	const valid = join(directory, "valid.json");
	const invalid = join(directory, "invalid.json");
	await writeFile(valid, JSON.stringify({ schemaVersion: 1 }));
	await writeFile(invalid, JSON.stringify({ schemaVersion: 2 }));
	const validResult = await run(["config", "check", valid]);
	expect(validResult.code).toBe(0);
	expect(validResult.stdout).toContain("OK");
	const invalidResult = await run(["config", "check", invalid]);
	expect(invalidResult.code).toBe(1);
	expect(invalidResult.stdout).toContain("FAIL");
});

test("parser keeps app verbs out of the legacy cli dispatcher and delegates socket clients", () => {
	expect(parseAppArgs(["daemon", "run"])).toEqual({ kind: "daemon", onlyNew: false });
	expect(parseAppArgs(["daemon", "run", "--only-new"])).toEqual({ kind: "daemon", onlyNew: true });
	expect(parseAppArgs(["daemon", "run", "--bogus"])).toEqual({ kind: "usage" });
	expect(parseAppArgs(["daemon", "nope"])).toEqual({ kind: "usage" });
	expect(parseAppArgs(["--socket", "/tmp/gateway.sock", "status"])).toEqual({
		kind: "cli",
		args: ["--socket", "/tmp/gateway.sock", "status"],
	});
});
