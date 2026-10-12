import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";

import { tmpdir } from "node:os";
import { join } from "node:path";

import { DISCORD_USAGE, parseDiscordArgs, USAGE_EXIT_CODE } from "../src/main";

describe("argv is resolved before any connection work", () => {
	test("no arguments still runs the adapter", () => {
		expect(parseDiscordArgs([])).toEqual({ kind: "run" });
	});

	test("--help and --version short-circuit", () => {
		expect(parseDiscordArgs(["--help"]).kind).toBe("help");
		expect(parseDiscordArgs(["-h"]).kind).toBe("help");
		expect(parseDiscordArgs(["--version"]).kind).toBe("version");
		expect(parseDiscordArgs(["-v"]).kind).toBe("version");
	});

	test("anything else is a usage error naming the offending argument", () => {
		const argv = parseDiscordArgs(["--boot-now"]);
		expect(argv.kind).toBe("usage");
		expect(argv.kind === "usage" && argv.message).toContain("--boot-now");
		expect(parseDiscordArgs(["--help", "extra"]).kind).toBe("usage");
	});
});

describe("the binary answers --help without booting an adapter", () => {
	async function run(args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-discord-cli-"));
		try {
			const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), ...args], {
				stdout: "pipe",
				stderr: "pipe",
				env: { ...process.env, GAJAEWAY_HOME: home },
			});
			const [stdout, stderr, code] = await Promise.all([
				new Response(child.stdout).text(),
				new Response(child.stderr).text(),
				child.exited,
			]);
			// Usage-only invocation must not create an adapter lease.

			expect(await Bun.file(join(home, "leases", "adapter-discord")).exists()).toBe(false);

			return { code, stdout, stderr };
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}

	test("--help prints usage on stdout and exits 0", async () => {
		const result = await run(["--help"]);
		expect(result.code).toBe(0);
		expect(result.stdout).toContain(DISCORD_USAGE);
	}, 30_000);

	test("--version prints the package version and exits 0", async () => {
		const result = await run(["--version"]);
		expect(result.code).toBe(0);
		expect(result.stdout.trim()).toMatch(/^\d+\.\d+\.\d+/);
	}, 30_000);

	test("an unknown flag exits non-zero with usage on stderr", async () => {
		const result = await run(["--boot-now"]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(DISCORD_USAGE);
	}, 30_000);
});