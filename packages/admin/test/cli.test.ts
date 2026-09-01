import { describe, expect, test } from "bun:test";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ADMIN_USAGE, USAGE_EXIT_CODE, usageFor } from "../src/cli";

describe("admin usage guard", () => {
	test("an empty argv is a usage error, not an implicit boot", () => {
		expect(usageFor([])).toBe(ADMIN_USAGE);
	});

	test("an unknown subcommand is a usage error", () => {
		for (const args of [["bogus"], ["--help"], ["-h"], ["serve", "extra"], ["Serve"]])
			expect(usageFor(args)).toBe(ADMIN_USAGE);
	});

	test("serve is the one argv that boots the console", () => {
		expect(usageFor(["serve"])).toBeUndefined();
	});
});

describe("the binary exits on a usage error instead of blocking", () => {
	async function run(args: string[]): Promise<{ code: number; stderr: string }> {
		const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), ...args], {
			stdout: "pipe",
			stderr: "pipe",
			// A home with no socket: reaching the connect at all would be the bug.
			env: { ...process.env, GAJAEWAY_HOME: join(tmpdir(), "gajaeway-admin-usage-nonexistent") },
		});
		const [stderr, code] = await Promise.all([new Response(child.stderr).text(), child.exited]);
		return { code, stderr };
	}

	test("no arguments prints usage on stderr and exits non-zero", async () => {
		const result = await run([]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(ADMIN_USAGE);
		// The old entry reported the unreachable socket, i.e. it had already tried to connect.
		expect(result.stderr).not.toContain("Unable to connect to gateway socket");
	}, 30_000);

	test("an unknown subcommand prints usage on stderr and exits non-zero", async () => {
		const result = await run(["bogus"]);
		expect(result.code).toBe(USAGE_EXIT_CODE);
		expect(result.stderr).toContain(ADMIN_USAGE);
	}, 30_000);
});
