import { expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseSlackArgs, SLACK_USAGE, USAGE_EXIT_CODE } from "../src/main";

test("Slack argv is resolved before connection work", () => {
	expect(parseSlackArgs([])).toEqual({ kind: "run" });
	for (const flag of ["--help", "-h"]) expect(parseSlackArgs([flag])).toEqual({ kind: "help" });
	for (const flag of ["--version", "-v"]) expect(parseSlackArgs([flag])).toEqual({ kind: "version" });
	expect(parseSlackArgs(["--help", "extra"]).kind).toBe("usage");
	expect(parseSlackArgs(["--boot-now"])).toEqual({
		kind: "usage",
		message: `gajaeway-slack: unexpected argument --boot-now\n${SLACK_USAGE}`,
	});
});

for (const flag of ["--help", "--version", "--boot-now"]) {
	test(`Slack ${flag} exits without writing a pidfile`, async () => {
		const home = await mkdtemp(join(tmpdir(), "gajaeway-slack-cli-"));
		try {
			const child = Bun.spawn(["bun", join(import.meta.dir, "../src/main.ts"), flag], {
				stdout: "pipe",
				stderr: "pipe",
				env: { ...process.env, GAJAEWAY_HOME: home },
			});
			const [stdout, stderr, code] = await Promise.all([
				new Response(child.stdout).text(),
				new Response(child.stderr).text(),
				child.exited,
			]);
			expect(await Bun.file(join(home, "adapter-slack.pid")).exists()).toBe(false);
			expect(code).toBe(flag === "--boot-now" ? USAGE_EXIT_CODE : 0);
			if (flag === "--help") expect(stdout.trim()).toBe(SLACK_USAGE);
			else if (flag === "--version") expect(stdout.trim()).toMatch(/^\d+\.\d+\.\d+/);
			else expect(stderr).toContain(SLACK_USAGE);
		} finally {
			await rm(home, { recursive: true, force: true });
		}
	}, 30_000);
}
