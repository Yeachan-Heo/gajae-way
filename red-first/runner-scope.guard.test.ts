/** a81bb27; fixture red-first-v1. Passing guard: normal runners must not collect historical reds. */
import { expect, test } from "bun:test";

test("normal package, local CI and build runners exclude red-first", async () => {
	const root = new URL("../", import.meta.url);
	const pkg = await Bun.file(new URL("package.json", root)).json();
	expect(pkg.scripts.test).toMatch(/^bun test packages(?:\/|\s|$)/);
	expect(pkg.scripts.test).not.toMatch(/red-first/);
	for (const path of ["scripts/ci-local.sh", ".github/workflows/build.yml"]) {
		const content = await Bun.file(new URL(path, root)).text();
		const commands = content.split("\n").filter((line) => /\bbun test\b/.test(line));
		expect(commands.length).toBeGreaterThan(0);
		for (const command of commands) {
			expect(command).toMatch(/\bbun test packages\//);
			expect(command).not.toMatch(/red-first/);
		}
	}
});
