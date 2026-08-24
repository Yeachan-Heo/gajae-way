import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";

const repoRoot = path.join(import.meta.dir, "..", "..");

function readSource(relative: string): string {
	return fs.readFileSync(path.join(repoRoot, relative), "utf8");
}

function importsOf(source: string): string[] {
	return [...source.matchAll(/from\s+"([^"]+)"/gu)].map((match) => match[1] as string);
}

/**
 * The P0-2 acceptance boundary, made checkable.
 *
 * The Telegram adapter must be a driver plus wiring over the shared runtime. If
 * it reaches into the gateway's session internals or the native core, the
 * protocol was not actually extracted and the next adapter would pay the same
 * cost again.
 */
test("the Telegram adapter imports no gateway session or native-core internals", () => {
	const files = ["src/adapter/telegram/main.ts", "src/adapter/telegram/platform.ts", "src/adapter/telegram/config.ts"];
	for (const file of files) {
		for (const specifier of importsOf(readSource(file))) {
			expect(specifier).not.toContain("main-session");
			expect(specifier).not.toContain("way-core");
			expect(specifier).not.toContain("native-loader");
			// It must also not reach into another adapter's internals.
			expect(specifier).not.toContain("adapter/discord");
		}
	}
});

test("the Telegram adapter is built on the shared runtime rather than its own delivery logic", () => {
	const main = readSource("src/adapter/telegram/main.ts");
	expect(main).toContain("../runtime/egress");
	expect(main).toContain("../runtime/ingress");
	expect(main).toContain("../runtime/session");
});

/**
 * The per-adapter delivery implementations are gone, not merely unused. A
 * lingering copy would be a parallel convention: the next maintainer could not
 * tell which one ships.
 */
test("the per-adapter Discord delivery implementations no longer exist", () => {
	for (const removed of ["outbox.ts", "route.ts", "ack.ts"]) {
		expect(fs.existsSync(path.join(repoRoot, "src/adapter/discord", removed))).toBe(false);
	}
});

test("the Discord adapter also runs on the shared runtime", () => {
	const main = readSource("src/adapter/discord/main.ts");
	expect(main).toContain("../runtime/egress");
	expect(main).toContain("../runtime/ingress");
});

test("both adapters are compiled as standalone executables", () => {
	const compile = readSource("scripts/compile.ts");
	expect(compile).toContain("gajaeway-telegram");
	expect(compile).toContain("src/adapter/telegram/main.ts");
	const embed = readSource("scripts/embed-native.ts");
	// The native-addon guard must admit the new executable or compile fails.
	expect(embed).toContain("gajaeway-telegram");
});
