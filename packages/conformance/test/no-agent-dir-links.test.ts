import { describe, expect, test } from "bun:test";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const ROOT = join(import.meta.dir, "..", "..", "..");
const ALLOWED_LINKS = new Set(["packages/cli/src/gjc-install.ts", "packages/gateway/src/persona/persona.ts"]);
const ALLOWED_LIVE_LINK_CHECKS = new Set([
	"packages/gateway/src/broker-host.ts",
	"packages/gateway/src/broker-agent/allowlist.ts",
]);
const LINK_CALL = /\b(?:symlink|symlinkSync|link|linkSync)\s*\(/;
const LIVE_LINK_SETTING = /GJC_AUTH_BROKER_URL|GJC_AUTH_BROKER_TOKEN|auth\.broker\.url/;

function sourceFiles(): string[] {
	const files: string[] = [];
	const walk = (directory: string) => {
		for (const entry of readdirSync(directory)) {
			if (entry === "node_modules" || entry === "dist") continue;
			const path = join(directory, entry);
			if (statSync(path).isDirectory()) walk(path);
			else if (/\.(?:ts|tsx|js)$/.test(entry) && !/\.test\.(?:ts|tsx|js)$/.test(entry)) files.push(path);
		}
	};
	for (const pkg of readdirSync(join(ROOT, "packages"))) {
		const source = join(ROOT, "packages", pkg, "src");
		try {
			if (statSync(source).isDirectory()) walk(source);
		} catch {
			// Some workspace packages are intentionally source-less.
		}
	}
	return files;
}

function violations(pattern: RegExp, allowed: ReadonlySet<string>, files: readonly string[]): string[] {
	const hits: string[] = [];
	for (const file of files) {
		const path = relative(ROOT, file);
		if (allowed.has(path)) continue;
		readFileSync(file, "utf8")
			.split("\n")
			.forEach((line, index) => {
				const code = line.trimStart();
				if (code.startsWith("//") || code.startsWith("/*") || code.startsWith("*") || code.startsWith("*/")) return;
				if (pattern.test(line)) hits.push(`${path}:${index + 1}: ${line.trim()}`);
			});
	}
	return hits;
}

describe("no live link to the operator GJC agent directory (AC5)", () => {
	const files = sourceFiles();

	test("the source scan covers every workspace package", () => {
		expect(files.some((path) => path.endsWith("packages/gateway/src/broker-host.ts"))).toBe(true);
		expect(files.some((path) => path.endsWith("packages/cli/src/gjc-install.ts"))).toBe(true);
	});

	test("symlink and hard-link APIs are limited to the current flip and persona workspace", () => {
		expect(LINK_CALL.test("symlink(source, current); linkSync(a, b);")).toBe(true);
		expect(violations(LINK_CALL, ALLOWED_LINKS, files)).toEqual([]);
	});

	test("live broker URL settings occur only in refusal checks", () => {
		expect(LIVE_LINK_SETTING.test("GJC_AUTH_BROKER_URL auth.broker.url")).toBe(true);
		expect(violations(LIVE_LINK_SETTING, ALLOWED_LIVE_LINK_CHECKS, files)).toEqual([]);
	});
});
