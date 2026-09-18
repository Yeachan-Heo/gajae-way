import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

/**
 * Dogfooding boundary test (spec AC-3, plan principle 2).
 *
 * Every SDK-consumer package (owner CLI, platform adapters) must be
 * implementable by a third party: it may import ONLY the public packages
 * (@gajaeway/sdk, @gajaeway/protocol, @gajaeway/log), Bun/node builtins, its
 * own files, and its declared platform library. Any import that reaches into
 * the gateway's internals (or the legacy tree) is a privileged import and
 * fails this test.
 */

const ROOT = join(import.meta.dir, "..", "..", "..");

/** Consumer packages and their extra allowed external deps (platform libs). */
const CONSUMER_PACKAGES: Record<string, readonly string[]> = {
	"packages/cli": [],
	// Adapters join this table in P1/P2; listing them now costs nothing.
	"packages/adapter-discord": ["discord.js", "@discordjs/ws", "@discordjs/rest"],
	"packages/adapter-telegram": ["grammy", "node-telegram-bot-api"],
	// Slack is hand-rolled over fetch + Bun's WebSocket: no platform library at all.
	"packages/adapter-slack": [],
};

// @gajaeway/log is public for the same reason as the protocol: it is an
// unprivileged, dependency-free service sink (console + fs only) that a
// third-party adapter needs in order to produce windowable logs at all.
const PUBLIC_IMPORTS = ["@gajaeway/sdk", "@gajaeway/protocol", "@gajaeway/log"];

const FORBIDDEN_PREFIXES = [
	"@gajaeway/gateway",
	"../gateway",
	"../../gateway",
	"../../../gateway",
	"../../src/",
	"../../../src/",
	"../../crates/",
];

function collectSourceFiles(dir: string, out: string[] = []): string[] {
	let entries: string[];
	try {
		entries = readdirSync(dir);
	} catch {
		return out;
	}
	for (const entry of entries) {
		if (entry === "node_modules" || entry.startsWith(".")) continue;
		const full = join(dir, entry);
		const stat = statSync(full);
		if (stat.isDirectory()) collectSourceFiles(full, out);
		else if (/\.(ts|tsx|js|mjs)$/.test(entry)) out.push(full);
	}
	return out;
}

const IMPORT_STATEMENT_RE = /^[ \t]*import\s+(?:type\s+)?(?:[\w${},*\s]+?from\s+)?["']([^"']+)["']/gm;
const EXPORT_FROM_RE = /^[ \t]*export\s+(?:type\s+)?[\w${},*\s]+?from\s+["']([^"']+)["']/gm;
const REQUIRE_RE = /require\(\s*["']([^"']+)["']\s*\)/g;

function importsOf(file: string): string[] {
	const text = readFileSync(file, "utf8");
	const specs: string[] = [];
	for (const re of [IMPORT_STATEMENT_RE, EXPORT_FROM_RE, REQUIRE_RE]) {
		for (const match of text.matchAll(re)) {
			const spec = match[1];
			if (spec) specs.push(spec);
		}
	}
	return specs;
}

function isBuiltin(spec: string): boolean {
	return spec.startsWith("node:") || spec.startsWith("bun:") || spec === "bun";
}

describe("sdk-boundary-dogfood", () => {
	for (const [pkg, platformDeps] of Object.entries(CONSUMER_PACKAGES)) {
		const pkgDir = join(ROOT, pkg);
		let exists = false;
		try {
			exists = statSync(pkgDir).isDirectory();
		} catch {
			exists = false;
		}
		if (!exists) continue; // adapters appear in later phases

		test(`${pkg} imports only the public SDK surface`, () => {
			const files = collectSourceFiles(pkgDir);
			expect(files.length).toBeGreaterThan(0);
			const violations: string[] = [];
			for (const file of files) {
				for (const spec of importsOf(file)) {
					if (isBuiltin(spec)) continue;
					if (spec.startsWith("./") || spec.startsWith("../")) {
						// Relative imports must stay inside the package.
						const resolved = join(file, "..", spec);
						if (!resolved.startsWith(pkgDir)) {
							violations.push(`${relative(ROOT, file)} escapes package: ${spec}`);
						}
						continue;
					}
					const allowed =
						PUBLIC_IMPORTS.some((p) => spec === p || spec.startsWith(`${p}/`)) ||
						platformDeps.some((p) => spec === p || spec.startsWith(`${p}/`));
					if (!allowed || FORBIDDEN_PREFIXES.some((p) => spec.startsWith(p))) {
						violations.push(`${relative(ROOT, file)} imports privileged module: ${spec}`);
					}
				}
			}
			expect(violations).toEqual([]);
		});
	}

	test("sdk package itself depends only on the protocol package", () => {
		const files = collectSourceFiles(join(ROOT, "packages/sdk"));
		const violations: string[] = [];
		for (const file of files) {
			for (const spec of importsOf(file)) {
				if (isBuiltin(spec) || spec.startsWith("./") || spec.startsWith("../")) continue;
				if (spec === "@gajaeway/protocol" || spec.startsWith("@gajaeway/protocol/")) continue;
				violations.push(`${relative(ROOT, file)} imports: ${spec}`);
			}
		}
		expect(violations).toEqual([]);
	});
});
