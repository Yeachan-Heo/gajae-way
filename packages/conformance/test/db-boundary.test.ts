import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const ROOT = join(import.meta.dir, "..", "..", "..");
const BUN_SQLITE_IMPORT = /\b(?:from\s*|import\s*(?:\(\s*)?|require\s*\(\s*)["']bun:sqlite["']/;
const GATEWAY_DB_LITERAL = /gateway\.db/;
const BROKER_AGENT_DB = "packages/gateway/src/broker-agent/agent-db.ts";
const ALLOWED_SQLITE_IMPORTS = ["packages/gateway/src/broker-agent/agent-db.ts"];
const ALLOWED_DB_LITERALS = ["packages/gateway/src/config.ts", "packages/home/src/layout.ts"];

function sourceFiles(): string[] {
	const files: string[] = [];
	const walk = (dir: string, inTest: boolean) => {
		for (const entry of readdirSync(dir)) {
			if (entry === "node_modules" || entry === "dist") continue;
			const path = join(dir, entry);
			if (statSync(path).isDirectory()) walk(path, inTest || entry === "test");
			else if (!inTest && /\.(?:ts|tsx|js|mjs|cjs)$/.test(entry)) files.push(path);
		}
	};
	for (const packageName of readdirSync(join(ROOT, "packages"))) {
		const src = join(ROOT, "packages", packageName, "src");
		try {
			if (statSync(src).isDirectory()) walk(src, false);
		} catch {
			// Packages without a source directory are outside this boundary scan.
		}
	}
	walk(join(ROOT, "scripts"), false);
	return files;
}

function lineViolations(pattern: RegExp, files: readonly string[], allowed: readonly string[] = []): string[] {
	const hits: string[] = [];
	for (const file of files) {
		const rel = relative(ROOT, file);
		if (allowed.includes(rel)) continue;
		readFileSync(file, "utf8")
			.split("\n")
			.forEach((line, index) => {
				if (pattern.test(line)) hits.push(`${rel}:${index + 1}: ${line.trim()}`);
			});
	}
	return hits;
}

function importsGatewayStore(source: string): string[] {
	const imports: string[] = [];
	const declaration = /\b(?:import|export)\s+([\s\S]*?)\s+from\s*["']([^"']+)["']/g;
	for (const match of source.matchAll(declaration)) {
		const clause = match[1] ?? "";
		const specifier = match[2] ?? "";
		const gatewayStorePath = /(?:^|\/)(?:packages\/)?gateway\/src\/store(?:\/|$)|(?:^|\/)gateway\/store(?:\/|$)/.test(
			specifier,
		);
		if (gatewayStorePath || /\b(?:Store|StoreCore)\b/.test(clause)) imports.push(match[0].trim());
	}
	return imports;
}

function gatewayStoreImportViolations(files: readonly string[]): string[] {
	const hits: string[] = [];
	for (const file of files) {
		const rel = relative(ROOT, file);
		if (rel.startsWith("packages/gateway/src/")) continue;
		for (const declaration of importsGatewayStore(readFileSync(file, "utf8"))) hits.push(`${rel}: ${declaration}`);
	}
	return hits;
}

describe("gateway database ownership boundary (AC3)", () => {
	const files = sourceFiles();

	test("scan covers every package source tree and scripts, excluding test directories", () => {
		const paths = files.map((file) => relative(ROOT, file));
		expect(paths).toContain("packages/gateway/src/store/core.ts");
		expect(paths).toContain("packages/gateway/src/broker-agent/agent-db.ts");
		expect(paths).toContain("packages/cli/src/main.ts");
		expect(paths).toContain("packages/home/src/layout.ts");
		expect(paths).not.toContain("packages/gateway/test/server.test.ts");
		expect(paths).toContain("scripts/spike-gjc-contract.ts");
		expect(files.length).toBeGreaterThan(50);
	});

	test("bun:sqlite imports stay in the gateway store and broker agent database", () => {
		expect(BUN_SQLITE_IMPORT.test('import { Database } from "bun:sqlite";')).toBe(true);
		const allowed = [
			...ALLOWED_SQLITE_IMPORTS,
			...files.map((file) => relative(ROOT, file)).filter((path) => path.startsWith("packages/gateway/src/store/")),
		];
		expect(lineViolations(BUN_SQLITE_IMPORT, files, allowed)).toEqual([]);
	});

	test("broker agent database does not import store/config or mention the database filename", () => {
		const forbidden =
			/(?:\b(?:from\s*|import\s*\(\s*|require\s*\(\s*)["'][^"']*(?:^|\/)(?:store|config)(?:\/|["'])|gateway\.db)/;
		expect(forbidden.test('import { Store } from "../store";')).toBe(true);
		const source = readFileSync(join(ROOT, BROKER_AGENT_DB), "utf8");
		expect(lineViolations(forbidden, [join(ROOT, BROKER_AGENT_DB)])).toEqual([]);
		expect(source).not.toMatch(GATEWAY_DB_LITERAL);
	});

	test("CLI and home sources have no bun:sqlite imports", () => {
		const paths = files
			.map((file) => ({ file, path: relative(ROOT, file) }))
			.filter(({ path }) => path.startsWith("packages/cli/src/") || path.startsWith("packages/home/src/"))
			.map(({ file }) => file);
		expect(paths.length).toBeGreaterThan(0);
		expect(lineViolations(BUN_SQLITE_IMPORT, paths)).toEqual([]);
	});

	test("only gateway source imports its store, Store, or StoreCore", () => {
		expect(importsGatewayStore('import { Store } from "../packages/gateway/src/store";')).toHaveLength(1);
		expect(importsGatewayStore('import { StoreCore } from "@gajae-gateway/gateway/x";')).toHaveLength(1);
		expect(importsGatewayStore('import { readFile } from "node:fs";')).toHaveLength(0);
		expect(gatewayStoreImportViolations(files)).toEqual([]);
	});

	test("gateway.db is a literal only in config and home layout", () => {
		expect(GATEWAY_DB_LITERAL.test('join(home, "gateway.db")')).toBe(true);
		expect(lineViolations(GATEWAY_DB_LITERAL, files, ALLOWED_DB_LITERALS)).toEqual([]);
	});

	test("persona self-ops instructions avoid SQLite and database filename instructions", () => {
		const directory = join(ROOT, "packages/gateway/src/persona/self-ops");
		const docs = readdirSync(directory)
			.filter((name) => name.endsWith(".md"))
			.map((name) => join(directory, name));
		expect(docs.length).toBeGreaterThan(0);
		const forbidden = /sqlite3|gateway\.db/i;
		for (const file of docs) expect(readFileSync(file, "utf8")).not.toMatch(forbidden);
	});
});
