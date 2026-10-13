import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { OWNER_FILE, TABLE_OWNER } from "../../gateway/src/store/ownership";
import { LEASE_NAMES } from "../../home/src/layout";

const ROOT = join(import.meta.dir, "..", "..", "..");
const MARKDOWN_DIRECTORIES = ["docs", "packages/gateway/src/persona/self-ops"] as const;

interface ForbiddenRule {
	readonly id: string;
	readonly pattern: RegExp;
	readonly positive: string;
}

const FORBIDDEN_RULES: readonly ForbiddenRule[] = [
	{ id: "global-user", pattern: /global-user/i, positive: "global-user authority" },
	{ id: "shared broker", pattern: /shared broker/i, positive: "shared broker lifecycle" },
	{ id: "daemon.pid", pattern: /daemon\.pid/i, positive: "daemon.pid" },
	{ id: "adapter-discord.pid", pattern: /adapter-discord\.pid/i, positive: "adapter-discord.pid" },
	{ id: "memory-receipts.jsonl", pattern: /memory-receipts\.jsonl/i, positive: "memory-receipts.jsonl" },
	{ id: "gjc-authority-cutover", pattern: /gjc-authority-cutover/i, positive: "gjc-authority-cutover" },
	{ id: "ops restore", pattern: /\bops\s+restore\b/i, positive: "gajaeway ops restore <backup>" },
	{ id: "ops upgrade", pattern: /\bops\s+upgrade\b/i, positive: "gajaeway ops upgrade" },
	{ id: "GJC_EXECUTABLE", pattern: /GJC_EXECUTABLE/i, positive: "GJC_EXECUTABLE=/path/to/gjc" },
	{ id: "BrokerSpawnerGuard", pattern: /BrokerSpawnerGuard/i, positive: "BrokerSpawnerGuard was removed" },
];

const REQUIRED_AUTHORITY_HEADERS = [
	"## Authority Map",
	"### Resource owners",
	"### Table owners",
	"### Transaction boundary",
	"### Lease names",
] as const;

function markdownFiles(): string[] {
	const files: string[] = [];
	const walk = (directory: string): void => {
		for (const entry of readdirSync(directory)) {
			const path = join(directory, entry);
			if (statSync(path).isDirectory()) walk(path);
			else if (entry.endsWith(".md")) files.push(path);
		}
	};

	for (const directory of MARKDOWN_DIRECTORIES) walk(join(ROOT, directory));
	for (const entry of readdirSync(ROOT)) {
		if (/^README.*\.md$/i.test(entry)) files.push(join(ROOT, entry));
	}
	return files.sort();
}

function forbiddenMatches(content: string): string[] {
	return FORBIDDEN_RULES.filter((rule) => rule.pattern.test(content)).map((rule) => rule.id);
}

function documentViolations(files: readonly string[]): string[] {
	const hits: string[] = [];
	for (const file of files) {
		const path = relative(ROOT, file);
		readFileSync(file, "utf8")
			.split(/\r?\n/)
			.forEach((line, index) => {
				for (const rule of FORBIDDEN_RULES) {
					if (rule.pattern.test(line)) hits.push(`${path}:${index + 1}: forbidden ${rule.id}`);
				}
			});
	}
	return hits;
}

function markdownCells(line: string): string[] {
	return line
		.trim()
		.replace(/^\|/, "")
		.replace(/\|$/, "")
		.split("|")
		.map((cell) => cell.trim().replace(/^`(.*)`$/, "$1"));
}

function tableRows(markdown: string, firstHeaderCell: string): string[][] {
	const lines = markdown.split(/\r?\n/);
	const header = lines.findIndex((line) => line.trim().startsWith("|") && markdownCells(line)[0] === firstHeaderCell);
	if (header < 0) return [];

	const rows: string[][] = [];
	for (let index = header + 2; index < lines.length; index++) {
		const line = lines[index]?.trim() ?? "";
		if (!line.startsWith("|")) break;
		const cells = markdownCells(line);
		if (cells.every((cell) => /^:?-+:?$/.test(cell))) continue;
		rows.push(cells);
	}
	return rows;
}

describe("current-state authority documentation", () => {
	const files = markdownFiles();
	const paths = files.map((file) => relative(ROOT, file));
	const architecturePath = join(ROOT, "docs/architecture.md");
	const architecture = readFileSync(architecturePath, "utf8");

	test("scans all required Markdown without historical-document exclusions", () => {
		for (const path of [
			"README.md",
			"docs/architecture.md",
			"docs/deployment.md",
			"docs/memory.md",
			"docs/runbooks/gajaeway-v1.md",
			"packages/gateway/src/persona/self-ops/config-and-restarts.md",
			"packages/gateway/src/persona/self-ops/service-control.md",
		])
			expect(paths).toContain(path);
		expect(files.length).toBeGreaterThan(6);
	});

	test("contains no obsolete authority or receipt language", () => {
		expect(documentViolations(files)).toEqual([]);
	});

	for (const rule of FORBIDDEN_RULES) {
		test(`positive self-test detects ${rule.id}`, () => {
			expect(forbiddenMatches(rule.positive)).toContain(rule.id);
		});
	}

	test("canonical offline restore syntax is not confused with an obsolete ops verb", () => {
		expect(forbiddenMatches("gajaeway-gateway restore /backup/gateway.db --abort")).toEqual([]);
	});

	test("architecture contains the required Authority Map headers", () => {
		for (const header of REQUIRED_AUTHORITY_HEADERS) expect(architecture).toContain(header);
	});

	test("documented SQLite table ownership exactly matches TABLE_OWNER and OWNER_FILE", () => {
		const actual = tableRows(architecture, "Table");
		const expected = Object.entries(TABLE_OWNER).map(([table, group]) => [table, group, OWNER_FILE[group]]);
		expect(actual).toEqual(expected);
	});

	test("documented kernel lease names exactly match LEASE_NAMES", () => {
		const rows = tableRows(architecture, "Lease");
		expect(rows.every((row) => row.length === 2)).toBe(true);
		expect(rows.map(([name]) => name)).toEqual([...LEASE_NAMES]);
	});
});
