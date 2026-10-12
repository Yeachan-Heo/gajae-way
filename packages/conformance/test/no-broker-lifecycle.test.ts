import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const ROOT = join(import.meta.dir, "..", "..", "..");
const GATEWAY_SRC = join(ROOT, "packages", "gateway", "src");
const BROKER = "packages/gateway/src/orchestrator/broker.ts";
const BROKER_HOST = "packages/gateway/src/broker-host.ts";

function sourceFiles(): string[] {
	const files: string[] = [];
	const walk = (directory: string): void => {
		for (const entry of readdirSync(directory)) {
			if (entry === "node_modules" || entry === "dist") continue;
			const path = join(directory, entry);
			if (statSync(path).isDirectory()) walk(path);
			else if (/\.(?:ts|tsx|js)$/.test(entry) && !/\.test\.(?:ts|tsx|js)$/.test(entry)) files.push(path);
		}
	};
	walk(GATEWAY_SRC);
	return files;
}

function hits(files: readonly string[], pattern: RegExp): string[] {
	const matches: string[] = [];
	for (const file of files) {
		const path = relative(ROOT, file);
		readFileSync(file, "utf8")
			.split("\n")
			.forEach((line, index) => {
				if (pattern.test(line)) matches.push(`${path}:${index + 1}: ${line.trim()}`);
			});
	}
	return matches;
}

describe("single broker lifecycle authority (AC6)", () => {
	const files = sourceFiles();
	const paths = files.map((file) => relative(ROOT, file));
	const brokerText = readFileSync(join(ROOT, BROKER), "utf8");
	const brokerHostText = readFileSync(join(ROOT, BROKER_HOST), "utf8");

	test("the legacy transient-unit and spawner lifecycle hooks are absent", () => {
		for (const pattern of [
			/\bStartTransientUnit\b/,
			/\bbusctl\b/,
			/\bsystemd-run\b/,
			/\bBrokerSpawnerGuard\b/,
			/\bcreateBrokerReleaser\b/,
			/\bbroker[_ -]?nudge\b/i,
			/\bnudgeBrokerAutostart\b/,
		]) {
			expect(hits(files, pattern)).toEqual([]);
		}
	});

	test("foreground broker commands are owned by broker-host only", () => {
		const locations = hits(files, /broker-internal|\bbroker run\b|"broker",\s*"run"/).map(
			(hit) => hit.split(":", 1)[0],
		);
		expect(locations.length).toBeGreaterThan(0);
		expect(locations.every((path) => path === BROKER_HOST)).toBe(true);
	});

	test("only broker client and host launch the configured gjc executable", () => {
		expect(brokerText).toMatch(/\bBun\.spawn\b/);
		expect(brokerHostText).toMatch(/\bBun\.spawn\b/);
		const nonOwners = files.filter((file) => {
			const path = relative(ROOT, file);
			return path !== BROKER && path !== BROKER_HOST;
		});
		expect(hits(nonOwners, /(?:cmd\s*:\s*\[\s*["']gjc["']|Bun\.spawn(?:Sync)?\s*\(\s*\[\s*["']gjc["'])/)).toEqual([]);
	});

	test("the attach-only environment contract is applied by both owners", () => {
		expect(brokerText).toMatch(/\[GJC_ATTACH_ONLY_ENV\]\s*:\s*["']0["']/);
		expect(brokerHostText).toMatch(/\[GJC_ATTACH_ONLY_ENV\]\s*:\s*["']0["']/);
	});

	test("the scan includes both lifecycle owners", () => {
		expect(paths).toContain(BROKER);
		expect(paths).toContain(BROKER_HOST);
	});
});
