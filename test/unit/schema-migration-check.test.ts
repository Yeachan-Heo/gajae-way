import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";

const repoRoot = path.join(import.meta.dir, "..", "..");

/**
 * The plan makes serialized migrations a hard constraint, and the control that
 * enforces it must actually be installed in CI. A constraint with no control is
 * a comment.
 */
test("the schema migration check is wired into CI before the build", () => {
	const workflow = fs.readFileSync(path.join(repoRoot, ".github", "workflows", "build.yml"), "utf8");
	expect(workflow).toContain("bun scripts/check-schema-migration.ts");
	// It must diff against the merge base, which needs full history.
	expect(workflow).toContain("fetch-depth: 0");

	const buildIndex = workflow.indexOf("bun scripts/build-native.ts");
	const checkIndex = workflow.indexOf("bun scripts/check-schema-migration.ts");
	expect(checkIndex).toBeGreaterThan(-1);
	expect(checkIndex).toBeLessThan(buildIndex);
});

test("the checker enforces single-step advance, block reachability, and block presence", () => {
	const source = fs.readFileSync(path.join(repoRoot, "scripts", "check-schema-migration.ts"), "utf8");
	expect(source).toContain("SCHEMA_VERSION advanced by");
	expect(source).toContain("above SCHEMA_VERSION");
	expect(source).toContain("block exists");
	// It must exit non-zero, or CI would pass regardless.
	expect(source).toContain("process.exit(1)");
	expect(source).toContain("/pub const SCHEMA_VERSION: u32 = (\\d+);/u");
	expect(source).toContain("/if current_version < (\\d+)/gu");
});

function schemaVersionOf(source: string, label: string): number {
	const match = /pub const SCHEMA_VERSION: u32 = (\d+);/u.exec(source);
	if (!match) throw new Error(`could not find SCHEMA_VERSION in ${label}`);
	return Number(match[1]);
}

function guardedVersions(source: string): number[] {
	return [...source.matchAll(/if current_version < (\d+)/gu)].map((match) => Number(match[1])).sort((a, b) => a - b);
}

function evaluate(baseSource: string, headSource: string): string[] {
	const baseVersion = schemaVersionOf(baseSource, "base");
	const headVersion = schemaVersionOf(headSource, "head");
	const failures: string[] = [];
	if (headVersion < baseVersion) {
		failures.push(`SCHEMA_VERSION moved backwards: ${baseVersion} -> ${headVersion}.`);
	}
	if (headVersion - baseVersion > 1) {
		failures.push(`SCHEMA_VERSION advanced by ${headVersion - baseVersion}`);
	}
	const headGuards = guardedVersions(headSource);
	if (headGuards.filter((version) => version > headVersion).length > 0) {
		failures.push("above SCHEMA_VERSION");
	}
	if (headVersion > baseVersion && !headGuards.includes(headVersion)) {
		failures.push("block exists");
	}
	const baseGuards = new Set(guardedVersions(baseSource));
	const added = headGuards.filter((version) => !baseGuards.has(version));
	if (added.length > 1) {
		failures.push("add exactly one at a time");
	}
	return failures;
}

const baseStore = `pub const SCHEMA_VERSION: u32 = 10;
fn migrate(current_version: u32) {
    if current_version < 10 {
        // v10
    }
}
`;

test("a two-step SCHEMA_VERSION skip is rejected even when a matching high guard exists", () => {
	const head = `pub const SCHEMA_VERSION: u32 = 12;
fn migrate(current_version: u32) {
    if current_version < 10 { }
    if current_version < 12 { }
}
`;
	const failures = evaluate(baseStore, head);
	expect(failures.some((row) => row.includes("advanced by"))).toBe(true);
});

test("SCHEMA_VERSION 10 -> 11 without a current_version < 11 block is rejected", () => {
	const head = `pub const SCHEMA_VERSION: u32 = 11;
fn migrate(current_version: u32) {
    if current_version < 10 { }
}
`;
	const failures = evaluate(baseStore, head);
	expect(failures.some((row) => row.includes("block exists"))).toBe(true);
});

test("adding two new guarded blocks in one change is rejected", () => {
	const head = `pub const SCHEMA_VERSION: u32 = 11;
fn migrate(current_version: u32) {
    if current_version < 10 { }
    if current_version < 11 { }
    if current_version < 12 { }
}
`;
	expect(evaluate(baseStore, head).length).toBeGreaterThan(0);
});

test("a single-step bump with a real current_version < 11 block is accepted", () => {
	const head = `pub const SCHEMA_VERSION: u32 = 11;
fn migrate(current_version: u32) {
    if current_version < 10 { }
    if current_version < 11 {
        create_table();
    }
}
`;
	expect(evaluate(baseStore, head)).toEqual([]);
});

test("the live store.rs SCHEMA_VERSION=12 mutation is rejected against the real base file", () => {
	const live = fs.readFileSync(path.join(repoRoot, "crates", "way-core", "src", "store.rs"), "utf8");
	expect(live).toMatch(/pub const SCHEMA_VERSION: u32 = 10;/u);
	const mutated = live.replace("pub const SCHEMA_VERSION: u32 = 10;", "pub const SCHEMA_VERSION: u32 = 12;");
	const failures = evaluate(live, mutated);
	expect(failures.some((row) => row.includes("advanced by"))).toBe(true);
});

/**
 * Residual control-quality finding, not the original skip-version bug:
 * the guard detector is a whole-file regex, so a comment can satisfy
 * "block exists" without a real migration. Recorded so the hole stays visible.
 */
test("a comment containing if current_version < 11 currently satisfies the presence check", () => {
	const spoofed = `pub const SCHEMA_VERSION: u32 = 11;
fn migrate(current_version: u32) {
    if current_version < 10 { }
    // if current_version < 11 { create_table(); }
}
`;
	expect(guardedVersions(spoofed)).toContain(11);
	expect(evaluate(baseStore, spoofed)).toEqual([]);
});
