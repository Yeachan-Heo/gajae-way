import { describe, expect, test } from "bun:test";
import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative } from "node:path";

/**
 * AC1: the kernel-flock lease in packages/home/src/lease.ts is the only
 * ownership primitive. Pidfiles, O_EXCL claims, elections, link CAS, PID-alive
 * ownership checks and "socket absent means stopped" probes may not reappear.
 */

const ROOT = join(import.meta.dir, "..", "..", "..");
const LEASE = "packages/home/src/lease.ts";

interface Rule {
	readonly id: string;
	readonly description: string;
	readonly pattern: RegExp;
	readonly allowed: readonly string[];
	/** A line that must match, proving the pattern is live. */
	readonly positive: string;
}

const RULES: readonly Rule[] = [
	{
		id: "R1",
		description: "flock/dlopen outside the lease helper",
		pattern: /\bflock\b|\bdlopen\(/,
		allowed: [LEASE],
		positive: `const lib = dlopen("libc.so.6", { flock: {} });`,
	},
	{
		id: "R2",
		description: "O_EXCL ownership claims",
		pattern: /\bO_EXCL\b/,
		allowed: [],
		positive: "open(path, constants.O_CREAT | constants.O_EXCL)",
	},
	{
		id: "R3",
		description: "pidfiles",
		pattern: /daemon\.pid|\bPID_FILE\b|["'`][^"'`\n]*\.pid["'`]/,
		allowed: [],
		positive: "const path = join(home, `adapter-discord.pid`);",
	},
	{
		id: "R4",
		description: "create-exclusive ('wx') writes used as ownership",
		pattern: /flag:\s*["']wx["']|\bopen\([^)\n]*["']wx["']/,
		// Random-named temp files and first-write-wins content seeding, never ownership.
		allowed: [
			"packages/gateway/src/persona/persona.ts",
			"packages/cli/src/setup.ts",
			"packages/adapter-slack/src/recovery.ts",
		],
		positive: `await writeFile(lockPath, pid, { flag: "wx" });`,
	},
	{
		id: "R5",
		description: "socket existence used as gateway liveness",
		pattern: /\b(?:existsSync|statSync|lstatSync|stat|lstat|access)\([^)\n]*\bsock(?:et)?(?:Path)?\b/i,
		allowed: [],
		positive: "await stat(socketPath);",
	},
	{
		id: "R6",
		description: "elections and link/mkdir CAS",
		pattern: /\.reclaim\.d|\blinkNoReplace\b|\.candidate["'`]|\bmkdir\([^)\n]*\.lock\b/,
		allowed: [],
		positive: "const election = `$" + "{path}.reclaim.d`;",
	},
	{
		id: "R7",
		description: "process.kill(pid, 0) liveness probes",
		pattern: /process\.kill\([^,\n]+,\s*0\s*\)/,
		// gjc discovery liveness (not gajaeway ownership) and scratch-dir GC.
		allowed: ["packages/gateway/src/orchestrator/broker.ts", "packages/conformance/src/scratch-tmp.ts"],
		positive: "process.kill(record.pid, 0);",
	},
];

function sourceFiles(): string[] {
	const files: string[] = [];
	const walk = (dir: string, inTest: boolean) => {
		for (const entry of readdirSync(dir)) {
			if (entry === "node_modules" || entry === "dist") continue;
			const path = join(dir, entry);
			if (statSync(path).isDirectory()) walk(path, inTest || entry === "test");
			else if (!inTest && /\.(?:ts|tsx|js)$/.test(entry) && !/\.test\.(?:ts|tsx|js)$/.test(entry)) files.push(path);
		}
	};
	for (const pkg of readdirSync(join(ROOT, "packages"))) {
		const src = join(ROOT, "packages", pkg, "src");
		try {
			if (statSync(src).isDirectory()) walk(src, false);
		} catch {}
	}
	walk(join(ROOT, "scripts"), false);
	return files;
}

function violations(rule: Rule, files: readonly string[]): string[] {
	const hits: string[] = [];
	for (const file of files) {
		const rel = relative(ROOT, file);
		if (rule.allowed.includes(rel)) continue;
		readFileSync(file, "utf8")
			.split("\n")
			.forEach((line, index) => {
				if (rule.pattern.test(line)) hits.push(`${rel}:${index + 1}: ${line.trim()}`);
			});
	}
	return hits;
}

describe("single lock primitive (AC1)", () => {
	const files = sourceFiles();

	test("the scan covers the workspace", () => {
		expect(files.map((file) => relative(ROOT, file))).toContain(LEASE);
		expect(files.length).toBeGreaterThan(50);
	});

	for (const rule of RULES) {
		test(`${rule.id}: no ${rule.description}`, () => {
			expect(rule.pattern.test(rule.positive)).toBe(true);
			expect(violations(rule, files)).toEqual([]);
		});
	}
});
