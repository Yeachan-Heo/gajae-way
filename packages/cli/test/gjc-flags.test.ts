import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { parseGjcCliArgs } from "../src/gjc";

/**
 * The CLI parses ONLY wrapper-owned flags (AC-2), and must not become a second
 * vendor adapter (AC-20 / ARCH-007 drift guard).
 */

describe("parseGjcCliArgs", () => {
	test("consumes --new and never forwards it", () => {
		const parsed = parseGjcCliArgs(["--new"]);
		expect(parsed.newSession).toBe(true);
		expect(parsed.rest).toEqual([]);
	});

	test("defaults to resuming when --new is absent", () => {
		expect(parseGjcCliArgs([]).newSession).toBe(false);
		expect(parseGjcCliArgs(["--model", "opus"]).newSession).toBe(false);
	});

	test("passes every other token through untouched for gateway classification", () => {
		const parsed = parseGjcCliArgs(["--model", "opus", "--worktree", "feature-x", "--resume", "abc"]);
		// Even a REFUSED flag passes through: refusal is the gateway's job, so the
		// closed allowlist has exactly one home.
		expect(parsed.rest).toEqual(["--model", "opus", "--worktree", "feature-x", "--resume", "abc"]);
	});

	test("--new mixed with forwarded tokens is stripped in place", () => {
		const parsed = parseGjcCliArgs(["--model", "opus", "--new", "--thinking"]);
		expect(parsed.newSession).toBe(true);
		expect(parsed.rest).toEqual(["--model", "opus", "--thinking"]);
	});

	test("does not consume --new after a `--` separator", () => {
		// After `--` the tokens are positional payload for gjc; a literal `--new`
		// there must not rotate the managed epoch.
		const parsed = parseGjcCliArgs(["--", "--new"]);
		expect(parsed.newSession).toBe(false);
		expect(parsed.rest).toEqual(["--", "--new"]);
	});

	test("still honors --new before a separator", () => {
		const parsed = parseGjcCliArgs(["--new", "--", "--new"]);
		expect(parsed.newSession).toBe(true);
		expect(parsed.rest).toEqual(["--", "--new"]);
	});

	test("the global parser leaves --socket alone after a separator", async () => {
		const { parseArgs } = await import("../src/main");
		// Before `--` it still retargets this process...
		expect(parseArgs(["--socket", "/tmp/a.sock", "gjc"]).socket).toBe("/tmp/a.sock");
		// ...but after `--` it is payload for the subcommand and must survive intact,
		// or `gajaeway gjc -- --socket X` would silently retarget the CLI instead of
		// reaching the gateway as an operator token.
		const parsed = parseArgs(["gjc", "--", "--socket", "/tmp/b.sock"]);
		expect(parsed.socket).not.toBe("/tmp/b.sock");
		expect(parsed.rest).toEqual(["--", "--socket", "/tmp/b.sock"]);
	});
});

describe("ARCH-007 drift guard (AC-20)", () => {
	test("the CLI source contains no copy of the gjc flag allowlist", () => {
		const source = readFileSync(join(import.meta.dir, "..", "src", "gjc.ts"), "utf8");
		// Strip comments: the invariant is documented in prose on purpose, and that
		// documentation must not be what trips this guard.
		const code = source
			.replace(/\/\*[\s\S]*?\*\//g, "")
			.split("\n")
			.filter((line) => !line.trim().startsWith("//"))
			.join("\n");

		// These are the gateway's classification vocabulary. If any appears in CLI
		// code, flag policy has leaked out of `classifyGjcWrapperFlags`.
		for (const token of [
			"--session-dir",
			"--append-system-prompt",
			"--system-prompt",
			"--thinking",
			"--worktree",
			"--mpreset",
			"--fork",
			"--no-session",
		]) {
			expect(code).not.toContain(token);
		}
	});

	test("the CLI knows only its own wrapper-owned flag", () => {
		const source = readFileSync(join(import.meta.dir, "..", "src", "gjc.ts"), "utf8");
		const code = source
			.replace(/\/\*[\s\S]*?\*\//g, "")
			.split("\n")
			.filter((line) => !line.trim().startsWith("//"))
			.join("\n");
		expect(code).toContain('"--new"');
	});
});
