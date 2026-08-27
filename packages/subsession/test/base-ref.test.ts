import { describe, expect, test } from "bun:test";
import { assertBaseIsAncestor, BaseRefResolutionError, type GitResult, resolveBaseRef } from "../src/base-ref";

const SHA_MAIN = "7e0150e0000000000000000000000000000000aa";
const SHA_DEV = "1111111111111111111111111111111111111111";

function fakeGit(refs: Record<string, string>, originHead?: string) {
	const calls: string[][] = [];
	const runGit = async (args: readonly string[]): Promise<GitResult> => {
		calls.push([...args]);
		if (args[0] === "symbolic-ref") {
			return originHead
				? { exitCode: 0, stdout: `refs/remotes/${originHead}\n`, stderr: "" }
				: { exitCode: 1, stdout: "", stderr: "" };
		}
		if (args[0] === "rev-parse") {
			const ref = (args[3] ?? "").replace(/\^\{commit\}$/, "");
			const sha = refs[ref];
			return sha ? { exitCode: 0, stdout: `${sha}\n`, stderr: "" } : { exitCode: 1, stdout: "", stderr: "" };
		}
		if (args[0] === "merge-base") {
			const [, , base, head] = args;
			return base && head && refs[head] === refs[base]
				? { exitCode: 0, stdout: "", stderr: "" }
				: { exitCode: 1, stdout: "", stderr: "" };
		}
		return { exitCode: 1, stdout: "", stderr: "unexpected" };
	};
	return { runGit, calls };
}

describe("resolveBaseRef", () => {
	test("prefers the caller's explicit ref over configuration", async () => {
		const { runGit } = fakeGit({ "origin/main": SHA_MAIN, "origin/dev": SHA_DEV }, "origin/main");
		const resolved = await resolveBaseRef({
			explicitRef: "origin/main",
			configuredRef: "origin/dev",
			runGit,
		});
		expect(resolved).toEqual({ baseRef: "origin/main", baseSha: SHA_MAIN, source: "explicit" });
	});

	test("falls back to the configured integration branch", async () => {
		const { runGit } = fakeGit({ "origin/dev": SHA_DEV }, "origin/main");
		const resolved = await resolveBaseRef({ configuredRef: "origin/dev", runGit });
		expect(resolved.source).toBe("configured");
		expect(resolved.baseSha).toBe(SHA_DEV);
	});

	test("falls back to origin/HEAD when nothing is configured", async () => {
		const { runGit } = fakeGit({ "origin/main": SHA_MAIN }, "origin/main");
		const resolved = await resolveBaseRef({ runGit });
		expect(resolved).toEqual({ baseRef: "origin/main", baseSha: SHA_MAIN, source: "origin-head" });
	});

	test("skips a configured branch that does not exist in this repository", async () => {
		// gajae-way has no origin/dev: the configured default must not win by name.
		const { runGit } = fakeGit({ "origin/main": SHA_MAIN }, "origin/main");
		const resolved = await resolveBaseRef({ configuredRef: "origin/dev", runGit });
		expect(resolved.baseRef).toBe("origin/main");
		expect(resolved.source).toBe("origin-head");
	});

	test("fails closed when no candidate resolves", async () => {
		const { runGit } = fakeGit({});
		await expect(resolveBaseRef({ runGit })).rejects.toBeInstanceOf(BaseRefResolutionError);
	});

	test("rejects an explicit ref that does not resolve instead of silently degrading", async () => {
		const { runGit } = fakeGit({ "origin/main": SHA_MAIN }, "origin/main");
		await expect(resolveBaseRef({ explicitRef: "origin/nope", runGit })).rejects.toThrow(
			/explicit baseRef origin\/nope/,
		);
	});

	test("never resolves to a bare local HEAD", async () => {
		const { runGit, calls } = fakeGit({ HEAD: SHA_MAIN }, undefined);
		await expect(resolveBaseRef({ runGit })).rejects.toBeInstanceOf(BaseRefResolutionError);
		expect(calls.some((call) => call.join(" ").includes("HEAD^{commit}"))).toBe(false);
	});
});

describe("assertBaseIsAncestor", () => {
	test("accepts a lane head that descends from the base", async () => {
		const { runGit } = fakeGit({ [SHA_MAIN]: SHA_MAIN, "feat/x": SHA_MAIN });
		await expect(assertBaseIsAncestor(runGit, SHA_MAIN, "feat/x")).resolves.toBeUndefined();
	});

	test("rejects a divergent lane head", async () => {
		const { runGit } = fakeGit({ [SHA_MAIN]: SHA_MAIN, "feat/x": SHA_DEV });
		await expect(assertBaseIsAncestor(runGit, SHA_MAIN, "feat/x")).rejects.toThrow(/not an ancestor/);
	});
});
