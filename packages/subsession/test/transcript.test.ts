import { describe, expect, test } from "bun:test";
import type { CliResult, ControllerOptions } from "../src/cli";
import {
	assertNoRetentionGap,
	fetchLastAssistant,
	RetentionGapError,
	TranscriptIncompleteError,
} from "../src/transcript";

const WORKTREE = "/wt/subsession-runtime";
const SESSION = "ad2f2494-2584-4d13-b7b6-c6ac24a1087f";

const envelope = (result: unknown): CliResult => ({
	exitCode: 0,
	stdout: JSON.stringify({ ok: true, result }),
	stderr: "",
});

function pagedController(pages: readonly unknown[], calls: string[][] = []): ControllerOptions {
	let index = 0;
	return {
		repo: WORKTREE,
		run: async (args) => {
			calls.push([...args]);
			const page = pages[Math.min(index, pages.length - 1)];
			index += 1;
			return envelope(page);
		},
	};
}

describe("fetchLastAssistant", () => {
	test("uses the raw query hatch, never a filesystem path", async () => {
		const calls: string[][] = [];
		const options = pagedController([{ text: "done", page: { complete: true } }], calls);
		await fetchLastAssistant(options, SESSION);
		expect(calls[0]).toEqual([
			"sdk",
			"session",
			"raw",
			"query",
			SESSION,
			"--query",
			"session.last_assistant",
			"--repo",
			WORKTREE,
		]);
		expect(calls.flat().join(" ")).not.toContain(".gjc");
	});

	test("follows the continuation cursor until page.complete", async () => {
		const calls: string[][] = [];
		const options = pagedController(
			[
				{ text: "part-1 ", page: { complete: false, cursor: "c1" } },
				{ text: "part-2 ", page: { complete: false, cursor: "c2" } },
				{ text: "part-3", page: { complete: true } },
			],
			calls,
		);
		const result = await fetchLastAssistant(options, SESSION);
		expect(result).toMatchObject({ text: "part-1 part-2 part-3", pages: 3, complete: true });
		expect(calls[1]?.slice(-2)).toEqual(["--cursor", "c1"]);
		expect(calls[2]?.slice(-2)).toEqual(["--cursor", "c2"]);
	});

	test("refuses to return a partial body when the cursor is missing", async () => {
		const options = pagedController([{ text: "half", page: { complete: false } }]);
		await expect(fetchLastAssistant(options, SESSION)).rejects.toBeInstanceOf(TranscriptIncompleteError);
	});

	test("bounds a runaway cursor loop", async () => {
		const options = pagedController([{ text: "x", page: { complete: false, cursor: "same" } }]);
		await expect(fetchLastAssistant(options, SESSION, { maxPages: 3 })).rejects.toThrow(
			/did not complete within 3 pages/,
		);
	});

	test("accepts a top-level cursor as well as page.cursor", async () => {
		const options = pagedController([
			{ text: "a", cursor: "c1", page: { complete: false } },
			{ text: "b", page: { complete: true } },
		]);
		expect((await fetchLastAssistant(options, SESSION)).text).toBe("ab");
	});
});

describe("assertNoRetentionGap", () => {
	test("passes when tail reported no gap", () => {
		expect(() => assertNoRetentionGap(SESSION, [{ kind: "turn.progress" }])).not.toThrow();
	});

	test("throws on a retention gap and points back at status", () => {
		expect(() => assertNoRetentionGap(SESSION, [{ kind: "retention_gap" }])).toThrow(RetentionGapError);
		try {
			assertNoRetentionGap(SESSION, [{ reason: "retention_gap" }]);
		} catch (error) {
			expect((error as Error).message).toMatch(/reconcile the operation with status/);
		}
	});
});
