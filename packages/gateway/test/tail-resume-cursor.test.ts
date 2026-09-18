import { expect, test } from "bun:test";
import type { CliRunner } from "@gajae-gateway/subsession";
import { TailRunner } from "../src/orchestrator/tail-runner";

/**
 * Resumable polling against gjc >= 0.17.2 (`session tail` returns `cursor`,
 * and honours `--after-transcript-id` on resume). Before that pairing the
 * gateway replayed the whole session on every poll and rejected the replay
 * as pre-turn history - 614 frames per turn, measured 2026-09-18 - and a
 * correctly-resumed poll still counted zero tool calls because tool use lives
 * in transcript rows, which a bare resume does not page.
 */

async function eventually(predicate: () => boolean, message: string): Promise<void> {
	for (let attempt = 0; attempt < 200; attempt++) {
		if (predicate()) return;
		await Bun.sleep(5);
	}
	expect(predicate(), message).toBe(true);
}

const ok = (result: Record<string, unknown>) => ({
	exitCode: 0,
	stdout: JSON.stringify({ ok: true, result }),
	stderr: "",
});
const fail = (code: string) => ({
	exitCode: 1,
	stdout: JSON.stringify({ ok: false, error: { code, message: code } }),
	stderr: "",
});
const row = (id: string, content: unknown) => ({
	kind: "transcript",
	id,
	payload: { role: "assistant", ts: "2026-09-18T00:00:00.000Z", content },
});

test("a poll stores the returned cursor and the last transcript row id, and sends both on the next poll", async () => {
	const argv: string[][] = [];
	let polls = 0;
	const run: CliRunner = async (args) => {
		if (!args.includes("tail")) return { exitCode: 0, stdout: "{}", stderr: "" };
		argv.push([...args]);
		polls++;
		if (polls === 1)
			return ok({
				items: [row("r1", [{ type: "text", text: "a" }]), row("r2", [{ type: "toolCall", name: "read" }])],
				cursor: "cur-1",
				terminal: false,
			});
		return ok({ items: [], cursor: "cur-2", terminal: false });
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-resume", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({ sessionId: "s", brokerGeneration: 1, repo: "/tmp/gajaeway-resume" });
	try {
		await tail.markAccepted("op-1");
		await eventually(() => polls >= 2, "second poll");
		const first = argv[0]!;
		const second = argv[1]!;
		// First poll: cursorless, no boundary.
		expect(first.includes("--cursor")).toBe(false);
		expect(first.includes("--after-transcript-id")).toBe(false);
		// Second poll: resumes from the returned cursor, after the last transcript row.
		expect(second[second.indexOf("--cursor") + 1]).toBe("cur-1");
		expect(second[second.indexOf("--after-transcript-id") + 1]).toBe("r2");
		expect(second.includes("--strict")).toBe(true);
	} finally {
		await tail.close();
	}
});

test("a synthetic generation:seq id is never used as the transcript boundary", async () => {
	const argv: string[][] = [];
	let polls = 0;
	const run: CliRunner = async (args) => {
		if (!args.includes("tail")) return { exitCode: 0, stdout: "{}", stderr: "" };
		argv.push([...args]);
		polls++;
		if (polls === 1)
			return ok({
				items: [
					{
						kind: "transcript",
						generation: 1,
						seq: 7,
						payload: { role: "assistant", content: [{ type: "text", text: "a" }] },
					},
				],
				cursor: "cur-1",
				terminal: false,
			});
		return ok({ items: [], cursor: "cur-2", terminal: false });
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-resume", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({ sessionId: "s", brokerGeneration: 1, repo: "/tmp/gajaeway-resume" });
	try {
		await tail.markAccepted("op-1");
		await eventually(() => polls >= 2, "second poll");
		expect(argv[1]!.includes("--after-transcript-id")).toBe(false);
	} finally {
		await tail.close();
	}
});

test("invalid_cursor / cursor_expired drop the cursor: the next poll is cursorless, and it is not an error", async () => {
	const argv: string[][] = [];
	const diagnostics: string[] = [];
	let polls = 0;
	const run: CliRunner = async (args) => {
		if (!args.includes("tail")) return { exitCode: 0, stdout: "{}", stderr: "" };
		argv.push([...args]);
		polls++;
		if (polls === 1) return ok({ items: [row("r1", [{ type: "text", text: "a" }])], cursor: "cur-1", terminal: false });
		if (polls === 2) return fail("invalid_cursor");
		if (polls === 3) return ok({ items: [], cursor: "cur-3", terminal: false });
		if (polls === 4) return fail("cursor_expired");
		return ok({ items: [], terminal: false });
	};
	const runner = new TailRunner({ run, repo: "/tmp/gajaeway-resume", pollIntervalMs: 1, sleep: (ms) => Bun.sleep(ms) });
	const tail = await runner.attach({
		sessionId: "s",
		brokerGeneration: 1,
		repo: "/tmp/gajaeway-resume",
		onDiagnostic: (line) => diagnostics.push(line),
	});
	try {
		await tail.markAccepted("op-1");
		await eventually(() => polls >= 5, "five polls");
		expect(argv[1]!.includes("--cursor")).toBe(true); // resumed with cur-1
		expect(argv[2]!.includes("--cursor")).toBe(false); // dropped after invalid_cursor
		expect(argv[3]!.includes("--cursor")).toBe(true); // resumed with cur-3
		expect(argv[4]!.includes("--cursor")).toBe(false); // dropped after cursor_expired
		expect(diagnostics.filter((d) => d.includes("tail_cursor_invalid"))).toHaveLength(2);
		expect(diagnostics.filter((d) => d.includes("tail_error"))).toEqual([]);
		// The transcript boundary survives a dropped cursor (it is about rows, not checkpoints).
		expect(argv[3]![argv[3]!.indexOf("--after-transcript-id") + 1]).toBe("r1");
	} finally {
		await tail.close();
	}
});
