import { afterEach, expect, test } from "bun:test";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { parseConfigFile } from "../src/config";
import { eventually, noticeOrigin, WorkFixture } from "./fixtures/work-lane-server";

const cleanups: Array<() => Promise<void>> = [];
afterEach(async () => {
	for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
});

/**
 * A real check command: unmet on its first run, met afterwards. It records the
 * lane identity it was given, so the test can tell which attempts it judged.
 */
async function flakyCheck(): Promise<{ readonly command: string; readonly seen: () => Promise<string[]> }> {
	const directory = await mkdtemp(join(tmpdir(), "gajaeway-completion-e2e-"));
	cleanups.push(() => rm(directory, { recursive: true, force: true }));
	const command = join(directory, "check.sh");
	const seen = join(directory, "seen");
	await writeFile(
		command,
		[
			"#!/bin/sh",
			`printf '%s %s\\n' "$GAJAEWAY_LANE_NAME" "$GAJAEWAY_OP_REF" >> ${JSON.stringify(seen)}`,
			`if [ "$(wc -l < ${JSON.stringify(seen)})" -eq 1 ]; then echo "C2 unmet: no test evidence"; exit 1; fi`,
			"exit 0",
			"",
		].join("\n"),
	);
	await chmod(command, 0o755);
	return { command, seen: async () => (await readFile(seen, "utf8")).trimEnd().split("\n") };
}

test("work.completionCheck from the gateway config runs the real command, re-steers once, then reports once", async () => {
	const check = await flakyCheck();
	// The same block an operator writes in config.json must survive the strict parser.
	const { work } = parseConfigFile({
		schemaVersion: 1,
		work: { completionCheck: { command: [check.command], timeoutMs: 10_000, maxResteers: 2 } },
	});
	const f = await WorkFixture.create({ completionCheck: work?.completionCheck });
	cleanups.push(() => f.cleanup());
	await f.start();
	const client = await f.connect();
	const started = await client.request("work.start", { name: "gated", text: "fixture-owned work", cwd: f.home });
	expect(started.error).toBeUndefined();
	const first: { opRef: string; sessionId: string } = started.result;

	await f.control(first.opRef, { terminal: true, text: "claimed done" });
	const sends = await eventually(
		() => f.calls("send"),
		(calls) => calls.length === 2,
		"completion re-steer send",
	);
	const resteer = sends[1]?.input as { opRef: string; sessionId: string; text: string };
	expect(resteer.sessionId).toBe(first.sessionId);
	expect(resteer.opRef).not.toBe(first.opRef);
	expect(resteer.text).toContain("Completion check failed (re-steer 1/2).");
	expect(resteer.text).toContain("C2 unmet: no test evidence");
	const parked = f.snapshot();
	expect(parked.runtimes.find((runtime) => runtime.opRef === first.opRef)).toMatchObject({ decision: "resteered" });
	expect(parked.deliveries).toEqual([]);
	expect(client.frames.filter((frame) => frame.event === "chat.message")).toEqual([]);

	await f.control(resteer.opRef, { terminal: true, text: "really done" });
	const done = await eventually(
		() => f.snapshot(),
		(snapshot) => snapshot.runtimes.length === 2 && snapshot.runtimes.every((runtime) => runtime.settledAt !== null),
		"both attempts settled",
	);
	expect(done.jobs[0].attempts.map((attempt: { sessionId: string }) => attempt.sessionId)).toEqual([
		first.sessionId,
		first.sessionId,
	]);
	expect(done.jobs[0].completionCheck).toBeUndefined();
	const reports = await eventually(
		() => client.frames.filter((frame) => frame.event === "chat.message"),
		(frames) => frames.length > 0,
		"lane report delivery",
	);
	expect(reports).toHaveLength(1);
	expect(reports[0].payload).toMatchObject({ origin: noticeOrigin, text: "[lane gated] completed: really done" });
	expect(f.calls("send")).toHaveLength(2);
	expect(await check.seen()).toEqual([`gated ${first.opRef}`, `gated ${resteer.opRef}`]);
}, 30_000);
